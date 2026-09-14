#include "eventflow/computation.hpp"
#include "status_test_support.hpp"
#include <gtest/gtest.h>
#include <map>

namespace eventflow::test {

struct EnvelopeEvent {
  std::string ticker;
  std::int64_t sequence = 0;
  double value = 0;
};

struct EnvelopeModelInput {
  double first = 0;
  double second = 0;
};

struct EnvelopeModelOutput {
  double score = 0;
};

} // namespace eventflow::test

namespace eventflow {

template <> struct SchemaTraits<test::EnvelopeEvent> {
  static auto fields() noexcept {
    using T = test::EnvelopeEvent;
    return std::tuple{field<&T::ticker>("ticker"), field<&T::sequence>("sequence"),
                      field<&T::value>("value")};
  }
};

template <> struct SchemaTraits<test::EnvelopeModelInput> {
  static auto fields() noexcept {
    using T = test::EnvelopeModelInput;
    return std::tuple{field<&T::first>("first"), field<&T::second>("second")};
  }
};

template <> struct SchemaTraits<test::EnvelopeModelOutput> {
  static auto fields() noexcept {
    return std::tuple{field<&test::EnvelopeModelOutput::score>("score")};
  }
};

} // namespace eventflow

namespace eventflow::test {
namespace {
using namespace std::chrono_literals;

StatusOr<EnvelopeModelInput> capture_pair(const EnvelopeEvent& first,
                                          const EnvelopeEvent& second) noexcept {
  return EnvelopeModelInput{.first = first.value, .second = second.value};
}

StatusOr<EnvelopeModelOutput> infer(const EnvelopeModelInput& input) noexcept {
  return EnvelopeModelOutput{.score = (input.first + input.second) * 0.5};
}

class RelationEnvelopeScopeTest : public testing::Test {
protected:
  Status start(bool unrelated) noexcept {
    EF_ASSIGN_OR_RETURN(auto catalog, Catalog::create());
    EF_ASSIGN_OR_RETURN(input,
                        catalog->define_sql("CREATE TABLE input(ticker STRING NOT NULL,sequence "
                                            "INT64 NOT NULL,value FLOAT64 NOT NULL,"
                                            "PRIMARY KEY(ticker,sequence)) PARTITION BY(ticker)"));
    EF_ASSIGN_OR_RETURN(
        auto session,
        catalog->define_candidate(
            {.name = "session",
             .scope_fields = {{.id = 1, .name = "ticker", .type = DataType::String}},
             .payload_fields = {{.id = 2, .name = "deadline", .type = DataType::Int64}}}));
    if (unrelated) {
      for (const auto sql :
           {"CREATE TABLE foreign_compound(ticker STRING NOT NULL,sequence INT64 NOT NULL,value "
            "FLOAT64 NOT NULL,PRIMARY KEY(ticker,sequence)) PARTITION BY(ticker,sequence)",
            "CREATE MATERIALIZED VIEW foreign_compound_mv AS SELECT ticker,sequence,value+1.0 AS "
            "value FROM foreign_compound",
            "CREATE MATERIALIZED VIEW foreign_compound_cascade AS SELECT value FROM "
            "foreign_compound_mv",
            "CREATE TABLE foreign_integer(sequence INT64 NOT NULL,value FLOAT64 NOT NULL,PRIMARY "
            "KEY(sequence)) PARTITION BY(sequence)",
            "CREATE MATERIALIZED VIEW foreign_integer_mv AS SELECT sequence,value*2.0 AS value "
            "FROM foreign_integer",
            "CREATE MATERIALIZED VIEW foreign_integer_cascade AS SELECT value FROM "
            "foreign_integer_mv"}) {
        EF_ASSIGN_OR_RETURN(auto relation, catalog->define_sql(sql));
        (void)relation;
      }
    }
    ProcessorConfig definition;
    definition.input_relations = {input};
    definition.requests.emplace();
    definition.requests->candidate = CandidateExpiryPolicy{
        .relation = session,
        .deadline_column = "deadline",
        .version = 1,
        .on_source = [session](RoundContext& round, const ProcessorEvent& event) noexcept
            -> StatusOr<std::optional<CandidateActivation>> {
          EF_ASSIGN_OR_RETURN(auto existing, round.read_relation(session, event.scope));
          if (existing)
            return std::optional<CandidateActivation>{};
          return std::optional<CandidateActivation>{
              CandidateActivation{.expected_epoch = 0, .payload = {std::int64_t(10'000'000)}}};
        }};
    const auto source =
        EventRef::source<EnvelopeEvent>({.kind = "input", .payload_relation = input})
            .key_by(&EnvelopeEvent::ticker);
    const auto pattern =
        Pattern::begin<EnvelopeEvent>("first").next("second").within(1s).after_match(
            CepSkipStrategy::SkipPastLastEvent);
    auto declaration =
        Trigger::pattern("pair", source, pattern)
            .in_session(SessionRef::candidate(session))
            .capture_match<&capture_pair>({"first", "second"}, "capture_pair.v1")
            .request(Function<&infer>{.id = "mean_score",
                                      .version = 1,
                                      .implementation_identity = "mean_score.v1"},
                     {.mode = RequestTriggerMode::Wait, .pending_for = 1s, .lifetime = 2s});
    EF_ASSIGN_OR_RETURN(auto snapshot, catalog->snapshot());
    EF_ASSIGN_OR_RETURN(auto bound, TriggerSet::bind(snapshot, definition, {declaration},
                                                     EngineConfig{.shard_count = 1}));
    const auto latest_id =
        bound.explanation["triggers"][0]["generated"]["latest_relation"].get<RelationId>();
    EF_ASSIGN_OR_RETURN(auto latest, bound.catalog.find(latest_id));
    latest_name = latest->name;
    EF_ASSIGN_OR_RETURN(auto memory,
                        MemoryStateStore::open({.shard_count = 1, .shard_owners = {}}));
    store = std::shared_ptr<IStateStore>(std::move(memory));
    EF_ASSIGN_OR_RETURN(owner,
                        EventProcessor::open(store, bound.catalog, EngineConfig{.shard_count = 1},
                                             bound.definition));
    EF_ASSIGN_OR_RETURN(executor, SimpleFunctionExecutor::create(bound.registry));
    return {};
  }

  ProcessorEvent source_event(std::int64_t offset, double value) const noexcept {
    const Row row{std::string("T"), offset, value};
    return {.source = {.source_id = "test", .topic = "input", .partition = 0, .offset = offset},
            .scope = {std::string("T")},
            .kind = "input",
            .values = row,
            .changes = {{.relation = input, .identity = {row[0], row[1]}, .row = row}},
            .processing_time_us = (offset + 1) * 100};
  }

  Status settle() noexcept {
    for (std::size_t round = 0; round < 32; ++round) {
      EF_ASSIGN_OR_RETURN(auto progress, owner->pump(300));
      EF_RETURN_IF_ERROR(progress.operation_status);
      EF_ASSIGN_OR_RETURN(auto active, owner->active_requests());
      if (active.empty() && !progress.work_remaining)
        return {};
      for (const auto& record : active) {
        EF_ASSIGN_OR_RETURN(
            auto claimed,
            owner->claim_tasks(
                {.scope = record.admission.scope, .request_id = record.admission.request_id}, 300));
        EF_RETURN_IF_ERROR(claimed.operation_status);
      }
      EF_ASSIGN_OR_RETURN(active, owner->active_requests());
      std::map<std::string, RequestKey> owners;
      for (const auto& record : active)
        for (const auto& node : record.state.nodes)
          if (node.phase == RequestNodePhase::Running)
            owners.emplace(node.task_id, RequestKey{.scope = record.admission.scope,
                                                    .request_id = record.admission.request_id});
      EF_ASSIGN_OR_RETURN(auto tasks, owner->pending_tasks(64));
      for (const auto& task : tasks)
        EF_RETURN_IF_ERROR(executor->submit(task));
      EF_ASSIGN_OR_RETURN(auto count, executor->run_ready(300));
      (void)count;
      EF_ASSIGN_OR_RETURN(auto completions, executor->poll(64));
      for (const auto& completion : completions) {
        const auto found = owners.find(completion.task_id);
        if (found == owners.end())
          return Status(ErrorCode::Internal, "Completion has no active request");
        EF_ASSIGN_OR_RETURN(auto result, owner->complete_request(found->second, completion, 300));
        EF_RETURN_IF_ERROR(result.operation_status);
      }
    }
    return Status(ErrorCode::ResourceExhausted, "Request did not settle within its test budget");
  }

  RelationId input = 0;
  std::string latest_name;
  std::shared_ptr<IStateStore> store;
  std::unique_ptr<EventProcessor> owner;
  std::unique_ptr<SimpleFunctionExecutor> executor;
};

TEST_F(RelationEnvelopeScopeTest, UnrelatedCompoundAndIntegerCascadesDoNotAffectRequestInference) {
  auto opened = start(true);
  ASSERT_TRUE(opened.ok()) << opened.message();
  auto first = owner->process(source_event(0, 2.0));
  ASSERT_TRUE(first.ok()) << first.status().message();
  ASSERT_TRUE(first->operation_status.ok()) << first->operation_status.message();
  EXPECT_TRUE(first->input_committed);
  auto second = owner->process(source_event(1, 4.0));
  ASSERT_TRUE(second.ok());
  ASSERT_TRUE(second->operation_status.ok()) << second->operation_status.message();
  EXPECT_TRUE(second->input_committed);
  auto finished = settle();
  ASSERT_TRUE(finished.ok()) << finished.message();
  auto result = owner->query_sql("SELECT score FROM " + latest_name + " WHERE ticker='T'");
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result->rows.size(), 1U);
  const auto* score = std::get_if<double>(&result->rows[0][0]);
  ASSERT_NE(score, nullptr);
  EXPECT_DOUBLE_EQ(*score, 3.0);
  EXPECT_EQ(executor->metrics().callback_calls, 1U);
}

TEST_F(RelationEnvelopeScopeTest, WrongDirectScopeReturnsStatusAndPreservesTheOwnerRevision) {
  ASSERT_TRUE(start(false).ok());
  const auto revision = take(store->status(0)).revision;
  for (const Row& scope : {Row{}, Row{std::int64_t(1)}, Row{std::string("T"), std::int64_t(1)}}) {
    auto input_event = source_event(0, 2.0);
    input_event.scope = scope;
    auto rejected = owner->process(input_event);
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), ErrorCode::InvalidArgument);
    EXPECT_EQ(take(store->status(0)).revision, revision);
  }
  auto valid = owner->process(source_event(0, 2.0));
  ASSERT_TRUE(valid.ok());
  EXPECT_TRUE(valid->input_committed);
}

} // namespace
} // namespace eventflow::test
