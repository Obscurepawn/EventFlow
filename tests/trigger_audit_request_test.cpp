#include "eventflow/computation.hpp"
#include "status_test_support.hpp"
#include <gtest/gtest.h>

namespace eventflow::test {

struct AuditSignal {
  std::string ticker;
  std::int64_t sequence = 0;
  double value = 0.0;
};

struct AuditQuote {
  std::string ticker;
  double value = 0.0;
};

struct AuditModelInput {
  double value = 0.0;
};

struct AuditModelOutput {
  double score = 0.0;
};

} // namespace eventflow::test

namespace eventflow {

template <> struct SchemaTraits<test::AuditSignal> {
  static auto fields() noexcept {
    using T = test::AuditSignal;
    return std::tuple{field<&T::ticker>("ticker"), field<&T::sequence>("sequence"),
                      field<&T::value>("value")};
  }
};

template <> struct SchemaTraits<test::AuditQuote> {
  static auto fields() noexcept {
    using T = test::AuditQuote;
    return std::tuple{field<&T::ticker>("ticker"), field<&T::value>("value")};
  }
};

template <> struct SchemaTraits<test::AuditModelInput> {
  static auto fields() noexcept {
    return std::tuple{field<&test::AuditModelInput::value>("value")};
  }
};

template <> struct SchemaTraits<test::AuditModelOutput> {
  static auto fields() noexcept {
    return std::tuple{field<&test::AuditModelOutput::score>("score")};
  }
};

} // namespace eventflow

namespace eventflow::test {
namespace {
using namespace std::chrono_literals;

StatusOr<AuditModelInput> capture_quote(const AuditQuote& quote) noexcept {
  return AuditModelInput{.value = quote.value};
}

StatusOr<AuditModelInput> capture_pair(const AuditSignal& first,
                                       const AuditSignal& second) noexcept {
  return AuditModelInput{.value = (first.value + second.value) * 0.5};
}

StatusOr<bool> positive_signal(const AuditSignal& signal) noexcept {
  return signal.value > 0.0;
}

StatusOr<AuditModelOutput> compute_score(const AuditModelInput& input) noexcept {
  return AuditModelOutput{.score = input.value * 2.0};
}

class TriggerAuditRequestTest : public testing::Test {
protected:
  Status start(RequestTriggerMode mode = RequestTriggerMode::Wait, bool once_per_candidate = false,
               bool pair = false) noexcept {
    EF_ASSIGN_OR_RETURN(auto catalog, Catalog::create());
    EF_ASSIGN_OR_RETURN(
        signals,
        catalog->define_sql("CREATE TABLE signals(ticker STRING NOT NULL,sequence INT64 NOT NULL,"
                            "value FLOAT64 NOT NULL,PRIMARY KEY(ticker,sequence)) "
                            "PARTITION BY(ticker)"));
    EF_ASSIGN_OR_RETURN(quotes, catalog->define_sql("CREATE TABLE quotes(ticker STRING NOT NULL,"
                                                    "value FLOAT64 NOT NULL,PRIMARY KEY(ticker)) "
                                                    "PARTITION BY(ticker)"));
    EF_ASSIGN_OR_RETURN(
        const auto session,
        catalog->define_candidate(
            {.name = "session",
             .scope_fields =
                 {{.id = 1, .name = "ticker", .type = DataType::String, .nullable = false}},
             .payload_fields = {
                 {.id = 2, .name = "deadline", .type = DataType::Int64, .nullable = false}}}));
    ProcessorConfig definition;
    definition.input_relations = {signals, quotes};
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
              CandidateActivation{.expected_epoch = 0, .payload = {std::int64_t{10'000'000}}}};
        }};
    const auto function = Function<&compute_score>{
        .id = "audit_score", .version = 1, .implementation_identity = "audit_score.v1"};
    const auto request = TriggerRequest{.id = "audit_request",
                                        .mode = mode,
                                        .pending_for = 1s,
                                        .lifetime = 2s,
                                        .once_per_candidate = once_per_candidate};
    const auto source = EventRef::source({.kind = "signal", .payload_relation = signals});
    auto declaration = Trigger::on_event("positive_signal", source)
                           .when(source.column<double>("value") > 0.0)
                           .emit("signal_ready")
                           .in_session(SessionRef::candidate(session))
                           .capture_current<&capture_quote>(quotes, "capture_quote.v1")
                           .request(function, request);
    if (pair) {
      const auto typed_source =
          EventRef::source<AuditSignal>({.kind = "signal", .payload_relation = signals})
              .key_by(&AuditSignal::ticker);
      const auto pattern = Pattern::begin<AuditSignal>("first")
                               .where<&positive_signal>("positive_signal.v1")
                               .next("second")
                               .where<&positive_signal>("positive_signal.v1")
                               .within(1s)
                               .after_match(CepSkipStrategy::SkipPastLastEvent);
      declaration = Trigger::pattern("positive_pair", typed_source, pattern)
                        .in_session(SessionRef::candidate(session))
                        .capture_match<&capture_pair>({"first", "second"}, "capture_pair.v1")
                        .request(function, request);
    }
    EF_ASSIGN_OR_RETURN(auto snapshot, catalog->snapshot());
    EF_ASSIGN_OR_RETURN(bound, TriggerSet::bind(snapshot, definition, {declaration},
                                                EngineConfig{.shard_count = 1}));
    EF_ASSIGN_OR_RETURN(auto memory,
                        MemoryStateStore::open({.shard_count = 1, .shard_owners = {}}));
    store = std::shared_ptr<MemoryStateStore>(std::move(memory));
    EF_ASSIGN_OR_RETURN(owner,
                        EventProcessor::open(store, bound.catalog, EngineConfig{.shard_count = 1},
                                             bound.definition));
    EF_ASSIGN_OR_RETURN(executor, SimpleFunctionExecutor::create(bound.registry));
    capture_name = bound.explanation["triggers"][0]["generated"]["capture_name"].get<std::string>();
    latest_name = bound.explanation["triggers"][0]["generated"]["latest_name"].get<std::string>();
    return {};
  }

  ProcessorEvent signal(std::int64_t offset, double value = 1.0) const noexcept {
    const Row row{std::string("T"), offset, value};
    return {.source = {.source_id = "test", .topic = "events", .partition = 0, .offset = offset},
            .scope = {std::string("T")},
            .kind = "signal",
            .values = row,
            .changes = {{.relation = signals, .identity = {row[0], row[1]}, .row = row}},
            .processing_time_us = (offset + 1) * 100};
  }

  ProcessorEvent quote(std::int64_t offset, double value) const noexcept {
    const Row row{std::string("T"), value};
    return {.source = {.source_id = "test", .topic = "events", .partition = 0, .offset = offset},
            .scope = {std::string("T")},
            .kind = "quote",
            .values = row,
            .changes = {{.relation = quotes, .identity = {row[0]}, .row = row}},
            .processing_time_us = (offset + 1) * 100};
  }

  StatusOr<QueryResult> audit_rows() noexcept {
    return owner->query_sql(
        "SELECT occurrence_id,request_id,rule_id,emission,firing_revision,binding_id,plan,"
        "candidate_epoch,occurred_at_us,evaluated_at_us,source_offset FROM trigger_audit "
        "WHERE scope_key_0='T'");
  }

  StatusOr<QueryResult> captured_rows() noexcept {
    return owner->query_sql("SELECT * FROM " + capture_name + " WHERE ticker='T'");
  }

  Status run_function(const RequestKey& key, std::int64_t now) noexcept {
    EF_ASSIGN_OR_RETURN(auto claimed, owner->claim_tasks(key, now));
    EF_RETURN_IF_ERROR(claimed.operation_status);
    EF_ASSIGN_OR_RETURN(auto tasks, owner->pending_tasks(16));
    for (const auto& task : tasks)
      EF_RETURN_IF_ERROR(executor->submit(task));
    EF_ASSIGN_OR_RETURN(const auto count, executor->run_ready(now));
    (void)count;
    EF_ASSIGN_OR_RETURN(auto completions, executor->poll(16));
    for (const auto& completion : completions) {
      EF_ASSIGN_OR_RETURN(auto completed, owner->complete_request(key, completion, now));
      EF_RETURN_IF_ERROR(completed.operation_status);
    }
    return {};
  }

  Status restore_snapshot() noexcept {
    EF_ASSIGN_OR_RETURN(const auto saved, store->create_snapshot("pending-audit"));
    (void)saved;
    owner.reset();
    EF_ASSIGN_OR_RETURN(auto restored, MemoryStateStore::from_snapshot(*store, "pending-audit"));
    store = std::shared_ptr<MemoryStateStore>(std::move(restored));
    EF_ASSIGN_OR_RETURN(owner,
                        EventProcessor::open(store, bound.catalog, EngineConfig{.shard_count = 1},
                                             bound.definition));
    return {};
  }

  BoundComputation bound;
  RelationId signals = 0;
  RelationId quotes = 0;
  std::string capture_name;
  std::string latest_name;
  std::shared_ptr<MemoryStateStore> store;
  std::unique_ptr<EventProcessor> owner;
  std::unique_ptr<SimpleFunctionExecutor> executor;
};

TEST_F(TriggerAuditRequestTest, WaitRecordsProposedRequestBeforeCaptureAndKeepsItAfterCompletion) {
  const auto started = start();
  ASSERT_TRUE(started.ok()) << started.message();
  const auto fired = take(owner->process(signal(0)));
  ASSERT_TRUE(fired.operation_status.ok()) << fired.operation_status.message();
  ASSERT_TRUE(fired.input_committed);
  const auto pending = take(owner->pending_occurrences(16));
  ASSERT_EQ(pending.size(), 1U);
  auto queried = audit_rows();
  ASSERT_TRUE(queried.ok()) << queried.status().message();
  ASSERT_EQ(queried->rows.size(), 1U);
  const auto audit = queried->rows;
  EXPECT_EQ(std::get<std::string>(audit[0][0]), pending[0].id);
  EXPECT_EQ(std::get<std::string>(audit[0][2]), "positive_signal");
  EXPECT_EQ(std::get<std::string>(audit[0][3]), "signal_ready");
  EXPECT_EQ(std::get<std::string>(audit[0][4]), std::to_string(fired.commit.revision));
  EXPECT_EQ(std::get<std::string>(audit[0][5]), "audit_request");
  EXPECT_EQ(std::get<std::string>(audit[0][7]), "1");
  const RequestKey key{.scope = {std::string("T")},
                       .request_id = std::get<std::string>(audit[0][1])};
  EXPECT_FALSE(key.request_id.empty());
  EXPECT_EQ(owner->read_request(key).status().code(), ErrorCode::NotFound);
  EXPECT_TRUE(take(captured_rows()).rows.empty());
  auto waiting = take(owner->admit_occurrence(pending[0].id, 100));
  ASSERT_TRUE(waiting.operation_status.ok());
  ASSERT_TRUE(waiting.request_result);
  EXPECT_EQ(waiting.request_result->disposition, RequestDisposition::Deferred);
  EXPECT_EQ(waiting.request_result->key.request_id, key.request_id);
  EXPECT_FALSE(waiting.request_result->record);
  EXPECT_EQ(take(audit_rows()).rows, audit);

  ASSERT_TRUE(take(owner->process(quote(1, 7.0))).input_committed);
  auto admitted = take(owner->admit_occurrence(pending[0].id, 200));
  ASSERT_TRUE(admitted.operation_status.ok()) << admitted.operation_status.message();
  ASSERT_TRUE(admitted.request_result && admitted.request_result->record);
  EXPECT_EQ(admitted.request_result->disposition, RequestDisposition::Accepted);
  const auto& record = *admitted.request_result->record;
  EXPECT_EQ(record.admission.request_id, key.request_id);
  EXPECT_EQ(record.admission.trigger_id, pending[0].id);
  EXPECT_EQ(record.admission.plan, std::get<std::string>(audit[0][6]));
  EXPECT_GT(record.state.manifest.capture_revision, fired.commit.revision);
  EXPECT_GT(record.state.manifest.admission_revision, record.state.manifest.capture_revision);
  EXPECT_TRUE(take(owner->pending_occurrences(16)).empty());
  EXPECT_EQ(take(captured_rows()).rows,
            (std::vector<Row>{{std::string("T"), key.request_id, 7.0}}));
  ASSERT_TRUE(run_function(key, 200).ok());
  EXPECT_EQ(take(owner->read_request(key)).state.phase, RequestPhase::Success);
  auto latest = take(owner->query_sql("SELECT score FROM " + latest_name + " WHERE ticker='T'"));
  EXPECT_EQ(latest.rows, (std::vector<Row>{{14.0}}));
  EXPECT_EQ(take(audit_rows()).rows, audit);
}

TEST_F(TriggerAuditRequestTest, TryOnceWithoutCaptureKeepsAuditAfterSkippingTheRequest) {
  const auto started = start(RequestTriggerMode::TryOnceAtFiring);
  ASSERT_TRUE(started.ok()) << started.message();
  const auto fired = take(owner->process(signal(0)));
  ASSERT_TRUE(fired.operation_status.ok()) << fired.operation_status.message();
  ASSERT_TRUE(fired.input_committed);
  auto queried = audit_rows();
  ASSERT_TRUE(queried.ok()) << queried.status().message();
  ASSERT_EQ(queried->rows.size(), 1U);
  const auto audit = queried->rows;
  const RequestKey key{.scope = {std::string("T")},
                       .request_id = std::get<std::string>(audit[0][1])};
  EXPECT_FALSE(key.request_id.empty());
  EXPECT_EQ(owner->read_request(key).status().code(), ErrorCode::NotFound);
  EXPECT_TRUE(take(captured_rows()).rows.empty());
  EXPECT_TRUE(take(owner->pending_occurrences(16)).empty());
  const auto candidate = take(owner->read_candidate(key.scope));
  ASSERT_EQ(candidate.bindings.size(), 1U);
  EXPECT_EQ(candidate.bindings[0].reason, RequestPendingReason::NotReady);
  EXPECT_EQ(candidate.bindings[0].skipped_count, 1U);
  EXPECT_FALSE(candidate.bindings[0].accepted);
  bool emitted = false;
  for (const auto& round : fired.committed_rounds)
    for (const auto& occurrence : round.occurrences) {
      EXPECT_EQ(occurrence.id, std::get<std::string>(audit[0][0]));
      EXPECT_EQ(std::get<std::string>(audit[0][4]), std::to_string(round.revision));
      emitted = true;
    }
  EXPECT_TRUE(emitted);

  ASSERT_TRUE(take(owner->process(quote(1, 9.0))).input_committed);
  ASSERT_TRUE(take(owner->pump(200)).operation_status.ok());
  EXPECT_EQ(owner->read_request(key).status().code(), ErrorCode::NotFound);
  EXPECT_TRUE(take(owner->active_requests()).empty());
  EXPECT_TRUE(take(captured_rows()).rows.empty());
  EXPECT_EQ(take(audit_rows()).rows, audit);
}

TEST_F(TriggerAuditRequestTest, FalsePredicatesAndConsumedOnceBindingsDoNotCreateAuditRows) {
  const auto started = start(RequestTriggerMode::Wait, true);
  ASSERT_TRUE(started.ok()) << started.message();
  ASSERT_TRUE(take(owner->process(signal(0, -1.0))).input_committed);
  auto empty = audit_rows();
  ASSERT_TRUE(empty.ok()) << empty.status().message();
  EXPECT_TRUE(empty->rows.empty());
  EXPECT_TRUE(take(owner->pending_occurrences(16)).empty());
  ASSERT_TRUE(take(owner->process(quote(1, 5.0))).input_committed);
  ASSERT_TRUE(take(owner->process(signal(2))).input_committed);
  const auto pending = take(owner->pending_occurrences(16));
  ASSERT_EQ(pending.size(), 1U);
  const auto audit = take(audit_rows()).rows;
  ASSERT_EQ(audit.size(), 1U);
  auto admitted = take(owner->admit_occurrence(pending[0].id, 300));
  ASSERT_TRUE(admitted.operation_status.ok());
  ASSERT_TRUE(admitted.request_result && admitted.request_result->record);
  EXPECT_EQ(admitted.request_result->disposition, RequestDisposition::Accepted);
  const auto candidate = take(owner->read_candidate({std::string("T")}));
  ASSERT_EQ(candidate.bindings.size(), 1U);
  EXPECT_TRUE(candidate.bindings[0].accepted);
  const auto consumed = take(owner->process(signal(3)));
  ASSERT_TRUE(consumed.operation_status.ok());
  EXPECT_TRUE(consumed.input_committed);
  for (const auto& round : consumed.committed_rounds)
    EXPECT_TRUE(round.occurrences.empty());
  EXPECT_TRUE(take(owner->pending_occurrences(16)).empty());
  EXPECT_EQ(take(audit_rows()).rows, audit);
}

TEST_F(TriggerAuditRequestTest, TypedCepMatchIsAuditedOnceAndRetainsItsCapturedValues) {
  const auto started = start(RequestTriggerMode::Wait, false, true);
  ASSERT_TRUE(started.ok()) << started.message();
  ASSERT_TRUE(take(owner->process(signal(0, 2.0))).input_committed);
  auto empty = audit_rows();
  ASSERT_TRUE(empty.ok()) << empty.status().message();
  EXPECT_TRUE(empty->rows.empty());
  const auto matched = take(owner->process(signal(1, 4.0)));
  ASSERT_TRUE(matched.operation_status.ok());
  ASSERT_TRUE(matched.input_committed);
  const auto occurrences = take(owner->pending_occurrences(16));
  ASSERT_EQ(occurrences.size(), 1U);
  ASSERT_TRUE(occurrences[0].cep_match);
  const auto audit = take(audit_rows()).rows;
  ASSERT_EQ(audit.size(), 1U);
  EXPECT_EQ(std::get<std::string>(audit[0][0]), occurrences[0].id);
  EXPECT_EQ(std::get<std::string>(audit[0][2]), "positive_pair");
  EXPECT_EQ(std::get<std::string>(audit[0][3]), "match");
  EXPECT_EQ(std::get<std::string>(audit[0][4]), std::to_string(matched.commit.revision));
  EXPECT_EQ(std::get<std::int64_t>(audit[0][8]), occurrences[0].emission.time_us);
  EXPECT_EQ(std::get<std::int64_t>(audit[0][9]), occurrences[0].evaluated_at_us);
  EXPECT_EQ(std::get<std::int64_t>(audit[0][10]), 1);
  const RequestKey key{.scope = {std::string("T")},
                       .request_id = std::get<std::string>(audit[0][1])};
  EXPECT_EQ(owner->read_request(key).status().code(), ErrorCode::NotFound);
  const auto later = take(owner->process(signal(2, 99.0)));
  ASSERT_TRUE(later.operation_status.ok()) << later.operation_status.message();
  ASSERT_TRUE(later.input_committed);
  const auto progress = take(owner->pump(300));
  ASSERT_TRUE(progress.operation_status.ok()) << progress.operation_status.message();
  const auto admitted = owner->read_request(key);
  ASSERT_TRUE(admitted.ok()) << admitted.status().message();
  EXPECT_EQ(admitted->admission.request_id, key.request_id);
  EXPECT_EQ(admitted->admission.trigger_id, occurrences[0].id);
  EXPECT_EQ(admitted->state.phase, RequestPhase::Accepted);
  EXPECT_TRUE(take(owner->pending_occurrences(16)).empty());
  EXPECT_EQ(take(audit_rows()).rows, audit);
  ASSERT_TRUE(run_function(key, 300).ok());
  EXPECT_EQ(take(owner->read_request(key)).state.phase, RequestPhase::Success);
  auto latest = take(owner->query_sql("SELECT score FROM " + latest_name + " WHERE ticker='T'"));
  EXPECT_EQ(latest.rows, (std::vector<Row>{{6.0}}));
  EXPECT_EQ(take(audit_rows()).rows, audit);
}

TEST_F(TriggerAuditRequestTest, SnapshotAndDuplicateSourceReplayPreservePendingAuditAndRequestIds) {
  const auto started = start();
  ASSERT_TRUE(started.ok()) << started.message();
  const auto input = signal(0);
  ASSERT_TRUE(take(owner->process(input)).input_committed);
  auto queried = audit_rows();
  ASSERT_TRUE(queried.ok()) << queried.status().message();
  ASSERT_EQ(queried->rows.size(), 1U);
  const auto audit = queried->rows;
  const auto occurrence_id = std::get<std::string>(audit[0][0]);
  const RequestKey key{.scope = {std::string("T")},
                       .request_id = std::get<std::string>(audit[0][1])};
  auto deferred = take(owner->admit_occurrence(occurrence_id, 100));
  ASSERT_TRUE(deferred.operation_status.ok());
  ASSERT_TRUE(deferred.request_result);
  EXPECT_EQ(deferred.request_result->disposition, RequestDisposition::Deferred);
  ASSERT_TRUE(restore_snapshot().ok());
  EXPECT_EQ(take(audit_rows()).rows, audit);
  EXPECT_EQ(owner->read_request(key).status().code(), ErrorCode::NotFound);
  auto pending = take(owner->pending_occurrences(16));
  ASSERT_EQ(pending.size(), 1U);
  EXPECT_EQ(pending[0].id, occurrence_id);
  const auto replayed = take(owner->process(input));
  ASSERT_TRUE(replayed.operation_status.ok());
  EXPECT_TRUE(replayed.input_committed);
  EXPECT_TRUE(replayed.duplicate);
  EXPECT_EQ(take(audit_rows()).rows, audit);
  EXPECT_EQ(take(owner->pending_occurrences(16)).size(), 1U);
  const auto positions = take(owner->processed_positions());
  ASSERT_EQ(positions.size(), 1U);
  EXPECT_EQ(positions[0].next_offset, 1);
  ASSERT_TRUE(take(owner->process(quote(1, 8.0))).input_committed);
  auto admitted = take(owner->admit_occurrence(occurrence_id, 200));
  ASSERT_TRUE(admitted.operation_status.ok());
  ASSERT_TRUE(admitted.request_result && admitted.request_result->record);
  EXPECT_EQ(admitted.request_result->record->admission.request_id, key.request_id);
  EXPECT_EQ(admitted.request_result->record->admission.trigger_id, occurrence_id);
  EXPECT_TRUE(take(owner->pending_occurrences(16)).empty());
  EXPECT_EQ(take(audit_rows()).rows, audit);
}

} // namespace
} // namespace eventflow::test
