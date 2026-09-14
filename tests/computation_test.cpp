#include "eventflow/computation.hpp"
#include "status_test_support.hpp"
#include <gtest/gtest.h>
#include <map>

namespace eventflow::test {
struct ModuleEvent {
  std::string ticker;
  std::int64_t sequence = 0;
  double value = 0;
};

struct ModelInput {
  double first = 0;
  double second = 0;
};

struct ModelOutput {
  double score = 0;
};
} // namespace eventflow::test

namespace eventflow {
template <> struct SchemaTraits<test::ModuleEvent> {
  static auto fields() noexcept {
    using T = test::ModuleEvent;
    return std::tuple{field<&T::ticker>("ticker"), field<&T::sequence>("sequence"),
                      field<&T::value>("value")};
  }
};

template <> struct SchemaTraits<test::ModelInput> {
  static auto fields() noexcept {
    using T = test::ModelInput;
    return std::tuple{field<&T::first>("first"), field<&T::second>("second")};
  }
};

template <> struct SchemaTraits<test::ModelOutput> {
  static auto fields() noexcept {
    return std::tuple{field<&test::ModelOutput::score>("score")};
  }
};
} // namespace eventflow

namespace eventflow::test {
namespace {
using namespace std::chrono_literals;

StatusOr<bool> positive(const ModuleEvent& event) noexcept {
  return event.value > 0.0;
}

std::size_t projection_calls = 0;

StatusOr<ModelInput> pair_input(const ModuleEvent& first, const ModuleEvent& second) noexcept {
  ++projection_calls;
  return ModelInput{.first = first.value, .second = second.value};
}

StatusOr<ModelOutput> model(const ModelInput& input) noexcept {
  return ModelOutput{.score = (input.first + input.second) / 2.0};
}

struct ModuleFixture {
  std::unique_ptr<Catalog> catalog;
  std::shared_ptr<FunctionRegistry> registry;
  ProcessorConfig definition;
  RelationId input = 0;
  RelationId session = 0;

  Status prepare(bool with_requests = true) noexcept {
    EF_ASSIGN_OR_RETURN(catalog, Catalog::create());
    EF_ASSIGN_OR_RETURN(registry, FunctionRegistry::create());
    EF_ASSIGN_OR_RETURN(input,
                        catalog->define_sql("CREATE TABLE input(ticker STRING NOT NULL,sequence "
                                            "INT64 NOT NULL,value FLOAT64 NOT NULL,"
                                            "PRIMARY KEY(ticker,sequence)) PARTITION BY(ticker)"));
    definition.input_relations = {input};
    if (!with_requests)
      return {};
    EF_ASSIGN_OR_RETURN(
        session,
        catalog->define_candidate(
            {.name = "session",
             .scope_fields =
                 {{.id = 1, .name = "ticker", .type = DataType::String, .nullable = false}},
             .payload_fields = {
                 {.id = 2, .name = "deadline", .type = DataType::Int64, .nullable = false}}}));
    definition.input_relations = {input};
    definition.requests.emplace();
    definition.requests->registry = registry;
    definition.requests->candidate = CandidateExpiryPolicy{
        .relation = session,
        .deadline_column = "deadline",
        .version = 1,
        .on_source = [relation = session](RoundContext& round, const ProcessorEvent& event) noexcept
            -> StatusOr<std::optional<CandidateActivation>> {
          EF_ASSIGN_OR_RETURN(auto existing, round.read_relation(relation, event.scope));
          if (existing)
            return std::optional<CandidateActivation>{};
          return std::optional<CandidateActivation>{
              CandidateActivation{.expected_epoch = 0, .payload = {std::int64_t{10'000'000}}}};
        }};
    return {};
  }

  Trigger pair(std::string id = "pair", bool with_session = true) const noexcept {
    auto source = EventRef::source<ModuleEvent>({.kind = "input", .payload_relation = input})
                      .key_by(&ModuleEvent::ticker);
    auto pattern = Pattern::begin<ModuleEvent>("first")
                       .where<&positive>("positive.v1")
                       .next("second")
                       .where<&positive>("positive.v1")
                       .within(1s)
                       .after_match(CepSkipStrategy::SkipPastLastEvent);
    auto rule = Trigger::pattern(std::move(id), source, pattern);
    if (with_session)
      rule = rule.in_session(SessionRef::candidate(session));
    return rule.capture_match<&pair_input>({"first", "second"}, "pair-input.v1")
        .request(
            Function<&model>{.id = "score", .version = 1, .implementation_identity = "score.v1"},
            {.mode = RequestTriggerMode::Wait, .pending_for = 1s, .lifetime = 2s});
  }
};

class LostReplyStore final : public IStateStore {
public:
  explicit LostReplyStore(std::shared_ptr<IStateStore> store) noexcept : store_(std::move(store)) {}

  StatusOr<ReadResult> get_many(std::uint32_t shard,
                                const std::vector<std::string>& keys) noexcept override {
    return store_->get_many(shard, keys);
  }

  StatusOr<ScanResult> scan(std::uint32_t shard, const std::string& prefix,
                            const std::optional<std::string>& after,
                            std::size_t limit) noexcept override {
    return store_->scan(shard, prefix, after, limit);
  }

  StatusOr<CommitResult> commit(const CommitBatch& batch) noexcept override {
    EF_ASSIGN_OR_RETURN(auto result, store_->commit(batch));
    if (lose_next && result.code == CommitCode::Committed) {
      lose_next = false;
      return CommitResult{.code = CommitCode::Unknown,
                          .revision = batch.expected_revision,
                          .duplicate = false,
                          .message = "Reply lost after durable commit"};
    }
    return result;
  }

  StatusOr<ShardStatus> status(std::uint32_t shard) noexcept override {
    return store_->status(shard);
  }

  StatusOr<StoreLimits> limits() noexcept override {
    return store_->limits();
  }

  bool lose_next = false;

private:
  std::shared_ptr<IStateStore> store_;
};

ProcessorEvent source_event(RelationId relation, std::int64_t offset, double value) noexcept {
  Row row{std::string("T"), offset, value};
  return {.source = {.source_id = "test", .topic = "input", .partition = 0, .offset = offset},
          .scope = {std::string("T")},
          .kind = "input",
          .values = row,
          .changes = {{.relation = relation, .identity = {row[0], row[1]}, .row = row}},
          .processing_time_us = (offset + 1) * 100};
}

Status settle(EventProcessor& processor, SimpleFunctionExecutor& executor,
              std::int64_t now) noexcept {
  for (std::size_t round = 0; round < 64; ++round) {
    EF_ASSIGN_OR_RETURN(auto progress, processor.pump(now));
    EF_RETURN_IF_ERROR(progress.operation_status);
    EF_ASSIGN_OR_RETURN(auto active, processor.active_requests());
    if (active.empty() && !progress.work_remaining)
      return {};
    for (const auto& record : active) {
      EF_ASSIGN_OR_RETURN(
          auto claimed,
          processor.claim_tasks(
              {.scope = record.admission.scope, .request_id = record.admission.request_id}, now));
      EF_RETURN_IF_ERROR(claimed.operation_status);
    }
    EF_ASSIGN_OR_RETURN(active, processor.active_requests());
    std::map<std::string, RequestKey> owners;
    for (const auto& record : active)
      for (const auto& node : record.state.nodes)
        if (node.phase == RequestNodePhase::Running)
          owners.emplace(node.task_id, RequestKey{.scope = record.admission.scope,
                                                  .request_id = record.admission.request_id});
    EF_ASSIGN_OR_RETURN(auto tasks, processor.pending_tasks(64));
    for (const auto& task : tasks)
      EF_RETURN_IF_ERROR(executor.submit(task));
    EF_ASSIGN_OR_RETURN(auto count, executor.run_ready(now));
    (void)count;
    EF_ASSIGN_OR_RETURN(auto outputs, executor.poll(64));
    for (const auto& output : outputs) {
      const auto owner = owners.find(output.task_id);
      if (owner == owners.end())
        return Status(ErrorCode::Internal, "Task owner missing");
      EF_ASSIGN_OR_RETURN(auto complete, processor.complete_request(owner->second, output, now));
      EF_RETURN_IF_ERROR(complete.operation_status);
    }
  }
  return Status(ErrorCode::ResourceExhausted, "Test drain did not settle");
}

TEST(Computation, StaticFunctionModuleBindsPrivatelyAndCanOpenTheActualOwner) {
  ModuleFixture fixture;
  ASSERT_TRUE(fixture.prepare().ok());
  const auto original = take(fixture.catalog->snapshot());
  auto bound = TriggerSet::bind(original, fixture.definition, {fixture.pair()},
                                EngineConfig{.shard_count = 1});
  ASSERT_TRUE(bound.ok()) << bound.status().message();
  EXPECT_EQ(take(fixture.catalog->snapshot()).fingerprint, original.fingerprint);
  EXPECT_EQ(fixture.registry->describe("score", 1).status().code(), ErrorCode::NotFound);
  ASSERT_TRUE(bound->registry->describe("score", 1).ok());
  EXPECT_GT(bound->catalog.relations.size(), original.relations.size());
  ASSERT_EQ(bound->definition.requests->plans.size(), 1U);
  EXPECT_EQ(bound->definition.requests->registry, bound->registry);
  auto store = take(MemoryStateStore::open({.shard_count = 1, .shard_owners = {}}));
  auto owner = EventProcessor::open(std::shared_ptr<IStateStore>(std::move(store)), bound->catalog,
                                    EngineConfig{.shard_count = 1}, bound->definition);
  ASSERT_TRUE(owner.ok()) << owner.status().message();
}

TEST(Computation, LateBindingFailureDoesNotPublishFunctionsOrRelations) {
  ModuleFixture fixture;
  ASSERT_TRUE(fixture.prepare().ok());
  const auto original = take(fixture.catalog->snapshot());
  auto invalid = fixture.pair("bad").in_session(SessionRef::candidate(99999));
  auto result = TriggerSet::bind(original, fixture.definition, {fixture.pair(), invalid});
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(take(fixture.catalog->snapshot()).fingerprint, original.fingerprint);
  EXPECT_EQ(fixture.registry->describe("score", 1).status().code(), ErrorCode::NotFound);
  EXPECT_TRUE(fixture.definition.requests->plans.empty());
}

TEST(Computation, SessionsAndDuplicateRuleIdentitiesAreValidatedBeforePublication) {
  ModuleFixture fixture;
  ASSERT_TRUE(fixture.prepare().ok());
  const auto catalog = take(fixture.catalog->snapshot());
  EXPECT_FALSE(TriggerSet::bind(catalog, fixture.definition, {fixture.pair("pair", false)}).ok());
  EXPECT_FALSE(
      TriggerSet::bind(catalog, fixture.definition, {fixture.pair(), fixture.pair()}).ok());
  auto no_policy = fixture.definition;
  no_policy.requests->candidate.reset();
  EXPECT_FALSE(TriggerSet::bind(catalog, no_policy, {fixture.pair()}).ok());
}

TEST(Computation, StaticDeclarationsCreateAPrivateRegistryWhenNoManualFunctionsExist) {
  ModuleFixture fixture;
  ASSERT_TRUE(fixture.prepare().ok());
  fixture.definition.requests->registry.reset();
  auto result =
      TriggerSet::bind(take(fixture.catalog->snapshot()), fixture.definition, {fixture.pair()});
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_TRUE(result->registry);
  EXPECT_TRUE(result->registry->describe("score", 1).ok());
  EXPECT_FALSE(fixture.definition.requests->registry);
}

TEST(Computation, RepeatedReferencesReuseOneFunctionWithoutSharingRequestRoots) {
  ModuleFixture fixture;
  ASSERT_TRUE(fixture.prepare().ok());
  auto result = TriggerSet::bind(take(fixture.catalog->snapshot()), fixture.definition,
                                 {fixture.pair("first_pair"), fixture.pair("second_pair")});
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(result->definition.requests->plans.size(), 2U);
  EXPECT_NE(result->definition.requests->plans[0].targets,
            result->definition.requests->plans[1].targets);
  EXPECT_TRUE(result->registry->describe("score", 1).ok());
}

TEST(Computation, ClockEventKindsCannotAliasAnotherScheduleOrDeclaredSource) {
  ModuleFixture fixture;
  ASSERT_TRUE(fixture.prepare(false).ok());
  const auto catalog = take(fixture.catalog->snapshot());
  const auto fast = Trigger::clock(
      "fast", {.policy_id = "fast_timer", .every = 1s, .phase = 0s, .event_kind = "tick"});
  const auto slow = Trigger::clock(
      "slow", {.policy_id = "slow_timer", .every = 10s, .phase = 0s, .event_kind = "tick"});
  EXPECT_FALSE(TriggerSet::bind(catalog, fixture.definition, {fast, slow}).ok());
  const auto source = Trigger::on_event(
      "source", EventRef::source({.kind = "tick", .payload_relation = fixture.input}));
  EXPECT_FALSE(TriggerSet::bind(catalog, fixture.definition, {fast, source}).ok());
  EXPECT_FALSE(TriggerSet::bind(catalog, fixture.definition, {source, fast}).ok());
  auto prior = fixture.definition;
  prior.periodic_policies = {
      {.id = "existing", .event_kind = "tick", .interval_us = 3'000'000, .phase_us = 0}};
  EXPECT_FALSE(TriggerSet::bind(catalog, prior, {fast}).ok());
}

TEST(Computation, DistinctClockKindsKeepIndependentSchedules) {
  ModuleFixture fixture;
  ASSERT_TRUE(fixture.prepare(false).ok());
  const auto fast = Trigger::clock(
      "fast", {.policy_id = "fast_timer", .every = 1s, .phase = 0s, .event_kind = "fast_tick"});
  const auto slow = Trigger::clock(
      "slow", {.policy_id = "slow_timer", .every = 10s, .phase = 0s, .event_kind = "slow_tick"});
  auto bound = take(TriggerSet::bind(take(fixture.catalog->snapshot()), fixture.definition,
                                     {fast, slow}, EngineConfig{.shard_count = 1}));
  auto store = take(MemoryStateStore::open({.shard_count = 1, .shard_owners = {}}));
  auto owner =
      take(EventProcessor::open(std::shared_ptr<IStateStore>(std::move(store)), bound.catalog,
                                EngineConfig{.shard_count = 1}, bound.definition));
  ASSERT_TRUE(take(owner->process(source_event(fixture.input, 0, 1.0))).input_committed);
  ASSERT_TRUE(take(owner->advance(1'000'000)).operation_status.ok());
  auto occurrences = take(owner->pending_occurrences(16));
  ASSERT_EQ(occurrences.size(), 1U);
  EXPECT_EQ(occurrences[0].rule_id, "fast");
}

TEST(Computation, SourcePartitionShapeMustMatchOwnerEvenWithoutStateReadsOrRequests) {
  ModuleFixture fixture;
  ASSERT_TRUE(fixture.prepare(false).ok());
  for (const auto& key : {std::string("sequence"), std::string("ticker,sequence")}) {
    const auto name = key == "sequence" ? "number_scope" : "compound_scope";
    auto relation = take(fixture.catalog->define_sql(
        "CREATE TABLE " + std::string(name) +
        "(ticker STRING NOT NULL,sequence INT64 NOT NULL,"
        "value FLOAT64 NOT NULL,PRIMARY KEY(ticker,sequence)) PARTITION BY(" +
        key + ")"));
    const auto catalog = take(fixture.catalog->snapshot());
    const auto untyped = Trigger::on_event(
        "foreign_source", EventRef::source({.kind = "foreign", .payload_relation = relation}));
    EXPECT_FALSE(TriggerSet::bind(catalog, fixture.definition, {untyped}).ok());
    auto source = EventRef::source<ModuleEvent>({.kind = "foreign", .payload_relation = relation});
    if (key == "sequence")
      source = source.key_by(&ModuleEvent::sequence);
    else
      source = source.key_by(&ModuleEvent::ticker, &ModuleEvent::sequence);
    auto typed = Trigger::pattern("foreign_pattern", source,
                                  Pattern::begin<ModuleEvent>("event").within(1s));
    EXPECT_FALSE(TriggerSet::bind(catalog, fixture.definition, {typed}).ok());
  }
}

TEST(Computation, MatchArgumentsSurviveUnknownAndSnapshotWithoutRepeatingProjection) {
  ModuleFixture fixture;
  ASSERT_TRUE(fixture.prepare().ok());
  auto bound = take(TriggerSet::bind(take(fixture.catalog->snapshot()), fixture.definition,
                                     {fixture.pair()}, EngineConfig{.shard_count = 1}));
  auto memory = std::shared_ptr<MemoryStateStore>(
      take(MemoryStateStore::open({.shard_count = 1, .shard_owners = {}})));
  auto observed = std::make_shared<LostReplyStore>(memory);
  auto processor = take(EventProcessor::open(observed, bound.catalog,
                                             EngineConfig{.shard_count = 1}, bound.definition));
  projection_calls = 0;
  ASSERT_TRUE(take(processor->process(source_event(fixture.input, 0, 2.0))).input_committed);
  observed->lose_next = true;
  auto unknown = take(processor->process(source_event(fixture.input, 1, 4.0)));
  EXPECT_FALSE(unknown.input_committed);
  EXPECT_TRUE(processor->has_pending_commit());
  EXPECT_EQ(projection_calls, 1U);
  auto confirmed = take(processor->resolve_pending_commit());
  EXPECT_TRUE(confirmed.input_committed);
  EXPECT_EQ(projection_calls, 1U);
  ASSERT_EQ(take(processor->pending_occurrences(16)).size(), 1U);
  ASSERT_TRUE(memory->create_snapshot("matched").ok());
  processor.reset();
  auto restored = take(MemoryStateStore::from_snapshot(*memory, "matched"));
  processor =
      take(EventProcessor::open(std::shared_ptr<IStateStore>(std::move(restored)), bound.catalog,
                                EngineConfig{.shard_count = 1}, bound.definition));
  ASSERT_TRUE(take(processor->process(source_event(fixture.input, 2, 99.0))).input_committed);
  auto executor = take(SimpleFunctionExecutor::create(bound.registry));
  ASSERT_TRUE(settle(*processor, *executor, 300).ok());
  EXPECT_EQ(projection_calls, 1U);
  const auto latest_id =
      bound.explanation["triggers"][0]["generated"]["latest_relation"].get<RelationId>();
  auto latest = take(bound.catalog.find(latest_id));
  auto result =
      take(processor->query_sql("SELECT score FROM " + latest->name + " WHERE ticker='T'"));
  ASSERT_EQ(result.rows.size(), 1U);
  EXPECT_DOUBLE_EQ(std::get<double>(result.rows[0][0]), 3.0);
}

} // namespace
} // namespace eventflow::test
