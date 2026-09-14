#include "eventflow/event_processor.hpp"
#include "eventflow/message_source.hpp"
#include "remote_harness.hpp"
#include "status_test_support.hpp"
#include <filesystem>
#include <gtest/gtest.h>
#include <limits>

namespace eventflow::test {
namespace {
TEST(EventProcessor, OpensACompiledLifecycleAgainstTheExistingEngine) {
  auto catalog = take(Catalog::create());
  const auto trades =
      take(catalog->define_sql("CREATE TABLE trades (k STRING NOT NULL, id INT64 NOT NULL, qty "
                               "INT64 NOT NULL, PRIMARY KEY(k,id)) PARTITION BY(k)"));
  StoreConfig store_config;
  store_config.shard_count = 1;
  auto store = std::shared_ptr<IStateStore>(take(MemoryStateStore::open(store_config)));
  EngineConfig engine_config;
  engine_config.shard_count = 1;
  ProcessorConfig config;
  config.input_relations = {trades};
  auto processor = EventProcessor::open(store, take(catalog->snapshot()), engine_config, config);
  ASSERT_TRUE(processor.ok()) << processor.status().message();
}

class ObservedProcessorStore final : public IStateStore {
public:
  explicit ObservedProcessorStore(std::shared_ptr<IStateStore> store) noexcept
      : store(std::move(store)) {}

  StatusOr<ReadResult> get_many(std::uint32_t shard,
                                const std::vector<std::string>& keys) noexcept override {
    return store->get_many(shard, keys);
  }

  StatusOr<ScanResult> scan(std::uint32_t shard, const std::string& prefix,
                            const std::optional<std::string>& after,
                            std::size_t limit) noexcept override {
    return store->scan(shard, prefix, after, limit);
  }

  StatusOr<StoreLimits> limits() noexcept override {
    return store->limits();
  }

  StatusOr<ShardStatus> status(std::uint32_t shard) noexcept override {
    return store->status(shard);
  }

  StatusOr<CommitResult> commit(const CommitBatch& batch) noexcept override {
    batches.push_back(batch);
    if (reject_in > 0 && --reject_in == 0)
      return CommitResult{CommitCode::Rejected, batch.expected_revision, false,
                          "injected rejection"};
    EF_ASSIGN_OR_RETURN(auto result, store->commit(batch));
    if (unknown_in > 0 && --unknown_in == 0)
      return CommitResult{CommitCode::Unknown, batch.expected_revision, false,
                          "lost commit response"};
    return result;
  }

  std::shared_ptr<IStateStore> store;
  std::size_t unknown_in = 0;
  std::size_t reject_in = 0;
  std::vector<CommitBatch> batches;
};

class LifecycleContract : public ::testing::TestWithParam<int> {
protected:
  std::string directory;
  std::unique_ptr<Catalog> catalog;
  std::shared_ptr<ObservedProcessorStore> store;
  std::unique_ptr<EventProcessor> processor;
  EngineConfig engine_config;
  ProcessorConfig config;
  RelationId trades = 0;
  RelationId totals = 0;
  RelationId captures = 0;

  void SetUp() noexcept override {
    char pattern[] = "/tmp/eventflow-lifecycle-XXXXXX";
    auto* created = ::mkdtemp(pattern);
    ASSERT_NE(created, nullptr);
    directory = created;
    StoreConfig storage;
    storage.shard_count = 1;
    std::unique_ptr<IStateStore> backend;
    if (GetParam() % 2 == 0)
      backend = take(MemoryStateStore::open(storage));
    else
      backend = take(RocksStateStore::open(directory + "/db", storage));
    if (GetParam() >= 2)
      backend = take(RemoteHarnessStore::open(std::move(backend)));
    store =
        std::make_shared<ObservedProcessorStore>(std::shared_ptr<IStateStore>(std::move(backend)));
    catalog = take(Catalog::create());
    trades = take(catalog->define_sql("CREATE TABLE trades (k STRING NOT NULL, id INT64 NOT NULL, "
                                      "qty INT64 NOT NULL, PRIMARY KEY(k,id)) PARTITION BY(k)"));
    totals = take(catalog->define_sql(
        "CREATE MATERIALIZED VIEW totals AS SELECT k, SUM(qty) AS total FROM trades GROUP BY k"));
    captures = take(catalog->define_sql(
        "CREATE TABLE captures (k STRING NOT NULL, oid STRING NOT NULL, qty INT64 NOT NULL, at "
        "TIMESTAMP_MICROS NOT NULL, PRIMARY KEY(k,oid)) PARTITION BY(k)"));
    take(catalog->define_sql("CREATE MATERIALIZED VIEW captured_features AS SELECT k,oid,qty * 2 "
                             "AS feature,at FROM captures"));
    engine_config.shard_count = 1;
    config.input_relations = {trades};
  }

  void TearDown() noexcept override {
    processor.reset();
    store.reset();
    std::error_code error;
    std::filesystem::remove_all(directory, error);
  }

  void open() noexcept {
    processor.reset();
    processor = take(EventProcessor::open(store, take(catalog->snapshot()), engine_config, config));
  }

  ProcessorEvent event(std::int64_t offset, std::int64_t at, std::int64_t id, std::int64_t qty,
                       std::string kind = "trade", std::string key = "a") noexcept {
    return {{"feed", "trades", 0, offset},
            Row{key},
            std::move(kind),
            {},
            {{trades, Row{key, id}, Row{key, id, qty}}},
            at};
  }

  ProcessorRule rule(bool threshold = false) noexcept {
    FsmGuard guard = [](const FsmContext& context) noexcept -> StatusOr<bool> {
      const auto found = context.reads.find("total");
      if (found == context.reads.end() || !found->second)
        return false;
      const auto* total = std::get_if<std::int64_t>(&(*found->second)[1]);
      if (!total)
        return Status(ErrorCode::InvalidArgument, "expected integer sum");
      return *total > 100;
    };
    ProcessorRule result;
    result.definition = threshold ? crossing("rule", std::move(guard)) : on_event("rule", "trade");
    result.definition.reads = {"total"};
    result.reads = {{"total", totals}};
    result.sink = OccurrenceSink{
        captures, [](const TriggerOccurrence& occurrence) noexcept -> StatusOr<Row> {
          const auto found = occurrence.reads.find("total");
          std::int64_t value = 0;
          if (found != occurrence.reads.end() && found->second)
            value = std::get<std::int64_t>((*found->second)[1]);
          return Row{occurrence.scope[0], occurrence.id, value, occurrence.evaluated_at_us};
        }};
    return result;
  }

  ProcessorRule timeout_rule() noexcept {
    auto result = rule();
    result.definition.states = {{"idle", {}}, {"armed", 5}};
    result.definition.initial = "idle";
    result.definition.transitions = {{"idle", "arm", "armed", {}, {}, {}},
                                     {"armed", "$timeout", "idle", {}, {}, "expired"}};
    return result;
  }

  ProcessResult process(const ProcessorEvent& value) noexcept {
    auto result = take(processor->process(value));
    EXPECT_TRUE(result.operation_status.ok()) << result.operation_status.message();
    EXPECT_TRUE(result.input_committed);
    return result;
  }

  std::int64_t total() noexcept {
    auto rows = take(processor->query_sql("SELECT total FROM totals WHERE k='a'"));
    return rows.rows.empty() ? 0 : std::get<std::int64_t>(rows.rows[0][0]);
  }
};

TEST_P(LifecycleContract, StableMvGuardDedupAndCapturedSubtreeCommitTogether) {
  config.rules = {rule(true)};
  open();
  process(event(10, 1, 1, 60));
  auto accepted = process(event(14, 2, 2, 50));
  ASSERT_EQ(accepted.committed_rounds.size(), 1U);
  ASSERT_EQ(accepted.committed_rounds[0].occurrences.size(), 1U);
  const auto occurrence = accepted.committed_rounds[0].occurrences[0];
  ASSERT_TRUE(occurrence.reads.find("total")->second);
  EXPECT_EQ(std::get<std::int64_t>((*occurrence.reads.find("total")->second)[1]), 110);
  EXPECT_EQ(total(), 110);
  auto captured = take(processor->query_sql("SELECT feature FROM captured_features WHERE k='a'"));
  ASSERT_EQ(captured.rows.size(), 1U);
  EXPECT_EQ(std::get<std::int64_t>(captured.rows[0][0]), 220);
  const auto revision = accepted.commit.revision;
  auto duplicate = process(event(14, 1000, 2, 50));
  EXPECT_TRUE(duplicate.duplicate);
  EXPECT_EQ(duplicate.commit.revision, revision);
  EXPECT_EQ(processor->processing_time_us(), 2);
  EXPECT_EQ(processor->process(event(14, 3, 2, 51)).status().code(), ErrorCode::Conflict);
  EXPECT_EQ(take(processor->pending_occurrences(100)).size(), 1U);
  auto positions = take(processor->processed_positions());
  ASSERT_EQ(positions.size(), 1U);
  EXPECT_EQ(positions[0].next_offset, 15);
}

TEST_P(LifecycleContract, ExpiryPrecedesSameTimeInputAndDeadlineReadsExcludeFutureInput) {
  config.rules = {timeout_rule()};
  config.ttl_policies = {{trades, 10}};
  open();
  process(event(0, 0, 1, 60, "arm"));
  auto result = process(event(1, 5, 2, 200));
  ASSERT_EQ(result.committed_rounds.size(), 2U);
  EXPECT_EQ(result.committed_rounds[0].processing_time_us, 5);
  ASSERT_EQ(result.committed_rounds[0].occurrences.size(), 1U);
  EXPECT_EQ(std::get<std::int64_t>(
                (*result.committed_rounds[0].occurrences[0].reads.find("total")->second)[1]),
            60);
  EXPECT_EQ(total(), 260);
}

TEST_P(LifecycleContract, DelayedAdvanceEvaluatesTimeoutAfterAllCurrentlyExpiredTtl) {
  config.rules = {timeout_rule()};
  config.ttl_policies = {{trades, 10}};
  open();
  process(event(0, 0, 1, 60, "arm"));
  auto advanced = take(processor->advance(20));
  ASSERT_TRUE(advanced.operation_status.ok()) << advanced.operation_status.message();
  ASSERT_EQ(advanced.committed_rounds.size(), 1U);
  EXPECT_EQ(advanced.committed_rounds[0].processing_time_us, 20);
  const auto& occurrence = advanced.committed_rounds[0].occurrences[0];
  EXPECT_FALSE(occurrence.reads.find("total")->second.has_value());
  EXPECT_EQ(occurrence.emission.time_us, 20);
  EXPECT_EQ(occurrence.emission.scheduled_time_us, 5);
  EXPECT_EQ(occurrence.evaluated_at_us, 20);
  EXPECT_EQ(total(), 0);
}

TEST_P(LifecycleContract, DelayedTimeoutCallbackAndFreshWaitUseCurrentProcessingTime) {
  std::int64_t seen_time = -1;
  auto timed = timeout_rule();
  timed.definition.states.push_back({"settling", 7});
  timed.definition.transitions[1].to = "settling";
  timed.definition.transitions[1].update =
      [&](const FsmContext& context) noexcept -> StatusOr<Row> {
    seen_time = context.event.time_us;
    return Row{};
  };
  timed.definition.transitions.push_back({"settling", "$timeout", "idle", {}, {}, "settled"});
  config.rules = {std::move(timed)};
  config.max_occurrences = 1; // No reservation for fictitious historical transition cycles.
  open();
  process(event(0, 0, 1, 60, "arm"));
  auto advanced = take(processor->advance(20));
  ASSERT_TRUE(advanced.operation_status.ok()) << advanced.operation_status.message();
  EXPECT_EQ(seen_time, 20);
  auto state = take(processor->read_fsm("rule", Row{std::string("a")}));
  EXPECT_EQ(state.state, "settling");
  EXPECT_EQ(state.entered_at_us, 20);
  EXPECT_EQ(state.deadline_us, 27);
  open();
  const auto occurrences = take(processor->pending_occurrences(1));
  ASSERT_EQ(occurrences.size(), 1u);
  EXPECT_EQ(occurrences[0].emission.time_us, 20);
  EXPECT_EQ(occurrences[0].emission.scheduled_time_us, 5);
  ASSERT_TRUE(take(processor->acknowledge_occurrences({occurrences[0].id})).operation_status.ok());
  ASSERT_TRUE(take(processor->advance(26)).operation_status.ok());
  EXPECT_TRUE(take(processor->pending_occurrences(1)).empty());
  ASSERT_TRUE(take(processor->advance(27)).operation_status.ok());
  EXPECT_EQ(take(processor->pending_occurrences(1)).size(), 1u);
}

TEST_P(LifecycleContract, TtlReplacementGenerationSurvivesReopen) {
  config.ttl_policies = {{trades, 10}};
  open();
  process(event(0, 1, 1, 3));
  process(event(1, 5, 1, 9));
  open();
  ASSERT_TRUE(take(processor->advance(11)).operation_status.ok());
  EXPECT_EQ(total(), 9);
  ASSERT_TRUE(take(processor->advance(15)).operation_status.ok());
  EXPECT_EQ(total(), 0);
  EXPECT_EQ(take(processor->processed_positions())[0].next_offset, 2);
}

TEST_P(LifecycleContract, UnknownDataCommitRetainsOriginalStateAndEffects) {
  std::size_t callback_calls = 0;
  auto definition = rule();
  definition.definition.transitions[0].guard = [&](const FsmContext&) noexcept -> StatusOr<bool> {
    ++callback_calls;
    return true;
  };
  config.rules = {definition};
  open();
  store->unknown_in = 1;
  auto uncertain = take(processor->process(event(0, 1, 1, 7)));
  EXPECT_EQ(uncertain.operation_status.code(), ErrorCode::UnknownCommit);
  EXPECT_FALSE(uncertain.input_committed);
  EXPECT_TRUE(uncertain.committed_rounds.empty());
  EXPECT_TRUE(processor->has_pending_commit());
  EXPECT_EQ(processor->processing_time_us(), 0);
  EXPECT_FALSE(processor->process(event(1, 2, 2, 10)).ok());
  EXPECT_FALSE(processor->query_sql("SELECT * FROM trades").ok());
  auto resolved = take(processor->resolve_pending_commit());
  ASSERT_TRUE(resolved.operation_status.ok());
  EXPECT_TRUE(resolved.input_committed);
  ASSERT_EQ(resolved.committed_rounds.size(), 1U);
  EXPECT_EQ(resolved.committed_rounds[0].occurrences.size(), 1U);
  EXPECT_EQ(callback_calls, 1U);
  EXPECT_EQ(total(), 7);
  EXPECT_EQ(store->batches[store->batches.size() - 1].commit_id,
            store->batches[store->batches.size() - 2].commit_id);
  EXPECT_FALSE(processor->resolve_pending_commit().ok());
}

TEST_P(LifecycleContract, UnknownTimerRoundResolvesThenContinuesOriginalInput) {
  config.rules = {timeout_rule()};
  open();
  process(event(0, 0, 1, 60, "arm"));
  store->unknown_in = 1;
  auto uncertain = take(processor->process(event(1, 6, 2, 200)));
  EXPECT_EQ(uncertain.operation_status.code(), ErrorCode::UnknownCommit);
  EXPECT_TRUE(uncertain.committed_rounds.empty());
  auto resolved = take(processor->resolve_pending_commit());
  ASSERT_TRUE(resolved.operation_status.ok());
  EXPECT_TRUE(resolved.input_committed);
  ASSERT_EQ(resolved.committed_rounds.size(), 2U);
  EXPECT_EQ(resolved.committed_rounds[0].occurrences.size(), 1U);
  EXPECT_EQ(resolved.committed_rounds[1].processing_time_us, 6);
  EXPECT_EQ(total(), 260);
}

TEST_P(LifecycleContract, PartialTimerEffectsAreNotRepublishedWhenDataUnknownResolves) {
  config.rules = {timeout_rule()};
  open();
  process(event(0, 0, 1, 60, "arm"));
  store->unknown_in = 2;
  auto uncertain = take(processor->process(event(1, 6, 2, 200)));
  ASSERT_EQ(uncertain.committed_rounds.size(), 1U);
  EXPECT_EQ(uncertain.committed_rounds[0].occurrences.size(), 1U);
  auto resolved = take(processor->resolve_pending_commit());
  EXPECT_TRUE(resolved.input_committed);
  ASSERT_EQ(resolved.committed_rounds.size(), 1U);
  EXPECT_TRUE(resolved.committed_rounds[0].occurrences.empty());
  EXPECT_EQ(take(processor->pending_occurrences(100)).size(), 1U);
}

TEST_P(LifecycleContract, TimerOutboxSurvivesLaterDataFailureAndReopen) {
  config.rules = {timeout_rule()};
  open();
  process(event(0, 0, 1, 60, "arm"));
  store->reject_in = 2;
  auto partial = take(processor->process(event(1, 6, 2, 200)));
  EXPECT_FALSE(partial.operation_status.ok());
  EXPECT_FALSE(partial.input_committed);
  ASSERT_EQ(partial.committed_rounds.size(), 1U);
  EXPECT_EQ(partial.committed_rounds[0].occurrences.size(), 1U);
  EXPECT_TRUE(processor->failed());
  open();
  auto pending = take(processor->pending_occurrences(100));
  ASSERT_EQ(pending.size(), 1U);
  EXPECT_EQ(pending[0].emission.name, "expired");
  EXPECT_EQ(total(), 60);
  EXPECT_EQ(take(processor->processed_positions())[0].next_offset, 1);
}

TEST_P(LifecycleContract, DurableOutboxBackpressureAndUnknownConsumptionDoNotLoseInput) {
  config.rules = {rule()};
  config.max_occurrences = 1;
  open();
  process(event(0, 1, 1, 3));
  auto full = take(processor->process(event(1, 2, 2, 4)));
  EXPECT_EQ(full.operation_status.code(), ErrorCode::ResourceExhausted);
  EXPECT_FALSE(full.input_committed);
  EXPECT_FALSE(processor->failed());
  EXPECT_EQ(total(), 3);
  const auto id = take(processor->pending_occurrences(1))[0].id;
  store->unknown_in = 1;
  auto uncertain = take(processor->acknowledge_occurrences({id}));
  EXPECT_EQ(uncertain.operation_status.code(), ErrorCode::UnknownCommit);
  ASSERT_TRUE(take(processor->resolve_pending_commit()).operation_status.ok());
  EXPECT_TRUE(take(processor->pending_occurrences(1)).empty());
  process(event(1, 2, 2, 4));
  EXPECT_EQ(total(), 7);
}

TEST_P(LifecycleContract, DedupRetentionRejectsOldUnknownPayloadRatherThanAcknowledgingIt) {
  config.max_dedup_entries = 2;
  open();
  process(event(10, 0, 1, 1));
  process(event(14, 1, 2, 2));
  process(event(21, 2, 3, 3));
  open();
  EXPECT_EQ(processor->process(event(10, 100, 1, 1)).status().code(),
            ErrorCode::FailedPrecondition);
  EXPECT_TRUE(process(event(14, 100, 2, 2)).duplicate);
  EXPECT_EQ(processor->process(event(14, 100, 2, 999)).status().code(), ErrorCode::Conflict);
  EXPECT_EQ(total(), 6);
  EXPECT_EQ(processor->processing_time_us(), 2);
}

TEST_P(LifecycleContract, AdmissionRejectsCrossScopeAndTimeOverflowBeforeRunningTimers) {
  config.ttl_policies = {{trades, 10}};
  config.max_keys = 1;
  open();
  process(event(0, 0, 1, 1));
  auto wrong_scope = event(1, 100, 2, 1);
  wrong_scope.changes[0].identity[0] = std::string("b");
  EXPECT_FALSE(processor->process(wrong_scope).ok());
  EXPECT_FALSE(processor->process(event(1, INT64_MAX, 2, 1)).ok());
  EXPECT_FALSE(processor->process(event(1, 1, 2, 1, "trade", "b")).ok());
  EXPECT_EQ(total(), 1);
  EXPECT_EQ(processor->processing_time_us(), 0);
  EXPECT_FALSE(processor->failed());
}

TEST_P(LifecycleContract, ReopenRejectsChangedDefinitionVersionWithoutMutatingStoredState) {
  config.rules = {rule()};
  open();
  process(event(0, 1, 1, 3));
  processor.reset();
  auto changed = config;
  ++changed.rules[0].definition.version;
  auto rejected = EventProcessor::open(store, take(catalog->snapshot()), engine_config, changed);
  EXPECT_EQ(rejected.status().code(), ErrorCode::FailedPrecondition);
  open();
  EXPECT_EQ(take(processor->pending_occurrences(100)).size(), 1U);
  EXPECT_EQ(total(), 3);
}

TEST_P(LifecycleContract, ExpirationResetsCrossingBeforeTheNextRise) {
  config.rules = {rule(true)};
  config.ttl_policies = {{trades, 10}};
  open();
  process(event(0, 0, 1, 60));
  process(event(1, 5, 2, 50));
  ASSERT_EQ(take(processor->pending_occurrences(100)).size(), 1U);
  ASSERT_TRUE(take(processor->advance(10)).operation_status.ok());
  EXPECT_EQ(total(), 50);
  process(event(2, 11, 3, 60));
  EXPECT_EQ(take(processor->pending_occurrences(100)).size(), 2U);
}

TEST_P(LifecycleContract, LargestIncrementableOffsetSurvivesReopen) {
  open();
  process(event(INT64_MAX - 1, 0, 1, 1));
  processor.reset();
  auto reopened = EventProcessor::open(store, take(catalog->snapshot()), engine_config, config);
  ASSERT_TRUE(reopened.ok()) << reopened.status().message();
  processor = std::move(reopened).value();
  EXPECT_EQ(take(processor->processed_positions())[0].next_offset, INT64_MAX);
}

TEST_P(LifecycleContract, PeriodicClockCoalescesOverdueSlotsAndRestoresNextSlot) {
  auto clock_rule = rule();
  clock_rule.definition = on_event("rule", "clock");
  clock_rule.definition.reads = {"total"};
  config.rules = {clock_rule};
  config.periodic_policies = {{"five", "clock", 5, 0}};
  open();
  process(event(0, 1, 1, 3));
  auto advanced = take(processor->advance(17));
  ASSERT_TRUE(advanced.operation_status.ok()) << advanced.operation_status.message();
  ASSERT_EQ(advanced.committed_rounds.size(), 1U);
  const auto& fired = advanced.committed_rounds[0];
  ASSERT_EQ(fired.clock_firings.size(), 1U);
  EXPECT_EQ(fired.processing_time_us, 17);
  EXPECT_EQ(fired.clock_firings[0].slot_us, 15);
  EXPECT_EQ(fired.clock_firings[0].skipped_slots, 2U);
  ASSERT_EQ(fired.occurrences.size(), 1U);
  EXPECT_EQ(fired.occurrences[0].evaluated_at_us, 17);
  EXPECT_EQ(fired.occurrences[0].skipped_slots, 2U);
  EXPECT_EQ(fired.occurrences[0].event_values, Row{std::int64_t{2}});
  open();
  auto positions = take(processor->periodic_positions());
  ASSERT_EQ(positions.size(), 1U);
  EXPECT_EQ(positions[0].next_slot_us, 20);
  EXPECT_EQ(positions[0].last_slot_us, 15);
  EXPECT_EQ(positions[0].skipped_slots, 2U);
  ASSERT_TRUE(take(processor->advance(19)).operation_status.ok());
  EXPECT_EQ(take(processor->pending_occurrences(100)).size(), 1U);
  ASSERT_TRUE(take(processor->advance(20)).operation_status.ok());
  EXPECT_EQ(take(processor->pending_occurrences(100)).size(), 2U);
}

TEST_P(LifecycleContract, DelayedClockSlot100DoesNotCaptureCandidateExpiredAt105WhenNow110) {
  auto clock_rule = rule();
  clock_rule.definition =
      on_event("rule", "clock", [](const FsmContext& context) noexcept -> StatusOr<bool> {
        return context.reads.find("total")->second.has_value();
      });
  clock_rule.definition.reads = {"total"};
  config.rules = {clock_rule};
  config.periodic_policies = {{"hundred", "clock", 100, 0}};
  config.ttl_policies = {{trades, 105}};
  open();
  process(event(0, 0, 1, 3));
  auto advanced = take(processor->advance(110));
  ASSERT_TRUE(advanced.operation_status.ok()) << advanced.operation_status.message();
  ASSERT_EQ(advanced.committed_rounds.size(), 1U);
  ASSERT_EQ(advanced.committed_rounds[0].clock_firings.size(), 1U);
  EXPECT_EQ(advanced.committed_rounds[0].clock_firings[0].slot_us, 100);
  EXPECT_EQ(advanced.committed_rounds[0].processing_time_us, 110);
  EXPECT_TRUE(advanced.committed_rounds[0].occurrences.empty());
  EXPECT_TRUE(take(processor->pending_occurrences(100)).empty());
  EXPECT_EQ(total(), 0);
}

TEST_P(LifecycleContract, UnknownPeriodicRoundPublishesOneFiringAndPreservesCoalescing) {
  auto clock_rule = rule();
  clock_rule.definition = on_event("rule", "clock");
  clock_rule.definition.reads = {"total"};
  config.rules = {clock_rule};
  config.periodic_policies = {{"five", "clock", 5, 0}};
  open();
  process(event(0, 0, 1, 3));
  store->unknown_in = 1;
  auto uncertain = take(processor->advance(17));
  EXPECT_EQ(uncertain.operation_status.code(), ErrorCode::UnknownCommit);
  auto resolved = take(processor->resolve_pending_commit());
  ASSERT_TRUE(resolved.operation_status.ok());
  ASSERT_EQ(resolved.committed_rounds.size(), 1U);
  ASSERT_EQ(resolved.committed_rounds[0].clock_firings.size(), 1U);
  EXPECT_EQ(resolved.committed_rounds[0].clock_firings[0].skipped_slots, 2U);
  EXPECT_EQ(take(processor->periodic_positions())[0].next_slot_us, 20);
  EXPECT_EQ(take(processor->pending_occurrences(100)).size(), 1U);
}

TEST_P(LifecycleContract, LatestPerKeyRelationReplacesCurrentRowAndRenewsTtl) {
  const auto latest = take(catalog->define_sql("CREATE TABLE latest_news (k STRING NOT NULL, text "
                                               "STRING NOT NULL, PRIMARY KEY(k)) PARTITION BY(k)"));
  take(catalog->define_sql("CREATE MATERIALIZED VIEW news_text AS SELECT k,text FROM latest_news"));
  config.input_relations.push_back(latest);
  config.ttl_policies = {{latest, 10}};
  open();
  auto first = event(0, 1, 1, 0, "news");
  first.changes = {{latest, Row{std::string("a")}, Row{std::string("a"), std::string("old")}}};
  process(first);
  auto second = first;
  second.source.offset = 1;
  second.processing_time_us = 5;
  second.changes[0].row = Row{std::string("a"), std::string("new")};
  process(second);
  open();
  ASSERT_TRUE(take(processor->advance(11)).operation_status.ok());
  auto rows = take(processor->query_sql("SELECT text FROM news_text WHERE k='a'"));
  ASSERT_EQ(rows.rows.size(), 1U);
  EXPECT_EQ(std::get<std::string>(rows.rows[0][0]), "new");
  ASSERT_TRUE(take(processor->advance(15)).operation_status.ok());
  EXPECT_TRUE(take(processor->query_sql("SELECT text FROM news_text WHERE k='a'")).rows.empty());
}

TEST_P(LifecycleContract, CallerAcknowledgesSourceOnlyAfterResolvingDurableCompute) {
  open();
  MemMockSourceConfig source_config;
  source_config.source_id = "feed";
  source_config.assignments = {{"trades", 0, {}}};
  auto source = take(MemMockMessageSource::open(source_config));
  SourcePartition partition{"feed", "trades", 0};
  ASSERT_TRUE(source->append(partition, 0, "a", "7").ok());
  auto deliveries = take(source->poll(1, 100, 0));
  ASSERT_EQ(deliveries.size(), 1U);
  const auto& token = deliveries[0].token;
  auto input = event(token.offset, 1, 1, 7);
  input.source = {token.partition.source_id, token.partition.topic, token.partition.partition,
                  token.offset};
  store->unknown_in = 1;
  auto uncertain = take(processor->process(input));
  EXPECT_FALSE(uncertain.input_committed);
  EXPECT_EQ(take(source->committed_position(partition)), -1);
  auto confirmed = take(processor->resolve_pending_commit());
  ASSERT_TRUE(confirmed.input_committed);
  ASSERT_TRUE(confirmed.operation_status.ok());
  ASSERT_TRUE(source->ack(token).ok());
  EXPECT_EQ(take(source->committed_position(partition)), 1);
}

TEST_P(LifecycleContract, CapturedEventValuesSurviveDedupEvictionAndReopen) {
  config.rules = {rule()};
  config.max_dedup_entries = 1;
  open();
  auto first = event(0, 1, 1, 3);
  first.values = Row{std::int64_t{99}, std::string("immutable")};
  process(first);
  process(event(1, 2, 2, 4));
  open();
  auto outbox = take(processor->pending_occurrences(100));
  ASSERT_EQ(outbox.size(), 2U);
  EXPECT_EQ(outbox[0].event_values, first.values);
  EXPECT_EQ(outbox[0].source, std::optional<SourceIdentity>{first.source});
}

TEST_P(LifecycleContract, RulesMayReadPriorRoundCaptureViewsWithoutSameRoundFeedback) {
  const auto capture_totals =
      take(catalog->define_sql("CREATE MATERIALIZED VIEW capture_totals AS SELECT k,SUM(qty) AS "
                               "qty FROM captures GROUP BY k"));
  auto producer = rule();
  ProcessorRule observer;
  observer.definition =
      on_event("observer", "trade", [](const FsmContext& context) noexcept -> StatusOr<bool> {
        return context.reads.find("prior")->second.has_value();
      });
  observer.definition.reads = {"prior"};
  observer.reads = {{"prior", capture_totals}};
  config.rules = {producer, observer};
  open();
  auto first = process(event(0, 1, 1, 3));
  ASSERT_EQ(first.committed_rounds[0].occurrences.size(), 1U);
  auto second = process(event(1, 2, 2, 4));
  ASSERT_EQ(second.committed_rounds[0].occurrences.size(), 2U);
  EXPECT_EQ(second.committed_rounds[0].occurrences[1].rule_id, "observer");
  EXPECT_EQ(std::get<std::int64_t>(
                (*second.committed_rounds[0].occurrences[1].reads.find("prior")->second)[1]),
            3);
}

TEST_P(LifecycleContract, RejectsInvalidReadAndFeedbackBindingsBeforeCreatingEngineState) {
  auto malformed = rule();
  malformed.reads = {{"total", trades}};
  config.rules = {malformed};
  EXPECT_FALSE(EventProcessor::open(store, take(catalog->snapshot()), engine_config, config).ok());
  EXPECT_EQ(take(store->status(0)).revision, 0U);
  malformed = rule();
  malformed.sink->relation = trades;
  config.rules = {malformed};
  EXPECT_FALSE(EventProcessor::open(store, take(catalog->snapshot()), engine_config, config).ok());
  EXPECT_EQ(take(store->status(0)).revision, 0U);
}

TEST_P(LifecycleContract, WrongScopeCaptureFactoryFailsWholeDataRound) {
  auto malformed = rule();
  malformed.sink->make_row = [](const TriggerOccurrence& occurrence) noexcept -> StatusOr<Row> {
    return Row{std::string("wrong-key"), occurrence.id, std::int64_t{0},
               occurrence.evaluated_at_us};
  };
  config.rules = {malformed};
  open();
  const auto revision = take(store->status(0)).revision;
  auto rejected = take(processor->process(event(0, 1, 1, 3)));
  EXPECT_FALSE(rejected.operation_status.ok());
  EXPECT_FALSE(rejected.input_committed);
  EXPECT_TRUE(processor->failed());
  EXPECT_EQ(take(store->status(0)).revision, revision);
  open();
  EXPECT_EQ(total(), 0);
  EXPECT_TRUE(take(processor->pending_occurrences(100)).empty());
  EXPECT_TRUE(take(processor->processed_positions()).empty());
}

INSTANTIATE_TEST_SUITE_P(MemoryRocksRemote, LifecycleContract, ::testing::Values(0, 1, 2, 3));
} // namespace
} // namespace eventflow::test
