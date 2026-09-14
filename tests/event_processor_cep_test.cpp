#include "eventflow/event_processor.hpp"
#include "eventflow/key_codec.hpp"
#include "remote_harness.hpp"
#include "status_test_support.hpp"
#include <filesystem>
#include <gtest/gtest.h>
#include <limits>

namespace eventflow::test {
namespace {
class CepObservedStore final : public IStateStore {
public:
  explicit CepObservedStore(std::shared_ptr<IStateStore> backend) noexcept
      : backend(std::move(backend)) {}

  StatusOr<ReadResult> get_many(std::uint32_t shard,
                                const std::vector<std::string>& keys) noexcept override {
    return backend->get_many(shard, keys);
  }

  StatusOr<ScanResult> scan(std::uint32_t shard, const std::string& prefix,
                            const std::optional<std::string>& after,
                            std::size_t limit) noexcept override {
    return backend->scan(shard, prefix, after, limit);
  }

  StatusOr<StoreLimits> limits() noexcept override {
    return backend->limits();
  }

  StatusOr<ShardStatus> status(std::uint32_t shard) noexcept override {
    return backend->status(shard);
  }

  StatusOr<CommitResult> commit(const CommitBatch& batch) noexcept override {
    batches.push_back(batch);
    if (unknown_before) {
      unknown_before = false;
      return CommitResult{CommitCode::Unknown, batch.expected_revision, false,
                          "unconfirmed before apply"};
    }
    EF_ASSIGN_OR_RETURN(auto result, backend->commit(batch));
    if (unknown_after) {
      unknown_after = false;
      return CommitResult{CommitCode::Unknown, batch.expected_revision, false,
                          "lost response after apply"};
    }
    return result;
  }

  std::shared_ptr<IStateStore> backend;
  bool unknown_before = false;
  bool unknown_after = false;
  std::vector<CommitBatch> batches;
};

class CepOwnerContract : public ::testing::TestWithParam<int> {
protected:
  std::string directory;
  std::shared_ptr<CepObservedStore> store;
  std::unique_ptr<Catalog> catalog;
  std::unique_ptr<EventProcessor> processor;
  EngineConfig engine_config;
  ProcessorConfig config;
  RelationId trades = 0;
  RelationId totals = 0;
  IStateStore* local_backend = nullptr;

  void SetUp() noexcept override {
    char path[] = "/tmp/eventflow-cep-owner-XXXXXX";
    auto* created = ::mkdtemp(path);
    ASSERT_NE(created, nullptr);
    directory = created;
    StoreConfig storage;
    storage.shard_count = 1;
    std::unique_ptr<IStateStore> backend;
    if (GetParam() % 2 == 0)
      backend = take(MemoryStateStore::open(storage));
    else
      backend = take(RocksStateStore::open(directory + "/db", storage));
    local_backend = backend.get();
    if (GetParam() >= 2)
      backend = take(RemoteHarnessStore::open(std::move(backend)));
    store = std::make_shared<CepObservedStore>(std::shared_ptr<IStateStore>(std::move(backend)));
    catalog = take(Catalog::create());
    trades = take(catalog->define_sql("CREATE TABLE trades (k STRING NOT NULL, id INT64 NOT NULL, "
                                      "qty INT64 NOT NULL, PRIMARY KEY(k,id)) PARTITION BY(k)"));
    totals = take(catalog->define_sql(
        "CREATE MATERIALIZED VIEW totals AS SELECT k, SUM(qty) AS total FROM trades GROUP BY k"));
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

  ProcessorCepRule rule() noexcept {
    ProcessorCepRule result;
    result.definition.id = "overlap";
    result.definition.within_us = 10;
    result.definition.pattern =
        CepPattern::event("a", "A").then(CepPattern::event("b", "B"), CepContiguity::FollowedByAny);
    return result;
  }

  ProcessorEvent event(std::int64_t offset, std::string kind, std::int64_t at,
                       std::int64_t qty = 1) noexcept {
    return {{"source", "feed", 0, offset},
            Row{std::string("a")},
            std::move(kind),
            Row{qty},
            {{trades, Row{std::string("a"), offset}, Row{std::string("a"), offset, qty}}},
            at};
  }

  ProcessResult process(const ProcessorEvent& input) noexcept {
    auto result = take(processor->process(input));
    EXPECT_TRUE(result.operation_status.ok()) << result.operation_status.message();
    EXPECT_TRUE(result.input_committed);
    return result;
  }

  CepStateSummary state() noexcept {
    return take(processor->read_cep("overlap", Row{std::string("a")}));
  }

  std::int64_t total() noexcept {
    const auto result = take(processor->query_sql("SELECT total FROM totals WHERE k='a'"));
    return result.rows.empty() ? 0 : std::get<std::int64_t>(result.rows[0][0]);
  }

  void snapshot_restore() noexcept {
    take(local_backend->create_snapshot("cep-owner"));
    StoreConfig storage;
    storage.shard_count = 1;
    std::unique_ptr<IStateStore> restored;
    if (GetParam() % 2 == 0)
      restored = take(MemoryStateStore::from_snapshot(
          *static_cast<MemoryStateStore*>(local_backend), "cep-owner"));
    else
      restored = take(RocksStateStore::from_snapshot(directory + "/db", "cep-owner",
                                                     directory + "/restored", storage));
    processor.reset();
    store.reset();
    local_backend = restored.get();
    if (GetParam() >= 2)
      restored = take(RemoteHarnessStore::open(std::move(restored)));
    store = std::make_shared<CepObservedStore>(std::shared_ptr<IStateStore>(std::move(restored)));
    open();
  }

  void unknown_retry(bool after) noexcept {
    int calls = 0;
    auto cep = rule();
    cep.definition.pattern = CepPattern::event("a", "A").then(
        CepPattern::event("b", "B",
                          [&](const CepContext&) noexcept -> StatusOr<bool> {
                            ++calls;
                            return true;
                          }),
        CepContiguity::FollowedByAny);
    config.cep_rules = {cep};
    open();
    process(event(0, "A", 0));
    process(event(1, "A", 1));
    store->unknown_after = after;
    store->unknown_before = !after;
    auto result = take(processor->process(event(2, "B", 2)));
    EXPECT_EQ(result.operation_status.code(), ErrorCode::UnknownCommit);
    EXPECT_FALSE(result.input_committed);
    EXPECT_TRUE(result.committed_rounds.empty());
    EXPECT_TRUE(processor->has_pending_commit());
    const auto observed = calls;
    EXPECT_EQ(observed, 2);
    EXPECT_FALSE(processor->read_cep("overlap", Row{std::string("a")}).ok());
    EXPECT_FALSE(processor->pending_occurrences(10).ok());
    const auto original = store->batches.back();
    result = take(processor->resolve_pending_commit());
    EXPECT_TRUE(result.operation_status.ok());
    EXPECT_TRUE(result.input_committed);
    EXPECT_EQ(calls, observed);
    ASSERT_EQ(result.committed_rounds.size(), 1U);
    EXPECT_EQ(result.committed_rounds[0].occurrences.size(), 2U);
    const auto& retry = store->batches.back();
    EXPECT_EQ(original.commit_id, retry.commit_id);
    ASSERT_EQ(original.mutations.size(), retry.mutations.size());
    for (std::size_t i = 0; i < original.mutations.size(); ++i) {
      EXPECT_EQ(original.mutations[i].key, retry.mutations[i].key);
      EXPECT_EQ(original.mutations[i].value, retry.mutations[i].value);
    }
    auto redelivery = event(2, "B", 5);
    EXPECT_TRUE(process(redelivery).duplicate);
    EXPECT_EQ(calls, observed);
    open();
    EXPECT_EQ(take(processor->pending_occurrences(10)).size(), 2U);
    EXPECT_EQ(take(processor->processed_positions())[0].next_offset, 3);
  }
};

TEST_P(CepOwnerContract, PersistsBothOverlappingMatchesInTheSourceCommit) {
  config.cep_rules = {rule()};
  open();
  process(event(0, "A", 0, 10));
  process(event(1, "A", 1, 20));
  const auto result = process(event(2, "B", 2, 15));
  ASSERT_EQ(result.committed_rounds.size(), 1U);
  const auto& output = result.committed_rounds[0].occurrences;
  ASSERT_EQ(output.size(), 2U);
  ASSERT_TRUE(output[0].cep_match);
  ASSERT_TRUE(output[1].cep_match);
  EXPECT_NE(output[0].id, output[1].id);
  EXPECT_EQ(output[0].cep_match->events[0].event.values, Row{std::int64_t(10)});
  EXPECT_EQ(output[1].cep_match->events[0].event.values, Row{std::int64_t(20)});
  EXPECT_EQ(take(processor->pending_occurrences(10)).size(), 2U);
  EXPECT_EQ(take(processor->processed_positions())[0].next_offset, 3);
  open();
  EXPECT_EQ(take(processor->pending_occurrences(10)).size(), 2U);
}

TEST_P(CepOwnerContract, UnknownBeforeApplyRetriesTheOriginalBatchWithoutMatching) {
  unknown_retry(false);
}

TEST_P(CepOwnerContract, UnknownAfterApplyRetriesTheOriginalBatchWithoutMatching) {
  unknown_retry(true);
}

TEST_P(CepOwnerContract, CapacityRejectsWholeRoundAndExpiryMakesTheOriginalInputRetryable) {
  auto cep = rule();
  cep.definition.limits.max_paths = 1;
  config.cep_rules = {cep};
  open();
  process(event(0, "A", 0, 10));
  const auto before_revision = take(store->status(0)).revision;
  const auto before_commits = store->batches.size();
  auto rejected = take(processor->process(event(1, "A", 1, 20)));
  EXPECT_EQ(rejected.operation_status.code(), ErrorCode::ResourceExhausted);
  EXPECT_FALSE(rejected.input_committed);
  EXPECT_TRUE(rejected.committed_rounds.empty());
  EXPECT_FALSE(processor->failed());
  EXPECT_EQ(store->batches.size(), before_commits);
  EXPECT_EQ(take(store->status(0)).revision, before_revision);
  EXPECT_EQ(processor->processing_time_us(), 0);
  EXPECT_EQ(total(), 10);
  EXPECT_EQ(state().event_sequence, 1U);
  ASSERT_EQ(state().paths.size(), 1U);
  EXPECT_EQ(take(processor->processed_positions())[0].next_offset, 1);
  auto advanced = take(processor->advance(10));
  EXPECT_TRUE(advanced.operation_status.ok());
  EXPECT_TRUE(state().paths.empty());
  process(event(1, "A", 10, 20));
  EXPECT_EQ(total(), 30);
  EXPECT_EQ(state().event_sequence, 2U);
}

TEST_P(CepOwnerContract, OutboxBackpressureRollsBackInputAndCanBeDrained) {
  auto cep = rule();
  cep.definition.pattern = CepPattern::event("a", "A");
  config.cep_rules = {cep};
  config.max_occurrences = 1;
  open();
  const auto first = process(event(0, "A", 0, 10)).committed_rounds[0].occurrences[0].id;
  const auto before = take(store->status(0)).revision;
  auto rejected = take(processor->process(event(1, "A", 1, 20)));
  EXPECT_EQ(rejected.operation_status.code(), ErrorCode::ResourceExhausted);
  EXPECT_FALSE(processor->failed());
  EXPECT_EQ(take(store->status(0)).revision, before);
  EXPECT_EQ(total(), 10);
  EXPECT_EQ(state().event_sequence, 1U);
  EXPECT_EQ(take(processor->processed_positions())[0].next_offset, 1);
  EXPECT_TRUE(take(processor->acknowledge_occurrences({first})).operation_status.ok());
  const auto second = process(event(1, "A", 1, 20)).committed_rounds[0].occurrences[0].id;
  EXPECT_NE(first, second);
  EXPECT_EQ(total(), 30);
}

TEST_P(CepOwnerContract, MultiMatchOverflowProducesNoPartialOutboxOrSourceProgress) {
  config.cep_rules = {rule()};
  config.max_occurrences = 1;
  open();
  process(event(0, "A", 0));
  process(event(1, "A", 1));
  const auto revision = take(store->status(0)).revision;
  const auto result = take(processor->process(event(2, "B", 2)));
  EXPECT_EQ(result.operation_status.code(), ErrorCode::ResourceExhausted);
  EXPECT_FALSE(result.input_committed);
  EXPECT_EQ(take(store->status(0)).revision, revision);
  EXPECT_TRUE(take(processor->pending_occurrences(1)).empty());
  EXPECT_EQ(state().event_sequence, 2U);
  EXPECT_EQ(total(), 2);
  EXPECT_EQ(take(processor->processed_positions())[0].next_offset, 2);
  EXPECT_FALSE(processor->failed());
}

TEST_P(CepOwnerContract, OptionalProceedsWithoutConsumingTheSameInputTwice) {
  auto cep = rule();
  cep.definition.pattern = CepPattern::event("a", "A")
                               .then(CepPattern::event("optional_c", "C").optional())
                               .then(CepPattern::event("c", "C"));
  config.cep_rules = {cep};
  open();
  process(event(0, "A", 0));
  auto output = process(event(1, "C", 1)).committed_rounds[0].occurrences;
  ASSERT_EQ(output.size(), 1U);
  ASSERT_TRUE(output[0].cep_match);
  EXPECT_EQ(output[0].cep_match->events.size(), 2U);
  EXPECT_EQ(output[0].cep_match->events.back().label, "c");
  open();
  EXPECT_EQ(take(processor->pending_occurrences(10)).size(), 1U);
}

TEST_P(CepOwnerContract, FirstNewsConsumesNextOfKindEvenWhenNegative) {
  auto cep = rule();
  cep.definition.pattern =
      CepPattern::event("arm", "Arm")
          .then(CepPattern::event("news", "News",
                                  [](const CepContext& c) noexcept -> StatusOr<bool> {
                                    return std::get<std::int64_t>(c.event.values[0]) > 0;
                                  }),
                CepContiguity::NextOfKind);
  config.cep_rules = {cep};
  open();
  process(event(0, "Arm", 0));
  process(event(1, "Trade", 1));
  process(event(2, "News", 2, -1));
  process(event(3, "News", 3, 1));
  EXPECT_TRUE(take(processor->pending_occurrences(10)).empty());
  process(event(4, "Arm", 4));
  process(event(5, "Trade", 5));
  EXPECT_EQ(process(event(6, "News", 6, 1)).committed_rounds[0].occurrences.size(), 1U);
}

TEST_P(CepOwnerContract, GuardSeparatesBranchHistoryFromTheFrozenCurrentMv) {
  auto cep = rule();
  cep.definition.reads = {"total"};
  cep.reads = {{"total", totals}};
  cep.definition.register_fields = {{1, "captured_total", DataType::Int64, false}};
  cep.definition.initial_registers = {std::int64_t(0)};
  std::vector<std::int64_t> current;
  std::vector<std::int64_t> history;
  cep.definition.pattern =
      CepPattern::event("a", "A", {},
                        [](const CepContext& c) noexcept -> StatusOr<Row> {
                          return Row{(*c.reads.at("total"))[1]};
                        })
          .then(CepPattern::event("b", "B",
                                  [&](const CepContext& c) noexcept -> StatusOr<bool> {
                                    current.push_back(
                                        std::get<std::int64_t>((*c.reads.at("total"))[1]));
                                    history.push_back(std::get<std::int64_t>(c.registers[0]));
                                    return std::get<std::int64_t>(c.event.values[0]) >
                                           std::get<std::int64_t>(c.history[0].event.values[0]);
                                  }),
                CepContiguity::FollowedByAny);
  config.cep_rules = {cep};
  open();
  process(event(0, "A", 0, 10));
  process(event(1, "A", 1, 20));
  open();
  const auto result = process(event(2, "B", 2, 15));
  EXPECT_EQ(current, (std::vector<std::int64_t>{45, 45}));
  EXPECT_EQ(history, (std::vector<std::int64_t>{10, 30}));
  const auto& output = result.committed_rounds[0].occurrences;
  ASSERT_EQ(output.size(), 1U);
  EXPECT_EQ(output[0].cep_match->registers, Row{std::int64_t(10)});
  EXPECT_EQ(std::get<std::int64_t>((*output[0].reads.at("total"))[1]), 45);
}

TEST_P(CepOwnerContract, TimeoutAtEqualityPrecedesTheNewDataAndSurvivesRecovery) {
  config.cep_rules = {rule()};
  open();
  process(event(0, "A", 0));
  open();
  const auto result = process(event(1, "B", 10));
  ASSERT_EQ(result.committed_rounds.size(), 2U);
  EXPECT_TRUE(result.committed_rounds[0].occurrences.empty());
  EXPECT_TRUE(result.committed_rounds[1].occurrences.empty());
  EXPECT_TRUE(state().paths.empty());
  EXPECT_EQ(state().time_us, 10);
  open();
  EXPECT_TRUE(take(processor->pending_occurrences(10)).empty());
}

TEST_P(CepOwnerContract, ExactSourceDedupAndPayloadConflictOccurBeforeMatcher) {
  int calls = 0;
  auto cep = rule();
  cep.definition.pattern =
      CepPattern::event("a", "A", [&](const CepContext&) noexcept -> StatusOr<bool> {
        ++calls;
        return true;
      });
  config.cep_rules = {cep};
  open();
  process(event(0, "A", 0));
  EXPECT_TRUE(process(event(0, "A", 20)).duplicate);
  const auto conflict = processor->process(event(0, "A", 20, 99));
  ASSERT_FALSE(conflict.ok());
  EXPECT_EQ(conflict.status().code(), ErrorCode::Conflict);
  EXPECT_EQ(calls, 1);
  EXPECT_EQ(state().event_sequence, 1U);
  EXPECT_EQ(processor->processing_time_us(), 0);
}

TEST_P(CepOwnerContract, PendingMatchesReserveOutboxAndKeepFrozenWitnessAcrossSnapshotRestore) {
  auto cep = rule();
  cep.definition.pattern = CepPattern::event("a", "A")
                               .then(CepPattern::event("z", "Z"))
                               .or_else(CepPattern::event("b", "B"));
  cep.definition.skip = CepSkipStrategy::SkipToNext;
  cep.definition.reads = {"total"};
  cep.reads = {{"total", totals}};
  auto audit = rule();
  audit.definition.id = "audit";
  audit.definition.pattern = CepPattern::event("ping", "Ping");
  config.cep_rules = {cep, audit};
  config.max_occurrences = 2;
  open();
  process(event(0, "A", 0, 10));
  process(event(1, "B", 1, 20));
  ASSERT_EQ(state().pending_matches, 1U);
  EXPECT_TRUE(take(processor->pending_occurrences(1)).empty());
  const auto audit_id = process(event(2, "Ping", 2, 1)).committed_rounds[0].occurrences[0].id;
  const auto revision = take(store->status(0)).revision;
  const auto rejected = take(processor->process(event(3, "B", 3, 30)));
  EXPECT_EQ(rejected.operation_status.code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(take(store->status(0)).revision, revision);
  EXPECT_EQ(state().pending_matches, 1U);
  process(event(3, "Trade", 3, 30));
  snapshot_restore();
  EXPECT_EQ(total(), 61);
  EXPECT_EQ(state().pending_matches, 1U);
  EXPECT_FALSE(state().paths.empty());
  EXPECT_EQ(take(processor->pending_occurrences(2))[0].id, audit_id);
  auto expired = take(processor->advance(10));
  EXPECT_TRUE(expired.operation_status.ok()) << expired.operation_status.message();
  auto outbox = take(processor->pending_occurrences(2));
  ASSERT_EQ(outbox.size(), 2U);
  ASSERT_TRUE(outbox[1].cep_match);
  EXPECT_EQ(outbox[1].cep_match->matched_at_us, 1);
  EXPECT_EQ(outbox[1].cep_match->emitted_at_us, 10);
  EXPECT_EQ(std::get<std::int64_t>((*outbox[1].reads.at("total"))[1]), 30);
  EXPECT_EQ(state().pending_matches, 0U);
  EXPECT_TRUE(state().paths.empty());
  EXPECT_TRUE(take(processor->advance(20)).operation_status.ok());
  open();
  EXPECT_EQ(take(processor->pending_occurrences(2))[1].id, outbox[1].id);
}

TEST_P(CepOwnerContract, CapturedSinkAndSimpleFsmShareTheCommitAndFrozenReadPhase) {
  const auto capture = take(catalog->define_sql("CREATE TABLE captures (k STRING NOT NULL, value "
                                                "INT64 NOT NULL, PRIMARY KEY(k)) PARTITION BY(k)"));
  ProcessorRule simple;
  simple.definition = on_event("simple", "B");
  simple.sink = OccurrenceSink{capture, [](const TriggerOccurrence& o) noexcept -> StatusOr<Row> {
                                 return Row{o.scope[0], std::int64_t(999)};
                               }};
  config.rules = {simple};
  auto cep = rule();
  cep.definition.reads = {"captured", "total"};
  cep.reads = {{"captured", capture}, {"total", totals}};
  cep.definition.pattern = CepPattern::event("a", "A").then(
      CepPattern::event("b", "B",
                        [](const CepContext& c) noexcept -> StatusOr<bool> {
                          return !c.reads.at("captured").has_value();
                        }),
      CepContiguity::FollowedByAny);
  config.cep_rules = {cep};
  open();
  process(event(0, "A", 0));
  process(event(1, "A", 1));
  auto result = process(event(2, "B", 2));
  ASSERT_EQ(result.committed_rounds.size(), 1U);
  EXPECT_EQ(result.committed_rounds[0].occurrences.size(), 3U);
  auto rows = take(processor->query_sql("SELECT value FROM captures WHERE k='a'"));
  ASSERT_EQ(rows.rows.size(), 1U);
  EXPECT_EQ(rows.rows[0][0], Value{std::int64_t(999)});
  EXPECT_FALSE(result.committed_rounds[0].occurrences[1].reads.at("captured"));
  EXPECT_FALSE(result.committed_rounds[0].occurrences[2].reads.at("captured"));
  open();
  EXPECT_EQ(take(processor->pending_occurrences(10)).size(), 3U);
}

TEST_P(CepOwnerContract, FailedCepRoundRollsBackEarlierFsmAndSinkWrites) {
  const auto capture = take(catalog->define_sql("CREATE TABLE captures (k STRING NOT NULL, value "
                                                "INT64 NOT NULL, PRIMARY KEY(k)) PARTITION BY(k)"));
  ProcessorRule simple;
  simple.definition = on_event("simple", "A", {}, true);
  simple.sink = OccurrenceSink{capture, [](const TriggerOccurrence& o) noexcept -> StatusOr<Row> {
                                 return Row{o.scope[0], std::int64_t(999)};
                               }};
  config.rules = {simple};
  auto cep = rule();
  cep.definition.pattern = CepPattern::event("a", "A");
  config.cep_rules = {cep};
  config.max_occurrences = 1;
  open();
  const auto before = take(store->status(0)).revision;
  const auto result = take(processor->process(event(0, "A", 0)));
  EXPECT_EQ(result.operation_status.code(), ErrorCode::ResourceExhausted);
  EXPECT_FALSE(processor->failed());
  EXPECT_EQ(take(store->status(0)).revision, before);
  EXPECT_TRUE(take(processor->processed_positions()).empty());
  EXPECT_TRUE(take(processor->pending_occurrences(1)).empty());
  EXPECT_TRUE(take(processor->query_sql("SELECT value FROM captures WHERE k='a'")).rows.empty());
  EXPECT_EQ(total(), 0);
  EXPECT_FALSE(processor->read_fsm("simple", Row{std::string("a")}).ok());
  EXPECT_FALSE(processor->read_cep("overlap", Row{std::string("a")}).ok());
}

TEST_P(CepOwnerContract, CommittedExpiryRemainsVisibleWhenLaterDataCapacityFails) {
  auto cep = rule();
  cep.definition.limits.max_paths = 1;
  config.cep_rules = {cep};
  config.ttl_policies = {{trades, 5}};
  open();
  process(event(0, "A", 0, 10));
  const auto before = take(store->status(0)).revision;
  auto result = take(processor->process(event(1, "A", 5, 20)));
  EXPECT_EQ(result.operation_status.code(), ErrorCode::ResourceExhausted);
  EXPECT_FALSE(result.input_committed);
  ASSERT_EQ(result.committed_rounds.size(), 1U);
  EXPECT_EQ(result.committed_rounds[0].revision, before + 1);
  EXPECT_EQ(total(), 0);
  EXPECT_EQ(state().event_sequence, 1U);
  EXPECT_EQ(take(processor->processed_positions())[0].next_offset, 1);
  EXPECT_EQ(processor->processing_time_us(), 5);
  EXPECT_FALSE(processor->failed());
}

TEST_P(CepOwnerContract, MalformedCepOutboxCannotReopenAsAValidMatch) {
  auto cep = rule();
  cep.definition.pattern = CepPattern::event("a", "A");
  config.cep_rules = {cep};
  open();
  process(event(0, "A", 0));
  auto batch = store->batches.back();
  batch.expected_revision = take(store->status(0)).revision;
  batch.commit_id = take(make_commit_id(batch.owner, batch.shard, batch.expected_revision));
  bool changed = false;
  for (auto& mutation : batch.mutations) {
    if (!mutation.value)
      continue;
    auto json = parse_json_exact(*mutation.value);
    if (!json.ok() || !json->is_object() || !json->contains("outbox"))
      continue;
    (*json)["outbox"][0]["cep_match"]["events"][0][1] = "changed-event-id";
    mutation.value = json->dump();
    changed = true;
  }
  ASSERT_TRUE(changed);
  EXPECT_EQ(take(store->commit(batch)).code, CommitCode::Committed);
  processor.reset();
  EXPECT_FALSE(EventProcessor::open(store, take(catalog->snapshot()), engine_config, config).ok());
}

TEST_P(CepOwnerContract, RejectsUnemittableDelayedWitnessBeforeItCanBlockExpiry) {
  auto cep = rule();
  cep.definition.pattern = CepPattern::event("a", "A")
                               .then(CepPattern::event("z", "Z"))
                               .or_else(CepPattern::event("b", "B"));
  cep.definition.skip = CepSkipStrategy::SkipToNext;
  config.cep_rules = {cep};
  config.max_occurrence_bytes = 768;
  open();
  process(event(0, "A", 0));
  auto oversized = event(1, "B", 1);
  oversized.values = {std::string(1024, 'x')};
  const auto before = take(store->status(0)).revision;
  auto rejected = take(processor->process(oversized));
  EXPECT_EQ(rejected.operation_status.code(), ErrorCode::ResourceExhausted);
  EXPECT_FALSE(rejected.input_committed);
  EXPECT_EQ(take(store->status(0)).revision, before);
  EXPECT_EQ(state().pending_matches, 0U);
  EXPECT_FALSE(processor->failed());
  EXPECT_TRUE(take(processor->advance(10)).operation_status.ok());
}

TEST_P(CepOwnerContract, CepSinkPersistsBothTypedCapturesWithTheMatchesAndSourcePosition) {
  const auto capture =
      take(catalog->define_sql("CREATE TABLE matches (k STRING NOT NULL, oid STRING NOT NULL, qty "
                               "INT64 NOT NULL, PRIMARY KEY(k,oid)) PARTITION BY(k)"));
  take(catalog->define_sql("CREATE MATERIALIZED VIEW matched_total AS SELECT k, SUM(qty) AS total "
                           "FROM matches GROUP BY k"));
  auto cep = rule();
  cep.sink = OccurrenceSink{capture, [](const TriggerOccurrence& o) noexcept -> StatusOr<Row> {
                              return Row{o.scope[0], o.id, o.cep_match->events[0].event.values[0]};
                            }};
  config.cep_rules = {cep};
  open();
  process(event(0, "A", 0, 10));
  process(event(1, "A", 1, 20));
  const auto count = store->batches.size();
  auto result = process(event(2, "B", 2, 15));
  ASSERT_EQ(result.committed_rounds.size(), 1U);
  EXPECT_EQ(store->batches.size(), count + 1);
  EXPECT_EQ(result.committed_rounds[0].occurrences.size(), 2U);
  auto query = take(processor->query_sql("SELECT total FROM matched_total WHERE k='a'"));
  ASSERT_EQ(query.rows.size(), 1U);
  EXPECT_EQ(query.rows[0][0], Value{std::int64_t(30)});
  open();
  EXPECT_EQ(take(processor->pending_occurrences(10)).size(), 2U);
  EXPECT_EQ(take(processor->processed_positions())[0].next_offset, 3);
}

TEST_P(CepOwnerContract, PermanentCallbackFailureRetainsTheExistingFailStopContract) {
  auto cep = rule();
  cep.definition.pattern =
      CepPattern::event("a", "A", [](const CepContext&) noexcept -> StatusOr<bool> {
        return Status(ErrorCode::InvalidArgument, "permanent callback failure");
      });
  config.cep_rules = {cep};
  open();
  const auto revision = take(store->status(0)).revision;
  const auto result = take(processor->process(event(0, "A", 0)));
  EXPECT_EQ(result.operation_status.code(), ErrorCode::InvalidArgument);
  EXPECT_FALSE(result.input_committed);
  EXPECT_TRUE(processor->failed());
  EXPECT_EQ(take(store->status(0)).revision, revision);
}

TEST_P(CepOwnerContract, FutureFsmTimeoutAndDelayedCepMustFitTheSameControlRound) {
  auto cep = rule();
  cep.definition.pattern = CepPattern::event("a", "A")
                               .then(CepPattern::event("z", "Z"))
                               .or_else(CepPattern::event("b", "B"));
  cep.definition.skip = CepSkipStrategy::SkipToNext;
  config.cep_rules = {cep};
  config.max_occurrences = 1;
  ProcessorRule alarm;
  alarm.definition.id = "alarm";
  alarm.definition.initial = "idle";
  alarm.definition.states = {{"idle", {}}, {"waiting", 10}, {"done", {}}};
  alarm.definition.transitions = {{"idle", "A", "waiting", {}, {}, {}},
                                  {"waiting", "$timeout", "done", {}, {}, "alarm"}};
  config.rules = {alarm};
  open();
  process(event(0, "A", 0));
  const auto before = take(store->status(0)).revision;
  const auto rejected = take(processor->process(event(1, "B", 1)));
  EXPECT_EQ(rejected.operation_status.code(), ErrorCode::ResourceExhausted);
  EXPECT_FALSE(rejected.input_committed);
  EXPECT_EQ(take(store->status(0)).revision, before);
  EXPECT_EQ(state().pending_matches, 0U);
  auto expired = take(processor->advance(10));
  EXPECT_TRUE(expired.operation_status.ok()) << expired.operation_status.message();
  auto output = take(processor->pending_occurrences(1));
  ASSERT_EQ(output.size(), 1U);
  EXPECT_EQ(output[0].rule_id, "alarm");
  EXPECT_FALSE(processor->failed());
}

TEST_P(CepOwnerContract, PendingSnapshotAdmissionAndRecoveryReserveTheWholeTimeoutEnvelope) {
  catalog = take(Catalog::create());
  trades = take(catalog->define_sql("CREATE TABLE trades (k STRING NOT NULL, id INT64 NOT NULL, "
                                    "qty INT64 NOT NULL, PRIMARY KEY(k,id)) PARTITION BY(k)"));
  config.input_relations = {trades};
  auto cep = rule();
  cep.definition.pattern = CepPattern::event("a", "A")
                               .then(CepPattern::event("z", "Z"))
                               .or_else(CepPattern::event("b", "B"));
  cep.definition.skip = CepSkipStrategy::SkipToNext;
  config.cep_rules = {cep};
  open();
  process(event(0, "A", 0));
  process(event(1, "B", 1));
  const auto key = take(row_key(0, 'S', config.namespace_id, Row{std::string("lifecycle")}));
  const auto stored = take(store->get_many(0, {key}));
  ASSERT_TRUE(stored.values[0]);
  config.max_state_bytes = stored.values[0]->size();
  processor.reset();
  const auto reopened =
      EventProcessor::open(store, take(catalog->snapshot()), engine_config, config);
  ASSERT_FALSE(reopened.ok());
  EXPECT_EQ(reopened.status().code(), ErrorCode::ResourceExhausted);
  store.reset();
  StoreConfig storage;
  storage.shard_count = 1;
  std::unique_ptr<IStateStore> fresh;
  if (GetParam() % 2 == 0)
    fresh = take(MemoryStateStore::open(storage));
  else
    fresh = take(RocksStateStore::open(directory + "/tight", storage));
  local_backend = fresh.get();
  if (GetParam() >= 2)
    fresh = take(RemoteHarnessStore::open(std::move(fresh)));
  store = std::make_shared<CepObservedStore>(std::shared_ptr<IStateStore>(std::move(fresh)));
  open();
  process(event(0, "A", 0));
  const auto before = take(store->status(0)).revision;
  const auto rejected = take(processor->process(event(1, "B", 1)));
  EXPECT_EQ(rejected.operation_status.code(), ErrorCode::ResourceExhausted);
  EXPECT_FALSE(rejected.input_committed);
  EXPECT_EQ(take(store->status(0)).revision, before);
  EXPECT_TRUE(take(processor->advance(10)).operation_status.ok());
}

TEST_P(CepOwnerContract, CapacityOneTimerChainCanPublishAckAndAdvanceAgain) {
  config.cep_rules = {rule()};
  config.max_occurrences = 1;
  ProcessorRule alarm;
  alarm.definition.id = "alarm";
  alarm.definition.initial = "idle";
  alarm.definition.states = {{"idle", {}}, {"waiting", 10}, {"again", 10}, {"done", {}}};
  alarm.definition.transitions = {{"idle", "Arm", "waiting", {}, {}, {}},
                                  {"waiting", "$timeout", "again", {}, {}, "first"},
                                  {"again", "$timeout", "done", {}, {}, "second"}};
  config.rules = {alarm};
  open();
  process(event(0, "Arm", 0));
  auto first = take(processor->advance(10));
  EXPECT_TRUE(first.operation_status.ok()) << first.operation_status.message();
  auto output = take(processor->pending_occurrences(1));
  ASSERT_EQ(output.size(), 1U);
  EXPECT_EQ(output[0].emission.name, "first");
  EXPECT_TRUE(take(processor->acknowledge_occurrences({output[0].id})).operation_status.ok());
  auto second = take(processor->advance(20));
  EXPECT_TRUE(second.operation_status.ok()) << second.operation_status.message();
  output = take(processor->pending_occurrences(1));
  ASSERT_EQ(output.size(), 1U);
  EXPECT_EQ(output[0].emission.name, "second");
  EXPECT_FALSE(processor->failed());
}

TEST_P(CepOwnerContract, ImpossiblePeriodicCepBudgetsAreRejectedBeforeOpeningEngine) {
  config.cep_rules = {rule()};
  config.periodic_policies = {{"clock", "tick", 5, 0}};
  const auto rejected =
      EventProcessor::open(store, take(catalog->snapshot()), engine_config, config);
  ASSERT_FALSE(rejected.ok());
  EXPECT_EQ(rejected.status().code(), ErrorCode::ResourceExhausted);
  EXPECT_NE(rejected.status().message().find("declared state budgets"), std::string_view::npos);
  EXPECT_EQ(take(store->status(0)).revision, 0U);
}

TEST_P(CepOwnerContract, CoordinatedPeriodicCepAndFsmBudgetsPermitRealClockTakeAndRecovery) {
  auto cep = rule();
  cep.definition.pattern = CepPattern::event("a", "A").then(CepPattern::event("clock", "tick"));
  cep.definition.reads = {"total"};
  cep.reads = {{"total", totals}};
  cep.definition.limits.max_state_bytes = 64 * 1024;
  cep.definition.limits.max_read_bytes = 4096;
  cep.definition.limits.max_event_bytes = 4096;
  cep.definition.limits.max_register_bytes = 4096;
  cep.definition.limits.max_matches_per_step = 4;
  cep.definition.limits.max_paths = 8;
  config.cep_rules = {cep};
  ProcessorRule alarm;
  alarm.definition = on_event("clock-rule", "tick");
  config.rules = {alarm};
  config.periodic_policies = {{"clock", "tick", 5, 0}};
  config.max_occurrences = 8;
  open();
  process(event(0, "A", 0, 7));
  open();
  const auto advanced = take(processor->advance(5));
  ASSERT_TRUE(advanced.operation_status.ok()) << advanced.operation_status.message();
  ASSERT_EQ(advanced.committed_rounds.size(), 1U);
  const auto& output = advanced.committed_rounds[0].occurrences;
  ASSERT_EQ(output.size(), 2U);
  ASSERT_TRUE(output[1].cep_match);
  ASSERT_EQ(output[1].cep_match->events.size(), 2U);
  EXPECT_EQ(output[1].cep_match->events[1].event.kind, "tick");
  EXPECT_EQ(output[1].cep_match->events[1].event.time_us, 5);
  EXPECT_EQ(std::get<std::int64_t>((*output[1].reads.at("total"))[1]), 7);
  EXPECT_EQ(state().event_sequence, 2U);
  open();
  const auto coalesced = take(processor->advance(20));
  ASSERT_TRUE(coalesced.operation_status.ok()) << coalesced.operation_status.message();
  ASSERT_EQ(coalesced.committed_rounds[0].clock_firings.size(), 1U);
  EXPECT_EQ(coalesced.committed_rounds[0].clock_firings[0].skipped_slots, 2U);
  EXPECT_EQ(take(processor->pending_occurrences(8)).size(), 3U);
}

TEST_P(CepOwnerContract, MaximumUnsignedOutputBudgetCannotOverflowControlReservationArithmetic) {
  auto cep = rule();
  cep.definition.pattern = CepPattern::event("a", "A")
                               .then(CepPattern::event("z", "Z"))
                               .or_else(CepPattern::event("b", "B"));
  cep.definition.skip = CepSkipStrategy::SkipToNext;
  config.cep_rules = {cep};
  config.max_occurrence_bytes = std::numeric_limits<std::size_t>::max();
  config.max_outbox_bytes = std::numeric_limits<std::size_t>::max();
  open();
  process(event(0, "A", 0));
  const auto before = take(store->status(0)).revision;
  const auto rejected = take(processor->process(event(1, "B", 1)));
  EXPECT_EQ(rejected.operation_status.code(), ErrorCode::ResourceExhausted);
  EXPECT_FALSE(rejected.input_committed);
  EXPECT_EQ(take(store->status(0)).revision, before);
  EXPECT_TRUE(take(processor->advance(10)).operation_status.ok());
  EXPECT_FALSE(processor->failed());
}

INSTANTIATE_TEST_SUITE_P(MemoryRocksHttp, CepOwnerContract, ::testing::Values(0, 1, 2, 3));
} // namespace
} // namespace eventflow::test
