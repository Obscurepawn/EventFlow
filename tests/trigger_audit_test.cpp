#include "eventflow/computation.hpp"
#include "eventflow/key_codec.hpp"
#include "status_test_support.hpp"
#include <gtest/gtest.h>
#include <set>

namespace eventflow::test {
namespace {

struct AuditComputation {
  BoundComputation bound;
  RelationId trades = 0;
};

StatusOr<AuditComputation> define_audit_computation(TriggerAuditConfig audit = {}) noexcept {
  using namespace std::chrono_literals;
  EF_ASSIGN_OR_RETURN(auto catalog, Catalog::create());
  EF_ASSIGN_OR_RETURN(
      auto trades,
      catalog->define_sql(
          "CREATE TABLE trades(ticker STRING NOT NULL, trade_id INT64 NOT NULL, quantity INT64 "
          "NOT NULL, PRIMARY KEY(ticker,trade_id)) PARTITION BY(ticker)"));
  EF_ASSIGN_OR_RETURN(
      auto totals, catalog->define_sql(
                       "CREATE MATERIALIZED VIEW totals AS SELECT ticker,SUM(quantity) AS volume "
                       "FROM trades GROUP BY ticker"));
  (void)totals;
  EF_ASSIGN_OR_RETURN(
      auto ready,
      catalog->define_sql(
          "CREATE MATERIALIZED VIEW volume_ready AS SELECT ticker,volume,volume>100 AS ready "
          "FROM totals"));
  const auto source = EventRef::source({.kind = "trade", .payload_relation = trades});
  const auto condition = Trigger::on_event("volume_condition", source)
                             .when(ViewRef(ready).column<bool>("ready").is_true());
  const auto clock =
      Trigger::clock("volume_clock",
                     {.policy_id = "five_seconds", .every = 5s, .phase = 0s, .event_kind = "tick"});
  ProcessorConfig definition;
  definition.input_relations = {trades};
  definition.trigger_audit = audit;
  EF_ASSIGN_OR_RETURN(auto snapshot, catalog->snapshot());
  EF_ASSIGN_OR_RETURN(auto bound, TriggerSet::bind(std::move(snapshot), std::move(definition),
                                                   {condition, clock}, {.shard_count = 1}));
  return AuditComputation{.bound = std::move(bound), .trades = trades};
}

class LostAuditReplyStore final : public IStateStore {
public:
  explicit LostAuditReplyStore(std::shared_ptr<IStateStore> store) noexcept
      : store_(std::move(store)) {}

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
    EF_ASSIGN_OR_RETURN(auto committed, store_->commit(batch));
    if (lose_reply && committed.code == CommitCode::Committed) {
      lose_reply = false;
      return CommitResult{.code = CommitCode::Unknown,
                          .revision = batch.expected_revision,
                          .duplicate = false,
                          .message = "test lost audit commit reply"};
    }
    return committed;
  }

  StatusOr<ShardStatus> status(std::uint32_t shard) noexcept override {
    return store_->status(shard);
  }

  StatusOr<StoreLimits> limits() noexcept override {
    return store_->limits();
  }

  bool lose_reply = false;

private:
  std::shared_ptr<IStateStore> store_;
};

struct AuditDriver {
  AuditComputation computation;
  std::shared_ptr<MemoryStateStore> store;
  std::unique_ptr<EventProcessor> processor;
};

StatusOr<AuditDriver> open_audit_driver(TriggerAuditConfig config = {}) noexcept {
  EF_ASSIGN_OR_RETURN(auto computation, define_audit_computation(config));
  EF_ASSIGN_OR_RETURN(auto memory, MemoryStateStore::open({.shard_count = 1, .shard_owners = {}}));
  auto store = std::shared_ptr<MemoryStateStore>(std::move(memory));
  EF_ASSIGN_OR_RETURN(auto processor,
                      EventProcessor::open(store, computation.bound.catalog, {.shard_count = 1},
                                           computation.bound.definition));
  return AuditDriver{.computation = std::move(computation),
                     .store = std::move(store),
                     .processor = std::move(processor)};
}

ProcessorEvent trade_event(RelationId relation, std::int64_t offset, std::int64_t quantity,
                           std::int64_t now_us, std::string key = "AAPL") noexcept {
  const Row row{key, offset, quantity};
  return ProcessorEvent{
      .source = {.source_id = "audit_feed", .topic = "trades", .partition = 0, .offset = offset},
      .scope = {key},
      .kind = "trade",
      .values = row,
      .changes = {{.relation = relation, .identity = {key, offset}, .row = row}},
      .processing_time_us = now_us};
}

Status accepted(EventProcessor& processor, const ProcessorEvent& event) noexcept {
  EF_ASSIGN_OR_RETURN(auto result, processor.process(event));
  EF_RETURN_IF_ERROR(result.operation_status);
  return check(result.input_committed, "expected confirmed test input");
}

Status advance_audit(EventProcessor& processor, std::int64_t now_us) noexcept {
  EF_ASSIGN_OR_RETURN(auto result, processor.advance(now_us));
  return result.operation_status;
}

StatusOr<std::size_t> audit_charge(const AuditDriver& driver, const Row& row) noexcept {
  EF_ASSIGN_OR_RETURN(auto relation, driver.computation.bound.catalog.find("trigger_audit"));
  EF_ASSIGN_OR_RETURN(auto identity, physical_identity(*relation, row));
  EF_ASSIGN_OR_RETURN(auto key, row_key(0, 'T', relation->id, identity));
  EF_ASSIGN_OR_RETURN(auto bytes, encode_row(relation->storage_schema, row));
  return key.size() + bytes.size();
}

TEST(TriggerAudit, BindingCreatesBuiltinRelationWithoutUserDdl) {
  auto created = define_audit_computation();
  ASSERT_TRUE(created.ok()) << created.status().message();
  auto audit = created->bound.catalog.find("trigger_audit");
  ASSERT_TRUE(audit.ok()) << audit.status().message();
  EXPECT_EQ((*audit)->kind, RelationKind::BaseTable);
  EXPECT_EQ((*audit)->base_write_role, BaseWriteRole::TriggerAudit);
  EXPECT_EQ((*audit)->partition_types, (std::vector<DataType>{DataType::String}));
}

TEST(TriggerAudit, SourceAndClockRowsSurviveOutboxAcknowledgementAndExactReplay) {
  auto driver = take(open_audit_driver());
  const auto event = trade_event(driver.computation.trades, 0, 101, 100);
  auto result = take(driver.processor->process(event));
  ASSERT_TRUE(result.operation_status.ok()) << result.operation_status.message();
  ASSERT_TRUE(result.input_committed);
  ASSERT_EQ(result.committed_rounds.back().occurrences.size(), 1U);
  const auto occurrence = result.committed_rounds.back().occurrences.front();
  auto audit = take(driver.processor->query_sql(
      "SELECT occurrence_id,rule_id,event_kind,occurred_at_us,evaluated_at_us,"
      "firing_revision,source_id,source_topic,source_partition,source_offset,request_id "
      "FROM trigger_audit WHERE scope_key_0='AAPL'"));
  EXPECT_EQ(audit.rows,
            (std::vector<Row>{{occurrence.id, std::string("volume_condition"), std::string("trade"),
                               std::int64_t{100}, std::int64_t{100},
                               std::to_string(result.committed_rounds.back().revision),
                               std::string("audit_feed"), std::string("trades"), std::int64_t{0},
                               std::int64_t{0}, std::monostate{}}}));
  EXPECT_TRUE(
      take(driver.processor->acknowledge_occurrences({occurrence.id})).operation_status.ok());
  EXPECT_TRUE(take(driver.processor->pending_occurrences(128)).empty());
  auto duplicate = take(driver.processor->process(event));
  EXPECT_TRUE(duplicate.input_committed && duplicate.duplicate);
  EXPECT_EQ(take(driver.processor->query_sql("SELECT occurrence_id FROM trigger_audit "
                                             "WHERE scope_key_0='AAPL'"))
                .rows.size(),
            1U);
  EXPECT_TRUE(advance_audit(*driver.processor, 5'000'000).ok());
  auto clock = take(driver.processor->query_sql(
      "SELECT scheduled_at_us,skipped_slots,source_id,source_offset FROM trigger_audit "
      "WHERE scope_key_0='AAPL' AND rule_id='volume_clock'"));
  EXPECT_EQ(clock.rows, (std::vector<Row>{{std::int64_t{5'000'000}, std::string("0"),
                                           std::monostate{}, std::monostate{}}}));
  EXPECT_EQ(take(driver.processor->processed_positions()).front().next_offset, 1);
}

TEST(TriggerAudit, RowRetentionIsPerBusinessKeyAndKeepsTheLatestFirings) {
  auto driver = take(open_audit_driver({.max_rows_per_scope = 2}));
  ASSERT_TRUE(accepted(*driver.processor, trade_event(driver.computation.trades, 0, 0, 100)).ok());
  ASSERT_TRUE(
      accepted(*driver.processor, trade_event(driver.computation.trades, 1, 0, 200, "MSFT")).ok());
  for (const auto now : {5'000'000, 10'000'000, 15'000'000})
    ASSERT_TRUE(advance_audit(*driver.processor, now).ok());
  for (const auto& key : {"AAPL", "MSFT"}) {
    auto result = take(driver.processor->query_sql("SELECT evaluated_at_us FROM trigger_audit",
                                                   {.scope = Row{std::string(key)}}));
    ASSERT_EQ(result.rows.size(), 2U);
    std::set<std::int64_t> times;
    for (const auto& row : result.rows)
      times.insert(std::get<std::int64_t>(row[0]));
    EXPECT_EQ(times, (std::set<std::int64_t>{10'000'000, 15'000'000}));
  }
}

TEST(TriggerAudit, ByteRetentionIncludesTheStorageKeyAndDoesNotDependOnOutboxAck) {
  auto sample = take(open_audit_driver());
  ASSERT_TRUE(accepted(*sample.processor, trade_event(sample.computation.trades, 0, 0, 100)).ok());
  ASSERT_TRUE(advance_audit(*sample.processor, 5'000'000).ok());
  auto row =
      take(sample.processor->query_sql("SELECT * FROM trigger_audit WHERE scope_key_0='AAPL'"))
          .rows.front();
  const auto budget = take(audit_charge(sample, row)) + 64;
  auto driver = take(open_audit_driver({.max_bytes_per_scope = budget}));
  ASSERT_TRUE(accepted(*driver.processor, trade_event(driver.computation.trades, 0, 0, 100)).ok());
  for (const auto now : {5'000'000, 10'000'000, 15'000'000})
    ASSERT_TRUE(advance_audit(*driver.processor, now).ok());
  auto rows =
      take(driver.processor->query_sql("SELECT * FROM trigger_audit WHERE scope_key_0='AAPL'"));
  ASSERT_EQ(rows.rows.size(), 1U);
  EXPECT_LE(take(audit_charge(driver, rows.rows[0])), budget);
  EXPECT_EQ(take(driver.processor->query_sql("SELECT evaluated_at_us FROM trigger_audit "
                                             "WHERE scope_key_0='AAPL'"))
                .rows,
            (std::vector<Row>{{std::int64_t{15'000'000}}}));
  EXPECT_EQ(take(driver.processor->pending_occurrences(128)).size(), 3U);
}

TEST(TriggerAudit, OversizedAuditRollsBackInputAndFsmWithoutPublishingAPhantomRow) {
  auto driver = take(open_audit_driver({.max_bytes_per_scope = 1}));
  auto result =
      take(driver.processor->process(trade_event(driver.computation.trades, 0, 101, 100)));
  EXPECT_FALSE(result.input_committed);
  EXPECT_EQ(result.operation_status.code(), ErrorCode::ResourceExhausted);
  EXPECT_FALSE(driver.processor->failed());
  EXPECT_TRUE(
      take(driver.processor->query_sql("SELECT * FROM trades WHERE ticker='AAPL'")).rows.empty());
  EXPECT_TRUE(
      take(driver.processor->query_sql("SELECT * FROM trigger_audit WHERE scope_key_0='AAPL'"))
          .rows.empty());
  EXPECT_TRUE(take(driver.processor->processed_positions()).empty());
  EXPECT_TRUE(take(driver.processor->pending_occurrences(128)).empty());
}

TEST(TriggerAudit, DefaultNameCollisionFailsBindingWithoutMutatingTheInputCatalog) {
  auto catalog = take(Catalog::create());
  auto input =
      take(catalog->define_sql("CREATE TABLE events(k STRING,PRIMARY KEY(k)) PARTITION BY(k)"));
  take(catalog->define_sql("CREATE TABLE trigger_audit(k STRING,PRIMARY KEY(k)) PARTITION BY(k)"));
  auto before = take(catalog->snapshot());
  ProcessorConfig definition;
  definition.input_relations = {input};
  auto bound = TriggerSet::bind(before, definition, {Trigger::on_event("rule", "events")},
                                {.shard_count = 1});
  ASSERT_FALSE(bound.ok());
  auto after = take(catalog->snapshot());
  EXPECT_EQ(after.fingerprint, before.fingerprint);
  EXPECT_EQ(after.version, before.version);
}

TEST(TriggerAudit, CanonicalScopeColumnsSupportCompositeKeysWithMetadataLikeNames) {
  auto catalog = take(Catalog::create());
  auto input = take(
      catalog->define_sql("CREATE TABLE events(rule_id STRING,request_id INT64,v INT64,"
                          "PRIMARY KEY(rule_id,request_id)) PARTITION BY(rule_id,request_id)"));
  ProcessorConfig definition;
  definition.input_relations = {input};
  auto bound = take(TriggerSet::bind(
      take(catalog->snapshot()), definition,
      {Trigger::on_event("composite_rule", "events").version(UINT64_MAX)}, {.shard_count = 1}));
  EXPECT_EQ(bound.explain()["trigger_audit"]["scope_columns"][0]["source_column"], "rule_id");
  EXPECT_EQ(bound.explain()["trigger_audit"]["scope_columns"][1]["source_column"], "request_id");
  auto memory = take(MemoryStateStore::open({.shard_count = 1, .shard_owners = {}}));
  auto processor = take(EventProcessor::open(std::shared_ptr<IStateStore>(std::move(memory)),
                                             bound.catalog, {.shard_count = 1}, bound.definition));
  const Row key{std::string("AAPL"), std::int64_t{7}};
  const Row row{std::string("AAPL"), std::int64_t{7}, std::int64_t{10}};
  auto result = take(processor->process(
      {.source = {.source_id = "composite", .topic = "events", .partition = 0, .offset = 0},
       .scope = key,
       .kind = "events",
       .values = row,
       .changes = {{.relation = input, .identity = key, .row = row}},
       .processing_time_us = 100}));
  ASSERT_TRUE(result.operation_status.ok()) << result.operation_status.message();
  auto audit = take(processor->query_sql("SELECT scope_key_0,scope_key_1,rule_id,rule_version "
                                         "FROM trigger_audit",
                                         {.scope = key}));
  EXPECT_EQ(audit.rows,
            (std::vector<Row>{{std::string("AAPL"), std::int64_t{7}, std::string("composite_rule"),
                               std::to_string(UINT64_MAX)}}));
}

TEST(TriggerAudit, RestoreRetainsAcknowledgedHistoryAndRejectsChangedRetentionPolicy) {
  auto driver = take(open_audit_driver());
  const auto event = trade_event(driver.computation.trades, 0, 101, 100);
  ASSERT_TRUE(accepted(*driver.processor, event).ok());
  auto occurrences = take(driver.processor->pending_occurrences(128));
  ASSERT_EQ(occurrences.size(), 1U);
  ASSERT_TRUE(
      take(driver.processor->acknowledge_occurrences({occurrences[0].id})).operation_status.ok());
  auto before =
      take(driver.processor->query_sql("SELECT * FROM trigger_audit WHERE scope_key_0='AAPL'"));
  take(driver.store->create_snapshot("audit-history"));
  auto restored = std::shared_ptr<MemoryStateStore>(
      take(MemoryStateStore::from_snapshot(*driver.store, "audit-history")));
  auto reopened =
      take(EventProcessor::open(restored, driver.computation.bound.catalog, {.shard_count = 1},
                                driver.computation.bound.definition));
  auto duplicate = take(reopened->process(event));
  ASSERT_TRUE(duplicate.input_committed && duplicate.duplicate);
  EXPECT_EQ(take(reopened->query_sql("SELECT * FROM trigger_audit WHERE scope_key_0='AAPL'")).rows,
            before.rows);
  EXPECT_TRUE(take(reopened->pending_occurrences(128)).empty());
  auto changed = driver.computation.bound.definition;
  changed.trigger_audit.max_rows_per_scope = 2;
  EXPECT_FALSE(
      EventProcessor::open(restored, driver.computation.bound.catalog, {.shard_count = 1}, changed)
          .ok());
}

TEST(TriggerAudit, UnknownSourceCommitConfirmsExactlyOneFiringAndOneInputContribution) {
  auto computation = take(define_audit_computation());
  auto memory = take(MemoryStateStore::open({.shard_count = 1, .shard_owners = {}}));
  auto store =
      std::make_shared<LostAuditReplyStore>(std::shared_ptr<IStateStore>(std::move(memory)));
  auto processor = take(EventProcessor::open(store, computation.bound.catalog, {.shard_count = 1},
                                             computation.bound.definition));
  const auto event = trade_event(computation.trades, 0, 101, 100);
  store->lose_reply = true;
  auto pending = take(processor->process(event));
  EXPECT_EQ(pending.operation_status.code(), ErrorCode::UnknownCommit);
  EXPECT_FALSE(pending.input_committed);
  ASSERT_TRUE(processor->has_pending_commit());
  EXPECT_FALSE(processor->query_sql("SELECT * FROM trigger_audit WHERE scope_key_0='AAPL'").ok());
  auto confirmed = take(processor->resolve_pending_commit());
  ASSERT_TRUE(confirmed.operation_status.ok()) << confirmed.operation_status.message();
  EXPECT_TRUE(confirmed.input_committed);
  EXPECT_EQ(take(processor->query_sql("SELECT volume FROM totals WHERE ticker='AAPL'")).rows,
            (std::vector<Row>{{std::int64_t{101}}}));
  auto history =
      take(processor->query_sql("SELECT occurrence_id,firing_revision FROM trigger_audit "
                                "WHERE scope_key_0='AAPL'"));
  ASSERT_EQ(history.rows.size(), 1U);
  EXPECT_EQ(std::get<std::string>(history.rows[0][1]),
            std::to_string(take(store->status(0)).revision));
  auto duplicate = take(processor->process(event));
  EXPECT_TRUE(duplicate.input_committed && duplicate.duplicate);
  EXPECT_EQ(take(processor->query_sql("SELECT occurrence_id,firing_revision FROM trigger_audit "
                                      "WHERE scope_key_0='AAPL'"))
                .rows,
            history.rows);
}

TEST(TriggerAudit, DelayedClockAuditsTheScheduledSlotAndActualEvaluationSeparately) {
  auto driver = take(open_audit_driver());
  ASSERT_TRUE(accepted(*driver.processor, trade_event(driver.computation.trades, 0, 0, 100)).ok());
  ASSERT_TRUE(advance_audit(*driver.processor, 17'000'000).ok());
  auto rows =
      take(driver.processor->query_sql("SELECT scheduled_at_us,evaluated_at_us,skipped_slots "
                                       "FROM trigger_audit WHERE scope_key_0='AAPL'"));
  EXPECT_EQ(rows.rows, (std::vector<Row>{{std::int64_t{15'000'000}, std::int64_t{17'000'000},
                                          std::string("2")}}));
}

} // namespace
} // namespace eventflow::test
