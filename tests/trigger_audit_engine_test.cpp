#include "../src/runtime/engine_internal.hpp"
#include "eventflow/key_codec.hpp"
#include "remote_harness.hpp"
#include "status_test_support.hpp"
#include <filesystem>
#include <gtest/gtest.h>

namespace eventflow::test {
namespace {
class AuditStore final : public IStateStore {
public:
  explicit AuditStore(std::shared_ptr<IStateStore> value) noexcept : backend(std::move(value)) {}

  StatusOr<StoreLimits> limits() noexcept override {
    return backend->limits();
  }

  StatusOr<ReadResult> get_many(std::uint32_t shard,
                                const std::vector<std::string>& keys) noexcept override {
    return backend->get_many(shard, keys);
  }

  StatusOr<ScanResult> scan(std::uint32_t shard, const std::string& prefix,
                            const std::optional<std::string>& after,
                            std::size_t count) noexcept override {
    return backend->scan(shard, prefix, after, count);
  }

  StatusOr<ShardStatus> status(std::uint32_t shard) noexcept override {
    return backend->status(shard);
  }

  StatusOr<CommitResult> commit(const CommitBatch& batch) noexcept override {
    batches.push_back(batch);
    EF_ASSIGN_OR_RETURN(auto result, backend->commit(batch));
    if (unknown_next && result.code == CommitCode::Committed) {
      unknown_next = false;
      return CommitResult{.code = CommitCode::Unknown,
                          .revision = batch.expected_revision,
                          .duplicate = false,
                          .message = "lost commit response"};
    }
    return result;
  }

  std::shared_ptr<IStateStore> backend;
  std::vector<CommitBatch> batches;
  bool unknown_next = false;
};

class TriggerAuditEngine : public testing::TestWithParam<int> {
protected:
  void SetUp() noexcept override {
    catalog = take(Catalog::create());
    input = take(
        catalog->define_sql("CREATE TABLE input(k STRING,v INT64,PRIMARY KEY(k)) PARTITION BY(k)"));
    audit = take(catalog->define_sql(
        "CREATE TABLE trigger_audit(k STRING,id INT64,payload STRING,PRIMARY KEY(k,id)) "
        "PARTITION BY(k)",
        DefinitionOptions{.partition_key = {},
                          .base_write_role = BaseWriteRole::TriggerAudit,
                          .identity_domain = {}}));
    audit_count = take(catalog->define_sql(
        "CREATE MATERIALIZED VIEW audit_count AS SELECT k,COUNT(*) AS n FROM trigger_audit "
        "GROUP BY k"));
    config.shard_count = 1;
    StoreConfig storage;
    storage.shard_count = 1;
    std::unique_ptr<IStateStore> backend;
    if (GetParam() % 2 == 0) {
      backend = take(MemoryStateStore::open(storage));
    } else {
      char pattern[] = "/tmp/eventflow-trigger-audit-XXXXXX";
      const auto* made = ::mkdtemp(pattern);
      ASSERT_NE(made, nullptr);
      path = made;
      backend = take(RocksStateStore::open(path + "/db", storage));
    }
    if (GetParam() >= 2)
      backend = take(RemoteHarnessStore::open(std::move(backend)));
    store = std::make_shared<AuditStore>(std::move(backend));
  }

  void TearDown() noexcept override {
    engine.reset();
    store.reset();
    if (!path.empty()) {
      std::error_code ignored;
      std::filesystem::remove_all(path, ignored);
    }
  }

  void open() noexcept {
    engine = take(Engine::open(store, take(catalog->snapshot()), config));
    store->batches.clear();
  }

  TableChange input_write() const noexcept {
    return {.relation = input,
            .identity = {std::string("a")},
            .row = Row{std::string("a"), std::int64_t{7}}};
  }

  TableChange audit_write(std::int64_t id = 1, std::string key = "a",
                          std::string payload = "fired") const noexcept {
    return {.relation = audit, .identity = {key, id}, .row = Row{key, id, std::move(payload)}};
  }

  QueryResult query(std::string_view sql, std::string key = "a") noexcept {
    return take(engine->query_sql(sql, QueryContext{.scope = Row{std::move(key)}}));
  }

  StatusOr<DispatchResult> lifecycle(const std::vector<TableChange>& writes,
                                     bool fail_after_write = false) noexcept {
    return EngineRuntimeAccess::apply_lifecycle(
        *engine, {input_write()}, DispatchContext{.processing_time_us = 5},
        [&](RoundContext& round, const std::vector<RowChange>&) noexcept -> Status {
          for (const auto& write : writes)
            EF_RETURN_IF_ERROR(EngineRuntimeAccess::write_trigger_audit(round, write));
          if (fail_after_write)
            return Status(ErrorCode::ResourceExhausted, "injected lifecycle capacity failure");
          return {};
        });
  }

  std::string path;
  std::unique_ptr<Catalog> catalog;
  std::unique_ptr<Engine> engine;
  std::shared_ptr<AuditStore> store;
  EngineConfig config;
  RelationId input = 0;
  RelationId audit = 0;
  RelationId audit_count = 0;
};

TEST_P(TriggerAuditEngine, LifecycleAuditAndSourceCommitTogetherAndSqlUsesExactScope) {
  open();
  const auto revision = engine->revision();
  const auto result = lifecycle({audit_write(1), audit_write(2, "b")});
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(result->commit.code, CommitCode::Committed);
  EXPECT_EQ(result->commit.revision, revision + 1);
  ASSERT_EQ(store->batches.size(), 1U);
  EXPECT_EQ(query("SELECT v FROM input").rows, (std::vector<Row>{{std::int64_t{7}}}));
  EXPECT_EQ(query("SELECT id,payload FROM trigger_audit").rows,
            (std::vector<Row>{{std::int64_t{1}, std::string("fired")}}));
  EXPECT_EQ(query("SELECT id FROM trigger_audit", "b").rows, (std::vector<Row>{{std::int64_t{2}}}));
  EXPECT_EQ(query("SELECT n FROM audit_count").rows, (std::vector<Row>{{std::int64_t{1}}}));
  EXPECT_FALSE(engine->query_sql("SELECT * FROM trigger_audit").ok());
}

TEST_P(TriggerAuditEngine, OrdinaryInputsHooksAndRequestFramesCannotForgeAuditRows) {
  open();
  EXPECT_EQ(engine->apply(audit_write()).status().code(), ErrorCode::InvalidArgument);
  auto input_result = EngineRuntimeAccess::apply_lifecycle(*engine, {audit_write()}, {}, {});
  EXPECT_EQ(input_result.status().code(), ErrorCode::InvalidArgument);
  auto runtime_result = EngineRuntimeAccess::apply(
      *engine, {}, [&](RoundContext&) noexcept -> StatusOr<std::vector<TableChange>> {
        return std::vector<TableChange>{audit_write()};
      });
  EXPECT_EQ(runtime_result.status().code(), ErrorCode::InvalidArgument);
  auto runtime_capability = EngineRuntimeAccess::apply(
      *engine, {}, [&](RoundContext& round) noexcept -> StatusOr<std::vector<TableChange>> {
        EXPECT_EQ(EngineRuntimeAccess::write_trigger_audit(round, audit_write()).code(),
                  ErrorCode::FailedPrecondition);
        return std::vector<TableChange>{};
      });
  ASSERT_TRUE(runtime_capability.ok()) << runtime_capability.status().message();
  auto ordinary_hook = engine->apply(
      input_write(), {},
      [&](RoundContext& round, const std::vector<RowChange>&) noexcept -> Status {
        EXPECT_EQ(EngineRuntimeAccess::write_trigger_audit(round, audit_write()).code(),
                  ErrorCode::FailedPrecondition);
        auto write = audit_write();
        EXPECT_EQ(round
                      .emit(RowChange{.relation = audit,
                                      .identity = write.identity,
                                      .old_row = {},
                                      .new_row = write.row})
                      .code(),
                  ErrorCode::InvalidArgument);
        auto key = take(row_key(round.shard(), 'T', audit, write.identity));
        EXPECT_EQ(round.stage(Mutation{.key = key, .value = "forged"}).code(),
                  ErrorCode::InvalidArgument);
        return {};
      });
  ASSERT_TRUE(ordinary_hook.ok()) << ordinary_hook.status().message();
  EXPECT_TRUE(query("SELECT * FROM trigger_audit").rows.empty());
}

TEST_P(TriggerAuditEngine, CapacityFailureRollsBackAuditSourceAndTimeThenAllowsRetry) {
  open();
  const auto revision = engine->revision();
  auto result = lifecycle({audit_write()}, true);
  EXPECT_EQ(result.status().code(), ErrorCode::ResourceExhausted);
  EXPECT_FALSE(engine->failed());
  EXPECT_EQ(engine->revision(), revision);
  EXPECT_EQ(EngineRuntimeAccess::time_us(*engine), 0);
  EXPECT_TRUE(store->batches.empty());
  EXPECT_TRUE(query("SELECT * FROM input").rows.empty());
  EXPECT_TRUE(query("SELECT * FROM trigger_audit").rows.empty());
  ASSERT_TRUE(lifecycle({audit_write()}).ok());
  EXPECT_EQ(query("SELECT n FROM audit_count").rows, (std::vector<Row>{{std::int64_t{1}}}));
}

TEST_P(TriggerAuditEngine, UnknownConfirmationRetainsOriginalBatchAndDoesNotRunHookAgain) {
  open();
  store->unknown_next = true;
  auto result = lifecycle({audit_write()});
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(result->commit.code, CommitCode::Unknown);
  EXPECT_TRUE(result->changes.empty());
  EXPECT_TRUE(engine->has_pending_commit());
  EXPECT_FALSE(engine->query_sql("SELECT * FROM trigger_audit WHERE k='a'").ok());
  ASSERT_EQ(store->batches.size(), 1U);
  const auto original = store->batches.front();
  auto confirmed = take(engine->resolve_pending_commit());
  EXPECT_EQ(confirmed.commit.code, CommitCode::Committed);
  EXPECT_TRUE(confirmed.commit.duplicate);
  ASSERT_EQ(store->batches.size(), 2U);
  const auto& retry = store->batches.back();
  EXPECT_EQ(original.commit_id, retry.commit_id);
  EXPECT_EQ(original.expected_revision, retry.expected_revision);
  ASSERT_EQ(original.mutations.size(), retry.mutations.size());
  for (std::size_t i = 0; i < original.mutations.size(); ++i) {
    EXPECT_EQ(original.mutations[i].key, retry.mutations[i].key);
    EXPECT_EQ(original.mutations[i].value, retry.mutations[i].value);
  }
  EXPECT_EQ(query("SELECT id FROM trigger_audit").rows, (std::vector<Row>{{std::int64_t{1}}}));
  EXPECT_EQ(query("SELECT v FROM input").revision, confirmed.commit.revision);
}

TEST_P(TriggerAuditEngine, LifecycleWriteValidatesRoleSchemaAndIdentityAndSupportsAtomicRetention) {
  open();
  ASSERT_TRUE(lifecycle({audit_write()}).ok());
  const auto result = EngineRuntimeAccess::apply_lifecycle(
      *engine, {}, DispatchContext{.processing_time_us = 6},
      [&](RoundContext& round, const std::vector<RowChange>&) noexcept -> Status {
        EXPECT_EQ(EngineRuntimeAccess::write_trigger_audit(round, input_write()).code(),
                  ErrorCode::InvalidArgument);
        auto invalid = audit_write();
        invalid.identity = {std::string("a")};
        EXPECT_EQ(EngineRuntimeAccess::write_trigger_audit(round, invalid).code(),
                  ErrorCode::InvalidArgument);
        invalid.row.reset();
        EXPECT_EQ(EngineRuntimeAccess::write_trigger_audit(round, invalid).code(),
                  ErrorCode::InvalidArgument);
        invalid = audit_write();
        invalid.identity[1] = true;
        EXPECT_EQ(EngineRuntimeAccess::write_trigger_audit(round, invalid).code(),
                  ErrorCode::InvalidArgument);
        invalid = audit_write();
        (*invalid.row)[1] = std::int64_t{2};
        EXPECT_EQ(EngineRuntimeAccess::write_trigger_audit(round, invalid).code(),
                  ErrorCode::InvalidArgument);
        invalid = audit_write();
        (*invalid.row)[2] = true;
        EXPECT_EQ(EngineRuntimeAccess::write_trigger_audit(round, invalid).code(),
                  ErrorCode::InvalidArgument);
        auto expired = audit_write();
        expired.row.reset();
        EF_RETURN_IF_ERROR(EngineRuntimeAccess::write_trigger_audit(round, expired));
        EF_RETURN_IF_ERROR(EngineRuntimeAccess::write_trigger_audit(round, audit_write(2)));
        return {};
      });
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(query("SELECT id FROM trigger_audit").rows, (std::vector<Row>{{std::int64_t{2}}}));
  EXPECT_EQ(query("SELECT n FROM audit_count").rows, (std::vector<Row>{{std::int64_t{1}}}));
}

TEST_P(TriggerAuditEngine, CandidatePrepareAndObserverNeverReceiveTheLifecycleAuditCapability) {
  ObservationRuleDefinition rule;
  rule.name = "audit_observer";
  rule.recipe_id = rule.name;
  rule.implementation_identity = "builtin.audit_observer";
  rule.program_fingerprint = R"({"guard":"true"})";
  rule.scope_fields = {Field{.id = 1, .name = "k", .type = DataType::String, .nullable = false}};
  rule.inputs = {ObservationInputDefinition{.id = "changed",
                                            .kind = ObservationInputKind::RelationChange,
                                            .event_kind = "changed",
                                            .relation = audit_count,
                                            .source_kind = {},
                                            .plan = {},
                                            .policy_id = {},
                                            .completion_targets = {},
                                            .change_mask = 7,
                                            .source_fields = {},
                                            .max_event_bytes = 64 * 1024}};
  take(catalog->define_observation(rule));
  open();
  std::size_t observer_calls = 0;
  ASSERT_TRUE(
      EngineRuntimeAccess::install_observer(
          *engine, 42000,
          [&](RoundContext& round, const ObservationRuleDescriptor&, const RuntimeCause&,
              const std::vector<RowChange>&) noexcept -> StatusOr<std::vector<TableChange>> {
            ++observer_calls;
            EXPECT_EQ(EngineRuntimeAccess::write_trigger_audit(round, audit_write(2)).code(),
                      ErrorCode::FailedPrecondition);
            return std::vector<TableChange>{};
          })
          .ok());
  auto result = EngineRuntimeAccess::apply_lifecycle(
      *engine, {input_write()}, {},
      [&](RoundContext& round, const std::vector<RowChange>&) noexcept -> Status {
        return EngineRuntimeAccess::write_trigger_audit(round, audit_write());
      },
      [&](RoundContext& round) noexcept -> StatusOr<std::vector<TableChange>> {
        EXPECT_EQ(EngineRuntimeAccess::write_trigger_audit(round, audit_write(3)).code(),
                  ErrorCode::FailedPrecondition);
        return std::vector<TableChange>{};
      });
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(observer_calls, 1U);
  EXPECT_EQ(query("SELECT id FROM trigger_audit").rows, (std::vector<Row>{{std::int64_t{1}}}));
}

TEST_P(TriggerAuditEngine, AuditWritesShareTheSourceRowMutationAndByteBudgets) {
  for (int budget = 0; budget < 3; ++budget) {
    SCOPED_TRACE(budget);
    config.max_rows = budget == 0 ? 1 : 10000;
    config.max_mutations = budget == 1 ? 2 : 10000;
    open();
    const auto revision = engine->revision();
    auto write = audit_write();
    if (budget == 2)
      write = audit_write(1, "a", std::string(engine->config().max_bytes + 1, 'x'));
    auto result = lifecycle({write});
    EXPECT_EQ(result.status().code(), ErrorCode::ResourceExhausted);
    EXPECT_FALSE(engine->failed());
    EXPECT_EQ(engine->revision(), revision);
    EXPECT_TRUE(store->batches.empty());
    EXPECT_TRUE(query("SELECT * FROM trigger_audit").rows.empty());
    EXPECT_TRUE(query("SELECT * FROM input").rows.empty());
    engine.reset();
  }
}

TEST_P(TriggerAuditEngine, AuditPropagationRejectsFeedbackIntoTheSourceGraph) {
  take(catalog->define_sql("CREATE MATERIALIZED VIEW feedback AS SELECT audit_count.n,input.v "
                           "FROM audit_count JOIN input ON audit_count.k=input.k"));
  open();
  const auto revision = engine->revision();
  auto result = lifecycle({audit_write()});
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), ErrorCode::InvalidArgument);
  EXPECT_NE(result.status().message().find("graph intersects"), std::string::npos);
  EXPECT_TRUE(engine->failed());
  EXPECT_TRUE(store->batches.empty());
  EXPECT_EQ(take(store->status(0)).revision, revision);
}

TEST(TriggerAuditShard, LifecycleRejectsForeignShardForBothInsertAndDelete) {
  auto catalog = take(Catalog::create());
  const auto audit = take(catalog->define_sql(
      "CREATE TABLE trigger_audit(k STRING,id INT64,PRIMARY KEY(k,id)) PARTITION BY(k)",
      DefinitionOptions{.partition_key = {},
                        .base_write_role = BaseWriteRole::TriggerAudit,
                        .identity_domain = {}}));
  StoreConfig storage;
  storage.shard_count = 2;
  auto store = std::shared_ptr<IStateStore>(take(MemoryStateStore::open(storage)));
  EngineConfig config;
  config.shard_count = 2;
  auto engine = take(Engine::open(store, take(catalog->snapshot()), config));
  std::string foreign;
  for (int i = 0; i < 100; ++i) {
    auto candidate = std::to_string(i);
    if (take(shard_for(Row{candidate}, 2)) == 1) {
      foreign = std::move(candidate);
      break;
    }
  }
  ASSERT_FALSE(foreign.empty());
  auto result = EngineRuntimeAccess::apply_lifecycle(
      *engine, {}, {}, [&](RoundContext& round, const std::vector<RowChange>&) noexcept -> Status {
        TableChange write{.relation = audit,
                          .identity = {foreign, std::int64_t{1}},
                          .row = Row{foreign, std::int64_t{1}}};
        EXPECT_EQ(EngineRuntimeAccess::write_trigger_audit(round, write).code(),
                  ErrorCode::InvalidArgument);
        write.row.reset();
        EXPECT_EQ(EngineRuntimeAccess::write_trigger_audit(round, write).code(),
                  ErrorCode::InvalidArgument);
        return {};
      });
  ASSERT_TRUE(result.ok()) << result.status().message();
}

INSTANTIATE_TEST_SUITE_P(MemoryRocksAndHttp, TriggerAuditEngine, testing::Values(0, 1, 2, 3));
} // namespace
} // namespace eventflow::test
