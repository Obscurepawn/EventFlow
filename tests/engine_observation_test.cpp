#include "../src/runtime/engine_internal.hpp"
#include "eventflow/key_codec.hpp"
#include "remote_harness.hpp"
#include "status_test_support.hpp"
#include <filesystem>
#include <gtest/gtest.h>

namespace eventflow::test {
namespace {
ObservationRuleDefinition observe_relation(const CatalogSnapshot& catalog, std::string name,
                                           RelationId input,
                                           std::optional<RelationId> sink = {}) noexcept {
  ObservationRuleDefinition rule;
  rule.name = std::move(name);
  rule.recipe_id = rule.name;
  rule.implementation_identity = "builtin." + rule.name;
  rule.program_fingerprint = R"({"guard":"true"})";
  rule.scope_fields = {{1, "k", DataType::String, false}};
  ObservationInputDefinition binding;
  binding.id = "change";
  binding.kind = ObservationInputKind::RelationChange;
  binding.event_kind = "changed";
  binding.relation = input;
  rule.inputs = {binding};
  auto relation = take(catalog.find(input));
  rule.reads.push_back({"current",
                        input,
                        {relation->storage_schema.fields.begin(),
                         relation->storage_schema.fields.begin() + relation->visible_count}});
  rule.sink_relation = sink;
  rule.max_sink_row_bytes = sink ? 4096 : 0;
  return rule;
}

TEST(EngineObservation, ReadsStableMvAndPublishesSinkAndPrivateStateInOneRevision) {
  auto catalog = take(Catalog::create());
  const auto input =
      take(catalog->define_sql("CREATE TABLE t(k STRING,v INT64,PRIMARY KEY(k)) PARTITION BY(k)"));
  const auto view =
      take(catalog->define_sql("CREATE MATERIALIZED VIEW doubled AS SELECT v*2 AS x FROM t"));
  const auto sink = take(
      catalog->define_sql("CREATE TABLE sink(k STRING,v INT64,PRIMARY KEY(k)) PARTITION BY(k)"));
  take(catalog->define_observation(
      observe_relation(take(catalog->snapshot()), "watch", view, sink)));
  StoreConfig storage;
  storage.shard_count = 1;
  auto store = std::shared_ptr<IStateStore>(take(MemoryStateStore::open(storage)));
  EngineConfig config;
  config.shard_count = 1;
  auto engine = take(Engine::open(store, take(catalog->snapshot()), config));
  const auto state_key = take(row_key(0, 'S', 42000, {std::string("state")}));
  std::size_t calls = 0;
  ASSERT_TRUE(EngineRuntimeAccess::install_observer(
                  *engine, 42000,
                  [&](RoundContext& round, const ObservationRuleDescriptor&, const RuntimeCause&,
                      const std::vector<RowChange>& changes) noexcept
                      -> StatusOr<std::vector<TableChange>> {
                    ++calls;
                    EXPECT_EQ(changes.size(), 2U);
                    EF_ASSIGN_OR_RETURN(auto row, round.read_relation(view, {std::string("a")}));
                    if (!row)
                      return Status(ErrorCode::Internal, "missing stabilized view");
                    EXPECT_EQ(std::get<std::int64_t>((*row)[0]), 14);
                    EF_RETURN_IF_ERROR(EngineRuntimeAccess::stage_observer_state(
                        round, {state_key, std::string("stepped")}));
                    return std::vector<TableChange>{
                        {sink, {std::string("a")}, Row{std::string("a"), (*row)[0]}}};
                  })
                  .ok());
  const auto revision = engine->revision();
  auto result =
      take(engine->apply({input, {std::string("a")}, Row{std::string("a"), std::int64_t{7}}}, {3}));
  EXPECT_EQ(result.commit.revision, revision + 1);
  EXPECT_EQ(calls, 1U);
  auto selected = take(engine->query_sql("SELECT v FROM sink WHERE k='a'"));
  ASSERT_EQ(selected.rows.size(), 1U);
  EXPECT_EQ(selected.rows[0], Row{std::int64_t{14}});
  const auto state = take(store->get_many(0, {state_key}));
  ASSERT_TRUE(state.values[0]);
  EXPECT_EQ(*state.values[0], "stepped");
  EXPECT_EQ(state.revision, selected.revision);
}

class ObserverStore final : public IStateStore {
public:
  explicit ObserverStore(std::shared_ptr<IStateStore> value) noexcept : backend(std::move(value)) {}

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
    ++commits;
    EF_ASSIGN_OR_RETURN(auto result, backend->commit(batch));
    if (unknown_next && result.code == CommitCode::Committed) {
      unknown_next = false;
      return CommitResult{CommitCode::Unknown, batch.expected_revision, false, "lost reply"};
    }
    return result;
  }

  std::shared_ptr<IStateStore> backend;
  bool unknown_next = false;
  std::size_t commits = 0;
};

class EngineObservationContract : public testing::TestWithParam<int> {
protected:
  void SetUp() noexcept override {
    catalog = take(Catalog::create());
    input = take(
        catalog->define_sql("CREATE TABLE t(k STRING,v INT64,PRIMARY KEY(k)) PARTITION BY(k)"));
    view = take(catalog->define_sql("CREATE MATERIALIZED VIEW doubled AS SELECT v*2 AS x FROM t"));
    sink = take(
        catalog->define_sql("CREATE TABLE sink(k STRING,v INT64,PRIMARY KEY(k)) PARTITION BY(k)"));
    sink_view = take(catalog->define_sql(
        "CREATE MATERIALIZED VIEW sink_total AS SELECT SUM(v) AS x FROM sink GROUP BY k"));
    StoreConfig config;
    config.shard_count = 1;
    std::unique_ptr<IStateStore> backend;
    if (GetParam() % 2 == 0) {
      backend = take(MemoryStateStore::open(config));
    } else {
      char pattern[] = "/tmp/eventflow-observer-XXXXXX";
      const auto* made = ::mkdtemp(pattern);
      ASSERT_NE(made, nullptr);
      path = made;
      backend = take(RocksStateStore::open(path + "/db", config));
    }
    if (GetParam() >= 2)
      backend = take(RemoteHarnessStore::open(std::move(backend)));
    store = std::make_shared<ObserverStore>(std::move(backend));
    state_key = take(row_key(0, 'S', 42000, {std::string("state")}));
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
    EngineConfig config;
    config.shard_count = 1;
    engine = take(Engine::open(store, take(catalog->snapshot()), config));
  }

  void install(RuntimeObserver callback) noexcept {
    const auto status = EngineRuntimeAccess::install_observer(*engine, 42000, std::move(callback));
    ASSERT_TRUE(status.ok()) << status.message();
  }

  TableChange change(std::int64_t value) noexcept {
    return {input, {std::string("a")}, Row{std::string("a"), value}};
  }

  std::string path, state_key;
  std::unique_ptr<Catalog> catalog;
  std::unique_ptr<Engine> engine;
  std::shared_ptr<ObserverStore> store;
  RelationId input = 0, view = 0, sink = 0, sink_view = 0;
};

TEST_P(EngineObservationContract, GraphOrderWaitsForFinalSinkAggregateEvenWhenDeclaredFirst) {
  take(catalog->define_observation(
      observe_relation(take(catalog->snapshot()), "consumer", sink_view)));
  take(catalog->define_observation(
      observe_relation(take(catalog->snapshot()), "producer", view, sink)));
  open();
  std::vector<std::string> order;
  std::int64_t observed = 0;
  install([&](RoundContext& round, const ObservationRuleDescriptor& rule, const RuntimeCause&,
              const auto&) noexcept -> StatusOr<std::vector<TableChange>> {
    order.push_back(rule.definition.name);
    EF_ASSIGN_OR_RETURN(auto row,
                        round.read_relation(rule.definition.reads[0].relation, {std::string("a")}));
    if (!row)
      return Status(ErrorCode::Internal, "expected staged singleton");
    if (rule.definition.name == "consumer") {
      observed = std::get<std::int64_t>((*row)[0]);
      return std::vector<TableChange>{};
    }
    return std::vector<TableChange>{{sink, {std::string("a")}, Row{std::string("a"), (*row)[0]}}};
  });
  take(engine->apply(change(1)));
  order.clear();
  auto result = take(engine->apply_many(
      {change(7), {sink, {std::string("a")}, Row{std::string("a"), std::int64_t{200}}}}));
  EXPECT_EQ(order, (std::vector<std::string>{"producer", "consumer"}));
  EXPECT_EQ(observed, 14);
  QueryContext scope;
  scope.scope = Row{std::string("a")};
  auto rows = take(engine->query_sql("SELECT x FROM sink_total", scope));
  ASSERT_EQ(rows.rows.size(), 1U);
  EXPECT_EQ(rows.rows[0], Row{std::int64_t{14}});
  EXPECT_EQ(rows.revision, result.commit.revision);
}

TEST_P(EngineObservationContract, MissingExecutorBlocksAdmissionWithoutFailingOrWriting) {
  take(catalog->define_observation(observe_relation(take(catalog->snapshot()), "watch", view)));
  open();
  const auto revision = engine->revision();
  const auto commits = store->commits;
  const auto result = engine->apply(change(7));
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), ErrorCode::FailedPrecondition);
  EXPECT_EQ(store->commits, commits);
  EXPECT_EQ(engine->revision(), revision);
  EXPECT_FALSE(engine->failed());
}

TEST_P(EngineObservationContract, UnknownConfirmationNeverRereadsOrReexecutesTheObserver) {
  take(catalog->define_observation(
      observe_relation(take(catalog->snapshot()), "watch", view, sink)));
  open();
  std::size_t calls = 0;
  install([&](RoundContext& round, const auto&, const auto&,
              const auto&) noexcept -> StatusOr<std::vector<TableChange>> {
    ++calls;
    EF_ASSIGN_OR_RETURN(auto row, round.read_relation(view, {std::string("a")}));
    EF_RETURN_IF_ERROR(EngineRuntimeAccess::stage_observer_state(round, {state_key, "one"}));
    return std::vector<TableChange>{{sink, {std::string("a")}, Row{std::string("a"), (*row)[0]}}};
  });
  store->unknown_next = true;
  auto result = take(engine->apply(change(7)));
  EXPECT_EQ(result.commit.code, CommitCode::Unknown);
  EXPECT_TRUE(result.changes.empty());
  EXPECT_TRUE(engine->has_pending_commit());
  EXPECT_EQ(engine->query_sql("SELECT v FROM sink WHERE k='a'").status().code(),
            ErrorCode::FailedPrecondition);
  EXPECT_EQ(take(engine->resolve_pending_commit()).commit.code, CommitCode::Committed);
  EXPECT_EQ(calls, 1U);
  EXPECT_EQ(take(store->get_many(0, {state_key})).values[0], std::optional<std::string>{"one"});
}

TEST_P(EngineObservationContract, PrivateStateCannotBeWrittenThroughGeneralOrRequestHelpers) {
  take(catalog->define_observation(observe_relation(take(catalog->snapshot()), "watch", view)));
  open();
  install([&](RoundContext& round, const auto&, const auto&,
              const auto&) noexcept -> StatusOr<std::vector<TableChange>> {
    EXPECT_EQ(round.stage({state_key, "wrong"}).code(), ErrorCode::InvalidArgument);
    EXPECT_FALSE(EngineRuntimeAccess::stage_request_state(round, {state_key, "wrong"}).ok());
    const auto foreign = take(row_key(0, 'S', 42001, {std::string("state")}));
    EXPECT_FALSE(EngineRuntimeAccess::stage_observer_state(round, {foreign, "wrong"}).ok());
    EXPECT_FALSE(
        round.emit({sink, {std::string("a")}, {}, Row{std::string("a"), std::int64_t{7}}}).ok());
    EF_RETURN_IF_ERROR(EngineRuntimeAccess::stage_observer_state(round, {state_key, "right"}));
    return std::vector<TableChange>{};
  });
  take(engine->apply(change(7)));
  EXPECT_EQ(take(store->get_many(0, {state_key})).values[0], std::optional<std::string>{"right"});
  EXPECT_FALSE(EngineRuntimeAccess::attach_request_owner(*engine, 42000).ok());
  EXPECT_FALSE(
      engine->install_sql("CREATE TABLE late(k STRING,PRIMARY KEY(k)) PARTITION BY(k)").ok());
}

TEST_P(EngineObservationContract, SinkOutsideDeclaredRelationOrEventScopeRejectsWholeRound) {
  take(catalog->define_observation(
      observe_relation(take(catalog->snapshot()), "watch", view, sink)));
  open();
  install([&](RoundContext& round, const auto&, const auto&,
              const auto&) noexcept -> StatusOr<std::vector<TableChange>> {
    EF_RETURN_IF_ERROR(
        EngineRuntimeAccess::stage_observer_state(round, {state_key, "not visible"}));
    return std::vector<TableChange>{
        {sink, {std::string("b")}, Row{std::string("b"), std::int64_t{7}}}};
  });
  const auto revision = engine->revision();
  const auto commits = store->commits;
  auto result = engine->apply(change(7));
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), ErrorCode::InvalidArgument);
  EXPECT_EQ(store->commits, commits);
  EXPECT_EQ(take(store->status(0)).revision, revision);
  EXPECT_FALSE(take(store->get_many(0, {state_key})).values[0]);
}

TEST_P(EngineObservationContract, SourceAndInternalClockCausesNeedNoRelationDelta) {
  auto rule = observe_relation(take(catalog->snapshot()), "watch", view);
  rule.inputs[0].kind = ObservationInputKind::Source;
  rule.inputs[0].relation.reset();
  rule.inputs[0].source_kind = "tick";
  rule.inputs[0].source_fields = {{1, "value", DataType::Int64, false}};
  const auto id = take(catalog->define_observation(rule));
  open();
  std::size_t calls = 0;
  install([&](RoundContext& round, const auto&, const RuntimeCause& cause,
              const auto& changes) noexcept -> StatusOr<std::vector<TableChange>> {
    EXPECT_TRUE(changes.empty());
    EXPECT_EQ(cause.inputs.size(), 1U);
    ++calls;
    EF_RETURN_IF_ERROR(EngineRuntimeAccess::stage_observer_state(
        round, {state_key, std::to_string(round.processing_time_us())}));
    return std::vector<TableChange>{};
  });
  RuntimeCause cause;
  RuntimeObservationInput event;
  event.kind = ObservationInputKind::Source;
  event.origin = "tick";
  event.scope = {std::string("a")};
  event.values = {std::int64_t{7}};
  cause.inputs.push_back(event);
  take(EngineRuntimeAccess::apply_lifecycle(*engine, {}, {1}, {}, {}, cause));
  cause.inputs[0].kind = ObservationInputKind::Clock;
  cause.inputs[0].advance_rule = id;
  take(EngineRuntimeAccess::apply_lifecycle(*engine, {}, {5}, {}, {}, cause));
  EXPECT_EQ(calls, 2U);
  EXPECT_EQ(take(store->get_many(0, {state_key})).values[0], std::optional<std::string>{"5"});
}

TEST_P(EngineObservationContract,
       RuntimeFinalStatusIsStableBeforeObserverAndSourceResolverStaysValid) {
  DefinitionOptions options;
  options.base_write_role = BaseWriteRole::RuntimeStatus;
  const auto status = take(catalog->define_sql(
      "CREATE TABLE status(k STRING,phase STRING,PRIMARY KEY(k)) PARTITION BY(k)", options));
  const auto status_view =
      take(catalog->define_sql("CREATE MATERIALIZED VIEW status_view AS SELECT phase FROM status"));
  take(catalog->define_observation(
      observe_relation(take(catalog->snapshot()), "watch", status_view)));
  open();
  std::size_t calls = 0;
  install([&](RoundContext& round, const auto&, const auto&,
              const auto& changes) noexcept -> StatusOr<std::vector<TableChange>> {
    ++calls;
    EF_ASSIGN_OR_RETURN(auto row, round.read_relation(status_view, {std::string("a")}));
    EXPECT_EQ((*row)[0], Value{std::string("done")});
    for (const auto& change : changes)
      if (change.relation == status_view)
        EXPECT_EQ((*change.new_row)[0], Value{std::string("done")});
    EF_RETURN_IF_ERROR(EngineRuntimeAccess::stage_observer_state(round, {state_key, "done"}));
    return std::vector<TableChange>{};
  });
  store->unknown_next = true;
  auto result = take(EngineRuntimeAccess::apply(
      *engine, {1},
      [&](RoundContext&) noexcept -> StatusOr<std::vector<TableChange>> {
        return std::vector<TableChange>{
            {status, {std::string("a")}, Row{std::string("a"), std::string("pending")}}};
      },
      [&](RoundContext&, const auto&) noexcept -> StatusOr<std::vector<TableChange>> {
        return std::vector<TableChange>{
            {status, {std::string("a")}, Row{std::string("a"), std::string("done")}}};
      }));
  EXPECT_EQ(result.commit.code, CommitCode::Unknown);
  EXPECT_EQ(take(engine->resolve_pending_commit()).commit.code, CommitCode::Committed);
  EXPECT_EQ(calls, 1U);
}

INSTANTIATE_TEST_SUITE_P(FourStores, EngineObservationContract, testing::Values(0, 1, 2, 3));
} // namespace
} // namespace eventflow::test
