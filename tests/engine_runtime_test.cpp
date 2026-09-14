#include "engine_internal.hpp"
#include "eventflow/key_codec.hpp"
#include "remote_harness.hpp"
#include "status_test_support.hpp"
#include <filesystem>
#include <gtest/gtest.h>

namespace eventflow::test {
namespace {
TEST(EngineRuntime, ControlledStatusWritesHaveOneAtomicRevision) {
  auto catalog = take(Catalog::create());
  DefinitionOptions options;
  options.base_write_role = BaseWriteRole::RuntimeStatus;
  const auto status_relation = take(catalog->define_sql(
      "CREATE TABLE status(k STRING, v INT64, PRIMARY KEY(k)) PARTITION BY(k)", options));
  StoreConfig store_config;
  store_config.shard_count = 1;
  auto store = std::shared_ptr<IStateStore>(take(MemoryStateStore::open(store_config)));
  EngineConfig config;
  config.shard_count = 1;
  auto engine = take(EngineRuntimeAccess::open(store, take(catalog->snapshot()), config,
                                               take(FunctionRegistry::create())));
  const auto before = engine->revision();
  auto result = EngineRuntimeAccess::apply(
      *engine, {10}, [&](RoundContext& context) noexcept -> StatusOr<std::vector<TableChange>> {
        EXPECT_EQ(context.revision(), before);
        return std::vector<TableChange>{
            {status_relation, Row{std::string("a")}, Row{std::string("a"), std::int64_t{4}}}};
      });
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(result->commit.code, CommitCode::Committed);
  EXPECT_EQ(result->commit.revision, before + 1);
  auto rows = take(engine->query_sql("SELECT v FROM status WHERE k='a'"));
  ASSERT_EQ(rows.rows.size(), 1U);
  EXPECT_EQ(rows.rows[0], Row{std::int64_t{4}});
}

class BridgeStore final : public IStateStore {
public:
  explicit BridgeStore(std::shared_ptr<IStateStore> delegate) noexcept
      : delegate(std::move(delegate)) {}

  StatusOr<ReadResult> get_many(std::uint32_t shard,
                                const std::vector<std::string>& keys) noexcept override {
    return delegate->get_many(shard, keys);
  }

  StatusOr<ScanResult> scan(std::uint32_t shard, const std::string& prefix,
                            const std::optional<std::string>& after,
                            std::size_t limit) noexcept override {
    EF_ASSIGN_OR_RETURN(auto result, delegate->scan(shard, prefix, after, limit));
    if (corrupt_scan_revision) {
      corrupt_scan_revision = false;
      ++result.revision;
    }
    if (repeat_scan_key && result.rows.size() > 1) {
      repeat_scan_key = false;
      result.rows[1].first = result.rows[0].first;
    }
    return result;
  }

  StatusOr<CommitResult> commit(const CommitBatch& batch) noexcept override {
    batches.push_back(batch);
    EF_ASSIGN_OR_RETURN(auto result, delegate->commit(batch));
    if (unknown_next) {
      unknown_next = false;
      return CommitResult{CommitCode::Unknown, batch.expected_revision, false, "lost response"};
    }
    return result;
  }

  StatusOr<StoreLimits> limits() noexcept override {
    return delegate->limits();
  }

  StatusOr<ShardStatus> status(std::uint32_t shard) noexcept override {
    return delegate->status(shard);
  }

  std::shared_ptr<IStateStore> delegate;
  bool unknown_next = false;
  bool corrupt_scan_revision = false;
  bool repeat_scan_key = false;
  std::vector<CommitBatch> batches;
};

class EngineRuntimeContract : public ::testing::TestWithParam<int> {
protected:
  std::string path;
  std::unique_ptr<Catalog> catalog;
  std::shared_ptr<BridgeStore> store;
  std::shared_ptr<FunctionRegistry> registry;
  std::unique_ptr<Engine> engine;
  EngineConfig config;
  RelationId capture = 0, projected = 0, function = 0, function_child = 0;
  FunctionViewDefinition function_definition;

  void SetUp() noexcept override {
    char pattern[] = "/tmp/eventflow-engine-runtime-XXXXXX";
    auto* created = ::mkdtemp(pattern);
    ASSERT_NE(created, nullptr);
    path = created;
    StoreConfig storage;
    storage.shard_count = 1;
    std::unique_ptr<IStateStore> backend;
    if (GetParam() % 2 == 0)
      backend = take(MemoryStateStore::open(storage));
    else
      backend = take(RocksStateStore::open(path + "/db", storage));
    if (GetParam() >= 2)
      backend = take(RemoteHarnessStore::open(std::move(backend)));
    store = std::make_shared<BridgeStore>(std::shared_ptr<IStateStore>(std::move(backend)));
    catalog = take(Catalog::create());
    registry = take(FunctionRegistry::create());
    config.shard_count = 1;
  }

  void TearDown() noexcept override {
    engine.reset();
    store.reset();
    std::error_code error;
    std::filesystem::remove_all(path, error);
  }

  RelationId status_table(const std::string& name) noexcept {
    DefinitionOptions options;
    options.base_write_role = BaseWriteRole::RuntimeStatus;
    return take(catalog->define_sql(
        "CREATE TABLE " + name + "(k STRING,v INT64,PRIMARY KEY(k)) PARTITION BY(k)", options));
  }

  TableChange write(RelationId id, std::int64_t value) noexcept {
    return {id, Row{std::string("a")}, Row{std::string("a"), value}};
  }

  void open() noexcept {
    engine = take(EngineRuntimeAccess::open(store, take(catalog->snapshot()), config, registry));
  }

  DispatchResult apply(const std::vector<TableChange>& rows, std::int64_t at = 1) noexcept {
    return take(EngineRuntimeAccess::apply(
        *engine, {at}, [&](RoundContext&) noexcept -> StatusOr<std::vector<TableChange>> {
          return std::vector<TableChange>{rows};
        }));
  }

  std::int64_t value(const std::string& table, const std::string& field = "v") noexcept {
    auto rows = take(engine->query_sql("SELECT " + field + " FROM " + table + " WHERE k='a'"));
    EXPECT_EQ(rows.rows.size(), 1U);
    return rows.rows.empty() ? -1 : std::get<std::int64_t>(rows.rows[0][0]);
  }

  void define_function_graph() noexcept {
    DefinitionOptions options;
    options.base_write_role = BaseWriteRole::RequestCapture;
    options.identity_domain = "requests";
    capture = take(catalog->define_sql("CREATE TABLE capture(k STRING,request_id STRING,x "
                                       "INT64,PRIMARY KEY(k,request_id)) PARTITION BY(k)",
                                       options));
    projected = take(catalog->define_sql(
        "CREATE MATERIALIZED VIEW projected AS SELECT k,request_id,x+1 AS x FROM capture"));
    const auto snapshot = take(catalog->snapshot());
    const auto* input = take(snapshot.find(projected));
    function_definition = {
        "f",
        "builtin.f",
        1,
        "builtin.f.v1",
        {{"input", projected, true,
          std::vector<Field>(input->storage_schema.fields.begin(),
                             input->storage_schema.fields.begin() + input->visible_count)}},
        {{1, "score", DataType::Int64, false}}};
    function = take(catalog->define_function(function_definition));
    function_child = take(catalog->define_sql(
        "CREATE MATERIALIZED VIEW function_child AS SELECT k,request_id,score*2 AS v FROM f"));
    ASSERT_TRUE(registry->add(builtin()).ok());
  }

  BuiltinFunction builtin() noexcept {
    return {{function_definition.function_id,
             function_definition.function_version,
             function_definition.implementation_identity,
             {{"input", true, function_definition.inputs[0].fields}},
             function_definition.output_fields,
             true},
            [](const FunctionInputs&) noexcept -> StatusOr<Row> { return Row{std::int64_t{7}}; }};
  }

  TableChange capture_row(const std::string& id = "r1", std::int64_t x = 3) noexcept {
    return {capture, Row{std::string("a"), id}, Row{std::string("a"), id, x}};
  }

  TableChange function_row(const std::string& id = "r1", std::int64_t x = 7) noexcept {
    const auto snapshot = take(catalog->snapshot());
    const auto* relation = take(snapshot.find(function));
    const Row identity{std::string("a"), id};
    auto stored = take(make_stored_row(*relation, Row{std::string("a"), id, x}, identity));
    return {function, identity, std::move(stored)};
  }
};

TEST_P(EngineRuntimeContract, JoinReturnsToOriginalImageAndPropagatesToItsDescendant) {
  const auto a = status_table("a");
  const auto b = status_table("b");
  const auto j = take(catalog->define_sql(
      "CREATE MATERIALIZED VIEW j AS SELECT a.k,a.v*b.v AS v FROM a JOIN b ON a.k=b.k"));
  const auto d = take(catalog->define_sql("CREATE MATERIALIZED VIEW d AS SELECT k,v FROM j"));
  open();
  apply({write(a, 1), write(b, 4)});
  auto result = take(EngineRuntimeAccess::apply(
      *engine, {2},
      [&](RoundContext&) noexcept -> StatusOr<std::vector<TableChange>> {
        return std::vector<TableChange>{write(a, 2)};
      },
      [&](RoundContext& context,
          const std::vector<RowChange>&) noexcept -> StatusOr<std::vector<TableChange>> {
        auto intermediate = take(context.read_relation(d, Row{std::string("a")}));
        EXPECT_EQ(std::get<std::int64_t>((*intermediate)[1]), 8);
        return std::vector<TableChange>{write(b, 2)};
      }));
  EXPECT_EQ(value("j"), 4);
  EXPECT_EQ(value("d"), 4);
  for (const auto& change : result.changes)
    EXPECT_TRUE(change.relation != j && change.relation != d);
}

TEST_P(EngineRuntimeContract, RevertedStatusAndFilteredAbsentImageResetAggregateDescendants) {
  const auto a = status_table("a");
  take(catalog->define_sql("CREATE MATERIALIZED VIEW positive AS SELECT k,v FROM a WHERE v>0"));
  take(catalog->define_sql(
      "CREATE MATERIALIZED VIEW summed AS SELECT k,SUM(v) AS v FROM positive GROUP BY k"));
  open();
  apply({write(a, -1)});
  auto result = take(EngineRuntimeAccess::apply(
      *engine, {2},
      [&](RoundContext&) noexcept -> StatusOr<std::vector<TableChange>> {
        return std::vector<TableChange>{write(a, 2)};
      },
      [&](RoundContext&,
          const std::vector<RowChange>&) noexcept -> StatusOr<std::vector<TableChange>> {
        return std::vector<TableChange>{write(a, -1)};
      }));
  EXPECT_TRUE(result.changes.empty());
  EXPECT_TRUE(take(engine->query_sql("SELECT v FROM positive WHERE k='a'")).rows.empty());
  EXPECT_TRUE(take(engine->query_sql("SELECT v FROM summed WHERE k='a'")).rows.empty());
}

TEST_P(EngineRuntimeContract, InsertThenDeleteHasNoFinalJoinOrAggregateRows) {
  const auto a = status_table("a");
  const auto b = status_table("b");
  take(catalog->define_sql(
      "CREATE MATERIALIZED VIEW j AS SELECT a.k,a.v*b.v AS v FROM a JOIN b ON a.k=b.k"));
  take(catalog->define_sql("CREATE MATERIALIZED VIEW g AS SELECT k,SUM(v) AS v FROM j GROUP BY k"));
  open();
  apply({write(a, 1)});
  auto result = take(EngineRuntimeAccess::apply(
      *engine, {2},
      [&](RoundContext&) noexcept -> StatusOr<std::vector<TableChange>> {
        return std::vector<TableChange>{write(b, 2)};
      },
      [&](RoundContext&,
          const std::vector<RowChange>&) noexcept -> StatusOr<std::vector<TableChange>> {
        return std::vector<TableChange>{{b, Row{std::string("a")}, std::nullopt}};
      }));
  EXPECT_TRUE(result.changes.empty());
  EXPECT_TRUE(take(engine->query_sql("SELECT v FROM j WHERE k='a'")).rows.empty());
  EXPECT_TRUE(take(engine->query_sql("SELECT v FROM g WHERE k='a'")).rows.empty());
}

TEST_P(EngineRuntimeContract,
       AggregateSecondPhaseRecomputesFromOriginalBeforeWithoutDoubleCounting) {
  const auto a = status_table("a");
  const auto b = status_table("b");
  take(catalog->define_sql(
      "CREATE MATERIALIZED VIEW j AS SELECT a.k,a.v*b.v AS v FROM a JOIN b ON a.k=b.k"));
  take(catalog->define_sql("CREATE MATERIALIZED VIEW g AS SELECT k,SUM(v) AS v FROM j GROUP BY k"));
  open();
  apply({write(a, 2), write(b, 1)});
  take(EngineRuntimeAccess::apply(
      *engine, {2},
      [&](RoundContext&) noexcept -> StatusOr<std::vector<TableChange>> {
        return std::vector<TableChange>{write(a, 4)};
      },
      [&](RoundContext&, const std::vector<RowChange>&) noexcept
          -> StatusOr<std::vector<TableChange>> { return std::vector<TableChange>{write(b, 2)}; }));
  EXPECT_EQ(value("j"), 8);
  EXPECT_EQ(value("g"), 8);
}

TEST_P(EngineRuntimeContract, FunctionCatalogNeedsPrivateOpenAndExactRegistrySignatures) {
  define_function_graph();
  const auto snapshot = take(catalog->snapshot());
  EXPECT_EQ(Engine::open(store, snapshot, config).status().code(), ErrorCode::FailedPrecondition);
  for (int mismatch = 0; mismatch < 6; ++mismatch) {
    auto alternative = take(FunctionRegistry::create());
    auto definition = builtin();
    if (mismatch == 0)
      definition.signature.implementation_identity = "different";
    if (mismatch == 1)
      definition.signature.inputs[0].alias = "different";
    if (mismatch == 2)
      definition.signature.inputs[0].required = false;
    if (mismatch == 3)
      definition.signature.inputs[0].fields[0].id = 999;
    if (mismatch == 4)
      definition.signature.output_fields[0].name = "different";
    if (mismatch == 5)
      definition.signature.output_fields[0].nullable = true;
    ASSERT_TRUE(alternative->add(std::move(definition)).ok());
    EXPECT_EQ(EngineRuntimeAccess::open(store, snapshot, config, alternative).status().code(),
              ErrorCode::Conflict);
  }
  EXPECT_EQ(EngineRuntimeAccess::open(store, snapshot, config, take(FunctionRegistry::create()))
                .status()
                .code(),
            ErrorCode::NotFound);
  EXPECT_EQ(take(store->status(0)).revision, 0U);
  open();
}

TEST_P(EngineRuntimeContract, CaptureSqlFunctionSqlUseOneCatalogAndOwnedWritesAreImmutable) {
  define_function_graph();
  const auto status = status_table("status");
  open();
  const auto before = engine->revision();
  auto captured = take(EngineRuntimeAccess::apply(
      *engine, {1},
      [&](RoundContext& context) noexcept -> StatusOr<std::vector<TableChange>> {
        EXPECT_EQ(context.revision(), before);
        EXPECT_FALSE(take(context.read_relation(capture, Row{std::string("a"), std::string("r1")},
                                                StateImage::Before)));
        return std::vector<TableChange>{capture_row()};
      },
      [&](RoundContext& context,
          const std::vector<RowChange>&) noexcept -> StatusOr<std::vector<TableChange>> {
        auto row = take(context.read_relation(projected, Row{std::string("a"), std::string("r1")}));
        EXPECT_EQ(std::get<std::int64_t>((*row)[2]), 4);
        EXPECT_FALSE(
            take(context.read_relation(function, Row{std::string("a"), std::string("r1")})));
        return std::vector<TableChange>{write(status, 1)};
      }));
  EXPECT_EQ(captured.commit.revision, before + 1);
  EXPECT_TRUE(take(engine->query_sql("SELECT score FROM f WHERE k='a'")).rows.empty());
  apply({function_row()}, 2);
  EXPECT_EQ(value("function_child"), 14);
  const auto revision = engine->revision();
  EXPECT_EQ(
      EngineRuntimeAccess::apply(*engine, {3},
                                 [&](RoundContext&) noexcept -> StatusOr<std::vector<TableChange>> {
                                   return std::vector<TableChange>{capture_row()};
                                 })
          .status()
          .code(),
      ErrorCode::Conflict);
  EXPECT_EQ(
      EngineRuntimeAccess::apply(*engine, {3},
                                 [&](RoundContext&) noexcept -> StatusOr<std::vector<TableChange>> {
                                   return std::vector<TableChange>{function_row("r1", 99)};
                                 })
          .status()
          .code(),
      ErrorCode::Conflict);
  EXPECT_FALSE(EngineRuntimeAccess::apply(
                   *engine, {3},
                   [&](RoundContext&) noexcept -> StatusOr<std::vector<TableChange>> {
                     return std::vector<TableChange>{
                         {function, Row{std::string("a"), std::string("r1")}, std::nullopt}};
                   })
                   .ok());
  EXPECT_EQ(engine->revision(), revision);
  EXPECT_FALSE(engine->failed());
  EXPECT_EQ(value("function_child"), 14);
}

TEST_P(EngineRuntimeContract, RejectsForgedFunctionVisibleIdentityAndFinalizeFrozenWrites) {
  define_function_graph();
  const auto status = status_table("status");
  open();
  auto forged = function_row();
  (*forged.row)[1] = std::string("forged-request");
  auto rejected = EngineRuntimeAccess::apply(
      *engine, {1}, [&](RoundContext&) noexcept -> StatusOr<std::vector<TableChange>> {
        return std::vector<TableChange>{forged};
      });
  EXPECT_FALSE(rejected.ok());
  EXPECT_FALSE(engine->failed());
  const auto before = engine->revision();
  auto wrong_phase = EngineRuntimeAccess::apply(
      *engine, {1},
      [&](RoundContext&) noexcept -> StatusOr<std::vector<TableChange>> {
        return std::vector<TableChange>{write(status, 1)};
      },
      [&](RoundContext&,
          const std::vector<RowChange>&) noexcept -> StatusOr<std::vector<TableChange>> {
        return std::vector<TableChange>{capture_row()};
      });
  EXPECT_EQ(wrong_phase.status().code(), ErrorCode::InvalidArgument);
  EXPECT_EQ(engine->revision(), before);
  EXPECT_TRUE(take(engine->query_sql("SELECT v FROM status WHERE k='a'")).rows.empty());
  EXPECT_FALSE(engine->failed());
}

TEST_P(EngineRuntimeContract, BeforePublishErrorsDiscardTypedAndRawStateWithoutPoisoningOwner) {
  const auto status = status_table("status");
  open();
  const auto before = engine->revision();
  const auto key = take(row_key(0, 'S', 800, Row{std::string("runtime")}));
  auto aborted = EngineRuntimeAccess::apply(
      *engine, {10},
      [&](RoundContext& context) noexcept -> StatusOr<std::vector<TableChange>> {
        EF_RETURN_IF_ERROR(context.stage({key, std::string("tentative")}));
        return std::vector<TableChange>{write(status, 4)};
      },
      [](RoundContext&,
         const std::vector<RowChange>&) noexcept -> StatusOr<std::vector<TableChange>> {
        return Status(ErrorCode::ResourceExhausted, "admission would block");
      });
  EXPECT_EQ(aborted.status().code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(engine->revision(), before);
  EXPECT_FALSE(engine->failed());
  EXPECT_FALSE(take(store->get_many(0, {key})).values[0]);
  EXPECT_TRUE(take(engine->query_sql("SELECT v FROM status WHERE k='a'")).rows.empty());
  EXPECT_EQ(apply({write(status, 5)}, 1).commit.code, CommitCode::Committed);
}

TEST_P(EngineRuntimeContract, RuntimeFramesCannotWritePublicInputsSqlViewsOrUseEmitBypass) {
  const auto source = take(
      catalog->define_sql("CREATE TABLE source(k STRING,v INT64,PRIMARY KEY(k)) PARTITION BY(k)"));
  const auto view =
      take(catalog->define_sql("CREATE MATERIALIZED VIEW v AS SELECT k,v FROM source"));
  const auto status = status_table("status");
  open();
  for (auto relation : {source, view}) {
    auto result = EngineRuntimeAccess::apply(
        *engine, {1}, [&](RoundContext&) noexcept -> StatusOr<std::vector<TableChange>> {
          return std::vector<TableChange>{write(relation, 7)};
        });
    EXPECT_EQ(result.status().code(), ErrorCode::InvalidArgument);
    EXPECT_FALSE(engine->failed());
  }
  auto bypass = EngineRuntimeAccess::apply(
      *engine, {1}, [&](RoundContext& context) noexcept -> StatusOr<std::vector<TableChange>> {
        EF_RETURN_IF_ERROR(context.emit(
            {source, Row{std::string("a")}, {}, Row{std::string("a"), std::int64_t{7}}}));
        return std::vector<TableChange>{};
      });
  EXPECT_EQ(bypass.status().code(), ErrorCode::InvalidArgument);
  EXPECT_FALSE(engine->failed());
  EXPECT_FALSE(engine->apply(write(status, 2)).ok());
  EXPECT_FALSE(engine->failed());
  EXPECT_EQ(take(engine->apply(write(source, 2))).commit.code, CommitCode::Committed);
}

TEST_P(EngineRuntimeContract, RuntimeReentryIsRejectedAndUnchangedFramesDoNotCommit) {
  const auto status = status_table("status");
  open();
  apply({write(status, 4)});
  const auto before = engine->revision();
  const auto commits = store->batches.size();
  auto unchanged = take(EngineRuntimeAccess::apply(
      *engine, {1}, [&](RoundContext&) noexcept -> StatusOr<std::vector<TableChange>> {
        EXPECT_EQ(engine->query_sql("SELECT v FROM status WHERE k='a'").status().code(),
                  ErrorCode::FailedPrecondition);
        EXPECT_EQ(EngineRuntimeAccess::apply(*engine, {1}, {}).status().code(),
                  ErrorCode::FailedPrecondition);
        return std::vector<TableChange>{write(status, 4)};
      }));
  EXPECT_TRUE(unchanged.commit.duplicate);
  EXPECT_EQ(unchanged.commit.revision, before);
  EXPECT_EQ(store->batches.size(), commits);
  EXPECT_FALSE(engine->failed());
}

TEST_P(EngineRuntimeContract, UnknownRetainsBothPhasesAndNeverRepeatsCallbacks) {
  const auto a = status_table("a");
  const auto b = status_table("b");
  take(catalog->define_sql(
      "CREATE MATERIALIZED VIEW j AS SELECT a.k,a.v*b.v AS v FROM a JOIN b ON a.k=b.k"));
  open();
  apply({write(a, 1), write(b, 4)});
  std::size_t preparations = 0, finalizations = 0;
  store->unknown_next = true;
  auto unknown = take(EngineRuntimeAccess::apply(
      *engine, {2},
      [&](RoundContext&) noexcept -> StatusOr<std::vector<TableChange>> {
        ++preparations;
        return std::vector<TableChange>{write(a, 2)};
      },
      [&](RoundContext&,
          const std::vector<RowChange>&) noexcept -> StatusOr<std::vector<TableChange>> {
        ++finalizations;
        return std::vector<TableChange>{write(b, 3)};
      }));
  EXPECT_EQ(unknown.commit.code, CommitCode::Unknown);
  EXPECT_EQ(EngineRuntimeAccess::time_us(*engine), 1);
  EXPECT_TRUE(unknown.changes.empty());
  EXPECT_FALSE(EngineRuntimeAccess::apply(*engine, {2}, {}).ok());
  auto resolved = take(engine->resolve_pending_commit());
  EXPECT_EQ(resolved.commit.code, CommitCode::Committed);
  EXPECT_EQ(value("j"), 6);
  EXPECT_EQ(EngineRuntimeAccess::time_us(*engine), 2);
  EXPECT_EQ(preparations, 1U);
  EXPECT_EQ(finalizations, 1U);
  EXPECT_EQ(store->batches[store->batches.size() - 1].commit_id,
            store->batches[store->batches.size() - 2].commit_id);
}

TEST_P(EngineRuntimeContract, ScopedScanReturnsBeforeAndStagedImagesUnderOneRevision) {
  DefinitionOptions options;
  options.base_write_role = BaseWriteRole::RuntimeStatus;
  const auto history = take(catalog->define_sql(
      "CREATE TABLE history(k STRING,id INT64,v INT64,PRIMARY KEY(k,id)) PARTITION BY(k)",
      options));
  config.page_rows = 1;
  open();
  apply({{history, Row{std::string("a"), std::int64_t{1}},
          Row{std::string("a"), std::int64_t{1}, std::int64_t{4}}},
         {history, Row{std::string("b"), std::int64_t{1}},
          Row{std::string("b"), std::int64_t{1}, std::int64_t{999}}}});
  const auto revision = engine->revision();
  auto result = take(EngineRuntimeAccess::apply(
      *engine, {2},
      [&](RoundContext& context) noexcept -> StatusOr<std::vector<TableChange>> {
        EXPECT_EQ(context.revision(), revision);
        auto rows = take(context.scan_relation(history, Row{std::string("a")}, StateImage::Before));
        EXPECT_EQ(rows.size(), 1U);
        EXPECT_EQ(std::get<std::int64_t>(rows[0][2]), 4);
        return std::vector<TableChange>{
            {history, Row{std::string("a"), std::int64_t{1}}, std::nullopt},
            {history, Row{std::string("a"), std::int64_t{2}},
             Row{std::string("a"), std::int64_t{2}, std::int64_t{8}}}};
      },
      [&](RoundContext& context,
          const std::vector<RowChange>&) noexcept -> StatusOr<std::vector<TableChange>> {
        const auto before =
            take(context.scan_relation(history, Row{std::string("a")}, StateImage::Before));
        const auto staged =
            take(context.scan_relation(history, Row{std::string("a")}, StateImage::Staged));
        EXPECT_EQ(std::get<std::int64_t>(before[0][1]), 1);
        EXPECT_EQ(std::get<std::int64_t>(staged[0][1]), 2);
        EXPECT_EQ(context.revision(), revision);
        return std::vector<TableChange>{};
      }));
  EXPECT_EQ(result.commit.revision, revision + 1);
}

TEST_P(EngineRuntimeContract, ScanBudgetsAndMalformedPaginationAbortOnlyThePrivateFrame) {
  const auto source = take(catalog->define_sql(
      "CREATE TABLE history(k STRING,id INT64,text STRING,PRIMARY KEY(k,id)) PARTITION BY(k)"));
  config.max_bytes = 8192;
  open();
  for (std::int64_t id = 0; id < 3; ++id)
    take(engine->apply(
        {source, Row{std::string("a"), id}, Row{std::string("a"), id, std::string(800, 'x')}}));
  const auto revision = engine->revision();
  auto too_many = EngineRuntimeAccess::apply(
      *engine, {}, [&](RoundContext& context) noexcept -> StatusOr<std::vector<TableChange>> {
        EF_ASSIGN_OR_RETURN(auto rows, context.scan_relation(source, Row{std::string("a")},
                                                             StateImage::Before, 2, 8192));
        (void)rows;
        return std::vector<TableChange>{};
      });
  EXPECT_EQ(too_many.status().code(), ErrorCode::ResourceExhausted);
  auto total = EngineRuntimeAccess::apply(
      *engine, {}, [&](RoundContext& context) noexcept -> StatusOr<std::vector<TableChange>> {
        for (int i = 0; i < 4; ++i) {
          EF_ASSIGN_OR_RETURN(auto rows, context.scan_relation(source, Row{std::string("a")},
                                                               StateImage::Before, 3, 8192));
          (void)rows;
        }
        return std::vector<TableChange>{};
      });
  EXPECT_EQ(total.status().code(), ErrorCode::ResourceExhausted);
  store->corrupt_scan_revision = true;
  auto stale = EngineRuntimeAccess::apply(
      *engine, {}, [&](RoundContext& context) noexcept -> StatusOr<std::vector<TableChange>> {
        EF_ASSIGN_OR_RETURN(auto rows, context.scan_relation(source, Row{std::string("a")}));
        (void)rows;
        return std::vector<TableChange>{};
      });
  EXPECT_EQ(stale.status().code(), ErrorCode::Conflict);
  store->repeat_scan_key = true;
  auto malformed = EngineRuntimeAccess::apply(
      *engine, {}, [&](RoundContext& context) noexcept -> StatusOr<std::vector<TableChange>> {
        EF_ASSIGN_OR_RETURN(auto rows, context.scan_relation(source, Row{std::string("a")}));
        (void)rows;
        return std::vector<TableChange>{};
      });
  EXPECT_EQ(malformed.status().code(), ErrorCode::FailedPrecondition);
  EXPECT_EQ(engine->revision(), revision);
  EXPECT_FALSE(engine->failed());
}

TEST_P(EngineRuntimeContract, DuplicateNewOnlyIdentityWithinOneFrameAndRuntimeRowBudgetAreAtomic) {
  define_function_graph();
  const auto status = status_table("status");
  open();
  const auto before = engine->revision();
  auto duplicate = EngineRuntimeAccess::apply(
      *engine, {1}, [&](RoundContext&) noexcept -> StatusOr<std::vector<TableChange>> {
        return std::vector<TableChange>{capture_row(), capture_row()};
      });
  EXPECT_EQ(duplicate.status().code(), ErrorCode::Conflict);
  EXPECT_FALSE(engine->failed());
  EXPECT_TRUE(take(engine->query_sql("SELECT x FROM capture WHERE k='a'")).rows.empty());
  auto deleted = EngineRuntimeAccess::apply(
      *engine, {1}, [&](RoundContext&) noexcept -> StatusOr<std::vector<TableChange>> {
        return std::vector<TableChange>{
            {capture, Row{std::string("a"), std::string("new")}, std::nullopt}};
      });
  EXPECT_EQ(deleted.status().code(), ErrorCode::InvalidArgument);
  EXPECT_EQ(engine->revision(), before);
  engine.reset();
  config.max_rows = 1;
  open();
  auto limited = EngineRuntimeAccess::apply(
      *engine, {1},
      [&](RoundContext&) noexcept -> StatusOr<std::vector<TableChange>> {
        return std::vector<TableChange>{write(status, 1)};
      },
      [&](RoundContext&,
          const std::vector<RowChange>&) noexcept -> StatusOr<std::vector<TableChange>> {
        return std::vector<TableChange>{write(status, 2)};
      });
  EXPECT_EQ(limited.status().code(), ErrorCode::ResourceExhausted);
  EXPECT_FALSE(engine->failed());
  EXPECT_EQ(engine->revision(), before);
}

TEST_P(EngineRuntimeContract, ScanRejectsPartialScopesAndInvalidImagesWithoutPoisoningFrame) {
  const auto status = status_table("status");
  open();
  for (const auto& scope :
       {Row{}, Row{std::int64_t{1}}, Row{std::string("a"), std::string("extra")}}) {
    auto rejected = EngineRuntimeAccess::apply(
        *engine, {}, [&](RoundContext& context) noexcept -> StatusOr<std::vector<TableChange>> {
          EF_ASSIGN_OR_RETURN(auto rows, context.scan_relation(status, scope));
          (void)rows;
          return std::vector<TableChange>{};
        });
    EXPECT_EQ(rejected.status().code(), ErrorCode::InvalidArgument);
  }
  auto bad_image = EngineRuntimeAccess::apply(
      *engine, {}, [&](RoundContext& context) noexcept -> StatusOr<std::vector<TableChange>> {
        EF_ASSIGN_OR_RETURN(auto rows, context.scan_relation(status, Row{std::string("a")},
                                                             static_cast<StateImage>(99)));
        (void)rows;
        return std::vector<TableChange>{};
      });
  EXPECT_EQ(bad_image.status().code(), ErrorCode::InvalidArgument);
  EXPECT_FALSE(engine->failed());
}

TEST_P(EngineRuntimeContract, PublicFlattenPreservesSignedZeroChanges) {
  const auto source = take(catalog->define_sql(
      "CREATE TABLE source(k STRING,v FLOAT64,PRIMARY KEY(k)) PARTITION BY(k)"));
  take(catalog->define_sql("CREATE MATERIALIZED VIEW projected_zero AS SELECT k,v FROM source"));
  open();
  take(engine->apply({source, Row{std::string("a")}, Row{std::string("a"), -0.0}}));
  auto changed = take(engine->apply({source, Row{std::string("a")}, Row{std::string("a"), 0.0}}));
  EXPECT_EQ(changed.changes.size(), 2U);
}

TEST_P(EngineRuntimeContract, EngineAllowsOnlyOneRequestOwnerUntilExplicitDetach) {
  status_table("status");
  open();
  EXPECT_TRUE(EngineRuntimeAccess::attach_request_owner(*engine).ok());
  EXPECT_EQ(EngineRuntimeAccess::attach_request_owner(*engine).code(), ErrorCode::Conflict);
  EngineRuntimeAccess::detach_request_owner(*engine);
  EXPECT_TRUE(EngineRuntimeAccess::attach_request_owner(*engine).ok());
  EngineRuntimeAccess::detach_request_owner(*engine);
  EXPECT_FALSE(engine->failed());
}

INSTANTIATE_TEST_SUITE_P(MemoryRocksRemote, EngineRuntimeContract, ::testing::Values(0, 1, 2, 3));
} // namespace
} // namespace eventflow::test
