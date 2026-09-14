#include "../src/runtime/engine_internal.hpp"
#include "eventflow/key_codec.hpp"
#include "eventflow/request_machine.hpp"
#include "remote_harness.hpp"
#include "status_test_support.hpp"
#include <filesystem>
#include <gtest/gtest.h>

namespace eventflow::test {
namespace {
struct LatestGraph {
  std::unique_ptr<Catalog> catalog;
  RelationId candidate = 0, capture = 0, result = 0, status = 0, latest = 0, child = 0;

  Status define() noexcept {
    EF_ASSIGN_OR_RETURN(catalog, Catalog::create());
    EF_ASSIGN_OR_RETURN(candidate,
                        catalog->define_candidate({"candidate",
                                                   {{1, "k", DataType::String, false}},
                                                   {{2, "text", DataType::String, false}}}));
    DefinitionOptions options;
    options.base_write_role = BaseWriteRole::RequestCapture;
    options.identity_domain = "requests";
    EF_ASSIGN_OR_RETURN(
        capture, catalog->define_sql("CREATE TABLE capture(k STRING,request_id STRING,x INT64 NOT "
                                     "NULL,PRIMARY KEY(k,request_id)) PARTITION BY(k)",
                                     options));
    EF_ASSIGN_OR_RETURN(
        result,
        catalog->define_sql(
            "CREATE MATERIALIZED VIEW result AS SELECT k,request_id,x AS score FROM capture"));
    auto snapshot = take(catalog->snapshot());
    auto input = take(snapshot.find(capture));
    EF_ASSIGN_OR_RETURN(status,
                        catalog->define_request_status(
                            {"request_meta", "requests", input->identity_fields, candidate}));
    EF_ASSIGN_OR_RETURN(latest, catalog->define_latest({"latest", result, status, candidate}));
    EF_ASSIGN_OR_RETURN(
        child, catalog->define_sql(
                   "CREATE MATERIALIZED VIEW doubled AS SELECT k,score*2 AS value FROM latest"));
    return {};
  }
};

TEST(EngineLatest, PublicOpenRejectsAnnotationsBeforeInitializingStorage) {
  LatestGraph graph;
  ASSERT_TRUE(graph.define().ok());
  StoreConfig storage;
  storage.shard_count = 1;
  auto store = std::shared_ptr<IStateStore>(take(MemoryStateStore::open(storage)));
  EngineConfig config;
  config.shard_count = 1;
  auto engine = Engine::open(store, take(graph.catalog->snapshot()), config);
  EXPECT_EQ(engine.status().code(), ErrorCode::FailedPrecondition);
  EXPECT_EQ(take(store->status(0)).revision, 0U);
}

TEST(EngineLatest, SuccessfulRequestSelectsThenPropagatesToOrdinarySqlInTheSameCommit) {
  LatestGraph graph;
  ASSERT_TRUE(graph.define().ok());
  StoreConfig storage;
  storage.shard_count = 1;
  auto store = std::shared_ptr<IStateStore>(take(MemoryStateStore::open(storage)));
  EngineConfig config;
  config.shard_count = 1;
  auto snapshot = take(graph.catalog->snapshot());
  auto engine =
      take(EngineRuntimeAccess::open(store, snapshot, config, take(FunctionRegistry::create())));
  auto activated = EngineRuntimeAccess::apply(
      *engine, {1}, [&](RoundContext& round) noexcept -> StatusOr<std::vector<TableChange>> {
        EF_ASSIGN_OR_RETURN(auto row, EngineRuntimeAccess::make_candidate_write(
                                          round, graph.candidate, {std::string("a")},
                                          CandidateOperation::Activate, 0, {std::string("news")}));
        return std::vector<TableChange>{std::move(row)};
      });
  ASSERT_TRUE(activated.ok()) << activated.status().message();
  RequestManifest manifest;
  manifest.scope = {std::string("a")};
  manifest.identity = {std::string("a"), std::string("r1")};
  manifest.request_id = "r1";
  manifest.candidate_epoch = 1;
  manifest.admission_seq = 1;
  manifest.catalog_fingerprint = snapshot.fingerprint;
  manifest.targets = {graph.result};
  auto completed = EngineRuntimeAccess::apply(
      *engine, {2},
      [&](RoundContext&) noexcept -> StatusOr<std::vector<TableChange>> {
        return std::vector<TableChange>{
            {graph.capture, manifest.identity,
             Row{std::string("a"), std::string("r1"), std::int64_t{7}}}};
      },
      [&](RoundContext& round,
          const std::vector<RowChange>&) noexcept -> StatusOr<std::vector<TableChange>> {
        EF_ASSIGN_OR_RETURN(auto row, EngineRuntimeAccess::make_request_status_write(
                                          round, graph.status, manifest, RequestPhase::Success));
        return std::vector<TableChange>{std::move(row)};
      });
  ASSERT_TRUE(completed.ok()) << completed.status().message();
  EXPECT_EQ(completed->commit.revision, activated->commit.revision + 1);
  auto rows =
      take(engine->query_sql("SELECT score,__ef_latest_candidate_epoch,__ef_latest_admission_seq "
                             "FROM latest WHERE k='a'"));
  ASSERT_EQ(rows.rows.size(), 1U);
  EXPECT_EQ(rows.rows[0], (Row{std::int64_t{7}, std::int64_t{1}, std::int64_t{1}}));
  EXPECT_EQ(take(engine->query_sql("SELECT value FROM doubled WHERE k='a'")).rows[0],
            Row{std::int64_t{14}});
}

class ObservedLatestStore final : public IStateStore {
public:
  explicit ObservedLatestStore(std::shared_ptr<IStateStore> value) noexcept
      : backend(std::move(value)) {}

  StatusOr<ReadResult> get_many(std::uint32_t shard,
                                const std::vector<std::string>& keys) noexcept override {
    return backend->get_many(shard, keys);
  }

  StatusOr<ScanResult> scan(std::uint32_t shard, const std::string& prefix,
                            const std::optional<std::string>& after,
                            std::size_t limit) noexcept override {
    ++scans;
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
    EF_ASSIGN_OR_RETURN(auto result, backend->commit(batch));
    if (unknown_next) {
      unknown_next = false;
      return CommitResult{CommitCode::Unknown, batch.expected_revision, false, "lost response"};
    }
    return result;
  }

  std::shared_ptr<IStateStore> backend;
  std::vector<CommitBatch> batches;
  bool unknown_next = false;
  std::size_t scans = 0;
};

class LatestContract : public testing::TestWithParam<int> {
protected:
  LatestGraph graph;
  std::shared_ptr<ObservedLatestStore> store;
  std::unique_ptr<Engine> engine;
  EngineConfig config;
  std::shared_ptr<FunctionRegistry> registry;
  std::string path;

  void SetUp() noexcept override {
    ASSERT_TRUE(graph.define().ok());
    char pattern[] = "/tmp/eventflow-latest-contract-XXXXXX";
    auto created = ::mkdtemp(pattern);
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
    store = std::make_shared<ObservedLatestStore>(std::shared_ptr<IStateStore>(std::move(backend)));
    config.shard_count = 1;
    registry = take(FunctionRegistry::create());
    reopen();
    activate(0);
  }

  void TearDown() noexcept override {
    engine.reset();
    store.reset();
    std::error_code error;
    std::filesystem::remove_all(path, error);
  }

  void reopen() noexcept {
    engine.reset();
    engine =
        take(EngineRuntimeAccess::open(store, take(graph.catalog->snapshot()), config, registry));
  }

  RequestManifest manifest(const std::string& id, std::int64_t sequence,
                           std::int64_t epoch = 1) noexcept {
    RequestManifest value;
    value.scope = {std::string("a")};
    value.identity = {std::string("a"), id};
    value.request_id = id;
    value.admission_seq = sequence;
    value.candidate_epoch = epoch;
    value.catalog_fingerprint = take(graph.catalog->snapshot()).fingerprint;
    value.targets = {graph.result};
    return value;
  }

  DispatchResult activate(std::int64_t previous) noexcept {
    return take(EngineRuntimeAccess::apply(
        *engine, {1}, [&](RoundContext& round) noexcept -> StatusOr<std::vector<TableChange>> {
          EF_ASSIGN_OR_RETURN(auto row,
                              EngineRuntimeAccess::make_candidate_write(
                                  round, graph.candidate, {std::string("a")},
                                  CandidateOperation::Activate, previous, {std::string("text")}));
          return std::vector<TableChange>{std::move(row)};
        }));
  }

  StatusOr<DispatchResult> stage(const RequestManifest& request, RequestPhase phase,
                                 std::optional<std::int64_t> score = {}) noexcept {
    return EngineRuntimeAccess::apply(
        *engine, {1},
        [&](RoundContext&) noexcept -> StatusOr<std::vector<TableChange>> {
          if (!score)
            return std::vector<TableChange>{};
          return std::vector<TableChange>{
              {graph.capture, request.identity, Row{request.scope[0], request.request_id, *score}}};
        },
        [&](RoundContext& round,
            const std::vector<RowChange>&) noexcept -> StatusOr<std::vector<TableChange>> {
          EF_ASSIGN_OR_RETURN(auto row, EngineRuntimeAccess::make_request_status_write(
                                            round, graph.status, request, phase));
          return std::vector<TableChange>{std::move(row)};
        });
  }

  std::vector<Row> latest() noexcept {
    return take(engine->query_sql(
                    "SELECT score,__ef_latest_candidate_epoch,__ef_latest_admission_seq FROM "
                    "latest WHERE k='a'"))
        .rows;
  }
};

TEST_P(LatestContract, WholeRequestSuccessGatesWinnerAndLaterFailedRequestKeepsLastSuccess) {
  auto first = manifest("r1", 1), second = manifest("r2", 2), third = manifest("r3", 3);
  take(stage(first, RequestPhase::Running, 10));
  EXPECT_TRUE(latest().empty());
  take(stage(second, RequestPhase::Running, 20));
  take(stage(second, RequestPhase::Success));
  EXPECT_EQ(latest()[0], (Row{std::int64_t{20}, std::int64_t{1}, std::int64_t{2}}));
  take(stage(first, RequestPhase::Success));
  EXPECT_EQ(latest()[0][0], Value(std::int64_t{20}));
  take(stage(third, RequestPhase::Failed, 30));
  EXPECT_EQ(latest()[0][2], Value(std::int64_t{2}));
  EXPECT_EQ(take(engine->query_sql("SELECT score FROM result WHERE k='a'")).rows.size(), 3U);
}

TEST_P(LatestContract, CandidateSwitchAndExpiryClearCurrentProjectionButKeepOldHistory) {
  auto old = manifest("old", 1), late = manifest("late", 2);
  take(stage(old, RequestPhase::Success, 7));
  take(stage(late, RequestPhase::Running, 8));
  activate(1);
  EXPECT_TRUE(latest().empty());
  EXPECT_TRUE(take(engine->query_sql("SELECT value FROM doubled WHERE k='a'")).rows.empty());
  take(stage(late, RequestPhase::Success));
  EXPECT_TRUE(latest().empty());
  take(stage(manifest("new", 3, 2), RequestPhase::Success, 11));
  EXPECT_EQ(latest()[0], (Row{std::int64_t{11}, std::int64_t{2}, std::int64_t{3}}));
  take(EngineRuntimeAccess::apply(
      *engine, {1}, [&](RoundContext& round) noexcept -> StatusOr<std::vector<TableChange>> {
        EF_ASSIGN_OR_RETURN(auto row, EngineRuntimeAccess::make_candidate_write(
                                          round, graph.candidate, {std::string("a")},
                                          CandidateOperation::Deactivate, 2, {}));
        return std::vector<TableChange>{std::move(row)};
      }));
  EXPECT_TRUE(latest().empty());
  EXPECT_TRUE(take(engine->query_sql("SELECT value FROM doubled WHERE k='a'")).rows.empty());
  EXPECT_EQ(take(engine->query_sql("SELECT score FROM result WHERE k='a'")).rows.size(), 3U);
  auto rejected = stage(manifest("resurrection", 4, 2), RequestPhase::Success, 12);
  EXPECT_EQ(rejected.status().code(), ErrorCode::FailedPrecondition);
  EXPECT_EQ(take(engine->query_sql("SELECT score FROM result WHERE k='a'")).rows.size(), 3U);
}

TEST_P(LatestContract, UnknownRetriesSameSelectorAndMetadataBatchWithoutCallbackReexecution) {
  auto request = manifest("r1", 1);
  take(stage(request, RequestPhase::Running, 9));
  int calls = 0;
  store->unknown_next = true;
  auto uncertain = take(EngineRuntimeAccess::apply(
      *engine, {1}, {},
      [&](RoundContext& round,
          const std::vector<RowChange>&) noexcept -> StatusOr<std::vector<TableChange>> {
        ++calls;
        EF_ASSIGN_OR_RETURN(auto row, EngineRuntimeAccess::make_request_status_write(
                                          round, graph.status, request, RequestPhase::Success));
        return std::vector<TableChange>{std::move(row)};
      }));
  EXPECT_EQ(uncertain.commit.code, CommitCode::Unknown);
  EXPECT_TRUE(uncertain.changes.empty());
  auto original = take(canonical_batch(store->batches.back()));
  EXPECT_FALSE(engine->query_sql("SELECT score FROM latest WHERE k='a'").ok());
  auto confirmed = take(engine->resolve_pending_commit());
  EXPECT_EQ(confirmed.commit.code, CommitCode::Committed);
  EXPECT_EQ(take(canonical_batch(store->batches.back())), original);
  EXPECT_EQ(calls, 1);
  EXPECT_EQ(latest()[0][0], Value(std::int64_t{9}));
}

TEST_P(LatestContract, ReopenAndNewCompletionsUsePointReadsWithoutScanningHistory) {
  for (std::int64_t i = 1; i <= 20; ++i)
    take(stage(manifest("r" + std::to_string(i), i), RequestPhase::Success, i));
  reopen();
  EXPECT_EQ(latest()[0][0], Value(std::int64_t{20}));
  auto scans = store->scans;
  take(stage(manifest("r21", 21), RequestPhase::Success, 21));
  EXPECT_EQ(store->scans, scans);
  EXPECT_EQ(latest()[0][0], Value(std::int64_t{21}));
}

TEST_P(LatestContract, DirectMetadataAndAlteredFactoryRowsAreRejectedAtomically) {
  auto revision = engine->revision();
  auto direct = EngineRuntimeAccess::apply(
      *engine, {1}, [&](RoundContext&) noexcept -> StatusOr<std::vector<TableChange>> {
        return std::vector<TableChange>{
            {graph.candidate,
             {std::string("a")},
             Row{std::string("a"), std::int64_t{100}, true, std::string("forged")}}};
      });
  EXPECT_EQ(direct.status().code(), ErrorCode::InvalidArgument);
  auto changed = EngineRuntimeAccess::apply(
      *engine, {1}, [&](RoundContext& round) noexcept -> StatusOr<std::vector<TableChange>> {
        EF_ASSIGN_OR_RETURN(auto row, EngineRuntimeAccess::make_candidate_write(
                                          round, graph.candidate, {std::string("a")},
                                          CandidateOperation::Activate, 1, {std::string("valid")}));
        (*row.row)[3] = std::string("changed");
        return std::vector<TableChange>{std::move(row)};
      });
  EXPECT_EQ(changed.status().code(), ErrorCode::InvalidArgument);
  EXPECT_EQ(engine->revision(), revision);
  EXPECT_FALSE(engine->failed());
  EXPECT_EQ(take(engine->query_sql("SELECT candidate_epoch FROM candidate WHERE k='a'")).rows[0],
            Row{std::int64_t{1}});
}

TEST_P(LatestContract, StatusPhaseOrderingAndSameSequenceConflictDoNotOverwriteWinner) {
  auto first = manifest("r1", 1);
  take(stage(first, RequestPhase::Success, 7));
  auto regression = stage(first, RequestPhase::Running);
  EXPECT_EQ(regression.status().code(), ErrorCode::Conflict);
  auto tied = stage(manifest("other", 1), RequestPhase::Success, 8);
  EXPECT_EQ(tied.status().code(), ErrorCode::Conflict);
  EXPECT_EQ(latest()[0][0], Value(std::int64_t{7}));
  EXPECT_EQ(take(engine->query_sql("SELECT score FROM result WHERE k='a'")).rows.size(), 1U);
  auto rewritten = first;
  rewritten.admission_seq = 9;
  EXPECT_EQ(stage(rewritten, RequestPhase::Success).status().code(), ErrorCode::Conflict);
}

TEST_P(LatestContract, ScopeAndManifestDomainCannotRedirectMetadata) {
  auto request = manifest("r1", 1);
  request.catalog_fingerprint = "other";
  EXPECT_EQ(stage(request, RequestPhase::Success, 7).status().code(), ErrorCode::InvalidArgument);
  request = manifest("r1", 1);
  request.identity.back() = std::string("different");
  EXPECT_EQ(stage(request, RequestPhase::Success, 7).status().code(), ErrorCode::InvalidArgument);
  request = manifest("r1", 1);
  request.targets = {graph.latest};
  EXPECT_EQ(stage(request, RequestPhase::Success, 7).status().code(), ErrorCode::InvalidArgument);
  EXPECT_TRUE(latest().empty());
  EXPECT_TRUE(take(engine->query_sql("SELECT score FROM result WHERE k='a'")).rows.empty());
}

TEST_P(LatestContract, SelectorRawStateCannotBeOverwrittenThroughAnOwnerCompanion) {
  auto request = manifest("r1", 1);
  take(stage(request, RequestPhase::Success, 7));
  auto revision = engine->revision();
  for (char kind : {'S', 'I'}) {
    auto key = take(row_key(0, kind, graph.latest, {std::string("a")}));
    auto corrupted = EngineRuntimeAccess::apply(
        *engine, {1}, [&](RoundContext& round) noexcept -> StatusOr<std::vector<TableChange>> {
          EF_RETURN_IF_ERROR(round.stage({key, std::string("bad")}));
          return std::vector<TableChange>{};
        });
    EXPECT_EQ(corrupted.status().code(), ErrorCode::InvalidArgument);
  }
  EXPECT_EQ(engine->revision(), revision);
  EXPECT_EQ(latest()[0][0], Value(std::int64_t{7}));
}

TEST_P(LatestContract, ReopenedScopedReadsRejectMissingOrInconsistentWinnerState) {
  take(stage(manifest("r1", 1), RequestPhase::Success, 7));
  auto state_key = take(row_key(0, 'S', graph.latest, {std::string("a")}));
  auto current = take(store->get_many(0, {state_key}));
  ASSERT_TRUE(current.values[0]);
  auto state = take(parse_json_exact(*current.values[0]));
  state["sequence"] = std::int64_t{2};
  CommitBatch batch{0, config.owner, engine->revision(), {}, {{state_key, state.dump()}}};
  batch.commit_id = take(make_commit_id(batch.owner, 0, batch.expected_revision));
  EXPECT_EQ(take(store->commit(batch)).code, CommitCode::Committed);
  reopen();
  auto result = engine->query_sql("SELECT score FROM latest WHERE k='a'");
  EXPECT_EQ(result.status().code(), ErrorCode::FailedPrecondition);
}

INSTANTIATE_TEST_SUITE_P(AllStores, LatestContract, testing::Values(0, 1, 2, 3));

} // namespace
} // namespace eventflow::test
