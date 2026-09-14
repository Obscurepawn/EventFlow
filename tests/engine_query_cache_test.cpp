#include "eventflow/engine.hpp"
#include "status_test_support.hpp"

#include <filesystem>
#include <gtest/gtest.h>
#include <unistd.h>

namespace eventflow {
namespace {
using test::take;

class QueryCacheFaultStore final : public IStateStore {
public:
  explicit QueryCacheFaultStore(std::shared_ptr<IStateStore> backend) noexcept
      : backend_(std::move(backend)) {}

  StatusOr<ReadResult> get_many(std::uint32_t shard,
                                const std::vector<std::string>& keys) noexcept override {
    return backend_->get_many(shard, keys);
  }

  StatusOr<ScanResult> scan(std::uint32_t shard, const std::string& prefix,
                            const std::optional<std::string>& after,
                            std::size_t limit) noexcept override {
    return backend_->scan(shard, prefix, after, limit);
  }

  StatusOr<CommitResult> commit(const CommitBatch& batch) noexcept override {
    EF_ASSIGN_OR_RETURN(auto result, backend_->commit(batch));
    if (lose_ack && result.code == CommitCode::Committed) {
      lose_ack = false;
      return CommitResult{CommitCode::Unknown, batch.expected_revision, false, "lost receipt"};
    }
    return result;
  }

  StatusOr<StoreLimits> limits() noexcept override {
    return backend_->limits();
  }

  StatusOr<ShardStatus> status(std::uint32_t shard) noexcept override {
    return backend_->status(shard);
  }

  bool lose_ack = false;

private:
  std::shared_ptr<IStateStore> backend_;
};

class EngineQueryCacheTest : public testing::TestWithParam<bool> {
protected:
  void SetUp() noexcept override {
    directory = "/tmp/eventflow-query-cache-XXXXXX";
    ASSERT_NE(::mkdtemp(directory.data()), nullptr);
    reopen();
  }

  void TearDown() noexcept override {
    engine.reset();
    fault.reset();
    std::error_code error;
    std::filesystem::remove_all(directory, error);
    EXPECT_FALSE(error);
  }

  void reopen(EngineConfig config = {}) noexcept {
    engine.reset();
    fault.reset();
    StoreConfig store_config;
    store_config.shard_count = 1;
    std::shared_ptr<IStateStore> backend;
    if (GetParam())
      backend = take(RocksStateStore::open(directory + "/db-" + std::to_string(next_database++),
                                           store_config));
    else
      backend = take(MemoryStateStore::open(store_config));
    fault = std::make_shared<QueryCacheFaultStore>(std::move(backend));
    auto catalog = take(Catalog::create());
    table = take(catalog->define_sql(
        "CREATE TABLE items (k STRING, id INT64, v INT64, PRIMARY KEY(k,id)) PARTITION BY(k)"));
    config.shard_count = 1;
    engine = take(Engine::open(fault, take(catalog->snapshot()), config));
    take(engine->apply_many({change("a", 10), change("b", 20)}));
  }

  TableChange change(const std::string& key, std::int64_t value) const noexcept {
    return {table, {key, std::int64_t{1}}, Row{key, std::int64_t{1}, value}};
  }

  static std::int64_t value(const QueryResult& result) noexcept {
    EXPECT_EQ(result.rows.size(), 1U);
    if (result.rows.size() != 1 || result.rows[0].empty())
      std::abort();
    const auto* number = std::get_if<std::int64_t>(&result.rows[0][0]);
    if (!number)
      std::abort();
    return *number;
  }

  static QueryContext scope(std::string key) noexcept {
    QueryContext context;
    context.scope = Row{std::move(key)};
    return context;
  }

  const std::string sql = "SELECT v FROM items WHERE k='a'";
  std::string directory;
  std::size_t next_database = 0;
  RelationId table = 0;
  std::shared_ptr<QueryCacheFaultStore> fault;
  std::unique_ptr<Engine> engine;
};

TEST_P(EngineQueryCacheTest, RepeatQueriesReusePlanButObserveNewRowsAndRevision) {
  const auto first = take(engine->query_sql(sql));
  EXPECT_EQ(value(first), 10);
  EXPECT_EQ(value(take(engine->query_sql(sql))), 10);
  auto metrics = engine->query_cache_metrics();
  EXPECT_EQ(metrics.misses, 1U);
  EXPECT_EQ(metrics.hits, 1U);
  EXPECT_EQ(metrics.entries, 1U);
  EXPECT_GT(metrics.bytes, sql.size());
  take(engine->apply(change("a", 30)));
  const auto updated = take(engine->query_sql(sql));
  EXPECT_EQ(value(updated), 30);
  EXPECT_GT(updated.revision, first.revision);
  EXPECT_EQ(engine->query_cache_metrics().hits, 2U);
}

TEST_P(EngineQueryCacheTest, CachedPlanRechecksExplicitScopeAndCurrentQueryBudget) {
  const std::string unscoped = "SELECT v FROM items";
  EXPECT_EQ(value(take(engine->query_sql(unscoped, scope("a")))), 10);
  EXPECT_EQ(value(take(engine->query_sql(unscoped, scope("b")))), 20);
  EXPECT_EQ(engine->query_cache_metrics().hits, 1U);
  EXPECT_EQ(engine->query_sql(unscoped).status().code(), ErrorCode::InvalidArgument);
  auto context = scope("a");
  context.max_rows = 0;
  EXPECT_EQ(engine->query_sql(unscoped, context).status().code(), ErrorCode::InvalidArgument);
  take(engine->query_sql(sql));
  EXPECT_EQ(engine->query_sql(sql, scope("b")).status().code(), ErrorCode::InvalidArgument);
}

TEST_P(EngineQueryCacheTest, ExactSqlTextUsesLruEntryBudget) {
  EngineConfig config;
  config.max_cached_queries = 2;
  reopen(config);
  const auto second = " " + sql;
  const auto third = "  " + sql;
  take(engine->query_sql(sql));
  take(engine->query_sql(second));
  take(engine->query_sql(sql));
  take(engine->query_sql(third));
  auto metrics = engine->query_cache_metrics();
  EXPECT_EQ(metrics.entries, 2U);
  EXPECT_EQ(metrics.hits, 1U);
  EXPECT_EQ(metrics.misses, 3U);
  EXPECT_EQ(metrics.evictions, 1U);
  take(engine->query_sql(second));
  EXPECT_EQ(engine->query_cache_metrics().misses, 4U);
  EXPECT_EQ(engine->query_cache_metrics().evictions, 2U);
}

TEST_P(EngineQueryCacheTest, ByteBudgetEvictsAndOversizedPlansStillExecute) {
  take(engine->query_sql(sql));
  const auto charge = engine->query_cache_metrics().bytes;
  ASSERT_GT(charge, 1U);
  EngineConfig config;
  config.max_query_cache_bytes = charge;
  reopen(config);
  take(engine->query_sql(sql));
  auto alternate = sql;
  alternate[0] = 's';
  take(engine->query_sql(alternate));
  EXPECT_EQ(engine->query_cache_metrics().entries, 1U);
  EXPECT_EQ(engine->query_cache_metrics().evictions, 1U);
  EXPECT_LE(engine->query_cache_metrics().bytes, config.max_query_cache_bytes);
  config.max_query_cache_bytes = charge - 1;
  reopen(config);
  EXPECT_EQ(value(take(engine->query_sql(sql))), 10);
  EXPECT_EQ(value(take(engine->query_sql(sql))), 10);
  EXPECT_EQ(engine->query_cache_metrics().entries, 0U);
  EXPECT_EQ(engine->query_cache_metrics().bytes, 0U);
  EXPECT_EQ(engine->query_cache_metrics().hits, 0U);
  EXPECT_EQ(engine->query_cache_metrics().misses, 2U);
}

TEST_P(EngineQueryCacheTest, EitherZeroBudgetDisablesPlanCaching) {
  for (const bool zero_entries : {true, false}) {
    EngineConfig config;
    if (zero_entries)
      config.max_cached_queries = 0;
    else
      config.max_query_cache_bytes = 0;
    reopen(config);
    take(engine->query_sql(sql));
    take(engine->query_sql(sql));
    EXPECT_EQ(engine->query_cache_metrics().entries, 0U);
    EXPECT_EQ(engine->query_cache_metrics().bytes, 0U);
    EXPECT_EQ(engine->query_cache_metrics().hits, 0U);
    EXPECT_EQ(engine->query_cache_metrics().misses, 2U);
  }
}

TEST_P(EngineQueryCacheTest, CatalogActivationClearsPlansAndRebindsToTheNewVersion) {
  auto original = take(Catalog::from_snapshot(take(engine->snapshot())));
  const auto external = take(original->bind_query_sql(sql));
  const auto old_version = take(engine->query_sql(sql)).catalog_version;
  auto installed =
      take(engine->install_sql("CREATE MATERIALIZED VIEW projected AS SELECT k, v FROM items"));
  ASSERT_EQ(installed.commit.code, CommitCode::Committed);
  EXPECT_EQ(engine->query_cache_metrics().entries, 0U);
  EXPECT_EQ(engine->query_cache_metrics().bytes, 0U);
  const auto rebound = take(engine->query_sql(sql));
  EXPECT_GT(rebound.catalog_version, old_version);
  EXPECT_EQ(engine->query_cache_metrics().misses, 2U);
  EXPECT_EQ(value(take(engine->query_sql("SELECT v FROM projected WHERE k='a'"))), 10);
  EXPECT_FALSE(engine->query(external).ok());
}

TEST_P(EngineQueryCacheTest, UnknownApplyBlocksWarmPlansUntilConfirmed) {
  take(engine->query_sql(sql));
  const auto before = engine->query_cache_metrics();
  fault->lose_ack = true;
  const auto pending = take(engine->apply(change("a", 40)));
  ASSERT_EQ(pending.commit.code, CommitCode::Unknown);
  EXPECT_EQ(engine->query_sql(sql).status().code(), ErrorCode::FailedPrecondition);
  EXPECT_EQ(engine->query_cache_metrics().hits, before.hits);
  EXPECT_EQ(engine->query_cache_metrics().misses, before.misses);
  ASSERT_TRUE(take(engine->resolve_pending_commit()).commit.duplicate);
  EXPECT_EQ(value(take(engine->query_sql(sql))), 40);
  EXPECT_EQ(engine->query_cache_metrics().hits, before.hits + 1);
}

TEST_P(EngineQueryCacheTest, UnknownInstallActivatesAndInvalidatesOnlyAfterConfirmation) {
  const auto before = take(engine->query_sql(sql));
  fault->lose_ack = true;
  const auto pending =
      take(engine->install_sql("CREATE MATERIALIZED VIEW projected AS SELECT k, v FROM items"));
  ASSERT_EQ(pending.commit.code, CommitCode::Unknown);
  EXPECT_EQ(take(engine->snapshot()).version, before.catalog_version);
  EXPECT_EQ(engine->query_cache_metrics().entries, 1U);
  EXPECT_FALSE(engine->query_sql(sql).ok());
  EXPECT_FALSE(take(engine->snapshot()).find("projected").ok());
  ASSERT_EQ(take(engine->resolve_pending_commit()).commit.code, CommitCode::Committed);
  EXPECT_GT(take(engine->snapshot()).version, before.catalog_version);
  EXPECT_EQ(engine->query_cache_metrics().entries, 0U);
  EXPECT_EQ(value(take(engine->query_sql(sql))), 10);
  EXPECT_EQ(value(take(engine->query_sql("SELECT v FROM projected WHERE k='a'"))), 10);
  EXPECT_EQ(engine->query_cache_metrics().misses, 3U);
}

TEST_P(EngineQueryCacheTest, PublicPlansRemainValidatedAndCannotMutateInternalEntries) {
  take(engine->query_sql(sql));
  auto catalog = take(Catalog::from_snapshot(take(engine->snapshot())));
  auto plan = take(catalog->bind_query_sql(sql));
  auto mutable_expression = std::make_shared<BoundExpr>(*plan.projection[0]);
  plan.projection[0] = mutable_expression;
  EXPECT_EQ(value(take(engine->query(plan))), 10);
  mutable_expression->slot = 99999;
  EXPECT_FALSE(engine->query(plan).ok());
  EXPECT_EQ(value(take(engine->query_sql(sql))), 10);
  EXPECT_EQ(engine->query_cache_metrics().hits, 1U);
  plan = take(catalog->bind_query_sql(sql));
  plan.catalog_fingerprint += "forged";
  EXPECT_FALSE(engine->query(plan).ok());
  plan = take(catalog->bind_query_sql(sql));
  plan.inferred_scope = Row{std::string("b")};
  EXPECT_FALSE(engine->query(plan).ok());
  EXPECT_EQ(engine->query_cache_metrics().entries, 1U);
}

TEST_P(EngineQueryCacheTest, WarmPlansCannotBypassReentrancyOrFailedShardGates) {
  take(engine->query_sql(sql));
  const auto before = engine->query_cache_metrics();
  auto output = engine->apply(
      change("a", 30), {}, [this](RoundContext&, const std::vector<RowChange>&) noexcept -> Status {
        EXPECT_EQ(engine->query_sql(sql).status().code(), ErrorCode::FailedPrecondition);
        return Status(ErrorCode::InvalidArgument, "injected permanent computation failure");
      });
  EXPECT_FALSE(output.ok());
  ASSERT_TRUE(engine->failed());
  EXPECT_EQ(engine->query_sql(sql).status().code(), ErrorCode::FailedPrecondition);
  EXPECT_EQ(engine->query_cache_metrics().hits, before.hits);
  EXPECT_EQ(engine->query_cache_metrics().misses, before.misses);
}

TEST_P(EngineQueryCacheTest, InvalidSqlAndFailedDdlDoNotReplaceTheWarmPlan) {
  take(engine->query_sql(sql));
  EXPECT_FALSE(engine->query_sql("SELECT missing FROM items").ok());
  EXPECT_FALSE(engine->query_sql("CREATE TABLE ignored(x INT64)").ok());
  EXPECT_FALSE(
      engine->install_sql("CREATE MATERIALIZED VIEW bad AS SELECT missing FROM items").ok());
  EXPECT_EQ(engine->query_cache_metrics().entries, 1U);
  EXPECT_EQ(value(take(engine->query_sql(sql))), 10);
  EXPECT_EQ(engine->query_cache_metrics().hits, 1U);
}

INSTANTIATE_TEST_SUITE_P(MemoryAndRocks, EngineQueryCacheTest, testing::Bool());
} // namespace
} // namespace eventflow
