#include "eventflow/cached_store.hpp"
#include "eventflow/engine.hpp"
#include "eventflow/key_codec.hpp"
#include "eventflow/kv_server.hpp"
#include "status_test_support.hpp"
#include <filesystem>
#include <gtest/gtest.h>
#include <unistd.h>

namespace eventflow {
namespace {
using test::take;

class CommitFault final : public IStateStore {
public:
  explicit CommitFault(std::shared_ptr<IStateStore> backend) noexcept
      : backend_(std::move(backend)) {}

  bool lose_ack = false;

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
      return CommitResult{CommitCode::Unknown, batch.expected_revision, false, "lost response"};
    }
    return result;
  }

  StatusOr<StoreLimits> limits() noexcept override {
    return backend_->limits();
  }

  StatusOr<ShardStatus> status(std::uint32_t shard) noexcept override {
    return backend_->status(shard);
  }

private:
  std::shared_ptr<IStateStore> backend_;
};

class EngineCacheContract : public testing::TestWithParam<bool> {
protected:
  void SetUp() noexcept override {
    directory = "/tmp/eventflow-engine-cache-XXXXXX";
    ASSERT_NE(::mkdtemp(directory.data()), nullptr);
    StoreConfig config;
    config.shard_count = 1;
    if (GetParam())
      backend = take(RocksStateStore::open(directory + "/db", config));
    else
      backend = take(MemoryStateStore::open(config));
    server = take(KvServer::create(backend));
    const auto port = take(server->start("127.0.0.1", 0));
    auto remote = std::shared_ptr<IStateStore>(
        take(RemoteStateStore::open("http://127.0.0.1:" + std::to_string(port))));
    fault = std::make_shared<CommitFault>(std::move(remote));
    cache = take(CachedStateStore::open(fault));
    auto catalog = take(Catalog::create());
    trades = take(catalog->define_sql("CREATE TABLE trades (k STRING NOT NULL, id INT64 NOT NULL, "
                                      "qty INT64 NOT NULL, PRIMARY KEY(k,id)) PARTITION BY(k)"));
    quotes = take(catalog->define_sql("CREATE TABLE quotes (k STRING NOT NULL, price INT64 NOT "
                                      "NULL, PRIMARY KEY(k)) PARTITION BY(k)"));
    take(catalog->define_sql(
        "CREATE MATERIALIZED VIEW volume AS SELECT k, SUM(qty) AS qty FROM trades GROUP BY k"));
    take(catalog->define_sql("CREATE MATERIALIZED VIEW amount AS SELECT volume.k, volume.qty * "
                             "quotes.price AS value FROM volume JOIN quotes ON volume.k=quotes.k"));
    EngineConfig engine_config;
    engine_config.shard_count = 1;
    engine = take(Engine::open(cache, take(catalog->snapshot()), engine_config));
    take(engine->apply({quotes, {std::string("a")}, Row{std::string("a"), std::int64_t(10)}}));
    take(engine->apply({trades,
                        {std::string("a"), std::int64_t(1)},
                        Row{std::string("a"), std::int64_t(1), std::int64_t(1)}}));
  }

  void TearDown() noexcept override {
    engine.reset();
    cache.reset();
    fault.reset();
    server.reset();
    backend.reset();
    std::error_code ignored;
    std::filesystem::remove_all(directory, ignored);
  }

  std::string directory;
  RelationId trades = 0;
  RelationId quotes = 0;
  std::shared_ptr<IStateStore> backend;
  std::unique_ptr<KvServer> server;
  std::shared_ptr<CommitFault> fault;
  std::shared_ptr<CachedStateStore> cache;
  std::unique_ptr<Engine> engine;
};

TEST_P(EngineCacheContract, OtherBranchCommitsPreserveReusableLookupState) {
  const auto before = cache->metrics();
  for (std::int64_t i = 2; i <= 5; ++i)
    ASSERT_EQ(take(engine->apply({trades,
                                  {std::string("a"), std::int64_t(1)},
                                  Row{std::string("a"), std::int64_t(1), i}}))
                  .commit.code,
              CommitCode::Committed);
  EXPECT_GT(cache->metrics().hits, before.hits);
  auto output = take(engine->query_sql("SELECT value FROM amount WHERE k='a'"));
  ASSERT_EQ(output.rows.size(), 1u);
  EXPECT_EQ(std::get<std::int64_t>(output.rows[0][0]), 50);
}

TEST_P(EngineCacheContract, UnknownBlocksWarmCacheUntilOriginalEngineRoundIsConfirmed) {
  const auto original_revision = engine->revision();
  fault->lose_ack = true;
  auto pending = take(engine->apply({trades,
                                     {std::string("a"), std::int64_t(1)},
                                     Row{std::string("a"), std::int64_t(1), std::int64_t(3)}}));
  EXPECT_EQ(pending.commit.code, CommitCode::Unknown);
  EXPECT_TRUE(pending.changes.empty());
  EXPECT_TRUE(cache->has_pending_commit());
  EXPECT_EQ(cache->metrics().entries, 0u);
  EXPECT_FALSE(engine->query_sql("SELECT value FROM amount WHERE k='a'").ok());
  EXPECT_FALSE(cache->get_many(0, {take(row_key(0, 'T', quotes, {std::string("a")}))}).ok());
  auto confirmed = take(engine->resolve_pending_commit());
  EXPECT_EQ(confirmed.commit.code, CommitCode::Committed);
  EXPECT_TRUE(confirmed.commit.duplicate);
  EXPECT_FALSE(confirmed.changes.empty());
  EXPECT_EQ(engine->revision(), original_revision + 1);
  EXPECT_FALSE(cache->has_pending_commit());
  auto output = take(engine->query_sql("SELECT value FROM amount WHERE k='a'"));
  ASSERT_EQ(output.rows.size(), 1u);
  EXPECT_EQ(std::get<std::int64_t>(output.rows[0][0]), 30);
  EXPECT_FALSE(engine->resolve_pending_commit().ok());
}

INSTANTIATE_TEST_SUITE_P(RemoteMemoryAndRemoteRocks, EngineCacheContract, testing::Bool());
} // namespace
} // namespace eventflow
