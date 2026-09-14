#include "eventflow/engine.hpp"
#include "eventflow/key_codec.hpp"
#include "remote_harness.hpp"
#include "status_test_support.hpp"
#include <filesystem>
#include <limits>
#include <unistd.h>

namespace eventflow::test {
namespace {
class BudgetStore final : public IStateStore {
public:
  explicit BudgetStore(std::shared_ptr<IStateStore> store) noexcept : backend(std::move(store)) {}

  StatusOr<StoreLimits> limits() noexcept override {
    return report_limits ? backend->limits() : IStateStore::limits();
  }

  StatusOr<ReadResult> get_many(std::uint32_t shard,
                                const std::vector<std::string>& keys) noexcept override {
    ++reads;
    return backend->get_many(shard, keys);
  }

  StatusOr<ScanResult> scan(std::uint32_t shard, const std::string& prefix,
                            const std::optional<std::string>& after,
                            std::size_t count) noexcept override {
    return backend->scan(shard, prefix, after, count);
  }

  StatusOr<CommitResult> commit(const CommitBatch& batch) noexcept override {
    ++commits;
    last_batch = batch;
    return backend->commit(batch);
  }

  StatusOr<ShardStatus> status(std::uint32_t shard) noexcept override {
    return backend->status(shard);
  }

  std::shared_ptr<IStateStore> backend;
  bool report_limits = true;
  std::size_t reads = 0;
  std::size_t commits = 0;
  CommitBatch last_batch;
};

TEST(EngineLimits, UnreportedCapabilityRejectsBeforeInitialization) {
  auto store = std::make_shared<BudgetStore>(take(MemoryStateStore::open()));
  store->report_limits = false;
  auto catalog = take(Catalog::create());
  auto opened = Engine::open(store, take(catalog->snapshot()));
  EXPECT_FALSE(opened.ok());
  EXPECT_EQ(store->commits, 0U);
  EXPECT_EQ(store->reads, 0U);
}

TEST(EngineLimits, PageSizeUsesActualStoreCapability) {
  StoreConfig storage;
  storage.shard_count = 1;
  storage.max_scan_rows = 2;
  auto store = std::shared_ptr<IStateStore>(take(MemoryStateStore::open(storage)));
  auto catalog = take(Catalog::create());
  auto table = take(catalog->define_sql(
      "CREATE TABLE t(k INT64 NOT NULL, id INT64 NOT NULL, PRIMARY KEY(k,id)) PARTITION BY(k)"));
  EngineConfig config;
  config.shard_count = 1;
  auto engine = take(Engine::open(store, take(catalog->snapshot()), config));
  for (std::int64_t i = 0; i < 5; ++i)
    take(engine->apply({table, {std::int64_t{1}, i}, Row{std::int64_t{1}, i}}));
  auto rows = engine->query_sql("SELECT * FROM t WHERE k=1");
  ASSERT_TRUE(rows.ok()) << rows.status().message();
  EXPECT_EQ(rows->rows.size(), 5U);
}

TEST(EngineLimits, OversizedRoundDoesNotReachCommitBoundary) {
  StoreConfig storage;
  storage.shard_count = 1;
  storage.max_batch_bytes = 8192;
  auto store = std::make_shared<BudgetStore>(take(MemoryStateStore::open(storage)));
  auto catalog = take(Catalog::create());
  auto table = take(catalog->define_sql(
      "CREATE TABLE t(k INT64 NOT NULL, payload STRING, PRIMARY KEY(k)) PARTITION BY(k)"));
  EngineConfig config;
  config.shard_count = 1;
  config.max_mutations = 16;
  auto engine = take(Engine::open(store, take(catalog->snapshot()), config));
  const auto commits = store->commits;
  const auto revision = engine->revision();
  auto result =
      engine->apply({table, {std::int64_t{1}}, Row{std::int64_t{1}, std::string(8192, 'x')}});
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(store->commits, commits);
  EXPECT_EQ(take(store->status(0)).revision, revision);
}

class EngineBudgetContract : public testing::TestWithParam<int> {
protected:
  void SetUp() noexcept override {
    storage.shard_count = 1;
    storage.owner = "escaped\n\"owner\\";
    storage.max_batch_bytes = 8192;
    storage.max_read_bytes = 8192;
    storage.max_mutations = 3;
    storage.max_scan_rows = 2;
    std::unique_ptr<IStateStore> backend;
    if (GetParam() % 2 == 0) {
      backend = take(MemoryStateStore::open(storage));
    } else {
      char pattern[] = "/tmp/eventflow-engine-budget-XXXXXX";
      const auto* created = ::mkdtemp(pattern);
      ASSERT_NE(created, nullptr);
      path = created;
      backend = take(RocksStateStore::open(path + "/db", storage));
    }
    if (GetParam() >= 2)
      backend = take(RemoteHarnessStore::open(std::move(backend)));
    store = std::make_shared<BudgetStore>(std::move(backend));
    requested.shard_count = 1;
    requested.owner = storage.owner;
  }

  void TearDown() noexcept override {
    store.reset();
    if (!path.empty()) {
      std::error_code ignored;
      std::filesystem::remove_all(path, ignored);
    }
  }

  std::string path;
  StoreConfig storage;
  EngineConfig requested;
  std::shared_ptr<BudgetStore> store;
};

TEST_P(EngineBudgetContract, ExactEffectiveRawBoundaryCommitsAndOneByteMoreNeverReachesStore) {
  auto catalog = take(Catalog::create());
  auto engine = take(Engine::open(store, take(catalog->snapshot()), requested));
  EXPECT_EQ(engine->config().max_mutations, 3U);
  EXPECT_EQ(engine->config().page_rows, 2U);
  const auto metadata = store->last_batch.mutations[0];
  const auto first = take(row_key(0, 'S', 12345, {std::int64_t{1}}));
  const auto second = take(row_key(0, 'S', 12345, {std::int64_t{2}}));
  const auto framing = metadata.key.size() + metadata.value->size() + first.size() + second.size();
  ASSERT_GT(engine->config().max_bytes, framing);
  const auto payload = std::string(engine->config().max_bytes - framing, 'x');
  auto write = [&](std::size_t extra) noexcept {
    return engine->apply_many({}, {}, [&](RoundContext& round, const auto&) noexcept -> Status {
      EF_RETURN_IF_ERROR(round.stage({first, payload + std::string(extra, 'x')}));
      return round.stage({second, std::string{}});
    });
  };
  EXPECT_EQ(take(write(0)).commit.code, CommitCode::Committed);
  auto worst = store->last_batch;
  worst.expected_revision = UINT64_MAX;
  worst.commit_id = take(make_commit_id(worst.owner, worst.shard, UINT64_MAX));
  EXPECT_EQ(take(canonical_batch(worst)).size(), storage.max_batch_bytes);
  const auto commits = store->commits;
  const auto revision = engine->revision();
  auto oversized = write(1);
  ASSERT_FALSE(oversized.ok());
  EXPECT_EQ(oversized.status().code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(store->commits, commits);
  EXPECT_EQ(take(store->status(0)).revision, revision);
}

TEST_P(EngineBudgetContract, ResolvingAnAlreadyEffectiveConfigIsIdempotentAndReadBounded) {
  auto first = take(resolve_engine_config(*store, requested));
  auto second = take(resolve_engine_config(*store, first));
  EXPECT_EQ(first.max_bytes, second.max_bytes);
  EXPECT_EQ(first.max_mutations, second.max_mutations);
  EXPECT_EQ(first.page_rows, second.page_rows);
  EXPECT_LE(first.max_bytes, take(store->limits()).max_read_bytes);
  EXPECT_EQ(store->reads, 0U);
  EXPECT_EQ(store->commits, 0U);
}

TEST_P(EngineBudgetContract, ImpossibleIdentityReservationRejectsBeforeInitialization) {
  requested.owner.assign(10000, '\n');
  auto catalog = take(Catalog::create());
  auto opened = Engine::open(store, take(catalog->snapshot()), requested);
  ASSERT_FALSE(opened.ok());
  EXPECT_EQ(opened.status().code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(store->reads, 0U);
  EXPECT_EQ(store->commits, 0U);
}

TEST(EngineLimits, HugeMutationRequestCannotOverflowReservation) {
  StoreConfig config;
  config.max_mutations = std::numeric_limits<std::size_t>::max();
  auto store = take(MemoryStateStore::open(config));
  EngineConfig requested;
  requested.max_mutations = std::numeric_limits<std::size_t>::max();
  auto effective = resolve_engine_config(*store, requested);
  ASSERT_FALSE(effective.ok());
  EXPECT_EQ(effective.status().code(), ErrorCode::ResourceExhausted);
}

INSTANTIATE_TEST_SUITE_P(FourStores, EngineBudgetContract, testing::Values(0, 1, 2, 3));
} // namespace
} // namespace eventflow::test
