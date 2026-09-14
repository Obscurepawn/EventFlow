#include "eventflow/cached_store.hpp"
#include "eventflow/key_codec.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace eventflow {
namespace {

template <class T> T take(StatusOr<T> value) noexcept {
  EXPECT_TRUE(value.ok()) << value.status().message();
  if (!value.ok())
    std::abort();
  return std::move(value).value();
}

class CountingStore final : public IStateStore {
public:
  enum class CommitFault { None, UnknownBefore, UnknownAfter, UnavailableAfter, InvalidRevision };

  explicit CountingStore(std::shared_ptr<IStateStore> backend) noexcept
      : backend(std::move(backend)) {}

  StatusOr<ReadResult> get_many(std::uint32_t shard,
                                const std::vector<std::string>& keys) noexcept override {
    ++get_calls;
    last_get_keys = keys;
    if (before_get) {
      auto pending = std::move(*before_get);
      before_get.reset();
      auto committed = backend->commit(pending);
      if (!committed.ok())
        return committed.status();
      if (committed->code != CommitCode::Committed)
        return Status(ErrorCode::Internal, "test race write was rejected");
    }
    return backend->get_many(shard, keys);
  }

  StatusOr<ScanResult> scan(std::uint32_t shard, const std::string& prefix,
                            const std::optional<std::string>& after,
                            std::size_t limit) noexcept override {
    return backend->scan(shard, prefix, after, limit);
  }

  StatusOr<CommitResult> commit(const CommitBatch& batch) noexcept override {
    ++commit_calls;
    const auto fault = next_commit;
    next_commit = CommitFault::None;
    if (fault == CommitFault::UnknownBefore)
      return CommitResult{CommitCode::Unknown, batch.expected_revision, false, "injected unknown"};
    auto result = backend->commit(batch);
    if (!result.ok())
      return result.status();
    if (fault == CommitFault::UnknownAfter)
      return CommitResult{CommitCode::Unknown, batch.expected_revision, false, "lost receipt"};
    if (fault == CommitFault::UnavailableAfter)
      return Status(ErrorCode::Unavailable, "lost response");
    if (fault == CommitFault::InvalidRevision)
      ++result->revision;
    return std::move(result).value();
  }

  StatusOr<StoreLimits> limits() noexcept override {
    return backend->limits();
  }

  StatusOr<ShardStatus> status(std::uint32_t shard) noexcept override {
    ++status_calls;
    if (fail_status)
      return Status(ErrorCode::Unavailable, "status unavailable");
    return backend->status(shard);
  }

  StatusOr<StoreSnapshot> create_snapshot(const std::string& id) noexcept override {
    return backend->create_snapshot(id);
  }

  StatusOr<StoreSnapshot> snapshot_status(const std::string& id) noexcept override {
    return backend->snapshot_status(id);
  }

  std::shared_ptr<IStateStore> backend;
  std::size_t get_calls = 0;
  std::size_t status_calls = 0;
  std::size_t commit_calls = 0;
  bool fail_status = false;
  CommitFault next_commit = CommitFault::None;
  std::optional<CommitBatch> before_get;
  std::vector<std::string> last_get_keys;
};

class CachedStoreTest : public testing::TestWithParam<bool> {
protected:
  void SetUp() noexcept override {
    char pattern[] = "/tmp/eventflow-cache-backend-XXXXXX";
    const auto* path = ::mkdtemp(pattern);
    ASSERT_NE(path, nullptr);
    directory = path;
    store_config.shard_count = 2;
    reopen();
  }

  void TearDown() noexcept override {
    cache.reset();
    backend.reset();
    std::error_code error;
    std::filesystem::remove_all(directory, error);
    EXPECT_FALSE(error);
  }

  void reopen(const CacheConfig& config = {}) noexcept {
    cache.reset();
    backend.reset();
    std::shared_ptr<IStateStore> real;
    if (GetParam())
      real = take(RocksStateStore::open(directory + "/db-" + std::to_string(next_database++),
                                        store_config));
    else
      real = take(MemoryStateStore::open(store_config));
    backend = std::make_shared<CountingStore>(std::move(real));
    cache = take(CachedStateStore::open(backend, config));
  }

  static std::string key(const std::string& suffix, std::uint32_t shard = 0) noexcept {
    return take(data_prefix(shard)) + suffix;
  }

  CommitBatch batch(std::uint64_t revision, std::vector<Mutation> mutations,
                    std::uint32_t shard = 0) const noexcept {
    return {shard, store_config.owner, revision,
            take(make_commit_id(store_config.owner, shard, revision)), std::move(mutations)};
  }

  StoreConfig store_config;
  std::shared_ptr<CountingStore> backend;
  std::unique_ptr<CachedStateStore> cache;
  std::string directory;
  std::size_t next_database = 0;
};

TEST_P(CachedStoreTest, StableHitValidatesRevisionWithoutReadingValuesAgain) {
  ASSERT_EQ(take(backend->commit(batch(0, {{key("a"), "one"}}))).code, CommitCode::Committed);
  auto first = take(cache->get_many(0, {key("a")}));
  auto second = take(cache->get_many(0, {key("a")}));
  EXPECT_EQ(first.values, second.values);
  EXPECT_EQ(second.revision, 1U);
  EXPECT_EQ(backend->get_calls, 1U);
  EXPECT_EQ(backend->status_calls, 2U);
  EXPECT_EQ(cache->metrics().hits, 1U);
}

TEST_P(CachedStoreTest, ExternalWriteInvalidatesConfirmedPositiveAndNegativeEntries) {
  take(backend->commit(batch(0, {{key("a"), "old"}})));
  take(cache->get_many(0, {key("a"), key("missing")}));
  take(backend->commit(batch(1, {{key("a"), "new"}, {key("missing"), "appeared"}})));
  const auto read = take(cache->get_many(0, {key("a"), key("missing")}));
  EXPECT_EQ(read.revision, 2U);
  EXPECT_EQ(read.values, (std::vector<std::optional<std::string>>{"new", "appeared"}));
  EXPECT_EQ(backend->get_calls, 2U);
  EXPECT_GE(cache->metrics().invalidations, 2U);
}

TEST_P(CachedStoreTest, NegativeEmptyBinaryAndDuplicateKeysRemainDistinct) {
  const std::string binary("a\0b", 3);
  take(backend->commit(batch(0, {{key("empty"), ""}, {key("binary"), binary}})));
  const std::vector<std::string> keys{key("missing"), key("empty"), key("binary"), key("missing")};
  const auto first = take(cache->get_many(0, keys));
  const auto second = take(cache->get_many(0, keys));
  ASSERT_EQ(second.values.size(), 4U);
  EXPECT_FALSE(second.values[0]);
  EXPECT_EQ(second.values[1], "");
  EXPECT_EQ(second.values[2], binary);
  EXPECT_FALSE(second.values[3]);
  EXPECT_EQ(first.values, second.values);
  EXPECT_EQ(backend->get_calls, 1U);
  EXPECT_EQ(cache->metrics().entries, 3U);
  EXPECT_EQ(cache->metrics().hits, 4U);
}

TEST_P(CachedStoreTest, PartialMissRefetchesEveryKeyAfterRevisionRace) {
  take(backend->commit(batch(0, {{key("a"), "old-a"}, {key("b"), "old-b"}})));
  take(cache->get_many(0, {key("a")}));
  backend->before_get = batch(1, {{key("a"), "new-a"}, {key("b"), "new-b"}});
  const auto read = take(cache->get_many(0, {key("a"), key("b")}));
  EXPECT_EQ(read.revision, 2U);
  EXPECT_EQ(read.values, (std::vector<std::optional<std::string>>{"new-a", "new-b"}));
  EXPECT_EQ(backend->last_get_keys, (std::vector<std::string>{key("a"), key("b")}));
  EXPECT_EQ(take(cache->get_many(0, {key("a"), key("b")})).values, read.values);
  EXPECT_EQ(backend->get_calls, 2U);
}

TEST_P(CachedStoreTest, UnknownAfterApplyBlocksReadsUntilExactCommittedRetry) {
  take(backend->commit(batch(0, {{key("a"), "old"}})));
  take(cache->get_many(0, {key("a")}));
  const auto update = batch(1, {{key("a"), "new"}});
  backend->next_commit = CountingStore::CommitFault::UnknownAfter;
  EXPECT_EQ(take(cache->commit(update)).code, CommitCode::Unknown);
  EXPECT_TRUE(cache->has_pending_commit());
  EXPECT_EQ(cache->metrics().entries, 0U);
  EXPECT_GT(cache->metrics().pending_bytes, 0U);
  EXPECT_EQ(cache->get_many(0, {key("a")}).status().code(), ErrorCode::UnknownCommit);
  EXPECT_EQ(cache->scan(0, key(""), {}, 8).status().code(), ErrorCode::UnknownCommit);
  EXPECT_EQ(cache->create_snapshot("uncertain").status().code(), ErrorCode::UnknownCommit);
  EXPECT_EQ(take(cache->status(0)).revision, 2U);
  cache->invalidate();
  EXPECT_TRUE(cache->has_pending_commit());
  EXPECT_EQ(cache->set_owner_epoch(2).code(), ErrorCode::UnknownCommit);
  auto other = update;
  other.mutations[0].value = "different";
  const auto calls = backend->commit_calls;
  EXPECT_EQ(cache->commit(other).status().code(), ErrorCode::UnknownCommit);
  EXPECT_EQ(backend->commit_calls, calls);
  EXPECT_EQ(backend->get_calls, 1U);
  const auto retried = take(cache->commit(update));
  EXPECT_EQ(retried.code, CommitCode::Committed);
  EXPECT_TRUE(retried.duplicate);
  EXPECT_FALSE(cache->has_pending_commit());
  EXPECT_EQ(cache->metrics().pending_bytes, 0U);
  EXPECT_EQ(take(cache->get_many(0, {key("a")})).values[0], "new");
}

TEST_P(CachedStoreTest, UnknownBeforeApplyRetriesTheSameContent) {
  const auto update = batch(0, {{key("a"), "new"}});
  backend->next_commit = CountingStore::CommitFault::UnknownBefore;
  EXPECT_EQ(take(cache->commit(update)).code, CommitCode::Unknown);
  EXPECT_EQ(cache->get_many(0, {key("a")}).status().code(), ErrorCode::UnknownCommit);
  const auto result = take(cache->commit(update));
  EXPECT_EQ(result.code, CommitCode::Committed);
  EXPECT_FALSE(result.duplicate);
  EXPECT_FALSE(cache->has_pending_commit());
  EXPECT_EQ(take(cache->get_many(0, {key("a")})).values[0], "new");
}

TEST_P(CachedStoreTest, UnavailableAfterCommitRetainsRetryIdentity) {
  const auto update = batch(0, {{key("a"), "new"}});
  backend->next_commit = CountingStore::CommitFault::UnavailableAfter;
  EXPECT_EQ(cache->commit(update).status().code(), ErrorCode::Unavailable);
  EXPECT_TRUE(cache->has_pending_commit());
  EXPECT_EQ(cache->get_many(0, {key("a")}).status().code(), ErrorCode::UnknownCommit);
  EXPECT_TRUE(take(cache->commit(update)).duplicate);
  EXPECT_FALSE(cache->has_pending_commit());
}

TEST_P(CachedStoreTest, InvalidSuccessRevisionIsTreatedAsUnknown) {
  const auto update = batch(0, {{key("a"), "new"}});
  backend->next_commit = CountingStore::CommitFault::InvalidRevision;
  EXPECT_EQ(take(cache->commit(update)).code, CommitCode::Unknown);
  EXPECT_TRUE(cache->has_pending_commit());
  EXPECT_EQ(take(cache->commit(update)).revision, 1U);
  EXPECT_FALSE(cache->has_pending_commit());
}

TEST_P(CachedStoreTest, RejectedWritePreservesCacheAndCommittedDeleteWritesThrough) {
  take(backend->commit(batch(0, {{key("a"), "old"}})));
  take(cache->get_many(0, {key("a")}));
  auto rejected = batch(1, {{key("a"), "bad"}});
  rejected.owner = "unauthorized";
  rejected.commit_id = take(make_commit_id(rejected.owner, 0, 1));
  EXPECT_EQ(take(cache->commit(rejected)).code, CommitCode::Rejected);
  EXPECT_EQ(take(cache->get_many(0, {key("a")})).values[0], "old");
  EXPECT_EQ(backend->get_calls, 1U);
  EXPECT_EQ(take(cache->commit(batch(1, {{key("a"), std::nullopt}}))).code, CommitCode::Committed);
  EXPECT_EQ(cache->metrics().entries, 1U);
  EXPECT_FALSE(take(cache->get_many(0, {key("a")})).values[0]);
  EXPECT_EQ(backend->get_calls, 1U);
}

TEST_P(CachedStoreTest, HotUntouchedKeySurvivesCommitsAcrossRounds) {
  take(backend->commit(batch(0, {{key("a"), "hot"}})));
  take(cache->get_many(0, {key("a")}));
  for (std::uint64_t revision = 1; revision <= 4; ++revision) {
    const auto committed = take(cache->commit(batch(revision, {{key("b"), "changed"}})));
    ASSERT_EQ(committed.code, CommitCode::Committed);
    EXPECT_EQ(cache->metrics().entries, 1U);
    const auto read = take(cache->get_many(0, {key("a")}));
    EXPECT_EQ(read.revision, revision + 1);
    EXPECT_EQ(read.values[0], "hot");
    EXPECT_EQ(backend->get_calls, 1U);
  }
  EXPECT_EQ(cache->metrics().hits, 4U);
  EXPECT_EQ(cache->metrics().invalidations, 0U);
}

TEST_P(CachedStoreTest, ResidentUpdatesAndTombstonesReuseTheOriginalReadAdmission) {
  take(backend->commit(batch(0, {{key("a"), "first"}})));
  take(cache->get_many(0, {key("a"), key("missing")}));
  take(cache->commit(batch(1, {{key("a"), "next"}, {key("missing"), ""}})));
  auto read = take(cache->get_many(0, {key("a"), key("missing")}));
  EXPECT_EQ(read.revision, 2U);
  EXPECT_EQ(read.values, (std::vector<std::optional<std::string>>{"next", ""}));
  take(cache->commit(batch(2, {{key("a"), std::nullopt}, {key("missing"), std::nullopt}})));
  read = take(cache->get_many(0, {key("a"), key("missing")}));
  EXPECT_FALSE(read.values[0]);
  EXPECT_FALSE(read.values[1]);
  take(cache->commit(batch(3, {{key("a"), "again"}})));
  read = take(cache->get_many(0, {key("a"), key("missing")}));
  EXPECT_EQ(read.values[0], "again");
  EXPECT_FALSE(read.values[1]);
  EXPECT_EQ(read.revision, 4U);
  EXPECT_EQ(cache->metrics().entries, 2U);
  EXPECT_EQ(backend->get_calls, 1U);
}

TEST_P(CachedStoreTest, GrowthBeyondReadAdmissionEvictsOnlyTheTouchedKey) {
  store_config.max_read_bytes = key("a").size() + 4;
  reopen();
  take(backend->commit(batch(0, {{key("a"), "x"}, {key("b"), "y"}})));
  take(cache->get_many(0, {key("a")}));
  take(cache->get_many(0, {key("b")}));
  ASSERT_EQ(take(cache->commit(batch(1, {{key("a"), std::string(8, 'a')}}))).code,
            CommitCode::Committed);
  EXPECT_EQ(cache->metrics().entries, 1U);
  EXPECT_EQ(take(cache->get_many(0, {key("b")})).values[0], "y");
  EXPECT_EQ(backend->get_calls, 2U);
  EXPECT_EQ(cache->get_many(0, {key("a")}).status().code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(backend->get_calls, 3U);
  EXPECT_EQ(cache->metrics().entries, 1U);
}

TEST_P(CachedStoreTest, AdmittedGrowthDoesNotExpandCombinedReadByteProof) {
  store_config.max_read_bytes = key("a").size() + key("b").size() + 6;
  reopen();
  take(backend->commit(batch(0, {{key("a"), "a"}, {key("b"), "bbbbb"}})));
  take(cache->get_many(0, {key("a"), key("b")}));
  take(cache->commit(batch(1, {{key("a"), "aaaaaa"}})));
  EXPECT_EQ(cache->metrics().entries, 2U);
  EXPECT_EQ(take(cache->get_many(0, {key("a")})).values[0], "aaaaaa");
  EXPECT_EQ(backend->get_calls, 1U);
  EXPECT_EQ(cache->get_many(0, {key("a"), key("b")}).status().code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(backend->get_calls, 2U);
}

TEST_P(CachedStoreTest, GrowthBeyondCacheBudgetDoesNotEvictUntouchedEntries) {
  take(cache->get_many(0, {key("a"), key("b")}));
  CacheConfig config;
  config.max_bytes = cache->metrics().bytes;
  reopen(config);
  take(cache->get_many(0, std::vector<std::string>(8, key("a"))));
  take(cache->get_many(0, {key("b")}));
  ASSERT_EQ(cache->metrics().bytes, config.max_bytes);
  take(cache->commit(batch(0, {{key("a"), "x"}})));
  EXPECT_EQ(cache->metrics().entries, 1U);
  EXPECT_EQ(cache->metrics().evictions, 1U);
  EXPECT_LT(cache->metrics().bytes, config.max_bytes);
  EXPECT_FALSE(take(cache->get_many(0, {key("b")})).values[0]);
  EXPECT_EQ(backend->get_calls, 2U);
}

TEST_P(CachedStoreTest, CommitCannotPromoteEntriesFromBeforeAnExternalRevision) {
  take(backend->commit(batch(0, {{key("a"), "old"}})));
  take(cache->get_many(0, {key("a")}));
  take(cache->get_many(1, {key("other", 1)}));
  take(backend->commit(batch(1, {{key("a"), "new"}})));
  take(cache->commit(batch(2, {{key("b"), "written"}})));
  EXPECT_EQ(cache->metrics().entries, 1U);
  EXPECT_FALSE(take(cache->get_many(1, {key("other", 1)})).values[0]);
  EXPECT_EQ(backend->get_calls, 2U);
  const auto read = take(cache->get_many(0, {key("a")}));
  EXPECT_EQ(read.revision, 3U);
  EXPECT_EQ(read.values[0], "new");
  EXPECT_EQ(backend->get_calls, 3U);
}

TEST_P(CachedStoreTest, DuplicateReceiptAdvancesOnlyExpectedRevisionEntries) {
  take(backend->commit(batch(0, {{key("a"), "old-a"}, {key("b"), "old-b"}})));
  take(cache->get_many(0, {key("a"), key("b")}));
  const auto update = batch(1, {{key("b"), "new-b"}});
  take(backend->commit(update));
  EXPECT_TRUE(take(cache->commit(update)).duplicate);
  EXPECT_EQ(cache->metrics().entries, 2U);
  auto read = take(cache->get_many(0, {key("a"), key("b")}));
  EXPECT_EQ(read.revision, 2U);
  EXPECT_EQ(read.values, (std::vector<std::optional<std::string>>{"old-a", "new-b"}));
  EXPECT_EQ(backend->get_calls, 1U);
  EXPECT_TRUE(take(cache->commit(update)).duplicate);
  EXPECT_EQ(cache->metrics().entries, 0U);
  read = take(cache->get_many(0, {key("b")}));
  EXPECT_EQ(read.values[0], "new-b");
  EXPECT_EQ(backend->get_calls, 2U);
}

TEST_P(CachedStoreTest, RepeatedMutationsWriteThroughOnlyTheFinalCommittedValue) {
  take(cache->get_many(0, {key("a")}));
  const auto charge = cache->metrics().bytes;
  auto committed = take(cache->commit(
      batch(0, {{key("a"), std::string(100, 'x')}, {key("a"), std::nullopt}, {key("a"), ""}})));
  ASSERT_EQ(committed.code, CommitCode::Committed);
  EXPECT_EQ(cache->metrics().entries, 1U);
  EXPECT_EQ(cache->metrics().bytes, charge);
  EXPECT_EQ(take(cache->get_many(0, {key("a")})).values[0], "");
  committed = take(cache->commit(batch(1, {{key("a"), "ignored"}, {key("a"), std::nullopt}})));
  ASSERT_EQ(committed.code, CommitCode::Committed);
  EXPECT_FALSE(take(cache->get_many(0, {key("a")})).values[0]);
  EXPECT_EQ(cache->metrics().bytes, charge);
  EXPECT_EQ(backend->get_calls, 1U);
}

TEST_P(CachedStoreTest, StatusFailureCannotServeAnUnvalidatedHit) {
  take(cache->get_many(0, {key("a")}));
  backend->fail_status = true;
  EXPECT_EQ(cache->get_many(0, {key("a")}).status().code(), ErrorCode::Unavailable);
  EXPECT_EQ(backend->get_calls, 1U);
  backend->fail_status = false;
  EXPECT_FALSE(take(cache->get_many(0, {key("a")})).values[0]);
  EXPECT_EQ(backend->get_calls, 1U);
}

TEST_P(CachedStoreTest, EntryBudgetAndLruEvictionAreSharedAcrossShards) {
  CacheConfig config;
  config.max_entries = 2;
  reopen(config);
  take(cache->get_many(0, {key("a")}));
  take(cache->get_many(0, {key("b")}));
  take(cache->get_many(0, {key("a")}));
  take(cache->get_many(1, {key("c", 1)}));
  EXPECT_EQ(cache->metrics().entries, 2U);
  EXPECT_EQ(cache->metrics().evictions, 1U);
  const auto calls = backend->get_calls;
  take(cache->get_many(0, {key("a")}));
  EXPECT_EQ(backend->get_calls, calls);
  take(cache->get_many(0, {key("b")}));
  EXPECT_EQ(backend->get_calls, calls + 1);
  EXPECT_EQ(cache->metrics().entries, 2U);
}

TEST_P(CachedStoreTest, ByteBudgetChargesNegativeEntriesAndBypassesOversizedEntries) {
  take(cache->get_many(0, {key("a")}));
  const auto entry_bytes = cache->metrics().bytes;
  EXPECT_GT(entry_bytes, key("a").size());
  CacheConfig config;
  config.max_bytes = entry_bytes;
  reopen(config);
  take(cache->get_many(0, {key("a")}));
  take(cache->get_many(0, {key("b")}));
  EXPECT_EQ(cache->metrics().entries, 1U);
  EXPECT_LE(cache->metrics().bytes, config.max_bytes);
  EXPECT_EQ(cache->metrics().evictions, 1U);
  config.max_bytes = 1;
  reopen(config);
  take(cache->get_many(0, {key("a")}));
  take(cache->get_many(0, {key("a")}));
  EXPECT_EQ(cache->metrics().entries, 0U);
  EXPECT_EQ(cache->metrics().bytes, 0U);
  EXPECT_EQ(backend->get_calls, 2U);
}

TEST_P(CachedStoreTest, OwnerEpochAndExplicitInvalidationClearResidentValues) {
  take(cache->get_many(0, {key("a")}));
  ASSERT_TRUE(cache->set_owner_epoch(9).ok());
  EXPECT_EQ(cache->metrics().entries, 0U);
  EXPECT_EQ(cache->metrics().owner_epoch, 9U);
  take(cache->get_many(0, {key("a")}));
  EXPECT_EQ(backend->get_calls, 2U);
  ASSERT_TRUE(cache->set_owner_epoch(9).ok());
  EXPECT_EQ(cache->metrics().entries, 1U);
  cache->invalidate();
  EXPECT_EQ(cache->metrics().bytes, 0U);
}

TEST_P(CachedStoreTest, CachedKeysCannotBypassBackendRequestCardinalityLimit) {
  store_config.max_read_keys = 1;
  reopen();
  take(cache->get_many(0, {key("a")}));
  take(cache->get_many(0, {key("b")}));
  const auto result = cache->get_many(0, {key("a"), key("b")});
  EXPECT_EQ(result.status().code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(backend->get_calls, 3U);
}

TEST_P(CachedStoreTest, CachedKeysCannotBypassBackendResponseByteLimit) {
  store_config.max_read_bytes = key("a").size() * 2 + 16;
  reopen();
  take(backend->commit(
      batch(0, {{key("a"), std::string(16, 'a')}, {key("b"), std::string(16, 'b')}})));
  take(cache->get_many(0, {key("x"), key("y")}));
  take(cache->get_many(0, {key("a")}));
  take(cache->get_many(0, {key("b")}));
  EXPECT_EQ(cache->get_many(0, {key("a"), key("b")}).status().code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(backend->get_calls, 4U);
}

TEST_P(CachedStoreTest, PendingIdentityIsBoundedBeforeSubmittingToBackend) {
  CacheConfig config;
  config.max_pending_bytes = 1;
  reopen(config);
  EXPECT_EQ(cache->commit(batch(0, {{key("a"), "new"}})).status().code(),
            ErrorCode::ResourceExhausted);
  EXPECT_EQ(backend->commit_calls, 0U);
  EXPECT_FALSE(cache->has_pending_commit());
}

TEST_P(CachedStoreTest, ZeroEntryBudgetDisablesCachingAndSnapshotMethodsPassThrough) {
  CacheConfig config;
  config.max_entries = 0;
  reopen(config);
  take(cache->get_many(0, {key("a")}));
  take(cache->get_many(0, {key("a")}));
  EXPECT_EQ(backend->get_calls, 2U);
  EXPECT_EQ(cache->metrics().entries, 0U);
  const auto created = take(cache->create_snapshot("cache-snapshot"));
  const auto found = take(cache->snapshot_status("cache-snapshot"));
  EXPECT_EQ(created.snapshot_id, found.snapshot_id);
  EXPECT_EQ(created.shards[0].revision, found.shards[0].revision);
}

TEST(CachedStoreConstruction, RejectsMissingBackendAndUnboundedRetryIdentity) {
  EXPECT_EQ(CachedStateStore::open(nullptr).status().code(), ErrorCode::InvalidArgument);
  std::shared_ptr<IStateStore> memory = take(MemoryStateStore::open());
  CacheConfig config;
  config.max_pending_bytes = 0;
  EXPECT_EQ(CachedStateStore::open(memory, config).status().code(), ErrorCode::InvalidArgument);
}

INSTANTIATE_TEST_SUITE_P(MemoryAndRocks, CachedStoreTest, testing::Values(false, true));

} // namespace
} // namespace eventflow
