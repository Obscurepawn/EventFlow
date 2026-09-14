#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <spawn.h>
#include <string>
#include <sys/resource.h>
#include <sys/wait.h>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

#include <rocksdb/db.h>

#include "eventflow/key_codec.hpp"
#include "eventflow/state_store.hpp"
#include "remote_harness.hpp"

namespace eventflow {
namespace {

template <class T> T take(StatusOr<T> result) noexcept {
  EXPECT_TRUE(result.ok()) << result.status().message();
  if (!result.ok())
    std::abort();
  return std::move(result).value();
}

template <class Function> void expect_status(Function&& function, ErrorCode code) noexcept {
  auto result = function();
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), code) << result.status().message();
}

class TemporaryDirectory {
public:
  TemporaryDirectory() noexcept {
    std::error_code error;
    auto root = std::filesystem::temp_directory_path(error);
    EXPECT_FALSE(error);
    if (error)
      std::abort();
    std::string pattern = (root / "eventflow-store-XXXXXX").string();
    std::vector<char> buffer(pattern.begin(), pattern.end());
    buffer.push_back('\0');
    const char* path = ::mkdtemp(buffer.data());
    EXPECT_NE(path, nullptr);
    if (!path)
      std::abort();
    path_ = path;
  }

  ~TemporaryDirectory() noexcept {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  const std::string& path() const noexcept {
    return path_;
  }

private:
  std::string path_;
};

class StoreContract : public testing::TestWithParam<int> {
protected:
  void SetUp() noexcept override {
    config_.shard_count = 2;
    store_ = take(create(config_));
  }

  StatusOr<std::unique_ptr<IStateStore>> create(const StoreConfig& config) noexcept {
    std::unique_ptr<IStateStore> backend;
    if (GetParam() % 2 == 1) {
      EF_ASSIGN_OR_RETURN(auto opened, RocksStateStore::open(directory_.path() + "/db", config));
      backend = std::move(opened);
    } else {
      EF_ASSIGN_OR_RETURN(auto opened, MemoryStateStore::open(config));
      backend = std::move(opened);
    }
    if (GetParam() >= 2) {
      EF_ASSIGN_OR_RETURN(auto remote, test::RemoteHarnessStore::open(std::move(backend)));
      return std::unique_ptr<IStateStore>(std::move(remote));
    }
    return backend;
  }

  void reset_with(StoreConfig config) noexcept {
    store_.reset();
    std::error_code error;
    std::filesystem::remove_all(directory_.path() + "/db", error);
    EXPECT_FALSE(error);
    if (error)
      std::abort();
    config_ = std::move(config);
    store_ = take(create(config_));
  }

  CommitBatch batch(std::uint64_t revision, std::vector<Mutation> mutations,
                    std::uint32_t shard = 0) const noexcept {
    const auto& owner = config_.shard_owners.empty() ? config_.owner : config_.shard_owners[shard];
    return {shard, owner, revision, take(make_commit_id(owner, shard, revision)),
            std::move(mutations)};
  }

  static std::string key(std::string suffix, std::uint32_t shard = 0) noexcept {
    return take(data_prefix(shard)) + suffix;
  }

  TemporaryDirectory directory_;
  StoreConfig config_;
  std::unique_ptr<IStateStore> store_;
};

static_assert(noexcept(MemoryStateStore::open(std::declval<const StoreConfig&>())));
static_assert(noexcept(RocksStateStore::open(std::declval<const std::string&>(),
                                             std::declval<const StoreConfig&>())));
static_assert(noexcept(
    std::declval<IStateStore&>().get_many(0, std::declval<const std::vector<std::string>&>())));
static_assert(noexcept(std::declval<IStateStore&>().scan(
    0, std::declval<const std::string&>(), std::declval<const std::optional<std::string>&>(), 1)));
static_assert(noexcept(std::declval<IStateStore&>().commit(std::declval<const CommitBatch&>())));
static_assert(noexcept(std::declval<IStateStore&>().status(0)));
static_assert(noexcept(canonical_batch(std::declval<const CommitBatch&>())));
static_assert(noexcept(make_commit_id(std::declval<const std::string&>(), 0, 0)));

TEST(StateStoreIdentity, UsesCanonicalOwnerShardRevisionArray) {
  EXPECT_EQ(take(make_commit_id("owner-7", 3, 42)), "[\"owner-7\",3,42]");
  EXPECT_EQ(take(make_commit_id("owner\"\\\n", 0, 0)), Json::array({"owner\"\\\n", 0, 0}).dump());
  CommitBatch a{0, "owner", 0, take(make_commit_id("owner", 0, 0)), {{"a", std::nullopt}}};
  auto b = a;
  b.mutations[0].value = "";
  EXPECT_NE(take(canonical_batch(a)), take(canonical_batch(b)));
  expect_status([]() noexcept { return make_commit_id("", 0, 0); }, ErrorCode::InvalidArgument);
  expect_status([]() noexcept { return make_commit_id(std::string("\xff", 1), 0, 0); },
                ErrorCode::InvalidArgument);
  b = a;
  b.mutations = {{"ab", "c"}};
  a.mutations = {{"a", "bc"}};
  EXPECT_NE(take(canonical_batch(a)), take(canonical_batch(b)));
}

TEST_P(StoreContract, BinaryEmptyMissingDeleteAndOrderedMutations) {
  const auto binary_key = key(std::string("a\0\xff", 3));
  const std::string binary_value("\0\xff\x80z", 4);
  EXPECT_EQ(take(store_->status(0)).revision, 0U);
  EXPECT_TRUE(take(store_->status(0)).last_commit_id.empty());
  auto first = batch(0, {{binary_key, binary_value},
                         {key("empty"), ""},
                         {key("repeat"), "first"},
                         {key("repeat"), std::nullopt},
                         {key("repeat"), "last"}});
  ASSERT_EQ(take(store_->commit(first)).code, CommitCode::Committed);
  auto values = take(
      store_->get_many(0, {binary_key, key("empty"), key("missing"), key("repeat"), binary_key}));
  ASSERT_EQ(values.values.size(), 5U);
  EXPECT_EQ(values.revision, 1U);
  EXPECT_EQ(values.values[0], binary_value);
  EXPECT_EQ(values.values[1], "");
  EXPECT_FALSE(values.values[2]);
  EXPECT_EQ(values.values[3], "last");
  EXPECT_EQ(values.values[4], binary_value);
  ASSERT_EQ(
      take(store_->commit(batch(1, {{binary_key, std::nullopt}, {key("missing"), std::nullopt}})))
          .code,
      CommitCode::Committed);
  EXPECT_FALSE(take(store_->get_many(0, {binary_key})).values.at(0));
  EXPECT_EQ(take(store_->status(0)).revision, 2U);
  EXPECT_EQ(take(store_->get_many(1, {})).revision, 0U);
}

TEST_P(StoreContract, EmptyBatchStillAdvancesAndDeduplicatesRevision) {
  auto empty = batch(0, {});
  auto result = take(store_->commit(empty));
  ASSERT_EQ(result.code, CommitCode::Committed);
  EXPECT_EQ(result.revision, 1U);
  EXPECT_FALSE(result.duplicate);
  EXPECT_EQ(take(store_->get_many(0, {})).revision, 1U);
  auto retry = take(store_->commit(empty));
  EXPECT_EQ(retry.code, CommitCode::Committed);
  EXPECT_TRUE(retry.duplicate);
  EXPECT_EQ(retry.revision, 1U);
}

TEST_P(StoreContract, ScanOrdersBinaryKeysAndUsesExclusiveCursor) {
  std::vector<std::string> keys = {
      key("p"), key(std::string("p\0", 2)), key("pa"), key("p\x7f"), key("p\x80"), key("p\xff")};
  std::vector<Mutation> mutations;
  for (auto it = keys.rbegin(); it != keys.rend(); ++it)
    mutations.push_back({*it, *it});
  mutations.push_back({key("q"), "outside"});
  ASSERT_EQ(take(store_->commit(batch(0, mutations))).code, CommitCode::Committed);
  std::vector<std::string> actual;
  std::optional<std::string> after;
  for (;;) {
    auto page = take(store_->scan(0, key("p"), after, 2));
    EXPECT_EQ(page.revision, 1U);
    if (page.rows.empty())
      break;
    ASSERT_LE(page.rows.size(), 2U);
    for (const auto& [name, value] : page.rows) {
      actual.push_back(name);
      EXPECT_EQ(value, name);
    }
    after = page.rows.back().first;
  }
  EXPECT_EQ(actual, keys);
  auto missing_cursor = take(store_->scan(0, key("p"), key("pb"), 10));
  ASSERT_EQ(missing_cursor.rows.size(), 3U);
  EXPECT_EQ(missing_cursor.rows.front().first, key("p\x7f"));
  EXPECT_TRUE(take(store_->scan(0, key("z"), std::nullopt, 2)).rows.empty());
}

TEST_P(StoreContract, RejectionCannotPublishAnyPartialMutation) {
  ASSERT_EQ(take(store_->commit(batch(0, {{key("keep"), "old"}}))).code, CommitCode::Committed);
  auto invalid = batch(1, {{key("keep"), "new"}, {key("new"), "new"}, {"Mrevision", "999"}});
  EXPECT_EQ(take(store_->commit(invalid)).code, CommitCode::Rejected);
  auto read = take(store_->get_many(0, {key("keep"), key("new")}));
  ASSERT_EQ(read.values.size(), 2U);
  EXPECT_EQ(read.revision, 1U);
  EXPECT_EQ(read.values[0], "old");
  EXPECT_FALSE(read.values[1]);
  EXPECT_EQ(take(store_->status(0)).last_commit_id, batch(0, {}).commit_id);
}

TEST_P(StoreContract, LastReceiptRequiresIdenticalPayloadAndAuthorizedOwner) {
  auto first = batch(0, {{key("a"), "one"}, {key("b"), "two"}});
  ASSERT_EQ(take(store_->commit(first)).code, CommitCode::Committed);
  auto duplicate = take(store_->commit(first));
  EXPECT_EQ(duplicate.code, CommitCode::Committed);
  EXPECT_TRUE(duplicate.duplicate);
  EXPECT_EQ(duplicate.revision, 1U);
  auto altered = first;
  altered.mutations[0].value = "evil";
  EXPECT_EQ(take(store_->commit(altered)).code, CommitCode::Rejected);
  altered = first;
  std::reverse(altered.mutations.begin(), altered.mutations.end());
  EXPECT_EQ(take(store_->commit(altered)).code, CommitCode::Rejected);
  altered = first;
  altered.owner = "unauthorized";
  EXPECT_EQ(take(store_->commit(altered)).code, CommitCode::Rejected);
  EXPECT_EQ(take(store_->status(0)).revision, 1U);
  EXPECT_EQ(take(store_->get_many(0, {key("a")})).values.at(0), "one");
}

TEST_P(StoreContract, HistoricalIdentityCannotBeReboundToNewRevision) {
  auto first = batch(0, {{key("a"), "first"}});
  ASSERT_EQ(take(store_->commit(first)).code, CommitCode::Committed);
  auto second = batch(1, {{key("a"), "second"}});
  ASSERT_EQ(take(store_->commit(second)).code, CommitCode::Committed);
  EXPECT_EQ(take(store_->commit(first)).code, CommitCode::Rejected);
  first.expected_revision = 2;
  EXPECT_EQ(take(store_->commit(first)).code, CommitCode::Rejected);
  auto arbitrary = batch(2, {{key("a"), "third"}});
  arbitrary.commit_id = "arbitrary-client-id";
  EXPECT_EQ(take(store_->commit(arbitrary)).code, CommitCode::Rejected);
  auto future = batch(3, {{key("a"), "future"}});
  EXPECT_EQ(take(store_->commit(future)).code, CommitCode::Rejected);
  EXPECT_EQ(take(store_->status(0)).revision, 2U);
  EXPECT_EQ(take(store_->status(0)).last_commit_id, second.commit_id);
  EXPECT_EQ(take(store_->get_many(0, {key("a")})).values.at(0), "second");
}

TEST_P(StoreContract, ShardOwnershipAndNamespacesAreValidated) {
  auto config = config_;
  config.shard_owners = {"assignment-a", "assignment-b"};
  reset_with(config);
  auto unauthorized = batch(0, {{key("a"), "a"}});
  unauthorized.owner = config.owner;
  unauthorized.commit_id = take(make_commit_id(unauthorized.owner, 0, 0));
  EXPECT_EQ(take(store_->commit(unauthorized)).code, CommitCode::Rejected);
  EXPECT_EQ(take(store_->commit(batch(0, {{key("a", 1), "cross-shard"}}))).code,
            CommitCode::Rejected);
  EXPECT_EQ(take(store_->commit(batch(0, {{key("a"), "a"}}))).code, CommitCode::Committed);
  EXPECT_EQ(take(store_->commit(batch(0, {{key("a", 1), "b"}}, 1))).code, CommitCode::Committed);
  expect_status([&]() noexcept { return store_->get_many(0, {key("a", 1)}); },
                ErrorCode::InvalidArgument);
  expect_status([&]() noexcept { return store_->get_many(0, {"Mrevision"}); },
                ErrorCode::InvalidArgument);
  expect_status([&]() noexcept { return store_->scan(0, "", std::nullopt, 1); },
                ErrorCode::InvalidArgument);
  expect_status([&]() noexcept { return store_->scan(0, take(data_prefix(1)), std::nullopt, 1); },
                ErrorCode::InvalidArgument);
  expect_status([&]() noexcept { return store_->scan(0, key("a"), key("b"), 1); },
                ErrorCode::InvalidArgument);
  expect_status([&]() noexcept { return store_->scan(0, key("a"), key("a", 1), 1); },
                ErrorCode::InvalidArgument);
  expect_status([&]() noexcept { return store_->status(2); }, ErrorCode::InvalidArgument);
  expect_status([&]() noexcept { return store_->get_many(2, {}); }, ErrorCode::InvalidArgument);
  expect_status([&]() noexcept { return store_->scan(2, take(data_prefix(2)), std::nullopt, 1); },
                ErrorCode::InvalidArgument);
  auto invalid_shard = batch(0, {});
  invalid_shard.shard = 2;
  expect_status([&]() noexcept { return store_->commit(invalid_shard); },
                ErrorCode::InvalidArgument);
}

TEST_P(StoreContract, CapacityLimitsAreExplicitAndRejectedBatchesDoNotAdvance) {
  auto config = config_;
  config.max_mutations = 2;
  config.max_batch_bytes = 256;
  config.max_read_keys = 2;
  config.max_scan_rows = 2;
  reset_with(config);
  EXPECT_EQ(
      take(store_->commit(batch(0, {{key("a"), "a"}, {key("b"), "b"}, {key("c"), "c"}}))).code,
      CommitCode::Rejected);
  EXPECT_EQ(take(store_->commit(batch(0, {{key("a"), std::string(300, 'x')}}))).code,
            CommitCode::Rejected);
  EXPECT_EQ(take(store_->status(0)).revision, 0U);
  EXPECT_EQ(take(store_->commit(batch(0, {{key("a"), "a"}, {key("b"), "b"}}))).code,
            CommitCode::Committed);
  expect_status([&]() noexcept { return store_->get_many(0, {key("a"), key("b"), key("c")}); },
                ErrorCode::ResourceExhausted);
  expect_status([&]() noexcept { return store_->scan(0, take(data_prefix(0)), std::nullopt, 3); },
                ErrorCode::ResourceExhausted);
  expect_status([&]() noexcept { return store_->scan(0, take(data_prefix(0)), std::nullopt, 0); },
                ErrorCode::InvalidArgument);
}

TEST_P(StoreContract, InvalidConfigurationIsRejected) {
  store_.reset();
  std::vector<StoreConfig> invalid;
  auto config = config_;
  config.shard_count = 0;
  invalid.push_back(config);
  config = config_;
  config.owner.clear();
  invalid.push_back(config);
  config = config_;
  config.owner = std::string("\xff", 1);
  invalid.push_back(config);
  config = config_;
  config.shard_owners = {"one"};
  invalid.push_back(config);
  config = config_;
  config.shard_owners = {"one", ""};
  invalid.push_back(config);
  config = config_;
  config.max_batch_bytes = 0;
  invalid.push_back(config);
  config = config_;
  config.max_mutations = 0;
  invalid.push_back(config);
  config = config_;
  config.max_read_keys = 0;
  invalid.push_back(config);
  config = config_;
  config.max_read_bytes = 0;
  invalid.push_back(config);
  config = config_;
  config.max_scan_rows = 0;
  invalid.push_back(config);
  for (const auto& bad : invalid)
    expect_status([&]() noexcept { return create(bad); }, ErrorCode::InvalidArgument);
}

TEST_P(StoreContract, CanonicalByteLimitAcceptsExactBoundary) {
  auto candidate = batch(0, {{key("a"), "value"}});
  auto config = config_;
  config.max_batch_bytes = take(canonical_batch(candidate)).size();
  reset_with(config);
  ASSERT_EQ(take(store_->commit(candidate)).code, CommitCode::Committed);
  auto oversized = batch(1, {{key("a"), "values"}});
  EXPECT_EQ(take(canonical_batch(oversized)).size(), config.max_batch_bytes + 1);
  EXPECT_EQ(take(store_->commit(oversized)).code, CommitCode::Rejected);
  EXPECT_EQ(take(store_->get_many(0, {key("a")})).values.at(0), "value");
  EXPECT_EQ(take(store_->status(0)).revision, 1U);
}

TEST_P(StoreContract, ReadsRejectByteOverflowIncludingRepeatedKeys) {
  auto config = config_;
  config.max_read_bytes = 2 * (key("a").size() + 10);
  reset_with(config);
  ASSERT_EQ(take(store_->commit(batch(0, {{key("a"), std::string(10, 'a')},
                                          {key("b"), std::string(10, 'b')},
                                          {key("c"), std::string(10, 'c')}})))
                .code,
            CommitCode::Committed);
  auto exact = take(store_->get_many(0, {key("a"), key("a")}));
  EXPECT_EQ(exact.values.size(), 2U);
  expect_status([&]() noexcept { return store_->get_many(0, {key("a"), key("a"), key("a")}); },
                ErrorCode::ResourceExhausted);
  expect_status(
      [&]() noexcept {
        return store_->get_many(0, {key("missing"), key("missing"), key("missing")});
      },
      ErrorCode::ResourceExhausted);
  auto page = take(store_->scan(0, take(data_prefix(0)), std::nullopt, 2));
  ASSERT_EQ(page.rows.size(), 2U);
  expect_status([&]() noexcept { return store_->scan(0, take(data_prefix(0)), std::nullopt, 3); },
                ErrorCode::ResourceExhausted);
  EXPECT_EQ(take(store_->scan(0, take(data_prefix(0)), page.rows.back().first, 2)).rows.size(), 1U);
  EXPECT_EQ(take(store_->status(0)).revision, 1U);
}

TEST_P(StoreContract, SingleOversizedValueAndReadKeyFailBeforeResponseConstruction) {
  auto config = config_;
  config.max_read_bytes = 16;
  reset_with(config);
  ASSERT_EQ(take(store_->commit(batch(0, {{key("a"), std::string(100, 'a')}}))).code,
            CommitCode::Committed);
  expect_status([&]() noexcept { return store_->get_many(0, {key("a")}); },
                ErrorCode::ResourceExhausted);
  expect_status([&]() noexcept { return store_->get_many(0, {key(std::string(100, 'b'))}); },
                ErrorCode::ResourceExhausted);
  expect_status([&]() noexcept { return store_->scan(0, take(data_prefix(0)), std::nullopt, 1); },
                ErrorCode::ResourceExhausted);
  EXPECT_EQ(take(store_->get_many(0, {key("no")})).values.size(), 1U);
}

TEST_P(StoreContract, ConcurrentSameRevisionAcceptsExactlyOneDistinctBatch) {
  constexpr std::size_t count = 8;
  std::barrier ready(static_cast<std::ptrdiff_t>(count));
  std::array<CommitResult, count> results;
  std::vector<std::thread> threads;
  for (std::size_t i = 0; i < count; ++i)
    threads.emplace_back([&, i]() noexcept {
      auto candidate = batch(0, {{key("winner"), std::to_string(i)}});
      ready.arrive_and_wait();
      results[i] = take(store_->commit(candidate));
    });
  for (auto& thread : threads)
    thread.join();
  std::size_t accepted = 0;
  for (std::size_t i = 0; i < count; ++i) {
    if (results[i].code == CommitCode::Committed) {
      ++accepted;
      EXPECT_FALSE(results[i].duplicate);
      EXPECT_EQ(take(store_->get_many(0, {key("winner")})).values.at(0), std::to_string(i));
    } else {
      EXPECT_EQ(results[i].code, CommitCode::Rejected);
    }
  }
  EXPECT_EQ(accepted, 1U);
  EXPECT_EQ(take(store_->status(0)).revision, 1U);
}

TEST_P(StoreContract, SingleReadNeverObservesMixedBatchState) {
  ASSERT_EQ(take(store_->commit(batch(0, {{key("left"), "1"}, {key("right"), "1"}}))).code,
            CommitCode::Committed);
  std::atomic<bool> done = false;
  std::atomic<bool> consistent = true;
  std::thread reader([&]() noexcept {
    do {
      auto read = take(store_->get_many(0, {key("left"), key("right")}));
      if (read.values.size() != 2 || read.values[0] != read.values[1] ||
          read.values[0] != std::to_string(read.revision))
        consistent = false;
      auto scan = take(store_->scan(0, take(data_prefix(0)), std::nullopt, 2));
      if (scan.rows.size() != 2 || scan.rows[0].second != scan.rows[1].second ||
          scan.rows[0].second != std::to_string(scan.revision))
        consistent = false;
    } while (!done.load());
  });
  for (std::uint64_t revision = 1; revision < 40; ++revision) {
    auto value = std::to_string(revision + 1);
    EXPECT_EQ(
        take(store_->commit(batch(revision, {{key("left"), value}, {key("right"), value}}))).code,
        CommitCode::Committed);
  }
  done = true;
  reader.join();
  EXPECT_TRUE(consistent.load());
}

std::string backend_name(const testing::TestParamInfo<int>& info) noexcept {
  return std::vector<std::string>{"Memory", "RocksDB", "RemoteMemory",
                                  "RemoteRocks"}[static_cast<std::size_t>(info.param)];
}

INSTANTIATE_TEST_SUITE_P(Adapters, StoreContract, testing::Values(0, 1, 2, 3), backend_name);

TEST(RocksStateStorePersistence, ReopenRestoresDataRevisionAndFullReceipt) {
  TemporaryDirectory directory;
  StoreConfig config;
  config.shard_count = 2;
  config.shard_owners = {"epoch-a", "epoch-b"};
  CommitBatch first{0,
                    "epoch-a",
                    0,
                    take(make_commit_id("epoch-a", 0, 0)),
                    {{take(data_prefix(0)) + "a", std::string("\0\xff", 2)}}};
  {
    auto store = take(RocksStateStore::open(directory.path() + "/db", config));
    ASSERT_EQ(take(store->commit(first)).code, CommitCode::Committed);
  }
  {
    auto store = take(RocksStateStore::open(directory.path() + "/db", config));
    EXPECT_EQ(take(store->status(0)).revision, 1U);
    EXPECT_EQ(take(store->status(0)).last_commit_id, first.commit_id);
    EXPECT_EQ(take(store->get_many(0, {take(data_prefix(0)) + "a"})).values.at(0),
              std::string("\0\xff", 2));
    auto retry = take(store->commit(first));
    EXPECT_EQ(retry.code, CommitCode::Committed);
    EXPECT_TRUE(retry.duplicate);
    EXPECT_EQ(retry.revision, 1U);
    first.mutations[0].value = "different";
    EXPECT_EQ(take(store->commit(first)).code, CommitCode::Rejected);
  }
  auto changed = config;
  changed.shard_owners[0] = "takeover";
  expect_status([&]() noexcept { return RocksStateStore::open(directory.path() + "/db", changed); },
                ErrorCode::FailedPrecondition);
  changed = config;
  changed.shard_count = 3;
  changed.shard_owners.push_back("epoch-c");
  expect_status([&]() noexcept { return RocksStateStore::open(directory.path() + "/db", changed); },
                ErrorCode::FailedPrecondition);
  auto original = take(RocksStateStore::open(directory.path() + "/db", config));
  EXPECT_EQ(take(original->status(0)).revision, 1U);
}

TEST(RocksStateStorePersistence, RetryingReceiptSurvivesReducedAdmissionLimits) {
  TemporaryDirectory directory;
  StoreConfig config;
  config.shard_count = 1;
  CommitBatch candidate{0,
                        config.owner,
                        0,
                        take(make_commit_id(config.owner, 0, 0)),
                        {{take(data_prefix(0)) + "a", "one"}, {take(data_prefix(0)) + "b", "two"}}};
  {
    auto store = take(RocksStateStore::open(directory.path() + "/db", config));
    ASSERT_EQ(take(store->commit(candidate)).code, CommitCode::Committed);
  }
  config.max_batch_bytes = 1;
  config.max_mutations = 1;
  auto store = take(RocksStateStore::open(directory.path() + "/db", config));
  auto retry = take(store->commit(candidate));
  EXPECT_EQ(retry.code, CommitCode::Committed);
  EXPECT_TRUE(retry.duplicate);
  EXPECT_EQ(retry.revision, 1U);
  candidate.mutations[0].value = "different";
  EXPECT_EQ(take(store->commit(candidate)).code, CommitCode::Rejected);
}

TEST(RocksStateStorePersistence, CorruptReceiptReturnsFailedPrecondition) {
  TemporaryDirectory directory;
  StoreConfig config;
  config.shard_count = 1;
  {
    auto store = take(RocksStateStore::open(directory.path() + "/db", config));
    CommitBatch batch{0, config.owner, 0, take(make_commit_id(config.owner, 0, 0)), {}};
    ASSERT_EQ(take(store->commit(batch)).code, CommitCode::Committed);
  }
  {
    rocksdb::DB* raw = nullptr;
    ASSERT_TRUE(rocksdb::DB::Open(rocksdb::Options{}, directory.path() + "/db", &raw).ok());
    std::unique_ptr<rocksdb::DB> db(raw);
    rocksdb::WriteOptions options;
    options.sync = true;
    ASSERT_TRUE(db->Put(options, std::string("M\0\0\0\0", 5), std::string("\xff", 1)).ok());
  }
  expect_status([&]() noexcept { return RocksStateStore::open(directory.path() + "/db", config); },
                ErrorCode::FailedPrecondition);
}

TEST(RocksStateStoreFailures, ActualWalWriteFailureRequiresReopen) {
  if (std::getenv("EVENTFLOW_TEST_STORE_IO_FAILURE_CHILD") == nullptr) {
    std::vector<std::string> environment;
    for (char** item = environ; *item; ++item)
      environment.emplace_back(*item);
    environment.emplace_back("EVENTFLOW_TEST_STORE_IO_FAILURE_CHILD=1");
    std::vector<char*> pointers;
    for (auto& item : environment)
      pointers.push_back(item.data());
    pointers.push_back(nullptr);
    std::string executable = "/proc/self/exe";
    std::string filter =
        "--gtest_filter=RocksStateStoreFailures.ActualWalWriteFailureRequiresReopen";
    std::array<char*, 3> arguments{executable.data(), filter.data(), nullptr};
    pid_t child = -1;
    ASSERT_EQ(posix_spawn(&child, executable.c_str(), nullptr, nullptr, arguments.data(),
                          pointers.data()),
              0);
    int status = 0;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
    return;
  }

  TemporaryDirectory directory;
  StoreConfig config;
  config.shard_count = 2;
  auto store = take(RocksStateStore::open(directory.path() + "/db", config));
  CommitBatch first{0,
                    config.owner,
                    0,
                    take(make_commit_id(config.owner, 0, 0)),
                    {{take(data_prefix(0)) + "a", "durable"}}};
  ASSERT_EQ(take(store->commit(first)).code, CommitCode::Committed);
  CommitBatch next{0,
                   config.owner,
                   1,
                   take(make_commit_id(config.owner, 0, 1)),
                   {{take(data_prefix(0)) + "a", std::string(1000, 'x')}}};

  struct rlimit original_limit{};
  ASSERT_EQ(getrlimit(RLIMIT_FSIZE, &original_limit), 0);
  struct sigaction original_signal{}, ignored_signal{};
  ignored_signal.sa_handler = SIG_IGN;
  ASSERT_EQ(sigemptyset(&ignored_signal.sa_mask), 0);
  ASSERT_EQ(sigaction(SIGXFSZ, &ignored_signal, &original_signal), 0);
  auto constrained = original_limit;
  constrained.rlim_cur = 0;
  ASSERT_EQ(setrlimit(RLIMIT_FSIZE, &constrained), 0);
  auto failed = store->commit(next);
  const int restore_limit = setrlimit(RLIMIT_FSIZE, &original_limit);
  const int restore_signal = sigaction(SIGXFSZ, &original_signal, nullptr);
  ASSERT_EQ(restore_limit, 0);
  ASSERT_EQ(restore_signal, 0);
  ASSERT_TRUE(failed.ok()) << failed.status().message();
  EXPECT_EQ(failed->code, CommitCode::Unknown);
  EXPECT_EQ(failed->revision, 1U);
  expect_status([&]() noexcept { return store->status(0); }, ErrorCode::UnknownCommit);
  expect_status([&]() noexcept { return store->status(1); }, ErrorCode::UnknownCommit);
  expect_status([&]() noexcept { return store->get_many(0, {take(data_prefix(0)) + "a"}); },
                ErrorCode::UnknownCommit);
  expect_status([&]() noexcept { return store->scan(1, take(data_prefix(1)), std::nullopt, 1); },
                ErrorCode::UnknownCommit);
  EXPECT_EQ(take(store->commit(next)).code, CommitCode::Unknown);
  CommitBatch other{1, config.owner, 0, take(make_commit_id(config.owner, 1, 0)), {}};
  EXPECT_EQ(take(store->commit(other)).code, CommitCode::Unknown);
  store.reset();
  store = take(RocksStateStore::open(directory.path() + "/db", config));
  EXPECT_GE(take(store->status(0)).revision, 1U);
  ASSERT_EQ(take(store->commit(next)).code, CommitCode::Committed);
  EXPECT_EQ(take(store->status(0)).revision, 2U);
  EXPECT_EQ(take(store->get_many(0, {take(data_prefix(0)) + "a"})).values.at(0),
            std::string(1000, 'x'));
}

} // namespace
} // namespace eventflow
