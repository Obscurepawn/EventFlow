#include "eventflow/key_codec.hpp"
#include "eventflow/kv_protocol.hpp"
#include "eventflow/kv_server.hpp"
#include "eventflow/state_store.hpp"
#include "status_test_support.hpp"
#include <atomic>
#include <cerrno>
#include <filesystem>
#include <gtest/gtest.h>
#include <httplib.h>
#include <thread>
#include <unistd.h>

namespace {
std::string fsync_failure_path;
std::atomic<int> fsync_failures_left = 0;
std::atomic<int> snapshot_root_syncs = 0;
} // namespace

extern "C" int __real_fsync(int descriptor) noexcept;

extern "C" int __wrap_fsync(int descriptor) noexcept {
  char path[4096];
  const auto link = "/proc/self/fd/" + std::to_string(descriptor);
  const auto length = ::readlink(link.c_str(), path, sizeof(path));
  if (length >= 0 &&
      std::string_view(path, static_cast<std::size_t>(length)) == fsync_failure_path) {
    ++snapshot_root_syncs;
    if (fsync_failures_left.load() > 0) {
      --fsync_failures_left;
      errno = EIO;
      return -1;
    }
  }
  return __real_fsync(descriptor);
}

namespace eventflow {
namespace {
using test::take;

struct SnapshotDirectory {
  SnapshotDirectory() noexcept {
    path = "/tmp/eventflow-snapshot-XXXXXX";
    if (!::mkdtemp(path.data()))
      std::abort();
  }

  ~SnapshotDirectory() noexcept {
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
  }

  std::string path;
};

class StoreSnapshotContract : public testing::TestWithParam<int> {
protected:
  void SetUp() noexcept override {
    config.shard_count = 2;
    config.shard_owners = {"owner-a", "owner-b"};
    config.max_snapshot_count = 2;
    if (GetParam() % 2 == 0)
      backend = take(MemoryStateStore::open(config));
    else
      backend = take(RocksStateStore::open(directory.path + "/db", config));
    if (GetParam() >= 2) {
      server = take(KvServer::create(backend));
      const auto port = take(server->start("127.0.0.1", 0));
      client = take(RemoteStateStore::open("http://127.0.0.1:" + std::to_string(port)));
      store = client.get();
    } else {
      store = backend.get();
    }
  }

  CommitBatch batch(std::uint32_t shard, std::uint64_t revision,
                    std::vector<Mutation> mutations) noexcept {
    const auto& owner = config.shard_owners[shard];
    return {shard, owner, revision, take(make_commit_id(owner, shard, revision)),
            std::move(mutations)};
  }

  std::string key(std::uint32_t shard, const std::string& suffix) noexcept {
    return take(data_prefix(shard)) + suffix;
  }

  StatusOr<std::unique_ptr<IStateStore>> restore(const std::string& id,
                                                 const std::string& destination) noexcept {
    if (GetParam() % 2 == 0) {
      EF_ASSIGN_OR_RETURN(auto result, MemoryStateStore::from_snapshot(
                                           *static_cast<MemoryStateStore*>(backend.get()), id));
      return std::unique_ptr<IStateStore>(std::move(result));
    }
    EF_ASSIGN_OR_RETURN(auto result, RocksStateStore::from_snapshot(directory.path + "/db", id,
                                                                    destination, config));
    return std::unique_ptr<IStateStore>(std::move(result));
  }

  SnapshotDirectory directory;
  StoreConfig config;
  std::shared_ptr<IStateStore> backend;
  std::unique_ptr<KvServer> server;
  std::unique_ptr<RemoteStateStore> client;
  IStateStore* store = nullptr;
};

TEST_P(StoreSnapshotContract, ImmutableRestoreIncludesRowsReceiptAndAssignment) {
  const std::string binary("\0\xffz", 3);
  const auto first =
      batch(0, 0, {{key(0, "a"), binary}, {key(0, "empty"), ""}, {key(0, "deleted"), "old"}});
  ASSERT_EQ(take(store->commit(first)).code, CommitCode::Committed);
  const auto second = batch(0, 1, {{key(0, "deleted"), std::nullopt}});
  ASSERT_EQ(take(store->commit(second)).code, CommitCode::Committed);
  const auto other = batch(1, 0, {{key(1, "b"), "shard-one"}});
  ASSERT_EQ(take(store->commit(other)).code, CommitCode::Committed);
  auto created = store->create_snapshot("stable-1");
  ASSERT_TRUE(created.ok()) << created.status().message();
  ASSERT_EQ(created->shards.size(), 2U);
  EXPECT_EQ(created->shards[0].revision, 2U);
  EXPECT_EQ(created->shards[1].revision, 1U);
  EXPECT_EQ(created->shard_owners, config.shard_owners);
  ASSERT_EQ(take(store->commit(batch(0, 2, {{key(0, "a"), "later"}}))).code, CommitCode::Committed);
  EXPECT_EQ(take(store->create_snapshot("stable-1")).shards[0].revision, 2U);
  EXPECT_EQ(take(store->snapshot_status("stable-1")).shards[0].last_commit_id, second.commit_id);
  auto opened = restore("stable-1", directory.path + "/restored");
  ASSERT_TRUE(opened.ok()) << opened.status().message();
  auto restored = std::move(opened).value();
  auto read = take(restored->get_many(0, {key(0, "a"), key(0, "empty"), key(0, "deleted")}));
  EXPECT_EQ(read.revision, 2U);
  EXPECT_EQ(read.values[0], binary);
  EXPECT_EQ(read.values[1], "");
  EXPECT_FALSE(read.values[2]);
  EXPECT_EQ(take(restored->get_many(1, {key(1, "b")})).values[0], "shard-one");
  EXPECT_TRUE(take(restored->commit(second)).duplicate);
  EXPECT_TRUE(take(restored->commit(other)).duplicate);
  auto wrong_owner = batch(1, 1, {});
  wrong_owner.owner = "other-owner";
  wrong_owner.commit_id = take(make_commit_id(wrong_owner.owner, 1, 1));
  EXPECT_EQ(take(restored->commit(wrong_owner)).code, CommitCode::Rejected);
  EXPECT_EQ(take(restored->commit(batch(0, 2, {{key(0, "a"), "restored-write"}}))).code,
            CommitCode::Committed);
  EXPECT_EQ(take(store->get_many(0, {key(0, "a")})).values[0], "later");
  auto again = take(restore("stable-1", directory.path + "/restored-again"));
  EXPECT_EQ(take(again->get_many(0, {key(0, "a")})).values[0], binary);
}

TEST_P(StoreSnapshotContract, RejectsUnsafeNamesAndBoundsRetentionWithoutReplacingExistingId) {
  for (const auto& invalid :
       {std::string(), std::string("../escape"), std::string("x/y"), std::string("/tmp/escape"),
        std::string("a.b"), std::string("a\0b", 3), std::string("\xff", 1),
        std::string(129, 'a')}) {
    EXPECT_EQ(store->create_snapshot(invalid).status().code(), ErrorCode::InvalidArgument);
    EXPECT_EQ(store->snapshot_status(invalid).status().code(), ErrorCode::InvalidArgument);
  }
  EXPECT_EQ(store->snapshot_status("absent").status().code(), ErrorCode::NotFound);
  auto created = store->create_snapshot("A_1-test");
  ASSERT_TRUE(created.ok()) << created.status().message();
  ASSERT_TRUE(store->create_snapshot("second").ok());
  EXPECT_EQ(store->create_snapshot("third").status().code(), ErrorCode::ResourceExhausted);
  EXPECT_TRUE(store->create_snapshot("A_1-test").ok());
  EXPECT_EQ(store->snapshot_status("A_1-test")->shards[0].revision, 0U);
}

TEST_P(StoreSnapshotContract, ConcurrentCommitCannotSplitDataFromReceipt) {
  std::atomic<bool> started = false;
  std::thread writer([&]() noexcept {
    started.store(true);
    for (std::uint64_t revision = 0; revision < 40; ++revision)
      EXPECT_EQ(take(backend->commit(
                         batch(0, revision, {{key(0, "counter"), std::to_string(revision + 1)}})))
                    .code,
                CommitCode::Committed);
  });
  while (!started.load())
    std::this_thread::yield();
  auto captured = store->create_snapshot("concurrent");
  writer.join();
  ASSERT_TRUE(captured.ok()) << captured.status().message();
  auto restored = take(restore("concurrent", directory.path + "/concurrent-restore"));
  auto read = take(restored->get_many(0, {key(0, "counter")}));
  EXPECT_EQ(read.revision, captured->shards[0].revision);
  if (read.revision == 0)
    EXPECT_FALSE(read.values[0]);
  else
    EXPECT_EQ(read.values[0], std::to_string(read.revision));
}

INSTANTIATE_TEST_SUITE_P(MemoryRocksRemoteMemoryRemoteRocks, StoreSnapshotContract,
                         testing::Values(0, 1, 2, 3));

TEST(StoreSnapshot, RocksSurvivesSourceReopenAndRefusesExistingRestorePath) {
  SnapshotDirectory directory;
  StoreConfig config;
  config.shard_count = 1;
  const auto path = directory.path + "/db";
  const auto key = take(data_prefix(0)) + "key";
  const CommitBatch first{
      0, config.owner, 0, take(make_commit_id(config.owner, 0, 0)), {{key, "before"}}};
  {
    auto store = take(RocksStateStore::open(path, config));
    ASSERT_EQ(take(store->commit(first)).code, CommitCode::Committed);
    ASSERT_TRUE(store->create_snapshot("persisted").ok());
    ASSERT_EQ(
        take(store->commit(
                 {0, config.owner, 1, take(make_commit_id(config.owner, 0, 1)), {{key, "after"}}}))
            .code,
        CommitCode::Committed);
  }
  auto reopened = take(RocksStateStore::open(path, config));
  EXPECT_EQ(take(reopened->snapshot_status("persisted")).shards[0].revision, 1U);
  EXPECT_EQ(take(reopened->create_snapshot("persisted")).shards[0].revision, 1U);
  EXPECT_EQ(RocksStateStore::from_snapshot(path, "persisted", path, config).status().code(),
            ErrorCode::Conflict);
  const auto empty = directory.path + "/empty";
  std::error_code error;
  ASSERT_TRUE(std::filesystem::create_directory(empty, error));
  EXPECT_EQ(RocksStateStore::from_snapshot(path, "persisted", empty, config).status().code(),
            ErrorCode::Conflict);
  auto mismatch = config;
  mismatch.owner = "different-assignment";
  EXPECT_EQ(RocksStateStore::from_snapshot(path, "persisted", directory.path + "/wrong", mismatch)
                .status()
                .code(),
            ErrorCode::FailedPrecondition);
  EXPECT_FALSE(std::filesystem::exists(directory.path + "/wrong"));
  auto restored =
      take(RocksStateStore::from_snapshot(path, "persisted", directory.path + "/restored", config));
  EXPECT_EQ(take(restored->get_many(0, {key})).values[0], "before");
  EXPECT_TRUE(take(restored->commit(first)).duplicate);
  EXPECT_EQ(take(reopened->get_many(0, {key})).values[0], "after");
}

TEST(StoreSnapshot, RocksRefusesSymlinkSnapshotRoot) {
  SnapshotDirectory directory;
  auto store = take(RocksStateStore::open(directory.path + "/db"));
  std::error_code error;
  ASSERT_TRUE(std::filesystem::create_directory(directory.path + "/outside", error));
  std::filesystem::create_directory_symlink(directory.path + "/outside",
                                            directory.path + "/db/snapshots", error);
  ASSERT_FALSE(error);
  EXPECT_EQ(store->create_snapshot("escape").status().code(), ErrorCode::FailedPrecondition);
  EXPECT_FALSE(std::filesystem::exists(directory.path + "/outside/escape"));
}

TEST(StoreSnapshot, PublishedSnapshotCannotBeOpenedAsWritableStore) {
  SnapshotDirectory directory;
  StoreConfig config;
  config.shard_count = 1;
  const auto path = directory.path + "/db";
  auto store = take(RocksStateStore::open(path, config));
  const auto key = take(data_prefix(0)) + "key";
  const CommitBatch batch{
      0, config.owner, 0, take(make_commit_id(config.owner, 0, 0)), {{key, "frozen"}}};
  ASSERT_EQ(take(store->commit(batch)).code, CommitCode::Committed);
  ASSERT_EQ(take(store->create_snapshot("fixed")).shards[0].revision, 1u);
  EXPECT_EQ(RocksStateStore::open(path + "/snapshots/fixed", config).status().code(),
            ErrorCode::FailedPrecondition);
  std::error_code error;
  std::filesystem::create_directory_symlink(path + "/snapshots/fixed", directory.path + "/alias",
                                            error);
  ASSERT_FALSE(error);
  EXPECT_EQ(RocksStateStore::open(directory.path + "/alias", config).status().code(),
            ErrorCode::FailedPrecondition);
  auto restored =
      take(RocksStateStore::from_snapshot(path, "fixed", directory.path + "/restored", config));
  ASSERT_EQ(take(restored->commit({0,
                                   config.owner,
                                   1,
                                   take(make_commit_id(config.owner, 0, 1)),
                                   {{key, "new-active-value"}}}))
                .code,
            CommitCode::Committed);
  EXPECT_EQ(take(store->create_snapshot("fixed")).shards[0].revision, 1u);
  auto again =
      take(RocksStateStore::from_snapshot(path, "fixed", directory.path + "/again", config));
  EXPECT_EQ(take(again->get_many(0, {key})).values[0], "frozen");
}

TEST(StoreSnapshot, RetryCompletesFailedSnapshotPublicationSync) {
  SnapshotDirectory directory;
  const auto path = directory.path + "/db";
  auto store = take(RocksStateStore::open(path));
  fsync_failure_path = path + "/snapshots";
  fsync_failures_left = 2;
  snapshot_root_syncs = 0;
  EXPECT_FALSE(store->create_snapshot("retry-sync").ok());
  EXPECT_EQ(snapshot_root_syncs, 1);
  // The directory exists, but neither a retry nor status may falsely confirm durability.
  EXPECT_FALSE(store->snapshot_status("retry-sync").ok());
  EXPECT_EQ(snapshot_root_syncs, 2);
  EXPECT_TRUE(store->create_snapshot("retry-sync").ok());
  EXPECT_EQ(snapshot_root_syncs, 3);
  fsync_failure_path.clear();
  fsync_failures_left = 0;
}

TEST(StoreSnapshot, RemoteLostAcknowledgementRetriesOriginalSnapshotBoundary) {
  auto memory = take(MemoryStateStore::open());
  httplib::Server proxy;
  proxy.new_task_queue = []() noexcept { return new httplib::ThreadPool(1); };
  bool first = true;
  proxy.Post("/v1/kv/create_snapshot", [&](const httplib::Request& request,
                                           httplib::Response& response) noexcept {
    auto body = take(parse_json_exact(request.body));
    auto id = take(kv_protocol::text(body, "snapshot_id"));
    auto snapshot = take(memory->create_snapshot(id));
    if (first) {
      first = false;
      response.status = 503;
      response.set_content(kv_protocol::encode_status(
                               Status(ErrorCode::Unavailable, "lost snapshot acknowledgement"))
                               .dump(),
                           "application/json");
      return;
    }
    Json shards = Json::array();
    for (const auto& shard : snapshot.shards)
      shards.push_back({{"revision", shard.revision}, {"last_commit_id", shard.last_commit_id}});
    response.set_content(Json{{"snapshot_id", snapshot.snapshot_id},
                              {"shards", std::move(shards)},
                              {"shard_owners", snapshot.shard_owners}}
                             .dump(),
                         "application/json");
  });
  const auto port = proxy.bind_to_any_port("127.0.0.1");
  ASSERT_GT(port, 0);
  std::thread listener([&]() noexcept { proxy.listen_after_bind(); });
  proxy.wait_until_ready();
  auto remote = take(RemoteStateStore::open("http://127.0.0.1:" + std::to_string(port)));
  EXPECT_EQ(remote->create_snapshot("retry").status().code(), ErrorCode::Unavailable);
  EXPECT_EQ(
      take(memory->commit({0, "deployment-1", 0, take(make_commit_id("deployment-1", 0, 0)), {}}))
          .code,
      CommitCode::Committed);
  auto retried = remote->create_snapshot("retry");
  EXPECT_TRUE(retried.ok()) << retried.status().message();
  if (retried.ok())
    EXPECT_EQ(retried->shards[0].revision, 0U);
  proxy.stop();
  listener.join();
}

TEST(StoreSnapshot, MemoryRestoredInstanceOutlivesSnapshotOwner) {
  auto source = take(MemoryStateStore::open());
  const auto key = take(data_prefix(0)) + "key";
  const CommitBatch update{
      0, "deployment-1", 0, take(make_commit_id("deployment-1", 0, 0)), {{key, "saved"}}};
  ASSERT_EQ(take(source->commit(update)).code, CommitCode::Committed);
  ASSERT_TRUE(source->create_snapshot("detached").ok());
  auto restored = take(MemoryStateStore::from_snapshot(*source, "detached"));
  source.reset();
  EXPECT_EQ(take(restored->get_many(0, {key})).values[0], "saved");
  EXPECT_TRUE(take(restored->commit(update)).duplicate);
}

TEST(StoreSnapshot, MemoryCapturesAllShardsAndAssignment) {
  StoreConfig config;
  config.shard_count = 2;
  config.shard_owners = {"owner-a", "owner-b"};
  auto store = take(MemoryStateStore::open(config));
  CommitBatch update{1,
                     "owner-b",
                     0,
                     take(make_commit_id("owner-b", 1, 0)),
                     {{take(data_prefix(1)) + "key", "value"}}};
  ASSERT_EQ(take(store->commit(update)).code, CommitCode::Committed);
  auto snapshot = store->create_snapshot("checkpoint-1");
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().message();
  EXPECT_EQ(snapshot->snapshot_id, "checkpoint-1");
  ASSERT_EQ(snapshot->shards.size(), 2U);
  EXPECT_EQ(snapshot->shards[0].revision, 0U);
  EXPECT_EQ(snapshot->shards[1].revision, 1U);
  EXPECT_EQ(snapshot->shards[1].last_commit_id, update.commit_id);
  EXPECT_EQ(snapshot->shard_owners, config.shard_owners);
}
} // namespace
} // namespace eventflow
