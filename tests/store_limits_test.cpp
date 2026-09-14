#include "eventflow/cached_store.hpp"
#include "eventflow/key_codec.hpp"
#include "eventflow/kv_protocol.hpp"
#include "eventflow/kv_server.hpp"
#include "status_test_support.hpp"

#include <filesystem>
#include <gtest/gtest.h>
#include <httplib.h>
#include <limits>
#include <thread>
#include <unistd.h>

namespace eventflow {
namespace {
using test::take;

class TemporaryDirectory {
public:
  TemporaryDirectory() noexcept {
    std::string pattern = "/tmp/eventflow-store-limits-XXXXXX";
    const auto* created = ::mkdtemp(pattern.data());
    if (!created)
      std::abort();
    path = created;
  }

  ~TemporaryDirectory() noexcept {
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
  }

  std::string path;
};

StoreConfig configured_limits() noexcept {
  StoreConfig config;
  config.shard_count = 1;
  config.max_batch_bytes = 256;
  config.max_mutations = 2;
  config.max_read_keys = 2;
  config.max_read_bytes = 48;
  config.max_scan_rows = 2;
  return config;
}

void expect_limits(const StoreLimits& limits, const StoreConfig& config) noexcept {
  EXPECT_EQ(limits.max_batch_bytes, config.max_batch_bytes);
  EXPECT_EQ(limits.max_mutations, config.max_mutations);
  EXPECT_EQ(limits.max_read_keys, config.max_read_keys);
  EXPECT_EQ(limits.max_read_bytes, config.max_read_bytes);
  EXPECT_EQ(limits.max_scan_rows, config.max_scan_rows);
}

CommitBatch batch(std::uint64_t revision, std::vector<Mutation> mutations) noexcept {
  return {0, "deployment-1", revision, take(make_commit_id("deployment-1", 0, revision)),
          std::move(mutations)};
}

std::string key(const std::string& suffix) noexcept {
  return take(data_prefix(0)) + suffix;
}

class StoreLimitsContract : public testing::TestWithParam<int> {
protected:
  void SetUp() noexcept override {
    if (GetParam() % 2 == 0)
      store = take(MemoryStateStore::open(config));
    else
      store = take(RocksStateStore::open(directory.path + "/db", config));
    if (GetParam() % 4 >= 2) {
      server = take(KvServer::create(store));
      const auto port = take(server->start("127.0.0.1", 0));
      store = take(RemoteStateStore::open("http://127.0.0.1:" + std::to_string(port)));
    }
    if (GetParam() >= 4)
      store = take(CachedStateStore::open(store));
  }

  void TearDown() noexcept override {
    store.reset();
    if (server)
      server->stop();
    server.reset();
  }

  TemporaryDirectory directory;
  StoreConfig config = configured_limits();
  std::unique_ptr<KvServer> server;
  std::shared_ptr<IStateStore> store;
};

TEST_P(StoreLimitsContract, ReportsEffectiveConfigurationWithoutChangingReceiptOrRows) {
  const auto initial = store->limits();
  ASSERT_TRUE(initial.ok()) << initial.status().message();
  expect_limits(*initial, config);
  EXPECT_EQ(take(store->status(0)).revision, 0U);
  const auto update = batch(0, {{key("a"), "value"}});
  ASSERT_EQ(take(store->commit(update)).code, CommitCode::Committed);
  const auto before = take(store->status(0));
  for (int query = 0; query < 3; ++query)
    expect_limits(take(store->limits()), config);
  const auto after = take(store->status(0));
  EXPECT_EQ(after.revision, before.revision);
  EXPECT_EQ(after.last_commit_id, before.last_commit_id);
  EXPECT_EQ(take(store->get_many(0, {key("a")})).values[0], "value");
  EXPECT_TRUE(take(store->commit(update)).duplicate);
}

TEST_P(StoreLimitsContract, EnforcesCompleteCanonicalByteBoundaryAndMutationCount) {
  const auto reported = store->limits();
  ASSERT_TRUE(reported.ok()) << reported.status().message();
  auto update = batch(0, {{key("a"), ""}});
  const auto overhead = take(canonical_batch(update)).size();
  ASSERT_LT(overhead, reported->max_batch_bytes);
  update.mutations[0].value = std::string(reported->max_batch_bytes - overhead, 'v');
  ASSERT_EQ(take(canonical_batch(update)).size(), reported->max_batch_bytes);
  auto oversized = update;
  oversized.mutations[0].value->push_back('v');
  EXPECT_EQ(take(store->commit(oversized)).code, CommitCode::Rejected);
  EXPECT_EQ(take(store->status(0)).revision, 0U);
  EXPECT_EQ(take(store->commit(update)).code, CommitCode::Committed);
  auto too_many =
      batch(1, {{key("a"), std::nullopt}, {key("b"), std::nullopt}, {key("c"), std::nullopt}});
  ASSERT_EQ(too_many.mutations.size(), reported->max_mutations + 1);
  ASSERT_LE(take(canonical_batch(too_many)).size(), reported->max_batch_bytes);
  EXPECT_EQ(take(store->commit(too_many)).code, CommitCode::Rejected);
  EXPECT_EQ(take(store->status(0)).revision, 1U);
  too_many.mutations.pop_back();
  EXPECT_EQ(take(store->commit(too_many)).code, CommitCode::Committed);
}

TEST_P(StoreLimitsContract, EnforcesReportedReadAndScanLimitsIncludingWarmCache) {
  const auto reported = store->limits();
  ASSERT_TRUE(reported.ok()) << reported.status().message();
  ASSERT_EQ(take(store->commit(batch(
                     0, {{key("a"), std::string(18, 'v')}, {key("b"), std::string(18, 'v')}})))
                .code,
            CommitCode::Committed);
  ASSERT_EQ(key("a").size() + key("b").size() + 36, reported->max_read_bytes);
  EXPECT_TRUE(store->get_many(0, {key("a"), key("b")}).ok());
  EXPECT_TRUE(store->get_many(0, {key("a"), key("b")}).ok());
  EXPECT_EQ(take(store->scan(0, take(data_prefix(0)), {}, reported->max_scan_rows)).rows.size(),
            reported->max_scan_rows);
  ASSERT_EQ(take(store->commit(batch(1, {{key("a"), std::string(19, 'v')}}))).code,
            CommitCode::Committed);
  EXPECT_TRUE(store->get_many(0, {key("a")}).ok());
  EXPECT_TRUE(store->get_many(0, {key("b")}).ok());
  EXPECT_EQ(store->get_many(0, {key("a"), key("b")}).status().code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(store->get_many(0, {key("x"), key("y"), key("z")}).status().code(),
            ErrorCode::ResourceExhausted);
  const auto prefix = take(data_prefix(0));
  EXPECT_EQ(store->scan(0, prefix, {}, reported->max_scan_rows + 1).status().code(),
            ErrorCode::ResourceExhausted);
  EXPECT_EQ(store->scan(0, prefix, {}, reported->max_scan_rows).status().code(),
            ErrorCode::ResourceExhausted);
  EXPECT_EQ(take(store->scan(0, prefix, {}, 1)).rows.size(), 1U);
  EXPECT_EQ(take(store->status(0)).revision, 2U);
}

INSTANTIATE_TEST_SUITE_P(LocalRemoteAndCachedBackends, StoreLimitsContract, testing::Range(0, 8));

class UnreportedStore : public IStateStore {
public:
  StatusOr<ReadResult> get_many(std::uint32_t, const std::vector<std::string>&) noexcept override {
    return Status(ErrorCode::Unavailable, "unreported fixture");
  }

  StatusOr<ScanResult> scan(std::uint32_t, const std::string&, const std::optional<std::string>&,
                            std::size_t) noexcept override {
    return Status(ErrorCode::Unavailable, "unreported fixture");
  }

  StatusOr<CommitResult> commit(const CommitBatch&) noexcept override {
    return Status(ErrorCode::Unavailable, "unreported fixture");
  }

  StatusOr<ShardStatus> status(std::uint32_t) noexcept override {
    return Status(ErrorCode::Unavailable, "unreported fixture");
  }
};

class UncertainStore final : public UnreportedStore {
public:
  StatusOr<StoreLimits> limits() noexcept override {
    const auto config = configured_limits();
    return StoreLimits{config.max_batch_bytes, config.max_mutations, config.max_read_keys,
                       config.max_read_bytes, config.max_scan_rows};
  }

  StatusOr<CommitResult> commit(const CommitBatch&) noexcept override {
    return CommitResult{CommitCode::Unknown, 0, false, "uncertain fixture"};
  }
};

TEST(StoreLimits, CachedMetadataQueryPreservesUnknownCommitFence) {
  auto cache = take(CachedStateStore::open(std::make_shared<UncertainStore>()));
  ASSERT_EQ(take(cache->commit(batch(0, {{key("a"), "value"}}))).code, CommitCode::Unknown);
  ASSERT_TRUE(cache->has_pending_commit());
  expect_limits(take(cache->limits()), configured_limits());
  EXPECT_TRUE(cache->has_pending_commit());
  EXPECT_EQ(cache->get_many(0, {key("a")}).status().code(), ErrorCode::UnknownCommit);
}

TEST(StoreLimits, UnreportedBackendFailsExplicitlyThroughCacheAndRpc) {
  auto backend = std::make_shared<UnreportedStore>();
  EXPECT_EQ(backend->limits().status().code(), ErrorCode::FailedPrecondition);
  auto cache = take(CachedStateStore::open(backend));
  EXPECT_EQ(cache->limits().status().code(), ErrorCode::FailedPrecondition);
  auto server = take(KvServer::create(backend));
  const auto port = take(server->start("127.0.0.1", 0));
  auto remote = take(RemoteStateStore::open("http://127.0.0.1:" + std::to_string(port)));
  EXPECT_EQ(remote->limits().status().code(), ErrorCode::FailedPrecondition);
}

TEST(StoreLimits, RestoresAndReopensReportTheirActualOpenConfiguration) {
  const auto config = configured_limits();
  auto memory = take(MemoryStateStore::open(config));
  ASSERT_TRUE(memory->create_snapshot("snapshot").ok());
  auto memory_copy = take(MemoryStateStore::from_snapshot(*memory, "snapshot"));
  ASSERT_TRUE(memory_copy->limits().ok());
  expect_limits(take(memory_copy->limits()), config);

  TemporaryDirectory directory;
  auto rocks = take(RocksStateStore::open(directory.path + "/source", config));
  ASSERT_TRUE(rocks->create_snapshot("snapshot").ok());
  auto changed = config;
  changed.max_batch_bytes = 1024;
  changed.max_mutations = 7;
  changed.max_read_keys = 11;
  changed.max_read_bytes = 333;
  changed.max_scan_rows = 17;
  auto restored = take(RocksStateStore::from_snapshot(directory.path + "/source", "snapshot",
                                                      directory.path + "/restored", changed));
  ASSERT_TRUE(restored->limits().ok());
  expect_limits(take(restored->limits()), changed);
  rocks.reset();
  auto reopened = take(RocksStateStore::open(directory.path + "/source", changed));
  expect_limits(take(reopened->limits()), changed);
}

class HttpFixture {
public:
  HttpFixture(std::string body, int status = 200,
              std::vector<std::string> encodings = {}) noexcept {
    server.new_task_queue = []() noexcept { return new httplib::ThreadPool(2, 8); };
    server.Post("/v1/kv/limits",
                [body = std::move(body), status, encodings = std::move(encodings)](
                    const httplib::Request& request, httplib::Response& reply) noexcept {
                  EXPECT_EQ(request.body, "{}");
                  reply.status = status;
                  for (const auto& encoding : encodings)
                    reply.set_header("Content-Encoding", encoding);
                  reply.set_content(body, "application/json");
                });
    port = server.bind_to_any_port("127.0.0.1");
    if (port <= 0)
      std::abort();
    listener = std::thread([this]() noexcept { server.listen_after_bind(); });
    server.wait_until_ready();
  }

  ~HttpFixture() noexcept {
    server.stop();
    listener.join();
  }

  std::string endpoint() const noexcept {
    return "http://127.0.0.1:" + std::to_string(port);
  }

  httplib::Server server;
  std::thread listener;
  int port = 0;
};

Json limits_json() noexcept {
  return {{"max_batch_bytes", 256},
          {"max_mutations", 2},
          {"max_read_keys", 2},
          {"max_read_bytes", 48},
          {"max_scan_rows", 2}};
}

TEST(StoreLimitsRpc, RejectsMissingUnknownMalformedAndDuplicateFields) {
  std::vector<std::string> invalid = {"[]", "null", "{}", "{", "true"};
  auto unknown = limits_json();
  unknown["unexpected"] = 1;
  invalid.push_back(unknown.dump());
  for (const auto* field :
       {"max_batch_bytes", "max_mutations", "max_read_keys", "max_read_bytes", "max_scan_rows"}) {
    auto missing = limits_json();
    missing.erase(field);
    invalid.push_back(missing.dump());
    for (const auto& value : std::vector<Json>{0, -1, 1.5, "32", true, nullptr, Json::array()}) {
      auto malformed = limits_json();
      malformed[field] = value;
      invalid.push_back(malformed.dump());
    }
  }
  invalid.push_back(
      R"({"max_batch_bytes":256,"max_mutations":2,"max_read_keys":2,"max_read_bytes":48,"max_scan_rows":2,"max_scan_rows":3})");
  invalid.push_back(
      R"({"max_batch_bytes":18446744073709551616,"max_mutations":2,"max_read_keys":2,"max_read_bytes":48,"max_scan_rows":2})");
  for (const auto& body : invalid) {
    SCOPED_TRACE(body);
    HttpFixture server(body);
    auto remote = take(RemoteStateStore::open(server.endpoint()));
    const auto result = remote->limits();
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), ErrorCode::InvalidArgument);
  }
}

TEST(StoreLimitsRpc, PreservesStructuredErrorsAndRejectsOversizedResponse) {
  HttpFixture unavailable(
      kv_protocol::encode_status(Status(ErrorCode::Unavailable, "offline")).dump(), 503);
  auto remote = take(RemoteStateStore::open(unavailable.endpoint()));
  EXPECT_EQ(remote->limits().status().code(), ErrorCode::Unavailable);
  HttpFixture exhausted(
      kv_protocol::encode_status(Status(ErrorCode::ResourceExhausted, "busy")).dump(), 400);
  remote = take(RemoteStateStore::open(exhausted.endpoint()));
  EXPECT_EQ(remote->limits().status().code(), ErrorCode::ResourceExhausted);
  HttpFixture oversized(std::string(4097, ' ') + limits_json().dump());
  remote = take(RemoteStateStore::open(oversized.endpoint()));
  EXPECT_EQ(remote->limits().status().code(), ErrorCode::Unavailable);
}

TEST(StoreLimitsRpc, CapsRepresentableBackendLimitsWithinExactMetadataResponseBudget) {
  auto json = limits_json();
  for (auto& item : json.items())
    item.value() = std::numeric_limits<std::size_t>::max();
  auto body = json.dump();
  ASSERT_LT(body.size(), 4096U);
  body.append(4096 - body.size(), ' ');
  HttpFixture server(std::move(body));
  auto remote = take(RemoteStateStore::open(server.endpoint()));
  const auto reported = remote->limits();
  ASSERT_TRUE(reported.ok()) << reported.status().message();
  EXPECT_EQ(reported->max_batch_bytes, 2796202U);
  EXPECT_EQ(reported->max_mutations, 310689U);
  EXPECT_EQ(reported->max_read_keys, 1677708U);
  EXPECT_EQ(reported->max_read_bytes, 4194272U);
  EXPECT_EQ(reported->max_scan_rows, 1048568U);
}

TEST(StoreLimitsRpc, RejectsDuplicateContentEncodingInEitherOrder) {
  for (const auto& encodings : std::vector<std::vector<std::string>>{
           {"gzip", "identity"}, {"identity", "gzip"}, {"identity", "identity"}}) {
    HttpFixture server(limits_json().dump(), 200, encodings);
    auto remote = take(RemoteStateStore::open(server.endpoint()));
    EXPECT_EQ(remote->limits().status().code(), ErrorCode::Unavailable);
  }
}

TEST(StoreLimitsRpc, ServiceRejectsNonemptyOrMalformedRequestWithoutChangingRevision) {
  auto backend = std::shared_ptr<IStateStore>(take(MemoryStateStore::open(configured_limits())));
  auto server = take(KvServer::create(backend));
  const auto port = take(server->start("127.0.0.1", 0));
  httplib::Client client("127.0.0.1", port);
  for (const auto* body : {"[]", "null", "{", "{\"shard\":0}", "{\"max_batch_bytes\":1}"}) {
    const auto response = client.Post("/v1/kv/limits", body, "application/json");
    ASSERT_TRUE(response);
    EXPECT_EQ(response->status, 400);
    EXPECT_EQ(kv_protocol::decode_status(take(parse_json_exact(response->body))).code(),
              ErrorCode::InvalidArgument);
  }
  EXPECT_EQ(take(backend->status(0)).revision, 0U);
}

static_assert(noexcept(std::declval<IStateStore&>().limits()));
} // namespace
} // namespace eventflow
