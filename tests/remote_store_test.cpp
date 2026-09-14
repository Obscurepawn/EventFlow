#include "eventflow/kv_protocol.hpp"
#include "eventflow/kv_server.hpp"
#include "kv_process.hpp"
#include "status_test_support.hpp"
#include <arpa/inet.h>
#include <atomic>
#include <gtest/gtest.h>
#include <httplib.h>
#include <limits>
#include <sys/socket.h>
#include <thread>

using namespace eventflow;
using eventflow::test::take;

namespace {
struct RunningServer {
  explicit RunningServer(std::shared_ptr<IStateStore> store) noexcept {
    server = take(KvServer::create(std::move(store)));
    port = take(server->start("127.0.0.1", 0));
  }

  ~RunningServer() noexcept {
    server->stop();
  }

  std::string endpoint() const noexcept {
    return "http://127.0.0.1:" + std::to_string(port);
  }

  std::unique_ptr<KvServer> server;
  std::thread thread;
  int port = 0;
};

struct RunningHttp {
  RunningHttp() noexcept {
    server.new_task_queue = []() noexcept { return new httplib::ThreadPool(2); };
  }

  void start() noexcept {
    port = server.bind_to_any_port("127.0.0.1");
    if (port <= 0) {
      ADD_FAILURE() << "cannot bind HTTP fixture";
      std::abort();
    }
    thread = std::thread([this]() noexcept { server.listen_after_bind(); });
  }

  ~RunningHttp() noexcept {
    server.stop();
    if (thread.joinable())
      thread.join();
  }

  std::string endpoint() const noexcept {
    return "http://127.0.0.1:" + std::to_string(port);
  }

  httplib::Server server;
  std::thread thread;
  int port = 0;
};

CommitBatch batch(std::uint64_t revision, std::vector<Mutation> mutations) noexcept {
  return {0, "deployment-1", revision, take(make_commit_id("deployment-1", 0, revision)),
          std::move(mutations)};
}

std::string key(std::string ticker, std::int64_t id = 1) noexcept {
  return take(row_key(0, 'T', 2, {std::move(ticker), id}));
}
} // namespace

TEST(RemoteStateStore, ReadsAndCommitsThroughActualHttp) {
  auto memory = std::shared_ptr<IStateStore>(take(MemoryStateStore::open()));
  RunningServer server(memory);
  auto client = take(RemoteStateStore::open(server.endpoint()));
  auto update = batch(0, {{key("AAPL"), "value"}});
  EXPECT_EQ(take(client->commit(update)).code, CommitCode::Committed);
  EXPECT_EQ(take(client->get_many(0, {key("AAPL")})).values[0], "value");
  EXPECT_TRUE(take(client->commit(update)).duplicate);
  EXPECT_EQ(take(client->status(0)).revision, 1U);
  EXPECT_EQ(take(client->scan(0, take(table_prefix(0, 'T', 2)), std::nullopt, 10)).rows.size(), 1U);
}

TEST(RemoteStateStore, MalformedAcknowledgementRemainsUnknown) {
  RunningHttp server;
  server.server.Post(
      "/v1/kv/commit", [](const httplib::Request&, httplib::Response& response) noexcept {
        response.set_content(R"({"code":"INVALID","revision":1,"duplicate":false,"message":"bad"})",
                             "application/json");
      });
  server.start();
  auto client = take(RemoteStateStore::open(server.endpoint()));
  EXPECT_EQ(take(client->commit(batch(0, {}))).code, CommitCode::Unknown);
}

class KvProcessContract : public testing::TestWithParam<std::string> {};

TEST_P(KvProcessContract, RealServiceSupportsBinaryAtomicBatchesAndPages) {
  eventflow::test::TempDirectory directory;
  auto process = take(eventflow::test::KvProcess::open(GetParam(), directory.path));
  auto client = take(RemoteStateStore::open(process->endpoint()));
  const auto a = key("AAPL", 1), b = key("AAPL", 2);
  const std::string binary("\0\xff", 2);
  auto update = batch(0, {{a, binary}, {b, std::string()}});
  ASSERT_EQ(take(client->commit(update)).code, CommitCode::Committed);
  auto values = take(client->get_many(0, {a, b, key("missing")}));
  ASSERT_EQ(values.values.size(), 3U);
  EXPECT_EQ(values.values[0], binary);
  EXPECT_EQ(values.values[1], std::string());
  EXPECT_FALSE(values.values[2]);
  auto first = take(client->scan(0, take(table_prefix(0, 'T', 2)), std::nullopt, 1));
  ASSERT_EQ(first.rows.size(), 1U);
  auto second = take(client->scan(0, take(table_prefix(0, 'T', 2)), first.rows[0].first, 1));
  ASSERT_EQ(second.rows.size(), 1U);
  EXPECT_EQ(first.rows[0].first, a);
  EXPECT_EQ(second.rows[0].first, b);
  EXPECT_TRUE(take(client->commit(update)).duplicate);
  auto invalid =
      batch(1, {{a, "changed"}, {take(row_key(1, 'T', 2, {std::string("MSFT")})), "cross-shard"}});
  EXPECT_EQ(take(client->commit(invalid)).code, CommitCode::Rejected);
  EXPECT_EQ(take(client->status(0)).revision, 1U);
  EXPECT_EQ(take(client->get_many(0, {a})).values[0], binary);
  EXPECT_EQ(take(client->commit(batch(1, {{a, std::nullopt}}))).code, CommitCode::Committed);
  EXPECT_FALSE(take(client->get_many(0, {a})).values[0]);
}

INSTANTIATE_TEST_SUITE_P(Backends, KvProcessContract, testing::Values("memory", "rocks"));

TEST(RemoteStateStore, RocksServiceRestartRetainsReceiptAndRows) {
  eventflow::test::TempDirectory directory;
  auto update = batch(0, {{key("AAPL"), "saved"}});
  {
    auto process = take(eventflow::test::KvProcess::open("rocks", directory.path));
    auto client = take(RemoteStateStore::open(process->endpoint()));
    ASSERT_EQ(take(client->commit(update)).code, CommitCode::Committed);
  }
  {
    auto process = take(eventflow::test::KvProcess::open("rocks", directory.path));
    auto client = take(RemoteStateStore::open(process->endpoint()));
    EXPECT_EQ(take(client->get_many(0, {key("AAPL")})).values[0], "saved");
    EXPECT_TRUE(take(client->commit(update)).duplicate);
    EXPECT_EQ(take(client->status(0)).revision, 1U);
  }
}

TEST(RemoteStateStore, MissingAcknowledgementRetriesSameCommittedIdentity) {
  auto memory = take(MemoryStateStore::open());
  RunningHttp proxy;
  std::atomic_bool lose_first = true;
  proxy.server.Post(
      "/v1/kv/commit", [&](const httplib::Request& request, httplib::Response& response) noexcept {
        auto decoded = take(kv_protocol::decode_batch(Json::parse(request.body, nullptr, false)));
        auto result = take(memory->commit(decoded));
        if (lose_first.exchange(false)) {
          response.status = 503;
          response.set_content(
              kv_protocol::encode_status(Status(ErrorCode::Unavailable, "lost ack")).dump(),
              "application/json");
        } else
          response.set_content(kv_protocol::encode(result).dump(), "application/json");
      });
  proxy.start();
  auto client = take(RemoteStateStore::open(proxy.endpoint()));
  auto update = batch(0, {{key("AAPL"), "once"}});
  EXPECT_EQ(take(client->commit(update)).code, CommitCode::Unknown);
  EXPECT_EQ(take(memory->status(0)).revision, 1U);
  auto retried = take(client->commit(update));
  EXPECT_EQ(retried.code, CommitCode::Committed);
  EXPECT_TRUE(retried.duplicate);
  EXPECT_EQ(retried.revision, 1U);
}

TEST(RemoteStateStore, WireValidationRejectsOverflowAndMalformedKeys) {
  auto memory = std::shared_ptr<IStateStore>(take(MemoryStateStore::open()));
  RunningServer server(memory);
  httplib::Client client(server.endpoint());
  for (const auto& body : {R"({"shard":-1,"keys":[]})", R"({"shard":4294967296,"keys":[]})",
                           R"({"shard":0,"keys":["zz"]})", R"({"shard":0,"keys":"wrong"})"}) {
    auto response = client.Post("/v1/kv/get", body, "application/json");
    ASSERT_TRUE(response);
    EXPECT_EQ(response->status, 400);
  }
  EXPECT_EQ(take(memory->status(0)).revision, 0U);
}

TEST(Status, ErrorsAndMoveOnlyResultsHaveNoThrowContract) {
  static_assert(noexcept(Status(ErrorCode::InvalidArgument, "bad input")));
  static_assert(noexcept(std::declval<IStateStore&>().status(0)));
  static_assert(
      noexcept(std::declval<RemoteStateStore&>().commit(std::declval<const CommitBatch&>())));
  Status status(ErrorCode::InvalidArgument, "bad input");
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.message(), "bad input");
  StatusOr<int> value(42);
  ASSERT_TRUE(value.ok());
  EXPECT_EQ(*value, 42);
  StatusOr<int> missing(Status{});
  EXPECT_EQ(missing.status().code(), ErrorCode::Internal);
  std::string long_message(1000, 'x');
  Status bounded(ErrorCode::Internal, long_message);
  EXPECT_EQ(bounded.message().size(), 384U);
}

TEST(KvServer, ImmediateStopAfterStartCannotRaceListenerStartup) {
  for (int i = 0; i < 20; ++i) {
    auto backend = std::shared_ptr<IStateStore>(take(MemoryStateStore::open()));
    auto server = take(KvServer::create(std::move(backend)));
    ASSERT_TRUE(server->start("127.0.0.1", 0).ok());
    server->stop();
    EXPECT_FALSE(server->is_running());
    EXPECT_EQ(server->start("127.0.0.1", 0).status().code(), ErrorCode::FailedPrecondition);
  }
}

TEST(Status, TruncationKeepsValidUtf8AtTheBoundary) {
  Status status(ErrorCode::Internal, std::string(383, 'x') + "\u4E2D\u6587");
  EXPECT_TRUE(valid_utf8(status.message()));
}

TEST(RemoteStateStore, EndpointValidationReturnsStatusBeforeCallingTransport) {
  for (const auto* endpoint :
       {"http://", "https://localhost:1", "http://:80", "http://localhost:0",
        "http://localhost:65536", "http://localhost:99999999999999999999999999999",
        "http://localhost:-1", "http://user@localhost:80", "http://localhost:80/path",
        "http://[::1", "http://a b:80"}) {
    auto opened = RemoteStateStore::open(endpoint);
    EXPECT_FALSE(opened.ok()) << endpoint;
    EXPECT_EQ(opened.status().code(), ErrorCode::InvalidArgument) << endpoint;
  }
  EXPECT_TRUE(RemoteStateStore::open("http://localhost").ok());
  EXPECT_TRUE(RemoteStateStore::open("http://127.0.0.1:8081").ok());
  EXPECT_TRUE(RemoteStateStore::open("http://[::1]:8081").ok());
}

TEST(RemoteStateStore, ImpossibleCommittedRevisionIsUnknown) {
  RunningHttp server;
  std::atomic<std::uint64_t> revision = 0;
  server.server.Post("/v1/kv/commit",
                     [&](const httplib::Request&, httplib::Response& response) noexcept {
                       response.set_content(Json{{"code", "COMMITTED"},
                                                 {"revision", revision.load()},
                                                 {"duplicate", false},
                                                 {"message", ""}}
                                                .dump(),
                                            "application/json");
                     });
  server.start();
  auto client = take(RemoteStateStore::open(server.endpoint()));
  for (auto wrong :
       {std::uint64_t(0), std::uint64_t(2), std::numeric_limits<std::uint64_t>::max()}) {
    revision = wrong;
    EXPECT_EQ(take(client->commit(batch(0, {}))).code, CommitCode::Unknown);
  }
}

TEST(KvServer, ChunkedRequestsAreRejectedBeforeReadingTheirBody) {
  auto store = std::shared_ptr<IStateStore>(take(MemoryStateStore::open()));
  RunningServer server(store);
  const int socket = ::socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(socket, 0);

  struct Close {
    int fd;

    ~Close() noexcept {
      ::close(fd);
    }
  } close{socket};

  timeval timeout{2, 0};
  ASSERT_EQ(::setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)), 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(static_cast<std::uint16_t>(server.port));
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  ASSERT_EQ(::connect(socket, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
  const std::string header =
      "POST /v1/kv/get HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n"
      "Content-Type: application/json\r\nConnection: close\r\n\r\n";
  ASSERT_EQ(::send(socket, header.data(), header.size(), MSG_NOSIGNAL),
            static_cast<ssize_t>(header.size()));
  char response[1024];
  const auto size = ::recv(socket, response, sizeof(response), 0);
  ASSERT_GT(size, 0);
  EXPECT_TRUE(
      std::string_view(response, static_cast<std::size_t>(size)).starts_with("HTTP/1.1 400"));
}

TEST(KvServer, MissingLengthIsRejectedBeforeReadingTheRpcBody) {
  auto store = std::shared_ptr<IStateStore>(take(MemoryStateStore::open()));
  RunningServer server(store);
  const int socket = ::socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(socket, 0);

  struct Close {
    int fd;

    ~Close() noexcept {
      ::close(fd);
    }
  } close{socket};

  timeval timeout{2, 0};
  ASSERT_EQ(::setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)), 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(static_cast<std::uint16_t>(server.port));
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  ASSERT_EQ(::connect(socket, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
  const std::string header = "POST /v1/kv/commit HTTP/1.1\r\nHost: localhost\r\n"
                             "Content-Type: application/json\r\nConnection: close\r\n\r\n";
  ASSERT_EQ(::send(socket, header.data(), header.size(), MSG_NOSIGNAL),
            static_cast<ssize_t>(header.size()));
  char response[1024];
  const auto size = ::recv(socket, response, sizeof(response), 0);
  ASSERT_GT(size, 0);
  EXPECT_TRUE(
      std::string_view(response, static_cast<std::size_t>(size)).starts_with("HTTP/1.1 400"));
  EXPECT_EQ(take(store->status(0)).revision, 0U);
}
