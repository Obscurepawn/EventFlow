#include "eventflow/cluster_client.hpp"
#include "eventflow/kv_protocol.hpp"
#include "status_test_support.hpp"
#include <atomic>
#include <gtest/gtest.h>
#include <httplib.h>
#include <mutex>
#include <thread>

namespace eventflow::test {
namespace {
struct MetaStub {
  MetaStub() noexcept {
    server.new_task_queue = []() noexcept { return new httplib::ThreadPool(2); };
  }

  void start() noexcept {
    port = server.bind_to_any_port("127.0.0.1");
    if (port <= 0) {
      ADD_FAILURE() << "cannot bind HTTP fixture";
      std::abort();
    }
    listener = std::thread([this]() noexcept { server.listen_after_bind(); });
  }

  ~MetaStub() noexcept {
    server.stop();
    if (listener.joinable())
      listener.join();
  }

  std::string endpoint() const noexcept {
    return "http://127.0.0.1:" + std::to_string(port);
  }

  httplib::Server server;
  std::thread listener;
  int port = 0;
};

Json discovery() noexcept {
  return Json{
      {"deployment_version", "1"},
      {"state", "RUNNING"},
      {"shard_count", 2},
      {"routes",
       Json::array(
           {{{"shard", 0}, {"node_id", "compute-a"}, {"endpoint", "http://127.0.0.1:9011"}},
            {{"shard", 1}, {"node_id", "compute-b"}, {"endpoint", "http://127.0.0.1:9012"}}})},
      {"now_us", "9007199254740993"}};
}

TEST(ClusterClient, BootstrapDiscoveryUsesMetaOnlyAndPreservesDecimalIdentity) {
  MetaStub stub;
  stub.server.Post("/v1/meta/discover",
                   [](const httplib::Request& request, httplib::Response& response) noexcept {
                     EXPECT_EQ(request.body, "{}");
                     response.set_content(discovery().dump(), "application/json");
                   });
  stub.start();
  auto opened = ClusterClient::open(stub.endpoint());
  ASSERT_TRUE(opened.ok()) << opened.status().message();
  auto result = take(opened.value()->discover());
  EXPECT_EQ(result["now_us"], "9007199254740993");
  EXPECT_EQ(result["routes"].size(), 2);
}

Json acknowledgement(bool committed, bool duplicate, ErrorCode code = ErrorCode::Ok) noexcept {
  return Json{{"input_committed", committed},
              {"duplicate", duplicate},
              {"operation_status",
               {{"code", static_cast<int>(code)},
                {"message", code == ErrorCode::Ok ? "" : "owner work remains"}}},
              {"shard", 1},
              {"node_id", "compute-b"},
              {"revision", "9007199254740993"}};
}

TEST(ClusterClient, AckRequiresConfirmedFlagAndKeepsPostCommitOperationError) {
  MetaStub stub;
  std::atomic<int> calls = 0;
  stub.server.Post(
      "/v1/meta/ingest", [&](const httplib::Request&, httplib::Response& response) noexcept {
        const auto call = calls.fetch_add(1);
        auto body = call == 0   ? acknowledgement(false, false)
                    : call == 1 ? acknowledgement(true, false, ErrorCode::ResourceExhausted)
                                : acknowledgement(true, true);
        response.set_content(body.dump(), "application/json");
      });
  stub.start();
  auto client = take(ClusterClient::open(stub.endpoint()));
  const SourceIdentity source{"sdk", "trades", 0, 9007199254740993LL};
  auto not_committed = take(client->ingest(
      "trade", {std::string("AAPL"), std::int64_t(1), std::int64_t(10)}, source, 10));
  EXPECT_TRUE(not_committed.operation_status.ok());
  EXPECT_FALSE(not_committed.input_committed);
  auto committed = take(client->ingest(
      "trade", {std::string("AAPL"), std::int64_t(1), std::int64_t(10)}, source, 10));
  EXPECT_TRUE(committed.input_committed);
  EXPECT_EQ(committed.operation_status.code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(committed.revision, 9007199254740993ULL);
  auto duplicate = take(client->ingest(
      "trade", {std::string("AAPL"), std::int64_t(1), std::int64_t(10)}, source, 10));
  EXPECT_TRUE(duplicate.input_committed);
  EXPECT_TRUE(duplicate.duplicate);
}

TEST(ClusterClient, ExplicitIngestRetryPreservesIdenticalPayloadIdentityAndTimes) {
  MetaStub stub;
  std::atomic<int> calls = 0;
  std::mutex mutex;
  std::vector<std::string> bodies;
  stub.server.Post("/v1/meta/ingest", [&](const httplib::Request& request,
                                          httplib::Response& response) noexcept {
    {
      std::lock_guard lock(mutex);
      bodies.push_back(request.body);
    }
    const auto call = calls.fetch_add(1);
    if (call == 0) {
      response.status = 503;
      response.set_content(kv_protocol::encode_status({ErrorCode::Unavailable, "retry"}).dump(),
                           "application/json");
    } else
      response.set_content(acknowledgement(call == 2, call == 2,
                                           call == 1 ? ErrorCode::UnknownCommit : ErrorCode::Ok)
                               .dump(),
                           "application/json");
  });
  stub.start();
  ClusterClientConfig config;
  config.ingest_attempts = 3;
  auto client = take(ClusterClient::open(stub.endpoint(), config));
  const auto now = 9007199254740995LL;
  auto result =
      take(client->ingest("trade", {std::string("AAPL"), std::int64_t(7), std::int64_t(120)},
                          {"durable-source", "topic", 3, 9007199254740993LL}, now, now - 1));
  EXPECT_TRUE(result.input_committed);
  EXPECT_TRUE(result.duplicate);
  EXPECT_EQ(calls.load(), 3);
  std::lock_guard lock(mutex);
  ASSERT_EQ(bodies.size(), 3);
  EXPECT_EQ(bodies[0], bodies[1]);
  EXPECT_EQ(bodies[1], bodies[2]);
  auto body = take(parse_json_exact(bodies[0]));
  EXPECT_EQ(body["source"]["offset"], "9007199254740993");
  EXPECT_EQ(body["now_us"], "9007199254740995");
  EXPECT_EQ(body["event_time_us"], "9007199254740994");
  EXPECT_EQ(body["source"]["source_id"], "durable-source");
}

TEST(ClusterClient, ControlMutationsNeverAutoRetryAfterUnknownOutcome) {
  MetaStub stub;
  std::atomic<int> calls = 0;
  for (const auto* operation : {"ddl", "deploy", "checkpoint"})
    stub.server.Post(
        std::string("/v1/meta/") + operation,
        [&](const httplib::Request&, httplib::Response& response) noexcept {
          ++calls;
          response.status = 503;
          response.set_content(
              kv_protocol::encode_status({ErrorCode::UnknownCommit, "inspect status"}).dump(),
              "application/json");
        });
  stub.start();
  ClusterClientConfig config;
  config.ingest_attempts = 8;
  auto client = take(ClusterClient::open(stub.endpoint(), config));
  EXPECT_EQ(client->ddl("CREATE TABLE t(k STRING,PRIMARY KEY(k)) PARTITION BY(k)").status().code(),
            ErrorCode::UnknownCommit);
  EXPECT_EQ(client->deploy().status().code(), ErrorCode::UnknownCommit);
  EXPECT_EQ(client->checkpoint("cp", 123).status().code(), ErrorCode::UnknownCommit);
  EXPECT_EQ(calls.load(), 3);
}

TEST(ClusterClient, DrainWaitsForAllWorkersAtOneFixedLogicalTime) {
  MetaStub stub;
  std::atomic<int> calls = 0;
  stub.server.Post("/v1/meta/pump",
                   [&](const httplib::Request& request, httplib::Response& response) noexcept {
                     auto body = take(parse_json_exact(request.body));
                     EXPECT_EQ(body["now_us"], "9007199254740993");
                     EXPECT_EQ(body["max_functions"], 7);
                     const auto call = ++calls;
                     response.set_content(Json{{"work_remaining", call < 3},
                                               {"clock_blocked_by_pending_input", false},
                                               {"now_us", body["now_us"]}}
                                              .dump(),
                                          "application/json");
                   });
  stub.start();
  auto client = take(ClusterClient::open(stub.endpoint()));
  auto result = take(client->drain(9007199254740993LL, 4, 7));
  EXPECT_FALSE(result["work_remaining"].get<bool>());
  EXPECT_EQ(calls.load(), 3);
}

TEST(ClusterClient, DrainDoesNotRetryAnAmbiguousPumpFailure) {
  MetaStub stub;
  std::atomic<int> calls = 0;
  stub.server.Post(
      "/v1/meta/pump", [&](const httplib::Request&, httplib::Response& response) noexcept {
        ++calls;
        response.status = 503;
        response.set_content(
            kv_protocol::encode_status({ErrorCode::UnknownCommit, "inspect the original operation"})
                .dump(),
            "application/json");
      });
  stub.start();
  auto client = take(ClusterClient::open(stub.endpoint()));
  EXPECT_EQ(client->drain(100).status().code(), ErrorCode::UnknownCommit);
  EXPECT_EQ(calls.load(), 1);
}

TEST(ClusterClient, DrainReturnsPendingInputBarrierWithoutSpinning) {
  MetaStub stub;
  std::atomic<int> calls = 0;
  stub.server.Post(
      "/v1/meta/pump", [&](const httplib::Request&, httplib::Response& response) noexcept {
        ++calls;
        response.set_content(
            Json{{"work_remaining", false}, {"clock_blocked_by_pending_input", true}}.dump(),
            "application/json");
      });
  stub.start();
  auto client = take(ClusterClient::open(stub.endpoint()));
  EXPECT_EQ(client->drain(100).status().code(), ErrorCode::FailedPrecondition);
  EXPECT_EQ(calls.load(), 1);
}

TEST(ClusterClient, DrainHasABoundedRoundBudgetAndRejectsMalformedProgress) {
  MetaStub stub;
  std::atomic<int> calls = 0;
  stub.server.Post(
      "/v1/meta/pump", [&](const httplib::Request&, httplib::Response& response) noexcept {
        const auto call = ++calls;
        auto result = Json{{"work_remaining", true}, {"clock_blocked_by_pending_input", false}};
        if (call == 3)
          result["work_remaining"] = "false";
        response.set_content(result.dump(), "application/json");
      });
  stub.start();
  auto client = take(ClusterClient::open(stub.endpoint()));
  EXPECT_EQ(client->drain(100, 0).status().code(), ErrorCode::InvalidArgument);
  EXPECT_EQ(calls.load(), 0);
  EXPECT_EQ(client->drain(100, 2).status().code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(calls.load(), 2);
  EXPECT_EQ(client->drain(100, 2).status().code(), ErrorCode::InvalidArgument);
  EXPECT_EQ(calls.load(), 3);
}

Json query_result() noexcept {
  return Json{{"fields", Json::array({{{"id", 1},
                                       {"name", "volume"},
                                       {"type", data_type_name(DataType::Int64)},
                                       {"nullable", false}}})},
              {"rows", Json::array({Json::array({9007199254740993LL})})},
              {"revision", "9007199254740994"},
              {"catalog_version", "42"},
              {"node_id", "compute-b"},
              {"shard", 1}};
}

TEST(ClusterClient, QueryDecodesExactInt64AndRejectsMismatchedRowsOrNumericMetadata) {
  MetaStub stub;
  std::atomic<int> calls = 0;
  stub.server.Post("/v1/meta/query",
                   [&](const httplib::Request& request, httplib::Response& response) noexcept {
                     auto body = take(parse_json_exact(request.body));
                     EXPECT_EQ(body["scope"][0], "AAPL");
                     EXPECT_EQ(body["max_rows"], 10);
                     auto result = query_result();
                     const auto call = calls.fetch_add(1);
                     if (call == 1)
                       result["rows"][0][0] = 1.5;
                     if (call == 2)
                       result["revision"] = 9007199254740994ULL;
                     if (call == 3)
                       result["rows"][0].push_back(2);
                     response.set_content(result.dump(), "application/json");
                   });
  stub.start();
  auto client = take(ClusterClient::open(stub.endpoint()));
  auto result = take(client->query("SELECT volume FROM trade_volume", {std::string("AAPL")}, 10));
  EXPECT_EQ(std::get<std::int64_t>(result.rows[0][0]), 9007199254740993LL);
  EXPECT_EQ(result.revision, 9007199254740994ULL);
  EXPECT_EQ(result.catalog_version, 42);
  EXPECT_EQ(result.node_id, "compute-b");
  for (int i = 0; i < 3; ++i)
    EXPECT_FALSE(client->query("SELECT volume FROM trade_volume", {std::string("AAPL")}, 10).ok());
}

TEST(ClusterClient, DiscoveryRejectsIncompleteRoutesAndRoundedMetadata) {
  MetaStub stub;
  std::atomic<int> calls = 0;
  stub.server.Post("/v1/meta/discover",
                   [&](const httplib::Request&, httplib::Response& response) noexcept {
                     auto result = discovery();
                     if (calls.fetch_add(1) == 0)
                       result["routes"][1]["shard"] = 0;
                     else
                       result["now_us"] = 9007199254740993LL;
                     response.set_content(result.dump(), "application/json");
                   });
  stub.start();
  auto client = take(ClusterClient::open(stub.endpoint()));
  EXPECT_FALSE(client->discover().ok());
  EXPECT_FALSE(client->discover().ok());
}

TEST(ClusterClient, ResponseByteLimitAndDuplicateKeysFailWithoutInventingSuccess) {
  MetaStub stub;
  stub.server.Post(
      "/v1/meta/status", [](const httplib::Request&, httplib::Response& response) noexcept {
        response.set_content(Json{{"padding", std::string(500, 'x')}}.dump(), "application/json");
      });
  stub.server.Post(
      "/v1/meta/pause", [](const httplib::Request&, httplib::Response& response) noexcept {
        response.set_content(R"({"state":"PAUSED","state":"RUNNING"})", "application/json");
      });
  stub.start();
  ClusterClientConfig config;
  config.max_response_bytes = 64;
  auto client = take(ClusterClient::open(stub.endpoint(), config));
  EXPECT_EQ(client->status().status().code(), ErrorCode::ResourceExhausted);
  EXPECT_FALSE(client->pause().ok());
}

TEST(ClusterClient, InvalidEndpointSourceOrOversizedRequestNeverReachesMeta) {
  for (const auto* endpoint : {"https://127.0.0.1:80", "http://user@host:80",
                               "http://127.0.0.1:80/path", "http://127.0.0.1:0"})
    EXPECT_FALSE(ClusterClient::open(endpoint).ok());
  ClusterClientConfig bad;
  bad.ingest_attempts = 0;
  EXPECT_FALSE(ClusterClient::open("http://127.0.0.1:80", bad).ok());
  MetaStub stub;
  std::atomic<int> calls = 0;
  stub.server.Post("/v1/meta/ingest",
                   [&](const httplib::Request&, httplib::Response& response) noexcept {
                     ++calls;
                     response.set_content(acknowledgement(true, false).dump(), "application/json");
                   });
  stub.start();
  ClusterClientConfig config;
  config.max_request_bytes = 64;
  auto client = take(ClusterClient::open(stub.endpoint(), config));
  EXPECT_FALSE(client->ingest("trade", {std::string("AAPL")}, {"s", "t", 0, -1}, 0).ok());
  EXPECT_FALSE(client->ingest("trade", {std::string("AAPL")}, {"s", "t", 0, INT64_MAX}, 0).ok());
  EXPECT_EQ(client->ingest("trade", {std::string("AAPL")}, {"s", "t", 0, 0}, 0).status().code(),
            ErrorCode::ResourceExhausted);
  EXPECT_EQ(calls.load(), 0);
}
} // namespace
} // namespace eventflow::test
