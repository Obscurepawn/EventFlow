#include "eventflow/compute_service.hpp"
#include "eventflow/key_codec.hpp"
#include "eventflow/kv_protocol.hpp"
#include "legacy_feature_fixture.hpp"
#include "remote_harness.hpp"
#include "status_test_support.hpp"
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <gtest/gtest.h>
#include <httplib.h>
#include <thread>

namespace eventflow::test {
namespace {
struct HttpReply {
  int status = 0;
  Json body = Json::object();
};

HttpReply post(httplib::Client& client, const std::string& operation, const Json& body) noexcept {
  auto response = client.Post("/v1/compute/" + operation, body.dump(), "application/json");
  if (!response) {
    ADD_FAILURE() << operation << ": HTTP transport failed";
    return {};
  }
  auto parsed = Json::parse(response->body, nullptr, false);
  if (parsed.is_discarded()) {
    ADD_FAILURE() << operation << ": invalid JSON: " << response->body;
    return {response->status, Json::object()};
  }
  return {response->status, std::move(parsed)};
}

Json success(HttpReply response) noexcept {
  EXPECT_EQ(response.status, 200) << response.body.dump();
  return std::move(response.body);
}

void rejected(const HttpReply& response) noexcept {
  EXPECT_GE(response.status, 400) << response.body.dump();
  EXPECT_LT(response.status, 600);
  EXPECT_TRUE(response.body.contains("error")) << response.body.dump();
  EXPECT_TRUE(response.body.contains("error_code")) << response.body.dump();
  EXPECT_FALSE(kv_protocol::decode_status(response.body).ok());
}

std::uint64_t integer(const Json& value) noexcept {
  if (value.is_string()) {
    const auto& text = value.get_ref<const std::string&>();
    std::uint64_t result = 0;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), result);
    if (!text.empty() && (text.size() == 1 || text.front() != '0') && parsed.ec == std::errc{} &&
        parsed.ptr == text.data() + text.size())
      return result;
  }
  ADD_FAILURE() << "expected canonical unsigned decimal string, got " << value.dump();
  return 0;
}

Json install_body() noexcept {
  return {{"deployment_version", "1"},
          {"recipe", "feature_computation"},
          {"catalog_fingerprint", take(example::DemoDefinition::create()).catalog.fingerprint}};
}

Json ingest_body(std::string relation, Json row, std::int64_t now_us, std::int64_t offset,
                 std::uint32_t shard = 0) noexcept {
  return {{"deployment_version", "1"},
          {"shard", shard},
          {"relation", std::move(relation)},
          {"row", std::move(row)},
          {"source",
           {{"source_id", "coordinator-feed"},
            {"topic", "market-input"},
            {"partition", 3},
            {"offset", std::to_string(offset)}}},
          {"now_us", std::to_string(now_us)}};
}

Json query_body(const std::string& sql, std::string ticker = "T",
                std::uint32_t shard = 0) noexcept {
  return {{"deployment_version", "1"},
          {"shard", shard},
          {"scope", Json::array({std::move(ticker)})},
          {"sql", sql},
          {"max_rows", 32},
          {"max_bytes", 65536}};
}

Json shard_report(const Json& report, std::uint32_t shard = 0) noexcept {
  const auto found = report.find("shards");
  if (found != report.end() && found->is_array())
    for (const auto& item : *found)
      if (item.contains("shard") && item["shard"] == Json(shard))
        return item;
  ADD_FAILURE() << "missing shard " << shard << " in " << report.dump();
  return Json::object();
}

void expect_position(const Json& report, std::int64_t next_offset,
                     std::uint32_t shard = 0) noexcept {
  const auto state = shard_report(report, shard);
  const auto positions = state.find("source_positions");
  ASSERT_TRUE(positions != state.end()) << state.dump();
  ASSERT_TRUE(positions->is_array());
  ASSERT_EQ(positions->size(), 1U) << positions->dump();
  const auto& position = (*positions)[0];
  EXPECT_EQ(position.value("source_id", ""), "coordinator-feed");
  EXPECT_EQ(position.value("topic", ""), "market-input");
  ASSERT_TRUE(position.contains("partition"));
  EXPECT_EQ(position["partition"], Json(3));
  ASSERT_TRUE(position.contains("next_offset"));
  EXPECT_EQ(integer(position["next_offset"]), static_cast<std::uint64_t>(next_offset));
}

class ComputeServiceContract : public testing::TestWithParam<int> {
public:
  ~ComputeServiceContract() noexcept override {
    client.reset();
    service.reset();
    store.reset();
    if (!path.empty()) {
      std::error_code error;
      std::filesystem::remove_all(path, error);
    }
  }

  Status start() noexcept {
    config.node_id = "compute-test";
    config.shard_count = 1;
    config.shards = {0};
    StoreConfig storage;
    storage.shard_count = 1;
    std::unique_ptr<IStateStore> backend;
    if (GetParam() % 2 == 0) {
      EF_ASSIGN_OR_RETURN(backend, MemoryStateStore::open(storage));
    } else {
      char pattern[] = "/tmp/eventflow-compute-contract-XXXXXX";
      const auto* directory = ::mkdtemp(pattern);
      if (!directory)
        return Status(ErrorCode::Unavailable, "cannot create test directory");
      path = directory;
      EF_ASSIGN_OR_RETURN(backend, RocksStateStore::open(path + "/db", storage));
    }
    if (GetParam() >= 2) {
      EF_ASSIGN_OR_RETURN(backend, RemoteHarnessStore::open(std::move(backend)));
    }
    store = std::shared_ptr<IStateStore>(std::move(backend));
    return restart();
  }

  Status restart() noexcept {
    client.reset();
    service.reset();
    EF_ASSIGN_OR_RETURN(
        service, ComputeService::create(store, config, test_support::legacy_feature_module()));
    EF_ASSIGN_OR_RETURN(auto port, service->start("127.0.0.1", 0));
    client = std::make_unique<httplib::Client>("127.0.0.1", port);
    client->set_connection_timeout(5);
    client->set_read_timeout(20);
    return {};
  }

  HttpReply call(const std::string& operation, const Json& body) noexcept {
    return post(*client, operation, body);
  }

  Json status() noexcept {
    return success(call("status", Json::object()));
  }

  std::uint64_t revision() noexcept {
    return take(store->status(0)).revision;
  }

  Json install() noexcept {
    return success(call("install", install_body()));
  }

  void complete_c8() noexcept {
    success(call("ingest", ingest_body("news", {"T", "n1", 1, "earnings update", 100.0}, 1, 10)));
    success(call("ingest", ingest_body("history", {"T", 1, 99.0, 2}, 2, 12)));
    success(call("ingest", ingest_body("history", {"T", 2, 100.0, 3}, 3, 20)));
    success(call("ingest", ingest_body("quote", {"T", 100.1, 100.3, 4}, 4, 30)));
    for (int round = 0; round < 16; ++round) {
      const auto progress = success(
          call("pump", {{"deployment_version", "1"}, {"now_us", "5"}, {"max_functions", 128}}));
      if (!progress.value("work_remaining", true))
        return;
    }
    ADD_FAILURE()
        << "Feature computation background work did not drain within its bounded progress rounds";
  }

  ComputeServiceConfig config;
  std::shared_ptr<IStateStore> store;
  std::unique_ptr<ComputeService> service;
  std::unique_ptr<httplib::Client> client;
  std::string path;
};

TEST_P(ComputeServiceContract, HealthAndInstallExposeTheExactCatalogAndAssignedShard) {
  ASSERT_TRUE(start().ok());
  auto health = client->Get("/health");
  ASSERT_TRUE(health);
  EXPECT_EQ(health->status, 200);
  EXPECT_TRUE(service->is_running());
  const auto installed = install();
  ASSERT_TRUE(installed.contains("deployment_version"));
  EXPECT_EQ(integer(installed["deployment_version"]), 1U);
  EXPECT_EQ(installed.value("catalog_fingerprint", ""),
            install_body()["catalog_fingerprint"].get<std::string>());
  EXPECT_EQ(installed.value("run_state", ""), "RUNNING");
  ASSERT_TRUE(installed.contains("shards"));
  ASSERT_EQ(installed["shards"].size(), 1U);
  const auto state = shard_report(installed);
  ASSERT_TRUE(state.contains("revision"));
  EXPECT_EQ(integer(state["revision"]), revision());
  const auto before = revision();
  install();
  EXPECT_EQ(revision(), before);
}

TEST_P(ComputeServiceContract, WrongCatalogFingerprintAndRecipeFailBeforeAnyStoreMutation) {
  ASSERT_TRUE(start().ok());
  const auto before = revision();
  auto candidate = install_body();
  candidate["catalog_fingerprint"] = "different-catalog";
  rejected(call("install", candidate));
  EXPECT_EQ(revision(), before);
  candidate = install_body();
  candidate["recipe"] = "unknown-recipe";
  rejected(call("install", candidate));
  EXPECT_EQ(revision(), before);
  install();
  const auto installed_revision = revision();
  candidate = install_body();
  candidate["catalog_fingerprint"] = "changed-under-the-same-version";
  rejected(call("install", candidate));
  EXPECT_EQ(revision(), installed_revision);
}

TEST_P(ComputeServiceContract, RealFeatureComputationPipelineProducesPredictionAndRiskOverHttp) {
  ASSERT_TRUE(start().ok());
  install();
  complete_c8();
  for (const auto& sql : {"SELECT expected_return FROM latest_prediction WHERE ticker='T'",
                          "SELECT risk_score FROM latest_risk WHERE ticker='T'"}) {
    const auto result = success(call("query", query_body(sql)));
    ASSERT_TRUE(result.contains("fields"));
    EXPECT_EQ(result["fields"].size(), 1U);
    ASSERT_TRUE(result.contains("rows"));
    ASSERT_EQ(result["rows"].size(), 1U) << result.dump();
    ASSERT_EQ(result["rows"][0].size(), 1U);
    ASSERT_TRUE(result["rows"][0][0].is_number());
    EXPECT_TRUE(std::isfinite(result["rows"][0][0].get<double>()));
    ASSERT_TRUE(result.contains("revision"));
    EXPECT_EQ(integer(result["revision"]), revision());
    ASSERT_TRUE(result.contains("catalog_version"));
    EXPECT_GT(integer(result["catalog_version"]), 0U);
  }
  expect_position(status(), 31);
}

TEST_P(ComputeServiceContract, ExternalSourceIdentityDeduplicatesAndRejectsChangedPayload) {
  ASSERT_TRUE(start().ok());
  install();
  const auto input = ingest_body("history", {"T", 1, 99.0, 1}, 1, 12);
  const auto accepted = success(call("ingest", input));
  EXPECT_EQ(accepted.value("input_committed", false), true);
  EXPECT_EQ(accepted.value("duplicate", true), false);
  ASSERT_TRUE(accepted.contains("operation_status"));
  EXPECT_EQ(accepted["operation_status"].value("code", -1), 0);
  const auto committed_revision = revision();
  const auto duplicate = success(call("ingest", input));
  EXPECT_EQ(duplicate.value("input_committed", false), true);
  EXPECT_EQ(duplicate.value("duplicate", false), true);
  EXPECT_EQ(revision(), committed_revision);
  expect_position(status(), 13);
  auto conflicting = input;
  conflicting["row"][2] = 123.0;
  rejected(call("ingest", conflicting));
  EXPECT_EQ(revision(), committed_revision);
  expect_position(status(), 13);
  const auto result =
      success(call("query", query_body("SELECT price FROM history WHERE ticker='T'")));
  ASSERT_TRUE(result.contains("rows"));
  ASSERT_EQ(result["rows"].size(), 1U);
  EXPECT_EQ(result["rows"][0], Json::array({99.0}));
}

TEST_P(ComputeServiceContract, InvalidSchemaShardAndDeploymentVersionDoNotAdvanceState) {
  ASSERT_TRUE(start().ok());
  install();
  const auto before = revision();
  const auto input = ingest_body("history", {"T", 1, 99.0, 1}, 1, 0);
  auto invalid = input;
  invalid["row"] = {"T", "not-an-int64", 99.0, 1};
  rejected(call("ingest", invalid));
  invalid = input;
  invalid["shard"] = 1;
  rejected(call("ingest", invalid));
  invalid = input;
  invalid["deployment_version"] = "2";
  rejected(call("ingest", invalid));
  invalid = input;
  invalid["deployment_version"] = 1;
  rejected(call("ingest", invalid));
  invalid = input;
  invalid["source"]["offset"] = 0;
  rejected(call("ingest", invalid));
  invalid = input;
  invalid["now_us"] = 1;
  rejected(call("ingest", invalid));
  invalid = input;
  invalid["source"]["offset"] = "01";
  rejected(call("ingest", invalid));
  invalid = input;
  invalid["relation"] = "latest_prediction";
  rejected(call("ingest", invalid));
  auto query = query_body("SELECT price FROM history WHERE ticker='T'");
  query["deployment_version"] = "2";
  rejected(call("query", query));
  EXPECT_EQ(revision(), before);
}

TEST_P(ComputeServiceContract, PauseBlocksNewInputAndResumeRetainsCommittedSourcePosition) {
  ASSERT_TRUE(start().ok());
  install();
  success(call("ingest", ingest_body("history", {"T", 1, 99.0, 1}, 1, 10)));
  success(call("pause", {{"deployment_version", "1"}}));
  EXPECT_EQ(status().value("run_state", ""), "PAUSED");
  const auto paused_revision = revision();
  const auto input = ingest_body("history", {"T", 2, 100.0, 2}, 2, 12);
  rejected(call("ingest", input));
  EXPECT_EQ(revision(), paused_revision);
  expect_position(status(), 11);
  success(call("resume", {{"deployment_version", "1"}, {"now_us", "2"}, {"max_functions", 128}}));
  EXPECT_EQ(status().value("run_state", ""), "RUNNING");
  success(call("ingest", input));
  expect_position(status(), 13);
  const auto result =
      success(call("query", query_body("SELECT price FROM history WHERE ticker='T'")));
  ASSERT_TRUE(result.contains("rows"));
  EXPECT_EQ(result["rows"].size(), 2U);
}

TEST_P(ComputeServiceContract, CheckpointRequiresQuiescenceAndReportsAFrozenRuntimeBoundary) {
  ASSERT_TRUE(start().ok());
  install();
  complete_c8();
  rejected(call("checkpoint", {{"deployment_version", "1"}}));
  success(call("quiesce", {{"deployment_version", "1"}, {"now_us", "6"}, {"max_functions", 128}}));
  const auto checkpoint = success(call("checkpoint", {{"deployment_version", "1"}}));
  EXPECT_EQ(checkpoint.value("run_state", ""), "QUIESCED");
  expect_position(checkpoint, 31);
  const auto frozen = shard_report(checkpoint);
  ASSERT_TRUE(frozen.contains("revision"));
  const auto frozen_revision = revision();
  EXPECT_EQ(integer(frozen["revision"]), frozen_revision);
  ASSERT_TRUE(frozen.contains("processing_time_us"));
  EXPECT_GE(integer(frozen["processing_time_us"]), 6U);
  rejected(call("ingest", ingest_body("history", {"T", 3, 101.0, 7}, 7, 31)));
  rejected(call("pump", {{"deployment_version", "1"}, {"now_us", "10"}, {"max_functions", 128}}));
  EXPECT_EQ(revision(), frozen_revision);
  const auto repeated = success(call("checkpoint", {{"deployment_version", "1"}}));
  EXPECT_EQ(shard_report(repeated), frozen);
}

TEST_P(ComputeServiceContract, ReinstallAfterComputeRestartRestoresResultsAndSourceDeduplication) {
  ASSERT_TRUE(start().ok());
  install();
  complete_c8();
  const auto sql = query_body("SELECT expected_return FROM latest_prediction WHERE ticker='T'");
  const auto before = success(call("query", sql));
  success(call("quiesce", {{"deployment_version", "1"}, {"now_us", "6"}, {"max_functions", 128}}));
  ASSERT_TRUE(restart().ok());
  install();
  expect_position(status(), 31);
  success(call("resume", {{"deployment_version", "1"}, {"now_us", "6"}, {"max_functions", 128}}));
  const auto restored = success(call("query", sql));
  ASSERT_TRUE(before.contains("rows"));
  ASSERT_TRUE(restored.contains("rows"));
  EXPECT_EQ(restored["rows"], before["rows"]);
  const auto recovered_revision = revision();
  success(call("ingest", ingest_body("quote", {"T", 100.1, 100.3, 4}, 4, 30)));
  EXPECT_EQ(revision(), recovered_revision);
  expect_position(status(), 31);
}

TEST_P(ComputeServiceContract, QueryBudgetsRejectOversizedResultsWithoutMutatingState) {
  ASSERT_TRUE(start().ok());
  install();
  success(call("ingest", ingest_body("history", {"T", 1, 99.0, 1}, 1, 0)));
  success(call("ingest", ingest_body("history", {"T", 2, 100.0, 2}, 2, 1)));
  const auto before = revision();
  auto query = query_body("SELECT ticker,price FROM history WHERE ticker='T'");
  query["max_rows"] = 1;
  rejected(call("query", query));
  query["max_rows"] = 32;
  query["max_bytes"] = 1;
  rejected(call("query", query));
  EXPECT_EQ(revision(), before);
  const auto result =
      success(call("query", query_body("SELECT price FROM history WHERE ticker='T'")));
  ASSERT_TRUE(result.contains("rows"));
  EXPECT_EQ(result["rows"].size(), 2U);
}

TEST_P(ComputeServiceContract, InstalledDdlAcceptsExtraPublicInputAndComputesDependentView) {
  ASSERT_TRUE(start().ok());
  const std::vector<std::string> ddl = {
      "CREATE TABLE extra_input(ticker STRING NOT NULL,value INT64 NOT NULL,"
      "PRIMARY KEY(ticker)) PARTITION BY(ticker)",
      "CREATE MATERIALIZED VIEW extra_twice AS SELECT ticker,value * 2 AS doubled FROM "
      "extra_input"};
  const auto definition = take(example::DemoDefinition::create({}, ddl));
  const auto installed =
      success(call("install", {{"deployment_version", "1"},
                               {"recipe", "feature_computation"},
                               {"catalog_fingerprint", definition.catalog.fingerprint},
                               {"ddl", ddl}}));
  EXPECT_EQ(installed.value("catalog_fingerprint", ""), definition.catalog.fingerprint);
  const auto accepted = success(call("ingest", ingest_body("extra_input", {"T", 9}, 1, 7)));
  EXPECT_TRUE(accepted.value("input_committed", false));
  ASSERT_TRUE(accepted.contains("operation_status"));
  EXPECT_EQ(accepted["operation_status"].value("code", -1), 0);
  const auto result =
      success(call("query", query_body("SELECT doubled FROM extra_twice WHERE ticker='T'")));
  ASSERT_TRUE(result.contains("rows"));
  EXPECT_EQ(result["rows"], Json::array({Json::array({18})}));
  ASSERT_TRUE(result.contains("revision"));
  EXPECT_EQ(integer(result["revision"]), revision());
  expect_position(status(), 8);
}

TEST_P(ComputeServiceContract, ReinstallRejectsAnOwnerDifferentFromThePersistedAssignment) {
  ASSERT_TRUE(start().ok());
  install();
  const auto installed_revision = revision();
  ASSERT_GT(installed_revision, 0U);
  config.owner = "different-owner";
  ASSERT_TRUE(restart().ok());
  rejected(call("install", install_body()));
  EXPECT_EQ(revision(), installed_revision);
}

std::string backend_name(const testing::TestParamInfo<int>& value) noexcept {
  static constexpr const char* names[] = {"Memory", "Rocks", "RemoteMemory", "RemoteRocks"};
  return names[value.param];
}

TEST_P(ComputeServiceContract, InstallChecksDesiredShardCountBeforeAnyInitialization) {
  ASSERT_TRUE(start().ok());
  const auto before = revision();
  auto body = install_body();
  body["shard_count"] = 2;
  rejected(call("install", body));
  EXPECT_EQ(revision(), before);
  body["shard_count"] = 1;
  success(call("install", body));
  const auto installed = revision();
  body["shard_count"] = 2;
  rejected(call("install", body));
  EXPECT_EQ(revision(), installed);
}

TEST_P(ComputeServiceContract, ComputeUsesBackgroundWorkersBeforeAnyFunctionPump) {
  ASSERT_TRUE(start().ok());
  install();
  success(
      call("ingest", ingest_body("news", {"T", "background", 1, "earnings update", 100.0}, 1, 0)));
  Json worker = shard_report(status());
  ASSERT_TRUE(worker.contains("executor"));
  EXPECT_EQ(worker["executor"].value("mode", std::string{}), "thread_pool");
  EXPECT_EQ(worker["executor"].value("worker_count", 0U), 2U);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (integer(worker["executor"]["callback_calls"]) == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    worker = shard_report(status());
  }
  EXPECT_GE(integer(worker["executor"]["callback_calls"]), 1U);
  auto current = success(call("query", query_body("SELECT vector FROM latest_embedding")));
  ASSERT_TRUE(current.contains("rows"));
  EXPECT_TRUE(current["rows"].empty());
  success(call("pump", {{"deployment_version", "1"}, {"now_us", "2"}, {"max_functions", 128}}));
  current = success(call("query", query_body("SELECT vector FROM latest_embedding")));
  EXPECT_EQ(current["rows"].size(), 1U);
}

TEST(ComputeExecutorMode, LogicalReadyTimeDoesNotDelayBackgroundWorkersUntilTheWallClockCatchesUp) {
  StoreConfig config;
  config.shard_count = 1;
  auto store = std::shared_ptr<IStateStore>(take(MemoryStateStore::open(config)));
  EngineConfig engine;
  engine.shard_count = 1;
  engine.max_rows = 20000;
  auto demo = take(example::FeatureComputationDemo::open(store, {}, engine, {}, true));
  constexpr std::int64_t now = 4'000'000'000'000'000;
  ASSERT_TRUE(demo->ingest("news",
                           {std::string("ASYNC"), std::string("n1"), std::int64_t{1},
                            std::string("earnings update"), 100.0},
                           now)
                  .ok());
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (demo->executor_metrics().completed_tasks == 0 &&
         std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  EXPECT_EQ(std::string(demo->executor_mode()), "thread_pool");
  EXPECT_EQ(demo->executor_worker_count(), 2U);
  EXPECT_EQ(demo->executor_metrics().callback_calls, 1U);
  EXPECT_EQ(demo->executor_metrics().completed_tasks, 1U);
  EXPECT_TRUE(take(demo->query_sql("SELECT vector FROM latest_embedding WHERE ticker='ASYNC'"))
                  .rows.empty());
  for (int attempt = 0; attempt < 10 && demo->executor_metrics().active_tasks != 0; ++attempt)
    EXPECT_TRUE(demo->run_functions(now).ok());
  EXPECT_EQ(
      take(demo->query_sql("SELECT vector FROM latest_embedding WHERE ticker='ASYNC'")).rows.size(),
      1U);
}

TEST(ComputeExecutorMode, LocalDemoRetainsExplicitDeterministicExecutionByDefault) {
  StoreConfig config;
  config.shard_count = 1;
  auto store = std::shared_ptr<IStateStore>(take(MemoryStateStore::open(config)));
  auto demo = take(example::FeatureComputationDemo::open(store));
  ASSERT_TRUE(demo->ingest("news",
                           {std::string("LOCAL"), std::string("n1"), std::int64_t{1},
                            std::string("earnings update"), 100.0},
                           1)
                  .ok());
  EXPECT_EQ(std::string(demo->executor_mode()), "simple");
  EXPECT_EQ(demo->executor_worker_count(), 0U);
  EXPECT_EQ(demo->executor_metrics().callback_calls, 0U);
  EXPECT_EQ(take(demo->run_functions(2)), 1U);
  EXPECT_EQ(demo->executor_metrics().callback_calls, 1U);
}

INSTANTIATE_TEST_SUITE_P(FourKvBackends, ComputeServiceContract, testing::Values(0, 1, 2, 3),
                         backend_name);

TEST(ComputeServiceConfiguration, RejectsInvalidStaticAssignmentsAndZeroTransportBudgets) {
  StoreConfig storage;
  storage.shard_count = 2;
  auto store = std::shared_ptr<IStateStore>(take(MemoryStateStore::open(storage)));
  ComputeServiceConfig config;
  config.node_id = "compute-0";
  config.shard_count = 2;
  config.shards = {0};
  EXPECT_FALSE(ComputeService::create(nullptr, config, test_support::legacy_feature_module()).ok());
  auto invalid = config;
  invalid.shards = {0, 0};
  EXPECT_FALSE(ComputeService::create(store, invalid, test_support::legacy_feature_module()).ok());
  invalid = config;
  invalid.shards = {2};
  EXPECT_FALSE(ComputeService::create(store, invalid, test_support::legacy_feature_module()).ok());
  invalid = config;
  invalid.max_request_bytes = 0;
  EXPECT_FALSE(ComputeService::create(store, invalid, test_support::legacy_feature_module()).ok());
  invalid = config;
  invalid.worker_threads = 0;
  EXPECT_FALSE(ComputeService::create(store, invalid, test_support::legacy_feature_module()).ok());
  EXPECT_EQ(take(store->status(0)).revision, 0U);
  EXPECT_EQ(take(store->status(1)).revision, 0U);
}

TEST(ComputeServiceRouting, TwoServicesWriteOnlyTheirAssignedHashShards) {
  StoreConfig storage;
  storage.shard_count = 2;
  auto store = std::shared_ptr<IStateStore>(take(MemoryStateStore::open(storage)));
  ComputeServiceConfig first_config;
  first_config.node_id = "compute-0";
  first_config.shard_count = 2;
  first_config.shards = {0};
  auto second_config = first_config;
  second_config.node_id = "compute-1";
  second_config.shards = {1};
  auto first =
      take(ComputeService::create(store, first_config, test_support::legacy_feature_module()));
  auto second =
      take(ComputeService::create(store, second_config, test_support::legacy_feature_module()));
  const auto first_port = take(first->start("127.0.0.1", 0));
  const auto second_port = take(second->start("127.0.0.1", 0));
  httplib::Client first_client("127.0.0.1", first_port);
  httplib::Client second_client("127.0.0.1", second_port);
  success(post(first_client, "install", install_body()));
  success(post(second_client, "install", install_body()));
  std::string tickers[2];
  for (std::size_t index = 0; index < 100 && (tickers[0].empty() || tickers[1].empty()); ++index) {
    const auto ticker = "symbol-" + std::to_string(index);
    tickers[take(shard_for({ticker}, 2))] = ticker;
  }
  ASSERT_FALSE(tickers[0].empty());
  ASSERT_FALSE(tickers[1].empty());
  const auto first_input = ingest_body("history", {tickers[0], 1, 99.0, 1}, 1, 10, 0);
  const auto second_input = ingest_body("history", {tickers[1], 1, 100.0, 1}, 1, 12, 1);
  const auto first_before = take(store->status(0)).revision;
  const auto second_before = take(store->status(1)).revision;
  rejected(post(first_client, "ingest", second_input));
  rejected(post(second_client, "ingest", first_input));
  auto wrong_hash = second_input;
  wrong_hash["shard"] = 0;
  rejected(post(first_client, "ingest", wrong_hash));
  EXPECT_EQ(take(store->status(0)).revision, first_before);
  EXPECT_EQ(take(store->status(1)).revision, second_before);
  success(post(first_client, "ingest", first_input));
  EXPECT_GT(take(store->status(0)).revision, first_before);
  EXPECT_EQ(take(store->status(1)).revision, second_before);
  success(post(second_client, "ingest", second_input));
  for (std::uint32_t shard = 0; shard < 2; ++shard) {
    auto& client = shard == 0 ? first_client : second_client;
    const auto sql = "SELECT ticker,price FROM history WHERE ticker='" + tickers[shard] + "'";
    const auto result = success(post(client, "query", query_body(sql, tickers[shard], shard)));
    ASSERT_TRUE(result.contains("rows"));
    ASSERT_EQ(result["rows"].size(), 1U);
    EXPECT_EQ(result["rows"][0], Json::array({tickers[shard], shard == 0 ? 99.0 : 100.0}));
    expect_position(success(post(client, "status", Json::object())), shard == 0 ? 11 : 13, shard);
  }
}
} // namespace
} // namespace eventflow::test
