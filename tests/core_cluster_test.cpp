#include "core_process_harness.hpp"
#include "eventflow/cluster_client.hpp"
#include "eventflow/key_codec.hpp"
#include <array>
#include <charconv>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <set>

#ifndef EVENTFLOW_BINARY_PATH
#error "core_cluster_test requires the current eventflow binary"
#endif

namespace eventflow::test {
namespace {
// Preserve RAII child cleanup when an RPC assertion fails; the older take helper aborts.
template <class T> T checked(StatusOr<T> value) noexcept {
  if (!value.ok()) {
    ADD_FAILURE() << value.status().message();
    return T{};
  }
  return std::move(value).value();
}

Json checked(StatusOr<Json> value) noexcept {
  if (!value.ok()) {
    ADD_FAILURE() << value.status().message();
    return Json::object();
  }
  return std::move(value).value();
}

Status write_config(const std::filesystem::path& path, const Json& body) noexcept {
  std::ofstream output(path);
  output << body.dump() << '\n';
  output.close();
  return check(!output.fail(), "cannot write core process configuration");
}

StatusOr<std::uint64_t> decimal_value(const Json& value) noexcept {
  if (!value.is_string())
    return Status(ErrorCode::InvalidArgument, "expected decimal string in core report");
  const auto& bytes = value.get_ref<const std::string&>();
  std::uint64_t result = 0;
  const auto [end, error] = std::from_chars(bytes.data(), bytes.data() + bytes.size(), result);
  if (error != std::errc{} || end != bytes.data() + bytes.size() || std::to_string(result) != bytes)
    return Status(ErrorCode::InvalidArgument, "invalid decimal string in core report");
  return result;
}

struct Delivery {
  std::string relation;
  Row row;
  SourceIdentity source;
  std::int64_t now = 0;
};

class CoreCluster {
public:
  ~CoreCluster() noexcept {
    const auto result = stop();
    if (!result.ok())
      ADD_FAILURE() << result.message() << '\n' << logs();
  }

  Status start(const std::string& backend) noexcept {
    path = "/tmp/eventflow-core-cluster-XXXXXX";
    if (!::mkdtemp(path.data()))
      return Status(ErrorCode::Unavailable, "cannot create core cluster directory");
    std::error_code error;
    std::filesystem::create_directories(path + "/build/bin", error);
    if (error)
      return Status(ErrorCode::Unavailable, error.message());
    binary = path + "/build/bin/eventflow";
    std::filesystem::copy_file(EVENTFLOW_BINARY_PATH, binary, std::filesystem::copy_options::none,
                               error);
    if (error)
      return Status(ErrorCode::Unavailable, error.message());
    const auto third = std::filesystem::path(__FILE__).parent_path().parent_path() / "cpp3rdlib";
    std::filesystem::create_directory_symlink(third, path + "/cpp3rdlib", error);
    if (error)
      return Status(ErrorCode::Unavailable, error.message());
    std::cout << "core_cluster_artifacts=" << path << '\n';
    backend_kind = backend;
    return start_generation(false, {});
  }

  Status restore(const std::string& checkpoint) noexcept {
    EF_RETURN_IF_ERROR(stop());
    ++generation;
    return start_generation(true, checkpoint);
  }

  Status stop() noexcept {
    sdk.reset();
    Status first;
    const auto stop_one = [&](const std::unique_ptr<CoreProcess>& child) noexcept {
      if (!child)
        return;
      const auto stopped = child->stop();
      if (first.ok() && !stopped.ok())
        first = stopped;
    };
    stop_one(coordinator);
    for (auto& worker : computes)
      stop_one(worker);
    stop_one(kv);
    return first;
  }

  std::string logs() const noexcept {
    std::string result = "cluster artifacts: " + path + "\n";
    if (kv)
      result += kv->output();
    for (const auto& worker : computes)
      if (worker)
        result += worker->output();
    if (coordinator)
      result += coordinator->output();
    return result;
  }

  std::array<pid_t, 4> pids() const noexcept {
    return {kv->pid(), computes[0]->pid(), computes[1]->pid(), coordinator->pid()};
  }

  Status stage_and_deploy() noexcept {
    EF_ASSIGN_OR_RETURN(auto first, sdk->ddl("CREATE MATERIALIZED VIEW core_doubled AS SELECT "
                                             "ticker,volume*2 AS doubled FROM trade_volume"));
    EF_ASSIGN_OR_RETURN(auto second, sdk->ddl("CREATE MATERIALIZED VIEW core_ready AS SELECT "
                                              "ticker,doubled>=200 AS ready FROM core_doubled"));
    (void)first;
    (void)second;
    EF_ASSIGN_OR_RETURN(auto result, sdk->deploy());
    return check(result.value("state", std::string{}) == "RUNNING",
                 "core deployment did not become RUNNING");
  }

  Status send(std::string relation, Row row, std::int64_t now) noexcept {
    Delivery delivery{
        std::move(relation), std::move(row), {"core-feed", "market", 0, next_offset}, now};
    EF_ASSIGN_OR_RETURN(auto result,
                        sdk->ingest(delivery.relation, delivery.row, delivery.source, now, now));
    EF_RETURN_IF_ERROR(check(result.input_committed, "SDK source input was not confirmed"));
    EF_RETURN_IF_ERROR(result.operation_status);
    EF_ASSIGN_OR_RETURN(auto shard, shard_for(Row{delivery.row[0]}, 2));
    EF_RETURN_IF_ERROR(
        check(result.shard == shard && result.node_id == "compute-" + std::to_string(shard),
              "SDK source route differs from business hash"));
    if (deliveries.empty())
      original = delivery;
    deliveries.push_back(std::move(delivery));
    ++next_offset;
    return {};
  }

  Status require_successful_bindings(const std::string& key,
                                     std::initializer_list<std::string_view> required) noexcept {
    EF_ASSIGN_OR_RETURN(auto records,
                        sdk->query("SELECT request_id,phase FROM request_status", {key}));
    std::set<std::string> succeeded;
    for (const auto& row : records.rows) {
      if (row.size() != 2 || !std::holds_alternative<std::string>(row[0]) ||
          !std::holds_alternative<std::string>(row[1]))
        return Status(ErrorCode::Internal, "invalid feature computation request audit row");
      if (std::get<std::string>(row[1]) != "SUCCEEDED")
        continue;
      EF_ASSIGN_OR_RETURN(auto identity, parse_json_exact(std::get<std::string>(row[0])));
      if (!identity.is_array() || identity.empty() || !identity[0].is_string())
        return Status(ErrorCode::Internal, "invalid feature computation binding provenance");
      succeeded.insert(identity[0].get<std::string>());
    }
    for (const auto binding : required)
      if (!succeeded.contains(std::string(binding)))
        return Status(ErrorCode::FailedPrecondition,
                      "missing successful feature computation request binding: " +
                          std::string(binding));
    return {};
  }

  Status populate() noexcept {
    for (std::size_t shard = 0; shard < keys.size(); ++shard) {
      for (std::size_t candidate = 0; candidate < 1000; ++candidate) {
        const auto key = "CORE" + std::to_string(candidate);
        EF_ASSIGN_OR_RETURN(auto hashed, shard_for(Row{key}, 2));
        if (hashed == shard) {
          keys[shard] = key;
          break;
        }
      }
      EF_RETURN_IF_ERROR(check(!keys[shard].empty(), "cannot find fixture business key"));
    }
    for (const auto& key : keys)
      EF_RETURN_IF_ERROR(
          send("news",
               {key, std::string("news1"), std::int64_t{1}, std::string("earnings update"), 100.0},
               1'000'000));
    for (const auto& key : keys)
      EF_RETURN_IF_ERROR(
          send("history", {key, std::int64_t{1}, 99.0, std::int64_t{1'100'000}}, 1'100'000));
    for (const auto& key : keys)
      EF_RETURN_IF_ERROR(
          send("history", {key, std::int64_t{2}, 100.0, std::int64_t{1'200'000}}, 1'200'000));
    for (const auto& key : keys)
      EF_RETURN_IF_ERROR(send("quote", {key, 100.1, 100.3, std::int64_t{1'300'000}}, 1'300'000));
    for (std::size_t i = 0; i < keys.size(); ++i)
      EF_RETURN_IF_ERROR(send(
          "trade", {keys[i], std::int64_t{1}, static_cast<std::int64_t>(130 + 40 * i)}, 1'400'000));
    EF_ASSIGN_OR_RETURN(auto initial, sdk->drain(1'500'000));
    (void)initial;
    EF_ASSIGN_OR_RETURN(auto second, sdk->drain(2'000'000));
    (void)second;
    for (const auto& key : keys)
      EF_RETURN_IF_ERROR(require_successful_bindings(key, {"a", "b", "c"}));
    return {};
  }

  Status advance_clock() noexcept {
    for (const auto& key : keys)
      EF_RETURN_IF_ERROR(send("quote", {key, 100.2, 100.4, std::int64_t{4'900'000}}, 4'900'000));
    for (std::size_t i = 0; i < keys.size(); ++i) {
      EF_ASSIGN_OR_RETURN(auto status,
                          sdk->query("SELECT request_id,phase FROM request_status", {keys[i]}));
      requests_before_clock[i] = status.rows.size();
    }
    EF_ASSIGN_OR_RETURN(auto result, sdk->drain(5'000'000));
    (void)result;
    for (std::size_t i = 0; i < keys.size(); ++i) {
      EF_ASSIGN_OR_RETURN(auto status,
                          sdk->query("SELECT request_id,phase FROM request_status", {keys[i]}));
      EF_RETURN_IF_ERROR(check(status.rows.size() > requests_before_clock[i],
                               "Clock D did not create a new request"));
      EF_RETURN_IF_ERROR(require_successful_bindings(keys[i], {"d"}));
    }
    return {};
  }

  StatusOr<ClusterIngestResult> replay_original() noexcept {
    return sdk->ingest(original.relation, original.row, original.source, original.now,
                       original.now);
  }

  std::unique_ptr<ClusterClient> sdk;
  std::array<std::string, 2> keys;
  std::array<std::size_t, 2> requests_before_clock{};
  std::vector<Delivery> deliveries;
  Delivery original;
  std::int64_t next_offset = 0;
  std::string path;

private:
  Status start_generation(bool restoring, const std::string& checkpoint) noexcept {
    const auto suffix = std::to_string(generation);
    kv = std::make_unique<CoreProcess>();
    std::vector<std::string> options = {
        "kv",     "--backend",  backend_kind,
        "--port", "0",          "--shards",
        "3",      "--data-dir", path + (restoring ? "/restored" : "/original")};
    if (restoring)
      options.insert(options.end(),
                     {"--restore-from", path + "/original", "--snapshot", checkpoint});
    EF_RETURN_IF_ERROR(
        kv->start(binary, options, "EVENTFLOW_KV_READY", path + "/kv-" + suffix + ".log"));
    Json nodes = Json::array();
    for (std::uint32_t shard = 0; shard < 2; ++shard) {
      computes[shard] = std::make_unique<CoreProcess>();
      const auto name = "compute-" + std::to_string(shard);
      const auto file = path + "/" + name + "-" + suffix + ".json";
      EF_RETURN_IF_ERROR(write_config(file, {{"node_id", name},
                                             {"kv_endpoint", kv->endpoint()},
                                             {"shard_count", 2},
                                             {"shards", Json::array({shard})},
                                             {"port", 0}}));
      EF_RETURN_IF_ERROR(computes[shard]->start(binary, {"compute", "--config", file},
                                                "EVENTFLOW_COMPUTE_READY",
                                                path + "/" + name + "-" + suffix + ".log"));
      nodes.push_back({{"node_id", name},
                       {"endpoint", computes[shard]->endpoint()},
                       {"shards", Json::array({shard})}});
    }
    coordinator = std::make_unique<CoreProcess>();
    const auto file = path + "/coordinator-" + suffix + ".json";
    Json config{{"cluster_id", "core-cluster-e2e"},
                {"kv_endpoint", kv->endpoint()},
                {"shard_count", 2},
                {"nodes", nodes},
                {"tick_interval_ms", 0},
                {"port", 0}};
    if (restoring)
      config["restore_checkpoint"] = checkpoint;
    EF_RETURN_IF_ERROR(write_config(file, config));
    EF_RETURN_IF_ERROR(coordinator->start(binary, {"coordinator", "--config", file},
                                          "EVENTFLOW_COORDINATOR_READY",
                                          path + "/coordinator-" + suffix + ".log"));
    ClusterClientConfig client_config;
    client_config.timeout_seconds = 60;
    EF_ASSIGN_OR_RETURN(sdk, ClusterClient::open(coordinator->endpoint(), client_config));
    return {};
  }

  std::string binary;
  std::string backend_kind;
  unsigned generation = 0;
  std::unique_ptr<CoreProcess> kv;
  std::array<std::unique_ptr<CoreProcess>, 2> computes;
  std::unique_ptr<CoreProcess> coordinator;
};

void assert_latest_and_cascade(CoreCluster& cluster) noexcept {
  std::set<std::string> nodes;
  for (std::uint32_t shard = 0; shard < 2; ++shard) {
    for (const auto* sql :
         {"SELECT expected_return FROM latest_prediction", "SELECT risk_score FROM latest_risk"}) {
      auto result = cluster.sdk->query(sql, {cluster.keys[shard]});
      ASSERT_TRUE(result.ok()) << result.status().message() << '\n' << cluster.logs();
      ASSERT_EQ(result->rows.size(), 1U) << sql;
      ASSERT_EQ(result->rows[0].size(), 1U);
      ASSERT_TRUE(std::holds_alternative<double>(result->rows[0][0]));
      EXPECT_TRUE(std::isfinite(std::get<double>(result->rows[0][0])));
      EXPECT_EQ(result->shard, shard);
      EXPECT_EQ(result->node_id, "compute-" + std::to_string(shard));
      EXPECT_GT(result->revision, 0U);
      nodes.insert(result->node_id);
    }
    const auto doubled =
        checked(cluster.sdk->query("SELECT doubled FROM core_doubled", {cluster.keys[shard]}));
    ASSERT_EQ(doubled.rows.size(), 1U);
    EXPECT_EQ(doubled.rows[0], Row{static_cast<std::int64_t>(260 + 80 * shard)});
    const auto condition =
        checked(cluster.sdk->query("SELECT ready FROM core_ready", {cluster.keys[shard]}));
    ASSERT_EQ(condition.rows.size(), 1U);
    EXPECT_EQ(condition.rows[0], Row{true});
  }
  EXPECT_EQ(nodes.size(), 2U);
}

void assert_prefix(const Json& status, std::int64_t next_offset, std::int64_t watermark) noexcept {
  ASSERT_TRUE(status.contains("source_progress"));
  const auto& progress = status["source_progress"];
  ASSERT_TRUE(progress.is_object());
  ASSERT_EQ(progress.size(), 1U);
  const auto& position = progress.begin().value();
  EXPECT_EQ(position.value("source_id", std::string{}), "core-feed");
  EXPECT_EQ(position.value("topic", std::string{}), "market");
  EXPECT_EQ(checked(decimal_value(position["next_offset"])),
            static_cast<std::uint64_t>(next_offset));
  ASSERT_TRUE(status.contains("watermark_us"));
  EXPECT_EQ(checked(decimal_value(status["watermark_us"])), static_cast<std::uint64_t>(watermark));
}

class CoreClusterContract : public testing::TestWithParam<std::string> {};

TEST_P(CoreClusterContract,
       FourRealProcessesRunFeatureComputationClockCascadeRoutingAndSourceReplay) {
  CoreCluster cluster;
  auto started = cluster.start(GetParam());
  ASSERT_TRUE(started.ok()) << started.message() << '\n' << cluster.logs();
  const auto pids = cluster.pids();
  const std::set<pid_t> unique(pids.begin(), pids.end());
  ASSERT_EQ(unique.size(), 4U);
  EXPECT_FALSE(unique.contains(::getpid()));
  auto discovered = checked(cluster.sdk->discover());
  EXPECT_EQ(discovered.value("state", std::string{}), "STAGING");
  ASSERT_EQ(discovered["routes"].size(), 2U);
  const auto staged = cluster.stage_and_deploy();
  ASSERT_TRUE(staged.ok()) << staged.message() << '\n' << cluster.logs();
  const auto catalog = checked(cluster.sdk->catalog());
  ASSERT_TRUE(catalog["catalog"].is_string());
  ASSERT_EQ(catalog["ddl"].size(), 2U);
  auto populated = cluster.populate();
  ASSERT_TRUE(populated.ok()) << populated.message() << '\n' << cluster.logs();
  assert_latest_and_cascade(cluster);
  auto clocked = cluster.advance_clock();
  ASSERT_TRUE(clocked.ok()) << clocked.message() << '\n' << cluster.logs();
  assert_latest_and_cascade(cluster);
  const auto before = checked(
      cluster.sdk->query("SELECT expected_return FROM latest_prediction", {cluster.keys[0]}));
  auto replay = cluster.replay_original();
  ASSERT_TRUE(replay.ok()) << replay.status().message();
  EXPECT_TRUE(replay->input_committed);
  EXPECT_TRUE(replay->duplicate);
  EXPECT_TRUE(replay->operation_status.ok());
  const auto after = checked(
      cluster.sdk->query("SELECT expected_return FROM latest_prediction", {cluster.keys[0]}));
  EXPECT_EQ(after.rows, before.rows);
  EXPECT_EQ(after.revision, before.revision);
  assert_prefix(checked(cluster.sdk->status()), cluster.next_offset, 4'900'000);
  auto paused = checked(cluster.sdk->pause());
  EXPECT_EQ(paused.value("state", std::string{}), "PAUSED");
  const auto blocked =
      cluster.sdk->ingest("trade", {cluster.keys[0], std::int64_t{2}, std::int64_t{7}},
                          {"core-feed", "market", 0, cluster.next_offset}, 5'050'000, 5'050'000);
  EXPECT_FALSE(blocked.ok());
  auto resumed = checked(cluster.sdk->resume());
  EXPECT_EQ(resumed.value("state", std::string{}), "RUNNING");
  ASSERT_TRUE(
      cluster.send("trade", {cluster.keys[0], std::int64_t{2}, std::int64_t{7}}, 5'050'000).ok());
  const auto extra =
      checked(cluster.sdk->query("SELECT doubled FROM core_doubled", {cluster.keys[0]}));
  ASSERT_EQ(extra.rows.size(), 1U);
  EXPECT_EQ(extra.rows[0], Row{std::int64_t{274}});
  auto stopped = cluster.stop();
  EXPECT_TRUE(stopped.ok()) << stopped.message() << '\n' << cluster.logs();
}

INSTANTIATE_TEST_SUITE_P(MemoryAndRocks, CoreClusterContract, testing::Values("memory", "rocks"));

TEST(CoreClusterRecovery, RocksSnapshotRestartsAllProcessesAndContinuesTheExactSourcePrefix) {
  CoreCluster cluster;
  auto started = cluster.start("rocks");
  ASSERT_TRUE(started.ok()) << started.message() << '\n' << cluster.logs();
  const auto staged = cluster.stage_and_deploy();
  ASSERT_TRUE(staged.ok()) << staged.message() << '\n' << cluster.logs();
  auto populated = cluster.populate();
  ASSERT_TRUE(populated.ok()) << populated.message() << '\n' << cluster.logs();
  auto clocked = cluster.advance_clock();
  ASSERT_TRUE(clocked.ok()) << clocked.message() << '\n' << cluster.logs();
  std::array<ClusterQueryResult, 2> before;
  for (std::size_t i = 0; i < 2; ++i)
    before[i] = checked(
        cluster.sdk->query("SELECT expected_return FROM latest_prediction", {cluster.keys[i]}));
  const auto source_before = checked(cluster.sdk->status());
  auto made = cluster.sdk->checkpoint("core-cut", 5'100'000);
  ASSERT_TRUE(made.ok()) << made.status().message() << '\n' << cluster.logs();
  const auto checkpoint = *made;
  EXPECT_TRUE(checkpoint.value("complete", false));
  EXPECT_EQ(checkpoint.value("snapshot_id", std::string{}), "core-cut");
  ASSERT_TRUE(checkpoint.contains("nodes"));
  ASSERT_EQ(checkpoint["nodes"].size(), 2U);
  EXPECT_EQ(checkpoint["source_progress"], source_before["source_progress"]);
  EXPECT_EQ(checkpoint["watermark_us"], source_before["watermark_us"]);
  ASSERT_FALSE(checkpoint.value("catalog_fingerprint", std::string{}).empty());
  std::array<std::uint64_t, 2> frozen_revisions{};
  for (const auto& node : checkpoint["nodes"]) {
    EXPECT_EQ(node.value("run_state", std::string{}), "QUIESCED");
    ASSERT_EQ(node["shards"].size(), 1U);
    const auto& shard = node["shards"][0];
    const auto id = shard["shard"].get<std::uint32_t>();
    ASSERT_LT(id, 2U);
    frozen_revisions[id] = checked(decimal_value(shard["revision"]));
    EXPECT_FALSE(shard.value("pending_commit", true));
    EXPECT_EQ(checked(decimal_value(shard["processing_time_us"])), 5'100'000U);
    EXPECT_FALSE(shard.value("last_commit_id", std::string{}).empty());
  }
  const auto old_pids = cluster.pids();
  auto restored = cluster.restore("core-cut");
  ASSERT_TRUE(restored.ok()) << restored.message() << '\n' << cluster.logs();
  for (auto pid : cluster.pids())
    EXPECT_EQ(std::find(old_pids.begin(), old_pids.end(), pid), old_pids.end());
  auto deployed = cluster.sdk->deploy();
  ASSERT_TRUE(deployed.ok()) << deployed.status().message() << '\n' << cluster.logs();
  ASSERT_EQ(checked(cluster.sdk->catalog())["ddl"].size(), 2U);
  const auto source_after = checked(cluster.sdk->status());
  EXPECT_EQ(source_after["source_progress"], source_before["source_progress"]);
  EXPECT_EQ(source_after["watermark_us"], source_before["watermark_us"]);
  for (std::size_t i = 0; i < 2; ++i) {
    auto after = checked(
        cluster.sdk->query("SELECT expected_return FROM latest_prediction", {cluster.keys[i]}));
    EXPECT_EQ(after.rows, before[i].rows);
    EXPECT_EQ(after.revision, frozen_revisions[i]);
  }
  auto replay = cluster.replay_original();
  ASSERT_TRUE(replay.ok()) << replay.status().message();
  EXPECT_TRUE(replay->input_committed);
  EXPECT_TRUE(replay->duplicate);
  EXPECT_TRUE(replay->operation_status.ok());
  assert_prefix(checked(cluster.sdk->status()), cluster.next_offset, 4'900'000);
  ASSERT_TRUE(
      cluster.send("trade", {cluster.keys[0], std::int64_t{2}, std::int64_t{7}}, 5'200'000).ok());
  auto extra = checked(cluster.sdk->query("SELECT doubled FROM core_doubled", {cluster.keys[0]}));
  ASSERT_EQ(extra.rows.size(), 1U);
  EXPECT_EQ(extra.rows[0], Row{std::int64_t{274}});
  EXPECT_GT(extra.revision, frozen_revisions[0]);
  assert_prefix(checked(cluster.sdk->status()), cluster.next_offset, 5'200'000);
  auto stopped = cluster.stop();
  EXPECT_TRUE(stopped.ok()) << stopped.message() << '\n' << cluster.logs();
}
} // namespace
} // namespace eventflow::test
