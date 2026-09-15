#include "eventflow/catalog_codec.hpp"
#include "eventflow/cluster_client.hpp"
#include "eventflow/compute_service.hpp"
#include "eventflow/coordinator_service.hpp"
#include "eventflow/key_codec.hpp"
#include "eventflow/kv_protocol.hpp"
#include "legacy_feature_fixture.hpp"
#include "status_test_support.hpp"
#include <array>
#include <atomic>
#include <charconv>
#include <gtest/gtest.h>
#include <httplib.h>
#include <thread>

namespace eventflow::test {
namespace {
const std::array<std::string, 3> staged_sql{
    "CREATE TABLE sdk_ticks(ticker STRING NOT NULL,id INT64 NOT NULL,qty INT64 NOT NULL,PRIMARY "
    "KEY(ticker,id)) PARTITION BY(ticker)",
    "CREATE MATERIALIZED VIEW sdk_totals AS SELECT ticker,SUM(qty) AS qty FROM sdk_ticks GROUP BY "
    "ticker",
    "CREATE MATERIALIZED VIEW sdk_twice AS SELECT ticker,qty*2 AS double_qty FROM sdk_totals"};

std::shared_ptr<MemoryStateStore> memory_store() noexcept {
  StoreConfig config;
  config.shard_count = 3;
  return std::shared_ptr<MemoryStateStore>(take(MemoryStateStore::open(config)));
}

std::string key_for(std::uint32_t shard) noexcept {
  for (std::size_t i = 0; i < 1000; ++i) {
    auto key = "SDK" + std::to_string(i);
    if (take(shard_for(Row{key}, 2)) == shard)
      return key;
  }
  ADD_FAILURE() << "could not find a fixture scope for shard";
  return {};
}

std::uint64_t number(const Json& value) noexcept {
  if (!value.is_string()) {
    ADD_FAILURE() << "expected decimal metadata, got " << value.dump();
    return 0;
  }
  const auto& text = value.get_ref<const std::string&>();
  std::uint64_t result = 0;
  const auto parsed = std::from_chars(text.data(), text.data() + text.size(), result);
  if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size() ||
      std::to_string(result) != text) {
    ADD_FAILURE() << "noncanonical metadata";
    return 0;
  }
  return result;
}

std::string source_key(const SourceIdentity& source) noexcept {
  return Json::array({source.source_id, source.topic, source.partition}).dump();
}

class PreComputeFailureProxy {
public:
  ~PreComputeFailureProxy() noexcept {
    server_.stop();
    if (listener_.joinable())
      listener_.join();
  }

  Status start(std::string target) noexcept {
    target_ = std::move(target);
    server_.new_task_queue = []() noexcept { return new httplib::ThreadPool(2); };
    server_.Post(R"(/v1/compute/(\w+))", [this](const httplib::Request& request,
                                                httplib::Response& response) noexcept {
      const bool ingest = request.path.ends_with("/ingest");
      if (ingest && drop_.exchange(false)) {
        response.status = 503;
        response.set_content(
            kv_protocol::encode_status({ErrorCode::Unavailable, "injected before Compute delivery"})
                .dump(),
            "application/json");
        return;
      }
      if (ingest)
        ++forwarded_;
      httplib::Client client(target_);
      client.set_connection_timeout(5);
      client.set_read_timeout(30);
      client.set_write_timeout(30);
      auto reply = client.Post(request.path, request.body, "application/json");
      if (!reply) {
        response.status = 503;
        response.set_content(
            kv_protocol::encode_status({ErrorCode::Unavailable, "proxy upstream unavailable"})
                .dump(),
            "application/json");
        return;
      }
      response.status = reply->status;
      response.set_content(reply->body, "application/json");
    });
    port_ = server_.bind_to_any_port("127.0.0.1");
    if (port_ <= 0)
      return Status(ErrorCode::Unavailable, "cannot bind Compute failure proxy");
    listener_ = std::thread([this]() noexcept { server_.listen_after_bind(); });
    server_.wait_until_ready();
    return {};
  }

  void drop_next_ingest() noexcept {
    drop_ = true;
  }

  std::size_t forwarded_ingests() const noexcept {
    return forwarded_.load();
  }

  std::string endpoint() const noexcept {
    return "http://127.0.0.1:" + std::to_string(port_);
  }

private:
  httplib::Server server_;
  std::thread listener_;
  std::string target_;
  int port_ = 0;
  std::atomic<bool> drop_{false};
  std::atomic<std::size_t> forwarded_{0};
};

class ClusterFixture {
public:
  ~ClusterFixture() noexcept {
    stop();
  }

  Status start(std::shared_ptr<IStateStore> store, std::string restore = {},
               bool proxy_first = false, std::uint32_t second_shard_count = 2) noexcept {
    backing = std::move(store);
    CoordinatorServiceConfig meta;
    meta.shard_count = 2;
    meta.tick_interval_ms = 0;
    meta.restore_checkpoint = std::move(restore);
    for (std::uint32_t i = 0; i < 2; ++i) {
      ComputeServiceConfig config;
      config.node_id = i == 0 ? "compute-a" : "compute-b";
      config.shard_count = i == 1 ? second_shard_count : 2;
      config.shards = {i};
      config.worker_threads = 2;
      config.max_queued_requests = 16;
      EF_ASSIGN_OR_RETURN(computes[i], ComputeService::create(
                                           backing, config, test_support::legacy_feature_module()));
      EF_ASSIGN_OR_RETURN(auto port, computes[i]->start("127.0.0.1", 0));
      auto endpoint = "http://127.0.0.1:" + std::to_string(port);
      if (i == 0 && proxy_first) {
        proxy = std::make_unique<PreComputeFailureProxy>();
        EF_RETURN_IF_ERROR(proxy->start(endpoint));
        endpoint = proxy->endpoint();
      }
      meta.nodes.push_back({config.node_id, std::move(endpoint), {i}});
    }
    EF_ASSIGN_OR_RETURN(coordinator, CoordinatorService::create(
                                         backing, meta, test_support::legacy_feature_module()));
    EF_ASSIGN_OR_RETURN(auto port, coordinator->start("127.0.0.1", 0));
    ClusterClientConfig client_config;
    client_config.timeout_seconds = 30;
    EF_ASSIGN_OR_RETURN(
        client, ClusterClient::open("http://127.0.0.1:" + std::to_string(port), client_config));
    return {};
  }

  void stop() noexcept {
    client.reset();
    coordinator.reset();
    proxy.reset();
    for (auto& compute : computes)
      compute.reset();
  }

  Status stage() noexcept {
    for (const auto& sql : staged_sql) {
      EF_ASSIGN_OR_RETURN(auto reply, client->ddl(sql));
      (void)reply;
    }
    return {};
  }

  Status deploy() noexcept {
    EF_ASSIGN_OR_RETURN(auto reply, client->deploy());
    return check(reply.value("state", std::string{}) == "RUNNING", "cluster did not become ready");
  }

  StatusOr<ClusterIngestResult> put(const std::string& key, std::int64_t id, std::int64_t qty,
                                    const SourceIdentity& source, std::int64_t now,
                                    std::optional<std::int64_t> event_time = {}) noexcept {
    return client->ingest("sdk_ticks", {key, id, qty}, source, now, event_time);
  }

  StatusOr<ClusterQueryResult> query(const std::string& key) noexcept {
    return client->query("SELECT double_qty FROM sdk_twice", {key}, 16, 65536);
  }

  std::shared_ptr<IStateStore> backing;
  std::array<std::unique_ptr<ComputeService>, 2> computes;
  std::unique_ptr<CoordinatorService> coordinator;
  std::unique_ptr<ClusterClient> client;
  std::unique_ptr<PreComputeFailureProxy> proxy;
};

void expect_value(const ClusterQueryResult& query, std::int64_t expected,
                  std::uint32_t shard) noexcept {
  ASSERT_EQ(query.rows.size(), 1);
  ASSERT_EQ(query.rows[0].size(), 1);
  ASSERT_TRUE(std::holds_alternative<std::int64_t>(query.rows[0][0]));
  EXPECT_EQ(std::get<std::int64_t>(query.rows[0][0]), expected);
  EXPECT_EQ(query.shard, shard);
  EXPECT_EQ(query.node_id, shard == 0 ? "compute-a" : "compute-b");
}

class MetaUnknownStore final : public IStateStore {
public:
  explicit MetaUnknownStore(std::shared_ptr<IStateStore> inner) noexcept
      : inner_(std::move(inner)) {}

  void arm(int successful_controls_before_unknown = 0) noexcept {
    remaining_ = successful_controls_before_unknown;
  }

  StatusOr<ReadResult> get_many(std::uint32_t shard,
                                const std::vector<std::string>& keys) noexcept override {
    return inner_->get_many(shard, keys);
  }

  StatusOr<ScanResult> scan(std::uint32_t shard, const std::string& prefix,
                            const std::optional<std::string>& after,
                            std::size_t limit) noexcept override {
    return inner_->scan(shard, prefix, after, limit);
  }

  StatusOr<ShardStatus> status(std::uint32_t shard) noexcept override {
    return inner_->status(shard);
  }

  StatusOr<StoreLimits> limits() noexcept override {
    return inner_->limits();
  }

  void arm_snapshot() noexcept {
    lose_snapshot_ = true;
  }

  void arm_snapshot_before_creation() noexcept {
    fail_snapshot_before_ = true;
  }

  StatusOr<StoreSnapshot> create_snapshot(const std::string& id) noexcept override {
    if (fail_snapshot_before_.exchange(false))
      return Status(ErrorCode::Unavailable, "injected before snapshot creation");
    EF_ASSIGN_OR_RETURN(auto snapshot, inner_->create_snapshot(id));
    if (lose_snapshot_.exchange(false))
      return Status(ErrorCode::Unavailable, "injected snapshot reply loss after creation");
    return snapshot;
  }

  StatusOr<StoreSnapshot> snapshot_status(const std::string& id) noexcept override {
    return inner_->snapshot_status(id);
  }

  StatusOr<CommitResult> commit(const CommitBatch& batch) noexcept override {
    bool lose = false;
    if (batch.shard == 2) {
      const auto count = remaining_.load();
      if (count >= 0) {
        remaining_ = count - 1;
        lose = count == 0;
      }
    }
    EF_ASSIGN_OR_RETURN(auto result, inner_->commit(batch));
    if (lose && result.code == CommitCode::Committed)
      return CommitResult{CommitCode::Unknown, result.revision, false,
                          "injected Meta acknowledgement loss"};
    return result;
  }

private:
  std::shared_ptr<IStateStore> inner_;
  std::atomic<int> remaining_{-1};
  std::atomic<bool> lose_snapshot_{false};
  std::atomic<bool> fail_snapshot_before_{false};
};

TEST(CoordinatorService, StagedSqlDeploysOneCatalogAndRoutesCascadesThroughTheSdk) {
  ClusterFixture cluster;
  ASSERT_TRUE(cluster.start(memory_store()).ok());
  auto initial = take(cluster.client->discover());
  EXPECT_EQ(initial["state"], "STAGING");
  ASSERT_TRUE(cluster.stage().ok());
  auto catalog = take(cluster.client->catalog());
  auto decoded = take(decode_catalog(catalog["catalog"].get<std::string>()));
  EXPECT_TRUE(decoded.find("sdk_ticks").ok());
  EXPECT_TRUE(decoded.find("sdk_twice").ok());
  ASSERT_TRUE(cluster.deploy().ok());
  const auto a = key_for(0), b = key_for(1);
  auto first = take(cluster.put(a, 1, 10, {"feed", "ticks", 0, 0}, 10));
  auto second = take(cluster.put(b, 1, 25, {"feed", "ticks", 0, 1}, 11));
  EXPECT_TRUE(first.input_committed);
  EXPECT_TRUE(second.input_committed);
  EXPECT_NE(first.node_id, second.node_id);
  expect_value(take(cluster.query(a)), 20, 0);
  expect_value(take(cluster.query(b)), 50, 1);
  auto status = take(cluster.client->discover());
  EXPECT_EQ(status["state"], "RUNNING");
  const SourceIdentity source{"feed", "ticks", 0, 0};
  EXPECT_EQ(number(status["source_progress"][source_key(source)]["next_offset"]), 2);
  EXPECT_FALSE(
      cluster.client->ddl("CREATE MATERIALIZED VIEW online AS SELECT * FROM sdk_ticks").ok());
}

TEST(CoordinatorService, SourceRetryIsAcknowledgedOnceAndChangedPayloadCannotAdvancePrefix) {
  ClusterFixture cluster;
  ASSERT_TRUE(cluster.start(memory_store()).ok());
  ASSERT_TRUE(cluster.stage().ok());
  ASSERT_TRUE(cluster.deploy().ok());
  const auto key = key_for(0);
  const SourceIdentity source{"source", "ticks", 3, 41};
  auto original = take(cluster.put(key, 1, 10, source, 100));
  EXPECT_TRUE(original.input_committed);
  const auto revision = take(cluster.backing->status(0)).revision;
  auto replay = take(cluster.put(key, 1, 10, source, 100));
  EXPECT_TRUE(replay.input_committed);
  EXPECT_TRUE(replay.duplicate);
  EXPECT_EQ(take(cluster.backing->status(0)).revision, revision);
  auto conflict = cluster.put(key, 1, 99, source, 100);
  ASSERT_FALSE(conflict.ok());
  EXPECT_EQ(conflict.status().code(), ErrorCode::Conflict);
  expect_value(take(cluster.query(key)), 20, 0);
  auto status = take(cluster.client->status());
  EXPECT_EQ(number(status["source_progress"][source_key(source)]["next_offset"]), 42);
  auto next = take(cluster.put(key, 2, 5, {"source", "ticks", 3, 42}, 101));
  EXPECT_TRUE(next.input_committed);
  expect_value(take(cluster.query(key)), 30, 0);
}

TEST(CoordinatorService, PausedCheckpointRetriesAndFrozenMemoryRestoreContinueTheSameSource) {
  auto original = memory_store();
  auto store = std::make_shared<MetaUnknownStore>(original);
  ClusterFixture cluster;
  ASSERT_TRUE(cluster.start(store).ok());
  ASSERT_TRUE(cluster.stage().ok());
  ASSERT_TRUE(cluster.deploy().ok());
  const auto key = key_for(0);
  const SourceIdentity source{"restore-source", "ticks", 0, 0};
  EXPECT_TRUE(take(cluster.put(key, 1, 10, source, 10)).input_committed);
  EXPECT_EQ(take(cluster.client->pause())["state"], "PAUSED");
  auto paused = cluster.put(key, 2, 15, {"restore-source", "ticks", 0, 1}, 11);
  ASSERT_FALSE(paused.ok());
  EXPECT_EQ(paused.status().code(), ErrorCode::FailedPrecondition);
  expect_value(take(cluster.query(key)), 20, 0);
  store->arm_snapshot();
  auto lost = cluster.client->checkpoint("core-memory-cp", 20);
  ASSERT_FALSE(lost.ok());
  EXPECT_EQ(lost.status().code(), ErrorCode::Unavailable);
  auto retried = cluster.client->checkpoint("core-memory-cp", 20);
  ASSERT_TRUE(retried.ok()) << retried.status().message();
  auto checkpoint = std::move(retried).value();
  EXPECT_TRUE(checkpoint.value("complete", false));
  EXPECT_EQ(checkpoint["snapshot_id"], "core-memory-cp");
  ASSERT_TRUE(checkpoint["nodes"].is_array());
  ASSERT_EQ(checkpoint["nodes"].size(), 2);
  EXPECT_EQ(take(cluster.client->checkpoint("core-memory-cp", 20)), checkpoint);
  EXPECT_EQ(take(cluster.client->status())["state"], "QUIESCED");
  auto snapshot = take(original->snapshot_status("core-memory-cp"));
  ASSERT_EQ(snapshot.shards.size(), 3);
  for (const auto& node : checkpoint["nodes"])
    for (const auto& report : node["shards"]) {
      const auto shard = report["shard"].get<std::uint32_t>();
      ASSERT_LT(shard, 2);
      EXPECT_EQ(snapshot.shards[shard].revision, number(report["revision"]));
    }
  cluster.stop();
  auto restored = std::shared_ptr<MemoryStateStore>(
      take(MemoryStateStore::from_snapshot(*original, "core-memory-cp")));
  ClusterFixture restarted;
  ASSERT_TRUE(restarted.start(restored, "core-memory-cp").ok());
  ASSERT_TRUE(restarted.deploy().ok());
  expect_value(take(restarted.query(key)), 20, 0);
  auto status = take(restarted.client->status());
  EXPECT_EQ(number(status["source_progress"][source_key(source)]["next_offset"]), 1);
  EXPECT_TRUE(
      take(restarted.put(key, 2, 15, {"restore-source", "ticks", 0, 1}, 30)).input_committed);
  expect_value(take(restarted.query(key)), 50, 0);
}

TEST(CoordinatorService, MissingAssignedWorkerNeverPublishesRunningOrAcceptsInput) {
  ClusterFixture cluster;
  ASSERT_TRUE(cluster.start(memory_store()).ok());
  cluster.computes[1]->stop();
  auto deployed = cluster.client->deploy();
  ASSERT_FALSE(deployed.ok());
  EXPECT_NE(take(cluster.client->discover())["state"], "RUNNING");
  auto input = cluster.client->ingest("trade", {key_for(0), std::int64_t(1), std::int64_t(10)},
                                      {"missing", "ticks", 0, 0}, 1);
  ASSERT_FALSE(input.ok());
  EXPECT_EQ(input.status().code(), ErrorCode::FailedPrecondition);
}

TEST(CoordinatorService, NewPartitionAndLateEventCannotRewindWatermarkOrProcessingClock) {
  ClusterFixture cluster;
  ASSERT_TRUE(cluster.start(memory_store()).ok());
  ASSERT_TRUE(cluster.stage().ok());
  ASSERT_TRUE(cluster.deploy().ok());
  const auto key = key_for(0);
  EXPECT_TRUE(
      take(cluster.put(key, 1, 1, {"watermark", "ticks", 0, 0}, 1000, 900)).input_committed);
  EXPECT_EQ(number(take(cluster.client->status())["watermark_us"]), 900);
  EXPECT_TRUE(
      take(cluster.put(key, 2, 1, {"watermark", "ticks", 1, 0}, 1010, 100)).input_committed);
  EXPECT_EQ(number(take(cluster.client->status())["watermark_us"]), 900);
  EXPECT_TRUE(
      take(cluster.put(key, 3, 1, {"watermark", "ticks", 1, 1}, 1020, 950)).input_committed);
  EXPECT_TRUE(
      take(cluster.put(key, 4, 1, {"watermark", "ticks", 0, 1}, 1030, 100)).input_committed);
  EXPECT_EQ(number(take(cluster.client->status())["watermark_us"]), 900);
  EXPECT_TRUE(
      take(cluster.put(key, 5, 1, {"watermark", "ticks", 0, 2}, 1200, 1100)).input_committed);
  EXPECT_EQ(number(take(cluster.client->status())["watermark_us"]), 950);
  EXPECT_TRUE(cluster.client->pump(1500).ok());
  auto backwards = cluster.client->pump(1400);
  ASSERT_FALSE(backwards.ok());
  auto status = take(cluster.client->status());
  EXPECT_EQ(number(status["now_us"]), 1500);
  EXPECT_EQ(number(status["watermark_us"]), 950);
}

TEST(CoordinatorService, MetaUnknownResolutionKeepsCatalogAndConfirmedSourcePrefix) {
  auto store = std::make_shared<MetaUnknownStore>(memory_store());
  ClusterFixture cluster;
  ASSERT_TRUE(cluster.start(store).ok());
  store->arm();
  auto ddl = cluster.client->ddl(staged_sql[0]);
  ASSERT_FALSE(ddl.ok());
  EXPECT_EQ(ddl.status().code(), ErrorCode::UnknownCommit);
  auto catalog = take(cluster.client->catalog());
  EXPECT_EQ(catalog["ddl"].size(), 1);
  auto decoded = take(decode_catalog(catalog["catalog"].get<std::string>()));
  EXPECT_TRUE(decoded.find("sdk_ticks").ok());
  EXPECT_TRUE(cluster.client->ddl(staged_sql[0]).ok());
  for (std::size_t i = 1; i < staged_sql.size(); ++i)
    EXPECT_TRUE(cluster.client->ddl(staged_sql[i]).ok());
  ASSERT_TRUE(cluster.deploy().ok());
  const auto key = key_for(0);
  const SourceIdentity source{"unknown", "ticks", 0, 0};
  store->arm(1);
  auto input = cluster.put(key, 1, 10, source, 10);
  ASSERT_FALSE(input.ok());
  EXPECT_EQ(input.status().code(), ErrorCode::UnknownCommit);
  auto replay = take(cluster.put(key, 1, 10, source, 10));
  EXPECT_TRUE(replay.input_committed);
  EXPECT_TRUE(replay.duplicate);
  expect_value(take(cluster.query(key)), 20, 0);
  auto status = take(cluster.client->status());
  EXPECT_EQ(number(status["source_progress"][source_key(source)]["next_offset"]), 1);
}

TEST(CoordinatorService, ServerClockAssignsDispatchTimeAndOriginalRetryKeepsItsReceipt) {
  ClusterFixture cluster;
  ASSERT_TRUE(cluster.start(memory_store()).ok());
  ASSERT_TRUE(cluster.stage().ok());
  ASSERT_TRUE(cluster.deploy().ok());
  ASSERT_TRUE(cluster.client->pump(100).ok());
  const auto key = key_for(0);
  const auto other_key = key_for(1);
  const SourceIdentity source{"clock-source", "ticks", 0, 0};
  auto first = take(cluster.put(key, 1, 10, source, 50, 40));
  EXPECT_TRUE(first.input_committed);
  EXPECT_FALSE(first.duplicate);
  EXPECT_EQ(number(first.details["processing_time_us"]), 100);
  EXPECT_EQ(number(first.details["shard_status"]["processing_time_us"]), 100);
  expect_value(take(cluster.query(key)), 20, 0);
  ASSERT_TRUE(cluster.client->pump(200).ok());
  const auto before_a = take(cluster.backing->status(0)).revision;
  const auto before_b = take(cluster.backing->status(1)).revision;
  auto replay = take(cluster.put(key, 1, 10, source, 50, 40));
  EXPECT_TRUE(replay.input_committed);
  EXPECT_TRUE(replay.duplicate);
  EXPECT_EQ(number(replay.details["processing_time_us"]), 100);
  EXPECT_EQ(number(replay.details["shard_status"]["processing_time_us"]), 200);
  EXPECT_EQ(take(cluster.backing->status(0)).revision, before_a);
  expect_value(take(cluster.query(key)), 20, 0);
  auto changed_time = cluster.put(key, 1, 10, source, 51, 40);
  ASSERT_FALSE(changed_time.ok());
  EXPECT_EQ(changed_time.status().code(), ErrorCode::Conflict);
  auto changed_event_time = cluster.put(key, 1, 10, source, 50, 41);
  ASSERT_FALSE(changed_event_time.ok());
  EXPECT_EQ(changed_event_time.status().code(), ErrorCode::Conflict);
  auto changed_shard = cluster.put(other_key, 1, 10, source, 50, 40);
  ASSERT_FALSE(changed_shard.ok());
  EXPECT_EQ(changed_shard.status().code(), ErrorCode::Conflict);
  EXPECT_EQ(take(cluster.backing->status(0)).revision, before_a);
  EXPECT_EQ(take(cluster.backing->status(1)).revision, before_b);
  EXPECT_TRUE(take(cluster.query(other_key)).rows.empty());
  const auto status = take(cluster.client->status());
  EXPECT_EQ(number(status["now_us"]), 200);
  EXPECT_EQ(number(status["source_progress"][source_key(source)]["next_offset"]), 1);
}

TEST(CoordinatorService, PendingDeliveryBeforeComputeBlocksClockUntilTheOriginalInputCommits) {
  ClusterFixture cluster;
  ASSERT_TRUE(cluster.start(memory_store(), {}, true).ok());
  ASSERT_TRUE(cluster.stage().ok());
  ASSERT_TRUE(cluster.deploy().ok());
  ASSERT_TRUE(cluster.proxy);
  const auto key = key_for(0);
  const SourceIdentity source{"pre-compute", "ticks", 0, 0};
  const auto before = take(cluster.backing->status(0)).revision;
  cluster.proxy->drop_next_ingest();
  auto first = cluster.put(key, 1, 10, source, 100);
  ASSERT_FALSE(first.ok());
  EXPECT_EQ(first.status().code(), ErrorCode::Unavailable);
  EXPECT_EQ(cluster.proxy->forwarded_ingests(), 0);
  EXPECT_EQ(take(cluster.backing->status(0)).revision, before);
  EXPECT_FALSE(take(cluster.client->status())["source_progress"].contains(source_key(source)));
  auto pumped = cluster.client->pump(200);
  ASSERT_TRUE(pumped.ok()) << pumped.status().message();
  EXPECT_EQ(number((*pumped)["now_us"]), 100);
  EXPECT_TRUE(pumped->value("clock_blocked_by_pending_input", false));
  for (const auto& node : (*pumped)["nodes"])
    for (const auto& shard : node["shards"])
      EXPECT_LE(number(shard["processing_time_us"]), 100);
  auto retried = cluster.put(key, 1, 10, source, 100);
  EXPECT_TRUE(retried.ok()) << retried.status().message();
  if (retried.ok())
    EXPECT_TRUE(retried->input_committed);
  auto status = take(cluster.client->status());
  EXPECT_EQ(number(status["source_progress"][source_key(source)]["next_offset"]), 1);
  auto next = cluster.put(key, 2, 5, {"pre-compute", "ticks", 0, 1}, 201);
  ASSERT_TRUE(next.ok()) << next.status().message();
  EXPECT_TRUE(next->input_committed);
  expect_value(take(cluster.query(key)), 30, 0);
  EXPECT_EQ(
      number(take(cluster.client->status())["source_progress"][source_key(source)]["next_offset"]),
      2);
}

TEST(CoordinatorService, SnapshotFailureBeforeCreationFreezesTheOriginalControlIntent) {
  auto store = std::make_shared<MetaUnknownStore>(memory_store());
  ClusterFixture cluster;
  ASSERT_TRUE(cluster.start(store).ok());
  ASSERT_TRUE(cluster.stage().ok());
  ASSERT_TRUE(cluster.deploy().ok());
  const auto key = key_for(0);
  EXPECT_TRUE(
      take(cluster.put(key, 1, 10, {"snapshot-before", "ticks", 0, 0}, 10)).input_committed);
  store->arm_snapshot_before_creation();
  auto failed = cluster.client->checkpoint("before-create", 20);
  ASSERT_FALSE(failed.ok());
  EXPECT_EQ(failed.status().code(), ErrorCode::Unavailable);
  EXPECT_EQ(store->snapshot_status("before-create").status().code(), ErrorCode::NotFound);
  const auto control_revision = take(store->status(2)).revision;
  const auto a_revision = take(store->status(0)).revision;
  const auto b_revision = take(store->status(1)).revision;
  const auto unchanged = [&](const StatusOr<Json>& operation) noexcept {
    if (operation.ok())
      EXPECT_EQ(operation->value("state", std::string{}), "QUIESCED");
    EXPECT_EQ(take(store->status(2)).revision, control_revision);
    EXPECT_EQ(take(store->status(0)).revision, a_revision);
    EXPECT_EQ(take(store->status(1)).revision, b_revision);
    EXPECT_EQ(take(cluster.client->status())["state"], "QUIESCED");
  };
  unchanged(cluster.client->pause());
  unchanged(cluster.client->deploy());
  unchanged(cluster.client->pump(21));
  unchanged(cluster.client->resume());
  auto retried = cluster.client->checkpoint("before-create", 20);
  ASSERT_TRUE(retried.ok()) << retried.status().message();
  EXPECT_TRUE(retried->value("complete", false));
  EXPECT_EQ(number((*retried)["control_revision"]), control_revision);
  auto snapshot = take(store->snapshot_status("before-create"));
  EXPECT_EQ(snapshot.shards[2].revision, control_revision);
  expect_value(take(cluster.query(key)), 20, 0);
}

TEST(CoordinatorService, MismatchedComputeLogicalShardCountRejectsBeforeWrongShardInitialization) {
  auto store = memory_store();
  ClusterFixture cluster;
  ASSERT_TRUE(cluster.start(store, {}, false, 3).ok());
  EXPECT_EQ(take(store->status(1)).revision, 0);
  auto deployed = cluster.client->deploy();
  EXPECT_FALSE(deployed.ok());
  EXPECT_NE(take(cluster.client->discover())["state"], "RUNNING");
  EXPECT_EQ(take(store->status(1)).revision, 0);
}

TEST(CoordinatorService, NativeUdfUnknownCommitRetainsArtifactAndImmutableRegistration) {
  auto original = memory_store();
  auto store = std::make_shared<MetaUnknownStore>(original);
  CoordinatorServiceConfig config;
  config.shard_count = 2;
  config.nodes = {{.node_id = "compute", .endpoint = "http://127.0.0.1:1", .shards = {0, 1}}};
  config.max_request_bytes = 4 * 1024 * 1024;
  config.allow_native_udfs = true;
  auto coordinator = take(CoordinatorService::create(store, config));
  const auto port = take(coordinator->start("127.0.0.1", 0));
  auto client = take(ClusterClient::open("http://127.0.0.1:" + std::to_string(port)));
  const auto before = take(store->status(2)).revision;
  store->arm();
  const auto unknown = client->register_udf(EVENTFLOW_TEST_UDF_PATH);
  ASSERT_FALSE(unknown.ok());
  EXPECT_EQ(unknown.status().code(), ErrorCode::UnknownCommit);
  auto ref = take(client->register_udf(EVENTFLOW_TEST_UDF_PATH));
  EXPECT_EQ(take(store->status(2)).revision, before + 1);
  const auto key = take(udf_storage_key(2, ref.digest));
  const auto bytes = take(store->get_many(2, {key}));
  ASSERT_EQ(bytes.values.size(), 1U);
  ASSERT_TRUE(bytes.values[0]);
  EXPECT_EQ(*bytes.values[0], take(read_udf_file(EVENTFLOW_TEST_UDF_PATH)));
  EXPECT_EQ(take(client->catalog())["udfs"].size(), 1U);
}

} // namespace
} // namespace eventflow::test
