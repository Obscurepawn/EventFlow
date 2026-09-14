#include "eventflow/cluster_client.hpp"
#include "eventflow/compute_service.hpp"
#include "eventflow/coordinator_service.hpp"
#include "status_test_support.hpp"
#include <charconv>
#include <chrono>
#include <gtest/gtest.h>
#include <thread>

namespace eventflow::test {
namespace {

const std::vector<std::string> sql_definition{
    "CREATE TABLE trades(ticker STRING NOT NULL,id INT64 NOT NULL,volume INT64 NOT NULL,"
    "PRIMARY KEY(ticker,id)) PARTITION BY(ticker)",
    // Aggregate raw trades first, then derive a feature from the updated aggregate.
    "CREATE MATERIALIZED VIEW totals AS SELECT ticker,SUM(volume) AS volume FROM trades GROUP BY "
    "ticker",
    "CREATE MATERIALIZED VIEW features AS SELECT ticker,volume*2 AS doubled FROM totals"};

TEST(SqlModuleService, DefaultModuleDeploysAndConsumesIntoCascadedViewsWithoutRequests) {
  auto bound = bind_computation_module({}, sql_definition);
  ASSERT_TRUE(bound.ok()) << bound.status().message();
  EXPECT_FALSE(bound->definition.requests);
  auto store = std::shared_ptr<IStateStore>(
      take(MemoryStateStore::open({.shard_count = 2, .shard_owners = {}})));
  auto compute = take(
      ComputeService::create(store, {.node_id = "sql-compute", .shard_count = 1, .shards = {0}}));
  const auto compute_port = take(compute->start("127.0.0.1", 0));
  MemMockSourceConfig delivery;
  delivery.source_id = "sql-input";
  delivery.assignments = {{.topic = "trades", .partition = 0, .next_offset = 0}};
  auto source = std::shared_ptr<MemMockMessageSource>(take(MemMockMessageSource::open(delivery)));
  const SourcePartition partition{.source_id = "sql-input", .topic = "trades", .partition = 0};
  ASSERT_TRUE(source->append(partition, 0, "ABC", R"({"ticker":"ABC","id":1,"volume":70})").ok());
  ASSERT_TRUE(source->append(partition, 1, "ABC", R"({"ticker":"ABC","id":2,"volume":50})").ok());
  CoordinatorServiceConfig config;
  config.shard_count = 1;
  config.nodes = {{.node_id = "sql-compute",
                   .endpoint = "http://127.0.0.1:" + std::to_string(compute_port),
                   .shards = {0}}};
  config.source = SourceSubscription{.source = source, .delivery = delivery, .relation = "trades"};
  auto coordinator = take(CoordinatorService::create(store, config));
  const auto port = take(coordinator->start("127.0.0.1", 0));
  auto client = take(ClusterClient::open("http://127.0.0.1:" + std::to_string(port)));
  for (const auto& ddl : sql_definition) {
    auto staged = client->ddl(ddl);
    ASSERT_TRUE(staged.ok()) << staged.status().message();
  }
  auto deployed = client->deploy();
  ASSERT_TRUE(deployed.ok()) << deployed.status().message();
  EXPECT_EQ((*deployed)["state"], "RUNNING");
  // The client only queries; the Coordinator's source loop must apply both records.
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  bool visible = false;
  do {
    auto rows =
        client->query("SELECT doubled FROM features WHERE ticker='ABC'", {std::string("ABC")});
    ASSERT_TRUE(rows.ok()) << rows.status().message();
    visible = rows->rows == std::vector<Row>{{std::int64_t{240}}};
    if (visible)
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  } while (std::chrono::steady_clock::now() < deadline);
  ASSERT_TRUE(visible);
  ASSERT_TRUE(client->pause().ok());
  // Pausing serializes access to the source before this direct contract assertion.
  EXPECT_EQ(take(source->committed_position(partition)), 2);
  const auto status = take(client->status());
  const auto clock = status["now_us"].get<std::string>();
  std::int64_t now_us = 0;
  const auto parsed = std::from_chars(clock.data(), clock.data() + clock.size(), now_us);
  ASSERT_EQ(parsed.ec, std::errc{});
  ASSERT_EQ(parsed.ptr, clock.data() + clock.size());
  auto checkpoint = client->checkpoint("sql-cut", now_us);
  ASSERT_TRUE(checkpoint.ok()) << checkpoint.status().message();
  ASSERT_TRUE(client->resume().ok());
}

TEST(SqlModuleService, OrdinaryTriggerRunsWithoutAFunctionRegistryOrRequestPlan) {
  auto catalog = take(Catalog::create());
  const auto input = take(catalog->define_sql(sql_definition.front()));
  ProcessorConfig config;
  config.input_relations = {input};
  // An ordinary trigger emits an occurrence; it does not invoke a function.
  auto trigger = Trigger::on_event("trade_received",
                                   EventRef::source({.kind = "trades", .payload_relation = input}));
  auto bound = take(TriggerSet::bind(take(catalog->snapshot()), config, {trigger},
                                     EngineConfig{.shard_count = 1}));
  ASSERT_FALSE(bound.registry);
  auto store = std::shared_ptr<IStateStore>(
      take(MemoryStateStore::open({.shard_count = 1, .shard_owners = {}})));
  auto opened = ComputationRuntime::open(store, std::move(bound), {.shard_count = 1});
  ASSERT_TRUE(opened.ok()) << opened.status().message();
  auto& runtime = *opened;
  auto ingested = take(runtime->ingest_source(
      "trades", {std::string("ABC"), std::int64_t{1}, std::int64_t{70}},
      {.source_id = "trigger-input", .topic = "trades", .partition = 0, .offset = 0}, 1));
  ASSERT_TRUE(ingested.input_committed);
  ASSERT_TRUE(ingested.operation_status.ok()) << ingested.operation_status.message();
  EXPECT_EQ(take(runtime->run_functions(2)), 0U);
  const auto occurrences = take(runtime->processor().pending_occurrences(16));
  ASSERT_EQ(occurrences.size(), 1U);
  EXPECT_EQ(occurrences.front().rule_id, "trade_received");
}

} // namespace
} // namespace eventflow::test
