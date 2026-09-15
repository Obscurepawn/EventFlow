#include "eventflow/cluster_client.hpp"
#include "eventflow/computation_runtime.hpp"
#include "eventflow/compute_service.hpp"
#include "eventflow/coordinator_service.hpp"
#include "eventflow/key_codec.hpp"
#include "eventflow/submitted_computation.hpp"
#include "status_test_support.hpp"
#include <gtest/gtest.h>
#include <httplib.h>

namespace eventflow::test {

SubmittedComputation submitted_definition(const FunctionRef& ref) noexcept {
  return SubmittedComputation{
      .name = "news_scoring",
      .ddl =
          {"CREATE TABLE news(ticker STRING NOT NULL,id INT64 NOT NULL,weighted FLOAT64 NOT NULL,"
           "PRIMARY KEY(ticker,id)) PARTITION BY(ticker)",
           "CREATE MATERIALIZED VIEW features AS SELECT ticker,COUNT(*) AS news_count,"
           "SUM(weighted) AS sentiment_price_sum FROM news GROUP BY ticker"},
      .input_relation = "news",
      .session_lifetime_us = 1000000,
      .triggers = {{.name = "impact",
                    .function = ref,
                    .capture_relation = "features",
                    .capture_fields = {"ticker", "news_count", "sentiment_price_sum"},
                    .conditions = {
                        {.column = "news_count", .comparison = "ge", .value = std::int64_t{2}}}}}};
}

TEST(SubmittedComputation, RejectsInvalidUtf8BeforeSerializingClientInput) {
  auto definition =
      submitted_definition({.id = "news_impact", .version = 1, .digest = std::string(64, 'a')});
  auto wire = take(encode_computation(definition));
  wire["name"] = std::string(1, static_cast<char>(0xff));
  EXPECT_EQ(decode_computation(wire).status().code(), ErrorCode::InvalidArgument);
  definition.name = std::string(1, static_cast<char>(0xff));
  EXPECT_EQ(encode_computation(definition).status().code(), ErrorCode::InvalidArgument);
}

TEST(FunctionReference, PreservesTheFullUnsignedVersionRange) {
  const FunctionRef ref{.id = "news_impact", .version = UINT64_MAX, .digest = std::string(64, 'a')};
  auto decoded = decode_function_ref(encode_function_ref(ref));
  ASSERT_TRUE(decoded.ok()) << decoded.status().message();
  EXPECT_EQ(decoded->version, UINT64_MAX);
}

TEST(NativeUdfService, ArtifactMutationBudgetFailureKeepsCoordinatorUsable) {
  auto store = std::shared_ptr<IStateStore>(
      take(MemoryStateStore::open({.shard_count = 2, .shard_owners = {}, .max_mutations = 1})));
  auto coordinator = take(CoordinatorService::create(
      store, {.shard_count = 1,
              .nodes = {{.node_id = "compute", .endpoint = "http://127.0.0.1:1", .shards = {0}}},
              .max_request_bytes = 4 * 1024 * 1024,
              .allow_native_udfs = true}));
  const auto port = take(coordinator->start("127.0.0.1", 0));
  auto client = take(ClusterClient::open("http://127.0.0.1:" + std::to_string(port)));
  EXPECT_EQ(client->register_udf(EVENTFLOW_TEST_UDF_PATH).status().code(),
            ErrorCode::ResourceExhausted);
  EXPECT_TRUE(client->catalog().ok());
}

TEST(SubmittedComputation, BindsUploadedFunctionAndExecutesInTheRealOwner) {
  auto library = take(UdfLibrary::open(take(read_udf_file(EVENTFLOW_TEST_UDF_PATH))));
  auto definition =
      submitted_definition({.id = "news_impact", .version = 1, .digest = library->digest()});
  auto bound = bind_submitted_computation(definition, {library}, {.shard_count = 1});
  ASSERT_TRUE(bound.ok()) << bound.status().message();
  const auto latest =
      bound->explain()["triggers"][0]["generated"]["latest_name"].get<std::string>();
  auto store = std::shared_ptr<IStateStore>(
      take(MemoryStateStore::open({.shard_count = 1, .shard_owners = {}})));
  auto runtime = take(
      ComputationRuntime::open(store, std::move(*bound), {.shard_count = 1}, {.threaded = false}));
  for (std::int64_t i = 0; i < 2; ++i) {
    const auto result = take(runtime->ingest_source(
        "news", {std::string("ACME"), i, i == 0 ? 47.0 : 56.0},
        {.source_id = "news", .topic = "news", .partition = 0, .offset = i}, 100 + i));
    ASSERT_TRUE(result.input_committed);
    ASSERT_TRUE(result.operation_status.ok()) << result.operation_status.message();
  }
  take(runtime->run_functions(102));
  const auto rows =
      take(runtime->query_sql("SELECT impact_score FROM " + latest + " WHERE ticker='ACME'"));
  EXPECT_EQ(rows.rows, (std::vector<Row>{{51.5}}));
}

TEST(NativeUdfService, RegistersAfterServersHaveStarted) {
  auto store = std::shared_ptr<IStateStore>(
      take(MemoryStateStore::open({.shard_count = 2, .shard_owners = {}})));
  auto compute = take(ComputeService::create(
      store, {.node_id = "compute", .shard_count = 1, .shards = {0}, .allow_native_udfs = true}));
  const auto compute_port = take(compute->start("127.0.0.1", 0));
  CoordinatorServiceConfig config;
  config.shard_count = 1;
  config.nodes = {{.node_id = "compute",
                   .endpoint = "http://127.0.0.1:" + std::to_string(compute_port),
                   .shards = {0}}};
  config.max_request_bytes = 4 * 1024 * 1024;
  config.allow_native_udfs = true;
  auto coordinator = take(CoordinatorService::create(store, config));
  const auto port = take(coordinator->start("127.0.0.1", 0));
  auto client = take(ClusterClient::open("http://127.0.0.1:" + std::to_string(port),
                                         {.max_request_bytes = 4 * 1024 * 1024}));
  auto registered = client->register_udf(EVENTFLOW_TEST_UDF_PATH);
  ASSERT_TRUE(registered.ok()) << registered.status().message();
  EXPECT_EQ(encode_function_ref(take(client->register_udf(EVENTFLOW_TEST_UDF_PATH))),
            encode_function_ref(*registered));
  auto altered = take(read_udf_file(EVENTFLOW_TEST_UDF_PATH));
  altered.push_back('\0');
  httplib::Client transport("127.0.0.1", port);
  transport.set_read_timeout(10);
  const auto conflict =
      transport.Post("/v1/meta/register_udf", Json{{"bytes", take(hex_encode(altered))}}.dump(),
                     "application/json");
  ASSERT_TRUE(conflict);
  EXPECT_EQ(conflict->status, 400);
  EXPECT_EQ(take(parse_json_exact(conflict->body))["error_code"],
            static_cast<int>(ErrorCode::Conflict));
  EXPECT_EQ(take(client->catalog())["udfs"].size(), 1U);
  auto definition = submitted_definition(*registered);
  auto invalid = definition;
  invalid.triggers[0].function.version = 2;
  EXPECT_EQ(client->submit_computation(invalid).status().code(), ErrorCode::NotFound);
  invalid = definition;
  invalid.triggers[0].capture_fields[1] = "sentiment_price_sum";
  EXPECT_EQ(client->submit_computation(invalid).status().code(), ErrorCode::InvalidArgument);
  auto submitted = client->submit_computation(definition);
  ASSERT_TRUE(submitted.ok()) << submitted.status().message();
  EXPECT_EQ(take(client->submit_computation(definition)), *submitted);
  const auto deployed = client->deploy();
  ASSERT_TRUE(deployed.ok()) << deployed.status().message();
  EXPECT_EQ((*deployed)["state"], "RUNNING");
  EXPECT_EQ(client->register_udf(EVENTFLOW_TEST_UDF_PATH).status().code(),
            ErrorCode::FailedPrecondition);
  EXPECT_EQ(client->submit_computation(definition).status().code(), ErrorCode::FailedPrecondition);
}

TEST(SubmittedComputation, CodecPreservesTypesAndRejectsInvalidDefinition) {
  auto definition =
      submitted_definition({.id = "news_impact", .version = 1, .digest = std::string(64, 'a')});
  const auto encoded = take(encode_computation(definition));
  const auto decoded = take(decode_computation(encoded));
  EXPECT_EQ(take(encode_computation(decoded)), encoded);
  auto invalid = encoded;
  invalid["unexpected"] = true;
  EXPECT_FALSE(decode_computation(invalid).ok());
  invalid = encoded;
  invalid["session_lifetime_us"] = "0";
  EXPECT_FALSE(decode_computation(invalid).ok());
  invalid = encoded;
  invalid["triggers"][0]["conditions"][0]["comparison"] = "unknown";
  EXPECT_FALSE(decode_computation(invalid).ok());
}

TEST(NativeUdfService, UploadRequiresExplicitEnablement) {
  auto store = std::shared_ptr<IStateStore>(
      take(MemoryStateStore::open({.shard_count = 2, .shard_owners = {}})));
  auto coordinator = take(CoordinatorService::create(
      store, {.shard_count = 1,
              .nodes = {{.node_id = "compute", .endpoint = "http://127.0.0.1:1", .shards = {0}}},
              .max_request_bytes = 4 * 1024 * 1024}));
  const auto port = take(coordinator->start("127.0.0.1", 0));
  auto client = take(ClusterClient::open("http://127.0.0.1:" + std::to_string(port)));
  EXPECT_EQ(client->register_udf(EVENTFLOW_TEST_UDF_PATH).status().code(),
            ErrorCode::FailedPrecondition);
}

} // namespace eventflow::test
