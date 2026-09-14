#include "eventflow/engine.hpp"
#include "status_test_support.hpp"
#include <gtest/gtest.h>
using namespace eventflow;
using eventflow::test::take;

TEST(Engine, OpensAValidatedCatalogAgainstRealStoreInterface) {
  auto catalog = take(Catalog::create());
  take(catalog->define_sql("CREATE TABLE trades (ticker STRING NOT NULL, id INT64 NOT NULL, "
                           "quantity INT64 NULL, PRIMARY KEY(ticker,id)) PARTITION BY(ticker)"));
  auto store = std::shared_ptr<IStateStore>(take(MemoryStateStore::open()));
  auto engine = Engine::open(store, take(catalog->snapshot()));
  ASSERT_TRUE(engine.ok()) << engine.status().message();
}

TEST(Engine, JoinCaptureBudgetIncludesBothInputsBeforeProjection) {
  auto catalog = take(Catalog::create());
  auto left = take(catalog->define_sql(
      "CREATE TABLE l (k STRING NOT NULL, payload STRING, PRIMARY KEY(k)) PARTITION BY(k)"));
  auto right = take(catalog->define_sql(
      "CREATE TABLE r (k STRING NOT NULL, payload STRING, PRIMARY KEY(k)) PARTITION BY(k)"));
  StoreConfig storage;
  storage.shard_count = 1;
  auto store = std::shared_ptr<IStateStore>(take(MemoryStateStore::open(storage)));
  EngineConfig config;
  config.shard_count = 1;
  auto engine = take(Engine::open(store, take(catalog->snapshot()), config));
  take(engine->apply({left, {std::string("a")}, Row{std::string("a"), std::string(1000, 'x')}}));
  take(engine->apply({right, {std::string("a")}, Row{std::string("a"), std::string(1000, 'y')}}));
  QueryContext query;
  query.max_bytes = 16;
  auto result = engine->query_sql("SELECT 1 AS one FROM l JOIN r ON l.k=r.k WHERE l.k='a'", query);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), ErrorCode::ResourceExhausted);
}

TEST(Engine, RuntimeOwnedCaptureCannotBeWrittenThroughOrdinaryInputOrHook) {
  auto catalog = take(Catalog::create());
  DefinitionOptions options;
  options.base_write_role = BaseWriteRole::RequestCapture;
  options.identity_domain = "requests";
  const auto capture =
      take(catalog->define_sql("CREATE TABLE captured(k STRING, request_id STRING, v INT64, "
                               "PRIMARY KEY(k,request_id)) PARTITION BY(k)",
                               options));
  StoreConfig config;
  config.shard_count = 1;
  auto store = std::shared_ptr<IStateStore>(take(MemoryStateStore::open(config)));
  EngineConfig engine_config;
  engine_config.shard_count = 1;
  auto engine = take(Engine::open(store, take(catalog->snapshot()), engine_config));
  const auto revision = engine->revision();
  Row identity{std::string("a"), std::string("request-1")};
  Row row{std::string("a"), std::string("request-1"), std::int64_t(7)};
  EXPECT_FALSE(engine->apply({capture, identity, row}).ok());
  EXPECT_EQ(engine->revision(), revision);
  auto hook = [&](RoundContext& context, const std::vector<RowChange>&) noexcept -> Status {
    return context.emit({capture, identity, {}, row});
  };
  EXPECT_FALSE(engine->apply_many({}, {}, hook).ok());
  EXPECT_EQ(take(store->status(0)).revision, revision);
}
