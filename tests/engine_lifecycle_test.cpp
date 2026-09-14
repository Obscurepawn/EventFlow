#include "../src/runtime/engine_internal.hpp"
#include "status_test_support.hpp"
#include <gtest/gtest.h>

namespace eventflow::test {
namespace {
TEST(EngineLifecycle, CapacityFailureDiscardsRowsAndTimeThenAllowsOriginalRetry) {
  auto catalog = take(Catalog::create());
  auto input = take(
      catalog->define_sql("CREATE TABLE input(k STRING,v INT64,PRIMARY KEY(k)) PARTITION BY(k)"));
  take(catalog->define_sql("CREATE MATERIALIZED VIEW output AS SELECT v*2 AS v FROM input"));
  StoreConfig storage;
  storage.shard_count = 1;
  auto store = std::shared_ptr<IStateStore>(take(MemoryStateStore::open(storage)));
  EngineConfig config;
  config.shard_count = 1;
  auto engine = take(Engine::open(store, take(catalog->snapshot()), config));
  auto revision = engine->revision();
  std::vector<TableChange> rows{
      {input, {std::string("a")}, Row{std::string("a"), std::int64_t{7}}}};
  auto result = EngineRuntimeAccess::apply_lifecycle(
      *engine, rows, {100}, [](RoundContext&, const std::vector<RowChange>&) noexcept -> Status {
        return {ErrorCode::ResourceExhausted, "CEP paths full"};
      });
  ASSERT_EQ(result.status().code(), ErrorCode::ResourceExhausted);
  EXPECT_FALSE(engine->failed());
  EXPECT_EQ(engine->revision(), revision);
  EXPECT_EQ(EngineRuntimeAccess::time_us(*engine), 0);
  EXPECT_TRUE(
      take(engine->query_sql("SELECT v FROM output", {Row{std::string("a")}})).rows.empty());
  auto retry = take(EngineRuntimeAccess::apply_lifecycle(*engine, rows, {100}, {}));
  EXPECT_EQ(retry.commit.code, CommitCode::Committed);
  EXPECT_EQ(retry.commit.revision, revision + 1);
  EXPECT_EQ(take(engine->query_sql("SELECT v FROM output", {Row{std::string("a")}})).rows[0],
            Row{std::int64_t{14}});
}

TEST(EngineLifecycle, PublicHookCapacityErrorStillFailsItsShard) {
  auto catalog = take(Catalog::create());
  take(catalog->define_sql("CREATE TABLE input(k STRING,v INT64,PRIMARY KEY(k)) PARTITION BY(k)"));
  StoreConfig storage;
  storage.shard_count = 1;
  auto store = std::shared_ptr<IStateStore>(take(MemoryStateStore::open(storage)));
  EngineConfig config;
  config.shard_count = 1;
  auto engine = take(Engine::open(store, take(catalog->snapshot()), config));
  auto result = engine->apply_many(
      {}, {1}, [](RoundContext&, const std::vector<RowChange>&) noexcept -> Status {
        return {ErrorCode::ResourceExhausted, "public error"};
      });
  EXPECT_EQ(result.status().code(), ErrorCode::ResourceExhausted);
  EXPECT_TRUE(engine->failed());
}

TEST(EngineLifecycle, PrivateEntryRetainsRolePermissionsAndPermanentFailurePolicy) {
  auto catalog = take(Catalog::create());
  DefinitionOptions options;
  options.base_write_role = BaseWriteRole::RuntimeStatus;
  auto status = take(catalog->define_sql(
      "CREATE TABLE status(k STRING,v INT64,PRIMARY KEY(k)) PARTITION BY(k)", options));
  StoreConfig storage;
  storage.shard_count = 1;
  auto store = std::shared_ptr<IStateStore>(take(MemoryStateStore::open(storage)));
  EngineConfig config;
  config.shard_count = 1;
  auto engine = take(Engine::open(store, take(catalog->snapshot()), config));
  std::vector<TableChange> rows{
      {status, {std::string("a")}, Row{std::string("a"), std::int64_t{7}}}};
  auto forbidden = EngineRuntimeAccess::apply_lifecycle(*engine, rows, {1}, {});
  EXPECT_EQ(forbidden.status().code(), ErrorCode::InvalidArgument);
  EXPECT_FALSE(engine->failed());
  auto failed = EngineRuntimeAccess::apply_lifecycle(
      *engine, {}, {2}, [](RoundContext&, const std::vector<RowChange>&) noexcept -> Status {
        return {ErrorCode::InvalidArgument, "invalid application predicate"};
      });
  EXPECT_EQ(failed.status().code(), ErrorCode::InvalidArgument);
  EXPECT_TRUE(engine->failed());
}

} // namespace
} // namespace eventflow::test
