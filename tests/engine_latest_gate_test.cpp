#include "../src/runtime/engine_internal.hpp"
#include "status_test_support.hpp"
#include <gtest/gtest.h>

namespace eventflow::test {
namespace {
TEST(EngineLatestGate, OrdinaryOpenCannotActivateRuntimeOwnedMetadata) {
  for (bool add_latest : {false, true}) {
    auto catalog = take(Catalog::create());
    auto candidate =
        take(catalog->define_candidate({"candidate", {{1, "k", DataType::String, false}}, {}}));
    if (add_latest) {
      DefinitionOptions options;
      options.base_write_role = BaseWriteRole::RequestCapture;
      options.identity_domain = "requests";
      auto capture =
          take(catalog->define_sql("CREATE TABLE capture(k STRING, request_id STRING, x INT64, "
                                   "PRIMARY KEY(k,request_id)) PARTITION BY(k)",
                                   options));
      auto result =
          take(catalog->define_sql("CREATE MATERIALIZED VIEW result AS SELECT x FROM capture"));
      auto current = take(catalog->snapshot());
      auto source = take(current.find(capture));
      auto status = take(catalog->define_request_status(
          {"request_status", "requests", source->identity_fields, candidate}));
      take(catalog->define_latest({"latest", result, status, candidate}));
    }
    for (bool internal : {false, true}) {
      StoreConfig storage;
      storage.shard_count = 1;
      auto store = std::shared_ptr<IStateStore>(take(MemoryStateStore::open(storage)));
      EngineConfig config;
      config.shard_count = 1;
      auto snapshot = take(catalog->snapshot());
      auto opened = internal ? EngineRuntimeAccess::open(store, snapshot, config,
                                                         std::shared_ptr<FunctionRegistry>{})
                             : Engine::open(store, snapshot, config);
      EXPECT_EQ(opened.status().code(), ErrorCode::FailedPrecondition);
      EXPECT_EQ(take(store->status(0)).revision, 0U);
    }
  }
}
} // namespace
} // namespace eventflow::test
