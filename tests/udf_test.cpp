#include "eventflow/udf.hpp"
#include "status_test_support.hpp"
#include <gtest/gtest.h>

namespace eventflow::test {

TEST(UdfLibrary, LoadsActualSharedLibraryFromBytes) {
  auto bytes = read_udf_file(EVENTFLOW_TEST_UDF_PATH);
  ASSERT_TRUE(bytes.ok()) << bytes.status().message();
  auto library = UdfLibrary::open(*bytes);
  ASSERT_TRUE(library.ok()) << library.status().message();
}

TEST(UdfDigest, UsesSha256ContentIdentity) {
  EXPECT_EQ(udf_digest(""), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  EXPECT_EQ(udf_digest("abc"), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
  EXPECT_EQ(udf_digest(std::string(1000000, 'a')),
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0");
}

TEST(UdfLibrary, RejectsMalformedBytesAndPinsCallbackLifetime) {
  EXPECT_EQ(UdfLibrary::open("not-an-elf").status().code(), ErrorCode::InvalidArgument);
  EXPECT_EQ(UdfLibrary::open(std::string(max_udf_bytes + 1, 'x')).status().code(),
            ErrorCode::InvalidArgument);
  auto library = take(UdfLibrary::open(take(read_udf_file(EVENTFLOW_TEST_UDF_PATH))));
  const std::weak_ptr<UdfLibrary> lifetime = library;
  auto registry = take(FunctionRegistry::create());
  ASSERT_TRUE(register_udf(*registry, library).ok());
  const auto signature = take(registry->describe("news_impact", 1));
  EXPECT_NE(signature.implementation_identity.find(library->digest()), std::string::npos);
  library.reset();
  auto clone = take(registry->clone());
  registry.reset();
  EXPECT_FALSE(lifetime.expired());
  auto executor = take(SimpleFunctionExecutor::create(clone));
  ASSERT_TRUE(executor
                  ->submit({.task_id = "news-score",
                            .function_id = "news_impact",
                            .function_version = 1,
                            .implementation_identity = signature.implementation_identity,
                            .inputs = {Row{std::string("ACME"), std::int64_t{2}, 103.0}}})
                  .ok());
  take(executor->run_ready(0));
  {
    const auto result = take(executor->poll(1));
    ASSERT_EQ(result.size(), 1U);
    EXPECT_EQ(result[0].output, (Row{std::string("ACME"), 51.5}));
  }
  executor.reset();
  clone.reset();
  EXPECT_TRUE(lifetime.expired());
}

TEST(UdfLibrary, RejectsIncompatibleBuildAndMissingEntry) {
  EXPECT_EQ(UdfLibrary::open(take(read_udf_file(EVENTFLOW_TEST_MISSING_UDF_ENTRY))).status().code(),
            ErrorCode::InvalidArgument);
  EXPECT_EQ(
      UdfLibrary::open(take(read_udf_file(EVENTFLOW_TEST_BAD_UDF_BUILD_PATH))).status().code(),
      ErrorCode::Conflict);
}

} // namespace eventflow::test
