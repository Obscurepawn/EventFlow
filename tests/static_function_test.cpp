#include "eventflow/function_executor.hpp"
#include "eventflow/static_function.hpp"
#include <gtest/gtest.h>
#include <limits>

namespace eventflow {
namespace static_function_test {

struct Input {
  double first = 0;
  double second = 0;
};

struct Output {
  double score = 0;
};

} // namespace static_function_test

template <> struct SchemaTraits<static_function_test::Input> {
  static inline std::size_t declaration_calls = 0;

  static auto fields() noexcept {
    ++declaration_calls;
    using T = static_function_test::Input;
    return std::tuple{field<&T::first>("first"), field<&T::second>("second")};
  }
};

template <> struct SchemaTraits<static_function_test::Output> {
  static inline std::size_t declaration_calls = 0;

  static auto fields() noexcept {
    ++declaration_calls;
    return std::tuple{field<&static_function_test::Output::score>("score")};
  }
};

namespace static_function_test {

// These declarations exercise rejected signatures without invoking exceptions.
StatusOr<Output> potentially_throwing(const Input&);
Output wrong_return(const Input&) noexcept;
StatusOr<Output> wrong_argument(Input&) noexcept;

namespace {

StatusOr<Output> calculate(const Input& input) noexcept {
  return Output{.score = (input.first + input.second) * 0.5};
}

StatusOr<Output> rejects(const Input&) noexcept {
  return Status(ErrorCode::FailedPrecondition, "Model rejected the input");
}

StatusOr<Output> non_finite(const Input&) noexcept {
  return Output{.score = std::numeric_limits<double>::infinity()};
}

static_assert(StaticFunctionCallable<&calculate>);
static_assert(!StaticFunctionCallable<&potentially_throwing>);
static_assert(!StaticFunctionCallable<&wrong_return>);
static_assert(!StaticFunctionCallable<&wrong_argument>);
static_assert(std::is_same_v<Function<&calculate>::input_type, Input>);
static_assert(std::is_same_v<Function<&calculate>::output_type, Output>);

TEST(StaticFunctionTest, DeclarationCreatesTypedAdapterWithExplicitIdentity) {
  const Function<&calculate> function{
      .id = "score", .version = 1, .implementation_identity = "score.mean.v1"};
  auto definition = function.definition();
  ASSERT_TRUE(definition.ok());
  EXPECT_EQ(definition->implementation_identity, "score.mean.v1");
  EXPECT_EQ(definition->input_fields.size(), 2U);
  const auto input_declarations = SchemaTraits<Input>::declaration_calls;
  const auto output_declarations = SchemaTraits<Output>::declaration_calls;
  auto output = definition->invoke({0.8, 0.6});
  ASSERT_TRUE(output.ok());
  ASSERT_EQ(output->size(), 1U);
  EXPECT_DOUBLE_EQ(*std::get_if<double>(&(*output)[0]), 0.7);
  EXPECT_FALSE(definition->invoke({std::string("bad"), 0.6}).ok());
  EXPECT_FALSE(definition->invoke({0.8}).ok());
  EXPECT_EQ(SchemaTraits<Input>::declaration_calls, input_declarations);
  EXPECT_EQ(SchemaTraits<Output>::declaration_calls, output_declarations);
}

TEST(StaticFunctionTest, DeclarationRejectsIncompleteIdentity) {
  auto function = Function<&calculate>{.id = "score", .version = 1, .implementation_identity = {}};
  EXPECT_FALSE(function.definition().ok());
  function.implementation_identity = "score.mean.v1";
  function.version = 0;
  EXPECT_FALSE(function.definition().ok());
  function.version = 1;
  function.id.clear();
  EXPECT_FALSE(function.definition().ok());
}

TEST(StaticFunctionTest, UserStatusAndInvalidOutputPropagate) {
  auto error =
      Function<&rejects>{.id = "reject", .version = 1, .implementation_identity = "reject.v1"}
          .definition();
  ASSERT_TRUE(error.ok());
  EXPECT_EQ(error->invoke({1.0, 2.0}).status().code(), ErrorCode::FailedPrecondition);
  auto invalid =
      Function<&non_finite>{.id = "invalid", .version = 1, .implementation_identity = "invalid.v1"}
          .definition();
  ASSERT_TRUE(invalid.ok());
  EXPECT_FALSE(invalid->invoke({1.0, 2.0}).ok());
}

TEST(StaticFunctionTest, NonCapturingNoexceptCallableUsesTheSameAdapter) {
  constexpr auto callback = [](const Input& input) noexcept -> StatusOr<Output> {
    return Output{.score = input.first};
  };
  static_assert(StaticFunctionCallable<callback>);
  auto definition =
      Function<callback>{.id = "first", .version = 1, .implementation_identity = "first.v1"}
          .definition();
  ASSERT_TRUE(definition.ok());
  auto output = definition->invoke({0.8, 0.6});
  ASSERT_TRUE(output.ok());
  EXPECT_DOUBLE_EQ(*std::get_if<double>(&(*output)[0]), 0.8);
}

class StaticFunctionExecutorTest : public testing::TestWithParam<bool> {};

TEST_P(StaticFunctionExecutorTest, TypedFunctionRunsThroughSimpleAndThreadPoolAdapters) {
  auto definition =
      Function<&calculate>{.id = "score", .version = 1, .implementation_identity = "score.mean.v1"}
          .definition();
  ASSERT_TRUE(definition.ok());
  auto registry = FunctionRegistry::create();
  ASSERT_TRUE(registry.ok());
  auto added = (*registry)->add(
      {.signature = {.function_id = definition->id,
                     .version = definition->version,
                     .implementation_identity = definition->implementation_identity,
                     .inputs = {{.alias = "input", .fields = definition->input_fields}},
                     .output_fields = definition->output_fields},
       .callback =
           [invoke = definition->invoke](const FunctionInputs& inputs) noexcept -> StatusOr<Row> {
         if (inputs.size() != 1 || !inputs[0])
           return Status(ErrorCode::InvalidArgument, "Static function input must be present");
         return invoke(*inputs[0]);
       }});
  ASSERT_TRUE(added.ok()) << added.message();
  std::unique_ptr<IFunctionExecutor> executor;
  SimpleFunctionExecutor* simple = nullptr;
  if (GetParam()) {
    auto opened = ThreadPoolFunctionExecutor::create(*registry);
    ASSERT_TRUE(opened.ok());
    executor = std::move(*opened);
  } else {
    auto opened = SimpleFunctionExecutor::create(*registry);
    ASSERT_TRUE(opened.ok());
    simple = opened->get();
    executor = std::move(*opened);
  }
  auto submitted = executor->submit({.task_id = "score-1",
                                     .function_id = "score",
                                     .function_version = 1,
                                     .implementation_identity = "score.mean.v1",
                                     .inputs = {Row{0.8, 0.6}}});
  ASSERT_TRUE(submitted.ok());
  if (simple) {
    auto ran = simple->run_ready(0);
    ASSERT_TRUE(ran.ok());
  }
  auto completed = executor->poll(1, 1000);
  ASSERT_TRUE(completed.ok());
  ASSERT_EQ(completed->size(), 1U);
  ASSERT_TRUE((*completed)[0].status.ok());
  EXPECT_DOUBLE_EQ(*std::get_if<double>(&(*completed)[0].output[0]), 0.7);
  EXPECT_EQ(executor->metrics().callback_calls, 1U);
  EXPECT_TRUE(executor->close().ok());
}

INSTANTIATE_TEST_SUITE_P(Adapters, StaticFunctionExecutorTest, testing::Bool());

} // namespace
} // namespace static_function_test
} // namespace eventflow
