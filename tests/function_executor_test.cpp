#include "eventflow/function_executor.hpp"
#include "status_test_support.hpp"
#include <atomic>
#include <chrono>
#include <gtest/gtest.h>
#include <latch>
#include <limits>
#include <thread>

namespace eventflow::test {
namespace {
BuiltinFunction addition() noexcept {
  return {{"add",
           1,
           "builtin:add:v1",
           {{"input", true, {{1, "value", DataType::Int64, false}}}},
           {{2, "result", DataType::Int64, false}},
           true},
          [](const FunctionInputs& inputs) noexcept -> StatusOr<Row> {
            const auto value = std::get<std::int64_t>((*inputs[0])[0]);
            if (value == INT64_MAX)
              return Status(ErrorCode::InvalidArgument, "addition overflow");
            return Row{value + 1};
          }};
}

TEST(FunctionRegistry, PublishesAnExactImmutableNamedVersion) {
  auto registry = take(FunctionRegistry::create());
  ASSERT_TRUE(registry->add(addition()).ok());
  auto signature = take(registry->describe("add", 1));
  EXPECT_EQ(signature.implementation_identity, "builtin:add:v1");
  ASSERT_EQ(signature.inputs.size(), 1U);
  EXPECT_EQ(signature.inputs[0].fields[0].name, "value");
  EXPECT_EQ(registry->add(addition()).code(), ErrorCode::Conflict);
}

FunctionTask task(std::string id = "request:scope:node:attempt:epoch",
                  std::int64_t value = 3) noexcept {
  return {std::move(id), "add", 1, "builtin:add:v1", {Row{value}}, 0};
}

TEST(SimpleFunctionExecutor, ExplicitClockAndPollingPreserveFrozenIdentityAndResult) {
  auto registry = take(FunctionRegistry::create());
  ASSERT_TRUE(registry->add(addition()).ok());
  auto created = SimpleFunctionExecutor::create(registry);
  ASSERT_TRUE(created.ok()) << created.status().message();
  auto executor = std::move(created).value();
  auto input = task();
  input.ready_at_us = 10;
  ASSERT_TRUE(executor->submit(input).ok());
  input.inputs[0] = Row{std::int64_t{999}};
  EXPECT_TRUE(take(executor->poll(1)).empty());
  EXPECT_EQ(take(executor->run_ready(9)), 0U);
  EXPECT_EQ(take(executor->run_ready(10)), 1U);
  auto complete = take(executor->poll(1));
  ASSERT_EQ(complete.size(), 1U);
  EXPECT_EQ(complete[0].task_id, input.task_id);
  EXPECT_EQ(complete[0].function_version, 1U);
  EXPECT_EQ(complete[0].implementation_identity, "builtin:add:v1");
  EXPECT_TRUE(complete[0].status.ok());
  EXPECT_EQ(complete[0].output, Row{std::int64_t{4}});
  EXPECT_EQ(complete[0].completed_at_us, 10);
}

class FunctionExecutorContract : public ::testing::TestWithParam<bool> {
protected:
  std::shared_ptr<FunctionRegistry> registry;
  std::unique_ptr<IFunctionExecutor> executor;
  FunctionExecutorConfig config;

  void SetUp() noexcept override {
    registry = take(FunctionRegistry::create());
    ASSERT_TRUE(registry->add(addition()).ok());
    open();
  }

  void open() noexcept {
    executor.reset();
    if (GetParam())
      executor = take(ThreadPoolFunctionExecutor::create(registry, config));
    else
      executor = take(SimpleFunctionExecutor::create(registry, config));
  }

  std::vector<FunctionCompletion> collect(std::size_t count) noexcept {
    if (!GetParam())
      take(static_cast<SimpleFunctionExecutor*>(executor.get())->run_ready(100));
    std::vector<FunctionCompletion> result;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (result.size() < count && std::chrono::steady_clock::now() < deadline) {
      auto batch = take(executor->poll(count - result.size(), 100));
      for (auto& completion : batch)
        result.push_back(std::move(completion));
    }
    EXPECT_EQ(result.size(), count);
    return result;
  }
};

TEST_P(FunctionExecutorContract, CapacityOneKeepsCompletionDrainableUntilPolled) {
  config.max_tasks = 1;
  open();
  ASSERT_TRUE(executor->submit(task("a")).ok());
  EXPECT_EQ(executor->submit(task("b")).code(), ErrorCode::ResourceExhausted);
  auto completed = collect(1);
  ASSERT_EQ(completed.size(), 1U);
  EXPECT_TRUE(completed[0].status.ok());
  EXPECT_EQ(executor->metrics().active_tasks, 0U);
  EXPECT_EQ(executor->metrics().active_input_bytes, 0U);
  EXPECT_EQ(executor->metrics().reserved_completion_bytes, 0U);
  ASSERT_TRUE(executor->submit(task("b")).ok());
  EXPECT_TRUE(collect(1)[0].status.ok());
}

TEST_P(FunctionExecutorContract, QueuedSubmissionIsIdempotentAndConflictingContentIsRejected) {
  auto value = task("queued");
  value.ready_at_us = INT64_MAX;
  ASSERT_TRUE(executor->submit(value).ok());
  ASSERT_TRUE(executor->submit(value).ok());
  auto altered = value;
  altered.inputs = {Row{std::int64_t{9}}};
  EXPECT_EQ(executor->submit(altered).code(), ErrorCode::Conflict);
  altered = value;
  altered.ready_at_us = 0;
  EXPECT_EQ(executor->submit(altered).code(), ErrorCode::Conflict);
  EXPECT_EQ(executor->metrics().active_tasks, 1U);
  EXPECT_EQ(executor->metrics().idempotent_submissions, 1U);
  ASSERT_TRUE(executor->cancel(value.task_id).ok());
  auto output = collect(1);
  ASSERT_EQ(output.size(), 1U);
  EXPECT_TRUE(output[0].cancelled);
  EXPECT_FALSE(output[0].status.ok());
  EXPECT_EQ(output[0].task_id, value.task_id);
  EXPECT_EQ(executor->metrics().callback_calls, 0U);
}

TEST_P(FunctionExecutorContract, RejectsMissingFunctionVersionSignatureAndInvalidInputs) {
  auto unknown = task();
  unknown.function_version = 99;
  EXPECT_EQ(executor->submit(unknown).code(), ErrorCode::NotFound);
  unknown = task();
  unknown.implementation_identity = "different-binary";
  EXPECT_EQ(executor->submit(unknown).code(), ErrorCode::Conflict);
  unknown = task();
  unknown.inputs = {Row{3.0}};
  EXPECT_EQ(executor->submit(unknown).code(), ErrorCode::InvalidArgument);
  unknown.inputs = {std::nullopt};
  EXPECT_EQ(executor->submit(unknown).code(), ErrorCode::InvalidArgument);
  unknown.inputs = {};
  EXPECT_EQ(executor->submit(unknown).code(), ErrorCode::InvalidArgument);
  unknown = task();
  unknown.task_id.clear();
  EXPECT_EQ(executor->submit(unknown).code(), ErrorCode::InvalidArgument);
  EXPECT_EQ(executor->metrics().active_tasks, 0U);
}

TEST_P(FunctionExecutorContract, OrdinaryFunctionFailureAndWrongOutputAreTerminalCompletions) {
  auto bad = addition();
  bad.signature.function_id = "wrong-output";
  bad.callback = [](const FunctionInputs&) noexcept -> StatusOr<Row> {
    return Row{std::string("wrong")};
  };
  ASSERT_TRUE(registry->add(std::move(bad)).ok());
  ASSERT_TRUE(executor->submit(task("overflow", INT64_MAX)).ok());
  auto wrong = task("wrong");
  wrong.function_id = "wrong-output";
  ASSERT_TRUE(executor->submit(wrong).ok());
  auto results = collect(2);
  ASSERT_EQ(results.size(), 2U);
  for (const auto& result : results) {
    EXPECT_EQ(result.status.code(), ErrorCode::InvalidArgument);
    EXPECT_TRUE(result.output.empty());
    EXPECT_FALSE(result.cancelled);
  }
  ASSERT_TRUE(executor->submit(task("healthy")).ok());
  EXPECT_TRUE(collect(1)[0].status.ok());
}

TEST_P(FunctionExecutorContract, OutputByteLimitProducesErrorWithoutExhaustingCompletionChannel) {
  config.max_output_bytes_per_task = 128;
  open();
  auto large = addition();
  large.signature.function_id = "large";
  large.signature.output_fields = {{1, "large", DataType::String, false}};
  large.callback = [](const FunctionInputs&) noexcept -> StatusOr<Row> {
    return Row{std::string(1000, 'x')};
  };
  ASSERT_TRUE(registry->add(std::move(large)).ok());
  auto input = task("large-result");
  input.function_id = "large";
  ASSERT_TRUE(executor->submit(input).ok());
  auto result = collect(1);
  ASSERT_EQ(result.size(), 1U);
  EXPECT_EQ(result[0].status.code(), ErrorCode::ResourceExhausted);
  EXPECT_TRUE(result[0].output.empty());
  EXPECT_EQ(executor->metrics().active_tasks, 0U);
}

TEST_P(FunctionExecutorContract, CloseCancelsQueuedWorkAndPreservesDrainableTerminalResults) {
  auto first = task("a");
  auto second = task("b");
  first.ready_at_us = second.ready_at_us = INT64_MAX;
  ASSERT_TRUE(executor->submit(first).ok());
  ASSERT_TRUE(executor->submit(second).ok());
  ASSERT_TRUE(executor->close().ok());
  ASSERT_TRUE(executor->close().ok());
  EXPECT_EQ(executor->submit(task("new")).code(), ErrorCode::FailedPrecondition);
  auto result = take(executor->poll(10));
  ASSERT_EQ(result.size(), 2U);
  EXPECT_TRUE(result[0].cancelled);
  EXPECT_TRUE(result[1].cancelled);
  EXPECT_TRUE(take(executor->poll(1)).empty());
  EXPECT_EQ(executor->metrics().callback_calls, 0U);
}

TEST_P(FunctionExecutorContract, ExactReceiptsReplayWithoutReexecutionAndEnforceContentIdentity) {
  config.max_receipts = 1;
  config.max_cache_entries = 0;
  open();
  ASSERT_TRUE(executor->submit(task("a", 1)).ok());
  const auto first = collect(1)[0];
  ASSERT_TRUE(executor->submit(task("a", 1)).ok());
  EXPECT_EQ(executor->submit(task("a", 2)).code(), ErrorCode::Conflict);
  const auto replay = collect(1)[0];
  EXPECT_EQ(replay.output, first.output);
  EXPECT_EQ(replay.completed_at_us, first.completed_at_us);
  EXPECT_EQ(executor->metrics().callback_calls, 1U);
  EXPECT_EQ(executor->metrics().receipt_replays, 1U);
  ASSERT_TRUE(executor->submit(task("b", 3)).ok());
  collect(1);
  EXPECT_EQ(executor->metrics().receipt_entries, 1U);
  ASSERT_TRUE(executor->submit(task("a", 5)).ok());
  EXPECT_EQ(collect(1)[0].output, Row{std::int64_t{6}});
  EXPECT_EQ(executor->metrics().callback_calls, 3U);
}

TEST_P(FunctionExecutorContract, ReceiptReplayAlsoReservesTaskAndCompletionCapacity) {
  config.max_tasks = 1;
  open();
  ASSERT_TRUE(executor->submit(task("a")).ok());
  collect(1);
  ASSERT_TRUE(executor->submit(task("a")).ok());
  EXPECT_EQ(executor->submit(task("b")).code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(executor->metrics().completed_tasks, 1U);
  EXPECT_GT(executor->metrics().reserved_completion_bytes, 0U);
  collect(1);
  ASSERT_TRUE(executor->submit(task("b")).ok());
  collect(1);
}

TEST_P(FunctionExecutorContract, PureResultsReuseExactInputsAcrossTaskIdentitiesButNotVersions) {
  ASSERT_TRUE(executor->submit(task("request-a")).ok());
  EXPECT_FALSE(collect(1)[0].from_cache);
  ASSERT_TRUE(executor->submit(task("request-b")).ok());
  EXPECT_TRUE(collect(1)[0].from_cache);
  EXPECT_EQ(executor->metrics().callback_calls, 1U);
  auto version2 = addition();
  version2.signature.version = 2;
  version2.signature.implementation_identity = "builtin:add:v2";
  version2.callback = [](const FunctionInputs& inputs) noexcept -> StatusOr<Row> {
    return Row{std::get<std::int64_t>((*inputs[0])[0]) + 2};
  };
  ASSERT_TRUE(registry->add(std::move(version2)).ok());
  auto input = task("request-c");
  input.function_version = 2;
  input.implementation_identity = "builtin:add:v2";
  ASSERT_TRUE(executor->submit(input).ok());
  auto result = collect(1)[0];
  EXPECT_FALSE(result.from_cache);
  EXPECT_EQ(result.output, Row{std::int64_t{5}});
  EXPECT_EQ(executor->metrics().callback_calls, 2U);
}

TEST_P(FunctionExecutorContract, CacheAndReceiptByteLimitsAreBoundedAndImpureCallbacksBypassCache) {
  config.max_cache_entries = 1;
  config.max_cache_bytes = 4096;
  config.max_receipts = 1;
  config.max_receipt_bytes = 4096;
  open();
  ASSERT_TRUE(executor->submit(task("a", 1)).ok());
  collect(1);
  ASSERT_TRUE(executor->submit(task("b", 2)).ok());
  collect(1);
  auto metrics = executor->metrics();
  EXPECT_LE(metrics.cache_entries, 1U);
  EXPECT_LE(metrics.cache_bytes, 4096U);
  EXPECT_LE(metrics.receipt_entries, 1U);
  EXPECT_LE(metrics.receipt_bytes, 4096U);
  EXPECT_GE(metrics.cache_evictions, 1U);
  auto impure = addition();
  impure.signature.function_id = "impure";
  impure.signature.pure = false;
  ASSERT_TRUE(registry->add(std::move(impure)).ok());
  const auto calls = executor->metrics().callback_calls;
  for (const auto* id : {"impure-a", "impure-b"}) {
    auto input = task(id);
    input.function_id = "impure";
    ASSERT_TRUE(executor->submit(input).ok());
    EXPECT_FALSE(collect(1)[0].from_cache);
  }
  EXPECT_EQ(executor->metrics().callback_calls, calls + 2);
}

TEST_P(FunctionExecutorContract, OptionalMissingAndPresentNullAreDistinctTypedCacheInputs) {
  auto optional = addition();
  optional.signature.function_id = "optional";
  optional.signature.inputs[0].required = false;
  optional.signature.inputs[0].fields[0].nullable = true;
  optional.callback = [](const FunctionInputs& inputs) noexcept -> StatusOr<Row> {
    return Row{std::int64_t{!inputs[0]                                                ? 1
                            : std::holds_alternative<std::monostate>((*inputs[0])[0]) ? 2
                                                                                      : 3}};
  };
  ASSERT_TRUE(registry->add(std::move(optional)).ok());
  auto input = task("missing");
  input.function_id = "optional";
  input.inputs = {std::nullopt};
  ASSERT_TRUE(executor->submit(input).ok());
  EXPECT_EQ(collect(1)[0].output, Row{std::int64_t{1}});
  input.task_id = "present-null";
  input.inputs = {Row{std::monostate{}}};
  ASSERT_TRUE(executor->submit(input).ok());
  EXPECT_EQ(collect(1)[0].output, Row{std::int64_t{2}});
  EXPECT_EQ(executor->metrics().cache_hits, 0U);
}

TEST_P(FunctionExecutorContract, ValidatesPollArgumentsAndUnknownCancellation) {
  EXPECT_FALSE(executor->poll(0).ok());
  EXPECT_FALSE(executor->poll(1, -1).ok());
  EXPECT_EQ(executor->cancel("unknown").code(), ErrorCode::NotFound);
}

TEST_P(FunctionExecutorContract,
       InputAndCompletionBudgetsRejectWithoutExecutingOrLosingReservations) {
  auto input = task("a");
  input.ready_at_us = INT64_MAX;
  ASSERT_TRUE(executor->submit(input).ok());
  const auto reserved = executor->metrics().reserved_completion_bytes;
  ASSERT_TRUE(executor->cancel("a").ok());
  collect(1);
  config.max_tasks = 2;
  config.max_completion_bytes = reserved;
  open();
  ASSERT_TRUE(executor->submit(input).ok());
  input.task_id = "b";
  EXPECT_EQ(executor->submit(input).code(), ErrorCode::ResourceExhausted);
  ASSERT_TRUE(executor->cancel("a").ok());
  collect(1);
  ASSERT_TRUE(executor->submit(input).ok());
  ASSERT_TRUE(executor->cancel("b").ok());
  collect(1);
  config.max_input_bytes = 1;
  open();
  EXPECT_EQ(executor->submit(task()).code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(executor->metrics().active_tasks, 0U);
  EXPECT_EQ(executor->metrics().callback_calls, 0U);
}

TEST_P(FunctionExecutorContract, RejectsNonfiniteAndInvalidUtf8ValuesAtBothBoundaries) {
  auto floating = addition();
  floating.signature.function_id = "floating";
  floating.signature.inputs[0].fields[0].type = DataType::Float64;
  floating.signature.output_fields[0].type = DataType::Float64;
  floating.callback = [](const FunctionInputs&) noexcept -> StatusOr<Row> {
    return Row{std::numeric_limits<double>::infinity()};
  };
  ASSERT_TRUE(registry->add(std::move(floating)).ok());
  auto input = task("not-a-number");
  input.function_id = "floating";
  input.inputs = {Row{std::numeric_limits<double>::quiet_NaN()}};
  EXPECT_EQ(executor->submit(input).code(), ErrorCode::InvalidArgument);
  input.inputs = {Row{1.0}};
  ASSERT_TRUE(executor->submit(input).ok());
  auto result = collect(1)[0];
  EXPECT_EQ(result.status.code(), ErrorCode::InvalidArgument);
  EXPECT_TRUE(result.output.empty());
  EXPECT_EQ(executor->metrics().cache_entries, 0U);
  auto strings = addition();
  strings.signature.function_id = "strings";
  strings.signature.inputs[0].fields[0].type = DataType::String;
  ASSERT_TRUE(registry->add(std::move(strings)).ok());
  input.task_id = "bad-utf8";
  input.function_id = "strings";
  input.inputs = {Row{std::string("\xff", 1)}};
  EXPECT_EQ(executor->submit(input).code(), ErrorCode::InvalidArgument);
}

TEST_P(FunctionExecutorContract, ExactCacheKeysDistinguishSignedFloatingZero) {
  auto floating = addition();
  floating.signature.function_id = "signbit";
  floating.signature.inputs[0].fields[0].type = DataType::Float64;
  floating.signature.output_fields[0].type = DataType::Bool;
  floating.callback = [](const FunctionInputs& inputs) noexcept -> StatusOr<Row> {
    return Row{std::signbit(std::get<double>((*inputs[0])[0]))};
  };
  ASSERT_TRUE(registry->add(std::move(floating)).ok());
  auto input = task("negative");
  input.function_id = "signbit";
  input.inputs = {Row{-0.0}};
  ASSERT_TRUE(executor->submit(input).ok());
  EXPECT_EQ(collect(1)[0].output, Row{true});
  input.task_id = "positive";
  input.inputs = {Row{0.0}};
  ASSERT_TRUE(executor->submit(input).ok());
  EXPECT_EQ(collect(1)[0].output, Row{false});
  EXPECT_EQ(executor->metrics().callback_calls, 2U);
}

TEST_P(FunctionExecutorContract, ReturnedRowsDoNotMutateCachedResultsOrReceiptCopies) {
  ASSERT_TRUE(executor->submit(task("a")).ok());
  auto first = collect(1);
  first[0].output[0] = std::int64_t{999};
  ASSERT_TRUE(executor->submit(task("a")).ok());
  EXPECT_EQ(collect(1)[0].output, Row{std::int64_t{4}});
  ASSERT_TRUE(executor->submit(task("b")).ok());
  EXPECT_EQ(collect(1)[0].output, Row{std::int64_t{4}});
}

INSTANTIATE_TEST_SUITE_P(SimpleAndThreads, FunctionExecutorContract, ::testing::Bool());

struct ReleaseGate {
  std::latch& gate;
  bool released = false;

  ~ReleaseGate() noexcept {
    release();
  }

  void release() noexcept {
    if (!released) {
      released = true;
      gate.count_down();
    }
  }
};

TEST(ThreadPoolFunctionExecutor, RealWorkersRunCallbacksConcurrently) {
  auto registry = take(FunctionRegistry::create());
  std::latch entered(2);
  std::latch gate(1);
  auto function = addition();
  function.callback = [&](const FunctionInputs& inputs) noexcept -> StatusOr<Row> {
    entered.count_down();
    gate.wait();
    return Row{std::get<std::int64_t>((*inputs[0])[0])};
  };
  ASSERT_TRUE(registry->add(std::move(function)).ok());
  FunctionExecutorConfig config;
  config.worker_count = 2;
  auto executor = take(ThreadPoolFunctionExecutor::create(registry, config));
  ReleaseGate release{gate};
  ASSERT_TRUE(executor->submit(task("a", 1)).ok());
  ASSERT_TRUE(executor->submit(task("b", 2)).ok());
  entered.wait();
  EXPECT_EQ(executor->metrics().running_tasks, 2U);
  release.release();
  auto first = take(executor->poll(2, 5000));
  if (first.size() < 2) {
    auto next = take(executor->poll(2 - first.size(), 5000));
    for (auto& item : next)
      first.push_back(std::move(item));
  }
  ASSERT_EQ(first.size(), 2U);
  EXPECT_TRUE(first[0].status.ok());
  EXPECT_TRUE(first[1].status.ok());
  EXPECT_EQ(executor->metrics().callback_calls, 2U);
}

TEST(ThreadPoolFunctionExecutor, RunningCancellationRetainsBudgetUntilBoundedCodeReturns) {
  auto registry = take(FunctionRegistry::create());
  std::latch entered(1);
  std::latch gate(1);
  auto function = addition();
  function.callback = [&](const FunctionInputs&) noexcept -> StatusOr<Row> {
    entered.count_down();
    gate.wait();
    return Row{std::int64_t{99}};
  };
  ASSERT_TRUE(registry->add(std::move(function)).ok());
  FunctionExecutorConfig config;
  config.max_tasks = 1;
  config.worker_count = 1;
  auto executor = take(ThreadPoolFunctionExecutor::create(registry, config));
  ReleaseGate release{gate};
  ASSERT_TRUE(executor->submit(task("running")).ok());
  entered.wait();
  ASSERT_TRUE(executor->cancel("running").ok());
  EXPECT_TRUE(take(executor->poll(1)).empty());
  EXPECT_EQ(executor->submit(task("replacement")).code(), ErrorCode::ResourceExhausted);
  release.release();
  auto completed = take(executor->poll(1, 5000));
  ASSERT_EQ(completed.size(), 1U);
  EXPECT_TRUE(completed[0].cancelled);
  EXPECT_TRUE(completed[0].output.empty());
  EXPECT_EQ(completed[0].task_id, "running");
  EXPECT_EQ(executor->metrics().active_tasks, 0U);
  EXPECT_EQ(executor->metrics().cache_entries, 0U);
}

TEST(ThreadPoolFunctionExecutor, CloseLetsQueuedCancellationDrainWhileJoiningRunningCallback) {
  auto registry = take(FunctionRegistry::create());
  std::latch entered(1);
  std::latch gate(1);
  auto function = addition();
  function.callback = [&](const FunctionInputs&) noexcept -> StatusOr<Row> {
    entered.count_down();
    gate.wait();
    return Row{std::int64_t{1}};
  };
  ASSERT_TRUE(registry->add(std::move(function)).ok());
  FunctionExecutorConfig config;
  config.worker_count = 1;
  config.max_tasks = 2;
  auto executor = take(ThreadPoolFunctionExecutor::create(registry, config));
  ReleaseGate release{gate};
  ASSERT_TRUE(executor->submit(task("running")).ok());
  entered.wait();
  ASSERT_TRUE(executor->submit(task("queued")).ok());
  Status close_status;
  std::thread closer([&]() noexcept { close_status = executor->close(); });
  auto queued = take(executor->poll(1, 5000));
  release.release();
  closer.join();
  ASSERT_TRUE(close_status.ok());
  ASSERT_EQ(queued.size(), 1U);
  EXPECT_TRUE(queued[0].cancelled);
  EXPECT_EQ(queued[0].task_id, "queued");
  auto running = take(executor->poll(1, 5000));
  ASSERT_EQ(running.size(), 1U);
  EXPECT_TRUE(running[0].cancelled);
  EXPECT_EQ(executor->metrics().callback_calls, 1U);
}

TEST(FunctionRegistry, ConcurrentRegistrationLookupAndCallerMutationKeepVersionsImmutable) {
  auto registry = take(FunctionRegistry::create());
  ASSERT_TRUE(registry->add(addition()).ok());
  auto copy = take(registry->describe("add", 1));
  copy.output_fields[0].name = "caller-mutated";
  EXPECT_EQ(take(registry->describe("add", 1)).output_fields[0].name, "result");
  std::latch start(1);
  std::atomic<bool> valid{true};
  std::thread writer([&]() noexcept {
    start.wait();
    for (std::uint64_t version = 2; version < 32; ++version) {
      auto function = addition();
      function.signature.version = version;
      function.signature.implementation_identity = "builtin:add:v" + std::to_string(version);
      if (!registry->add(std::move(function)).ok())
        valid = false;
    }
  });
  std::thread reader([&]() noexcept {
    start.wait();
    for (int i = 0; i < 100; ++i) {
      auto original = registry->describe("add", 1);
      if (!original.ok() || original->implementation_identity != "builtin:add:v1")
        valid = false;
    }
  });
  start.count_down();
  writer.join();
  reader.join();
  EXPECT_TRUE(valid.load());
  EXPECT_EQ(take(registry->describe("add", 31)).version, 31U);
}

TEST(FunctionExecutor, CallbackSpareCapacityDoesNotEscapeIntoRetainedCompletion) {
  for (bool threaded : {false, true}) {
    auto registry = take(FunctionRegistry::create());
    auto function = addition();
    function.signature.output_fields = {{1, "text", DataType::String, false},
                                        {2, "vector", DataType::FloatVector, false}};
    function.callback = [](const FunctionInputs&) noexcept -> StatusOr<Row> {
      Row output;
      output.reserve(100000);
      std::string text = "ok";
      text.reserve(100000);
      std::vector<double> values{1.0};
      values.reserve(100000);
      output.emplace_back(std::move(text));
      output.emplace_back(std::move(values));
      return output;
    };
    ASSERT_TRUE(registry->add(std::move(function)).ok());
    FunctionExecutorConfig config;
    config.max_output_bytes_per_task = 1024;
    std::unique_ptr<IFunctionExecutor> executor;
    if (threaded)
      executor = take(ThreadPoolFunctionExecutor::create(registry, config));
    else
      executor = take(SimpleFunctionExecutor::create(registry, config));
    ASSERT_TRUE(executor->submit(task("capacity")).ok());
    if (!threaded)
      EXPECT_EQ(take(static_cast<SimpleFunctionExecutor*>(executor.get())->run_ready(0)), 1U);
    auto results = take(executor->poll(1, 5000));
    ASSERT_EQ(results.size(), 1U);
    ASSERT_TRUE(results[0].status.ok()) << results[0].status.message();
    const auto& row = results[0].output;
    ASSERT_EQ(row.size(), 2U);
    EXPECT_EQ(std::get<std::string>(row[0]), "ok");
    EXPECT_EQ(std::get<std::vector<double>>(row[1]), std::vector<double>{1.0});
    auto retained = sizeof(Row) + row.capacity() * sizeof(Value) +
                    std::get<std::string>(row[0]).capacity() + 1 +
                    std::get<std::vector<double>>(row[1]).capacity() * sizeof(double);
    EXPECT_LE(retained, config.max_output_bytes_per_task);
  }
}
} // namespace
} // namespace eventflow::test
