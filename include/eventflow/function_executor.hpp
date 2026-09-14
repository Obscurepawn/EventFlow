#pragma once

#include "eventflow/types.hpp"
#include <functional>
#include <memory>
#include <optional>

namespace eventflow {

struct FunctionInputSignature {
  std::string alias;
  bool required = true;
  std::vector<Field> fields;
};

struct FunctionSignature {
  std::string function_id;
  std::uint64_t version = 0;
  std::string implementation_identity;
  std::vector<FunctionInputSignature> inputs;
  std::vector<Field> output_fields; // Worker payload only; no host-owned identity columns.
  bool pure = true;
};

using FunctionInputs = std::vector<std::optional<Row>>;
using BuiltinFunctionCallback = std::function<StatusOr<Row>(const FunctionInputs&)>;

struct BuiltinFunction {
  FunctionSignature signature;
  BuiltinFunctionCallback callback;
};

struct FunctionRegistryConfig {
  std::size_t max_functions = 128;
  std::size_t max_signature_bytes = 1024 * 1024;
};

// Immutable entries for compiled-in callbacks. This is not a plugin loader or user UDF family.
class FunctionRegistry {
public:
  static StatusOr<std::shared_ptr<FunctionRegistry>>
  create(const FunctionRegistryConfig& config = {}) noexcept;
  ~FunctionRegistry() noexcept;
  Status add(BuiltinFunction function) noexcept;
  StatusOr<FunctionSignature> describe(std::string_view function_id,
                                       std::uint64_t version) const noexcept;

  // Clone the immutable entries into a private registry before binding a module.
  // Adding new declarations to the clone never publishes into the source registry.
  StatusOr<std::shared_ptr<FunctionRegistry>> clone() const noexcept;

private:
  struct Impl;
  explicit FunctionRegistry(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
  friend struct FunctionExecutorState;
};

struct FunctionTask {
  // The owner canonically encodes scope/request/relation/attempt/owner_epoch here. The executor
  // echoes this opaque identity; it does not authorize KV writes or interpret business fences.
  std::string task_id;
  std::string function_id;
  std::uint64_t function_version = 0;
  std::string implementation_identity;
  FunctionInputs inputs;        // Already frozen; copied on successful submit.
  std::int64_t ready_at_us = 0; // Scheduling only, excluded from reusable-result cache keys.
};

struct FunctionCompletion {
  std::string task_id;
  std::string function_id;
  std::uint64_t function_version = 0;
  std::string implementation_identity;
  Status status;
  Row output;
  bool cancelled = false;
  bool from_cache = false;
  std::int64_t completed_at_us = 0;
};

struct FunctionExecutorConfig {
  // Includes queued, running and completed-but-not-polled tasks. submit never waits for space.
  std::size_t max_tasks = 64;
  std::size_t max_input_bytes = 4 * 1024 * 1024;
  std::size_t max_output_bytes_per_task = 256 * 1024;
  std::size_t max_completion_bytes = 32 * 1024 * 1024;
  std::size_t max_receipts = 64;
  std::size_t max_receipt_bytes = 4 * 1024 * 1024;
  std::size_t max_cache_entries = 64;
  std::size_t max_cache_bytes = 4 * 1024 * 1024;
  std::size_t worker_count = 2;
};

struct FunctionExecutorMetrics {
  std::size_t active_tasks = 0;
  std::size_t queued_tasks = 0;
  std::size_t running_tasks = 0;
  std::size_t completed_tasks = 0;
  std::size_t active_input_bytes = 0;
  std::size_t reserved_completion_bytes = 0;
  std::size_t receipt_entries = 0;
  std::size_t receipt_bytes = 0;
  std::size_t cache_entries = 0;
  std::size_t cache_bytes = 0;
  std::uint64_t submitted = 0;
  std::uint64_t idempotent_submissions = 0;
  std::uint64_t receipt_replays = 0;
  std::uint64_t callback_calls = 0;
  std::uint64_t cache_hits = 0;
  std::uint64_t cache_misses = 0;
  std::uint64_t cache_evictions = 0;
};

class IFunctionExecutor {
public:
  virtual ~IFunctionExecutor() noexcept = default;
  virtual Status submit(const FunctionTask& task) noexcept = 0;
  // Queued cancellation produces a terminal completion without calling user code. Running code
  // is not force-killed: its eventual completion is cancelled and capacity remains reserved.
  virtual Status cancel(std::string_view task_id) noexcept = 0;
  // Transfers ownership and releases the task's reservation. The caller must retain the returned
  // completion while its owner commit is UNKNOWN. Bounded receipts permit exact local replay;
  // after eviction physical re-execution is allowed, so durable owner fences remain necessary.
  virtual StatusOr<std::vector<FunctionCompletion>> poll(std::size_t max_completions,
                                                         int timeout_ms = 0) noexcept = 0;
  virtual FunctionExecutorMetrics metrics() const noexcept = 0;
  // Rejects new work, cancels queued work, and requests running cancellation. Terminal results
  // remain drainable. Threaded close joins bounded callbacks; no C++ thread is force-killed.
  virtual Status close() noexcept = 0;
};

class SimpleFunctionExecutor final : public IFunctionExecutor {
public:
  static StatusOr<std::unique_ptr<SimpleFunctionExecutor>>
  create(std::shared_ptr<FunctionRegistry> registry,
         const FunctionExecutorConfig& config = {}) noexcept;
  ~SimpleFunctionExecutor() noexcept override;
  // Deterministic explicit progress. poll never executes callbacks or sleeps in this adapter.
  StatusOr<std::size_t> run_ready(std::int64_t now_us, std::size_t max_tasks = 64) noexcept;
  Status submit(const FunctionTask&) noexcept override;
  Status cancel(std::string_view task_id) noexcept override;
  StatusOr<std::vector<FunctionCompletion>> poll(std::size_t, int = 0) noexcept override;
  FunctionExecutorMetrics metrics() const noexcept override;
  Status close() noexcept override;

private:
  struct Impl;
  explicit SimpleFunctionExecutor(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

class ThreadPoolFunctionExecutor final : public IFunctionExecutor {
public:
  static StatusOr<std::unique_ptr<ThreadPoolFunctionExecutor>>
  create(std::shared_ptr<FunctionRegistry> registry,
         const FunctionExecutorConfig& config = {}) noexcept;
  ~ThreadPoolFunctionExecutor() noexcept override;
  Status submit(const FunctionTask&) noexcept override;
  Status cancel(std::string_view task_id) noexcept override;
  StatusOr<std::vector<FunctionCompletion>> poll(std::size_t, int = 0) noexcept override;
  FunctionExecutorMetrics metrics() const noexcept override;
  Status close() noexcept override;

private:
  struct Impl;
  explicit ThreadPoolFunctionExecutor(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};
} // namespace eventflow
