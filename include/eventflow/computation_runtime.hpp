#pragma once

#include "eventflow/computation.hpp"
#include <deque>
#include <map>
#include <set>

namespace eventflow {

using ComputationBinder = StatusOr<BoundComputation> (*)(const std::vector<std::string>&) noexcept;

struct ComputationModule {
  std::string name = "sql";
  std::uint64_t version = 1;
  ComputationBinder bind = nullptr;
};

StatusOr<BoundComputation> bind_computation_module(const ComputationModule& module,
                                                   const std::vector<std::string>& ddl) noexcept;

struct ComputationRuntimeConfig {
  bool threaded = true;
  FunctionExecutorConfig executor;
};

struct RuntimeIngestResult {
  bool input_committed = false;
  bool duplicate = false;
  Status operation_status;
};

// A generic shard runtime owns EventProcessor and a bounded function executor.
// The application supplies its complete bound definition; no example policy is installed here.
class ComputationRuntime {
public:
  static StatusOr<std::unique_ptr<ComputationRuntime>>
  open(std::shared_ptr<IStateStore> store, BoundComputation definition, const EngineConfig& engine,
       const ComputationRuntimeConfig& config = {}) noexcept;
  ~ComputationRuntime() noexcept;
  StatusOr<RuntimeIngestResult> ingest_source(std::string_view relation, Row row,
                                              const SourceIdentity& source,
                                              std::int64_t now_us) noexcept;
  Status pump(std::int64_t now_us) noexcept;
  StatusOr<std::size_t> run_functions(std::int64_t now_us, std::size_t limit = 128) noexcept;
  StatusOr<QueryResult> query_sql(std::string_view sql) noexcept;
  StatusOr<RequestRecord> read_request(const RequestKey&) noexcept;
  const BoundComputation& definition() const noexcept;
  EventProcessor& processor() noexcept;
  FunctionExecutorMetrics executor_metrics() const noexcept;
  const char* executor_mode() const noexcept;
  std::size_t executor_worker_count() const noexcept;

private:
  ComputationRuntime() noexcept = default;
  Status consume(ProcessResult result) noexcept;
  Status synchronize() noexcept;
  Status resolve_pending() noexcept;
  Status claim_ready(std::int64_t now_us) noexcept;
  void forget(const std::string& task_id) noexcept;
  BoundComputation definition_;
  ComputationRuntimeConfig config_;
  std::unique_ptr<EventProcessor> processor_;
  std::unique_ptr<IFunctionExecutor> executor_;
  SimpleFunctionExecutor* simple_ = nullptr;
  std::map<std::string, RequestRecord> active_;
  std::map<std::string, RequestKey> task_owners_;
  std::set<std::string> submitted_;
  std::deque<FunctionCompletion> completions_;
};

} // namespace eventflow
