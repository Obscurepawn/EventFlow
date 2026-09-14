#pragma once

#include "eventflow/computation_runtime.hpp"
#include "eventflow/event_processor.hpp"
#include <deque>
#include <set>

namespace eventflow::example {

// Retained exclusively as a fixture for existing regression tests.
// Application policy and deterministic demonstration functions, not trading advice.
struct DemoConfig {
  std::int64_t candidate_ttl_us = 300'000'000;
  std::int64_t ready_wait_us = 2'000'000;
  std::int64_t quote_max_age_us = 2'000'000;
  std::int64_t confirmation_window_us = 30'000'000;
  double confirmation_return = 0.001;
  std::int64_t trade_window_us = 10'000'000;
  std::int64_t confirmation_volume = 100;
  std::int64_t periodic_interval_us = 5'000'000;
  std::int64_t history_window_us = 120'000'000;
  std::int64_t request_lifetime_us = 30'000'000;
  std::int64_t encoder_version = 1;
  std::size_t history_max_rows = 256;
  std::size_t audit_limit = 1024;
  // Durable high-water records are independent of the TTL-limited news body.
  std::uint32_t news_version_namespace_id = 0xfffe0000U;
};

struct DemoRelations {
  RelationId news = 0, quote = 0, history = 0, trade = 0;
  RelationId candidate = 0, request_status = 0;
  RelationId text_capture = 0, text_sql = 0, embedding = 0, latest_embedding = 0;
  RelationId quote_capture = 0, history_capture = 0, embedding_capture = 0;
  RelationId market_sql = 0, features = 0, prediction = 0, risk = 0;
  RelationId latest_prediction = 0, latest_risk = 0;
};

struct DemoDefinition {
  CatalogSnapshot catalog;
  std::shared_ptr<FunctionRegistry> registry;
  ProcessorConfig processor;
  DemoRelations relations;
  static StatusOr<DemoDefinition> create(const DemoConfig& config = {},
                                         const std::vector<std::string>& ddl = {}) noexcept;
};

struct DemoIngestResult {
  bool input_committed = false;
  bool duplicate = false;
  Status operation_status;
};

struct DemoRequestEvent {
  std::uint64_t revision = 0;
  std::int64_t time_us = 0;
  RequestOperationResult result;
};

// One owner advances each shard; Catalog/RequestMachine define all dependencies.
// Standalone runs use deterministic execution. Compute services select the thread pool.
class FeatureComputationDemo {
public:
  static StatusOr<std::unique_ptr<FeatureComputationDemo>>
  open(std::shared_ptr<IStateStore> store, const DemoConfig& config = {},
       const EngineConfig& engine = EngineConfig{.shard_count = 1, .max_rows = 20000},
       const std::vector<std::string>& ddl = {}, bool threaded = false) noexcept;
  ~FeatureComputationDemo() noexcept;
  // Rows use the declared Catalog schema; source identity and offsets are driver-owned.
  Status ingest(std::string_view relation, Row row, std::int64_t now_us) noexcept;
  // Network/source adapters preserve the original delivery identity on every retry.
  // Input confirmation is independent of later trigger/function backpressure.
  StatusOr<DemoIngestResult> ingest_source(std::string_view relation, Row row,
                                           const SourceIdentity& source,
                                           std::int64_t now_us) noexcept;
  Status pump(std::int64_t now_us) noexcept;
  StatusOr<std::size_t> run_functions(std::int64_t now_us, std::size_t limit = 128) noexcept;
  StatusOr<QueryResult> query_sql(std::string_view sql) noexcept;
  StatusOr<RequestRecord> read_request(const RequestKey& key) noexcept;
  const std::deque<DemoRequestEvent>& request_events() const noexcept;
  const std::deque<ClockFiring>& clock_events() const noexcept;
  const DemoDefinition& definition() const noexcept;
  EventProcessor& processor() noexcept;
  FunctionExecutorMetrics executor_metrics() const noexcept;
  const char* executor_mode() const noexcept;
  std::size_t executor_worker_count() const noexcept;

private:
  FeatureComputationDemo() noexcept = default;
  Status consume(ProcessResult result) noexcept;
  Status claim_ready(std::int64_t now_us) noexcept;
  Status resolve_pending() noexcept;
  Status synchronize_confirmed() noexcept;
  void forget_completion(const std::string& task_id) noexcept;
  DemoConfig config_;
  DemoDefinition definition_;
  std::unique_ptr<EventProcessor> processor_;
  std::unique_ptr<IFunctionExecutor> executor_;
  SimpleFunctionExecutor* simple_executor_ = nullptr;
  std::size_t executor_workers_ = 0;
  std::map<std::string, RequestRecord> active_;
  std::map<std::string, RequestKey> task_owners_;
  std::set<std::string> submitted_tasks_;
  // Polled results remain owned until their durable request/attempt is settled.
  std::deque<FunctionCompletion> completions_;
  std::deque<DemoRequestEvent> request_events_;
  std::deque<ClockFiring> clock_events_;
  std::int64_t next_offset_ = 0;
};
} // namespace eventflow::example

namespace eventflow::test_support {

// Explicitly installs the legacy business graph for existing regression tests.
ComputationModule legacy_feature_module() noexcept;

} // namespace eventflow::test_support
