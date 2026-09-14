#pragma once
#include "eventflow/catalog.hpp"
#include "eventflow/state_store.hpp"
#include <functional>

namespace eventflow {
struct EngineState;
struct RoundState;
struct EngineRuntimeAccess;
struct RuntimeCause;
class FunctionRegistry;

struct RowChange {
  RelationId relation = 0;
  Row identity; // Physical identity order; images include hidden storage columns.
  std::optional<Row> old_row;
  std::optional<Row> new_row;
};

struct TableChange {
  RelationId relation = 0;
  Row identity;
  std::optional<Row> row; // nullopt is Delete; a Row is a full storage-schema Upsert.
};

struct DispatchContext {
  std::int64_t processing_time_us = 0;
};

struct EngineConfig {
  std::uint32_t shard = 0;
  std::uint32_t shard_count = 8;
  std::string owner = "deployment-1";
  std::size_t max_rows = 10000;
  std::size_t max_bytes = 4 * 1024 * 1024;
  std::size_t max_mutations = 10000;
  std::size_t page_rows = 64;
  // Per-Engine limits; a Compute node must divide its budget among instances.
  // Either zero limit disables the internal SQL plan cache.
  std::size_t max_cached_queries = 128;
  std::size_t max_query_cache_bytes = 4 * 1024 * 1024;
};

// Resolves a conservative, simultaneously usable budget against the actual adapter.
// No shard state is read or changed. Meta can persist the returned effective values.
StatusOr<EngineConfig> resolve_engine_config(IStateStore& store,
                                             const EngineConfig& requested) noexcept;

struct QueryCacheMetrics {
  std::uint64_t hits = 0;
  std::uint64_t misses = 0;
  std::uint64_t evictions = 0;
  std::size_t bytes = 0;
  std::size_t entries = 0;
};

struct QueryContext {
  std::optional<Row> scope;
  std::size_t max_rows = 10000;
  std::size_t max_bytes = 4 * 1024 * 1024;
};

struct QueryResult {
  std::vector<Field> fields;
  std::vector<Row> rows;
  std::uint64_t revision = 0;
  std::uint64_t catalog_version = 0;
};

struct DispatchResult {
  CommitResult commit;
  std::vector<RowChange> changes;
};

struct InstallResult {
  RelationId relation = 0;
  CommitResult commit;
};
enum class StateImage { Before, Staged };

class RoundContext {
public:
  ~RoundContext() noexcept;
  std::int64_t processing_time_us() const noexcept;
  std::uint32_t shard() const noexcept;
  std::uint64_t revision() const noexcept;
  StatusOr<std::optional<std::string>> read(const std::string& key,
                                            StateImage image = StateImage::Staged) noexcept;
  Status stage(Mutation mutation) noexcept;
  StatusOr<std::optional<Row>> read_relation(RelationId relation, const Row& identity,
                                             StateImage image = StateImage::Staged) noexcept;
  // Returns complete storage-schema rows for one business scope. Both the per-call capture
  // limits and the shared round read budget apply; this is not a historical/MVCC read API.
  StatusOr<std::vector<Row>> scan_relation(RelationId relation, const Row& scope,
                                           StateImage image = StateImage::Staged,
                                           std::size_t max_rows = 10000,
                                           std::size_t max_bytes = 4 * 1024 * 1024) noexcept;
  Status emit(RowChange change) noexcept;

private:
  explicit RoundContext(std::unique_ptr<RoundState> state) noexcept;
  std::unique_ptr<RoundState> state_;
  friend struct EngineState;
  friend class Engine;
  friend struct EngineRuntimeAccess;
};

using RoundHook = std::function<Status(RoundContext&, const std::vector<RowChange>&)>;

class Engine {
public:
  static StatusOr<std::unique_ptr<Engine>> open(std::shared_ptr<IStateStore> store,
                                                CatalogSnapshot catalog,
                                                const EngineConfig& config = {}) noexcept;
  ~Engine() noexcept;
  StatusOr<DispatchResult> apply(const TableChange& change, const DispatchContext& context = {},
                                 const RoundHook& hook = {}) noexcept;
  StatusOr<DispatchResult> apply_many(const std::vector<TableChange>& changes,
                                      const DispatchContext& context = {},
                                      const RoundHook& hook = {}) noexcept;
  StatusOr<DispatchResult> resolve_pending_commit() noexcept;
  StatusOr<InstallResult> install(const StatementAst& definition,
                                  const DefinitionOptions& options = {}) noexcept;
  StatusOr<InstallResult> install_sql(std::string_view sql,
                                      const DefinitionOptions& options = {}) noexcept;
  StatusOr<QueryResult> query(const BoundPlan& plan, const QueryContext& context = {}) noexcept;
  StatusOr<QueryResult> query_sql(std::string_view sql, const QueryContext& context = {}) noexcept;
  StatusOr<CatalogSnapshot> snapshot() const noexcept;
  std::uint64_t revision() const noexcept;
  bool has_pending_commit() const noexcept;
  bool failed() const noexcept;
  QueryCacheMetrics query_cache_metrics() const noexcept;
  const EngineConfig& config() const noexcept;

private:
  explicit Engine(std::unique_ptr<EngineState> state) noexcept;
  static StatusOr<std::unique_ptr<Engine>>
  open_runtime(std::shared_ptr<IStateStore> store, CatalogSnapshot catalog,
               const EngineConfig& config, std::shared_ptr<FunctionRegistry> registry) noexcept;
  // Only callers holding OperationGuard may execute an internally trusted plan.
  StatusOr<QueryResult> query_trusted(const BoundPlan& plan, const QueryContext& context) noexcept;
  StatusOr<DispatchResult> resolve_commit(bool request_owner) noexcept;
  StatusOr<DispatchResult> apply_input_round(
      const std::vector<TableChange>& inputs, const DispatchContext& context, const RoundHook& hook,
      bool recover_capacity,
      const std::function<StatusOr<std::vector<TableChange>>(RoundContext&)>& owned_prepare = {},
      const RuntimeCause* cause = nullptr) noexcept;
  std::unique_ptr<EngineState> state_;
  friend struct EngineRuntimeAccess;
};
} // namespace eventflow
