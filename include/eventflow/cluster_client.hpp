#pragma once
#include "eventflow/source_identity.hpp"
#include "eventflow/submitted_computation.hpp"
#include "eventflow/types.hpp"
#include <memory>
#include <optional>

namespace eventflow {
struct ClusterClientConfig {
  int timeout_seconds = 10;
  std::size_t max_request_bytes = 1024 * 1024;
  std::size_t max_response_bytes = 16 * 1024 * 1024;
  // Only ingest may retry transient transport/server failures; every attempt uses identical bytes.
  std::size_t ingest_attempts = 1;
  std::size_t max_udf_request_bytes = 2 * max_udf_bytes + 65536;
};

struct ClusterIngestResult {
  bool input_committed = false;
  bool duplicate = false;
  Status operation_status;
  std::uint32_t shard = 0;
  std::string node_id;
  std::optional<std::uint64_t> revision;
  Json details;
};

struct ClusterQueryResult {
  std::vector<Field> fields;
  std::vector<Row> rows;
  std::uint64_t revision = 0;
  std::uint64_t catalog_version = 0;
  std::uint32_t shard = 0;
  std::string node_id;
  Json details;
};

// Bootstrap-only synchronous SDK. Coordinator owns routing and admission decisions.
// Control mutations are sent once; a transport failure does not prove they were not applied.
class ClusterClient {
public:
  static StatusOr<std::unique_ptr<ClusterClient>>
  open(std::string_view endpoint, const ClusterClientConfig& config = {}) noexcept;
  ~ClusterClient() noexcept;
  StatusOr<Json> discover() noexcept;
  StatusOr<Json> catalog() noexcept;
  StatusOr<Json> ddl(std::string_view sql) noexcept;
  StatusOr<Json> deploy() noexcept;
  StatusOr<FunctionRef> register_udf(const std::string& library_path) noexcept;
  StatusOr<Json> submit_computation(const SubmittedComputation& definition) noexcept;
  StatusOr<ClusterIngestResult> ingest(std::string_view relation, const Row& row,
                                       const SourceIdentity& source, std::int64_t now_us,
                                       std::optional<std::int64_t> event_time_us = {}) noexcept;
  StatusOr<Json> pump(std::int64_t now_us, std::size_t max_functions = 4096) noexcept;
  // Continue successful pump rounds at one logical time until runnable work settles.
  // Return immediately on an ambiguous failure or an unresolved input clock barrier.
  StatusOr<Json> drain(std::int64_t now_us, std::size_t max_rounds = 64,
                       std::size_t max_functions = 4096) noexcept;
  StatusOr<ClusterQueryResult> query(std::string_view sql, const Row& scope,
                                     std::size_t max_rows = 10000,
                                     std::size_t max_bytes = 4 * 1024 * 1024) noexcept;
  StatusOr<Json> pause() noexcept;
  StatusOr<Json> resume() noexcept;
  StatusOr<Json> checkpoint(std::string_view checkpoint_id, std::int64_t now_us) noexcept;
  StatusOr<Json> status() noexcept;
  StatusOr<Json> read_request(const Row& scope, std::string_view request_id) noexcept;

private:
  struct Impl;
  explicit ClusterClient(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};
} // namespace eventflow
