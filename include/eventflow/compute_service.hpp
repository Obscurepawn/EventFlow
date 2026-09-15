#pragma once

#include "eventflow/computation_runtime.hpp"
#include "eventflow/state_store.hpp"
#include <string>
#include <vector>

namespace eventflow {
struct ComputeServiceConfig {
  std::string node_id;
  std::uint32_t shard_count = 1;
  std::vector<std::uint32_t> shards;
  std::string owner = "deployment-1";
  std::size_t max_request_bytes = 1024 * 1024;
  std::size_t max_response_bytes = 4 * 1024 * 1024;
  std::size_t max_functions_per_call = 4096;
  std::size_t worker_threads = 4;
  std::size_t max_queued_requests = 64;
  bool allow_native_udfs = false;
};

// One immutable shard assignment per process. HTTP operations serialize each
// shard owner; a global admission gate establishes pause/checkpoint barriers.
// The production process supplies RemoteStateStore; tests may inject local KV.
class ComputeService {
public:
  static StatusOr<std::unique_ptr<ComputeService>> create(std::shared_ptr<IStateStore>,
                                                          const ComputeServiceConfig&,
                                                          const ComputationModule& = {}) noexcept;
  ~ComputeService() noexcept;
  StatusOr<int> start(const std::string& host, int port) noexcept;
  bool is_running() const noexcept;
  void stop() noexcept;

private:
  struct Impl;
  explicit ComputeService(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;
};
} // namespace eventflow
