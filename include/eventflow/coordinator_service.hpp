#pragma once
#include "eventflow/computation_runtime.hpp"
#include "eventflow/source_poller.hpp"
#include "eventflow/state_store.hpp"
#include <memory>
#include <string>
#include <vector>

namespace eventflow {
struct ComputeNode {
  std::string node_id;
  std::string endpoint;
  std::vector<std::uint32_t> shards;
};

struct SourceSubscription {
  std::shared_ptr<IMessageSource> source;
  MessageSourceConfig delivery;
  std::string relation;
  std::uint32_t poll_interval_ms = 10;
};

struct CoordinatorServiceConfig {
  std::string cluster_id = "eventflow-core";
  std::uint32_t shard_count = 2;
  std::string owner = "deployment-1";
  std::vector<ComputeNode> nodes;
  std::string restore_checkpoint;
  // Zero selects explicit logical clock advancement for deterministic demos/tests.
  std::uint32_t tick_interval_ms = 0;
  std::size_t max_request_bytes = 1024 * 1024;
  std::size_t max_response_bytes = 4 * 1024 * 1024;
  std::optional<SourceSubscription> source;
};

class CoordinatorService {
public:
  static StatusOr<std::unique_ptr<CoordinatorService>>
  create(std::shared_ptr<IStateStore>, const CoordinatorServiceConfig&,
         const ComputationModule& = {}) noexcept;
  ~CoordinatorService() noexcept;
  StatusOr<int> start(const std::string& host, int port) noexcept;
  bool is_running() const noexcept;
  void stop() noexcept;

private:
  struct Impl;
  explicit CoordinatorService(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;
};
} // namespace eventflow
