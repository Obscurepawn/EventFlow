#pragma once

#include "eventflow/message_source.hpp"

namespace eventflow {

struct HttpMessageServerConfig {
  std::vector<SourceAssignment> partitions;
  std::size_t max_messages = 10000;
  std::size_t max_log_bytes = 64 * 1024 * 1024;
  std::size_t max_message_bytes = 1024 * 1024;
};

// A bounded in-memory HTTP delivery service for local deployments and contract tests.
// Acknowledgement never deletes retained records, so database checkpoints can replay.
class HttpMessageServer {
public:
  static StatusOr<std::unique_ptr<HttpMessageServer>>
  create(const HttpMessageServerConfig&) noexcept;
  ~HttpMessageServer() noexcept;
  StatusOr<int> start(const std::string& host, int port) noexcept;
  void stop() noexcept;
  bool is_running() const noexcept;
  Status append(const std::string& topic, std::int32_t partition, std::int64_t offset,
                std::optional<std::string> key, std::optional<std::string> payload,
                std::optional<std::int64_t> timestamp_ms = {}) noexcept;

private:
  struct Impl;
  explicit HttpMessageServer(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace eventflow
