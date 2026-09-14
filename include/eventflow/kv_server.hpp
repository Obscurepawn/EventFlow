#pragma once
#include "eventflow/state_store.hpp"
#include <memory>
#include <string>

namespace eventflow {
class KvServer {
public:
  static StatusOr<std::unique_ptr<KvServer>> create(std::shared_ptr<IStateStore> store) noexcept;
  ~KvServer() noexcept;
  StatusOr<int> start(const std::string& host, int port) noexcept;
  bool is_running() const noexcept;
  void stop() noexcept;

private:
  struct Impl;
  explicit KvServer(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};
} // namespace eventflow
