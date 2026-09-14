#pragma once
#include "eventflow/kv_server.hpp"
#include <memory>
#include <thread>

namespace eventflow::test {
// Tests the same contract over HTTP; the backend is still the real selected adapter.
class RemoteHarnessStore final : public IStateStore {
public:
  static StatusOr<std::unique_ptr<RemoteHarnessStore>>
  open(std::unique_ptr<IStateStore> backend) noexcept {
    std::shared_ptr<IStateStore> shared(std::move(backend));
    EF_ASSIGN_OR_RETURN(auto server, KvServer::create(std::move(shared)));
    EF_ASSIGN_OR_RETURN(auto port, server->start("127.0.0.1", 0));
    EF_ASSIGN_OR_RETURN(auto client,
                        RemoteStateStore::open("http://127.0.0.1:" + std::to_string(port)));
    auto result = std::unique_ptr<RemoteHarnessStore>(
        new RemoteHarnessStore(std::move(server), std::move(client)));
    return result;
  }

  ~RemoteHarnessStore() noexcept override {
    client_.reset();
    server_->stop();
  }

  StatusOr<ReadResult> get_many(std::uint32_t shard,
                                const std::vector<std::string>& keys) noexcept override {
    return client_->get_many(shard, keys);
  }

  StatusOr<ScanResult> scan(std::uint32_t shard, const std::string& prefix,
                            const std::optional<std::string>& after,
                            std::size_t limit) noexcept override {
    return client_->scan(shard, prefix, after, limit);
  }

  StatusOr<CommitResult> commit(const CommitBatch& batch) noexcept override {
    return client_->commit(batch);
  }

  StatusOr<StoreLimits> limits() noexcept override {
    return client_->limits();
  }

  StatusOr<ShardStatus> status(std::uint32_t shard) noexcept override {
    return client_->status(shard);
  }

private:
  RemoteHarnessStore(std::unique_ptr<KvServer> server,
                     std::unique_ptr<RemoteStateStore> client) noexcept
      : server_(std::move(server)), client_(std::move(client)) {}

  std::unique_ptr<KvServer> server_;
  std::unique_ptr<RemoteStateStore> client_;
};
} // namespace eventflow::test
