#pragma once
#include "eventflow/status.hpp"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace eventflow {
struct Mutation {
  std::string key;
  std::optional<std::string> value; // nullopt means Delete; an empty string is a value.
};

struct CommitBatch {
  std::uint32_t shard = 0;
  std::string owner;
  std::uint64_t expected_revision = 0;
  std::string commit_id;
  std::vector<Mutation> mutations;
};
enum class CommitCode { Committed, Rejected, Unknown };

struct CommitResult {
  CommitCode code = CommitCode::Rejected;
  std::uint64_t revision = 0;
  bool duplicate = false;
  std::string message;
};

struct ReadResult {
  std::uint64_t revision = 0;
  std::vector<std::optional<std::string>> values;
};

struct ScanResult {
  std::uint64_t revision = 0;
  std::vector<std::pair<std::string, std::string>> rows;
};

struct ShardStatus {
  std::uint64_t revision = 0;
  std::string last_commit_id;
};

// A storage boundary only; source positions and runtime state belong to the coordinator.
struct StoreSnapshot {
  std::string snapshot_id;
  std::vector<ShardStatus> shards;
  std::vector<std::string> shard_owners;
};

struct StoreConfig {
  std::uint32_t shard_count = 8;
  std::string owner = "deployment-1";
  std::vector<std::string>
      shard_owners; // Optional per-shard assignment tokens; otherwise use owner.
  std::size_t max_batch_bytes = 4 * 1024 * 1024;
  std::size_t max_mutations = 10000;
  std::size_t max_read_keys = 10000;
  std::size_t max_read_bytes =
      4 * 1024 * 1024; // Raw key + value bytes per get_many or scan response.
  std::size_t max_scan_rows = 10000;
  std::size_t max_snapshot_count = 64;
};

struct StoreLimits {
  // A simultaneously usable adapter budget. A remote adapter may advertise less
  // than the backend accepts to reserve its wire encoding overhead.
  // Complete canonical batch bytes, including header, identity and mutation framing.
  std::size_t max_batch_bytes = 0;
  std::size_t max_mutations = 0;
  std::size_t max_read_keys = 0;
  // Raw key and value bytes per get_many or scan response.
  std::size_t max_read_bytes = 0;
  std::size_t max_scan_rows = 0;
};

class IStateStore {
public:
  virtual ~IStateStore() noexcept = default;
  virtual StatusOr<ReadResult> get_many(std::uint32_t shard,
                                        const std::vector<std::string>& keys) noexcept = 0;
  virtual StatusOr<ScanResult> scan(std::uint32_t shard, const std::string& prefix,
                                    const std::optional<std::string>& after,
                                    std::size_t limit) noexcept = 0;
  virtual StatusOr<CommitResult> commit(const CommitBatch& batch) noexcept = 0;
  virtual StatusOr<ShardStatus> status(std::uint32_t shard) noexcept = 0;

  // IDs contain 1..128 ASCII letters, digits, '-' or '_'. Existing IDs are immutable.
  virtual StatusOr<StoreSnapshot> create_snapshot(const std::string&) noexcept {
    return Status(ErrorCode::FailedPrecondition, "store does not support snapshots");
  }

  virtual StatusOr<StoreSnapshot> snapshot_status(const std::string&) noexcept {
    return Status(ErrorCode::FailedPrecondition, "store does not support snapshots");
  }

  // Reports effective adapter capacity without reading or changing shard state.
  virtual StatusOr<StoreLimits> limits() noexcept {
    return Status(ErrorCode::FailedPrecondition, "store does not report limits");
  }
};

class MemoryStateStore final : public IStateStore {
public:
  static StatusOr<std::unique_ptr<MemoryStateStore>> open(const StoreConfig& config = {}) noexcept;
  // Snapshots are process-local and retained by source. The returned store is independent.
  static StatusOr<std::unique_ptr<MemoryStateStore>>
  from_snapshot(const MemoryStateStore& source, const std::string& snapshot_id) noexcept;
  ~MemoryStateStore() noexcept override;
  StatusOr<ReadResult> get_many(std::uint32_t, const std::vector<std::string>&) noexcept override;
  StatusOr<ScanResult> scan(std::uint32_t, const std::string&, const std::optional<std::string>&,
                            std::size_t) noexcept override;
  StatusOr<CommitResult> commit(const CommitBatch&) noexcept override;
  StatusOr<ShardStatus> status(std::uint32_t) noexcept override;
  StatusOr<StoreSnapshot> create_snapshot(const std::string&) noexcept override;
  StatusOr<StoreSnapshot> snapshot_status(const std::string&) noexcept override;
  StatusOr<StoreLimits> limits() noexcept override;

private:
  struct Impl;
  explicit MemoryStateStore(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

class RocksStateStore final : public IStateStore {
public:
  static StatusOr<std::unique_ptr<RocksStateStore>> open(const std::string& path,
                                                         const StoreConfig& config = {}) noexcept;
  // Reads source_path/snapshots/<id>; restore_path must not exist. Never opens the snapshot
  // writable.
  static StatusOr<std::unique_ptr<RocksStateStore>>
  from_snapshot(const std::string& source_path, const std::string& snapshot_id,
                const std::string& restore_path, const StoreConfig& config = {}) noexcept;
  ~RocksStateStore() noexcept override;
  StatusOr<ReadResult> get_many(std::uint32_t, const std::vector<std::string>&) noexcept override;
  StatusOr<ScanResult> scan(std::uint32_t, const std::string&, const std::optional<std::string>&,
                            std::size_t) noexcept override;
  StatusOr<CommitResult> commit(const CommitBatch&) noexcept override;
  StatusOr<ShardStatus> status(std::uint32_t) noexcept override;
  StatusOr<StoreSnapshot> create_snapshot(const std::string&) noexcept override;
  StatusOr<StoreSnapshot> snapshot_status(const std::string&) noexcept override;
  StatusOr<StoreLimits> limits() noexcept override;

private:
  struct Impl;
  explicit RocksStateStore(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

// This client never invents a new commit_id after an ambiguous transport failure.
class RemoteStateStore final : public IStateStore {
public:
  static StatusOr<std::unique_ptr<RemoteStateStore>> open(const std::string& endpoint,
                                                          int timeout_seconds = 5) noexcept;
  ~RemoteStateStore() noexcept override;
  StatusOr<ReadResult> get_many(std::uint32_t, const std::vector<std::string>&) noexcept override;
  StatusOr<ScanResult> scan(std::uint32_t, const std::string&, const std::optional<std::string>&,
                            std::size_t) noexcept override;
  StatusOr<CommitResult> commit(const CommitBatch&) noexcept override;
  StatusOr<ShardStatus> status(std::uint32_t) noexcept override;
  StatusOr<StoreSnapshot> create_snapshot(const std::string&) noexcept override;
  StatusOr<StoreSnapshot> snapshot_status(const std::string&) noexcept override;
  StatusOr<StoreLimits> limits() noexcept override;

private:
  struct Impl;
  explicit RemoteStateStore(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

// Shared argument validation and receipt bytes keep adapters behaviorally identical.
Status validate_snapshot_id(const std::string& snapshot_id) noexcept;
StatusOr<std::string> canonical_batch(const CommitBatch& batch) noexcept;
StatusOr<std::string> make_commit_id(const std::string& owner, std::uint32_t shard,
                                     std::uint64_t expected_revision) noexcept;
} // namespace eventflow
