#pragma once

#include "eventflow/state_store.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>

namespace eventflow {

struct CacheConfig {
  // Either zero limit disables entry caching, but not ambiguous-commit fencing.
  std::size_t max_entries = 65536;
  // Retained key/value bytes plus a fixed metadata charge, not process RSS.
  std::size_t max_bytes = 16 * 1024 * 1024;
  // Pending retry identity is separate from evictable cache storage.
  std::size_t max_pending_bytes = 16 * 1024 * 1024;
  std::uint64_t owner_epoch = 0;
};

struct CacheMetrics {
  std::uint64_t hits = 0;   // Requested key occurrences served from cache.
  std::uint64_t misses = 0; // Requested key occurrences fetched as a complete batch.
  std::uint64_t backend_reads = 0;
  std::uint64_t validation_reads = 0;
  std::uint64_t evictions = 0;
  std::uint64_t invalidations = 0;
  std::uint64_t unknown_commits = 0;
  std::uint64_t blocked_reads = 0;
  std::size_t entries = 0;
  std::size_t bytes = 0;
  std::size_t pending_bytes = 0;
  std::uint64_t owner_epoch = 0;
};

// One serialized owner uses an instance; concurrent calls are not supported.
// No dirty state is retained. An ambiguous commit blocks data access until its
// exact retry is confirmed, even if status() observes a newer backend revision.
// Confirmed commits advance eligible resident entries and write through only
// within their original backend read admission and the shared cache budget.
class CachedStateStore final : public IStateStore {
public:
  static StatusOr<std::unique_ptr<CachedStateStore>> open(std::shared_ptr<IStateStore> backend,
                                                          const CacheConfig& config = {}) noexcept;
  ~CachedStateStore() noexcept override;

  StatusOr<ReadResult> get_many(std::uint32_t shard,
                                const std::vector<std::string>& keys) noexcept override;
  StatusOr<ScanResult> scan(std::uint32_t shard, const std::string& prefix,
                            const std::optional<std::string>& after,
                            std::size_t limit) noexcept override;
  StatusOr<CommitResult> commit(const CommitBatch& batch) noexcept override;
  StatusOr<ShardStatus> status(std::uint32_t shard) noexcept override;
  StatusOr<StoreSnapshot> create_snapshot(const std::string& snapshot_id) noexcept override;
  StatusOr<StoreSnapshot> snapshot_status(const std::string& snapshot_id) noexcept override;
  StatusOr<StoreLimits> limits() noexcept override;

  CacheMetrics metrics() const noexcept;
  bool has_pending_commit() const noexcept;
  // Explicit context invalidation never discards an unresolved retry identity.
  void invalidate() noexcept;
  // The deployment owner must call this when its assignment context changes.
  Status set_owner_epoch(std::uint64_t owner_epoch) noexcept;

private:
  struct Impl;
  explicit CachedStateStore(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace eventflow
