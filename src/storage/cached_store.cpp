#include "eventflow/cached_store.hpp"

#include <algorithm>
#include <iterator>
#include <limits>
#include <list>
#include <map>
#include <string_view>
#include <utility>

namespace eventflow {
namespace {

bool add_bytes(std::size_t bytes, std::size_t limit, std::size_t& total) noexcept {
  if (bytes > limit - total)
    return false;

  total += bytes;
  return true;
}

Status pending_error() noexcept {
  return Status(ErrorCode::UnknownCommit, "cache is waiting for the exact pending commit retry");
}

} // namespace

struct CachedStateStore::Impl {
  using Key = std::pair<std::uint32_t, std::string>;

  struct Entry {
    std::optional<std::string> value;
    std::uint64_t revision = 0;

    // These conservative lower bounds were admitted by a complete backend read.
    // They prevent combined cache hits from bypassing hidden backend read limits.
    std::size_t admitted_keys = 0;
    std::size_t admitted_bytes = 0;
    std::size_t charge = 0;
    std::list<Key>::iterator lru_position;
  };

  using Entries = std::map<Key, Entry>;

  std::shared_ptr<IStateStore> backend;
  CacheConfig config;
  CacheMetrics counters;
  Entries entries;
  std::list<Key> lru;
  std::optional<std::string> pending_identity;

  bool enabled() const noexcept {
    return config.max_entries != 0 && config.max_bytes != 0;
  }

  void erase_entry(Entries::iterator entry) noexcept {
    counters.bytes -= entry->second.charge;
    lru.erase(entry->second.lru_position);
    entries.erase(entry);
  }

  void clear_entries() noexcept {
    counters.invalidations += entries.size();
    entries.clear();
    lru.clear();
    counters.bytes = 0;
  }

  void invalidate_shard(std::uint32_t shard,
                        std::optional<std::uint64_t> keep_revision = {}) noexcept {
    auto entry = entries.lower_bound(Key{shard, {}});
    while (entry != entries.end() && entry->first.first == shard) {
      auto current = entry++;
      if (!keep_revision || current->second.revision != *keep_revision) {
        erase_entry(current);
        ++counters.invalidations;
      }
    }
  }

  void insert(std::uint32_t shard, const std::string& key, const std::optional<std::string>& value,
              std::uint64_t revision, std::size_t admitted_keys,
              std::size_t admitted_bytes) noexcept {
    // Both retained key copies, negative entries, and fixed node metadata are
    // charged. Entry count separately bounds allocator/container overhead.
    constexpr auto fixed_charge = sizeof(Key) * 2 + sizeof(Entry) + sizeof(void*) * 8;
    std::size_t charge = 0;
    if (!add_bytes(fixed_charge, config.max_bytes, charge) ||
        !add_bytes(key.size(), config.max_bytes, charge) ||
        !add_bytes(key.size(), config.max_bytes, charge) ||
        (value && !add_bytes(value->size(), config.max_bytes, charge)))
      return;

    const Key lookup{shard, key};
    auto existing = entries.find(lookup);
    if (existing != entries.end())
      erase_entry(existing);

    while (!entries.empty() &&
           (entries.size() >= config.max_entries || charge > config.max_bytes - counters.bytes)) {
      erase_entry(entries.find(lru.front()));
      ++counters.evictions;
    }

    lru.push_back(lookup);
    entries.emplace(lookup, Entry{.value = value,
                                  .revision = revision,
                                  .admitted_keys = admitted_keys,
                                  .admitted_bytes = admitted_bytes,
                                  .charge = charge,
                                  .lru_position = std::prev(lru.end())});
    counters.bytes += charge;
  }

  void apply_confirmed_commit(const CommitBatch& batch, std::uint64_t revision) noexcept {
    // Only the expected revision can be advanced across this exact transition.
    // Older entries may have missed external writes; a duplicate receipt must
    // not overwrite entries that were read at a different revision either.
    invalidate_shard(batch.shard, batch.expected_revision);
    auto entry = entries.lower_bound(Key{batch.shard, {}});
    if (entry == entries.end() || entry->first.first != batch.shard)
      return;

    // Mutations are ordered by the store contract. Borrow the last value for
    // each key so temporary growth within one batch cannot force an eviction.
    std::map<std::string_view, const Mutation*> final_mutations;
    for (const auto& mutation : batch.mutations)
      final_mutations[mutation.key] = &mutation;

    while (entry != entries.end() && entry->first.first == batch.shard) {
      auto current = entry++;
      auto& resident = current->second;
      resident.revision = revision;
      const auto mutation = final_mutations.find(current->first.second);
      if (mutation == final_mutations.end())
        continue;

      const auto& value = mutation->second->value;
      const auto value_bytes = value ? value->size() : 0;
      std::size_t read_bytes = 0;
      auto charge = resident.charge - (resident.value ? resident.value->size() : 0);

      // A successful write proves neither a larger read limit nor a larger
      // cache budget. Keep both admission bounds unchanged across revisions.
      if (resident.admitted_keys == 0 ||
          !add_bytes(current->first.second.size(), resident.admitted_bytes, read_bytes) ||
          !add_bytes(value_bytes, resident.admitted_bytes, read_bytes) ||
          !add_bytes(value_bytes, config.max_bytes, charge) ||
          charge > config.max_bytes - (counters.bytes - resident.charge)) {
        erase_entry(current);
        ++counters.evictions;
        continue;
      }

      counters.bytes -= resident.charge;

      // Destroy the old string before replacement so shrinking values do not
      // retain the old allocation behind a smaller logical byte charge.
      resident.value.reset();
      if (value)
        resident.value.emplace(*value);

      resident.charge = charge;
      counters.bytes += charge;
      lru.splice(lru.end(), lru, resident.lru_position);
    }
  }

  StatusOr<std::string> identity(const CommitBatch& batch) const noexcept {
    // Preflight retained payload before encoding, including a lower bound on
    // per-mutation framing. The exact canonical size is checked afterwards.
    std::size_t bytes = 0;
    if (!add_bytes(batch.owner.size(), config.max_pending_bytes, bytes) ||
        !add_bytes(batch.commit_id.size(), config.max_pending_bytes, bytes) ||
        batch.mutations.size() > (config.max_pending_bytes - bytes) / sizeof(std::uint64_t))
      return Status(ErrorCode::ResourceExhausted, "commit exceeds cache retry identity budget");

    bytes += batch.mutations.size() * sizeof(std::uint64_t);
    for (const auto& mutation : batch.mutations) {
      if (!add_bytes(mutation.key.size(), config.max_pending_bytes, bytes) ||
          (mutation.value && !add_bytes(mutation.value->size(), config.max_pending_bytes, bytes)))
        return Status(ErrorCode::ResourceExhausted, "commit exceeds cache retry identity budget");
    }

    EF_ASSIGN_OR_RETURN(auto canonical, canonical_batch(batch));
    if (canonical.size() > config.max_pending_bytes)
      return Status(ErrorCode::ResourceExhausted, "commit exceeds cache retry identity budget");

    return canonical;
  }

  void remember_pending(std::string identity) noexcept {
    clear_entries();
    pending_identity = std::move(identity);
    ++counters.unknown_commits;
  }
};

CachedStateStore::CachedStateStore(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}

CachedStateStore::~CachedStateStore() noexcept = default;

StatusOr<StoreLimits> CachedStateStore::limits() noexcept {
  return impl_->backend->limits();
}

StatusOr<std::unique_ptr<CachedStateStore>>
CachedStateStore::open(std::shared_ptr<IStateStore> backend, const CacheConfig& config) noexcept {
  if (!backend)
    return Status(ErrorCode::InvalidArgument, "cache requires a backend");
  if (config.max_pending_bytes == 0)
    return Status(ErrorCode::InvalidArgument, "cache retry identity budget must be positive");

  auto impl = std::make_unique<Impl>();
  impl->backend = std::move(backend);
  impl->config = config;
  return std::unique_ptr<CachedStateStore>(new CachedStateStore(std::move(impl)));
}

StatusOr<ReadResult> CachedStateStore::get_many(std::uint32_t shard,
                                                const std::vector<std::string>& keys) noexcept {
  if (impl_->pending_identity) {
    ++impl_->counters.blocked_reads;
    return pending_error();
  }

  if (impl_->enabled()) {
    ++impl_->counters.validation_reads;
    EF_ASSIGN_OR_RETURN(auto confirmed, impl_->backend->status(shard));
    impl_->invalidate_shard(shard, confirmed.revision);

    bool complete = !keys.empty();
    std::size_t bytes = 0;
    std::size_t admitted_bytes = std::numeric_limits<std::size_t>::max();
    for (const auto& key : keys) {
      const auto entry = impl_->entries.find({shard, key});
      if (entry == impl_->entries.end() || entry->second.admitted_keys < keys.size()) {
        complete = false;
        break;
      }

      admitted_bytes = std::min(admitted_bytes, entry->second.admitted_bytes);

      // A smaller bound from a later entry may be below bytes accumulated so far.
      if (bytes > admitted_bytes || !add_bytes(key.size(), admitted_bytes, bytes) ||
          (entry->second.value && !add_bytes(entry->second.value->size(), admitted_bytes, bytes))) {
        complete = false;
        break;
      }
    }

    if (complete) {
      ReadResult result{.revision = confirmed.revision, .values = {}};
      result.values.reserve(keys.size());
      for (const auto& key : keys) {
        auto entry = impl_->entries.find({shard, key});
        result.values.push_back(entry->second.value);
        impl_->lru.splice(impl_->lru.end(), impl_->lru, entry->second.lru_position);
      }

      impl_->counters.hits += keys.size();
      return result;
    }
  }

  // A miss always fetches the original complete request. A concurrent backend
  // commit can change this read's revision without mixing it with cached values.
  ++impl_->counters.backend_reads;
  impl_->counters.misses += keys.size();
  EF_ASSIGN_OR_RETURN(auto result, impl_->backend->get_many(shard, keys));
  if (result.values.size() != keys.size()) {
    impl_->invalidate_shard(shard);
    return Status(ErrorCode::Internal, "backend returned an invalid read cardinality");
  }

  if (!impl_->enabled())
    return result;

  std::size_t bytes = 0;
  constexpr auto limit = std::numeric_limits<std::size_t>::max();
  for (std::size_t i = 0; i < keys.size(); ++i) {
    if (!add_bytes(keys[i].size(), limit, bytes) ||
        (result.values[i] && !add_bytes(result.values[i]->size(), limit, bytes))) {
      impl_->invalidate_shard(shard);
      return Status(ErrorCode::Internal, "backend read size overflows the address space");
    }
  }

  impl_->invalidate_shard(shard, result.revision);
  for (std::size_t i = 0; i < keys.size(); ++i)
    impl_->insert(shard, keys[i], result.values[i], result.revision, keys.size(), bytes);

  return result;
}

StatusOr<ScanResult> CachedStateStore::scan(std::uint32_t shard, const std::string& prefix,
                                            const std::optional<std::string>& after,
                                            std::size_t limit) noexcept {
  if (impl_->pending_identity) {
    ++impl_->counters.blocked_reads;
    return pending_error();
  }

  return impl_->backend->scan(shard, prefix, after, limit);
}

StatusOr<CommitResult> CachedStateStore::commit(const CommitBatch& batch) noexcept {
  EF_ASSIGN_OR_RETURN(auto identity, impl_->identity(batch));
  if (impl_->pending_identity && identity != *impl_->pending_identity)
    return pending_error();

  auto result = impl_->backend->commit(batch);
  if (!result.ok()) {
    if (result.status().code() == ErrorCode::Unavailable ||
        result.status().code() == ErrorCode::UnknownCommit)
      impl_->remember_pending(std::move(identity));

    return result.status();
  }

  if (result->code == CommitCode::Committed &&
      (batch.expected_revision == std::numeric_limits<std::uint64_t>::max() ||
       result->revision != batch.expected_revision + 1)) {
    impl_->remember_pending(std::move(identity));
    return CommitResult{.code = CommitCode::Unknown,
                        .revision = result->revision,
                        .duplicate = result->duplicate,
                        .message = "backend reported an invalid confirmed revision"};
  }

  if (result->code == CommitCode::Unknown)
    impl_->remember_pending(std::move(identity));
  else if (result->code == CommitCode::Committed) {
    impl_->pending_identity.reset();
    impl_->apply_confirmed_commit(batch, result->revision);
  }

  // Rejection cannot prove the outcome of an earlier ambiguous commit, e.g.
  // after another owner's write replaced its receipt. Keep that case blocked.
  return std::move(result).value();
}

StatusOr<ShardStatus> CachedStateStore::status(std::uint32_t shard) noexcept {
  EF_ASSIGN_OR_RETURN(auto result, impl_->backend->status(shard));
  impl_->invalidate_shard(shard, result.revision);
  return result;
}

StatusOr<StoreSnapshot> CachedStateStore::create_snapshot(const std::string& snapshot_id) noexcept {
  if (impl_->pending_identity)
    return pending_error();

  return impl_->backend->create_snapshot(snapshot_id);
}

StatusOr<StoreSnapshot> CachedStateStore::snapshot_status(const std::string& snapshot_id) noexcept {
  return impl_->backend->snapshot_status(snapshot_id);
}

CacheMetrics CachedStateStore::metrics() const noexcept {
  auto result = impl_->counters;
  result.entries = impl_->entries.size();
  result.pending_bytes = impl_->pending_identity ? impl_->pending_identity->size() : 0;
  result.owner_epoch = impl_->config.owner_epoch;

  return result;
}

bool CachedStateStore::has_pending_commit() const noexcept {
  return impl_->pending_identity.has_value();
}

void CachedStateStore::invalidate() noexcept {
  // Dropping cached rows cannot settle an UNKNOWN write. Keep its retry identity and read barrier.
  impl_->clear_entries();
}

Status CachedStateStore::set_owner_epoch(std::uint64_t owner_epoch) noexcept {
  if (impl_->pending_identity)
    return pending_error();
  if (impl_->config.owner_epoch != owner_epoch)
    impl_->clear_entries();

  impl_->config.owner_epoch = owner_epoch;
  return {};
}

} // namespace eventflow
