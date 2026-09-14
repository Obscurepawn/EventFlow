#include "eventflow/state_store.hpp"

#include <atomic>
#include <cerrno>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <map>
#include <mutex>
#include <string_view>
#include <unistd.h>

#include <rocksdb/db.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/write_batch.h>

#include "eventflow/key_codec.hpp"
#include "eventflow/types.hpp"

namespace eventflow {
namespace {

constexpr std::string_view batch_header = "EventFlow.CommitBatch.v1";
constexpr const char* snapshot_marker = "EVENTFLOW_IMMUTABLE_SNAPSHOT_V1";

StatusOr<bool> has_snapshot_marker(const std::filesystem::path& path) noexcept {
  std::error_code error;
  const auto state = std::filesystem::symlink_status(path / snapshot_marker, error);
  if (error == std::errc::no_such_file_or_directory)
    return false;
  if (error)
    return Status(ErrorCode::Unavailable, error.message());

  return state.type() != std::filesystem::file_type::not_found;
}

void append_u64(std::string& output, std::uint64_t value) noexcept {
  for (int shift = 56; shift >= 0; shift -= 8)
    output.push_back(static_cast<char>(value >> shift));
}

void append_bytes(std::string& output, const std::string& value) noexcept {
  append_u64(output, value.size());
  output.append(value);
}

std::optional<std::size_t> canonical_size(const CommitBatch& batch, std::size_t limit) noexcept {
  std::size_t size = 0;
  auto add = [&](std::size_t count) noexcept {
    if (count > limit - size)
      return false;

    size += count;
    return true;
  };
  if (!add(batch_header.size()) || !add(40) || !add(batch.owner.size()) ||
      !add(batch.commit_id.size()))
    return std::nullopt;

  for (const auto& mutation : batch.mutations) {
    if (!add(9) || !add(mutation.key.size()))
      return std::nullopt;
    if (mutation.value && (!add(8) || !add(mutation.value->size())))
      return std::nullopt;
  }

  return size;
}

StatusOr<StoreConfig> validate_config(const StoreConfig& input) noexcept {
  EF_RETURN_IF_ERROR(check(input.shard_count > 0, "shard_count must be positive"));
  EF_RETURN_IF_ERROR(check(input.max_batch_bytes > 0, "max_batch_bytes must be positive"));
  EF_RETURN_IF_ERROR(check(input.max_mutations > 0, "max_mutations must be positive"));
  EF_RETURN_IF_ERROR(check(input.max_read_keys > 0, "max_read_keys must be positive"));
  EF_RETURN_IF_ERROR(check(input.max_read_bytes > 0, "max_read_bytes must be positive"));
  EF_RETURN_IF_ERROR(check(input.max_scan_rows > 0, "max_scan_rows must be positive"));
  EF_RETURN_IF_ERROR(check(input.max_snapshot_count > 0, "max_snapshot_count must be positive"));
  StoreConfig config = input;
  if (config.shard_owners.empty()) {
    EF_RETURN_IF_ERROR(check(!config.owner.empty() && valid_utf8(config.owner),
                             "owner must be nonempty valid UTF-8"));
    config.shard_owners.assign(config.shard_count, config.owner);
  } else {
    EF_RETURN_IF_ERROR(check(config.shard_owners.size() == config.shard_count,
                             "shard_owners must contain exactly shard_count assignments"));
    for (const auto& owner : config.shard_owners)
      EF_RETURN_IF_ERROR(
          check(!owner.empty() && valid_utf8(owner), "owner must be nonempty valid UTF-8"));
  }

  return config;
}

Status check_rocks(const rocksdb::Status& status) noexcept {
  if (status.ok())
    return {};

  return Status(ErrorCode::Unavailable, status.ToString());
}

Status consume_read_bytes(std::size_t bytes, std::size_t& remaining) noexcept {
  if (bytes > remaining)
    return Status(ErrorCode::ResourceExhausted, "read exceeds max_read_bytes");

  remaining -= bytes;
  return {};
}

std::string receipt_key(std::uint32_t shard) noexcept {
  std::string result(1, 'M');
  for (int shift = 24; shift >= 0; shift -= 8)
    result.push_back(static_cast<char>(shard >> shift));

  return result;
}

struct Receipt {
  std::uint64_t revision = 0;
  std::string id;
  std::string canonical;

  void swap(Receipt& other) noexcept {
    std::swap(revision, other.revision);
    id.swap(other.id);
    canonical.swap(other.canonical);
  }
};

std::string encode_receipt(const Receipt& receipt) noexcept {
  const std::vector<std::uint8_t> canonical(receipt.canonical.begin(), receipt.canonical.end());
  const auto bytes =
      Json::to_cbor(Json::array({receipt.revision, receipt.id, Json::binary(canonical)}));
  return std::string(bytes.begin(), bytes.end());
}

StatusOr<Receipt> decode_receipt(const std::string& bytes) noexcept {
  const auto value = Json::from_cbor(bytes, true, false);
  if (value.is_discarded() || !value.is_array() || value.size() != 3 ||
      !value[0].is_number_unsigned() || !value[1].is_string() || !value[2].is_binary())
    return Status(ErrorCode::FailedPrecondition, "malformed persisted shard receipt");

  const auto& canonical = value[2].get_binary();
  Receipt result{.revision = value[0].get<std::uint64_t>(),
                 .id = value[1].get<std::string>(),
                 .canonical = std::string(canonical.begin(), canonical.end())};
  if (result.revision == 0 || result.id.empty() || result.canonical.empty() ||
      !valid_utf8(result.id))
    return Status(ErrorCode::FailedPrecondition, "invalid persisted shard receipt");

  return result;
}

struct Shard {
  std::mutex mutex;
  Receipt receipt;
  std::map<std::string, std::string> rows;
};

struct MemorySnapshot {
  StoreSnapshot info;
  std::vector<Receipt> receipts;
  std::vector<std::map<std::string, std::string>> rows;
};

std::string assignment_bytes(const StoreConfig& config) noexcept {
  return Json{
      {"format", 1}, {"shard_count", config.shard_count}, {"shard_owners", config.shard_owners}}
      .dump(-1, ' ', false, Json::error_handler_t::replace);
}

Status filesystem_error(const std::error_code& error) noexcept {
  return Status(error == std::errc::no_such_file_or_directory ? ErrorCode::NotFound
                                                              : ErrorCode::Unavailable,
                error.message());
}

Status require_directory(const std::filesystem::path& path) noexcept {
  std::error_code error;
  const auto state = std::filesystem::symlink_status(path, error);
  if (error)
    return filesystem_error(error);
  if (state.type() == std::filesystem::file_type::not_found)
    return Status(ErrorCode::NotFound, "snapshot directory does not exist");
  if (!std::filesystem::is_directory(state) || std::filesystem::is_symlink(state))
    return Status(ErrorCode::FailedPrecondition, "snapshot path must be a real directory");

  return {};
}

StatusOr<std::filesystem::path> snapshot_root(const std::string& source_path,
                                              bool create) noexcept {
  std::error_code error;
  auto root = std::filesystem::canonical(source_path, error);
  if (error)
    return filesystem_error(error);

  root /= "snapshots";
  if (create) {
    std::filesystem::create_directory(root, error);
    if (error && error != std::errc::file_exists)
      return filesystem_error(error);
  }

  EF_RETURN_IF_ERROR(require_directory(root));
  return root;
}

StatusOr<StoreSnapshot> read_snapshot_info(const std::filesystem::path& path, const std::string& id,
                                           const StoreConfig& config) noexcept {
  EF_RETURN_IF_ERROR(require_directory(path));
  EF_ASSIGN_OR_RETURN(auto immutable, has_snapshot_marker(path));
  if (!immutable)
    return Status(ErrorCode::FailedPrecondition, "snapshot publication marker is missing");

  rocksdb::Options options;
  options.create_if_missing = false;
  rocksdb::DB* opened = nullptr;
  EF_RETURN_IF_ERROR(check_rocks(rocksdb::DB::OpenForReadOnly(options, path.string(), &opened)));
  std::unique_ptr<rocksdb::DB> snapshot(opened);
  std::string saved;
  EF_RETURN_IF_ERROR(check_rocks(snapshot->Get(rocksdb::ReadOptions{}, "Mconfig", &saved)));
  if (saved != assignment_bytes(config))
    return Status(ErrorCode::FailedPrecondition, "snapshot shard count or assignment differs");

  StoreSnapshot info{.snapshot_id = id, .shards = {}, .shard_owners = config.shard_owners};
  for (std::uint32_t shard = 0; shard < config.shard_count; ++shard) {
    std::string bytes;
    const auto status = snapshot->Get(rocksdb::ReadOptions{}, receipt_key(shard), &bytes);
    if (status.IsNotFound()) {
      info.shards.push_back({});
      continue;
    }

    EF_RETURN_IF_ERROR(check_rocks(status));
    EF_ASSIGN_OR_RETURN(auto receipt, decode_receipt(bytes));
    EF_ASSIGN_OR_RETURN(auto expected,
                        make_commit_id(config.shard_owners[shard], shard, receipt.revision - 1));
    if (receipt.id != expected)
      return Status(ErrorCode::FailedPrecondition, "snapshot receipt does not match assignment");

    info.shards.push_back({.revision = receipt.revision, .last_commit_id = std::move(receipt.id)});
  }

  return info;
}

Status check_snapshot_capacity(const std::filesystem::path& root, std::size_t limit) noexcept {
  std::error_code error;
  std::filesystem::directory_iterator iterator(root, error), end;
  if (error)
    return filesystem_error(error);

  std::size_t count = 0;
  while (iterator != end) {
    if (++count >= limit)
      return Status(ErrorCode::ResourceExhausted, "snapshot count limit reached");

    iterator.increment(error);
    if (error)
      return filesystem_error(error);
  }

  return {};
}

Status sync_path(const std::filesystem::path& path, bool directory) noexcept {
  const int descriptor =
      ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | (directory ? O_DIRECTORY : 0));
  if (descriptor < 0)
    return filesystem_error(std::error_code(errno, std::generic_category()));

  int result = 0;
  do {
    result = ::fsync(descriptor);
  } while (result < 0 && errno == EINTR);
  const auto error = std::error_code(result < 0 ? errno : 0, std::generic_category());
  const int closed = ::close(descriptor);
  if (error)
    return filesystem_error(error);
  if (closed < 0)
    return filesystem_error(std::error_code(errno, std::generic_category()));

  return {};
}

Status mark_snapshot_immutable(const std::filesystem::path& path) noexcept {
  const auto marker = path / snapshot_marker;
  const int descriptor =
      ::open(marker.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0444);
  if (descriptor < 0)
    return filesystem_error(std::error_code(errno, std::generic_category()));
  if (::close(descriptor) < 0)
    return filesystem_error(std::error_code(errno, std::generic_category()));

  EF_RETURN_IF_ERROR(sync_path(marker, false));
  return sync_path(path, true);
}

// Both adapters use the same validation and receipt state machine.
class StoreEngine {
public:
  explicit StoreEngine(StoreConfig config) noexcept : config_(std::move(config)) {
    shards_.reserve(config_.shard_count);
    for (std::uint32_t shard = 0; shard < config_.shard_count; ++shard)
      shards_.push_back(std::make_unique<Shard>());
  }

  Status open(const std::string& path) noexcept {
    EF_RETURN_IF_ERROR(check(!path.empty(), "RocksDB path must not be empty"));
    EF_ASSIGN_OR_RETURN(auto immutable, has_snapshot_marker(path));
    if (immutable)
      return Status(ErrorCode::FailedPrecondition,
                    "immutable snapshot cannot be opened as an active store; use from_snapshot");

    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* opened = nullptr;
    EF_RETURN_IF_ERROR(check_rocks(rocksdb::DB::Open(options, path, &opened)));
    db_.reset(opened);
    std::error_code path_error;
    path_ = std::filesystem::canonical(path, path_error).string();
    if (path_error)
      return filesystem_error(path_error);

    const std::string config_key = "Mconfig";
    const auto expected = assignment_bytes(config_);
    std::string saved;
    const auto status = db_->Get(rocksdb::ReadOptions{}, config_key, &saved);
    if (status.IsNotFound()) {
      std::unique_ptr<rocksdb::Iterator> iterator(db_->NewIterator(rocksdb::ReadOptions{}));
      iterator->SeekToFirst();
      EF_RETURN_IF_ERROR(check_rocks(iterator->status()));
      if (iterator->Valid())
        return Status(ErrorCode::FailedPrecondition,
                      "nonempty RocksDB has no EventFlow assignment metadata");

      rocksdb::WriteOptions write;
      write.sync = true;
      write.disableWAL = false;
      EF_RETURN_IF_ERROR(check_rocks(db_->Put(write, config_key, expected)));
    } else {
      EF_RETURN_IF_ERROR(check_rocks(status));
      if (saved != expected)
        return Status(ErrorCode::FailedPrecondition,
                      "persisted shard count or owner assignment differs from StoreConfig");
    }

    for (std::uint32_t shard = 0; shard < config_.shard_count; ++shard)
      EF_RETURN_IF_ERROR(load_receipt(shard, *shards_[shard]));

    return {};
  }

  StatusOr<ReadResult> get_many(std::uint32_t shard,
                                const std::vector<std::string>& keys) noexcept {
    EF_RETURN_IF_ERROR(validate_shard(shard));
    if (keys.size() > config_.max_read_keys)
      return Status(ErrorCode::ResourceExhausted, "get_many exceeds max_read_keys");

    std::size_t remaining = config_.max_read_bytes;
    for (const auto& key : keys) {
      EF_RETURN_IF_ERROR(
          check(key_in_shard(shard, key), "read key belongs to another shard or namespace"));
      EF_RETURN_IF_ERROR(consume_read_bytes(key.size(), remaining));
    }

    auto& state = *shards_[shard];
    std::lock_guard lock(state.mutex);
    EF_RETURN_IF_ERROR(available());
    ReadResult result;
    result.revision = state.receipt.revision;
    result.values.reserve(keys.size());

    for (const auto& key : keys) {
      if (db_) {
        rocksdb::PinnableSlice value;
        const auto status =
            db_->Get(rocksdb::ReadOptions{}, db_->DefaultColumnFamily(), key, &value);
        if (status.IsNotFound())
          result.values.emplace_back(std::nullopt);
        else {
          EF_RETURN_IF_ERROR(check_rocks(status));
          EF_RETURN_IF_ERROR(consume_read_bytes(value.size(), remaining));
          result.values.emplace_back(value.ToString());
        }
      } else {
        const auto found = state.rows.find(key);
        if (found == state.rows.end())
          result.values.emplace_back(std::nullopt);
        else {
          EF_RETURN_IF_ERROR(consume_read_bytes(found->second.size(), remaining));
          result.values.emplace_back(found->second);
        }
      }
    }

    return result;
  }

  StatusOr<ScanResult> scan(std::uint32_t shard, const std::string& prefix,
                            const std::optional<std::string>& after, std::size_t limit) noexcept {
    EF_RETURN_IF_ERROR(validate_shard(shard));
    EF_RETURN_IF_ERROR(
        check(key_in_shard(shard, prefix), "scan prefix belongs to another shard or namespace"));
    EF_RETURN_IF_ERROR(check(limit > 0, "scan limit must be positive"));
    if (limit > config_.max_scan_rows)
      return Status(ErrorCode::ResourceExhausted, "scan exceeds max_scan_rows");
    if (after)
      EF_RETURN_IF_ERROR(
          check(after->starts_with(prefix), "scan cursor must belong to the requested prefix"));

    auto& state = *shards_[shard];
    std::lock_guard lock(state.mutex);
    EF_RETURN_IF_ERROR(available());
    ScanResult result;
    result.revision = state.receipt.revision;
    std::size_t remaining = config_.max_read_bytes;
    if (db_) {
      std::unique_ptr<rocksdb::Iterator> iterator(db_->NewIterator(rocksdb::ReadOptions{}));
      iterator->Seek(after ? *after : prefix);
      if (after && iterator->Valid() && iterator->key().ToStringView() == *after)
        iterator->Next();

      while (iterator->Valid() && result.rows.size() < limit) {
        if (!iterator->key().ToStringView().starts_with(prefix))
          break;

        EF_RETURN_IF_ERROR(consume_read_bytes(iterator->key().size(), remaining));
        EF_RETURN_IF_ERROR(consume_read_bytes(iterator->value().size(), remaining));
        result.rows.emplace_back(iterator->key().ToString(), iterator->value().ToString());
        iterator->Next();
      }

      EF_RETURN_IF_ERROR(check_rocks(iterator->status()));
    } else {
      auto iterator = after ? state.rows.upper_bound(*after) : state.rows.lower_bound(prefix);
      while (iterator != state.rows.end() && result.rows.size() < limit &&
             iterator->first.starts_with(prefix)) {
        EF_RETURN_IF_ERROR(consume_read_bytes(iterator->first.size(), remaining));
        EF_RETURN_IF_ERROR(consume_read_bytes(iterator->second.size(), remaining));
        result.rows.push_back(*iterator);
        ++iterator;
      }
    }

    return result;
  }

  StatusOr<CommitResult> commit(const CommitBatch& batch) noexcept {
    EF_RETURN_IF_ERROR(validate_shard(batch.shard));
    auto& state = *shards_[batch.shard];
    std::lock_guard lock(state.mutex);
    auto reject = [&](const char* message) noexcept {
      return CommitResult{.code = CommitCode::Rejected,
                          .revision = state.receipt.revision,
                          .duplicate = false,
                          .message = message};
    };
    if (batch.owner != config_.shard_owners[batch.shard])
      return reject("owner assignment mismatch");

    EF_ASSIGN_OR_RETURN(auto expected_id,
                        make_commit_id(batch.owner, batch.shard, batch.expected_revision));
    if (batch.commit_id != expected_id)
      return reject("commit_id is not bound to owner, shard and expected_revision");
    if (!available().ok())
      return CommitResult{.code = CommitCode::Unknown,
                          .revision = state.receipt.revision,
                          .duplicate = false,
                          .message = "RocksDB requires reopen after an uncertain write"};

    if (batch.commit_id == state.receipt.id) {
      const auto size = canonical_size(batch, state.receipt.canonical.size());
      if (!size || *size != state.receipt.canonical.size())
        return reject("commit identity was used with different content");

      EF_ASSIGN_OR_RETURN(auto canonical, canonical_batch(batch));
      if (canonical != state.receipt.canonical)
        return reject("commit identity was used with different content");

      return CommitResult{.code = CommitCode::Committed,
                          .revision = state.receipt.revision,
                          .duplicate = true,
                          .message = {}};
    }

    if (batch.expected_revision != state.receipt.revision)
      return reject("expected_revision mismatch or stale commit");
    if (batch.expected_revision == std::numeric_limits<std::uint64_t>::max())
      return reject("revision exhausted");
    if (batch.mutations.size() > config_.max_mutations)
      return reject("batch exceeds max_mutations");
    if (!canonical_size(batch, config_.max_batch_bytes))
      return reject("batch exceeds max_batch_bytes");

    for (const auto& mutation : batch.mutations)
      if (!key_in_shard(batch.shard, mutation.key))
        return reject("mutation key belongs to another shard or namespace");

    EF_ASSIGN_OR_RETURN(auto canonical, canonical_batch(batch));
    Receipt next{.revision = state.receipt.revision + 1,
                 .id = batch.commit_id,
                 .canonical = std::move(canonical)};
    if (db_) {
      rocksdb::WriteBatch write_batch;
      for (const auto& mutation : batch.mutations) {
        const auto staged = mutation.value ? write_batch.Put(mutation.key, *mutation.value)
                                           : write_batch.Delete(mutation.key);
        if (!staged.ok())
          return CommitResult{.code = CommitCode::Rejected,
                              .revision = state.receipt.revision,
                              .duplicate = false,
                              .message = staged.ToString()};
      }

      // Data and its receipt must survive together, so a retry can recognize an applied batch.
      const auto receipt_status = write_batch.Put(receipt_key(batch.shard), encode_receipt(next));
      if (!receipt_status.ok())
        return CommitResult{.code = CommitCode::Rejected,
                            .revision = state.receipt.revision,
                            .duplicate = false,
                            .message = receipt_status.ToString()};

      rocksdb::WriteOptions options;
      // A successful acknowledgement requires the WAL to be durable, not just the memtable updated.
      options.sync = true;
      options.disableWAL = false;
      const auto status = db_->Write(options, &write_batch);
      if (!status.ok()) {
        // A memtable or WAL read cannot resolve a failed sync. Reopen the database before serving
        // again.
        failed_.store(true, std::memory_order_release);
        return CommitResult{.code = CommitCode::Unknown,
                            .revision = state.receipt.revision,
                            .duplicate = false,
                            .message = status.ToString()};
      }
    } else {
      auto staged = state.rows;
      for (const auto& mutation : batch.mutations) {
        if (mutation.value)
          staged.insert_or_assign(mutation.key, *mutation.value);
        else
          staged.erase(mutation.key);
      }

      state.rows.swap(staged);
    }

    state.receipt.swap(next);
    return CommitResult{.code = CommitCode::Committed,
                        .revision = state.receipt.revision,
                        .duplicate = false,
                        .message = {}};
  }

  StatusOr<ShardStatus> status(std::uint32_t shard) noexcept {
    EF_RETURN_IF_ERROR(validate_shard(shard));
    auto& state = *shards_[shard];
    std::lock_guard lock(state.mutex);
    EF_RETURN_IF_ERROR(available());

    return ShardStatus{.revision = state.receipt.revision, .last_commit_id = state.receipt.id};
  }

  const StoreConfig& config() const noexcept {
    return config_;
  }

  StatusOr<StoreSnapshot> create_snapshot(const std::string& id) noexcept {
    EF_RETURN_IF_ERROR(validate_snapshot_id(id));
    std::lock_guard snapshot_lock(snapshot_mutex_);
    std::vector<std::unique_lock<std::mutex>> locks;
    locks.reserve(shards_.size());

    // A fixed lock order blocks all shard writes at one storage boundary.
    for (auto& shard : shards_)
      locks.emplace_back(shard->mutex);

    EF_RETURN_IF_ERROR(available());
    if (db_) {
      EF_ASSIGN_OR_RETURN(auto root, snapshot_root(path_, true));
      const auto target = root / id;
      const auto existing_status = require_directory(target);
      if (existing_status.ok()) {
        // A previous publication may have renamed successfully but failed its parent fsync.
        // Merely observing the directory is not a durable acknowledgement.
        EF_RETURN_IF_ERROR(sync_path(root, true));
        return read_snapshot_info(target, id, config_);
      }

      if (existing_status.code() != ErrorCode::NotFound)
        return existing_status;

      EF_RETURN_IF_ERROR(check_snapshot_capacity(root, config_.max_snapshot_count));
      rocksdb::FlushOptions flush;
      flush.wait = true;
      EF_RETURN_IF_ERROR(check_rocks(db_->Flush(flush)));
      rocksdb::Checkpoint* raw = nullptr;
      EF_RETURN_IF_ERROR(check_rocks(rocksdb::Checkpoint::Create(db_.get(), &raw)));
      std::unique_ptr<rocksdb::Checkpoint> checkpoint(raw);

      // Publish the immutable marker with the directory. A failed staging directory is kept
      // for inspection; retries cannot silently recapture a later storage boundary.
      const auto staging = root / ("." + id + ".building");
      EF_RETURN_IF_ERROR(check_rocks(checkpoint->CreateCheckpoint(staging.string())));
      EF_RETURN_IF_ERROR(mark_snapshot_immutable(staging));
      std::error_code error;
      std::filesystem::rename(staging, target, error);
      if (error)
        return filesystem_error(error);

      EF_RETURN_IF_ERROR(sync_path(root, true));
      return read_snapshot_info(target, id, config_);
    }

    const auto existing = memory_snapshots_.find(id);
    if (existing != memory_snapshots_.end())
      return StoreSnapshot(existing->second.info);
    if (memory_snapshots_.size() >= config_.max_snapshot_count)
      return Status(ErrorCode::ResourceExhausted, "snapshot count limit reached");

    MemorySnapshot snapshot;
    snapshot.info = {.snapshot_id = id, .shards = {}, .shard_owners = config_.shard_owners};
    for (const auto& shard : shards_) {
      snapshot.info.shards.push_back(
          {.revision = shard->receipt.revision, .last_commit_id = shard->receipt.id});
      snapshot.receipts.push_back(shard->receipt);
      snapshot.rows.push_back(shard->rows);
    }

    auto info = snapshot.info;
    memory_snapshots_.emplace(id, std::move(snapshot));
    return info;
  }

  StatusOr<StoreSnapshot> snapshot_status(const std::string& id) noexcept {
    EF_RETURN_IF_ERROR(validate_snapshot_id(id));
    std::lock_guard lock(snapshot_mutex_);
    if (db_) {
      EF_ASSIGN_OR_RETURN(auto root, snapshot_root(path_, false));
      EF_RETURN_IF_ERROR(sync_path(root, true));
      return read_snapshot_info(root / id, id, config_);
    }

    const auto found = memory_snapshots_.find(id);
    if (found == memory_snapshots_.end())
      return Status(ErrorCode::NotFound, "snapshot does not exist");

    return StoreSnapshot(found->second.info);
  }

  Status copy_memory_snapshot(const std::string& id, StoreEngine& destination) noexcept {
    EF_RETURN_IF_ERROR(validate_snapshot_id(id));
    std::lock_guard lock(snapshot_mutex_);
    const auto found = memory_snapshots_.find(id);
    if (found == memory_snapshots_.end())
      return Status(ErrorCode::NotFound, "snapshot does not exist");

    for (std::size_t index = 0; index < shards_.size(); ++index) {
      destination.shards_[index]->receipt = found->second.receipts[index];
      destination.shards_[index]->rows = found->second.rows[index];
    }

    return {};
  }

private:
  Status validate_shard(std::uint32_t shard) const noexcept {
    return check(shard < config_.shard_count, "invalid shard");
  }

  Status available() const noexcept {
    if (failed_.load(std::memory_order_acquire))
      return Status(ErrorCode::UnknownCommit, "RocksDB requires reopen after an uncertain write");

    return {};
  }

  Status load_receipt(std::uint32_t shard, Shard& state) noexcept {
    std::string bytes;
    const auto status = db_->Get(rocksdb::ReadOptions{}, receipt_key(shard), &bytes);
    if (status.IsNotFound())
      return {};

    EF_RETURN_IF_ERROR(check_rocks(status));
    EF_ASSIGN_OR_RETURN(auto saved, decode_receipt(bytes));
    EF_ASSIGN_OR_RETURN(auto expected,
                        make_commit_id(config_.shard_owners[shard], shard, saved.revision - 1));
    if (saved.id != expected)
      return Status(ErrorCode::FailedPrecondition,
                    "persisted receipt does not match shard assignment or revision");

    state.receipt.swap(saved);
    return {};
  }

  StoreConfig config_;
  std::vector<std::unique_ptr<Shard>> shards_;
  std::unique_ptr<rocksdb::DB> db_;
  std::string path_;
  std::atomic<bool> failed_{false};
  std::mutex snapshot_mutex_;
  std::map<std::string, MemorySnapshot> memory_snapshots_;
};

} // namespace

Status validate_snapshot_id(const std::string& id) noexcept {
  EF_RETURN_IF_ERROR(
      check(!id.empty() && id.size() <= 128, "snapshot ID must contain 1..128 bytes"));
  for (const auto character : id) {
    const auto c = static_cast<unsigned char>(character);
    EF_RETURN_IF_ERROR(check((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                                 (c >= '0' && c <= '9') || c == '-' || c == '_',
                             "snapshot ID must use ASCII letters, digits, '-' or '_'"));
  }

  return {};
}

StatusOr<std::string> canonical_batch(const CommitBatch& batch) noexcept {
  std::string output(batch_header);
  append_u64(output, batch.shard);
  append_bytes(output, batch.owner);
  append_u64(output, batch.expected_revision);
  append_bytes(output, batch.commit_id);
  append_u64(output, batch.mutations.size());

  for (const auto& mutation : batch.mutations) {
    append_bytes(output, mutation.key);
    output.push_back(mutation.value ? '\1' : '\0');
    if (mutation.value)
      append_bytes(output, *mutation.value);
  }

  return output;
}

StatusOr<std::string> make_commit_id(const std::string& owner, std::uint32_t shard,
                                     std::uint64_t expected_revision) noexcept {
  EF_RETURN_IF_ERROR(
      check(!owner.empty() && valid_utf8(owner), "owner must be nonempty valid UTF-8"));
  return Json::array({owner, shard, expected_revision})
      .dump(-1, ' ', false, Json::error_handler_t::replace);
}

struct MemoryStateStore::Impl : StoreEngine {
  using StoreEngine::StoreEngine;
};

struct RocksStateStore::Impl : StoreEngine {
  using StoreEngine::StoreEngine;
};

StatusOr<std::unique_ptr<MemoryStateStore>>
MemoryStateStore::open(const StoreConfig& config) noexcept {
  EF_ASSIGN_OR_RETURN(auto validated, validate_config(config));
  auto impl = std::make_unique<Impl>(std::move(validated));
  return std::unique_ptr<MemoryStateStore>(new MemoryStateStore(std::move(impl)));
}

StatusOr<std::unique_ptr<MemoryStateStore>>
MemoryStateStore::from_snapshot(const MemoryStateStore& source, const std::string& id) noexcept {
  EF_ASSIGN_OR_RETURN(auto result, open(source.impl_->config()));
  EF_RETURN_IF_ERROR(source.impl_->copy_memory_snapshot(id, *result->impl_));
  return result;
}

MemoryStateStore::MemoryStateStore(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}

MemoryStateStore::~MemoryStateStore() noexcept = default;

StatusOr<StoreLimits> MemoryStateStore::limits() noexcept {
  const auto& config = impl_->config();
  return StoreLimits{.max_batch_bytes = config.max_batch_bytes,
                     .max_mutations = config.max_mutations,
                     .max_read_keys = config.max_read_keys,
                     .max_read_bytes = config.max_read_bytes,
                     .max_scan_rows = config.max_scan_rows};
}

StatusOr<ReadResult> MemoryStateStore::get_many(std::uint32_t shard,
                                                const std::vector<std::string>& keys) noexcept {
  return impl_->get_many(shard, keys);
}

StatusOr<ScanResult> MemoryStateStore::scan(std::uint32_t shard, const std::string& prefix,
                                            const std::optional<std::string>& after,
                                            std::size_t limit) noexcept {
  return impl_->scan(shard, prefix, after, limit);
}

StatusOr<CommitResult> MemoryStateStore::commit(const CommitBatch& batch) noexcept {
  return impl_->commit(batch);
}

StatusOr<ShardStatus> MemoryStateStore::status(std::uint32_t shard) noexcept {
  return impl_->status(shard);
}

StatusOr<StoreSnapshot> MemoryStateStore::create_snapshot(const std::string& id) noexcept {
  return impl_->create_snapshot(id);
}

StatusOr<StoreSnapshot> MemoryStateStore::snapshot_status(const std::string& id) noexcept {
  return impl_->snapshot_status(id);
}

StatusOr<std::unique_ptr<RocksStateStore>>
RocksStateStore::open(const std::string& path, const StoreConfig& config) noexcept {
  EF_ASSIGN_OR_RETURN(auto validated, validate_config(config));
  auto impl = std::make_unique<Impl>(std::move(validated));
  EF_RETURN_IF_ERROR(impl->open(path));
  return std::unique_ptr<RocksStateStore>(new RocksStateStore(std::move(impl)));
}

StatusOr<std::unique_ptr<RocksStateStore>>
RocksStateStore::from_snapshot(const std::string& source_path, const std::string& id,
                               const std::string& restore_path,
                               const StoreConfig& config) noexcept {
  EF_RETURN_IF_ERROR(validate_snapshot_id(id));
  EF_RETURN_IF_ERROR(check(!restore_path.empty() && restore_path.find('\0') == std::string::npos,
                           "restore path must be nonempty and contain no NUL"));
  EF_ASSIGN_OR_RETURN(auto validated, validate_config(config));
  EF_ASSIGN_OR_RETURN(auto root, snapshot_root(source_path, false));
  const auto source = root / id;
  EF_ASSIGN_OR_RETURN(auto info, read_snapshot_info(source, id, validated));
  (void)info;
  std::error_code error;
  const auto existing = std::filesystem::symlink_status(restore_path, error);
  if (error && error != std::errc::no_such_file_or_directory)
    return filesystem_error(error);
  if (existing.type() != std::filesystem::file_type::not_found)
    return Status(ErrorCode::Conflict, "restore destination already exists");

  error.clear();
  auto target = std::filesystem::weakly_canonical(restore_path, error);
  if (error)
    return filesystem_error(error);
  if (target == root || target.string().starts_with(root.string() + "/"))
    return Status(ErrorCode::InvalidArgument,
                  "restore destination cannot be inside snapshot storage");

  // Checkpoint files are copied, never hard-linked into a writable restored database.
  std::vector<std::filesystem::path> files;
  std::filesystem::directory_iterator iterator(source, error), end;
  if (error)
    return filesystem_error(error);

  while (iterator != end) {
    const auto state = iterator->symlink_status(error);
    if (error)
      return filesystem_error(error);
    if (!std::filesystem::is_regular_file(state) || std::filesystem::is_symlink(state))
      return Status(ErrorCode::FailedPrecondition,
                    "snapshot contains an unexpected non-file entry");

    // Only the newly copied destination becomes writable; the source keeps its marker.
    if (iterator->path().filename() != snapshot_marker)
      files.push_back(iterator->path());

    iterator.increment(error);
    if (error)
      return filesystem_error(error);
  }

  if (!std::filesystem::create_directory(target, error)) {
    if (error)
      return filesystem_error(error);

    return Status(ErrorCode::Conflict, "restore destination already exists");
  }

  // A failed copy leaves a reserved directory for operator inspection; never replace it on retry.
  for (const auto& file : files) {
    const auto destination = target / file.filename();
    std::filesystem::copy_file(file, destination, std::filesystem::copy_options::none, error);
    if (error)
      return filesystem_error(error);

    EF_RETURN_IF_ERROR(sync_path(destination, false));
  }

  EF_RETURN_IF_ERROR(sync_path(target, true));
  EF_RETURN_IF_ERROR(sync_path(target.parent_path(), true));
  return open(target.string(), validated);
}

RocksStateStore::RocksStateStore(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}

RocksStateStore::~RocksStateStore() noexcept = default;

StatusOr<StoreLimits> RocksStateStore::limits() noexcept {
  const auto& config = impl_->config();
  return StoreLimits{.max_batch_bytes = config.max_batch_bytes,
                     .max_mutations = config.max_mutations,
                     .max_read_keys = config.max_read_keys,
                     .max_read_bytes = config.max_read_bytes,
                     .max_scan_rows = config.max_scan_rows};
}

StatusOr<ReadResult> RocksStateStore::get_many(std::uint32_t shard,
                                               const std::vector<std::string>& keys) noexcept {
  return impl_->get_many(shard, keys);
}

StatusOr<ScanResult> RocksStateStore::scan(std::uint32_t shard, const std::string& prefix,
                                           const std::optional<std::string>& after,
                                           std::size_t limit) noexcept {
  return impl_->scan(shard, prefix, after, limit);
}

StatusOr<CommitResult> RocksStateStore::commit(const CommitBatch& batch) noexcept {
  return impl_->commit(batch);
}

StatusOr<ShardStatus> RocksStateStore::status(std::uint32_t shard) noexcept {
  return impl_->status(shard);
}

StatusOr<StoreSnapshot> RocksStateStore::create_snapshot(const std::string& id) noexcept {
  return impl_->create_snapshot(id);
}

StatusOr<StoreSnapshot> RocksStateStore::snapshot_status(const std::string& id) noexcept {
  return impl_->snapshot_status(id);
}

} // namespace eventflow
