#pragma once
#include "eventflow/types.hpp"
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace eventflow {
// A stable, versioned hash is part of persistent partitioning, not std::hash.
std::uint64_t stable_hash(std::string_view bytes) noexcept;
StatusOr<std::uint32_t> shard_for(const Row& partition_key, std::uint32_t shard_count) noexcept;
StatusOr<std::string> encode_key_parts(const Row& parts) noexcept;
StatusOr<std::string> data_prefix(std::uint32_t shard) noexcept;
StatusOr<std::string> table_prefix(std::uint32_t shard, char kind,
                                   std::uint32_t object_id) noexcept;
StatusOr<std::string> row_key(std::uint32_t shard, char kind, std::uint32_t object_id,
                              const Row& key) noexcept;
bool key_in_shard(std::uint32_t shard, std::string_view key) noexcept;
StatusOr<std::optional<std::string>> prefix_end(std::string_view prefix) noexcept;
StatusOr<std::string> hex_encode(std::string_view bytes) noexcept;
StatusOr<std::string> hex_decode(std::string_view text) noexcept;
} // namespace eventflow
