#include "eventflow/key_codec.hpp"

#include <limits>

#include "eventflow/status.hpp"

namespace eventflow {
namespace {

template <class UInt> void append_be(std::string& bytes, UInt value) noexcept {
  for (int shift = static_cast<int>(sizeof(UInt) * 8) - 8; shift >= 0; shift -= 8) {
    bytes.push_back(static_cast<char>((value >> shift) & 0xff));
  }
}

StatusOr<int> hex_digit(char c) noexcept {
  if (c >= '0' && c <= '9')
    return c - '0';
  if (c >= 'a' && c <= 'f')
    return c - 'a' + 10;
  if (c >= 'A' && c <= 'F')
    return c - 'A' + 10;

  return Status(ErrorCode::InvalidArgument, "Invalid hexadecimal digit");
}

} // namespace

std::uint64_t stable_hash(std::string_view bytes) noexcept {
  // FNV-1a 64 version 1: constants and byte order are persistent format choices.
  std::uint64_t hash = 0xcbf29ce484222325ULL;
  for (const unsigned char byte : bytes) {
    hash ^= byte;
    hash *= 0x100000001b3ULL;
  }

  return hash;
}

StatusOr<std::uint32_t> shard_for(const Row& partition_key, std::uint32_t shard_count) noexcept {
  EF_RETURN_IF_ERROR(check(shard_count > 0, "Shard count must be positive"));
  EF_RETURN_IF_ERROR(check(!partition_key.empty(), "Partition key must not be empty"));
  EF_ASSIGN_OR_RETURN(auto bytes, encode_key_parts(partition_key));
  return static_cast<std::uint32_t>(stable_hash(bytes) % shard_count);
}

StatusOr<std::string> encode_key_parts(const Row& parts) noexcept {
  std::string bytes;
  for (const auto& part : parts) {
    if (const auto* boolean = std::get_if<bool>(&part)) {
      bytes.push_back('\x01');
      bytes.push_back(*boolean ? '\x01' : '\x00');
    } else if (const auto* integer = std::get_if<std::int64_t>(&part)) {
      bytes.push_back('\x02');
      // Flip the sign bit so byte ordering places negative integers before nonnegative ones.
      append_be(bytes, static_cast<std::uint64_t>(*integer) ^ (std::uint64_t{1} << 63));
    } else if (const auto* string = std::get_if<std::string>(&part)) {
      bytes.push_back('\x03');
      for (const char byte : *string) {
        bytes.push_back(byte);
        if (byte == '\0')
          bytes.push_back(static_cast<char>(0xff));
      }

      // NUL is escaped as 00 ff; 00 00 terminates one complete key part.
      bytes.append(2, '\0');
    } else {
      return Status(ErrorCode::InvalidArgument,
                    "Key parts must be non-NULL Bool, Int64, TimestampMicros or String");
    }
  }

  return bytes;
}

StatusOr<std::string> data_prefix(std::uint32_t shard) noexcept {
  std::string bytes{"D"};
  append_be(bytes, shard);
  return bytes;
}

StatusOr<std::string> table_prefix(std::uint32_t shard, char kind,
                                   std::uint32_t object_id) noexcept {
  EF_RETURN_IF_ERROR(
      check(kind == 'T' || kind == 'S' || kind == 'I', "Unknown key namespace kind"));
  EF_ASSIGN_OR_RETURN(auto bytes, data_prefix(shard));
  bytes.push_back(kind);
  append_be(bytes, object_id);

  return bytes;
}

StatusOr<std::string> row_key(std::uint32_t shard, char kind, std::uint32_t object_id,
                              const Row& key) noexcept {
  EF_RETURN_IF_ERROR(check(!key.empty(), "Row key must not be empty"));
  EF_ASSIGN_OR_RETURN(auto prefix, table_prefix(shard, kind, object_id));
  EF_ASSIGN_OR_RETURN(auto parts, encode_key_parts(key));

  return prefix + parts;
}

bool key_in_shard(std::uint32_t shard, std::string_view key) noexcept {
  if (key.size() < 5 || key[0] != 'D')
    return false;

  for (std::size_t i = 0; i < 4; ++i) {
    if (static_cast<unsigned char>(key[i + 1]) != ((shard >> (24 - i * 8)) & 0xff))
      return false;
  }

  return true;
}

StatusOr<std::optional<std::string>> prefix_end(std::string_view prefix) noexcept {
  std::string end(prefix);
  for (std::size_t i = end.size(); i > 0; --i) {
    const auto byte = static_cast<unsigned char>(end[i - 1]);
    if (byte != 0xff) {
      end[i - 1] = static_cast<char>(byte + 1);
      end.resize(i);
      return std::optional<std::string>{std::move(end)};
    }
  }

  return std::optional<std::string>{};
}

StatusOr<std::string> hex_encode(std::string_view bytes) noexcept {
  static constexpr char digits[] = "0123456789abcdef";
  EF_RETURN_IF_ERROR(
      check(bytes.size() <= std::numeric_limits<std::size_t>::max() / 2, "Hex input is too large"));
  std::string result;
  result.reserve(bytes.size() * 2);

  for (const unsigned char byte : bytes) {
    result.push_back(digits[byte >> 4]);
    result.push_back(digits[byte & 0xf]);
  }

  return result;
}

StatusOr<std::string> hex_decode(std::string_view text) noexcept {
  EF_RETURN_IF_ERROR(check(text.size() % 2 == 0, "Hex input must contain complete bytes"));
  std::string bytes;
  bytes.reserve(text.size() / 2);
  for (std::size_t i = 0; i < text.size(); i += 2) {
    EF_ASSIGN_OR_RETURN(const auto high, hex_digit(text[i]));
    EF_ASSIGN_OR_RETURN(const auto low, hex_digit(text[i + 1]));
    bytes.push_back(static_cast<char>((high << 4) | low));
  }

  return bytes;
}

} // namespace eventflow
