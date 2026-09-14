#pragma once
#include "eventflow/key_codec.hpp"
#include "eventflow/state_store.hpp"
#include <limits>

namespace eventflow::kv_protocol {
inline StatusOr<const Json*> member(const Json& object, const char* name) noexcept {
  if (!object.is_object())
    return Status(ErrorCode::InvalidArgument, "expected JSON object");
  auto found = object.find(name);
  if (found == object.end())
    return Status(ErrorCode::InvalidArgument, "missing protocol field");
  return &*found;
}

inline StatusOr<std::uint64_t> unsigned_number(const Json& value) noexcept {
  if (value.is_number_unsigned())
    return value.get<std::uint64_t>();
  if (!value.is_number_integer() || value.get<std::int64_t>() < 0)
    return Status(ErrorCode::InvalidArgument, "expected non-negative integer");
  return static_cast<std::uint64_t>(value.get<std::int64_t>());
}

inline StatusOr<std::uint64_t> number(const Json& object, const char* name) noexcept {
  EF_ASSIGN_OR_RETURN(auto value, member(object, name));
  return unsigned_number(*value);
}

inline StatusOr<std::uint32_t> shard(const Json& value) noexcept {
  EF_ASSIGN_OR_RETURN(auto result, unsigned_number(value));
  if (result > std::numeric_limits<std::uint32_t>::max())
    return Status(ErrorCode::InvalidArgument, "shard integer overflow");
  return static_cast<std::uint32_t>(result);
}

inline StatusOr<std::uint32_t> shard_field(const Json& object) noexcept {
  EF_ASSIGN_OR_RETURN(auto value, member(object, "shard"));
  return shard(*value);
}

inline StatusOr<std::string> text(const Json& object, const char* name) noexcept {
  EF_ASSIGN_OR_RETURN(auto value, member(object, name));
  if (!value->is_string())
    return Status(ErrorCode::InvalidArgument, "expected string field");
  return value->get<std::string>();
}

inline StatusOr<std::string> bytes(const Json& value) noexcept {
  if (!value.is_string())
    return Status(ErrorCode::InvalidArgument, "expected hex string");
  return hex_decode(value.get_ref<const std::string&>());
}

inline StatusOr<Json> encode(const CommitBatch& batch) noexcept {
  if (!valid_utf8(batch.owner) || !valid_utf8(batch.commit_id))
    return Status(ErrorCode::InvalidArgument, "invalid commit identity encoding");
  Json mutations = Json::array();
  for (const auto& mutation : batch.mutations) {
    EF_ASSIGN_OR_RETURN(auto key, hex_encode(mutation.key));
    Json value;
    if (mutation.value) {
      EF_ASSIGN_OR_RETURN(auto encoded, hex_encode(*mutation.value));
      value = std::move(encoded);
    }
    mutations.push_back({{"key", std::move(key)}, {"value", std::move(value)}});
  }
  return Json{{"shard", batch.shard},
              {"owner", batch.owner},
              {"expected_revision", batch.expected_revision},
              {"commit_id", batch.commit_id},
              {"mutations", std::move(mutations)}};
}

inline StatusOr<CommitBatch> decode_batch(const Json& json) noexcept {
  CommitBatch batch;
  EF_ASSIGN_OR_RETURN(batch.shard, shard_field(json));
  EF_ASSIGN_OR_RETURN(batch.owner, text(json, "owner"));
  EF_ASSIGN_OR_RETURN(batch.expected_revision, number(json, "expected_revision"));
  EF_ASSIGN_OR_RETURN(batch.commit_id, text(json, "commit_id"));
  EF_ASSIGN_OR_RETURN(auto mutations, member(json, "mutations"));
  if (!mutations->is_array())
    return Status(ErrorCode::InvalidArgument, "mutations must be array");
  for (const auto& entry : *mutations) {
    EF_ASSIGN_OR_RETURN(auto key_value, member(entry, "key"));
    EF_ASSIGN_OR_RETURN(auto key, bytes(*key_value));
    EF_ASSIGN_OR_RETURN(auto value, member(entry, "value"));
    std::optional<std::string> decoded;
    if (!value->is_null()) {
      EF_ASSIGN_OR_RETURN(decoded, bytes(*value));
    }
    batch.mutations.push_back({.key = std::move(key), .value = std::move(decoded)});
  }
  return batch;
}

inline Json encode(const CommitResult& result) noexcept {
  const char* code = result.code == CommitCode::Committed  ? "COMMITTED"
                     : result.code == CommitCode::Rejected ? "REJECTED"
                                                           : "UNKNOWN";
  return Json{{"code", code},
              {"revision", result.revision},
              {"duplicate", result.duplicate},
              {"message", valid_utf8(result.message) ? result.message : "non-UTF-8 error detail"}};
}

inline StatusOr<CommitResult> decode_result(const Json& json) noexcept {
  EF_ASSIGN_OR_RETURN(auto code, text(json, "code"));
  if (code != "COMMITTED" && code != "REJECTED" && code != "UNKNOWN")
    return Status(ErrorCode::InvalidArgument, "invalid commit status");
  EF_ASSIGN_OR_RETURN(auto revision, number(json, "revision"));
  EF_ASSIGN_OR_RETURN(auto duplicate, member(json, "duplicate"));
  if (!duplicate->is_boolean())
    return Status(ErrorCode::InvalidArgument, "invalid duplicate flag");
  EF_ASSIGN_OR_RETURN(auto message, text(json, "message"));
  return CommitResult{.code = code == "COMMITTED"  ? CommitCode::Committed
                              : code == "REJECTED" ? CommitCode::Rejected
                                                   : CommitCode::Unknown,
                      .revision = revision,
                      .duplicate = duplicate->get<bool>(),
                      .message = std::move(message)};
}

inline Json encode_status(Status status) noexcept {
  return Json{{"error", valid_utf8(status.message()) ? std::string(status.message())
                                                     : "non-UTF-8 error detail"},
              {"error_code", static_cast<int>(status.code())}};
}

inline Status decode_status(const Json& json) noexcept {
  auto code = number(json, "error_code");
  auto message = text(json, "error");
  if (!code.ok() || !message.ok() || *code == 0 ||
      *code > static_cast<std::uint64_t>(ErrorCode::UnknownCommit))
    return Status(ErrorCode::Unavailable, "invalid KV error response");
  return Status(static_cast<ErrorCode>(*code), *message);
}
} // namespace eventflow::kv_protocol
