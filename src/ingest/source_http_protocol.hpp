#pragma once

#include "eventflow/key_codec.hpp"
#include "eventflow/message_source.hpp"
#include <charconv>

namespace eventflow::source_http {

inline Status invalid(std::string_view message) noexcept {
  return {ErrorCode::InvalidArgument, message};
}

inline Status fields(const Json& body, std::initializer_list<const char*> required,
                     std::initializer_list<const char*> optional = {}) noexcept {
  if (!body.is_object())
    return invalid("source message must be an object");
  for (const auto* name : required)
    if (!body.contains(name))
      return invalid("source message is missing a required field");
  for (const auto& field : body.items()) {
    bool known = false;
    for (const auto* name : required)
      known |= field.key() == name;
    for (const auto* name : optional)
      known |= field.key() == name;
    if (!known)
      return invalid("source message has an unknown field");
  }
  return {};
}

inline StatusOr<std::int64_t> decimal(const Json& value, bool allow_missing = false) noexcept {
  if (!value.is_string())
    return invalid("source position must be a decimal string");
  const auto& text = value.get_ref<const std::string&>();
  std::int64_t number = 0;
  const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), number);
  if (error != std::errc{} || end != text.data() + text.size() || std::to_string(number) != text ||
      number < (allow_missing ? -1 : 0))
    return invalid("invalid source decimal position");
  return std::int64_t(number);
}

inline StatusOr<SourcePartition> identity(const Json& body) noexcept {
  if (!body.is_object() || !body.contains("source_id") || !body["source_id"].is_string() ||
      !body.contains("topic") || !body["topic"].is_string() || !body.contains("partition") ||
      !body["partition"].is_number_integer())
    return invalid("invalid source partition identity");
  const auto source = body["source_id"].get<std::string>();
  const auto topic = body["topic"].get<std::string>();
  const auto& partition = body["partition"];
  if (source.empty() || source.size() > 256 || source.find('\0') != std::string::npos ||
      topic.empty() || topic.size() > 249 || topic.find('\0') != std::string::npos ||
      (partition.is_number_unsigned() && partition.get<std::uint64_t>() > INT32_MAX) ||
      (!partition.is_number_unsigned() &&
       (partition.get<std::int64_t>() < 0 || partition.get<std::int64_t>() > INT32_MAX)))
    return invalid("source partition identity exceeds its bounds");
  return SourcePartition{
      .source_id = source, .topic = topic, .partition = partition.get<std::int32_t>()};
}

inline Json identity_json(const SourcePartition& partition) noexcept {
  return Json{{"source_id", partition.source_id},
              {"topic", partition.topic},
              {"partition", partition.partition}};
}

inline StatusOr<std::optional<std::string>> bytes(const Json& value, std::size_t limit) noexcept {
  if (value.is_null())
    return std::optional<std::string>{};
  if (!value.is_string())
    return invalid("source bytes must be null or a hexadecimal string");
  const auto& text = value.get_ref<const std::string&>();
  if (text.size() % 2 != 0 || text.size() / 2 > limit)
    return Status(ErrorCode::ResourceExhausted, "source bytes exceed their limit");
  EF_ASSIGN_OR_RETURN(auto decoded, hex_decode(text));
  return std::optional<std::string>(std::move(decoded));
}

inline StatusOr<Json> encode_message(const SourceMessage& message) noexcept {
  Json result{{"offset", std::to_string(message.token.offset)},
              {"key_hex", nullptr},
              {"payload_hex", nullptr},
              {"timestamp_ms", nullptr}};
  if (message.key) {
    EF_ASSIGN_OR_RETURN(auto key, hex_encode(*message.key));
    result["key_hex"] = std::move(key);
  }
  if (message.payload) {
    EF_ASSIGN_OR_RETURN(auto payload, hex_encode(*message.payload));
    result["payload_hex"] = std::move(payload);
  }
  if (message.timestamp_ms)
    result["timestamp_ms"] = std::to_string(*message.timestamp_ms);
  return result;
}

inline StatusOr<SourceMessage> decode_message(const Json& body, const SourcePartition& partition,
                                              std::size_t limit) noexcept {
  EF_RETURN_IF_ERROR(fields(body, {"offset", "key_hex", "payload_hex", "timestamp_ms"}));
  EF_ASSIGN_OR_RETURN(auto offset, decimal(body["offset"]));
  if (offset == INT64_MAX)
    return invalid("source offset is exhausted");
  EF_ASSIGN_OR_RETURN(auto key, bytes(body["key_hex"], limit));
  EF_ASSIGN_OR_RETURN(auto payload, bytes(body["payload_hex"], limit - (key ? key->size() : 0)));
  std::optional<std::int64_t> timestamp;
  if (!body["timestamp_ms"].is_null()) {
    EF_ASSIGN_OR_RETURN(auto time, decimal(body["timestamp_ms"]));
    timestamp = time;
  }
  return SourceMessage{.token = {.partition = partition, .offset = offset},
                       .key = std::move(key),
                       .payload = std::move(payload),
                       .timestamp_ms = timestamp};
}

} // namespace eventflow::source_http
