#include "eventflow/http_message_server.hpp"
#include "source_http_protocol.hpp"
#include <algorithm>
#include <httplib.h>
#include <map>
#include <mutex>
#include <thread>

namespace eventflow {

struct HttpMessageServer::Impl {
  HttpMessageServerConfig config;
  std::map<std::pair<std::string, std::int32_t>, std::vector<SourceMessage>> logs;
  std::map<std::string, std::int64_t> positions;
  std::size_t messages = 0;
  std::size_t bytes = 0;
  std::mutex gate;
  httplib::Server server;
  std::thread listener;
  bool started = false;

  Status append_locked(const std::string& topic, std::int32_t partition, std::int64_t offset,
                       std::optional<std::string> key, std::optional<std::string> payload,
                       std::optional<std::int64_t> timestamp) noexcept {
    const auto found = logs.find({topic, partition});
    if (found == logs.end() || offset < 0 || offset == INT64_MAX ||
        (!found->second.empty() && offset <= found->second.back().token.offset) ||
        (timestamp && *timestamp < 0))
      return source_http::invalid("unknown partition or non-increasing message offset");
    const auto key_bytes = key ? key->size() : 0;
    const auto payload_bytes = payload ? payload->size() : 0;
    if (key_bytes > config.max_message_bytes ||
        payload_bytes > config.max_message_bytes - key_bytes || messages >= config.max_messages ||
        key_bytes + payload_bytes > config.max_log_bytes - bytes)
      return Status(ErrorCode::ResourceExhausted, "retained HTTP message log is full");
    found->second.push_back(
        {.token = {.partition = {.source_id = {}, .topic = topic, .partition = partition},
                   .offset = offset},
         .key = std::move(key),
         .payload = std::move(payload),
         .timestamp_ms = timestamp});
    ++messages;
    bytes += key_bytes + payload_bytes;
    return {};
  }

  StatusOr<Json> handle(const std::string& action, const Json& body) noexcept {
    std::lock_guard lock(gate);
    EF_ASSIGN_OR_RETURN(auto partition, source_http::identity(body));
    const auto found = logs.find({partition.topic, partition.partition});
    if (found == logs.end())
      return source_http::invalid("HTTP message partition does not exist");
    const auto position_key = source_http::identity_json(partition).dump();
    if (action == "publish") {
      EF_RETURN_IF_ERROR(source_http::fields(
          body, {"source_id", "topic", "partition", "key_hex", "payload_hex", "timestamp_ms"}));
      Json encoded = body;
      encoded.erase("source_id");
      encoded.erase("topic");
      encoded.erase("partition");
      const auto next = found->second.empty() ? 0 : found->second.back().token.offset + 1;
      encoded["offset"] = std::to_string(next);
      EF_ASSIGN_OR_RETURN(
          auto message, source_http::decode_message(encoded, partition, config.max_message_bytes));
      EF_RETURN_IF_ERROR(append_locked(partition.topic, partition.partition, next,
                                       std::move(message.key), std::move(message.payload),
                                       message.timestamp_ms));
      return Json{{"offset", std::to_string(next)}};
    }
    if (action == "position") {
      EF_RETURN_IF_ERROR(source_http::fields(body, {"source_id", "topic", "partition"}));
      const auto position = positions.find(position_key);
      return Json{
          {"next_offset", std::to_string(position == positions.end() ? -1 : position->second)}};
    }
    if (action == "ack") {
      EF_RETURN_IF_ERROR(
          source_http::fields(body, {"source_id", "topic", "partition", "next_offset"}));
      EF_ASSIGN_OR_RETURN(auto next, source_http::decimal(body["next_offset"]));
      if (found->second.empty() || next > found->second.back().token.offset + 1)
        return source_http::invalid("acknowledgement exceeds the retained source log");
      if (!positions.contains(position_key) && positions.size() >= 4096)
        return Status(ErrorCode::ResourceExhausted, "too many HTTP source consumers");
      auto [position, inserted] = positions.try_emplace(position_key, next);
      if (!inserted)
        position->second = std::max(position->second, next);
      return Json{{"next_offset", std::to_string(position->second)}};
    }
    if (action != "poll")
      return source_http::invalid("unknown HTTP source operation");
    EF_RETURN_IF_ERROR(source_http::fields(body, {"source_id", "topic", "partition", "next_offset",
                                                  "max_messages", "max_bytes", "timeout_ms"}));
    EF_ASSIGN_OR_RETURN(auto next, source_http::decimal(body["next_offset"]));
    if (!body["max_messages"].is_number_unsigned() || !body["max_bytes"].is_number_unsigned() ||
        !body["timeout_ms"].is_number_integer() || body["max_messages"].get<std::uint64_t>() != 1 ||
        body["max_bytes"].get<std::uint64_t>() == 0 || body["timeout_ms"].get<std::int64_t>() != 0)
      return source_http::invalid("HTTP polling requires one bounded, nonblocking record");
    const auto record =
        std::lower_bound(found->second.begin(), found->second.end(), next,
                         [](const SourceMessage& item, std::int64_t offset) noexcept {
                           return item.token.offset < offset;
                         });
    Json result = Json::array();
    if (record != found->second.end()) {
      const auto size =
          (record->key ? record->key->size() : 0) + (record->payload ? record->payload->size() : 0);
      if (size > body["max_bytes"].get<std::uint64_t>())
        return Status(ErrorCode::ResourceExhausted, "poll budget cannot fit the next HTTP record");
      EF_ASSIGN_OR_RETURN(auto encoded, source_http::encode_message(*record));
      result.push_back(std::move(encoded));
    }
    return Json{{"messages", std::move(result)}};
  }
};

HttpMessageServer::HttpMessageServer(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

HttpMessageServer::~HttpMessageServer() noexcept {
  stop();
}

StatusOr<std::unique_ptr<HttpMessageServer>>
HttpMessageServer::create(const HttpMessageServerConfig& config) noexcept {
  if (config.partitions.empty() || config.partitions.size() > 1024 || config.max_messages == 0 ||
      config.max_message_bytes == 0 || config.max_message_bytes > 4 * 1024 * 1024 ||
      config.max_log_bytes < config.max_message_bytes)
    return source_http::invalid("invalid HTTP message server capacity");
  auto impl = std::make_unique<Impl>();
  impl->config = config;
  for (const auto& partition : config.partitions) {
    if (partition.topic.empty() || partition.topic.size() > 249 || partition.partition < 0 ||
        !impl->logs.try_emplace({partition.topic, partition.partition}).second)
      return source_http::invalid("invalid or duplicate HTTP source partition");
  }
  impl->server.new_task_queue = []() noexcept { return new httplib::ThreadPool(4, 32); };
  impl->server.set_payload_max_length(config.max_message_bytes * 2 + 4096);
  impl->server.set_read_timeout(2);
  impl->server.set_write_timeout(2);
  impl->server.Get("/health", [](const httplib::Request&, httplib::Response& response) noexcept {
    response.set_content(R"({"status":"ready","service":"http-message-source"})",
                         "application/json");
  });
  auto* raw = impl.get();
  impl->server.Post(R"(/v1/source/(\w+))", [raw](const httplib::Request& request,
                                                 httplib::Response& response) noexcept {
    auto body = parse_json_exact(request.body);
    auto result =
        body.ok() ? raw->handle(request.matches[1].str(), *body) : StatusOr<Json>(body.status());
    if (result.ok()) {
      response.set_content(result->dump(), "application/json");
    } else {
      response.status = 400;
      response.set_content(Json{{"error", std::string(result.status().message())},
                                {"error_code", static_cast<int>(result.status().code())}}
                               .dump(),
                           "application/json");
    }
  });
  return std::unique_ptr<HttpMessageServer>(new HttpMessageServer(std::move(impl)));
}

StatusOr<int> HttpMessageServer::start(const std::string& host, int port) noexcept {
  if (impl_->started || port < 0 || port > 65535)
    return source_http::invalid("invalid HTTP message server start");
  const auto bound = port == 0 ? impl_->server.bind_to_any_port(host)
                               : (impl_->server.bind_to_port(host, port) ? port : -1);
  if (bound <= 0)
    return Status(ErrorCode::Unavailable, "HTTP message server could not listen");
  impl_->started = true;
  impl_->listener = std::thread([this]() noexcept { impl_->server.listen_after_bind(); });
  impl_->server.wait_until_ready();
  return int(bound);
}

void HttpMessageServer::stop() noexcept {
  if (!impl_ || !impl_->started)
    return;
  impl_->server.stop();
  if (impl_->listener.joinable())
    impl_->listener.join();
  impl_->started = false;
}

bool HttpMessageServer::is_running() const noexcept {
  return impl_->server.is_running();
}

Status HttpMessageServer::append(const std::string& topic, std::int32_t partition,
                                 std::int64_t offset, std::optional<std::string> key,
                                 std::optional<std::string> payload,
                                 std::optional<std::int64_t> timestamp_ms) noexcept {
  std::lock_guard lock(impl_->gate);
  return impl_->append_locked(topic, partition, offset, std::move(key), std::move(payload),
                              timestamp_ms);
}

} // namespace eventflow
