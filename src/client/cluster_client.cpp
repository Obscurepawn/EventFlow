#include "eventflow/cluster_client.hpp"
#include "eventflow/key_codec.hpp"
#include <algorithm>
#include <arpa/inet.h>
#include <array>
#include <charconv>
#include <httplib.h>
#include <limits>
#include <mutex>
#include <set>

namespace eventflow {
namespace {
Status invalid(std::string_view message) noexcept {
  return {ErrorCode::InvalidArgument, message};
}

Status unavailable(std::string_view message) noexcept {
  return {ErrorCode::Unavailable, message};
}

struct Endpoint {
  std::string host;
  int port = 80;
};

StatusOr<Endpoint> parse_endpoint(std::string_view text) noexcept {
  EF_RETURN_IF_ERROR(check(text.starts_with("http://") && text.size() <= 512,
                           "Cluster endpoint must be a bounded http:// origin"));
  text.remove_prefix(7);
  if (text.ends_with('/'))
    text.remove_suffix(1);

  EF_RETURN_IF_ERROR(check(!text.empty() && text.find_first_of("/?#@") == std::string_view::npos,
                           "Cluster endpoint must contain only host and port"));
  std::string_view host, port_text;
  bool has_port = false;
  if (text.starts_with('[')) {
    auto end = text.find(']');
    EF_RETURN_IF_ERROR(check(end != std::string_view::npos, "invalid IPv6 endpoint"));
    host = text.substr(1, end - 1);
    auto suffix = text.substr(end + 1);
    if (!suffix.empty()) {
      EF_RETURN_IF_ERROR(check(suffix.starts_with(':'), "invalid endpoint suffix"));
      has_port = true;
      port_text = suffix.substr(1);
    }

    in6_addr address{};
    std::string ipv6(host);
    EF_RETURN_IF_ERROR(
        check(::inet_pton(AF_INET6, ipv6.c_str(), &address) == 1, "invalid IPv6 address"));
  } else {
    auto colon = text.find(':');
    host = text.substr(0, colon);
    if (colon != std::string_view::npos) {
      has_port = true;
      port_text = text.substr(colon + 1);
    }

    EF_RETURN_IF_ERROR(check(!host.empty() && host.size() <= 253, "invalid endpoint host"));
    for (const auto character : host) {
      const auto c = static_cast<unsigned char>(character);
      EF_RETURN_IF_ERROR(check((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                                   (c >= '0' && c <= '9') || c == '-' || c == '.',
                               "invalid endpoint host character"));
    }
  }

  unsigned port = 80;
  if (has_port) {
    auto [end, error] =
        std::from_chars(port_text.data(), port_text.data() + port_text.size(), port);
    EF_RETURN_IF_ERROR(check(error == std::errc{} && end == port_text.data() + port_text.size() &&
                                 port > 0 && port <= 65535,
                             "invalid endpoint port"));
  }

  return Endpoint{.host = std::string(host), .port = static_cast<int>(port)};
}

StatusOr<const Json*> member(const Json& json, const char* key) noexcept {
  if (!json.is_object())
    return invalid("Cluster response must be an object");

  auto found = json.find(key);
  if (found == json.end())
    return invalid("Missing Cluster response field");

  return &*found;
}

StatusOr<std::string> text(const Json& json, const char* key) noexcept {
  EF_ASSIGN_OR_RETURN(auto value, member(json, key));
  if (!value->is_string() || !valid_utf8(value->get_ref<const std::string&>()))
    return invalid("Cluster response field must be UTF-8 text");

  return value->get<std::string>();
}

StatusOr<std::uint64_t> unsigned_integer(const Json& value) noexcept {
  if (!value.is_number_integer() || (!value.is_number_unsigned() && value.get<std::int64_t>() < 0))
    return invalid("Cluster response field must be a nonnegative integer");

  return value.get<std::uint64_t>();
}

StatusOr<std::uint64_t> decimal(const Json& value) noexcept {
  if (!value.is_string())
    return invalid("Cluster 64-bit metadata must be a decimal string");

  const auto& string = value.get_ref<const std::string&>();
  if (string.empty() || string.size() > 20 || (string.size() > 1 && string.front() == '0'))
    return invalid("Noncanonical Cluster decimal identity");

  std::uint64_t result = 0;
  const auto [end, error] = std::from_chars(string.data(), string.data() + string.size(), result);
  if (error != std::errc{} || end != string.data() + string.size())
    return invalid("Invalid Cluster decimal identity");

  return result;
}

StatusOr<std::uint64_t> decimal_field(const Json& json, const char* key) noexcept {
  EF_ASSIGN_OR_RETURN(auto value, member(json, key));
  return decimal(*value);
}

StatusOr<std::uint32_t> shard_field(const Json& json, const char* key = "shard") noexcept {
  EF_ASSIGN_OR_RETURN(auto value, member(json, key));
  EF_ASSIGN_OR_RETURN(auto number, unsigned_integer(*value));
  if (number > UINT32_MAX)
    return invalid("Cluster shard exceeds UInt32");

  return static_cast<std::uint32_t>(number);
}

Status response_error(const Json& json) noexcept {
  auto value = member(json, "error_code");
  auto message = text(json, "error");
  if (!value.ok() || !message.ok())
    return unavailable("Malformed Cluster error response");

  auto code = unsigned_integer(**value);
  if (!code.ok() || !*code || *code > static_cast<std::uint64_t>(ErrorCode::UnknownCommit))
    return unavailable("Malformed Cluster error code");

  return {static_cast<ErrorCode>(*code), *message};
}

Status decode_operation_status(const Json& json, Status& result) noexcept {
  EF_ASSIGN_OR_RETURN(auto value, member(json, "code"));
  EF_ASSIGN_OR_RETURN(auto code, unsigned_integer(*value));
  EF_ASSIGN_OR_RETURN(auto message, text(json, "message"));
  if (code > static_cast<std::uint64_t>(ErrorCode::UnknownCommit))
    return invalid("Invalid nested Cluster operation status");

  result = Status(static_cast<ErrorCode>(code), message);
  return {};
}

StatusOr<Json> response_json(std::string_view bytes) noexcept {
  std::size_t depth = 0;
  bool quoted = false, escaped = false;
  for (char c : bytes) {
    if (quoted) {
      if (escaped)
        escaped = false;
      else if (c == '\\')
        escaped = true;
      else if (c == '"')
        quoted = false;
    } else if (c == '"')
      quoted = true;
    else if (c == '[' || c == '{') {
      if (++depth >= 64)
        return invalid("Cluster response nesting exceeds limit");
    } else if ((c == ']' || c == '}') && depth)
      --depth;
  }

  EF_ASSIGN_OR_RETURN(auto exact, parse_json_exact(bytes));
  std::array<std::set<std::string>, 64> keys;
  bool duplicate = false;
  auto checked = Json::parse(
      bytes,
      [&](int level, Json::parse_event_t event, Json& value) noexcept {
        if (event == Json::parse_event_t::object_start)
          keys[static_cast<std::size_t>(level)].clear();
        else if (event == Json::parse_event_t::key)
          duplicate |=
              !keys[static_cast<std::size_t>(level - 1)].insert(value.get<std::string>()).second;
        return true;
      },
      false);
  if (duplicate || checked.is_discarded() || !exact.is_object())
    return invalid("Malformed or duplicate Cluster response fields");

  return exact;
}

StatusOr<Json> row_json(const Row& row, std::size_t limit) noexcept {
  if (row.size() > 4096)
    return invalid("Cluster row exceeds field limit");

  Json result = Json::array();
  for (const auto& value : row) {
    if (const auto* string = std::get_if<std::string>(&value); string && string->size() > limit)
      return Status(ErrorCode::ResourceExhausted, "Cluster row String exceeds request budget");
    if (const auto* vector = std::get_if<std::vector<double>>(&value);
        vector && vector->size() > limit / sizeof(double))
      return Status(ErrorCode::ResourceExhausted, "Cluster row vector exceeds request budget");

    EF_ASSIGN_OR_RETURN(auto encoded, value_to_json(value));
    result.push_back(std::move(encoded));
  }

  if (result.dump().size() > limit)
    return Status(ErrorCode::ResourceExhausted, "Cluster row exceeds request budget");

  return result;
}

Status request_text(std::string_view value, std::size_t limit) noexcept {
  return check(!value.empty() && value.size() <= limit && valid_utf8(value),
               "Empty, oversized or invalid Cluster request text");
}

StatusOr<ClusterIngestResult> decode_ingest(Json json) noexcept {
  ClusterIngestResult result;
  EF_ASSIGN_OR_RETURN(auto committed, member(json, "input_committed"));
  EF_ASSIGN_OR_RETURN(auto duplicate, member(json, "duplicate"));
  if (!committed->is_boolean() || !duplicate->is_boolean())
    return invalid("Cluster acknowledgement flags must be boolean");

  result.input_committed = committed->get<bool>();
  result.duplicate = duplicate->get<bool>();
  EF_ASSIGN_OR_RETURN(auto operation, member(json, "operation_status"));
  EF_RETURN_IF_ERROR(decode_operation_status(*operation, result.operation_status));
  EF_ASSIGN_OR_RETURN(result.shard, shard_field(json));
  EF_ASSIGN_OR_RETURN(result.node_id, text(json, "node_id"));
  if (result.node_id.empty())
    return invalid("Cluster acknowledgement has no worker identity");
  if (json.contains("revision")) {
    EF_ASSIGN_OR_RETURN(auto revision, decimal_field(json, "revision"));
    result.revision = revision;
  }

  result.details = std::move(json);
  return result;
}

StatusOr<ClusterQueryResult> decode_query(Json json, std::size_t max_rows) noexcept {
  ClusterQueryResult result;
  EF_ASSIGN_OR_RETURN(auto fields, member(json, "fields"));
  EF_ASSIGN_OR_RETURN(auto rows, member(json, "rows"));
  if (!fields->is_array() || fields->empty() || fields->size() > 4096 || !rows->is_array())
    return invalid("Invalid Cluster query rows or fields");
  if (rows->size() > max_rows)
    return Status(ErrorCode::ResourceExhausted, "Query response exceeds requested bounds");

  for (const auto& field : *fields) {
    Field value;
    EF_ASSIGN_OR_RETURN(value.id, shard_field(field, "id"));
    EF_ASSIGN_OR_RETURN(value.name, text(field, "name"));
    EF_ASSIGN_OR_RETURN(auto type, text(field, "type"));
    EF_ASSIGN_OR_RETURN(value.type, parse_data_type(type));
    EF_ASSIGN_OR_RETURN(auto nullable, member(field, "nullable"));
    if (value.name.empty() || !nullable->is_boolean())
      return invalid("Invalid Cluster query field signature");

    value.nullable = nullable->get<bool>();
    result.fields.push_back(std::move(value));
  }

  for (const auto& encoded : *rows) {
    if (!encoded.is_array() || encoded.size() != result.fields.size())
      return invalid("Query row width differs from declared fields");

    Row row;
    for (std::size_t i = 0; i < result.fields.size(); ++i) {
      EF_ASSIGN_OR_RETURN(auto value, value_from_json(encoded[i], result.fields[i].type,
                                                      result.fields[i].nullable));
      row.push_back(std::move(value));
    }

    result.rows.push_back(std::move(row));
  }

  EF_ASSIGN_OR_RETURN(result.revision, decimal_field(json, "revision"));
  EF_ASSIGN_OR_RETURN(result.catalog_version, decimal_field(json, "catalog_version"));
  EF_ASSIGN_OR_RETURN(result.shard, shard_field(json));
  EF_ASSIGN_OR_RETURN(result.node_id, text(json, "node_id"));
  if (result.node_id.empty())
    return invalid("Cluster query response has no worker identity");

  result.details = std::move(json);
  return result;
}
} // namespace

struct ClusterClient::Impl {
  ClusterClientConfig config;
  std::unique_ptr<httplib::Client> client;
  std::mutex mutex;

  StatusOr<Json> post(std::string_view operation, const std::string& encoded) noexcept {
    const auto request_limit =
        operation == "register_udf" ? config.max_udf_request_bytes : config.max_request_bytes;
    if (encoded.size() > request_limit)
      return Status(ErrorCode::ResourceExhausted, "Cluster request exceeds byte limit");

    std::lock_guard lock(mutex);
    std::string body;
    bool too_large = false;
    httplib::Request request;
    request.method = "POST";
    request.path = "/v1/meta/" + std::string(operation);
    request.body = encoded;
    request.set_header("Content-Type", "application/json");
    request.response_handler = [&](const httplib::Response& response) noexcept {
      if (response.has_header("Transfer-Encoding") ||
          response.get_header_value_count("Content-Length") != 1 ||
          response.get_header_value_count("Content-Encoding") > 1 ||
          (response.has_header("Content-Encoding") &&
           response.get_header_value("Content-Encoding") != "identity"))
        return false;

      const auto length = response.get_header_value("Content-Length");
      std::size_t value = 0;
      const auto [end, error] =
          std::from_chars(length.data(), length.data() + length.size(), value);
      if (error != std::errc{} || end != length.data() + length.size())
        return false;

      too_large = value > config.max_response_bytes;
      return !too_large;
    };
    request.content_receiver = [&](const char* bytes, std::size_t size, std::uint64_t,
                                   std::uint64_t) noexcept {
      if (size > config.max_response_bytes - body.size()) {
        too_large = true;
        return false;
      }

      body.append(bytes, size);
      return true;
    };
    auto response = client->send(request);
    if (too_large)
      return Status(ErrorCode::ResourceExhausted, "Cluster response exceeds byte limit");
    if (!response)
      return unavailable("Cluster transport failure; operation outcome may be unknown");

    EF_ASSIGN_OR_RETURN(auto json, response_json(body));
    if (response->status != 200)
      return response_error(json);

    return json;
  }

  StatusOr<Json> call(std::string_view operation, const Json& body) noexcept {
    return post(operation, body.dump());
  }
};

ClusterClient::ClusterClient(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}

ClusterClient::~ClusterClient() noexcept = default;

StatusOr<std::unique_ptr<ClusterClient>>
ClusterClient::open(std::string_view endpoint, const ClusterClientConfig& config) noexcept {
  if (config.timeout_seconds <= 0 || config.timeout_seconds > 3600 || !config.max_request_bytes ||
      config.max_request_bytes > 16 * 1024 * 1024 || !config.max_response_bytes ||
      config.max_response_bytes > 64 * 1024 * 1024 || !config.ingest_attempts ||
      config.ingest_attempts > 8 || !config.max_udf_request_bytes ||
      config.max_udf_request_bytes > 16 * 1024 * 1024)
    return invalid("Invalid bounded Cluster client configuration");

  EF_ASSIGN_OR_RETURN(auto address, parse_endpoint(endpoint));
  auto impl = std::make_unique<Impl>();
  impl->config = config;
  impl->client = std::make_unique<httplib::Client>(address.host, address.port);
  impl->client->set_connection_timeout(config.timeout_seconds);
  impl->client->set_read_timeout(config.timeout_seconds);
  impl->client->set_write_timeout(config.timeout_seconds);
  impl->client->set_keep_alive(false);
  impl->client->set_follow_location(false);
  impl->client->set_decompress(false);
  impl->client->set_tcp_nodelay(true);

  return std::unique_ptr<ClusterClient>(new ClusterClient(std::move(impl)));
}

StatusOr<Json> ClusterClient::discover() noexcept {
  EF_ASSIGN_OR_RETURN(auto result, impl_->call("discover", Json::object()));
  EF_ASSIGN_OR_RETURN(auto version, decimal_field(result, "deployment_version"));
  (void)version;
  EF_ASSIGN_OR_RETURN(auto state, text(result, "state"));
  if (state != "STAGING" && state != "INSTALLING" && state != "RUNNING" && state != "PAUSED" &&
      state != "QUIESCED")
    return invalid("Unknown deployment state");

  EF_ASSIGN_OR_RETURN(auto count, shard_field(result, "shard_count"));
  EF_ASSIGN_OR_RETURN(auto routes, member(result, "routes"));
  if (!count || count > 65536 || !routes->is_array() || routes->size() != count)
    return invalid("Discovery must describe every data shard");

  std::set<std::uint32_t> known;
  for (const auto& route : *routes) {
    EF_ASSIGN_OR_RETURN(auto shard, shard_field(route));
    EF_ASSIGN_OR_RETURN(auto node, text(route, "node_id"));
    EF_ASSIGN_OR_RETURN(auto endpoint, text(route, "endpoint"));
    EF_ASSIGN_OR_RETURN(auto address, parse_endpoint(endpoint));
    (void)address;
    if (shard >= count || !known.insert(shard).second || node.empty())
      return invalid("Duplicate or invalid discovery route");
  }

  EF_ASSIGN_OR_RETURN(auto now, decimal_field(result, "now_us"));
  if (now > INT64_MAX)
    return invalid("Discovery time exceeds Int64");

  return result;
}

StatusOr<Json> ClusterClient::catalog() noexcept {
  return impl_->call("catalog", Json::object());
}

StatusOr<Json> ClusterClient::ddl(std::string_view sql) noexcept {
  EF_RETURN_IF_ERROR(request_text(sql, impl_->config.max_request_bytes));
  return impl_->call("ddl", Json{{"sql", std::string(sql)}});
}

StatusOr<FunctionRef> ClusterClient::register_udf(const std::string& library_path) noexcept {
  EF_ASSIGN_OR_RETURN(auto bytes, read_udf_file(library_path));
  const auto digest = udf_digest(bytes);
  EF_ASSIGN_OR_RETURN(auto encoded, hex_encode(bytes));
  EF_ASSIGN_OR_RETURN(auto result,
                      impl_->call("register_udf", Json{{"bytes", std::move(encoded)}}));
  EF_ASSIGN_OR_RETURN(auto ref, decode_function_ref(result));
  if (ref.digest != digest)
    return Status(ErrorCode::Conflict, "registered UDF differs from the uploaded bytes");
  return ref;
}

StatusOr<Json> ClusterClient::submit_computation(const SubmittedComputation& definition) noexcept {
  EF_ASSIGN_OR_RETURN(auto encoded, encode_computation(definition));
  return impl_->call("submit_computation", Json{{"definition", std::move(encoded)}});
}

StatusOr<Json> ClusterClient::deploy() noexcept {
  return impl_->call("deploy", Json::object());
}

StatusOr<ClusterIngestResult>
ClusterClient::ingest(std::string_view relation, const Row& row, const SourceIdentity& source,
                      std::int64_t now_us, std::optional<std::int64_t> event_time_us) noexcept {
  EF_RETURN_IF_ERROR(request_text(relation, 256));
  EF_RETURN_IF_ERROR(request_text(source.source_id, 128));
  EF_RETURN_IF_ERROR(request_text(source.topic, 249));
  if (row.empty() || source.partition < 0 || source.offset < 0 || source.offset == INT64_MAX ||
      now_us < 0 || (event_time_us && *event_time_us < 0))
    return invalid("Invalid Cluster source identity, row or time");

  EF_ASSIGN_OR_RETURN(auto values, row_json(row, impl_->config.max_request_bytes));
  Json body{{"relation", std::string(relation)},
            {"row", std::move(values)},
            {"source",
             {{"source_id", source.source_id},
              {"topic", source.topic},
              {"partition", source.partition},
              {"offset", std::to_string(source.offset)}}},
            {"now_us", std::to_string(now_us)}};
  if (event_time_us)
    body["event_time_us"] = std::to_string(*event_time_us);

  const auto encoded = body.dump();
  for (std::size_t attempt = 0; attempt < impl_->config.ingest_attempts; ++attempt) {
    auto response = impl_->post("ingest", encoded);
    const bool last = attempt + 1 == impl_->config.ingest_attempts;
    if (!response.ok()) {
      const auto code = response.status().code();
      if (!last && (code == ErrorCode::Unavailable || code == ErrorCode::UnknownCommit))
        continue;

      return response.status();
    }

    EF_ASSIGN_OR_RETURN(auto result, decode_ingest(std::move(response).value()));
    const auto code = result.operation_status.code();
    if (!result.input_committed && !last &&
        (code == ErrorCode::Unavailable || code == ErrorCode::UnknownCommit))
      continue;

    return result;
  }

  return unavailable("Cluster ingest retry budget exhausted");
}

StatusOr<Json> ClusterClient::pump(std::int64_t now_us, std::size_t max_functions) noexcept {
  if (now_us < 0 || !max_functions || max_functions > 4096)
    return invalid("Invalid Cluster pump time or function limit");

  return impl_->call("pump",
                     Json{{"now_us", std::to_string(now_us)}, {"max_functions", max_functions}});
}

StatusOr<Json> ClusterClient::drain(std::int64_t now_us, std::size_t max_rounds,
                                    std::size_t max_functions) noexcept {
  if (!max_rounds || max_rounds > 4096)
    return invalid("Invalid Cluster drain round limit");

  for (std::size_t round = 0; round < max_rounds; ++round) {
    EF_ASSIGN_OR_RETURN(auto progress, pump(now_us, max_functions));
    EF_ASSIGN_OR_RETURN(auto remaining, member(progress, "work_remaining"));
    EF_ASSIGN_OR_RETURN(auto clock_blocked, member(progress, "clock_blocked_by_pending_input"));
    if (!remaining->is_boolean() || !clock_blocked->is_boolean())
      return invalid("Cluster pump progress flags must be boolean");
    if (clock_blocked->get<bool>())
      return Status(ErrorCode::FailedPrecondition,
                    "Resolve the original pending input before advancing the cluster clock");
    if (!remaining->get<bool>())
      return progress;
  }

  return Status(ErrorCode::ResourceExhausted,
                "Cluster drain round budget exhausted; runnable function work remains");
}

StatusOr<ClusterQueryResult> ClusterClient::query(std::string_view sql, const Row& scope,
                                                  std::size_t max_rows,
                                                  std::size_t max_bytes) noexcept {
  EF_RETURN_IF_ERROR(request_text(sql, impl_->config.max_request_bytes));
  if (scope.empty() || scope.size() > 64 || !max_rows || max_rows > 1000000 || !max_bytes ||
      max_bytes > 16 * 1024 * 1024)
    return invalid("Invalid Cluster query scope or limits");

  for (const auto& value : scope)
    if (!std::holds_alternative<std::string>(value) && !std::holds_alternative<bool>(value) &&
        !std::holds_alternative<std::int64_t>(value))
      return invalid("Cluster scope requires nonnullable key values");

  EF_ASSIGN_OR_RETURN(auto values, row_json(scope, impl_->config.max_request_bytes));
  EF_ASSIGN_OR_RETURN(auto response, impl_->call("query", Json{{"sql", std::string(sql)},
                                                               {"scope", std::move(values)},
                                                               {"max_rows", max_rows},
                                                               {"max_bytes", max_bytes}}));
  return decode_query(std::move(response), max_rows);
}

StatusOr<Json> ClusterClient::pause() noexcept {
  return impl_->call("pause", Json::object());
}

StatusOr<Json> ClusterClient::resume() noexcept {
  return impl_->call("resume", Json::object());
}

StatusOr<Json> ClusterClient::checkpoint(std::string_view checkpoint_id,
                                         std::int64_t now_us) noexcept {
  EF_RETURN_IF_ERROR(request_text(checkpoint_id, 128));
  if (now_us < 0)
    return invalid("Invalid checkpoint time");

  return impl_->call("checkpoint", Json{{"checkpoint_id", std::string(checkpoint_id)},
                                        {"now_us", std::to_string(now_us)}});
}

StatusOr<Json> ClusterClient::status() noexcept {
  return impl_->call("status", Json::object());
}

StatusOr<Json> ClusterClient::read_request(const Row& scope, std::string_view request_id) noexcept {
  EF_RETURN_IF_ERROR(request_text(request_id, 4096));
  if (scope.size() != 1 || !std::holds_alternative<std::string>(scope[0]))
    return invalid("Core request audit needs one string business key");

  EF_ASSIGN_OR_RETURN(auto encoded, row_json(scope, impl_->config.max_request_bytes));
  return impl_->call("request",
                     {{"scope", std::move(encoded)}, {"request_id", std::string(request_id)}});
}
} // namespace eventflow
