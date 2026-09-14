#include "eventflow/kv_protocol.hpp"
#include "eventflow/state_store.hpp"
#include <algorithm>
#include <arpa/inet.h>
#include <charconv>
#include <httplib.h>
#include <limits>
#include <mutex>

namespace eventflow {
namespace {
struct Endpoint {
  std::string host;
  int port = 80;
};

StatusOr<Endpoint> parse_endpoint(std::string_view text) noexcept {
  EF_RETURN_IF_ERROR(check(text.starts_with("http://") && text.size() <= 512,
                           "KV endpoint must be a bounded http:// origin"));
  text.remove_prefix(7);
  if (text.ends_with('/'))
    text.remove_suffix(1);

  EF_RETURN_IF_ERROR(check(!text.empty() && text.find_first_of("/?#@") == std::string_view::npos,
                           "KV endpoint must contain only host and port"));
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

StatusOr<StoreSnapshot> decode_snapshot(const Json& json, const std::string& expected_id) noexcept {
  EF_ASSIGN_OR_RETURN(auto id, kv_protocol::text(json, "snapshot_id"));
  EF_RETURN_IF_ERROR(check(id == expected_id, "snapshot response identity mismatch"));
  EF_ASSIGN_OR_RETURN(auto shards, kv_protocol::member(json, "shards"));
  EF_ASSIGN_OR_RETURN(auto owners, kv_protocol::member(json, "shard_owners"));
  EF_RETURN_IF_ERROR(check(shards->is_array() && !shards->empty() && owners->is_array() &&
                               shards->size() == owners->size() &&
                               shards->size() <= std::numeric_limits<std::uint32_t>::max(),
                           "snapshot response must bind every shard assignment"));
  StoreSnapshot result{.snapshot_id = std::move(id), .shards = {}, .shard_owners = {}};

  for (std::size_t index = 0; index < shards->size(); ++index) {
    const auto& owner = (*owners)[index];
    EF_RETURN_IF_ERROR(check(owner.is_string(), "snapshot owner must be string"));
    result.shard_owners.push_back(owner.get<std::string>());
    EF_RETURN_IF_ERROR(
        check(!result.shard_owners.back().empty() && valid_utf8(result.shard_owners.back()),
              "snapshot owner must be nonempty UTF-8"));
    EF_ASSIGN_OR_RETURN(auto revision, kv_protocol::number((*shards)[index], "revision"));
    EF_ASSIGN_OR_RETURN(auto commit_id, kv_protocol::text((*shards)[index], "last_commit_id"));
    if (revision == 0) {
      EF_RETURN_IF_ERROR(check(commit_id.empty(), "zero revision must have no commit ID"));
    } else {
      EF_ASSIGN_OR_RETURN(auto expected,
                          make_commit_id(result.shard_owners.back(),
                                         static_cast<std::uint32_t>(index), revision - 1));
      EF_RETURN_IF_ERROR(
          check(commit_id == expected, "snapshot receipt does not match assignment"));
    }

    result.shards.push_back({.revision = revision, .last_commit_id = std::move(commit_id)});
  }

  return result;
}

StatusOr<std::size_t> limit_field(const Json& json, const char* name) noexcept {
  EF_ASSIGN_OR_RETURN(auto value, kv_protocol::number(json, name));
  EF_RETURN_IF_ERROR(check(value > 0 && value <= std::numeric_limits<std::size_t>::max(),
                           "store limit must be a positive size_t integer"));
  return static_cast<std::size_t>(value);
}

StatusOr<StoreLimits> decode_limits(const Json& json) noexcept {
  EF_RETURN_IF_ERROR(check(json.is_object() && json.size() == 5,
                           "store limits response must contain exactly five fields"));
  StoreLimits result;
  EF_ASSIGN_OR_RETURN(result.max_batch_bytes, limit_field(json, "max_batch_bytes"));
  EF_ASSIGN_OR_RETURN(result.max_mutations, limit_field(json, "max_mutations"));
  EF_ASSIGN_OR_RETURN(result.max_read_keys, limit_field(json, "max_read_keys"));
  EF_ASSIGN_OR_RETURN(result.max_read_bytes, limit_field(json, "max_read_bytes"));
  EF_ASSIGN_OR_RETURN(result.max_scan_rows, limit_field(json, "max_scan_rows"));

  return result;
}
} // namespace

struct RemoteStateStore::Impl {
  std::unique_ptr<httplib::Client> client;
  std::mutex mutex;

  StatusOr<Json> call(const std::string& path, const Json& body) noexcept {
    std::lock_guard lock(mutex);
    auto response = client->Post(path, body.dump(), "application/json");
    if (!response)
      return Status(ErrorCode::Unavailable, "KV transport failure");

    auto json = Json::parse(response->body, nullptr, false);
    if (json.is_discarded())
      return Status(ErrorCode::Unavailable, "malformed KV response");
    if (response->status != 200)
      return kv_protocol::decode_status(json);

    return json;
  }

  StatusOr<Json> call_limits() noexcept {
    std::lock_guard lock(mutex);

    // This fixed-size metadata reply never needs an unbounded response accumulator.
    constexpr std::size_t max_response_bytes = 4096;
    std::string body;
    httplib::Request request;
    request.method = "POST";
    request.path = "/v1/kv/limits";
    request.body = "{}";
    request.set_header("Content-Type", "application/json");
    request.response_handler = [](const httplib::Response& response) noexcept {
      if (response.has_header("Transfer-Encoding") ||
          response.get_header_value_count("Content-Length") != 1 ||
          response.get_header_value_count("Content-Encoding") > 1 ||
          (response.has_header("Content-Encoding") &&
           response.get_header_value("Content-Encoding") != "identity"))
        return false;

      const auto length = response.get_header_value("Content-Length");
      std::size_t bytes = 0;
      const auto [end, error] =
          std::from_chars(length.data(), length.data() + length.size(), bytes);
      return error == std::errc{} && end == length.data() + length.size() &&
             bytes <= max_response_bytes;
    };
    request.content_receiver = [&body](const char* bytes, std::size_t count, std::uint64_t,
                                       std::uint64_t) noexcept {
      if (count > max_response_bytes - body.size())
        return false;

      body.append(bytes, count);
      return true;
    };
    const auto response = client->send(request);
    if (!response)
      return Status(ErrorCode::Unavailable, "invalid or unavailable bounded KV limits response");

    std::size_t root_keys = 0;
    auto json = Json::parse(
        body,
        [&root_keys](int depth, Json::parse_event_t event, Json&) noexcept {
          if (depth == 1 && event == Json::parse_event_t::key)
            ++root_keys;

          return true;
        },
        false);
    EF_RETURN_IF_ERROR(check(!json.is_discarded() && json.is_object() && root_keys == json.size(),
                             "malformed or duplicate KV limits response fields"));
    if (response->status != 200) {
      if (json.size() != 2)
        return Status(ErrorCode::Unavailable, "invalid KV limits error response");

      return kv_protocol::decode_status(json);
    }

    return json;
  }
};

StatusOr<std::unique_ptr<RemoteStateStore>> RemoteStateStore::open(const std::string& endpoint,
                                                                   int timeout) noexcept {
  EF_RETURN_IF_ERROR(check(timeout > 0, "RPC timeout must be positive"));
  EF_ASSIGN_OR_RETURN(auto address, parse_endpoint(endpoint));
  auto impl = std::make_unique<Impl>();
  impl->client = std::make_unique<httplib::Client>(address.host, address.port);
  impl->client->set_connection_timeout(timeout);
  impl->client->set_read_timeout(timeout);
  impl->client->set_write_timeout(timeout);
  impl->client->set_keep_alive(true);
  impl->client->set_tcp_nodelay(true);

  return std::unique_ptr<RemoteStateStore>(new RemoteStateStore(std::move(impl)));
}

RemoteStateStore::RemoteStateStore(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}

RemoteStateStore::~RemoteStateStore() noexcept = default;

StatusOr<StoreLimits> RemoteStateStore::limits() noexcept {
  EF_ASSIGN_OR_RETURN(auto json, impl_->call_limits());
  EF_ASSIGN_OR_RETURN(auto result, decode_limits(json));
  constexpr std::size_t wire_bytes = 16 * 1024 * 1024;
  constexpr std::size_t fixed_bytes = 128;

  // Canonical bytes include both owner and commit ID: even arbitrary escaped
  // UTF-8 identities fit within six times that representation. Binary keys and
  // values need only hex expansion. This deliberately advertises a conservative
  // rectangle; individual larger operations may still fit the real HTTP frame.
  result.max_batch_bytes = std::min(result.max_batch_bytes, wire_bytes / 6);
  result.max_mutations = std::min(result.max_mutations, result.max_batch_bytes / 9);
  result.max_read_bytes = std::min(result.max_read_bytes, (wire_bytes - fixed_bytes) / 4);
  const auto remaining = wire_bytes - fixed_bytes - 2 * result.max_read_bytes;
  result.max_read_keys = std::min(result.max_read_keys, remaining / 5);
  result.max_scan_rows = std::min(result.max_scan_rows, remaining / 8);

  // Scan requests also require prefix and after to be individually within the
  // raw read byte bound. The generic response receiver has no hard size cap;
  // these limits describe an operation profile, not hostile-response handling.
  if (result.max_batch_bytes == 0 || result.max_mutations == 0 || result.max_read_keys == 0 ||
      result.max_read_bytes == 0 || result.max_scan_rows == 0)
    return Status(ErrorCode::ResourceExhausted, "store has no usable remote budget");

  return result;
}

StatusOr<ReadResult> RemoteStateStore::get_many(std::uint32_t shard,
                                                const std::vector<std::string>& keys) noexcept {
  Json encoded = Json::array();
  for (const auto& key : keys) {
    EF_ASSIGN_OR_RETURN(auto item, hex_encode(key));
    encoded.push_back(std::move(item));
  }

  EF_ASSIGN_OR_RETURN(auto json,
                      impl_->call("/v1/kv/get", {{"shard", shard}, {"keys", std::move(encoded)}}));
  EF_ASSIGN_OR_RETURN(auto revision, kv_protocol::number(json, "revision"));
  EF_ASSIGN_OR_RETURN(auto values, kv_protocol::member(json, "values"));
  EF_RETURN_IF_ERROR(
      check(values->is_array() && values->size() == keys.size(), "KV response count mismatch"));
  ReadResult result{.revision = revision, .values = {}};

  for (const auto& value : *values) {
    if (value.is_null())
      result.values.emplace_back(std::nullopt);
    else {
      EF_ASSIGN_OR_RETURN(auto decoded, kv_protocol::bytes(value));
      result.values.emplace_back(std::move(decoded));
    }
  }

  return result;
}

StatusOr<ScanResult> RemoteStateStore::scan(std::uint32_t shard, const std::string& prefix,
                                            const std::optional<std::string>& after,
                                            std::size_t limit) noexcept {
  EF_ASSIGN_OR_RETURN(auto encoded_prefix, hex_encode(prefix));
  Json encoded_after;
  if (after) {
    EF_ASSIGN_OR_RETURN(auto value, hex_encode(*after));
    encoded_after = std::move(value);
  }

  EF_ASSIGN_OR_RETURN(auto json, impl_->call("/v1/kv/scan", {{"shard", shard},
                                                             {"prefix", std::move(encoded_prefix)},
                                                             {"after", std::move(encoded_after)},
                                                             {"limit", limit}}));
  EF_ASSIGN_OR_RETURN(auto revision, kv_protocol::number(json, "revision"));
  EF_ASSIGN_OR_RETURN(auto rows, kv_protocol::member(json, "rows"));
  EF_RETURN_IF_ERROR(check(rows->is_array(), "KV rows must be array"));
  ScanResult result{.revision = revision, .rows = {}};

  for (const auto& row : *rows) {
    EF_RETURN_IF_ERROR(check(row.is_array() && row.size() == 2, "KV row must be pair"));
    EF_ASSIGN_OR_RETURN(auto key, kv_protocol::bytes(row[0]));
    EF_ASSIGN_OR_RETURN(auto value, kv_protocol::bytes(row[1]));
    result.rows.emplace_back(std::move(key), std::move(value));
  }

  return result;
}

StatusOr<CommitResult> RemoteStateStore::commit(const CommitBatch& batch) noexcept {
  EF_ASSIGN_OR_RETURN(auto body, kv_protocol::encode(batch));
  auto reply = impl_->call("/v1/kv/commit", body);
  if (!reply.ok()) {
    const auto status = reply.status();
    if (status.code() == ErrorCode::Unavailable || status.code() == ErrorCode::UnknownCommit ||
        status.code() == ErrorCode::Internal)
      return CommitResult{.code = CommitCode::Unknown,
                          .revision = 0,
                          .duplicate = false,
                          .message = std::string(status.message())};

    return status;
  }

  // An invalid acknowledgement never proves that the write was rejected.
  auto decoded = kv_protocol::decode_result(*reply);
  if (!decoded.ok())
    return CommitResult{.code = CommitCode::Unknown,
                        .revision = 0,
                        .duplicate = false,
                        .message = "unconfirmed KV acknowledgement"};
  if (decoded->code == CommitCode::Committed &&
      (batch.expected_revision == std::numeric_limits<std::uint64_t>::max() ||
       decoded->revision != batch.expected_revision + 1)) {
    return CommitResult{.code = CommitCode::Unknown,
                        .revision = 0,
                        .duplicate = false,
                        .message = "impossible KV commit revision"};
  }

  return std::move(decoded).value();
}

StatusOr<ShardStatus> RemoteStateStore::status(std::uint32_t shard) noexcept {
  EF_ASSIGN_OR_RETURN(auto json, impl_->call("/v1/kv/status", {{"shard", shard}}));
  EF_ASSIGN_OR_RETURN(auto revision, kv_protocol::number(json, "revision"));
  EF_ASSIGN_OR_RETURN(auto id, kv_protocol::text(json, "last_commit_id"));
  return ShardStatus{.revision = revision, .last_commit_id = std::move(id)};
}

StatusOr<StoreSnapshot> RemoteStateStore::create_snapshot(const std::string& id) noexcept {
  EF_RETURN_IF_ERROR(validate_snapshot_id(id));
  EF_ASSIGN_OR_RETURN(auto json, impl_->call("/v1/kv/create_snapshot", {{"snapshot_id", id}}));
  return decode_snapshot(json, id);
}

StatusOr<StoreSnapshot> RemoteStateStore::snapshot_status(const std::string& id) noexcept {
  EF_RETURN_IF_ERROR(validate_snapshot_id(id));
  EF_ASSIGN_OR_RETURN(auto json, impl_->call("/v1/kv/snapshot_status", {{"snapshot_id", id}}));
  return decode_snapshot(json, id);
}
} // namespace eventflow
