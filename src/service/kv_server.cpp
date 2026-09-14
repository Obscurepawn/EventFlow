#include "eventflow/kv_server.hpp"
#include "eventflow/kv_protocol.hpp"
#include <charconv>
#include <functional>
#include <httplib.h>
#include <mutex>
#include <thread>

namespace eventflow {
namespace {
Json encode_snapshot(const StoreSnapshot& snapshot) noexcept {
  Json shards = Json::array();
  for (const auto& shard : snapshot.shards)
    shards.push_back({{"revision", shard.revision}, {"last_commit_id", shard.last_commit_id}});

  return Json{{"snapshot_id", snapshot.snapshot_id},
              {"shards", std::move(shards)},
              {"shard_owners", snapshot.shard_owners}};
}
} // namespace

struct KvServer::Impl {
  std::shared_ptr<IStateStore> store;
  std::unique_ptr<httplib::Server> server;
  std::mutex lifecycle;
  std::thread listener;
  bool started = false;

  void route(const std::string& path, std::function<StatusOr<Json>(const Json&)> fn) noexcept {
    server->Post(path, [fn = std::move(fn)](const httplib::Request& request,
                                            httplib::Response& response) noexcept {
      auto json = parse_json_exact(request.body);
      auto result = json.ok() ? fn(*json) : StatusOr<Json>(json.status());
      if (result.ok()) {
        response.set_content(result->dump(), "application/json");
      } else {
        const auto status = result.status();
        response.status = status.code() == ErrorCode::Unavailable ||
                                  status.code() == ErrorCode::UnknownCommit ||
                                  status.code() == ErrorCode::Internal
                              ? 503
                              : 400;
        response.set_content(kv_protocol::encode_status(status).dump(), "application/json");
      }
    });
  }

  void init() noexcept {
    server = std::make_unique<httplib::Server>();
    server->new_task_queue = []() noexcept { return new httplib::ThreadPool(4, 64); };
    server->set_payload_max_length(16 * 1024 * 1024);
    server->set_tcp_nodelay(true);

    // This RPC protocol uses bounded Content-Length frames. Reject transfer coding
    // before httplib can accumulate an unbounded chunked request body.
    server->set_pre_routing_handler([](const httplib::Request& request,
                                       httplib::Response& response) noexcept {
      Status rejected;
      const bool rpc = request.method == "POST" && request.path.starts_with("/v1/kv/");
      const bool health = request.method == "GET" && request.path == "/health";
      if (!rpc && !health) {
        rejected = Status(ErrorCode::InvalidArgument, "unsupported RPC method or path");
      } else if (request.has_header("Transfer-Encoding")) {
        rejected = Status(ErrorCode::InvalidArgument, "chunked RPC bodies are unsupported");
      } else {
        const auto count = request.get_header_value_count("Content-Length");
        if ((rpc && count != 1) || count > 1) {
          rejected = Status(ErrorCode::InvalidArgument, "RPC requires one Content-Length");
        } else if (count == 1) {
          const auto length = request.get_header_value("Content-Length");
          std::uint64_t bytes = 0;
          auto [end, error] = std::from_chars(length.data(), length.data() + length.size(), bytes);
          if (error != std::errc{} || end != length.data() + length.size()) {
            rejected = Status(ErrorCode::InvalidArgument, "invalid Content-Length");
          } else if (bytes > 16 * 1024 * 1024) {
            rejected = Status(ErrorCode::ResourceExhausted, "RPC body exceeds 16 MiB");
          }
        }
      }

      if (!rejected.ok()) {
        response.status = rejected.code() == ErrorCode::ResourceExhausted ? 413 : 400;
        response.set_header("Connection", "close");
        response.set_content(kv_protocol::encode_status(rejected).dump(), "application/json");
        return httplib::Server::HandlerResponse::Handled;
      }

      return httplib::Server::HandlerResponse::Unhandled;
    });
    server->Get("/health", [](const httplib::Request&, httplib::Response& response) noexcept {
      response.set_content(R"({"status":"ready","service":"eventflow-kv"})", "application/json");
    });
    route("/v1/kv/limits", [this](const Json& json) noexcept -> StatusOr<Json> {
      EF_RETURN_IF_ERROR(
          check(json.is_object() && json.empty(), "store limits request must be an empty object"));
      EF_ASSIGN_OR_RETURN(auto limits, store->limits());
      return Json{{"max_batch_bytes", limits.max_batch_bytes},
                  {"max_mutations", limits.max_mutations},
                  {"max_read_keys", limits.max_read_keys},
                  {"max_read_bytes", limits.max_read_bytes},
                  {"max_scan_rows", limits.max_scan_rows}};
    });
    route("/v1/kv/get", [this](const Json& json) noexcept -> StatusOr<Json> {
      EF_ASSIGN_OR_RETURN(auto shard, kv_protocol::shard_field(json));
      EF_ASSIGN_OR_RETURN(auto input, kv_protocol::member(json, "keys"));
      EF_RETURN_IF_ERROR(check(input->is_array(), "keys must be array"));
      std::vector<std::string> keys;

      for (const auto& key : *input) {
        EF_ASSIGN_OR_RETURN(auto decoded, kv_protocol::bytes(key));
        keys.push_back(std::move(decoded));
      }

      EF_ASSIGN_OR_RETURN(auto result, store->get_many(shard, keys));
      Json values = Json::array();
      for (const auto& value : result.values) {
        if (!value)
          values.push_back(nullptr);
        else {
          EF_ASSIGN_OR_RETURN(auto encoded, hex_encode(*value));
          values.push_back(std::move(encoded));
        }
      }

      return Json{{"revision", result.revision}, {"values", std::move(values)}};
    });
    route("/v1/kv/scan", [this](const Json& json) noexcept -> StatusOr<Json> {
      EF_ASSIGN_OR_RETURN(auto shard, kv_protocol::shard_field(json));
      EF_ASSIGN_OR_RETURN(auto prefix_value, kv_protocol::member(json, "prefix"));
      EF_ASSIGN_OR_RETURN(auto prefix, kv_protocol::bytes(*prefix_value));
      EF_ASSIGN_OR_RETURN(auto after_value, kv_protocol::member(json, "after"));
      std::optional<std::string> after;
      if (!after_value->is_null()) {
        EF_ASSIGN_OR_RETURN(after, kv_protocol::bytes(*after_value));
      }

      EF_ASSIGN_OR_RETURN(auto limit, kv_protocol::number(json, "limit"));
      EF_ASSIGN_OR_RETURN(auto result, store->scan(shard, prefix, after, limit));
      Json rows = Json::array();
      for (const auto& [key, value] : result.rows) {
        EF_ASSIGN_OR_RETURN(auto encoded_key, hex_encode(key));
        EF_ASSIGN_OR_RETURN(auto encoded_value, hex_encode(value));
        rows.push_back(Json::array({std::move(encoded_key), std::move(encoded_value)}));
      }

      return Json{{"revision", result.revision}, {"rows", std::move(rows)}};
    });
    route("/v1/kv/commit", [this](const Json& json) noexcept -> StatusOr<Json> {
      EF_ASSIGN_OR_RETURN(auto batch, kv_protocol::decode_batch(json));
      EF_ASSIGN_OR_RETURN(auto result, store->commit(batch));
      return kv_protocol::encode(result);
    });
    route("/v1/kv/status", [this](const Json& json) noexcept -> StatusOr<Json> {
      EF_ASSIGN_OR_RETURN(auto shard, kv_protocol::shard_field(json));
      EF_ASSIGN_OR_RETURN(auto result, store->status(shard));
      return Json{{"revision", result.revision}, {"last_commit_id", result.last_commit_id}};
    });
    route("/v1/kv/create_snapshot", [this](const Json& json) noexcept -> StatusOr<Json> {
      EF_ASSIGN_OR_RETURN(auto id, kv_protocol::text(json, "snapshot_id"));
      EF_ASSIGN_OR_RETURN(auto result, store->create_snapshot(id));
      return encode_snapshot(result);
    });
    route("/v1/kv/snapshot_status", [this](const Json& json) noexcept -> StatusOr<Json> {
      EF_ASSIGN_OR_RETURN(auto id, kv_protocol::text(json, "snapshot_id"));
      EF_ASSIGN_OR_RETURN(auto result, store->snapshot_status(id));
      return encode_snapshot(result);
    });
  }
};

StatusOr<std::unique_ptr<KvServer>> KvServer::create(std::shared_ptr<IStateStore> store) noexcept {
  EF_RETURN_IF_ERROR(check(static_cast<bool>(store), "KV backend must exist"));
  auto impl = std::make_unique<Impl>();
  impl->store = std::move(store);
  impl->init();

  return std::unique_ptr<KvServer>(new KvServer(std::move(impl)));
}

KvServer::KvServer(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}

KvServer::~KvServer() noexcept {
  stop();
}

StatusOr<int> KvServer::start(const std::string& host, int port) noexcept {
  EF_RETURN_IF_ERROR(check(port >= 0 && port <= 65535, "port out of range"));
  std::lock_guard lock(impl_->lifecycle);
  if (impl_->started)
    return Status(ErrorCode::FailedPrecondition, "KV service starts only once");

  int actual = port;
  if (port == 0)
    actual = impl_->server->bind_to_any_port(host);
  else if (!impl_->server->bind_to_port(host, port))
    actual = -1;
  if (actual < 0)
    return Status(ErrorCode::Unavailable, "cannot bind KV service");

  impl_->started = true;
  impl_->listener =
      std::thread([state = impl_.get()]() noexcept { state->server->listen_after_bind(); });

  // cpp-httplib ignores stop() before is_running(). Do not expose that race to callers.
  impl_->server->wait_until_ready();
  return int(actual);
}

bool KvServer::is_running() const noexcept {
  return impl_->server->is_running();
}

void KvServer::stop() noexcept {
  std::lock_guard lock(impl_->lifecycle);
  if (impl_->listener.joinable()) {
    impl_->server->stop();
    impl_->listener.join();
  }
}
} // namespace eventflow
