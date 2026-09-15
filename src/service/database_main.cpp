#include "eventflow/database_main.hpp"
#include "eventflow/compute_service.hpp"
#include "eventflow/coordinator_service.hpp"
#include "eventflow/kv_server.hpp"
#include "eventflow/types.hpp"
#include "eventflow/udf.hpp"
#include <charconv>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

namespace eventflow {
namespace {
volatile std::sig_atomic_t stopping = 0;

void on_signal(int) noexcept {
  stopping = 1;
}

StatusOr<unsigned> parse_unsigned(const std::string& text) noexcept {
  unsigned value = 0;
  auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc{} || end != text.data() + text.size())
    return Status(ErrorCode::InvalidArgument, "invalid unsigned option");

  return unsigned(value);
}

StatusOr<std::string> required_text(const Json& object, const char* field) noexcept {
  EF_RETURN_IF_ERROR(check(object.contains(field) && object[field].is_string(),
                           "missing or invalid source text field"));
  const auto& value = object[field].get_ref<const std::string&>();
  EF_RETURN_IF_ERROR(check(!value.empty() && value.find('\0') == std::string::npos,
                           "source text field must be nonempty and contain no null bytes"));
  return std::string(value);
}

StatusOr<std::uint64_t> optional_positive(const Json& object, const char* field,
                                          std::uint64_t fallback, std::uint64_t maximum) noexcept {
  if (!object.contains(field))
    return std::uint64_t(fallback);

  const auto& value = object[field];
  EF_RETURN_IF_ERROR(check(value.is_number_unsigned(), "invalid source numeric field"));
  const auto number = value.get<std::uint64_t>();
  EF_RETURN_IF_ERROR(
      check(number > 0 && number <= maximum, "source numeric field is out of range"));
  return std::uint64_t(number);
}

StatusOr<std::int64_t> parse_offset(const Json& value) noexcept {
  EF_RETURN_IF_ERROR(check(value.is_string(), "source next_offset must be a decimal string"));
  const auto& text = value.get_ref<const std::string&>();
  EF_RETURN_IF_ERROR(check(!text.empty() && (text.size() == 1 || text.front() != '0') &&
                               text.front() >= '0' && text.front() <= '9',
                           "source next_offset must be a canonical nonnegative decimal"));
  std::int64_t offset = 0;
  const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), offset);
  EF_RETURN_IF_ERROR(check(error == std::errc{} && end == text.data() + text.size(),
                           "source next_offset is invalid or out of range"));
  return std::int64_t(offset);
}

StatusOr<SourceSubscription> parse_source(const Json& json) noexcept {
  EF_RETURN_IF_ERROR(check(json.is_object(), "source configuration must be an object"));
  EF_ASSIGN_OR_RETURN(auto type, required_text(json, "type"));
  EF_RETURN_IF_ERROR(check(type == "http" || type == "kafka", "unknown source type"));
  EF_ASSIGN_OR_RETURN(auto source_id, required_text(json, "source_id"));
  EF_ASSIGN_OR_RETURN(auto relation, required_text(json, "relation"));
  EF_RETURN_IF_ERROR(check(json.contains("assignments") && json["assignments"].is_array() &&
                               !json["assignments"].empty() && json["assignments"].size() <= 1024,
                           "source needs between 1 and 1024 assignments"));
  EF_ASSIGN_OR_RETURN(auto poll_interval, optional_positive(json, "poll_interval_ms", 10, 1000));
  MessageSourceConfig delivery;
  delivery.source_id = std::move(source_id);
  EF_ASSIGN_OR_RETURN(auto max_message_bytes,
                      optional_positive(json, "max_message_bytes", 262144, 4 * 1024 * 1024));
  delivery.max_message_bytes = static_cast<std::size_t>(max_message_bytes);

  for (const auto& assignment : json["assignments"]) {
    EF_RETURN_IF_ERROR(check(assignment.is_object(), "source assignment must be an object"));
    EF_ASSIGN_OR_RETURN(auto topic, required_text(assignment, "topic"));
    EF_RETURN_IF_ERROR(check(assignment.contains("partition") &&
                                 assignment["partition"].is_number_unsigned() &&
                                 assignment["partition"].get<std::uint64_t>() <= INT32_MAX,
                             "source partition must be a nonnegative int32 integer"));
    SourceAssignment value{.topic = std::move(topic),
                           .partition = assignment["partition"].get<std::int32_t>(),
                           .next_offset = {}};
    if (assignment.contains("next_offset")) {
      EF_ASSIGN_OR_RETURN(auto offset, parse_offset(assignment["next_offset"]));
      value.next_offset = offset;
    }
    delivery.assignments.push_back(std::move(value));
  }

  std::shared_ptr<IMessageSource> source;
  if (type == "http") {
    HttpSourceConfig config;
    static_cast<MessageSourceConfig&>(config) = delivery;
    EF_ASSIGN_OR_RETURN(config.endpoint, required_text(json, "endpoint"));
    EF_ASSIGN_OR_RETURN(auto timeout, optional_positive(json, "operation_timeout_ms", 1000, 60000));
    EF_ASSIGN_OR_RETURN(auto max_response, optional_positive(json, "max_response_bytes",
                                                             16 * 1024 * 1024, 64 * 1024 * 1024));
    config.operation_timeout_ms = static_cast<int>(timeout);
    config.max_response_bytes = static_cast<std::size_t>(max_response);
    EF_ASSIGN_OR_RETURN(auto adapter, HttpMessageSource::open(config));
    source = std::move(adapter);
  } else {
    KafkaSourceConfig config;
    static_cast<MessageSourceConfig&>(config) = delivery;
    EF_ASSIGN_OR_RETURN(config.bootstrap_servers, required_text(json, "bootstrap_servers"));
    EF_ASSIGN_OR_RETURN(config.group_id, required_text(json, "group_id"));
    EF_ASSIGN_OR_RETURN(auto adapter, KafkaMessageSource::open(config));
    source = std::move(adapter);
  }

  return SourceSubscription{.source = std::move(source),
                            .delivery = std::move(delivery),
                            .relation = std::move(relation),
                            .poll_interval_ms = static_cast<std::uint32_t>(poll_interval)};
}

Status run(int argc, char** argv, const ComputationModule& module) noexcept {
  if (argc < 2 || std::string_view(argv[1]) == "--help") {
    std::cout
        << "EventFlow 0.1\n"
           "Usage: eventflow kv --backend memory|rocks --port PORT "
           "[--host HOST] [--data-dir DIR] [--shards N] [--owner TOKEN]\n"
           "       eventflow compute --config FILE\n"
           "       eventflow coordinator --config FILE\n"
           "KV restore: --backend rocks --restore-from OLD_DIR --snapshot ID --data-dir NEW_DIR\n";
    return {};
  }

  if (std::string_view(argv[1]) == "compute" || std::string_view(argv[1]) == "coordinator") {
    EF_RETURN_IF_ERROR(check(argc == 4 && std::string_view(argv[2]) == "--config",
                             "service requires --config FILE"));
    std::error_code error;
    const auto size = std::filesystem::file_size(argv[3], error);
    EF_RETURN_IF_ERROR(
        check(!error && size <= 1024 * 1024, "configuration unavailable or too large"));
    std::ifstream file(argv[3]);
    std::string bytes((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    EF_ASSIGN_OR_RETURN(auto json, parse_json_exact(bytes));
    EF_RETURN_IF_ERROR(
        check(json.is_object() && json.contains("kv_endpoint") && json["kv_endpoint"].is_string(),
              "invalid service configuration"));
    for (const auto* field : {"host", "owner", "node_id", "cluster_id", "restore_checkpoint"})
      if (json.contains(field))
        EF_RETURN_IF_ERROR(check(json[field].is_string(), "invalid text configuration field"));

    for (const auto* field : {"port", "shard_count", "tick_interval_ms"})
      if (json.contains(field))
        EF_RETURN_IF_ERROR(check(json[field].is_number_unsigned() &&
                                     json[field].get<std::uint64_t>() <= UINT32_MAX,
                                 "invalid numeric configuration field"));

    const auto host = json.value("host", std::string("127.0.0.1"));
    const auto port = json.value("port", 0U);
    EF_RETURN_IF_ERROR(check(port <= 65535, "invalid service port"));
    EF_ASSIGN_OR_RETURN(auto remote,
                        RemoteStateStore::open(json["kv_endpoint"].get<std::string>(), 30));
    std::shared_ptr<IStateStore> store(std::move(remote));
    std::signal(SIGTERM, on_signal);
    std::signal(SIGINT, on_signal);
    if (std::string_view(argv[1]) == "compute") {
      EF_RETURN_IF_ERROR(
          check(json.contains("node_id") && json.contains("shards") && json["shards"].is_array(),
                "Compute needs node_id and shards"));
      ComputeServiceConfig config;
      config.node_id = json["node_id"].get<std::string>();
      config.shard_count = json.value("shard_count", 1U);
      config.owner = json.value("owner", std::string("deployment-1"));
      if (json.contains("allow_native_udfs")) {
        EF_RETURN_IF_ERROR(
            check(json["allow_native_udfs"].is_boolean(), "allow_native_udfs must be boolean"));
        config.allow_native_udfs = json["allow_native_udfs"].get<bool>();
      }

      for (const auto& shard : json["shards"]) {
        EF_RETURN_IF_ERROR(
            check(shard.is_number_unsigned() && shard.get<std::uint64_t>() <= UINT32_MAX,
                  "invalid shard"));
        config.shards.push_back(shard.get<std::uint32_t>());
      }

      EF_ASSIGN_OR_RETURN(auto server, ComputeService::create(store, config, module));
      EF_ASSIGN_OR_RETURN(auto bound, server->start(host, static_cast<int>(port)));
      std::cout << "EVENTFLOW_COMPUTE_READY " << bound << std::endl;
      while (!stopping && server->is_running())
        std::this_thread::sleep_for(std::chrono::milliseconds(20));

      server->stop();
      if (!stopping)
        return Status(ErrorCode::Unavailable, "Compute listener stopped unexpectedly");
    } else {
      EF_RETURN_IF_ERROR(
          check(json.contains("nodes") && json["nodes"].is_array(), "Coordinator needs nodes"));
      CoordinatorServiceConfig config;
      config.cluster_id = json.value("cluster_id", std::string("eventflow-core"));
      config.shard_count = json.value("shard_count", 2U);
      config.owner = json.value("owner", std::string("deployment-1"));
      if (json.contains("allow_native_udfs")) {
        EF_RETURN_IF_ERROR(
            check(json["allow_native_udfs"].is_boolean(), "allow_native_udfs must be boolean"));
        config.allow_native_udfs = json["allow_native_udfs"].get<bool>();
      }

      config.restore_checkpoint = json.value("restore_checkpoint", std::string{});
      config.tick_interval_ms = json.value("tick_interval_ms", 100U);
      if (config.allow_native_udfs)
        config.max_request_bytes = 2 * max_udf_bytes + 65536;

      for (const auto& node : json["nodes"]) {
        EF_RETURN_IF_ERROR(check(node.is_object() && node.contains("node_id") &&
                                     node["node_id"].is_string() && node.contains("endpoint") &&
                                     node["endpoint"].is_string() && node.contains("shards") &&
                                     node["shards"].is_array(),
                                 "invalid node assignment"));
        ComputeNode value{.node_id = node["node_id"].get<std::string>(),
                          .endpoint = node["endpoint"].get<std::string>(),
                          .shards = {}};
        for (const auto& shard : node["shards"]) {
          EF_RETURN_IF_ERROR(
              check(shard.is_number_unsigned() && shard.get<std::uint64_t>() <= UINT32_MAX,
                    "invalid node shard"));
          value.shards.push_back(shard.get<std::uint32_t>());
        }

        config.nodes.push_back(std::move(value));
      }

      if (json.contains("source")) {
        EF_ASSIGN_OR_RETURN(auto subscription, parse_source(json["source"]));
        config.source = std::move(subscription);
      }

      EF_ASSIGN_OR_RETURN(auto server, CoordinatorService::create(store, config, module));
      EF_ASSIGN_OR_RETURN(auto bound, server->start(host, static_cast<int>(port)));
      std::cout << "EVENTFLOW_COORDINATOR_READY " << bound << std::endl;
      while (!stopping && server->is_running())
        std::this_thread::sleep_for(std::chrono::milliseconds(20));

      server->stop();
      if (!stopping)
        return Status(ErrorCode::Unavailable, "Coordinator listener stopped unexpectedly");
    }

    return {};
  }

  EF_RETURN_IF_ERROR(check(std::string_view(argv[1]) == "kv", "unknown command"));
  std::string backend = "memory", host = "127.0.0.1", directory = "run/kv";
  std::string restore_from, snapshot;
  int port = 8081;
  StoreConfig config;

  for (int i = 2; i < argc; i += 2) {
    EF_RETURN_IF_ERROR(check(i + 1 < argc, "missing option value"));
    std::string option = argv[i], value = argv[i + 1];

    if (option == "--backend")
      backend = value;
    else if (option == "--host")
      host = value;
    else if (option == "--data-dir")
      directory = value;
    else if (option == "--owner")
      config.owner = value;
    else if (option == "--restore-from")
      restore_from = value;
    else if (option == "--snapshot")
      snapshot = value;
    else if (option == "--port") {
      EF_ASSIGN_OR_RETURN(auto number, parse_unsigned(value));
      EF_RETURN_IF_ERROR(check(number <= 65535, "invalid port"));
      port = static_cast<int>(number);
    } else if (option == "--shards") {
      EF_ASSIGN_OR_RETURN(auto number, parse_unsigned(value));
      EF_RETURN_IF_ERROR(check(number > 0 && number <= 65536, "invalid shard count"));
      config.shard_count = number;
    } else
      return Status(ErrorCode::InvalidArgument, "unknown option");
  }

  std::shared_ptr<IStateStore> store;
  if (backend == "memory") {
    EF_ASSIGN_OR_RETURN(auto memory, MemoryStateStore::open(config));
    store = std::move(memory);
  } else if (backend == "rocks") {
    auto opened = restore_from.empty()
                      ? RocksStateStore::open(directory, config)
                      : RocksStateStore::from_snapshot(restore_from, snapshot, directory, config);
    EF_ASSIGN_OR_RETURN(auto rocks, std::move(opened));
    store = std::move(rocks);
  } else
    return Status(ErrorCode::InvalidArgument, "invalid backend");
  EF_ASSIGN_OR_RETURN(auto server, KvServer::create(std::move(store)));
  EF_ASSIGN_OR_RETURN(auto bound, server->start(host, port));
  std::signal(SIGTERM, on_signal);
  std::signal(SIGINT, on_signal);
  std::cout << "EVENTFLOW_KV_READY " << bound << std::endl;

  while (!stopping && server->is_running())
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

  server->stop();
  return stopping ? Status{} : Status(ErrorCode::Unavailable, "KV listener stopped unexpectedly");
}
} // namespace

int database_main(int argc, char** argv, const ComputationModule& module) noexcept {
  stopping = 0;
  const auto status = run(argc, argv, module);
  if (!status.ok()) {
    std::cerr << "eventflow: " << status.message() << '\n';
    return 1;
  }

  return 0;
}

} // namespace eventflow
