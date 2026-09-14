#include "eventflow/coordinator_service.hpp"
#include "eventflow/catalog_codec.hpp"
#include "eventflow/computation_runtime.hpp"
#include "eventflow/key_codec.hpp"
#include "eventflow/kv_protocol.hpp"
#include "eventflow/time.hpp"
#include <atomic>
#include <charconv>
#include <chrono>
#include <httplib.h>
#include <mutex>
#include <set>
#include <thread>

namespace eventflow {
namespace {
Status invalid(std::string_view text) noexcept {
  return {ErrorCode::InvalidArgument, text};
}

Status blocked(std::string_view text) noexcept {
  return {ErrorCode::FailedPrecondition, text};
}

Status fields(const Json& body, std::initializer_list<const char*> required,
              std::initializer_list<const char*> optional = {}) noexcept {
  if (!body.is_object())
    return invalid("Meta request must be an object");

  for (const auto* field : required)
    if (!body.contains(field))
      return invalid("Meta request is missing a required field");

  for (const auto& field : body.items()) {
    bool known = false;
    for (const auto* name : required)
      known |= field.key() == name;

    for (const auto* name : optional)
      known |= field.key() == name;

    if (!known)
      return invalid("unknown Meta request field");
  }

  return {};
}

StatusOr<std::uint64_t> decimal(const Json& value) noexcept {
  if (!value.is_string())
    return invalid("expected a decimal string");

  const auto& text = value.get_ref<const std::string&>();
  std::uint64_t result = 0;
  const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), result);
  if (error != std::errc{} || end != text.data() + text.size() || std::to_string(result) != text)
    return invalid("invalid canonical decimal string");

  return result;
}

StatusOr<std::int64_t> time_field(const Json& body, const char* field) noexcept {
  EF_ASSIGN_OR_RETURN(auto value, kv_protocol::member(body, field));
  EF_ASSIGN_OR_RETURN(auto number, decimal(*value));
  if (number > INT64_MAX)
    return invalid("time or offset exceeds int64 range");

  return static_cast<std::int64_t>(number);
}

StatusOr<Row> row_value(const Json& value, const std::vector<Field>& fields) noexcept {
  if (!value.is_array() || value.size() != fields.size() || value.size() > 4096)
    return invalid("invalid row array or schema width");

  Row result;
  for (std::size_t i = 0; i < value.size(); ++i) {
    EF_ASSIGN_OR_RETURN(auto cell, value_from_json(value[i], fields[i].type, fields[i].nullable));
    result.push_back(std::move(cell));
  }

  return result;
}

Json operation_status(const Json& source) noexcept {
  if (source.contains("code") && source.contains("message"))
    return source;

  return {{"code", source.value("error_code", static_cast<int>(ErrorCode::Unavailable))},
          {"message", source.value("error", std::string("missing operation status"))}};
}

StatusOr<Json> call_node(const ComputeNode& node, const std::string& action,
                         const Json& request) noexcept {
  httplib::Client client(node.endpoint);
  if (!client.is_valid())
    return invalid("invalid Compute endpoint");

  client.set_connection_timeout(3);
  client.set_read_timeout(30);
  client.set_write_timeout(30);
  const auto body = request.dump();
  if (body.size() > 1024 * 1024)
    return Status(ErrorCode::ResourceExhausted, "Compute request too large");

  auto reply = client.Post("/v1/compute/" + action, body, "application/json");
  if (!reply)
    return Status(ErrorCode::Unavailable, "Compute transport did not confirm operation");
  if (reply->body.size() > 4 * 1024 * 1024)
    return Status(ErrorCode::ResourceExhausted, "Compute response exceeds core bound");

  EF_ASSIGN_OR_RETURN(auto json, parse_json_exact(reply->body));
  if (reply->status != 200)
    return kv_protocol::decode_status(json);
  if (!json.is_object())
    return invalid("Compute response must be an object");

  return json;
}
} // namespace

struct CoordinatorService::Impl {
  std::shared_ptr<IStateStore> store;
  CoordinatorServiceConfig config;
  ComputationModule module;
  std::unique_ptr<SourcePoller> source_poller;
  Status background_status;
  std::vector<std::size_t> routes;
  std::string key;
  std::uint64_t revision = 0;
  Json state;
  CatalogSnapshot catalog;
  std::optional<CommitBatch> pending_batch;
  Json pending_state;
  bool failed = false;
  std::mutex gate;
  std::unique_ptr<httplib::Server> server;
  std::thread listener, ticker;
  std::atomic<bool> stopping{false};
  bool started = false;

  Json source_description() const noexcept {
    if (!config.source)
      return nullptr;
    Json assignments = Json::array();
    for (const auto& partition : config.source->delivery.assignments)
      assignments.push_back({{"topic", partition.topic},
                             {"partition", partition.partition},
                             {"next_offset", std::to_string(partition.next_offset.value_or(0))}});
    return Json{{"source_id", config.source->delivery.source_id},
                {"relation", config.source->relation},
                {"assignments", std::move(assignments)}};
  }

  Status source_paused(bool paused) noexcept {
    if (!source_poller)
      return {};
    source_poller->pause();
    for (const auto& partition : config.source->delivery.assignments) {
      const SourcePartition identity{.source_id = config.source->delivery.source_id,
                                     .topic = partition.topic,
                                     .partition = partition.partition};
      EF_RETURN_IF_ERROR(paused ? config.source->source->pause(identity)
                                : config.source->source->resume(identity));
    }
    if (!paused)
      source_poller->resume();
    return {};
  }

  StatusOr<SourceApplyResult> apply_source(const SourceMessage& message,
                                           std::int64_t now_us) noexcept {
    if (!config.source || !message.payload)
      return invalid("automatic source requires a non-null row payload");
    const auto& subscription = *config.source;
    const auto& identity = message.token.partition;
    const auto assigned = std::any_of(
        subscription.delivery.assignments.begin(), subscription.delivery.assignments.end(),
        [&](const SourceAssignment& item) noexcept {
          return item.topic == identity.topic && item.partition == identity.partition;
        });
    if (identity.source_id != subscription.delivery.source_id || !assigned)
      return invalid("source returned a delivery outside its configured assignment");
    EF_ASSIGN_OR_RETURN(auto relation, catalog.find(subscription.relation));
    EF_ASSIGN_OR_RETURN(auto payload, parse_json_exact(*message.payload));
    auto decoded = payload.is_object() ? relation->storage_schema.from_json(payload)
                                       : row_value(payload, relation->storage_schema.fields);
    if (!decoded.ok())
      return decoded.status();
    Json row = Json::array();
    for (const auto& value : *decoded) {
      EF_ASSIGN_OR_RETURN(auto encoded, value_to_json(value));
      row.push_back(std::move(encoded));
    }
    Json source{{"source_id", identity.source_id},
                {"topic", identity.topic},
                {"partition", identity.partition},
                {"offset", std::to_string(message.token.offset)}};
    std::int64_t event_time = now_us;
    if (message.timestamp_ms) {
      if (*message.timestamp_ms < 0 || *message.timestamp_ms > INT64_MAX / 1000)
        return invalid("source timestamp cannot be represented in microseconds");
      event_time = *message.timestamp_ms * 1000;
    }
    Json body{{"relation", subscription.relation},
              {"row", std::move(row)},
              {"source", std::move(source)},
              {"now_us", std::to_string(now_us)},
              {"event_time_us", std::to_string(event_time)}};
    if (!state["pending_input"].is_null()) {
      const auto& pending = state["pending_input"];
      if (pending["relation"] != body["relation"] || pending["row"] != body["row"] ||
          pending["source"] != body["source"])
        return Status(ErrorCode::Conflict, "source replay changed the pending database input");
      body = pending;
    }
    EF_ASSIGN_OR_RETURN(auto reply, ingest(body));
    const auto& status = reply["operation_status"];
    if (!status.is_object() || !status.contains("code") || !status["code"].is_number_integer() ||
        !status.contains("message") || !status["message"].is_string())
      return invalid("Compute returned an invalid input operation status");
    const auto code = status["code"].get<std::int64_t>();
    if (code < 0 || code > static_cast<int>(ErrorCode::UnknownCommit))
      return invalid("Compute returned an unknown input operation code");
    return SourceApplyResult{
        .input_committed = reply.value("input_committed", false),
        .operation_status = code == 0 ? Status{}
                                      : Status(static_cast<ErrorCode>(code),
                                               status["message"].get_ref<const std::string&>())};
  }

  Status prepare_source() noexcept {
    if (!config.source)
      return {};
    for (const auto& assignment : config.source->delivery.assignments) {
      const SourcePartition partition{.source_id = config.source->delivery.source_id,
                                      .topic = assignment.topic,
                                      .partition = assignment.partition};
      const auto source_key =
          Json::array({partition.source_id, partition.topic, partition.partition}).dump();
      auto next = assignment.next_offset.value_or(0);
      if (state["progress"].contains(source_key)) {
        EF_ASSIGN_OR_RETURN(next, time_field(state["progress"][source_key], "next_offset"));
      }
      // Resume from the database cut, never from a newer broker acknowledgement.
      EF_RETURN_IF_ERROR(config.source->source->seek(partition, next));
    }
    EF_ASSIGN_OR_RETURN(
        source_poller,
        SourcePoller::open(config.source->source,
                           [this](const SourceMessage& message, std::int64_t now) noexcept {
                             return apply_source(message, now);
                           },
                           {.max_message_bytes = config.source->delivery.max_message_bytes}));
    return source_paused(true);
  }

  std::string version() const noexcept {
    return state["deployment_version"].get<std::string>();
  }

  std::string phase() const noexcept {
    return state["state"].get<std::string>();
  }

  Status resolve() noexcept {
    if (failed)
      return blocked("Meta owner is failed; restore a consistent checkpoint");
    if (!pending_batch)
      return {};

    EF_ASSIGN_OR_RETURN(auto result, store->commit(*pending_batch));
    if (result.code == CommitCode::Unknown)
      return Status(ErrorCode::UnknownCommit, "Meta commit remains unconfirmed");
    if (result.code != CommitCode::Committed || result.revision != revision + 1) {
      failed = true;
      return blocked("Meta commit was rejected");
    }

    ++revision;
    state = std::move(pending_state);
    pending_batch.reset();
    if (state["catalog_fingerprint"] != catalog.fingerprint) {
      const auto rebuilt = rebuild();
      if (!rebuilt.ok()) {
        failed = true;
        return rebuilt;
      }
    }

    return {};
  }

  Status save(Json next) noexcept {
    EF_RETURN_IF_ERROR(resolve());
    EF_ASSIGN_OR_RETURN(auto id, make_commit_id(config.owner, config.shard_count, revision));
    CommitBatch batch{.shard = config.shard_count,
                      .owner = config.owner,
                      .expected_revision = revision,
                      .commit_id = std::move(id),
                      .mutations = {{.key = key, .value = next.dump()}}};
    EF_ASSIGN_OR_RETURN(auto limits, store->limits());
    EF_ASSIGN_OR_RETURN(auto bytes, canonical_batch(batch));
    if (bytes.size() > limits.max_batch_bytes ||
        key.size() + batch.mutations[0].value->size() > limits.max_read_bytes)
      return Status(ErrorCode::ResourceExhausted, "Meta state exceeds actual KV capacity");

    pending_batch = std::move(batch);
    pending_state = std::move(next);
    return resolve();
  }

  Status rebuild() noexcept {
    if (!state.is_object() || !state.contains("ddl") || !state["ddl"].is_array() ||
        state["ddl"].size() > 64)
      return invalid("invalid saved deployment DDL");

    std::vector<std::string> ddl;
    for (const auto& sql : state["ddl"]) {
      if (!sql.is_string() || sql.get_ref<const std::string&>().size() > 65536)
        return invalid("invalid saved DDL statement");

      ddl.push_back(sql.get<std::string>());
    }

    EF_ASSIGN_OR_RETURN(auto definition, bind_computation_module(module, ddl));
    if (state.contains("catalog_fingerprint") &&
        state["catalog_fingerprint"] != definition.catalog.fingerprint)
      return Status(ErrorCode::Conflict, "stored Catalog differs from this recipe implementation");

    catalog = std::move(definition.catalog);
    return {};
  }

  StatusOr<Json> fanout(const std::string& action, const Json& request) noexcept {
    // Each Compute is independent. Heavy function progress must not be serialized
    // by a sequence of blocking Coordinator RPC calls.
    std::vector<Json> replies(config.nodes.size());
    std::vector<Status> statuses(config.nodes.size());
    std::vector<std::thread> threads;

    for (std::size_t i = 0; i < config.nodes.size(); ++i)
      threads.emplace_back([&, i]() noexcept {
        auto result = call_node(config.nodes[i], action, request);
        if (result.ok())
          replies[i] = std::move(result).value();
        else
          statuses[i] = result.status();
      });

    for (auto& thread : threads)
      thread.join();

    for (const auto& status : statuses)
      EF_RETURN_IF_ERROR(status);

    Json result = Json::array();
    for (auto& reply : replies)
      result.push_back(std::move(reply));

    return result;
  }

  Status verify_reports(const Json& reports, bool quiesced) noexcept {
    if (!reports.is_array() || reports.size() != config.nodes.size())
      return invalid("incomplete worker reports");

    for (std::size_t i = 0; i < reports.size(); ++i) {
      const auto& report = reports[i];
      if (!report.is_object())
        return invalid("invalid worker report");

      for (const auto* field :
           {"node_id", "deployment_version", "catalog_fingerprint", "run_state"})
        if (!report.contains(field) || !report[field].is_string())
          return invalid("invalid worker report field");

      EF_ASSIGN_OR_RETURN(auto worker_shards, kv_protocol::number(report, "shard_count"));
      if (worker_shards != config.shard_count)
        return Status(ErrorCode::Conflict, "worker logical shard count differs from deployment");
      if (report.value("node_id", std::string{}) != config.nodes[i].node_id ||
          report.value("deployment_version", std::string{}) != version() ||
          report.value("catalog_fingerprint", std::string{}) != catalog.fingerprint ||
          !report.contains("shards") || !report["shards"].is_array())
        return Status(ErrorCode::Conflict, "worker installed a different deployment image");
      if (quiesced && report.value("run_state", std::string{}) != "QUIESCED")
        return blocked("worker did not establish its checkpoint barrier");
      if (!quiesced && report["run_state"] != "RUNNING")
        return blocked("worker has not activated the deployment");

      std::set<std::uint32_t> expected(config.nodes[i].shards.begin(),
                                       config.nodes[i].shards.end());
      for (const auto& shard : report["shards"]) {
        if (!shard.is_object() || !shard.contains("owner") || !shard["owner"].is_string() ||
            !shard.contains("installed") || !shard["installed"].is_boolean() ||
            !shard.contains("failed") || !shard["failed"].is_boolean() ||
            !shard.contains("pending_commit") || !shard["pending_commit"].is_boolean())
          return invalid("invalid worker shard report");

        EF_ASSIGN_OR_RETURN(auto id, kv_protocol::shard_field(shard));
        if (expected.erase(id) != 1 || !shard.value("installed", false) ||
            shard.value("owner", std::string{}) != config.owner || shard.value("failed", true) ||
            shard.value("pending_commit", true))
          return blocked("worker shard is not confirmed");
      }

      if (!expected.empty())
        return blocked("worker omitted an assigned shard");
    }

    return {};
  }

  Status verify_source_cut(const Json& reports) noexcept {
    for (const auto& node : reports)
      for (const auto& shard : node["shards"]) {
        if (!shard.contains("source_positions") || !shard["source_positions"].is_array())
          return invalid("checkpoint omits worker source positions");

        for (const auto& position : shard["source_positions"]) {
          EF_ASSIGN_OR_RETURN(auto source_id, kv_protocol::text(position, "source_id"));
          EF_ASSIGN_OR_RETURN(auto topic, kv_protocol::text(position, "topic"));
          EF_ASSIGN_OR_RETURN(auto partition, kv_protocol::number(position, "partition"));
          const auto identity = Json::array({source_id, topic, partition}).dump();
          if (!state["progress"].contains(identity))
            return blocked("worker contains input outside the coordinated source prefix");

          EF_ASSIGN_OR_RETURN(auto worker_next, time_field(position, "next_offset"));
          EF_ASSIGN_OR_RETURN(auto global_next,
                              time_field(state["progress"][identity], "next_offset"));
          if (worker_next > global_next)
            return blocked("worker source position exceeds confirmed prefix");
        }
      }
    return {};
  }

  Json discovery() noexcept {
    Json result{{"cluster_id", config.cluster_id},
                {"deployment_version", version()},
                {"state", phase()},
                {"shard_count", config.shard_count},
                {"now_us", state["now_us"]},
                {"routes", Json::array()},
                {"meta_revision", std::to_string(revision)},
                {"source_progress", state["progress"]},
                {"watermark_us", state["watermark_us"]},
                {"config", state["config"]},
                {"worker_reports", state["worker_reports"]}};
    if (source_poller) {
      const auto& progress = source_poller->progress();
      result["source_reader"] = {
          {"source_id", config.source->delivery.source_id},
          {"relation", config.source->relation},
          {"polled", std::to_string(progress.polled)},
          {"acknowledged", std::to_string(progress.acknowledged)},
          {"paused", progress.paused},
          {"pending", progress.pending},
          {"input_committed", progress.input_committed},
          {"failed", progress.failed},
          {"operation_status", kv_protocol::encode_status(progress.operation_status)}};
    }
    result["background_status"] = kv_protocol::encode_status(background_status);
    for (std::uint32_t shard = 0; shard < config.shard_count; ++shard) {
      const auto& node = config.nodes[routes[shard]];
      result["routes"].push_back(
          {{"shard", shard}, {"node_id", node.node_id}, {"endpoint", node.endpoint}});
    }

    return result;
  }

  StatusOr<Json> deploy() noexcept {
    if (phase() == "RUNNING") {
      EF_RETURN_IF_ERROR(source_paused(false));
      return discovery();
    }
    if (phase() != "STAGING" && phase() != "INSTALLING")
      return blocked("use resume for a paused installed deployment");

    if (config.source) {
      EF_ASSIGN_OR_RETURN(auto input, catalog.find(config.source->relation));
      if (input->kind != RelationKind::BaseTable ||
          input->base_write_role != BaseWriteRole::PublicInput)
        return invalid("automatic source must bind to a public input table");
    }
    Json next = state;
    next["state"] = "INSTALLING";
    EF_RETURN_IF_ERROR(save(std::move(next)));
    EF_ASSIGN_OR_RETURN(auto reports,
                        fanout("install", {{"deployment_version", version()},
                                           {"recipe", module.name},
                                           {"recipe_version", std::to_string(module.version)},
                                           {"catalog_fingerprint", catalog.fingerprint},
                                           {"ddl", state["ddl"]},
                                           {"shard_count", config.shard_count}}));
    EF_RETURN_IF_ERROR(verify_reports(reports, false));
    next = state;
    next["state"] = "RUNNING";
    next["worker_reports"] = reports;
    EF_RETURN_IF_ERROR(save(std::move(next)));

    EF_RETURN_IF_ERROR(source_paused(false));
    return discovery();
  }

  StatusOr<Json> pump(std::int64_t now, std::size_t count) noexcept {
    if (phase() != "RUNNING")
      return blocked("deployment is not running");

    EF_ASSIGN_OR_RETURN(auto previous, time_field(state, "now_us"));
    if (now < previous)
      return invalid("cluster processing time cannot move backwards");

    const auto requested_now = now;
    bool clock_blocked = false;
    if (!state["pending_input"].is_null() && !state.value("pending_is_replay", false)) {
      EF_ASSIGN_OR_RETURN(auto pending_time, time_field(state, "pending_dispatch_us"));

      // Until delivery is confirmed, the original event must still be admissible
      // at its fixed time. Function progress may free capacity at that same time.
      if (now > pending_time) {
        now = pending_time;
        clock_blocked = true;
      }

      if (now < previous)
        return blocked("pending delivery cannot be overtaken by the cluster clock");
    }

    // Persist the target before fanout. Partial worker success must not allow a
    // later dispatch to rewind a shard that already advanced to this time.
    Json next = state;
    next["now_us"] = std::to_string(now);
    if (now != previous)
      EF_RETURN_IF_ERROR(save(std::move(next)));

    EF_ASSIGN_OR_RETURN(auto progress, fanout("pump", {{"deployment_version", version()},
                                                       {"now_us", std::to_string(now)},
                                                       {"max_functions", count}}));
    bool work_remaining = false;

    for (const auto& node : progress) {
      EF_ASSIGN_OR_RETURN(auto remaining, kv_protocol::member(node, "work_remaining"));
      if (!remaining->is_boolean())
        return invalid("invalid worker progress flag");

      work_remaining |= remaining->get<bool>();
    }

    EF_ASSIGN_OR_RETURN(auto reports, fanout("status", Json::object()));
    EF_RETURN_IF_ERROR(verify_reports(reports, false));
    return Json{
        {"nodes", std::move(reports)},      {"progress", std::move(progress)},
        {"now_us", std::to_string(now)},    {"requested_now_us", std::to_string(requested_now)},
        {"work_remaining", work_remaining}, {"clock_blocked_by_pending_input", clock_blocked}};
  }

  StatusOr<Json> ingest(const Json& body) noexcept {
    if (phase() != "RUNNING")
      return blocked("deployment input is paused or uninstalled");

    EF_ASSIGN_OR_RETURN(auto relation_name, kv_protocol::text(body, "relation"));
    EF_ASSIGN_OR_RETURN(auto relation, catalog.find(relation_name));
    EF_ASSIGN_OR_RETURN(auto raw_row, kv_protocol::member(body, "row"));
    EF_ASSIGN_OR_RETURN(auto row, row_value(*raw_row, relation->storage_schema.fields));
    EF_RETURN_IF_ERROR(relation->storage_schema.validate(row));
    EF_ASSIGN_OR_RETURN(auto scope, partition_values(*relation, row));
    EF_ASSIGN_OR_RETURN(auto shard, shard_for(scope, config.shard_count));
    EF_ASSIGN_OR_RETURN(auto source, kv_protocol::member(body, "source"));
    EF_ASSIGN_OR_RETURN(auto source_id, kv_protocol::text(*source, "source_id"));
    EF_ASSIGN_OR_RETURN(auto topic, kv_protocol::text(*source, "topic"));
    EF_ASSIGN_OR_RETURN(auto partition, kv_protocol::number(*source, "partition"));
    EF_ASSIGN_OR_RETURN(auto offset, time_field(*source, "offset"));
    EF_ASSIGN_OR_RETURN(auto now, time_field(body, "now_us"));
    auto event_time = now;
    if (body.contains("event_time_us")) {
      EF_ASSIGN_OR_RETURN(event_time, time_field(body, "event_time_us"));
    }

    if (source_id.empty() || source_id.size() > 128 || topic.empty() || topic.size() > 249 ||
        partition > INT32_MAX || offset == INT64_MAX)
      return invalid("invalid source identity");

    const auto source_key = Json::array({source_id, topic, partition}).dump();
    EF_ASSIGN_OR_RETURN(auto cluster_time, time_field(state, "now_us"));
    bool old_delivery = false;
    if (state["progress"].contains(source_key)) {
      EF_ASSIGN_OR_RETURN(auto next_offset,
                          time_field(state["progress"][source_key], "next_offset"));
      old_delivery = offset < next_offset;
    }

    auto dispatch_time = std::max(now, cluster_time);
    if (old_delivery) {
      const auto receipt = std::find_if(
          state["receipts"].begin(), state["receipts"].end(), [&](const Json& value) noexcept {
            return value["source"] == source_key && value["offset"] == std::to_string(offset);
          });
      if (receipt == state["receipts"].end())
        return blocked(
            "delivery precedes retained routing receipts; use checkpoint source positions");
      if ((*receipt)["submitted_us"] != std::to_string(now) ||
          (*receipt)["event_us"] != std::to_string(event_time) || (*receipt)["shard"] != shard)
        return Status(ErrorCode::Conflict,
                      "source identity changed its original time or business shard");

      EF_ASSIGN_OR_RETURN(dispatch_time, time_field(*receipt, "dispatch_us"));
    }

    if (!state["progress"].contains(source_key) && state["progress"].size() >= 64)
      return Status(ErrorCode::ResourceExhausted, "too many source partitions");
    if (!state["pending_input"].is_null() && state["pending_input"] != body)
      return Status(ErrorCode::Conflict, "resolve the original source delivery before advancing");
    if (!state["pending_input"].is_null()) {
      EF_ASSIGN_OR_RETURN(dispatch_time, time_field(state, "pending_dispatch_us"));
    }

    Json next = state;
    // Persist the delivery and assigned time before forwarding it. A retry must use this same
    // input and time even if Compute applied it but its reply was lost.
    next["pending_input"] = body;
    next["pending_is_replay"] = old_delivery;
    next["pending_dispatch_us"] = std::to_string(dispatch_time);
    next["now_us"] = std::to_string(std::max(cluster_time, dispatch_time));
    EF_RETURN_IF_ERROR(save(std::move(next)));
    const auto& node = config.nodes[routes[shard]];
    Json forwarded{{"deployment_version", version()},
                   {"shard", shard},
                   {"relation", relation_name},
                   {"row", *raw_row},
                   {"source", *source},
                   {"now_us", std::to_string(dispatch_time)}};
    auto called = call_node(node, "ingest", forwarded);
    if (!called.ok()) {
      if (called.status().code() == ErrorCode::InvalidArgument ||
          called.status().code() == ErrorCode::Conflict) {
        next = state;
        next["pending_input"] = nullptr;
        EF_RETURN_IF_ERROR(save(std::move(next)));
      }

      return called.status();
    }

    auto reply = std::move(called).value();
    // Input may be committed even when later work reports an error. Source progress follows this
    // explicit confirmation, not HTTP success or the status of the whole operation.
    const bool confirmed = reply.value("input_committed", false) || reply.value("duplicate", false);
    reply["input_committed"] = confirmed;
    reply["shard"] = shard;
    reply["node_id"] = node.node_id;
    reply["processing_time_us"] = std::to_string(dispatch_time);
    if (reply.contains("shard_status") && reply["shard_status"].contains("revision"))
      reply["revision"] = reply["shard_status"]["revision"];

    reply["operation_status"] = operation_status(reply.value("operation_status", Json::object()));
    if (!confirmed)
      return reply;

    next = state;
    next["pending_input"] = nullptr;
    if (!old_delivery) {
      if (next["receipts"].size() == 256)
        next["receipts"].erase(next["receipts"].begin());

      next["receipts"].push_back({{"source", source_key},
                                  {"offset", std::to_string(offset)},
                                  {"submitted_us", std::to_string(now)},
                                  {"event_us", std::to_string(event_time)},
                                  {"dispatch_us", std::to_string(dispatch_time)},
                                  {"shard", shard}});
    }

    std::int64_t prior_offset = 0, max_event = event_time;
    if (next["progress"].contains(source_key)) {
      EF_ASSIGN_OR_RETURN(prior_offset, time_field(next["progress"][source_key], "next_offset"));
      EF_ASSIGN_OR_RETURN(auto prior_time,
                          time_field(next["progress"][source_key], "max_event_us"));
      max_event = std::max(max_event, prior_time);
    }

    next["progress"][source_key] = {
        {"source_id", source_id},
        {"topic", topic},
        {"partition", partition},
        {"next_offset", std::to_string(std::max(prior_offset, offset + 1))},
        {"max_event_us", std::to_string(max_event)}};
    std::int64_t watermark = INT64_MAX;

    for (const auto& item : next["progress"].items()) {
      EF_ASSIGN_OR_RETURN(auto time, time_field(item.value(), "max_event_us"));
      watermark = std::min(watermark, time);
    }

    if (!state["watermark_us"].is_null()) {
      EF_ASSIGN_OR_RETURN(auto previous_watermark, decimal(state["watermark_us"]));
      reply["late"] = event_time < static_cast<std::int64_t>(previous_watermark);
      watermark = std::max(watermark, static_cast<std::int64_t>(previous_watermark));
    } else
      reply["late"] = false;
    next["watermark_us"] = std::to_string(watermark);
    EF_ASSIGN_OR_RETURN(auto previous, time_field(next, "now_us"));
    next["now_us"] = std::to_string(std::max(previous, dispatch_time));
    EF_RETURN_IF_ERROR(save(std::move(next)));

    return reply;
  }

  StatusOr<Json> finish_checkpoint(const std::string& id) noexcept {
    auto existing = store->snapshot_status(id);
    if (!existing.ok() && existing.status().code() != ErrorCode::NotFound)
      return existing.status();

    auto result = existing.ok() ? std::move(existing) : store->create_snapshot(id);
    EF_ASSIGN_OR_RETURN(auto snapshot, std::move(result));
    EF_ASSIGN_OR_RETURN(auto control_revision, decimal(state["checkpoint"]["control_revision"]));
    if (snapshot.shards.size() <= config.shard_count ||
        snapshot.shards[config.shard_count].revision != control_revision)
      return Status(ErrorCode::Conflict,
                    "snapshot control revision differs from the frozen manifest");

    for (const auto& node : state["checkpoint"]["nodes"])
      for (const auto& shard : node["shards"]) {
        EF_ASSIGN_OR_RETURN(auto number, kv_protocol::shard_field(shard));
        EF_ASSIGN_OR_RETURN(auto expected, decimal(shard["revision"]));
        EF_ASSIGN_OR_RETURN(auto receipt, kv_protocol::text(shard, "last_commit_id"));
        if (number >= config.shard_count || snapshot.shards[number].revision != expected ||
            snapshot.shards[number].last_commit_id != receipt)
          return Status(ErrorCode::Conflict, "snapshot data changed across the worker barrier");
      }
    Json next = state;
    next["checkpoint"]["complete"] = true;
    next["checkpoint"]["snapshot_id"] = snapshot.snapshot_id;
    EF_RETURN_IF_ERROR(save(std::move(next)));

    return Json(state["checkpoint"]);
  }

  StatusOr<Json> checkpoint(const Json& body) noexcept {
    EF_ASSIGN_OR_RETURN(auto id, kv_protocol::text(body, "checkpoint_id"));
    EF_RETURN_IF_ERROR(validate_snapshot_id(id));
    EF_ASSIGN_OR_RETURN(auto now, time_field(body, "now_us"));
    if (state["checkpoint"].is_object() &&
        state["checkpoint"].value("checkpoint_id", std::string{}) == id &&
        state["checkpoint"].value("complete", false))
      return Json(state["checkpoint"]);
    if (state["checkpoint"].is_object() && !state["checkpoint"].value("complete", false)) {
      if (state["checkpoint"].value("checkpoint_id", std::string{}) != id)
        return blocked("finish the original checkpoint before creating another");

      return finish_checkpoint(id);
    }

    auto prior_snapshot = store->snapshot_status(id);
    if (prior_snapshot.ok())
      return Status(ErrorCode::Conflict, "checkpoint ID already belongs to an older snapshot");
    if (prior_snapshot.status().code() != ErrorCode::NotFound)
      return prior_snapshot.status();
    if (!state["pending_input"].is_null())
      return blocked("source delivery is not confirmed");
    if (source_poller && source_poller->progress().pending &&
        !source_poller->progress().input_committed)
      return blocked("finish the retained source input before checkpoint");
    if (phase() != "RUNNING" && phase() != "PAUSED" && phase() != "QUIESCED")
      return blocked("checkpoint needs an installed deployment");

    EF_ASSIGN_OR_RETURN(auto previous_time, time_field(state, "now_us"));
    if (now < previous_time)
      return invalid("checkpoint cannot rewind cluster time");

    EF_RETURN_IF_ERROR(source_paused(true));
    Json next = state;
    next["state"] = "PAUSED";
    next["now_us"] = std::to_string(now);
    EF_RETURN_IF_ERROR(save(std::move(next)));

    // Stop workers at a confirmed boundary and verify their source positions before freezing the
    // manifest. The later KV snapshot must contain exactly these worker and control revisions.
    EF_ASSIGN_OR_RETURN(auto reports, fanout("quiesce", {{"deployment_version", version()},
                                                         {"now_us", std::to_string(now)},
                                                         {"max_functions", 4096}}));
    EF_RETURN_IF_ERROR(verify_reports(reports, true));
    EF_RETURN_IF_ERROR(verify_source_cut(reports));

    next = state;
    next["state"] = "QUIESCED";
    next["now_us"] = std::to_string(now);
    next["checkpoint"] = {{"checkpoint_id", id},
                          {"complete", false},
                          {"control_revision", std::to_string(revision + 1)},
                          {"now_us", std::to_string(now)},
                          {"nodes", reports},
                          {"source_progress", state["progress"]},
                          {"watermark_us", state["watermark_us"]},
                          {"deployment_version", version()},
                          {"catalog_fingerprint", catalog.fingerprint}};
    EF_RETURN_IF_ERROR(save(std::move(next)));

    return finish_checkpoint(id);
  }

  StatusOr<Json> handle(const std::string& action, const Json& body) noexcept {
    std::lock_guard lock(gate);
    EF_RETURN_IF_ERROR(resolve());

    if (!body.is_object())
      return invalid("Meta request must be an object");

    if (action == "ddl")
      EF_RETURN_IF_ERROR(fields(body, {"sql"}));
    else if (action == "ingest")
      EF_RETURN_IF_ERROR(fields(body, {"relation", "row", "source", "now_us"}, {"event_time_us"}));
    else if (action == "pump")
      EF_RETURN_IF_ERROR(fields(body, {"now_us"}, {"max_functions"}));
    else if (action == "query")
      EF_RETURN_IF_ERROR(fields(body, {"sql", "scope"}, {"max_rows", "max_bytes"}));
    else if (action == "request")
      EF_RETURN_IF_ERROR(fields(body, {"scope", "request_id"}));
    else if (action == "checkpoint")
      EF_RETURN_IF_ERROR(fields(body, {"checkpoint_id", "now_us"}));
    else
      EF_RETURN_IF_ERROR(fields(body, {}));

    const bool frozen =
        state["checkpoint"].is_object() && !state["checkpoint"].value("complete", false);
    if (frozen && action != "checkpoint" && action != "discover" && action != "status" &&
        action != "catalog" && action != "query" && action != "request")
      return blocked("finish the frozen checkpoint before any further control mutation");

    if (action == "discover")
      return discovery();
    if (action == "status") {
      auto result = discovery();
      if (phase() == "RUNNING" || phase() == "PAUSED" || phase() == "QUIESCED") {
        auto reports = fanout("status", Json::object());
        if (reports.ok())
          result["live_worker_reports"] = std::move(reports).value();
        else
          result["worker_error"] = kv_protocol::encode_status(reports.status());
      }

      return result;
    }

    if (action == "catalog") {
      EF_ASSIGN_OR_RETURN(auto wire, encode_catalog(catalog));
      return Json{{"catalog", std::move(wire)},
                  {"ddl", state["ddl"]},
                  {"deployment_version", version()},
                  {"recipe", module.name},
                  {"recipe_version", std::to_string(module.version)},
                  {"config", state["config"]}};
    }

    if (action == "ddl") {
      if (phase() != "STAGING" || !config.restore_checkpoint.empty())
        return blocked("core DDL is staged before deployment; online migration is not implemented");

      EF_ASSIGN_OR_RETURN(auto sql, kv_protocol::text(body, "sql"));
      if (sql.empty() || sql.size() > 65536)
        return invalid("invalid DDL length");

      Json next = state;
      bool duplicate =
          std::find(next["ddl"].begin(), next["ddl"].end(), Json(sql)) != next["ddl"].end();
      if (!duplicate)
        next["ddl"].push_back(sql);
      if (next["ddl"].size() > 64)
        return Status(ErrorCode::ResourceExhausted, "too many staged DDL statements");

      auto statements = next["ddl"].get<std::vector<std::string>>();
      EF_ASSIGN_OR_RETURN(auto definition, bind_computation_module(module, statements));
      for (const auto& relation : definition.catalog.relations)
        if (relation->kind == RelationKind::BaseTable &&
            relation->base_write_role == BaseWriteRole::PublicInput &&
            relation->partition_types != std::vector<DataType>{DataType::String})
          return invalid("core feature computation deployment uses a single string business key");

      next["catalog_fingerprint"] = definition.catalog.fingerprint;
      EF_RETURN_IF_ERROR(save(std::move(next)));
      catalog = std::move(definition.catalog);
      return Json{{"state", "STAGED"},
                  {"duplicate", duplicate},
                  {"catalog_version", std::to_string(catalog.version)}};
    }

    if (action == "deploy")
      return deploy();
    if (action == "ingest")
      return config.source ? StatusOr<Json>(blocked("automatic source owns the deployment input"))
                           : ingest(body);
    if (action == "pump") {
      EF_ASSIGN_OR_RETURN(auto now, time_field(body, "now_us"));
      std::size_t count = 4096;
      if (body.contains("max_functions")) {
        EF_ASSIGN_OR_RETURN(auto value, kv_protocol::number(body, "max_functions"));
        if (value == 0 || value > 4096)
          return invalid("invalid function progress bound");

        count = static_cast<std::size_t>(value);
      }

      return pump(now, count);
    }

    if (action == "query" || action == "request") {
      if (phase() != "RUNNING" && phase() != "PAUSED" && phase() != "QUIESCED")
        return blocked("deployment query is not ready");

      EF_ASSIGN_OR_RETURN(auto scope_json, kv_protocol::member(body, "scope"));
      EF_ASSIGN_OR_RETURN(
          auto scope,
          row_value(*scope_json,
                    {{.id = 1, .name = "ticker", .type = DataType::String, .nullable = false}}));
      if (scope.size() != 1 || !std::holds_alternative<std::string>(scope[0]))
        return invalid("core query needs one string business key");

      EF_ASSIGN_OR_RETURN(auto shard, shard_for(scope, config.shard_count));
      auto request = body;
      request["deployment_version"] = version();
      request["shard"] = shard;
      const auto& node = config.nodes[routes[shard]];
      EF_ASSIGN_OR_RETURN(auto reply, call_node(node, action, request));
      reply["shard"] = shard;
      reply["node_id"] = node.node_id;

      return reply;
    }

    if (action == "pause" || action == "resume") {
      if (action == "resume" && state["checkpoint"].is_object() &&
          !state["checkpoint"].value("complete", false))
        return blocked("finish the checkpoint before resume");
      if (phase() != "RUNNING" && phase() != "PAUSED" && phase() != "QUIESCED")
        return blocked("deployment is not installed");

      EF_RETURN_IF_ERROR(source_paused(true));
      Json next = state;
      next["state"] = "PAUSED";
      EF_RETURN_IF_ERROR(save(std::move(next)));
      EF_ASSIGN_OR_RETURN(auto replies, fanout(action, {{"deployment_version", version()}}));
      if (action == "resume")
        EF_RETURN_IF_ERROR(verify_reports(replies, false));

      next = state;
      next["state"] = action == "pause" ? "PAUSED" : "RUNNING";
      EF_RETURN_IF_ERROR(save(std::move(next)));
      EF_RETURN_IF_ERROR(source_paused(action == "pause"));
      return discovery();
    }

    if (action == "checkpoint")
      return checkpoint(body);

    return invalid("unknown Meta operation");
  }
};

CoordinatorService::CoordinatorService(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

CoordinatorService::~CoordinatorService() noexcept {
  stop();
}

StatusOr<std::unique_ptr<CoordinatorService>>
CoordinatorService::create(std::shared_ptr<IStateStore> store,
                           const CoordinatorServiceConfig& config,
                           const ComputationModule& module) noexcept {
  if (!store || config.shard_count == 0 || config.shard_count > 16 || config.nodes.empty() ||
      config.nodes.size() > 16 || config.cluster_id.empty() || config.cluster_id.size() > 128 ||
      config.owner.empty() || config.owner.size() > 128 || config.max_request_bytes == 0 ||
      config.max_response_bytes == 0 || config.tick_interval_ms > 1000)
    return invalid("invalid core Coordinator configuration");

  auto impl = std::make_unique<Impl>();
  impl->store = std::move(store);
  impl->config = config;
  impl->module = module;
  if (config.source) {
    const auto& source = *config.source;
    if (!source.source || source.relation.empty() || source.relation.size() > 256 ||
        source.delivery.source_id.empty() || source.delivery.source_id.size() > 128 ||
        source.delivery.assignments.empty() || source.delivery.assignments.size() > 64 ||
        source.delivery.max_message_bytes == 0 ||
        source.delivery.max_message_bytes > 4 * 1024 * 1024 || source.poll_interval_ms == 0 ||
        source.poll_interval_ms > 1000)
      return invalid("invalid automatic source subscription");
    std::set<std::pair<std::string, std::int32_t>> assignments;
    for (const auto& partition : source.delivery.assignments)
      if (partition.topic.empty() || partition.topic.size() > 249 || partition.partition < 0 ||
          (partition.next_offset && *partition.next_offset < 0) ||
          !assignments.emplace(partition.topic, partition.partition).second)
        return invalid("invalid automatic source partition assignment");
  }

  impl->routes.assign(config.shard_count, config.nodes.size());
  std::set<std::string> names;

  for (std::size_t i = 0; i < config.nodes.size(); ++i) {
    const auto& node = config.nodes[i];
    if (node.node_id.empty() || !names.insert(node.node_id).second ||
        !node.endpoint.starts_with("http://") || node.endpoint.size() > 512 || node.shards.empty())
      return invalid("invalid or duplicate Compute assignment");

    for (auto shard : node.shards) {
      if (shard >= config.shard_count || impl->routes[shard] != config.nodes.size())
        return invalid("Compute assignments overlap or exceed shard count");

      impl->routes[shard] = i;
    }
  }

  if (std::find(impl->routes.begin(), impl->routes.end(), config.nodes.size()) !=
      impl->routes.end())
    return invalid("Compute assignments do not cover every shard");

  Json boot_config{{"owner", config.owner},
                   {"shard_count", config.shard_count},
                   {"tick_interval_ms", config.tick_interval_ms},
                   {"recipe", module.name},
                   {"recipe_version", std::to_string(module.version)},
                   {"nodes", Json::array()}};
  for (const auto& node : config.nodes)
    boot_config["nodes"].push_back(
        {{"node_id", node.node_id}, {"endpoint", node.endpoint}, {"shards", node.shards}});

  EF_ASSIGN_OR_RETURN(impl->key,
                      row_key(config.shard_count, 'S', 0xfffd0000U, {std::string("cluster")}));

  EF_ASSIGN_OR_RETURN(auto current, impl->store->status(config.shard_count));
  impl->revision = current.revision;
  if (current.revision > 0) {
    EF_ASSIGN_OR_RETURN(auto expected,
                        make_commit_id(config.owner, config.shard_count, current.revision - 1));
    if (current.last_commit_id != expected)
      return Status(ErrorCode::Conflict, "Meta owner differs from stored receipt");
  }

  EF_ASSIGN_OR_RETURN(auto read, impl->store->get_many(config.shard_count, {impl->key}));
  if (read.revision != current.revision || read.values.size() != 1)
    return blocked("Meta read revision changed");
  if (read.values[0]) {
    EF_ASSIGN_OR_RETURN(impl->state, parse_json_exact(*read.values[0]));
    auto& state = impl->state;
    if (!state.is_object() || !state.contains("format") || !state["format"].is_number_integer() ||
        !state.contains("cluster_id") || !state["cluster_id"].is_string() ||
        !state.contains("shard_count") || !state["shard_count"].is_number_integer() ||
        state.value("format", 0) != 2 ||
        state.value("cluster_id", std::string{}) != config.cluster_id ||
        state.value("shard_count", 0U) != config.shard_count || !state.contains("state") ||
        !state["state"].is_string() || !state.contains("progress") ||
        !state["progress"].is_object() || state["progress"].size() > 64 ||
        !state.contains("pending_input") || !state.contains("checkpoint") ||
        !state.contains("receipts") || !state["receipts"].is_array() ||
        state["receipts"].size() > 256 || !state.contains("config") ||
        !state["config"].is_object() || !state.contains("worker_reports"))
      return invalid("invalid stored Meta image");

    for (const auto& receipt : state["receipts"]) {
      if (!receipt.is_object())
        return invalid("invalid routing receipt");

      EF_ASSIGN_OR_RETURN(auto source_name, kv_protocol::text(receipt, "source"));
      (void)source_name;
      for (const auto* field : {"offset", "submitted_us", "event_us", "dispatch_us"}) {
        EF_ASSIGN_OR_RETURN(auto time, time_field(receipt, field));
        (void)time;
      }

      EF_ASSIGN_OR_RETURN(auto shard, kv_protocol::shard_field(receipt));
      if (shard >= config.shard_count)
        return invalid("routing receipt shard exceeds deployment");
    }

    if (state["config"].value("recipe", Json{}) != Json(module.name) ||
        state["config"].value("recipe_version", Json{}) != Json(std::to_string(module.version)))
      return Status(ErrorCode::Conflict,
                    "stored computation module identity differs from this process");
    const auto source_description = impl->source_description();
    if (state.value("source_subscription", Json{}) != source_description) {
      if (impl->phase() != "STAGING" || !state["progress"].empty() ||
          !state["pending_input"].is_null())
        return Status(ErrorCode::Conflict, "source subscription differs from the saved deployment");
      state["source_subscription"] = source_description;
    }
    EF_ASSIGN_OR_RETURN(auto version, decimal(state["deployment_version"]));
    if (version != 1)
      return invalid("unsupported core deployment version");

    EF_ASSIGN_OR_RETURN(auto now, time_field(state, "now_us"));
    (void)now;
    EF_RETURN_IF_ERROR(impl->rebuild());
    if (!config.restore_checkpoint.empty()) {
      EF_RETURN_IF_ERROR(validate_snapshot_id(config.restore_checkpoint));
      if (impl->phase() != "QUIESCED" || !state["pending_input"].is_null() ||
          !state["checkpoint"].is_object() ||
          state["checkpoint"].value("checkpoint_id", std::string{}) != config.restore_checkpoint)
        return blocked("KV does not contain the requested frozen checkpoint image");

      EF_RETURN_IF_ERROR(impl->verify_reports(state["checkpoint"]["nodes"], true));
      EF_RETURN_IF_ERROR(impl->verify_source_cut(state["checkpoint"]["nodes"]));
      EF_ASSIGN_OR_RETURN(auto frozen_control, decimal(state["checkpoint"]["control_revision"]));
      if (impl->revision != frozen_control)
        return blocked("restore requires the actual KV snapshot, not a later live database");

      for (const auto& node : state["checkpoint"]["nodes"])
        for (const auto& shard : node["shards"]) {
          EF_ASSIGN_OR_RETURN(auto id, kv_protocol::shard_field(shard));
          EF_ASSIGN_OR_RETURN(auto expected, decimal(shard["revision"]));
          EF_ASSIGN_OR_RETURN(auto actual, impl->store->status(id));
          if (actual.revision != expected ||
              actual.last_commit_id != shard["last_commit_id"].get<std::string>())
            return blocked("restored shard differs from checkpoint manifest");
        }
      state["state"] = "STAGING";
      state["checkpoint"]["complete"] = true;
      state["checkpoint"]["snapshot_id"] = config.restore_checkpoint;

      // Endpoint changes are permitted only as part of this explicit stopped
      // restore; verify_reports above pins logical node IDs and shard ownership.
      state["config"] = boot_config;
    } else if (impl->phase() != "STAGING")
      return blocked("existing cluster needs explicit checkpoint restore");
    else {
      state["config"] = boot_config;
      EF_RETURN_IF_ERROR(impl->save(state));
    }
  } else {
    if (!config.restore_checkpoint.empty())
      return blocked("no checkpoint metadata in KV");

    impl->state = {{"format", 2},
                   {"cluster_id", config.cluster_id},
                   {"shard_count", config.shard_count},
                   {"deployment_version", "1"},
                   {"state", "STAGING"},
                   {"ddl", Json::array()},
                   {"now_us", "0"},
                   {"progress", Json::object()},
                   {"watermark_us", nullptr},
                   {"pending_input", nullptr},
                   {"pending_dispatch_us", "0"},
                   {"pending_is_replay", false},
                   {"receipts", Json::array()},
                   {"checkpoint", nullptr},
                   {"config", boot_config},
                   {"worker_reports", Json::array()}};
    if (config.source)
      impl->state["source_subscription"] = impl->source_description();
    EF_RETURN_IF_ERROR(impl->rebuild());
    impl->state["catalog_fingerprint"] = impl->catalog.fingerprint;
    EF_RETURN_IF_ERROR(impl->save(impl->state));
  }

  EF_RETURN_IF_ERROR(impl->prepare_source());
  impl->server = std::make_unique<httplib::Server>();
  impl->server->new_task_queue = []() noexcept { return new httplib::ThreadPool(4, 64); };
  impl->server->set_payload_max_length(config.max_request_bytes);
  impl->server->set_read_timeout(10);
  impl->server->set_write_timeout(30);
  auto* raw = impl.get();
  impl->server->Get("/health", [raw](const httplib::Request&,
                                     httplib::Response& response) noexcept {
    std::lock_guard lock(raw->gate);
    response.set_content(
        Json{{"alive", true}, {"ready", raw->phase() == "RUNNING"}, {"state", raw->phase()}}.dump(),
        "application/json");
  });
  impl->server->Post(R"(/v1/meta/(\w+))", [raw](const httplib::Request& request,
                                                httplib::Response& response) noexcept {
    auto body = parse_json_exact(request.body);
    auto result =
        body.ok() ? raw->handle(request.matches[1].str(), *body) : StatusOr<Json>(body.status());
    if (result.ok()) {
      auto text = result->dump();
      if (text.size() <= raw->config.max_response_bytes) {
        response.set_content(std::move(text), "application/json");
        return;
      }

      result = Status(ErrorCode::ResourceExhausted, "Meta response exceeds configured capacity");
    }

    const auto status = result.status();
    response.status = status.code() == ErrorCode::Unavailable ||
                              status.code() == ErrorCode::UnknownCommit ||
                              status.code() == ErrorCode::Internal
                          ? 503
                          : 400;
    response.set_content(kv_protocol::encode_status(status).dump(), "application/json");
  });

  return std::unique_ptr<CoordinatorService>(new CoordinatorService(std::move(impl)));
}

StatusOr<int> CoordinatorService::start(const std::string& host, int port) noexcept {
  if (impl_->started || port < 0 || port > 65535)
    return invalid("invalid Coordinator start");

  const int bound = port == 0 ? impl_->server->bind_to_any_port(host)
                              : (impl_->server->bind_to_port(host, port) ? port : -1);
  if (bound <= 0)
    return Status(ErrorCode::Unavailable, "Coordinator listen failed");

  impl_->started = true;
  impl_->listener = std::thread([this]() noexcept { impl_->server->listen_after_bind(); });
  impl_->server->wait_until_ready();
  impl_->stopping.store(false);
  if (impl_->config.tick_interval_ms != 0 || impl_->config.source)
    impl_->ticker = std::thread([this]() noexcept {
      const auto source_interval =
          impl_->config.source ? impl_->config.source->poll_interval_ms : 1000U;
      const auto tick_interval =
          impl_->config.tick_interval_ms == 0 ? source_interval : impl_->config.tick_interval_ms;
      const auto interval = std::min(source_interval, tick_interval);
      auto sampled = SystemClock::create();
      if (!sampled.ok()) {
        std::lock_guard lock(impl_->gate);
        impl_->background_status = sampled.status();
        return;
      }
      auto clock = std::move(sampled).value();
      auto next_source_poll = std::chrono::steady_clock::now();
      while (!impl_->stopping.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(interval));
        std::lock_guard lock(impl_->gate);
        if (impl_->phase() != "RUNNING")
          continue;
        const auto resolved = impl_->resolve();
        if (!resolved.ok()) {
          impl_->background_status = resolved;
          continue;
        }
        auto now = time_field(impl_->state, "now_us");
        auto sampled_now = clock.now_us();
        if (!now.ok() || !sampled_now.ok()) {
          impl_->background_status = now.ok() ? sampled_now.status() : now.status();
          continue;
        }
        const auto wall = std::chrono::steady_clock::now();
        if (impl_->source_poller && wall >= next_source_poll) {
          auto progress = impl_->source_poller->step(std::max(*now, *sampled_now));
          impl_->background_status = progress.ok() ? progress->operation_status : progress.status();
          next_source_poll = wall + std::chrono::milliseconds(source_interval);
        }
        // Function completions must make progress even while no new message is available.
        // A retained unconfirmed input prevents the clock from overtaking its fixed time.
        now = time_field(impl_->state, "now_us");
        if (!now.ok()) {
          impl_->background_status = now.status();
          continue;
        }
        auto advance_to = impl_->config.tick_interval_ms ? std::max(*now, *sampled_now) : *now;
        if (impl_->source_poller) {
          const auto& source = impl_->source_poller->progress();
          if (source.pending && !source.input_committed && source.pending_time_us)
            advance_to = std::min(advance_to, *source.pending_time_us);
        }
        if (advance_to < *now)
          continue;
        auto progress = impl_->pump(advance_to, 4096);
        if (!progress.ok())
          impl_->background_status = progress.status();
      }
    });

  return int(bound);
}

bool CoordinatorService::is_running() const noexcept {
  return impl_->server->is_running();
}

void CoordinatorService::stop() noexcept {
  if (!impl_ || !impl_->started)
    return;

  impl_->stopping.store(true);
  if (impl_->ticker.joinable())
    impl_->ticker.join();

  impl_->server->stop();
  if (impl_->listener.joinable())
    impl_->listener.join();

  impl_->started = false;
}
} // namespace eventflow
