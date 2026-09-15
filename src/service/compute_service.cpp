#include "eventflow/compute_service.hpp"
#include "eventflow/cached_store.hpp"
#include "eventflow/computation_runtime.hpp"
#include "eventflow/key_codec.hpp"
#include "eventflow/kv_protocol.hpp"
#include "eventflow/submitted_computation.hpp"
#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <httplib.h>
#include <map>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <thread>

namespace eventflow {
namespace {
enum class RunState { Uninstalled, Installing, Running, Paused, Quiesced };

const char* state_name(RunState state) noexcept {
  switch (state) {
  case RunState::Uninstalled:
    return "UNINSTALLED";
  case RunState::Installing:
    return "INSTALLING";
  case RunState::Running:
    return "RUNNING";
  case RunState::Paused:
    return "PAUSED";
  case RunState::Quiesced:
    return "QUIESCED";
  }

  return "UNINSTALLED";
}

Status invalid(std::string_view message) noexcept {
  return {ErrorCode::InvalidArgument, message};
}

Status blocked(std::string_view message) noexcept {
  return {ErrorCode::FailedPrecondition, message};
}

Status full(std::string_view message) noexcept {
  return {ErrorCode::ResourceExhausted, message};
}

bool text_ok(std::string_view value, std::size_t limit = 128) noexcept {
  return !value.empty() && value.size() <= limit && valid_utf8(value);
}

Status fields(const Json& json, std::initializer_list<const char*> required,
              std::initializer_list<const char*> optional = {}) noexcept {
  if (!json.is_object())
    return invalid("Compute request must be a JSON object");

  for (const auto* name : required)
    if (!json.contains(name))
      return invalid("Compute request is missing a field");

  for (const auto& item : json.items()) {
    bool known = false;
    for (const auto* name : required)
      known |= item.key() == name;

    for (const auto* name : optional)
      known |= item.key() == name;

    if (!known)
      return invalid("Compute request has an unknown field");
  }

  return {};
}

StatusOr<std::uint64_t> decimal(const Json& json) noexcept {
  if (!json.is_string())
    return invalid("64-bit Compute fields require canonical decimal strings");

  const auto& text = json.get_ref<const std::string&>();
  std::uint64_t value = 0;
  if (text.empty() || text.size() > 20 || (text.size() > 1 && text[0] == '0'))
    return invalid("invalid canonical decimal integer");

  const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc{} || end != text.data() + text.size())
    return invalid("invalid canonical decimal integer");

  return value;
}

StatusOr<std::int64_t> time_value(const Json& json) noexcept {
  EF_ASSIGN_OR_RETURN(auto value, decimal(json));
  if (value > INT64_MAX)
    return invalid("Compute time or offset exceeds Int64");

  return static_cast<std::int64_t>(value);
}

StatusOr<std::size_t> count_value(const Json& json, std::size_t limit) noexcept {
  EF_ASSIGN_OR_RETURN(auto value, kv_protocol::unsigned_number(json));
  if (value == 0 || value > limit)
    return invalid("Compute count is outside its configured bound");

  return static_cast<std::size_t>(value);
}

StatusOr<Json> request_json(std::string_view bytes) noexcept {
  // Reject deep containers before either parser allocates its traversal stack.
  std::size_t depth = 0;
  bool quoted = false, escaped = false;
  for (auto c : bytes) {
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
      if (++depth > 32)
        return invalid("Compute JSON nesting exceeds bound");
    } else if (c == ']' || c == '}') {
      if (depth == 0)
        return invalid("unbalanced Compute JSON");

      --depth;
    }
  }

  EF_ASSIGN_OR_RETURN(auto exact, parse_json_exact(bytes));
  bool duplicate = false;
  std::array<std::set<std::string>, 32> keys;
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
  if (checked.is_discarded() || duplicate)
    return invalid("duplicate or malformed Compute JSON fields");

  return exact;
}

StatusOr<Row> typed_row(const Json& values, const std::vector<Field>& schema) noexcept {
  if (!values.is_array() || values.size() != schema.size())
    return invalid("Compute row does not match Catalog arity");

  Row result;
  for (std::size_t i = 0; i < schema.size(); ++i) {
    EF_ASSIGN_OR_RETURN(auto value, value_from_json(values[i], schema[i].type, schema[i].nullable));
    result.push_back(std::move(value));
  }

  return result;
}

StatusOr<Row> scope_row(const Json& values) noexcept {
  if (!values.is_array() || values.empty() || values.size() > 64)
    return invalid("invalid Compute business scope");

  Row result;
  for (const auto& value : values) {
    const auto type = value.is_string()    ? DataType::String
                      : value.is_boolean() ? DataType::Bool
                                           : DataType::Int64;
    EF_ASSIGN_OR_RETURN(auto decoded, value_from_json(value, type, false));
    result.push_back(std::move(decoded));
  }

  return result;
}

StatusOr<SourceIdentity> source_identity(const Json& json) noexcept {
  EF_RETURN_IF_ERROR(fields(json, {"source_id", "topic", "partition", "offset"}));
  SourceIdentity source;
  EF_ASSIGN_OR_RETURN(source.source_id, kv_protocol::text(json, "source_id"));
  EF_ASSIGN_OR_RETURN(source.topic, kv_protocol::text(json, "topic"));
  EF_ASSIGN_OR_RETURN(auto partition, kv_protocol::number(json, "partition"));
  EF_ASSIGN_OR_RETURN(source.offset, time_value(json["offset"]));
  if (!text_ok(source.source_id) || !text_ok(source.topic, 249) || partition > INT32_MAX ||
      source.offset == INT64_MAX)
    return invalid("invalid bounded Compute source identity");

  source.partition = static_cast<std::int32_t>(partition);
  return source;
}

int http_status(const Status& status) noexcept {
  switch (status.code()) {
  case ErrorCode::InvalidArgument:
    return 400;
  case ErrorCode::NotFound:
    return 404;
  case ErrorCode::Conflict:
  case ErrorCode::FailedPrecondition:
    return 409;
  case ErrorCode::ResourceExhausted:
    return 429;
  default:
    return 503;
  }
}

Json progress_status(const Status& status) noexcept {
  return Json{{"code", static_cast<int>(status.code())},
              {"message", valid_utf8(status.message()) ? std::string(status.message())
                                                       : "non-UTF-8 error detail"}};
}
} // namespace

struct ComputeService::Impl {
  struct Shard {
    std::mutex mutex;
    std::shared_ptr<CachedStateStore> cache;
    std::unique_ptr<ComputationRuntime> runtime;
  };

  std::shared_ptr<IStateStore> store;
  ComputeServiceConfig config;
  ComputationModule module;
  std::map<std::uint32_t, std::unique_ptr<Shard>> shards;
  std::shared_mutex gate;
  RunState state = RunState::Uninstalled;
  std::uint64_t deployment_version = 0;
  std::string fingerprint;
  std::vector<std::string> ddl;
  std::uint64_t catalog_version = 0;
  std::unique_ptr<httplib::Server> server;
  std::mutex lifecycle;
  std::thread listener;
  bool started = false;

  Status version(const Json& body) const noexcept {
    EF_ASSIGN_OR_RETURN(auto requested, decimal(body["deployment_version"]));
    if (requested == 0 || requested != deployment_version)
      return {ErrorCode::Conflict, "Compute deployment version mismatch"};
    if (state == RunState::Uninstalled || state == RunState::Installing)
      return blocked("Compute deployment is not fully installed");

    return {};
  }

  StatusOr<Shard*> selected(const Json& body) noexcept {
    EF_ASSIGN_OR_RETURN(auto id, kv_protocol::shard_field(body));
    const auto found = shards.find(id);
    if (found == shards.end())
      return Status(ErrorCode::Conflict, "Compute does not own the requested shard");

    return found->second.get();
  }

  Status scope_matches(const Row& scope, const Json& body) const noexcept {
    EF_ASSIGN_OR_RETURN(auto expected, shard_for(scope, config.shard_count));
    EF_ASSIGN_OR_RETURN(auto requested, kv_protocol::shard_field(body));
    if (expected != requested)
      return {ErrorCode::Conflict, "business scope does not belong to requested shard"};

    return {};
  }

  // Caller holds the shard mutex or the exclusive admission gate.
  StatusOr<Json> shard_report(std::uint32_t id, Shard& shard) noexcept {
    EF_ASSIGN_OR_RETURN(auto stored, store->status(id));
    Json result{{"shard", id},
                {"owner", config.owner},
                {"installed", bool(shard.runtime)},
                {"revision", std::to_string(stored.revision)},
                {"last_commit_id", stored.last_commit_id}};
    if (!shard.runtime)
      return result;

    auto& processor = shard.runtime->processor();
    result["processing_time_us"] = std::to_string(processor.processing_time_us());
    result["pending_commit"] = processor.has_pending_commit();
    result["failed"] = processor.failed();

    const auto executor = shard.runtime->executor_metrics();
    result["executor_active_tasks"] = executor.active_tasks;
    result["executor"] = {{"mode", shard.runtime->executor_mode()},
                          {"worker_count", shard.runtime->executor_worker_count()},
                          {"callback_calls", std::to_string(executor.callback_calls)},
                          {"submitted", std::to_string(executor.submitted)},
                          {"completed_tasks", executor.completed_tasks}};

    const auto& effective = processor.engine_config();
    result["effective_engine_config"] = {{"max_bytes", effective.max_bytes},
                                         {"max_rows", effective.max_rows},
                                         {"max_mutations", effective.max_mutations},
                                         {"page_rows", effective.page_rows},
                                         {"shard_count", effective.shard_count}};

    const auto& runtime = shard.runtime->definition().definition;
    result["effective_runtime_config"] = {
        {"max_keys", runtime.max_keys},
        {"max_timers", runtime.max_timers},
        {"max_state_bytes", runtime.max_state_bytes},
        {"max_occurrences", runtime.max_occurrences},
        {"max_event_bytes", runtime.max_event_bytes},
        {"max_request_records",
         runtime.requests ? runtime.requests->runtime.max_active_requests : 0},
        {"max_request_record_bytes",
         runtime.requests ? runtime.requests->runtime.max_record_bytes : 0},
        {"max_request_index_bytes",
         runtime.requests ? runtime.requests->runtime.max_index_bytes : 0}};

    if (shard.cache) {
      const auto metrics = shard.cache->metrics();
      result["cache"] = {{"hits", std::to_string(metrics.hits)},
                         {"misses", std::to_string(metrics.misses)},
                         {"bytes", metrics.bytes},
                         {"entries", metrics.entries},
                         {"pending_bytes", metrics.pending_bytes}};
    }

    result["source_positions"] = Json::array();
    if (processor.has_pending_commit() || processor.failed())
      return result;

    EF_ASSIGN_OR_RETURN(auto positions, processor.processed_positions());
    for (const auto& position : positions)
      result["source_positions"].push_back({{"source_id", position.source_id},
                                            {"topic", position.topic},
                                            {"partition", position.partition},
                                            {"next_offset", std::to_string(position.next_offset)}});

    std::vector<RequestRecord> active;
    if (runtime.requests) {
      EF_ASSIGN_OR_RETURN(active, processor.active_requests());
    }
    std::size_t pending = 0, ready = 0;
    for (const auto& record : active)
      for (const auto& node : record.state.nodes) {
        pending += node.phase == RequestNodePhase::Running;
        ready += node.phase == RequestNodePhase::Ready;
      }
    result["active_requests"] = active.size();
    result["pending_tasks"] = pending;
    result["ready_tasks"] = ready;
    EF_ASSIGN_OR_RETURN(
        auto occurrences,
        processor.pending_occurrences(shard.runtime->definition().definition.max_occurrences));
    result["pending_occurrences"] = occurrences.size();

    return result;
  }

  StatusOr<Json> report() noexcept {
    Json result{{"node_id", config.node_id},
                {"deployment_version", std::to_string(deployment_version)},
                {"recipe", module.name},
                {"catalog_fingerprint", fingerprint},
                {"catalog_version", std::to_string(catalog_version)},
                {"run_state", state_name(state)},
                {"shard_count", config.shard_count},
                {"owner", config.owner},
                {"shards", Json::array()}};
    for (auto& [id, shard] : shards) {
      EF_ASSIGN_OR_RETURN(auto item, shard_report(id, *shard));
      result["shards"].push_back(std::move(item));
    }

    return result;
  }

  StatusOr<Json> install(const Json& body) noexcept {
    EF_RETURN_IF_ERROR(fields(body, {"deployment_version", "recipe", "catalog_fingerprint"},
                              {"ddl", "shard_count", "recipe_version", "submitted", "udfs"}));
    if (body.contains("shard_count")) {
      EF_ASSIGN_OR_RETURN(auto requested_shards, kv_protocol::number(body, "shard_count"));
      if (requested_shards != config.shard_count)
        return Status(ErrorCode::Conflict,
                      "Compute shard count differs from the requested deployment");
    }

    EF_ASSIGN_OR_RETURN(auto requested, decimal(body["deployment_version"]));
    EF_ASSIGN_OR_RETURN(auto recipe, kv_protocol::text(body, "recipe"));
    EF_ASSIGN_OR_RETURN(auto expected, kv_protocol::text(body, "catalog_fingerprint"));
    if (requested == 0 || recipe != module.name)
      return invalid("Compute computation module differs from the deployment");
    std::uint64_t module_version = 1;
    if (body.contains("recipe_version")) {
      EF_ASSIGN_OR_RETURN(module_version, decimal(body["recipe_version"]));
    }
    if (module_version != module.version)
      return invalid("Compute computation module version differs from the deployment");

    std::vector<std::string> statements;
    if (body.contains("ddl")) {
      if (!body["ddl"].is_array() || body["ddl"].size() > 64)
        return invalid("Compute DDL list exceeds bound");

      for (const auto& value : body["ddl"]) {
        if (!value.is_string() || !text_ok(value.get_ref<const std::string&>(), 64 * 1024))
          return invalid("invalid Compute DDL statement");

        statements.push_back(value.get<std::string>());
      }
    }

    const auto submitted = body.value("submitted", Json{});
    const auto registered = body.value("udfs", Json::array());
    if (!submitted.is_null() && (!config.allow_native_udfs || module.bind || module.name != "sql"))
      return blocked("this Compute does not enable submitted native computations");
    if (!submitted.is_null()) {
      EF_ASSIGN_OR_RETURN(auto decoded, decode_computation(submitted));
      if (decoded.ddl != statements)
        return Status(ErrorCode::Conflict, "installed DDL differs from submitted computation");
    }
    EF_ASSIGN_OR_RETURN(auto definition,
                        submitted.is_null() ? bind_computation_module(module, statements)
                                            : load_submitted_computation(
                                                  *store, config.shard_count, submitted, registered,
                                                  {.shard_count = config.shard_count}));
    if (expected != definition.catalog.fingerprint)
      return Status(ErrorCode::Conflict,
                    "Compute Catalog fingerprint differs from deployment image");

    // Every admitted source partition must still fit a future checkpoint report.
    const auto per_shard =
        2048 + definition.definition.max_source_partitions * (6 * (128 + 249) + 256);
    const auto fixed = Json(expected).dump().size() + 4096;
    if (fixed > config.max_response_bytes ||
        shards.size() > (config.max_response_bytes - fixed) / per_shard)
      return full("Compute response budget cannot report the installed source-position envelope");
    if (deployment_version != 0 && (requested != deployment_version || expected != fingerprint))
      return Status(ErrorCode::Conflict, "Compute cannot replace its installed immutable image");
    if (state != RunState::Uninstalled && state != RunState::Installing)
      return report();

    deployment_version = requested;
    fingerprint = std::move(expected);
    ddl = std::move(statements);
    catalog_version = definition.catalog.version;
    state = RunState::Installing;

    for (auto& [id, shard] : shards) {
      if (shard->runtime)
        continue;

      EF_ASSIGN_OR_RETURN(auto receipt, store->status(id));
      if (receipt.revision != 0) {
        EF_ASSIGN_OR_RETURN(auto expected_receipt,
                            make_commit_id(config.owner, id, receipt.revision - 1));
        if (receipt.last_commit_id != expected_receipt)
          return Status(ErrorCode::Conflict,
                        "Compute persisted shard receipt differs from its configured owner");
      }

      EngineConfig engine;
      engine.shard = id;
      engine.shard_count = config.shard_count;
      engine.owner = config.owner;
      engine.max_rows = 20000;

      // CachedStateStore is deliberately per serialized shard owner. The evictable
      // node cache budget is divided across shards; pending commit identity is
      // separately sized from actual transport/storage capacity and cannot evict.
      CacheConfig cache_config;
      cache_config.owner_epoch = deployment_version;
      cache_config.max_entries = std::max<std::size_t>(1, 65536 / shards.size());
      cache_config.max_bytes = 16 * 1024 * 1024 / shards.size();
      EF_ASSIGN_OR_RETURN(auto limits, store->limits());
      cache_config.max_pending_bytes = limits.max_batch_bytes;
      EF_ASSIGN_OR_RETURN(auto cache, CachedStateStore::open(store, cache_config));
      shard->cache = std::move(cache);
      EF_ASSIGN_OR_RETURN(auto bound, submitted.is_null()
                                          ? bind_computation_module(module, ddl)
                                          : load_submitted_computation(
                                                *store, config.shard_count, submitted, registered,
                                                {.shard_count = config.shard_count}));
      EF_ASSIGN_OR_RETURN(shard->runtime,
                          ComputationRuntime::open(shard->cache, std::move(bound), engine));
    }

    state = RunState::Running;
    return report();
  }

  StatusOr<Json> ingest(const Json& body) noexcept {
    EF_RETURN_IF_ERROR(
        fields(body, {"deployment_version", "shard", "relation", "row", "source", "now_us"}));
    EF_RETURN_IF_ERROR(version(body));
    if (state != RunState::Running)
      return blocked("Compute input admission is paused");

    EF_ASSIGN_OR_RETURN(auto shard, selected(body));
    std::lock_guard lock(shard->mutex);
    EF_ASSIGN_OR_RETURN(auto relation, kv_protocol::text(body, "relation"));
    EF_ASSIGN_OR_RETURN(auto descriptor, shard->runtime->definition().catalog.find(relation));
    EF_ASSIGN_OR_RETURN(auto row, typed_row(body["row"], descriptor->storage_schema.fields));
    EF_ASSIGN_OR_RETURN(auto scope, partition_values(*descriptor, row));
    EF_RETURN_IF_ERROR(scope_matches(scope, body));
    EF_ASSIGN_OR_RETURN(auto source, source_identity(body["source"]));
    EF_ASSIGN_OR_RETURN(auto now, time_value(body["now_us"]));
    EF_ASSIGN_OR_RETURN(auto result,
                        shard->runtime->ingest_source(relation, std::move(row), source, now));
    Json response{{"input_committed", result.input_committed},
                  {"duplicate", result.duplicate},
                  {"operation_status", progress_status(result.operation_status)}};
    EF_ASSIGN_OR_RETURN(auto id, kv_protocol::shard_field(body));

    // Do not turn an already confirmed input into a false negative merely because
    // subsequent owner work cannot yet provide a complete progress report.
    auto progress = shard_report(id, *shard);
    if (progress.ok())
      response["shard_status"] = std::move(progress).value();
    else
      response["progress_error"] = progress_status(progress.status());
    return response;
  }

  StatusOr<Json> query(const Json& body) noexcept {
    EF_RETURN_IF_ERROR(
        fields(body, {"deployment_version", "shard", "scope", "sql"}, {"max_rows", "max_bytes"}));
    EF_RETURN_IF_ERROR(version(body));
    EF_ASSIGN_OR_RETURN(auto shard, selected(body));
    EF_ASSIGN_OR_RETURN(auto scope, scope_row(body["scope"]));
    EF_RETURN_IF_ERROR(scope_matches(scope, body));
    EF_ASSIGN_OR_RETURN(auto sql, kv_protocol::text(body, "sql"));
    if (!text_ok(sql, 64 * 1024))
      return invalid("Compute SQL exceeds its query bound");

    QueryContext context;
    context.scope = std::move(scope);
    context.max_bytes = std::min<std::size_t>(context.max_bytes, config.max_response_bytes / 4);
    if (body.contains("max_rows")) {
      EF_ASSIGN_OR_RETURN(context.max_rows, count_value(body["max_rows"], 20000));
    }

    if (body.contains("max_bytes")) {
      EF_ASSIGN_OR_RETURN(context.max_bytes,
                          count_value(body["max_bytes"], config.max_response_bytes));
    }

    std::lock_guard lock(shard->mutex);
    EF_ASSIGN_OR_RETURN(auto result, shard->runtime->processor().query_sql(sql, context));
    Json response{{"fields", Json::array()},
                  {"rows", Json::array()},
                  {"revision", std::to_string(result.revision)},
                  {"catalog_version", std::to_string(result.catalog_version)},
                  {"deployment_version", std::to_string(deployment_version)}};
    for (const auto& field : result.fields)
      response["fields"].push_back({{"id", field.id},
                                    {"name", field.name},
                                    {"type", data_type_name(field.type)},
                                    {"nullable", field.nullable}});

    for (const auto& row : result.rows) {
      Json values = Json::array();
      for (const auto& value : row) {
        EF_ASSIGN_OR_RETURN(auto encoded, value_to_json(value));
        values.push_back(std::move(encoded));
      }

      response["rows"].push_back(std::move(values));
    }

    return response;
  }

  StatusOr<Json> request_audit(const Json& body) noexcept {
    EF_RETURN_IF_ERROR(fields(body, {"deployment_version", "shard", "scope", "request_id"}));
    EF_RETURN_IF_ERROR(version(body));
    EF_ASSIGN_OR_RETURN(auto shard, selected(body));
    EF_ASSIGN_OR_RETURN(auto scope, scope_row(body["scope"]));
    EF_RETURN_IF_ERROR(scope_matches(scope, body));
    EF_ASSIGN_OR_RETURN(auto id, kv_protocol::text(body, "request_id"));
    if (!text_ok(id, 4096))
      return invalid("invalid request identity");

    std::lock_guard lock(shard->mutex);
    EF_ASSIGN_OR_RETURN(auto record,
                        shard->runtime->read_request({.scope = scope, .request_id = id}));
    const auto& manifest = record.state.manifest;
    const char* phase = record.state.phase == RequestPhase::Success     ? "SUCCEEDED"
                        : record.state.phase == RequestPhase::Failed    ? "FAILED"
                        : record.state.phase == RequestPhase::Cancelled ? "CANCELLED"
                        : record.state.phase == RequestPhase::Running   ? "RUNNING"
                                                                        : "ACCEPTED";
    return Json{{"scope", body["scope"]},
                {"request_id", record.admission.request_id},
                {"plan", record.admission.plan},
                {"phase", phase},
                {"candidate_epoch", std::to_string(manifest.candidate_epoch)},
                {"capture_revision", std::to_string(manifest.capture_revision)},
                {"admission_revision", std::to_string(manifest.admission_revision)},
                {"admission_seq", std::to_string(manifest.admission_seq)},
                {"admitted_at_us", std::to_string(manifest.admitted_at_us)},
                {"deadline_us", std::to_string(manifest.deadline_us)},
                {"last_transition_us", std::to_string(record.state.now_us)},
                {"owner", manifest.owner_token},
                {"owner_epoch", std::to_string(manifest.owner_epoch)},
                {"catalog_fingerprint", manifest.catalog_fingerprint},
                {"deployment_version", std::to_string(deployment_version)}};
  }

  StatusOr<Json> pump(const Json& body, bool quiesce) noexcept {
    EF_RETURN_IF_ERROR(fields(body, {"deployment_version", "now_us"}, {"max_functions"}));
    EF_RETURN_IF_ERROR(version(body));
    if (state == RunState::Quiesced) {
      if (quiesce)
        return report();

      return blocked("quiesced Compute requires resume before pumping");
    }

    EF_ASSIGN_OR_RETURN(auto now, time_value(body["now_us"]));
    std::size_t limit = std::min<std::size_t>(128, config.max_functions_per_call);
    if (body.contains("max_functions")) {
      EF_ASSIGN_OR_RETURN(limit, count_value(body["max_functions"], config.max_functions_per_call));
    }

    if (quiesce)
      state = RunState::Paused;

    std::size_t completed = 0;
    bool remaining = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    do {
      remaining = false;
      for (auto& [id, shard] : shards) {
        (void)id;
        std::lock_guard lock(shard->mutex);
        const auto at = std::max(now, shard->runtime->processor().processing_time_us());
        if (completed < limit) {
          EF_ASSIGN_OR_RETURN(auto count, shard->runtime->run_functions(at, limit - completed));
          completed += count;
        } else {
          EF_RETURN_IF_ERROR(shard->runtime->pump(at));
        }

        auto& processor = shard->runtime->processor();
        remaining |= processor.has_pending_commit() || processor.failed() ||
                     shard->runtime->executor_metrics().active_tasks != 0;
        if (shard->runtime->definition().definition.requests && !processor.has_pending_commit() &&
            !processor.failed()) {
          EF_ASSIGN_OR_RETURN(auto active, processor.active_requests());
          for (const auto& record : active)
            for (const auto& node : record.state.nodes)
              remaining |=
                  node.phase == RequestNodePhase::Running || node.phase == RequestNodePhase::Ready;
        }
      }

      if (!remaining || completed >= limit || std::chrono::steady_clock::now() >= deadline)
        break;

      // A waiting callback owns a worker thread only. Ordinary queries can take
      // the released shard mutex between these bounded owner polling slices.
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    } while (true);
    if (quiesce && !remaining)
      state = RunState::Quiesced;

    Json response{{"completed_functions", completed},
                  {"work_remaining", remaining},
                  {"run_state", state_name(state)},
                  {"deployment_version", std::to_string(deployment_version)}};
    if (quiesce) {
      EF_ASSIGN_OR_RETURN(response, report());
      response["completed_functions"] = completed;
      response["work_remaining"] = remaining;
    }

    return response;
  }

  StatusOr<Json> control(const std::string& operation, const Json& body) noexcept {
    if (operation == "status") {
      EF_RETURN_IF_ERROR(fields(body, {}));
      return report();
    }

    if (operation == "install")
      return install(body);
    if (operation == "quiesce")
      return pump(body, true);
    if (operation == "resume")
      EF_RETURN_IF_ERROR(fields(body, {"deployment_version"}, {"now_us", "max_functions"}));
    else
      EF_RETURN_IF_ERROR(fields(body, {"deployment_version"}));
    EF_RETURN_IF_ERROR(version(body));
    if (operation == "pause") {
      if (state != RunState::Quiesced)
        state = RunState::Paused;
    } else if (operation == "resume") {
      if (body.contains("max_functions") && !body.contains("now_us"))
        return invalid("Compute resume function budget requires an explicit time");
      if (body.contains("now_us")) {
        EF_ASSIGN_OR_RETURN(auto now, time_value(body["now_us"]));
        (void)now;
        if (body.contains("max_functions")) {
          EF_ASSIGN_OR_RETURN(auto limit,
                              count_value(body["max_functions"], config.max_functions_per_call));
          (void)limit;
        }
      }

      for (const auto& [id, shard] : shards) {
        (void)id;
        if (shard->runtime->processor().has_pending_commit() ||
            shard->runtime->processor().failed())
          return blocked("Compute cannot resume an unconfirmed or failed shard");
      }

      if (body.contains("now_us")) {
        state = RunState::Paused;
        EF_ASSIGN_OR_RETURN(auto advanced, pump(body, false));
        (void)advanced;
      }

      state = RunState::Running;
    } else if (operation == "checkpoint") {
      if (state != RunState::Quiesced)
        return blocked("Compute checkpoint report requires a quiesced owner");

      for (const auto& [id, shard] : shards) {
        (void)id;
        if (shard->runtime->processor().has_pending_commit() ||
            shard->runtime->processor().failed() ||
            shard->runtime->executor_metrics().active_tasks != 0)
          return blocked("Compute checkpoint has unconfirmed or volatile task work");
      }

      EF_ASSIGN_OR_RETURN(auto response, report());
      response["checkpoint_ready"] = true;
      return response;
    }

    return report();
  }

  void reply(httplib::Response& response, StatusOr<Json> result) noexcept {
    if (!result.ok()) {
      response.status = http_status(result.status());
      response.set_content(kv_protocol::encode_status(result.status()).dump(), "application/json");
      return;
    }

    auto body = result->dump();
    if (body.size() > config.max_response_bytes) {
      const auto status = full("Compute response exceeds configured byte bound");
      response.status = http_status(status);
      response.set_content(kv_protocol::encode_status(status).dump(), "application/json");
      return;
    }

    response.set_content(body, "application/json");
  }

  void init() noexcept {
    server = std::make_unique<httplib::Server>();
    server->new_task_queue = [this]() noexcept {
      return new httplib::ThreadPool(config.worker_threads, config.max_queued_requests);
    };
    server->set_payload_max_length(config.max_request_bytes);
    server->set_tcp_nodelay(true);
    server->set_pre_routing_handler([this](const httplib::Request& request,
                                           httplib::Response& response) noexcept {
      if (request.method == "GET" && request.path == "/health")
        return httplib::Server::HandlerResponse::Unhandled;

      Status error;
      if (request.method != "POST" || !request.path.starts_with("/v1/compute/"))
        error = invalid("unsupported Compute method or path");
      else if (request.has_header("Transfer-Encoding") ||
               request.get_header_value_count("Content-Length") != 1 ||
               request.get_header_value_count("Content-Encoding") > 1 ||
               (request.has_header("Content-Encoding") &&
                request.get_header_value("Content-Encoding") != "identity"))
        error = invalid("Compute requires one unencoded Content-Length frame");
      else {
        const auto text = request.get_header_value("Content-Length");
        std::size_t bytes = 0;
        const auto [end, parsed] = std::from_chars(text.data(), text.data() + text.size(), bytes);
        if (parsed != std::errc{} || end != text.data() + text.size())
          error = invalid("invalid Compute Content-Length");
        else if (bytes > config.max_request_bytes)
          error = full("Compute request exceeds configured byte bound");
      }

      if (error.ok())
        return httplib::Server::HandlerResponse::Unhandled;

      reply(response, error);
      response.set_header("Connection", "close");
      return httplib::Server::HandlerResponse::Handled;
    });
    server->Get("/health", [this](const httplib::Request&, httplib::Response& response) noexcept {
      std::shared_lock lock(gate);
      reply(response, Json{{"status", state == RunState::Running ? "ready" : "alive"},
                           {"ready", state == RunState::Running},
                           {"service", "eventflow-compute"},
                           {"node_id", config.node_id},
                           {"run_state", state_name(state)},
                           {"deployment_version", std::to_string(deployment_version)}});
    });

    for (const auto* name : {"install", "ingest", "pump", "query", "request", "pause", "resume",
                             "quiesce", "status", "checkpoint"}) {
      const std::string operation = name;
      server->Post(
          "/v1/compute/" + operation,
          [this, operation](const httplib::Request& request, httplib::Response& response) noexcept {
            auto body = request_json(request.body);
            if (!body.ok()) {
              reply(response, body.status());
              return;
            }

            if (operation == "ingest" || operation == "query" || operation == "request" ||
                operation == "pump") {
              std::shared_lock lock(gate);
              auto result = operation == "ingest"    ? ingest(*body)
                            : operation == "query"   ? query(*body)
                            : operation == "request" ? request_audit(*body)
                                                     : pump(*body, false);
              reply(response, std::move(result));
            } else {
              std::unique_lock lock(gate);
              reply(response, control(operation, *body));
            }
          });
    }
  }
};

ComputeService::ComputeService(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}

ComputeService::~ComputeService() noexcept {
  stop();
}

StatusOr<std::unique_ptr<ComputeService>>
ComputeService::create(std::shared_ptr<IStateStore> store, const ComputeServiceConfig& config,
                       const ComputationModule& module) noexcept {
  if (!store || !text_ok(config.node_id) || !text_ok(config.owner) || config.shard_count == 0 ||
      config.shards.empty() || config.shards.size() > 16 || config.max_request_bytes == 0 ||
      config.max_request_bytes > 16 * 1024 * 1024 || config.max_response_bytes < 4096 ||
      config.max_response_bytes > 16 * 1024 * 1024 || config.max_functions_per_call == 0 ||
      config.max_functions_per_call > 4096 || config.worker_threads == 0 ||
      config.worker_threads > 64 || config.max_queued_requests == 0 ||
      config.max_queued_requests > 1024)
    return invalid("invalid bounded Compute service configuration");

  auto impl = std::make_unique<Impl>();
  impl->store = std::move(store);
  impl->config = config;
  impl->module = module;
  for (auto shard : config.shards) {
    if (shard >= config.shard_count ||
        !impl->shards.emplace(shard, std::make_unique<Impl::Shard>()).second)
      return invalid("Compute shards must be unique members of the data assignment");
  }

  impl->init();
  return std::unique_ptr<ComputeService>(new ComputeService(std::move(impl)));
}

StatusOr<int> ComputeService::start(const std::string& host, int port) noexcept {
  if (port < 0 || port > 65535)
    return invalid("Compute port is out of range");

  std::lock_guard lock(impl_->lifecycle);
  if (impl_->started)
    return blocked("Compute service starts only once");

  int actual = port;
  if (port == 0)
    actual = impl_->server->bind_to_any_port(host);
  else if (!impl_->server->bind_to_port(host, port))
    actual = -1;
  if (actual < 0)
    return Status(ErrorCode::Unavailable, "cannot bind Compute service");

  impl_->started = true;
  impl_->listener =
      std::thread([state = impl_.get()]() noexcept { state->server->listen_after_bind(); });
  impl_->server->wait_until_ready();

  return actual;
}

bool ComputeService::is_running() const noexcept {
  return impl_->server->is_running();
}

void ComputeService::stop() noexcept {
  std::lock_guard lock(impl_->lifecycle);
  if (impl_->listener.joinable()) {
    impl_->server->stop();
    impl_->listener.join();
  }
}
} // namespace eventflow
