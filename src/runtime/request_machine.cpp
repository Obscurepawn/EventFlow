#include "eventflow/request_machine.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <map>
#include <set>

namespace eventflow {
namespace {
bool terminal(RequestPhase phase) noexcept {
  return phase == RequestPhase::Success || phase == RequestPhase::Failed ||
         phase == RequestPhase::Cancelled;
}

bool completed(RequestNodePhase phase) noexcept {
  return phase == RequestNodePhase::Succeeded || phase == RequestNodePhase::Failed ||
         phase == RequestNodePhase::Skipped;
}

bool same_fields(const std::vector<Field>& left, const std::vector<Field>& right) noexcept {
  if (left.size() != right.size())
    return false;

  for (std::size_t i = 0; i < left.size(); ++i)
    if (left[i].id != right[i].id || left[i].name != right[i].name ||
        left[i].type != right[i].type || left[i].nullable != right[i].nullable)
      return false;

  return true;
}

Status consume(std::size_t bytes, std::size_t limit, std::size_t& used) noexcept {
  if (used > limit || bytes > limit - used)
    return Status(ErrorCode::ResourceExhausted, "request state or input byte budget exceeded");

  used += bytes;
  return {};
}

Status check_value(const Value& value, const Field& field, std::size_t limit) noexcept {
  if (std::holds_alternative<std::monostate>(value))
    return check(field.nullable, "NULL in nonnullable request field");

  bool matches = false;
  switch (field.type) {
  case DataType::Bool:
    matches = std::holds_alternative<bool>(value);
    break;
  case DataType::Int64:
  case DataType::TimestampMicros:
    matches = std::holds_alternative<std::int64_t>(value);
    break;
  case DataType::Float64:
    matches = std::holds_alternative<double>(value);
    break;
  case DataType::String:
    matches = std::holds_alternative<std::string>(value);
    break;
  case DataType::FloatVector:
    matches = std::holds_alternative<std::vector<double>>(value);
    break;
  }

  EF_RETURN_IF_ERROR(check(matches, "request value has the wrong type"));
  if (const auto* number = std::get_if<double>(&value))
    EF_RETURN_IF_ERROR(check(std::isfinite(*number), "nonfinite request Float64"));
  if (const auto* text = std::get_if<std::string>(&value)) {
    if (text->size() > limit)
      return Status(ErrorCode::ResourceExhausted, "request string exceeds byte budget");

    EF_RETURN_IF_ERROR(check(valid_utf8(*text), "invalid UTF-8 request string"));
  }

  if (const auto* vector = std::get_if<std::vector<double>>(&value)) {
    if (vector->size() > limit / sizeof(double))
      return Status(ErrorCode::ResourceExhausted, "request vector exceeds byte budget");

    for (const auto number : *vector)
      EF_RETURN_IF_ERROR(check(std::isfinite(number), "nonfinite request vector element"));
  }

  return {};
}

StatusOr<Json> row_json(const Row& row, const std::vector<Field>& fields,
                        std::size_t limit) noexcept {
  EF_RETURN_IF_ERROR(check(row.size() == fields.size(), "request row size differs from signature"));
  Json result = Json::array();
  std::size_t used = 2;

  for (std::size_t i = 0; i < row.size(); ++i) {
    EF_RETURN_IF_ERROR(check_value(row[i], fields[i], limit));
    EF_ASSIGN_OR_RETURN(auto value, value_to_json(row[i]));
    EF_RETURN_IF_ERROR(consume(value.dump().size() + (i != 0), limit, used));
    result.push_back(std::move(value));
  }

  EF_RETURN_IF_ERROR(check(used <= limit, "request row budget is too small"));
  return result;
}

StatusOr<Json> inputs_json(const FunctionInputs& inputs, const FunctionViewDefinition& function,
                           std::size_t limit) noexcept {
  EF_RETURN_IF_ERROR(check(inputs.size() == function.inputs.size(),
                           "frozen input count differs from function signature"));
  Json result = Json::array();
  std::size_t used = 2;

  for (std::size_t i = 0; i < inputs.size(); ++i) {
    Json row = nullptr;
    if (inputs[i]) {
      EF_ASSIGN_OR_RETURN(row, row_json(*inputs[i], function.inputs[i].fields, limit));
    } else {
      EF_RETURN_IF_ERROR(check(!function.inputs[i].required, "required frozen input is missing"));
    }

    EF_RETURN_IF_ERROR(consume(row.dump().size() + (i != 0), limit, used));
    result.push_back(std::move(row));
  }

  return result;
}

Json error_json(const Status& error) noexcept {
  return Json{{"code", static_cast<int>(error.code())}, {"message", std::string(error.message())}};
}

Status valid_error(const Status& error) noexcept {
  return check(error.code() >= ErrorCode::Ok && error.code() <= ErrorCode::UnknownCommit &&
                   valid_utf8(error.message()) && (!error.ok() || error.message().empty()),
               "invalid request error metadata");
}

void clear_work(RequestNodeState& node) noexcept {
  node.ready_at_us = 0;
  node.started_at_us = 0;
  node.attempt_deadline_us = 0;
  std::string{}.swap(node.task_id);
  FunctionInputs{}.swap(node.frozen_inputs);
  std::string{}.swap(node.frozen_input_identity);
}

void stop_node(RequestNodeState& node, RequestNodePhase phase, Status error) noexcept {
  clear_work(node);
  node.phase = phase;
  node.has_output = false;
  node.error = error;
}

void remember_cancel(RequestMachineTransition& transition, const std::string& task_id) noexcept {
  if (!task_id.empty() &&
      std::find(transition.cancel_task_ids.begin(), transition.cancel_task_ids.end(), task_id) ==
          transition.cancel_task_ids.end())
    transition.cancel_task_ids.push_back(task_id);
}

void remember_settlement(RequestMachineTransition& transition, const RequestNodeState& node,
                         RequestAttemptOutcome outcome) noexcept {
  if (node.phase == RequestNodePhase::Running)
    transition.settlements.push_back(
        {.relation = node.relation, .attempt = node.attempt, .outcome = outcome});
}

Status members(const Json& value, std::initializer_list<std::string_view> names) noexcept {
  EF_RETURN_IF_ERROR(check(value.is_object() && value.size() == names.size(),
                           "request codec object has unknown or missing members"));
  for (const auto name : names)
    EF_RETURN_IF_ERROR(check(value.contains(std::string(name)), "request codec member is missing"));

  return {};
}

StatusOr<std::uint64_t> read_unsigned(const Json& value) noexcept {
  EF_RETURN_IF_ERROR(check(value.is_number_integer(), "request codec expects an integer"));
  if (!value.is_number_unsigned())
    EF_RETURN_IF_ERROR(
        check(value.get<std::int64_t>() >= 0, "request codec expects a nonnegative integer"));

  return value.get<std::uint64_t>();
}

StatusOr<std::int64_t> read_time(const Json& value) noexcept {
  EF_ASSIGN_OR_RETURN(const auto result, read_unsigned(value));
  EF_RETURN_IF_ERROR(
      check(result <= static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()),
            "request time exceeds Int64"));
  return static_cast<std::int64_t>(result);
}

StatusOr<std::string> read_string(const Json& value) noexcept {
  EF_RETURN_IF_ERROR(check(value.is_string(), "request codec expects a string"));
  return value.get<std::string>();
}

Status read_error(const Json& value, Status& result) noexcept {
  EF_RETURN_IF_ERROR(members(value, {"code", "message"}));
  EF_ASSIGN_OR_RETURN(const auto code, read_unsigned(value["code"]));
  EF_ASSIGN_OR_RETURN(auto message, read_string(value["message"]));
  EF_RETURN_IF_ERROR(check(code <= static_cast<unsigned>(ErrorCode::UnknownCommit) &&
                               message.size() <= 384 && valid_utf8(message),
                           "invalid encoded request error"));
  result = Status(static_cast<ErrorCode>(code), message);
  EF_RETURN_IF_ERROR(valid_error(result));

  return {};
}

StatusOr<Row> read_row_json(const Json& value, const std::vector<Field>& fields) noexcept {
  EF_RETURN_IF_ERROR(check(value.is_array() && value.size() == fields.size(),
                           "encoded request row has wrong arity"));
  Row result;
  for (std::size_t i = 0; i < fields.size(); ++i) {
    EF_ASSIGN_OR_RETURN(auto item, value_from_json(value[i], fields[i].type, fields[i].nullable));
    result.push_back(std::move(item));
  }

  return result;
}

bool shallow_json(std::string_view bytes) noexcept {
  std::size_t depth = 0;
  bool quoted = false, escaped = false;
  for (const char value : bytes) {
    if (quoted) {
      if (escaped)
        escaped = false;
      else if (value == '\\')
        escaped = true;
      else if (value == '"')
        quoted = false;
    } else if (value == '"')
      quoted = true;
    else if (value == '[' || value == '{') {
      if (++depth > 64)
        return false;
    } else if (value == ']' || value == '}') {
      if (depth == 0)
        return false;

      --depth;
    }
  }

  return depth == 0 && !quoted;
}
} // namespace

StatusOr<Json> request_manifest_to_json(const RequestManifest& manifest) noexcept {
  EF_RETURN_IF_ERROR(
      check(!manifest.scope.empty() && manifest.identity.size() == manifest.scope.size() + 1 &&
                !manifest.request_id.empty() && valid_utf8(manifest.request_id) &&
                !manifest.owner_token.empty() && valid_utf8(manifest.owner_token) &&
                valid_utf8(manifest.catalog_fingerprint) && manifest.admission_seq > 0 &&
                manifest.admitted_at_us >= 0 && manifest.deadline_us > manifest.admitted_at_us,
            "invalid manifest structure, identity or lifetime"));
  if (manifest.capture_revision != 0 || manifest.admission_revision != 0)
    EF_RETURN_IF_ERROR(
        check(manifest.capture_revision != std::numeric_limits<std::uint64_t>::max() &&
                  manifest.admission_revision == manifest.capture_revision + 1,
              "capture revision must precede admission revision"));

  const auto encode_keys = [](const Row& row) noexcept -> StatusOr<Json> {
    Json encoded = Json::array();
    for (const auto& value : row) {
      EF_RETURN_IF_ERROR(check(std::holds_alternative<bool>(value) ||
                                   std::holds_alternative<std::int64_t>(value) ||
                                   std::holds_alternative<std::string>(value),
                               "manifest key must be a non-NULL Bool, integer or String"));
      EF_ASSIGN_OR_RETURN(auto item, value_to_json(value));
      encoded.push_back(std::move(item));
    }

    return encoded;
  };
  EF_ASSIGN_OR_RETURN(auto scope, encode_keys(manifest.scope));
  EF_ASSIGN_OR_RETURN(auto identity, encode_keys(manifest.identity));
  EF_RETURN_IF_ERROR(
      check(std::equal(manifest.scope.begin(), manifest.scope.end(), manifest.identity.begin()) &&
                manifest.identity.back() == Value(manifest.request_id),
            "manifest identity differs from scope or request ID"));
  std::set<RelationId> targets;

  for (const auto target : manifest.targets)
    EF_RETURN_IF_ERROR(
        check(target > 0 && targets.insert(target).second, "invalid or duplicate manifest target"));

  return Json{{"scope", std::move(scope)},
              {"identity", std::move(identity)},
              {"request_id", manifest.request_id},
              {"admission_seq", manifest.admission_seq},
              {"candidate_epoch", manifest.candidate_epoch},
              {"owner_token", manifest.owner_token},
              {"owner_epoch", manifest.owner_epoch},
              {"admitted_at_us", manifest.admitted_at_us},
              {"deadline_us", manifest.deadline_us},
              {"capture_revision", manifest.capture_revision},
              {"admission_revision", manifest.admission_revision},
              {"catalog_fingerprint", manifest.catalog_fingerprint},
              {"targets", manifest.targets}};
}

StatusOr<RequestManifest> request_manifest_from_json(const Json& value) noexcept {
  EF_RETURN_IF_ERROR(
      members(value, {"scope", "identity", "request_id", "admission_seq", "candidate_epoch",
                      "owner_token", "owner_epoch", "admitted_at_us", "deadline_us",
                      "capture_revision", "admission_revision", "catalog_fingerprint", "targets"}));
  RequestManifest result;
  const auto read_keys = [](const Json& encoded) noexcept -> StatusOr<Row> {
    EF_RETURN_IF_ERROR(check(encoded.is_array(), "manifest identity must be an array"));
    Row row;
    for (const auto& item : encoded) {
      DataType type;
      if (item.is_boolean())
        type = DataType::Bool;
      else if (item.is_number_integer())
        type = DataType::Int64;
      else if (item.is_string())
        type = DataType::String;
      else
        return Status(ErrorCode::InvalidArgument, "manifest key has an invalid type");
      EF_ASSIGN_OR_RETURN(auto value, value_from_json(item, type, false));
      row.push_back(std::move(value));
    }

    return row;
  };
  EF_ASSIGN_OR_RETURN(result.scope, read_keys(value["scope"]));
  EF_ASSIGN_OR_RETURN(result.identity, read_keys(value["identity"]));
  EF_ASSIGN_OR_RETURN(result.request_id, read_string(value["request_id"]));
  EF_ASSIGN_OR_RETURN(result.admission_seq, read_unsigned(value["admission_seq"]));
  EF_ASSIGN_OR_RETURN(result.candidate_epoch, read_unsigned(value["candidate_epoch"]));
  EF_ASSIGN_OR_RETURN(result.owner_token, read_string(value["owner_token"]));
  EF_ASSIGN_OR_RETURN(result.owner_epoch, read_unsigned(value["owner_epoch"]));
  EF_ASSIGN_OR_RETURN(result.admitted_at_us, read_time(value["admitted_at_us"]));
  EF_ASSIGN_OR_RETURN(result.deadline_us, read_time(value["deadline_us"]));
  EF_ASSIGN_OR_RETURN(result.capture_revision, read_unsigned(value["capture_revision"]));
  EF_ASSIGN_OR_RETURN(result.admission_revision, read_unsigned(value["admission_revision"]));
  EF_ASSIGN_OR_RETURN(result.catalog_fingerprint, read_string(value["catalog_fingerprint"]));
  EF_RETURN_IF_ERROR(check(value["targets"].is_array(), "manifest targets must be an array"));

  for (const auto& item : value["targets"]) {
    EF_ASSIGN_OR_RETURN(const auto target, read_unsigned(item));
    EF_RETURN_IF_ERROR(check(target > 0 && target <= std::numeric_limits<RelationId>::max(),
                             "manifest target is out of range"));
    result.targets.push_back(static_cast<RelationId>(target));
  }

  EF_ASSIGN_OR_RETURN(auto checked, request_manifest_to_json(result));
  (void)checked;
  return result;
}

struct RequestMachine::Impl {
  CatalogSnapshot catalog;
  std::shared_ptr<FunctionRegistry> registry;
  RequestMachineConfig config;
  std::vector<RelationId> targets;
  std::vector<RelationId> relations;
  std::map<RelationId, std::size_t> positions;
  std::vector<Field> identity_fields;
  std::vector<Field> partition_fields;
  std::size_t terminal_overhead_bytes = 0;

  const RelationDescriptor& descriptor(std::size_t position) const noexcept {
    return *catalog.relations[relations[position] - 1];
  }

  const RequestNodeState& parent(const RequestMachineState& state, RelationId id) const noexcept {
    return state.nodes[positions.find(id)->second];
  }

  Status validate_manifest(const RequestManifest& manifest) const noexcept {
    EF_RETURN_IF_ERROR(
        check(manifest.catalog_fingerprint == catalog.fingerprint && manifest.targets == targets,
              "request manifest has different Catalog or targets"));
    EF_RETURN_IF_ERROR(check(!manifest.request_id.empty() &&
                                 manifest.request_id.size() <= config.max_identifier_bytes &&
                                 valid_utf8(manifest.request_id) && !manifest.owner_token.empty() &&
                                 manifest.owner_token.size() <= config.max_identifier_bytes &&
                                 valid_utf8(manifest.owner_token),
                             "invalid request or owner identity"));
    EF_RETURN_IF_ERROR(
        check(manifest.admission_seq > 0 && manifest.admitted_at_us >= 0 &&
                  manifest.deadline_us > manifest.admitted_at_us &&
                  manifest.deadline_us - manifest.admitted_at_us <= config.max_lifetime_us,
              "invalid request admission sequence or lifetime"));
    if (manifest.capture_revision != 0 || manifest.admission_revision != 0)
      EF_RETURN_IF_ERROR(
          check(manifest.capture_revision != std::numeric_limits<std::uint64_t>::max() &&
                    manifest.admission_revision == manifest.capture_revision + 1,
                "capture revision must precede the admission commit revision"));

    EF_ASSIGN_OR_RETURN(auto scope,
                        row_json(manifest.scope, partition_fields, config.max_state_bytes));
    EF_ASSIGN_OR_RETURN(auto identity,
                        row_json(manifest.identity, identity_fields, config.max_state_bytes));
    (void)scope;
    (void)identity;
    EF_RETURN_IF_ERROR(
        check(std::equal(manifest.scope.begin(), manifest.scope.end(), manifest.identity.begin()) &&
                  manifest.identity.back() == Value(manifest.request_id),
              "request identity differs from scope or request ID"));
    return {};
  }

  StatusOr<RequestManifest> normalize(RequestManifest manifest) const noexcept {
    if (manifest.catalog_fingerprint.empty())
      manifest.catalog_fingerprint = catalog.fingerprint;
    if (manifest.targets.empty())
      manifest.targets = targets;
    else
      std::sort(manifest.targets.begin(), manifest.targets.end());
    EF_RETURN_IF_ERROR(validate_manifest(manifest));

    return manifest;
  }

  StatusOr<Json> manifest_json(const RequestManifest& manifest) const noexcept {
    EF_ASSIGN_OR_RETURN(auto scope,
                        row_json(manifest.scope, partition_fields, config.max_state_bytes));
    EF_ASSIGN_OR_RETURN(auto identity,
                        row_json(manifest.identity, identity_fields, config.max_state_bytes));
    (void)scope;
    (void)identity;

    return request_manifest_to_json(manifest);
  }

  StatusOr<std::string> canonical_task(const RequestMachineState& state,
                                       std::size_t position) const noexcept {
    const auto& node = state.nodes[position];
    const auto& function = descriptor(position).function->definition;
    EF_ASSIGN_OR_RETURN(auto manifest, manifest_json(state.manifest));

    // The full Catalog fingerprint and exact input witness remain in durable
    // state. The executor's bounded opaque ID identifies the owner's attempt.
    manifest.erase("catalog_fingerprint");
    manifest["catalog_version"] = catalog.version;
    Json input_versions = Json::array();

    for (const auto& input : function.inputs) {
      const auto& source = parent(state, input.relation);
      input_versions.push_back({{"relation", source.relation},
                                {"attempt", source.attempt},
                                {"has_output", source.has_output}});
    }

    auto result = Json{{"format", 1},
                       {"manifest", std::move(manifest)},
                       {"relation", node.relation},
                       {"attempt", node.attempt},
                       {"started_at_us", node.started_at_us},
                       {"attempt_deadline_us", node.attempt_deadline_us},
                       {"identity_domain", descriptor(position).identity_domain},
                       {"function_id", function.function_id},
                       {"function_version", function.function_version},
                       {"implementation_identity", function.implementation_identity},
                       {"input_versions", std::move(input_versions)}}
                      .dump();
    if (result.size() > std::min<std::size_t>(config.max_state_bytes, 4096))
      return Status(ErrorCode::ResourceExhausted, "canonical task identity exceeds request budget");

    return result;
  }

  FunctionTask task(const RequestMachineState& state, std::size_t position) const noexcept {
    const auto& node = state.nodes[position];
    const auto& function = descriptor(position).function->definition;
    return {.task_id = node.task_id,
            .function_id = function.function_id,
            .function_version = function.function_version,
            .implementation_identity = function.implementation_identity,
            .inputs = node.frozen_inputs,
            .ready_at_us = node.started_at_us};
  }

  StatusOr<Json> node_json(const RequestNodeState& node, std::size_t position) const noexcept {
    Json inputs = Json::array();
    if (!node.frozen_inputs.empty()) {
      EF_ASSIGN_OR_RETURN(inputs,
                          inputs_json(node.frozen_inputs, descriptor(position).function->definition,
                                      config.max_frozen_input_bytes));
    }

    return Json{{"relation", node.relation},
                {"phase", static_cast<int>(node.phase)},
                {"attempt", node.attempt},
                {"ready_at_us", node.ready_at_us},
                {"started_at_us", node.started_at_us},
                {"attempt_deadline_us", node.attempt_deadline_us},
                {"task_id", node.task_id},
                {"frozen_inputs", std::move(inputs)},
                {"frozen_input_identity", node.frozen_input_identity},
                {"has_output", node.has_output},
                {"error", error_json(node.error)}};
  }

  static Json envelope(Json manifest, Json nodes, RequestPhase phase, std::int64_t now_us,
                       const Status& error) noexcept {
    return Json{
        {"format", 1},      {"manifest", std::move(manifest)}, {"phase", static_cast<int>(phase)},
        {"now_us", now_us}, {"nodes", std::move(nodes)},       {"error", error_json(error)}};
  }

  StatusOr<Json> state_json(const RequestMachineState& state) const noexcept {
    EF_ASSIGN_OR_RETURN(auto manifest, manifest_json(state.manifest));
    Json nodes = Json::array();
    for (std::size_t i = 0; i < state.nodes.size(); ++i) {
      EF_ASSIGN_OR_RETURN(auto node, node_json(state.nodes[i], i));
      nodes.push_back(std::move(node));
    }

    return envelope(std::move(manifest), std::move(nodes), state.phase, state.now_us, state.error);
  }

  StatusOr<std::size_t> terminal_overhead() const noexcept {
    // A terminal transition clears all work. Bound its remaining metadata with
    // maximum-width counters/time and the largest escaped Status messages.
    const Status worst_error(ErrorCode::UnknownCommit, std::string(384, char{1}));
    Json nodes = Json::array();

    for (std::size_t i = 0; i < relations.size(); ++i) {
      RequestNodeState node;
      node.relation = relations[i];
      node.phase = RequestNodePhase::Skipped;
      node.error = worst_error;
      if (descriptor(i).kind == RelationKind::FunctionView)
        node.attempt = config.max_attempts;

      EF_ASSIGN_OR_RETURN(auto encoded, node_json(node, i));
      nodes.push_back(std::move(encoded));
    }

    const auto encoded = envelope(Json(nullptr), std::move(nodes), RequestPhase::Failed,
                                  std::numeric_limits<std::int64_t>::max(), worst_error)
                             .dump();

    // The real immutable manifest replaces this four-byte JSON null value.
    return encoded.size() - 4;
  }

  Status validate_state(const RequestMachineState& state) const noexcept {
    EF_RETURN_IF_ERROR(validate_manifest(state.manifest));
    EF_RETURN_IF_ERROR(check(
        state.phase >= RequestPhase::Accepted && state.phase <= RequestPhase::Cancelled &&
            state.now_us >= state.manifest.admitted_at_us && state.nodes.size() == relations.size(),
        "invalid request phase, time or node set"));
    EF_RETURN_IF_ERROR(valid_error(state.error));
    if (!terminal(state.phase)) {
      EF_ASSIGN_OR_RETURN(auto manifest, manifest_json(state.manifest));
      auto reserved = manifest.dump().size();
      EF_RETURN_IF_ERROR(consume(terminal_overhead_bytes, config.max_state_bytes, reserved));
    }

    std::size_t running = 0, input_bytes = 0;
    bool attempted = false;
    for (std::size_t i = 0; i < state.nodes.size(); ++i) {
      const auto& node = state.nodes[i];
      const auto& relation = descriptor(i);
      EF_RETURN_IF_ERROR(check(
          node.relation == relations[i] && node.phase >= RequestNodePhase::Waiting &&
              node.phase <= RequestNodePhase::Skipped && node.attempt <= config.max_attempts &&
              node.task_id.size() <= config.max_state_bytes,
          "request nodes differ from the Catalog graph or attempt bounds"));
      EF_RETURN_IF_ERROR(valid_error(node.error));
      attempted = attempted || node.attempt != 0;

      const bool work =
          node.phase == RequestNodePhase::Ready || node.phase == RequestNodePhase::Running;
      if (work) {
        EF_RETURN_IF_ERROR(check(relation.kind == RelationKind::FunctionView && !node.has_output &&
                                     node.ready_at_us >= state.manifest.admitted_at_us &&
                                     node.ready_at_us <= state.manifest.deadline_us,
                                 "invalid active function node"));

        EF_ASSIGN_OR_RETURN(auto inputs,
                            inputs_json(node.frozen_inputs, relation.function->definition,
                                        config.max_frozen_input_bytes));
        const auto input_identity = inputs.dump();
        EF_RETURN_IF_ERROR(check(node.frozen_input_identity == input_identity,
                                 "frozen input witness was altered"));
        EF_RETURN_IF_ERROR(
            consume(input_identity.size(), config.max_frozen_input_bytes, input_bytes));

        for (std::size_t input = 0; input < relation.function->definition.inputs.size(); ++input) {
          const auto& source = parent(state, relation.function->definition.inputs[input].relation);
          EF_RETURN_IF_ERROR(check(source.phase == RequestNodePhase::Succeeded &&
                                       source.has_output == node.frozen_inputs[input].has_value(),
                                   "active function input is not a completed dependency"));
        }
      } else {
        EF_RETURN_IF_ERROR(check(node.task_id.empty() && node.frozen_inputs.empty() &&
                                     node.frozen_input_identity.empty() && node.ready_at_us == 0 &&
                                     node.started_at_us == 0 && node.attempt_deadline_us == 0,
                                 "inactive node retains work metadata"));
      }

      if (node.phase == RequestNodePhase::Running) {
        ++running;
        EF_RETURN_IF_ERROR(check(node.attempt > 0 && node.started_at_us >= node.ready_at_us &&
                                     node.started_at_us <= state.now_us &&
                                     node.attempt_deadline_us > node.started_at_us &&
                                     node.attempt_deadline_us > state.now_us &&
                                     node.attempt_deadline_us <= state.manifest.deadline_us &&
                                     node.error.ok(),
                                 "invalid running attempt timing"));
        EF_RETURN_IF_ERROR(check(node.attempt_deadline_us ==
                                     node.started_at_us +
                                         std::min(config.attempt_timeout_us,
                                                  state.manifest.deadline_us - node.started_at_us),
                                 "running deadline differs from the configured attempt lifetime"));

        EF_ASSIGN_OR_RETURN(auto expected, canonical_task(state, i));
        EF_RETURN_IF_ERROR(
            check(node.task_id == expected, "running task identity or frozen inputs were altered"));
      } else if (node.phase == RequestNodePhase::Ready) {
        EF_RETURN_IF_ERROR(check(node.attempt < config.max_attempts && node.task_id.empty() &&
                                     node.started_at_us == 0 && node.attempt_deadline_us == 0,
                                 "invalid retry or ready node"));
        EF_RETURN_IF_ERROR(check(node.attempt == 0
                                     ? node.error.ok()
                                     : (node.error.code() == ErrorCode::Unavailable ||
                                        node.error.code() == ErrorCode::ResourceExhausted),
                                 "ready attempt has an impossible error state"));
      } else if (node.phase == RequestNodePhase::Waiting) {
        EF_RETURN_IF_ERROR(check(node.attempt == 0 && !node.has_output && node.error.ok(),
                                 "invalid waiting node"));
      } else if (node.phase == RequestNodePhase::Succeeded) {
        EF_RETURN_IF_ERROR(check(node.error.ok() && (relation.kind != RelationKind::FunctionView ||
                                                     (node.attempt > 0 && node.has_output)),
                                 "invalid successful node"));

        EF_ASSIGN_OR_RETURN(auto parents, dependencies(relation));

        for (const auto dependency : parents)
          EF_RETURN_IF_ERROR(check(parent(state, dependency).phase == RequestNodePhase::Succeeded,
                                   "successful node has an incomplete dependency"));

        if (relation.kind == RelationKind::SqlView && node.has_output)
          EF_RETURN_IF_ERROR(check(parent(state, relation.plan->left).has_output,
                                   "SQL output exists without its input"));
        if (relation.kind == RelationKind::FunctionView)
          for (const auto& input : relation.function->definition.inputs)
            EF_RETURN_IF_ERROR(check(!input.required || parent(state, input.relation).has_output,
                                     "successful function lacks a required input"));
      } else {
        EF_RETURN_IF_ERROR(check(!node.has_output && !node.error.ok(),
                                 "failed or skipped node lacks a terminal error"));
        EF_RETURN_IF_ERROR(
            check(terminal(state.phase), "failed ancestor was not propagated to the request"));
        if (node.phase == RequestNodePhase::Failed)
          EF_RETURN_IF_ERROR(check(relation.kind == RelationKind::FunctionView && node.attempt > 0,
                                   "failed node has no function attempt"));
      }

      if (relation.kind != RelationKind::FunctionView)
        EF_RETURN_IF_ERROR(
            check(node.attempt == 0 && !work && node.phase != RequestNodePhase::Failed,
                  "nonfunction node has attempt state"));
      if (terminal(state.phase))
        EF_RETURN_IF_ERROR(
            check(completed(node.phase), "terminal request retains unfinished nodes"));
    }

    EF_RETURN_IF_ERROR(
        check(running <= config.max_running_tasks, "too many running request tasks"));
    bool all_success = true, target_failure = false;
    for (const auto id : targets) {
      const auto phase = parent(state, id).phase;
      all_success = all_success && phase == RequestNodePhase::Succeeded;
      target_failure =
          target_failure || phase == RequestNodePhase::Failed || phase == RequestNodePhase::Skipped;
    }

    if (state.phase == RequestPhase::Success)
      EF_RETURN_IF_ERROR(
          check(all_success && state.error.ok(), "successful request has unsuccessful targets"));
    else if (state.phase == RequestPhase::Failed || state.phase == RequestPhase::Cancelled)
      EF_RETURN_IF_ERROR(check(target_failure && !state.error.ok(),
                               "terminal failed request lacks failed targets"));
    else
      EF_RETURN_IF_ERROR(check(!all_success && !target_failure && state.error.ok() &&
                                   state.now_us < state.manifest.deadline_us &&
                                   ((state.phase == RequestPhase::Running) == attempted),
                               "request phase disagrees with progress or deadline"));
    EF_ASSIGN_OR_RETURN(auto encoded, state_json(state));
    if (encoded.dump().size() > config.max_state_bytes)
      return Status(ErrorCode::ResourceExhausted, "serialized request exceeds state budget");

    return {};
  }

  StatusOr<std::optional<Row>> read(const ReadRequestRow& reader, const RequestMachineState& state,
                                    RelationId id) const noexcept {
    EF_ASSIGN_OR_RETURN(auto row, reader(id, state.manifest.identity));
    if (row) {
      EF_ASSIGN_OR_RETURN(const auto relation, catalog.find(id));
      EF_ASSIGN_OR_RETURN(auto identity, physical_identity(*relation, *row));
      EF_RETURN_IF_ERROR(check(identity == state.manifest.identity,
                               "staged row belongs to a different request identity"));
    }

    return row;
  }

  Status finish(RequestMachineTransition& transition) const noexcept {
    auto& state = transition.state;
    for (std::size_t i = 0; i < state.nodes.size(); ++i) {
      auto& node = state.nodes[i];
      if (node.phase != RequestNodePhase::Waiting)
        continue;

      EF_ASSIGN_OR_RETURN(auto parents, dependencies(descriptor(i)));
      for (const auto dependency : parents) {
        const auto phase = parent(state, dependency).phase;
        if (phase == RequestNodePhase::Failed || phase == RequestNodePhase::Skipped) {
          stop_node(node, RequestNodePhase::Skipped,
                    Status(ErrorCode::FailedPrecondition, "request ancestor failed"));
          break;
        }
      }
    }

    bool success = true;
    Status failure;
    for (const auto target : targets) {
      const auto& node = parent(state, target);
      success = success && node.phase == RequestNodePhase::Succeeded;
      if (node.phase == RequestNodePhase::Failed || node.phase == RequestNodePhase::Skipped)
        failure = node.error;
    }

    if (!failure.ok()) {
      state.phase = RequestPhase::Failed;
      state.error = failure;
      for (auto& node : state.nodes) {
        if (!completed(node.phase)) {
          remember_cancel(transition, node.task_id);
          remember_settlement(transition, node, RequestAttemptOutcome::Cancelled);
          stop_node(node, RequestNodePhase::Skipped,
                    Status(ErrorCode::FailedPrecondition, "request failed"));
        }
      }
    } else if (success) {
      state.phase = RequestPhase::Success;
      state.error = {};
    } else {
      state.phase = std::any_of(state.nodes.begin(), state.nodes.end(),
                                [](const auto& node) noexcept { return node.attempt != 0; })
                        ? RequestPhase::Running
                        : RequestPhase::Accepted;
    }

    return {};
  }

  void retry_or_fail(RequestMachineTransition& transition, std::size_t position, Status error,
                     RequestAttemptOutcome outcome) const noexcept {
    auto& state = transition.state;
    auto& node = state.nodes[position];
    remember_cancel(transition, node.task_id);
    remember_settlement(transition, node, outcome);
    if (node.attempt < config.max_attempts) {
      node.phase = RequestNodePhase::Ready;
      node.ready_at_us = state.now_us + std::min(config.retry_backoff_us,
                                                 state.manifest.deadline_us - state.now_us);
      node.started_at_us = 0;
      node.attempt_deadline_us = 0;
      std::string{}.swap(node.task_id);
      node.error = error;
    } else {
      stop_node(node, RequestNodePhase::Failed, error);
    }
  }

  Status advance(RequestMachineTransition& transition, std::int64_t now_us) const noexcept {
    auto& state = transition.state;
    EF_RETURN_IF_ERROR(check(now_us >= state.now_us, "request time cannot move backwards"));
    if (terminal(state.phase))
      return {};

    state.now_us = now_us;
    if (now_us >= state.manifest.deadline_us) {
      state.phase = RequestPhase::Failed;
      state.error = Status(ErrorCode::FailedPrecondition, "request deadline expired");
      for (auto& node : state.nodes) {
        if (!completed(node.phase)) {
          remember_cancel(transition, node.task_id);
          remember_settlement(transition, node, RequestAttemptOutcome::TimedOut);
          const auto phase = node.phase == RequestNodePhase::Running ? RequestNodePhase::Failed
                                                                     : RequestNodePhase::Skipped;
          stop_node(node, phase, state.error);
        }
      }

      return {};
    }

    for (std::size_t i = 0; i < state.nodes.size(); ++i)
      if (state.nodes[i].phase == RequestNodePhase::Running &&
          state.nodes[i].attempt_deadline_us <= now_us)
        retry_or_fail(transition, i, Status(ErrorCode::Unavailable, "function attempt timed out"),
                      RequestAttemptOutcome::TimedOut);

    return finish(transition);
  }
};

RequestMachine::RequestMachine(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}

RequestMachine::~RequestMachine() noexcept = default;

StatusOr<std::unique_ptr<RequestMachine>>
RequestMachine::create(CatalogSnapshot catalog, std::shared_ptr<FunctionRegistry> registry,
                       std::vector<RelationId> targets,
                       const RequestMachineConfig& config) noexcept {
  EF_RETURN_IF_ERROR(check(registry && !targets.empty() && config.max_nodes > 0 &&
                               config.max_state_bytes > 0 && config.max_frozen_input_bytes > 0 &&
                               config.max_output_bytes > 0 && config.max_identifier_bytes > 0 &&
                               config.max_running_tasks > 0 && config.max_attempts > 0 &&
                               config.attempt_timeout_us > 0 && config.retry_backoff_us >= 0 &&
                               config.max_lifetime_us > 0,
                           "invalid request machine configuration"));
  if (targets.size() > config.max_nodes || catalog.fingerprint.size() > config.max_state_bytes)
    return Status(ErrorCode::ResourceExhausted, "request graph or Catalog exceeds budget");

  std::sort(targets.begin(), targets.end());
  EF_RETURN_IF_ERROR(check(std::adjacent_find(targets.begin(), targets.end()) == targets.end(),
                           "duplicate request target"));
  EF_ASSIGN_OR_RETURN(auto validated, Catalog::from_snapshot(catalog));
  EF_ASSIGN_OR_RETURN(catalog, validated->snapshot());
  EF_ASSIGN_OR_RETURN(const auto first, catalog.find(targets.front()));
  EF_RETURN_IF_ERROR(check(!first->identity_domain.empty() &&
                               first->identity_fields.size() == first->partition_types.size() + 1 &&
                               first->identity_fields.back().type == DataType::String,
                           "request target does not preserve a complete request identity"));
  auto impl = std::make_unique<Impl>();
  impl->config = config;
  impl->targets = targets;
  impl->identity_fields = first->identity_fields;
  impl->partition_fields.assign(first->identity_fields.begin(),
                                first->identity_fields.begin() + first->partition_types.size());
  std::set<RelationId> needed;
  auto pending = targets;

  while (!pending.empty()) {
    const auto id = pending.back();
    pending.pop_back();
    if (!needed.insert(id).second)
      continue;
    if (needed.size() > config.max_nodes)
      return Status(ErrorCode::ResourceExhausted, "request ancestor count exceeds budget");

    EF_ASSIGN_OR_RETURN(const auto relation, catalog.find(id));
    EF_RETURN_IF_ERROR(check(relation->identity_domain == first->identity_domain &&
                                 relation->identity_types == first->identity_types &&
                                 relation->partition_types == first->partition_types &&
                                 same_fields(relation->identity_fields, first->identity_fields),
                             "request graph mixes identity domains or partitions"));

    if (relation->kind == RelationKind::BaseTable) {
      EF_RETURN_IF_ERROR(check(relation->base_write_role == BaseWriteRole::RequestCapture,
                               "request graph root is not RequestCapture"));
    } else if (relation->kind == RelationKind::SqlView) {
      EF_RETURN_IF_ERROR(check(!relation->plan->right && relation->plan->group_by.empty(),
                               "request SQL changes full identity"));
    } else if (relation->kind == RelationKind::FunctionView) {
      const auto& function = relation->function->definition;
      EF_ASSIGN_OR_RETURN(auto signature,
                          registry->describe(function.function_id, function.function_version));
      EF_RETURN_IF_ERROR(
          check(signature.function_id == function.function_id &&
                    signature.version == function.function_version &&
                    signature.implementation_identity == function.implementation_identity &&
                    signature.inputs.size() == function.inputs.size() &&
                    same_fields(signature.output_fields, function.output_fields),
                "registered function does not match request output signature or implementation"));

      for (std::size_t i = 0; i < signature.inputs.size(); ++i)
        EF_RETURN_IF_ERROR(
            check(signature.inputs[i].alias == function.inputs[i].alias &&
                      signature.inputs[i].required == function.inputs[i].required &&
                      same_fields(signature.inputs[i].fields, function.inputs[i].fields),
                  "registered function input signature differs from Catalog"));
    } else {
      return Status(ErrorCode::InvalidArgument, "unsupported request relation kind");
    }

    EF_ASSIGN_OR_RETURN(auto parents, dependencies(*relation));
    pending.insert(pending.end(), parents.begin(), parents.end());
  }

  impl->relations.assign(needed.begin(), needed.end());
  for (std::size_t i = 0; i < impl->relations.size(); ++i)
    impl->positions.emplace(impl->relations[i], i);

  impl->catalog = std::move(catalog);
  impl->registry = std::move(registry);
  EF_ASSIGN_OR_RETURN(impl->terminal_overhead_bytes, impl->terminal_overhead());
  return std::unique_ptr<RequestMachine>(new RequestMachine(std::move(impl)));
}

StatusOr<RequestMachineState> RequestMachine::begin(RequestManifest manifest) const noexcept {
  EF_ASSIGN_OR_RETURN(manifest, impl_->normalize(std::move(manifest)));
  RequestMachineState result;
  result.now_us = manifest.admitted_at_us;
  result.manifest = std::move(manifest);

  for (const auto id : impl_->relations) {
    RequestNodeState node;
    node.relation = id;
    result.nodes.push_back(std::move(node));
  }

  // Every eventual attempt must fit the executor's opaque-ID limit before
  // admission. Values affect the separate input budget, not this upper bound.
  auto upper_bound = result;
  for (std::size_t i = 0; i < upper_bound.nodes.size(); ++i)
    if (impl_->descriptor(i).kind == RelationKind::FunctionView) {
      upper_bound.nodes[i].attempt = impl_->config.max_attempts;
      upper_bound.nodes[i].started_at_us = upper_bound.manifest.deadline_us - 1;
      upper_bound.nodes[i].attempt_deadline_us = upper_bound.manifest.deadline_us;
    }
  for (std::size_t i = 0; i < upper_bound.nodes.size(); ++i)
    if (impl_->descriptor(i).kind == RelationKind::FunctionView) {
      EF_ASSIGN_OR_RETURN(auto task_id, impl_->canonical_task(upper_bound, i));
      (void)task_id;
    }
  EF_RETURN_IF_ERROR(impl_->validate_state(result));

  return result;
}

StatusOr<RequestMachineTransition>
RequestMachine::refresh(const RequestMachineState& state,
                        const ReadRequestRow& reader) const noexcept {
  EF_RETURN_IF_ERROR(impl_->validate_state(state));
  EF_RETURN_IF_ERROR(check(reader != nullptr, "request refresh requires a staged-row reader"));
  RequestMachineTransition result;
  result.state = state;
  if (terminal(state.phase))
    return result;

  for (std::size_t i = 0; i < result.state.nodes.size(); ++i) {
    auto& node = result.state.nodes[i];
    if (node.phase != RequestNodePhase::Waiting)
      continue;

    const auto& relation = impl_->descriptor(i);
    EF_ASSIGN_OR_RETURN(auto parents, dependencies(relation));
    bool ready = true, failed = false;
    for (const auto dependency : parents) {
      const auto phase = impl_->parent(result.state, dependency).phase;
      ready = ready && phase == RequestNodePhase::Succeeded;
      failed = failed || phase == RequestNodePhase::Failed || phase == RequestNodePhase::Skipped;
    }

    if (failed) {
      stop_node(node, RequestNodePhase::Skipped,
                Status(ErrorCode::FailedPrecondition, "request ancestor failed"));
      continue;
    }

    if (!ready)
      continue;
    if (relation.kind != RelationKind::FunctionView) {
      if (relation.kind == RelationKind::SqlView) {
        EF_ASSIGN_OR_RETURN(auto source, impl_->read(reader, result.state, relation.plan->left));
        EF_RETURN_IF_ERROR(
            check(source.has_value() == impl_->parent(result.state, relation.plan->left).has_output,
                  "SQL refresh ran before the completed input row was staged"));
      }

      EF_ASSIGN_OR_RETURN(auto row, impl_->read(reader, result.state, relation.id));
      if (relation.kind == RelationKind::SqlView &&
          !impl_->parent(result.state, relation.plan->left).has_output)
        EF_RETURN_IF_ERROR(check(!row, "SQL produced output without a completed input row"));

      node.phase = RequestNodePhase::Succeeded;
      node.has_output = row.has_value();
    } else {
      bool missing = false;
      for (const auto& input : relation.function->definition.inputs)
        missing =
            missing || (input.required && !impl_->parent(result.state, input.relation).has_output);

      if (missing) {
        stop_node(node, RequestNodePhase::Skipped,
                  Status(ErrorCode::FailedPrecondition, "required input completed without a row"));
        continue;
      }

      // All dependencies have finished. Freeze their rows once so every retry of this node uses
      // the same arguments, even if other requests or source rows change later.
      for (const auto& input : relation.function->definition.inputs) {
        EF_ASSIGN_OR_RETURN(auto row, impl_->read(reader, result.state, input.relation));
        EF_RETURN_IF_ERROR(
            check(row.has_value() == impl_->parent(result.state, input.relation).has_output,
                  "staged dependency presence changed after completion"));
        if (row) {
          EF_ASSIGN_OR_RETURN(const auto source, impl_->catalog.find(input.relation));
          EF_ASSIGN_OR_RETURN(auto visible, visible_row(*source, *row));
          node.frozen_inputs.emplace_back(std::move(visible));
        } else {
          node.frozen_inputs.emplace_back(std::nullopt);
        }
      }

      EF_ASSIGN_OR_RETURN(auto input_identity,
                          inputs_json(node.frozen_inputs, relation.function->definition,
                                      impl_->config.max_frozen_input_bytes));
      node.frozen_input_identity = input_identity.dump();
      node.phase = RequestNodePhase::Ready;
      node.ready_at_us = result.state.now_us;
    }
  }

  EF_RETURN_IF_ERROR(impl_->finish(result));
  EF_RETURN_IF_ERROR(impl_->validate_state(result.state));
  return result;
}

StatusOr<RequestMachineTransition>
RequestMachine::claim_ready(const RequestMachineState& state, std::int64_t now_us,
                            std::size_t max_tasks) const noexcept {
  EF_RETURN_IF_ERROR(impl_->validate_state(state));
  RequestMachineTransition result;
  result.state = state;
  EF_RETURN_IF_ERROR(impl_->advance(result, now_us));
  if (!terminal(result.state.phase)) {
    auto running = static_cast<std::size_t>(std::count_if(
        result.state.nodes.begin(), result.state.nodes.end(),
        [](const auto& node) noexcept { return node.phase == RequestNodePhase::Running; }));
    for (std::size_t i = 0; i < result.state.nodes.size() && result.tasks.size() < max_tasks &&
                            running < impl_->config.max_running_tasks;
         ++i) {
      auto& node = result.state.nodes[i];
      if (node.phase != RequestNodePhase::Ready || node.ready_at_us > now_us)
        continue;

      ++node.attempt;
      node.phase = RequestNodePhase::Running;
      node.started_at_us = now_us;
      node.attempt_deadline_us =
          now_us + std::min(impl_->config.attempt_timeout_us, state.manifest.deadline_us - now_us);
      node.error = {};
      EF_ASSIGN_OR_RETURN(node.task_id, impl_->canonical_task(result.state, i));
      result.tasks.push_back(impl_->task(result.state, i));
      ++running;
    }

    EF_RETURN_IF_ERROR(impl_->finish(result));
  }

  EF_RETURN_IF_ERROR(impl_->validate_state(result.state));
  return result;
}

StatusOr<RequestMachineTransition>
RequestMachine::prepare_completion(const RequestMachineState& state,
                                   const FunctionCompletion& completion,
                                   std::int64_t now_us) const noexcept {
  EF_RETURN_IF_ERROR(impl_->validate_state(state));
  EF_RETURN_IF_ERROR(check(now_us >= state.now_us, "request time cannot move backwards"));
  RequestMachineTransition result;
  result.state = state;
  result.disposition = CompletionDisposition::Ignored;
  std::size_t selected = state.nodes.size();
  if (!terminal(state.phase)) {
    for (std::size_t i = 0; i < state.nodes.size(); ++i) {
      if (state.nodes[i].phase != RequestNodePhase::Running ||
          state.nodes[i].task_id != completion.task_id)
        continue;

      const auto& definition = impl_->descriptor(i).function->definition;
      if (completion.function_id == definition.function_id &&
          completion.function_version == definition.function_version &&
          completion.implementation_identity == definition.implementation_identity)
        selected = i;
      break;
    }
  }

  if (selected == state.nodes.size())
    return result;

  // Expire the current attempt before accepting its reply. A completion arriving exactly at the
  // deadline is too late, even when it reports success.
  EF_RETURN_IF_ERROR(impl_->advance(result, now_us));
  if (terminal(result.state.phase) ||
      result.state.nodes[selected].phase != RequestNodePhase::Running) {
    EF_RETURN_IF_ERROR(impl_->validate_state(result.state));
    return result;
  }

  EF_RETURN_IF_ERROR(valid_error(completion.status));
  auto& node = result.state.nodes[selected];

  if (completion.cancelled) {
    remember_settlement(result, node, RequestAttemptOutcome::Cancelled);
    stop_node(node, RequestNodePhase::Failed,
              Status(ErrorCode::FailedPrecondition, "function attempt was cancelled"));
  } else if (!completion.status.ok()) {
    if (completion.status.code() == ErrorCode::Unavailable ||
        completion.status.code() == ErrorCode::ResourceExhausted)
      impl_->retry_or_fail(result, selected, completion.status, RequestAttemptOutcome::Failed);
    else {
      remember_settlement(result, node, RequestAttemptOutcome::Failed);
      stop_node(node, RequestNodePhase::Failed, completion.status);
    }
  } else {
    const auto& relation = impl_->descriptor(selected);
    EF_ASSIGN_OR_RETURN(auto encoded,
                        row_json(completion.output, relation.function->definition.output_fields,
                                 impl_->config.max_output_bytes));
    (void)encoded;

    Row visible = state.manifest.identity;
    visible.insert(visible.end(), completion.output.begin(), completion.output.end());
    EF_ASSIGN_OR_RETURN(auto stored, make_stored_row(relation, visible, state.manifest.identity));
    EF_ASSIGN_OR_RETURN(auto encoded_stored, row_json(stored, relation.storage_schema.fields,
                                                      impl_->config.max_output_bytes));
    (void)encoded_stored;

    result.output = TableChange{
        .relation = relation.id, .identity = state.manifest.identity, .row = std::move(stored)};

    remember_settlement(result, node, RequestAttemptOutcome::Succeeded);
    clear_work(node);
    node.phase = RequestNodePhase::Succeeded;
    node.has_output = true;
    node.error = {};
  }

  result.disposition = CompletionDisposition::Accepted;
  EF_RETURN_IF_ERROR(impl_->finish(result));
  EF_RETURN_IF_ERROR(impl_->validate_state(result.state));
  return result;
}

StatusOr<RequestMachineTransition>
RequestMachine::advance_time(const RequestMachineState& state, std::int64_t now_us) const noexcept {
  EF_RETURN_IF_ERROR(impl_->validate_state(state));
  RequestMachineTransition result;
  result.state = state;
  EF_RETURN_IF_ERROR(impl_->advance(result, now_us));
  EF_RETURN_IF_ERROR(impl_->validate_state(result.state));

  return result;
}

StatusOr<RequestMachineTransition> RequestMachine::cancel(const RequestMachineState& state,
                                                          std::int64_t now_us) const noexcept {
  EF_RETURN_IF_ERROR(impl_->validate_state(state));
  RequestMachineTransition result;
  result.state = state;
  EF_RETURN_IF_ERROR(impl_->advance(result, now_us));
  if (!terminal(result.state.phase)) {
    result.state.phase = RequestPhase::Cancelled;
    result.state.error = Status(ErrorCode::FailedPrecondition, "request cancelled");
    for (auto& node : result.state.nodes)
      if (!completed(node.phase)) {
        remember_cancel(result, node.task_id);
        remember_settlement(result, node, RequestAttemptOutcome::Cancelled);
        stop_node(node, RequestNodePhase::Skipped, result.state.error);
      }
  }

  EF_RETURN_IF_ERROR(impl_->validate_state(result.state));
  return result;
}

StatusOr<std::vector<FunctionTask>>
RequestMachine::outbox(const RequestMachineState& state) const noexcept {
  EF_RETURN_IF_ERROR(impl_->validate_state(state));
  std::vector<FunctionTask> result;
  for (std::size_t i = 0; i < state.nodes.size(); ++i)
    if (state.nodes[i].phase == RequestNodePhase::Running)
      result.push_back(impl_->task(state, i));

  return result;
}

StatusOr<std::string> RequestMachine::encode(const RequestMachineState& state) const noexcept {
  EF_RETURN_IF_ERROR(impl_->validate_state(state));
  EF_ASSIGN_OR_RETURN(auto value, impl_->state_json(state));
  return value.dump();
}

StatusOr<RequestMachineState>
RequestMachine::decode(std::string_view bytes,
                       const RequestManifest& expected_manifest) const noexcept {
  if (bytes.size() > impl_->config.max_state_bytes)
    return Status(ErrorCode::ResourceExhausted, "request state bytes exceed budget");

  EF_RETURN_IF_ERROR(check(shallow_json(bytes), "invalid or excessive request JSON nesting"));
  EF_ASSIGN_OR_RETURN(auto expected, impl_->normalize(expected_manifest));
  EF_ASSIGN_OR_RETURN(auto value, parse_json_exact(bytes));
  EF_RETURN_IF_ERROR(members(value, {"format", "manifest", "phase", "now_us", "nodes", "error"}));
  EF_ASSIGN_OR_RETURN(const auto version, read_unsigned(value["format"]));
  EF_RETURN_IF_ERROR(check(version == 1, "unsupported request state format"));

  EF_ASSIGN_OR_RETURN(auto expected_json, impl_->manifest_json(expected));
  EF_RETURN_IF_ERROR(check(value["manifest"] == expected_json,
                           "encoded request manifest differs from trusted request identity"));

  RequestMachineState result;
  result.manifest = std::move(expected);
  EF_ASSIGN_OR_RETURN(const auto phase, read_unsigned(value["phase"]));
  EF_RETURN_IF_ERROR(check(phase <= static_cast<unsigned>(RequestPhase::Cancelled),
                           "invalid encoded request phase"));
  result.phase = static_cast<RequestPhase>(phase);
  EF_ASSIGN_OR_RETURN(result.now_us, read_time(value["now_us"]));
  EF_RETURN_IF_ERROR(read_error(value["error"], result.error));

  const auto& nodes = value["nodes"];
  EF_RETURN_IF_ERROR(check(nodes.is_array() && nodes.size() == impl_->relations.size(),
                           "encoded request node set differs from Catalog"));
  for (std::size_t i = 0; i < nodes.size(); ++i) {
    const auto& encoded = nodes[i];
    EF_RETURN_IF_ERROR(
        members(encoded, {"relation", "phase", "attempt", "ready_at_us", "started_at_us",
                          "attempt_deadline_us", "task_id", "frozen_inputs",
                          "frozen_input_identity", "has_output", "error"}));

    RequestNodeState node;
    EF_ASSIGN_OR_RETURN(const auto relation, read_unsigned(encoded["relation"]));
    EF_RETURN_IF_ERROR(
        check(relation == impl_->relations[i], "encoded request RelationId differs from Catalog"));
    node.relation = static_cast<RelationId>(relation);

    EF_ASSIGN_OR_RETURN(const auto node_phase, read_unsigned(encoded["phase"]));
    EF_RETURN_IF_ERROR(check(node_phase <= static_cast<unsigned>(RequestNodePhase::Skipped),
                             "invalid encoded node phase"));
    node.phase = static_cast<RequestNodePhase>(node_phase);

    EF_ASSIGN_OR_RETURN(const auto attempt, read_unsigned(encoded["attempt"]));
    EF_RETURN_IF_ERROR(
        check(attempt <= impl_->config.max_attempts, "encoded attempt exceeds limit"));
    node.attempt = static_cast<std::uint32_t>(attempt);
    EF_ASSIGN_OR_RETURN(node.ready_at_us, read_time(encoded["ready_at_us"]));
    EF_ASSIGN_OR_RETURN(node.started_at_us, read_time(encoded["started_at_us"]));
    EF_ASSIGN_OR_RETURN(node.attempt_deadline_us, read_time(encoded["attempt_deadline_us"]));

    EF_ASSIGN_OR_RETURN(node.task_id, read_string(encoded["task_id"]));
    EF_ASSIGN_OR_RETURN(node.frozen_input_identity, read_string(encoded["frozen_input_identity"]));
    EF_RETURN_IF_ERROR(
        check(encoded["has_output"].is_boolean(), "encoded output presence is not Bool"));
    node.has_output = encoded["has_output"].get<bool>();
    EF_RETURN_IF_ERROR(read_error(encoded["error"], node.error));

    const auto& inputs = encoded["frozen_inputs"];
    EF_RETURN_IF_ERROR(check(inputs.is_array(), "encoded frozen inputs are not an array"));
    if (!inputs.empty()) {
      const auto& descriptor = impl_->descriptor(i);
      EF_RETURN_IF_ERROR(check(descriptor.kind == RelationKind::FunctionView &&
                                   inputs.size() == descriptor.function->definition.inputs.size(),
                               "encoded frozen input signature is invalid"));

      for (std::size_t input = 0; input < inputs.size(); ++input) {
        if (inputs[input].is_null())
          node.frozen_inputs.emplace_back(std::nullopt);
        else {
          EF_ASSIGN_OR_RETURN(
              auto row,
              read_row_json(inputs[input], descriptor.function->definition.inputs[input].fields));
          node.frozen_inputs.emplace_back(std::move(row));
        }
      }
    }

    result.nodes.push_back(std::move(node));
  }

  EF_RETURN_IF_ERROR(impl_->validate_state(result));
  EF_ASSIGN_OR_RETURN(auto canonical, encode(result));
  EF_RETURN_IF_ERROR(check(canonical == bytes, "request state encoding is not canonical"));
  return result;
}
} // namespace eventflow
