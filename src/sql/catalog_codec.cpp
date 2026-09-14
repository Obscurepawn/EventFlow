#include "eventflow/catalog_codec.hpp"
#include <algorithm>
#include <limits>

namespace eventflow {
namespace {
Status invalid(std::string_view message) noexcept {
  return {ErrorCode::InvalidArgument, message};
}

StatusOr<const Json*> member(const Json& object, const char* name) noexcept {
  if (!object.is_object())
    return invalid("expected Catalog metadata object");

  auto found = object.find(name);
  if (found == object.end())
    return invalid("missing Catalog metadata field");

  return &*found;
}

StatusOr<std::string> text_field(const Json& object, const char* name) noexcept {
  EF_ASSIGN_OR_RETURN(auto value, member(object, name));
  if (!value->is_string())
    return invalid("expected Catalog metadata string");

  return value->get<std::string>();
}

StatusOr<bool> bool_field(const Json& object, const char* name) noexcept {
  EF_ASSIGN_OR_RETURN(auto value, member(object, name));
  if (!value->is_boolean())
    return invalid("expected Catalog metadata boolean");

  return value->get<bool>();
}

StatusOr<std::uint64_t> number(const Json& value, std::uint64_t maximum = UINT64_MAX) noexcept {
  if (!value.is_number_integer() || (!value.is_number_unsigned() && value.get<std::int64_t>() < 0))
    return invalid("expected nonnegative Catalog integer");

  auto result = value.get<std::uint64_t>();
  if (result > maximum)
    return invalid("Catalog integer is out of range");

  return result;
}

StatusOr<std::uint64_t> number_field(const Json& object, const char* name,
                                     std::uint64_t maximum = UINT64_MAX) noexcept {
  EF_ASSIGN_OR_RETURN(auto value, member(object, name));
  return number(*value, maximum);
}

StatusOr<const Json*> array_field(const Json& object, const char* name,
                                  std::size_t limit) noexcept {
  EF_ASSIGN_OR_RETURN(auto value, member(object, name));
  if (!value->is_array())
    return invalid("expected Catalog metadata array");
  if (value->size() > limit)
    return Status(ErrorCode::ResourceExhausted, "Catalog array limit exceeded");

  return value;
}

StatusOr<std::vector<Field>> decode_fields(const Json& object, const char* name,
                                           std::size_t limit) noexcept {
  EF_ASSIGN_OR_RETURN(auto array, array_field(object, name, limit));
  std::vector<Field> result;
  for (const auto& entry : *array) {
    EF_ASSIGN_OR_RETURN(auto id, number_field(entry, "id", UINT32_MAX));
    EF_ASSIGN_OR_RETURN(auto label, text_field(entry, "name"));
    EF_ASSIGN_OR_RETURN(auto type_name, text_field(entry, "type"));
    EF_ASSIGN_OR_RETURN(auto type, parse_data_type(type_name));
    EF_ASSIGN_OR_RETURN(auto nullable, bool_field(entry, "nullable"));
    result.push_back({.id = static_cast<std::uint32_t>(id),
                      .name = std::move(label),
                      .type = type,
                      .nullable = nullable});
  }

  return result;
}

StatusOr<std::vector<DataType>> decode_types(const Json& object, const char* name,
                                             std::size_t limit) noexcept {
  EF_ASSIGN_OR_RETURN(auto array, array_field(object, name, limit));
  std::vector<DataType> result;
  for (const auto& value : *array) {
    if (!value.is_string())
      return invalid("expected Catalog type name");

    EF_ASSIGN_OR_RETURN(auto type, parse_data_type(value.get<std::string>()));
    result.push_back(type);
  }

  return result;
}

StatusOr<std::vector<std::size_t>> decode_slots(const Json& object, const char* name, bool nullable,
                                                std::size_t limit) noexcept {
  EF_ASSIGN_OR_RETURN(auto array, array_field(object, name, limit));
  std::vector<std::size_t> result;
  for (const auto& value : *array) {
    if (nullable && value.is_null())
      result.push_back(kNoSlot);
    else {
      EF_ASSIGN_OR_RETURN(auto slot, number(value, std::numeric_limits<std::size_t>::max()));
      result.push_back(static_cast<std::size_t>(slot));
    }
  }

  return result;
}

struct Decoder {
  CatalogCodecConfig config;
  std::size_t expression_nodes = 0;

  StatusOr<std::vector<Field>> fields(const Json& object, const char* name) const noexcept {
    return decode_fields(object, name, config.max_fields_per_relation);
  }

  StatusOr<std::vector<DataType>> types(const Json& object, const char* name) const noexcept {
    return decode_types(object, name, config.max_fields_per_relation);
  }

  StatusOr<std::vector<std::size_t>> slots(const Json& object, const char* name,
                                           bool nullable = false) const noexcept {
    return decode_slots(object, name, nullable, config.max_fields_per_relation);
  }

  StatusOr<BoundExprPtr> expression(const Json& value, std::size_t depth = 0) noexcept {
    if (value.is_null())
      return BoundExprPtr{};
    if (depth > config.max_expression_depth || expression_nodes == config.max_expression_nodes)
      return Status(ErrorCode::ResourceExhausted, "Catalog expression budget exceeded");

    ++expression_nodes;
    auto result = std::make_shared<BoundExpr>();
    EF_ASSIGN_OR_RETURN(
        auto kind, number_field(value, "kind", static_cast<std::uint64_t>(BoundExprKind::Cast)));
    result->kind = static_cast<BoundExprKind>(kind);
    EF_ASSIGN_OR_RETURN(auto type, text_field(value, "type"));
    EF_ASSIGN_OR_RETURN(result->type, parse_data_type(type));
    EF_ASSIGN_OR_RETURN(result->nullable, bool_field(value, "nullable"));
    EF_ASSIGN_OR_RETURN(result->untyped_null, bool_field(value, "untyped_null"));
    EF_ASSIGN_OR_RETURN(auto slot,
                        number_field(value, "slot", std::numeric_limits<std::size_t>::max()));
    result->slot = static_cast<std::size_t>(slot);
    EF_ASSIGN_OR_RETURN(result->op, text_field(value, "op"));
    EF_ASSIGN_OR_RETURN(auto literal, member(value, "literal"));
    if (!literal->is_null()) {
      EF_ASSIGN_OR_RETURN(result->literal, value_from_json(*literal, result->type, false));
    }

    EF_ASSIGN_OR_RETURN(auto args, array_field(value, "args", 1024));
    for (const auto& argument : *args) {
      EF_ASSIGN_OR_RETURN(auto child, expression(argument, depth + 1));
      result->args.push_back(std::move(child));
    }

    return BoundExprPtr(std::move(result));
  }

  StatusOr<std::vector<BoundExprPtr>> expressions(const Json& value, const char* name) noexcept {
    EF_ASSIGN_OR_RETURN(auto array, array_field(value, name, config.max_fields_per_relation));
    std::vector<BoundExprPtr> result;
    for (const auto& item : *array) {
      EF_ASSIGN_OR_RETURN(auto parsed, expression(item));
      result.push_back(std::move(parsed));
    }

    return result;
  }

  Status domain(const Json& json, std::string& name, std::vector<Field>& identity) noexcept {
    if (json.contains("identity_domain")) {
      EF_ASSIGN_OR_RETURN(name, text_field(json, "identity_domain"));
      EF_ASSIGN_OR_RETURN(identity, fields(json, "identity_fields"));
    }

    return {};
  }

  StatusOr<std::shared_ptr<const BoundPlan>> plan(const Json& json,
                                                  const std::string& prefix) noexcept {
    auto result = std::make_shared<BoundPlan>();
    EF_ASSIGN_OR_RETURN(result->catalog_version, number_field(json, "catalog_version"));
    result->catalog_fingerprint = prefix;
    EF_ASSIGN_OR_RETURN(auto left, number_field(json, "left", UINT32_MAX));
    result->left = static_cast<RelationId>(left);
    EF_ASSIGN_OR_RETURN(auto right, member(json, "right"));
    if (!right->is_null()) {
      EF_ASSIGN_OR_RETURN(auto id, number(*right, UINT32_MAX));
      result->right = static_cast<RelationId>(id);
    }

    EF_ASSIGN_OR_RETURN(auto filter, member(json, "filter"));
    EF_ASSIGN_OR_RETURN(result->filter, expression(*filter));
    EF_ASSIGN_OR_RETURN(result->group_by, expressions(json, "group_by"));
    EF_ASSIGN_OR_RETURN(result->projection, expressions(json, "projection"));
    EF_ASSIGN_OR_RETURN(auto aggregates,
                        array_field(json, "aggregates", config.max_fields_per_relation));
    for (const auto& entry : *aggregates) {
      EF_ASSIGN_OR_RETURN(
          auto kind,
          number_field(entry, "kind", static_cast<std::uint64_t>(AggregateKind::Average)));
      EF_ASSIGN_OR_RETURN(auto argument, member(entry, "argument"));
      EF_ASSIGN_OR_RETURN(auto expr, expression(*argument));
      EF_ASSIGN_OR_RETURN(auto type_name, text_field(entry, "result_type"));
      EF_ASSIGN_OR_RETURN(auto type, parse_data_type(type_name));
      result->aggregates.push_back({.kind = static_cast<AggregateKind>(kind),
                                    .argument = std::move(expr),
                                    .result_type = type});
    }

    EF_ASSIGN_OR_RETURN(result->output_fields, fields(json, "output_fields"));
    EF_ASSIGN_OR_RETURN(result->identity_types, types(json, "identity_types"));
    EF_ASSIGN_OR_RETURN(result->partition_types, types(json, "partition_types"));
    EF_ASSIGN_OR_RETURN(result->partition_visible_slots,
                        slots(json, "partition_visible_slots", true));
    EF_ASSIGN_OR_RETURN(result->unique_per_partition, bool_field(json, "unique_per_partition"));
    EF_RETURN_IF_ERROR(domain(json, result->identity_domain, result->identity_fields));
    EF_ASSIGN_OR_RETURN(auto scope, member(json, "inferred_scope"));
    if (!scope->is_null()) {
      if (!scope->is_array() || scope->size() != result->partition_types.size())
        return invalid("invalid Catalog inferred scope");

      Row row;
      for (std::size_t i = 0; i < scope->size(); ++i) {
        EF_ASSIGN_OR_RETURN(auto value,
                            value_from_json((*scope)[i], result->partition_types[i], false));
        row.push_back(std::move(value));
      }

      result->inferred_scope = std::move(row);
    }

    return std::shared_ptr<const BoundPlan>(std::move(result));
  }

  StatusOr<std::shared_ptr<const RelationDescriptor>> relation(const Json& json,
                                                               const std::string& prefix) noexcept {
    auto result = std::make_shared<RelationDescriptor>();
    EF_ASSIGN_OR_RETURN(auto id, number_field(json, "id", UINT32_MAX));
    result->id = static_cast<RelationId>(id);
    EF_ASSIGN_OR_RETURN(result->name, text_field(json, "name"));
    EF_ASSIGN_OR_RETURN(auto schema, member(json, "storage_schema"));
    EF_ASSIGN_OR_RETURN(auto schema_fields,
                        array_field(*schema, "fields", config.max_fields_per_relation));
    (void)schema_fields;
    EF_ASSIGN_OR_RETURN(auto primary,
                        array_field(*schema, "primary_key", config.max_fields_per_relation));
    EF_ASSIGN_OR_RETURN(auto partition,
                        array_field(*schema, "partition_key", config.max_fields_per_relation));
    (void)primary;
    (void)partition;
    EF_ASSIGN_OR_RETURN(result->storage_schema, schema_from_json(*schema));
    EF_ASSIGN_OR_RETURN(auto visible, number_field(json, "visible_count"));
    if (visible > config.max_fields_per_relation)
      return Status(ErrorCode::ResourceExhausted, "Catalog visible field budget exceeded");

    result->visible_count = static_cast<std::size_t>(visible);
    EF_ASSIGN_OR_RETURN(result->physical_key_slots, slots(json, "physical_key_slots"));
    EF_ASSIGN_OR_RETURN(result->partition_slots, slots(json, "partition_slots"));
    EF_ASSIGN_OR_RETURN(result->partition_visible_slots,
                        slots(json, "partition_visible_slots", true));
    EF_ASSIGN_OR_RETURN(result->identity_types, types(json, "identity_types"));
    EF_ASSIGN_OR_RETURN(result->partition_types, types(json, "partition_types"));
    EF_ASSIGN_OR_RETURN(result->unique_per_partition, bool_field(json, "unique_per_partition"));
    EF_ASSIGN_OR_RETURN(auto state_id, number_field(json, "state_id", UINT32_MAX));
    result->state_id = static_cast<std::uint32_t>(state_id);
    EF_RETURN_IF_ERROR(domain(json, result->identity_domain, result->identity_fields));
    if (json.contains("base_write_role")) {
      EF_ASSIGN_OR_RETURN(auto role, text_field(json, "base_write_role"));
      if (role == "request_capture")
        result->base_write_role = BaseWriteRole::RequestCapture;
      else if (role == "runtime_status")
        result->base_write_role = BaseWriteRole::RuntimeStatus;
      else if (role == "trigger_audit")
        result->base_write_role = BaseWriteRole::TriggerAudit;
      else
        return invalid("unknown Catalog write role");
    }

    EF_ASSIGN_OR_RETURN(auto sql, member(json, "plan"));
    if (!sql->is_null()) {
      EF_ASSIGN_OR_RETURN(result->plan, plan(*sql, prefix));
      result->kind = RelationKind::SqlView;
    }

    if (json.contains("kind")) {
      EF_ASSIGN_OR_RETURN(auto kind, text_field(json, "kind"));
      if (kind == "function_view")
        result->kind = RelationKind::FunctionView;
      else if (kind == "latest_view")
        result->kind = RelationKind::LatestView;
      else
        return invalid("unknown Catalog relation kind");
    }

    if (json.contains("function")) {
      EF_ASSIGN_OR_RETURN(auto function, member(json, "function"));
      EF_ASSIGN_OR_RETURN(auto definition, member(*function, "definition"));
      auto value = std::make_shared<FunctionViewPlan>();
      EF_ASSIGN_OR_RETURN(value->catalog_version, number_field(*function, "catalog_version"));
      value->catalog_fingerprint = prefix;
      auto& target = value->definition;
      EF_ASSIGN_OR_RETURN(target.name, text_field(*definition, "name"));
      EF_ASSIGN_OR_RETURN(target.function_id, text_field(*definition, "function_id"));
      EF_ASSIGN_OR_RETURN(target.function_version, number_field(*definition, "function_version"));
      EF_ASSIGN_OR_RETURN(target.implementation_identity,
                          text_field(*definition, "implementation_identity"));
      EF_ASSIGN_OR_RETURN(target.output_fields, fields(*definition, "output_fields"));
      EF_ASSIGN_OR_RETURN(auto inputs, array_field(*definition, "inputs", 32));

      for (const auto& input : *inputs) {
        FunctionInput entry;
        EF_ASSIGN_OR_RETURN(entry.alias, text_field(input, "alias"));
        EF_ASSIGN_OR_RETURN(auto relation, number_field(input, "relation", UINT32_MAX));
        entry.relation = static_cast<RelationId>(relation);
        EF_ASSIGN_OR_RETURN(entry.required, bool_field(input, "required"));
        EF_ASSIGN_OR_RETURN(entry.fields, fields(input, "fields"));
        target.inputs.push_back(std::move(entry));
      }

      result->function = std::move(value);
    }

    if (json.contains("candidate")) {
      EF_ASSIGN_OR_RETURN(auto annotation, member(json, "candidate"));
      auto value = std::make_shared<CandidateAnnotation>();
      EF_ASSIGN_OR_RETURN(auto format, number_field(*annotation, "format", UINT32_MAX));
      value->format = static_cast<std::uint32_t>(format);
      EF_ASSIGN_OR_RETURN(value->definition.name, text_field(*annotation, "name"));
      EF_ASSIGN_OR_RETURN(value->definition.scope_fields, fields(*annotation, "scope_fields"));
      EF_ASSIGN_OR_RETURN(value->definition.payload_fields, fields(*annotation, "payload_fields"));
      result->candidate = std::move(value);
    }

    if (json.contains("request_status")) {
      EF_ASSIGN_OR_RETURN(auto annotation, member(json, "request_status"));
      auto value = std::make_shared<RequestStatusAnnotation>();
      EF_ASSIGN_OR_RETURN(auto format, number_field(*annotation, "format", UINT32_MAX));
      value->format = static_cast<std::uint32_t>(format);
      EF_ASSIGN_OR_RETURN(value->definition.name, text_field(*annotation, "name"));
      EF_ASSIGN_OR_RETURN(value->definition.request_domain,
                          text_field(*annotation, "request_domain"));
      EF_ASSIGN_OR_RETURN(value->definition.identity_fields,
                          fields(*annotation, "identity_fields"));
      EF_ASSIGN_OR_RETURN(auto candidate,
                          number_field(*annotation, "candidate_relation", UINT32_MAX));
      value->definition.candidate_relation = static_cast<RelationId>(candidate);
      result->request_status = std::move(value);
    }

    if (json.contains("latest")) {
      EF_ASSIGN_OR_RETURN(auto annotation, member(json, "latest"));
      EF_ASSIGN_OR_RETURN(auto definition, member(*annotation, "definition"));
      auto value = std::make_shared<LatestViewPlan>();
      EF_ASSIGN_OR_RETURN(auto format, number_field(*annotation, "format", UINT32_MAX));
      value->format = static_cast<std::uint32_t>(format);
      EF_ASSIGN_OR_RETURN(value->catalog_version, number_field(*annotation, "catalog_version"));
      value->catalog_fingerprint = prefix;
      EF_ASSIGN_OR_RETURN(value->definition.name, text_field(*definition, "name"));
      EF_ASSIGN_OR_RETURN(auto source, number_field(*definition, "result_relation", UINT32_MAX));
      EF_ASSIGN_OR_RETURN(auto status,
                          number_field(*definition, "request_status_relation", UINT32_MAX));
      EF_ASSIGN_OR_RETURN(auto candidate,
                          number_field(*definition, "candidate_relation", UINT32_MAX));
      value->definition.result_relation = static_cast<RelationId>(source);
      value->definition.request_status_relation = static_cast<RelationId>(status);
      value->definition.candidate_relation = static_cast<RelationId>(candidate);
      result->latest = std::move(value);
    }

    return std::shared_ptr<const RelationDescriptor>(std::move(result));
  }

  StatusOr<ObservationRuleDescriptor> observation(const Json& json) noexcept {
    ObservationRuleDescriptor result;
    EF_ASSIGN_OR_RETURN(auto format, number_field(json, "format", 1));
    if (format != 1)
      return invalid("unknown observation wire format");

    EF_ASSIGN_OR_RETURN(auto id, number_field(json, "id", UINT32_MAX));
    result.id = static_cast<std::uint32_t>(id);
    EF_ASSIGN_OR_RETURN(auto after,
                        number_field(json, "after_relation_count", config.max_relations));
    result.after_relation_count = static_cast<std::size_t>(after);
    EF_ASSIGN_OR_RETURN(auto object, member(json, "definition"));
    auto& definition = result.definition;
    EF_ASSIGN_OR_RETURN(definition.name, text_field(*object, "name"));
    EF_ASSIGN_OR_RETURN(definition.version, number_field(*object, "version"));
    EF_ASSIGN_OR_RETURN(auto program, number_field(*object, "program_kind", 1));
    definition.program_kind = static_cast<ObservationProgramKind>(program);
    EF_ASSIGN_OR_RETURN(definition.recipe_id, text_field(*object, "recipe_id"));
    EF_ASSIGN_OR_RETURN(definition.recipe_version, number_field(*object, "recipe_version"));
    EF_ASSIGN_OR_RETURN(definition.implementation_identity,
                        text_field(*object, "implementation_identity"));
    EF_ASSIGN_OR_RETURN(definition.program_fingerprint, text_field(*object, "program_fingerprint"));
    EF_ASSIGN_OR_RETURN(definition.scope_fields, fields(*object, "scope_fields"));
    EF_ASSIGN_OR_RETURN(auto sink, member(*object, "sink_relation"));
    if (!sink->is_null()) {
      EF_ASSIGN_OR_RETURN(auto sink_id, number(*sink, UINT32_MAX));
      definition.sink_relation = static_cast<std::uint32_t>(sink_id);
    }

    EF_ASSIGN_OR_RETURN(auto sink_bytes,
                        number_field(*object, "max_sink_row_bytes", 16 * 1024 * 1024));
    definition.max_sink_row_bytes = static_cast<std::size_t>(sink_bytes);
    EF_ASSIGN_OR_RETURN(auto targets, array_field(*object, "activation_targets", 64));

    for (const auto& target : *targets) {
      EF_ASSIGN_OR_RETURN(auto target_id, number(target, UINT32_MAX));
      definition.activation_targets.push_back(static_cast<std::uint32_t>(target_id));
    }

    EF_ASSIGN_OR_RETURN(auto inputs, array_field(*object, "inputs", 64));
    for (const auto& entry : *inputs) {
      ObservationInputDefinition input;
      EF_ASSIGN_OR_RETURN(input.id, text_field(entry, "id"));
      EF_ASSIGN_OR_RETURN(auto kind, number_field(entry, "kind", 5));
      input.kind = static_cast<ObservationInputKind>(kind);
      EF_ASSIGN_OR_RETURN(input.event_kind, text_field(entry, "event_kind"));
      EF_ASSIGN_OR_RETURN(auto relation, member(entry, "relation"));
      if (!relation->is_null()) {
        EF_ASSIGN_OR_RETURN(auto relation_id, number(*relation, UINT32_MAX));
        input.relation = static_cast<std::uint32_t>(relation_id);
      }

      EF_ASSIGN_OR_RETURN(input.source_kind, text_field(entry, "source_kind"));
      EF_ASSIGN_OR_RETURN(input.plan, text_field(entry, "plan"));
      EF_ASSIGN_OR_RETURN(input.policy_id, text_field(entry, "policy_id"));
      EF_ASSIGN_OR_RETURN(auto mask, number_field(entry, "change_mask", UINT8_MAX));
      input.change_mask = static_cast<std::uint8_t>(mask);
      EF_ASSIGN_OR_RETURN(input.source_fields, fields(entry, "source_fields"));
      EF_ASSIGN_OR_RETURN(auto event_bytes,
                          number_field(entry, "max_event_bytes", 16 * 1024 * 1024));
      input.max_event_bytes = static_cast<std::size_t>(event_bytes);
      EF_ASSIGN_OR_RETURN(auto completion_targets, array_field(entry, "completion_targets", 64));

      for (const auto& target : *completion_targets) {
        EF_ASSIGN_OR_RETURN(auto target_id, number(target, UINT32_MAX));
        input.completion_targets.push_back(static_cast<std::uint32_t>(target_id));
      }

      definition.inputs.push_back(std::move(input));
    }

    EF_ASSIGN_OR_RETURN(auto reads, array_field(*object, "reads", 64));
    for (const auto& entry : *reads) {
      ObservationReadDefinition read;
      EF_ASSIGN_OR_RETURN(read.alias, text_field(entry, "alias"));
      EF_ASSIGN_OR_RETURN(auto relation_id, number_field(entry, "relation", UINT32_MAX));
      read.relation = static_cast<std::uint32_t>(relation_id);
      EF_ASSIGN_OR_RETURN(read.fields, fields(entry, "fields"));
      EF_ASSIGN_OR_RETURN(auto row_bytes, number_field(entry, "max_row_bytes", 16 * 1024 * 1024));
      read.max_row_bytes = static_cast<std::size_t>(row_bytes);
      definition.reads.push_back(std::move(read));
    }

    EF_ASSIGN_OR_RETURN(auto layouts, array_field(json, "event_fields", 64));
    for (const auto& layout : *layouts) {
      Json wrapper{{"fields", layout}};
      EF_ASSIGN_OR_RETURN(auto decoded,
                          decode_fields(wrapper, "fields", 2 * config.max_fields_per_relation + 3));
      result.event_fields.push_back(std::move(decoded));
    }

    return result;
  }
};

Status config_ok(const CatalogCodecConfig& config) noexcept {
  if (!config.max_bytes || !config.max_relations || !config.max_fields_per_relation ||
      !config.max_provenance_bytes || !config.max_expression_nodes ||
      !config.max_expression_depth || config.max_expression_depth > 128 ||
      config.max_fields_per_relation > (std::numeric_limits<std::size_t>::max() - 3) / 2)
    return invalid("invalid Catalog codec limits");

  return {};
}

Status document_depth(std::string_view bytes) noexcept {
  bool quoted = false, escaped = false;
  std::size_t depth = 0;
  for (char c : bytes) {
    if (quoted) {
      if (escaped)
        escaped = false;
      else if (c == '\\')
        escaped = true;
      else if (c == '"')
        quoted = false;
      continue;
    }

    if (c == '"')
      quoted = true;
    else if (c == '{' || c == '[') {
      if (++depth > 512)
        return {ErrorCode::ResourceExhausted, "Catalog JSON nesting budget exceeded"};
    } else if ((c == '}' || c == ']') && depth)
      --depth;
  }

  return {};
}
} // namespace

StatusOr<CatalogSnapshot> decode_catalog(std::string_view bytes,
                                         const CatalogCodecConfig& config) noexcept {
  EF_RETURN_IF_ERROR(config_ok(config));
  if (bytes.size() > config.max_bytes)
    return Status(ErrorCode::ResourceExhausted, "Catalog wire byte budget exceeded");

  EF_RETURN_IF_ERROR(document_depth(bytes));
  EF_ASSIGN_OR_RETURN(auto json, parse_json_exact(bytes));
  EF_ASSIGN_OR_RETURN(auto format, number_field(json, "layout_version"));
  if (format != 1)
    return invalid("unknown Catalog wire format");

  CatalogSnapshot result;
  EF_ASSIGN_OR_RETURN(result.version, number_field(json, "catalog_version"));
  EF_ASSIGN_OR_RETURN(auto relations, array_field(json, "relations", config.max_relations));
  Decoder decoder{.config = config, .expression_nodes = 0};
  const Json* observations = nullptr;
  if (json.contains("observations")) {
    EF_ASSIGN_OR_RETURN(observations, array_field(json, "observations", 256));
    for (const auto& entry : *observations) {
      EF_ASSIGN_OR_RETURN(auto observation, decoder.observation(entry));
      result.observations.push_back(std::move(observation));
    }
  }

  Json prefix{{"layout_version", 1}, {"catalog_version", 0}, {"relations", Json::array()}};
  std::size_t observation_index = 0;
  const auto append_observations = [&]() noexcept {
    while (observation_index < result.observations.size() &&
           result.observations[observation_index].after_relation_count == result.relations.size()) {
      if (!prefix.contains("observations"))
        prefix["observations"] = Json::array();

      prefix["observations"].push_back((*observations)[observation_index]);
      ++observation_index;
    }

    prefix["catalog_version"] = result.relations.size() + observation_index;
  };
  append_observations();
  std::size_t provenance_bytes = 0;

  for (const auto& entry : *relations) {
    const bool has_plan = (entry.contains("plan") && !entry["plan"].is_null()) ||
                          entry.contains("function") || entry.contains("latest");
    const auto binding = has_plan ? prefix.dump() : std::string{};
    if (has_plan) {
      if (binding.size() > config.max_provenance_bytes - provenance_bytes)
        return Status(ErrorCode::ResourceExhausted, "Catalog binding provenance budget exceeded");

      provenance_bytes += binding.size();
    }

    EF_ASSIGN_OR_RETURN(auto relation, decoder.relation(entry, binding));
    result.relations.push_back(std::move(relation));
    prefix["relations"].push_back(entry);
    append_observations();
  }

  // Rebuilding validates all semantic invariants and exact canonical fields, including
  // metadata ignored by structural parsing. Unknown or altered fields cannot disappear.
  result.fingerprint = json.dump();
  EF_ASSIGN_OR_RETURN(auto catalog, Catalog::from_snapshot(result));

  return catalog->snapshot();
}

StatusOr<std::string> encode_catalog(const CatalogSnapshot& catalog,
                                     const CatalogCodecConfig& config) noexcept {
  EF_RETURN_IF_ERROR(config_ok(config));
  if (catalog.relations.size() > config.max_relations ||
      catalog.fingerprint.size() > config.max_bytes)
    return Status(ErrorCode::ResourceExhausted, "Catalog exceeds configured wire limits");

  EF_ASSIGN_OR_RETURN(auto validated, Catalog::from_snapshot(catalog));
  EF_ASSIGN_OR_RETURN(auto snapshot, validated->snapshot());
  EF_ASSIGN_OR_RETURN(auto decoded, decode_catalog(snapshot.fingerprint, config));
  return std::move(decoded.fingerprint);
}
} // namespace eventflow
