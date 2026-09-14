#include "eventflow/catalog.hpp"
#include "binder_internal.hpp"

#include <algorithm>
#include <set>

namespace eventflow {
namespace {
Status name_ok(const std::string& name) noexcept {
  std::string folded = name;
  for (auto& ch : folded)
    if (ch >= 'A' && ch <= 'Z')
      ch = static_cast<char>(ch + ('a' - 'A'));

  return check(!name.empty() && valid_utf8(name) && !folded.starts_with("__ef_"),
               "Invalid or reserved identifier: " + name);
}

Status validate_kind(const RelationDescriptor& relation) noexcept {
  if (relation.candidate || relation.request_status) {
    EF_RETURN_IF_ERROR(
        check(relation.kind == RelationKind::BaseTable &&
                  relation.base_write_role == BaseWriteRole::RuntimeStatus &&
                  !(relation.candidate && relation.request_status) &&
                  relation.identity_domain.empty() && relation.identity_fields.empty(),
              "Lifecycle annotations require an exclusive RuntimeStatus base table"));
    EF_RETURN_IF_ERROR(check((!relation.candidate || relation.candidate->format == 1) &&
                                 (!relation.request_status || relation.request_status->format == 1),
                             "Unknown lifecycle metadata format"));
  }

  if (relation.kind == RelationKind::BaseTable) {
    EF_RETURN_IF_ERROR(check(relation.base_write_role == BaseWriteRole::PublicInput ||
                                 relation.base_write_role == BaseWriteRole::RequestCapture ||
                                 relation.base_write_role == BaseWriteRole::RuntimeStatus ||
                                 relation.base_write_role == BaseWriteRole::TriggerAudit,
                             "Unknown base write role"));
    return check(!relation.plan && !relation.function && !relation.latest,
                 "Base table cannot carry a view plan");
  }

  EF_RETURN_IF_ERROR(check(relation.base_write_role == BaseWriteRole::PublicInput,
                           "Base write roles cannot be attached to views"));
  if (relation.kind == RelationKind::SqlView)
    return check(relation.plan && !relation.function && !relation.latest,
                 "SQL view must carry only its SQL plan");
  if (relation.kind == RelationKind::FunctionView)
    return check(!relation.plan && relation.function && !relation.latest,
                 "Function view must carry only its function plan");
  if (relation.kind == RelationKind::LatestView)
    return check(!relation.plan && !relation.function && relation.latest &&
                     relation.latest->format == 1 && relation.identity_domain.empty() &&
                     relation.identity_fields.empty(),
                 "Latest view requires its versioned plan and no request domain");

  return Status(ErrorCode::InvalidArgument, "Unknown relation kind");
}

Status validate_domain(const RelationDescriptor& relation,
                       const CatalogSnapshot& catalog) noexcept {
  EF_RETURN_IF_ERROR(check(!relation.identity_domain.empty() || relation.identity_fields.empty(),
                           "Identity fields require a request domain"));
  const auto& domain = relation.request_status ? relation.request_status->definition.request_domain
                                               : relation.identity_domain;
  const auto& identity = relation.request_status
                             ? relation.request_status->definition.identity_fields
                             : relation.identity_fields;
  if (domain.empty())
    return {};

  for (const auto& existing : catalog.relations) {
    const auto& existing_domain = existing->request_status
                                      ? existing->request_status->definition.request_domain
                                      : existing->identity_domain;
    const auto& existing_identity = existing->request_status
                                        ? existing->request_status->definition.identity_fields
                                        : existing->identity_fields;
    if (existing_domain == domain)
      EF_RETURN_IF_ERROR(
          check(existing->partition_types == relation.partition_types &&
                    existing->identity_types == relation.identity_types &&
                    sql_internal::same_fields(existing_identity, identity),
                "Request domain has a different physical identity or partition layout"));
  }

  return {};
}

StatusOr<RelationDescriptor> table_descriptor(const CreateTableAst& definition,
                                              const DefinitionOptions& options,
                                              RelationId id) noexcept {
  EF_RETURN_IF_ERROR(name_ok(definition.name));
  EF_RETURN_IF_ERROR(check(definition.partition_key.empty() || options.partition_key.empty() ||
                               definition.partition_key == options.partition_key,
                           "SQL and DefinitionOptions partition keys conflict"));
  RelationDescriptor result;
  result.id = id;
  result.name = definition.name;
  result.base_write_role = options.base_write_role;
  EF_RETURN_IF_ERROR(check(options.base_write_role == BaseWriteRole::PublicInput ||
                               options.base_write_role == BaseWriteRole::RequestCapture ||
                               options.base_write_role == BaseWriteRole::RuntimeStatus ||
                               options.base_write_role == BaseWriteRole::TriggerAudit,
                           "Unknown base write role"));
  if (options.base_write_role != BaseWriteRole::RequestCapture)
    EF_RETURN_IF_ERROR(check(options.identity_domain.empty(),
                             "Only RequestCapture declares a request identity domain"));

  auto& schema = result.storage_schema;
  schema.id = id;
  schema.name = definition.name;
  schema.primary_key = definition.primary_key;
  schema.partition_key =
      definition.partition_key.empty() ? options.partition_key : definition.partition_key;
  EF_RETURN_IF_ERROR(check(definition.columns.size() < std::numeric_limits<std::uint32_t>::max(),
                           "Too many fields"));
  for (const auto& column : definition.columns) {
    EF_RETURN_IF_ERROR(name_ok(column.name));
    const auto primary = std::find(schema.primary_key.begin(), schema.primary_key.end(),
                                   column.name) != schema.primary_key.end();
    schema.fields.push_back({.id = static_cast<std::uint32_t>(schema.fields.size() + 1),
                             .name = column.name,
                             .type = column.type,
                             .nullable = column.nullable.value_or(!primary)});
  }

  EF_RETURN_IF_ERROR(schema.validate_definition());
  result.visible_count = schema.fields.size();
  for (const auto& name : schema.partition_key) {
    EF_ASSIGN_OR_RETURN(const auto slot, schema.index_of(name));
    result.partition_slots.push_back(slot);
    result.physical_key_slots.push_back(slot);
    result.partition_types.push_back(schema.fields[slot].type);
    result.identity_types.push_back(schema.fields[slot].type);
  }

  result.partition_visible_slots = result.partition_slots;
  for (const auto& name : schema.primary_key) {
    if (std::find(schema.partition_key.begin(), schema.partition_key.end(), name) !=
        schema.partition_key.end())
      continue;

    EF_ASSIGN_OR_RETURN(const auto slot, schema.index_of(name));
    result.physical_key_slots.push_back(slot);
    result.identity_types.push_back(schema.fields[slot].type);
  }

  result.unique_per_partition = schema.primary_key.size() == schema.partition_key.size();
  if (options.base_write_role == BaseWriteRole::RequestCapture) {
    EF_RETURN_IF_ERROR(name_ok(options.identity_domain));
    EF_RETURN_IF_ERROR(
        check(options.identity_domain.size() <= 256 &&
                  result.identity_types.size() == result.partition_types.size() + 1 &&
                  result.identity_types.back() == DataType::String,
              "RequestCapture requires a bounded domain and one nonpartition String request ID"));
    result.identity_domain = options.identity_domain;

    for (const auto slot : result.physical_key_slots) {
      auto field = schema.fields[slot];
      field.id = static_cast<std::uint32_t>(result.identity_fields.size() + 1);
      result.identity_fields.push_back(std::move(field));
    }
  }

  return result;
}

Status canonical_fields(std::vector<Field>& fields, std::set<std::uint32_t>& ids,
                        std::set<std::string>& names, std::size_t offset) noexcept {
  EF_RETURN_IF_ERROR(check(fields.size() + offset < std::numeric_limits<std::uint32_t>::max(),
                           "Too many lifecycle fields"));
  for (std::size_t i = 0; i < fields.size(); ++i) {
    auto& field = fields[i];
    EF_RETURN_IF_ERROR(name_ok(field.name));
    EF_RETURN_IF_ERROR(check(ids.insert(field.id).second && names.insert(field.name).second &&
                                 !data_type_name(field.type).empty(),
                             "Duplicate lifecycle field ID/name or unknown type"));
    field.id = static_cast<std::uint32_t>(offset + i + 1);
  }

  return {};
}

StatusOr<RelationDescriptor> candidate_descriptor(CandidateDefinition definition,
                                                  RelationId id) noexcept {
  EF_RETURN_IF_ERROR(check(!definition.scope_fields.empty(), "Candidate scope cannot be empty"));
  std::set<std::uint32_t> ids;
  std::set<std::string> names{"candidate_epoch", "active"};
  EF_RETURN_IF_ERROR(canonical_fields(definition.scope_fields, ids, names, 0));
  EF_RETURN_IF_ERROR(
      canonical_fields(definition.payload_fields, ids, names, definition.scope_fields.size() + 2));
  CreateTableAst table;
  table.name = definition.name;

  for (const auto& field : definition.scope_fields) {
    table.columns.push_back({.name = field.name, .type = field.type, .nullable = field.nullable});
    table.primary_key.push_back(field.name);
    table.partition_key.push_back(field.name);
  }

  table.columns.push_back({.name = "candidate_epoch", .type = DataType::Int64, .nullable = false});
  table.columns.push_back({.name = "active", .type = DataType::Bool, .nullable = false});
  for (const auto& field : definition.payload_fields)
    table.columns.push_back({.name = field.name, .type = field.type, .nullable = field.nullable});

  DefinitionOptions options;
  options.base_write_role = BaseWriteRole::RuntimeStatus;
  EF_ASSIGN_OR_RETURN(auto result, table_descriptor(table, options, id));
  result.candidate = std::make_shared<const CandidateAnnotation>(
      CandidateAnnotation{.format = 1, .definition = std::move(definition)});
  return result;
}

StatusOr<RelationDescriptor> request_status_descriptor(RequestStatusDefinition definition,
                                                       const CatalogSnapshot& catalog,
                                                       RelationId id) noexcept {
  EF_RETURN_IF_ERROR(name_ok(definition.request_domain));
  EF_RETURN_IF_ERROR(
      check(definition.request_domain.size() <= 256 && definition.identity_fields.size() >= 2 &&
                definition.identity_fields.back().type == DataType::String,
            "RequestStatus requires a bounded domain and scope plus String request ID"));
  std::set<std::uint32_t> ids;
  std::set<std::string> names{"candidate_epoch", "admission_seq", "phase"};
  EF_RETURN_IF_ERROR(canonical_fields(definition.identity_fields, ids, names, 0));
  EF_ASSIGN_OR_RETURN(const auto candidate, catalog.find(definition.candidate_relation));
  EF_RETURN_IF_ERROR(check(candidate->id < id && candidate->candidate,
                           "RequestStatus requires an earlier Candidate relation"));
  const std::vector<Field> scope(definition.identity_fields.begin(),
                                 definition.identity_fields.end() - 1);
  EF_RETURN_IF_ERROR(
      check(sql_internal::same_fields(scope, candidate->candidate->definition.scope_fields),
            "RequestStatus scope differs from Candidate identity"));
  CreateTableAst table;
  table.name = definition.name;

  for (const auto& field : definition.identity_fields) {
    table.columns.push_back({.name = field.name, .type = field.type, .nullable = field.nullable});
    table.primary_key.push_back(field.name);
  }

  table.partition_key.assign(table.primary_key.begin(), table.primary_key.end() - 1);
  table.columns.push_back({.name = "candidate_epoch", .type = DataType::Int64, .nullable = false});
  table.columns.push_back({.name = "admission_seq", .type = DataType::Int64, .nullable = false});
  table.columns.push_back({.name = "phase", .type = DataType::String, .nullable = false});
  DefinitionOptions options;
  options.base_write_role = BaseWriteRole::RuntimeStatus;
  EF_ASSIGN_OR_RETURN(auto result, table_descriptor(table, options, id));
  result.request_status = std::make_shared<const RequestStatusAnnotation>(
      RequestStatusAnnotation{.format = 1, .definition = std::move(definition)});
  return result;
}

Status materialized_schema(RelationDescriptor& result, const std::vector<Field>& visible) noexcept {
  auto& schema = result.storage_schema;
  schema.id = result.id;
  schema.name = result.name;
  schema.fields = visible;
  result.visible_count = visible.size();
  EF_RETURN_IF_ERROR(check(schema.fields.size() + result.identity_types.size() <
                               std::numeric_limits<std::uint32_t>::max(),
                           "Too many fields"));
  // Keep physical identity even when SELECT omits key columns. Rows with equal visible values
  // must remain distinct so later updates and deletions can find the right row.
  for (std::size_t i = 0; i < result.identity_types.size(); ++i) {
    const auto slot = schema.fields.size();
    const auto hidden_name = "__ef_key_" + std::to_string(i);
    schema.fields.push_back({.id = static_cast<std::uint32_t>(slot + 1),
                             .name = hidden_name,
                             .type = result.identity_types[i],
                             .nullable = false});
    schema.primary_key.push_back(hidden_name);
    result.physical_key_slots.push_back(slot);
    if (i < result.partition_types.size()) {
      schema.partition_key.push_back(hidden_name);
      result.partition_slots.push_back(slot);
    }
  }

  return schema.validate_definition();
}

StatusOr<RelationDescriptor> view_descriptor(const std::string& name, BoundPlan plan,
                                             RelationId id) noexcept {
  EF_RETURN_IF_ERROR(name_ok(name));
  RelationDescriptor result;
  result.id = id;
  result.name = name;
  result.kind = RelationKind::SqlView;
  result.state_id = id;
  result.visible_count = plan.output_fields.size();
  result.partition_visible_slots = plan.partition_visible_slots;
  result.identity_types = plan.identity_types;
  result.partition_types = plan.partition_types;
  result.unique_per_partition = plan.unique_per_partition;
  result.identity_domain = plan.identity_domain;
  result.identity_fields = plan.identity_fields;
  EF_RETURN_IF_ERROR(materialized_schema(result, plan.output_fields));
  result.plan = std::make_shared<const BoundPlan>(std::move(plan));

  return result;
}

StatusOr<RelationDescriptor> latest_descriptor(const LatestViewDefinition& definition,
                                               const CatalogSnapshot& catalog,
                                               RelationId id) noexcept {
  EF_RETURN_IF_ERROR(name_ok(definition.name));
  EF_ASSIGN_OR_RETURN(const auto source, catalog.find(definition.result_relation));
  EF_ASSIGN_OR_RETURN(const auto status, catalog.find(definition.request_status_relation));
  EF_ASSIGN_OR_RETURN(const auto candidate, catalog.find(definition.candidate_relation));
  EF_RETURN_IF_ERROR(check(
      source->id < id && status->id < id && candidate->id < id &&
          (source->kind == RelationKind::FunctionView || source->kind == RelationKind::SqlView) &&
          !source->identity_domain.empty() && status->request_status && candidate->candidate,
      "Latest requires earlier request result, RequestStatus and Candidate inputs"));

  const auto& status_definition = status->request_status->definition;
  EF_RETURN_IF_ERROR(check(
      status_definition.candidate_relation == candidate->id &&
          status_definition.request_domain == source->identity_domain &&
          sql_internal::same_fields(status_definition.identity_fields, source->identity_fields) &&
          source->identity_types == status->identity_types &&
          source->partition_types == status->partition_types &&
          source->partition_types == candidate->partition_types,
      "Latest inputs disagree on domain, identity or Candidate scope"));

  const std::vector<Field> scope(source->identity_fields.begin(),
                                 source->identity_fields.end() - 1);
  EF_RETURN_IF_ERROR(
      check(sql_internal::same_fields(scope, candidate->candidate->definition.scope_fields),
            "Latest result scope differs from Candidate identity"));

  RelationDescriptor result;
  result.id = id;
  result.name = definition.name;
  result.kind = RelationKind::LatestView;
  result.state_id = id;
  result.partition_types = source->partition_types;
  result.identity_types = result.partition_types;
  result.partition_visible_slots = source->partition_visible_slots;
  result.unique_per_partition = true;

  std::vector<Field> visible(source->storage_schema.fields.begin(),
                             source->storage_schema.fields.begin() + source->visible_count);
  for (const auto& field : visible)
    EF_RETURN_IF_ERROR(check(!sql_internal::latest_provenance_name(field.name),
                             "Latest source collides with generated provenance"));

  visible.push_back({.id = static_cast<std::uint32_t>(visible.size() + 1),
                     .name = "__ef_latest_candidate_epoch",
                     .type = DataType::Int64,
                     .nullable = false});
  visible.push_back({.id = static_cast<std::uint32_t>(visible.size() + 1),
                     .name = "__ef_latest_admission_seq",
                     .type = DataType::Int64,
                     .nullable = false});
  EF_RETURN_IF_ERROR(materialized_schema(result, visible));
  result.latest = std::make_shared<const LatestViewPlan>(
      LatestViewPlan{.catalog_version = catalog.version,
                     .catalog_fingerprint = catalog.fingerprint,
                     .format = 1,
                     .definition = definition});
  return result;
}

bool same_function_contract(const FunctionViewDefinition& left,
                            const FunctionViewDefinition& right) noexcept {
  if (left.implementation_identity != right.implementation_identity ||
      left.inputs.size() != right.inputs.size() ||
      !sql_internal::same_fields(left.output_fields, right.output_fields))
    return false;

  for (std::size_t i = 0; i < left.inputs.size(); ++i)
    if (left.inputs[i].alias != right.inputs[i].alias ||
        left.inputs[i].required != right.inputs[i].required ||
        !sql_internal::same_fields(left.inputs[i].fields, right.inputs[i].fields))
      return false;

  return true;
}

StatusOr<RelationDescriptor> function_descriptor(const FunctionViewDefinition& definition,
                                                 const CatalogSnapshot& catalog,
                                                 RelationId id) noexcept {
  EF_RETURN_IF_ERROR(name_ok(definition.name));
  EF_RETURN_IF_ERROR(name_ok(definition.function_id));
  EF_RETURN_IF_ERROR(
      check(definition.function_id.size() <= 256 && definition.function_version > 0 &&
                !definition.implementation_identity.empty() &&
                definition.implementation_identity.size() <= 4096 &&
                valid_utf8(definition.implementation_identity),
            "Function requires a bounded ID, positive version and implementation identity"));
  EF_RETURN_IF_ERROR(check(!definition.inputs.empty() && definition.inputs.size() <= 32 &&
                               !definition.output_fields.empty() &&
                               definition.output_fields.size() <= 1024,
                           "Function input or output signature exceeds its bounds"));
  RelationDescriptor result;
  result.id = id;
  result.name = definition.name;
  result.kind = RelationKind::FunctionView;
  result.state_id = id;
  std::set<std::string> aliases;

  for (const auto& input : definition.inputs) {
    EF_RETURN_IF_ERROR(name_ok(input.alias));
    EF_RETURN_IF_ERROR(check(input.alias.size() <= 256 && aliases.insert(input.alias).second,
                             "Duplicate or oversized function input alias"));

    EF_ASSIGN_OR_RETURN(const auto source, catalog.find(input.relation));
    EF_RETURN_IF_ERROR(check(source->id < id && !source->identity_domain.empty(),
                             "Function inputs require an earlier full request-identity relation"));
    EF_RETURN_IF_ERROR(
        check(source->visible_count <= 1024 && input.fields.size() == source->visible_count,
              "Function input signature size mismatch or excessive size"));

    const std::vector<Field> fields(source->storage_schema.fields.begin(),
                                    source->storage_schema.fields.begin() + source->visible_count);
    EF_RETURN_IF_ERROR(check(sql_internal::same_fields(input.fields, fields),
                             "Function input signature differs from visible source fields"));

    if (result.identity_domain.empty()) {
      result.identity_domain = source->identity_domain;
      result.identity_fields = source->identity_fields;
      result.identity_types = source->identity_types;
      result.partition_types = source->partition_types;
    } else {
      EF_RETURN_IF_ERROR(
          check(result.identity_domain == source->identity_domain &&
                    result.identity_types == source->identity_types &&
                    result.partition_types == source->partition_types &&
                    sql_internal::same_fields(result.identity_fields, source->identity_fields),
                "Function inputs must align the same request domain and full physical identity"));
    }
  }

  std::vector<Field> visible = result.identity_fields;
  std::set<std::string> names;
  for (const auto& field : visible)
    names.insert(field.name);

  for (std::size_t i = 0; i < definition.output_fields.size(); ++i) {
    auto field = definition.output_fields[i];
    EF_RETURN_IF_ERROR(name_ok(field.name));
    EF_RETURN_IF_ERROR(check(field.id == i + 1 && field.name.size() <= 256 &&
                                 !data_type_name(field.type).empty() &&
                                 names.insert(field.name).second,
                             "Invalid payload field or collision with host-owned identity"));
    field.id = static_cast<std::uint32_t>(visible.size() + 1);
    visible.push_back(std::move(field));
  }

  for (std::size_t i = 0; i < result.partition_types.size(); ++i)
    result.partition_visible_slots.push_back(i);

  // One executable version has one contract. Relation bindings may differ, but
  // aliases, optionality and field signatures cannot redefine that version.
  for (const auto& existing : catalog.relations) {
    if (existing->kind != RelationKind::FunctionView)
      continue;

    const auto& declared = existing->function->definition;
    if (declared.function_id == definition.function_id &&
        declared.function_version == definition.function_version)
      EF_RETURN_IF_ERROR(check(same_function_contract(declared, definition),
                               "Function ID/version has conflicting implementation or signature"));
  }

  EF_RETURN_IF_ERROR(materialized_schema(result, visible));
  result.function = std::make_shared<const FunctionViewPlan>(
      FunctionViewPlan{.catalog_version = catalog.version,
                       .catalog_fingerprint = catalog.fingerprint,
                       .definition = definition});
  return result;
}

Json function_plan_json(const FunctionViewPlan& plan) noexcept {
  const auto& definition = plan.definition;
  Json inputs = Json::array();
  for (const auto& input : definition.inputs)
    inputs.push_back({{"alias", input.alias},
                      {"relation", input.relation},
                      {"required", input.required},
                      {"fields", sql_internal::fields_json(input.fields)}});

  return Json{{"catalog_version", plan.catalog_version},
              {"definition",
               {{"name", definition.name},
                {"function_id", definition.function_id},
                {"function_version", definition.function_version},
                {"implementation_identity", definition.implementation_identity},
                {"inputs", std::move(inputs)},
                {"output_fields", sql_internal::fields_json(definition.output_fields)}}}};
}

StatusOr<Json> descriptor_json(const RelationDescriptor& relation) noexcept {
  EF_RETURN_IF_ERROR(validate_kind(relation));
  EF_ASSIGN_OR_RETURN(auto schema, schema_to_json(relation.storage_schema));
  Json identity = Json::array(), partition = Json::array(), visible_partition = Json::array();
  for (const auto type : relation.identity_types)
    identity.push_back(data_type_name(type));

  for (const auto type : relation.partition_types)
    partition.push_back(data_type_name(type));

  for (const auto slot : relation.partition_visible_slots)
    visible_partition.push_back(slot == kNoSlot ? Json(nullptr) : Json(slot));

  Json plan = nullptr;
  if (relation.plan) {
    EF_ASSIGN_OR_RETURN(plan, bound_plan_json(*relation.plan));
  }

  Json result{{"id", relation.id},
              {"name", relation.name},
              {"storage_schema", std::move(schema)},
              {"visible_count", relation.visible_count},
              {"physical_key_slots", relation.physical_key_slots},
              {"partition_slots", relation.partition_slots},
              {"partition_visible_slots", std::move(visible_partition)},
              {"identity_types", std::move(identity)},
              {"partition_types", std::move(partition)},
              {"unique_per_partition", relation.unique_per_partition},
              {"state_id", relation.state_id},
              {"plan", std::move(plan)}};

  // Keep ordinary SQL catalogs byte-compatible with their existing canonical
  // representation. Its plan presence already identifies BaseTable vs SqlView.
  switch (relation.base_write_role) {
  case BaseWriteRole::PublicInput:
    break;
  case BaseWriteRole::RequestCapture:
    result["base_write_role"] = "request_capture";
    break;
  case BaseWriteRole::RuntimeStatus:
    result["base_write_role"] = "runtime_status";
    break;
  case BaseWriteRole::TriggerAudit:
    result["base_write_role"] = "trigger_audit";
    break;
  }

  if (!relation.identity_domain.empty() || !relation.identity_fields.empty()) {
    result["identity_domain"] = relation.identity_domain;
    result["identity_fields"] = sql_internal::fields_json(relation.identity_fields);
  }

  if (relation.function) {
    result["kind"] = "function_view";
    result["function"] = function_plan_json(*relation.function);
  }

  if (relation.candidate) {
    const auto& annotation = *relation.candidate;
    result["candidate"] = {
        {"format", annotation.format},
        {"name", annotation.definition.name},
        {"scope_fields", sql_internal::fields_json(annotation.definition.scope_fields)},
        {"payload_fields", sql_internal::fields_json(annotation.definition.payload_fields)}};
  }

  if (relation.request_status) {
    const auto& annotation = *relation.request_status;
    result["request_status"] = {
        {"format", annotation.format},
        {"name", annotation.definition.name},
        {"request_domain", annotation.definition.request_domain},
        {"identity_fields", sql_internal::fields_json(annotation.definition.identity_fields)},
        {"candidate_relation", annotation.definition.candidate_relation}};
  }

  if (relation.latest) {
    const auto& plan = *relation.latest;
    result["kind"] = "latest_view";
    result["latest"] = {{"format", plan.format},
                        {"catalog_version", plan.catalog_version},
                        {"definition",
                         {{"name", plan.definition.name},
                          {"result_relation", plan.definition.result_relation},
                          {"request_status_relation", plan.definition.request_status_relation},
                          {"candidate_relation", plan.definition.candidate_relation}}}};
  }

  return result;
}

constexpr std::size_t observation_rule_limit = 256;
constexpr std::size_t observation_item_limit = 64;
constexpr std::size_t observation_field_limit = 4096;
constexpr std::size_t observation_byte_limit = 16 * 1024 * 1024;

Status observation_name(std::string_view value) noexcept {
  return check(!value.empty() && value.size() <= 256 && valid_utf8(value),
               "Observation identifier is empty, invalid or too long");
}

Status observation_fields(const std::vector<Field>& fields, bool key = false,
                          std::size_t limit = observation_field_limit) noexcept {
  if (fields.size() > limit)
    return {ErrorCode::ResourceExhausted, "Observation field limit exceeded"};

  std::set<std::string> names;
  for (std::size_t i = 0; i < fields.size(); ++i) {
    const auto& field = fields[i];
    EF_RETURN_IF_ERROR(observation_name(field.name));
    EF_RETURN_IF_ERROR(check(field.id == i + 1 && names.insert(field.name).second &&
                                 !data_type_name(field.type).empty(),
                             "Observation fields need unique names and canonical IDs/types"));
    if (key)
      EF_RETURN_IF_ERROR(check(!field.nullable && field.type != DataType::Float64 &&
                                   field.type != DataType::FloatVector,
                               "Observation scope must use nonnullable key types"));
  }

  return {};
}

Status observation_bytes(std::size_t bytes) noexcept {
  return check(bytes > 0 && bytes <= observation_byte_limit,
               "Observation byte envelope must be positive and bounded");
}

Status observation_scope(const RelationDescriptor& relation,
                         const std::vector<Field>& scope) noexcept {
  EF_RETURN_IF_ERROR(check(relation.partition_types.size() == scope.size(),
                           "Observation relation has a different scope width"));
  for (std::size_t i = 0; i < scope.size(); ++i) {
    const auto slot =
        i < relation.partition_visible_slots.size() ? relation.partition_visible_slots[i] : kNoSlot;
    EF_RETURN_IF_ERROR(check(relation.partition_types[i] == scope[i].type &&
                                 (relation.kind != RelationKind::BaseTable || slot == kNoSlot ||
                                  (slot < relation.visible_count &&
                                   relation.storage_schema.fields[slot].name == scope[i].name)),
                             "Observation relation has a different scope signature"));
  }

  return {};
}

StatusOr<std::vector<Field>> observation_event_fields(const ObservationInputDefinition& input,
                                                      const CatalogSnapshot& catalog,
                                                      const std::vector<Field>& scope) noexcept {
  EF_RETURN_IF_ERROR(observation_name(input.id));
  EF_RETURN_IF_ERROR(observation_name(input.event_kind));
  EF_RETURN_IF_ERROR(check(input.event_kind != "*" && input.event_kind != "$timeout",
                           "Observation kind cannot impersonate an internal timeout"));
  EF_RETURN_IF_ERROR(observation_bytes(input.max_event_bytes));
  std::vector<Field> result;
  const auto add = [&](std::string name, DataType type, bool nullable = false) noexcept {
    result.push_back({.id = static_cast<std::uint32_t>(result.size() + 1),
                      .name = std::move(name),
                      .type = type,
                      .nullable = nullable});
  };
  const bool change = input.kind == ObservationInputKind::RelationChange;
  const bool expiry = input.kind == ObservationInputKind::ExpiryNotice;
  const bool completion = input.kind == ObservationInputKind::AttemptSettled ||
                          input.kind == ObservationInputKind::RequestTerminal;
  if (input.kind != ObservationInputKind::Source && !change && !expiry && !completion &&
      input.kind != ObservationInputKind::Clock)
    return Status(ErrorCode::InvalidArgument, "Unknown observation input kind");

  EF_RETURN_IF_ERROR(
      check((input.kind == ObservationInputKind::Source || input.source_fields.empty()) &&
                (input.kind == ObservationInputKind::Source || input.source_kind.empty()) &&
                (input.kind == ObservationInputKind::Clock || input.policy_id.empty()) &&
                (completion || (input.plan.empty() && input.completion_targets.empty())) &&
                ((change || expiry) == input.relation.has_value()) &&
                (change ? input.change_mask > 0 && input.change_mask <= 7 : input.change_mask == 7),
            "Observation input has incompatible selectors or payload"));
  if (input.kind == ObservationInputKind::Source) {
    EF_RETURN_IF_ERROR(observation_name(input.source_kind));
    EF_RETURN_IF_ERROR(observation_fields(input.source_fields));
    return std::vector<Field>(input.source_fields);
  }

  if (change || expiry) {
    EF_ASSIGN_OR_RETURN(auto relation, catalog.find(*input.relation));
    EF_RETURN_IF_ERROR(observation_scope(*relation, scope));
    if (change) {
      add("op", DataType::String);
      add("has_before", DataType::Bool);
      add("has_after", DataType::Bool);
      for (const auto* side : {"before_", "after_"})
        for (std::size_t i = 0; i < relation->visible_count; ++i) {
          const auto& field = relation->storage_schema.fields[i];
          add(std::string(side) + field.name, field.type, true);
        }
    } else {
      EF_RETURN_IF_ERROR(check(relation->kind == RelationKind::BaseTable,
                               "Expiry input must refer to a base relation"));
      add("scheduled_time_us", DataType::TimestampMicros);
      add("expired_at_us", DataType::TimestampMicros);
    }
  } else if (completion) {
    EF_RETURN_IF_ERROR(observation_name(input.plan));
    EF_RETURN_IF_ERROR(check(!input.completion_targets.empty() &&
                                 input.completion_targets.size() <= observation_item_limit &&
                                 (input.kind != ObservationInputKind::AttemptSettled ||
                                  input.completion_targets.size() == 1),
                             "Completion input requires its exact bounded target relations"));
    std::set<RelationId> targets;
    std::string request_domain;

    for (auto id : input.completion_targets) {
      EF_RETURN_IF_ERROR(check(targets.insert(id).second, "Duplicate completion target"));
      EF_ASSIGN_OR_RETURN(auto relation, catalog.find(id));
      EF_RETURN_IF_ERROR(observation_scope(*relation, scope));
      if (request_domain.empty())
        request_domain = relation->identity_domain;

      EF_RETURN_IF_ERROR(check(relation->identity_domain == request_domain,
                               "Completion targets belong to different Request domains"));
      EF_RETURN_IF_ERROR(check(!relation->identity_domain.empty() &&
                                   (relation->kind == RelationKind::FunctionView ||
                                    (input.kind == ObservationInputKind::RequestTerminal &&
                                     relation->kind == RelationKind::SqlView)),
                               "Completion target must belong to a Request computation"));
    }

    add("plan", DataType::String);
    add("request_id", DataType::String);
    add("candidate_epoch", DataType::String);
    if (input.kind == ObservationInputKind::AttemptSettled) {
      add("node_id", DataType::Int64);
      add("attempt", DataType::Int64);
    }

    add("outcome", DataType::String);
  } else {
    EF_RETURN_IF_ERROR(observation_name(input.policy_id));
    add("policy_id", DataType::String);
    add("scheduled_time_us", DataType::TimestampMicros);
    add("skipped_slots", DataType::Int64);
  }

  EF_RETURN_IF_ERROR(observation_fields(result, false, 2 * observation_field_limit + 3));
  return result;
}

StatusOr<ObservationRuleDescriptor>
observation_descriptor(const ObservationRuleDefinition& definition,
                       const CatalogSnapshot& catalog) noexcept {
  EF_RETURN_IF_ERROR(observation_name(definition.name));
  EF_RETURN_IF_ERROR(observation_name(definition.recipe_id));
  EF_RETURN_IF_ERROR(observation_name(definition.implementation_identity));
  EF_RETURN_IF_ERROR(check(definition.version > 0 && definition.recipe_version > 0 &&
                               (definition.program_kind == ObservationProgramKind::Fsm ||
                                definition.program_kind == ObservationProgramKind::Cep),
                           "Invalid observation program kind/version"));
  if (definition.inputs.empty() || definition.inputs.size() > observation_item_limit ||
      definition.reads.size() > observation_item_limit ||
      definition.activation_targets.size() > observation_item_limit ||
      definition.program_fingerprint.empty() || definition.program_fingerprint.size() > 64 * 1024)
    return Status(ErrorCode::ResourceExhausted,
                  "Observation definition exceeds its bounded envelope");

  // Check nesting before parsing a caller-supplied recipe descriptor.
  std::size_t depth = 0;
  bool quoted = false, escaped = false;
  for (char c : definition.program_fingerprint) {
    if (quoted) {
      if (escaped)
        escaped = false;
      else if (c == '\\')
        escaped = true;
      else if (c == '"')
        quoted = false;
    } else if (c == '"')
      quoted = true;
    else if (c == '{' || c == '[') {
      if (++depth > 64)
        return Status(ErrorCode::ResourceExhausted, "Observation recipe nesting limit exceeded");
    } else if ((c == '}' || c == ']') && depth)
      --depth;
  }

  EF_ASSIGN_OR_RETURN(auto program, parse_json_exact(definition.program_fingerprint));
  EF_RETURN_IF_ERROR(check(program.is_object() && program.dump() == definition.program_fingerprint,
                           "Observation program fingerprint must be a canonical JSON object"));
  EF_RETURN_IF_ERROR(check(!definition.scope_fields.empty(), "Observation scope is empty"));
  EF_RETURN_IF_ERROR(observation_fields(definition.scope_fields, true));
  ObservationRuleDescriptor result{.id =
                                       static_cast<std::uint32_t>(catalog.observations.size() + 1),
                                   .after_relation_count = catalog.relations.size(),
                                   .definition = definition,
                                   .event_fields = {}};
  std::set<std::string> input_ids, read_aliases, input_routes;

  for (const auto& input : definition.inputs) {
    EF_RETURN_IF_ERROR(check(input_ids.insert(input.id).second, "Duplicate observation input ID"));
    const auto route =
        Json::array({static_cast<unsigned>(input.kind),
                     input.relation ? Json(*input.relation) : Json(), input.source_kind, input.plan,
                     input.policy_id, input.completion_targets})
            .dump();
    EF_RETURN_IF_ERROR(check(input_routes.insert(route).second, "Overlapping observation inputs"));
    EF_ASSIGN_OR_RETURN(auto fields,
                        observation_event_fields(input, catalog, definition.scope_fields));
    result.event_fields.push_back(std::move(fields));
  }

  for (const auto& read : definition.reads) {
    EF_RETURN_IF_ERROR(observation_name(read.alias));
    EF_RETURN_IF_ERROR(
        check(read_aliases.insert(read.alias).second, "Duplicate observation read alias"));
    EF_RETURN_IF_ERROR(observation_bytes(read.max_row_bytes));
    EF_RETURN_IF_ERROR(observation_fields(read.fields));
    EF_ASSIGN_OR_RETURN(auto relation, catalog.find(read.relation));
    EF_RETURN_IF_ERROR(observation_scope(*relation, definition.scope_fields));
    std::vector<Field> fields(relation->storage_schema.fields.begin(),
                              relation->storage_schema.fields.begin() + relation->visible_count);
    EF_RETURN_IF_ERROR(
        check(relation->unique_per_partition && sql_internal::same_fields(fields, read.fields),
              "Observation read is not a singleton or has a different visible signature"));
  }

  if (definition.sink_relation) {
    EF_ASSIGN_OR_RETURN(auto sink, catalog.find(*definition.sink_relation));
    EF_RETURN_IF_ERROR(observation_scope(*sink, definition.scope_fields));
    EF_RETURN_IF_ERROR(check(sink->kind == RelationKind::BaseTable &&
                                 sink->base_write_role == BaseWriteRole::PublicInput,
                             "Observation sink must be a PublicInput base relation"));
    EF_RETURN_IF_ERROR(observation_bytes(definition.max_sink_row_bytes));
  } else
    EF_RETURN_IF_ERROR(
        check(definition.max_sink_row_bytes == 0, "Sink bytes require a sink relation"));
  std::set<RelationId> targets;

  for (auto id : definition.activation_targets) {
    EF_RETURN_IF_ERROR(check(targets.insert(id).second, "Duplicate activation target"));
    EF_ASSIGN_OR_RETURN(auto relation, catalog.find(id));
    EF_RETURN_IF_ERROR(observation_scope(*relation, definition.scope_fields));
    EF_RETURN_IF_ERROR(
        check(!relation->identity_domain.empty() && (relation->kind == RelationKind::FunctionView ||
                                                     relation->kind == RelationKind::SqlView),
              "Activation target must belong to a Request computation"));
  }

  return result;
}

StatusOr<ObservationGraph> derive_observation_graph(const CatalogSnapshot& catalog) noexcept {
  if (catalog.observations.size() > observation_rule_limit || catalog.relations.size() > 4096)
    return Status(ErrorCode::ResourceExhausted, "Observation graph exceeds bounded node limit");

  const auto relation_count = catalog.relations.size();
  const auto size = relation_count + catalog.observations.size();
  std::vector<std::set<std::size_t>> edges(size);
  std::vector<std::size_t> incoming(size);
  ObservationGraph result;
  const auto edge = [&](std::size_t from, std::size_t to) noexcept -> Status {
    EF_RETURN_IF_ERROR(check(from < size && to < size, "Observation graph has an unknown node"));
    if (edges[from].insert(to).second)
      ++incoming[to];

    return {};
  };
  for (std::size_t i = 0; i < relation_count; ++i) {
    EF_RETURN_IF_ERROR(check(catalog.relations[i] && catalog.relations[i]->id == i + 1,
                             "Invalid graph relation order"));
    EF_ASSIGN_OR_RETURN(auto parents, dependencies(*catalog.relations[i]));
    for (auto parent : parents)
      EF_RETURN_IF_ERROR(edge(parent - 1, i));
  }

  for (std::size_t i = 0; i < catalog.observations.size(); ++i) {
    const auto& observation = catalog.observations[i];
    const auto node = relation_count + i;
    std::set<RelationId> sources;
    for (const auto& input : observation.definition.inputs) {
      if (input.relation)
        sources.insert(*input.relation);
      if (input.kind == ObservationInputKind::RelationChange && input.relation)
        result.subscribers_by_relation[*input.relation].push_back(observation.id);

      sources.insert(input.completion_targets.begin(), input.completion_targets.end());
    }

    for (const auto& read : observation.definition.reads)
      sources.insert(read.relation);

    for (auto source : sources) {
      EF_RETURN_IF_ERROR(edge(source - 1, node));
    }

    if (observation.definition.sink_relation)
      EF_RETURN_IF_ERROR(edge(node, *observation.definition.sink_relation - 1));

    for (auto target : observation.definition.activation_targets) {
      EF_ASSIGN_OR_RETURN(auto root, catalog.find(target));
      std::vector<RelationId> pending{target};
      std::set<RelationId> seen;
      while (!pending.empty()) {
        const auto current = pending.back();
        pending.pop_back();
        if (!seen.insert(current).second)
          continue;

        EF_ASSIGN_OR_RETURN(auto relation, catalog.find(current));
        if (relation->identity_domain != root->identity_domain)
          continue;

        EF_RETURN_IF_ERROR(edge(node, current - 1));
        EF_ASSIGN_OR_RETURN(auto parents, dependencies(*relation));
        pending.insert(pending.end(), parents.begin(), parents.end());
      }

      for (const auto& relation : catalog.relations)
        if (relation->request_status &&
            relation->request_status->definition.request_domain == root->identity_domain)
          EF_RETURN_IF_ERROR(edge(node, relation->id - 1));
    }
  }

  std::set<std::size_t> ready;
  for (std::size_t i = 0; i < size; ++i)
    if (!incoming[i])
      ready.insert(i);

  std::size_t visited = 0;
  while (!ready.empty()) {
    const auto node = *ready.begin();
    ready.erase(ready.begin());
    ++visited;
    if (node >= relation_count)
      result.rule_order.push_back(catalog.observations[node - relation_count].id);

    for (auto target : edges[node])
      if (!--incoming[target])
        ready.insert(target);
  }

  EF_RETURN_IF_ERROR(
      check(visited == size, "Observation/sink/activation dependencies contain causal feedback"));
  return result;
}

StatusOr<std::string> fingerprint(const CatalogSnapshot& catalog) noexcept {
  Json relations = Json::array();
  for (const auto& relation : catalog.relations) {
    EF_ASSIGN_OR_RETURN(auto json, descriptor_json(*relation));
    relations.push_back(std::move(json));
  }

  Json result{{"layout_version", 1},
              {"catalog_version", catalog.version},
              {"relations", std::move(relations)}};
  if (!catalog.observations.empty()) {
    result["observations"] = Json::array();
    for (const auto& observation : catalog.observations) {
      EF_ASSIGN_OR_RETURN(auto json, observation_json(observation));
      result["observations"].push_back(std::move(json));
    }
  }

  return result.dump();
}

BoundExprPtr copy_expression(const BoundExprPtr& expression) noexcept {
  if (!expression)
    return {};

  BoundExpr copied = *expression;
  for (auto& argument : copied.args)
    argument = copy_expression(argument);

  return std::make_shared<const BoundExpr>(std::move(copied));
}

BoundPlan copy_plan(const BoundPlan& plan) noexcept {
  BoundPlan copied = plan;
  copied.filter = copy_expression(plan.filter);
  for (auto& expression : copied.group_by)
    expression = copy_expression(expression);

  for (auto& expression : copied.projection)
    expression = copy_expression(expression);

  for (auto& aggregate : copied.aggregates)
    aggregate.argument = copy_expression(aggregate.argument);

  return copied;
}

Status validate_expression(const BoundExprPtr& expression, const std::vector<Field>& inputs,
                           std::size_t depth = 0) noexcept {
  EF_RETURN_IF_ERROR(
      check(expression && depth <= 128, "Invalid bound expression or excessive nesting"));
  const auto& expr = *expression;
  EF_RETURN_IF_ERROR(check(valid_utf8(expr.op), "Invalid bound operator encoding"));
  EF_RETURN_IF_ERROR(
      check(expr.kind == BoundExprKind::Input || expr.slot == 0, "Unexpected slot metadata"));
  EF_RETURN_IF_ERROR(check(expr.kind == BoundExprKind::Literal ||
                               std::holds_alternative<std::monostate>(expr.literal),
                           "Unexpected literal metadata"));
  EF_RETURN_IF_ERROR(check(!expr.untyped_null && !data_type_name(expr.type).empty(),
                           "Bound expression has no concrete type"));
  for (const auto& argument : expr.args)
    EF_RETURN_IF_ERROR(validate_expression(argument, inputs, depth + 1));

  if (expr.kind == BoundExprKind::Input) {
    EF_RETURN_IF_ERROR(check(expr.args.empty() && expr.op.empty() && expr.slot < inputs.size(),
                             "Bound input slot is out of range"));
    const auto& field = inputs[expr.slot];
    return check(expr.type == field.type && expr.nullable == field.nullable,
                 "Bound input type differs from its source field");
  }

  if (expr.kind == BoundExprKind::Literal) {
    EF_RETURN_IF_ERROR(
        check(expr.args.empty() && expr.op.empty() &&
                  expr.nullable == std::holds_alternative<std::monostate>(expr.literal),
              "Literal metadata is not canonical"));
    EF_ASSIGN_OR_RETURN(auto json, value_to_json(expr.literal));
    EF_ASSIGN_OR_RETURN(auto value, value_from_json(json, expr.type, expr.nullable));

    return check(value.index() == expr.literal.index(),
                 "Bound literal representation differs from its type");
  }

  if (expr.kind == BoundExprKind::IsNull) {
    return check(expr.args.size() == 1 && expr.type == DataType::Bool && !expr.nullable &&
                     (expr.op == "is_null" || expr.op == "is_not_null"),
                 "Invalid bound NULL predicate");
  }

  if (expr.kind == BoundExprKind::Cast) {
    return check(
        expr.args.size() == 1 && expr.op.empty() && expr.type == DataType::Float64 &&
            (expr.args[0]->type == DataType::Int64 || expr.args[0]->type == DataType::Float64) &&
            expr.nullable == expr.args[0]->nullable,
        "Invalid bound CAST");
  }

  if (expr.kind == BoundExprKind::Unary) {
    EF_RETURN_IF_ERROR(check(expr.args.size() == 1 && expr.type == expr.args[0]->type &&
                                 expr.nullable == expr.args[0]->nullable,
                             "Invalid bound unary expression"));
    return check((expr.op == "not" && expr.type == DataType::Bool) ||
                     ((expr.op == "+" || expr.op == "-") &&
                      (expr.type == DataType::Int64 || expr.type == DataType::Float64)),
                 "Invalid bound unary operator or type");
  }

  EF_RETURN_IF_ERROR(check(expr.kind == BoundExprKind::Binary && expr.args.size() == 2,
                           "Invalid bound expression kind"));
  const auto type = expr.args[0]->type;
  EF_RETURN_IF_ERROR(check(type == expr.args[1]->type &&
                               expr.nullable == (expr.args[0]->nullable || expr.args[1]->nullable),
                           "Invalid binary operand types or NULL metadata"));
  if (expr.op == "and" || expr.op == "or")
    return check(type == DataType::Bool && expr.type == DataType::Bool, "Invalid logical types");
  if (expr.op == "+" || expr.op == "-" || expr.op == "*" || expr.op == "/")
    return check((type == DataType::Int64 || type == DataType::Float64) && expr.type == type,
                 "Invalid arithmetic types");

  const bool equal = expr.op == "=" || expr.op == "!=";
  const bool ordered = expr.op == "<" || expr.op == "<=" || expr.op == ">" || expr.op == ">=";
  return check((equal || ordered) && expr.type == DataType::Bool && type != DataType::FloatVector &&
                   (!ordered || type != DataType::Bool),
               "Invalid comparison types or operator");
}

bool constant_expression(const BoundExprPtr& expression) noexcept {
  if (expression->kind == BoundExprKind::Input)
    return false;

  for (const auto& argument : expression->args)
    if (!constant_expression(argument))
      return false;

  return true;
}

void collect_scope(const BoundExprPtr& expression,
                   const std::map<std::size_t, std::size_t>& partition_inputs,
                   std::vector<std::optional<Value>>& values) noexcept {
  if (!expression || expression->kind != BoundExprKind::Binary)
    return;
  if (expression->op == "and") {
    collect_scope(expression->args[0], partition_inputs, values);
    collect_scope(expression->args[1], partition_inputs, values);
    return;
  }

  if (expression->op != "=")
    return;

  for (std::size_t side = 0; side < 2; ++side) {
    const auto& column = expression->args[side];
    const auto& value = expression->args[1 - side];
    if (column->kind != BoundExprKind::Input || !constant_expression(value))
      continue;

    const auto key = partition_inputs.find(column->slot);
    if (key == partition_inputs.end() || values[key->second])
      continue;

    auto evaluated = evaluate(*value, {});
    if (evaluated.ok() && !std::holds_alternative<std::monostate>(*evaluated))
      values[key->second] = std::move(*evaluated);
  }
}

Status validate_plan(const BoundPlan& plan, const CatalogSnapshot& previous) noexcept {
  if (plan.catalog_version != previous.version || plan.catalog_fingerprint != previous.fingerprint)
    return Status(ErrorCode::Conflict, "Plan provenance differs from the binding catalog");

  EF_ASSIGN_OR_RETURN(const auto left, previous.find(plan.left));
  std::vector<Field> inputs(left->storage_schema.fields.begin(),
                            left->storage_schema.fields.begin() + left->visible_count);
  std::vector<std::size_t> partition_input = left->partition_visible_slots;
  EF_RETURN_IF_ERROR(check(plan.partition_types == left->partition_types,
                           "Plan partition types differ from its input"));
  if (plan.right) {
    EF_ASSIGN_OR_RETURN(const auto right, previous.find(*plan.right));
    EF_RETURN_IF_ERROR(check(left->unique_per_partition && right->unique_per_partition &&
                                 left->partition_types == right->partition_types,
                             "Plan has an invalid one-to-one join"));
    const auto offset = inputs.size();

    for (std::size_t i = 0; i < partition_input.size(); ++i)
      if (partition_input[i] == kNoSlot && right->partition_visible_slots[i] != kNoSlot)
        partition_input[i] = offset + right->partition_visible_slots[i];

    inputs.insert(inputs.end(), right->storage_schema.fields.begin(),
                  right->storage_schema.fields.begin() + right->visible_count);
  }

  if (plan.filter) {
    EF_RETURN_IF_ERROR(validate_expression(plan.filter, inputs));
    EF_RETURN_IF_ERROR(check(plan.filter->type == DataType::Bool, "Plan filter is not Bool"));
  }

  const bool grouped = !plan.group_by.empty();
  EF_RETURN_IF_ERROR(check(grouped || plan.aggregates.empty(), "Aggregate plan has no GROUP BY"));
  std::vector<Field> projected_inputs = inputs;
  if (grouped) {
    EF_RETURN_IF_ERROR(check(plan.group_by.size() == partition_input.size(),
                             "Plan GROUP BY does not cover partition key"));
    projected_inputs.clear();
    for (std::size_t i = 0; i < plan.group_by.size(); ++i) {
      EF_RETURN_IF_ERROR(validate_expression(plan.group_by[i], inputs));
      EF_RETURN_IF_ERROR(check(plan.group_by[i]->kind == BoundExprKind::Input &&
                                   plan.group_by[i]->slot == partition_input[i],
                               "Plan GROUP BY is not in canonical partition order"));
      projected_inputs.push_back({.id = 0,
                                  .name = {},
                                  .type = plan.group_by[i]->type,
                                  .nullable = plan.group_by[i]->nullable});
    }
  }

  for (const auto& aggregate : plan.aggregates) {
    const bool count =
        aggregate.kind == AggregateKind::Count || aggregate.kind == AggregateKind::CountAll;
    if (aggregate.kind == AggregateKind::CountAll)
      EF_RETURN_IF_ERROR(check(!aggregate.argument, "COUNT(*) has an argument"));
    else {
      EF_RETURN_IF_ERROR(validate_expression(aggregate.argument, inputs));
      EF_RETURN_IF_ERROR(check(aggregate.kind == AggregateKind::Count ||
                                   aggregate.kind == AggregateKind::Sum ||
                                   aggregate.kind == AggregateKind::Average,
                               "Unknown aggregate kind"));
      if (!count)
        EF_RETURN_IF_ERROR(check(aggregate.argument->type == DataType::Int64 ||
                                     aggregate.argument->type == DataType::Float64,
                                 "Aggregate argument is not numeric"));
    }

    const auto expected =
        count ? DataType::Int64
              : (aggregate.kind == AggregateKind::Average ? DataType::Float64
                                                          : aggregate.argument->type);
    EF_RETURN_IF_ERROR(check(aggregate.result_type == expected, "Aggregate result type mismatch"));
    projected_inputs.push_back(
        {.id = 0, .name = {}, .type = aggregate.result_type, .nullable = !count});
  }

  EF_RETURN_IF_ERROR(
      check(!plan.projection.empty() && plan.projection.size() == plan.output_fields.size(),
            "Plan output fields do not match projection"));
  std::set<std::string> names;

  for (std::size_t i = 0; i < plan.projection.size(); ++i) {
    EF_RETURN_IF_ERROR(validate_expression(plan.projection[i], projected_inputs));
    const auto& field = plan.output_fields[i];
    if (!sql_internal::generated_projection(field.name, plan.projection[i], projected_inputs))
      EF_RETURN_IF_ERROR(name_ok(field.name));

    EF_RETURN_IF_ERROR(check(field.id == i + 1 && names.insert(field.name).second &&
                                 field.type == plan.projection[i]->type &&
                                 field.nullable == plan.projection[i]->nullable,
                             "Plan output field metadata mismatch"));
  }

  EF_RETURN_IF_ERROR(check(
      plan.identity_types == (grouped || plan.right ? left->partition_types : left->identity_types),
      "Plan identity types mismatch"));
  if (grouped || plan.right) {
    EF_RETURN_IF_ERROR(check(plan.identity_domain.empty() && plan.identity_fields.empty(),
                             "Identity-changing SQL cannot retain a request domain"));
  } else {
    EF_RETURN_IF_ERROR(
        check(plan.identity_domain == left->identity_domain &&
                  sql_internal::same_fields(plan.identity_fields, left->identity_fields),
              "SQL request identity domain differs from its source"));
  }

  EF_RETURN_IF_ERROR(
      check(plan.unique_per_partition == (grouped || plan.right || left->unique_per_partition),
            "Plan uniqueness proof mismatch"));
  EF_RETURN_IF_ERROR(check(plan.partition_visible_slots.size() == plan.partition_types.size(),
                           "Plan partition visibility size mismatch"));
  std::vector<std::size_t> visible_partition(plan.partition_types.size(), kNoSlot);

  for (std::size_t output = 0; output < plan.projection.size(); ++output) {
    const auto& expression = plan.projection[output];
    if (expression->kind != BoundExprKind::Input)
      continue;

    for (std::size_t key = 0; key < visible_partition.size(); ++key) {
      const auto source = expression->slot;
      bool matches =
          grouped ? source == key : sql_internal::partition_lineage(*left, source, previous) == key;
      if (!grouped && plan.right) {
        EF_ASSIGN_OR_RETURN(const auto right, previous.find(*plan.right));
        matches = matches || (source >= left->visible_count &&
                              sql_internal::partition_lineage(*right, source - left->visible_count,
                                                              previous) == key);
      }

      if (matches && visible_partition[key] == kNoSlot)
        visible_partition[key] = output;
    }
  }

  EF_RETURN_IF_ERROR(check(plan.partition_visible_slots == visible_partition,
                           "Plan partition visibility has incorrect lineage"));
  std::map<std::size_t, std::size_t> partition_inputs;
  for (std::size_t slot = 0; slot < left->visible_count; ++slot) {
    const auto key = sql_internal::partition_lineage(*left, slot, previous);
    if (key)
      partition_inputs.emplace(slot, *key);
  }

  if (plan.right) {
    EF_ASSIGN_OR_RETURN(const auto right, previous.find(*plan.right));
    for (std::size_t slot = 0; slot < right->visible_count; ++slot) {
      const auto key = sql_internal::partition_lineage(*right, slot, previous);
      if (key)
        partition_inputs.emplace(left->visible_count + slot, *key);
    }
  }

  std::vector<std::optional<Value>> values(plan.partition_types.size());
  collect_scope(plan.filter, partition_inputs, values);
  std::optional<Row> expected_scope;
  if (std::all_of(values.begin(), values.end(),
                  [](const auto& value) noexcept { return value.has_value(); })) {
    Row row;
    for (auto& value : values)
      row.push_back(std::move(*value));

    expected_scope = std::move(row);
  }

  EF_RETURN_IF_ERROR(check(plan.inferred_scope == expected_scope,
                           "Plan scope is not implied by its bound filter"));
  return {};
}
} // namespace

Status sql_internal::validate_relation_metadata(const RelationDescriptor& relation) noexcept {
  EF_RETURN_IF_ERROR(validate_kind(relation));
  if (relation.candidate) {
    EF_ASSIGN_OR_RETURN(auto expected,
                        candidate_descriptor(relation.candidate->definition, relation.id));
    EF_ASSIGN_OR_RETURN(auto actual_json, descriptor_json(relation));
    EF_ASSIGN_OR_RETURN(auto expected_json, descriptor_json(expected));

    return check(actual_json == expected_json,
                 "Candidate annotation differs from canonical schema");
  }

  if (relation.request_status) {
    const auto& definition = relation.request_status->definition;
    EF_RETURN_IF_ERROR(check(definition.identity_fields.size() >= 2 &&
                                 definition.candidate_relation > 0 &&
                                 definition.candidate_relation < relation.id,
                             "RequestStatus annotation has an invalid identity or dependency"));
    CandidateDefinition candidate;
    candidate.name = "candidate";
    candidate.scope_fields.assign(definition.identity_fields.begin(),
                                  definition.identity_fields.end() - 1);
    EF_ASSIGN_OR_RETURN(auto parent,
                        candidate_descriptor(std::move(candidate), definition.candidate_relation));
    CatalogSnapshot context;
    context.relations.push_back(std::make_shared<const RelationDescriptor>(std::move(parent)));
    EF_ASSIGN_OR_RETURN(auto expected, request_status_descriptor(definition, context, relation.id));
    EF_ASSIGN_OR_RETURN(auto actual_json, descriptor_json(relation));
    EF_ASSIGN_OR_RETURN(auto expected_json, descriptor_json(expected));

    return check(actual_json == expected_json,
                 "RequestStatus annotation differs from canonical schema");
  }

  if (relation.latest) {
    const auto& definition = relation.latest->definition;
    EF_RETURN_IF_ERROR(check(
        definition.name == relation.name && relation.state_id == relation.id &&
            relation.state_id != 0 && relation.unique_per_partition &&
            relation.identity_types == relation.partition_types && definition.result_relation > 0 &&
            definition.result_relation < relation.id && definition.request_status_relation > 0 &&
            definition.request_status_relation < relation.id && definition.candidate_relation > 0 &&
            definition.candidate_relation < relation.id &&
            definition.result_relation != definition.request_status_relation &&
            definition.result_relation != definition.candidate_relation &&
            definition.request_status_relation != definition.candidate_relation &&
            relation.visible_count >= 3 &&
            relation.visible_count <= relation.storage_schema.fields.size(),
        "Latest annotation has invalid dependencies, state or key layout"));
    const auto& epoch = relation.storage_schema.fields[relation.visible_count - 2];
    const auto& sequence = relation.storage_schema.fields[relation.visible_count - 1];
    EF_RETURN_IF_ERROR(
        check(epoch.name == "__ef_latest_candidate_epoch" &&
                  sequence.name == "__ef_latest_admission_seq" && epoch.type == DataType::Int64 &&
                  sequence.type == DataType::Int64 && !epoch.nullable && !sequence.nullable,
              "Latest schema requires canonical provenance columns"));
  }

  return {};
}

StatusOr<std::vector<RelationId>> dependencies(const RelationDescriptor& relation) noexcept {
  EF_RETURN_IF_ERROR(validate_kind(relation));
  std::vector<RelationId> result;
  const auto append = [&result](RelationId id) noexcept -> Status {
    EF_RETURN_IF_ERROR(check(id != 0, "Relation dependency ID must be positive"));
    if (std::find(result.begin(), result.end(), id) == result.end())
      result.push_back(id);

    return {};
  };
  if (relation.kind == RelationKind::SqlView) {
    EF_RETURN_IF_ERROR(append(relation.plan->left));
    if (relation.plan->right)
      EF_RETURN_IF_ERROR(append(*relation.plan->right));
  } else if (relation.kind == RelationKind::FunctionView) {
    EF_RETURN_IF_ERROR(
        check(!relation.function->definition.inputs.empty(), "Function view has no dependencies"));
    for (const auto& input : relation.function->definition.inputs)
      EF_RETURN_IF_ERROR(append(input.relation));
  } else if (relation.kind == RelationKind::LatestView) {
    EF_RETURN_IF_ERROR(append(relation.latest->definition.result_relation));
    EF_RETURN_IF_ERROR(append(relation.latest->definition.request_status_relation));
    EF_RETURN_IF_ERROR(append(relation.latest->definition.candidate_relation));
  } else if (relation.request_status) {
    EF_RETURN_IF_ERROR(append(relation.request_status->definition.candidate_relation));
  }

  return result;
}

Status validate_bound_plan(const BoundPlan& plan, const CatalogSnapshot& catalog) noexcept {
  EF_ASSIGN_OR_RETURN(auto validated, Catalog::from_snapshot(catalog));
  EF_ASSIGN_OR_RETURN(auto snapshot, validated->snapshot());
  return validate_plan(plan, snapshot);
}

StatusOr<Json> observation_json(const ObservationRuleDescriptor& observation) noexcept {
  const auto& definition = observation.definition;
  Json inputs = Json::array(), reads = Json::array(), layouts = Json::array();
  for (const auto& input : definition.inputs)
    inputs.push_back({{"id", input.id},
                      {"kind", static_cast<unsigned>(input.kind)},
                      {"event_kind", input.event_kind},
                      {"relation", input.relation ? Json(*input.relation) : Json()},
                      {"source_kind", input.source_kind},
                      {"plan", input.plan},
                      {"policy_id", input.policy_id},
                      {"completion_targets", input.completion_targets},
                      {"change_mask", input.change_mask},
                      {"source_fields", sql_internal::fields_json(input.source_fields)},
                      {"max_event_bytes", input.max_event_bytes}});

  for (const auto& read : definition.reads)
    reads.push_back({{"alias", read.alias},
                     {"relation", read.relation},
                     {"fields", sql_internal::fields_json(read.fields)},
                     {"max_row_bytes", read.max_row_bytes}});

  for (const auto& fields : observation.event_fields)
    layouts.push_back(sql_internal::fields_json(fields));

  return Json{
      {"format", 1},
      {"id", observation.id},
      {"after_relation_count", observation.after_relation_count},
      {"definition",
       {{"name", definition.name},
        {"version", definition.version},
        {"program_kind", static_cast<unsigned>(definition.program_kind)},
        {"recipe_id", definition.recipe_id},
        {"recipe_version", definition.recipe_version},
        {"implementation_identity", definition.implementation_identity},
        {"program_fingerprint", definition.program_fingerprint},
        {"scope_fields", sql_internal::fields_json(definition.scope_fields)},
        {"inputs", std::move(inputs)},
        {"reads", std::move(reads)},
        {"sink_relation", definition.sink_relation ? Json(*definition.sink_relation) : Json()},
        {"max_sink_row_bytes", definition.max_sink_row_bytes},
        {"activation_targets", definition.activation_targets}}},
      {"event_fields", std::move(layouts)}};
}

StatusOr<const ObservationRuleDescriptor*>
CatalogSnapshot::find_observation(std::string_view name) const noexcept {
  for (const auto& observation : observations)
    if (observation.definition.name == name)
      return &observation;

  return Status(ErrorCode::NotFound, "Observation rule not found");
}

StatusOr<ObservationGraph> observation_graph(const CatalogSnapshot& catalog) noexcept {
  EF_ASSIGN_OR_RETURN(auto valid, Catalog::from_snapshot(catalog));
  EF_ASSIGN_OR_RETURN(auto snapshot, valid->snapshot());
  return derive_observation_graph(snapshot);
}

StatusOr<const RelationDescriptor*> CatalogSnapshot::find(RelationId id) const noexcept {
  for (const auto& relation : relations)
    if (relation && relation->id == id)
      return relation.get();

  return Status(ErrorCode::NotFound, "Relation ID not found: " + std::to_string(id));
}

StatusOr<const RelationDescriptor*> CatalogSnapshot::find(const std::string& name) const noexcept {
  for (const auto& relation : relations)
    if (relation && relation->name == name)
      return relation.get();

  return Status(ErrorCode::NotFound, "Relation not found: " + name);
}

StatusOr<std::unique_ptr<Catalog>> Catalog::create() noexcept {
  auto result = std::unique_ptr<Catalog>(new Catalog);
  EF_ASSIGN_OR_RETURN(result->state_.fingerprint, fingerprint(result->state_));
  return result;
}

StatusOr<std::unique_ptr<Catalog>>
Catalog::from_snapshot(const CatalogSnapshot& snapshot) noexcept {
  EF_RETURN_IF_ERROR(
      check(snapshot.observations.size() <= observation_rule_limit &&
                snapshot.version == snapshot.relations.size() + snapshot.observations.size() &&
                snapshot.relations.size() < std::numeric_limits<RelationId>::max(),
            "Catalog version or size is invalid"));
  EF_ASSIGN_OR_RETURN(auto result, create());
  std::set<std::string> names;
  std::size_t observation_index = 0;
  const auto append_observations = [&]() noexcept -> Status {
    while (observation_index < snapshot.observations.size() &&
           snapshot.observations[observation_index].after_relation_count ==
               result->state_.relations.size()) {
      const auto& original = snapshot.observations[observation_index];
      EF_RETURN_IF_ERROR(
          check(names.insert(original.definition.name).second, "Duplicate Catalog object name"));
      EF_ASSIGN_OR_RETURN(auto id, result->define_observation(original.definition));
      (void)id;
      EF_ASSIGN_OR_RETURN(auto actual, observation_json(original));
      EF_ASSIGN_OR_RETURN(auto expected, observation_json(result->state_.observations.back()));
      EF_RETURN_IF_ERROR(
          check(actual == expected, "Observation descriptor differs from its definition"));
      ++observation_index;
    }

    return {};
  };
  EF_RETURN_IF_ERROR(append_observations());
  for (const auto& relation : snapshot.relations) {
    EF_RETURN_IF_ERROR(check(relation && relation->id == result->state_.relations.size() + 1,
                             "Catalog relation IDs are not in definition order"));
    EF_RETURN_IF_ERROR(name_ok(relation->name));
    EF_RETURN_IF_ERROR(
        check(names.insert(relation->name).second, "Duplicate Catalog relation name"));
    EF_RETURN_IF_ERROR(relation->storage_schema.validate_definition());
    EF_RETURN_IF_ERROR(check(relation->visible_count > 0 &&
                                 relation->visible_count <= relation->storage_schema.fields.size(),
                             "Invalid visible column count"));
    EF_RETURN_IF_ERROR(validate_kind(*relation));
    RelationDescriptor expected;
    if (relation->kind == RelationKind::SqlView) {
      EF_RETURN_IF_ERROR(validate_plan(*relation->plan, result->state_));
      EF_ASSIGN_OR_RETURN(
          expected, view_descriptor(relation->name, copy_plan(*relation->plan), relation->id));
    } else if (relation->kind == RelationKind::FunctionView) {
      const auto& function = *relation->function;
      if (function.catalog_version != result->state_.version ||
          function.catalog_fingerprint != result->state_.fingerprint)
        return Status(ErrorCode::Conflict, "Function provenance differs from the binding catalog");

      EF_ASSIGN_OR_RETURN(expected,
                          function_descriptor(function.definition, result->state_, relation->id));
    } else if (relation->kind == RelationKind::LatestView) {
      const auto& latest = *relation->latest;
      if (latest.catalog_version != result->state_.version ||
          latest.catalog_fingerprint != result->state_.fingerprint)
        return Status(ErrorCode::Conflict, "Latest provenance differs from the binding catalog");

      EF_ASSIGN_OR_RETURN(expected,
                          latest_descriptor(latest.definition, result->state_, relation->id));
    } else if (relation->candidate) {
      EF_ASSIGN_OR_RETURN(expected,
                          candidate_descriptor(relation->candidate->definition, relation->id));
    } else if (relation->request_status) {
      EF_ASSIGN_OR_RETURN(expected, request_status_descriptor(relation->request_status->definition,
                                                              result->state_, relation->id));
    } else {
      CreateTableAst definition;
      definition.name = relation->name;
      definition.primary_key = relation->storage_schema.primary_key;
      definition.partition_key = relation->storage_schema.partition_key;

      for (const auto& field : relation->storage_schema.fields)
        definition.columns.push_back(
            {.name = field.name, .type = field.type, .nullable = field.nullable});

      DefinitionOptions options;
      options.base_write_role = relation->base_write_role;
      options.identity_domain = relation->identity_domain;
      EF_ASSIGN_OR_RETURN(expected, table_descriptor(definition, options, relation->id));
    }

    EF_RETURN_IF_ERROR(validate_domain(expected, result->state_));
    EF_ASSIGN_OR_RETURN(auto actual_json, descriptor_json(*relation));
    EF_ASSIGN_OR_RETURN(auto expected_json, descriptor_json(expected));
    EF_RETURN_IF_ERROR(
        check(actual_json == expected_json, "Catalog descriptor differs from its schema and plan"));
    result->state_.relations.push_back(
        std::make_shared<const RelationDescriptor>(std::move(expected)));
    ++result->state_.version;
    EF_ASSIGN_OR_RETURN(result->state_.fingerprint, fingerprint(result->state_));
    EF_RETURN_IF_ERROR(append_observations());
  }

  EF_RETURN_IF_ERROR(check(observation_index == snapshot.observations.size(),
                           "Observation definition order is invalid"));
  EF_RETURN_IF_ERROR(check(snapshot.fingerprint == result->state_.fingerprint,
                           "Catalog fingerprint does not match definitions"));
  return result;
}

StatusOr<CatalogSnapshot> Catalog::snapshot() const noexcept {
  return CatalogSnapshot(state_);
}

StatusOr<RelationId> Catalog::define(const StatementAst& statement,
                                     const DefinitionOptions& options) noexcept {
  EF_RETURN_IF_ERROR(check(state_.relations.size() < std::numeric_limits<RelationId>::max() - 1,
                           "Catalog relation ID range exhausted"));
  const auto id = static_cast<RelationId>(state_.relations.size() + 1);
  const auto* table = std::get_if<CreateTableAst>(&statement);
  const auto* view = std::get_if<CreateViewAst>(&statement);
  EF_RETURN_IF_ERROR(
      check(table || view, "Catalog define requires CREATE TABLE or CREATE MATERIALIZED VIEW"));
  const auto& name = table ? table->name : view->name;
  EF_RETURN_IF_ERROR(name_ok(name));
  if (state_.find(name).ok() || state_.find_observation(name).ok())
    return Status(ErrorCode::Conflict, "Relation already exists: " + name);

  RelationDescriptor descriptor;
  if (table) {
    EF_ASSIGN_OR_RETURN(descriptor, table_descriptor(*table, options, id));
  } else {
    EF_RETURN_IF_ERROR(check(options.partition_key.empty() &&
                                 options.base_write_role == BaseWriteRole::PublicInput &&
                                 options.identity_domain.empty(),
                             "A view inherits its partition key; DefinitionOptions is for tables"));
    EF_ASSIGN_OR_RETURN(auto plan, sql_internal::bind_select_trusted(view->query, state_));
    EF_ASSIGN_OR_RETURN(descriptor, view_descriptor(name, std::move(plan), id));
  }

  EF_RETURN_IF_ERROR(validate_domain(descriptor, state_));
  CatalogSnapshot candidate = state_;
  candidate.relations.push_back(std::make_shared<const RelationDescriptor>(std::move(descriptor)));
  ++candidate.version;
  EF_ASSIGN_OR_RETURN(candidate.fingerprint, fingerprint(candidate));
  state_ = std::move(candidate);

  return RelationId(id);
}

StatusOr<RelationId> Catalog::define_sql(std::string_view sql,
                                         const DefinitionOptions& options) noexcept {
  EF_ASSIGN_OR_RETURN(auto statement, parse_sql(sql));
  return define(statement, options);
}

StatusOr<RelationId> Catalog::define_function(const FunctionViewDefinition& definition) noexcept {
  EF_RETURN_IF_ERROR(check(state_.relations.size() < std::numeric_limits<RelationId>::max() - 1,
                           "Catalog relation ID range exhausted"));
  EF_RETURN_IF_ERROR(name_ok(definition.name));
  if (state_.find(definition.name).ok() || state_.find_observation(definition.name).ok())
    return Status(ErrorCode::Conflict, "Relation already exists: " + definition.name);

  const auto id = static_cast<RelationId>(state_.relations.size() + 1);
  EF_ASSIGN_OR_RETURN(auto descriptor, function_descriptor(definition, state_, id));
  EF_RETURN_IF_ERROR(validate_domain(descriptor, state_));
  CatalogSnapshot candidate = state_;
  candidate.relations.push_back(std::make_shared<const RelationDescriptor>(std::move(descriptor)));
  ++candidate.version;
  EF_ASSIGN_OR_RETURN(candidate.fingerprint, fingerprint(candidate));
  state_ = std::move(candidate);

  return RelationId(id);
}

StatusOr<RelationId> Catalog::define_candidate(const CandidateDefinition& definition) noexcept {
  EF_RETURN_IF_ERROR(check(state_.relations.size() < std::numeric_limits<RelationId>::max() - 1,
                           "Catalog relation ID range exhausted"));
  EF_RETURN_IF_ERROR(name_ok(definition.name));
  if (state_.find(definition.name).ok() || state_.find_observation(definition.name).ok())
    return Status(ErrorCode::Conflict, "Relation already exists: " + definition.name);

  const auto id = static_cast<RelationId>(state_.relations.size() + 1);
  EF_ASSIGN_OR_RETURN(auto descriptor, candidate_descriptor(definition, id));
  EF_RETURN_IF_ERROR(validate_domain(descriptor, state_));
  CatalogSnapshot candidate = state_;
  candidate.relations.push_back(std::make_shared<const RelationDescriptor>(std::move(descriptor)));
  ++candidate.version;
  EF_ASSIGN_OR_RETURN(candidate.fingerprint, fingerprint(candidate));
  state_ = std::move(candidate);

  return RelationId(id);
}

StatusOr<RelationId>
Catalog::define_request_status(const RequestStatusDefinition& definition) noexcept {
  EF_RETURN_IF_ERROR(check(state_.relations.size() < std::numeric_limits<RelationId>::max() - 1,
                           "Catalog relation ID range exhausted"));
  EF_RETURN_IF_ERROR(name_ok(definition.name));
  if (state_.find(definition.name).ok() || state_.find_observation(definition.name).ok())
    return Status(ErrorCode::Conflict, "Relation already exists: " + definition.name);

  const auto id = static_cast<RelationId>(state_.relations.size() + 1);
  EF_ASSIGN_OR_RETURN(auto descriptor, request_status_descriptor(definition, state_, id));
  EF_RETURN_IF_ERROR(validate_domain(descriptor, state_));
  CatalogSnapshot candidate = state_;
  candidate.relations.push_back(std::make_shared<const RelationDescriptor>(std::move(descriptor)));
  ++candidate.version;
  EF_ASSIGN_OR_RETURN(candidate.fingerprint, fingerprint(candidate));
  state_ = std::move(candidate);

  return RelationId(id);
}

StatusOr<RelationId> Catalog::define_latest(const LatestViewDefinition& definition) noexcept {
  EF_RETURN_IF_ERROR(check(state_.relations.size() < std::numeric_limits<RelationId>::max() - 1,
                           "Catalog relation ID range exhausted"));
  EF_RETURN_IF_ERROR(name_ok(definition.name));
  if (state_.find(definition.name).ok() || state_.find_observation(definition.name).ok())
    return Status(ErrorCode::Conflict, "Relation already exists: " + definition.name);

  const auto id = static_cast<RelationId>(state_.relations.size() + 1);
  EF_ASSIGN_OR_RETURN(auto descriptor, latest_descriptor(definition, state_, id));
  EF_RETURN_IF_ERROR(validate_domain(descriptor, state_));
  CatalogSnapshot candidate = state_;
  candidate.relations.push_back(std::make_shared<const RelationDescriptor>(std::move(descriptor)));
  ++candidate.version;
  EF_ASSIGN_OR_RETURN(candidate.fingerprint, fingerprint(candidate));
  state_ = std::move(candidate);

  return RelationId(id);
}

StatusOr<std::uint32_t>
Catalog::define_observation(const ObservationRuleDefinition& definition) noexcept {
  if (state_.observations.size() >= observation_rule_limit)
    return Status(ErrorCode::ResourceExhausted, "Observation rule count limit exceeded");
  if (state_.find(definition.name).ok() || state_.find_observation(definition.name).ok())
    return Status(ErrorCode::Conflict, "Catalog object already exists");

  EF_ASSIGN_OR_RETURN(auto descriptor, observation_descriptor(definition, state_));
  for (const auto& prior : state_.observations) {
    const auto& existing = prior.definition;
    if (existing.recipe_id != definition.recipe_id ||
        existing.recipe_version != definition.recipe_version)
      continue;

    bool signatures = prior.event_fields.size() == descriptor.event_fields.size() &&
                      existing.reads.size() == definition.reads.size();
    if (signatures) {
      for (std::size_t i = 0; i < prior.event_fields.size(); ++i)
        signatures &= sql_internal::same_fields(prior.event_fields[i], descriptor.event_fields[i]);

      for (std::size_t i = 0; i < existing.reads.size(); ++i)
        signatures &=
            existing.reads[i].alias == definition.reads[i].alias &&
            sql_internal::same_fields(existing.reads[i].fields, definition.reads[i].fields);
    }

    EF_RETURN_IF_ERROR(
        check(signatures && existing.program_kind == definition.program_kind &&
                  existing.implementation_identity == definition.implementation_identity &&
                  existing.program_fingerprint == definition.program_fingerprint &&
                  sql_internal::same_fields(existing.scope_fields, definition.scope_fields),
              "Observation recipe ID/version has a conflicting program or signature"));
  }

  CatalogSnapshot candidate = state_;
  const auto id = descriptor.id;
  candidate.observations.push_back(std::move(descriptor));
  ++candidate.version;
  EF_ASSIGN_OR_RETURN(auto graph, derive_observation_graph(candidate));
  (void)graph;
  EF_ASSIGN_OR_RETURN(candidate.fingerprint, fingerprint(candidate));
  state_ = std::move(candidate);

  return std::uint32_t(id);
}

StatusOr<BoundPlan> Catalog::bind_query(const SelectAst& query) const noexcept {
  return sql_internal::bind_select_trusted(query, state_);
}

StatusOr<BoundPlan> Catalog::bind_query_sql(std::string_view sql) const noexcept {
  EF_ASSIGN_OR_RETURN(auto statement, parse_sql(sql));
  const auto* query = std::get_if<SelectAst>(&statement);
  EF_RETURN_IF_ERROR(check(query != nullptr, "Query requires a SELECT statement"));
  return bind_query(*query);
}

StatusOr<std::string> Catalog::explain(RelationId id) const noexcept {
  EF_ASSIGN_OR_RETURN(const auto relation, state_.find(id));
  EF_ASSIGN_OR_RETURN(auto json, descriptor_json(*relation));
  if (relation->kind == RelationKind::BaseTable) {
    json["operators"] = Json::array({"ReadRelation"});
    json["has_internal_state"] = false;
  } else if (relation->kind == RelationKind::FunctionView) {
    json["operators"] = Json::array({"ReadRequestInputs", "FunctionView"});
    json["runtime_activation_required"] = true;
  } else if (relation->kind == RelationKind::LatestView) {
    json["operators"] =
        Json::array({"ReadCandidate", "ReadRequestStatus", "ReadRequestResult", "LatestSelector"});
    json["has_internal_state"] = true;
    json["runtime_activation_required"] = true;
  }

  EF_ASSIGN_OR_RETURN(auto inputs, dependencies(*relation));
  json["dependencies"] = inputs;
  return json.dump(2);
}
} // namespace eventflow
