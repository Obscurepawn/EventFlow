#include "eventflow/computation.hpp"
#include "trigger_audit_internal.hpp"
#include <algorithm>
#include <set>

namespace eventflow {
namespace {

Status invalid(std::string_view message) noexcept {
  return {ErrorCode::InvalidArgument, message};
}

bool same_fields(const std::vector<Field>& left, const std::vector<Field>& right) noexcept {
  if (left.size() != right.size())
    return false;

  for (std::size_t i = 0; i < left.size(); ++i)
    if (left[i].name != right[i].name || left[i].type != right[i].type ||
        left[i].nullable != right[i].nullable)
      return false;

  return true;
}

std::string stable_suffix(const BoundTrigger& trigger) noexcept {
  static constexpr char digits[] = "0123456789abcdef";
  std::string suffix;
  for (const unsigned char byte : trigger.id) {
    suffix.push_back(digits[byte >> 4]);
    suffix.push_back(digits[byte & 15]);
  }

  return suffix + "_" + std::to_string(trigger.version);
}

Status validate_session(const CatalogSnapshot& catalog, const ProcessorConfig& definition,
                        const BoundTrigger& trigger) noexcept {
  if (!trigger.request)
    return {};
  if (!trigger.session || !definition.requests || !definition.requests->candidate)
    return invalid("Request trigger requires an explicit session and candidate activation policy");

  const auto& policy = *definition.requests->candidate;
  if (trigger.session->relation != policy.relation || !policy.on_source || policy.version == 0)
    return invalid("Trigger session differs from the processor candidate policy");

  EF_ASSIGN_OR_RETURN(auto candidate, catalog.find(policy.relation));
  if (!candidate->candidate)
    return invalid("Session reference must identify a Candidate relation");

  const auto compatible = [&](RelationId id) noexcept -> Status {
    EF_ASSIGN_OR_RETURN(auto relation, catalog.find(id));
    return check(candidate->partition_types == relation->partition_types,
                 "Trigger capture and session must have the same business-key shape");
  };

  for (const auto& event : trigger.events)
    if (event.payload_relation != 0)
      EF_RETURN_IF_ERROR(compatible(event.payload_relation));

  if (trigger.rule)
    for (const auto& [alias, relation] : trigger.rule->reads) {
      (void)alias;
      EF_RETURN_IF_ERROR(compatible(relation));
    }
  if (trigger.pattern)
    for (const auto& [alias, relation] : trigger.pattern->reads) {
      (void)alias;
      EF_RETURN_IF_ERROR(compatible(relation));
    }
  if (trigger.capture)
    for (auto relation : trigger.capture->reads)
      EF_RETURN_IF_ERROR(compatible(relation));

  return {};
}

Status register_static_function(FunctionRegistry& registry,
                                const StaticFunctionDefinition& function,
                                const std::vector<Field>& input_fields) noexcept {
  FunctionSignature signature{
      .function_id = function.id,
      .version = function.version,
      .implementation_identity = function.implementation_identity,
      .inputs = {{.alias = "input", .required = true, .fields = input_fields}},
      .output_fields = function.output_fields,
      .pure = true};
  auto existing = registry.describe(function.id, function.version);
  if (existing.ok()) {
    if (existing->implementation_identity != signature.implementation_identity ||
        existing->inputs.size() != 1 || existing->inputs[0].alias != "input" ||
        !existing->inputs[0].required || !existing->pure ||
        !same_fields(existing->inputs[0].fields, input_fields) ||
        !same_fields(existing->output_fields, signature.output_fields))
      return Status(ErrorCode::Conflict,
                    "Static function identity has conflicting schema or implementation");

    return {};
  }

  if (existing.status().code() != ErrorCode::NotFound)
    return existing.status();

  return registry.add({.signature = std::move(signature),
                       .callback = [invoke = function.invoke](
                                       const FunctionInputs& inputs) noexcept -> StatusOr<Row> {
                         if (inputs.size() != 1 || !inputs.front())
                           return invalid("Static function requires one captured input row");

                         return invoke(*inputs.front());
                       }});
}

// The generated SQL projection keeps request identity hidden while exposing
// exactly the static function's typed payload. It permits function reuse across rules.
Status generate_request(Catalog& catalog, FunctionRegistry& registry, ProcessorConfig& definition,
                        BoundTrigger& trigger, Json& explanation) noexcept {
  auto& options = *trigger.request;
  const auto& function = *trigger.function;
  if (!trigger.capture || !same_fields(trigger.capture->fields, function.input_fields))
    return invalid("Static function input differs from its declared capture projection");
  if (!options.plan.empty() || options.make_admission)
    return invalid(
        "Generated function requests cannot override an existing plan or admission recipe");

  EF_ASSIGN_OR_RETURN(auto snapshot, catalog.snapshot());
  EF_ASSIGN_OR_RETURN(auto candidate, snapshot.find(trigger.session->relation));
  const auto suffix = stable_suffix(trigger);
  const auto capture_name = "trigger_capture_" + suffix;
  const auto input_name = "trigger_input_" + suffix;
  const auto result_name = "trigger_result_" + suffix;
  const auto status_name = "trigger_status_" + suffix;
  const auto latest_name = "trigger_latest_" + suffix;
  const auto domain = "trigger_requests_" + suffix;

  CreateTableAst table{.name = capture_name, .columns = {}, .primary_key = {}, .partition_key = {}};
  std::set<std::string> names;
  for (const auto& field : candidate->candidate->definition.scope_fields) {
    if (field.name == "request_id")
      return invalid("Session scope collides with request identity");

    table.columns.push_back({.name = field.name, .type = field.type, .nullable = false});
    table.primary_key.push_back(field.name);
    table.partition_key.push_back(field.name);
    names.insert(field.name);
  }

  table.columns.push_back({.name = "request_id", .type = DataType::String, .nullable = false});
  table.primary_key.push_back("request_id");
  names.insert("request_id");
  SelectAst projection{.projection = {},
                       .from = {.name = capture_name, .alias = {}},
                       .join = {},
                       .where = {},
                       .group_by = {}};
  for (std::size_t i = 0; i < function.input_fields.size(); ++i) {
    const auto& field = function.input_fields[i];
    auto stored_name = "trigger_value_" + std::to_string(i);
    while (names.contains(stored_name))
      stored_name.push_back('_');

    names.insert(stored_name);
    table.columns.push_back({.name = stored_name, .type = field.type, .nullable = field.nullable});
    projection.projection.push_back({.expression = column(stored_name), .alias = field.name});
  }

  EF_ASSIGN_OR_RETURN(
      auto capture,
      catalog.define(StatementAst(table), {.partition_key = {},
                                           .base_write_role = BaseWriteRole::RequestCapture,
                                           .identity_domain = domain}));
  EF_ASSIGN_OR_RETURN(auto input, catalog.define(StatementAst(CreateViewAst{
                                      .name = input_name, .query = std::move(projection)})));
  EF_ASSIGN_OR_RETURN(snapshot, catalog.snapshot());
  EF_ASSIGN_OR_RETURN(auto input_relation, snapshot.find(input));
  const auto visible_fields = std::vector<Field>(input_relation->storage_schema.fields.begin(),
                                                 input_relation->storage_schema.fields.begin() +
                                                     input_relation->visible_count);
  if (!same_fields(visible_fields, function.input_fields))
    return invalid("Generated function projection lost its typed payload schema");

  EF_RETURN_IF_ERROR(register_static_function(registry, function, visible_fields));
  EF_ASSIGN_OR_RETURN(auto result, catalog.define_function(
                                       {.name = result_name,
                                        .function_id = function.id,
                                        .function_version = function.version,
                                        .implementation_identity = function.implementation_identity,
                                        .inputs = {{.alias = "input",
                                                    .relation = input,
                                                    .required = true,
                                                    .fields = visible_fields}},
                                        .output_fields = function.output_fields}));
  EF_ASSIGN_OR_RETURN(snapshot, catalog.snapshot());
  EF_ASSIGN_OR_RETURN(auto root, snapshot.find(capture));
  EF_ASSIGN_OR_RETURN(auto status, catalog.define_request_status(
                                       {.name = status_name,
                                        .request_domain = domain,
                                        .identity_fields = root->identity_fields,
                                        .candidate_relation = trigger.session->relation}));
  EF_ASSIGN_OR_RETURN(auto latest,
                      catalog.define_latest({.name = latest_name,
                                             .result_relation = result,
                                             .request_status_relation = status,
                                             .candidate_relation = trigger.session->relation}));

  const auto capture_source = *trigger.capture;
  const auto expected_fields = function.input_fields;
  const auto plan_name = "trigger_plan_" + suffix;
  auto machine = options.machine.value_or(RequestMachineConfig{.max_nodes = 32,
                                                               .max_state_bytes = 256 * 1024,
                                                               .max_frozen_input_bytes = 64 * 1024,
                                                               .max_output_bytes = 64 * 1024});
  std::int64_t lifetime = 30'000'000;
  if (options.lifetime) {
    EF_ASSIGN_OR_RETURN(lifetime, options.lifetime->microseconds());
  }

  if (lifetime <= 0)
    return invalid("Request lifetime must be positive");

  machine.attempt_timeout_us = std::min(machine.attempt_timeout_us, lifetime);
  RequestPlanDefinition plan{
      .name = plan_name,
      .version = trigger.version,
      .targets = {result},
      .machine = machine,
      .default_lifetime_us = lifetime,
      .capture = [capture, capture_source, expected_fields](
                     RoundContext& round,
                     const RequestAdmission& admission) noexcept -> StatusOr<RequestCapture> {
        Row values;
        if (capture_source.source == CaptureSource::Match) {
          values = admission.arguments;
        } else {
          EF_ASSIGN_OR_RETURN(auto current, capture_source.current(round, admission.scope));
          if (!current)
            return RequestCapture{.ready = false, .rows = {}};

          values = std::move(*current);
        }

        EF_RETURN_IF_ERROR(validate_row_fields(expected_fields, values));
        Row identity = admission.scope;
        identity.emplace_back(admission.request_id);
        Row stored = identity;
        stored.insert(stored.end(), values.begin(), values.end());

        return RequestCapture{
            .ready = true,
            .rows = {
                {.relation = capture, .identity = std::move(identity), .row = std::move(stored)}}};
      },
      .request_status_relation = status};
  definition.requests->plans.push_back(std::move(plan));
  options.plan = plan_name;
  options.lifetime.reset();
  if (capture_source.source == CaptureSource::Match) {
    options.make_admission =
        [project = capture_source.match, expected_fields](
            RoundContext&,
            const TriggerOccurrence& occurrence) noexcept -> StatusOr<RequestAdmission> {
      EF_ASSIGN_OR_RETURN(auto arguments, project(occurrence));
      EF_RETURN_IF_ERROR(validate_row_fields(expected_fields, arguments));
      RequestAdmission admission;
      admission.arguments = std::move(arguments);

      return admission;
    };
  }

  explanation = {
      {"capture_relation", capture},
      {"capture_name", capture_name},
      {"input_relation", input},
      {"function_relation", result},
      {"function_name", result_name},
      {"status_relation", status},
      {"latest_relation", latest},
      {"latest_name", latest_name},
      {"plan", plan_name},
      {"function_id", function.id},
      {"function_version", function.version},
      {"implementation_identity", function.implementation_identity},
      {"capture_source", capture_source.source == CaptureSource::Match ? "match" : "current"},
      {"capture_signature", capture_source.signature},
      {"lifetime_us", lifetime}};
  return {};
}

Status validate_plans(const CatalogSnapshot& catalog, const ProcessorConfig& definition) noexcept {
  if (!definition.requests)
    return {};

  const auto& requests = *definition.requests;
  if (!requests.registry || requests.runtime.max_active_requests == 0 ||
      requests.runtime.max_active_bytes == 0 || requests.runtime.max_record_bytes < 2 ||
      requests.runtime.max_index_bytes == 0 || requests.runtime.max_argument_bytes == 0)
    return invalid("Request runtime budgets and registry must be defined");

  std::set<std::string> names;
  std::map<std::string, RelationId> statuses;
  for (const auto& plan : requests.plans) {
    if (plan.name.empty() || !names.insert(plan.name).second || plan.version == 0 ||
        !plan.capture || plan.targets.empty() || plan.default_lifetime_us <= 0 ||
        plan.default_lifetime_us > plan.machine.max_lifetime_us)
      return invalid("Invalid request plan definition");

    auto limits = plan.machine;
    limits.max_state_bytes =
        std::min(limits.max_state_bytes, requests.runtime.max_record_bytes / 2);
    EF_ASSIGN_OR_RETURN(auto machine,
                        RequestMachine::create(catalog, requests.registry, plan.targets, limits));
    (void)machine;
    EF_ASSIGN_OR_RETURN(auto target, catalog.find(plan.targets.front()));
    if (plan.request_status_relation) {
      EF_ASSIGN_OR_RETURN(auto status, catalog.find(*plan.request_status_relation));
      if (!status->request_status ||
          status->request_status->definition.request_domain != target->identity_domain ||
          !same_fields(status->request_status->definition.identity_fields, target->identity_fields))
        return invalid("Request status does not match the plan identity domain");

      const auto [found, inserted] = statuses.emplace(target->identity_domain, status->id);
      if (!inserted && found->second != status->id)
        return invalid("Request domain has conflicting status producers");
    }

    for (const auto& relation : catalog.relations) {
      if (!relation->latest)
        continue;

      EF_ASSIGN_OR_RETURN(auto source, catalog.find(relation->latest->definition.result_relation));
      if (source->identity_domain == target->identity_domain &&
          plan.request_status_relation != relation->latest->definition.request_status_relation)
        return invalid("Request plan status differs from its Latest view");
    }
  }

  return {};
}
} // namespace

StatusOr<BoundComputation> TriggerSet::bind(CatalogSnapshot original, ProcessorConfig definition,
                                            std::vector<Trigger> triggers,
                                            const EngineConfig& runtime) noexcept {
  if (triggers.empty() || triggers.size() > 64)
    return invalid("A computation requires one to 64 Trigger declarations");

  // Build on private copies. A failed binding must not leave half of a computation registered in
  // the caller's Catalog or function registry.
  EF_ASSIGN_OR_RETURN(auto catalog, Catalog::from_snapshot(original));
  std::shared_ptr<FunctionRegistry> registry;
  if (definition.requests) {
    if (definition.requests->registry) {
      EF_ASSIGN_OR_RETURN(registry, definition.requests->registry->clone());
    } else {
      EF_ASSIGN_OR_RETURN(registry, FunctionRegistry::create());
    }

    definition.requests->registry = registry;
  }

  std::set<std::string> rule_ids;
  for (const auto& rule : definition.rules)
    rule_ids.insert(rule.definition.id);

  for (const auto& rule : definition.cep_rules)
    rule_ids.insert(rule.definition.id);

  std::set<std::string> clocks;
  std::set<std::string> clock_kinds;
  for (const auto& clock : definition.periodic_policies) {
    clocks.insert(clock.id);
    if (!clock_kinds.insert(clock.event_kind).second)
      return invalid("Clock policies must use distinct event kinds");
  }

  std::set<std::string> source_kinds;
  std::optional<std::vector<DataType>> owner_scope;
  for (auto id : definition.input_relations) {
    EF_ASSIGN_OR_RETURN(auto input, original.find(id));
    source_kinds.insert(input->name);
    if (!owner_scope)
      owner_scope = input->partition_types;
    if (input->partition_types != *owner_scope)
      return invalid("Owner input relations require the same business-key shape");
  }

  if (!owner_scope)
    return invalid("Computation requires an owner input relation");

  Json explanations = Json::array();
  for (const auto& declaration : triggers) {
    EF_ASSIGN_OR_RETURN(auto trigger, bind_trigger(original, declaration));
    if (!rule_ids.insert(trigger.id).second)
      return invalid("Duplicate Trigger definition identity");

    for (const auto& event : trigger.events) {
      if (!trigger.clock && event.kind != "*")
        source_kinds.insert(event.kind);
      if (event.payload_relation == 0)
        continue;

      EF_ASSIGN_OR_RETURN(auto source, original.find(event.payload_relation));
      if (source->partition_types != *owner_scope)
        return invalid("Trigger source partition shape differs from its owner inputs");
    }

    EF_RETURN_IF_ERROR(validate_session(original, definition, trigger));
    Json generated = nullptr;
    if (trigger.function) {
      if (!registry)
        return invalid("Static function request requires a session-bound registry");

      EF_RETURN_IF_ERROR(generate_request(*catalog, *registry, definition, trigger, generated));
    }

    if (trigger.rule)
      definition.rules.push_back(std::move(*trigger.rule));
    if (trigger.pattern)
      definition.cep_rules.push_back(std::move(*trigger.pattern));
    if (trigger.clock) {
      if (!clocks.insert(trigger.clock->id).second)
        return invalid("Duplicate Clock policy identity");
      if (!clock_kinds.insert(trigger.clock->event_kind).second)
        return invalid("Clock policies must use distinct event kinds");

      definition.periodic_policies.push_back(std::move(*trigger.clock));
    }

    if (trigger.request) {
      auto& request = *trigger.request;
      if (request.lifetime || request.machine)
        if (!trigger.function)
          return invalid("Existing request plans own their lifetime and machine limits");

      EF_ASSIGN_OR_RETURN(auto pending, request.pending_for.microseconds());
      auto admission = request.make_admission;
      if (!admission)
        admission = [](RoundContext&,
                       const TriggerOccurrence&) noexcept -> StatusOr<RequestAdmission> {
          return RequestAdmission{};
        };

      std::string emission = "match";
      if (!trigger.pattern) {
        const auto& rule = definition.rules.back();
        const auto found = std::find_if(
            rule.definition.transitions.begin(), rule.definition.transitions.end(),
            [](const FsmTransition& transition) noexcept { return !transition.emit.empty(); });
        if (found == rule.definition.transitions.end())
          return invalid("Request Trigger has no emission");

        emission = found->emit;
      }

      definition.requests->trigger_bindings.push_back(
          {.id = request.id.empty() ? trigger.id : request.id,
           .version = request.version,
           .rule_id = trigger.id,
           .emission_name = emission,
           .plan = request.plan,
           .once_per_candidate = request.once_per_candidate,
           .pending_lifetime_us = pending,
           .make_admission = std::move(admission),
           .required_accepted_binding = request.required_accepted_binding,
           .mode = *request.mode,
           .deadline_column = request.deadline_column});
    }

    trigger.explanation["generated"] = std::move(generated);
    explanations.push_back(std::move(trigger.explanation));
  }

  for (const auto& kind : clock_kinds)
    if (source_kinds.contains(kind))
      return invalid("Clock event kind collides with a declared source kind");

  if (definition.trigger_audit.relation != 0)
    return invalid("Trigger audit relation identity is assigned by TriggerSet::bind");

  EF_ASSIGN_OR_RETURN(definition.trigger_audit.relation,
                      define_trigger_audit_relation(*catalog, *owner_scope));
  EF_ASSIGN_OR_RETURN(auto input, original.find(definition.input_relations.front()));
  Json scope_columns = Json::array();

  for (std::size_t i = 0; i < input->partition_slots.size(); ++i)
    scope_columns.push_back(
        {{"audit_column", "scope_key_" + std::to_string(i)},
         {"source_column", input->storage_schema.fields[input->partition_slots[i]].name}});

  Json audit = {{"relation", definition.trigger_audit.relation},
                {"name", trigger_audit_table_name},
                {"scope_columns", std::move(scope_columns)},
                {"max_rows_per_scope", definition.trigger_audit.max_rows_per_scope},
                {"max_bytes_per_scope", definition.trigger_audit.max_bytes_per_scope}};
  EF_ASSIGN_OR_RETURN(auto snapshot, catalog->snapshot());
  EF_RETURN_IF_ERROR(validate_plans(snapshot, definition));
  EF_RETURN_IF_ERROR(EventProcessor::validate_definition(snapshot, runtime, definition));

  return BoundComputation{.catalog = std::move(snapshot),
                          .registry = std::move(registry),
                          .definition = std::move(definition),
                          .explanation = {{"binding_version", 1},
                                          {"triggers", std::move(explanations)},
                                          {"trigger_audit", std::move(audit)}}};
}
} // namespace eventflow
