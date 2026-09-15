#include "eventflow/trigger.hpp"
#include <algorithm>

namespace eventflow {
namespace {

enum class TriggerKind { Event, Clock, Pattern };

bool valid_name(const std::string& value) noexcept {
  return !value.empty() && value.size() <= 128 && valid_utf8(value);
}

Status validate_event(const EventSource& event) noexcept {
  return check(valid_name(event.kind) && event.kind != "*" && event.kind != "$timeout" &&
                   event.kind != "$expire",
               "Trigger event kind is invalid or reserved");
}

CepLimits default_pattern_limits() noexcept {
  return {.max_nodes = 128,
          .max_paths = 32,
          .max_history_nodes = 128,
          .max_events = 128,
          .max_pending_matches = 32,
          .max_matches_per_step = 8,
          .max_timeouts_per_step = 32,
          .max_events_per_match = 8,
          .max_work_per_step = 4096,
          .max_event_bytes = 16 * 1024,
          .max_register_bytes = 8 * 1024,
          .max_read_bytes = 64 * 1024,
          .max_output_bytes = 64 * 1024,
          .max_state_bytes = 128 * 1024};
}

Json field_definitions(const std::vector<Field>& fields) noexcept {
  Json result = Json::array();
  for (const auto& field : fields)
    result.push_back(Json{{"id", field.id},
                          {"name", field.name},
                          {"type", data_type_name(field.type)},
                          {"nullable", field.nullable}});

  return result;
}

Json limit_definition(const CepLimits& limits) noexcept {
  return Json{{"nodes", limits.max_nodes},
              {"paths", limits.max_paths},
              {"history_nodes", limits.max_history_nodes},
              {"events", limits.max_events},
              {"pending_matches", limits.max_pending_matches},
              {"matches_per_step", limits.max_matches_per_step},
              {"timeouts_per_step", limits.max_timeouts_per_step},
              {"events_per_match", limits.max_events_per_match},
              {"work_per_step", limits.max_work_per_step},
              {"event_bytes", limits.max_event_bytes},
              {"register_bytes", limits.max_register_bytes},
              {"read_bytes", limits.max_read_bytes},
              {"output_bytes", limits.max_output_bytes},
              {"state_bytes", limits.max_state_bytes}};
}

Status merge_reads(std::map<std::string, RelationId>& target,
                   const std::map<std::string, RelationId>& added) noexcept {
  for (const auto& [alias, relation] : added) {
    const auto [found, inserted] = target.emplace(alias, relation);
    EF_RETURN_IF_ERROR(check(inserted || found->second == relation,
                             "Trigger read alias names different relations: " + alias));
  }

  return {};
}

Status validate_scope(const CatalogSnapshot& catalog,
                      const std::map<std::string, RelationId>& reads,
                      const std::vector<EventSource>& events) noexcept {
  std::optional<std::vector<DataType>> scope;
  std::map<std::string, RelationId> event_layouts;
  const auto add = [&](RelationId id) noexcept -> Status {
    if (id == 0)
      return {};

    EF_ASSIGN_OR_RETURN(auto relation, catalog.find(id));
    if (!scope)
      scope = relation->partition_types;

    return check(*scope == relation->partition_types,
                 "Trigger inputs and reads require the same business scope");
  };
  for (const auto& [alias, relation] : reads) {
    (void)alias;
    EF_RETURN_IF_ERROR(add(relation));
  }

  for (const auto& event : events) {
    const auto [existing, inserted] = event_layouts.emplace(event.kind, event.payload_relation);
    EF_RETURN_IF_ERROR(check(inserted || existing->second == event.payload_relation,
                             "Trigger event kind has conflicting payload schemas: " + event.kind));
    EF_RETURN_IF_ERROR(add(event.payload_relation));
  }

  return {};
}

Status validate_request(const TriggerRequest& request, bool generated) noexcept {
  EF_RETURN_IF_ERROR(check(request.version > 0, "Trigger request version must be positive"));
  EF_RETURN_IF_ERROR(check(request.mode.has_value(), "Trigger request mode must be explicit"));
  EF_RETURN_IF_ERROR(check(*request.mode == RequestTriggerMode::Wait ||
                               *request.mode == RequestTriggerMode::TryOnceAtFiring,
                           "Trigger request mode is invalid"));
  EF_ASSIGN_OR_RETURN(auto pending, request.pending_for.microseconds());
  EF_RETURN_IF_ERROR(check(pending > 0, "Trigger pending_for must be positive"));
  if (request.lifetime) {
    EF_ASSIGN_OR_RETURN(auto lifetime, request.lifetime->microseconds());
    EF_RETURN_IF_ERROR(check(lifetime > 0, "Trigger request lifetime must be positive"));
  }

  EF_RETURN_IF_ERROR(check((generated && request.id.empty()) || valid_name(request.id),
                           "Trigger request ID must be valid"));
  EF_RETURN_IF_ERROR(check((generated && request.plan.empty()) || valid_name(request.plan),
                           "Trigger request plan must be valid"));
  EF_RETURN_IF_ERROR(check(request.required_accepted_binding.empty() ||
                               valid_name(request.required_accepted_binding),
                           "Trigger prerequisite binding is invalid"));
  EF_RETURN_IF_ERROR(check(request.deadline_column.empty() || valid_name(request.deadline_column),
                           "Trigger deadline column is invalid"));
  return {};
}

} // namespace

struct Trigger::Data {
  std::string id;
  std::uint64_t version = 1;
  std::string implementation_identity;
  TriggerKind kind = TriggerKind::Event;
  std::optional<EventRef> first;
  std::optional<ClockSchedule> clock;
  std::function<StatusOr<BoundTriggerPattern>(const CatalogSnapshot&)> pattern;
  Predicate first_condition;
  Predicate second_condition;
  std::optional<EventRef> second;
  std::optional<NextPolicy> next_policy;
  std::optional<TriggerDuration> within;
  bool rearm = false;
  std::string emission = "trigger";
  CepLimits limits = default_pattern_limits();
  bool explicit_limits = false;
  std::optional<SessionRef> session;
  std::optional<TriggerRequest> request;
  std::optional<StaticFunctionDefinition> function;
  TriggerCaptureBinder capture;
  Status status;

  void fail(Status failure) noexcept {
    if (status.ok())
      status = failure;
  }
};

Trigger::Trigger(std::shared_ptr<const Data> data) noexcept : data_(std::move(data)) {}

Trigger Trigger::on_event(std::string id, EventRef event) noexcept {
  auto data = std::make_shared<Data>();
  data->id = std::move(id);
  data->first = std::move(event);
  data->fail(
      check(valid_name(data->id), "Trigger ID must be nonempty UTF-8 and at most 128 bytes"));
  data->fail(validate_event(data->first->definition()));

  return Trigger(std::move(data));
}

Trigger Trigger::on_event(std::string id, std::string event_kind) noexcept {
  if (event_kind == "*") {
    auto data = std::make_shared<Data>();
    data->id = std::move(id);
    data->first = EventRef::source({.kind = "*"});
    data->fail(
        check(valid_name(data->id), "Trigger ID must be nonempty UTF-8 and at most 128 bytes"));
    return Trigger(std::move(data));
  }

  return on_event(std::move(id), EventRef::source({.kind = std::move(event_kind)}));
}

Trigger Trigger::clock(std::string id, ClockSchedule schedule) noexcept {
  auto data = std::make_shared<Data>();
  data->id = std::move(id);
  data->kind = TriggerKind::Clock;
  data->clock = std::move(schedule);
  data->fail(
      check(valid_name(data->id), "Trigger ID must be nonempty UTF-8 and at most 128 bytes"));
  return Trigger(std::move(data));
}

Trigger Trigger::from_pattern(
    std::string id,
    std::function<StatusOr<BoundTriggerPattern>(const CatalogSnapshot&)> binder) noexcept {
  auto data = std::make_shared<Data>();
  data->id = std::move(id);
  data->kind = TriggerKind::Pattern;
  data->pattern = std::move(binder);
  data->emission = "match";
  data->fail(
      check(valid_name(data->id), "Trigger ID must be nonempty UTF-8 and at most 128 bytes"));
  return Trigger(std::move(data));
}

Trigger Trigger::pattern(std::string id, Pattern pattern) noexcept {
  return from_pattern(std::move(id),
                      [pattern = std::move(pattern)](const CatalogSnapshot& catalog) noexcept {
                        return bind_trigger_pattern(catalog, pattern);
                      });
}

Trigger Trigger::version(std::uint64_t version) const noexcept {
  auto data = std::make_shared<Data>(*data_);
  data->version = version;
  data->fail(check(version > 0, "Trigger version must be positive"));
  return Trigger(std::move(data));
}

Trigger Trigger::implementation_identity(std::string identity) const noexcept {
  auto data = std::make_shared<Data>(*data_);
  data->fail(check(!identity.empty() && identity.size() <= 1024 && valid_utf8(identity),
                   "Trigger implementation identity must be nonempty UTF-8"));
  data->implementation_identity = std::move(identity);

  return Trigger(std::move(data));
}

Trigger Trigger::when(Predicate predicate) const noexcept {
  auto data = std::make_shared<Data>(*data_);
  data->fail(check(data->kind != TriggerKind::Pattern, "Pattern conditions belong in where()"));
  if (data->second)
    data->second_condition = data->second_condition && std::move(predicate);
  else
    data->first_condition = data->first_condition && std::move(predicate);
  return Trigger(std::move(data));
}

Trigger Trigger::then_next(EventRef event, NextPolicy policy) const noexcept {
  auto data = std::make_shared<Data>(*data_);
  data->fail(check(data->kind != TriggerKind::Pattern && !data->second,
                   "then_next supports exactly one ordinary Trigger wait"));
  data->fail(check(policy == NextPolicy::OfKind || policy == NextPolicy::Strict,
                   "then_next requires a valid adjacency policy"));
  data->fail(validate_event(event.definition()));
  data->second = std::move(event);
  data->next_policy = policy;

  return Trigger(std::move(data));
}

Trigger Trigger::within(TriggerDuration duration) const noexcept {
  auto data = std::make_shared<Data>(*data_);
  data->fail(
      check(data->kind != TriggerKind::Pattern, "Pattern windows belong in Pattern::within"));
  auto checked = duration.microseconds();
  data->fail(checked.ok() ? check(*checked > 0, "Trigger within must be positive")
                          : checked.status());
  data->within = duration;

  return Trigger(std::move(data));
}

Trigger Trigger::rearm_on_next_event() const noexcept {
  auto data = std::make_shared<Data>(*data_);
  data->fail(check(data->kind != TriggerKind::Pattern,
                   "Pattern start and skip policies control rearming"));
  data->rearm = true;

  return Trigger(std::move(data));
}

Trigger Trigger::emit(std::string emission) const noexcept {
  auto data = std::make_shared<Data>(*data_);
  data->fail(check(valid_name(emission), "Trigger emission must be valid"));
  data->fail(check(data->kind != TriggerKind::Pattern || emission == "match",
                   "Pattern emission is fixed to match"));
  data->emission = std::move(emission);

  return Trigger(std::move(data));
}

Trigger Trigger::limits(CepLimits limits) const noexcept {
  auto data = std::make_shared<Data>(*data_);
  data->fail(
      check(data->kind == TriggerKind::Pattern, "CEP limits apply only to Pattern triggers"));
  data->limits = limits;
  data->explicit_limits = true;

  return Trigger(std::move(data));
}

Trigger Trigger::in_session(SessionRef session) const noexcept {
  auto data = std::make_shared<Data>(*data_);
  data->fail(check(session.relation != 0, "Trigger session requires a Candidate relation"));
  data->session = session;
  return Trigger(std::move(data));
}

Trigger Trigger::request(TriggerRequest request) const noexcept {
  auto data = std::make_shared<Data>(*data_);
  data->fail(check(!data->capture && !data->function,
                   "A captured input requires its typed Function request"));
  data->fail(check(!data->request, "Trigger request is already declared"));
  data->fail(validate_request(request, false));
  data->request = std::move(request);

  return Trigger(std::move(data));
}

Trigger Trigger::with_capture(TriggerCaptureBinder binder) const noexcept {
  auto data = std::make_shared<Data>(*data_);
  data->fail(check(!data->capture && !data->request,
                   "Trigger capture must be declared once before its Function request"));
  data->capture = std::move(binder);

  return Trigger(std::move(data));
}

Trigger Trigger::capture_current(RelationId view, std::vector<std::string> fields,
                                 std::vector<Field> expected) const noexcept {
  return with_capture([view, fields = std::move(fields), expected = std::move(expected)](
                          const CatalogSnapshot& catalog, const std::vector<PatternLabelInfo>&,
                          const std::string&) noexcept -> StatusOr<BoundInputCapture> {
    EF_ASSIGN_OR_RETURN(auto relation, catalog.find(view));
    EF_RETURN_IF_ERROR(
        check(relation->unique_per_partition && !fields.empty() && fields.size() <= 64,
              "Current capture requires a unique view and one to 64 fields"));
    if (!expected.empty() && expected.size() != fields.size())
      return Status(ErrorCode::InvalidArgument, "Capture field count differs from function input");
    std::vector<std::size_t> slots;
    std::vector<Field> output;
    for (std::size_t i = 0; i < fields.size(); ++i) {
      EF_ASSIGN_OR_RETURN(auto slot, relation->storage_schema.index_of(fields[i]));
      if (slot >= relation->visible_count)
        return Status(ErrorCode::InvalidArgument, "Capture cannot read hidden fields");
      auto field = relation->storage_schema.fields[slot];
      if (!expected.empty()) {
        if (expected[i].type != field.type)
          return Status(ErrorCode::InvalidArgument,
                        "Capture column type differs from function input");
        field = expected[i];
      }
      slots.push_back(slot);
      output.push_back(std::move(field));
    }
    return BoundInputCapture{
        .source = CaptureSource::Current,
        .fields = output,
        .reads = {view},
        .signature = Json{{"format", "named_current_capture.v1"},
                          {"view", view},
                          {"columns", fields},
                          {"output", field_definitions(output)}}
                         .dump(),
        .match = {},
        .current = [view, slots = std::move(slots),
                    output](RoundContext& round,
                            const Row& scope) noexcept -> StatusOr<std::optional<Row>> {
          EF_ASSIGN_OR_RETURN(auto current, round.read_relation(view, scope));
          if (!current)
            return std::optional<Row>{};
          Row result;
          for (std::size_t i = 0; i < slots.size(); ++i) {
            if (slots[i] >= current->size())
              return Status(ErrorCode::InvalidArgument, "Capture row is narrower than its schema");
            const auto& value = (*current)[slots[i]];
            if (std::holds_alternative<std::monostate>(value) && !output[i].nullable)
              return std::optional<Row>{};
            result.push_back(value);
          }
          return std::optional<Row>(std::move(result));
        }};
  });
}

Trigger Trigger::request(StaticFunctionDefinition function, TriggerRequest request) const noexcept {
  return with_function(std::move(function), std::move(request));
}

Trigger Trigger::with_function(StaticFunctionDefinition function,
                               TriggerRequest request) const noexcept {
  auto data = std::make_shared<Data>(*data_);
  data->fail(check(bool(data->capture) && !data->request && !data->function,
                   "Static Function request requires one input capture"));
  data->fail(
      check(!request.make_admission, "Static Function capture cannot override make_admission"));
  data->fail(validate_request(request, true));
  data->function = std::move(function);
  data->request = std::move(request);

  return Trigger(std::move(data));
}

Trigger Trigger::with_error(Status status) const noexcept {
  auto data = std::make_shared<Data>(*data_);
  data->fail(status);
  return Trigger(std::move(data));
}

StatusOr<BoundTrigger> bind_trigger(const CatalogSnapshot& catalog,
                                    const Trigger& trigger) noexcept {
  const auto& data = *trigger.data_;
  EF_RETURN_IF_ERROR(data.status);

  BoundTrigger output{.id = data.id,
                      .version = data.version,
                      .rule = {},
                      .pattern = {},
                      .clock = {},
                      .session = data.session,
                      .request = data.request,
                      .function = data.function,
                      .capture = {},
                      .explanation = {}};

  Json metadata{{"compiler", "eventflow.trigger.v1"},
                {"id", data.id},
                {"version", std::to_string(data.version)},
                {"implementation_identity", data.implementation_identity},
                {"emission", data.emission}};
  std::vector<PatternLabelInfo> labels;
  std::vector<EventSource> events;
  std::map<std::string, RelationId> reads;

  if (data.session) {
    EF_ASSIGN_OR_RETURN(auto candidate, catalog.find(data.session->relation));
    EF_RETURN_IF_ERROR(
        check(bool(candidate->candidate), "Trigger session must reference a Candidate relation"));
    metadata["session"] = data.session->relation;
  }

  if (data.kind == TriggerKind::Pattern) {
    EF_RETURN_IF_ERROR(check(bool(data.pattern), "Trigger Pattern binder is empty"));
    EF_ASSIGN_OR_RETURN(auto bound, data.pattern(catalog));
    labels = std::move(bound.labels);
    events = std::move(bound.events);
    reads = std::move(bound.reads);

    EF_ASSIGN_OR_RETURN(metadata["pattern"], parse_json_exact(bound.signature));
    metadata["kind"] = "pattern";
    metadata["within_us"] = std::to_string(bound.within_us);
    metadata["skip"] = static_cast<int>(bound.skip);
    metadata["input_kinds"] = bound.input_kinds;
    metadata["limits"] = limit_definition(data.limits);
    metadata["limits_source"] = data.explicit_limits ? "explicit" : "bounded_default";

    CepDefinition definition{.id = data.id,
                             .version = data.version,
                             .pattern = std::move(bound.pattern),
                             .within_us = bound.within_us,
                             .skip = bound.skip,
                             .reads = {},
                             .register_fields = {},
                             .initial_registers = {},
                             .limits = data.limits};
    for (const auto& [alias, relation] : reads) {
      (void)relation;
      definition.reads.push_back(alias);
    }

    output.pattern = ProcessorCepRule{.definition = std::move(definition),
                                      .reads = reads,
                                      .sink = {},
                                      .input_kinds = std::move(bound.input_kinds)};
  } else {
    std::optional<EventRef> first = data.first;
    if (data.kind == TriggerKind::Clock) {
      const auto& schedule = *data.clock;
      EF_RETURN_IF_ERROR(check(valid_name(schedule.policy_id), "Clock policy ID must be valid"));
      EF_ASSIGN_OR_RETURN(auto every, schedule.every.microseconds());
      EF_ASSIGN_OR_RETURN(auto phase, schedule.phase.microseconds());
      EF_RETURN_IF_ERROR(
          check(every > 0 && phase >= 0, "Clock every must be positive and phase nonnegative"));

      auto kind = schedule.event_kind.empty() ? "__trigger_clock_" + data.id : schedule.event_kind;
      EF_RETURN_IF_ERROR(validate_event({.kind = kind}));
      output.clock = PeriodicPolicy{
          .id = schedule.policy_id, .event_kind = kind, .interval_us = every, .phase_us = phase};
      first = EventRef::source({.kind = std::move(kind)});
      metadata["clock"] = Json{{"policy_id", schedule.policy_id},
                               {"event_kind", first->definition().kind},
                               {"every_us", std::to_string(every)},
                               {"phase_us", std::to_string(phase)}};
    }

    EF_RETURN_IF_ERROR(check(first.has_value(), "Trigger input event is missing"));
    EF_RETURN_IF_ERROR(check(!data.second || (data.within && data.rearm),
                             "then_next requires within and rearm_on_next_event"));
    EF_RETURN_IF_ERROR(check(data.second || (!data.within && !data.rearm),
                             "within and rearm_on_next_event require then_next"));
    EF_ASSIGN_OR_RETURN(auto first_guard,
                        bind_trigger_predicate(catalog, data.first_condition, first));
    reads = std::move(first_guard.reads);
    events = std::move(first_guard.events);
    EF_ASSIGN_OR_RETURN(metadata["condition"], parse_json_exact(first_guard.signature));
    metadata["kind"] = data.kind == TriggerKind::Clock ? "clock" : "event";
    metadata["event"] = first->definition().kind;

    FsmDefinition definition;
    definition.id = data.id;
    definition.version = data.version;
    definition.initial = "idle";
    definition.states = {{.name = "idle", .timeout_us = {}}};

    if (!data.second) {
      definition.transitions.push_back({.from = "idle",
                                        .event = first->definition().kind,
                                        .to = "idle",
                                        .guard = std::move(first_guard.fsm),
                                        .update = {},
                                        .emit = data.emission});
    } else {

      EF_ASSIGN_OR_RETURN(auto within, data.within->microseconds());
      EF_ASSIGN_OR_RETURN(auto second_guard,
                          bind_trigger_predicate(catalog, data.second_condition, data.second));
      EF_RETURN_IF_ERROR(merge_reads(reads, second_guard.reads));
      events.insert(events.end(), second_guard.events.begin(), second_guard.events.end());

      metadata["next"] =
          Json{{"event", data.second->definition().kind},
               {"policy", *data.next_policy == NextPolicy::Strict ? "strict" : "of_kind"},
               {"within_us", std::to_string(within)},
               {"rearm", "next_event"}};
      EF_ASSIGN_OR_RETURN(metadata["next"]["condition"], parse_json_exact(second_guard.signature));

      definition.states.push_back({.name = "armed", .timeout_us = within});
      definition.transitions = {{.from = "idle",
                                 .event = first->definition().kind,
                                 .to = "armed",
                                 .guard = std::move(first_guard.fsm),
                                 .update = {},
                                 .emit = {}},
                                {.from = "armed",
                                 .event = data.second->definition().kind,
                                 .to = "idle",
                                 .guard = std::move(second_guard.fsm),
                                 .update = {},
                                 .emit = data.emission},
                                {.from = "armed",
                                 .event = data.second->definition().kind,
                                 .to = "idle",
                                 .guard = {},
                                 .update = {},
                                 .emit = {}},
                                {.from = "armed",
                                 .event = "$timeout",
                                 .to = "idle",
                                 .guard = {},
                                 .update = {},
                                 .emit = {}}};

      if (*data.next_policy == NextPolicy::Strict)
        definition.transitions.push_back(
            {.from = "armed", .event = "*", .to = "idle", .guard = {}, .update = {}, .emit = {}});
    }

    for (const auto& [alias, relation] : reads) {
      (void)relation;
      definition.reads.push_back(alias);
    }

    output.rule = ProcessorRule{.definition = std::move(definition), .reads = reads, .sink = {}};
  }

  EF_RETURN_IF_ERROR(validate_scope(catalog, reads, events));
  metadata["reads"] = Json::object();
  for (const auto& [alias, relation] : reads)
    metadata["reads"][alias] = relation;

  if (data.request) {
    EF_RETURN_IF_ERROR(validate_request(*data.request, data.function.has_value()));
    EF_ASSIGN_OR_RETURN(auto pending, data.request->pending_for.microseconds());
    metadata["request"] =
        Json{{"id", data.request->id},
             {"version", std::to_string(data.request->version)},
             {"plan", data.request->plan},
             {"mode", *data.request->mode == RequestTriggerMode::Wait ? "wait" : "try_once"},
             {"pending_us", std::to_string(pending)},
             {"once_per_candidate", data.request->once_per_candidate},
             {"required_accepted_binding", data.request->required_accepted_binding},
             {"deadline_column", data.request->deadline_column},
             {"custom_admission", bool(data.request->make_admission)}};
    if (data.request->lifetime) {
      EF_ASSIGN_OR_RETURN(auto lifetime, data.request->lifetime->microseconds());
      metadata["request"]["lifetime_us"] = std::to_string(lifetime);
    }

    if (data.request->make_admission)
      EF_RETURN_IF_ERROR(check(!data.implementation_identity.empty(),
                               "Custom admission requires Trigger implementation_identity"));
  }

  if (data.function) {
    metadata["function"] = Json{{"id", data.function->id},
                                {"version", std::to_string(data.function->version)},
                                {"implementation_identity", data.function->implementation_identity},
                                {"input_fields", field_definitions(data.function->input_fields)},
                                {"output_fields", field_definitions(data.function->output_fields)}};
  }

  if (data.capture) {
    EF_RETURN_IF_ERROR(check(data.function.has_value() && data.request.has_value(),
                             "Trigger capture requires a static Function request"));
    const auto identity = data.id + ":" + std::to_string(data.version) + ":" +
                          data.function->implementation_identity + ":" +
                          data.implementation_identity;
    EF_ASSIGN_OR_RETURN(auto capture, data.capture(catalog, labels, identity));
    EF_RETURN_IF_ERROR(
        check(capture.source != CaptureSource::Match || data.kind == TriggerKind::Pattern,
              "capture_match requires a Pattern trigger"));
    EF_RETURN_IF_ERROR(check(capture.fields.size() == data.function->input_fields.size(),
                             "Capture output does not match Function input schema"));
    for (std::size_t i = 0; i < capture.fields.size(); ++i) {
      const auto& actual = capture.fields[i];
      const auto& expected = data.function->input_fields[i];
      EF_RETURN_IF_ERROR(
          check(actual.name == expected.name && actual.type == expected.type &&
                    actual.nullable == expected.nullable,
                "Capture output does not match Function input field: " + expected.name));
    }

    EF_ASSIGN_OR_RETURN(metadata["capture"], parse_json_exact(capture.signature));
    output.capture = std::move(capture);
  }

  const auto signature = metadata.dump();
  if (output.rule) {
    output.rule->definition.definition_identity = signature;
    EF_ASSIGN_OR_RETURN(auto program, FsmProgram::create(output.rule->definition));
    (void)program;
  } else {
    output.pattern->definition.definition_identity = signature;
    EF_ASSIGN_OR_RETURN(auto program, CepProgram::create(output.pattern->definition));
    (void)program;
  }

  output.explanation = std::move(metadata);
  output.events = std::move(events);
  if (data.kind == TriggerKind::Event && data.first &&
      data.first->definition().payload_relation == 0)
    output.events.push_back(data.first->definition());

  return output;
}

} // namespace eventflow
