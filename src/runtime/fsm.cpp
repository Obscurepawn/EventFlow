#include "eventflow/fsm.hpp"
#include <algorithm>
#include <set>

namespace eventflow {
namespace {
bool valid_name(std::string_view name) noexcept {
  return !name.empty() && name.size() <= 512 && valid_utf8(name);
}

StatusOr<Json> row_json(const Row& row) noexcept {
  Json values = Json::array();
  for (const auto& value : row) {
    EF_ASSIGN_OR_RETURN(auto encoded, value_to_json(value));
    values.push_back(std::move(encoded));
  }

  return values;
}

Status validate_registers(const std::vector<Field>& fields, const Row& registers) noexcept {
  EF_RETURN_IF_ERROR(check(registers.size() == fields.size(), "FSM register count mismatch"));
  for (std::size_t i = 0; i < fields.size(); ++i) {
    EF_ASSIGN_OR_RETURN(auto json, value_to_json(registers[i]));
    EF_ASSIGN_OR_RETURN(auto typed, value_from_json(json, fields[i].type, fields[i].nullable));

    // No implicit coercion of already typed state, including Int64 to Float64.
    EF_RETURN_IF_ERROR(check(typed.index() == registers[i].index(), "FSM register type mismatch"));
  }

  return {};
}

const FsmStateDefinition* find_state(const FsmDefinition& definition,
                                     const std::string& name) noexcept {
  for (const auto& state : definition.states)
    if (state.name == name)
      return &state;

  return nullptr;
}

StatusOr<std::uint64_t> unsigned_field(const Json& json, const char* field) noexcept {
  if (!json.contains(field) || !json[field].is_number_integer() ||
      (!json[field].is_number_unsigned() && json[field].get<std::int64_t>() < 0))
    return Status(ErrorCode::InvalidArgument, "invalid FSM unsigned integer");

  return json[field].get<std::uint64_t>();
}

StatusOr<std::int64_t> time_field(const Json& json, const char* field) noexcept {
  EF_ASSIGN_OR_RETURN(auto value, unsigned_field(json, field));
  EF_RETURN_IF_ERROR(check(value <= INT64_MAX, "FSM time overflow"));
  return static_cast<std::int64_t>(value);
}

Status enter(FsmState& state, const FsmStateDefinition& target, std::int64_t time_us) noexcept {
  state.state = target.name;
  state.time_us = time_us;
  state.entered_at_us = time_us;
  state.deadline_us.reset();
  if (target.timeout_us) {
    if (time_us > INT64_MAX - *target.timeout_us)
      return Status(ErrorCode::ResourceExhausted, "FSM deadline exhausted");

    state.deadline_us = time_us + *target.timeout_us;
  }

  return {};
}
} // namespace

FsmProgram::FsmProgram(FsmDefinition definition) noexcept : definition_(std::move(definition)) {}

StatusOr<std::unique_ptr<FsmProgram>> FsmProgram::create(FsmDefinition definition) noexcept {
  EF_RETURN_IF_ERROR(check(definition.definition_identity.size() <= 1024 * 1024 &&
                               valid_utf8(definition.definition_identity),
                           "FSM definition identity must be valid UTF-8 within 1 MiB"));
  EF_RETURN_IF_ERROR(
      check(valid_name(definition.id) && definition.version > 0 && !definition.states.empty() &&
                definition.states.size() <= 128 && definition.transitions.size() <= 1024 &&
                definition.register_fields.size() <= 64 && definition.reads.size() <= 64 &&
                definition.max_steps > 0 && definition.max_steps <= 1024 &&
                definition.max_state_bytes > 0 && definition.max_state_bytes <= 16 * 1024 * 1024,
            "invalid FSM definition or limits"));
  EF_RETURN_IF_ERROR(
      check(definition.reset_event.empty() ||
                (valid_name(definition.reset_event) && definition.reset_event != "$timeout" &&
                 definition.reset_event != "*"),
            "invalid FSM reset event"));
  std::set<std::string> states;

  for (const auto& state : definition.states) {
    EF_RETURN_IF_ERROR(check(valid_name(state.name) && states.insert(state.name).second &&
                                 (!state.timeout_us || *state.timeout_us > 0),
                             "invalid FSM state or timeout"));
  }

  EF_RETURN_IF_ERROR(check(states.contains(definition.initial), "unknown initial FSM state"));
  std::set<std::string> names;
  std::set<std::uint32_t> ids;
  for (const auto& field : definition.register_fields)
    EF_RETURN_IF_ERROR(check(valid_name(field.name) && names.insert(field.name).second &&
                                 ids.insert(field.id).second && !data_type_name(field.type).empty(),
                             "invalid FSM register field"));

  EF_RETURN_IF_ERROR(validate_registers(definition.register_fields, definition.initial_registers));
  names.clear();
  for (const auto& read : definition.reads)
    EF_RETURN_IF_ERROR(check(valid_name(read) && names.insert(read).second,
                             "invalid or duplicate FSM read alias"));

  for (const auto& transition : definition.transitions) {
    EF_RETURN_IF_ERROR(check(states.contains(transition.from) && states.contains(transition.to) &&
                                 valid_name(transition.event) &&
                                 (transition.emit.empty() || valid_name(transition.emit)),
                             "invalid FSM transition"));
    if (transition.event == "$timeout")
      EF_RETURN_IF_ERROR(check(find_state(definition, transition.from)->timeout_us.has_value(),
                               "timeout transition requires state deadline"));
  }

  for (const auto& state : definition.states) {
    if (!state.timeout_us)
      continue;

    bool fallback = false;
    for (const auto& transition : definition.transitions)
      fallback |=
          transition.from == state.name && transition.event == "$timeout" && !transition.guard;

    EF_RETURN_IF_ERROR(check(fallback, "timed state requires an unconditional timeout fallback"));
  }

  auto program = std::unique_ptr<FsmProgram>(new FsmProgram(std::move(definition)));
  EF_ASSIGN_OR_RETURN(auto state, program->initial_state());
  EF_ASSIGN_OR_RETURN(auto encoded, program->encode_state(state));
  (void)encoded;

  return program;
}

StatusOr<FsmState> FsmProgram::initial_state(std::int64_t time_us) const noexcept {
  EF_RETURN_IF_ERROR(check(time_us >= 0, "negative FSM processing time"));
  FsmState state;
  state.program_id = definition_.id;
  state.program_version = definition_.version;
  state.registers = definition_.initial_registers;
  EF_RETURN_IF_ERROR(enter(state, *find_state(definition_, definition_.initial), time_us));
  EF_ASSIGN_OR_RETURN(auto encoded, encode_state(state));
  (void)encoded;

  return state;
}

Status FsmProgram::validate_state(const FsmState& state) const noexcept {
  EF_RETURN_IF_ERROR(
      check(state.program_id == definition_.id && state.program_version == definition_.version,
            "FSM program identity or version mismatch"));
  const auto* definition = find_state(definition_, state.state);
  EF_RETURN_IF_ERROR(check(definition && state.time_us >= 0 && state.entered_at_us >= 0 &&
                               state.entered_at_us <= state.time_us,
                           "invalid FSM state or processing time"));
  if (definition->timeout_us) {
    EF_RETURN_IF_ERROR(
        check(state.entered_at_us <= INT64_MAX - *definition->timeout_us && state.deadline_us &&
                  *state.deadline_us == state.entered_at_us + *definition->timeout_us &&
                  *state.deadline_us > state.time_us,
              "invalid FSM state deadline"));
  } else
    EF_RETURN_IF_ERROR(check(!state.deadline_us, "untimed FSM state has a deadline"));
  return validate_registers(definition_.register_fields, state.registers);
}

Status FsmProgram::validate_reads(const FsmReads& reads) const noexcept {
  EF_RETURN_IF_ERROR(
      check(reads.size() == definition_.reads.size(), "FSM declared read set mismatch"));
  std::size_t bytes = 0;
  for (const auto& alias : definition_.reads) {
    const auto found = reads.find(alias);
    EF_RETURN_IF_ERROR(check(found != reads.end(), "missing declared FSM view read"));
    if (!found->second)
      continue;

    EF_ASSIGN_OR_RETURN(auto encoded, row_json(*found->second));
    const auto size = encoded.dump().size();
    if (size > definition_.max_state_bytes - bytes)
      return Status(ErrorCode::ResourceExhausted, "FSM read budget exceeded");

    bytes += size;
  }

  return {};
}

Status FsmProgram::transition(FsmStep& output, const FsmEvent& event, const FsmReads& reads,
                              std::size_t& steps) const noexcept {
  for (const auto& transition : definition_.transitions) {
    if (transition.from != output.state.state ||
        !(transition.event == event.kind || (transition.event == "*" && event.kind != "$timeout")))
      continue;

    const FsmContext context{.state = output.state, .event = event, .reads = reads};
    if (transition.guard) {
      EF_ASSIGN_OR_RETURN(auto passes, transition.guard(context));
      if (!passes)
        continue;
    }

    if (steps == definition_.max_steps || output.state.transition_sequence == UINT64_MAX)
      return Status(ErrorCode::ResourceExhausted, "FSM transition budget or sequence exhausted");

    Row registers = output.state.registers;
    if (transition.update) {
      EF_ASSIGN_OR_RETURN(registers, transition.update(context));
      EF_RETURN_IF_ERROR(validate_registers(definition_.register_fields, registers));
    }

    EF_RETURN_IF_ERROR(enter(output.state, *find_state(definition_, transition.to), event.time_us));
    output.state.registers = std::move(registers);
    ++output.state.transition_sequence;
    ++steps;
    EF_ASSIGN_OR_RETURN(auto encoded, encode_state(output.state));
    (void)encoded;
    if (!transition.emit.empty())
      output.emissions.push_back({.name = transition.emit,
                                  .cause = event.occurrence_id,
                                  .transition_sequence = output.state.transition_sequence,
                                  .time_us = event.time_us,
                                  .scheduled_time_us = event.scheduled_time_us});

    return {};
  }

  return {};
}

StatusOr<FsmStep> FsmProgram::advance(const FsmState& state, std::int64_t time_us,
                                      const FsmReads& reads) const noexcept {
  EF_ASSIGN_OR_RETURN(auto encoded, encode_state(state));
  (void)encoded;
  EF_RETURN_IF_ERROR(validate_reads(reads));
  EF_RETURN_IF_ERROR(check(time_us >= state.time_us, "FSM time cannot move backwards"));
  FsmStep output{.state = state, .emissions = {}};
  std::size_t steps = 0;
  if (output.state.deadline_us && *output.state.deadline_us <= time_us) {
    const auto deadline = *output.state.deadline_us;

    // Timer identity includes the current generation and original scheduled time.
    const auto cause =
        Json::array({definition_.id, output.state.transition_sequence, deadline}).dump();

    // Entering another timed state starts its relative wait now. Missed historical deadline
    // slots are not processing times; replay can explicitly advance each earlier deadline.
    EF_RETURN_IF_ERROR(transition(output,
                                  {.kind = "$timeout",
                                   .occurrence_id = cause,
                                   .time_us = time_us,
                                   .values = {},
                                   .scheduled_time_us = deadline},
                                  reads, steps));
  }

  output.state.time_us = time_us;
  EF_ASSIGN_OR_RETURN(auto final_state, encode_state(output.state));
  (void)final_state;
  return output;
}

StatusOr<FsmStep> FsmProgram::step(const FsmState& state, const FsmEvent& event,
                                   const FsmReads& reads) const noexcept {
  EF_RETURN_IF_ERROR(check(valid_name(event.kind) && event.kind != "$timeout" &&
                               event.kind != "*" && valid_name(event.occurrence_id),
                           "invalid external FSM event"));
  EF_RETURN_IF_ERROR(check(!event.scheduled_time_us || (*event.scheduled_time_us >= 0 &&
                                                        *event.scheduled_time_us <= event.time_us),
                           "scheduled event time exceeds processing time"));
  EF_ASSIGN_OR_RETURN(auto values, row_json(event.values));
  if (values.dump().size() > definition_.max_state_bytes)
    return Status(ErrorCode::ResourceExhausted, "FSM event exceeds byte budget");

  EF_ASSIGN_OR_RETURN(auto output, advance(state, event.time_us, reads));
  std::size_t steps =
      static_cast<std::size_t>(output.state.transition_sequence - state.transition_sequence);
  if (!definition_.reset_event.empty() && event.kind == definition_.reset_event) {
    if (steps == definition_.max_steps || output.state.transition_sequence == UINT64_MAX)
      return Status(ErrorCode::ResourceExhausted, "FSM reset sequence exhausted");

    const auto next_sequence = output.state.transition_sequence + 1;
    EF_ASSIGN_OR_RETURN(output.state, initial_state(event.time_us));
    output.state.transition_sequence = next_sequence;
  } else
    EF_RETURN_IF_ERROR(transition(output, event, reads, steps));

  // Reset updates the generation after constructing initial state. Even ignored events can
  // grow the time encoding, so every successful path must remain persistable within budget.
  EF_ASSIGN_OR_RETURN(auto final_state, encode_state(output.state));
  (void)final_state;

  return output;
}

StatusOr<std::string> FsmProgram::encode_state(const FsmState& state) const noexcept {
  EF_RETURN_IF_ERROR(validate_state(state));
  EF_ASSIGN_OR_RETURN(auto registers, row_json(state.registers));
  Json deadline = nullptr;
  if (state.deadline_us)
    deadline = *state.deadline_us;

  Json encoded{{"format", 1},
               {"program_id", state.program_id},
               {"program_version", state.program_version},
               {"state", state.state},
               {"transition_sequence", state.transition_sequence},
               {"time_us", state.time_us},
               {"entered_at_us", state.entered_at_us},
               {"deadline_us", deadline},
               {"registers", std::move(registers)}};

  // Definition metadata is encoded once, outside mutable application registers.
  // Omitting the field preserves exact legacy snapshot bytes.
  if (!definition_.definition_identity.empty())
    encoded["definition_identity"] = definition_.definition_identity;

  auto bytes = encoded.dump();
  if (bytes.size() > definition_.max_state_bytes)
    return Status(ErrorCode::ResourceExhausted, "FSM state exceeds byte budget");

  return bytes;
}

StatusOr<FsmState> FsmProgram::decode_state(std::string_view bytes) const noexcept {
  EF_RETURN_IF_ERROR(
      check(bytes.size() <= definition_.max_state_bytes, "FSM snapshot exceeds budget"));
  EF_ASSIGN_OR_RETURN(auto json, parse_json_exact(bytes));
  if (!json.is_object() || !json.contains("format") || json["format"] != 1 ||
      !json.contains("program_id") || !json["program_id"].is_string() || !json.contains("state") ||
      !json["state"].is_string() || !json.contains("deadline_us") || !json.contains("registers") ||
      !json["registers"].is_array() ||
      json["registers"].size() != definition_.register_fields.size())
    return Status(ErrorCode::InvalidArgument, "invalid FSM snapshot layout");

  const auto identity = json.find("definition_identity");
  if (definition_.definition_identity.empty()) {
    EF_RETURN_IF_ERROR(check(identity == json.end(), "FSM snapshot definition identity mismatch"));
  } else {
    EF_RETURN_IF_ERROR(check(identity != json.end() && identity->is_string() &&
                                 *identity == definition_.definition_identity,
                             "FSM snapshot definition identity mismatch"));
  }

  FsmState state;
  state.program_id = json["program_id"].get<std::string>();
  state.state = json["state"].get<std::string>();
  EF_ASSIGN_OR_RETURN(state.program_version, unsigned_field(json, "program_version"));
  EF_ASSIGN_OR_RETURN(state.transition_sequence, unsigned_field(json, "transition_sequence"));
  EF_ASSIGN_OR_RETURN(state.time_us, time_field(json, "time_us"));
  EF_ASSIGN_OR_RETURN(state.entered_at_us, time_field(json, "entered_at_us"));
  if (!json["deadline_us"].is_null()) {
    EF_ASSIGN_OR_RETURN(auto deadline, time_field(json, "deadline_us"));
    state.deadline_us = deadline;
  }

  for (std::size_t i = 0; i < definition_.register_fields.size(); ++i) {
    const auto& field = definition_.register_fields[i];
    EF_ASSIGN_OR_RETURN(auto value,
                        value_from_json(json["registers"][i], field.type, field.nullable));
    state.registers.push_back(std::move(value));
  }

  EF_RETURN_IF_ERROR(validate_state(state));
  return state;
}

FsmGuard all_guards(std::vector<FsmGuard> guards) noexcept {
  return [guards = std::move(guards)](const FsmContext& context) noexcept -> StatusOr<bool> {
    for (const auto& guard : guards) {
      if (!guard)
        continue;

      EF_ASSIGN_OR_RETURN(auto value, guard(context));
      if (!value)
        return false;
    }

    return true;
  };
}

FsmGuard any_guards(std::vector<FsmGuard> guards) noexcept {
  return [guards = std::move(guards)](const FsmContext& context) noexcept -> StatusOr<bool> {
    for (const auto& guard : guards) {
      if (!guard)
        return true;

      EF_ASSIGN_OR_RETURN(auto value, guard(context));
      if (value)
        return true;
    }

    return false;
  };
}

FsmGuard not_guard(FsmGuard guard) noexcept {
  return [guard = std::move(guard)](const FsmContext& context) noexcept -> StatusOr<bool> {
    if (!guard)
      return false;

    EF_ASSIGN_OR_RETURN(auto value, guard(context));
    return !value;
  };
}

FsmDefinition on_event(std::string id, std::string event, FsmGuard guard, bool once) noexcept {
  FsmDefinition definition;
  definition.id = std::move(id);
  definition.states = {{.name = "waiting", .timeout_us = {}}, {.name = "done", .timeout_us = {}}};
  definition.initial = "waiting";
  definition.reset_event = "reset";
  definition.transitions.push_back({.from = "waiting",
                                    .event = std::move(event),
                                    .to = once ? "done" : "waiting",
                                    .guard = std::move(guard),
                                    .update = {},
                                    .emit = "trigger"});
  return definition;
}

FsmDefinition crossing(std::string id, FsmGuard predicate) noexcept {
  FsmDefinition definition;
  definition.id = std::move(id);
  definition.states = {{.name = "below", .timeout_us = {}}, {.name = "above", .timeout_us = {}}};
  definition.initial = "below";
  definition.reset_event = "reset";
  definition.transitions = {{.from = "below",
                             .event = "*",
                             .to = "above",
                             .guard = predicate,
                             .update = {},
                             .emit = "trigger"},
                            {.from = "above",
                             .event = "*",
                             .to = "below",
                             .guard = not_guard(std::move(predicate)),
                             .update = {},
                             .emit = {}}};
  return definition;
}

FsmDefinition followed_by(std::string id, std::string first, std::string second,
                          FsmGuard first_guard, FsmGuard second_guard,
                          std::optional<std::int64_t> within_us, bool strict_next) noexcept {
  FsmDefinition definition;
  definition.id = std::move(id);
  definition.states = {{.name = "waiting", .timeout_us = {}},
                       {.name = "armed", .timeout_us = within_us},
                       {.name = "accepted", .timeout_us = {}},
                       {.name = "rejected", .timeout_us = {}}};
  definition.initial = "waiting";
  definition.reset_event = "reset";

  definition.transitions = {{.from = "waiting",
                             .event = std::move(first),
                             .to = "armed",
                             .guard = std::move(first_guard),
                             .update = {},
                             .emit = {}},
                            {.from = "armed",
                             .event = second,
                             .to = "accepted",
                             .guard = std::move(second_guard),
                             .update = {},
                             .emit = "trigger"}};

  if (strict_next)
    definition.transitions.push_back({.from = "armed",
                                      .event = std::move(second),
                                      .to = "rejected",
                                      .guard = {},
                                      .update = {},
                                      .emit = {}});

  if (within_us)
    definition.transitions.push_back({.from = "armed",
                                      .event = "$timeout",
                                      .to = "waiting",
                                      .guard = {},
                                      .update = {},
                                      .emit = {}});

  return definition;
}
} // namespace eventflow
