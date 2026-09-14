#include "eventflow/observation_program.hpp"
#include "eventflow/catalog.hpp"
#include <algorithm>
#include <limits>
#include <set>

namespace eventflow {
namespace {
Status invalid(std::string_view message) noexcept {
  return {ErrorCode::InvalidArgument, message};
}

Status full(std::string_view message) noexcept {
  return {ErrorCode::ResourceExhausted, message};
}

Status add_bytes(std::size_t& total, std::size_t value) noexcept {
  if (value > std::numeric_limits<std::size_t>::max() - total)
    return full("Observation bound overflow");

  total += value;
  return {};
}

StatusOr<std::size_t> multiply(std::size_t value, std::size_t factor) noexcept {
  if (factor && value > std::numeric_limits<std::size_t>::max() / factor)
    return full("Observation bound overflow");

  return std::size_t(value * factor);
}

Status depth_ok(std::string_view bytes) noexcept {
  std::size_t depth = 0;
  bool quoted = false, escaped = false;
  for (char ch : bytes) {
    if (quoted) {
      if (escaped)
        escaped = false;
      else if (ch == '\\')
        escaped = true;
      else if (ch == '"')
        quoted = false;
    } else if (ch == '"')
      quoted = true;
    else if (ch == '{' || ch == '[') {
      if (++depth > 128)
        return full("Observation JSON nesting limit exceeded");
    } else if ((ch == '}' || ch == ']') && depth)
      --depth;
  }

  return {};
}

bool name_ok(std::string_view value) noexcept {
  return !value.empty() && value.size() <= 512 && valid_utf8(value) &&
         value.find('\0') == std::string_view::npos;
}

Status fields_ok(const std::vector<Field>& fields) noexcept {
  if (fields.size() > 8195)
    return full("Observation field signature is too large");

  std::set<std::string> names;
  for (std::size_t i = 0; i < fields.size(); ++i)
    EF_RETURN_IF_ERROR(check(fields[i].id == i + 1 && name_ok(fields[i].name) &&
                                 names.insert(fields[i].name).second &&
                                 !data_type_name(fields[i].type).empty(),
                             "Invalid observation field signature"));

  return {};
}

Json fields_json(const std::vector<Field>& fields) noexcept {
  Json result = Json::array();
  for (const auto& field : fields)
    result.push_back({{"id", field.id},
                      {"name", field.name},
                      {"type", std::string(data_type_name(field.type))},
                      {"nullable", field.nullable}});

  return result;
}

StatusOr<Json> typed_row(const Row& values, const std::vector<Field>* fields,
                         std::size_t limit) noexcept {
  if (limit < 2 || (fields && fields->size() != values.size()))
    return invalid("Observation row shape mismatch");

  Json result = Json::array();
  std::size_t used = 2;
  for (std::size_t i = 0; i < values.size(); ++i) {
    const auto& value = values[i];
    if (const auto* text = std::get_if<std::string>(&value); text && text->size() > limit)
      return full("Observation String exceeds row budget");
    if (const auto* vector = std::get_if<std::vector<double>>(&value);
        vector && vector->size() > limit / sizeof(double))
      return full("Observation vector exceeds row budget");

    EF_ASSIGN_OR_RETURN(auto encoded, value_to_json(value));
    if (fields) {
      const auto& field = (*fields)[i];
      EF_ASSIGN_OR_RETURN(auto checked, value_from_json(encoded, field.type, field.nullable));
      if (checked.index() != value.index())
        return invalid("Observation value differs from declared type");
    }

    Json entry = Json::array({value.index(), std::move(encoded)});
    const auto amount = entry.dump().size() + (result.empty() ? 0 : 1);
    if (amount > limit - used)
      return full("Observation typed row exceeds byte budget");

    used += amount;
    result.push_back(std::move(entry));
  }

  return result;
}

StatusOr<Json> event_json(const FsmEvent& event, const std::vector<Field>* fields,
                          std::size_t limit) noexcept {
  if (!name_ok(event.kind) || !name_ok(event.occurrence_id) || event.kind == "*" ||
      event.kind == "$timeout" || event.time_us < 0 ||
      (event.scheduled_time_us &&
       (*event.scheduled_time_us < 0 || *event.scheduled_time_us > event.time_us)))
    return invalid("Invalid observation event identity/kind/time");

  EF_ASSIGN_OR_RETURN(auto values, typed_row(event.values, fields, limit));
  Json result{{"id", event.occurrence_id},
              {"kind", event.kind},
              {"time", event.time_us},
              {"values", std::move(values)}};
  if (event.scheduled_time_us)
    result["scheduled_time_us"] = *event.scheduled_time_us;
  if (result.dump().size() > limit)
    return full("Observation event exceeds byte budget");

  return result;
}

StatusOr<Json> reads_json(const FsmReads& reads, std::size_t limit) noexcept {
  Json result = Json::object();
  for (const auto& [alias, row] : reads) {
    if (row) {
      EF_ASSIGN_OR_RETURN(auto values, typed_row(*row, nullptr, limit));
      result[alias] = std::move(values);
    } else
      result[alias] = nullptr;
  }

  if (result.dump().size() > limit)
    return full("Observation read map exceeds byte budget");

  return result;
}

StatusOr<Json> outputs_json(const ObservationProgramStep& step, std::size_t limit) noexcept {
  Json result{
      {"emissions", Json::array()}, {"matches", Json::array()}, {"timeouts", Json::array()}};
  for (const auto& output : step.emissions)
    result["emissions"].push_back(
        {{"name", output.name},
         {"cause", output.cause},
         {"sequence", output.transition_sequence},
         {"time", output.time_us},
         {"scheduled", output.scheduled_time_us ? Json(*output.scheduled_time_us) : Json()}});

  const auto captures = [&](const std::vector<CepCapture>& values) noexcept -> StatusOr<Json> {
    Json encoded = Json::array();
    for (const auto& capture : values) {
      const auto& event = capture.event;
      EF_ASSIGN_OR_RETURN(auto row, typed_row(event.values, nullptr, limit));
      encoded.push_back({{"label", capture.label},
                         {"event",
                          {{"id", event.id},
                           {"kind", event.kind},
                           {"time", event.time_us},
                           {"values", std::move(row)}}}});
    }

    return encoded;
  };
  for (const auto& match : step.matches) {
    EF_ASSIGN_OR_RETURN(auto events, captures(match.events));
    EF_ASSIGN_OR_RETURN(auto registers, typed_row(match.registers, nullptr, limit));
    EF_ASSIGN_OR_RETURN(auto reads, reads_json(match.reads, limit));
    result["matches"].push_back({{"id", match.id},
                                 {"events", std::move(events)},
                                 {"registers", std::move(registers)},
                                 {"reads", std::move(reads)},
                                 {"matched_at", match.matched_at_us},
                                 {"emitted_at", match.emitted_at_us}});
  }

  for (const auto& timeout : step.timeouts) {
    EF_ASSIGN_OR_RETURN(auto events, captures(timeout.events));
    result["timeouts"].push_back({{"path_id", timeout.path_id},
                                  {"events", std::move(events)},
                                  {"scheduled", timeout.scheduled_time_us},
                                  {"time", timeout.time_us}});
  }

  if (result.dump().size() > limit)
    return full("Observation output envelope exceeds prepaid bound");

  return result;
}

StatusOr<Row> decode_typed_row(const Json& values, const std::vector<Field>& fields,
                               std::size_t limit) noexcept {
  if (!values.is_array() || values.size() != fields.size())
    return invalid("Restored observation row has wrong field count");

  Row row;
  for (std::size_t i = 0; i < fields.size(); ++i) {
    const auto& entry = values[i];
    if (!entry.is_array() || entry.size() != 2 || !entry[0].is_number_integer() ||
        (!entry[0].is_number_unsigned() && entry[0].get<std::int64_t>() < 0))
      return invalid("Invalid restored observation value tag");

    const auto tag = entry[0].get<std::uint64_t>();
    EF_ASSIGN_OR_RETURN(auto value, value_from_json(entry[1], fields[i].type, fields[i].nullable));
    if (tag != value.index())
      return invalid("Restored observation value violates Catalog type");

    row.push_back(std::move(value));
  }

  EF_ASSIGN_OR_RETURN(auto checked, typed_row(row, &fields, limit));
  (void)checked;
  return row;
}

Status descriptor_ok(const ObservationRuleDescriptor& descriptor) noexcept {
  const auto& d = descriptor.definition;
  if (!descriptor.id || !name_ok(d.name) || !d.version || !name_ok(d.recipe_id) ||
      !d.recipe_version || !name_ok(d.implementation_identity) || d.inputs.empty() ||
      d.inputs.size() > 64 || d.reads.size() > 64 ||
      d.inputs.size() != descriptor.event_fields.size() || d.program_fingerprint.empty() ||
      d.program_fingerprint.size() > 64 * 1024)
    return invalid("Incomplete Catalog observation descriptor");

  EF_RETURN_IF_ERROR(fields_ok(d.scope_fields));
  if (d.scope_fields.empty())
    return invalid("Observation scope is empty");

  std::set<std::string> bindings, aliases;
  std::map<std::string, std::string> kind_signatures;
  for (std::size_t i = 0; i < d.inputs.size(); ++i) {
    const auto& input = d.inputs[i];
    if (!name_ok(input.id) || !name_ok(input.event_kind) || !bindings.insert(input.id).second ||
        !input.max_event_bytes || input.max_event_bytes > 16 * 1024 * 1024 ||
        static_cast<unsigned>(input.kind) >
            static_cast<unsigned>(ObservationInputKind::ExpiryNotice))
      return invalid("Invalid observation input binding");

    EF_RETURN_IF_ERROR(fields_ok(descriptor.event_fields[i]));
    const auto signature = fields_json(descriptor.event_fields[i]).dump();
    auto [found, inserted] = kind_signatures.emplace(input.event_kind, signature);
    if (!inserted && found->second != signature)
      return invalid("One event kind has conflicting typed signatures");
    if (input.kind == ObservationInputKind::Source &&
        fields_json(input.source_fields) != fields_json(descriptor.event_fields[i]))
      return invalid("Source event fields differ from Catalog signature");
  }

  for (const auto& read : d.reads) {
    if (!name_ok(read.alias) || !aliases.insert(read.alias).second || !read.relation ||
        !read.max_row_bytes || read.max_row_bytes > 16 * 1024 * 1024)
      return invalid("Invalid observation read binding");

    EF_RETURN_IF_ERROR(fields_ok(read.fields));
    if (read.fields.empty())
      return invalid("Observation read signature is empty");
  }

  return {};
}
} // namespace

namespace {
StatusOr<std::string> fsm_fingerprint(const FsmDefinition& definition) noexcept {
  Json states = Json::array(), transitions = Json::array();
  for (const auto& state : definition.states)
    states.push_back({{"name", state.name},
                      {"timeout_us", state.timeout_us ? Json(*state.timeout_us) : Json()}});

  for (const auto& transition : definition.transitions)
    transitions.push_back({{"from", transition.from},
                           {"event", transition.event},
                           {"to", transition.to},
                           {"guard", static_cast<bool>(transition.guard)},
                           {"update", static_cast<bool>(transition.update)},
                           {"emit", transition.emit}});

  EF_ASSIGN_OR_RETURN(auto registers,
                      typed_row(definition.initial_registers, &definition.register_fields,
                                definition.max_state_bytes));
  auto result = Json{{"format", 1},
                     {"kind", "fsm"},
                     {"id", definition.id},
                     {"version", definition.version},
                     {"states", std::move(states)},
                     {"initial", definition.initial},
                     {"fields", fields_json(definition.register_fields)},
                     {"registers", std::move(registers)},
                     {"reads", definition.reads},
                     {"transitions", std::move(transitions)},
                     {"reset_event", definition.reset_event},
                     {"max_steps", definition.max_steps},
                     {"max_state_bytes", definition.max_state_bytes}}
                    .dump();
  if (result.size() > 64 * 1024)
    return full("Observation program declaration exceeds Catalog budget");

  return result;
}
} // namespace

StatusOr<std::string> observation_program_fingerprint(const FsmDefinition& definition) noexcept {
  EF_ASSIGN_OR_RETURN(auto program, FsmProgram::create(definition));
  (void)program;
  return fsm_fingerprint(definition);
}

StatusOr<std::string> observation_program_fingerprint(const CepDefinition& definition) noexcept {
  EF_ASSIGN_OR_RETURN(auto program, CepProgram::create(definition));
  if (program->fingerprint().size() > 64 * 1024)
    return full("Observation program declaration exceeds Catalog budget");

  return std::string(program->fingerprint());
}

struct ObservationProgram::Impl {
  using State = std::variant<FsmState, CepState>;
  ObservationRuleDescriptor descriptor;
  std::string binding_fingerprint;
  ObservationProgramBounds limits;
  std::size_t raw_state_limit = 0;
  std::unique_ptr<FsmProgram> fsm;
  std::unique_ptr<CepProgram> cep;

  Status validate_reads(const FsmReads& reads) const noexcept {
    if (reads.size() != descriptor.definition.reads.size())
      return invalid("Observation read alias set mismatch");

    for (const auto& read : descriptor.definition.reads) {
      auto found = reads.find(read.alias);
      if (found == reads.end())
        return invalid("Missing declared observation read");
      if (found->second) {
        EF_ASSIGN_OR_RETURN(auto row, typed_row(*found->second, &read.fields, read.max_row_bytes));
        (void)row;
      }
    }

    EF_ASSIGN_OR_RETURN(auto encoded, reads_json(reads, limits.max_read_bytes));
    (void)encoded;
    return {};
  }

  Json envelope(std::string raw) const noexcept {
    return Json{{"format", 1},
                {"binding_fingerprint", binding_fingerprint},
                {"program_fingerprint", descriptor.definition.program_fingerprint},
                {"state", std::move(raw)}};
  }

  Status validate_cep_image(std::string_view bytes) const noexcept {
    EF_ASSIGN_OR_RETURN(auto image, parse_json_exact(bytes));

    // CepProgram has already checked its versioned wire structure and graph identities.
    for (const auto& event : image["events"]) {
      const auto kind = event["kind"].get<std::string>();
      std::optional<std::size_t> binding;
      std::size_t event_limit = 0;
      for (std::size_t i = 0; i < descriptor.definition.inputs.size(); ++i)
        if (descriptor.definition.inputs[i].event_kind == kind) {
          binding = i;
          event_limit = std::max(event_limit, descriptor.definition.inputs[i].max_event_bytes);
        }
      if (!binding)
        return invalid("Restored CEP event has no Catalog binding");

      EF_ASSIGN_OR_RETURN(
          auto values,
          decode_typed_row(event["values"], descriptor.event_fields[*binding], event_limit));
      FsmEvent restored{.kind = kind,
                        .occurrence_id = event["id"].get<std::string>(),
                        .time_us = event["time"].get<std::int64_t>(),
                        .values = std::move(values),
                        .scheduled_time_us = {}};
      EF_ASSIGN_OR_RETURN(auto checked,
                          event_json(restored, &descriptor.event_fields[*binding], event_limit));
      (void)checked;
    }

    for (const auto& pending : image["pending"]) {
      const auto& stored = pending[3];
      FsmReads reads;
      for (const auto& read : descriptor.definition.reads) {
        auto found = stored.find(read.alias);
        if (found == stored.end())
          return invalid("Restored CEP match is missing a read alias");
        if (found->is_null())
          reads.emplace(read.alias, std::nullopt);
        else {
          EF_ASSIGN_OR_RETURN(auto values,
                              decode_typed_row(*found, read.fields, read.max_row_bytes));
          reads.emplace(read.alias, std::move(values));
        }
      }

      EF_RETURN_IF_ERROR(validate_reads(reads));
    }

    return {};
  }

  StatusOr<State> decode(std::string_view bytes) const noexcept {
    if (bytes.size() > limits.max_state_bytes)
      return full("Observation state exceeds prepaid bound");

    EF_RETURN_IF_ERROR(depth_ok(bytes));
    EF_ASSIGN_OR_RETURN(auto json, parse_json_exact(bytes));
    if (!json.is_object() || json.size() != 4 || !json.contains("format") ||
        !json["format"].is_number_integer() || json["format"] != 1 ||
        !json.contains("binding_fingerprint") ||
        json["binding_fingerprint"] != binding_fingerprint ||
        !json.contains("program_fingerprint") ||
        json["program_fingerprint"] != descriptor.definition.program_fingerprint ||
        !json.contains("state") || !json["state"].is_string() || json.dump() != bytes)
      return invalid("Observation state identity or canonical envelope mismatch");

    const auto& raw = json["state"].get_ref<const std::string&>();
    if (raw.size() > raw_state_limit)
      return full("Observation inner state exceeds program limit");

    EF_RETURN_IF_ERROR(depth_ok(raw));
    if (fsm) {
      EF_ASSIGN_OR_RETURN(auto state, fsm->decode_state(raw));
      return State{std::move(state)};
    }

    EF_ASSIGN_OR_RETURN(auto state, cep->decode_state(raw));
    EF_RETURN_IF_ERROR(validate_cep_image(raw));
    return State{std::move(state)};
  }

  StatusOr<std::string> encode(const State& state) const noexcept {
    std::string raw;
    if (fsm) {
      EF_ASSIGN_OR_RETURN(raw, fsm->encode_state(std::get<FsmState>(state)));
    } else {
      EF_ASSIGN_OR_RETURN(raw, cep->encode_state(std::get<CepState>(state)));
    }

    auto encoded = envelope(std::move(raw)).dump();
    if (encoded.size() > limits.max_state_bytes)
      return full("Observation encoded state exceeds prepaid bound");

    return encoded;
  }

  StatusOr<ObservationProgramStateInfo> info(const State& state) const noexcept {
    if (fsm) {
      const auto& value = std::get<FsmState>(state);
      return ObservationProgramStateInfo{.time_us = value.time_us,
                                         .next_deadline_us = value.deadline_us};
    }

    EF_ASSIGN_OR_RETURN(auto summary, cep->inspect(std::get<CepState>(state)));
    return ObservationProgramStateInfo{.time_us = summary.time_us,
                                       .next_deadline_us = summary.next_deadline_us};
  }

  StatusOr<ObservationProgramStep> finish(State state,
                                          ObservationProgramStep result) const noexcept {
    EF_ASSIGN_OR_RETURN(result.next_state, encode(state));
    EF_ASSIGN_OR_RETURN(auto summary, info(state));
    result.time_us = summary.time_us;
    result.next_deadline_us = summary.next_deadline_us;
    if (result.emissions.size() + result.matches.size() + result.timeouts.size() >
        limits.max_output_count)
      return full("Observation output count exceeds prepaid bound");

    EF_ASSIGN_OR_RETURN(auto encoded, outputs_json(result, limits.max_output_bytes));
    (void)encoded;
    return result;
  }
};

ObservationProgram::ObservationProgram(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

ObservationProgram::~ObservationProgram() noexcept = default;

StatusOr<std::unique_ptr<ObservationProgram>>
ObservationProgram::bind(const ObservationRuleDescriptor& descriptor, std::string_view identity,
                         ObservationProgramDefinition definition) noexcept {
  EF_RETURN_IF_ERROR(descriptor_ok(descriptor));
  const auto& expected = descriptor.definition;
  if (identity != expected.implementation_identity)
    return invalid("Observation implementation identity mismatch");

  auto impl = std::make_unique<Impl>();
  impl->descriptor = descriptor;
  EF_ASSIGN_OR_RETURN(auto binding, observation_json(descriptor));
  impl->binding_fingerprint = binding.dump();
  impl->limits.max_read_bytes = 2;
  std::size_t row_budget = 0;
  std::set<std::string> aliases;

  for (const auto& read : expected.reads) {
    aliases.insert(read.alias);
    EF_RETURN_IF_ERROR(add_bytes(row_budget, read.max_row_bytes));
    EF_RETURN_IF_ERROR(
        add_bytes(impl->limits.max_read_bytes, std::max<std::size_t>(read.max_row_bytes, 4)));
    EF_RETURN_IF_ERROR(add_bytes(impl->limits.max_read_bytes, Json(read.alias).dump().size() + 2));
  }

  for (const auto& input : expected.inputs)
    impl->limits.max_event_bytes = std::max(impl->limits.max_event_bytes, input.max_event_bytes);

  std::vector<std::string> actual_reads;
  std::string program_fingerprint;
  if (auto* fsm = std::get_if<FsmDefinition>(&definition)) {
    if (expected.program_kind != ObservationProgramKind::Fsm || fsm->id != expected.recipe_id ||
        fsm->version != expected.recipe_version)
      return invalid("Observation FSM recipe identity/version mismatch");

    actual_reads = fsm->reads;
    EF_ASSIGN_OR_RETURN(impl->fsm, FsmProgram::create(*fsm));
    EF_ASSIGN_OR_RETURN(program_fingerprint, fsm_fingerprint(*fsm));
    impl->raw_state_limit = fsm->max_state_bytes;
    if (row_budget > fsm->max_state_bytes || impl->limits.max_event_bytes > fsm->max_state_bytes)
      return invalid("Observation event/read envelope exceeds FSM capacity");

    // One step can execute one overdue timeout transition and one data/reset transition.
    impl->limits.max_output_count = std::min<std::size_t>(fsm->max_steps, 2);
    std::size_t name_bytes = 2;
    for (const auto& transition : fsm->transitions)
      name_bytes = std::max(name_bytes, Json(transition.emit).dump().size());

    const auto timeout_cause = Json::array({fsm->id, UINT64_MAX, INT64_MAX}).dump();
    const auto cause_bytes = std::max<std::size_t>(6 * 512 + 2, Json(timeout_cause).dump().size());
    EF_ASSIGN_OR_RETURN(impl->limits.max_output_bytes,
                        multiply(name_bytes + cause_bytes + 192, impl->limits.max_output_count));
    EF_RETURN_IF_ERROR(add_bytes(impl->limits.max_output_bytes, 64));
  } else {
    auto& cep = std::get<CepDefinition>(definition);
    if (expected.program_kind != ObservationProgramKind::Cep || cep.id != expected.recipe_id ||
        cep.version != expected.recipe_version)
      return invalid("Observation CEP recipe identity/version mismatch");

    actual_reads = cep.reads;
    EF_ASSIGN_OR_RETURN(impl->cep, CepProgram::create(cep));
    program_fingerprint = std::string(impl->cep->fingerprint());
    impl->raw_state_limit = cep.limits.max_state_bytes;
    if (impl->limits.max_read_bytes > cep.limits.max_read_bytes ||
        impl->limits.max_event_bytes > cep.limits.max_event_bytes)
      return invalid("Observation event/read envelope exceeds CEP capacity");

    impl->limits.max_output_count = cep.limits.max_matches_per_step;
    EF_RETURN_IF_ERROR(add_bytes(impl->limits.max_output_count, cep.limits.max_timeouts_per_step));
    EF_ASSIGN_OR_RETURN(impl->limits.max_output_bytes, multiply(cep.limits.max_output_bytes, 6));
    EF_ASSIGN_OR_RETURN(auto overhead, multiply(impl->limits.max_output_count, 256));
    EF_RETURN_IF_ERROR(add_bytes(impl->limits.max_output_bytes, overhead));
    EF_RETURN_IF_ERROR(add_bytes(impl->limits.max_output_bytes, 64));
  }

  if (actual_reads.size() != aliases.size() ||
      std::set<std::string>(actual_reads.begin(), actual_reads.end()) != aliases)
    return invalid("Compiled program read aliases differ from Catalog binding");
  if (program_fingerprint != expected.program_fingerprint)
    return invalid("Compiled observation program differs from canonical Catalog definition");

  // Canonical inner JSON contains no raw control characters; quoting expands it at most 2x.
  EF_ASSIGN_OR_RETURN(impl->limits.max_state_bytes, multiply(impl->raw_state_limit, 2));
  EF_RETURN_IF_ERROR(add_bytes(impl->limits.max_state_bytes, impl->envelope({}).dump().size()));
  return std::unique_ptr<ObservationProgram>(new ObservationProgram(std::move(impl)));
}

StatusOr<std::string> ObservationProgram::initial_state(std::int64_t at_us) const noexcept {
  if (at_us < 0)
    return invalid("Negative observation program time");
  if (impl_->fsm) {
    EF_ASSIGN_OR_RETURN(auto state, impl_->fsm->initial_state(at_us));
    return impl_->encode(Impl::State{std::move(state)});
  }

  EF_ASSIGN_OR_RETURN(auto state, impl_->cep->initial_state(at_us));
  return impl_->encode(Impl::State{std::move(state)});
}

StatusOr<ObservationProgramStep> ObservationProgram::step(std::string_view encoded_state,
                                                          std::string_view binding_id,
                                                          const FsmEvent& event,
                                                          const FsmReads& reads) const noexcept {
  const auto& bindings = impl_->descriptor.definition.inputs;
  auto input = std::find_if(bindings.begin(), bindings.end(),
                            [&](const auto& item) noexcept { return item.id == binding_id; });
  if (input == bindings.end() || input->event_kind != event.kind)
    return invalid("Observation event does not match its input binding");

  const auto index = static_cast<std::size_t>(input - bindings.begin());
  EF_ASSIGN_OR_RETURN(auto event_bytes, event_json(event, &impl_->descriptor.event_fields[index],
                                                   input->max_event_bytes));
  (void)event_bytes;
  EF_RETURN_IF_ERROR(impl_->validate_reads(reads));
  EF_ASSIGN_OR_RETURN(auto previous, impl_->decode(encoded_state));
  EF_ASSIGN_OR_RETURN(auto previous_info, impl_->info(previous));
  if (event.time_us < previous_info.time_us)
    return invalid("Observation program time cannot move backwards");

  const FsmReads frozen_reads = reads;
  const FsmEvent frozen_event = event;
  ObservationProgramStep result;
  if (impl_->fsm) {
    EF_ASSIGN_OR_RETURN(auto next,
                        impl_->fsm->step(std::get<FsmState>(previous), frozen_event, frozen_reads));
    result.emissions = std::move(next.emissions);
    return impl_->finish(Impl::State{std::move(next.state)}, std::move(result));
  }

  CepEvent cep_event{.id = frozen_event.occurrence_id,
                     .kind = frozen_event.kind,
                     .time_us = frozen_event.time_us,
                     .values = frozen_event.values};
  EF_ASSIGN_OR_RETURN(auto next,
                      impl_->cep->step(std::get<CepState>(previous), cep_event, frozen_reads));
  result.matches = std::move(next.matches);
  result.timeouts = std::move(next.timeouts);

  return impl_->finish(Impl::State{std::move(next.state)}, std::move(result));
}

StatusOr<ObservationProgramStep> ObservationProgram::advance(std::string_view encoded_state,
                                                             std::int64_t now_us,
                                                             const FsmReads& reads) const noexcept {
  EF_RETURN_IF_ERROR(impl_->validate_reads(reads));
  EF_ASSIGN_OR_RETURN(auto previous, impl_->decode(encoded_state));
  EF_ASSIGN_OR_RETURN(auto previous_info, impl_->info(previous));
  if (now_us < 0 || now_us < previous_info.time_us)
    return invalid("Observation program time cannot move backwards");

  const FsmReads frozen_reads = reads;
  ObservationProgramStep result;
  if (impl_->fsm) {
    EF_ASSIGN_OR_RETURN(auto next,
                        impl_->fsm->advance(std::get<FsmState>(previous), now_us, frozen_reads));
    result.emissions = std::move(next.emissions);
    return impl_->finish(Impl::State{std::move(next.state)}, std::move(result));
  }

  EF_ASSIGN_OR_RETURN(auto next, impl_->cep->advance(std::get<CepState>(previous), now_us));
  result.matches = std::move(next.matches);
  result.timeouts = std::move(next.timeouts);
  return impl_->finish(Impl::State{std::move(next.state)}, std::move(result));
}

StatusOr<ObservationProgramStateInfo>
ObservationProgram::inspect_state(std::string_view encoded_state) const noexcept {
  EF_ASSIGN_OR_RETURN(auto state, impl_->decode(encoded_state));
  return impl_->info(state);
}

const ObservationProgramBounds& ObservationProgram::bounds() const noexcept {
  return impl_->limits;
}
} // namespace eventflow
