#include "eventflow/event_processor.hpp"
#include "engine_internal.hpp"
#include "eventflow/exact_sum.hpp"
#include "eventflow/key_codec.hpp"
#include "trigger_audit_internal.hpp"

#include <algorithm>
#include <limits>
#include <set>

namespace eventflow {
namespace {
Status bad(std::string_view message) noexcept {
  return {ErrorCode::InvalidArgument, message};
}

Status full(std::string_view message) noexcept {
  return {ErrorCode::ResourceExhausted, message};
}

bool text_ok(std::string_view text, std::size_t maximum = 256) noexcept {
  return !text.empty() && text.size() <= maximum && valid_utf8(text) &&
         text.find('\0') == std::string_view::npos;
}

bool accepts_input(const ProcessorCepRule& rule, std::string_view kind) noexcept {
  return rule.input_kinds.empty() || std::find(rule.input_kinds.begin(), rule.input_kinds.end(),
                                               kind) != rule.input_kinds.end();
}

bool fields(const Json& json, std::initializer_list<const char*> names) noexcept {
  if (!json.is_object())
    return false;

  for (const auto* name : names)
    if (!json.contains(name))
      return false;

  return true;
}

StatusOr<std::uint64_t> number(const Json& value) noexcept {
  if (!value.is_number_integer() || (!value.is_number_unsigned() && value.get<std::int64_t>() < 0))
    return bad("invalid lifecycle unsigned integer");

  return value.get<std::uint64_t>();
}

StatusOr<std::int64_t> time_number(const Json& value) noexcept {
  EF_ASSIGN_OR_RETURN(const auto result, number(value));
  if (result > INT64_MAX)
    return bad("lifecycle time or offset overflow");

  return static_cast<std::int64_t>(result);
}

StatusOr<Json> row_json(const Row& row) noexcept {
  Json result = Json::array();
  for (const auto& value : row) {
    EF_ASSIGN_OR_RETURN(auto encoded, value_to_json(value));
    result.push_back(Json::array({value.index(), std::move(encoded)}));
  }

  return result;
}

StatusOr<Row> json_row(const Json& json) noexcept {
  if (!json.is_array())
    return bad("invalid lifecycle typed row");

  Row row;
  constexpr DataType types[] = {DataType::Int64,   DataType::Bool,   DataType::Int64,
                                DataType::Float64, DataType::String, DataType::FloatVector};
  for (const auto& entry : json) {
    if (!entry.is_array() || entry.size() != 2)
      return bad("invalid lifecycle value");

    EF_ASSIGN_OR_RETURN(const auto tag, number(entry[0]));
    if (tag > 5 || (tag == 0 && !entry[1].is_null()))
      return bad("invalid lifecycle value tag");

    EF_ASSIGN_OR_RETURN(auto value, value_from_json(entry[1], types[tag], tag == 0));
    if (value.index() != tag)
      return bad("lifecycle value type mismatch");

    row.push_back(std::move(value));
  }

  return row;
}

StatusOr<std::string> scope_key(const Row& row) noexcept {
  EF_ASSIGN_OR_RETURN(auto bytes, encode_key_parts(row));
  return hex_encode(bytes);
}

Json source_json(const SourceIdentity& source) noexcept {
  return Json::array({source.source_id, source.topic, source.partition, source.offset});
}

Status validate_source(const SourceIdentity& source) noexcept {
  return check(text_ok(source.source_id) && text_ok(source.topic, 249) && source.partition >= 0 &&
                   source.offset >= 0 && source.offset < INT64_MAX,
               "invalid durable source identity");
}

StatusOr<SourceIdentity> json_source(const Json& json, bool position = false) noexcept {
  if (!json.is_array() || json.size() != 4 || !json[0].is_string() || !json[1].is_string())
    return bad("invalid source identity encoding");

  EF_ASSIGN_OR_RETURN(const auto partition, number(json[2]));
  EF_ASSIGN_OR_RETURN(const auto offset, time_number(json[3]));
  if (partition > INT32_MAX)
    return bad("source partition overflow");

  SourceIdentity result{.source_id = json[0].get<std::string>(),
                        .topic = json[1].get<std::string>(),
                        .partition = static_cast<std::int32_t>(partition),
                        .offset = offset};
  if (position) {
    EF_RETURN_IF_ERROR(
        check(text_ok(result.source_id) && text_ok(result.topic, 249) && result.offset > 0,
              "invalid processed source position"));
  } else {
    EF_RETURN_IF_ERROR(validate_source(result));
  }

  return result;
}

std::string source_partition_key(const SourceIdentity& source) noexcept {
  return Json::array({source.source_id, source.topic, source.partition}).dump();
}

StatusOr<Json> reads_json(const FsmReads& reads) noexcept {
  Json result = Json::object();
  for (const auto& [name, row] : reads) {
    if (!row)
      result[name] = nullptr;
    else {
      EF_ASSIGN_OR_RETURN(auto encoded, row_json(*row));
      result[name] = std::move(encoded);
    }
  }

  return result;
}

StatusOr<FsmReads> json_reads(const Json& json) noexcept {
  if (!json.is_object())
    return bad("invalid occurrence read witness");

  FsmReads result;
  for (auto it = json.begin(); it != json.end(); ++it) {
    if (it.value().is_null())
      result.emplace(it.key(), std::nullopt);
    else {
      EF_ASSIGN_OR_RETURN(auto row, json_row(it.value()));
      result.emplace(it.key(), std::move(row));
    }
  }

  return result;
}

StatusOr<Json> match_json(const CepMatch& match) noexcept {
  EF_ASSIGN_OR_RETURN(auto registers, row_json(match.registers));
  EF_ASSIGN_OR_RETURN(auto reads, reads_json(match.reads));
  Json result{{"id", match.id},
              {"registers", std::move(registers)},
              {"reads", std::move(reads)},
              {"matched_at", match.matched_at_us},
              {"emitted_at", match.emitted_at_us},
              {"events", Json::array()}};
  for (const auto& capture : match.events) {
    EF_ASSIGN_OR_RETURN(auto values, row_json(capture.event.values));
    result["events"].push_back(Json::array({capture.label, capture.event.id, capture.event.kind,
                                            capture.event.time_us, std::move(values)}));
  }

  return result;
}

StatusOr<CepMatch> json_match(const Json& json) noexcept {
  if (!fields(json, {"id", "registers", "reads", "matched_at", "emitted_at", "events"}) ||
      !json["id"].is_string() || !json["events"].is_array())
    return bad("invalid persisted CEP match");

  CepMatch match;
  match.id = json["id"].get<std::string>();
  EF_ASSIGN_OR_RETURN(match.registers, json_row(json["registers"]));
  EF_ASSIGN_OR_RETURN(match.reads, json_reads(json["reads"]));
  EF_ASSIGN_OR_RETURN(match.matched_at_us, time_number(json["matched_at"]));
  EF_ASSIGN_OR_RETURN(match.emitted_at_us, time_number(json["emitted_at"]));
  std::set<std::string> ids;
  std::int64_t previous_time = 0;

  for (const auto& entry : json["events"]) {
    if (!entry.is_array() || entry.size() != 5 || !entry[0].is_string() || !entry[1].is_string() ||
        !entry[2].is_string())
      return bad("invalid CEP captured event");

    CepCapture capture{.label = entry[0].get<std::string>(),
                       .event = {.id = entry[1].get<std::string>(),
                                 .kind = entry[2].get<std::string>(),
                                 .time_us = 0,
                                 .values = {}}};
    EF_ASSIGN_OR_RETURN(capture.event.time_us, time_number(entry[3]));
    EF_ASSIGN_OR_RETURN(capture.event.values, json_row(entry[4]));
    if (!text_ok(capture.label) || !text_ok(capture.event.id, 4096) ||
        !text_ok(capture.event.kind) || !ids.insert(capture.event.id).second ||
        capture.event.time_us < previous_time || capture.event.time_us > match.matched_at_us)
      return bad("invalid CEP capture identity or time");

    previous_time = capture.event.time_us;
    match.events.push_back(std::move(capture));
  }

  if (match.events.empty() || match.matched_at_us > match.emitted_at_us)
    return bad("invalid CEP match progress");

  return match;
}

StatusOr<Json> occurrence_json(const TriggerOccurrence& occurrence) noexcept {
  EF_ASSIGN_OR_RETURN(auto scope, row_json(occurrence.scope));
  EF_ASSIGN_OR_RETURN(auto reads, reads_json(occurrence.reads));
  EF_ASSIGN_OR_RETURN(auto values, row_json(occurrence.event_values));
  Json result{{"id", occurrence.id},
              {"scope", std::move(scope)},
              {"rule", occurrence.rule_id},
              {"version", occurrence.rule_version},
              {"name", occurrence.emission.name},
              {"cause", occurrence.emission.cause},
              {"sequence", occurrence.emission.transition_sequence},
              {"time", occurrence.emission.time_us},
              {"scheduled_time", occurrence.emission.scheduled_time_us
                                     ? Json(*occurrence.emission.scheduled_time_us)
                                     : Json(nullptr)},
              {"reads", std::move(reads)},
              {"source", occurrence.source ? source_json(*occurrence.source) : Json(nullptr)},
              {"event_kind", occurrence.event_kind},
              {"event_values", std::move(values)},
              {"skipped_slots", occurrence.skipped_slots},
              {"evaluated_at", occurrence.evaluated_at_us}};
  if (occurrence.cep_match) {
    EF_ASSIGN_OR_RETURN(result["cep_match"], match_json(*occurrence.cep_match));
  }

  return result;
}

StatusOr<TriggerOccurrence> json_occurrence(const Json& json) noexcept {
  if (!fields(json, {"id", "scope", "rule", "version", "name", "cause", "sequence", "time",
                     "scheduled_time", "reads", "source", "event_kind", "event_values",
                     "skipped_slots", "evaluated_at"}) ||
      !json["id"].is_string() || !json["rule"].is_string() || !json["name"].is_string() ||
      !json["cause"].is_string() || !json["event_kind"].is_string())
    return bad("invalid durable occurrence");

  TriggerOccurrence result;
  result.id = json["id"].get<std::string>();
  result.rule_id = json["rule"].get<std::string>();
  EF_ASSIGN_OR_RETURN(result.scope, json_row(json["scope"]));
  EF_ASSIGN_OR_RETURN(result.rule_version, number(json["version"]));
  result.emission.name = json["name"].get<std::string>();
  result.emission.cause = json["cause"].get<std::string>();
  EF_ASSIGN_OR_RETURN(result.emission.transition_sequence, number(json["sequence"]));
  EF_ASSIGN_OR_RETURN(result.emission.time_us, time_number(json["time"]));
  if (!json["scheduled_time"].is_null()) {
    EF_ASSIGN_OR_RETURN(auto scheduled, time_number(json["scheduled_time"]));
    if (scheduled > result.emission.time_us)
      return bad("scheduled occurrence time exceeds processing time");

    result.emission.scheduled_time_us = scheduled;
  }

  EF_ASSIGN_OR_RETURN(result.reads, json_reads(json["reads"]));
  result.event_kind = json["event_kind"].get<std::string>();
  EF_ASSIGN_OR_RETURN(result.event_values, json_row(json["event_values"]));
  EF_ASSIGN_OR_RETURN(result.skipped_slots, number(json["skipped_slots"]));
  EF_ASSIGN_OR_RETURN(result.evaluated_at_us, time_number(json["evaluated_at"]));
  if (!json["source"].is_null()) {
    EF_ASSIGN_OR_RETURN(auto source, json_source(json["source"]));
    result.source = std::move(source);
  }

  if (json.contains("cep_match")) {
    EF_ASSIGN_OR_RETURN(auto match, json_match(json["cep_match"]));
    result.cep_match = std::move(match);
  }

  return result;
}

struct RelationEnvelope {
  std::size_t row_bytes = 0;
  std::size_t key_bytes = 0;
  std::size_t state_bytes = 0;
};

struct BindingAudit {
  std::uint64_t skipped_count = 0;
  std::optional<std::int64_t> last_slot_or_time;
  RequestPendingReason reason = RequestPendingReason::None;
};

struct CandidateState {
  std::int64_t epoch = 0;
  std::int64_t deadline_us = 0;
  bool active = false;
  std::vector<std::uint8_t> bindings;
  std::vector<bool> level_dirty;
  std::vector<std::int64_t> level_deadlines;
  std::int64_t started_at_us = 0;
  std::vector<std::int64_t> deadline_caps;
  std::vector<BindingAudit> audits;
};

struct ExpiryNotice {
  std::uint64_t sequence = 0;
  std::string key;
  std::int64_t deleted_at_us = 0;
};

struct PendingRequestIntent {
  std::size_t binding = 0;
  RequestAdmission admission;
  std::int64_t deadline_us = 0;
};

struct KeyState {
  Row scope;
  std::vector<FsmState> rules;
  std::vector<CepState> cep;
  std::optional<CandidateState> candidate = {};
};

struct TtlState {
  RelationId relation;
  Row identity;
  std::uint64_t generation;
  std::int64_t deadline;
};

struct ClockState {
  std::size_t policy;
  std::string key;
  std::int64_t next_us;
  std::optional<std::int64_t> last_us;
  std::uint64_t skipped = 0;
};

struct DedupReceipt {
  SourceIdentity source;
  std::string payload;
};

struct LifecycleState {
  std::int64_t time_us = 0;
  std::uint64_t sequence = 0;
  std::uint64_t generation = 0;
  std::map<std::string, KeyState> keys;
  std::map<std::string, TtlState> ttl;
  std::map<std::string, ClockState> clocks;
  std::vector<DedupReceipt> receipts;
  std::map<std::string, ProcessedPosition> positions;
  std::vector<TriggerOccurrence> outbox;
  std::int64_t timer_frontier_us = 0;
  std::uint64_t notice_sequence = 0;
  std::vector<ExpiryNotice> notices;
  std::map<std::string, PendingRequestIntent> intents;
};
enum class OperationKind { Data, Advance, Acknowledge, Admit, Complete, Cancel, Claim, Pump };

struct Operation {
  OperationKind kind = OperationKind::Advance;
  ProcessorEvent event;
  std::string payload;
  std::int64_t target_us = 0;
  std::vector<std::string> ids;
  RequestKey request = {};
  FunctionCompletion completion = {};
  std::size_t limit = 16;
  unsigned phase = 0;
  std::set<std::string> attempted = {};
  bool recheck_intents = false;
  unsigned resume_phase = 8;
};

StatusOr<std::set<RelationId>> reachable(const CatalogSnapshot& catalog,
                                         std::set<RelationId> roots) noexcept {
  for (const auto& relation : catalog.relations) {
    EF_ASSIGN_OR_RETURN(auto inputs, dependencies(*relation));
    for (auto dependency : inputs)
      if (roots.contains(dependency))
        roots.insert(relation->id);
  }

  return roots;
}
} // namespace

struct EventProcessor::Impl {
  std::shared_ptr<IStateStore> store;
  CatalogSnapshot catalog;
  EngineConfig engine_config;
  ProcessorConfig config;
  std::unique_ptr<Engine> engine;
  std::unique_ptr<RequestRuntime> requests;
  std::size_t deadline_payload_index = 0;
  std::size_t max_control_emissions = 0;
  std::vector<std::optional<std::size_t>> binding_deadline_indices;
  PendingCommitOwner pending_owner = PendingCommitOwner::Lifecycle;
  std::optional<RequestOperationResult> pending_request_result;
  bool pending_specified_request = false;
  std::vector<std::unique_ptr<FsmProgram>> programs;
  std::vector<std::unique_ptr<CepProgram>> cep_programs;
  Schema scope_schema;
  std::string fingerprint;
  std::string state_key;
  LifecycleState state;
  std::optional<Operation> pending_operation;
  std::unique_ptr<LifecycleState> pending_state;
  std::vector<TriggerOccurrence> pending_occurrences;
  std::vector<ClockFiring> pending_clock_firings;
  bool pending_final = false;
  bool active = false;

  Status ready() const noexcept {
    if (active)
      return {ErrorCode::FailedPrecondition, "event processor is not reentrant"};
    if (engine->has_pending_commit())
      return {ErrorCode::FailedPrecondition, "event processor has an unresolved commit"};
    if (engine->failed())
      return {ErrorCode::FailedPrecondition, "event processor shard is failed"};

    EF_ASSIGN_OR_RETURN(auto status, store->status(engine_config.shard));
    if (status.revision != engine->revision())
      return {ErrorCode::Conflict, "event processor owner state is stale"};

    return {};
  }

  Status validate_scope(const Row& scope) const noexcept {
    EF_RETURN_IF_ERROR(scope_schema.validate(scope));
    EF_ASSIGN_OR_RETURN(const auto shard, shard_for(scope, engine_config.shard_count));
    return check(shard == engine_config.shard, "event scope belongs to another shard");
  }

  Status bind() noexcept;

  Status write_audit(RoundContext& round, const LifecycleState& next,
                     const std::vector<TriggerOccurrence>& occurrences) const noexcept;
  Status reserve_control(const LifecycleState& value, Json snapshot,
                         std::size_t pending_matches) const noexcept;

  StatusOr<std::string> encode(const LifecycleState& value) const noexcept;
  StatusOr<LifecycleState> decode(std::string_view bytes) const noexcept;

  StatusOr<std::string> event_payload(const ProcessorEvent& event) const noexcept;
  StatusOr<FsmReads> read_views(RoundContext& round,
                                const std::map<std::string, RelationId>& bindings,
                                const Row& scope) const noexcept;

  Status emit(RoundContext& round, LifecycleState& next, const Row& scope, std::size_t rule,
              const FsmStep& step, const FsmReads& reads,
              const std::optional<SourceIdentity>& source,
              std::vector<TriggerOccurrence>& occurrences, std::string_view event_kind,
              const Row& event_values = {}, std::uint64_t skipped_slots = 0) noexcept;

  Status emit_cep(RoundContext& round, LifecycleState& next, const Row& scope, std::size_t rule,
                  const CepStep& step, const std::optional<SourceIdentity>& source,
                  std::vector<TriggerOccurrence>& occurrences, std::string_view event_kind,
                  const Row& event_values = {}, std::uint64_t skipped_slots = 0) noexcept;

  std::optional<std::int64_t> next_deadline(std::int64_t target) const noexcept;
  Status reserve_outbox(std::size_t count) const noexcept;
  Status data_candidate(LifecycleState& next, const Operation& operation) const noexcept;
  Status round(Operation& operation, std::int64_t at, bool final, ProcessResult& result) noexcept;
  ProcessResult run(Operation operation, ProcessResult result = {}) noexcept;

  Status bind_requests(Json& definition, const std::vector<DataType>& partition_types) noexcept;
  StatusOr<bool> freeze_intent(RoundContext& round, LifecycleState& next,
                               const TriggerOccurrence& occurrence) noexcept;
  StatusOr<std::vector<TableChange>> prepare_candidate(RoundContext& round, LifecycleState& next,
                                                       const ProcessorEvent& event) noexcept;

  Status publish_lifecycle(std::unique_ptr<LifecycleState> next,
                           const std::vector<TableChange>& inputs, std::int64_t now_us,
                           const RoundHook& hook, const RuntimePrepare& prepare,
                           ProcessResult& result) noexcept;

  Status update_frontier(LifecycleState& value) const noexcept;
  Status expiry_fence(std::int64_t now_us, ProcessResult& result) noexcept;
  Status timer_round(std::int64_t now_us, ProcessResult& result, bool& worked) noexcept;

  Status request_round(OperationKind kind, const RequestKey& key,
                       const FunctionCompletion& completion, std::int64_t now_us, std::size_t limit,
                       const std::string& intent_id,
                       std::optional<std::pair<std::string, std::size_t>> level, bool specified,
                       ProcessResult& result) noexcept;
  ProcessResult run_requests(Operation operation, ProcessResult result) noexcept;

  StatusOr<std::map<RelationId, RelationEnvelope>>
  relation_envelopes(const Row& scope) const noexcept;

  Status reserve_request_publication(const LifecycleState& value,
                                     const RequestAdmission& admission) const noexcept;
  std::size_t try_once_capacity(std::string_view event,
                                std::optional<std::string_view> rule = {}) const noexcept;

  Status request_ready(const RequestKey* key, std::int64_t now_us) const noexcept;
  PendingOperationInfo pending_info(const Operation& operation) const noexcept;

  void publish_request(RequestRuntimeResult value, ProcessResult& result,
                       RequestOperationResult operation_result, bool specified) noexcept;
};

Status EventProcessor::Impl::bind() noexcept {
  if (config.namespace_id == 0 || config.input_relations.empty() ||
      config.input_relations.size() > 256 || config.rules.size() > 64 ||
      config.cep_rules.size() > 64 || config.max_keys == 0 || config.max_timers == 0 ||
      config.max_source_partitions == 0 || config.max_dedup_entries == 0 ||
      config.max_dedup_bytes == 0 || config.max_event_bytes == 0 ||
      config.max_event_bytes > config.max_dedup_bytes || config.max_state_bytes == 0 ||
      config.max_state_bytes > 64 * 1024 * 1024 || config.max_occurrences == 0 ||
      config.max_occurrence_bytes == 0 || config.max_occurrence_bytes > config.max_outbox_bytes ||
      config.max_rounds_per_call == 0)
    return bad("invalid event processor definition or budget");

  for (const auto& relation : catalog.relations)
    if (relation->state_id == config.namespace_id)
      return bad("lifecycle namespace collides with aggregate state");

  std::set<RelationId> inputs;
  std::vector<DataType> partition_types;
  for (auto id : config.input_relations) {
    EF_ASSIGN_OR_RETURN(auto relation, catalog.find(id));
    if (relation->kind != RelationKind::BaseTable ||
        relation->base_write_role != BaseWriteRole::PublicInput || !inputs.insert(id).second)
      return bad("input roots must be distinct base relations");
    if (partition_types.empty())
      partition_types = relation->partition_types;
    if (partition_types != relation->partition_types)
      return bad("input roots require the same business partition types");
  }

  for (std::size_t i = 0; i < partition_types.size(); ++i) {
    scope_schema.fields.push_back({.id = static_cast<std::uint32_t>(i + 1),
                                   .name = "key_" + std::to_string(i),
                                   .type = partition_types[i],
                                   .nullable = false});
    scope_schema.primary_key.push_back(scope_schema.fields.back().name);
    scope_schema.partition_key.push_back(scope_schema.fields.back().name);
  }

  Json definition{{"format", 1},
                  {"catalog", catalog.fingerprint},
                  {"namespace", config.namespace_id},
                  {"inputs", config.input_relations},
                  {"rules", Json::array()},
                  {"ttl", Json::array()},
                  {"periodic", Json::array()}};
  EF_ASSIGN_OR_RETURN(auto input_graph, reachable(catalog, inputs));
  std::set<std::string> ids;

  for (const auto& rule : config.rules) {
    if (!ids.insert(rule.definition.id).second || rule.reads.size() != rule.definition.reads.size())
      return bad("duplicate rule or unmatched declared reads");

    for (const auto& alias : rule.definition.reads) {
      const auto found = rule.reads.find(alias);
      if (found == rule.reads.end())
        return bad("missing rule read binding");

      EF_ASSIGN_OR_RETURN(auto relation, catalog.find(found->second));
      if (!relation->unique_per_partition || relation->partition_types != partition_types)
        return bad("FSM reads must be unique per business key with matching partition types");
    }

    if (rule.sink) {
      EF_ASSIGN_OR_RETURN(auto relation, catalog.find(rule.sink->relation));
      if (relation->kind != RelationKind::BaseTable ||
          relation->base_write_role != BaseWriteRole::PublicInput || !rule.sink->make_row ||
          relation->partition_types != partition_types)
        return bad("occurrence sink requires a matching base relation and typed row factory");

      EF_ASSIGN_OR_RETURN(auto output_graph, reachable(catalog, {rule.sink->relation}));
      for (auto id : output_graph)
        if (input_graph.contains(id))
          return bad("occurrence sink graph intersects input graph");
    }

    const auto& fsm = rule.definition;
    Json item{{"id", fsm.id},
              {"version", fsm.version},
              {"initial", fsm.initial},
              {"reset", fsm.reset_event},
              {"reads", rule.reads},
              {"states", Json::array()},
              {"transitions", Json::array()},
              {"fields", Json::array()},
              {"max_steps", fsm.max_steps},
              {"max_state_bytes", fsm.max_state_bytes},
              {"sink", rule.sink ? Json(rule.sink->relation) : Json(nullptr)}};
    if (!fsm.definition_identity.empty())
      item["definition_identity"] = fsm.definition_identity;

    EF_ASSIGN_OR_RETURN(item["registers"], row_json(fsm.initial_registers));
    for (const auto& field : fsm.register_fields)
      item["fields"].push_back(
          Json::array({field.id, field.name, static_cast<int>(field.type), field.nullable}));

    for (const auto& state : fsm.states)
      item["states"].push_back(
          Json::array({state.name, state.timeout_us ? Json(*state.timeout_us) : Json(nullptr)}));

    for (const auto& transition : fsm.transitions)
      item["transitions"].push_back(Json::array(
          {transition.from, transition.event, transition.to, static_cast<bool>(transition.guard),
           static_cast<bool>(transition.update), transition.emit}));

    EF_ASSIGN_OR_RETURN(auto program, FsmProgram::create(fsm));
    programs.push_back(std::move(program));
    definition["rules"].push_back(std::move(item));
  }

  for (const auto& rule : config.cep_rules) {
    if (!ids.insert(rule.definition.id).second || rule.reads.size() != rule.definition.reads.size())
      return bad("duplicate CEP/FSM identity or unmatched declared reads");
    if (rule.input_kinds.size() > 64)
      return bad("CEP input domain exceeds the event kind limit");

    std::set<std::string> input_kinds;
    for (const auto& kind : rule.input_kinds)
      if (!text_ok(kind, 128) || kind == "*" || kind == "$timeout" ||
          !input_kinds.insert(kind).second)
        return bad("CEP input domain requires distinct nonempty event kinds");

    for (const auto& alias : rule.definition.reads) {
      const auto found = rule.reads.find(alias);
      if (found == rule.reads.end())
        return bad("missing CEP read binding");

      EF_ASSIGN_OR_RETURN(auto relation, catalog.find(found->second));
      if (!relation->unique_per_partition || relation->partition_types != partition_types)
        return bad("CEP reads must be unique per business key with matching partition types");
    }

    if (rule.sink) {
      EF_ASSIGN_OR_RETURN(auto relation, catalog.find(rule.sink->relation));
      if (relation->kind != RelationKind::BaseTable ||
          relation->base_write_role != BaseWriteRole::PublicInput || !rule.sink->make_row ||
          relation->partition_types != partition_types)
        return bad("CEP sink requires a matching public base relation and typed row factory");

      EF_ASSIGN_OR_RETURN(auto output_graph, reachable(catalog, {rule.sink->relation}));
      for (auto id : output_graph)
        if (input_graph.contains(id))
          return bad("CEP sink graph intersects input graph");
    }

    EF_ASSIGN_OR_RETURN(auto program, CepProgram::create(rule.definition));
    if (!definition.contains("cep"))
      definition["cep"] = Json::array();

    auto item = Json::array({std::string(program->fingerprint()), rule.reads,
                             rule.sink ? Json(rule.sink->relation) : Json(nullptr)});

    // Preserve exact legacy fingerprint bytes for rules observing the complete host stream.
    if (!rule.input_kinds.empty())
      item.push_back(rule.input_kinds);

    definition["cep"].push_back(std::move(item));
    cep_programs.push_back(std::move(program));
  }

  std::set<RelationId> ttl_relations;
  for (const auto& ttl : config.ttl_policies) {
    if (!inputs.contains(ttl.relation) || ttl.ttl_us <= 0 ||
        !ttl_relations.insert(ttl.relation).second)
      return bad("TTL requires a unique positive policy on a declared input table");

    definition["ttl"].push_back(Json::array({ttl.relation, ttl.ttl_us}));
  }

  ids.clear();
  if (config.periodic_policies.size() > 64)
    return bad("too many periodic policies");

  for (const auto& policy : config.periodic_policies) {
    if (!text_ok(policy.id, 128) || !text_ok(policy.event_kind, 128) || policy.event_kind == "*" ||
        policy.event_kind == "$timeout" || policy.event_kind == "$expire" ||
        policy.interval_us <= 0 || policy.phase_us < 0 || !ids.insert(policy.id).second)
      return bad("invalid periodic policy");

    definition["periodic"].push_back(
        Json::array({policy.id, policy.event_kind, policy.interval_us, policy.phase_us}));
  }

  if (config.requests)
    EF_RETURN_IF_ERROR(bind_requests(definition, partition_types));

  EF_RETURN_IF_ERROR(
      validate_trigger_audit_relation(catalog, config.trigger_audit, partition_types));
  if (config.trigger_audit.relation) {
    if (config.trigger_audit.max_rows_per_scope > engine_config.max_rows ||
        config.trigger_audit.max_bytes_per_scope > engine_config.max_bytes)
      return full("trigger audit retention must fit the owner scan budgets");

    definition["trigger_audit"] = {
        {"format", 1},
        {"relation", config.trigger_audit.relation},
        {"max_rows_per_scope", config.trigger_audit.max_rows_per_scope},
        {"max_bytes_per_scope", config.trigger_audit.max_bytes_per_scope}};
  }

  fingerprint = definition.dump();
  if (fingerprint.size() > config.max_state_bytes / 2)
    return full("processor definition exceeds snapshot budget");
  if (!cep_programs.empty() && !config.periodic_policies.empty()) {
    // Periodic input may TAKE in every CEP program. Reject an impossible declared envelope
    // before opening an Engine, rather than presenting it as a transient first-input backlog.
    std::size_t state_bytes = 0;
    std::size_t outputs = 0;
    std::size_t timers = config.periodic_policies.size();

    for (const auto& rule : config.cep_rules) {
      const auto& limits = rule.definition.limits;
      const auto quoted_state = 2 * limits.max_state_bytes + 2;
      if (quoted_state > config.max_state_bytes - state_bytes)
        return full("periodic CEP declared state budgets exceed processor control capacity");

      state_bytes += quoted_state;
      if (limits.max_paths > config.max_timers - std::min(timers, config.max_timers))
        return full("periodic CEP declared path budgets exceed processor timer capacity");

      timers += limits.max_paths;
      for (std::size_t i = 0; i < config.periodic_policies.size(); ++i) {
        if (limits.max_matches_per_step > config.max_occurrences - outputs)
          return full("periodic CEP declared match budgets exceed processor output capacity");

        outputs += limits.max_matches_per_step;
      }
    }

    const auto available = config.max_state_bytes - state_bytes;
    if (outputs > config.max_outbox_bytes / config.max_occurrence_bytes ||
        (outputs != 0 && (config.max_occurrence_bytes >= available ||
                          outputs > available / (config.max_occurrence_bytes + 1))))
      return full("periodic CEP declared output budgets exceed processor control byte capacity");
  }

  EF_ASSIGN_OR_RETURN(state_key, row_key(engine_config.shard, 'S', config.namespace_id,
                                         Row{std::string("lifecycle")}));
  return {};
}

Status EventProcessor::Impl::bind_requests(Json& definition,
                                           const std::vector<DataType>& partition_types) noexcept {
  const auto& request = *config.requests;
  if (!request.registry || request.plans.empty() || request.max_pending_intents == 0 ||
      request.runtime.namespace_id == config.namespace_id || request.trigger_bindings.size() > 64 ||
      request.level_bindings.size() > 64 || !request.candidate)
    return bad("request processor requires registry, plans, candidate policy and distinct budgets");

  const auto& policy = *request.candidate;
  EF_ASSIGN_OR_RETURN(auto relation, catalog.find(policy.relation));
  if (!relation->candidate || relation->partition_types != partition_types || !policy.on_source ||
      policy.version == 0 || !text_ok(policy.deadline_column))
    return bad("invalid typed candidate expiry policy");

  const auto& payload = relation->candidate->definition.payload_fields;
  auto deadline = std::find_if(payload.begin(), payload.end(), [&](const Field& field) noexcept {
    return field.name == policy.deadline_column;
  });
  if (deadline == payload.end() || deadline->type != DataType::Int64 || deadline->nullable)
    return bad("candidate deadline must name a non-null Int64 payload field");

  deadline_payload_index = static_cast<std::size_t>(deadline - payload.begin());
  std::set<std::string> plans;
  for (const auto& plan : request.plans) {
    if (!plans.insert(plan.name).second)
      return bad("duplicate request plan");
    if (plan.request_status_relation) {
      EF_ASSIGN_OR_RETURN(auto status, catalog.find(*plan.request_status_relation));
      if (!status->request_status ||
          status->request_status->definition.candidate_relation != policy.relation)
        return bad("request plan status must use the processor candidate");
    }
  }

  Json encoded{
      {"candidate", Json::array({policy.relation, policy.deadline_column, policy.version})},
      {"namespace", request.runtime.namespace_id},
      {"runtime_limits",
       Json::array({request.runtime.owner_epoch, request.runtime.max_active_requests,
                    request.runtime.max_active_bytes, request.runtime.max_record_bytes,
                    request.runtime.max_index_bytes, request.runtime.max_argument_bytes,
                    request.max_pending_intents})},
      {"lifecycle_limits", Json::array({config.max_state_bytes, config.max_event_bytes})},
      {"bindings", Json::array()},
      {"levels", Json::array()}};
  std::set<std::string> ids;
  std::set<std::pair<std::string, std::string>> emissions;

  for (const auto& binding : request.trigger_bindings) {
    if (!text_ok(binding.id, 128) || binding.version == 0 || !plans.contains(binding.plan) ||
        !binding.make_admission || binding.pending_lifetime_us <= 0 ||
        !ids.insert(binding.id).second ||
        !emissions.emplace(binding.rule_id, binding.emission_name).second)
      return bad("invalid or ambiguous request trigger binding");

    bool found = false;
    for (const auto& rule : config.rules)
      if (rule.definition.id == binding.rule_id)
        for (const auto& transition : rule.definition.transitions)
          found |= transition.emit == binding.emission_name;

    for (const auto& rule : config.cep_rules)
      found |= rule.definition.id == binding.rule_id && binding.emission_name == "match";

    if (!found)
      return bad("request binding has no declared emission");
    if (binding.mode != RequestTriggerMode::Wait &&
        binding.mode != RequestTriggerMode::TryOnceAtFiring)
      return bad("unknown request trigger mode");
    if (binding.mode == RequestTriggerMode::TryOnceAtFiring && config.max_rounds_per_call < 2)
      return bad("try-once triggers require at least two rounds per call");

    std::optional<std::size_t> deadline_index;
    if (!binding.deadline_column.empty()) {
      const auto field =
          std::find_if(payload.begin(), payload.end(), [&](const Field& value) noexcept {
            return value.name == binding.deadline_column;
          });
      if (field == payload.end() || field->type != DataType::Int64 || field->nullable)
        return bad("pending deadline cap must name a non-null Int64 Candidate payload field");

      deadline_index = static_cast<std::size_t>(field - payload.begin());
    }

    binding_deadline_indices.push_back(deadline_index);
    encoded["bindings"].push_back(Json::array(
        {binding.id, binding.version, binding.rule_id, binding.emission_name, binding.plan,
         binding.once_per_candidate, binding.pending_lifetime_us, binding.required_accepted_binding,
         static_cast<int>(binding.mode), binding.deadline_column}));
  }

  for (const auto& binding : request.level_bindings) {
    if (!text_ok(binding.id, 128) || binding.version == 0 || !plans.contains(binding.plan) ||
        binding.pending_lifetime_us <= 0 || !ids.insert(binding.id).second)
      return bad("invalid level request binding");

    encoded["levels"].push_back(
        Json::array({binding.id, binding.version, binding.plan, binding.pending_lifetime_us}));
  }

  for (const auto& binding : request.trigger_bindings) {
    if (binding.required_accepted_binding.empty())
      continue;

    bool supported = false;
    for (const auto& other : request.trigger_bindings)
      supported |= other.id == binding.required_accepted_binding && other.once_per_candidate;

    for (const auto& other : request.level_bindings)
      supported |= other.id == binding.required_accepted_binding;

    if (!supported || binding.required_accepted_binding == binding.id)
      return bad("required admission binding must name a different candidate once binding");
  }

  const auto check_control_event = [&](std::string_view event, bool cep) noexcept -> Status {
    std::size_t outputs = 0;
    for (const auto& rule : config.rules)
      outputs += std::any_of(rule.definition.transitions.begin(), rule.definition.transitions.end(),
                             [&](const FsmTransition& transition) noexcept {
                               return !transition.emit.empty() &&
                                      (transition.event == event || transition.event == "*");
                             });

    if (cep)
      for (const auto& rule : config.cep_rules) {
        if (rule.definition.limits.max_matches_per_step >
            config.max_occurrences - std::min(outputs, config.max_occurrences))
          return full("periodic event requires a larger combined FSM/CEP output budget");

        outputs += rule.definition.limits.max_matches_per_step;
      }
    max_control_emissions = std::max(max_control_emissions, outputs);
    return outputs > config.max_occurrences ||
                   outputs > config.max_outbox_bytes / config.max_occurrence_bytes
               ? full("one lifecycle event exceeds the output count or byte budget")
               : Status{};
  };
  if (!config.ttl_policies.empty())
    EF_RETURN_IF_ERROR(check_control_event("$expire", false));

  for (const auto& policy : config.periodic_policies)
    EF_RETURN_IF_ERROR(check_control_event(policy.event_kind, true));

  definition["requests"] = std::move(encoded);
  return {};
}

StatusOr<bool> EventProcessor::Impl::freeze_intent(RoundContext& round, LifecycleState& next,
                                                   const TriggerOccurrence& occurrence) noexcept {
  if (!config.requests)
    return true;

  const auto& bindings = config.requests->trigger_bindings;
  for (std::size_t index = 0; index < bindings.size(); ++index) {
    const auto& binding = bindings[index];
    if (binding.rule_id != occurrence.rule_id || binding.emission_name != occurrence.emission.name)
      continue;

    EF_ASSIGN_OR_RETURN(auto key, scope_key(occurrence.scope));
    auto owned = next.keys.find(key);
    if (owned == next.keys.end() || !owned->second.candidate || !owned->second.candidate->active)
      return false;

    auto& candidate = *owned->second.candidate;
    if (candidate.deadline_us <= next.time_us ||
        (binding.once_per_candidate && candidate.bindings[index] != 0))
      return false;
    if (binding.once_per_candidate)
      for (const auto& [id, existing] : next.intents) {
        (void)id;
        if (existing.binding == index && existing.admission.scope == occurrence.scope &&
            existing.admission.candidate_epoch == static_cast<std::uint64_t>(candidate.epoch))
          return false;
      }

    // Build capture_match arguments from this occurrence and retain them in the pending intent.
    // Waiting for admission must not replace the matched events with newer input.
    EF_ASSIGN_OR_RETURN(auto admission, binding.make_admission(round, occurrence));
    if ((!admission.plan.empty() && admission.plan != binding.plan) ||
        (!admission.scope.empty() && admission.scope != occurrence.scope) ||
        (admission.candidate_epoch != 0 &&
         admission.candidate_epoch != static_cast<std::uint64_t>(candidate.epoch)))
      return bad("admission factory changed binding scope, plan or candidate");
    if (next.time_us > INT64_MAX - binding.pending_lifetime_us)
      return full("pending request deadline overflow");

    admission.plan = binding.plan;
    admission.scope = occurrence.scope;
    admission.candidate_epoch = static_cast<std::uint64_t>(candidate.epoch);
    admission.trigger_id = occurrence.id;
    admission.request_id = Json::array({binding.id, binding.version, occurrence.id}).dump();
    const auto plan = std::find_if(
        config.requests->plans.begin(), config.requests->plans.end(),
        [&](const RequestPlanDefinition& value) noexcept { return value.name == binding.plan; });
    if (admission.lifetime_us < 0 || admission.lifetime_us > plan->machine.max_lifetime_us)
      return bad("binding produced an invalid request lifetime");
    if (admission.request_id.size() >
            std::min<std::size_t>(1024, plan->machine.max_identifier_bytes) ||
        admission.trigger_id.size() > 4096)
      return full("binding produced an oversized request identity");

    const auto deadline = std::min({candidate.deadline_us, candidate.deadline_caps[index],
                                    next.time_us + binding.pending_lifetime_us});
    if (deadline <= next.time_us) {
      if (candidate.bindings[index] == 0)
        candidate.bindings[index] = 2;

      candidate.audits[index].reason = RequestPendingReason::Expired;
      candidate.audits[index].last_slot_or_time =
          occurrence.emission.scheduled_time_us.value_or(occurrence.evaluated_at_us);
      return false;
    }

    if (binding.mode == RequestTriggerMode::TryOnceAtFiring &&
        candidate.audits[index].skipped_count == UINT64_MAX)
      return full("try-once skipped counter is exhausted");

    candidate.audits[index].last_slot_or_time =
        occurrence.emission.scheduled_time_us.value_or(occurrence.evaluated_at_us);
    candidate.audits[index].reason = RequestPendingReason::None;
    next.intents.emplace(occurrence.id, PendingRequestIntent{.binding = index,
                                                             .admission = std::move(admission),
                                                             .deadline_us = deadline});
    break;
  }

  return true;
}

StatusOr<std::vector<TableChange>>
EventProcessor::Impl::prepare_candidate(RoundContext& round, LifecycleState& next,
                                        const ProcessorEvent& event) noexcept {
  if (!config.requests)
    return std::vector<TableChange>{};

  const auto& request = *config.requests;
  const auto& policy = *request.candidate;
  EF_ASSIGN_OR_RETURN(auto key, scope_key(event.scope));
  auto& owned = next.keys.at(key);
  if (owned.candidate)
    std::fill(owned.candidate->level_dirty.begin(), owned.candidate->level_dirty.end(), true);

  EF_ASSIGN_OR_RETURN(auto activation, policy.on_source(round, event));
  if (!activation)
    return std::vector<TableChange>{};
  if (activation->payload.size() <= deadline_payload_index)
    return bad("candidate payload lacks declared deadline");

  const auto* deadline = std::get_if<std::int64_t>(&activation->payload[deadline_payload_index]);
  if (!deadline || *deadline <= event.processing_time_us)
    return bad("candidate activation requires a future typed deadline");

  const auto expected = owned.candidate ? owned.candidate->epoch : 0;
  if (activation->expected_epoch != expected)
    return Status(ErrorCode::Conflict, "candidate policy used a stale epoch");

  EF_ASSIGN_OR_RETURN(auto write, EngineRuntimeAccess::make_candidate_write(
                                      round, policy.relation, event.scope,
                                      CandidateOperation::Activate, expected, activation->payload));
  EF_ASSIGN_OR_RETURN(auto candidate_relation, catalog.find(policy.relation));
  EF_ASSIGN_OR_RETURN(auto candidate_bytes,
                      encode_row(candidate_relation->storage_schema, *write.row));
  if (candidate_bytes.size() > config.max_event_bytes)
    return full("candidate payload exceeds its declared event byte envelope");

  CandidateState candidate;
  candidate.epoch = expected + 1;
  candidate.active = true;
  candidate.deadline_us = *deadline;
  candidate.started_at_us = event.processing_time_us;

  for (const auto& index : binding_deadline_indices) {
    const auto cap = index ? std::get<std::int64_t>(activation->payload[*index]) : *deadline;
    if (cap < 0)
      return bad("candidate pending deadline cap cannot be negative");

    candidate.deadline_caps.push_back(std::min(cap, *deadline));
  }

  candidate.audits.resize(request.trigger_bindings.size() + request.level_bindings.size());
  candidate.bindings.resize(request.trigger_bindings.size() + request.level_bindings.size(), 0);
  candidate.level_dirty.resize(request.level_bindings.size(), true);
  for (const auto& binding : request.level_bindings) {
    const auto identity = Json::array({binding.id, binding.version, key, candidate.epoch}).dump();
    const auto plan = std::find_if(
        request.plans.begin(), request.plans.end(),
        [&](const RequestPlanDefinition& value) noexcept { return value.name == binding.plan; });
    if (identity.size() > std::min<std::size_t>(1024, plan->machine.max_identifier_bytes))
      return full("candidate level request identity exceeds its plan limit");
    if (event.processing_time_us > INT64_MAX - binding.pending_lifetime_us)
      return full("candidate level deadline overflow");

    candidate.level_deadlines.push_back(
        std::min(*deadline, event.processing_time_us + binding.pending_lifetime_us));
  }

  owned.candidate = std::move(candidate);
  return std::vector<TableChange>{std::move(write)};
}

Status EventProcessor::Impl::reserve_control(const LifecycleState& value, Json snapshot,
                                             std::size_t pending_matches) const noexcept {
  if (config.requests) {
    Json fence = snapshot;
    fence["time"] = INT64_MAX;
    fence["timer_frontier"] = INT64_MAX;
    fence["notice_sequence"] = UINT64_MAX;

    for (auto& notice : fence["deferred_events"]) {
      notice[1] = UINT64_MAX;
      notice[3] = INT64_MAX;
    }

    std::size_t mutations = 2;
    std::size_t rows = value.ttl.size();
    const auto add_closure = [&](RelationId root) noexcept -> Status {
      EF_ASSIGN_OR_RETURN(auto affected, reachable(catalog, {root}));
      for (auto id : affected) {
        EF_ASSIGN_OR_RETURN(auto relation, catalog.find(id));
        const auto count =
            1U +
            (relation->latest || (relation->plan && !relation->plan->group_by.empty()) ? 1U : 0U);
        if (mutations > engine_config.max_mutations ||
            count > engine_config.max_mutations - mutations)
          return full("all-shard expiry closure exceeds mutation reservation");

        mutations += count;
      }

      return {};
    };
    for (const auto& [id, ttl] : value.ttl) {
      (void)id;
      EF_ASSIGN_OR_RETURN(auto relation, catalog.find(ttl.relation));
      Row scope(ttl.identity.begin(), ttl.identity.begin() + relation->partition_types.size());
      EF_ASSIGN_OR_RETURN(auto key, scope_key(scope));
      fence["deferred_events"].push_back(
          Json::array({"TtlExpiryNotice", UINT64_MAX, key, INT64_MAX}));
      EF_RETURN_IF_ERROR(add_closure(ttl.relation));
    }

    fence["ttl"] = Json::array();
    for (auto& candidate : fence["candidates"]) {
      candidate[3] = false;
      for (auto& dirty : candidate[5])
        dirty = false;

      for (auto& audit : candidate[9]) {
        audit[0] = UINT64_MAX;
        audit[1] = INT64_MAX;
        audit[2] = static_cast<int>(RequestPendingReason::OnceConsumed);
      }
    }

    for (const auto& [key, owned] : value.keys) {
      (void)key;
      if (owned.candidate && owned.candidate->active) {
        ++rows;
        EF_RETURN_IF_ERROR(add_closure(config.requests->candidate->relation));
      }
    }

    const auto bytes = fence.dump().size();
    const auto metadata = Json(catalog.fingerprint).dump().size() + 256;
    std::size_t relation_bytes = 0;
    const auto reserve_scope = [&](RelationId root, const Row& scope) noexcept -> Status {
      EF_ASSIGN_OR_RETURN(auto envelopes, relation_envelopes(scope));
      EF_ASSIGN_OR_RETURN(auto affected, reachable(catalog, {root}));
      auto reads = affected;
      for (auto id : affected) {
        EF_ASSIGN_OR_RETURN(auto relation, catalog.find(id));
        if (relation->plan) {
          reads.insert(relation->plan->left);
          if (relation->plan->right)
            reads.insert(*relation->plan->right);
        }
      }

      for (auto id : reads) {
        EF_ASSIGN_OR_RETURN(auto relation, catalog.find(id));

        // Candidate declarations do not synthesize RequestStatus history rows.
        if (relation->request_status)
          continue;

        const auto found = envelopes.find(id);
        if (found == envelopes.end())
          return bad("expiry relation has no compatible owner scope envelope");

        const auto& envelope = found->second;
        for (const auto size : {envelope.row_bytes, envelope.key_bytes, envelope.state_bytes,
                                envelope.state_bytes ? envelope.key_bytes : 0}) {
          if (size > engine_config.max_bytes - relation_bytes)
            return full("all-shard expiry relation bytes exceed the control reservation");

          relation_bytes += size;
        }
      }

      return {};
    };
    for (const auto& [key, ttl] : value.ttl) {
      (void)key;
      EF_ASSIGN_OR_RETURN(auto relation, catalog.find(ttl.relation));
      Row scope(ttl.identity.begin(), ttl.identity.begin() + relation->partition_types.size());
      EF_RETURN_IF_ERROR(reserve_scope(ttl.relation, scope));
    }

    for (const auto& [key, owned] : value.keys) {
      (void)key;
      if (owned.candidate && owned.candidate->active)
        EF_RETURN_IF_ERROR(reserve_scope(config.requests->candidate->relation, owned.scope));
    }

    if (relation_bytes > engine_config.max_bytes ||
        bytes > engine_config.max_bytes - relation_bytes ||
        metadata + state_key.size() > engine_config.max_bytes - relation_bytes - bytes)
      return full("all-shard expiry snapshot and relation closure exceed the byte reservation");
    if (bytes > config.max_state_bytes || rows > engine_config.max_rows ||
        metadata > engine_config.max_bytes ||
        state_key.size() > engine_config.max_bytes - metadata ||
        bytes > engine_config.max_bytes - metadata - state_key.size())
      return full("expiry notice and maximum-width control snapshot reservation unavailable");
  }

  if (cep_programs.empty() && !config.requests)
    return {};

  // Existing durable output can be acknowledged before retrying control work. Count its
  // actual storage separately: including it here would prevent a one-slot timer chain from
  // publishing the first alarm while reserving the following alarm.
  snapshot["outbox"] = Json::array();
  snapshot["time"] = INT64_MAX;
  std::size_t future_outputs = pending_matches;
  std::size_t extra_state_bytes = 0;
  std::size_t future_timers = value.ttl.size() + value.clocks.size();
  const auto add_outputs = [&](std::size_t count) noexcept -> Status {
    if (config.requests) {
      if (count > config.max_occurrences)
        return full("one control event exceeds the available output budget");

      // Request mode drains independent timer events in separate bounded revisions.
      future_outputs = std::max(future_outputs, count);
      return {};
    }

    if (count > config.max_occurrences - future_outputs)
      return full("combined FSM/CEP control output count reservation unavailable");

    future_outputs += count;
    return {};
  };
  const auto add_state_bound = [&](std::size_t maximum, std::size_t encoded) noexcept -> Status {
    const auto additional = maximum > encoded ? maximum - encoded : 0;
    if (additional > config.max_state_bytes - extra_state_bytes)
      return full("control state expansion reservation unavailable");

    extra_state_bytes += additional;
    return {};
  };
  std::set<std::string> expiring_scopes;

  for (const auto& [id, ttl] : value.ttl) {
    (void)id;
    EF_ASSIGN_OR_RETURN(auto relation, catalog.find(ttl.relation));
    Row scope(ttl.identity.begin(), ttl.identity.begin() + relation->partition_types.size());
    EF_ASSIGN_OR_RETURN(auto key, scope_key(scope));
    expiring_scopes.insert(std::move(key));
  }

  std::size_t key_index = 0;
  for (const auto& [key, owned] : value.keys) {
    auto& encoded_key = snapshot["keys"][key_index++];
    for (std::size_t i = 0; i < owned.rules.size(); ++i) {
      std::vector<std::string_view> events;
      if (expiring_scopes.contains(key))
        events.push_back("$expire");

      for (const auto& policy : config.periodic_policies)
        events.push_back(policy.event_kind);

      const auto& definition = config.rules[i].definition;
      std::set<std::string> control_states{owned.rules[i].state};
      std::vector<std::string> work{owned.rules[i].state};
      bool may_timeout = false;

      for (std::size_t cursor = 0; cursor < work.size(); ++cursor) {
        const auto current = std::find_if(
            definition.states.begin(), definition.states.end(),
            [&](const FsmStateDefinition& state) noexcept { return state.name == work[cursor]; });
        const bool timed = current != definition.states.end() && current->timeout_us.has_value();
        may_timeout |= timed;
        const auto add_state = [&](const std::string& name) noexcept {
          if (control_states.insert(name).second)
            work.push_back(name);
        };

        for (const auto event : events)
          if (!definition.reset_event.empty() && event == definition.reset_event)
            add_state(definition.initial);

        for (const auto& transition : definition.transitions) {
          if (transition.from != work[cursor])
            continue;

          bool enabled = timed && transition.event == "$timeout";
          for (const auto event : events)
            if (event != definition.reset_event &&
                (transition.event == event || transition.event == "*"))
              enabled = true;

          if (enabled)
            add_state(transition.to);
        }
      }

      // A clock/expiry can enter a timed state without emitting. Reserve its next control
      // round as well, so the first clock cannot become an uncommittable no-output loop.
      if (may_timeout)
        events.insert(events.begin(), "$timeout");

      bool may_transition = false;
      bool may_update = false;
      bool may_reset = false;
      bool may_start_timer = owned.rules[i].deadline_us.has_value();

      for (const auto event : events) {
        if (!definition.reset_event.empty() && event == definition.reset_event) {
          may_reset = true;
          may_transition = true;
          const auto initial = std::find_if(definition.states.begin(), definition.states.end(),
                                            [&](const FsmStateDefinition& state) noexcept {
                                              return state.name == definition.initial;
                                            });
          may_start_timer |= initial != definition.states.end() && initial->timeout_us.has_value();
          continue;
        }

        bool may_emit = false;
        for (const auto& transition : definition.transitions) {
          if (!control_states.contains(transition.from) ||
              (transition.event != event && !(transition.event == "*" && event != "$timeout")))
            continue;

          may_transition = true;
          may_update |= static_cast<bool>(transition.update);
          may_emit |= !transition.emit.empty();
          const auto target = std::find_if(definition.states.begin(), definition.states.end(),
                                           [&](const FsmStateDefinition& state) noexcept {
                                             return state.name == transition.to;
                                           });
          may_start_timer |= target != definition.states.end() && target->timeout_us.has_value();
        }

        // One deterministic transition per FSM per timeout/expiry/coalesced clock event.
        if (may_emit)
          EF_RETURN_IF_ERROR(add_outputs(1));
      }

      future_timers += may_start_timer;
      EF_ASSIGN_OR_RETURN(auto fsm, parse_json_exact(encoded_key[2][i].get<std::string>()));
      fsm["time_us"] = INT64_MAX;
      if (may_reset) {
        Json initial_registers = Json::array();
        for (const auto& value : definition.initial_registers) {
          EF_ASSIGN_OR_RETURN(auto encoded, value_to_json(value));
          initial_registers.push_back(std::move(encoded));
        }

        if (Json(initial_registers.dump()).dump().size() >
            Json(fsm["registers"].dump()).dump().size())
          fsm["registers"] = std::move(initial_registers);
      }

      if (may_transition) {
        fsm["transition_sequence"] = UINT64_MAX;
        fsm["entered_at_us"] = INT64_MAX;
        if (may_start_timer)
          fsm["deadline_us"] = INT64_MAX;

        std::string longest = owned.rules[i].state;
        for (const auto& state : control_states)
          if (Json(Json(state).dump()).dump().size() > Json(Json(longest).dump()).dump().size())
            longest = state;

        fsm["state"] = std::move(longest);
      }

      encoded_key[2][i] = fsm.dump();
      if (may_update) {
        // The FSM codec bounds a callback's full resulting JSON. Its enclosing JSON string
        // adds quotes and at most one escape per byte; no future callback is executed here.
        EF_RETURN_IF_ERROR(
            add_state_bound(2 * definition.max_state_bytes + 2, encoded_key[2][i].dump().size()));
      }
    }

    for (std::size_t i = 0; i < owned.cep.size(); ++i) {
      EF_ASSIGN_OR_RETURN(auto cep, parse_json_exact(encoded_key[3][i].get<std::string>()));
      cep["time"] = INT64_MAX;
      encoded_key[3][i] = cep.dump();
      if (config.periodic_policies.empty()) {
        // Pure expiry only releases immutable graphs. Keep all current graphs in this bound
        // while separately charging every delayed output, including JSON array separators.
        EF_ASSIGN_OR_RETURN(auto summary, cep_programs[i]->inspect(owned.cep[i]));
        future_timers += summary.paths.size();
      } else {
        const auto& limits = config.cep_rules[i].definition.limits;
        for (std::size_t policy = 0; policy < config.periodic_policies.size(); ++policy)
          EF_RETURN_IF_ERROR(add_outputs(limits.max_matches_per_step));

        // A periodic TAKE can grow paths, registers and history, unlike a pure advance.
        // Its explicit component state limit is the conservative envelope for this frame.
        EF_RETURN_IF_ERROR(
            add_state_bound(2 * limits.max_state_bytes + 2, encoded_key[3][i].dump().size()));
        if (limits.max_paths > config.max_timers - std::min(future_timers, config.max_timers))
          return full("periodic CEP timer reservation unavailable");

        future_timers += limits.max_paths;
      }
    }
  }

  if (future_timers > config.max_timers)
    return full("combined control timer reservation unavailable");

  for (auto& clock : snapshot["clocks"]) {
    clock[3] = INT64_MAX;
    clock[4] = INT64_MAX;
    clock[5] = static_cast<std::uint64_t>(INT64_MAX);
  }

  if (config.requests && !value.keys.empty())
    future_outputs = std::max(future_outputs, max_control_emissions);
  if (future_outputs > config.max_outbox_bytes / config.max_occurrence_bytes)
    return full("combined FSM/CEP control output byte reservation unavailable");

  const auto retained = snapshot.dump().size();
  if (retained > config.max_state_bytes || extra_state_bytes > config.max_state_bytes - retained)
    return full("control snapshot state expansion reservation unavailable");

  const auto available = config.max_state_bytes - retained - extra_state_bytes;

  // occurrence_json values are inserted directly into an array, not encoded as strings.
  // Charging one comma per item conservatively covers the complete serialized envelope.
  if (future_outputs != 0 && (config.max_occurrence_bytes >= available ||
                              future_outputs > available / (config.max_occurrence_bytes + 1)))
    return full("control snapshot output expansion reservation unavailable");

  return {};
}

StatusOr<std::string> EventProcessor::Impl::encode(const LifecycleState& value) const noexcept {
  if (value.keys.size() > config.max_keys ||
      value.positions.size() > config.max_source_partitions ||
      value.receipts.size() > config.max_dedup_entries ||
      value.outbox.size() > config.max_occurrences)
    return full("lifecycle entry budget exceeded");

  Json json{{"format", config.requests ? 2 : 1},
            {"fingerprint", fingerprint},
            {"time", value.time_us},
            {"sequence", value.sequence},
            {"generation", value.generation},
            {"keys", Json::array()},
            {"ttl", Json::array()},
            {"clocks", Json::array()},
            {"receipts", Json::array()},
            {"positions", Json::array()},
            {"outbox", Json::array()}};
  std::size_t timers = value.ttl.size() + value.clocks.size() + value.notices.size();
  std::size_t reserved_matches = 0;

  for (const auto& [key, state] : value.keys) {
    EF_ASSIGN_OR_RETURN(auto scope, row_json(state.scope));
    Json rules = Json::array();
    if (state.rules.size() != programs.size())
      return bad("persisted rule count mismatch");

    for (std::size_t i = 0; i < state.rules.size(); ++i) {
      EF_ASSIGN_OR_RETURN(auto encoded, programs[i]->encode_state(state.rules[i]));
      rules.push_back(std::move(encoded));
      timers += state.rules[i].deadline_us.has_value();
    }

    Json entry = Json::array({key, std::move(scope), std::move(rules)});
    if (state.cep.size() != cep_programs.size())
      return bad("persisted CEP count mismatch");
    if (!cep_programs.empty()) {
      Json cep = Json::array();
      for (std::size_t i = 0; i < state.cep.size(); ++i) {
        EF_ASSIGN_OR_RETURN(auto encoded, cep_programs[i]->encode_state(state.cep[i]));
        EF_ASSIGN_OR_RETURN(auto summary, cep_programs[i]->inspect(state.cep[i]));
        if (summary.paths.size() > config.max_timers - std::min(timers, config.max_timers))
          return full("CEP timer budget exceeded");

        timers += summary.paths.size();
        if (summary.pending_matches > config.max_occurrences - reserved_matches)
          return full("CEP pending output reservation exceeded");

        reserved_matches += summary.pending_matches;
        if (summary.pending_matches != 0) {
          // Validate the maximum-width timeout envelope before retaining a delayed match.
          // The pure drain does not run guards or publish a candidate state. Reserving count
          // alone would allow an oversized witness to make every later expiry uncommittable.
          EF_ASSIGN_OR_RETURN(auto drain, cep_programs[i]->advance(state.cep[i], INT64_MAX));
          const auto& rule = config.cep_rules[i];

          for (const auto& match : drain.matches) {
            TriggerOccurrence future{
                .id = Json::array({config.namespace_id, key, "cep", match.id}).dump(),
                .scope = state.scope,
                .rule_id = rule.definition.id,
                .rule_version = rule.definition.version,
                .emission = {.name = "match",
                             .cause = match.id,
                             .transition_sequence = UINT64_MAX,
                             .time_us = match.matched_at_us,
                             .scheduled_time_us = {}},
                .reads = match.reads,
                .source = {},
                .event_kind = "$timeout",
                .event_values = {},
                .skipped_slots = 0,
                .evaluated_at_us = INT64_MAX,
                .cep_match = match};
            EF_ASSIGN_OR_RETURN(auto future_json, occurrence_json(future));
            if (future_json.dump().size() > config.max_occurrence_bytes)
              return full("CEP delayed witness exceeds its timeout outbox reservation");
          }
        }

        cep.push_back(std::move(encoded));
      }

      entry.push_back(std::move(cep));
    }

    json["keys"].push_back(std::move(entry));
  }

  if (timers > config.max_timers)
    return full("lifecycle timer budget exceeded");

  for (const auto& [key, timer] : value.ttl) {
    EF_ASSIGN_OR_RETURN(auto identity, row_json(timer.identity));
    json["ttl"].push_back(
        Json::array({key, timer.relation, std::move(identity), timer.generation, timer.deadline}));
  }

  for (const auto& [key, timer] : value.clocks)
    json["clocks"].push_back(
        Json::array({key, timer.policy, timer.key, timer.next_us,
                     timer.last_us ? Json(*timer.last_us) : Json(nullptr), timer.skipped}));

  std::size_t receipt_bytes = 0;
  for (const auto& receipt : value.receipts) {
    if (receipt.payload.size() > config.max_dedup_bytes - receipt_bytes)
      return full("dedup byte budget exceeded");

    receipt_bytes += receipt.payload.size();
    json["receipts"].push_back(Json::array({source_json(receipt.source), receipt.payload}));
  }

  for (const auto& [key, position] : value.positions) {
    (void)key;
    json["positions"].push_back(Json::array(
        {position.source_id, position.topic, position.partition, position.next_offset}));
  }

  std::size_t outbox_bytes = 0;
  for (const auto& occurrence : value.outbox) {
    EF_ASSIGN_OR_RETURN(auto encoded, occurrence_json(occurrence));
    const auto bytes = encoded.dump().size();
    if (bytes > config.max_occurrence_bytes || bytes > config.max_outbox_bytes - outbox_bytes)
      return full("durable outbox byte budget exceeded");

    outbox_bytes += bytes;
    json["outbox"].push_back(std::move(encoded));
  }

  // An accepted delayed match owns outbox space before its earlier competing path expires.
  // Timeout cleanup can therefore publish that frozen match even when data admission is paused.
  if (reserved_matches > config.max_occurrences - value.outbox.size() ||
      reserved_matches > (config.max_outbox_bytes - outbox_bytes) / config.max_occurrence_bytes)
    return full("CEP delayed matches require reserved durable outbox capacity");
  if (config.requests) {
    json["timer_frontier"] = value.timer_frontier_us;
    json["notice_sequence"] = value.notice_sequence;
    json["deferred_events"] = Json::array();
    json["candidates"] = Json::array();
    json["request_intents"] = Json::array();

    for (const auto& notice : value.notices)
      json["deferred_events"].push_back(
          Json::array({"TtlExpiryNotice", notice.sequence, notice.key, notice.deleted_at_us}));

    std::size_t intent_slots = value.intents.size();
    for (const auto& [key, owned] : value.keys) {
      if (!owned.candidate)
        continue;

      const auto& candidate = *owned.candidate;
      intent_slots += config.requests->level_bindings.size();
      Json audits = Json::array();
      for (const auto& audit : candidate.audits)
        audits.push_back(
            Json::array({audit.skipped_count,
                         audit.last_slot_or_time ? Json(*audit.last_slot_or_time) : Json(nullptr),
                         static_cast<int>(audit.reason)}));

      json["candidates"].push_back(
          Json::array({key, candidate.epoch, candidate.deadline_us, candidate.active,
                       candidate.bindings, candidate.level_dirty, candidate.level_deadlines,
                       candidate.started_at_us, candidate.deadline_caps, std::move(audits)}));
    }

    if (intent_slots > config.requests->max_pending_intents)
      return full("combined occurrence and level intent budget exceeded");

    for (const auto& [id, intent] : value.intents) {
      const auto& admission = intent.admission;
      EF_ASSIGN_OR_RETURN(auto scope, row_json(admission.scope));
      EF_ASSIGN_OR_RETURN(auto arguments, row_json(admission.arguments));
      if (arguments.dump().size() > config.requests->runtime.max_argument_bytes)
        return full("pending admission arguments exceed request budget");

      json["request_intents"].push_back(
          Json::array({id, intent.binding, admission.plan, std::move(scope), admission.request_id,
                       admission.trigger_id, admission.candidate_epoch, admission.lifetime_us,
                       std::move(arguments), intent.deadline_us}));
    }
  }

  auto bytes = json.dump();
  if (bytes.size() > config.max_state_bytes)
    return full("lifecycle snapshot exceeds byte budget");

  EF_RETURN_IF_ERROR(reserve_control(value, std::move(json), reserved_matches));
  return bytes;
}

StatusOr<LifecycleState> EventProcessor::Impl::decode(std::string_view bytes) const noexcept {
  if (bytes.size() > config.max_state_bytes)
    return full("stored lifecycle snapshot exceeds byte budget");

  EF_ASSIGN_OR_RETURN(auto json, parse_json_exact(bytes));
  if (!fields(json, {"format", "fingerprint", "time", "sequence", "generation", "keys", "ttl",
                     "clocks", "receipts", "positions", "outbox"}) ||
      json["format"] != (config.requests ? 2 : 1) || json["fingerprint"] != fingerprint ||
      !json["keys"].is_array() || !json["ttl"].is_array() || !json["clocks"].is_array() ||
      !json["receipts"].is_array() || !json["positions"].is_array() || !json["outbox"].is_array())
    return Status(ErrorCode::FailedPrecondition, "lifecycle snapshot/configuration mismatch");

  LifecycleState result;
  EF_ASSIGN_OR_RETURN(result.time_us, time_number(json["time"]));
  EF_ASSIGN_OR_RETURN(result.sequence, number(json["sequence"]));
  EF_ASSIGN_OR_RETURN(result.generation, number(json["generation"]));
  result.timer_frontier_us = result.time_us;
  if (config.requests) {
    if (!fields(json, {"timer_frontier", "notice_sequence", "deferred_events", "candidates",
                       "request_intents"}) ||
        !json["deferred_events"].is_array() || !json["candidates"].is_array() ||
        !json["request_intents"].is_array())
      return bad("invalid request lifecycle extension");

    EF_ASSIGN_OR_RETURN(result.timer_frontier_us, time_number(json["timer_frontier"]));
    EF_ASSIGN_OR_RETURN(result.notice_sequence, number(json["notice_sequence"]));
    if (result.timer_frontier_us > result.time_us)
      return bad("timer frontier exceeds confirmed owner time");
  }

  for (const auto& entry : json["keys"]) {
    if (!entry.is_array() || entry.size() != (cep_programs.empty() ? 3U : 4U) ||
        !entry[0].is_string() || !entry[2].is_array() || entry[2].size() != programs.size() ||
        (!cep_programs.empty() && (!entry[3].is_array() || entry[3].size() != cep_programs.size())))
      return bad("invalid persisted key state");

    KeyState key;
    EF_ASSIGN_OR_RETURN(key.scope, json_row(entry[1]));
    EF_RETURN_IF_ERROR(validate_scope(key.scope));
    EF_ASSIGN_OR_RETURN(auto encoded_key, scope_key(key.scope));
    if (entry[0] != encoded_key)
      return bad("persisted scope key mismatch");

    for (std::size_t i = 0; i < programs.size(); ++i) {
      if (!entry[2][i].is_string())
        return bad("invalid FSM bytes");

      EF_ASSIGN_OR_RETURN(auto fsm, programs[i]->decode_state(entry[2][i].get<std::string>()));
      if (fsm.time_us > result.time_us ||
          (fsm.deadline_us && *fsm.deadline_us <= result.timer_frontier_us))
        return bad("persisted FSM progress mismatch");

      key.rules.push_back(std::move(fsm));
    }

    for (std::size_t i = 0; i < cep_programs.size(); ++i) {
      if (!entry[3][i].is_string())
        return bad("invalid CEP bytes");

      EF_ASSIGN_OR_RETURN(auto cep, cep_programs[i]->decode_state(entry[3][i].get<std::string>()));
      EF_ASSIGN_OR_RETURN(auto summary, cep_programs[i]->inspect(cep));
      if (summary.time_us > result.time_us ||
          (summary.next_deadline_us && *summary.next_deadline_us <= result.timer_frontier_us))
        return bad("persisted CEP progress mismatch");

      key.cep.push_back(std::move(cep));
    }

    if (!result.keys.emplace(encoded_key, std::move(key)).second)
      return bad("duplicate persisted scope");
  }

  for (const auto& entry : json["ttl"]) {
    if (!entry.is_array() || entry.size() != 5 || !entry[0].is_string())
      return bad("invalid persisted TTL");

    EF_ASSIGN_OR_RETURN(const auto relation_id, number(entry[1]));
    if (relation_id > UINT32_MAX)
      return bad("TTL relation ID overflow");

    TtlState ttl;
    ttl.relation = static_cast<RelationId>(relation_id);
    EF_ASSIGN_OR_RETURN(ttl.identity, json_row(entry[2]));
    EF_ASSIGN_OR_RETURN(ttl.generation, number(entry[3]));
    EF_ASSIGN_OR_RETURN(ttl.deadline, time_number(entry[4]));
    EF_ASSIGN_OR_RETURN(auto relation, catalog.find(ttl.relation));
    if (relation->plan || ttl.identity.size() != relation->identity_types.size() ||
        ttl.generation == 0 || ttl.generation > result.generation || ttl.deadline <= result.time_us)
      return bad("invalid persisted TTL generation or deadline");
    if (std::none_of(
            config.ttl_policies.begin(), config.ttl_policies.end(),
            [&](const TtlPolicy& policy) noexcept { return policy.relation == ttl.relation; }))
      return bad("persisted timer has no declared TTL policy");

    for (std::size_t i = 0; i < ttl.identity.size(); ++i) {
      EF_ASSIGN_OR_RETURN(auto json_value, value_to_json(ttl.identity[i]));
      EF_ASSIGN_OR_RETURN(auto typed,
                          value_from_json(json_value, relation->identity_types[i], false));
      if (typed.index() != ttl.identity[i].index())
        return bad("persisted TTL identity type mismatch");
    }

    Row ttl_scope(ttl.identity.begin(), ttl.identity.begin() + relation->partition_types.size());
    EF_ASSIGN_OR_RETURN(auto ttl_scope_key, scope_key(ttl_scope));
    if (!result.keys.contains(ttl_scope_key))
      return bad("TTL has no owned business scope");

    EF_ASSIGN_OR_RETURN(auto identity_key, scope_key(ttl.identity));
    const auto expected_key = std::to_string(ttl.relation) + ":" + identity_key;
    if (entry[0] != expected_key || !result.ttl.emplace(expected_key, std::move(ttl)).second)
      return bad("TTL identity mismatch");
  }

  for (const auto& entry : json["positions"]) {
    EF_ASSIGN_OR_RETURN(auto source, json_source(entry, true));
    if (!result.positions
             .emplace(source_partition_key(source), ProcessedPosition{.source_id = source.source_id,
                                                                      .topic = source.topic,
                                                                      .partition = source.partition,
                                                                      .next_offset = source.offset})
             .second)
      return bad("duplicate persisted source partition");
  }

  for (const auto& entry : json["clocks"]) {
    if (!entry.is_array() || entry.size() != 6 || !entry[0].is_string() || !entry[2].is_string())
      return bad("invalid periodic state");

    EF_ASSIGN_OR_RETURN(const auto policy_index, number(entry[1]));
    if (policy_index >= config.periodic_policies.size())
      return bad("invalid periodic policy index");

    ClockState clock{.policy = static_cast<std::size_t>(policy_index),
                     .key = entry[2].get<std::string>(),
                     .next_us = 0,
                     .last_us = {},
                     .skipped = 0};
    EF_ASSIGN_OR_RETURN(clock.next_us, time_number(entry[3]));
    if (!entry[4].is_null()) {
      EF_ASSIGN_OR_RETURN(auto last, time_number(entry[4]));
      clock.last_us = last;
    }

    EF_ASSIGN_OR_RETURN(clock.skipped, number(entry[5]));
    const auto& policy = config.periodic_policies[clock.policy];
    const auto expected = Json::array({policy.id, clock.key}).dump();
    if (!result.keys.contains(clock.key) || entry[0] != expected ||
        clock.next_us <= result.timer_frontier_us || clock.next_us < policy.phase_us ||
        (clock.next_us - policy.phase_us) % policy.interval_us != 0 ||
        (clock.last_us && *clock.last_us > result.time_us) ||
        !result.clocks.emplace(expected, clock).second)
      return bad("periodic slot or key mismatch");
  }

  if (result.clocks.size() != result.keys.size() * config.periodic_policies.size())
    return bad("persisted periodic schedule is incomplete");

  std::map<std::string, std::int64_t> receipt_offsets;
  for (const auto& entry : json["receipts"]) {
    if (!entry.is_array() || entry.size() != 2 || !entry[1].is_string())
      return bad("invalid dedup receipt");

    EF_ASSIGN_OR_RETURN(auto source, json_source(entry[0]));
    const auto position = result.positions.find(source_partition_key(source));
    if (position == result.positions.end() || position->second.next_offset <= source.offset)
      return bad("dedup receipt exceeds progress");

    const auto partition_key = source_partition_key(source);
    const auto prior = receipt_offsets.find(partition_key);
    if (prior != receipt_offsets.end() && prior->second >= source.offset)
      return bad("dedup receipt order is invalid");

    receipt_offsets.insert_or_assign(partition_key, source.offset);
    result.receipts.push_back(
        {.source = std::move(source), .payload = entry[1].get<std::string>()});
  }

  std::set<std::string> occurrence_ids;
  for (const auto& entry : json["outbox"]) {
    EF_ASSIGN_OR_RETURN(auto occurrence, json_occurrence(entry));
    EF_RETURN_IF_ERROR(validate_scope(occurrence.scope));
    if (occurrence.emission.time_us > occurrence.evaluated_at_us ||
        occurrence.evaluated_at_us > result.time_us || !occurrence_ids.insert(occurrence.id).second)
      return bad("invalid outbox time or duplicate identity");

    EF_ASSIGN_OR_RETURN(auto key, scope_key(occurrence.scope));
    const auto owner = result.keys.find(key);
    if (owner == result.keys.end())
      return bad("outbox occurrence has no owned scope");

    const std::map<std::string, RelationId>* bindings = nullptr;
    if (occurrence.cep_match) {
      const auto rule = std::find_if(config.cep_rules.begin(), config.cep_rules.end(),
                                     [&](const ProcessorCepRule& candidate) noexcept {
                                       return candidate.definition.id == occurrence.rule_id;
                                     });
      if (rule == config.cep_rules.end() || rule->definition.version != occurrence.rule_version)
        return bad("outbox CEP rule/version mismatch");

      bindings = &rule->reads;
      const auto& match = *occurrence.cep_match;
      Json identity = Json::array();
      for (const auto& capture : match.events)
        identity.push_back(Json::array({capture.label, capture.event.id}));

      const auto expected_match =
          Json::array({occurrence.rule_id, occurrence.rule_version, identity}).dump();
      const auto expected = Json::array({config.namespace_id, key, "cep", expected_match}).dump();
      if (match.id != expected_match || occurrence.id != expected ||
          occurrence.emission.name != "match" || occurrence.emission.cause != match.id ||
          occurrence.emission.transition_sequence == 0 ||
          occurrence.emission.transition_sequence > result.sequence ||
          occurrence.emission.time_us != match.matched_at_us ||
          occurrence.evaluated_at_us != match.emitted_at_us ||
          occurrence.emission.scheduled_time_us || occurrence.reads != match.reads ||
          match.events.size() > rule->definition.limits.max_events_per_match ||
          match.registers.size() != rule->definition.register_fields.size() ||
          match.events.back().event.time_us != match.matched_at_us ||
          match.matched_at_us - match.events.front().event.time_us >= rule->definition.within_us)
        return bad("outbox CEP match identity, witness or time mismatch");

      for (std::size_t i = 0; i < match.registers.size(); ++i) {
        EF_ASSIGN_OR_RETURN(auto encoded, value_to_json(match.registers[i]));
        const auto& field = rule->definition.register_fields[i];
        EF_ASSIGN_OR_RETURN(auto typed, value_from_json(encoded, field.type, field.nullable));
        if (typed.index() != match.registers[i].index())
          return bad("outbox CEP register type mismatch");
      }
    } else {
      const auto rule = std::find_if(config.rules.begin(), config.rules.end(),
                                     [&](const ProcessorRule& candidate) noexcept {
                                       return candidate.definition.id == occurrence.rule_id;
                                     });
      if (rule == config.rules.end() || rule->definition.version != occurrence.rule_version)
        return bad("outbox rule or version mismatch");

      bindings = &rule->reads;
      const auto expected_id =
          Json::array({config.namespace_id, key, occurrence.rule_id, occurrence.rule_version,
                       occurrence.emission.transition_sequence})
              .dump();
      const auto rule_index = static_cast<std::size_t>(rule - config.rules.begin());
      if (occurrence.id != expected_id || occurrence.emission.transition_sequence == 0 ||
          occurrence.emission.transition_sequence >
              owner->second.rules[rule_index].transition_sequence)
        return bad("outbox occurrence identity mismatch");
    }

    if (bindings->size() != occurrence.reads.size())
      return bad("outbox declared read count mismatch");

    for (const auto& [alias, relation_id] : *bindings) {
      const auto witness = occurrence.reads.find(alias);
      if (witness == occurrence.reads.end())
        return bad("missing occurrence read witness");
      if (!witness->second)
        continue;

      EF_ASSIGN_OR_RETURN(auto relation, catalog.find(relation_id));
      if (witness->second->size() != relation->visible_count)
        return bad("occurrence witness arity mismatch");

      for (std::size_t i = 0; i < relation->visible_count; ++i) {
        EF_ASSIGN_OR_RETURN(auto encoded_value, value_to_json((*witness->second)[i]));
        const auto& field = relation->storage_schema.fields[i];
        EF_ASSIGN_OR_RETURN(auto typed, value_from_json(encoded_value, field.type, field.nullable));
        if (typed.index() != (*witness->second)[i].index())
          return bad("occurrence witness type mismatch");
      }
    }

    result.outbox.push_back(std::move(occurrence));
  }

  if (config.requests) {
    const auto& request = *config.requests;
    std::uint64_t previous_sequence = 0;
    for (const auto& entry : json["deferred_events"]) {
      if (!entry.is_array() || entry.size() != 4 || entry[0] != "TtlExpiryNotice" ||
          !entry[2].is_string())
        return bad("unknown or malformed deferred lifecycle event");

      ExpiryNotice notice;
      EF_ASSIGN_OR_RETURN(notice.sequence, number(entry[1]));
      notice.key = entry[2].get<std::string>();
      EF_ASSIGN_OR_RETURN(notice.deleted_at_us, time_number(entry[3]));
      if (notice.sequence <= previous_sequence || notice.sequence > result.notice_sequence ||
          !result.keys.contains(notice.key) || notice.deleted_at_us > result.time_us ||
          notice.deleted_at_us <= result.timer_frontier_us)
        return bad("invalid deferred expiry order or progress");

      previous_sequence = notice.sequence;
      result.notices.push_back(std::move(notice));
    }

    for (const auto& entry : json["candidates"]) {
      if (!entry.is_array() || entry.size() != 10 || !entry[0].is_string() ||
          !entry[3].is_boolean() || !entry[4].is_array() || !entry[5].is_array() ||
          !entry[6].is_array() || !entry[8].is_array() || !entry[9].is_array())
        return bad("invalid durable candidate state");

      const auto key = entry[0].get<std::string>();
      auto owner = result.keys.find(key);
      if (owner == result.keys.end() || owner->second.candidate)
        return bad("candidate scope missing or duplicated");

      CandidateState candidate;
      EF_ASSIGN_OR_RETURN(candidate.epoch, time_number(entry[1]));
      EF_ASSIGN_OR_RETURN(candidate.deadline_us, time_number(entry[2]));
      candidate.active = entry[3].get<bool>();
      if (candidate.epoch == 0 || (candidate.active && candidate.deadline_us <= result.time_us) ||
          entry[4].size() != request.trigger_bindings.size() + request.level_bindings.size() ||
          entry[5].size() != request.level_bindings.size() || entry[6].size() != entry[5].size())
        return bad("candidate expiry or reserved binding slots mismatch");

      for (const auto& value : entry[4]) {
        EF_ASSIGN_OR_RETURN(auto status, number(value));
        if (status > 2)
          return bad("unknown candidate binding disposition");

        candidate.bindings.push_back(static_cast<std::uint8_t>(status));
      }

      for (const auto& value : entry[5]) {
        if (!value.is_boolean())
          return bad("invalid level dirty flag");

        candidate.level_dirty.push_back(value.get<bool>());
      }

      for (const auto& value : entry[6]) {
        EF_ASSIGN_OR_RETURN(auto deadline, time_number(value));
        candidate.level_deadlines.push_back(deadline);
      }

      EF_ASSIGN_OR_RETURN(candidate.started_at_us, time_number(entry[7]));
      if (candidate.started_at_us > result.time_us ||
          candidate.started_at_us >= candidate.deadline_us ||
          entry[8].size() != request.trigger_bindings.size() ||
          entry[9].size() != candidate.bindings.size())
        return bad("candidate start time or audit reservation mismatch");

      for (const auto& value : entry[8]) {
        EF_ASSIGN_OR_RETURN(auto deadline, time_number(value));
        if (deadline > candidate.deadline_us)
          return bad("pending cap exceeds candidate expiry");

        candidate.deadline_caps.push_back(deadline);
      }

      for (const auto& value : entry[9]) {
        if (!value.is_array() || value.size() != 3)
          return bad("invalid candidate binding audit");

        BindingAudit audit;
        EF_ASSIGN_OR_RETURN(audit.skipped_count, number(value[0]));
        if (!value[1].is_null()) {
          EF_ASSIGN_OR_RETURN(auto time, time_number(value[1]));
          if (time < candidate.started_at_us || time > result.time_us)
            return bad("invalid binding audit time");

          audit.last_slot_or_time = time;
        }

        EF_ASSIGN_OR_RETURN(auto reason, number(value[2]));
        if (reason > static_cast<unsigned>(RequestPendingReason::OnceConsumed))
          return bad("unknown candidate audit reason");

        audit.reason = static_cast<RequestPendingReason>(reason);
        const auto slot = candidate.audits.size();
        const bool can_skip =
            slot < request.trigger_bindings.size() &&
            request.trigger_bindings[slot].mode == RequestTriggerMode::TryOnceAtFiring;
        if ((!can_skip && audit.skipped_count != 0) ||
            (!audit.last_slot_or_time &&
             (audit.skipped_count != 0 || reason != 0 || candidate.bindings[slot] == 1)))
          return bad("binding audit violates its configured lifecycle mode");

        candidate.audits.push_back(audit);
      }

      owner->second.candidate = std::move(candidate);
    }

    for (const auto& entry : json["request_intents"]) {
      if (!entry.is_array() || entry.size() != 10 || !entry[0].is_string() ||
          !entry[2].is_string() || !entry[4].is_string() || !entry[5].is_string())
        return bad("invalid pending request intent");

      const auto id = entry[0].get<std::string>();
      PendingRequestIntent intent;
      EF_ASSIGN_OR_RETURN(auto binding, number(entry[1]));
      if (binding >= request.trigger_bindings.size())
        return bad("unknown intent binding");

      intent.binding = static_cast<std::size_t>(binding);
      auto& admission = intent.admission;
      admission.plan = entry[2].get<std::string>();
      EF_ASSIGN_OR_RETURN(admission.scope, json_row(entry[3]));
      admission.request_id = entry[4].get<std::string>();
      admission.trigger_id = entry[5].get<std::string>();
      EF_ASSIGN_OR_RETURN(admission.candidate_epoch, number(entry[6]));
      EF_ASSIGN_OR_RETURN(admission.lifetime_us, time_number(entry[7]));
      EF_ASSIGN_OR_RETURN(admission.arguments, json_row(entry[8]));
      EF_ASSIGN_OR_RETURN(intent.deadline_us, time_number(entry[9]));
      EF_ASSIGN_OR_RETURN(auto owner_key, scope_key(admission.scope));
      auto candidate_owner = result.keys.find(owner_key);
      if (candidate_owner == result.keys.end() || !candidate_owner->second.candidate ||
          admission.candidate_epoch >
              static_cast<std::uint64_t>(candidate_owner->second.candidate->epoch))
        return bad("intent candidate epoch is outside the durable owner fence");

      const auto& configured = request.trigger_bindings[intent.binding];
      const auto expected_id = Json::array({configured.id, configured.version, id}).dump();
      auto occurrence =
          std::find_if(result.outbox.begin(), result.outbox.end(),
                       [&](const TriggerOccurrence& value) noexcept { return value.id == id; });
      if (occurrence != result.outbox.end() &&
          admission.candidate_epoch ==
              static_cast<std::uint64_t>(candidate_owner->second.candidate->epoch)) {
        if (occurrence->evaluated_at_us > INT64_MAX - configured.pending_lifetime_us ||
            intent.deadline_us !=
                std::min(candidate_owner->second.candidate->deadline_caps[intent.binding],
                         occurrence->evaluated_at_us + configured.pending_lifetime_us))
          return bad("pending intent does not match its absolute candidate cap");
      }

      if (occurrence == result.outbox.end() || occurrence->scope != admission.scope ||
          occurrence->rule_id != configured.rule_id ||
          occurrence->emission.name != configured.emission_name ||
          admission.plan != configured.plan || admission.request_id != expected_id ||
          admission.trigger_id != id || admission.candidate_epoch == 0 ||
          intent.deadline_us <= occurrence->evaluated_at_us ||
          !result.intents.emplace(id, std::move(intent)).second)
        return bad("pending intent does not match its immutable occurrence");
    }

    for (const auto& occurrence : result.outbox)
      for (const auto& binding : request.trigger_bindings)
        if (binding.rule_id == occurrence.rule_id &&
            binding.emission_name == occurrence.emission.name &&
            !result.intents.contains(occurrence.id))
          return bad("bound occurrence has no admission intent");
  }

  EF_ASSIGN_OR_RETURN(auto encoded, encode(result));
  (void)encoded;
  return result;
}

StatusOr<std::string>
EventProcessor::Impl::event_payload(const ProcessorEvent& event) const noexcept {
  EF_RETURN_IF_ERROR(validate_source(event.source));
  EF_RETURN_IF_ERROR(validate_scope(event.scope));
  if (!text_ok(event.kind, 128) || event.kind == "*" || event.kind == "$timeout" ||
      event.kind == "$expire" || event.processing_time_us < 0)
    return bad("invalid event kind or processing time");

  EF_ASSIGN_OR_RETURN(auto scope, row_json(event.scope));
  EF_ASSIGN_OR_RETURN(auto values, row_json(event.values));
  Json payload{{"scope", std::move(scope)},
               {"kind", event.kind},
               {"values", std::move(values)},
               {"changes", Json::array()}};
  if (event.changes.size() > engine_config.max_rows)
    return full("event has too many relation changes");

  for (const auto& change : event.changes) {
    if (std::find(config.input_relations.begin(), config.input_relations.end(), change.relation) ==
        config.input_relations.end())
      return bad("event may mutate only declared input tables");

    EF_ASSIGN_OR_RETURN(auto relation, catalog.find(change.relation));
    if (change.identity.size() != relation->identity_types.size())
      return bad("incomplete event row identity");

    Schema identity_schema;
    for (std::size_t i = 0; i < relation->identity_types.size(); ++i) {
      identity_schema.fields.push_back({.id = static_cast<std::uint32_t>(i),
                                        .name = "id_" + std::to_string(i),
                                        .type = relation->identity_types[i],
                                        .nullable = false});
      identity_schema.primary_key.push_back(identity_schema.fields.back().name);
      identity_schema.partition_key.push_back(identity_schema.fields.back().name);
    }

    EF_RETURN_IF_ERROR(identity_schema.validate(change.identity));
    Row row_scope(change.identity.begin(),
                  change.identity.begin() + relation->partition_types.size());
    if (row_scope != event.scope)
      return bad("event changes must all belong to the declared business key");

    Json row = nullptr;
    if (change.row) {
      EF_RETURN_IF_ERROR(relation->storage_schema.validate(*change.row));
      EF_ASSIGN_OR_RETURN(auto identity, physical_identity(*relation, *change.row));
      if (identity != change.identity)
        return bad("event upsert identity mismatch");

      EF_ASSIGN_OR_RETURN(row, row_json(*change.row));
    }

    EF_ASSIGN_OR_RETURN(auto identity, row_json(change.identity));
    payload["changes"].push_back(
        Json::array({change.relation, std::move(identity), std::move(row)}));
  }

  // Admission time is not broker payload: a later redelivery remains the same durable event.
  auto encoded = payload.dump();
  if (encoded.size() > config.max_event_bytes)
    return full("event exceeds exact dedup payload budget");

  return encoded;
}

StatusOr<FsmReads>
EventProcessor::Impl::read_views(RoundContext& round,
                                 const std::map<std::string, RelationId>& bindings,
                                 const Row& scope) const noexcept {
  FsmReads reads;
  for (const auto& [alias, id] : bindings) {
    EF_ASSIGN_OR_RETURN(auto stored, round.read_relation(id, scope));
    if (!stored)
      reads.emplace(alias, std::nullopt);
    else {
      EF_ASSIGN_OR_RETURN(auto relation, catalog.find(id));
      EF_ASSIGN_OR_RETURN(auto visible, visible_row(*relation, *stored));
      reads.emplace(alias, std::move(visible));
    }
  }

  return reads;
}

Status EventProcessor::Impl::write_audit(
    RoundContext& round, const LifecycleState& next,
    const std::vector<TriggerOccurrence>& occurrences) const noexcept {
  if (!config.trigger_audit.relation || occurrences.empty())
    return {};
  if (round.revision() == UINT64_MAX)
    return full("trigger audit revision overflow");

  EF_ASSIGN_OR_RETURN(auto relation, catalog.find(config.trigger_audit.relation));
  std::vector<Row> rows;
  rows.reserve(occurrences.size());
  for (const auto& occurrence : occurrences) {
    Row row = occurrence.scope;
    row.resize(occurrence.scope.size() +
               static_cast<std::size_t>(TriggerAuditColumn::SourceOffset) + 1);
    const auto set = [&](TriggerAuditColumn column, Value value) noexcept {
      row[occurrence.scope.size() + static_cast<std::size_t>(column)] = std::move(value);
    };
    set(TriggerAuditColumn::OccurrenceId, occurrence.id);
    set(TriggerAuditColumn::RuleId, occurrence.rule_id);
    set(TriggerAuditColumn::RuleVersion, std::to_string(occurrence.rule_version));
    set(TriggerAuditColumn::Emission, occurrence.emission.name);
    set(TriggerAuditColumn::EventKind, occurrence.event_kind);
    set(TriggerAuditColumn::OccurredAtUs, occurrence.emission.time_us);
    set(TriggerAuditColumn::EvaluatedAtUs, occurrence.evaluated_at_us);
    if (occurrence.emission.scheduled_time_us)
      set(TriggerAuditColumn::ScheduledAtUs, *occurrence.emission.scheduled_time_us);

    set(TriggerAuditColumn::SkippedSlots, std::to_string(occurrence.skipped_slots));
    set(TriggerAuditColumn::FiringRevision, std::to_string(round.revision() + 1));
    const auto intent = next.intents.find(occurrence.id);
    if (intent != next.intents.end()) {
      const auto& pending = intent->second;
      const auto& binding = config.requests->trigger_bindings[pending.binding];
      set(TriggerAuditColumn::RequestId, pending.admission.request_id);
      set(TriggerAuditColumn::BindingId, binding.id);
      set(TriggerAuditColumn::Plan, pending.admission.plan);
      set(TriggerAuditColumn::CandidateEpoch, std::to_string(pending.admission.candidate_epoch));
      set(TriggerAuditColumn::RequestMode,
          std::string(binding.mode == RequestTriggerMode::Wait ? "WAIT" : "TRY_ONCE_AT_FIRING"));
      set(TriggerAuditColumn::IntentDeadlineUs, pending.deadline_us);
    }

    if (occurrence.source) {
      set(TriggerAuditColumn::SourceId, occurrence.source->source_id);
      set(TriggerAuditColumn::SourceTopic, occurrence.source->topic);
      set(TriggerAuditColumn::SourcePartition,
          static_cast<std::int64_t>(occurrence.source->partition));
      set(TriggerAuditColumn::SourceOffset, occurrence.source->offset);
    }

    rows.push_back(std::move(row));
  }

  return write_trigger_audit_rows(round, *relation, config.trigger_audit, rows,
                                  config.max_occurrence_bytes);
}

Status EventProcessor::Impl::emit(RoundContext& round, LifecycleState& next, const Row& scope,
                                  std::size_t rule_index, const FsmStep& step,
                                  const FsmReads& reads,
                                  const std::optional<SourceIdentity>& source,
                                  std::vector<TriggerOccurrence>& occurrences,
                                  std::string_view event_kind, const Row& event_values,
                                  std::uint64_t skipped_slots) noexcept {
  const auto& rule = config.rules[rule_index];
  EF_ASSIGN_OR_RETURN(auto key, scope_key(scope));

  for (const auto& emission : step.emissions) {
    TriggerOccurrence occurrence{
        .id = Json::array({config.namespace_id, key, rule.definition.id, rule.definition.version,
                           emission.transition_sequence})
                  .dump(),
        .scope = scope,
        .rule_id = rule.definition.id,
        .rule_version = rule.definition.version,
        .emission = emission,
        .reads = reads,
        .source = source,
        .event_kind = std::string(event_kind),
        .event_values = event_values,
        .skipped_slots = skipped_slots,
        .evaluated_at_us = round.processing_time_us(),
        .cep_match = {}};
    EF_ASSIGN_OR_RETURN(auto retain, freeze_intent(round, next, occurrence));
    if (!retain)
      continue;

    EF_ASSIGN_OR_RETURN(auto encoded, occurrence_json(occurrence));
    if (encoded.dump().size() > config.max_occurrence_bytes)
      return full("occurrence witness exceeds byte budget");
    if (rule.sink) {
      EF_ASSIGN_OR_RETURN(auto row, rule.sink->make_row(occurrence));
      EF_ASSIGN_OR_RETURN(auto relation, catalog.find(rule.sink->relation));
      EF_RETURN_IF_ERROR(relation->storage_schema.validate(row));
      if (config.requests) {
        EF_ASSIGN_OR_RETURN(auto encoded_row, encode_row(relation->storage_schema, row));
        if (encoded_row.size() > config.max_event_bytes)
          return full("occurrence sink exceeds its declared relation byte envelope");
      }

      EF_ASSIGN_OR_RETURN(auto row_scope, partition_values(*relation, row));
      if (row_scope != scope)
        return bad("occurrence factory changed the business scope");

      EF_ASSIGN_OR_RETURN(auto identity, physical_identity(*relation, row));
      EF_RETURN_IF_ERROR(round.emit({.relation = relation->id,
                                     .identity = std::move(identity),
                                     .old_row = {},
                                     .new_row = std::move(row)}));
    }

    next.outbox.push_back(occurrence);
    occurrences.push_back(std::move(occurrence));
  }

  return {};
}

Status EventProcessor::Impl::emit_cep(RoundContext& round, LifecycleState& next, const Row& scope,
                                      std::size_t rule_index, const CepStep& step,
                                      const std::optional<SourceIdentity>& source,
                                      std::vector<TriggerOccurrence>& occurrences,
                                      std::string_view event_kind, const Row& event_values,
                                      std::uint64_t skipped_slots) noexcept {
  const auto& rule = config.cep_rules[rule_index];
  EF_ASSIGN_OR_RETURN(auto key, scope_key(scope));

  for (const auto& match : step.matches) {
    TriggerOccurrence occurrence{
        .id = Json::array({config.namespace_id, key, "cep", match.id}).dump(),
        .scope = scope,
        .rule_id = rule.definition.id,
        .rule_version = rule.definition.version,
        .emission = {.name = "match",
                     .cause = match.id,
                     .transition_sequence = next.sequence,
                     .time_us = match.matched_at_us,
                     .scheduled_time_us = {}},
        .reads = match.reads,
        .source = source,
        .event_kind = std::string(event_kind),
        .event_values = event_values,
        .skipped_slots = skipped_slots,
        .evaluated_at_us = round.processing_time_us(),
        .cep_match = match};
    EF_ASSIGN_OR_RETURN(auto retain, freeze_intent(round, next, occurrence));
    if (!retain)
      continue;

    EF_ASSIGN_OR_RETURN(auto encoded, occurrence_json(occurrence));
    if (encoded.dump().size() > config.max_occurrence_bytes)
      return full("CEP occurrence witness exceeds byte budget");
    if (rule.sink) {
      EF_ASSIGN_OR_RETURN(auto row, rule.sink->make_row(occurrence));
      EF_ASSIGN_OR_RETURN(auto relation, catalog.find(rule.sink->relation));
      EF_RETURN_IF_ERROR(relation->storage_schema.validate(row));
      if (config.requests) {
        EF_ASSIGN_OR_RETURN(auto encoded_row, encode_row(relation->storage_schema, row));
        if (encoded_row.size() > config.max_event_bytes)
          return full("occurrence sink exceeds its declared relation byte envelope");
      }

      EF_ASSIGN_OR_RETURN(auto row_scope, partition_values(*relation, row));
      if (row_scope != scope)
        return bad("CEP occurrence factory changed the business scope");

      EF_ASSIGN_OR_RETURN(auto identity, physical_identity(*relation, row));
      EF_RETURN_IF_ERROR(round.emit({.relation = relation->id,
                                     .identity = std::move(identity),
                                     .old_row = {},
                                     .new_row = std::move(row)}));
    }

    next.outbox.push_back(occurrence);
    occurrences.push_back(std::move(occurrence));
  }

  return {};
}

std::optional<std::int64_t>
EventProcessor::Impl::next_deadline(std::int64_t target) const noexcept {
  std::optional<std::int64_t> next;
  for (const auto& [key, timer] : state.ttl) {
    (void)key;
    if (!next || timer.deadline < *next)
      next = timer.deadline;
  }

  for (const auto& [key, value] : state.keys) {
    (void)key;
    for (const auto& fsm : value.rules)
      if (fsm.deadline_us && (!next || *fsm.deadline_us < *next))
        next = fsm.deadline_us;

    for (std::size_t i = 0; i < value.cep.size(); ++i) {
      const auto summary = cep_programs[i]->inspect(value.cep[i]);

      // Owned states originate only from successful steps or validated persisted snapshots.
      if (summary.ok() && summary->next_deadline_us &&
          (!next || *summary->next_deadline_us < *next))
        next = summary->next_deadline_us;
    }
  }

  for (const auto& [id, clock] : state.clocks) {
    (void)id;
    if (clock.next_us > target)
      continue;

    const auto interval = config.periodic_policies[clock.policy].interval_us;
    const auto at = clock.next_us + ((target - clock.next_us) / interval) * interval;
    if (!next || at < *next)
      next = at;
  }

  return next;
}

Status EventProcessor::Impl::reserve_outbox(std::size_t count) const noexcept {
  if (count > config.max_occurrences - state.outbox.size())
    return full("consume pending occurrences before accepting more triggers");

  std::size_t used = 0;
  for (const auto& occurrence : state.outbox) {
    EF_ASSIGN_OR_RETURN(auto json, occurrence_json(occurrence));
    used += json.dump().size();
  }

  if (count > (config.max_outbox_bytes - used) / config.max_occurrence_bytes)
    return full("outbox byte reservation unavailable; consume pending occurrences");

  return {};
}

Status EventProcessor::Impl::data_candidate(LifecycleState& next,
                                            const Operation& operation) const noexcept {
  const auto& event = operation.event;
  EF_ASSIGN_OR_RETURN(auto key, scope_key(event.scope));
  auto found = next.keys.find(key);
  if (found == next.keys.end()) {
    if (next.keys.size() >= config.max_keys)
      return full("business key state budget exceeded");

    KeyState value{.scope = event.scope, .rules = {}, .cep = {}};
    for (const auto& program : programs) {
      EF_ASSIGN_OR_RETURN(auto initial, program->initial_state(event.processing_time_us));
      value.rules.push_back(std::move(initial));
    }

    for (const auto& program : cep_programs) {
      EF_ASSIGN_OR_RETURN(auto initial, program->initial_state(event.processing_time_us));
      value.cep.push_back(std::move(initial));
    }

    found = next.keys.emplace(key, std::move(value)).first;
    for (std::size_t i = 0; i < config.periodic_policies.size(); ++i) {
      const auto& policy = config.periodic_policies[i];
      auto next_slot = policy.phase_us;
      if (next_slot <= event.processing_time_us) {
        const auto steps = (event.processing_time_us - next_slot) / policy.interval_us;
        const auto last = next_slot + steps * policy.interval_us;
        if (last > INT64_MAX - policy.interval_us)
          return full("next periodic slot would overflow");

        next_slot = last + policy.interval_us;
      }

      next.clocks.emplace(
          Json::array({policy.id, key}).dump(),
          ClockState{.policy = i, .key = key, .next_us = next_slot, .last_us = {}, .skipped = 0});
    }
  }

  if (next.sequence == UINT64_MAX)
    return full("input admission sequence exhausted");

  ++next.sequence;
  for (const auto& change : event.changes) {
    const auto policy = std::find_if(
        config.ttl_policies.begin(), config.ttl_policies.end(),
        [&](const TtlPolicy& ttl) noexcept { return ttl.relation == change.relation; });
    if (policy == config.ttl_policies.end())
      continue;

    EF_ASSIGN_OR_RETURN(auto identity, scope_key(change.identity));
    auto timer_key = std::to_string(change.relation) + ":" + identity;
    if (!change.row)
      next.ttl.erase(timer_key);
    else {
      if (next.generation == UINT64_MAX || event.processing_time_us > INT64_MAX - policy->ttl_us)
        return full("TTL generation or deadline exhausted");

      next.ttl.insert_or_assign(timer_key,
                                TtlState{.relation = change.relation,
                                         .identity = change.identity,
                                         .generation = ++next.generation,
                                         .deadline = event.processing_time_us + policy->ttl_us});
    }
  }

  // Reserve a possible timed state for every program of this key before entering Engine.
  std::size_t timers = next.ttl.size() + next.clocks.size() + programs.size();
  for (const auto& [other_key, value] : next.keys)
    if (other_key != key)
      for (const auto& fsm : value.rules)
        timers += fsm.deadline_us.has_value();

  if (timers > config.max_timers)
    return full("timer capacity reservation unavailable");

  const auto partition = source_partition_key(event.source);
  if (!next.positions.contains(partition) && next.positions.size() == config.max_source_partitions)
    return full("source partition budget exceeded");

  next.positions.insert_or_assign(partition,
                                  ProcessedPosition{.source_id = event.source.source_id,
                                                    .topic = event.source.topic,
                                                    .partition = event.source.partition,
                                                    .next_offset = event.source.offset + 1});
  next.receipts.push_back({.source = event.source, .payload = operation.payload});
  std::size_t bytes = 0;

  for (const auto& receipt : next.receipts)
    bytes += receipt.payload.size();

  while (next.receipts.size() > config.max_dedup_entries || bytes > config.max_dedup_bytes) {
    bytes -= next.receipts.front().payload.size();
    next.receipts.erase(next.receipts.begin());
  }

  return {};
}

Status EventProcessor::Impl::round(Operation& operation, std::int64_t at, bool final,
                                   ProcessResult& result) noexcept {
  auto next = std::make_unique<LifecycleState>(state);
  next->time_us = at;
  const bool data = final && operation.kind == OperationKind::Data;
  const bool ack = operation.kind == OperationKind::Acknowledge;
  std::vector<TableChange> inputs;
  std::set<std::string> expired_scopes;
  std::vector<ClockFiring> clock_firings;
  std::vector<std::pair<std::size_t, ClockFiring>> scheduled_clocks;
  std::size_t emission_capacity = 0;
  if (data) {
    if (config.requests) {
      const auto extra = try_once_capacity(operation.event.kind);
      const auto available = config.max_rounds_per_call -
                             std::min(result.committed_rounds.size(), config.max_rounds_per_call);
      if (extra >= available)
        return full("source requires reserved try-once admission rounds");
    }

    EF_RETURN_IF_ERROR(data_candidate(*next, operation));
    inputs = operation.event.changes;
    for (const auto& rule : config.rules)
      if (std::any_of(rule.definition.transitions.begin(), rule.definition.transitions.end(),
                      [&](const FsmTransition& transition) noexcept {
                        return !transition.emit.empty() &&
                               (transition.event == operation.event.kind ||
                                transition.event == "*");
                      }))
        ++emission_capacity;
  } else if (!ack) {
    for (auto it = next->ttl.begin(); it != next->ttl.end();) {
      if (it->second.deadline > at) {
        ++it;
        continue;
      }

      // The current persisted generation is authoritative; replacement overwrites the old timer.
      const auto live = state.ttl.find(it->first);
      if (live != state.ttl.end() && live->second.generation == it->second.generation) {
        inputs.push_back({.relation = it->second.relation,
                          .identity = it->second.identity,
                          .row = std::nullopt});
        EF_ASSIGN_OR_RETURN(auto relation, catalog.find(it->second.relation));
        Row scope(it->second.identity.begin(),
                  it->second.identity.begin() + relation->partition_types.size());
        EF_ASSIGN_OR_RETURN(auto scope_id, scope_key(scope));
        expired_scopes.insert(std::move(scope_id));
      }

      it = next->ttl.erase(it);
    }

    for (const auto& [key, value] : state.keys) {
      for (std::size_t i = 0; i < value.rules.size(); ++i) {
        if (value.rules[i].deadline_us && *value.rules[i].deadline_us <= at &&
            std::any_of(config.rules[i].definition.transitions.begin(),
                        config.rules[i].definition.transitions.end(),
                        [](const FsmTransition& transition) noexcept {
                          return transition.event == "$timeout" && !transition.emit.empty();
                        })) {
          // One current timeout transition; any newly entered relative wait starts at now.
          ++emission_capacity;
        }

        if (expired_scopes.contains(key) &&
            std::any_of(config.rules[i].definition.transitions.begin(),
                        config.rules[i].definition.transitions.end(),
                        [](const FsmTransition& transition) noexcept {
                          return !transition.emit.empty() &&
                                 (transition.event == "$expire" || transition.event == "*");
                        }))
          ++emission_capacity;
      }
    }

    for (auto& [id, clock] : next->clocks) {
      (void)id;
      if (clock.next_us > operation.target_us)
        continue;

      const auto& policy = config.periodic_policies[clock.policy];
      const auto skipped = (operation.target_us - clock.next_us) / policy.interval_us;
      const auto slot = clock.next_us + skipped * policy.interval_us;
      if (slot > at)
        continue;
      if (slot > INT64_MAX - policy.interval_us)
        return full("next periodic slot would overflow");

      ClockFiring firing{.policy_id = policy.id,
                         .scope = next->keys.find(clock.key)->second.scope,
                         .slot_us = slot,
                         .skipped_slots = static_cast<std::uint64_t>(skipped)};
      clock.next_us = slot + policy.interval_us;
      clock.last_us = slot;
      clock.skipped = firing.skipped_slots;
      scheduled_clocks.emplace_back(clock.policy, firing);
      clock_firings.push_back(std::move(firing));

      for (const auto& rule : config.rules)
        if (std::any_of(rule.definition.transitions.begin(), rule.definition.transitions.end(),
                        [&](const FsmTransition& transition) noexcept {
                          return !transition.emit.empty() &&
                                 (transition.event == policy.event_kind || transition.event == "*");
                        }))
          ++emission_capacity;
    }
  } else {
    std::erase_if(next->outbox, [&](const TriggerOccurrence& occurrence) noexcept {
      return std::find(operation.ids.begin(), operation.ids.end(), occurrence.id) !=
             operation.ids.end();
    });
  }

  EF_RETURN_IF_ERROR(reserve_outbox(emission_capacity));
  std::vector<TriggerOccurrence> occurrences;
  const auto hook = [&](RoundContext& context, const std::vector<RowChange>&) noexcept -> Status {
    if (!ack) {
      // Freeze every declared read before emitting any capture row. Reading an occurrence-derived
      // MV in a later round is allowed, but no rule observes a half-stabilized same-round capture.
      std::map<std::string, std::vector<FsmReads>> captured;
      std::map<std::string, std::vector<FsmReads>> captured_cep;
      std::size_t captured_bytes = 0;

      for (const auto& [key, value] : next->keys) {
        if (data && value.scope != operation.event.scope)
          continue;

        auto& all_reads = captured[key];
        for (std::size_t i = 0; i < value.rules.size(); ++i) {
          EF_ASSIGN_OR_RETURN(auto reads, read_views(context, config.rules[i].reads, value.scope));
          EF_ASSIGN_OR_RETURN(auto json, reads_json(reads));
          const auto bytes = json.dump().size();
          if (bytes > config.max_state_bytes - captured_bytes)
            return full("round declared-read budget exceeded");

          captured_bytes += bytes;
          all_reads.push_back(std::move(reads));
        }

        auto& cep_reads = captured_cep[key];
        for (const auto& rule : config.cep_rules) {
          EF_ASSIGN_OR_RETURN(auto reads, read_views(context, rule.reads, value.scope));
          EF_ASSIGN_OR_RETURN(auto json, reads_json(reads));
          const auto bytes = json.dump().size();
          if (bytes > config.max_state_bytes - captured_bytes)
            return full("round CEP declared-read budget exceeded");

          captured_bytes += bytes;
          cep_reads.push_back(std::move(reads));
        }
      }

      for (auto& [key, value] : next->keys) {
        (void)key;
        if (data && value.scope != operation.event.scope)
          continue;

        for (std::size_t i = 0; i < value.rules.size(); ++i) {
          const auto& reads = captured.find(key)->second[i];
          auto transition =
              data ? programs[i]->step(
                         value.rules[i],
                         FsmEvent{.kind = operation.event.kind,
                                  .occurrence_id = "input:" + std::to_string(next->sequence),
                                  .time_us = at,
                                  .values = operation.event.values},
                         reads)
                   : programs[i]->advance(value.rules[i], at, reads);
          if (!transition.ok())
            return transition.status();

          EF_RETURN_IF_ERROR(
              emit(context, *next, value.scope, i, transition.value(), reads,
                   data ? std::optional<SourceIdentity>{operation.event.source} : std::nullopt,
                   occurrences, data ? operation.event.kind : "$timeout",
                   data ? operation.event.values : Row{}));
          value.rules[i] = std::move(transition->state);
          if (!data && expired_scopes.contains(key)) {
            // A window's logical deletion changes predicates even without a new source record.
            // Timeouts run first; the ordinary expiry event then sees the same stable MV values.
            EF_ASSIGN_OR_RETURN(
                auto expired,
                programs[i]->step(
                    value.rules[i],
                    FsmEvent{.kind = "$expire",
                             .occurrence_id = "expiry:" + std::to_string(engine->revision() + 1),
                             .time_us = at,
                             .values = {}},
                    reads));
            EF_RETURN_IF_ERROR(
                emit(context, *next, value.scope, i, expired, reads, {}, occurrences, "$expire"));
            value.rules[i] = std::move(expired.state);
          }

          for (const auto& [policy_index, firing] : scheduled_clocks) {
            if (firing.scope != value.scope)
              continue;

            const auto& policy = config.periodic_policies[policy_index];
            const Row clock_values{static_cast<std::int64_t>(firing.skipped_slots)};
            EF_ASSIGN_OR_RETURN(
                auto clock_step,
                programs[i]->step(value.rules[i],
                                  FsmEvent{.kind = policy.event_kind,
                                           .occurrence_id = "clock:" + policy.id + ":" +
                                                            std::to_string(firing.slot_us),
                                           .time_us = at,
                                           .values = clock_values},
                                  reads));
            for (auto& emission : clock_step.emissions)
              if (!emission.scheduled_time_us)
                emission.scheduled_time_us = firing.slot_us;

            EF_RETURN_IF_ERROR(emit(context, *next, value.scope, i, clock_step, reads, {},
                                    occurrences, policy.event_kind, clock_values,
                                    firing.skipped_slots));
            value.rules[i] = std::move(clock_step.state);
          }
        }

        for (std::size_t i = 0; i < value.cep.size(); ++i) {
          const auto& reads = captured_cep.find(key)->second[i];
          const bool consumes_input =
              data && accepts_input(config.cep_rules[i], operation.event.kind);
          auto transition =
              consumes_input
                  ? cep_programs[i]->step(value.cep[i],
                                          CepEvent{.id = source_json(operation.event.source).dump(),
                                                   .kind = operation.event.kind,
                                                   .time_us = at,
                                                   .values = operation.event.values},
                                          reads)
                  : cep_programs[i]->advance(value.cep[i], at);
          if (!transition.ok())
            return transition.status();

          EF_RETURN_IF_ERROR(emit_cep(
              context, *next, value.scope, i, transition.value(),
              consumes_input ? std::optional<SourceIdentity>{operation.event.source} : std::nullopt,
              occurrences, consumes_input ? operation.event.kind : "$timeout",
              consumes_input ? operation.event.values : Row{}));
          value.cep[i] = std::move(transition->state);

          // CEP history records accepted immutable events. TTL deletes do not retract a TAKE.
          // Periodic events use durable policy/slot identity and the same fixed current reads.
          for (const auto& [policy_index, firing] : scheduled_clocks) {
            if (firing.scope != value.scope)
              continue;

            const auto& policy = config.periodic_policies[policy_index];
            const Row values{static_cast<std::int64_t>(firing.skipped_slots)};
            const bool consumes_clock = accepts_input(config.cep_rules[i], policy.event_kind);
            EF_ASSIGN_OR_RETURN(auto clock_step,
                                consumes_clock ? cep_programs[i]->step(
                                                     value.cep[i],
                                                     CepEvent{.id = "clock:" + policy.id + ":" +
                                                                    std::to_string(firing.slot_us),
                                                              .kind = policy.event_kind,
                                                              .time_us = at,
                                                              .values = values},
                                                     reads)
                                               : cep_programs[i]->advance(value.cep[i], at));
            EF_RETURN_IF_ERROR(emit_cep(context, *next, value.scope, i, clock_step, {}, occurrences,
                                        consumes_clock ? policy.event_kind : "$timeout",
                                        consumes_clock ? values : Row{},
                                        consumes_clock ? firing.skipped_slots : 0));
            value.cep[i] = std::move(clock_step.state);
          }
        }
      }
    }

    EF_RETURN_IF_ERROR(write_audit(context, *next, occurrences));
    if (config.requests)
      EF_RETURN_IF_ERROR(update_frontier(*next));

    EF_ASSIGN_OR_RETURN(auto encoded, encode(*next));
    return context.stage({.key = state_key, .value = std::move(encoded)});
  };
  RuntimePrepare prepare;
  if (config.requests && data)
    prepare = [&](RoundContext& context) noexcept {
      return prepare_candidate(context, *next, operation.event);
    };

  auto dispatched =
      config.requests ? EngineRuntimeAccess::apply_lifecycle(
                            *engine, inputs, {.processing_time_us = at}, hook, prepare)
      : cep_programs.empty() && !config.trigger_audit.relation
          ? engine->apply_many(inputs, {.processing_time_us = at}, hook)
          : EngineRuntimeAccess::apply_lifecycle(*engine, inputs, {.processing_time_us = at}, hook);
  if (!dispatched.ok()) {
    if (engine->has_pending_commit()) {
      pending_state = std::move(next);
      pending_occurrences = std::move(occurrences);
      pending_clock_firings = std::move(clock_firings);
      pending_final = final;
      pending_owner = PendingCommitOwner::Lifecycle;
      result.commit = {.code = CommitCode::Unknown,
                       .revision = engine->revision(),
                       .duplicate = false,
                       .message = "round commit response unavailable"};
    }

    return dispatched.status();
  }

  result.commit = dispatched->commit;
  if (dispatched->commit.code == CommitCode::Unknown) {
    pending_state = std::move(next);
    pending_occurrences = std::move(occurrences);
    pending_clock_firings = std::move(clock_firings);
    pending_final = final;
    pending_owner = PendingCommitOwner::Lifecycle;

    return {ErrorCode::UnknownCommit, "lifecycle round has an unresolved commit"};
  }

  if (dispatched->commit.code != CommitCode::Committed)
    return {ErrorCode::Conflict, "lifecycle round commit rejected"};

  state = std::move(*next);
  result.committed_rounds.push_back({.revision = engine->revision(),
                                     .processing_time_us = at,
                                     .changes = std::move(dispatched->changes),
                                     .occurrences = std::move(occurrences),
                                     .clock_firings = std::move(clock_firings)});
  result.input_committed = data;

  return {};
}

Status EventProcessor::Impl::update_frontier(LifecycleState& value) const noexcept {
  std::int64_t frontier = value.time_us;
  const auto retain = [&](std::int64_t due) noexcept {
    if (due <= value.time_us)
      frontier = std::min(frontier, due - 1);
  };

  for (const auto& [key, owned] : value.keys) {
    (void)key;
    for (const auto& rule : owned.rules)
      if (rule.deadline_us)
        retain(*rule.deadline_us);

    for (std::size_t i = 0; i < owned.cep.size(); ++i) {
      EF_ASSIGN_OR_RETURN(auto summary, cep_programs[i]->inspect(owned.cep[i]));
      if (summary.next_deadline_us)
        retain(*summary.next_deadline_us);
    }
  }

  for (const auto& [key, clock] : value.clocks) {
    (void)key;
    retain(clock.next_us);
  }

  for (const auto& notice : value.notices)
    retain(notice.deleted_at_us);

  if (frontier < value.timer_frontier_us)
    return bad("new timer debt would cross the confirmed frontier");

  value.timer_frontier_us = frontier;
  return {};
}

Status EventProcessor::Impl::publish_lifecycle(std::unique_ptr<LifecycleState> next,
                                               const std::vector<TableChange>& inputs,
                                               std::int64_t now_us, const RoundHook& hook,
                                               const RuntimePrepare& prepare,
                                               ProcessResult& result) noexcept {
  next->time_us = now_us;
  auto outcome = EngineRuntimeAccess::apply_lifecycle(
      *engine, inputs, {.processing_time_us = now_us},
      [&](RoundContext& round, const std::vector<RowChange>& changes) noexcept -> Status {
        if (hook)
          EF_RETURN_IF_ERROR(hook(round, changes));

        EF_RETURN_IF_ERROR(update_frontier(*next));
        EF_ASSIGN_OR_RETURN(auto encoded, encode(*next));
        return round.stage({.key = state_key, .value = std::move(encoded)});
      },
      prepare);
  if (!outcome.ok() || outcome->commit.code == CommitCode::Unknown) {
    if (engine->has_pending_commit()) {
      pending_state = std::move(next);
      pending_owner = PendingCommitOwner::Lifecycle;
      pending_final = false;
      result.commit = outcome.ok() ? outcome->commit
                                   : CommitResult{.code = CommitCode::Unknown,
                                                  .revision = engine->revision(),
                                                  .duplicate = false,
                                                  .message = "lifecycle response unavailable"};
    }

    return outcome.ok() ? Status(ErrorCode::UnknownCommit, "lifecycle commit is unresolved")
                        : outcome.status();
  }

  result.commit = outcome->commit;
  if (outcome->commit.code != CommitCode::Committed)
    return Status(ErrorCode::Conflict, "lifecycle commit rejected");

  state = std::move(*next);
  result.committed_rounds.push_back({.revision = engine->revision(),
                                     .processing_time_us = now_us,
                                     .changes = std::move(outcome->changes),
                                     .occurrences = {},
                                     .clock_firings = {}});
  return {};
}

Status EventProcessor::Impl::expiry_fence(std::int64_t now_us, ProcessResult& result) noexcept {
  auto next = std::make_unique<LifecycleState>(state);
  next->time_us = now_us;
  std::vector<TableChange> inputs;
  std::set<std::string> expired_scopes;

  for (auto it = next->ttl.begin(); it != next->ttl.end();) {
    if (it->second.deadline > now_us) {
      ++it;
      continue;
    }

    inputs.push_back({.relation = it->second.relation, .identity = it->second.identity, .row = {}});
    EF_ASSIGN_OR_RETURN(auto relation, catalog.find(it->second.relation));
    Row scope(it->second.identity.begin(),
              it->second.identity.begin() + relation->partition_types.size());
    EF_ASSIGN_OR_RETURN(auto key, scope_key(scope));
    expired_scopes.insert(std::move(key));
    it = next->ttl.erase(it);
  }

  for (const auto& key : expired_scopes) {
    if (next->notice_sequence == UINT64_MAX)
      return full("expiry notice identity exhausted");

    next->notices.push_back(
        {.sequence = ++next->notice_sequence, .key = key, .deleted_at_us = now_us});
    if (next->keys.at(key).candidate) {
      auto& flags = next->keys.at(key).candidate->level_dirty;
      std::fill(flags.begin(), flags.end(), true);
    }
  }

  std::vector<std::string> expired_candidates;
  for (auto& [key, owned] : next->keys)
    if (owned.candidate && owned.candidate->active && owned.candidate->deadline_us <= now_us) {
      owned.candidate->active = false;
      for (std::size_t i = 0; i < owned.candidate->bindings.size(); ++i)
        if (owned.candidate->bindings[i] == 0) {
          owned.candidate->bindings[i] = 2;
          owned.candidate->audits[i].reason = RequestPendingReason::Expired;
          owned.candidate->audits[i].last_slot_or_time = owned.candidate->deadline_us;
        }
      std::fill(owned.candidate->level_dirty.begin(), owned.candidate->level_dirty.end(), false);
      expired_candidates.push_back(key);
    }
  if (inputs.empty() && expired_candidates.empty())
    return {};

  auto* proposed = next.get();
  const RuntimePrepare prepare =
      [&](RoundContext& round) noexcept -> StatusOr<std::vector<TableChange>> {
    std::vector<TableChange> changes;
    for (const auto& key : expired_candidates) {
      const auto& owned = proposed->keys.at(key);
      EF_ASSIGN_OR_RETURN(auto change,
                          EngineRuntimeAccess::make_candidate_write(
                              round, config.requests->candidate->relation, owned.scope,
                              CandidateOperation::Deactivate, owned.candidate->epoch, {}));
      changes.push_back(std::move(change));
    }

    return changes;
  };
  return publish_lifecycle(std::move(next), inputs, now_us, {}, prepare, result);
}

std::size_t EventProcessor::Impl::try_once_capacity(
    std::string_view event, std::optional<std::string_view> selected_rule) const noexcept {
  if (!config.requests)
    return 0;

  std::size_t result = 0;
  for (const auto& binding : config.requests->trigger_bindings) {
    if (binding.mode != RequestTriggerMode::TryOnceAtFiring ||
        (selected_rule && binding.rule_id != *selected_rule))
      continue;

    for (const auto& rule : config.rules)
      if (rule.definition.id == binding.rule_id &&
          std::any_of(rule.definition.transitions.begin(), rule.definition.transitions.end(),
                      [&](const FsmTransition& transition) noexcept {
                        return transition.emit == binding.emission_name &&
                               (transition.event == event ||
                                (event != "$timeout" && transition.event == "*"));
                      }))
        ++result;

    if (event != "$expire")
      for (const auto& rule : config.cep_rules)
        if (rule.definition.id == binding.rule_id) {
          const auto count = rule.definition.limits.max_matches_per_step;
          if (count > config.max_rounds_per_call - std::min(result, config.max_rounds_per_call))
            return config.max_rounds_per_call;

          result += count;
        }
  }

  return result;
}

Status EventProcessor::Impl::timer_round(std::int64_t now_us, ProcessResult& result,
                                         bool& worked) noexcept {
  enum class TimerKind { Fsm, Cep, Expiry, Clock };
  TimerKind kind = TimerKind::Fsm;
  std::string selected;
  std::size_t index = 0;
  worked = false;

  for (const auto& [key, owned] : state.keys) {
    for (std::size_t i = 0; i < owned.rules.size(); ++i)
      if (owned.rules[i].deadline_us && *owned.rules[i].deadline_us <= now_us) {
        selected = key;
        index = i;
        worked = true;
        break;
      }
    if (worked)
      break;
  }

  if (!worked) {
    kind = TimerKind::Cep;
    for (const auto& [key, owned] : state.keys) {
      for (std::size_t i = 0; i < owned.cep.size(); ++i) {
        EF_ASSIGN_OR_RETURN(auto summary, cep_programs[i]->inspect(owned.cep[i]));
        if (summary.next_deadline_us && *summary.next_deadline_us <= now_us) {
          selected = key;
          index = i;
          worked = true;
          break;
        }
      }

      if (worked)
        break;
    }
  }

  if (!worked && !state.notices.empty()) {
    kind = TimerKind::Expiry;
    selected = state.notices.front().key;
    worked = true;
  }

  std::string clock_id;
  if (!worked) {
    kind = TimerKind::Clock;
    for (const auto& [id, clock] : state.clocks)
      if (clock.next_us <= now_us) {
        clock_id = id;
        selected = clock.key;
        index = clock.policy;
        worked = true;
        break;
      }
  }

  if (!worked)
    return {};

  std::size_t extra = 0;
  if (kind == TimerKind::Fsm)
    extra = try_once_capacity("$timeout", config.rules[index].definition.id);
  else if (kind == TimerKind::Cep)
    extra = try_once_capacity("$timeout", config.cep_rules[index].definition.id);
  else
    extra = try_once_capacity(
        kind == TimerKind::Expiry ? "$expire" : config.periodic_policies[index].event_kind);
  const auto available = config.max_rounds_per_call -
                         std::min(result.committed_rounds.size(), config.max_rounds_per_call);
  if (extra >= available)
    return full("timer requires reserved try-once admission rounds");

  auto next = std::make_unique<LifecycleState>(state);
  auto* proposed = next.get();
  auto& owned = proposed->keys.at(selected);
  std::size_t capacity = 0;
  const auto may_emit = [&](std::size_t rule, std::string_view event) noexcept {
    return std::any_of(config.rules[rule].definition.transitions.begin(),
                       config.rules[rule].definition.transitions.end(),
                       [&](const FsmTransition& transition) noexcept {
                         return !transition.emit.empty() &&
                                (transition.event == event ||
                                 (event != "$timeout" && transition.event == "*"));
                       });
  };
  if (kind == TimerKind::Fsm)
    capacity = may_emit(index, "$timeout");
  if (kind == TimerKind::Expiry || kind == TimerKind::Clock)
    for (std::size_t i = 0; i < owned.rules.size(); ++i)
      capacity += may_emit(
          i, kind == TimerKind::Expiry ? "$expire" : config.periodic_policies[index].event_kind);

  EF_RETURN_IF_ERROR(reserve_outbox(capacity));
  std::vector<TriggerOccurrence> occurrences;
  std::vector<ClockFiring> clocks;
  const auto hook = [&](RoundContext& round, const std::vector<RowChange>&) noexcept -> Status {
    std::vector<FsmReads> reads;
    std::vector<FsmReads> cep_reads;
    for (const auto& rule : config.rules) {
      EF_ASSIGN_OR_RETURN(auto captured, read_views(round, rule.reads, owned.scope));
      reads.push_back(std::move(captured));
    }

    for (const auto& rule : config.cep_rules) {
      EF_ASSIGN_OR_RETURN(auto captured, read_views(round, rule.reads, owned.scope));
      cep_reads.push_back(std::move(captured));
    }

    if (kind == TimerKind::Fsm) {
      EF_ASSIGN_OR_RETURN(auto step,
                          programs[index]->advance(owned.rules[index], now_us, reads[index]));
      EF_RETURN_IF_ERROR(emit(round, *proposed, owned.scope, index, step, reads[index], {},
                              occurrences, "$timeout"));
      owned.rules[index] = std::move(step.state);
    } else if (kind == TimerKind::Cep) {
      EF_ASSIGN_OR_RETURN(auto step, cep_programs[index]->advance(owned.cep[index], now_us));
      EF_RETURN_IF_ERROR(
          emit_cep(round, *proposed, owned.scope, index, step, {}, occurrences, "$timeout"));
      owned.cep[index] = std::move(step.state);
    } else {
      std::string event_kind, event_id;
      Row values;
      std::uint64_t skipped = 0;
      std::optional<std::int64_t> scheduled;
      if (kind == TimerKind::Expiry) {
        const auto notice = proposed->notices.front();
        proposed->notices.erase(proposed->notices.begin());
        event_kind = "$expire";
        event_id = "expiry:" + std::to_string(notice.sequence);
        scheduled = notice.deleted_at_us;
      } else {
        auto& clock = proposed->clocks.at(clock_id);
        const auto& policy = config.periodic_policies[index];
        skipped = static_cast<std::uint64_t>((now_us - clock.next_us) / policy.interval_us);
        const auto slot = clock.next_us + static_cast<std::int64_t>(skipped) * policy.interval_us;
        if (slot > INT64_MAX - policy.interval_us)
          return full("periodic slot overflow");

        clock.next_us = slot + policy.interval_us;
        clock.last_us = slot;
        clock.skipped = skipped;
        event_kind = policy.event_kind;
        scheduled = slot;
        event_id = "clock:" + policy.id + ":" + std::to_string(slot);
        values.emplace_back(static_cast<std::int64_t>(skipped));
        clocks.push_back({.policy_id = policy.id,
                          .scope = owned.scope,
                          .slot_us = slot,
                          .skipped_slots = skipped});
      }

      for (std::size_t i = 0; i < owned.rules.size(); ++i) {
        EF_ASSIGN_OR_RETURN(auto step, programs[i]->step(owned.rules[i],
                                                         FsmEvent{.kind = event_kind,
                                                                  .occurrence_id = event_id,
                                                                  .time_us = now_us,
                                                                  .values = values,
                                                                  .scheduled_time_us = scheduled},
                                                         reads[i]));
        if (scheduled)
          for (auto& emission : step.emissions)
            emission.scheduled_time_us = scheduled;

        EF_RETURN_IF_ERROR(emit(round, *proposed, owned.scope, i, step, reads[i], {}, occurrences,
                                event_kind, values, skipped));
        owned.rules[i] = std::move(step.state);
      }

      if (kind == TimerKind::Clock)
        for (std::size_t i = 0; i < owned.cep.size(); ++i) {
          const bool consumes_clock = accepts_input(config.cep_rules[i], event_kind);
          EF_ASSIGN_OR_RETURN(auto step, consumes_clock
                                             ? cep_programs[i]->step(owned.cep[i],
                                                                     CepEvent{.id = event_id,
                                                                              .kind = event_kind,
                                                                              .time_us = now_us,
                                                                              .values = values},
                                                                     cep_reads[i])
                                             : cep_programs[i]->advance(owned.cep[i], now_us));
          EF_RETURN_IF_ERROR(emit_cep(round, *proposed, owned.scope, i, step, {}, occurrences,
                                      consumes_clock ? event_kind : "$timeout",
                                      consumes_clock ? values : Row{},
                                      consumes_clock ? skipped : 0));
          owned.cep[i] = std::move(step.state);
        }
    }

    return write_audit(round, *proposed, occurrences);
  };
  auto status = publish_lifecycle(std::move(next), {}, now_us, hook, {}, result);
  if (status.ok()) {
    result.committed_rounds.back().occurrences = std::move(occurrences);
    result.committed_rounds.back().clock_firings = std::move(clocks);
  } else if (engine->has_pending_commit()) {
    pending_occurrences = std::move(occurrences);
    pending_clock_firings = std::move(clocks);
  }

  return status;
}

StatusOr<std::map<RelationId, RelationEnvelope>>
EventProcessor::Impl::relation_envelopes(const Row& scope) const noexcept {
  EF_RETURN_IF_ERROR(validate_scope(scope));
  std::vector<DataType> owner_partition_types;
  for (const auto& field : scope_schema.fields)
    owner_partition_types.push_back(field.type);

  std::size_t max_output_bytes = 0, max_capture_bytes = 0, max_identifier_bytes = 0;
  for (const auto& definition : config.requests->plans) {
    max_output_bytes = std::max(max_output_bytes, definition.machine.max_output_bytes);
    max_capture_bytes = std::max(max_capture_bytes, definition.machine.max_frozen_input_bytes);
    max_identifier_bytes = std::max(
        max_identifier_bytes, std::min<std::size_t>(1024, definition.machine.max_identifier_bytes));
  }

  std::map<RelationId, RelationEnvelope> result;
  std::map<RelationId, std::vector<std::size_t>> columns;
  const auto scalar = [](DataType type, std::size_t variable) noexcept -> std::size_t {
    switch (type) {
    case DataType::Bool:
      return 5;
    case DataType::Int64:
    case DataType::TimestampMicros:
      return 20;
    case DataType::Float64:
      return 32;
    case DataType::String:
    case DataType::FloatVector:
      return variable;
    }

    return variable;
  };
  const auto expression_bound =
      [&](const auto& self, const BoundExpr& expression,
          const std::vector<std::size_t>& input) noexcept -> StatusOr<std::size_t> {
    if (expression.kind == BoundExprKind::Input) {
      if (expression.slot >= input.size())
        return bad("invalid bound expression input");

      return std::size_t{input[expression.slot]};
    }

    if (expression.kind == BoundExprKind::Literal) {
      EF_ASSIGN_OR_RETURN(auto literal, value_to_json(expression.literal));
      return std::size_t{std::max<std::size_t>(4, literal.dump().size())};
    }

    if (expression.type != DataType::String && expression.type != DataType::FloatVector)
      return scalar(expression.type, 0);
    if (expression.kind == BoundExprKind::Cast && expression.args.size() == 1)
      return self(self, *expression.args[0], input);

    return bad("request publication requires a bounded variable-width SQL expression");
  };
  for (const auto& relation : catalog.relations) {
    // Catalog may contain independent graphs owned by other key shapes. Their
    // columns cannot use this owner's concrete key values. SQL descendants keep
    // the parent's partition types, including projected-away hidden key fields.
    if (relation->partition_types != owner_partition_types)
      continue;
    if (relation->partition_slots.size() != scope.size() ||
        relation->visible_count > relation->storage_schema.fields.size())
      return bad("relation envelope has an invalid partition or visible layout");

    const auto variable = relation->function || relation->latest ? max_output_bytes
                          : relation->base_write_role == BaseWriteRole::RequestCapture
                              ? max_capture_bytes
                              : config.max_event_bytes;
    std::vector<std::size_t> bounds;

    for (const auto& field : relation->storage_schema.fields)
      bounds.push_back(scalar(field.type, variable));

    if (relation->plan) {
      const auto& sql = *relation->plan;
      const auto left = columns.find(sql.left);
      if (left == columns.end())
        return bad("SQL envelope dependency has no compatible owner scope");

      auto input = left->second;
      if (sql.right) {
        const auto right = columns.find(*sql.right);
        if (right == columns.end())
          return bad("SQL join envelope dependency has no compatible owner scope");

        input.insert(input.end(), right->second.begin(), right->second.end());
      }

      if (!sql.group_by.empty()) {
        std::vector<std::size_t> grouped;
        for (const auto& expression : sql.group_by) {
          EF_ASSIGN_OR_RETURN(auto bound, expression_bound(expression_bound, *expression, input));
          grouped.push_back(bound);
        }

        for (const auto& aggregate : sql.aggregates)
          grouped.push_back(scalar(aggregate.result_type, 0));

        input = std::move(grouped);
      }

      if (sql.projection.size() > bounds.size())
        return bad("SQL envelope projection exceeds its stored layout");

      for (std::size_t i = 0; i < sql.projection.size(); ++i) {
        EF_ASSIGN_OR_RETURN(bounds[i],
                            expression_bound(expression_bound, *sql.projection[i], input));
      }
    }

    if (relation->latest) {
      const auto source = columns.find(relation->latest->definition.result_relation);
      if (source == columns.end())
        return bad("Latest envelope dependency has no compatible owner scope");

      const auto& result = source->second;
      for (std::size_t i = 0; i < result.size() && i < relation->visible_count; ++i)
        bounds[i] = result[i];

      for (std::size_t i = result.size(); i < relation->visible_count; ++i)
        bounds[i] = scalar(relation->storage_schema.fields[i].type, 6 * max_identifier_bytes + 2);
    }

    for (std::size_t i = 0; i < relation->partition_slots.size(); ++i) {
      if (relation->partition_slots[i] >= bounds.size())
        return bad("relation envelope partition slot exceeds its stored layout");

      EF_ASSIGN_OR_RETURN(auto encoded, value_to_json(scope[i]));
      bounds[relation->partition_slots[i]] = encoded.dump().size();
      if (i < relation->partition_visible_slots.size() &&
          relation->partition_visible_slots[i] != kNoSlot) {
        if (relation->partition_visible_slots[i] >= relation->visible_count)
          return bad("relation envelope partition slot exceeds its visible layout");

        bounds[relation->partition_visible_slots[i]] = encoded.dump().size();
      }
    }

    if (relation->function) {
      if (scope.size() >= bounds.size())
        return bad("Function envelope has no request identity field");

      bounds[scope.size()] = 6 * max_identifier_bytes + 2;
    }

    if (!relation->identity_domain.empty() || relation->request_status) {
      if (relation->physical_key_slots.empty() ||
          relation->physical_key_slots.back() >= bounds.size())
        return bad("request envelope has an invalid identity slot");

      bounds[relation->physical_key_slots.back()] = 6 * max_identifier_bytes + 2;
    }

    if (relation->request_status) {
      if (bounds.empty())
        return bad("request status envelope has no fields");

      bounds.back() = 11;
    }

    columns.emplace(relation->id, std::vector<std::size_t>(
                                      bounds.begin(), bounds.begin() + relation->visible_count));
    RelationEnvelope envelope;
    const auto add = [&](std::size_t& total, std::size_t count) noexcept -> Status {
      if (count > engine_config.max_bytes - std::min(total, engine_config.max_bytes))
        return full("one declared relation exceeds the control byte budget");

      total += count;
      return {};
    };
    EF_RETURN_IF_ERROR(add(envelope.row_bytes, 128 + bounds.size()));

    for (const auto bound : bounds)
      EF_RETURN_IF_ERROR(add(envelope.row_bytes, bound));

    EF_ASSIGN_OR_RETURN(auto key, row_key(engine_config.shard, 'T', relation->id, scope));
    envelope.key_bytes = key.size();
    for (std::size_t i = scope.size(); i < relation->physical_key_slots.size(); ++i) {
      if (relation->physical_key_slots[i] >= bounds.size())
        return bad("relation envelope identity slot exceeds its stored layout");

      const auto bound = bounds[relation->physical_key_slots[i]];
      if (engine_config.max_bytes < 4 || bound > (engine_config.max_bytes - 4) / 2)
        return full("relation identity exceeds the control key envelope");

      EF_RETURN_IF_ERROR(add(envelope.key_bytes, 2 * bound + 4));
    }

    if (relation->latest) {
      EF_ASSIGN_OR_RETURN(auto scope_json, row_json(scope));
      envelope.state_bytes = 256 + scope_json.dump().size() + 12 * max_identifier_bytes;
    }

    if (relation->plan && !relation->plan->group_by.empty()) {
      EF_ASSIGN_OR_RETURN(auto encoded_scope, row_json(scope));
      envelope.state_bytes = 128 + encoded_scope.dump().size() +
                             relation->plan->aggregates.size() * (2 * ExactSum::kEncodedSize + 96);
    }

    result.emplace(relation->id, envelope);
  }

  return result;
}

Status EventProcessor::Impl::reserve_request_publication(
    const LifecycleState& value, const RequestAdmission& admission) const noexcept {
  const auto& request = *config.requests;
  const auto plan = std::find_if(
      request.plans.begin(), request.plans.end(),
      [&](const RequestPlanDefinition& item) noexcept { return item.name == admission.plan; });
  if (plan == request.plans.end())
    return bad("unknown request plan");

  EF_ASSIGN_OR_RETURN(auto lifecycle, encode(value));
  (void)lifecycle;
  Json scope_json = Json::array(), arguments_json = Json::array();
  for (const auto& value : admission.scope) {
    EF_ASSIGN_OR_RETURN(auto encoded, value_to_json(value));
    scope_json.push_back(std::move(encoded));
  }

  for (const auto& value : admission.arguments) {
    EF_ASSIGN_OR_RETURN(auto encoded, value_to_json(value));
    arguments_json.push_back(std::move(encoded));
  }

  const auto envelope = Json{{"plan", admission.plan},
                             {"scope", std::move(scope_json)},
                             {"request_id", admission.request_id},
                             {"trigger_id", admission.trigger_id},
                             {"candidate_epoch", admission.candidate_epoch},
                             {"lifetime_us", admission.lifetime_us},
                             {"arguments", std::move(arguments_json)}}
                            .dump()
                            .size();
  const auto machine_bytes =
      std::min(plan->machine.max_state_bytes, request.runtime.max_record_bytes / 2);
  if (machine_bytes > request.runtime.max_record_bytes ||
      envelope + 64 > request.runtime.max_record_bytes - machine_bytes)
    return full("request terminal record envelope exceeds its configured bound");

  std::size_t bytes = 0;
  std::size_t mutations = 4; // Lifecycle companion, request record/index and Engine metadata.
  const auto add = [&](std::size_t count) noexcept -> Status {
    if (count > engine_config.max_bytes - bytes)
      return full("request terminal relation and companion byte reservation unavailable");

    bytes += count;
    return {};
  };

  // Other accepted source rounds may grow lifecycle state before this request terminates.
  // Its entire configured bound is reserved, not just today's encoded companion.
  EF_RETURN_IF_ERROR(add(config.max_state_bytes));
  EF_RETURN_IF_ERROR(add(machine_bytes + envelope + 64));
  EF_RETURN_IF_ERROR(add(request.runtime.max_index_bytes));
  EF_RETURN_IF_ERROR(add(config.max_event_bytes));
  EF_RETURN_IF_ERROR(add(Json(catalog.fingerprint).dump().size() + state_key.size() + 512));
  Row identity = admission.scope;
  identity.emplace_back(admission.request_id);
  EF_ASSIGN_OR_RETURN(auto identity_bytes, row_json(identity));
  EF_RETURN_IF_ERROR(add(2 * identity_bytes.dump().size() + 256));
  std::set<RelationId> ancestors(plan->targets.begin(), plan->targets.end());

  for (auto it = catalog.relations.rbegin(); it != catalog.relations.rend(); ++it) {
    if (!ancestors.contains((*it)->id))
      continue;

    EF_ASSIGN_OR_RETURN(auto dependencies, eventflow::dependencies(**it));
    ancestors.insert(dependencies.begin(), dependencies.end());
  }

  std::set<RelationId> roots;
  for (const auto id : ancestors) {
    EF_ASSIGN_OR_RETURN(auto relation, catalog.find(id));
    if (relation->kind == RelationKind::BaseTable)
      roots.insert(id);
  }

  if (plan->request_status_relation)
    roots.insert(*plan->request_status_relation);

  EF_ASSIGN_OR_RETURN(auto affected, reachable(catalog, roots));
  EF_ASSIGN_OR_RETURN(auto envelopes, relation_envelopes(admission.scope));
  for (const auto id : affected) {
    const auto found = envelopes.find(id);
    if (found == envelopes.end())
      return bad("request publication relation has no compatible owner scope envelope");

    const auto& envelope = found->second;
    EF_RETURN_IF_ERROR(add(envelope.key_bytes));
    EF_RETURN_IF_ERROR(add(envelope.row_bytes));
    ++mutations;
    if (envelope.state_bytes) {
      EF_RETURN_IF_ERROR(add(envelope.key_bytes));
      EF_RETURN_IF_ERROR(add(envelope.state_bytes));
      ++mutations;
    }
  }

  if (mutations > engine_config.max_mutations || affected.size() > engine_config.max_rows)
    return full("request terminal publication exceeds relation mutation or row reservation");

  return {};
}

void EventProcessor::Impl::publish_request(RequestRuntimeResult value, ProcessResult& result,
                                           RequestOperationResult operation_result,
                                           bool specified) noexcept {
  result.commit = value.commit;
  operation_result.record = std::move(value.record);
  if (value.deferred)
    operation_result.disposition = RequestDisposition::Deferred;
  else if (value.duplicate)
    operation_result.disposition = RequestDisposition::Duplicate;
  if (specified)
    result.request_result = operation_result;
  if (value.deferred || value.duplicate)
    return;

  CommittedRound committed;
  committed.revision = value.commit.revision;
  committed.processing_time_us = state.time_us;
  committed.changes = std::move(value.changes);
  committed.tasks = std::move(value.tasks);
  committed.cancel_task_ids = std::move(value.cancel_task_ids);
  committed.requests.push_back(std::move(operation_result));
  result.committed_rounds.push_back(std::move(committed));
}

Status EventProcessor::Impl::request_round(OperationKind kind, const RequestKey& requested_key,
                                           const FunctionCompletion& completion,
                                           std::int64_t now_us, std::size_t limit,
                                           const std::string& intent_id,
                                           std::optional<std::pair<std::string, std::size_t>> level,
                                           bool specified, ProcessResult& result) noexcept {
  auto next = std::make_unique<LifecycleState>(state);
  next->time_us = now_us;

  RequestAdmission admission;
  RequestKey key = requested_key;
  std::optional<std::size_t> binding_index;
  std::string candidate_key;
  bool expired = false, try_once = false;
  RequestPendingReason expiry_reason = RequestPendingReason::Expired;
  RequestPendingReason missing = RequestPendingReason::None;
  std::int64_t logical_time = now_us;

  if (kind == OperationKind::Admit) {
    if (level) {
      candidate_key = level->first;
      auto& owned = next->keys.at(candidate_key);
      const auto& binding = config.requests->level_bindings[level->second];
      auto& candidate = *owned.candidate;

      binding_index = config.requests->trigger_bindings.size() + level->second;
      admission.plan = binding.plan;
      admission.scope = owned.scope;
      admission.candidate_epoch = static_cast<std::uint64_t>(candidate.epoch);
      admission.trigger_id =
          Json::array({binding.id, binding.version, candidate_key, candidate.epoch}).dump();
      admission.request_id = admission.trigger_id;

      expired = !candidate.active || candidate.level_deadlines[level->second] <= now_us ||
                candidate.bindings[*binding_index] != 0;
      logical_time = expired ? std::min(now_us, candidate.level_deadlines[level->second])
                             : candidate.started_at_us;
    } else {
      const auto found = next->intents.find(intent_id);
      if (found == next->intents.end())
        return Status(ErrorCode::NotFound, "occurrence has no pending request intent");

      const auto& intent = found->second;
      admission = intent.admission;
      binding_index = intent.binding;
      EF_ASSIGN_OR_RETURN(candidate_key, scope_key(admission.scope));
      const auto& candidate = next->keys.at(candidate_key).candidate;
      const auto& binding = config.requests->trigger_bindings[intent.binding];
      try_once = binding.mode == RequestTriggerMode::TryOnceAtFiring;
      const auto occurrence = std::find_if(
          next->outbox.begin(), next->outbox.end(),
          [&](const TriggerOccurrence& value) noexcept { return value.id == intent_id; });
      if (occurrence == next->outbox.end())
        return bad("request intent has no immutable occurrence");

      logical_time = occurrence->emission.scheduled_time_us.value_or(occurrence->evaluated_at_us);
      expired = intent.deadline_us <= now_us || !candidate || !candidate->active ||
                static_cast<std::uint64_t>(candidate->epoch) != admission.candidate_epoch ||
                (binding.once_per_candidate && candidate->bindings[*binding_index] != 0);
      if (candidate && static_cast<std::uint64_t>(candidate->epoch) != admission.candidate_epoch)
        expiry_reason = RequestPendingReason::Replaced;
      else if (candidate && binding.once_per_candidate && candidate->bindings[*binding_index] == 1)
        expiry_reason = RequestPendingReason::OnceConsumed;
      if (!expired && try_once && occurrence->evaluated_at_us != now_us)
        missing = RequestPendingReason::NoCapacity;
      if (!expired && !binding.required_accepted_binding.empty()) {
        bool accepted = false;
        for (std::size_t i = 0; i < config.requests->trigger_bindings.size(); ++i)
          if (config.requests->trigger_bindings[i].id == binding.required_accepted_binding)
            accepted = candidate->bindings[i] == 1;

        for (std::size_t i = 0; i < config.requests->level_bindings.size(); ++i)
          if (config.requests->level_bindings[i].id == binding.required_accepted_binding)
            accepted = candidate->bindings[config.requests->trigger_bindings.size() + i] == 1;

        if (!accepted)
          missing = RequestPendingReason::NoPrerequisite;
      }
    }

    key = {.scope = admission.scope, .request_id = admission.request_id};
  } else {
    EF_ASSIGN_OR_RETURN(candidate_key, scope_key(key.scope));
  }

  RequestOperationResult operation_result{.key = key,
                                          .disposition = kind == OperationKind::Admit
                                                             ? RequestDisposition::Accepted
                                                             : RequestDisposition::Updated,
                                          .record = {}};
  const auto remove_intent = [&]() noexcept {
    if (intent_id.empty())
      return;

    next->intents.erase(intent_id);
    std::erase_if(next->outbox,
                  [&](const TriggerOccurrence& value) noexcept { return value.id == intent_id; });
  };
  const auto finish_without_request = [&](RequestPendingReason reason,
                                          Status detail) noexcept -> Status {
    const bool skipped = try_once && !expired;
    operation_result.disposition = expired   ? RequestDisposition::Expired
                                   : skipped ? RequestDisposition::Skipped
                                             : RequestDisposition::Deferred;
    operation_result.reason = detail;
    if (specified)
      result.request_result = operation_result;
    if (expired || skipped)
      remove_intent();

    bool changed = expired || skipped;
    auto& candidate = next->keys.at(candidate_key).candidate;
    if (candidate && binding_index &&
        static_cast<std::uint64_t>(candidate->epoch) == admission.candidate_epoch) {
      auto& audit = candidate->audits[*binding_index];
      changed |= audit.reason != reason || audit.last_slot_or_time != logical_time;
      audit.reason = reason;
      audit.last_slot_or_time = logical_time;
      if (skipped) {
        if (audit.skipped_count == UINT64_MAX)
          return full("try-once skipped counter exhausted");

        ++audit.skipped_count;
      }

      if (expired && candidate->bindings[*binding_index] == 0)
        candidate->bindings[*binding_index] = 2;
      if (expired && level)
        candidate->level_dirty[level->second] = false;
    }

    if (!expired && !skipped)
      result.work_remaining = true;
    if (!changed)
      return {};

    auto status = publish_lifecycle(std::move(next), {}, now_us, {}, {}, result);
    if (status.ok())
      result.committed_rounds.back().requests.push_back(operation_result);
    else if (engine->has_pending_commit()) {
      pending_request_result = operation_result;
      pending_specified_request = specified;
      if (specified)
        result.request_result.reset();
    }

    return status;
  };
  if (expired)
    return finish_without_request(
        expiry_reason, {ErrorCode::FailedPrecondition, "pending request expired or fenced"});
  if (missing != RequestPendingReason::None)
    return finish_without_request(
        missing,
        {ErrorCode::Unavailable, "request prerequisite or original firing slot is unavailable"});
  if (kind == OperationKind::Admit) {
    const auto reserved = reserve_request_publication(*next, admission);
    if (!reserved.ok()) {
      if (reserved.code() != ErrorCode::ResourceExhausted)
        return reserved;

      return finish_without_request(RequestPendingReason::NoCapacity, reserved);
    }
  }

  bool companion_staged = false;
  const RequestCommitHook companion =
      [&](RoundContext& round,
          const RequestRecord&) noexcept -> StatusOr<std::vector<TableChange>> {
    remove_intent();
    auto found = next->keys.find(candidate_key);
    if (found != next->keys.end() && found->second.candidate) {
      auto& candidate = *found->second.candidate;
      if (binding_index &&
          static_cast<std::uint64_t>(candidate.epoch) == admission.candidate_epoch) {
        candidate.bindings[*binding_index] = 1;
        candidate.audits[*binding_index].reason = RequestPendingReason::None;
        candidate.audits[*binding_index].last_slot_or_time = logical_time;
      }

      std::fill(candidate.level_dirty.begin(), candidate.level_dirty.end(), true);
      if (level)
        candidate.level_dirty[level->second] = false;
    }

    EF_RETURN_IF_ERROR(update_frontier(*next));
    EF_ASSIGN_OR_RETURN(auto encoded, encode(*next));
    EF_RETURN_IF_ERROR(round.stage({.key = state_key, .value = std::move(encoded)}));
    companion_staged = true;

    return std::vector<TableChange>{};
  };
  auto outcome = kind == OperationKind::Admit ? requests->admit(admission, now_us, companion)
                 : kind == OperationKind::Complete
                     ? requests->complete(key.scope, key.request_id, completion, now_us, companion)
                 : kind == OperationKind::Cancel
                     ? requests->cancel(key.scope, key.request_id, now_us, companion)
                 : kind == OperationKind::Claim
                     ? requests->claim(key.scope, key.request_id, now_us, limit, companion)
                     : requests->advance(key.scope, key.request_id, now_us, companion);
  if (!outcome.ok() || outcome->commit.code == CommitCode::Unknown) {
    if (requests->has_pending_commit()) {
      pending_state = std::move(next);
      pending_owner = PendingCommitOwner::Request;
      pending_request_result = operation_result;
      pending_specified_request = specified;
      pending_final = false;
      result.commit = outcome.ok() ? outcome->commit
                                   : CommitResult{.code = CommitCode::Unknown,
                                                  .revision = engine->revision(),
                                                  .duplicate = false,
                                                  .message = "request response unavailable"};
      return {ErrorCode::UnknownCommit, "request commit is unresolved"};
    }

    if (!outcome.ok() && kind == OperationKind::Admit &&
        outcome.status().code() == ErrorCode::ResourceExhausted)
      return finish_without_request(RequestPendingReason::NoCapacity, outcome.status());

    return outcome.ok() ? Status(ErrorCode::UnknownCommit, "request commit unresolved")
                        : outcome.status();
  }

  if (outcome->commit.code != CommitCode::Committed) {
    result.commit = outcome->commit;
    return {ErrorCode::Conflict, "request commit rejected"};
  }

  if (kind == OperationKind::Admit && outcome->duplicate)
    return Status(ErrorCode::FailedPrecondition,
                  "pending admission already has a committed request");
  if (outcome->deferred)
    return finish_without_request(RequestPendingReason::NotReady,
                                  {ErrorCode::Unavailable, "request capture is not ready"});
  if (companion_staged)
    state = std::move(*next);

  publish_request(std::move(outcome).value(), result, operation_result, specified);
  return {};
}

PendingOperationInfo EventProcessor::Impl::pending_info(const Operation& operation) const noexcept {
  ProcessorOperationKind kind = ProcessorOperationKind::Advance;
  switch (operation.kind) {
  case OperationKind::Data:
    kind = ProcessorOperationKind::Source;
    break;
  case OperationKind::Advance:
    break;
  case OperationKind::Acknowledge:
    kind = ProcessorOperationKind::Acknowledge;
    break;
  case OperationKind::Admit:
    kind = ProcessorOperationKind::AdmitOccurrence;
    break;
  case OperationKind::Complete:
    kind = ProcessorOperationKind::CompleteRequest;
    break;
  case OperationKind::Cancel:
    kind = ProcessorOperationKind::CancelRequest;
    break;
  case OperationKind::Claim:
    kind = ProcessorOperationKind::ClaimTasks;
    break;
  case OperationKind::Pump:
    kind = ProcessorOperationKind::Pump;
    break;
  }

  return {.kind = kind,
          .commit_owner = pending_owner,
          .fixed_now_us = operation.target_us,
          .awaiting_commit = true};
}

ProcessResult EventProcessor::Impl::run_requests(Operation operation,
                                                 ProcessResult result) noexcept {
  // The saved phase lets an UNKNOWN commit resume after confirmation without repeating completed
  // work. Every phase uses the same processing time, including after a delayed retry.
  result.commit = {.code = CommitCode::Committed,
                   .revision = engine->revision(),
                   .duplicate = false,
                   .message = {}};
  const auto at = operation.target_us;
  if (operation.kind == OperationKind::Acknowledge && operation.phase == 0)
    operation.phase = 7;

  while (true) {
    if (operation.phase == 8) {
      result.work_remaining |= !state.intents.empty() || state.timer_frontier_us < state.time_us;
      for (const auto& [key, owned] : state.keys) {
        (void)key;
        if (owned.candidate)
          for (std::size_t i = 0; i < owned.candidate->level_dirty.size(); ++i)
            result.work_remaining |=
                owned.candidate->level_dirty[i] &&
                owned.candidate->bindings[config.requests->trigger_bindings.size() + i] == 0;
      }

      return result;
    }

    if (result.committed_rounds.size() >= config.max_rounds_per_call) {
      result.operation_status = full("processor round budget reached");
      result.work_remaining = true;
      return result;
    }

    Status status;
    if (operation.phase == 0) {
      operation.phase = 1;
      status = expiry_fence(at, result);
    } else if (operation.phase == 1) {
      operation.phase = 2;
      if (operation.kind == OperationKind::Admit || operation.kind == OperationKind::Complete ||
          operation.kind == OperationKind::Cancel || operation.kind == OperationKind::Claim) {
        status = request_round(
            operation.kind, operation.request, operation.completion, at, operation.limit,
            operation.ids.empty() ? std::string{} : operation.ids[0], {}, true, result);
      }
    } else if (operation.phase == 2) {
      auto active_requests = requests->active_requests();
      if (!active_requests.ok())
        status = active_requests.status();
      else {
        bool found = false;
        for (const auto& record : active_requests.value()) {
          bool due = record.state.manifest.deadline_us <= at;
          for (const auto& node : record.state.nodes)
            due |= node.phase == RequestNodePhase::Running && node.attempt_deadline_us <= at;

          const auto id = record.admission.request_id;
          auto encoded_scope = scope_key(record.admission.scope);
          if (!encoded_scope.ok()) {
            status = encoded_scope.status();
            break;
          }

          const auto visit = Json::array({encoded_scope.value(), id}).dump();
          if (due && operation.attempted.insert(visit).second) {
            status = request_round(OperationKind::Advance,
                                   {.scope = record.admission.scope, .request_id = id}, {}, at, 0,
                                   {}, {}, false, result);
            found = true;
            break;
          }
        }

        if (!found && status.ok()) {
          operation.phase = 3;
          operation.attempted.clear();
          if (operation.kind == OperationKind::Admit && !operation.ids.empty())
            operation.attempted.insert(operation.ids[0]);
        }
      }
    } else if (operation.phase == 3 || operation.phase == 5) {
      bool found = false;
      for (const auto& [id, intent] : state.intents) {
        (void)intent;
        if (operation.attempted.insert(id).second) {
          status = request_round(OperationKind::Admit, {}, {}, at, 0, id, {}, false, result);
          found = true;
          break;
        }
      }

      if (!found) {
        ++operation.phase;
        operation.attempted.clear();
      }
    } else if (operation.phase == 4) {
      bool found = false;
      for (const auto& [key, owned] : state.keys) {
        if (!owned.candidate)
          continue;

        for (std::size_t i = 0; i < config.requests->level_bindings.size(); ++i) {
          const auto slot = config.requests->trigger_bindings.size() + i;
          const auto visit = Json::array({key, i}).dump();
          if (owned.candidate->level_dirty[i] && owned.candidate->bindings[slot] == 0 &&
              operation.attempted.insert(visit).second) {
            const auto before = result.committed_rounds.size();
            status =
                request_round(OperationKind::Admit, {}, {}, at, 0, {}, {{key, i}}, false, result);
            if (status.ok() && result.committed_rounds.size() > before &&
                !result.committed_rounds.back().requests.empty() &&
                result.committed_rounds.back().requests.back().disposition ==
                    RequestDisposition::Accepted)
              operation.recheck_intents = true;

            found = true;
            break;
          }
        }

        if (found)
          break;
      }

      if (!found) {
        operation.phase = operation.recheck_intents ? 5 : 6;
        operation.attempted.clear();
      }
    } else if (operation.phase == 6) {
      bool worked = false;
      operation.phase = 9;
      operation.resume_phase = 6;
      status = timer_round(at, result, worked);
      if (!worked && status.ok())
        operation.phase = 7;
    } else if (operation.phase == 7) {
      operation.phase = 8;
      if (operation.kind == OperationKind::Data || operation.kind == OperationKind::Acknowledge) {
        if (operation.kind == OperationKind::Data && try_once_capacity(operation.event.kind) != 0) {
          operation.phase = 9;
          operation.resume_phase = 8;
        }

        status = round(operation, at, true, result);
      } else if (state.time_us < at)
        status = publish_lifecycle(std::make_unique<LifecycleState>(state), {}, at, {}, {}, result);
    } else if (operation.phase == 9) {
      bool found = false;
      for (const auto& [id, intent] : state.intents)
        if (config.requests->trigger_bindings[intent.binding].mode ==
            RequestTriggerMode::TryOnceAtFiring) {
          status = request_round(OperationKind::Admit, {}, {}, at, 0, id, {}, false, result);
          found = true;
          break;
        }
      if (!found)
        operation.phase = operation.resume_phase;
    } else {
      result.work_remaining |= state.timer_frontier_us < state.time_us;
      return result;
    }

    if (!status.ok()) {
      result.operation_status = status;
      result.work_remaining = true;
      if (engine->has_pending_commit()) {
        pending_operation = operation;
        result.pending = pending_info(operation);
      }

      return result;
    }
  }
}

Status EventProcessor::Impl::request_ready(const RequestKey* key,
                                           std::int64_t now_us) const noexcept {
  EF_RETURN_IF_ERROR(ready());
  if (!requests)
    return {ErrorCode::FailedPrecondition, "request integration is disabled"};
  if (now_us < state.time_us)
    return bad("processing time cannot move backwards");
  if (key) {
    EF_RETURN_IF_ERROR(validate_scope(key->scope));
    if (!text_ok(key->request_id, 4096))
      return bad("invalid request identity");
  }

  return {};
}

ProcessResult EventProcessor::Impl::run(Operation operation, ProcessResult result) noexcept {
  if (config.requests)
    return run_requests(std::move(operation), std::move(result));

  result.commit = {.code = CommitCode::Committed,
                   .revision = engine->revision(),
                   .duplicate = false,
                   .message = {}};
  std::size_t rounds = 0;

  while (true) {
    auto due = next_deadline(operation.target_us);
    // A data event at a wait's deadline cannot satisfy it: process the timeout round first.
    const bool final =
        operation.kind == OperationKind::Acknowledge || !due || *due > operation.target_us;
    if (final && operation.kind == OperationKind::Advance && state.time_us == operation.target_us)
      return result;
    if (rounds++ == config.max_rounds_per_call) {
      result.operation_status =
          full("advance round budget reached; retry after consuming committed output");
      return result;
    }

    // The caller samples processing time once. Missed deadlines are audit metadata, never an
    // invitation to reconstruct an old market snapshot. All overdue TTLs expire before guards.
    const auto at = operation.target_us;
    result.operation_status = round(operation, at, final, result);
    if (!result.operation_status.ok()) {
      if (engine->has_pending_commit())
        pending_operation = std::move(operation);

      return result;
    }

    if (final)
      return result;
  }
}

EventProcessor::EventProcessor(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}

EventProcessor::~EventProcessor() noexcept = default;

Status EventProcessor::validate_definition(CatalogSnapshot catalog, EngineConfig engine_config,
                                           ProcessorConfig config) noexcept {
  auto impl = std::make_unique<Impl>();
  impl->catalog = std::move(catalog);
  impl->engine_config = std::move(engine_config);
  impl->config = std::move(config);

  return impl->bind();
}

StatusOr<std::unique_ptr<EventProcessor>> EventProcessor::open(std::shared_ptr<IStateStore> store,
                                                               CatalogSnapshot catalog,
                                                               EngineConfig engine_config,
                                                               ProcessorConfig config) noexcept {
  if (!store)
    return bad("event processor requires a state store");

  auto impl = std::make_unique<Impl>();
  impl->store = std::move(store);
  impl->catalog = std::move(catalog);
  EF_ASSIGN_OR_RETURN(impl->engine_config, resolve_engine_config(*impl->store, engine_config));
  impl->config = std::move(config);
  EF_RETURN_IF_ERROR(impl->bind());
  if (impl->config.requests) {
    EF_ASSIGN_OR_RETURN(impl->engine,
                        EngineRuntimeAccess::open(impl->store, impl->catalog, impl->engine_config,
                                                  impl->config.requests->registry));
  } else {
    EF_ASSIGN_OR_RETURN(impl->engine,
                        Engine::open(impl->store, impl->catalog, impl->engine_config));
  }

  impl->engine_config = impl->engine->config();
  EF_ASSIGN_OR_RETURN(auto loaded,
                      impl->store->get_many(impl->engine_config.shard, {impl->state_key}));
  if (loaded.revision != impl->engine->revision() || loaded.values.size() != 1)
    return Status(ErrorCode::Conflict, "lifecycle initialization revision mismatch");
  if (loaded.values[0]) {
    EF_ASSIGN_OR_RETURN(impl->state, impl->decode(*loaded.values[0]));
  } else {
    if (impl->engine->revision() != 1)
      return Status(ErrorCode::FailedPrecondition,
                    "cannot attach lifecycle to preexisting unmanaged data");

    EF_ASSIGN_OR_RETURN(auto bytes, impl->encode(impl->state));
    auto initialized = impl->engine->apply_many(
        {}, {}, [&](RoundContext& round, const std::vector<RowChange>&) noexcept {
          return round.stage({.key = impl->state_key, .value = bytes});
        });
    if (!initialized.ok())
      return initialized.status();
    if (initialized->commit.code != CommitCode::Committed)
      return Status(ErrorCode::UnknownCommit, "lifecycle initialization was not confirmed");
  }

  if (impl->config.requests) {
    const auto& policy = *impl->config.requests->candidate;
    EF_ASSIGN_OR_RETURN(auto relation, impl->catalog.find(policy.relation));
    EF_ASSIGN_OR_RETURN(auto prefix, table_prefix(impl->engine_config.shard, 'T', policy.relation));
    std::optional<std::string> after;
    std::size_t observed_candidates = 0;
    std::size_t expected_candidates = 0;

    for (const auto& [key, owned] : impl->state.keys) {
      (void)key;
      expected_candidates += owned.candidate.has_value();
    }

    while (true) {
      EF_ASSIGN_OR_RETURN(auto page, impl->store->scan(impl->engine_config.shard, prefix, after,
                                                       impl->engine_config.page_rows));
      if (page.revision != impl->engine->revision())
        return Status(ErrorCode::Conflict, "candidate recovery revision changed");
      if (page.rows.empty())
        break;

      for (const auto& [stored_key, bytes] : page.rows) {
        if (++observed_candidates > expected_candidates)
          return bad("unmanaged candidate has no durable lifecycle owner");

        EF_ASSIGN_OR_RETURN(auto row, decode_row(relation->storage_schema, bytes));
        EF_ASSIGN_OR_RETURN(auto scope, partition_values(*relation, row));
        EF_ASSIGN_OR_RETURN(auto key, scope_key(scope));
        EF_ASSIGN_OR_RETURN(auto canonical,
                            row_key(impl->engine_config.shard, 'T', policy.relation, scope));
        const auto owned = impl->state.keys.find(key);
        if (owned == impl->state.keys.end() || !owned->second.candidate || stored_key != canonical)
          return bad("candidate identity does not match its durable lifecycle owner");

        const auto& candidate = *owned->second.candidate;
        if (std::get<std::int64_t>(row[scope.size()]) != candidate.epoch ||
            std::get<bool>(row[scope.size() + 1]) != candidate.active ||
            std::get<std::int64_t>(row[scope.size() + 2 + impl->deadline_payload_index]) !=
                candidate.deadline_us)
          return bad("candidate relation and lifecycle expiry fence disagree");

        for (std::size_t i = 0; i < impl->binding_deadline_indices.size(); ++i) {
          const auto column = impl->binding_deadline_indices[i];
          const auto cap = column ? std::get<std::int64_t>(row[scope.size() + 2 + *column])
                                  : candidate.deadline_us;
          if (candidate.deadline_caps[i] != std::min(cap, candidate.deadline_us))
            return bad("candidate pending cap and lifecycle audit disagree");
        }
      }

      after = page.rows.back().first;
    }

    if (observed_candidates != expected_candidates)
      return bad("durable lifecycle candidate row is missing");

    auto plans = impl->config.requests->plans;
    for (auto& plan : plans) {
      if (!plan.capture)
        return bad("request plan requires a capture callback");

      auto capture = plan.capture;
      const auto limit = plan.machine.max_frozen_input_bytes;
      const auto catalog = impl->catalog;
      plan.capture = [capture, limit, catalog](
                         RoundContext& round,
                         const RequestAdmission& admission) noexcept -> StatusOr<RequestCapture> {
        EF_ASSIGN_OR_RETURN(auto captured, capture(round, admission));
        std::size_t bytes = 0;

        for (const auto& change : captured.rows) {
          if (!change.row)
            return bad("request capture cannot delete input rows");

          EF_ASSIGN_OR_RETURN(auto relation, catalog.find(change.relation));
          EF_ASSIGN_OR_RETURN(auto encoded, encode_row(relation->storage_schema, *change.row));
          if (encoded.size() > limit - bytes)
            return full("capture exceeds its publication input reservation");

          bytes += encoded.size();
        }

        return captured;
      };
    }

    EF_ASSIGN_OR_RETURN(impl->requests,
                        RequestRuntime::open(*impl->engine, impl->store,
                                             impl->config.requests->registry, std::move(plans),
                                             impl->config.requests->runtime));
    EF_ASSIGN_OR_RETURN(auto active_requests, impl->requests->active_requests());

    for (const auto& record : active_requests)
      EF_RETURN_IF_ERROR(impl->reserve_request_publication(impl->state, record.admission));
  }

  return std::unique_ptr<EventProcessor>(new EventProcessor(std::move(impl)));
}

StatusOr<ProcessResult> EventProcessor::process(const ProcessorEvent& event) noexcept {
  EF_RETURN_IF_ERROR(impl_->ready());
  EF_ASSIGN_OR_RETURN(auto payload, impl_->event_payload(event));
  const auto position = impl_->state.positions.find(source_partition_key(event.source));
  if (position != impl_->state.positions.end() &&
      event.source.offset < position->second.next_offset) {
    const auto receipt = std::find_if(
        impl_->state.receipts.begin(), impl_->state.receipts.end(),
        [&](const DedupReceipt& value) noexcept { return value.source == event.source; });
    if (receipt == impl_->state.receipts.end())
      return Status(
          ErrorCode::FailedPrecondition,
          "delivery precedes retained exact dedup history; restore a compatible checkpoint");
    if (receipt->payload != payload)
      return Status(ErrorCode::Conflict, "same durable identity has different payload");

    ProcessResult result;
    result.commit = {.code = CommitCode::Committed,
                     .revision = impl_->engine->revision(),
                     .duplicate = true,
                     .message = {}};
    result.input_committed = true;
    result.duplicate = true;

    return result;
  }

  if (event.processing_time_us < impl_->state.time_us)
    return bad("processing time cannot move backwards");

  Operation operation{.kind = OperationKind::Data,
                      .event = event,
                      .payload = std::move(payload),
                      .target_us = event.processing_time_us,
                      .ids = {}};

  // Validate capacity/overflow before any earlier due timer subround is allowed to commit.
  auto candidate = impl_->state;
  if (!impl_->config.requests)
    EF_RETURN_IF_ERROR(impl_->data_candidate(candidate, operation));

  impl_->active = true;
  auto result = impl_->run(std::move(operation));
  impl_->active = false;
  return result;
}

StatusOr<ProcessResult> EventProcessor::advance(std::int64_t time_us) noexcept {
  EF_RETURN_IF_ERROR(impl_->ready());
  if (time_us < impl_->state.time_us)
    return bad("processing time cannot move backwards");

  impl_->active = true;
  auto result = impl_->run({.kind = OperationKind::Advance,
                            .event = {},
                            .payload = {},
                            .target_us = time_us,
                            .ids = {}});
  impl_->active = false;

  return result;
}

StatusOr<ProcessResult> EventProcessor::admit_occurrence(std::string_view id,
                                                         std::int64_t now_us) noexcept {
  EF_RETURN_IF_ERROR(impl_->request_ready(nullptr, now_us));
  if (!impl_->state.intents.contains(std::string(id)))
    return Status(ErrorCode::NotFound, "occurrence has no request binding");

  Operation operation{.kind = OperationKind::Admit,
                      .event = {},
                      .payload = {},
                      .target_us = now_us,
                      .ids = {std::string(id)}};
  impl_->active = true;
  auto result = impl_->run(std::move(operation));
  impl_->active = false;

  return result;
}

StatusOr<ProcessResult> EventProcessor::complete_request(const RequestKey& key,
                                                         const FunctionCompletion& completion,
                                                         std::int64_t now_us) noexcept {
  EF_RETURN_IF_ERROR(impl_->request_ready(&key, now_us));
  Operation operation{
      .kind = OperationKind::Complete, .event = {}, .payload = {}, .target_us = now_us, .ids = {}};
  operation.request = key;
  operation.completion = completion;
  impl_->active = true;
  auto result = impl_->run(std::move(operation));
  impl_->active = false;

  return result;
}

StatusOr<ProcessResult> EventProcessor::cancel_request(const RequestKey& key,
                                                       std::int64_t now_us) noexcept {
  EF_RETURN_IF_ERROR(impl_->request_ready(&key, now_us));
  Operation operation{
      .kind = OperationKind::Cancel, .event = {}, .payload = {}, .target_us = now_us, .ids = {}};
  operation.request = key;
  impl_->active = true;
  auto result = impl_->run(std::move(operation));
  impl_->active = false;

  return result;
}

StatusOr<ProcessResult> EventProcessor::claim_tasks(const RequestKey& key, std::int64_t now_us,
                                                    std::size_t limit) noexcept {
  EF_RETURN_IF_ERROR(impl_->request_ready(&key, now_us));
  if (limit == 0)
    return bad("task claim limit must be positive");

  Operation operation{
      .kind = OperationKind::Claim, .event = {}, .payload = {}, .target_us = now_us, .ids = {}};
  operation.request = key;
  operation.limit = limit;
  impl_->active = true;
  auto result = impl_->run(std::move(operation));
  impl_->active = false;

  return result;
}

StatusOr<ProcessResult> EventProcessor::pump(std::int64_t now_us) noexcept {
  EF_RETURN_IF_ERROR(impl_->request_ready(nullptr, now_us));
  impl_->active = true;
  auto result = impl_->run(
      {.kind = OperationKind::Pump, .event = {}, .payload = {}, .target_us = now_us, .ids = {}});
  impl_->active = false;

  return result;
}

StatusOr<std::optional<PendingOperationInfo>> EventProcessor::pending_operation() const noexcept {
  if (!impl_->pending_operation)
    return std::optional<PendingOperationInfo>{};

  return std::optional<PendingOperationInfo>{impl_->pending_info(*impl_->pending_operation)};
}

StatusOr<RequestRecord> EventProcessor::read_request(const RequestKey& key) noexcept {
  EF_RETURN_IF_ERROR(impl_->request_ready(&key, impl_->state.time_us));
  return impl_->requests->read(key.scope, key.request_id);
}

StatusOr<std::vector<FunctionTask>>
EventProcessor::pending_tasks(std::size_t limit) const noexcept {
  EF_RETURN_IF_ERROR(impl_->request_ready(nullptr, impl_->state.time_us));
  return impl_->requests->pending_tasks(limit);
}

StatusOr<std::vector<RequestRecord>> EventProcessor::active_requests() const noexcept {
  EF_RETURN_IF_ERROR(impl_->request_ready(nullptr, impl_->state.time_us));
  return impl_->requests->active_requests();
}

StatusOr<CandidateProgress> EventProcessor::read_candidate(const Row& scope) const noexcept {
  EF_RETURN_IF_ERROR(impl_->request_ready(nullptr, impl_->state.time_us));
  EF_RETURN_IF_ERROR(impl_->validate_scope(scope));
  EF_ASSIGN_OR_RETURN(auto key, scope_key(scope));
  const auto found = impl_->state.keys.find(key);
  if (found == impl_->state.keys.end() || !found->second.candidate)
    return Status(ErrorCode::NotFound, "scope has no candidate");

  const auto& candidate = *found->second.candidate;
  CandidateProgress result{.scope = scope,
                           .candidate_epoch = candidate.epoch,
                           .active = candidate.active,
                           .started_at_us = candidate.started_at_us,
                           .deadline_us = candidate.deadline_us,
                           .bindings = {}};
  const auto& request = *impl_->config.requests;

  for (std::size_t i = 0; i < candidate.bindings.size(); ++i) {
    const auto& id = i < request.trigger_bindings.size()
                         ? request.trigger_bindings[i].id
                         : request.level_bindings[i - request.trigger_bindings.size()].id;
    const auto& audit = candidate.audits[i];
    result.bindings.push_back({.binding_id = id,
                               .accepted = candidate.bindings[i] == 1,
                               .expired = candidate.bindings[i] == 2,
                               .skipped_count = audit.skipped_count,
                               .last_slot_or_time = audit.last_slot_or_time,
                               .reason = audit.reason});
  }

  return result;
}

StatusOr<ProcessResult> EventProcessor::resolve_pending_commit() noexcept {
  if (impl_->active || !impl_->pending_operation || !impl_->pending_state ||
      !impl_->engine->has_pending_commit())
    return Status(ErrorCode::FailedPrecondition, "no resolvable lifecycle commit");
  if (impl_->config.requests) {
    ProcessResult result;
    result.pending = impl_->pending_info(*impl_->pending_operation);
    if (impl_->pending_owner == PendingCommitOwner::Request) {
      auto resolved = impl_->requests->resolve_pending_commit();
      if (!resolved.ok()) {
        result.operation_status = resolved.status();
        result.commit = {.code = CommitCode::Unknown,
                         .revision = impl_->engine->revision(),
                         .duplicate = false,
                         .message = "request remains unresolved"};
        return result;
      }

      result.commit = resolved->commit;
      if (resolved->commit.code != CommitCode::Committed) {
        result.operation_status = {resolved->commit.code == CommitCode::Unknown
                                       ? ErrorCode::UnknownCommit
                                       : ErrorCode::Conflict,
                                   "request commit remains unresolved or rejected"};
        return result;
      }

      impl_->state = std::move(*impl_->pending_state);
      impl_->pending_state.reset();
      impl_->publish_request(std::move(resolved).value(), result, *impl_->pending_request_result,
                             impl_->pending_specified_request);
    } else {
      auto resolved = impl_->engine->resolve_pending_commit();
      if (!resolved.ok()) {
        result.operation_status = resolved.status();
        result.commit = {.code = CommitCode::Unknown,
                         .revision = impl_->engine->revision(),
                         .duplicate = false,
                         .message = "lifecycle remains unresolved"};
        return result;
      }

      result.commit = resolved->commit;
      if (resolved->commit.code != CommitCode::Committed) {
        result.operation_status = {resolved->commit.code == CommitCode::Unknown
                                       ? ErrorCode::UnknownCommit
                                       : ErrorCode::Conflict,
                                   "lifecycle commit remains unresolved or rejected"};
        return result;
      }

      impl_->state = std::move(*impl_->pending_state);
      impl_->pending_state.reset();
      result.committed_rounds.push_back({.revision = impl_->engine->revision(),
                                         .processing_time_us = impl_->state.time_us,
                                         .changes = std::move(resolved->changes),
                                         .occurrences = std::move(impl_->pending_occurrences),
                                         .clock_firings = std::move(impl_->pending_clock_firings)});
      if (impl_->pending_request_result) {
        result.committed_rounds.back().requests.push_back(*impl_->pending_request_result);
        if (impl_->pending_specified_request)
          result.request_result = impl_->pending_request_result;
      }

      result.input_committed =
          impl_->pending_final && impl_->pending_operation->kind == OperationKind::Data;
    }

    auto operation = std::move(*impl_->pending_operation);
    if (operation.phase == 4 && impl_->pending_request_result &&
        impl_->pending_request_result->disposition == RequestDisposition::Accepted)
      operation.recheck_intents = true;

    impl_->pending_operation.reset();
    impl_->pending_request_result.reset();
    impl_->pending_owner = PendingCommitOwner::Lifecycle;
    result.pending.reset();
    impl_->active = true;
    result = impl_->run(std::move(operation), std::move(result));
    impl_->active = false;

    return result;
  }

  auto resolved = impl_->engine->resolve_pending_commit();
  if (!resolved.ok())
    return resolved.status();

  ProcessResult result;
  result.commit = resolved->commit;
  if (result.commit.code != CommitCode::Committed) {
    result.operation_status = {result.commit.code == CommitCode::Unknown ? ErrorCode::UnknownCommit
                                                                         : ErrorCode::Conflict,
                               "lifecycle commit remains unresolved or rejected"};
    return result;
  }

  impl_->state = std::move(*impl_->pending_state);
  impl_->pending_state.reset();
  result.committed_rounds.push_back({.revision = impl_->engine->revision(),
                                     .processing_time_us = impl_->state.time_us,
                                     .changes = std::move(resolved->changes),
                                     .occurrences = std::move(impl_->pending_occurrences),
                                     .clock_firings = std::move(impl_->pending_clock_firings)});
  auto operation = std::move(*impl_->pending_operation);
  impl_->pending_operation.reset();
  if (impl_->pending_final) {
    result.input_committed = operation.kind == OperationKind::Data;
    return result;
  }

  impl_->active = true;
  result = impl_->run(std::move(operation), std::move(result));
  impl_->active = false;
  return result;
}

StatusOr<std::vector<TriggerOccurrence>>
EventProcessor::pending_occurrences(std::size_t limit) const noexcept {
  EF_RETURN_IF_ERROR(impl_->ready());
  if (limit == 0 || limit > impl_->config.max_occurrences)
    return bad("invalid outbox read limit");

  return std::vector<TriggerOccurrence>(impl_->state.outbox.begin(),
                                        impl_->state.outbox.begin() +
                                            std::min(limit, impl_->state.outbox.size()));
}

StatusOr<ProcessResult>
EventProcessor::acknowledge_occurrences(const std::vector<std::string>& ids) noexcept {
  EF_RETURN_IF_ERROR(impl_->ready());
  if (ids.empty() || ids.size() > impl_->config.max_occurrences)
    return bad("invalid outbox acknowledgement size");

  std::set<std::string> distinct;
  for (const auto& id : ids) {
    if (!distinct.insert(id).second)
      return bad("duplicate occurrence acknowledgement");
    if (impl_->state.intents.contains(id))
      return Status(ErrorCode::FailedPrecondition, "request occurrence requires atomic admission");
    if (std::none_of(impl_->state.outbox.begin(), impl_->state.outbox.end(),
                     [&](const TriggerOccurrence& value) noexcept { return value.id == id; }))
      return Status(ErrorCode::NotFound, "occurrence is not pending");
  }

  impl_->active = true;
  auto result = impl_->run({.kind = OperationKind::Acknowledge,
                            .event = {},
                            .payload = {},
                            .target_us = impl_->state.time_us,
                            .ids = ids});
  impl_->active = false;

  return result;
}

StatusOr<std::vector<ProcessedPosition>> EventProcessor::processed_positions() const noexcept {
  EF_RETURN_IF_ERROR(impl_->ready());
  std::vector<ProcessedPosition> result;
  for (const auto& [key, position] : impl_->state.positions) {
    (void)key;
    result.push_back(position);
  }

  return result;
}

StatusOr<std::vector<PeriodicPosition>> EventProcessor::periodic_positions() const noexcept {
  EF_RETURN_IF_ERROR(impl_->ready());
  std::vector<PeriodicPosition> result;
  for (const auto& [id, clock] : impl_->state.clocks) {
    (void)id;
    result.push_back({.policy_id = impl_->config.periodic_policies[clock.policy].id,
                      .scope = impl_->state.keys.find(clock.key)->second.scope,
                      .next_slot_us = clock.next_us,
                      .last_slot_us = clock.last_us,
                      .skipped_slots = clock.skipped});
  }

  return result;
}

StatusOr<FsmState> EventProcessor::read_fsm(const std::string& rule_id,
                                            const Row& scope) const noexcept {
  EF_RETURN_IF_ERROR(impl_->ready());
  EF_RETURN_IF_ERROR(impl_->validate_scope(scope));
  EF_ASSIGN_OR_RETURN(auto key, scope_key(scope));
  const auto found = impl_->state.keys.find(key);
  if (found == impl_->state.keys.end())
    return Status(ErrorCode::NotFound, "business key has no FSM state");

  for (std::size_t i = 0; i < impl_->config.rules.size(); ++i)
    if (impl_->config.rules[i].definition.id == rule_id)
      return FsmState{found->second.rules[i]};

  return Status(ErrorCode::NotFound, "unknown FSM rule");
}

StatusOr<CepStateSummary> EventProcessor::read_cep(const std::string& rule_id,
                                                   const Row& scope) const noexcept {
  EF_RETURN_IF_ERROR(impl_->ready());
  EF_RETURN_IF_ERROR(impl_->validate_scope(scope));
  EF_ASSIGN_OR_RETURN(auto key, scope_key(scope));
  const auto found = impl_->state.keys.find(key);
  if (found == impl_->state.keys.end())
    return Status(ErrorCode::NotFound, "business key has no CEP state");

  for (std::size_t i = 0; i < impl_->config.cep_rules.size(); ++i)
    if (impl_->config.cep_rules[i].definition.id == rule_id)
      return impl_->cep_programs[i]->inspect(found->second.cep[i]);

  return Status(ErrorCode::NotFound, "unknown CEP rule");
}

StatusOr<QueryResult> EventProcessor::query_sql(std::string_view sql,
                                                const QueryContext& context) noexcept {
  EF_RETURN_IF_ERROR(impl_->ready());
  return impl_->engine->query_sql(sql, context);
}

std::int64_t EventProcessor::processing_time_us() const noexcept {
  return impl_->state.time_us;
}

const EngineConfig& EventProcessor::engine_config() const noexcept {
  return impl_->engine_config;
}

bool EventProcessor::has_pending_commit() const noexcept {
  return impl_->engine->has_pending_commit();
}

bool EventProcessor::failed() const noexcept {
  return impl_->engine->failed();
}
} // namespace eventflow
