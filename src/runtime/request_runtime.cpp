#include "eventflow/request_runtime.hpp"
#include "engine_internal.hpp"
#include "eventflow/key_codec.hpp"
#include <algorithm>
#include <map>
#include <set>
#include <tuple>

namespace eventflow {
namespace {
Status invalid(std::string_view message) noexcept {
  return {ErrorCode::InvalidArgument, message};
}

bool name_ok(std::string_view text) noexcept {
  return !text.empty() && text.size() <= 1024 && valid_utf8(text);
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

bool members(const Json& json, std::initializer_list<const char*> names) noexcept {
  if (!json.is_object() || json.size() != names.size())
    return false;

  for (const auto* name : names)
    if (!json.contains(name))
      return false;

  return true;
}

StatusOr<std::uint64_t> unsigned_value(const Json& json) noexcept {
  if (!json.is_number_integer() || (!json.is_number_unsigned() && json.get<std::int64_t>() < 0))
    return invalid("expected nonnegative integer");

  return json.get<std::uint64_t>();
}

StatusOr<Json> row_json(const Row& row) noexcept {
  Json result = Json::array();
  for (const auto& value : row) {
    EF_ASSIGN_OR_RETURN(auto encoded, value_to_json(value));
    result.push_back(std::move(encoded));
  }

  return result;
}

StatusOr<Row> parse_row(const Json& json) noexcept {
  if (!json.is_array())
    return invalid("expected typed row array");

  Row result;
  for (const auto& value : json) {
    DataType type;
    if (value.is_null()) {
      result.emplace_back(std::monostate{});
      continue;
    }

    if (value.is_boolean())
      type = DataType::Bool;
    else if (value.is_number_integer())
      type = DataType::Int64;
    else if (value.is_number_float())
      type = DataType::Float64;
    else if (value.is_string())
      type = DataType::String;
    else if (value.is_array())
      type = DataType::FloatVector;
    else
      return invalid("unsupported request argument value");
    EF_ASSIGN_OR_RETURN(auto decoded, value_from_json(value, type, false));
    result.push_back(std::move(decoded));
  }

  return result;
}

StatusOr<Json> admission_json(const RequestAdmission& admission) noexcept {
  EF_ASSIGN_OR_RETURN(auto scope, row_json(admission.scope));
  EF_ASSIGN_OR_RETURN(auto arguments, row_json(admission.arguments));
  return Json{{"plan", admission.plan},
              {"scope", std::move(scope)},
              {"request_id", admission.request_id},
              {"trigger_id", admission.trigger_id},
              {"candidate_epoch", admission.candidate_epoch},
              {"lifetime_us", admission.lifetime_us},
              {"arguments", std::move(arguments)}};
}

StatusOr<RequestAdmission> parse_admission(const Json& json) noexcept {
  if (!members(json, {"plan", "scope", "request_id", "trigger_id", "candidate_epoch", "lifetime_us",
                      "arguments"}) ||
      !json["plan"].is_string() || !json["request_id"].is_string() ||
      !json["trigger_id"].is_string())
    return invalid("invalid request admission record");

  RequestAdmission result;
  result.plan = json["plan"].get<std::string>();
  result.request_id = json["request_id"].get<std::string>();
  result.trigger_id = json["trigger_id"].get<std::string>();
  EF_ASSIGN_OR_RETURN(result.scope, parse_row(json["scope"]));
  EF_ASSIGN_OR_RETURN(result.arguments, parse_row(json["arguments"]));
  EF_ASSIGN_OR_RETURN(result.candidate_epoch, unsigned_value(json["candidate_epoch"]));
  EF_ASSIGN_OR_RETURN(auto lifetime, unsigned_value(json["lifetime_us"]));
  EF_RETURN_IF_ERROR(check(lifetime <= INT64_MAX, "request lifetime overflow"));
  result.lifetime_us = static_cast<std::int64_t>(lifetime);

  return result;
}

bool terminal(RequestPhase phase) noexcept {
  return phase == RequestPhase::Success || phase == RequestPhase::Failed ||
         phase == RequestPhase::Cancelled;
}

StatusOr<std::string> status_phase(RequestPhase phase) noexcept {
  switch (phase) {
  case RequestPhase::Accepted:
    return std::string("ACCEPTED");
  case RequestPhase::Running:
    return std::string("RUNNING");
  case RequestPhase::Success:
    return std::string("SUCCEEDED");
  case RequestPhase::Failed:
    return std::string("FAILED");
  case RequestPhase::Cancelled:
    return std::string("CANCELLED");
  }

  return invalid("invalid request status phase");
}

StatusOr<std::string> attempt_outcome(RequestAttemptOutcome outcome) noexcept {
  switch (outcome) {
  case RequestAttemptOutcome::Succeeded:
    return std::string("SUCCEEDED");
  case RequestAttemptOutcome::Failed:
    return std::string("FAILED");
  case RequestAttemptOutcome::Cancelled:
    return std::string("CANCELLED");
  case RequestAttemptOutcome::TimedOut:
    return std::string("TIMED_OUT");
  }

  return invalid("invalid request attempt settlement outcome");
}

Status observation_cause(RuntimeCause& cause, const RequestRecord& record,
                         std::vector<RequestAttemptSettlement> settlements,
                         bool first_terminal) noexcept {
  if (settlements.size() > record.state.nodes.size())
    return invalid("request attempt summaries exceed the bounded node set");

  std::sort(
      settlements.begin(), settlements.end(), [](const auto& left, const auto& right) noexcept {
        return std::tie(left.relation, left.attempt) < std::tie(right.relation, right.attempt);
      });
  cause.inputs.clear();
  const auto make_input = [&](ObservationInputKind kind) noexcept {
    RuntimeObservationInput input;
    input.kind = kind;
    input.origin = record.admission.plan;
    input.scope = record.state.manifest.scope;
    input.values = {record.admission.plan, record.state.manifest.request_id,
                    std::to_string(record.state.manifest.candidate_epoch)};
    return input;
  };

  for (std::size_t i = 0; i < settlements.size(); ++i) {
    const auto& settled = settlements[i];
    if (settled.relation == 0 || settled.attempt == 0 ||
        (i > 0 && settlements[i - 1].relation == settled.relation &&
         settlements[i - 1].attempt == settled.attempt))
      return invalid("invalid or duplicate request attempt settlement");

    auto input = make_input(ObservationInputKind::AttemptSettled);
    input.relation = settled.relation;
    input.values.emplace_back(static_cast<std::int64_t>(settled.relation));
    input.values.emplace_back(static_cast<std::int64_t>(settled.attempt));
    EF_ASSIGN_OR_RETURN(auto outcome, attempt_outcome(settled.outcome));
    input.values.emplace_back(std::move(outcome));
    cause.inputs.push_back(std::move(input));
  }

  if (first_terminal) {
    auto input = make_input(ObservationInputKind::RequestTerminal);
    EF_ASSIGN_OR_RETURN(auto outcome, status_phase(record.state.phase));
    input.values.emplace_back(std::move(outcome));
    cause.inputs.push_back(std::move(input));
  }

  return {};
}

struct CallGuard {
  explicit CallGuard(bool& flag) noexcept : active(flag) {
    active = true;
  }

  ~CallGuard() noexcept {
    active = false;
  }

  bool& active;
};
enum class Control { Claim, Complete, Advance, Cancel };
} // namespace

struct RequestRuntime::Impl {
  struct Plan {
    RequestPlanDefinition definition;
    std::unique_ptr<RequestMachine> machine;
    std::set<RelationId> captures;
    std::vector<DataType> scope_types;
  };

  struct Book {
    std::uint64_t next_sequence = 1;
    std::map<std::string, RequestRecord> active;
  };

  struct Pending {
    Book book;
    RequestRuntimeResult result;
  };

  Engine* engine = nullptr;
  std::shared_ptr<IStateStore> store;
  EngineConfig engine_config;
  RequestRuntimeConfig config;
  CatalogSnapshot catalog;
  std::map<std::string, Plan, std::less<>> plans;
  std::string index_key;
  std::string fingerprint;
  Book book;
  std::optional<Pending> pending;
  bool active = false;
  bool attached = false;

  ~Impl() noexcept {
    if (attached)
      EngineRuntimeAccess::detach_request_owner(*engine);
  }

  Status ready() const noexcept {
    if (active || pending || engine->has_pending_commit() || engine->failed())
      return {ErrorCode::FailedPrecondition, "request owner is busy, failed or awaiting a commit"};

    EF_ASSIGN_OR_RETURN(auto status, store->status(engine_config.shard));
    if (status.revision != engine->revision())
      return {ErrorCode::Conflict, "request owner revision is stale"};

    return {};
  }

  StatusOr<std::string> key(const Row& scope, std::string_view request_id) const noexcept {
    EF_RETURN_IF_ERROR(check(name_ok(request_id) && !scope.empty(), "invalid request identity"));
    EF_ASSIGN_OR_RETURN(auto shard, shard_for(scope, engine_config.shard_count));
    EF_RETURN_IF_ERROR(check(shard == engine_config.shard, "request belongs to another shard"));
    EF_ASSIGN_OR_RETURN(auto encoded, encode_key_parts(scope));
    EF_ASSIGN_OR_RETURN(auto hex, hex_encode(encoded));

    return row_key(engine_config.shard, 'S', config.namespace_id,
                   {std::string("request"), std::move(hex), std::string(request_id)});
  }

  Status validate_admission(const RequestAdmission& admission) const noexcept {
    EF_RETURN_IF_ERROR(check(name_ok(admission.plan) && name_ok(admission.request_id) &&
                                 admission.trigger_id.size() <= 4096 &&
                                 valid_utf8(admission.trigger_id) && admission.lifetime_us >= 0,
                             "invalid request admission"));
    const auto found = plans.find(admission.plan);
    if (found == plans.end())
      return {ErrorCode::NotFound, "unknown request plan"};
    if (found->second.definition.request_status_relation &&
        (admission.candidate_epoch == 0 || admission.candidate_epoch > INT64_MAX))
      return {ErrorCode::ResourceExhausted, "request candidate epoch exceeds status integer range"};

    const auto& types = found->second.scope_types;
    EF_RETURN_IF_ERROR(
        check(admission.scope.size() == types.size(), "request scope arity mismatch"));
    for (std::size_t i = 0; i < types.size(); ++i) {
      EF_ASSIGN_OR_RETURN(auto json, value_to_json(admission.scope[i]));
      EF_ASSIGN_OR_RETURN(auto typed, value_from_json(json, types[i], false));
      EF_RETURN_IF_ERROR(
          check(typed.index() == admission.scope[i].index(), "request scope type mismatch"));
    }

    EF_ASSIGN_OR_RETURN(auto encoded, row_json(admission.arguments));
    if (encoded.dump().size() > config.max_argument_bytes)
      return {ErrorCode::ResourceExhausted, "request arguments exceed budget"};

    EF_ASSIGN_OR_RETURN(auto envelope, admission_json(admission));
    if (envelope.dump().size() + 64 > config.max_record_bytes / 2)
      return {ErrorCode::ResourceExhausted,
              "request admission envelope exceeds record reservation"};

    EF_ASSIGN_OR_RETURN(auto unused, key(admission.scope, admission.request_id));
    (void)unused;
    return {};
  }

  Status reserve_transition(const RequestAdmission& admission) const noexcept {
    const auto& plan = plans.find(admission.plan)->second;

    // Include the finite Catalog closure, not just record/index/status. A completion
    // may materialize result, Latest, hidden selector/aggregate state and SQL children.
    std::set<RelationId> affected = plan.captures;
    if (plan.definition.request_status_relation)
      affected.insert(*plan.definition.request_status_relation);

    std::size_t mutations = 3; // Request record, active index and Engine metadata.
    for (const auto& relation : catalog.relations) {
      EF_ASSIGN_OR_RETURN(auto inputs, dependencies(*relation));
      if (std::any_of(inputs.begin(), inputs.end(),
                      [&](RelationId id) noexcept { return affected.contains(id); }))
        affected.insert(relation->id);
      if (!affected.contains(relation->id))
        continue;

      const std::size_t count =
          1 + (relation->latest || (relation->plan && !relation->plan->group_by.empty()) ? 1 : 0);
      if (mutations > engine_config.max_mutations ||
          count > engine_config.max_mutations - mutations)
        return {ErrorCode::ResourceExhausted,
                "request Catalog closure exceeds its transition mutation reservation"};

      mutations += count;
    }

    // Every plan needs its record, index and Engine metadata even without a typed
    // RequestStatus relation. Reserve full bounds before capture and on recovery.
    // This is the owned metadata tuple only;
    // arbitrary downstream SQL and companion writes need their owner's separate budget.
    EF_ASSIGN_OR_RETURN(auto record_key, key(admission.scope, admission.request_id));
    EF_ASSIGN_OR_RETURN(auto metadata_bytes, EngineRuntimeAccess::metadata_bytes(*engine));
    std::size_t reserved = 0;
    const auto add = [&](std::size_t size) noexcept -> Status {
      if (size > engine_config.max_bytes - reserved)
        return {ErrorCode::ResourceExhausted,
                "request metadata exceeds its terminal byte reservation"};

      reserved += size;
      return {};
    };
    for (const auto size : {config.max_record_bytes, config.max_index_bytes, record_key.size(),
                            index_key.size(), metadata_bytes})
      EF_RETURN_IF_ERROR(add(size));

    if (!plan.definition.request_status_relation)
      return {};

    EF_ASSIGN_OR_RETURN(auto relation, catalog.find(*plan.definition.request_status_relation));
    Row identity = admission.scope;
    identity.emplace_back(admission.request_id);
    Row row = identity;
    row.emplace_back(static_cast<std::int64_t>(admission.candidate_epoch));
    row.emplace_back(std::int64_t{INT64_MAX});
    row.emplace_back(std::string("CANCELLED")); // SUCCEEDED has the same maximum phase length.
    EF_ASSIGN_OR_RETURN(auto status_bytes, encode_row(relation->storage_schema, row));
    EF_ASSIGN_OR_RETURN(auto status_key, row_key(engine_config.shard, 'T', relation->id, identity));
    EF_RETURN_IF_ERROR(add(status_key.size()));
    EF_RETURN_IF_ERROR(add(status_bytes.size()));

    // The three owned mutations also need the Engine's mandatory metadata mutation.
    if (engine_config.max_mutations < 4)
      return {ErrorCode::ResourceExhausted,
              "request record, index and status exceed the terminal mutation reservation"};

    return {};
  }

  Status validate_candidate(RoundContext& context, const RequestAdmission& admission) noexcept {
    const auto& plan = plans.find(admission.plan)->second;
    if (!plan.definition.request_status_relation)
      return {};

    EF_ASSIGN_OR_RETURN(auto status, catalog.find(*plan.definition.request_status_relation));
    EF_ASSIGN_OR_RETURN(auto candidate,
                        context.read_relation(status->request_status->definition.candidate_relation,
                                              admission.scope));
    if (!candidate || !std::get<bool>((*candidate)[admission.scope.size() + 1]) ||
        std::get<std::int64_t>((*candidate)[admission.scope.size()]) !=
            static_cast<std::int64_t>(admission.candidate_epoch))
      return {ErrorCode::FailedPrecondition, "request does not match the active candidate"};

    return {};
  }

  StatusOr<std::vector<TableChange>>
  status_and_companion(RoundContext& context, const RequestRecord& record,
                       const RequestCommitHook& companion) noexcept {
    std::vector<TableChange> writes;
    const auto& plan = plans.find(record.admission.plan)->second;
    if (plan.definition.request_status_relation) {
      EF_ASSIGN_OR_RETURN(auto status, EngineRuntimeAccess::make_request_status_write(
                                           context, *plan.definition.request_status_relation,
                                           record.state.manifest, record.state.phase));
      writes.push_back(std::move(status));
    }

    if (companion) {
      EF_ASSIGN_OR_RETURN(auto additional, companion(context, record));
      for (auto& write : additional)
        writes.push_back(std::move(write));
    }

    return writes;
  }

  StatusOr<std::string> encode_record(const RequestRecord& record) const noexcept {
    EF_RETURN_IF_ERROR(validate_admission(record.admission));
    const auto found = plans.find(record.admission.plan);
    EF_ASSIGN_OR_RETURN(auto encoded, found->second.machine->encode(record.state));
    EF_ASSIGN_OR_RETURN(auto state, parse_json_exact(encoded));
    EF_ASSIGN_OR_RETURN(auto admission, admission_json(record.admission));
    auto result =
        Json{{"format", 1}, {"admission", std::move(admission)}, {"state", std::move(state)}}
            .dump();
    if (result.size() > config.max_record_bytes)
      return Status(ErrorCode::ResourceExhausted, "request record exceeds budget");

    return result;
  }

  StatusOr<RequestRecord> decode_record(std::string_view bytes, const Row& scope,
                                        std::string_view request_id) const noexcept {
    EF_RETURN_IF_ERROR(
        check(bytes.size() <= config.max_record_bytes, "request record exceeds read budget"));
    EF_ASSIGN_OR_RETURN(auto json, parse_json_exact(bytes));
    if (!members(json, {"format", "admission", "state"}) || json["format"] != 1 ||
        !json["state"].is_object() || !json["state"].contains("manifest"))
      return invalid("invalid stored request record");

    RequestRecord record;
    EF_ASSIGN_OR_RETURN(record.admission, parse_admission(json["admission"]));
    EF_RETURN_IF_ERROR(validate_admission(record.admission));
    if (record.admission.scope != scope || record.admission.request_id != request_id)
      return invalid("stored request does not match its lookup identity");

    EF_ASSIGN_OR_RETURN(auto manifest, request_manifest_from_json(json["state"]["manifest"]));
    const auto& plan = plans.find(record.admission.plan)->second;
    if (manifest.scope != scope || manifest.request_id != request_id ||
        manifest.owner_token != engine_config.owner || manifest.owner_epoch != config.owner_epoch ||
        manifest.candidate_epoch != record.admission.candidate_epoch ||
        manifest.catalog_fingerprint != catalog.fingerprint ||
        manifest.targets != plan.definition.targets || manifest.admission_seq >= book.next_sequence)
      return invalid("stored request manifest does not match owner provenance");
    if (plan.definition.request_status_relation &&
        (manifest.admission_seq == 0 || manifest.admission_seq > INT64_MAX))
      return invalid("stored request sequence exceeds status integer range");

    EF_ASSIGN_OR_RETURN(record.state, plan.machine->decode(json["state"].dump(), manifest));
    return record;
  }

  StatusOr<std::string> encode_index(const Book& candidate) const noexcept {
    if (candidate.active.size() > config.max_active_requests)
      return Status(ErrorCode::ResourceExhausted, "active request count limit reached");

    std::size_t bytes = 0;
    Json active = Json::array();
    for (const auto& [id, record] : candidate.active) {
      (void)id;
      EF_ASSIGN_OR_RETURN(auto encoded, encode_record(record));
      EF_ASSIGN_OR_RETURN(auto admission, admission_json(record.admission));
      const auto& plan = plans.find(record.admission.plan)->second;

      // Reserve future node/task growth at admission. Charging only today's Ready
      // bytes could accept a request that can never acquire its first task.
      const auto reserved = std::max(encoded.size(), plan.definition.machine.max_state_bytes +
                                                         admission.dump().size() + 64);
      if (reserved > config.max_active_bytes - bytes)
        return Status(ErrorCode::ResourceExhausted, "active request byte limit reached");

      bytes += reserved;
      EF_ASSIGN_OR_RETURN(auto scope, row_json(record.admission.scope));
      active.push_back({{"plan", record.admission.plan},
                        {"scope", std::move(scope)},
                        {"request_id", record.admission.request_id}});
    }

    auto result = Json{{"format", 1},
                       {"fingerprint", fingerprint},
                       {"next_sequence", candidate.next_sequence},
                       {"active", std::move(active)}}
                      .dump();
    if (result.size() > config.max_index_bytes)
      return Status(ErrorCode::ResourceExhausted, "request index byte limit reached");

    return result;
  }

  StatusOr<RequestRecord> load(const Row& scope, std::string_view request_id) noexcept {
    EF_ASSIGN_OR_RETURN(auto id, key(scope, request_id));
    const auto found = book.active.find(id);
    if (found != book.active.end())
      return RequestRecord(found->second);

    EF_ASSIGN_OR_RETURN(auto read, store->get_many(engine_config.shard, {id}));
    if (read.revision != engine->revision() || read.values.size() != 1)
      return Status(ErrorCode::Conflict, "request read revision mismatch");
    if (!read.values[0])
      return Status(ErrorCode::NotFound, "request does not exist");

    EF_ASSIGN_OR_RETURN(auto record, decode_record(*read.values[0], scope, request_id));
    const auto& plan = plans.find(record.admission.plan)->second;
    if (plan.definition.request_status_relation) {
      EF_ASSIGN_OR_RETURN(auto relation, catalog.find(*plan.definition.request_status_relation));
      Row row = record.state.manifest.identity;
      row.emplace_back(static_cast<std::int64_t>(record.state.manifest.candidate_epoch));
      row.emplace_back(static_cast<std::int64_t>(record.state.manifest.admission_seq));
      EF_ASSIGN_OR_RETURN(auto phase, status_phase(record.state.phase));
      row.emplace_back(std::move(phase));
      EF_ASSIGN_OR_RETURN(auto expected, encode_row(relation->storage_schema, row));
      EF_ASSIGN_OR_RETURN(auto status_key, row_key(engine_config.shard, 'T', relation->id,
                                                   record.state.manifest.identity));
      EF_ASSIGN_OR_RETURN(auto status, store->get_many(engine_config.shard, {status_key}));
      if (status.revision != engine->revision() || status.values.size() != 1)
        return Status(ErrorCode::Conflict, "request status read revision mismatch");
      if (!status.values[0] || *status.values[0] != expected)
        return Status(ErrorCode::FailedPrecondition,
                      "stored request and controlled status disagree");
    }

    return record;
  }

  Status stage(RoundContext& context, Book& candidate, const RequestRecord& record) noexcept {
    EF_ASSIGN_OR_RETURN(auto id, key(record.admission.scope, record.admission.request_id));
    EF_ASSIGN_OR_RETURN(auto encoded, encode_record(record));
    if (terminal(record.state.phase))
      candidate.active.erase(id);
    else
      candidate.active.insert_or_assign(id, record);
    EF_ASSIGN_OR_RETURN(auto index, encode_index(candidate));
    EF_RETURN_IF_ERROR(EngineRuntimeAccess::stage_request_state(
        context, {.key = std::move(id), .value = std::move(encoded)}));
    return EngineRuntimeAccess::stage_request_state(context,
                                                    {.key = index_key, .value = std::move(index)});
  }

  StatusOr<RequestRuntimeResult> publish(StatusOr<DispatchResult> outcome, Book candidate,
                                         RequestRuntimeResult result) noexcept {
    if (!outcome.ok()) {
      if (engine->has_pending_commit())
        pending = Pending{.book = std::move(candidate), .result = std::move(result)};

      return outcome.status();
    }

    if (outcome->commit.code == CommitCode::Unknown) {
      // Retain tasks privately until their request state is confirmed; workers must not execute
      // work that the owner has not yet durably accepted.
      pending = Pending{.book = std::move(candidate), .result = std::move(result)};
      RequestRuntimeResult uncertain;
      uncertain.commit = outcome->commit;
      return uncertain;
    }

    result.commit = outcome->commit;
    if (outcome->commit.code != CommitCode::Committed) {
      result.record.reset();
      result.tasks.clear();
      result.cancel_task_ids.clear();
      result.accepted = false;

      return result;
    }

    book = std::move(candidate);
    result.changes = std::move(outcome->changes);
    return result;
  }

  StatusOr<RequestRuntimeResult> control(Control kind, const Row& scope,
                                         std::string_view request_id, std::int64_t now_us,
                                         std::size_t max_tasks,
                                         const FunctionCompletion* completion,
                                         const RequestCommitHook& companion) noexcept;
};

RequestRuntime::RequestRuntime(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}

RequestRuntime::~RequestRuntime() noexcept = default;

StatusOr<std::unique_ptr<RequestRuntime>> RequestRuntime::open(
    Engine& engine, std::shared_ptr<IStateStore> store, std::shared_ptr<FunctionRegistry> registry,
    std::vector<RequestPlanDefinition> definitions, const RequestRuntimeConfig& config) noexcept {
  EF_RETURN_IF_ERROR(check(store && registry && !definitions.empty() && definitions.size() <= 128 &&
                               config.namespace_id != 0 && config.max_active_requests > 0 &&
                               config.max_active_bytes > 0 && config.max_record_bytes >= 128 &&
                               config.max_index_bytes > 0 && config.max_argument_bytes > 0,
                           "invalid request runtime configuration"));
  EF_RETURN_IF_ERROR(check(store.get() == EngineRuntimeAccess::state_store(engine).get(),
                           "request owner must use its Engine state store"));
  auto impl = std::make_unique<Impl>();
  impl->engine = &engine;
  impl->store = std::move(store);
  impl->config = config;
  impl->engine_config = EngineRuntimeAccess::config(engine);
  EF_ASSIGN_OR_RETURN(impl->catalog, engine.snapshot());

  for (const auto& relation : impl->catalog.relations)
    EF_RETURN_IF_ERROR(check(relation->state_id != config.namespace_id,
                             "request namespace collides with aggregate state"));

  Json descriptions = Json::array();
  std::map<std::string, RelationId> status_bindings;
  for (auto& definition : definitions) {
    // Keep a separate envelope allowance. The machine reserves its own worst terminal
    // representation, so cancellation cannot be blocked by this outer record wrapper.
    definition.machine.max_state_bytes =
        std::min(definition.machine.max_state_bytes, config.max_record_bytes / 2);
    EF_RETURN_IF_ERROR(
        check(name_ok(definition.name) && definition.version > 0 && definition.capture &&
                  definition.default_lifetime_us > 0 &&
                  definition.default_lifetime_us <= definition.machine.max_lifetime_us,
              "invalid request plan"));
    if (impl->plans.contains(definition.name))
      return invalid("duplicate request plan name");

    std::sort(definition.targets.begin(), definition.targets.end());
    Impl::Plan plan;
    EF_ASSIGN_OR_RETURN(
        plan.machine,
        RequestMachine::create(impl->catalog, registry, definition.targets, definition.machine));
    std::set<RelationId> ancestors(definition.targets.begin(), definition.targets.end());

    for (auto it = impl->catalog.relations.rbegin(); it != impl->catalog.relations.rend(); ++it)
      if (ancestors.contains((*it)->id)) {
        EF_ASSIGN_OR_RETURN(auto inputs, dependencies(**it));
        ancestors.insert(inputs.begin(), inputs.end());
        if ((*it)->kind == RelationKind::BaseTable)
          plan.captures.insert((*it)->id);
      }
    EF_ASSIGN_OR_RETURN(auto target, impl->catalog.find(definition.targets.front()));
    plan.scope_types = target->partition_types;
    if (definition.request_status_relation) {
      EF_ASSIGN_OR_RETURN(auto status, impl->catalog.find(*definition.request_status_relation));
      EF_RETURN_IF_ERROR(
          check(status->kind == RelationKind::BaseTable && status->request_status &&
                    status->base_write_role == BaseWriteRole::RuntimeStatus &&
                    status->request_status->definition.request_domain == target->identity_domain &&
                    same_fields(status->request_status->definition.identity_fields,
                                target->identity_fields),
                "request plan status binding does not match its complete identity domain"));
      const auto [binding, inserted] =
          status_bindings.emplace(target->identity_domain, *definition.request_status_relation);
      if (!inserted && binding->second != *definition.request_status_relation)
        return invalid("request domain has conflicting status producer bindings");
    }

    for (const auto& relation : impl->catalog.relations) {
      if (!relation->latest)
        continue;

      EF_ASSIGN_OR_RETURN(auto source,
                          impl->catalog.find(relation->latest->definition.result_relation));
      if (source->identity_domain == target->identity_domain &&
          definition.request_status_relation !=
              relation->latest->definition.request_status_relation)
        return invalid("request plan requires the status binding used by its domain's Latest");
    }

    const auto& c = definition.machine;
    descriptions.push_back(
        {{"name", definition.name},
         {"version", definition.version},
         {"targets", definition.targets},
         {"lifetime", definition.default_lifetime_us},
         {"limits",
          Json::array({c.max_nodes, c.max_state_bytes, c.max_frozen_input_bytes, c.max_output_bytes,
                       c.max_identifier_bytes, c.max_running_tasks, c.max_attempts,
                       c.attempt_timeout_us, c.retry_backoff_us, c.max_lifetime_us})}});

    // Omit the field entirely for legacy plans so existing fingerprints remain byte-identical.
    if (definition.request_status_relation)
      descriptions.back()["request_status_relation"] = *definition.request_status_relation;

    const auto name = definition.name;
    plan.definition = std::move(definition);
    impl->plans.emplace(name, std::move(plan));
  }

  impl->fingerprint = Json{{"catalog", impl->catalog.fingerprint},
                           {"plans", std::move(descriptions)},
                           {"namespace", config.namespace_id},
                           {"owner_epoch", config.owner_epoch}}
                          .dump();
  EF_ASSIGN_OR_RETURN(impl->index_key, row_key(impl->engine_config.shard, 'S', config.namespace_id,
                                               {std::string("index")}));
  EF_RETURN_IF_ERROR(impl->ready());
  EF_RETURN_IF_ERROR(EngineRuntimeAccess::attach_request_owner(engine, config.namespace_id));
  impl->attached = true;
  EF_ASSIGN_OR_RETURN(auto read,
                      impl->store->get_many(impl->engine_config.shard, {impl->index_key}));
  if (read.revision != engine.revision() || read.values.size() != 1)
    return Status(ErrorCode::Conflict, "request index revision mismatch");
  if (read.values[0]) {
    EF_RETURN_IF_ERROR(
        check(read.values[0]->size() <= config.max_index_bytes, "request index exceeds budget"));
    EF_ASSIGN_OR_RETURN(auto json, parse_json_exact(*read.values[0]));
    if (!members(json, {"format", "fingerprint", "next_sequence", "active"}) ||
        json["format"] != 1 || json["fingerprint"] != impl->fingerprint ||
        !json["active"].is_array() || json["active"].size() > config.max_active_requests)
      return invalid("request index layout or configuration mismatch");

    EF_ASSIGN_OR_RETURN(impl->book.next_sequence, unsigned_value(json["next_sequence"]));
    EF_RETURN_IF_ERROR(check(impl->book.next_sequence > 0, "request sequence is invalid"));
    for (const auto& item : json["active"]) {
      if (!members(item, {"plan", "scope", "request_id"}) || !item["plan"].is_string() ||
          !item["request_id"].is_string())
        return invalid("invalid active request entry");

      EF_ASSIGN_OR_RETURN(auto scope, parse_row(item["scope"]));
      const auto request_id = item["request_id"].get<std::string>();
      EF_ASSIGN_OR_RETURN(auto id, impl->key(scope, request_id));
      if (impl->book.active.contains(id))
        return invalid("duplicate active request identity");

      EF_ASSIGN_OR_RETURN(auto record, impl->load(scope, request_id));
      if (record.admission.plan != item["plan"].get<std::string>() || terminal(record.state.phase))
        return invalid("active index disagrees with request record");

      EF_RETURN_IF_ERROR(impl->reserve_transition(record.admission));
      impl->book.active.emplace(std::move(id), std::move(record));
    }

    EF_ASSIGN_OR_RETURN(auto validated, impl->encode_index(impl->book));
    (void)validated;
  } else {
    EF_ASSIGN_OR_RETURN(auto prefix,
                        table_prefix(impl->engine_config.shard, 'S', config.namespace_id));
    EF_ASSIGN_OR_RETURN(auto existing, impl->store->scan(impl->engine_config.shard, prefix, {}, 1));
    if (existing.revision != engine.revision() || !existing.rows.empty())
      return Status(ErrorCode::FailedPrecondition, "request records exist without an index");

    EF_ASSIGN_OR_RETURN(auto bytes, impl->encode_index(impl->book));
    EF_ASSIGN_OR_RETURN(
        auto committed,
        EngineRuntimeAccess::apply(
            engine, {.processing_time_us = EngineRuntimeAccess::time_us(engine)},
            [&](RoundContext& context) noexcept -> StatusOr<std::vector<TableChange>> {
              EF_RETURN_IF_ERROR(EngineRuntimeAccess::stage_request_state(
                  context, {.key = impl->index_key, .value = bytes}));
              return std::vector<TableChange>{};
            }));
    if (committed.commit.code != CommitCode::Committed)
      return Status(ErrorCode::UnknownCommit, "request index initialization not confirmed");
  }

  return std::unique_ptr<RequestRuntime>(new RequestRuntime(std::move(impl)));
}

StatusOr<RequestRuntimeResult> RequestRuntime::admit(const RequestAdmission& admission,
                                                     std::int64_t now_us,
                                                     const RequestCommitHook& companion) noexcept {
  EF_RETURN_IF_ERROR(impl_->ready());
  EF_RETURN_IF_ERROR(impl_->validate_admission(admission));
  CallGuard guard(impl_->active);
  auto existing = impl_->load(admission.scope, admission.request_id);
  if (existing.ok()) {
    EF_ASSIGN_OR_RETURN(auto first, admission_json(existing->admission));
    EF_ASSIGN_OR_RETURN(auto retry, admission_json(admission));
    if (first.dump() != retry.dump())
      return Status(ErrorCode::Conflict, "request identity is bound to a different admission");

    RequestRuntimeResult duplicate;
    duplicate.commit = {.code = CommitCode::Committed,
                        .revision = impl_->engine->revision(),
                        .duplicate = true,
                        .message = {}};
    duplicate.accepted = true;
    duplicate.duplicate = true;
    duplicate.record = std::move(existing).value();

    return duplicate;
  }

  if (existing.status().code() != ErrorCode::NotFound)
    return existing.status();
  if (impl_->book.active.size() >= impl_->config.max_active_requests ||
      impl_->book.next_sequence == UINT64_MAX)
    return Status(ErrorCode::ResourceExhausted, "request admission capacity exhausted");

  auto& plan = impl_->plans.find(admission.plan)->second;
  if (plan.definition.request_status_relation && impl_->book.next_sequence > INT64_MAX)
    return Status(ErrorCode::ResourceExhausted,
                  "request admission sequence exceeds status integer range");

  EF_RETURN_IF_ERROR(impl_->reserve_transition(admission));
  const auto lifetime =
      admission.lifetime_us == 0 ? plan.definition.default_lifetime_us : admission.lifetime_us;
  EF_RETURN_IF_ERROR(check(now_us >= 0 && lifetime <= plan.definition.machine.max_lifetime_us &&
                               now_us <= INT64_MAX - lifetime,
                           "invalid request deadline"));
  RequestRecord record;
  record.admission = admission;
  auto candidate = impl_->book;
  RequestRuntimeResult result;
  RuntimeCause cause;
  auto outcome = EngineRuntimeAccess::apply(
      *impl_->engine, {.processing_time_us = now_us},
      [&](RoundContext& context) noexcept -> StatusOr<std::vector<TableChange>> {
        if (context.revision() == UINT64_MAX)
          return Status(ErrorCode::ResourceExhausted, "capture revision exhausted");

        // capture_current reads the confirmed revision here. Its captured rows and request state
        // are written together in the next revision, recorded below in the manifest.
        EF_RETURN_IF_ERROR(impl_->validate_candidate(context, admission));
        EF_ASSIGN_OR_RETURN(auto captured, plan.definition.capture(context, admission));
        if (!captured.ready) {
          if (!captured.rows.empty())
            return invalid("deferred capture cannot include mutations");

          result.deferred = true;

          // A missing prerequisite is not a processing-time event. Discard the
          // private frame, including any accidental capture-side staged data.
          return Status(ErrorCode::Unavailable, "request capture is not ready");
        }

        Row identity = admission.scope;
        identity.emplace_back(admission.request_id);
        std::set<RelationId> written;
        for (const auto& row : captured.rows)
          if (!plan.captures.contains(row.relation) || row.identity != identity || !row.row ||
              !written.insert(row.relation).second)
            return invalid(
                "capture must write each declared root at most once at this request identity");

        RequestManifest manifest;
        manifest.scope = admission.scope;
        manifest.identity = std::move(identity);
        manifest.request_id = admission.request_id;
        manifest.admission_seq = candidate.next_sequence++;
        manifest.candidate_epoch = admission.candidate_epoch;
        manifest.owner_token = impl_->engine_config.owner;
        manifest.owner_epoch = impl_->config.owner_epoch;
        manifest.admitted_at_us = now_us;
        manifest.deadline_us = now_us + lifetime;
        manifest.capture_revision = context.revision();
        manifest.admission_revision = context.revision() + 1;
        EF_ASSIGN_OR_RETURN(record.state, plan.machine->begin(std::move(manifest)));

        return std::move(captured.rows);
      },
      [&](RoundContext& context,
          const std::vector<RowChange>&) noexcept -> StatusOr<std::vector<TableChange>> {
        if (result.deferred)
          return std::vector<TableChange>{};

        EF_ASSIGN_OR_RETURN(auto refreshed,
                            plan.machine->refresh(record.state, [&](RelationId relation,
                                                                    const Row& identity) noexcept {
                              return context.read_relation(relation, identity);
                            }));
        record.state = std::move(refreshed.state);
        result.cancel_task_ids = std::move(refreshed.cancel_task_ids);
        EF_RETURN_IF_ERROR(observation_cause(cause, record, std::move(refreshed.settlements),
                                             terminal(record.state.phase)));
        EF_RETURN_IF_ERROR(impl_->stage(context, candidate, record));
        result.accepted = true;
        result.record = record;

        return impl_->status_and_companion(context, record, companion);
      },
      cause);
  if (result.deferred && !outcome.ok()) {
    result.commit = {.code = CommitCode::Committed,
                     .revision = impl_->engine->revision(),
                     .duplicate = true,
                     .message = "request capture deferred without a commit"};
    return result;
  }

  return impl_->publish(std::move(outcome), std::move(candidate), std::move(result));
}

StatusOr<RequestRuntimeResult>
RequestRuntime::Impl::control(Control kind, const Row& scope, std::string_view request_id,
                              std::int64_t now_us, std::size_t max_tasks,
                              const FunctionCompletion* completion,
                              const RequestCommitHook& companion) noexcept {
  EF_RETURN_IF_ERROR(ready());
  CallGuard guard(active);
  EF_ASSIGN_OR_RETURN(auto record, load(scope, request_id));
  const bool was_terminal = terminal(record.state.phase);
  auto& plan = plans.find(record.admission.plan)->second;
  StatusOr<RequestMachineTransition> transition =
      kind == Control::Claim ? plan.machine->claim_ready(record.state, now_us, max_tasks)
      : kind == Control::Complete
          ? plan.machine->prepare_completion(record.state, *completion, now_us)
      : kind == Control::Advance ? plan.machine->advance_time(record.state, now_us)
                                 : plan.machine->cancel(record.state, now_us);
  if (!transition.ok())
    return transition.status();

  EF_ASSIGN_OR_RETURN(auto before, plan.machine->encode(record.state));
  EF_ASSIGN_OR_RETURN(auto after, plan.machine->encode(transition->state));
  if (before == after && !transition->output && transition->tasks.empty() &&
      transition->cancel_task_ids.empty() && transition->settlements.empty()) {
    RequestRuntimeResult result;
    result.commit = {.code = CommitCode::Committed,
                     .revision = engine->revision(),
                     .duplicate = true,
                     .message = {}};
    result.accepted = true;
    result.duplicate = true;
    result.record = std::move(record);

    return result;
  }

  auto candidate = book;
  RequestRuntimeResult result;
  result.accepted = true;
  result.tasks = std::move(transition->tasks);
  result.cancel_task_ids = std::move(transition->cancel_task_ids);
  auto settlements = std::move(transition->settlements);
  record.state = std::move(transition->state);
  auto output = std::move(transition->output);
  RuntimeCause cause;
  auto outcome = EngineRuntimeAccess::apply(
      *engine, {.processing_time_us = now_us},
      [&](RoundContext&) noexcept -> StatusOr<std::vector<TableChange>> {
        std::vector<TableChange> writes;
        if (output)
          writes.push_back(*output);

        return writes;
      },
      [&](RoundContext& context,
          const std::vector<RowChange>&) noexcept -> StatusOr<std::vector<TableChange>> {
        EF_ASSIGN_OR_RETURN(auto refreshed,
                            plan.machine->refresh(record.state, [&](RelationId relation,
                                                                    const Row& identity) noexcept {
                              return context.read_relation(relation, identity);
                            }));
        record.state = std::move(refreshed.state);
        settlements.insert(settlements.end(), refreshed.settlements.begin(),
                           refreshed.settlements.end());
        EF_RETURN_IF_ERROR(observation_cause(cause, record, std::move(settlements),
                                             !was_terminal && terminal(record.state.phase)));
        result.cancel_task_ids.insert(result.cancel_task_ids.end(),
                                      refreshed.cancel_task_ids.begin(),
                                      refreshed.cancel_task_ids.end());
        EF_RETURN_IF_ERROR(stage(context, candidate, record));
        result.record = record;

        return status_and_companion(context, record, companion);
      },
      cause);
  return publish(std::move(outcome), std::move(candidate), std::move(result));
}

StatusOr<RequestRuntimeResult> RequestRuntime::claim(const Row& scope, std::string_view id,
                                                     std::int64_t now, std::size_t max_tasks,
                                                     const RequestCommitHook& companion) noexcept {
  return impl_->control(Control::Claim, scope, id, now, max_tasks, nullptr, companion);
}

StatusOr<RequestRuntimeResult>
RequestRuntime::complete(const Row& scope, std::string_view id,
                         const FunctionCompletion& completion, std::int64_t now,
                         const RequestCommitHook& companion) noexcept {
  return impl_->control(Control::Complete, scope, id, now, 0, &completion, companion);
}

StatusOr<RequestRuntimeResult>
RequestRuntime::advance(const Row& scope, std::string_view id, std::int64_t now,
                        const RequestCommitHook& companion) noexcept {
  return impl_->control(Control::Advance, scope, id, now, 0, nullptr, companion);
}

StatusOr<RequestRuntimeResult> RequestRuntime::cancel(const Row& scope, std::string_view id,
                                                      std::int64_t now,
                                                      const RequestCommitHook& companion) noexcept {
  return impl_->control(Control::Cancel, scope, id, now, 0, nullptr, companion);
}

StatusOr<RequestRuntimeResult> RequestRuntime::resolve_pending_commit() noexcept {
  if (impl_->active || !impl_->pending || !impl_->engine->has_pending_commit())
    return Status(ErrorCode::FailedPrecondition, "request owner has no pending commit");

  CallGuard guard(impl_->active);
  EF_ASSIGN_OR_RETURN(auto outcome, EngineRuntimeAccess::resolve_request_commit(*impl_->engine));
  if (outcome.commit.code != CommitCode::Committed) {
    RequestRuntimeResult result;
    result.commit = std::move(outcome.commit);
    return result;
  }

  impl_->book = std::move(impl_->pending->book);
  auto result = std::move(impl_->pending->result);
  result.commit = std::move(outcome.commit);
  result.changes = std::move(outcome.changes);
  impl_->pending.reset();

  return result;
}

StatusOr<RequestRecord> RequestRuntime::read(const Row& scope, std::string_view id) noexcept {
  EF_RETURN_IF_ERROR(impl_->ready());
  return impl_->load(scope, id);
}

StatusOr<std::vector<RequestRecord>> RequestRuntime::active_requests() const noexcept {
  EF_RETURN_IF_ERROR(impl_->ready());
  std::vector<RequestRecord> result;
  for (const auto& [id, record] : impl_->book.active) {
    (void)id;
    result.push_back(record);
  }

  return result;
}

StatusOr<std::vector<FunctionTask>>
RequestRuntime::pending_tasks(std::size_t limit) const noexcept {
  EF_RETURN_IF_ERROR(impl_->ready());
  EF_RETURN_IF_ERROR(check(limit > 0, "task page limit must be positive"));
  std::vector<FunctionTask> result;
  for (const auto& [id, record] : impl_->book.active) {
    (void)id;
    const auto& plan = impl_->plans.find(record.admission.plan)->second;
    EF_ASSIGN_OR_RETURN(auto tasks, plan.machine->outbox(record.state));
    for (auto& task : tasks) {
      if (result.size() == limit)
        return result;

      result.push_back(std::move(task));
    }
  }

  return result;
}

bool RequestRuntime::has_pending_commit() const noexcept {
  return impl_->pending.has_value();
}
} // namespace eventflow
