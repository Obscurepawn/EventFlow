#include "eventflow/submitted_computation.hpp"
#include "eventflow/key_codec.hpp"
#include "eventflow/kv_protocol.hpp"
#include <algorithm>
#include <charconv>
#include <set>

namespace eventflow {
namespace {

Status object_fields(const Json& value, std::initializer_list<const char*> fields) noexcept {
  if (!value.is_object() || value.size() != fields.size())
    return Status(ErrorCode::InvalidArgument, "submitted definition has unknown or missing fields");
  for (const auto* name : fields)
    if (!value.contains(name))
      return Status(ErrorCode::InvalidArgument, "submitted definition is missing a field");
  return {};
}

StatusOr<std::string> name_field(const Json& value, const char* name) noexcept {
  EF_ASSIGN_OR_RETURN(auto result, kv_protocol::text(value, name));
  if (result.empty() || result.size() > 128)
    return Status(ErrorCode::InvalidArgument, "submitted object name exceeds bounds");
  for (char c : result)
    if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'))
      return Status(ErrorCode::InvalidArgument, "submitted names require ASCII identifiers");
  return result;
}

StatusOr<std::int64_t> duration_field(const Json& value, const char* name,
                                      bool zero = false) noexcept {
  EF_ASSIGN_OR_RETURN(auto text, kv_protocol::text(value, name));
  std::int64_t result = 0;
  const auto parsed = std::from_chars(text.data(), text.data() + text.size(), result);
  if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size() ||
      text != std::to_string(result) || result < (zero ? 0 : 1))
    return Status(ErrorCode::InvalidArgument, "expected a canonical positive Int64 decimal");
  return std::int64_t(result);
}

bool digest_valid(const std::string& value) noexcept {
  return value.size() == 64 && std::all_of(value.begin(), value.end(), [](char c) noexcept {
           return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
         });
}

template <typename T>
Predicate compare(RelationId relation, const ComputationCondition& condition,
                  const T& value) noexcept {
  const auto column = ViewRef(relation).column<T>(condition.column);
  if (condition.comparison == "eq")
    return column == value;
  if (condition.comparison == "ne")
    return column != value;
  if (condition.comparison == "gt")
    return column > value;
  if (condition.comparison == "ge")
    return column >= value;
  if (condition.comparison == "lt")
    return column < value;
  return column <= value;
}

} // namespace

Json encode_function_ref(const FunctionRef& value) noexcept {
  return {{"id", value.id}, {"version", std::to_string(value.version)}, {"digest", value.digest}};
}

StatusOr<FunctionRef> decode_function_ref(const Json& value) noexcept {
  EF_RETURN_IF_ERROR(object_fields(value, {"id", "version", "digest"}));
  FunctionRef result;
  EF_ASSIGN_OR_RETURN(result.id, name_field(value, "id"));
  EF_ASSIGN_OR_RETURN(auto version, kv_protocol::text(value, "version"));
  const auto parsed =
      std::from_chars(version.data(), version.data() + version.size(), result.version);
  if (parsed.ec != std::errc{} || parsed.ptr != version.data() + version.size() ||
      result.version == 0 || version != std::to_string(result.version))
    return Status(ErrorCode::InvalidArgument,
                  "expected a canonical positive UInt64 function version");
  EF_ASSIGN_OR_RETURN(result.digest, kv_protocol::text(value, "digest"));
  if (!digest_valid(result.digest))
    return Status(ErrorCode::InvalidArgument, "invalid UDF SHA-256 digest");
  return result;
}

StatusOr<Json> encode_computation(const SubmittedComputation& definition) noexcept {
  // SDK callers can construct strings without passing through the network JSON parser.
  if (definition.ddl.empty() || definition.ddl.size() > 64 || definition.triggers.empty() ||
      definition.triggers.size() > 16)
    return Status(ErrorCode::InvalidArgument, "submitted definition exceeds declaration limits");
  if (!valid_utf8(definition.name) || !valid_utf8(definition.input_relation))
    return Status(ErrorCode::InvalidArgument, "computation names must be UTF-8");
  for (const auto& ddl : definition.ddl)
    if (ddl.size() > 65536 || !valid_utf8(ddl))
      return Status(ErrorCode::InvalidArgument, "computation SQL must be UTF-8");
  Json triggers = Json::array();
  for (const auto& trigger : definition.triggers) {
    if (trigger.capture_fields.size() > 64 || trigger.conditions.size() > 16)
      return Status(ErrorCode::InvalidArgument,
                    "submitted trigger exceeds field or condition limits");
    if (!valid_utf8(trigger.name) || !valid_utf8(trigger.function.id) ||
        !valid_utf8(trigger.function.digest) || !valid_utf8(trigger.capture_relation))
      return Status(ErrorCode::InvalidArgument, "trigger names must be UTF-8");
    for (const auto& field : trigger.capture_fields)
      if (!valid_utf8(field))
        return Status(ErrorCode::InvalidArgument, "capture field names must be UTF-8");
    Json conditions = Json::array();
    for (const auto& condition : trigger.conditions) {
      if (!valid_utf8(condition.column) || !valid_utf8(condition.comparison))
        return Status(ErrorCode::InvalidArgument, "condition names must be UTF-8");
      const auto index = condition.value.index();
      if (index < 1 || index > 4)
        return Status(ErrorCode::InvalidArgument, "condition requires a scalar non-null literal");
      constexpr std::array<DataType, 4> types{DataType::Bool, DataType::Int64, DataType::Float64,
                                              DataType::String};
      EF_ASSIGN_OR_RETURN(auto value, value_to_json(condition.value));
      conditions.push_back({{"column", condition.column},
                            {"comparison", condition.comparison},
                            {"type", data_type_name(types[index - 1])},
                            {"value", std::move(value)}});
    }
    triggers.push_back({{"name", trigger.name},
                        {"function", encode_function_ref(trigger.function)},
                        {"capture_relation", trigger.capture_relation},
                        {"capture_fields", trigger.capture_fields},
                        {"conditions", std::move(conditions)},
                        {"every_us", std::to_string(trigger.every_us)},
                        {"once_per_session", trigger.once_per_session},
                        {"pending_us", std::to_string(trigger.pending_us)},
                        {"lifetime_us", std::to_string(trigger.lifetime_us)}});
  }
  Json result{{"format", 1},
              {"name", definition.name},
              {"ddl", definition.ddl},
              {"input_relation", definition.input_relation},
              {"session_lifetime_us", std::to_string(definition.session_lifetime_us)},
              {"triggers", std::move(triggers)}};
  EF_ASSIGN_OR_RETURN(auto checked, decode_computation(result));
  (void)checked;
  return result;
}

StatusOr<SubmittedComputation> decode_computation(const Json& value) noexcept {
  EF_RETURN_IF_ERROR(object_fields(
      value, {"format", "name", "ddl", "input_relation", "session_lifetime_us", "triggers"}));
  if (!value["format"].is_number_integer() || value["format"] != 1 || !value["ddl"].is_array() ||
      value["ddl"].empty() || value["ddl"].size() > 64 || !value["triggers"].is_array() ||
      value["triggers"].empty() || value["triggers"].size() > 16 ||
      value.dump(-1, ' ', false, Json::error_handler_t::replace).size() > 256 * 1024)
    return Status(ErrorCode::InvalidArgument,
                  "submitted definition exceeds supported format or bounds");
  SubmittedComputation result;
  EF_ASSIGN_OR_RETURN(result.name, name_field(value, "name"));
  EF_ASSIGN_OR_RETURN(result.input_relation, name_field(value, "input_relation"));
  EF_ASSIGN_OR_RETURN(result.session_lifetime_us, duration_field(value, "session_lifetime_us"));
  for (const auto& ddl : value["ddl"]) {
    if (!ddl.is_string() || !valid_utf8(ddl.get_ref<const std::string&>()) ||
        ddl.get_ref<const std::string&>().empty() ||
        ddl.get_ref<const std::string&>().size() > 65536)
      return Status(ErrorCode::InvalidArgument, "invalid submitted DDL");
    result.ddl.push_back(ddl.get<std::string>());
  }
  for (const auto& item : value["triggers"]) {
    EF_RETURN_IF_ERROR(
        object_fields(item, {"name", "function", "capture_relation", "capture_fields", "conditions",
                             "every_us", "once_per_session", "pending_us", "lifetime_us"}));
    SubmittedTrigger trigger;
    EF_ASSIGN_OR_RETURN(trigger.name, name_field(item, "name"));
    EF_ASSIGN_OR_RETURN(trigger.function, decode_function_ref(item["function"]));
    EF_ASSIGN_OR_RETURN(trigger.capture_relation, name_field(item, "capture_relation"));
    EF_ASSIGN_OR_RETURN(trigger.every_us, duration_field(item, "every_us", true));
    EF_ASSIGN_OR_RETURN(trigger.pending_us, duration_field(item, "pending_us"));
    EF_ASSIGN_OR_RETURN(trigger.lifetime_us, duration_field(item, "lifetime_us"));
    if (!item["once_per_session"].is_boolean() || !item["capture_fields"].is_array() ||
        item["capture_fields"].empty() || item["capture_fields"].size() > 64 ||
        !item["conditions"].is_array() || item["conditions"].size() > 16)
      return Status(ErrorCode::InvalidArgument, "invalid capture or condition bounds");
    trigger.once_per_session = item["once_per_session"].get<bool>();
    for (const auto& field : item["capture_fields"]) {
      EF_ASSIGN_OR_RETURN(auto name, name_field(Json{{"name", field}}, "name"));
      trigger.capture_fields.push_back(std::move(name));
    }
    for (const auto& condition : item["conditions"]) {
      EF_RETURN_IF_ERROR(object_fields(condition, {"column", "comparison", "type", "value"}));
      ComputationCondition decoded;
      EF_ASSIGN_OR_RETURN(decoded.column, name_field(condition, "column"));
      EF_ASSIGN_OR_RETURN(decoded.comparison, kv_protocol::text(condition, "comparison"));
      const std::set<std::string> operators{"eq", "ne", "gt", "ge", "lt", "le"};
      if (!operators.contains(decoded.comparison))
        return Status(ErrorCode::InvalidArgument, "unsupported condition comparison");
      EF_ASSIGN_OR_RETURN(auto type_name, kv_protocol::text(condition, "type"));
      EF_ASSIGN_OR_RETURN(auto type, parse_data_type(type_name));
      if (type != DataType::Bool && type != DataType::Int64 && type != DataType::Float64 &&
          type != DataType::String)
        return Status(ErrorCode::InvalidArgument, "unsupported condition literal type");
      EF_ASSIGN_OR_RETURN(decoded.value, value_from_json(condition["value"], type, false));
      trigger.conditions.push_back(std::move(decoded));
    }
    result.triggers.push_back(std::move(trigger));
  }
  return result;
}

StatusOr<std::string> udf_storage_key(std::uint32_t shard, const std::string& digest) noexcept {
  if (!digest_valid(digest))
    return Status(ErrorCode::InvalidArgument, "invalid UDF content identity");
  EF_ASSIGN_OR_RETURN(auto prefix, data_prefix(shard));
  return prefix + "meta:udf:" + digest;
}

StatusOr<BoundComputation>
bind_submitted_computation(const SubmittedComputation& submitted,
                           const std::vector<std::shared_ptr<UdfLibrary>>& libraries,
                           const EngineConfig& engine) noexcept {
  EF_ASSIGN_OR_RETURN(auto wire, encode_computation(submitted));
  const auto identity = "submitted.v1:" + udf_digest(wire.dump());
  EF_ASSIGN_OR_RETURN(auto catalog, Catalog::create());
  for (const auto& ddl : submitted.ddl) {
    EF_ASSIGN_OR_RETURN(auto relation, catalog->define_sql(ddl));
    (void)relation;
  }
  EF_ASSIGN_OR_RETURN(auto initial, catalog->snapshot());
  EF_ASSIGN_OR_RETURN(auto input, initial.find(submitted.input_relation));
  if (input->kind != RelationKind::BaseTable ||
      input->base_write_role != BaseWriteRole::PublicInput ||
      input->partition_types != std::vector<DataType>{DataType::String})
    return Status(ErrorCode::InvalidArgument,
                  "submitted computation requires one string business key");
  std::vector<Field> scope_fields;
  for (auto slot : input->partition_slots)
    scope_fields.push_back(input->storage_schema.fields[slot]);
  for (std::size_t i = 0; i < scope_fields.size(); ++i)
    scope_fields[i].id = static_cast<std::uint32_t>(i + 1);
  EF_ASSIGN_OR_RETURN(
      auto session,
      catalog->define_candidate(
          {.name = submitted.name + "_session",
           .scope_fields = scope_fields,
           .payload_fields = {{.id = static_cast<std::uint32_t>(scope_fields.size() + 3),
                               .name = "deadline_us",
                               .type = DataType::Int64}}}));
  EF_ASSIGN_OR_RETURN(auto snapshot, catalog->snapshot());
  EF_ASSIGN_OR_RETURN(auto candidate, snapshot.find(session));
  EF_ASSIGN_OR_RETURN(auto epoch_slot, candidate->storage_schema.index_of("candidate_epoch"));
  EF_ASSIGN_OR_RETURN(auto active_slot, candidate->storage_schema.index_of("active"));
  ProcessorConfig definition;
  definition.input_relations = {input->id};
  definition.requests.emplace();
  definition.requests->candidate = CandidateExpiryPolicy{
      .relation = session,
      .deadline_column = "deadline_us",
      .version = 1,
      .on_source = [session, epoch_slot, active_slot, ttl = submitted.session_lifetime_us](
                       RoundContext& round, const ProcessorEvent& event) noexcept
          -> StatusOr<std::optional<CandidateActivation>> {
        EF_ASSIGN_OR_RETURN(auto current, round.read_relation(session, event.scope));
        std::int64_t epoch = 0;
        if (current) {
          if (current->size() <= std::max(epoch_slot, active_slot))
            return Status(ErrorCode::InvalidArgument, "invalid session state row");
          const auto* active = std::get_if<bool>(&(*current)[active_slot]);
          const auto* previous = std::get_if<std::int64_t>(&(*current)[epoch_slot]);
          if (!active || !previous)
            return Status(ErrorCode::InvalidArgument, "invalid session state fields");
          if (*active)
            return std::optional<CandidateActivation>{};
          epoch = *previous;
        }
        if (event.processing_time_us > INT64_MAX - ttl)
          return Status(ErrorCode::InvalidArgument, "session deadline overflows Int64");
        return std::optional<CandidateActivation>(CandidateActivation{
            .expected_epoch = epoch, .payload = {event.processing_time_us + ttl}});
      }};
  std::vector<Trigger> triggers;
  for (const auto& item : submitted.triggers) {
    auto found =
        std::find_if(libraries.begin(), libraries.end(), [&](const auto& library) noexcept {
          return library && library->digest() == item.function.digest &&
                 library->definition().signature.function_id == item.function.id &&
                 library->definition().signature.version == item.function.version;
        });
    if (found == libraries.end())
      return Status(ErrorCode::NotFound,
                    "submitted computation references an unavailable UDF version");
    auto library = *found;
    const auto& signature = library->definition().signature;
    if (signature.inputs.size() != 1 || !signature.inputs[0].required || !signature.pure)
      return Status(ErrorCode::InvalidArgument,
                    "submitted UDF requires one pure captured row input");
    EF_ASSIGN_OR_RETURN(auto view, snapshot.find(item.capture_relation));
    auto trigger =
        item.every_us == 0
            ? Trigger::on_event(
                  item.name, EventRef::source({.kind = input->name, .payload_relation = input->id}))
            : Trigger::clock(item.name, {.policy_id = item.name + "_clock",
                                         .every = std::chrono::microseconds(item.every_us),
                                         .phase = {},
                                         .event_kind = item.name + "_clock"});
    auto predicate = Predicate::constant(true);
    for (const auto& condition : item.conditions) {
      auto next = std::visit(
          [&](const auto& value) noexcept -> Predicate {
            using T = std::decay_t<decltype(value)>;
            if constexpr (trigger_detail::supported_type<T>)
              return compare(view->id, condition, value);
            else
              return Predicate::constant(false);
          },
          condition.value);
      predicate = predicate && std::move(next);
    }
    StaticFunctionDefinition function{
        .id = signature.function_id,
        .version = signature.version,
        .implementation_identity =
            signature.implementation_identity + ":sha256:" + library->digest(),
        .input_fields = signature.inputs[0].fields,
        .output_fields = signature.output_fields,
        .invoke = [library](const Row& row) noexcept -> StatusOr<Row> {
          return library->definition().invoke(FunctionInputs{row});
        }};
    triggers.push_back(trigger.implementation_identity(identity)
                           .when(std::move(predicate))
                           .in_session(SessionRef::candidate(session))
                           .capture_current(view->id, item.capture_fields, function.input_fields)
                           .request(std::move(function),
                                    {.id = item.name + "_request",
                                     .mode = item.every_us ? RequestTriggerMode::TryOnceAtFiring
                                                           : RequestTriggerMode::Wait,
                                     .pending_for = std::chrono::microseconds(item.pending_us),
                                     .lifetime = std::chrono::microseconds(item.lifetime_us),
                                     .once_per_candidate = item.once_per_session}));
  }
  return TriggerSet::bind(std::move(snapshot), std::move(definition), std::move(triggers), engine);
}

StatusOr<BoundComputation> load_submitted_computation(IStateStore& store, std::uint32_t shard,
                                                      const Json& wire, const Json& registered,
                                                      const EngineConfig& engine) noexcept {
  EF_ASSIGN_OR_RETURN(auto submitted, decode_computation(wire));
  if (!registered.is_array() || registered.size() > 16)
    return Status(ErrorCode::InvalidArgument, "invalid registered UDF list");
  std::vector<std::shared_ptr<UdfLibrary>> libraries;
  std::set<std::string> loaded;
  for (const auto& trigger : submitted.triggers) {
    const auto ref = encode_function_ref(trigger.function);
    if (std::find(registered.begin(), registered.end(), ref) == registered.end())
      return Status(ErrorCode::NotFound, "function is not registered in this deployment");
    if (!loaded.insert(trigger.function.digest).second)
      continue;
    EF_ASSIGN_OR_RETURN(auto key, udf_storage_key(shard, trigger.function.digest));
    EF_ASSIGN_OR_RETURN(auto read, store.get_many(shard, {key}));
    if (read.values.size() != 1 || !read.values[0])
      return Status(ErrorCode::NotFound, "registered UDF artifact is missing from KV");
    if (read.values[0]->size() > max_udf_bytes ||
        udf_digest(*read.values[0]) != trigger.function.digest)
      return Status(ErrorCode::Conflict,
                    "stored UDF artifact content differs from registered digest");
    EF_ASSIGN_OR_RETURN(auto library, UdfLibrary::open(*read.values[0]));
    libraries.push_back(std::move(library));
  }
  return bind_submitted_computation(submitted, libraries, engine);
}

} // namespace eventflow
