#pragma once

#include "eventflow/static_function.hpp"
#include "eventflow/typed_trigger.hpp"

namespace eventflow {

enum class NextPolicy { OfKind, Strict };
enum class CaptureSource { Match, Current };

struct SessionRef {
  RelationId relation = 0;

  static SessionRef candidate(RelationId relation) noexcept {
    return {.relation = relation};
  }
};

struct ClockSchedule {
  std::string policy_id;
  TriggerDuration every;
  TriggerDuration phase = {};
  std::string event_kind = {};
};

struct TriggerRequest {
  std::string id = {};
  std::uint64_t version = 1;
  std::string plan = {};
  std::optional<RequestTriggerMode> mode = {};
  TriggerDuration pending_for = {};
  std::optional<TriggerDuration> lifetime = {};
  bool once_per_candidate = false;
  std::string required_accepted_binding = {};
  std::string deadline_column = {};
  std::function<StatusOr<RequestAdmission>(RoundContext&, const TriggerOccurrence&)>
      make_admission = {};
  std::optional<RequestMachineConfig> machine = {};
};

struct BoundInputCapture {
  CaptureSource source = CaptureSource::Match;
  std::vector<Field> fields;
  std::vector<RelationId> reads;
  std::string signature;
  std::function<StatusOr<Row>(const TriggerOccurrence&)> match;
  std::function<StatusOr<std::optional<Row>>(RoundContext&, const Row&)> current;
};

using TriggerCaptureBinder = std::function<StatusOr<BoundInputCapture>(
    const CatalogSnapshot&, const std::vector<PatternLabelInfo>&, const std::string&)>;

struct BoundTrigger {
  std::string id;
  std::uint64_t version = 1;
  std::optional<ProcessorRule> rule;
  std::optional<ProcessorCepRule> pattern;
  std::optional<PeriodicPolicy> clock;
  std::optional<SessionRef> session;
  std::optional<TriggerRequest> request;
  std::optional<StaticFunctionDefinition> function;
  std::optional<BoundInputCapture> capture;
  Json explanation;
  // Retain source declarations for module-level owner and Clock validation.
  std::vector<EventSource> events = {};
};

template <typename Input> class CapturedTrigger;

// Business declarations, bound before an owner starts. These objects do not
// execute callbacks, start timers, or mutate Catalog/Registry during chaining.
class Trigger {
public:
  static Trigger on_event(std::string id, EventRef event) noexcept;
  static Trigger on_event(std::string id, std::string event_kind) noexcept;
  static Trigger clock(std::string id, ClockSchedule schedule) noexcept;
  static Trigger pattern(std::string id, Pattern pattern) noexcept;

  template <TypedSchema T>
  static Trigger pattern(std::string id, TypedEventRef<T> source,
                         TypedPattern<T> pattern) noexcept {
    return from_pattern(
        std::move(id),
        [source = std::move(source), pattern = std::move(pattern)](
            const CatalogSnapshot& catalog) noexcept -> StatusOr<BoundTriggerPattern> {
          return pattern.bind(catalog, source);
        });
  }

  Trigger version(std::uint64_t version) const noexcept;
  Trigger implementation_identity(std::string identity) const noexcept;
  Trigger when(Predicate predicate) const noexcept;
  Trigger then_next(EventRef event, NextPolicy policy) const noexcept;
  Trigger within(TriggerDuration duration) const noexcept;
  Trigger rearm_on_next_event() const noexcept;
  Trigger emit(std::string emission) const noexcept;
  Trigger limits(CepLimits limits) const noexcept;
  Trigger in_session(SessionRef session) const noexcept;
  Trigger request(TriggerRequest request) const noexcept;

  template <auto Fn>
  auto capture_match(std::vector<std::string> labels,
                     std::string implementation_identity = {}) const noexcept;

  template <auto Fn>
    requires StaticFunctionCallable<Fn>
  auto capture_current(RelationId view, std::string implementation_identity = {}) const noexcept;

private:
  struct Data;
  explicit Trigger(std::shared_ptr<const Data> data) noexcept;
  static Trigger from_pattern(
      std::string id,
      std::function<StatusOr<BoundTriggerPattern>(const CatalogSnapshot&)> binder) noexcept;
  Trigger with_capture(TriggerCaptureBinder binder) const noexcept;
  Trigger with_function(StaticFunctionDefinition function, TriggerRequest request) const noexcept;
  Trigger with_error(Status status) const noexcept;
  std::shared_ptr<const Data> data_;
  template <typename Input> friend class CapturedTrigger;
  friend StatusOr<BoundTrigger> bind_trigger(const CatalogSnapshot&, const Trigger&) noexcept;
};

// A projection's output type is carried into request(), so incompatible static
// functions fail C++ type checking instead of failing during model execution.
template <typename Input> class CapturedTrigger {
public:
  template <auto Fn>
    requires(StaticFunctionCallable<Fn> && std::is_same_v<typename Function<Fn>::input_type, Input>)
  Trigger request(const Function<Fn>& function, TriggerRequest request) const noexcept {
    auto definition = function.definition();
    if (!definition.ok())
      return trigger_.with_error(definition.status());
    return trigger_.with_function(std::move(definition).value(), std::move(request));
  }

private:
  explicit CapturedTrigger(Trigger trigger) noexcept : trigger_(std::move(trigger)) {}

  Trigger trigger_;
  friend class Trigger;
};

template <auto Fn>
auto Trigger::capture_match(std::vector<std::string> labels,
                            std::string implementation_identity) const noexcept {
  using Output = typename typed_trigger_detail::ProjectionTraits<decltype(Fn)>::output_type;
  auto binder = [labels = std::move(labels), identity = std::move(implementation_identity)](
                    const CatalogSnapshot& catalog, const std::vector<PatternLabelInfo>& metadata,
                    const std::string& module_identity) noexcept -> StatusOr<BoundInputCapture> {
    EF_ASSIGN_OR_RETURN(auto projection,
                        bind_match_projection<Fn>(catalog, metadata, labels,
                                                  identity.empty() ? module_identity : identity));
    return BoundInputCapture{.source = CaptureSource::Match,
                             .fields = std::move(projection.output_fields),
                             .reads = {},
                             .signature = std::move(projection.signature),
                             .match = std::move(projection.project),
                             .current = {}};
  };
  return CapturedTrigger<Output>(with_capture(std::move(binder)));
}

template <auto Fn>
  requires StaticFunctionCallable<Fn>
auto Trigger::capture_current(RelationId view, std::string implementation_identity) const noexcept {
  using Output = typename Function<Fn>::output_type;
  auto binder = [view, identity = std::move(implementation_identity)](
                    const CatalogSnapshot& catalog, const std::vector<PatternLabelInfo>&,
                    const std::string& module_identity) noexcept -> StatusOr<BoundInputCapture> {
    EF_ASSIGN_OR_RETURN(auto relation, catalog.find(view));
    if (!relation->unique_per_partition)
      return Status(ErrorCode::InvalidArgument,
                    "capture_current requires one row per business key");
    EF_RETURN_IF_ERROR(check(relation->visible_count <= relation->storage_schema.fields.size(),
                             "capture_current relation has an invalid visible schema"));
    const auto fields =
        std::vector<Field>(relation->storage_schema.fields.begin(),
                           relation->storage_schema.fields.begin() + relation->visible_count);
    EF_ASSIGN_OR_RETURN(
        auto mapper,
        (Function<Fn>{.id = "capture_current",
                      .version = 1,
                      .implementation_identity = identity.empty() ? module_identity : identity})
            .definition());
    EF_ASSIGN_OR_RETURN(auto binding, bind_row_fields(mapper.input_fields, fields));
    const auto mapper_identity = mapper.implementation_identity;
    auto output_fields = mapper.output_fields;
    const auto describe = [](const std::vector<Field>& columns) noexcept {
      Json result = Json::array();
      for (const auto& column : columns)
        result.push_back(Json{{"id", column.id},
                              {"name", column.name},
                              {"type", data_type_name(column.type)},
                              {"nullable", column.nullable}});
      return result;
    };
    auto signature = Json{{"source", "current"},
                          {"relation", view},
                          {"identity", mapper_identity},
                          {"schema", describe(fields)},
                          {"input_fields", describe(mapper.input_fields)},
                          {"output_fields", describe(output_fields)}}
                         .dump();
    return BoundInputCapture{
        .source = CaptureSource::Current,
        .fields = std::move(output_fields),
        .reads = {view},
        .signature = std::move(signature),
        .match = {},
        .current = [view, descriptor = *relation, binding = std::move(binding),
                    mapper = std::move(mapper)](RoundContext& round, const Row& scope) noexcept
            -> StatusOr<std::optional<Row>> {
          EF_ASSIGN_OR_RETURN(auto stored, round.read_relation(view, scope));
          if (!stored)
            return std::optional<Row>{};
          EF_ASSIGN_OR_RETURN(auto visible, visible_row(descriptor, *stored));
          EF_ASSIGN_OR_RETURN(auto encoded, binding.project(visible));
          EF_ASSIGN_OR_RETURN(auto output, mapper.invoke(encoded));
          return std::optional<Row>(std::move(output));
        }};
  };
  return CapturedTrigger<Output>(with_capture(std::move(binder)));
}

// Binds one declaration without publishing Catalog or function changes. The
// module binder validates sessions, plan references and complete owner budgets.
StatusOr<BoundTrigger> bind_trigger(const CatalogSnapshot& catalog,
                                    const Trigger& trigger) noexcept;

} // namespace eventflow
