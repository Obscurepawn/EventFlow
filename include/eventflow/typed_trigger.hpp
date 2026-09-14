#pragma once

#include "eventflow/event_processor.hpp"
#include "eventflow/trigger_pattern.hpp"
#include "eventflow/typed_schema.hpp"
#include <algorithm>
#include <functional>
#include <set>

namespace eventflow {

StatusOr<std::vector<Field>> trigger_event_fields(const CatalogSnapshot& catalog,
                                                  const EventSource& source) noexcept;
Status validate_typed_event_key(const CatalogSnapshot& catalog, const EventSource& source,
                                const std::vector<std::string>& keys) noexcept;

// A typed source describes a same-shard input domain. key_by validates the
// existing Catalog partition key; it never repartitions or filters by value.
template <typename T> class TypedEventRef {
  static_assert(TypedSchema<T>, "Typed events require explicit noexcept SchemaTraits");

public:
  explicit TypedEventRef(EventSource source) noexcept : source_(std::move(source)) {}

  template <typename... Members> TypedEventRef key_by(Members... members) const noexcept {
    static_assert((std::is_member_object_pointer_v<Members> && ...),
                  "key_by accepts data member pointers");
    auto next = *this;
    next.keys_.clear();
    const auto mappings = SchemaTraits<T>::fields();
    const auto append = [&](auto member) noexcept {
      std::optional<std::string> name;
      std::apply(
          [&](const auto&... mapping) noexcept {
            const auto check = [&](const auto& field_mapping) noexcept {
              using Mapping = std::remove_cvref_t<decltype(field_mapping)>;
              if constexpr (std::is_same_v<decltype(member),
                                           std::remove_cv_t<decltype(Mapping::member)>>)
                if (member == Mapping::member)
                  name = field_mapping.name;
            };
            (check(mapping), ...);
          },
          mappings);
      if (!name)
        next.status_ =
            Status(ErrorCode::InvalidArgument, "key_by member is missing from SchemaTraits");
      else
        next.keys_.push_back(std::move(*name));
    };
    (append(members), ...);
    return next;
  }

  const EventSource& definition() const noexcept {
    return source_;
  }

  StatusOr<EventRef> bind(const CatalogSnapshot& catalog) const noexcept {
    EF_RETURN_IF_ERROR(status_);
    EF_RETURN_IF_ERROR(validate_typed_event_key(catalog, source_, keys_));
    EF_ASSIGN_OR_RETURN(auto fields, trigger_event_fields(catalog, source_));
    EF_ASSIGN_OR_RETURN(auto binding, bind_typed_schema<T>(fields));
    (void)binding;
    return EventRef::source(source_);
  }

private:
  EventSource source_;
  std::vector<std::string> keys_;
  Status status_;
};

template <typename T> TypedEventRef<T> EventRef::source(EventSource source) noexcept {
  return TypedEventRef<T>(std::move(source));
}

// Typed patterns use the same bounded E-NFA as Pattern. Every event of the
// declared kind reaches where(), including negative or malformed news events.
template <typename T> class TypedPattern {
  static_assert(TypedSchema<T>, "Typed patterns require explicit noexcept SchemaTraits");
  using Builder = std::function<StatusOr<Pattern>(const EventRef&)>;

public:
  static TypedPattern begin(std::string label) noexcept {
    return TypedPattern(
        [label = std::move(label)](const EventRef& event) noexcept -> StatusOr<Pattern> {
          return Pattern::begin(label, event);
        });
  }

  template <auto Fn> TypedPattern where(std::string implementation_identity) const noexcept {
    static_assert(
        []() noexcept {
          using Callback = std::remove_cv_t<decltype(Fn)>;
          if constexpr (std::is_pointer_v<Callback> || std::is_member_pointer_v<Callback>)
            return Fn != nullptr;
          return true;
        }(),
        "Typed guard function must not be null");
    return where(Fn, std::move(implementation_identity));
  }

  template <typename Fn>
  TypedPattern where(Fn function, std::string implementation_identity) const noexcept {
    static_assert(std::is_nothrow_invocable_r_v<StatusOr<bool>, const Fn&, const T&> &&
                      std::is_same_v<std::invoke_result_t<const Fn&, const T&>, StatusOr<bool>>,
                  "Typed guards must return StatusOr<bool> and be noexcept");
    if constexpr (std::is_pointer_v<Fn> || std::is_member_pointer_v<Fn>) {
      if (function == nullptr) {
        auto next = *this;
        if (next.status_.ok())
          next.status_ =
              Status(ErrorCode::InvalidArgument, "Typed guard function must not be null");
        return next;
      }
    }
    return append([function = std::move(function), identity = std::move(implementation_identity)](
                      Pattern pattern, const EventRef& event) noexcept -> StatusOr<Pattern> {
      EF_ASSIGN_OR_RETURN(auto expected, typed_fields<T>());
      EF_ASSIGN_OR_RETURN(auto binding, bind_typed_schema<T>(expected));
      auto callback = [function,
                       binding = std::move(binding)](const Row& values) noexcept -> StatusOr<bool> {
        EF_ASSIGN_OR_RETURN(auto typed, binding.decode(values));
        return std::invoke(function, typed);
      };
      return pattern.where(
          Predicate::event_guard(event, std::move(expected), std::move(callback), identity));
    });
  }

  TypedPattern where(Predicate predicate) const noexcept {
    return append([predicate = std::move(predicate)](Pattern pattern, const EventRef&) noexcept
                      -> StatusOr<Pattern> { return pattern.where(predicate); });
  }

  TypedPattern next(std::string label) const noexcept {
    return add_event(std::move(label), CepContiguity::Strict);
  }

  TypedPattern next_of_kind(std::string label) const noexcept {
    return add_event(std::move(label), CepContiguity::NextOfKind);
  }

  TypedPattern followed_by(std::string label) const noexcept {
    return add_event(std::move(label), CepContiguity::FollowedBy);
  }

  TypedPattern followed_by_any(std::string label) const noexcept {
    return add_event(std::move(label), CepContiguity::FollowedByAny);
  }

  TypedPattern then(TypedPattern right,
                    CepContiguity contiguity = CepContiguity::FollowedBy) const noexcept {
    return combine(std::move(right), false, contiguity);
  }

  TypedPattern or_else(TypedPattern right) const noexcept {
    return combine(std::move(right), true, CepContiguity::FollowedBy);
  }

  // Group operators wrap the complete expression, just like untyped Pattern.
  TypedPattern optional() const noexcept {
    return append([](Pattern pattern, const EventRef&) noexcept -> StatusOr<Pattern> {
      return pattern.optional();
    });
  }

  TypedPattern repeat(std::size_t minimum, std::size_t maximum,
                      CepContiguity inner = CepContiguity::FollowedBy) const noexcept {
    return append(
        [minimum, maximum, inner](Pattern pattern, const EventRef&) noexcept -> StatusOr<Pattern> {
          return pattern.repeat(minimum, maximum, inner);
        });
  }

  TypedPattern within(TriggerDuration duration) const noexcept {
    return append([duration](Pattern pattern, const EventRef&) noexcept -> StatusOr<Pattern> {
      return pattern.within(duration);
    });
  }

  TypedPattern after_match(CepSkipStrategy strategy) const noexcept {
    return append([strategy](Pattern pattern, const EventRef&) noexcept -> StatusOr<Pattern> {
      return pattern.after_match(strategy);
    });
  }

  StatusOr<BoundTriggerPattern> bind(const CatalogSnapshot& catalog,
                                     const TypedEventRef<T>& source) const noexcept {
    EF_ASSIGN_OR_RETURN(auto event, source.bind(catalog));
    EF_ASSIGN_OR_RETURN(auto pattern, materialize(event));
    EF_ASSIGN_OR_RETURN(auto result, bind_trigger_pattern(catalog, pattern));
    result.input_kinds = {event.definition().kind};
    result.signature = Json{{"format", "typed_pattern.v1"},
                            {"pattern", result.signature},
                            {"input_kinds", result.input_kinds}}
                           .dump();
    return result;
  }

private:
  explicit TypedPattern(Builder builder) noexcept : builder_(std::move(builder)) {}

  StatusOr<Pattern> materialize(const EventRef& event) const noexcept {
    EF_RETURN_IF_ERROR(status_);
    return builder_(event);
  }

  template <typename Transform> TypedPattern append(Transform transform) const noexcept {
    auto next = *this;
    if (!status_.ok())
      return next;
    if (depth_ >= 64) {
      next.status_ =
          Status(ErrorCode::InvalidArgument, "Typed Pattern declaration exceeds bounded depth");
      return next;
    }
    next.builder_ = [previous = builder_, transform = std::move(transform)](
                        const EventRef& event) noexcept -> StatusOr<Pattern> {
      EF_ASSIGN_OR_RETURN(auto pattern, previous(event));
      return transform(std::move(pattern), event);
    };
    ++next.depth_;
    return next;
  }

  TypedPattern add_event(std::string label, CepContiguity contiguity) const noexcept {
    return append([label = std::move(label), contiguity](
                      Pattern pattern, const EventRef& event) noexcept -> StatusOr<Pattern> {
      return pattern.then(Pattern::begin(label, event), contiguity);
    });
  }

  TypedPattern combine(TypedPattern right, bool alternative,
                       CepContiguity contiguity) const noexcept {
    auto next = *this;
    if (!status_.ok())
      return next;
    if (!right.status_.ok()) {
      next.status_ = right.status_;
      return next;
    }
    if (depth_ + right.depth_ >= 64) {
      next.status_ =
          Status(ErrorCode::InvalidArgument, "Typed Pattern declaration exceeds bounded depth");
      return next;
    }
    next = append([right = right.builder_, alternative, contiguity](
                      Pattern pattern, const EventRef& event) noexcept -> StatusOr<Pattern> {
      EF_ASSIGN_OR_RETURN(auto other, right(event));
      return alternative ? pattern.or_else(std::move(other))
                         : pattern.then(std::move(other), contiguity);
    });
    next.depth_ = depth_ + right.depth_ + 1;
    return next;
  }

  Builder builder_;
  std::size_t depth_ = 1;
  Status status_;
};

template <typename T> TypedPattern<T> Pattern::begin(std::string label) noexcept {
  return TypedPattern<T>::begin(std::move(label));
}

struct BoundMatchProjection {
  std::vector<Field> output_fields;
  std::string signature;
  std::function<StatusOr<Row>(const TriggerOccurrence&)> project;
};

namespace typed_trigger_detail {
StatusOr<PatternLabelInfo> select_label(const std::vector<PatternLabelInfo>& labels,
                                        std::string_view name) noexcept;
StatusOr<std::vector<const CepEvent*>> matched_events(const TriggerOccurrence& occurrence,
                                                      const PatternLabelInfo& label) noexcept;
StatusOr<std::string> projection_signature(const std::vector<std::string>& labels,
                                           const std::vector<PatternLabelInfo>& metadata,
                                           const std::vector<Field>& output,
                                           const std::vector<std::vector<Field>>& inputs,
                                           std::string_view identity) noexcept;

template <typename Signature, typename = void> struct ProjectionTraits {
  static_assert(sizeof(Signature) == 0,
                "Match projections must be noexcept function pointers returning StatusOr<Output>");
};

template <typename Output, typename... Args>
struct ProjectionTraits<StatusOr<Output> (*)(Args...) noexcept> {
  using output_type = Output;
  using arguments = std::tuple<Args...>;
  static constexpr std::size_t arity = sizeof...(Args);
};

template <typename Owner, typename Output, typename... Args>
struct ProjectionTraits<StatusOr<Output> (Owner::*)(Args...) const noexcept>
    : ProjectionTraits<StatusOr<Output> (*)(Args...) noexcept> {};

template <typename Callable>
struct ProjectionTraits<Callable, std::void_t<decltype(&Callable::operator())>>
    : ProjectionTraits<decltype(&Callable::operator())> {
  static_assert(std::is_empty_v<Callable>,
                "Static match projections require captureless callables");
};

template <typename T> struct ProjectionArgument {
  using event_type = T;
  static constexpr int cardinality = 0;
};

template <typename T> struct ProjectionArgument<std::optional<T>> {
  using event_type = T;
  static constexpr int cardinality = 1;
};

template <typename T> struct ProjectionArgument<std::vector<T>> {
  using event_type = T;
  static constexpr int cardinality = 2;
};

template <typename Arg> struct BoundArgument {
  using Storage = std::remove_cvref_t<Arg>;
  using Shape = ProjectionArgument<Storage>;
  using Event = typename Shape::event_type;
  PatternLabelInfo label;
  TypedRowBinding<Event> binding;
};

template <typename Arg>
StatusOr<BoundArgument<Arg>> bind_argument(const CatalogSnapshot& catalog,
                                           const std::vector<PatternLabelInfo>& labels,
                                           const std::string& selected) noexcept {
  using Bound = BoundArgument<Arg>;
  static_assert(Bound::Shape::cardinality != 0 || (std::is_lvalue_reference_v<Arg> &&
                                                   std::is_const_v<std::remove_reference_t<Arg>>),
                "A required match event parameter must be const T&");
  EF_ASSIGN_OR_RETURN(auto label, select_label(labels, selected));
  const bool compatible = Bound::Shape::cardinality == 0 ? label.minimum == 1 && label.maximum == 1
                          : Bound::Shape::cardinality == 1
                              ? label.minimum == 0 && label.maximum == 1
                              : label.maximum > 1;
  if (!compatible)
    return Status(ErrorCode::InvalidArgument,
                  "Match label cardinality does not match projection parameter");
  EF_ASSIGN_OR_RETURN(auto fields, trigger_event_fields(catalog, label.event));
  EF_ASSIGN_OR_RETURN(auto binding, bind_typed_schema<typename Bound::Event>(fields));
  return Bound{.label = std::move(label), .binding = std::move(binding)};
}

template <typename Arg>
StatusOr<typename BoundArgument<Arg>::Storage>
decode_argument(const BoundArgument<Arg>& argument, const TriggerOccurrence& occurrence) noexcept {
  using Bound = BoundArgument<Arg>;
  EF_ASSIGN_OR_RETURN(auto events, matched_events(occurrence, argument.label));
  if constexpr (Bound::Shape::cardinality == 0) {
    return argument.binding.decode(events.front()->values);
  } else if constexpr (Bound::Shape::cardinality == 1) {
    typename Bound::Storage result;
    if (!events.empty()) {
      EF_ASSIGN_OR_RETURN(auto event, argument.binding.decode(events.front()->values));
      result = std::move(event);
    }
    return result;
  } else {
    typename Bound::Storage result;
    for (const auto* event : events) {
      EF_ASSIGN_OR_RETURN(auto value, argument.binding.decode(event->values));
      result.push_back(std::move(value));
    }
    return result;
  }
}

template <auto Fn, std::size_t... Index>
StatusOr<BoundMatchProjection>
bind_projection(const CatalogSnapshot& catalog, const std::vector<PatternLabelInfo>& labels,
                const std::vector<std::string>& selected, std::string_view identity,
                std::index_sequence<Index...>) noexcept {
  using Traits = ProjectionTraits<decltype(Fn)>;
  using Args = typename Traits::arguments;
  using Output = typename Traits::output_type;
  static_assert(TypedSchema<Output>, "Projection output requires explicit SchemaTraits");
  std::tuple<std::optional<BoundArgument<std::tuple_element_t<Index, Args>>>...> arguments;
  Status status;
  const auto bind_one = [&]<std::size_t I>() noexcept {
    if (!status.ok())
      return;
    auto bound = bind_argument<std::tuple_element_t<I, Args>>(catalog, labels, selected[I]);
    if (!bound.ok())
      status = bound.status();
    else
      std::get<I>(arguments) = std::move(*bound);
  };
  (bind_one.template operator()<Index>(), ...);
  EF_RETURN_IF_ERROR(status);
  EF_ASSIGN_OR_RETURN(auto fields, typed_fields<Output>());
  std::vector<std::vector<Field>> input_fields;
  const auto describe_one = [&]<std::size_t I>() noexcept {
    if (!status.ok())
      return;
    using Event = typename BoundArgument<std::tuple_element_t<I, Args>>::Event;
    auto described = typed_fields<Event>();
    if (!described.ok())
      status = described.status();
    else
      input_fields.push_back(std::move(*described));
  };
  (describe_one.template operator()<Index>(), ...);
  EF_RETURN_IF_ERROR(status);
  EF_ASSIGN_OR_RETURN(auto signature,
                      projection_signature(selected, labels, fields, input_fields, identity));
  auto project = [arguments = std::move(arguments)](
                     const TriggerOccurrence& occurrence) noexcept -> StatusOr<Row> {
    std::tuple<std::optional<std::remove_cvref_t<std::tuple_element_t<Index, Args>>>...> decoded;
    Status status;
    const auto decode_one = [&]<std::size_t I>() noexcept {
      if (!status.ok())
        return;
      auto value = decode_argument(*std::get<I>(arguments), occurrence);
      if (!value.ok())
        status = value.status();
      else
        std::get<I>(decoded) = std::move(*value);
    };
    (decode_one.template operator()<Index>(), ...);
    EF_RETURN_IF_ERROR(status);
    EF_ASSIGN_OR_RETURN(auto result, std::invoke(Fn, *std::get<Index>(decoded)...));
    return encode_typed(result);
  };
  return BoundMatchProjection{.output_fields = std::move(fields),
                              .signature = std::move(signature),
                              .project = std::move(project)};
}
} // namespace typed_trigger_detail

template <auto Fn>
using MatchProjectionOutput =
    typename typed_trigger_detail::ProjectionTraits<decltype(Fn)>::output_type;

// Project immutable matched events in explicit parameter order. Required,
// optional and repeated labels require const T&, optional<T> and vector<T>
// parameters respectively. No current MV or latest source row is consulted.
template <auto Fn>
StatusOr<BoundMatchProjection>
bind_match_projection(const CatalogSnapshot& catalog, const std::vector<PatternLabelInfo>& labels,
                      const std::vector<std::string>& selected,
                      std::string_view implementation_identity) noexcept {
  using Traits = typed_trigger_detail::ProjectionTraits<decltype(Fn)>;
  static_assert(
      []() noexcept {
        if constexpr (std::is_pointer_v<std::remove_cv_t<decltype(Fn)>>)
          return Fn != nullptr;
        return true;
      }(),
      "Match projection function must not be null");
  static_assert(Traits::arity > 0 && Traits::arity <= 64,
                "Projection requires 1 to 64 noexcept typed parameters");
  if (selected.size() != Traits::arity)
    return Status(ErrorCode::InvalidArgument,
                  "Match projection label count differs from function arity");
  return typed_trigger_detail::bind_projection<Fn>(catalog, labels, selected,
                                                   implementation_identity,
                                                   std::make_index_sequence<Traits::arity>{});
}

} // namespace eventflow
