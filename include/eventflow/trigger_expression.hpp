#pragma once

#include "eventflow/catalog.hpp"
#include "eventflow/cep.hpp"
#include <chrono>
#include <limits>
#include <numeric>
#include <type_traits>

namespace eventflow {

struct EventSource {
  std::string kind;
  RelationId payload_relation = 0;
};

struct TriggerReadBinding {
  std::string alias;
  RelationId relation = 0;
};

struct TriggerExpressionNode;
using TriggerExpressionPtr = std::shared_ptr<const TriggerExpressionNode>;
class EventRef;
template <typename T> class TypedEventRef;
template <typename T> class TriggerColumn;
struct BoundTriggerPredicate;
using RowBoolCallback = std::function<StatusOr<bool>(const Row&)>;

// Missing rows and NULL compare false. Use is_null() or value_or() explicitly
// when absence should satisfy a condition. Boolean operators short-circuit.
class Predicate {
public:
  Predicate() noexcept = default;
  static Predicate constant(bool value) noexcept;
  static Predicate guard(std::vector<TriggerReadBinding> reads, FsmGuard guard,
                         std::string implementation_identity) noexcept;
  static Predicate event_guard(EventRef event, std::vector<Field> expected_fields,
                               RowBoolCallback guard, std::string implementation_identity) noexcept;
  Predicate operator!() const noexcept;
  Predicate operator&&(Predicate right) const noexcept;
  Predicate operator||(Predicate right) const noexcept;

private:
  explicit Predicate(TriggerExpressionPtr node) noexcept;
  TriggerExpressionPtr node_;
  template <typename T> friend class TriggerColumn;
  friend StatusOr<BoundTriggerPredicate> bind_trigger_predicate(const CatalogSnapshot&,
                                                                const Predicate&,
                                                                std::optional<EventRef>) noexcept;
};

namespace trigger_detail {
template <typename T>
inline constexpr bool supported_type = std::is_same_v<T, bool> || std::is_same_v<T, std::int64_t> ||
                                       std::is_same_v<T, double> || std::is_same_v<T, std::string>;

template <typename T> constexpr DataType column_type() noexcept {
  static_assert(supported_type<T>, "Trigger columns support bool, int64_t, double and std::string");
  if constexpr (std::is_same_v<T, bool>)
    return DataType::Bool;
  else if constexpr (std::is_same_v<T, std::int64_t>)
    return DataType::Int64;
  else if constexpr (std::is_same_v<T, double>)
    return DataType::Float64;
  else
    return DataType::String;
}

TriggerExpressionPtr column(EventSource event, RelationId relation, std::string alias,
                            std::string name, DataType type, bool event_column) noexcept;
TriggerExpressionPtr literal(Value value, DataType type) noexcept;
TriggerExpressionPtr operation(std::string operation, TriggerExpressionPtr left,
                               TriggerExpressionPtr right = {}) noexcept;
} // namespace trigger_detail

template <typename T> class TriggerColumn {
  static_assert(trigger_detail::supported_type<T>,
                "Trigger columns support bool, int64_t, double and std::string");

public:
  Predicate operator==(const T& value) const noexcept {
    return compare("eq", value);
  }

  Predicate operator!=(const T& value) const noexcept {
    return compare("ne", value);
  }

  Predicate operator<(const T& value) const noexcept {
    return compare("lt", value);
  }

  Predicate operator<=(const T& value) const noexcept {
    return compare("le", value);
  }

  Predicate operator>(const T& value) const noexcept {
    return compare("gt", value);
  }

  Predicate operator>=(const T& value) const noexcept {
    return compare("ge", value);
  }

  Predicate operator==(const TriggerColumn& value) const noexcept {
    return compare("eq", value);
  }

  Predicate operator!=(const TriggerColumn& value) const noexcept {
    return compare("ne", value);
  }

  Predicate operator<(const TriggerColumn& value) const noexcept {
    return compare("lt", value);
  }

  Predicate operator<=(const TriggerColumn& value) const noexcept {
    return compare("le", value);
  }

  Predicate operator>(const TriggerColumn& value) const noexcept {
    return compare("gt", value);
  }

  Predicate operator>=(const TriggerColumn& value) const noexcept {
    return compare("ge", value);
  }

  Predicate is_true() const noexcept
    requires std::is_same_v<T, bool>
  {
    return Predicate(trigger_detail::operation("is_true", node_));
  }

  Predicate is_null() const noexcept {
    return Predicate(trigger_detail::operation("is_null", node_));
  }

  TriggerColumn value_or(const T& value) const noexcept {
    return TriggerColumn(trigger_detail::operation(
        "value_or", node_,
        trigger_detail::literal(Value(value), trigger_detail::column_type<T>())));
  }

private:
  explicit TriggerColumn(TriggerExpressionPtr node) noexcept : node_(std::move(node)) {}

  Predicate compare(std::string op, const T& value) const noexcept {
    return Predicate(trigger_detail::operation(
        std::move(op), node_,
        trigger_detail::literal(Value(value), trigger_detail::column_type<T>())));
  }

  Predicate compare(std::string op, const TriggerColumn& value) const noexcept {
    return Predicate(trigger_detail::operation(std::move(op), node_, value.node_));
  }

  TriggerExpressionPtr node_;
  friend class EventRef;
  friend class ViewRef;
};

class EventRef {
public:
  static EventRef source(EventSource definition) noexcept;
  template <typename T> static TypedEventRef<T> source(EventSource definition) noexcept;
  const EventSource& definition() const noexcept;

  template <typename T> TriggerColumn<T> column(std::string name) const noexcept {
    return TriggerColumn<T>(trigger_detail::column(definition_, 0, {}, std::move(name),
                                                   trigger_detail::column_type<T>(), true));
  }

private:
  explicit EventRef(EventSource definition) noexcept;
  EventSource definition_;
};

class ViewRef {
public:
  explicit ViewRef(RelationId relation, std::string alias = {}) noexcept;

  template <typename T> TriggerColumn<T> column(std::string name) const noexcept {
    return TriggerColumn<T>(trigger_detail::column({}, relation_, alias_, std::move(name),
                                                   trigger_detail::column_type<T>(), false));
  }

private:
  RelationId relation_ = 0;
  std::string alias_;
};

struct BoundTriggerPredicate {
  FsmGuard fsm;
  CepGuard cep; // Empty when an opaque FSM guard has no safe CEP adapter.
  std::map<std::string, RelationId> reads;
  std::vector<EventSource> events;
  std::string signature;
};

StatusOr<BoundTriggerPredicate>
bind_trigger_predicate(const CatalogSnapshot& catalog, const Predicate& predicate,
                       std::optional<EventRef> current_event = {}) noexcept;

// Conversion is exact and checked before multiplying into microseconds. A
// builder retains conversion errors until its status-returning compile step.
class TriggerDuration {
public:
  TriggerDuration() noexcept = default;

  template <typename Rep, typename Period>
    requires(std::is_integral_v<Rep> && !std::is_same_v<Rep, bool> && sizeof(Rep) <= 8)
  TriggerDuration(std::chrono::duration<Rep, Period> duration) noexcept {
    const __int128 count = duration.count();
    const bool negative = count < 0;
    const auto magnitude = static_cast<unsigned __int128>(negative ? -count : count);
    const auto divisor = std::gcd(std::uintmax_t(Period::den), std::uintmax_t(1'000'000));
    const auto denominator = std::uintmax_t(Period::den) / divisor;
    const auto multiplier = std::uintmax_t(1'000'000) / divisor;
    const auto numerator = magnitude * static_cast<std::uintmax_t>(Period::num);
    if (numerator % denominator != 0) {
      status_ = Status(ErrorCode::InvalidArgument, "Trigger duration must be exact microseconds");
      return;
    }
    const auto quotient = numerator / denominator;
    const auto limit = static_cast<unsigned __int128>(std::numeric_limits<std::int64_t>::max()) +
                       static_cast<unsigned>(negative);
    if (quotient > limit / multiplier) {
      status_ = Status(ErrorCode::InvalidArgument, "Trigger duration overflows microseconds");
      return;
    }
    const auto signed_value = static_cast<__int128>(quotient * multiplier);
    microseconds_ = static_cast<std::int64_t>(negative ? -signed_value : signed_value);
  }

  static TriggerDuration from_microseconds(std::int64_t microseconds) noexcept {
    return TriggerDuration(std::chrono::microseconds(microseconds));
  }

  StatusOr<std::int64_t> microseconds() const noexcept {
    if (!status_.ok())
      return status_;
    return std::int64_t(microseconds_);
  }

private:
  std::int64_t microseconds_ = 0;
  Status status_;
};

} // namespace eventflow
