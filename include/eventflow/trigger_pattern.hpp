#pragma once

#include "eventflow/trigger_expression.hpp"

namespace eventflow {

struct TriggerPatternNode;
struct BoundTriggerPattern;
template <typename T> class TypedPattern;

// Immutable declarations compiled to the existing bounded E-NFA. A sequence can
// retain several overlapping starts; use Trigger::then_next for one armed wait.
class Pattern {
public:
  Pattern() noexcept = default;
  static Pattern begin(std::string label, EventRef event) noexcept;
  template <typename T> static TypedPattern<T> begin(std::string label) noexcept;

  // Conditions on the most recently appended event are combined with AND.
  // Apply conditions before optional(), repeat() or or_else(): those operations
  // create a group and no longer identify one unambiguous event to modify.
  Pattern where(Predicate predicate) const noexcept;
  Pattern next(std::string label, EventRef event) const noexcept;
  Pattern next_of_kind(std::string label, EventRef event) const noexcept;
  Pattern followed_by(std::string label, EventRef event) const noexcept;
  Pattern followed_by_any(std::string label, EventRef event) const noexcept;

  // Compose a whole group. Options belong to the final pattern: a nested right
  // group must not have its own within() or after_match() options.
  Pattern then(Pattern next, CepContiguity contiguity = CepContiguity::FollowedBy) const noexcept;
  Pattern or_else(Pattern alternative) const noexcept;
  // These operators wrap the whole current expression, like CepPattern. To make
  // only one event optional, build that event separately and append with then().
  Pattern optional() const noexcept;
  Pattern repeat(std::size_t minimum, std::size_t maximum,
                 CepContiguity inner = CepContiguity::FollowedBy) const noexcept;

  // Mandatory positive processing-time bound, with an exclusive deadline.
  Pattern within(TriggerDuration duration) const noexcept;
  Pattern after_match(CepSkipStrategy strategy) const noexcept;

private:
  explicit Pattern(std::shared_ptr<const TriggerPatternNode> node) noexcept;
  Pattern combine(Pattern next, bool alternative, CepContiguity contiguity) const noexcept;
  std::shared_ptr<const TriggerPatternNode> node_;
  std::optional<TriggerDuration> within_;
  CepSkipStrategy skip_ = CepSkipStrategy::NoSkip;
  bool explicit_skip_ = false;
  Status status_;
  friend StatusOr<BoundTriggerPattern> bind_trigger_pattern(const CatalogSnapshot&,
                                                            const Pattern&) noexcept;
};

struct PatternLabelInfo {
  std::string label;
  EventSource event;
  std::size_t minimum = 1;
  std::size_t maximum = 1;
};

struct BoundTriggerPattern {
  CepPattern pattern;
  std::int64_t within_us = 0;
  CepSkipStrategy skip = CepSkipStrategy::NoSkip;
  std::map<std::string, RelationId> reads;
  std::string signature;
  std::vector<EventSource> events;
  std::vector<PatternLabelInfo> labels = {};
  std::vector<std::string> input_kinds = {};
};

// Binds visible field names and declared read schemas. The caller passes the
// result and its actual owner budgets to CepProgram::create for final syntax,
// label uniqueness, repetition and resource-limit validation.
StatusOr<BoundTriggerPattern> bind_trigger_pattern(const CatalogSnapshot& catalog,
                                                   const Pattern& pattern) noexcept;

} // namespace eventflow
