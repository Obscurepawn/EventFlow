#include "eventflow/trigger_pattern.hpp"

#include <algorithm>
#include <limits>

namespace eventflow {
namespace {
enum class PatternKind { Event, Sequence, Alternative, Repeat };
constexpr std::size_t kMaxPatternNodes = 4096;
constexpr std::size_t kMaxPatternDepth = 64;

Status invalid(std::string_view message) noexcept {
  return {ErrorCode::InvalidArgument, message};
}
} // namespace

struct TriggerPatternNode {
  PatternKind kind = PatternKind::Event;
  std::string label = {};
  std::optional<EventRef> event = {};
  Predicate predicate = {};
  std::shared_ptr<const TriggerPatternNode> left = {};
  std::shared_ptr<const TriggerPatternNode> right = {};
  CepContiguity contiguity = CepContiguity::FollowedBy;
  std::size_t minimum = 1;
  std::size_t maximum = 1;
  std::size_t count = 1;
  std::size_t depth = 1;
};

namespace {
using PatternNode = std::shared_ptr<const TriggerPatternNode>;

StatusOr<PatternNode> attach_predicate(const PatternNode& node, Predicate predicate) noexcept {
  if (!node)
    return invalid("Pattern requires begin() before where()");

  auto next = std::make_shared<TriggerPatternNode>(*node);
  if (node->kind == PatternKind::Event) {
    next->predicate = next->predicate && std::move(predicate);
  } else if (node->kind == PatternKind::Sequence) {
    EF_ASSIGN_OR_RETURN(next->right, attach_predicate(node->right, std::move(predicate)));
  } else {
    return invalid("Pattern where() is ambiguous after a group; condition its events first");
  }

  return PatternNode(std::move(next));
}

struct BoundNode {
  CepPattern pattern;
  Json signature;
  std::vector<PatternLabelInfo> labels;
};

Status merge_dependencies(BoundTriggerPattern& target,
                          const BoundTriggerPredicate& source) noexcept {
  for (const auto& [alias, relation] : source.reads) {
    auto [position, inserted] = target.reads.emplace(alias, relation);
    if (!inserted && position->second != relation)
      return invalid("Pattern read alias refers to different relations");
  }

  for (const auto& event : source.events) {
    const auto found = std::find_if(
        target.events.begin(), target.events.end(),
        [&](const EventSource& existing) noexcept { return existing.kind == event.kind; });
    if (found == target.events.end())
      target.events.push_back(event);
    else if (found->payload_relation != event.payload_relation)
      return invalid("Pattern event kind refers to different payload schemas");
  }

  return {};
}

StatusOr<BoundNode> bind_node(const CatalogSnapshot& catalog, const PatternNode& node,
                              BoundTriggerPattern& result) noexcept {
  if (!node)
    return invalid("Pattern requires a nonempty declaration");
  if (node->kind == PatternKind::Event) {
    EF_ASSIGN_OR_RETURN(auto guard, bind_trigger_predicate(catalog, node->predicate, node->event));
    if (!guard.cep)
      return invalid("Pattern conditions cannot use opaque FSM-only guards");

    EF_RETURN_IF_ERROR(merge_dependencies(result, guard));
    return BoundNode{.pattern = CepPattern::event(node->label, node->event->definition().kind,
                                                  std::move(guard.cep)),
                     .signature = Json::array({"event", node->label, guard.signature}),
                     .labels = {{.label = node->label, .event = node->event->definition()}}};
  }

  EF_ASSIGN_OR_RETURN(auto left, bind_node(catalog, node->left, result));
  if (node->kind == PatternKind::Repeat) {
    for (auto& label : left.labels) {
      if ((node->minimum &&
           label.minimum > std::numeric_limits<std::size_t>::max() / node->minimum) ||
          (node->maximum &&
           label.maximum > std::numeric_limits<std::size_t>::max() / node->maximum))
        return invalid("Pattern label cardinality overflows");

      label.minimum *= node->minimum;
      label.maximum *= node->maximum;
    }

    return BoundNode{.pattern = left.pattern.repeat(node->minimum, node->maximum, node->contiguity),
                     .signature = Json::array({"repeat", std::move(left.signature), node->minimum,
                                               node->maximum, static_cast<int>(node->contiguity)}),
                     .labels = std::move(left.labels)};
  }

  EF_ASSIGN_OR_RETURN(auto right, bind_node(catalog, node->right, result));
  if (node->kind == PatternKind::Alternative) {
    for (auto& label : left.labels)
      label.minimum = 0;

    for (auto& label : right.labels)
      label.minimum = 0;
  }

  left.labels.insert(left.labels.end(), right.labels.begin(), right.labels.end());
  if (node->kind == PatternKind::Alternative)
    return BoundNode{.pattern = left.pattern.or_else(std::move(right.pattern)),
                     .signature = Json::array(
                         {"alternative", std::move(left.signature), std::move(right.signature)}),
                     .labels = std::move(left.labels)};

  return BoundNode{
      .pattern = left.pattern.then(std::move(right.pattern), node->contiguity),
      .signature = Json::array({"sequence", std::move(left.signature), std::move(right.signature),
                                static_cast<int>(node->contiguity)}),
      .labels = std::move(left.labels)};
}
} // namespace

Pattern::Pattern(std::shared_ptr<const TriggerPatternNode> node) noexcept
    : node_(std::move(node)) {}

Pattern Pattern::begin(std::string label, EventRef event) noexcept {
  return Pattern(std::make_shared<TriggerPatternNode>(TriggerPatternNode{
      .kind = PatternKind::Event, .label = std::move(label), .event = std::move(event)}));
}

Pattern Pattern::where(Predicate predicate) const noexcept {
  auto next = *this;
  if (!next.status_.ok())
    return next;

  auto node = attach_predicate(next.node_, std::move(predicate));
  if (!node.ok())
    next.status_ = node.status();
  else
    next.node_ = std::move(*node);
  return next;
}

Pattern Pattern::combine(Pattern right, bool alternative, CepContiguity contiguity) const noexcept {
  auto next = *this;
  if (!next.status_.ok())
    return next;
  if (!right.status_.ok()) {
    next.status_ = right.status_;
    return next;
  }

  if (!node_ || !right.node_) {
    next.status_ = invalid("Pattern composition requires nonempty operands");
    return next;
  }

  if (right.within_ || right.explicit_skip_) {
    next.status_ = invalid("Configure within() and after_match() on the final Pattern only");
    return next;
  }

  const auto depth = std::max(node_->depth, right.node_->depth) + 1;
  if (depth > kMaxPatternDepth || node_->count >= kMaxPatternNodes - right.node_->count) {
    next.status_ = invalid("Pattern declaration exceeds the bounded syntax size or depth");
    return next;
  }

  const auto count = node_->count + right.node_->count + 1;
  next.node_ = std::make_shared<TriggerPatternNode>(
      TriggerPatternNode{.kind = alternative ? PatternKind::Alternative : PatternKind::Sequence,
                         .left = node_,
                         .right = std::move(right.node_),
                         .contiguity = contiguity,
                         .count = count,
                         .depth = depth});
  return next;
}

Pattern Pattern::next(std::string label, EventRef event) const noexcept {
  return then(begin(std::move(label), std::move(event)), CepContiguity::Strict);
}

Pattern Pattern::next_of_kind(std::string label, EventRef event) const noexcept {
  return then(begin(std::move(label), std::move(event)), CepContiguity::NextOfKind);
}

Pattern Pattern::followed_by(std::string label, EventRef event) const noexcept {
  return then(begin(std::move(label), std::move(event)), CepContiguity::FollowedBy);
}

Pattern Pattern::followed_by_any(std::string label, EventRef event) const noexcept {
  return then(begin(std::move(label), std::move(event)), CepContiguity::FollowedByAny);
}

Pattern Pattern::then(Pattern next, CepContiguity contiguity) const noexcept {
  return combine(std::move(next), false, contiguity);
}

Pattern Pattern::or_else(Pattern alternative) const noexcept {
  return combine(std::move(alternative), true, CepContiguity::FollowedBy);
}

Pattern Pattern::optional() const noexcept {
  return repeat(0, 1);
}

Pattern Pattern::repeat(std::size_t minimum, std::size_t maximum,
                        CepContiguity inner) const noexcept {
  auto next = *this;
  if (!next.status_.ok())
    return next;
  if (!node_) {
    next.status_ = invalid("Pattern requires begin() before repeat() or optional()");
    return next;
  }

  if (node_->depth == kMaxPatternDepth || node_->count == kMaxPatternNodes) {
    next.status_ = invalid("Pattern declaration exceeds the bounded syntax size or depth");
    return next;
  }

  next.node_ = std::make_shared<TriggerPatternNode>(TriggerPatternNode{.kind = PatternKind::Repeat,
                                                                       .left = node_,
                                                                       .contiguity = inner,
                                                                       .minimum = minimum,
                                                                       .maximum = maximum,
                                                                       .count = node_->count + 1,
                                                                       .depth = node_->depth + 1});
  return next;
}

Pattern Pattern::within(TriggerDuration duration) const noexcept {
  auto next = *this;
  if (!next.status_.ok())
    return next;

  const auto value = duration.microseconds();
  if (!value.ok())
    next.status_ = value.status();
  else if (*value <= 0)
    next.status_ = invalid("Pattern within() requires a positive processing-time duration");
  else
    next.within_ = duration;
  return next;
}

Pattern Pattern::after_match(CepSkipStrategy strategy) const noexcept {
  auto next = *this;
  if (!next.status_.ok())
    return next;
  if (strategy != CepSkipStrategy::NoSkip && strategy != CepSkipStrategy::SkipToNext &&
      strategy != CepSkipStrategy::SkipPastLastEvent)
    next.status_ = invalid("Pattern after_match() requires a supported skip strategy");
  else {
    next.skip_ = strategy;
    next.explicit_skip_ = true;
  }

  return next;
}

StatusOr<BoundTriggerPattern> bind_trigger_pattern(const CatalogSnapshot& catalog,
                                                   const Pattern& pattern) noexcept {
  EF_RETURN_IF_ERROR(pattern.status_);
  if (!pattern.within_)
    return invalid("Pattern requires an explicit positive within() duration");

  EF_ASSIGN_OR_RETURN(const auto within_us, pattern.within_->microseconds());
  BoundTriggerPattern result{.pattern = {},
                             .within_us = within_us,
                             .skip = pattern.skip_,
                             .reads = {},
                             .signature = {},
                             .events = {}};
  EF_ASSIGN_OR_RETURN(auto node, bind_node(catalog, pattern.node_, result));
  result.pattern = std::move(node.pattern);
  result.labels = std::move(node.labels);
  result.signature = Json{{"format", "trigger_pattern.v1"},
                          {"pattern", std::move(node.signature)},
                          {"within_us", within_us},
                          {"skip", static_cast<int>(pattern.skip_)}}
                         .dump();
  return result;
}

} // namespace eventflow
