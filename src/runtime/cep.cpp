#include "eventflow/cep.hpp"

#include <algorithm>
#include <limits>
#include <map>
#include <set>
#include <tuple>

namespace eventflow {
namespace {
Status invalid(std::string_view message) noexcept {
  return {ErrorCode::InvalidArgument, message};
}

Status full(std::string_view message) noexcept {
  return {ErrorCode::ResourceExhausted, message};
}

bool name_ok(std::string_view value, std::size_t maximum = 256) noexcept {
  return !value.empty() && value.size() <= maximum && valid_utf8(value) &&
         value.find('\0') == std::string_view::npos;
}

StatusOr<Json> typed_row(const Row& row, std::size_t limit) noexcept {
  if (row.size() > limit / 4)
    return full("CEP typed row exceeds budget");

  Json result = Json::array();
  std::size_t used = 2;
  for (const auto& value : row) {
    if (const auto* text = std::get_if<std::string>(&value); text && text->size() > limit)
      return full("CEP string exceeds budget");
    if (const auto* vector = std::get_if<std::vector<double>>(&value);
        vector && vector->size() > limit / sizeof(double))
      return full("CEP vector exceeds budget");

    EF_ASSIGN_OR_RETURN(auto encoded, value_to_json(value));
    Json entry = Json::array({value.index(), std::move(encoded)});
    const auto bytes = entry.dump().size() + (result.empty() ? 0 : 1);
    if (used > limit || bytes > limit - used)
      return full("CEP typed row exceeds budget");

    used += bytes;
    result.push_back(std::move(entry));
  }

  return result;
}

StatusOr<std::uint64_t> unsigned_number(const Json& value) noexcept {
  if (!value.is_number_integer() || (!value.is_number_unsigned() && value.get<std::int64_t>() < 0))
    return invalid("invalid CEP unsigned integer");

  return value.get<std::uint64_t>();
}

StatusOr<std::int64_t> time_number(const Json& value) noexcept {
  EF_ASSIGN_OR_RETURN(const auto number, unsigned_number(value));
  if (number > INT64_MAX)
    return invalid("CEP time overflow");

  return static_cast<std::int64_t>(number);
}

StatusOr<Row> parse_row(const Json& json, std::size_t limit) noexcept {
  if (!json.is_array() || json.size() > limit / 4)
    return invalid("invalid CEP typed row");

  constexpr DataType types[] = {DataType::Int64,   DataType::Bool,   DataType::Int64,
                                DataType::Float64, DataType::String, DataType::FloatVector};
  Row row;
  for (const auto& entry : json) {
    if (!entry.is_array() || entry.size() != 2)
      return invalid("invalid CEP typed value");

    EF_ASSIGN_OR_RETURN(const auto tag, unsigned_number(entry[0]));
    if (tag > 5 || (tag == 0 && !entry[1].is_null()))
      return invalid("invalid CEP value tag");

    EF_ASSIGN_OR_RETURN(auto value, value_from_json(entry[1], types[tag], tag == 0));
    if (value.index() != tag)
      return invalid("CEP value type mismatch");

    row.push_back(std::move(value));
  }

  EF_ASSIGN_OR_RETURN(auto encoded, typed_row(row, limit));
  (void)encoded;
  return row;
}

bool has_fields(const Json& value, std::initializer_list<const char*> fields) noexcept {
  if (!value.is_object())
    return false;

  for (const auto* field : fields)
    if (!value.contains(field))
      return false;

  return true;
}

Status validate_registers(const std::vector<Field>& fields, const Row& row,
                          std::size_t limit) noexcept {
  if (fields.size() != row.size())
    return invalid("CEP register arity mismatch");

  EF_ASSIGN_OR_RETURN(auto encoded, typed_row(row, limit));
  (void)encoded;
  for (std::size_t i = 0; i < row.size(); ++i) {
    EF_ASSIGN_OR_RETURN(auto json, value_to_json(row[i]));
    EF_ASSIGN_OR_RETURN(auto typed, value_from_json(json, fields[i].type, fields[i].nullable));
    if (typed.index() != row[i].index())
      return invalid("CEP register type mismatch");
  }

  return {};
}

StatusOr<Json> encode_reads(const FsmReads& reads, std::size_t limit) noexcept {
  Json result = Json::object();
  std::size_t used = 2;
  for (const auto& [alias, row] : reads) {
    if (!name_ok(alias))
      return invalid("invalid CEP read alias");

    Json value = nullptr;
    if (row) {
      EF_ASSIGN_OR_RETURN(value, typed_row(*row, limit));
    }

    const auto bytes = Json{{alias, value}}.dump().size() - 2 + (result.empty() ? 0 : 1);
    if (used > limit || bytes > limit - used)
      return full("CEP read snapshot exceeds budget");

    used += bytes;
    result[alias] = std::move(value);
  }

  return result;
}

StatusOr<FsmReads> parse_reads(const Json& json, std::size_t limit) noexcept {
  if (!json.is_object() || json.size() > 64)
    return invalid("invalid CEP read snapshot");

  FsmReads reads;
  for (auto it = json.begin(); it != json.end(); ++it) {
    if (it.value().is_null())
      reads.emplace(it.key(), std::nullopt);
    else {
      EF_ASSIGN_OR_RETURN(auto row, parse_row(it.value(), limit));
      reads.emplace(it.key(), std::move(row));
    }
  }

  EF_ASSIGN_OR_RETURN(auto encoded, encode_reads(reads, limit));
  (void)encoded;
  return reads;
}

StatusOr<Json> event_json(const CepEvent& event, std::size_t limit) noexcept {
  if (!name_ok(event.id, 4096) || !name_ok(event.kind) || event.kind == "*" || event.time_us < 0)
    return invalid("invalid CEP event identity, kind or time");

  EF_ASSIGN_OR_RETURN(auto row, typed_row(event.values, limit));
  Json json{
      {"id", event.id}, {"kind", event.kind}, {"time", event.time_us}, {"values", std::move(row)}};
  if (json.dump().size() > limit)
    return full("CEP event exceeds byte budget");

  return json;
}

struct Work {
  std::size_t remaining;

  Status use() noexcept {
    if (remaining == 0)
      return full("CEP transition and history work budget exceeded");

    --remaining;
    return {};
  }
};
enum class PatternKind { Event, Sequence, Alternative, Repeat, Greedy, Negative };

struct StoredEvent {
  CepEvent event;
  std::uint64_t sequence = 0;
};

struct HistoryNode {
  std::uint64_t id = 0;
  std::uint32_t atom = 0;
  std::string label;
  std::shared_ptr<const HistoryNode> previous;
  std::shared_ptr<const StoredEvent> event;
  std::uint64_t start_sequence = 0;
  std::int64_t start_time_us = 0;
  std::size_t length = 0;
};

using History = std::shared_ptr<const HistoryNode>;

struct Path {
  std::uint32_t node = 0;
  History tail;
  Row registers;
};

struct PendingMatch {
  std::string id;
  History tail;
  Row registers;
  std::shared_ptr<const FsmReads> reads;
  std::int64_t matched_at_us = 0;
};

struct Edge {
  CepAction action;
  std::uint32_t target;
};

struct Node {
  std::vector<Edge> edges;
  bool accepting = false;
  std::string label;
  std::string kind;
  CepGuard guard;
  CepUpdate update;
  CepContiguity contiguity = CepContiguity::Strict;

  bool atom() const noexcept {
    return !label.empty();
  }
};

StatusOr<std::vector<CepCapture>> materialize(const History& tail, Work* work = nullptr) noexcept {
  std::vector<CepCapture> result;
  for (auto cursor = tail; cursor; cursor = cursor->previous) {
    if (work)
      EF_RETURN_IF_ERROR(work->use());

    result.push_back({.label = cursor->label, .event = cursor->event->event});
  }

  std::reverse(result.begin(), result.end());
  return result;
}

Json history_identity(const History& tail) noexcept {
  std::vector<const HistoryNode*> reversed;
  for (auto cursor = tail.get(); cursor; cursor = cursor->previous.get())
    reversed.push_back(cursor);

  Json identity = Json::array();
  for (auto it = reversed.rbegin(); it != reversed.rend(); ++it)
    identity.push_back(Json::array({(*it)->label, (*it)->event->event.id}));

  return identity;
}
} // namespace

struct CepPatternNode {
  PatternKind kind = PatternKind::Event;
  std::string label;
  std::string event_kind;
  CepGuard guard;
  CepUpdate update;
  std::shared_ptr<const CepPatternNode> left;
  std::shared_ptr<const CepPatternNode> right;
  CepContiguity contiguity = CepContiguity::FollowedBy;
  std::size_t minimum = 0;
  std::optional<std::size_t> maximum;
};

struct CepStateData {
  std::string fingerprint;
  std::int64_t time_us = 0;
  std::uint64_t event_sequence = 0;
  std::uint64_t next_history_id = 1;
  std::string last_event_id;
  std::vector<Path> paths;
  std::vector<PendingMatch> pending;
};

CepPattern::CepPattern(std::shared_ptr<const CepPatternNode> node) noexcept
    : node_(std::move(node)) {}

CepPattern CepPattern::event(std::string label, std::string kind, CepGuard guard,
                             CepUpdate update) noexcept {
  auto node = std::make_shared<CepPatternNode>();
  node->label = std::move(label);
  node->event_kind = std::move(kind);
  node->guard = std::move(guard);
  node->update = std::move(update);

  return CepPattern(std::move(node));
}

CepPattern CepPattern::then(CepPattern next, CepContiguity contiguity) const noexcept {
  auto node = std::make_shared<CepPatternNode>();
  node->kind = PatternKind::Sequence;
  node->left = node_;
  node->right = std::move(next.node_);
  node->contiguity = contiguity;

  return CepPattern(std::move(node));
}

CepPattern CepPattern::or_else(CepPattern alternative) const noexcept {
  auto node = std::make_shared<CepPatternNode>();
  node->kind = PatternKind::Alternative;
  node->left = node_;
  node->right = std::move(alternative.node_);

  return CepPattern(std::move(node));
}

CepPattern CepPattern::optional() const noexcept {
  return repeat(0, 1);
}

CepPattern CepPattern::repeat(std::size_t minimum, std::optional<std::size_t> maximum,
                              CepContiguity inner) const noexcept {
  auto node = std::make_shared<CepPatternNode>();
  node->kind = PatternKind::Repeat;
  node->left = node_;
  node->minimum = minimum;
  node->maximum = maximum;
  node->contiguity = inner;

  return CepPattern(std::move(node));
}

CepPattern CepPattern::greedy() const noexcept {
  auto node = std::make_shared<CepPatternNode>();
  node->kind = PatternKind::Greedy;
  node->left = node_;
  return CepPattern(std::move(node));
}

CepPattern CepPattern::negate() const noexcept {
  auto node = std::make_shared<CepPatternNode>();
  node->kind = PatternKind::Negative;
  node->left = node_;
  return CepPattern(std::move(node));
}

CepState::CepState(std::shared_ptr<const CepStateData> data) noexcept : data_(std::move(data)) {}

struct CepProgram::Impl {
  CepDefinition definition;
  std::vector<Node> nodes;
  std::uint32_t start = 0;
  std::string fingerprint;

  struct Fragment {
    std::uint32_t start;
    std::uint32_t end; // Reached only after this operand consumed at least one event.
    std::optional<std::uint32_t> empty_end;
  };

  struct PatternInfo {
    Json json;
    bool nullable = false;
    std::size_t maximum = 0;
  };

  StatusOr<PatternInfo> validate_pattern(const std::shared_ptr<const CepPatternNode>& pattern,
                                         std::set<std::string>& labels, std::size_t depth,
                                         std::size_t& count) const noexcept;
  StatusOr<std::uint32_t> add_node() noexcept;
  StatusOr<Fragment> compile(const std::shared_ptr<const CepPatternNode>& pattern,
                             CepContiguity incoming) noexcept;
  Status validate_reads(const FsmReads& reads) const noexcept;
  Status state_matches(const CepStateData& state) const noexcept;
  StatusOr<std::vector<std::uint32_t>> closure(std::uint32_t node,
                                               Work* work = nullptr) const noexcept;
  StatusOr<std::string> match_id(const History& tail) const noexcept;
  StatusOr<std::string> path_id(const Path& path) const noexcept;
  StatusOr<Json> encode(const CepStateData& state) const noexcept;
  StatusOr<std::shared_ptr<CepStateData>> decode(const Json& json) const noexcept;
  Status finish(CepStateData& state, CepStep& result) const noexcept;
  Status expire(CepStateData& state, std::int64_t now, CepStep& result, Work& work) const noexcept;
  Status outputs_fit(const CepStep& result) const noexcept;
  Status reserve_cleanup(const CepStateData& state) const noexcept;
};

StatusOr<CepProgram::Impl::PatternInfo>
CepProgram::Impl::validate_pattern(const std::shared_ptr<const CepPatternNode>& pattern,
                                   std::set<std::string>& labels, std::size_t depth,
                                   std::size_t& count) const noexcept {
  if (!pattern || depth > 64 || ++count > definition.limits.max_nodes)
    return invalid("missing, deeply nested or oversized CEP pattern");
  if (pattern->kind == PatternKind::Greedy || pattern->kind == PatternKind::Negative)
    return invalid("negative and greedy CEP patterns are not supported");
  if (pattern->contiguity != CepContiguity::Strict &&
      pattern->contiguity != CepContiguity::FollowedBy &&
      pattern->contiguity != CepContiguity::FollowedByAny &&
      pattern->contiguity != CepContiguity::NextOfKind)
    return invalid("invalid CEP contiguity");
  if (pattern->kind == PatternKind::Event) {
    if (!name_ok(pattern->label) || !name_ok(pattern->event_kind) ||
        !labels.insert(pattern->label).second)
      return invalid("CEP labels must be valid and unique in the pattern syntax");

    return PatternInfo{.json = Json::array({"event", pattern->label, pattern->event_kind,
                                            static_cast<bool>(pattern->guard),
                                            static_cast<bool>(pattern->update)}),
                       .nullable = false,
                       .maximum = 1};
  }

  EF_ASSIGN_OR_RETURN(auto left, validate_pattern(pattern->left, labels, depth + 1, count));
  if (pattern->kind == PatternKind::Repeat) {
    if (!pattern->maximum)
      return invalid("unbounded CEP repetition is not supported");
    if (pattern->minimum > *pattern->maximum || *pattern->maximum > 64 || left.nullable)
      return invalid("invalid CEP repetition or nullable repeated body");
    if (*pattern->maximum != 0 &&
        left.maximum > definition.limits.max_events_per_match / *pattern->maximum)
      return full("CEP repetition exceeds match length budget");

    return PatternInfo{.json =
                           Json::array({"repeat", left.json, pattern->minimum, *pattern->maximum,
                                        static_cast<int>(pattern->contiguity)}),
                       .nullable = pattern->minimum == 0,
                       .maximum = left.maximum * *pattern->maximum};
  }

  EF_ASSIGN_OR_RETURN(auto right, validate_pattern(pattern->right, labels, depth + 1, count));
  if (pattern->kind == PatternKind::Sequence) {
    if (left.maximum > definition.limits.max_events_per_match - right.maximum)
      return full("CEP sequence exceeds match length budget");

    return PatternInfo{.json = Json::array({"sequence", left.json, right.json,
                                            static_cast<int>(pattern->contiguity)}),
                       .nullable = left.nullable && right.nullable,
                       .maximum = left.maximum + right.maximum};
  }

  return PatternInfo{.json = Json::array({"or", left.json, right.json}),
                     .nullable = left.nullable || right.nullable,
                     .maximum = std::max(left.maximum, right.maximum)};
}

StatusOr<std::uint32_t> CepProgram::Impl::add_node() noexcept {
  if (nodes.size() == definition.limits.max_nodes)
    return full("compiled CEP graph exceeds node budget");

  nodes.emplace_back();
  return static_cast<std::uint32_t>(nodes.size() - 1);
}

StatusOr<CepProgram::Impl::Fragment>
CepProgram::Impl::compile(const std::shared_ptr<const CepPatternNode>& pattern,
                          CepContiguity incoming) noexcept {
  EF_ASSIGN_OR_RETURN(const auto begin, add_node());
  EF_ASSIGN_OR_RETURN(const auto end, add_node());
  std::optional<std::uint32_t> empty;
  if (pattern->kind == PatternKind::Event) {
    nodes[begin].label = pattern->label;
    nodes[begin].kind = pattern->event_kind;
    nodes[begin].guard = pattern->guard;
    nodes[begin].update = pattern->update;
    nodes[begin].contiguity = incoming;
    nodes[begin].edges.push_back({.action = CepAction::Take, .target = end});
    if (incoming != CepContiguity::Strict)
      nodes[begin].edges.push_back({.action = CepAction::Ignore, .target = begin});
  } else if (pattern->kind == PatternKind::Repeat) {
    if (pattern->minimum == 0) {
      EF_ASSIGN_OR_RETURN(auto empty_node, add_node());
      empty = empty_node;
      nodes[begin].edges.push_back({.action = CepAction::Proceed, .target = empty_node});
    }

    auto previous = begin;
    for (std::size_t i = 0; i < *pattern->maximum; ++i) {
      EF_ASSIGN_OR_RETURN(auto body,
                          compile(pattern->left, i == 0 ? incoming : pattern->contiguity));
      nodes[previous].edges.push_back({.action = CepAction::Proceed, .target = body.start});
      previous = body.end;
      if (i + 1 >= pattern->minimum)
        nodes[previous].edges.push_back({.action = CepAction::Proceed, .target = end});
    }
  } else {
    EF_ASSIGN_OR_RETURN(auto left, compile(pattern->left, incoming));
    EF_ASSIGN_OR_RETURN(auto right, compile(pattern->right, pattern->kind == PatternKind::Sequence
                                                                ? pattern->contiguity
                                                                : incoming));

    nodes[begin].edges.push_back({.action = CepAction::Proceed, .target = left.start});
    nodes[right.end].edges.push_back({.action = CepAction::Proceed, .target = end});

    if (pattern->kind == PatternKind::Sequence) {
      nodes[left.end].edges.push_back({.action = CepAction::Proceed, .target = right.start});
      if (right.empty_end)
        nodes[*right.empty_end].edges.push_back({.action = CepAction::Proceed, .target = end});

      if (left.empty_end) {
        // Skipping a nullable left operand has not started this group. Its right operand
        // must still honor the group's incoming contiguity, rather than the internal edge.
        EF_ASSIGN_OR_RETURN(auto first_right, compile(pattern->right, incoming));
        nodes[*left.empty_end].edges.push_back(
            {.action = CepAction::Proceed, .target = first_right.start});
        nodes[first_right.end].edges.push_back({.action = CepAction::Proceed, .target = end});

        if (first_right.empty_end) {
          EF_ASSIGN_OR_RETURN(auto empty_node, add_node());
          empty = empty_node;
          nodes[*first_right.empty_end].edges.push_back(
              {.action = CepAction::Proceed, .target = empty_node});
        }
      }
    } else {
      nodes[begin].edges.push_back({.action = CepAction::Proceed, .target = right.start});
      nodes[left.end].edges.push_back({.action = CepAction::Proceed, .target = end});

      if (left.empty_end || right.empty_end) {
        EF_ASSIGN_OR_RETURN(auto empty_node, add_node());
        empty = empty_node;
        if (left.empty_end)
          nodes[*left.empty_end].edges.push_back(
              {.action = CepAction::Proceed, .target = empty_node});
        if (right.empty_end)
          nodes[*right.empty_end].edges.push_back(
              {.action = CepAction::Proceed, .target = empty_node});
      }
    }
  }

  return Fragment{.start = begin, .end = end, .empty_end = empty};
}

StatusOr<std::vector<std::uint32_t>> CepProgram::Impl::closure(std::uint32_t node,
                                                               Work* work) const noexcept {
  if (node >= nodes.size())
    return invalid("invalid CEP graph node");

  std::vector<std::uint32_t> todo{node};
  std::set<std::uint32_t> visited;
  std::vector<std::uint32_t> result;
  while (!todo.empty()) {
    const auto current = todo.back();
    todo.pop_back();
    if (!visited.insert(current).second)
      continue;
    if (work)
      EF_RETURN_IF_ERROR(work->use());
    if (nodes[current].atom() || nodes[current].accepting)
      result.push_back(current);

    for (const auto& edge : nodes[current].edges)
      if (edge.action == CepAction::Proceed)
        todo.push_back(edge.target);
  }

  std::sort(result.begin(), result.end());
  return result;
}

Status CepProgram::Impl::validate_reads(const FsmReads& reads) const noexcept {
  if (reads.size() != definition.reads.size())
    return invalid("CEP declared read set mismatch");

  for (const auto& alias : definition.reads)
    if (!reads.contains(alias))
      return invalid("missing CEP declared read");

  EF_ASSIGN_OR_RETURN(auto json, encode_reads(reads, definition.limits.max_read_bytes));
  (void)json;
  return {};
}

Status CepProgram::Impl::state_matches(const CepStateData& state) const noexcept {
  if (state.fingerprint != fingerprint)
    return Status(ErrorCode::FailedPrecondition, "CEP state and definition mismatch");

  return {};
}

StatusOr<std::string> CepProgram::Impl::match_id(const History& tail) const noexcept {
  return Json::array({definition.id, definition.version, history_identity(tail)}).dump();
}

StatusOr<std::string> CepProgram::Impl::path_id(const Path& path) const noexcept {
  EF_ASSIGN_OR_RETURN(auto registers,
                      typed_row(path.registers, definition.limits.max_register_bytes));
  return Json::array({path.node, history_identity(path.tail), std::move(registers)}).dump();
}

StatusOr<Json> CepProgram::Impl::encode(const CepStateData& state) const noexcept {
  EF_RETURN_IF_ERROR(state_matches(state));
  const auto& limits = definition.limits;
  if (state.paths.size() > limits.max_paths || state.pending.size() > limits.max_pending_matches)
    return full("CEP path or completed-match budget exceeded");

  std::map<std::uint64_t, History> history;
  std::map<std::string, std::shared_ptr<const StoredEvent>> events;
  const auto collect = [&](History cursor) noexcept -> Status {
    while (cursor) {
      if (!history.emplace(cursor->id, cursor).second)
        break;

      events.emplace(cursor->event->event.id, cursor->event);
      if (history.size() > limits.max_history_nodes || events.size() > limits.max_events)
        return full("CEP shared history or event budget exceeded");

      cursor = cursor->previous;
    }

    return {};
  };
  for (const auto& path : state.paths)
    EF_RETURN_IF_ERROR(collect(path.tail));

  for (const auto& match : state.pending)
    EF_RETURN_IF_ERROR(collect(match.tail));

  Json json{{"format", 1},
            {"fingerprint", fingerprint},
            {"time", state.time_us},
            {"event_sequence", state.event_sequence},
            {"next_history_id", state.next_history_id},
            {"last_event_id", state.last_event_id},
            {"events", Json::array()},
            {"nodes", Json::array()},
            {"paths", Json::array()},
            {"pending", Json::array()}};
  for (const auto& [id, event] : events) {
    (void)id;
    EF_ASSIGN_OR_RETURN(auto encoded, event_json(event->event, limits.max_event_bytes));
    encoded["sequence"] = event->sequence;
    json["events"].push_back(std::move(encoded));
  }

  for (const auto& [id, node] : history)
    json["nodes"].push_back(Json::array({id, node->atom, node->previous ? node->previous->id : 0,
                                         node->event->event.id, node->label}));

  for (const auto& path : state.paths) {
    EF_ASSIGN_OR_RETURN(auto registers, typed_row(path.registers, limits.max_register_bytes));
    json["paths"].push_back(Json::array({path.node, path.tail->id, std::move(registers)}));
  }

  for (const auto& match : state.pending) {
    EF_ASSIGN_OR_RETURN(auto registers, typed_row(match.registers, limits.max_register_bytes));
    EF_ASSIGN_OR_RETURN(auto reads, encode_reads(*match.reads, limits.max_read_bytes));
    json["pending"].push_back(Json::array(
        {match.id, match.tail->id, std::move(registers), std::move(reads), match.matched_at_us}));
  }

  if (json.dump().size() > limits.max_state_bytes)
    return full("CEP state snapshot exceeds byte budget");

  EF_RETURN_IF_ERROR(reserve_cleanup(state));
  return json;
}

Status CepProgram::Impl::reserve_cleanup(const CepStateData& state) const noexcept {
  // Every accepted state must be drainable without admitting another external input. This
  // conservative dry run reserves timeout work/output, including unblocked completed matches.
  auto candidate = state;
  CepStep output;
  Work work{.remaining = definition.limits.max_work_per_step};

  return expire(candidate, INT64_MAX, output, work);
}

Status CepProgram::Impl::outputs_fit(const CepStep& result) const noexcept {
  const auto& limits = definition.limits;
  if (result.matches.size() > limits.max_matches_per_step ||
      result.timeouts.size() > limits.max_timeouts_per_step)
    return full("CEP step output count exceeded");

  std::size_t bytes = 0;
  const auto charge = [&](std::size_t amount) noexcept -> Status {
    if (amount > limits.max_output_bytes - bytes)
      return full("CEP step output byte budget exceeded");

    bytes += amount;
    return {};
  };
  const auto captures = [&](const std::vector<CepCapture>& values) noexcept -> Status {
    for (const auto& capture : values) {
      EF_ASSIGN_OR_RETURN(auto json, event_json(capture.event, limits.max_event_bytes));
      EF_RETURN_IF_ERROR(charge(json.dump().size() + capture.label.size() + 32));
    }

    return {};
  };
  for (const auto& match : result.matches) {
    EF_RETURN_IF_ERROR(charge(match.id.size() + 128));
    EF_RETURN_IF_ERROR(captures(match.events));
    EF_ASSIGN_OR_RETURN(auto registers, typed_row(match.registers, limits.max_register_bytes));
    EF_ASSIGN_OR_RETURN(auto reads, encode_reads(match.reads, limits.max_read_bytes));
    EF_RETURN_IF_ERROR(charge(registers.dump().size() + reads.dump().size()));
  }

  for (const auto& timeout : result.timeouts) {
    EF_RETURN_IF_ERROR(charge(timeout.path_id.size() + 64));
    EF_RETURN_IF_ERROR(captures(timeout.events));
  }

  return {};
}

Status CepProgram::Impl::finish(CepStateData& state, CepStep& result) const noexcept {
  std::sort(state.pending.begin(), state.pending.end(),
            [](const PendingMatch& a, const PendingMatch& b) noexcept {
              return std::tie(a.tail->start_sequence, a.matched_at_us, a.id) <
                     std::tie(b.tail->start_sequence, b.matched_at_us, b.id);
            });
  while (!state.pending.empty()) {
    const auto& first = state.pending.front();
    if (definition.skip != CepSkipStrategy::NoSkip &&
        std::any_of(state.paths.begin(), state.paths.end(), [&](const Path& path) noexcept {
          return path.tail->start_sequence < first.tail->start_sequence;
        }))
      break;
    if (result.matches.size() == definition.limits.max_matches_per_step)
      return full("CEP match output count exceeded");

    auto selected = std::move(state.pending.front());
    state.pending.erase(state.pending.begin());
    EF_ASSIGN_OR_RETURN(auto history, materialize(selected.tail));
    result.matches.push_back({.id = selected.id,
                              .events = std::move(history),
                              .registers = selected.registers,
                              .reads = *selected.reads,
                              .matched_at_us = selected.matched_at_us,
                              .emitted_at_us = state.time_us});
    EF_RETURN_IF_ERROR(outputs_fit(result));
    const auto prunes = [&](const History& tail) noexcept {
      return definition.skip == CepSkipStrategy::SkipToNext
                 ? tail->start_sequence == selected.tail->start_sequence
                 : definition.skip == CepSkipStrategy::SkipPastLastEvent &&
                       tail->start_sequence <= selected.tail->event->sequence;
    };
    std::erase_if(state.paths, [&](const Path& path) noexcept { return prunes(path.tail); });
    std::erase_if(state.pending,
                  [&](const PendingMatch& match) noexcept { return prunes(match.tail); });
  }

  return {};
}

Status CepProgram::Impl::expire(CepStateData& state, std::int64_t now, CepStep& result,
                                Work& work) const noexcept {
  if (now < state.time_us)
    return invalid("CEP processing time cannot move backwards");

  state.time_us = now;
  for (auto it = state.paths.begin(); it != state.paths.end();) {
    EF_RETURN_IF_ERROR(work.use());
    const auto deadline = it->tail->start_time_us + definition.within_us;
    if (deadline > now) {
      ++it;
      continue;
    }

    if (result.timeouts.size() == definition.limits.max_timeouts_per_step)
      return full("CEP timeout output count exceeded");

    EF_ASSIGN_OR_RETURN(auto id, path_id(*it));
    EF_ASSIGN_OR_RETURN(auto captures, materialize(it->tail, &work));
    result.timeouts.push_back({.path_id = std::move(id),
                               .events = std::move(captures),
                               .scheduled_time_us = deadline,
                               .time_us = now});
    EF_RETURN_IF_ERROR(outputs_fit(result));
    it = state.paths.erase(it); // Shared prefixes survive while any other root owns them.
  }

  return finish(state, result);
}

CepProgram::CepProgram(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}

CepProgram::~CepProgram() noexcept = default;

StatusOr<std::unique_ptr<CepProgram>> CepProgram::create(CepDefinition definition) noexcept {
  if (definition.definition_identity.size() > 1024 * 1024 ||
      !valid_utf8(definition.definition_identity))
    return invalid("CEP definition identity must be valid UTF-8 within 1 MiB");

  const auto& l = definition.limits;
  if (!name_ok(definition.id) || definition.version == 0 || definition.within_us <= 0 ||
      definition.reads.size() > 64 || definition.register_fields.size() > 64 || l.max_nodes == 0 ||
      l.max_nodes > 4096 || l.max_paths == 0 || l.max_paths > 100000 || l.max_history_nodes == 0 ||
      l.max_history_nodes > 1000000 || l.max_events == 0 || l.max_events > 1000000 ||
      l.max_pending_matches == 0 || l.max_pending_matches > 100000 || l.max_matches_per_step == 0 ||
      l.max_matches_per_step > 100000 || l.max_timeouts_per_step == 0 ||
      l.max_timeouts_per_step > 100000 || l.max_events_per_match == 0 ||
      l.max_events_per_match > 256 || l.max_work_per_step == 0 || l.max_work_per_step > 10000000 ||
      l.max_event_bytes < 64 || l.max_register_bytes < 2 || l.max_read_bytes < 2 ||
      l.max_output_bytes == 0 || l.max_output_bytes > 64 * 1024 * 1024 || l.max_state_bytes == 0 ||
      l.max_state_bytes > 64 * 1024 * 1024 || l.max_event_bytes > l.max_state_bytes ||
      l.max_register_bytes > l.max_state_bytes || l.max_read_bytes > l.max_state_bytes ||
      (definition.skip != CepSkipStrategy::NoSkip &&
       definition.skip != CepSkipStrategy::SkipToNext &&
       definition.skip != CepSkipStrategy::SkipPastLastEvent))
    return invalid("invalid CEP definition or limits");

  std::set<std::string> names;
  std::set<std::uint32_t> ids;
  Json fields = Json::array();
  for (const auto& field : definition.register_fields) {
    if (!name_ok(field.name) || !names.insert(field.name).second || !ids.insert(field.id).second ||
        data_type_name(field.type).empty())
      return invalid("invalid CEP register schema");

    fields.push_back(
        Json::array({field.id, field.name, static_cast<int>(field.type), field.nullable}));
  }

  EF_RETURN_IF_ERROR(validate_registers(definition.register_fields, definition.initial_registers,
                                        l.max_register_bytes));
  names.clear();
  for (const auto& alias : definition.reads)
    if (!name_ok(alias) || !names.insert(alias).second)
      return invalid("invalid or duplicate CEP read alias");

  auto impl = std::make_unique<Impl>();
  impl->definition = std::move(definition);
  names.clear();
  std::size_t count = 0;
  EF_ASSIGN_OR_RETURN(auto info,
                      impl->validate_pattern(impl->definition.pattern.node_, names, 0, count));
  if (info.nullable || info.maximum > impl->definition.limits.max_events_per_match)
    return invalid("CEP patterns must consume events within the configured match length");

  EF_ASSIGN_OR_RETURN(auto fragment,
                      impl->compile(impl->definition.pattern.node_, CepContiguity::Strict));
  impl->start = fragment.start;
  impl->nodes[fragment.end].accepting = true;

  // Reject configurations that cannot even evaluate a fresh input under a conservative
  // all-start-branches reservation. No callbacks are invoked during compilation.
  Work startup{.remaining = impl->definition.limits.max_work_per_step};
  EF_ASSIGN_OR_RETURN(auto roots, impl->closure(impl->start, &startup));

  for (auto root : roots) {
    const auto& node = impl->nodes[root];
    if (!node.atom())
      continue;

    EF_RETURN_IF_ERROR(startup.use());
    if (node.guard)
      EF_RETURN_IF_ERROR(startup.use());
    if (node.update)
      EF_RETURN_IF_ERROR(startup.use());

    for (const auto& edge : node.edges) {
      EF_RETURN_IF_ERROR(startup.use());
      if (edge.action == CepAction::Take) {
        EF_ASSIGN_OR_RETURN(auto following, impl->closure(edge.target, &startup));
        (void)following;
      }
    }
  }

  EF_ASSIGN_OR_RETURN(auto registers, typed_row(impl->definition.initial_registers,
                                                impl->definition.limits.max_register_bytes));
  const auto& limits = impl->definition.limits;
  Json budgets =
      Json::array({limits.max_nodes, limits.max_paths, limits.max_history_nodes, limits.max_events,
                   limits.max_pending_matches, limits.max_matches_per_step,
                   limits.max_timeouts_per_step, limits.max_events_per_match,
                   limits.max_work_per_step, limits.max_event_bytes, limits.max_register_bytes,
                   limits.max_read_bytes, limits.max_output_bytes, limits.max_state_bytes});
  Json fingerprint{{"format", 1},
                   {"id", impl->definition.id},
                   {"version", impl->definition.version},
                   {"pattern", std::move(info.json)},
                   {"within", impl->definition.within_us},
                   {"skip", static_cast<int>(impl->definition.skip)},
                   {"reads", impl->definition.reads},
                   {"fields", std::move(fields)},
                   {"registers", std::move(registers)},
                   {"limits", std::move(budgets)}};

  // Keep immutable compiler metadata out of every candidate's register row.
  // Empty identity leaves the legacy fingerprint byte-for-byte unchanged.
  if (!impl->definition.definition_identity.empty())
    fingerprint["definition_identity"] = impl->definition.definition_identity;

  impl->fingerprint = fingerprint.dump();
  auto program = std::unique_ptr<CepProgram>(new CepProgram(std::move(impl)));
  EF_ASSIGN_OR_RETURN(auto state, program->initial_state());
  (void)state;

  return program;
}

StatusOr<CepState> CepProgram::initial_state(std::int64_t time_us) const noexcept {
  if (time_us < 0)
    return invalid("negative CEP processing time");

  auto data = std::make_shared<CepStateData>();
  data->fingerprint = impl_->fingerprint;
  data->time_us = time_us;
  EF_ASSIGN_OR_RETURN(auto json, impl_->encode(*data));
  (void)json;

  return CepState(std::move(data));
}

StatusOr<CepStep> CepProgram::step(const CepState& state, const CepEvent& event,
                                   const FsmReads& reads) const noexcept {
  if (!state.data_)
    return invalid("uninitialized CEP state");

  EF_RETURN_IF_ERROR(impl_->state_matches(*state.data_));
  EF_RETURN_IF_ERROR(impl_->validate_reads(reads));
  EF_ASSIGN_OR_RETURN(auto encoded_event,
                      event_json(event, impl_->definition.limits.max_event_bytes));
  (void)encoded_event;
  if (state.data_->last_event_id == event.id)
    return Status(ErrorCode::FailedPrecondition, "owner must deduplicate CEP input identities");
  if (state.data_->event_sequence == UINT64_MAX)
    return full("CEP event sequence exhausted");

  auto next = std::make_shared<CepStateData>(*state.data_);
  const auto& limits = impl_->definition.limits;
  Work work{.remaining = limits.max_work_per_step};
  CepStep result;
  EF_RETURN_IF_ERROR(impl_->expire(*next, event.time_us, result, work));
  EF_ASSIGN_OR_RETURN(auto owned_before, impl_->encode(*next));

  for (const auto& retained : owned_before["events"])
    if (retained["id"] == event.id)
      return Status(ErrorCode::Conflict, "CEP identity was reused within retained history");

  const auto old_nodes = owned_before["nodes"].size();
  const auto old_events = owned_before["events"].size();
  ++next->event_sequence;
  next->last_event_id = event.id;
  auto current =
      std::make_shared<StoredEvent>(StoredEvent{.event = event, .sequence = next->event_sequence});
  auto captured_reads = std::make_shared<const FsmReads>(reads);
  std::vector<Path> inputs = std::move(next->paths);
  next->paths.clear();
  EF_ASSIGN_OR_RETURN(auto starting, impl_->closure(impl_->start, &work));

  for (auto node : starting)
    if (impl_->nodes[node].atom())
      inputs.push_back(
          {.node = node, .tail = {}, .registers = impl_->definition.initial_registers});

  std::map<std::tuple<std::uint64_t, std::uint32_t, std::string>, History> interned;
  std::set<std::string> path_ids;
  std::set<std::string> match_ids;
  for (const auto& match : next->pending)
    match_ids.insert(match.id);

  const auto keep_path = [&](Path path) noexcept -> Status {
    EF_ASSIGN_OR_RETURN(auto id, impl_->path_id(path));
    if (!path_ids.insert(id).second)
      return {};
    if (next->paths.size() == limits.max_paths)
      return full("CEP active path budget exceeded");

    next->paths.push_back(std::move(path));
    return {};
  };
  for (const auto& path : inputs) {
    EF_RETURN_IF_ERROR(work.use());
    const auto& node = impl_->nodes[path.node];
    EF_ASSIGN_OR_RETURN(auto history, materialize(path.tail, &work));
    for (const auto& capture : history)
      if (capture.event.id == event.id)
        return Status(ErrorCode::Conflict, "CEP identity was reused within retained history");

    const CepContext context{
        .event = event, .registers = path.registers, .history = history, .reads = reads};
    const bool kind_matches = node.kind == "*" || node.kind == event.kind;
    bool take = kind_matches;
    if (take && node.guard) {
      EF_RETURN_IF_ERROR(work.use());
      EF_ASSIGN_OR_RETURN(take, node.guard(context));
    }

    for (const auto& edge : node.edges) {
      EF_RETURN_IF_ERROR(work.use());
      if (edge.action == CepAction::Ignore) {
        const bool ignore = node.contiguity == CepContiguity::FollowedByAny ||
                            (node.contiguity == CepContiguity::NextOfKind ? !kind_matches : !take);
        if (path.tail && ignore)
          EF_RETURN_IF_ERROR(keep_path(path));

        continue;
      }

      if (edge.action != CepAction::Take || !take)
        continue;
      if (!path.tail && event.time_us > INT64_MAX - impl_->definition.within_us)
        return full("CEP relative deadline exhausted");

      Row registers = path.registers;
      if (node.update) {
        EF_RETURN_IF_ERROR(work.use());
        EF_ASSIGN_OR_RETURN(registers, node.update(context));
      }

      EF_RETURN_IF_ERROR(validate_registers(impl_->definition.register_fields, registers,
                                            limits.max_register_bytes));
      const auto key = std::make_tuple(path.tail ? path.tail->id : 0, path.node, event.id);
      History tail;
      if (const auto found = interned.find(key); found != interned.end())
        tail = found->second;
      else {
        if (old_nodes + interned.size() >= limits.max_history_nodes ||
            old_events >= limits.max_events)
          return full("CEP working shared buffer budget exceeded");
        if (next->next_history_id == UINT64_MAX)
          return full("CEP shared history identity exhausted");

        auto added = std::make_shared<HistoryNode>();
        added->id = next->next_history_id++;
        added->atom = path.node;
        added->label = node.label;
        added->previous = path.tail;
        added->event = current;
        added->start_sequence = path.tail ? path.tail->start_sequence : current->sequence;
        added->start_time_us = path.tail ? path.tail->start_time_us : event.time_us;
        added->length = path.tail ? path.tail->length + 1 : 1;
        if (added->length > limits.max_events_per_match)
          return full("CEP match history length exceeded");

        tail = std::move(added);
        interned.emplace(key, tail);
      }

      // Only epsilon edges are traversed after TAKE. A successor consumes the next input.
      EF_ASSIGN_OR_RETURN(auto following, impl_->closure(edge.target, &work));
      for (auto successor : following) {
        if (impl_->nodes[successor].accepting) {
          EF_ASSIGN_OR_RETURN(auto id, impl_->match_id(tail));
          if (match_ids.insert(id).second) {
            if (next->pending.size() == limits.max_pending_matches)
              return full("CEP completed-match staging budget exceeded");

            next->pending.push_back({.id = std::move(id),
                                     .tail = tail,
                                     .registers = registers,
                                     .reads = captured_reads,
                                     .matched_at_us = event.time_us});
          }
        } else
          EF_RETURN_IF_ERROR(keep_path({.node = successor, .tail = tail, .registers = registers}));
      }
    }
  }

  EF_RETURN_IF_ERROR(impl_->finish(*next, result));
  EF_ASSIGN_OR_RETURN(auto json, impl_->encode(*next));
  (void)json;
  EF_RETURN_IF_ERROR(impl_->outputs_fit(result));
  result.state = CepState(std::move(next));

  return result;
}

StatusOr<CepStep> CepProgram::advance(const CepState& state, std::int64_t time_us) const noexcept {
  if (!state.data_)
    return invalid("uninitialized CEP state");

  EF_RETURN_IF_ERROR(impl_->state_matches(*state.data_));
  auto next = std::make_shared<CepStateData>(*state.data_);
  CepStep result;
  Work work{.remaining = impl_->definition.limits.max_work_per_step};
  EF_RETURN_IF_ERROR(impl_->expire(*next, time_us, result, work));
  EF_ASSIGN_OR_RETURN(auto json, impl_->encode(*next));
  (void)json;
  result.state = CepState(std::move(next));

  return result;
}

StatusOr<std::string> CepProgram::encode_state(const CepState& state) const noexcept {
  if (!state.data_)
    return invalid("uninitialized CEP state");

  EF_ASSIGN_OR_RETURN(auto json, impl_->encode(*state.data_));
  return json.dump();
}

std::string_view CepProgram::fingerprint() const noexcept {
  return impl_->fingerprint;
}

StatusOr<std::shared_ptr<CepStateData>> CepProgram::Impl::decode(const Json& json) const noexcept {
  const auto& limits = definition.limits;
  if (!has_fields(json, {"format", "fingerprint", "time", "event_sequence", "next_history_id",
                         "last_event_id", "events", "nodes", "paths", "pending"}) ||
      json["format"] != 1 || json["fingerprint"] != fingerprint ||
      !json["last_event_id"].is_string() || !json["events"].is_array() ||
      !json["nodes"].is_array() || !json["paths"].is_array() || !json["pending"].is_array())
    return Status(ErrorCode::FailedPrecondition, "CEP snapshot and definition mismatch");
  if (json["events"].size() > limits.max_events ||
      json["nodes"].size() > limits.max_history_nodes || json["paths"].size() > limits.max_paths ||
      json["pending"].size() > limits.max_pending_matches)
    return full("CEP snapshot entry budget exceeded");

  auto result = std::make_shared<CepStateData>();
  result->fingerprint = fingerprint;
  EF_ASSIGN_OR_RETURN(result->time_us, time_number(json["time"]));
  EF_ASSIGN_OR_RETURN(result->event_sequence, unsigned_number(json["event_sequence"]));
  EF_ASSIGN_OR_RETURN(result->next_history_id, unsigned_number(json["next_history_id"]));
  result->last_event_id = json["last_event_id"].get<std::string>();
  if (result->next_history_id == 0 ||
      (result->event_sequence == 0 ? !result->last_event_id.empty()
                                   : !name_ok(result->last_event_id, 4096)))
    return invalid("invalid CEP snapshot progress");

  std::map<std::string, std::shared_ptr<const StoredEvent>> events;
  std::set<std::uint64_t> sequences;
  std::map<std::uint64_t, std::int64_t> event_times;
  for (const auto& entry : json["events"]) {
    if (!has_fields(entry, {"id", "kind", "time", "values", "sequence"}) ||
        !entry["id"].is_string() || !entry["kind"].is_string())
      return invalid("invalid CEP stored event");

    auto event = std::make_shared<StoredEvent>();
    event->event.id = entry["id"].get<std::string>();
    event->event.kind = entry["kind"].get<std::string>();
    EF_ASSIGN_OR_RETURN(event->event.time_us, time_number(entry["time"]));
    EF_ASSIGN_OR_RETURN(event->event.values, parse_row(entry["values"], limits.max_event_bytes));
    EF_ASSIGN_OR_RETURN(event->sequence, unsigned_number(entry["sequence"]));
    EF_ASSIGN_OR_RETURN(auto encoded, event_json(event->event, limits.max_event_bytes));
    (void)encoded;
    if (event->sequence == 0 || event->sequence > result->event_sequence ||
        event->event.time_us > result->time_us || !sequences.insert(event->sequence).second ||
        (event->sequence == result->event_sequence && event->event.id != result->last_event_id) ||
        (event->event.id == result->last_event_id && event->sequence != result->event_sequence) ||
        !events.emplace(event->event.id, event).second)
      return invalid("duplicate or inconsistent CEP stored event");

    event_times.emplace(event->sequence, event->event.time_us);
  }

  std::int64_t previous_time = 0;
  for (const auto& [sequence, time] : event_times) {
    (void)sequence;
    if (time < previous_time)
      return invalid("CEP stored events reverse processing time across paths");

    previous_time = time;
  }

  std::map<std::uint64_t, History> history;
  std::set<std::tuple<std::uint64_t, std::uint32_t, std::string>> identities;
  std::uint64_t previous_id = 0;
  for (const auto& entry : json["nodes"]) {
    if (!entry.is_array() || entry.size() != 5 || !entry[3].is_string() || !entry[4].is_string())
      return invalid("invalid CEP history node");

    EF_ASSIGN_OR_RETURN(const auto id, unsigned_number(entry[0]));
    EF_ASSIGN_OR_RETURN(const auto atom, unsigned_number(entry[1]));
    EF_ASSIGN_OR_RETURN(const auto predecessor, unsigned_number(entry[2]));
    if (id <= previous_id || id >= result->next_history_id || atom >= nodes.size() ||
        !nodes[atom].atom() || predecessor >= id ||
        (predecessor != 0 && !history.contains(predecessor)))
      return invalid("CEP history node order, graph or predecessor is invalid");

    previous_id = id;
    const auto event = events.find(entry[3].get<std::string>());
    if (event == events.end() || entry[4] != nodes[atom].label ||
        (nodes[atom].kind != "*" && nodes[atom].kind != event->second->event.kind))
      return invalid("CEP history label or event reference mismatch");

    auto node = std::make_shared<HistoryNode>();
    node->id = id;
    node->atom = static_cast<std::uint32_t>(atom);
    node->label = nodes[atom].label;
    node->event = event->second;
    if (predecessor)
      node->previous = history.find(predecessor)->second;

    node->start_sequence = node->previous ? node->previous->start_sequence : node->event->sequence;
    node->start_time_us =
        node->previous ? node->previous->start_time_us : node->event->event.time_us;
    node->length = node->previous ? node->previous->length + 1 : 1;
    if (node->length > limits.max_events_per_match ||
        node->start_time_us > INT64_MAX - definition.within_us ||
        node->event->event.time_us >= node->start_time_us + definition.within_us ||
        (node->previous && (node->previous->event->sequence >= node->event->sequence ||
                            node->previous->event->event.time_us > node->event->event.time_us ||
                            (nodes[atom].contiguity == CepContiguity::Strict &&
                             node->event->sequence - node->previous->event->sequence != 1))))
      return invalid("CEP history sequence, deadline or length is invalid");

    const auto previous_state =
        node->previous ? nodes[node->previous->atom].edges[0].target : start;
    EF_ASSIGN_OR_RETURN(auto reachable, closure(previous_state));
    if (std::find(reachable.begin(), reachable.end(), node->atom) == reachable.end() ||
        !identities.emplace(predecessor, node->atom, node->event->event.id).second)
      return invalid("CEP history is not a legal graph path");

    history.emplace(id, std::move(node));
  }

  std::set<std::string> path_ids;
  for (const auto& entry : json["paths"]) {
    if (!entry.is_array() || entry.size() != 3)
      return invalid("invalid CEP active path");

    EF_ASSIGN_OR_RETURN(const auto node, unsigned_number(entry[0]));
    EF_ASSIGN_OR_RETURN(const auto tail, unsigned_number(entry[1]));
    if (node >= nodes.size() || !nodes[node].atom() || !history.contains(tail))
      return invalid("CEP active path has a missing reference");

    Path path{.node = static_cast<std::uint32_t>(node),
              .tail = history.find(tail)->second,
              .registers = {}};
    EF_ASSIGN_OR_RETURN(path.registers, parse_row(entry[2], limits.max_register_bytes));
    EF_RETURN_IF_ERROR(
        validate_registers(definition.register_fields, path.registers, limits.max_register_bytes));
    EF_ASSIGN_OR_RETURN(auto reachable, closure(nodes[path.tail->atom].edges[0].target));
    EF_ASSIGN_OR_RETURN(auto identity, path_id(path));
    if (path.tail->start_time_us + definition.within_us <= result->time_us ||
        (nodes[path.node].contiguity == CepContiguity::Strict &&
         path.tail->event->sequence != result->event_sequence) ||
        std::find(reachable.begin(), reachable.end(), path.node) == reachable.end() ||
        !path_ids.insert(identity).second)
      return invalid("expired, duplicate or unreachable CEP active path");

    result->paths.push_back(std::move(path));
  }

  std::set<std::string> match_ids;
  for (const auto& entry : json["pending"]) {
    if (!entry.is_array() || entry.size() != 5 || !entry[0].is_string())
      return invalid("invalid CEP completed match");

    EF_ASSIGN_OR_RETURN(const auto tail, unsigned_number(entry[1]));
    if (!history.contains(tail))
      return invalid("CEP completed match has a missing reference");

    PendingMatch match;
    match.id = entry[0].get<std::string>();
    match.tail = history.find(tail)->second;
    EF_ASSIGN_OR_RETURN(match.registers, parse_row(entry[2], limits.max_register_bytes));
    EF_RETURN_IF_ERROR(
        validate_registers(definition.register_fields, match.registers, limits.max_register_bytes));
    EF_ASSIGN_OR_RETURN(auto reads, parse_reads(entry[3], limits.max_read_bytes));
    EF_RETURN_IF_ERROR(validate_reads(reads));
    match.reads = std::make_shared<const FsmReads>(std::move(reads));
    EF_ASSIGN_OR_RETURN(match.matched_at_us, time_number(entry[4]));
    EF_ASSIGN_OR_RETURN(auto identity, match_id(match.tail));
    EF_ASSIGN_OR_RETURN(auto reachable, closure(nodes[match.tail->atom].edges[0].target));
    const bool final =
        std::any_of(reachable.begin(), reachable.end(),
                    [&](std::uint32_t node) noexcept { return nodes[node].accepting; });
    if (!final || match.id != identity || !match_ids.insert(match.id).second ||
        match.matched_at_us != match.tail->event->event.time_us ||
        match.matched_at_us > result->time_us || definition.skip == CepSkipStrategy::NoSkip ||
        std::none_of(result->paths.begin(), result->paths.end(), [&](const Path& path) noexcept {
          return path.tail->start_sequence < match.tail->start_sequence;
        }))
      return invalid("inconsistent CEP completed match");

    result->pending.push_back(std::move(match));
  }

  // Re-encoding visits only owned roots. Unreachable nodes/events are never silently retained.
  EF_ASSIGN_OR_RETURN(auto canonical, encode(*result));
  if (canonical["nodes"].size() != history.size() || canonical["events"].size() != events.size())
    return invalid("CEP snapshot contains orphaned history or events");

  return result;
}

StatusOr<CepState> CepProgram::decode_state(std::string_view bytes) const noexcept {
  if (bytes.size() > impl_->definition.limits.max_state_bytes)
    return full("CEP snapshot exceeds byte budget");

  EF_ASSIGN_OR_RETURN(auto json, parse_json_exact(bytes));
  EF_ASSIGN_OR_RETURN(auto state, impl_->decode(json));
  return CepState(std::move(state));
}

StatusOr<CepStateSummary> CepProgram::inspect(const CepState& state) const noexcept {
  if (!state.data_)
    return invalid("uninitialized CEP state");

  EF_ASSIGN_OR_RETURN(auto json, impl_->encode(*state.data_));
  CepStateSummary summary;
  summary.time_us = state.data_->time_us;
  summary.event_sequence = state.data_->event_sequence;
  summary.history_nodes = json["nodes"].size();
  summary.event_records = json["events"].size();
  summary.pending_matches = state.data_->pending.size();

  for (const auto& path : state.data_->paths) {
    EF_ASSIGN_OR_RETURN(auto id, impl_->path_id(path));
    EF_ASSIGN_OR_RETURN(auto history, materialize(path.tail));
    const auto deadline = path.tail->start_time_us + impl_->definition.within_us;
    summary.paths.push_back({.id = std::move(id),
                             .node = path.node,
                             .registers = path.registers,
                             .history = std::move(history),
                             .deadline_us = deadline});
    if (!summary.next_deadline_us || deadline < *summary.next_deadline_us)
      summary.next_deadline_us = deadline;
  }

  return summary;
}
} // namespace eventflow
