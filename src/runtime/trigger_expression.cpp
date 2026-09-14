#include "eventflow/trigger_expression.hpp"
#include "eventflow/typed_schema.hpp"
#include <algorithm>
#include <cmath>

namespace eventflow {

struct TriggerExpressionNode {
  std::string op;
  DataType type = DataType::Bool;
  Value value;
  EventSource event;
  RelationId relation = 0;
  std::string alias;
  std::string column;
  TriggerExpressionPtr left;
  TriggerExpressionPtr right;
  std::vector<TriggerReadBinding> reads;
  FsmGuard guard;
  std::vector<Field> expected_fields;
  RowBoolCallback event_guard;
  std::string implementation_identity;
};

namespace trigger_detail {
TriggerExpressionPtr column(EventSource event, RelationId relation, std::string alias,
                            std::string name, DataType type, bool event_column) noexcept {
  auto node = std::make_shared<TriggerExpressionNode>();
  node->op = event_column ? "event_column" : "view_column";
  node->event = std::move(event);
  node->relation = relation;
  node->alias = std::move(alias);
  node->column = std::move(name);
  node->type = type;

  return node;
}

TriggerExpressionPtr literal(Value value, DataType type) noexcept {
  auto node = std::make_shared<TriggerExpressionNode>();
  node->op = "literal";
  node->type = type;
  node->value = std::move(value);

  return node;
}

TriggerExpressionPtr operation(std::string operation, TriggerExpressionPtr left,
                               TriggerExpressionPtr right) noexcept {
  auto node = std::make_shared<TriggerExpressionNode>();
  node->op = std::move(operation);
  node->left = std::move(left);
  node->right = std::move(right);

  return node;
}
} // namespace trigger_detail

Predicate::Predicate(TriggerExpressionPtr node) noexcept : node_(std::move(node)) {}

Predicate Predicate::constant(bool value) noexcept {
  return Predicate(trigger_detail::literal(value, DataType::Bool));
}

Predicate Predicate::guard(std::vector<TriggerReadBinding> reads, FsmGuard guard,
                           std::string implementation_identity) noexcept {
  auto node = std::make_shared<TriggerExpressionNode>();
  node->op = "opaque_guard";
  node->reads = std::move(reads);
  node->guard = std::move(guard);
  node->implementation_identity = std::move(implementation_identity);

  return Predicate(std::move(node));
}

Predicate Predicate::event_guard(EventRef event, std::vector<Field> expected_fields,
                                 RowBoolCallback guard,
                                 std::string implementation_identity) noexcept {
  auto node = std::make_shared<TriggerExpressionNode>();
  node->op = "typed_event_guard";
  node->event = event.definition();
  node->expected_fields = std::move(expected_fields);
  node->event_guard = std::move(guard);
  node->implementation_identity = std::move(implementation_identity);

  return Predicate(std::move(node));
}

Predicate Predicate::operator!() const noexcept {
  return Predicate(trigger_detail::operation("not", node_));
}

Predicate Predicate::operator&&(Predicate right) const noexcept {
  return Predicate(trigger_detail::operation("and", node_, std::move(right.node_)));
}

Predicate Predicate::operator||(Predicate right) const noexcept {
  return Predicate(trigger_detail::operation("or", node_, std::move(right.node_)));
}

EventRef::EventRef(EventSource definition) noexcept : definition_(std::move(definition)) {}

EventRef EventRef::source(EventSource definition) noexcept {
  return EventRef(std::move(definition));
}

const EventSource& EventRef::definition() const noexcept {
  return definition_;
}

ViewRef::ViewRef(RelationId relation, std::string alias) noexcept
    : relation_(relation), alias_(std::move(alias)) {}

namespace {
struct EvaluationContext {
  std::string_view kind;
  const Row& values;
  const FsmReads& reads;
  const FsmContext* fsm = nullptr;
};

using Evaluator = std::function<StatusOr<Value>(const EvaluationContext&)>;

struct BoundExpression {
  Evaluator evaluate;
  Json signature;
  bool opaque = false;
};

struct RowLayout {
  Schema storage;
  std::size_t visible_count = 0;
};

struct BoundEvent {
  EventSource source;
  std::optional<RowLayout> layout;
};

struct BoundRead {
  RelationId relation = 0;
  RowLayout layout;
};

Status validate_value(const Value& value, const Field& field) noexcept {
  if (std::holds_alternative<std::monostate>(value))
    return check(field.nullable, "Trigger field is unexpectedly NULL: " + field.name);

  bool matches = false;
  switch (field.type) {
  case DataType::Bool:
    matches = std::holds_alternative<bool>(value);
    break;
  case DataType::Int64:
  case DataType::TimestampMicros:
    matches = std::holds_alternative<std::int64_t>(value);
    break;
  case DataType::Float64:
    matches = std::holds_alternative<double>(value);
    break;
  case DataType::String:
    matches = std::holds_alternative<std::string>(value);
    break;
  case DataType::FloatVector:
    matches = std::holds_alternative<std::vector<double>>(value);
    break;
  }

  EF_RETURN_IF_ERROR(check(matches, "Trigger value type does not match field: " + field.name));
  if (const auto* real = std::get_if<double>(&value))
    EF_RETURN_IF_ERROR(
        check(std::isfinite(*real), "Trigger Float64 must be finite: " + field.name));
  if (const auto* vector = std::get_if<std::vector<double>>(&value)) {
    for (const auto element : *vector)
      EF_RETURN_IF_ERROR(
          check(std::isfinite(element), "Trigger FloatVector must be finite: " + field.name));
  }

  if (const auto* text = std::get_if<std::string>(&value))
    EF_RETURN_IF_ERROR(
        check(valid_utf8(*text), "Trigger String must be valid UTF-8: " + field.name));

  return {};
}

Status validate_row(const RowLayout& layout, const Row& row, bool event_payload) noexcept {
  // EventProcessor reads expose visible fields. A direct advanced caller may
  // supply the full stored row, whose hidden identity fields must also validate.
  const bool visible = row.size() == layout.visible_count;
  const bool stored = !event_payload && row.size() == layout.storage.fields.size();
  EF_RETURN_IF_ERROR(check(visible || stored,
                           "Trigger row size does not match bound schema: " + layout.storage.name));
  for (std::size_t i = 0; i < row.size(); ++i)
    EF_RETURN_IF_ERROR(validate_value(row[i], layout.storage.fields[i]));

  return {};
}

StatusOr<bool> boolean(const Value& value) noexcept {
  const auto* result = std::get_if<bool>(&value);
  if (!result)
    return Status(ErrorCode::InvalidArgument, "Trigger predicate did not produce Bool");

  return bool(*result);
}

template <typename T>
bool scalar_compare(const T& left, const T& right, std::string_view op) noexcept {
  if (op == "eq")
    return left == right;
  if (op == "ne")
    return left != right;
  if (op == "lt")
    return left < right;
  if (op == "le")
    return left <= right;
  if (op == "gt")
    return left > right;

  return left >= right;
}

StatusOr<Value> compare_values(const Value& left, const Value& right,
                               std::string_view op) noexcept {
  if (std::holds_alternative<std::monostate>(left) || std::holds_alternative<std::monostate>(right))
    return Value(false);

  EF_RETURN_IF_ERROR(check(left.index() == right.index(), "Trigger comparison types differ"));
  if (const auto* value = std::get_if<bool>(&left))
    return Value(scalar_compare(*value, *std::get_if<bool>(&right), op));
  if (const auto* value = std::get_if<std::int64_t>(&left))
    return Value(scalar_compare(*value, *std::get_if<std::int64_t>(&right), op));
  if (const auto* value = std::get_if<double>(&left))
    return Value(scalar_compare(*value, *std::get_if<double>(&right), op));
  if (const auto* value = std::get_if<std::string>(&left))
    return Value(scalar_compare(*value, *std::get_if<std::string>(&right), op));

  return Status(ErrorCode::InvalidArgument, "Trigger comparison type is unsupported");
}

class Binder {
public:
  explicit Binder(const CatalogSnapshot& catalog, std::optional<EventSource> current) noexcept
      : catalog_(catalog), current_(std::move(current)) {}

  Status add_event(const EventSource& event) noexcept {
    EF_RETURN_IF_ERROR(check(!event.kind.empty() && valid_utf8(event.kind),
                             "Trigger event kind must be nonempty UTF-8"));
    const auto existing = events.find(event.kind);
    if (existing != events.end())
      return check(existing->second.source.payload_relation == event.payload_relation,
                   "Trigger event kind has conflicting payload schemas: " + event.kind);

    BoundEvent bound{.source = event, .layout = {}};
    if (event.payload_relation != 0) {
      EF_ASSIGN_OR_RETURN(auto relation, catalog_.find(event.payload_relation));
      EF_RETURN_IF_ERROR(validate_relation(*relation));
      EF_RETURN_IF_ERROR(bind_scope(*relation));
      bound.layout =
          RowLayout{.storage = relation->storage_schema, .visible_count = relation->visible_count};
    }

    events.emplace(event.kind, std::move(bound));
    return {};
  }

  StatusOr<std::string> add_read(RelationId id, std::string alias) noexcept {
    EF_ASSIGN_OR_RETURN(auto relation, catalog_.find(id));
    EF_RETURN_IF_ERROR(validate_relation(*relation));
    EF_RETURN_IF_ERROR(check(relation->unique_per_partition,
                             "Trigger read must be unique per business scope: " + relation->name));
    EF_RETURN_IF_ERROR(bind_scope(*relation));
    if (alias.empty())
      alias = relation->name;

    EF_RETURN_IF_ERROR(
        check(!alias.empty() && valid_utf8(alias), "Trigger read alias must be nonempty UTF-8"));
    const auto existing = reads.find(alias);
    if (existing != reads.end()) {
      EF_RETURN_IF_ERROR(check(existing->second.relation == id,
                               "Trigger read alias names different relations: " + alias));
      return std::move(alias);
    }

    reads.emplace(alias, BoundRead{.relation = id,
                                   .layout = {.storage = relation->storage_schema,
                                              .visible_count = relation->visible_count}});
    return std::move(alias);
  }

  StatusOr<BoundExpression> bind(const TriggerExpressionPtr& node, std::size_t depth = 0) noexcept {
    EF_RETURN_IF_ERROR(check(depth <= 128 && ++nodes_ <= 4096,
                             "Trigger expression exceeds bounded depth or node count"));
    if (!node)
      return BoundExpression{
          .evaluate = [](const EvaluationContext&) noexcept -> StatusOr<Value> {
            return Value(true);
          },
          .signature =
              Json{{"op", "literal"}, {"type", data_type_name(DataType::Bool)}, {"value", true}}};
    if (node->op == "literal") {
      EF_RETURN_IF_ERROR(
          validate_value(node->value, {.id = 0, .name = "literal", .type = node->type}));
      EF_ASSIGN_OR_RETURN(auto value_json, value_to_json(node->value));
      return BoundExpression{
          .evaluate = [value = node->value](const EvaluationContext&) noexcept -> StatusOr<Value> {
            return Value(value);
          },
          .signature = Json{{"op", "literal"},
                            {"type", data_type_name(node->type)},
                            {"value", std::move(value_json)}}};
    }

    if (node->op == "event_column" || node->op == "view_column")
      return bind_column(*node);
    if (node->op == "opaque_guard")
      return bind_opaque(*node);
    if (node->op == "typed_event_guard")
      return bind_event_guard(*node);

    EF_ASSIGN_OR_RETURN(auto left, bind(node->left, depth + 1));
    if (node->op == "not" || node->op == "is_true" || node->op == "is_null") {
      const auto op = node->op;
      auto signature = Json{{"op", op}, {"argument", std::move(left.signature)}};
      return BoundExpression{
          .evaluate = [op, evaluate = std::move(left.evaluate)](
                          const EvaluationContext& context) noexcept -> StatusOr<Value> {
            EF_ASSIGN_OR_RETURN(auto value, evaluate(context));
            if (op == "is_null")
              return Value(std::holds_alternative<std::monostate>(value));
            if (op == "is_true" && std::holds_alternative<std::monostate>(value))
              return Value(false);

            EF_ASSIGN_OR_RETURN(auto result, boolean(value));
            return Value(op == "not" ? !result : result);
          },
          .signature = std::move(signature),
          .opaque = left.opaque};
    }

    const auto op = node->op;
    const bool supported = op == "and" || op == "or" || op == "value_or" || op == "eq" ||
                           op == "ne" || op == "lt" || op == "le" || op == "gt" || op == "ge";
    EF_RETURN_IF_ERROR(check(supported, "Unsupported Trigger expression operation"));
    EF_ASSIGN_OR_RETURN(auto right, bind(node->right, depth + 1));
    auto signature = Json{
        {"op", op}, {"left", std::move(left.signature)}, {"right", std::move(right.signature)}};
    return BoundExpression{
        .evaluate = [op, left_eval = std::move(left.evaluate),
                     right_eval = std::move(right.evaluate)](
                        const EvaluationContext& context) noexcept -> StatusOr<Value> {
          EF_ASSIGN_OR_RETURN(auto lhs, left_eval(context));
          if (op == "and" || op == "or") {
            EF_ASSIGN_OR_RETURN(auto truth, boolean(lhs));
            if ((op == "and" && !truth) || (op == "or" && truth))
              return Value(truth);

            EF_ASSIGN_OR_RETURN(auto rhs, right_eval(context));
            EF_ASSIGN_OR_RETURN(auto result, boolean(rhs));
            return Value(result);
          }

          if (op == "value_or" && !std::holds_alternative<std::monostate>(lhs))
            return std::move(lhs);

          EF_ASSIGN_OR_RETURN(auto rhs, right_eval(context));
          if (op == "value_or")
            return std::move(rhs);

          return compare_values(lhs, rhs, op);
        },
        .signature = std::move(signature),
        .opaque = left.opaque || right.opaque};
  }

  std::map<std::string, BoundEvent> events;
  std::map<std::string, BoundRead> reads;

private:
  StatusOr<BoundExpression> bind_event_guard(const TriggerExpressionNode& node) noexcept {
    EF_RETURN_IF_ERROR(check(bool(node.event_guard), "Typed event guard must be callable"));
    EF_RETURN_IF_ERROR(
        check(!node.implementation_identity.empty() && valid_utf8(node.implementation_identity),
              "Typed event guard requires a stable implementation identity"));
    EF_RETURN_IF_ERROR(
        check(node.event.payload_relation != 0, "Typed event guard requires a payload relation"));
    if (current_)
      EF_RETURN_IF_ERROR(check(current_->kind == node.event.kind &&
                                   current_->payload_relation == node.event.payload_relation,
                               "Typed event guard must refer to the current event source"));

    EF_RETURN_IF_ERROR(add_event(node.event));
    EF_ASSIGN_OR_RETURN(auto relation, catalog_.find(node.event.payload_relation));
    const std::vector<Field> actual(relation->storage_schema.fields.begin(),
                                    relation->storage_schema.fields.begin() +
                                        relation->visible_count);
    EF_ASSIGN_OR_RETURN(auto binding, bind_row_fields(node.expected_fields, actual));
    Json expected = Json::array();

    for (const auto& field : node.expected_fields)
      expected.push_back(Json{{"id", field.id},
                              {"name", field.name},
                              {"type", data_type_name(field.type)},
                              {"nullable", field.nullable}});

    return BoundExpression{
        .evaluate = [kind = node.event.kind, guard = node.event_guard,
                     binding = std::move(binding)](
                        const EvaluationContext& context) noexcept -> StatusOr<Value> {
          if (context.kind != kind)
            return Value(false);

          EF_ASSIGN_OR_RETURN(auto projected, binding.project(context.values));
          EF_ASSIGN_OR_RETURN(auto accepted, guard(projected));
          return Value(accepted);
        },
        .signature = Json{{"op", "typed_event_guard"},
                          {"kind", node.event.kind},
                          {"expected_fields", std::move(expected)},
                          {"implementation", node.implementation_identity}}};
  }

  Status validate_relation(const RelationDescriptor& relation) const noexcept {
    EF_RETURN_IF_ERROR(relation.storage_schema.validate_definition());
    return check(relation.visible_count > 0 &&
                     relation.visible_count <= relation.storage_schema.fields.size(),
                 "Trigger relation has an invalid visible layout: " + relation.name);
  }

  Status bind_scope(const RelationDescriptor& relation) noexcept {
    if (!scope_)
      scope_ = relation.partition_types;

    return check(*scope_ == relation.partition_types,
                 "Trigger relation has an incompatible business scope: " + relation.name);
  }

  StatusOr<BoundExpression> bind_column(const TriggerExpressionNode& node) noexcept {
    const bool event = node.op == "event_column";
    RelationId id = node.relation;
    std::string alias;
    if (event) {
      EF_RETURN_IF_ERROR(check(node.event.payload_relation != 0,
                               "Trigger event column requires a payload relation"));
      if (current_)
        EF_RETURN_IF_ERROR(check(current_->kind == node.event.kind &&
                                     current_->payload_relation == node.event.payload_relation,
                                 "Trigger event column must refer to the current event source"));

      EF_RETURN_IF_ERROR(add_event(node.event));
      id = node.event.payload_relation;
    } else {
      EF_ASSIGN_OR_RETURN(alias, add_read(id, node.alias));
    }

    EF_ASSIGN_OR_RETURN(auto relation, catalog_.find(id));
    EF_ASSIGN_OR_RETURN(auto index, relation->storage_schema.index_of(node.column));
    EF_RETURN_IF_ERROR(
        check(index < relation->visible_count, "Trigger column is hidden: " + node.column));
    const auto actual_type = relation->storage_schema.fields[index].type;
    EF_RETURN_IF_ERROR(check(
        actual_type == node.type ||
            (actual_type == DataType::TimestampMicros && node.type == DataType::Int64),
        "Trigger column " + node.column + " requires " + std::string(data_type_name(node.type))));
    const RowLayout layout{.storage = relation->storage_schema,
                           .visible_count = relation->visible_count};
    const auto kind = node.event.kind;

    return BoundExpression{
        .evaluate = [event, kind, alias, layout,
                     index](const EvaluationContext& context) noexcept -> StatusOr<Value> {
          if (event) {
            if (context.kind != kind)
              return Value(std::monostate{});

            EF_RETURN_IF_ERROR(validate_row(layout, context.values, true));
            return Value(context.values[index]);
          }

          const auto found = context.reads.find(alias);
          if (found == context.reads.end())
            return Status(ErrorCode::InvalidArgument, "Trigger declared read is missing: " + alias);
          if (!found->second)
            return Value(std::monostate{});

          EF_RETURN_IF_ERROR(validate_row(layout, *found->second, false));
          return Value((*found->second)[index]);
        },
        .signature = Json{{"op", node.op},
                          {"relation", id},
                          {"event", kind},
                          {"alias", alias},
                          {"column", node.column},
                          {"index", index},
                          {"type", data_type_name(node.type)}}};
  }

  StatusOr<BoundExpression> bind_opaque(const TriggerExpressionNode& node) noexcept {
    EF_RETURN_IF_ERROR(check(bool(node.guard), "Trigger opaque guard must be callable"));
    EF_RETURN_IF_ERROR(
        check(!node.implementation_identity.empty() && valid_utf8(node.implementation_identity),
              "Trigger opaque guard requires a stable implementation identity"));
    std::map<std::string, RowLayout> layouts;
    Json declared = Json::array();

    for (const auto& read : node.reads) {
      EF_ASSIGN_OR_RETURN(auto alias, add_read(read.relation, read.alias));
      layouts.emplace(alias, reads.find(alias)->second.layout);
    }

    for (const auto& [alias, layout] : layouts)
      declared.push_back(alias);

    return BoundExpression{
        .evaluate = [guard = node.guard, layouts = std::move(layouts)](
                        const EvaluationContext& context) noexcept -> StatusOr<Value> {
          if (!context.fsm)
            return Status(ErrorCode::FailedPrecondition,
                          "Opaque FSM guard cannot evaluate a CEP context");

          for (const auto& [alias, layout] : layouts) {
            const auto found = context.reads.find(alias);
            if (found == context.reads.end())
              return Status(ErrorCode::InvalidArgument,
                            "Trigger declared read is missing: " + alias);
            if (found->second)
              EF_RETURN_IF_ERROR(validate_row(layout, *found->second, false));
          }

          EF_ASSIGN_OR_RETURN(auto accepted, guard(*context.fsm));
          return Value(accepted);
        },
        .signature = Json{{"op", "opaque_guard"},
                          {"implementation", node.implementation_identity},
                          {"reads", std::move(declared)}},
        .opaque = true};
  }

  const CatalogSnapshot& catalog_;
  std::optional<EventSource> current_;
  std::optional<std::vector<DataType>> scope_;
  std::size_t nodes_ = 0;
};

Status validate_current_payload(const std::map<std::string, BoundEvent>& events,
                                std::string_view kind, const Row& values) noexcept {
  // Validate before the expression: even a constant-false or short-circuited
  // predicate cannot hide a malformed event that claims this source schema.
  const auto found = events.find(std::string(kind));
  if (found != events.end() && found->second.layout)
    return validate_row(*found->second.layout, values, true);

  return {};
}
} // namespace

StatusOr<BoundTriggerPredicate>
bind_trigger_predicate(const CatalogSnapshot& catalog, const Predicate& predicate,
                       std::optional<EventRef> current_event) noexcept {
  std::optional<EventSource> current;
  if (current_event)
    current = current_event->definition();

  Binder binder(catalog, current);
  if (current)
    EF_RETURN_IF_ERROR(binder.add_event(*current));

  EF_ASSIGN_OR_RETURN(auto expression, binder.bind(predicate.node_));
  BoundTriggerPredicate result;
  Json reads = Json::array();
  Json events = Json::array();

  for (const auto& [alias, read] : binder.reads) {
    result.reads.emplace(alias, read.relation);
    EF_ASSIGN_OR_RETURN(auto schema, schema_to_json(read.layout.storage));
    reads.push_back(Json{{"alias", alias},
                         {"relation", read.relation},
                         {"schema", std::move(schema)},
                         {"visible_count", read.layout.visible_count}});
  }

  for (const auto& [kind, event] : binder.events) {
    result.events.push_back(event.source);
    Json declaration{{"kind", kind}, {"relation", event.source.payload_relation}};
    if (event.layout) {
      EF_ASSIGN_OR_RETURN(auto schema, schema_to_json(event.layout->storage));
      declaration["schema"] = std::move(schema);
      declaration["visible_count"] = event.layout->visible_count;
    }

    events.push_back(std::move(declaration));
  }

  result.signature = Json{{"compiler", "eventflow.trigger.expression.v1"},
                          {"expression", std::move(expression.signature)},
                          {"reads", std::move(reads)},
                          {"events", std::move(events)}}
                         .dump();
  result.fsm = [evaluate = expression.evaluate,
                events = binder.events](const FsmContext& context) noexcept -> StatusOr<bool> {
    EF_RETURN_IF_ERROR(validate_current_payload(events, context.event.kind, context.event.values));
    EF_ASSIGN_OR_RETURN(auto value, evaluate({.kind = context.event.kind,
                                              .values = context.event.values,
                                              .reads = context.reads,
                                              .fsm = &context}));
    return boolean(value);
  };
  if (!expression.opaque) {
    result.cep = [evaluate = std::move(expression.evaluate), events = std::move(binder.events)](
                     const CepContext& context) noexcept -> StatusOr<bool> {
      EF_RETURN_IF_ERROR(
          validate_current_payload(events, context.event.kind, context.event.values));
      EF_ASSIGN_OR_RETURN(auto value, evaluate({.kind = context.event.kind,
                                                .values = context.event.values,
                                                .reads = context.reads}));
      return boolean(value);
    };
  }

  return result;
}

} // namespace eventflow
