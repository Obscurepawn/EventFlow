#include "binder_internal.hpp"
#include "eventflow/catalog.hpp"

#include <algorithm>
#include <bit>
#include <cmath>
#include <limits>

namespace eventflow {
namespace {

bool is_null_value(const Value& value) noexcept {
  return std::holds_alternative<std::monostate>(value);
}

bool numeric_type(DataType type) noexcept {
  return type == DataType::Int64 || type == DataType::Float64;
}

Status validate_value(const Value& value, DataType type, bool nullable) noexcept {
  EF_RETURN_IF_ERROR(check(!data_type_name(type).empty(), "Unknown expression DataType"));
  if (is_null_value(value))
    return check(nullable, "Nonnullable value is NULL");

  bool matches = false;
  switch (type) {
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

  EF_RETURN_IF_ERROR(check(matches, "Value does not match bound expression type"));
  if (const auto* number = std::get_if<double>(&value))
    EF_RETURN_IF_ERROR(check(std::isfinite(*number), "Nonfinite Float64 value"));
  if (const auto* vector = std::get_if<std::vector<double>>(&value)) {
    for (const auto number : *vector)
      EF_RETURN_IF_ERROR(check(std::isfinite(number), "Nonfinite FloatVector element"));
  }

  if (const auto* text = std::get_if<std::string>(&value))
    EF_RETURN_IF_ERROR(check(valid_utf8(*text), "Invalid UTF-8 String"));

  return {};
}

Status validate_arguments(const BoundExpr& expression, std::size_t count) noexcept {
  EF_RETURN_IF_ERROR(check(expression.args.size() == count, "Wrong expression operand count"));
  for (const auto& argument : expression.args)
    EF_RETURN_IF_ERROR(check(argument != nullptr, "Missing expression operand"));

  return {};
}

StatusOr<Value> evaluate_unary(const BoundExpr& expression, const Row& input) noexcept {
  EF_RETURN_IF_ERROR(validate_arguments(expression, 1));
  const auto& argument = *expression.args[0];
  if (expression.op == "not") {
    EF_RETURN_IF_ERROR(check(expression.type == DataType::Bool && argument.type == DataType::Bool,
                             "NOT requires Bool operands and result"));
    EF_ASSIGN_OR_RETURN(auto value, evaluate(argument, input));
    if (is_null_value(value))
      return Value(std::monostate{});

    return Value(!*std::get_if<bool>(&value));
  }

  EF_RETURN_IF_ERROR(check(expression.op == "+" || expression.op == "-", "Unknown unary operator"));
  EF_RETURN_IF_ERROR(check(numeric_type(argument.type) && expression.type == argument.type,
                           "Unary arithmetic requires matching numeric types"));
  EF_ASSIGN_OR_RETURN(auto value, evaluate(argument, input));
  if (is_null_value(value) || expression.op == "+")
    return value;
  if (const auto* integer = std::get_if<std::int64_t>(&value)) {
    EF_RETURN_IF_ERROR(check(*integer != std::numeric_limits<std::int64_t>::min(),
                             "Int64 unary negation overflow"));
    return Value(-*integer);
  }

  return Value(-*std::get_if<double>(&value));
}

StatusOr<Value> evaluate_boolean(const BoundExpr& expression, const Row& input) noexcept {
  EF_RETURN_IF_ERROR(check(expression.type == DataType::Bool &&
                               expression.args[0]->type == DataType::Bool &&
                               expression.args[1]->type == DataType::Bool,
                           "AND/OR require Bool operands and result"));
  const bool is_and = expression.op == "and";

  // FALSE determines AND, and TRUE determines OR. Skip the right operand in those cases so an
  // unused expression cannot raise an error. NULL still needs the other operand to decide.
  EF_ASSIGN_OR_RETURN(auto left, evaluate(*expression.args[0], input));
  const auto* left_bool = std::get_if<bool>(&left);
  if (left_bool && *left_bool != is_and)
    return Value(*left_bool);

  EF_ASSIGN_OR_RETURN(auto right, evaluate(*expression.args[1], input));
  const auto* right_bool = std::get_if<bool>(&right);
  if (right_bool && *right_bool != is_and)
    return Value(*right_bool);
  if (!left_bool || !right_bool)
    return Value(std::monostate{});

  return Value(is_and);
}

StatusOr<Value> integer_arithmetic(std::string_view op, std::int64_t left,
                                   std::int64_t right) noexcept {
  std::int64_t result = 0;
  bool overflow = false;
  if (op == "+")
    overflow = __builtin_add_overflow(left, right, &result);
  else if (op == "-")
    overflow = __builtin_sub_overflow(left, right, &result);
  else if (op == "*")
    overflow = __builtin_mul_overflow(left, right, &result);
  else {
    EF_RETURN_IF_ERROR(check(right != 0, "Int64 division by zero"));
    EF_RETURN_IF_ERROR(check(left != std::numeric_limits<std::int64_t>::min() || right != -1,
                             "Int64 division overflow"));
    result = left / right;
  }

  EF_RETURN_IF_ERROR(check(!overflow, "Int64 arithmetic overflow"));
  return Value(result);
}

StatusOr<Value> float_arithmetic(std::string_view op, double left, double right) noexcept {
  double result = 0;
  if (op == "+")
    result = left + right;
  else if (op == "-")
    result = left - right;
  else if (op == "*")
    result = left * right;
  else {
    EF_RETURN_IF_ERROR(check(right != 0, "Float64 division by zero"));
    result = left / right;
  }

  EF_RETURN_IF_ERROR(check(std::isfinite(result), "Nonfinite Float64 arithmetic result"));
  return Value(result);
}

template <typename T> int compare(const Value& left, const Value& right) noexcept {
  const auto& lhs = *std::get_if<T>(&left);
  const auto& rhs = *std::get_if<T>(&right);
  return lhs < rhs ? -1 : lhs > rhs ? 1 : 0;
}

StatusOr<Value> evaluate_binary(const BoundExpr& expression, const Row& input) noexcept {
  EF_RETURN_IF_ERROR(validate_arguments(expression, 2));
  if (expression.op == "and" || expression.op == "or")
    return evaluate_boolean(expression, input);

  const auto type = expression.args[0]->type;
  EF_RETURN_IF_ERROR(check(type == expression.args[1]->type, "Binary operand types must match"));
  const auto& op = expression.op;
  const bool arithmetic = op == "+" || op == "-" || op == "*" || op == "/";
  const bool equality = op == "=" || op == "!=" || op == "<>";
  if (arithmetic) {
    EF_RETURN_IF_ERROR(check(numeric_type(type) && expression.type == type,
                             "Arithmetic requires matching numeric operands and result"));
  } else {
    EF_RETURN_IF_ERROR(check(equality || op == "<" || op == "<=" || op == ">" || op == ">=",
                             "Unknown binary operator"));
    EF_RETURN_IF_ERROR(check(expression.type == DataType::Bool && type != DataType::FloatVector &&
                                 (type != DataType::Bool || equality),
                             "Unsupported comparison types"));
  }

  EF_ASSIGN_OR_RETURN(auto left, evaluate(*expression.args[0], input));
  EF_ASSIGN_OR_RETURN(auto right, evaluate(*expression.args[1], input));
  if (is_null_value(left) || is_null_value(right))
    return Value(std::monostate{});
  if (arithmetic) {
    if (type == DataType::Int64)
      return integer_arithmetic(op, *std::get_if<std::int64_t>(&left),
                                *std::get_if<std::int64_t>(&right));

    return float_arithmetic(op, *std::get_if<double>(&left), *std::get_if<double>(&right));
  }

  int comparison = 0;
  switch (type) {
  case DataType::Bool:
    comparison = compare<bool>(left, right);
    break;
  case DataType::Int64:
  case DataType::TimestampMicros:
    comparison = compare<std::int64_t>(left, right);
    break;
  case DataType::Float64:
    comparison = compare<double>(left, right);
    break;
  case DataType::String:
    comparison = compare<std::string>(left, right);
    break;
  default:
    return Status(ErrorCode::InvalidArgument, "Unsupported comparison type");
  }

  if (op == "=")
    return Value(comparison == 0);
  if (op == "!=" || op == "<>")
    return Value(comparison != 0);
  if (op == "<")
    return Value(comparison < 0);
  if (op == "<=")
    return Value(comparison <= 0);
  if (op == ">")
    return Value(comparison > 0);

  return Value(comparison >= 0);
}

StatusOr<Value> evaluate_cast(const BoundExpr& expression, const Row& input) noexcept {
  EF_RETURN_IF_ERROR(validate_arguments(expression, 1));
  const auto& argument = *expression.args[0];
  EF_RETURN_IF_ERROR(check(expression.type == DataType::Float64 &&
                               (numeric_type(argument.type) || argument.untyped_null),
                           "CAST supports only numeric values to Float64"));
  EF_ASSIGN_OR_RETURN(auto value, evaluate(argument, input));
  if (is_null_value(value) || argument.type == DataType::Float64)
    return value;

  const auto integer = *std::get_if<std::int64_t>(&value);
  const auto magnitude = integer < 0 ? std::uint64_t{0} - static_cast<std::uint64_t>(integer)
                                     : static_cast<std::uint64_t>(integer);
  const auto bits = std::bit_width(magnitude);
  EF_RETURN_IF_ERROR(check(bits <= 53 || (magnitude & ((std::uint64_t{1} << (bits - 53)) - 1)) == 0,
                           "Integer is not exactly representable as Float64"));
  return Value(static_cast<double>(integer));
}

StatusOr<Value> evaluate_impl(const BoundExpr& expression, const Row& input) noexcept {
  switch (expression.kind) {
  case BoundExprKind::Input:
    EF_RETURN_IF_ERROR(validate_arguments(expression, 0));
    EF_RETURN_IF_ERROR(check(expression.slot < input.size(), "Input expression slot out of range"));
    return Value(input[expression.slot]);
  case BoundExprKind::Literal:
    EF_RETURN_IF_ERROR(validate_arguments(expression, 0));
    return Value(expression.literal);
  case BoundExprKind::Unary:
    return evaluate_unary(expression, input);
  case BoundExprKind::Binary:
    return evaluate_binary(expression, input);
  case BoundExprKind::IsNull: {
    EF_RETURN_IF_ERROR(validate_arguments(expression, 1));
    EF_RETURN_IF_ERROR(check(expression.type == DataType::Bool &&
                                 (expression.op == "is_null" || expression.op == "is_not_null"),
                             "Invalid IS NULL expression"));
    EF_ASSIGN_OR_RETURN(auto value, evaluate(*expression.args[0], input));

    return Value(is_null_value(value) == (expression.op == "is_null"));
  }
  case BoundExprKind::Cast:
    return evaluate_cast(expression, input);
  }

  return Status(ErrorCode::InvalidArgument, "Unknown expression kind");
}

Status validate_layout(const RelationDescriptor& relation) noexcept {
  EF_RETURN_IF_ERROR(sql_internal::validate_relation_metadata(relation));
  const auto& schema = relation.storage_schema;
  EF_RETURN_IF_ERROR(schema.validate_definition());
  EF_RETURN_IF_ERROR(
      check(relation.visible_count > 0 && relation.visible_count <= schema.fields.size(),
            "Invalid visible column count"));
  EF_RETURN_IF_ERROR(
      check(relation.physical_key_slots.size() == schema.primary_key.size() &&
                relation.identity_types.size() == schema.primary_key.size() &&
                relation.partition_slots.size() == schema.partition_key.size() &&
                relation.partition_types.size() == schema.partition_key.size() &&
                relation.partition_visible_slots.size() == schema.partition_key.size(),
            "Relation key metadata size mismatch"));
  std::vector<std::size_t> canonical_key;
  canonical_key.reserve(schema.primary_key.size());

  for (std::size_t i = 0; i < schema.partition_key.size(); ++i) {
    EF_ASSIGN_OR_RETURN(const auto slot, schema.index_of(schema.partition_key[i]));
    canonical_key.push_back(slot);
    EF_RETURN_IF_ERROR(check(relation.partition_slots[i] == slot &&
                                 relation.partition_types[i] == schema.fields[slot].type,
                             "Invalid partition slot or type"));
    const auto visible = relation.partition_visible_slots[i];
    EF_RETURN_IF_ERROR(
        check(visible == kNoSlot || (visible < relation.visible_count &&
                                     schema.fields[visible].type == relation.partition_types[i]),
              "Invalid visible partition slot"));
  }

  for (const auto& name : schema.primary_key) {
    if (std::find(schema.partition_key.begin(), schema.partition_key.end(), name) ==
        schema.partition_key.end()) {
      EF_ASSIGN_OR_RETURN(const auto slot, schema.index_of(name));
      canonical_key.push_back(slot);
    }
  }

  EF_RETURN_IF_ERROR(check(relation.physical_key_slots == canonical_key,
                           "Physical identity must have canonical partition prefix"));
  for (std::size_t i = 0; i < canonical_key.size(); ++i) {
    EF_RETURN_IF_ERROR(check(relation.identity_types[i] == schema.fields[canonical_key[i]].type,
                             "Physical identity type mismatch"));
  }

  if (relation.kind == RelationKind::BaseTable) {
    EF_RETURN_IF_ERROR(check(relation.visible_count == schema.fields.size(),
                             "Base table visible row must contain every storage column"));
    EF_RETURN_IF_ERROR(check(relation.partition_visible_slots == relation.partition_slots,
                             "Base table partition lineage mismatch"));
  } else {
    EF_RETURN_IF_ERROR(check(schema.fields.size() - relation.visible_count == canonical_key.size(),
                             "MV storage must append exactly the hidden identity columns"));
    for (std::size_t i = 0; i < canonical_key.size(); ++i) {
      const auto slot = relation.visible_count + i;
      EF_RETURN_IF_ERROR(
          check(canonical_key[i] == slot && schema.fields[slot].name.starts_with("__ef_"),
                "MV hidden identity layout mismatch"));
    }
  }

  if (relation.kind == RelationKind::FunctionView) {
    EF_RETURN_IF_ERROR(check(relation.visible_count >= canonical_key.size() &&
                                 relation.identity_fields.size() == canonical_key.size(),
                             "Function view must expose its complete host-owned identity"));
    for (std::size_t i = 0; i < canonical_key.size(); ++i)
      EF_RETURN_IF_ERROR(check(schema.fields[i].name == relation.identity_fields[i].name &&
                                   schema.fields[i].type == relation.identity_types[i] &&
                                   !schema.fields[i].nullable,
                               "Function view identity prefix mismatch"));
  }

  return {};
}

Status validate_stored_row(const RelationDescriptor& relation, const Row& row) noexcept {
  EF_RETURN_IF_ERROR(validate_layout(relation));
  EF_RETURN_IF_ERROR(relation.storage_schema.validate(row));
  for (std::size_t i = 0; i < relation.partition_slots.size(); ++i) {
    const auto visible = relation.partition_visible_slots[i];
    EF_RETURN_IF_ERROR(check(visible == kNoSlot || row[visible] == row[relation.partition_slots[i]],
                             "Visible partition value disagrees with physical identity"));
  }

  if (relation.kind == RelationKind::FunctionView)
    for (std::size_t i = 0; i < relation.physical_key_slots.size(); ++i)
      EF_RETURN_IF_ERROR(check(row[i] == row[relation.physical_key_slots[i]],
                               "Function view visible identity disagrees with physical identity"));
  if (relation.candidate || relation.request_status || relation.latest) {
    const auto epoch_slot =
        relation.candidate
            ? relation.candidate->definition.scope_fields.size()
            : (relation.request_status ? relation.request_status->definition.identity_fields.size()
                                       : relation.visible_count - 2);
    EF_RETURN_IF_ERROR(check(std::get<std::int64_t>(row[epoch_slot]) > 0,
                             "Lifecycle candidate epoch must be positive"));
    if (relation.request_status || relation.latest) {
      EF_RETURN_IF_ERROR(check(std::get<std::int64_t>(row[epoch_slot + 1]) > 0,
                               "Lifecycle admission sequence must be positive"));
    }

    if (relation.request_status) {
      const auto& phase = std::get<std::string>(row[epoch_slot + 2]);
      EF_RETURN_IF_ERROR(check(phase == "ACCEPTED" || phase == "RUNNING" || phase == "SUCCEEDED" ||
                                   phase == "FAILED" || phase == "CANCELLED",
                               "Unknown RequestStatus phase"));
    }
  }

  return {};
}

Row gather_slots(const Row& stored, const std::vector<std::size_t>& slots) noexcept {
  Row result;
  result.reserve(slots.size());
  for (const auto slot : slots)
    result.push_back(stored[slot]);

  return result;
}

} // namespace

StatusOr<Value> evaluate(const BoundExpr& expression, const Row& input) noexcept {
  if (expression.untyped_null) {
    EF_RETURN_IF_ERROR(
        check(expression.kind == BoundExprKind::Literal && is_null_value(expression.literal),
              "Untyped NULL must be a NULL literal"));
  }

  EF_ASSIGN_OR_RETURN(auto result, evaluate_impl(expression, input));
  EF_RETURN_IF_ERROR(
      validate_value(result, expression.type, expression.nullable || expression.untyped_null));
  return result;
}

StatusOr<bool> passes_filter(const BoundExprPtr& expression, const Row& input) noexcept {
  if (!expression)
    return true;

  EF_RETURN_IF_ERROR(check(expression->type == DataType::Bool, "Filter must have Bool type"));
  EF_ASSIGN_OR_RETURN(auto value, evaluate(*expression, input));
  const auto* result = std::get_if<bool>(&value);
  return result && *result;
}

StatusOr<Row> evaluate_projection(const BoundPlan& plan, const Row& input) noexcept {
  EF_RETURN_IF_ERROR(check(plan.projection.size() == plan.output_fields.size(),
                           "Projection and output schema sizes differ"));
  Row result;
  result.reserve(plan.projection.size());

  for (std::size_t i = 0; i < plan.projection.size(); ++i) {
    const auto& expression = plan.projection[i];
    const auto& field = plan.output_fields[i];
    EF_RETURN_IF_ERROR(
        check(expression && !expression->untyped_null && expression->type == field.type,
              "Projection expression and output field types differ"));
    EF_ASSIGN_OR_RETURN(auto value, evaluate(*expression, input));
    EF_RETURN_IF_ERROR(validate_value(value, field.type, field.nullable));
    result.push_back(std::move(value));
  }

  return result;
}

StatusOr<Row> visible_row(const RelationDescriptor& relation, const Row& stored) noexcept {
  EF_RETURN_IF_ERROR(validate_stored_row(relation, stored));
  return Row(stored.begin(), stored.begin() + relation.visible_count);
}

StatusOr<Row> physical_identity(const RelationDescriptor& relation, const Row& stored) noexcept {
  EF_RETURN_IF_ERROR(validate_stored_row(relation, stored));
  return gather_slots(stored, relation.physical_key_slots);
}

StatusOr<Row> partition_values(const RelationDescriptor& relation, const Row& stored) noexcept {
  EF_RETURN_IF_ERROR(validate_stored_row(relation, stored));
  return gather_slots(stored, relation.partition_slots);
}

StatusOr<Row> make_stored_row(const RelationDescriptor& relation, const Row& visible,
                              const Row& identity) noexcept {
  EF_RETURN_IF_ERROR(validate_layout(relation));
  EF_RETURN_IF_ERROR(check(visible.size() == relation.visible_count &&
                               identity.size() == relation.identity_types.size(),
                           "Visible row or identity size mismatch"));
  for (std::size_t i = 0; i < identity.size(); ++i)
    EF_RETURN_IF_ERROR(validate_value(identity[i], relation.identity_types[i], false));

  Row stored = visible;
  if (relation.kind != RelationKind::BaseTable)
    stored.insert(stored.end(), identity.begin(), identity.end());

  EF_RETURN_IF_ERROR(validate_stored_row(relation, stored));
  for (std::size_t i = 0; i < identity.size(); ++i) {
    EF_RETURN_IF_ERROR(check(stored[relation.physical_key_slots[i]] == identity[i],
                             "Row primary key does not match supplied identity"));
  }

  return stored;
}

} // namespace eventflow
