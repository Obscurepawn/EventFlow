#include "binder_internal.hpp"
#include "eventflow/catalog.hpp"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <set>

namespace eventflow {
namespace {
std::string lower(std::string text) noexcept {
  for (auto& ch : text)
    if (ch >= 'A' && ch <= 'Z')
      ch = static_cast<char>(ch + ('a' - 'A'));

  return text;
}

Status name_ok(const std::string& name) noexcept {
  return check(!name.empty() && valid_utf8(name) && !lower(name).starts_with("__ef_"),
               "Invalid or reserved identifier: " + name);
}

bool numeric(DataType type) noexcept {
  return type == DataType::Int64 || type == DataType::Float64;
}

BoundExprPtr node(BoundExpr expression) noexcept {
  return std::make_shared<const BoundExpr>(std::move(expression));
}

BoundExprPtr input(std::size_t slot, const Field& field) noexcept {
  BoundExpr result;
  result.kind = BoundExprKind::Input;
  result.slot = slot;
  result.type = field.type;
  result.nullable = field.nullable;

  return node(std::move(result));
}

StatusOr<BoundExprPtr> typed_nulls(const BoundExprPtr& expression, DataType type) noexcept {
  if (!expression->untyped_null)
    return BoundExprPtr(expression);

  EF_RETURN_IF_ERROR(check(expression->kind == BoundExprKind::Literal || numeric(type),
                           "Untyped NULL arithmetic requires a numeric context"));
  BoundExpr result = *expression;
  result.type = type;
  result.untyped_null = false;

  for (auto& argument : result.args) {
    EF_ASSIGN_OR_RETURN(argument, typed_nulls(argument, type));
  }

  return node(std::move(result));
}

StatusOr<BoundExprPtr> literal_expr(const ExprAst& expression) noexcept {
  BoundExpr result;
  result.literal = expression.literal;
  result.nullable = std::holds_alternative<std::monostate>(result.literal);
  result.untyped_null = result.nullable && !expression.literal_type;
  if (expression.literal_type)
    result.type = *expression.literal_type;
  else {
    switch (result.literal.index()) {
    case 0:
    case 2:
      result.type = DataType::Int64;
      break;
    case 1:
      result.type = DataType::Bool;
      break;
    case 3:
      result.type = DataType::Float64;
      break;
    case 4:
      result.type = DataType::String;
      break;
    case 5:
      result.type = DataType::FloatVector;
      break;
    default:
      return Status(ErrorCode::InvalidArgument, "Invalid literal");
    }
  }

  EF_RETURN_IF_ERROR(check(!data_type_name(result.type).empty(), "Unknown literal type"));
  EF_ASSIGN_OR_RETURN(auto json, value_to_json(result.literal));
  EF_ASSIGN_OR_RETURN(auto validated, value_from_json(json, result.type, result.nullable));

  // Validate the C++ value representation as well as its JSON conversion.
  EF_RETURN_IF_ERROR(check(validated.index() == result.literal.index(),
                           "Literal representation does not match its type"));
  return node(std::move(result));
}

StatusOr<BoundExprPtr> number_expr(const std::string& text) noexcept {
  EF_RETURN_IF_ERROR(check(!text.empty(), "Empty numeric literal"));
  BoundExpr result;
  const auto* begin = text.data();
  const auto* end = begin + text.size();
  if (text.find_first_of(".eE") == std::string::npos) {
    std::int64_t value = 0;
    const auto parsed = std::from_chars(begin, end, value);
    EF_RETURN_IF_ERROR(check(parsed.ec == std::errc{} && parsed.ptr == end,
                             "Invalid or overflowing Int64 literal"));
    result.literal = value;
  } else {
    double value = 0;
    const auto parsed = std::from_chars(begin, end, value, std::chars_format::general);
    EF_RETURN_IF_ERROR(check(parsed.ec == std::errc{} && parsed.ptr == end && std::isfinite(value),
                             "Invalid or nonfinite Float64 literal"));
    result.type = DataType::Float64;
    result.literal = value;
  }

  return node(std::move(result));
}

struct Binding {
  const RelationDescriptor* relation;
  std::string qualifier;
  std::size_t offset;
};

class SelectBinder {
public:
  SelectBinder(const SelectAst& query, const CatalogSnapshot& catalog) noexcept
      : query_(query), catalog_(catalog) {}

  StatusOr<BoundPlan> run() noexcept;

private:
  StatusOr<std::size_t> resolve(const ExprAst& expression) const noexcept;
  StatusOr<BoundExprPtr> bind(const ExprAstPtr& expression, bool grouped, bool aggregates,
                              std::size_t depth = 0) noexcept;
  StatusOr<BoundExprPtr> aggregate(const ExprAst& expression, std::size_t depth) noexcept;
  Status check_join(const ExprAstPtr& expression, std::set<std::size_t>& keys,
                    std::size_t depth = 0) noexcept;
  std::optional<std::size_t> partition_index(std::size_t slot) const noexcept;
  void infer_scope(const BoundExprPtr& expression,
                   std::vector<std::optional<Value>>& values) noexcept;
  const SelectAst& query_;
  const CatalogSnapshot& catalog_;
  std::vector<Binding> bindings_;
  std::vector<Field> fields_;
  std::map<std::size_t, std::size_t> grouped_slots_;
  BoundPlan plan_;
};

StatusOr<std::size_t> SelectBinder::resolve(const ExprAst& expression) const noexcept {
  if (!sql_internal::latest_provenance_name(expression.text))
    EF_RETURN_IF_ERROR(name_ok(expression.text));
  if (!expression.qualifier.empty())
    EF_RETURN_IF_ERROR(name_ok(expression.qualifier));

  std::size_t slot = kNoSlot;
  for (const auto& binding : bindings_) {
    if (!expression.qualifier.empty() && expression.qualifier != binding.qualifier)
      continue;

    for (std::size_t i = 0; i < binding.relation->visible_count; ++i) {
      if (binding.relation->storage_schema.fields[i].name != expression.text)
        continue;

      EF_RETURN_IF_ERROR(check(slot == kNoSlot, "Ambiguous column: " + expression.text));
      slot = binding.offset + i;
    }
  }

  EF_RETURN_IF_ERROR(check(slot != kNoSlot, "Unknown column: " + expression.text));
  return slot;
}

std::optional<std::size_t> SelectBinder::partition_index(std::size_t slot) const noexcept {
  for (const auto& binding : bindings_) {
    if (slot >= binding.offset && slot - binding.offset < binding.relation->visible_count)
      return sql_internal::partition_lineage(*binding.relation, slot - binding.offset, catalog_);
  }

  return {};
}

StatusOr<BoundExprPtr> SelectBinder::aggregate(const ExprAst& expression,
                                               std::size_t depth) noexcept {
  const auto name = lower(expression.text);
  EF_RETURN_IF_ERROR(check(name == "sum" || name == "avg" || name == "count",
                           "Unknown function: " + expression.text));
  EF_RETURN_IF_ERROR(check(expression.args.size() == 1 && expression.args[0],
                           "Aggregate requires exactly one argument"));
  AggregateSpec spec;
  bool nullable = false;
  if (expression.args[0]->kind == ExprAstKind::Star) {
    EF_RETURN_IF_ERROR(check(name == "count" && expression.args[0]->qualifier.empty() &&
                                 expression.args[0]->args.empty(),
                             "Only COUNT(*) accepts a star argument"));
    spec.kind = AggregateKind::CountAll;
  } else {
    EF_ASSIGN_OR_RETURN(spec.argument, bind(expression.args[0], false, false, depth + 1));
    if (name == "count") {
      spec.kind = AggregateKind::Count;
      EF_ASSIGN_OR_RETURN(spec.argument, typed_nulls(spec.argument, DataType::Int64));
    } else {
      EF_RETURN_IF_ERROR(check(!spec.argument->untyped_null && numeric(spec.argument->type),
                               "SUM/AVG requires a typed numeric argument"));
      spec.kind = name == "sum" ? AggregateKind::Sum : AggregateKind::Average;
      spec.result_type = name == "sum" ? spec.argument->type : DataType::Float64;
      nullable = true;
    }
  }

  const auto slot = plan_.group_by.size() + plan_.aggregates.size();
  const auto type = spec.result_type;
  plan_.aggregates.push_back(std::move(spec));
  return input(slot, Field{.id = 0, .name = {}, .type = type, .nullable = nullable});
}

StatusOr<BoundExprPtr> SelectBinder::bind(const ExprAstPtr& expression, bool grouped,
                                          bool aggregates, std::size_t depth) noexcept {
  EF_RETURN_IF_ERROR(check(expression != nullptr && depth <= 128,
                           "Missing expression or expression nesting exceeds 128"));
  const auto& ast = *expression;
  const auto arity = [&ast](std::size_t n) noexcept {
    return check(ast.args.size() == n, "Invalid expression operand count");
  };
  if (ast.kind == ExprAstKind::Column) {
    EF_RETURN_IF_ERROR(arity(0));
    EF_ASSIGN_OR_RETURN(auto slot, resolve(ast));
    if (grouped) {
      const auto found = grouped_slots_.find(slot);
      EF_RETURN_IF_ERROR(
          check(found != grouped_slots_.end(), "Nonaggregate column is not a GROUP BY key"));
      return input(found->second, fields_[slot]);
    }

    return input(slot, fields_[slot]);
  }

  if (ast.kind == ExprAstKind::Literal) {
    EF_RETURN_IF_ERROR(arity(0));
    return literal_expr(ast);
  }

  if (ast.kind == ExprAstKind::Number) {
    EF_RETURN_IF_ERROR(arity(0));
    return number_expr(ast.text);
  }

  if (ast.kind == ExprAstKind::Star)
    return Status(ErrorCode::InvalidArgument, "Star is only valid as a projection or COUNT(*)");
  if (ast.kind == ExprAstKind::Call) {
    EF_RETURN_IF_ERROR(
        check(aggregates && grouped, "Aggregate is not permitted here or lacks GROUP BY"));
    return aggregate(ast, depth);
  }

  BoundExpr result;
  result.op = lower(ast.text);
  if (ast.kind == ExprAstKind::Unary) {
    EF_RETURN_IF_ERROR(arity(1));
    EF_RETURN_IF_ERROR(check(result.op == "+" || result.op == "-" || result.op == "not",
                             "Unknown unary operator"));

    // Parsing the signed magnitude permits the one valid Int64 minimum literal.
    if (result.op == "-" && ast.args[0] && ast.args[0]->kind == ExprAstKind::Number &&
        ast.args[0]->args.empty() && ast.args[0]->text == "9223372036854775808")
      return number_expr("-9223372036854775808");

    EF_ASSIGN_OR_RETURN(auto argument, bind(ast.args[0], grouped, aggregates, depth + 1));
    if (result.op == "not") {
      EF_ASSIGN_OR_RETURN(argument, typed_nulls(argument, DataType::Bool));
      EF_RETURN_IF_ERROR(check(argument->type == DataType::Bool, "NOT requires Bool"));
    } else
      EF_RETURN_IF_ERROR(check(argument->untyped_null || numeric(argument->type),
                               "Unary arithmetic requires a number"));

    result.kind = BoundExprKind::Unary;
    result.type = argument->type;
    result.nullable = argument->nullable;
    result.untyped_null = argument->untyped_null;
    result.args = {std::move(argument)};
  } else if (ast.kind == ExprAstKind::IsNull) {
    EF_RETURN_IF_ERROR(arity(1));
    EF_RETURN_IF_ERROR(
        check(result.op == "is_null" || result.op == "is_not_null", "Unknown NULL predicate"));

    EF_ASSIGN_OR_RETURN(auto argument, bind(ast.args[0], grouped, aggregates, depth + 1));

    result.kind = BoundExprKind::IsNull;
    result.type = DataType::Bool;
    EF_ASSIGN_OR_RETURN(argument, typed_nulls(argument, DataType::Int64));
    result.args = {std::move(argument)};
  } else if (ast.kind == ExprAstKind::Cast) {
    EF_RETURN_IF_ERROR(arity(1));
    EF_RETURN_IF_ERROR(
        check(ast.literal_type == DataType::Float64, "Only CAST AS FLOAT64 is supported"));

    EF_ASSIGN_OR_RETURN(auto argument, bind(ast.args[0], grouped, aggregates, depth + 1));
    EF_ASSIGN_OR_RETURN(argument, typed_nulls(argument, DataType::Float64));
    EF_RETURN_IF_ERROR(check(numeric(argument->type), "CAST AS FLOAT64 requires a numeric input"));

    result.kind = BoundExprKind::Cast;
    result.type = DataType::Float64;
    result.op.clear();
    result.nullable = argument->nullable;
    result.args = {std::move(argument)};
  } else if (ast.kind == ExprAstKind::Binary) {
    EF_RETURN_IF_ERROR(arity(2));
    EF_ASSIGN_OR_RETURN(auto left, bind(ast.args[0], grouped, aggregates, depth + 1));
    EF_ASSIGN_OR_RETURN(auto right, bind(ast.args[1], grouped, aggregates, depth + 1));

    const bool logical = result.op == "and" || result.op == "or";
    const bool arithmetic =
        result.op == "+" || result.op == "-" || result.op == "*" || result.op == "/";
    const bool equal = result.op == "=" || result.op == "!=" || result.op == "<>";
    const bool ordered =
        result.op == "<" || result.op == "<=" || result.op == ">" || result.op == ">=";
    EF_RETURN_IF_ERROR(check(logical || arithmetic || equal || ordered, "Unknown binary operator"));
    if (result.op == "<>")
      result.op = "!=";

    if (logical) {
      EF_ASSIGN_OR_RETURN(left, typed_nulls(left, DataType::Bool));
      EF_ASSIGN_OR_RETURN(right, typed_nulls(right, DataType::Bool));
    } else {
      if (!right->untyped_null) {
        EF_ASSIGN_OR_RETURN(left, typed_nulls(left, right->type));
      }

      if (!left->untyped_null) {
        EF_ASSIGN_OR_RETURN(right, typed_nulls(right, left->type));
      }

      if (!arithmetic && left->untyped_null && right->untyped_null) {
        EF_ASSIGN_OR_RETURN(left, typed_nulls(left, DataType::Int64));
        EF_ASSIGN_OR_RETURN(right, typed_nulls(right, DataType::Int64));
      }
    }

    EF_RETURN_IF_ERROR(
        check(left->type == right->type, "Operands require the same type; use explicit CAST"));
    if (logical)
      EF_RETURN_IF_ERROR(check(left->type == DataType::Bool, "AND/OR requires Bool"));
    if (arithmetic)
      EF_RETURN_IF_ERROR(check(numeric(left->type), "Arithmetic requires Int64 or Float64"));
    if (equal || ordered) {
      EF_RETURN_IF_ERROR(
          check(left->type != DataType::FloatVector, "FloatVector cannot be compared"));
      EF_RETURN_IF_ERROR(check(!ordered || left->type != DataType::Bool,
                               "Bool only supports equality comparison"));
    }

    result.kind = BoundExprKind::Binary;
    result.type = arithmetic ? left->type : DataType::Bool;
    result.nullable = left->nullable || right->nullable;
    result.untyped_null = arithmetic && left->untyped_null && right->untyped_null;
    result.args = {std::move(left), std::move(right)};
  } else
    return Status(ErrorCode::InvalidArgument, "Unknown expression kind");
  return node(std::move(result));
}

Status SelectBinder::check_join(const ExprAstPtr& expression, std::set<std::size_t>& keys,
                                std::size_t depth) noexcept {
  EF_RETURN_IF_ERROR(
      check(expression && depth <= 128, "Missing JOIN predicate or excessive nesting"));
  const auto& ast = *expression;
  EF_RETURN_IF_ERROR(
      check(ast.kind == ExprAstKind::Binary && ast.args.size() == 2 && ast.args[0] && ast.args[1],
            "JOIN ON must compare full partition keys"));
  if (lower(ast.text) == "and") {
    EF_RETURN_IF_ERROR(check_join(ast.args[0], keys, depth + 1));
    return check_join(ast.args[1], keys, depth + 1);
  }

  EF_RETURN_IF_ERROR(check(ast.text == "=" && ast.args[0]->kind == ExprAstKind::Column &&
                               ast.args[1]->kind == ExprAstKind::Column &&
                               ast.args[0]->args.empty() && ast.args[1]->args.empty(),
                           "JOIN ON must be an AND of partition column equalities"));
  EF_ASSIGN_OR_RETURN(auto left, resolve(*ast.args[0]));
  EF_ASSIGN_OR_RETURN(auto right, resolve(*ast.args[1]));
  const auto split = bindings_[1].offset;
  EF_RETURN_IF_ERROR(
      check((left < split) != (right < split), "JOIN equality must connect left and right"));
  const auto left_key = partition_index(left);
  const auto right_key = partition_index(right);
  EF_RETURN_IF_ERROR(check(left_key && right_key && *left_key == *right_key,
                           "JOIN must match corresponding partition key positions"));
  EF_RETURN_IF_ERROR(check(keys.insert(*left_key).second, "Duplicate JOIN partition equality"));

  return {};
}

bool constant(const BoundExprPtr& expression) noexcept {
  if (expression->kind == BoundExprKind::Input)
    return false;

  for (const auto& argument : expression->args)
    if (!constant(argument))
      return false;

  return true;
}

void SelectBinder::infer_scope(const BoundExprPtr& expression,
                               std::vector<std::optional<Value>>& values) noexcept {
  if (!expression || expression->kind != BoundExprKind::Binary)
    return;
  if (expression->op == "and") {
    infer_scope(expression->args[0], values);
    infer_scope(expression->args[1], values);
    return;
  }

  if (expression->op != "=")
    return;

  for (std::size_t side = 0; side != 2; ++side) {
    const auto& column = expression->args[side];
    const auto& value = expression->args[1 - side];
    if (column->kind != BoundExprKind::Input || !constant(value))
      continue;

    const auto key = partition_index(column->slot);
    if (!key || values[*key])
      continue;

    auto evaluated = evaluate(*value, {});
    if (evaluated.ok() && !std::holds_alternative<std::monostate>(*evaluated))
      values[*key] = std::move(*evaluated);
  }
}

StatusOr<BoundPlan> SelectBinder::run() noexcept {
  EF_RETURN_IF_ERROR(name_ok(query_.from.name));
  if (!query_.from.alias.empty())
    EF_RETURN_IF_ERROR(name_ok(query_.from.alias));

  EF_ASSIGN_OR_RETURN(const auto left, catalog_.find(query_.from.name));
  bindings_.push_back({.relation = left,
                       .qualifier = query_.from.alias.empty() ? left->name : query_.from.alias,
                       .offset = 0});
  fields_.assign(left->storage_schema.fields.begin(),
                 left->storage_schema.fields.begin() + left->visible_count);
  plan_.catalog_version = catalog_.version;
  plan_.catalog_fingerprint = catalog_.fingerprint;
  plan_.left = left->id;
  plan_.partition_types = left->partition_types;
  plan_.identity_types = left->identity_types;
  plan_.identity_domain = left->identity_domain;
  plan_.identity_fields = left->identity_fields;
  plan_.unique_per_partition = left->unique_per_partition;
  if (query_.join) {
    EF_RETURN_IF_ERROR(name_ok(query_.join->right.name));
    if (!query_.join->right.alias.empty())
      EF_RETURN_IF_ERROR(name_ok(query_.join->right.alias));

    EF_ASSIGN_OR_RETURN(const auto right, catalog_.find(query_.join->right.name));
    const auto qualifier =
        query_.join->right.alias.empty() ? right->name : query_.join->right.alias;
    EF_RETURN_IF_ERROR(check(qualifier != bindings_[0].qualifier, "Duplicate relation qualifier"));
    EF_RETURN_IF_ERROR(check(left->unique_per_partition && right->unique_per_partition,
                             "JOIN requires proof of at most one row per partition on both sides"));
    EF_RETURN_IF_ERROR(check(left->partition_types == right->partition_types,
                             "JOIN partition key types must match"));
    bindings_.push_back({.relation = right, .qualifier = qualifier, .offset = fields_.size()});
    fields_.insert(fields_.end(), right->storage_schema.fields.begin(),
                   right->storage_schema.fields.begin() + right->visible_count);
    std::set<std::size_t> keys;
    EF_RETURN_IF_ERROR(check_join(query_.join->condition, keys));
    EF_RETURN_IF_ERROR(check(keys.size() == plan_.partition_types.size(),
                             "JOIN ON must cover every partition key column"));
    plan_.right = right->id;
    plan_.identity_types = plan_.partition_types;
    plan_.identity_domain.clear();
    plan_.identity_fields.clear();
    plan_.unique_per_partition = true;
  }

  if (query_.where) {
    EF_ASSIGN_OR_RETURN(plan_.filter, bind(query_.where, false, false));
    EF_ASSIGN_OR_RETURN(plan_.filter, typed_nulls(plan_.filter, DataType::Bool));
    EF_RETURN_IF_ERROR(check(plan_.filter->type == DataType::Bool, "WHERE requires Bool"));
    std::vector<std::optional<Value>> values(plan_.partition_types.size());
    infer_scope(plan_.filter, values);
    if (std::all_of(values.begin(), values.end(),
                    [](const auto& item) noexcept { return item.has_value(); })) {
      Row scope;
      for (auto& item : values)
        scope.push_back(std::move(*item));

      plan_.inferred_scope = std::move(scope);
    }
  }

  const bool grouped = !query_.group_by.empty();
  if (grouped) {
    std::set<std::size_t> keys;
    for (const auto& expression : query_.group_by) {
      EF_RETURN_IF_ERROR(
          check(expression && expression->kind == ExprAstKind::Column && expression->args.empty(),
                "GROUP BY requires simple partition key columns"));
      EF_ASSIGN_OR_RETURN(const auto slot, resolve(*expression));
      const auto key = partition_index(slot);
      EF_RETURN_IF_ERROR(check(key.has_value(), "GROUP BY contains a nonpartition column"));
      EF_RETURN_IF_ERROR(check(keys.insert(*key).second, "Duplicate GROUP BY partition column"));
    }

    EF_RETURN_IF_ERROR(check(keys.size() == plan_.partition_types.size(),
                             "GROUP BY must cover the full partition key"));
    for (std::size_t key = 0; key < plan_.partition_types.size(); ++key) {
      std::size_t canonical = kNoSlot;
      for (const auto& binding : bindings_) {
        for (std::size_t visible = 0; visible < binding.relation->visible_count; ++visible) {
          if (sql_internal::partition_lineage(*binding.relation, visible, catalog_) != key)
            continue;

          const auto slot = binding.offset + visible;
          grouped_slots_[slot] = key;
          if (canonical == kNoSlot)
            canonical = slot;
        }
      }

      EF_RETURN_IF_ERROR(check(canonical != kNoSlot, "GROUP BY partition key is not visible"));
      plan_.group_by.push_back(input(canonical, fields_[canonical]));
    }

    plan_.identity_types = plan_.partition_types;
    plan_.identity_domain.clear();
    plan_.identity_fields.clear();
    plan_.unique_per_partition = true;
  }

  EF_RETURN_IF_ERROR(check(!query_.projection.empty(), "SELECT requires projections"));
  std::set<std::string> names;
  const auto add_projection = [this, &names](BoundExprPtr expression,
                                             std::string name) noexcept -> Status {
    if (!sql_internal::generated_projection(name, expression, fields_))
      EF_RETURN_IF_ERROR(name_ok(name));

    EF_RETURN_IF_ERROR(check(names.insert(name).second, "Duplicate output column name: " + name));
    EF_RETURN_IF_ERROR(check(!expression->untyped_null, "Output NULL has no concrete type"));
    plan_.output_fields.push_back({.id = static_cast<std::uint32_t>(plan_.output_fields.size() + 1),
                                   .name = std::move(name),
                                   .type = expression->type,
                                   .nullable = expression->nullable});
    plan_.projection.push_back(std::move(expression));

    return {};
  };
  for (const auto& item : query_.projection) {
    EF_RETURN_IF_ERROR(check(item.expression != nullptr, "Missing projection expression"));
    if (item.expression->kind == ExprAstKind::Star) {
      EF_RETURN_IF_ERROR(check(item.alias.empty() && item.expression->args.empty(),
                               "Star projection cannot have alias or operands"));
      bool matched = false;
      for (const auto& binding : bindings_) {
        if (!item.expression->qualifier.empty() && item.expression->qualifier != binding.qualifier)
          continue;

        matched = true;
        for (std::size_t i = 0; i < binding.relation->visible_count; ++i) {
          const auto slot = binding.offset + i;
          auto projected = input(slot, fields_[slot]);
          if (grouped) {
            const auto found = grouped_slots_.find(slot);
            EF_RETURN_IF_ERROR(
                check(found != grouped_slots_.end(), "Star projects a nongrouped column"));
            projected = input(found->second, fields_[slot]);
          }

          EF_RETURN_IF_ERROR(add_projection(std::move(projected), fields_[slot].name));
        }
      }

      EF_RETURN_IF_ERROR(check(matched, "Unknown star qualifier"));
    } else {
      if (!item.alias.empty())
        EF_RETURN_IF_ERROR(name_ok(item.alias));

      EF_ASSIGN_OR_RETURN(auto projected, bind(item.expression, grouped, grouped));
      const auto name = item.alias.empty() && item.expression->kind == ExprAstKind::Column
                            ? item.expression->text
                            : item.alias;
      EF_RETURN_IF_ERROR(add_projection(std::move(projected), name));
    }
  }

  plan_.partition_visible_slots.assign(plan_.partition_types.size(), kNoSlot);
  for (std::size_t i = 0; i < plan_.projection.size(); ++i) {
    const auto& expression = plan_.projection[i];
    if (expression->kind != BoundExprKind::Input)
      continue;

    const auto key = grouped ? (expression->slot < plan_.group_by.size()
                                    ? std::optional<std::size_t>(expression->slot)
                                    : std::nullopt)
                             : partition_index(expression->slot);
    if (key && plan_.partition_visible_slots[*key] == kNoSlot)
      plan_.partition_visible_slots[*key] = i;
  }

  return std::move(plan_);
}

StatusOr<Json> expression_json(const BoundExprPtr& expression, std::size_t depth = 0) noexcept {
  if (!expression)
    return Json(nullptr);

  EF_RETURN_IF_ERROR(
      check(depth <= 128 && valid_utf8(expression->op) && !data_type_name(expression->type).empty(),
            "Invalid expression metadata or nesting"));
  EF_ASSIGN_OR_RETURN(auto value, value_to_json(expression->literal));
  Json arguments = Json::array();

  for (const auto& argument : expression->args) {
    EF_ASSIGN_OR_RETURN(auto json, expression_json(argument, depth + 1));
    arguments.push_back(std::move(json));
  }

  return Json{{"kind", static_cast<int>(expression->kind)},
              {"type", data_type_name(expression->type)},
              {"nullable", expression->nullable},
              {"untyped_null", expression->untyped_null},
              {"slot", expression->slot},
              {"literal", std::move(value)},
              {"op", expression->op},
              {"args", std::move(arguments)}};
}
} // namespace

StatusOr<BoundPlan> sql_internal::bind_select_trusted(const SelectAst& query,
                                                      const CatalogSnapshot& catalog) noexcept {
  return SelectBinder(query, catalog).run();
}

StatusOr<BoundPlan> bind_select(const SelectAst& query, const CatalogSnapshot& catalog) noexcept {
  EF_ASSIGN_OR_RETURN(auto validated, Catalog::from_snapshot(catalog));
  return validated->bind_query(query);
}

StatusOr<Json> bound_plan_json(const BoundPlan& plan) noexcept {
  // Provenance is validated separately; embedding its full catalog here would
  // recursively expand every subsequent materialized-view definition.
  Json output = Json::array(), groups = Json::array(), projections = Json::array(),
       aggregates = Json::array();
  Json identity = Json::array(), partition = Json::array(), partition_slots = Json::array();

  for (const auto& field : plan.output_fields) {
    EF_RETURN_IF_ERROR(check(valid_utf8(field.name) && !data_type_name(field.type).empty(),
                             "Invalid output field metadata"));
    output.push_back({{"id", field.id},
                      {"name", field.name},
                      {"type", data_type_name(field.type)},
                      {"nullable", field.nullable}});
  }

  for (const auto& expression : plan.group_by) {
    EF_ASSIGN_OR_RETURN(auto json, expression_json(expression));
    groups.push_back(std::move(json));
  }

  for (const auto& expression : plan.projection) {
    EF_ASSIGN_OR_RETURN(auto json, expression_json(expression));
    projections.push_back(std::move(json));
  }

  for (const auto& aggregate : plan.aggregates) {
    EF_ASSIGN_OR_RETURN(auto argument, expression_json(aggregate.argument));
    aggregates.push_back({{"kind", static_cast<int>(aggregate.kind)},
                          {"argument", std::move(argument)},
                          {"result_type", data_type_name(aggregate.result_type)},
                          {"state_encoding_version", 1}});
  }

  for (const auto type : plan.identity_types)
    identity.push_back(data_type_name(type));

  for (const auto type : plan.partition_types)
    partition.push_back(data_type_name(type));

  for (const auto slot : plan.partition_visible_slots)
    partition_slots.push_back(slot == kNoSlot ? Json(nullptr) : Json(slot));

  EF_ASSIGN_OR_RETURN(auto filter, expression_json(plan.filter));
  Json scope = nullptr;
  if (plan.inferred_scope) {
    scope = Json::array();
    for (const auto& value : *plan.inferred_scope) {
      EF_ASSIGN_OR_RETURN(auto json, value_to_json(value));
      scope.push_back(std::move(json));
    }
  }

  Json operators = Json::array({"ReadRelation"});
  if (plan.right)
    operators.push_back("LookupJoin");
  if (plan.filter)
    operators.push_back("Filter");
  if (!plan.group_by.empty())
    operators.push_back("Aggregate");

  operators.push_back("Project");
  Json result{{"catalog_version", plan.catalog_version},
              {"left", plan.left},
              {"right", plan.right ? Json(*plan.right) : Json(nullptr)},
              {"filter", std::move(filter)},
              {"group_by", std::move(groups)},
              {"aggregates", std::move(aggregates)},
              {"projection", std::move(projections)},
              {"output_fields", std::move(output)},
              {"identity_types", std::move(identity)},
              {"partition_types", std::move(partition)},
              {"partition_visible_slots", std::move(partition_slots)},
              {"unique_per_partition", plan.unique_per_partition},
              {"inferred_scope", std::move(scope)},
              {"operators", std::move(operators)},
              {"has_internal_state", !plan.group_by.empty()}};
  if (!plan.identity_domain.empty() || !plan.identity_fields.empty()) {
    result["identity_domain"] = plan.identity_domain;
    result["identity_fields"] = sql_internal::fields_json(plan.identity_fields);
  }

  return result;
}
} // namespace eventflow
