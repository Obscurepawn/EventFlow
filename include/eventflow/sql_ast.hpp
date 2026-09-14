#pragma once
#include "eventflow/types.hpp"
#include <memory>
#include <optional>
#include <string_view>

namespace eventflow {
struct ExprAst;
using ExprAstPtr = std::shared_ptr<const ExprAst>;
enum class ExprAstKind { Column, Literal, Number, Unary, Binary, IsNull, Cast, Call, Star };

// Identifiers in an AST are canonical: SQL unquoted identifiers are lowercase.
struct ExprAst {
  ExprAstKind kind = ExprAstKind::Literal;
  std::string text;
  std::string qualifier;
  Value literal;
  std::optional<DataType> literal_type;
  std::vector<ExprAstPtr> args;
  std::size_t line = 0;
  std::size_t column = 0;
};

struct SelectItem {
  ExprAstPtr expression;
  std::string alias;
};

struct TableRef {
  std::string name;
  std::string alias;
};

struct JoinAst {
  TableRef right;
  ExprAstPtr condition;
};

struct SelectAst {
  std::vector<SelectItem> projection;
  TableRef from;
  std::optional<JoinAst> join;
  ExprAstPtr where;
  std::vector<ExprAstPtr> group_by;
};

struct ColumnAst {
  std::string name;
  DataType type;
  std::optional<bool> nullable;
};

struct CreateTableAst {
  std::string name;
  std::vector<ColumnAst> columns;
  std::vector<std::string> primary_key;
  std::vector<std::string> partition_key;
};

struct CreateViewAst {
  std::string name;
  SelectAst query;
};

using StatementAst = std::variant<CreateTableAst, CreateViewAst, SelectAst>;
StatusOr<StatementAst> parse_sql(std::string_view sql) noexcept;
StatusOr<std::string> sql_identifier(std::string_view name, bool quoted = false) noexcept;
// These builders construct the same AST accepted by the SQL binder.
ExprAstPtr column(std::string name, std::string qualifier = {}) noexcept;
ExprAstPtr literal(Value value, std::optional<DataType> type = {}) noexcept;
ExprAstPtr number(std::string text) noexcept;
ExprAstPtr unary(std::string op, ExprAstPtr argument) noexcept;
ExprAstPtr binary(std::string op, ExprAstPtr left, ExprAstPtr right) noexcept;
ExprAstPtr call(std::string name, std::vector<ExprAstPtr> arguments) noexcept;
ExprAstPtr star(std::string qualifier = {}) noexcept;
ExprAstPtr is_null(ExprAstPtr argument, bool negate = false) noexcept;
ExprAstPtr cast(ExprAstPtr argument, DataType type) noexcept;
} // namespace eventflow
