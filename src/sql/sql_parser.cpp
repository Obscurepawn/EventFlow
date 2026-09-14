#include "eventflow/sql_ast.hpp"
#include "generated/EventFlowSqlLexer.h"
#include "generated/EventFlowSqlParser.h"
#include <algorithm>
#include <cstdio>
#include <unordered_map>

namespace eventflow {
namespace {
using Parser = EventFlowSqlParser;
constexpr std::size_t kMaximumSqlBytes = 256 * 1024;
constexpr std::size_t kMaximumTokens = 4096;
constexpr std::size_t kMaximumParenthesisDepth = 64;
constexpr std::size_t kMaximumExpressionDepth = 128;

std::string lowercase_ascii(std::string text) noexcept {
  for (char& character : text) {
    if (character >= 'A' && character <= 'Z')
      character = static_cast<char>(character - 'A' + 'a');
  }

  return text;
}

std::string unquote(std::string_view text) noexcept {
  std::string result;
  result.reserve(text.size() - 2);
  for (std::size_t index = 1; index + 1 < text.size(); ++index) {
    result += text[index];
    if (text[index] == text.front())
      ++index;
  }

  return result;
}

std::string identifier(Parser::IdentifierContext* context) noexcept {
  auto text = context->getText();
  return context->QUOTED_IDENTIFIER() ? unquote(text) : lowercase_ascii(std::move(text));
}

DataType data_type(Parser::DataTypeContext* context) noexcept {
  if (context->K_BOOL())
    return DataType::Bool;
  if (context->K_INT64())
    return DataType::Int64;
  if (context->K_FLOAT64())
    return DataType::Float64;
  if (context->K_STRING())
    return DataType::String;
  if (context->K_TIMESTAMP_MICROS())
    return DataType::TimestampMicros;

  return DataType::FloatVector;
}

ExprAst expression_node(ExprAstKind kind, std::string text = {},
                        std::vector<ExprAstPtr> arguments = {}) noexcept {
  ExprAst node;
  node.kind = kind;
  node.text = std::move(text);
  node.args = std::move(arguments);

  return node;
}

class SyntaxErrors final : public antlr4::BaseErrorListener {
public:
  void syntaxError(antlr4::Recognizer*, antlr4::Token*, std::size_t line, std::size_t column,
                   const std::string& message, std::exception_ptr) noexcept override {
    if (!status_.ok())
      return;

    char detail[384];
    std::snprintf(detail, sizeof(detail), "SQL syntax error at %zu:%zu: %.250s", line, column + 1,
                  message.c_str());
    status_ = Status(ErrorCode::InvalidArgument, detail);
  }

  Status status() const noexcept {
    return status_;
  }

private:
  Status status_;
};

Status check_token_budget(antlr4::CommonTokenStream& tokens) noexcept {
  if (tokens.size() > kMaximumTokens)
    return Status(ErrorCode::ResourceExhausted, "SQL token budget exceeded");

  std::size_t depth = 0;
  for (auto* token : tokens.getTokens()) {
    if (token->getType() == Parser::LPAREN) {
      if (++depth > kMaximumParenthesisDepth)
        return Status(ErrorCode::ResourceExhausted, "SQL parenthesis nesting budget exceeded");
    } else if (token->getType() == Parser::RPAREN && depth > 0) {
      --depth;
    }
  }

  return {};
}

class AstBuilder final {
public:
  StatusOr<StatementAst> build(Parser::StatementContext* context) noexcept {
    if (context->createTable())
      return StatementAst{table(context->createTable())};

    StatementAst statement;
    if (auto* view = context->createView())
      statement = CreateViewAst{.name = identifier(view->identifier()),
                                .query = select(view->selectQuery())};
    else
      statement = select(context->selectQuery());
    if (!status_.ok())
      return status_;

    return statement;
  }

private:
  ExprAstPtr at(ExprAst expression, antlr4::Token* token) noexcept {
    if (!status_.ok())
      return {};

    std::size_t depth = 1;
    for (const auto& argument : expression.args) {
      if (!argument)
        return {};

      depth = std::max(depth, depths_.find(argument.get())->second + 1);
    }

    if (depth > kMaximumExpressionDepth) {
      status_ = Status(ErrorCode::ResourceExhausted, "SQL expression depth budget exceeded");
      return {};
    }

    expression.line = token->getLine();
    expression.column = token->getCharPositionInLine();
    auto result = std::make_shared<const ExprAst>(std::move(expression));
    depths_.emplace(result.get(), depth);

    return result;
  }

  std::vector<std::string> identifiers(Parser::IdentifierListContext* context) noexcept {
    std::vector<std::string> names;
    for (auto* name : context->identifier())
      names.push_back(identifier(name));

    return names;
  }

  CreateTableAst table(Parser::CreateTableContext* context) noexcept {
    CreateTableAst result;
    result.name = identifier(context->identifier());
    for (auto* definition : context->columnDefinition()) {
      std::optional<bool> nullable;
      if (definition->K_NULL())
        nullable = !definition->K_NOT();

      result.columns.push_back({.name = identifier(definition->identifier()),
                                .type = data_type(definition->dataType()),
                                .nullable = nullable});
    }

    result.primary_key = identifiers(context->identifierList(0));
    if (context->K_PARTITION())
      result.partition_key = identifiers(context->identifierList(1));

    return result;
  }

  TableRef table_reference(Parser::TableReferenceContext* context) noexcept {
    TableRef result{.name = identifier(context->identifier(0)), .alias = {}};
    if (context->identifier().size() == 2)
      result.alias = identifier(context->identifier(1));

    return result;
  }

  SelectAst select(Parser::SelectQueryContext* context) noexcept {
    SelectAst result;
    for (auto* item : context->selectItem()) {
      result.projection.push_back(
          {.expression = expression(item->expression()),
           .alias = item->identifier() ? identifier(item->identifier()) : ""});
    }

    result.from = table_reference(context->tableReference());
    if (auto* join = context->joinClause())
      result.join = JoinAst{.right = table_reference(join->tableReference()),
                            .condition = expression(join->expression())};

    const auto expressions = context->expression();
    std::size_t first_group_expression = 0;
    if (context->K_WHERE()) {
      result.where = expression(expressions[0]);
      first_group_expression = 1;
    }

    for (std::size_t index = first_group_expression; index < expressions.size(); ++index)
      result.group_by.push_back(expression(expressions[index]));

    return result;
  }

  ExprAstPtr expression(Parser::ExpressionContext* context) noexcept {
    return or_expression(context->orExpression());
  }

  template <class Context, class Child, class Build>
  ExprAstPtr fold(Context* context, const std::vector<Child*>& children, Build build) noexcept {
    auto result = build(children[0]);
    for (std::size_t index = 1; index < children.size() && status_.ok(); ++index) {
      auto operation = lowercase_ascii(context->children[2 * index - 1]->getText());
      auto right = build(children[index]);
      result = at(expression_node(ExprAstKind::Binary, std::move(operation),
                                  {std::move(result), std::move(right)}),
                  context->getStart());
    }

    return result;
  }

  ExprAstPtr or_expression(Parser::OrExpressionContext* context) noexcept {
    return fold(context, context->andExpression(),
                [this](auto* child) noexcept { return and_expression(child); });
  }

  ExprAstPtr and_expression(Parser::AndExpressionContext* context) noexcept {
    return fold(context, context->notExpression(),
                [this](auto* child) noexcept { return not_expression(child); });
  }

  ExprAstPtr not_expression(Parser::NotExpressionContext* context) noexcept {
    auto result = comparison(context->comparisonExpression());
    const auto operators = context->K_NOT();
    for (auto iterator = operators.rbegin(); iterator != operators.rend() && status_.ok();
         ++iterator) {
      result = at(expression_node(ExprAstKind::Unary, "not", {std::move(result)}),
                  (*iterator)->getSymbol());
    }

    return result;
  }

  ExprAstPtr comparison(Parser::ComparisonExpressionContext* context) noexcept {
    auto left = additive(context->additiveExpression(0));
    if (context->K_IS())
      return at(expression_node(ExprAstKind::IsNull, context->K_NOT() ? "is_not_null" : "is_null",
                                {std::move(left)}),
                context->getStart());
    if (context->additiveExpression().size() == 1)
      return left;

    auto right = additive(context->additiveExpression(1));
    return at(expression_node(ExprAstKind::Binary, context->children[1]->getText(),
                              {std::move(left), std::move(right)}),
              context->getStart());
  }

  ExprAstPtr additive(Parser::AdditiveExpressionContext* context) noexcept {
    return fold(context, context->multiplicativeExpression(),
                [this](auto* child) noexcept { return multiplicative(child); });
  }

  ExprAstPtr multiplicative(Parser::MultiplicativeExpressionContext* context) noexcept {
    return fold(context, context->unaryExpression(),
                [this](auto* child) noexcept { return unary_expression(child); });
  }

  ExprAstPtr unary_expression(Parser::UnaryExpressionContext* context) noexcept {
    auto result = primary(context->primaryExpression());
    for (std::size_t index = context->children.size() - 1; index > 0 && status_.ok(); --index) {
      auto* operation = static_cast<antlr4::tree::TerminalNode*>(context->children[index - 1]);
      result = at(expression_node(ExprAstKind::Unary, operation->getText(), {std::move(result)}),
                  operation->getSymbol());
    }

    return result;
  }

  ExprAstPtr primary(Parser::PrimaryExpressionContext* context) noexcept {
    auto* start = context->getStart();
    if (context->K_CAST()) {
      auto result = expression_node(ExprAstKind::Cast, {}, {expression(context->expression(0))});
      result.literal_type = data_type(context->dataType());
      return at(std::move(result), start);
    }

    const auto names = context->identifier();
    if (!names.empty()) {
      if (context->LPAREN()) {
        std::vector<ExprAstPtr> arguments;
        for (auto* argument : context->expression())
          arguments.push_back(expression(argument));

        return at(expression_node(ExprAstKind::Call, identifier(names[0]), std::move(arguments)),
                  start);
      }

      auto result = expression_node(context->STAR() ? ExprAstKind::Star : ExprAstKind::Column);
      if (context->DOT()) {
        result.qualifier = identifier(names[0]);
        if (!context->STAR())
          result.text = identifier(names[1]);
      } else {
        result.text = identifier(names[0]);
      }

      return at(std::move(result), start);
    }

    if (context->LPAREN())
      return expression(context->expression(0));
    if (context->NUMBER())
      return at(expression_node(ExprAstKind::Number, context->NUMBER()->getText()), start);
    if (context->STAR())
      return at(expression_node(ExprAstKind::Star), start);

    auto result = expression_node(ExprAstKind::Literal);
    if (context->STRING_LITERAL()) {
      result.literal = unquote(context->STRING_LITERAL()->getText());
      result.literal_type = DataType::String;
    } else if (context->K_TRUE() || context->K_FALSE()) {
      result.literal = context->K_TRUE() != nullptr;
      result.literal_type = DataType::Bool;
    }

    return at(std::move(result), start);
  }

  Status status_;
  std::unordered_map<const ExprAst*, std::size_t> depths_;
};
} // namespace

StatusOr<StatementAst> parse_sql(std::string_view sql) noexcept {
  if (sql.size() > kMaximumSqlBytes)
    return Status(ErrorCode::ResourceExhausted, "SQL byte budget exceeded");
  if (!valid_utf8(sql))
    return Status(ErrorCode::InvalidArgument, "SQL must be valid UTF-8");
  if (sql.find('\0') != std::string_view::npos)
    return Status(ErrorCode::InvalidArgument, "SQL must not contain NUL");

  SyntaxErrors errors;
  antlr4::ANTLRInputStream input(sql);
  EventFlowSqlLexer lexer(&input);
  lexer.removeErrorListeners();
  lexer.addErrorListener(&errors);
  antlr4::CommonTokenStream tokens(&lexer);
  tokens.fill();
  EF_RETURN_IF_ERROR(errors.status());
  EF_RETURN_IF_ERROR(check_token_budget(tokens));

  Parser parser(&tokens);
  parser.removeErrorListeners();
  parser.addErrorListener(&errors);
  auto* parsed = parser.statement();
  EF_RETURN_IF_ERROR(errors.status());

  return AstBuilder{}.build(parsed);
}

StatusOr<std::string> sql_identifier(std::string_view name, bool quoted) noexcept {
  if (name.empty() || name.find('\0') != std::string_view::npos || !valid_utf8(name))
    return Status(ErrorCode::InvalidArgument,
                  "Identifier must be nonempty valid UTF-8 without NUL");
  if (quoted)
    return std::string(name);

  for (std::size_t index = 0; index < name.size(); ++index) {
    const auto character = static_cast<unsigned char>(name[index]);
    const bool letter = (character >= 'A' && character <= 'Z') ||
                        (character >= 'a' && character <= 'z') || character == '_' ||
                        character >= 128;
    const bool digit = character >= '0' && character <= '9';
    if (!letter && !(index > 0 && digit))
      return Status(ErrorCode::InvalidArgument, "Invalid unquoted identifier");
  }

  return lowercase_ascii(std::string(name));
}

ExprAstPtr column(std::string name, std::string qualifier) noexcept {
  auto result = expression_node(ExprAstKind::Column, std::move(name));
  result.qualifier = std::move(qualifier);
  return std::make_shared<const ExprAst>(std::move(result));
}

ExprAstPtr literal(Value value, std::optional<DataType> type) noexcept {
  auto result = expression_node(ExprAstKind::Literal);
  result.literal = std::move(value);
  result.literal_type = type;
  return std::make_shared<const ExprAst>(std::move(result));
}

ExprAstPtr number(std::string text) noexcept {
  return std::make_shared<const ExprAst>(expression_node(ExprAstKind::Number, std::move(text)));
}

ExprAstPtr unary(std::string op, ExprAstPtr argument) noexcept {
  return std::make_shared<const ExprAst>(
      expression_node(ExprAstKind::Unary, std::move(op), {std::move(argument)}));
}

ExprAstPtr binary(std::string op, ExprAstPtr left, ExprAstPtr right) noexcept {
  return std::make_shared<const ExprAst>(
      expression_node(ExprAstKind::Binary, std::move(op), {std::move(left), std::move(right)}));
}

ExprAstPtr call(std::string name, std::vector<ExprAstPtr> arguments) noexcept {
  return std::make_shared<const ExprAst>(
      expression_node(ExprAstKind::Call, std::move(name), std::move(arguments)));
}

ExprAstPtr star(std::string qualifier) noexcept {
  auto result = expression_node(ExprAstKind::Star);
  result.qualifier = std::move(qualifier);
  return std::make_shared<const ExprAst>(std::move(result));
}

ExprAstPtr is_null(ExprAstPtr argument, bool negate) noexcept {
  return std::make_shared<const ExprAst>(expression_node(
      ExprAstKind::IsNull, negate ? "is_not_null" : "is_null", {std::move(argument)}));
}

ExprAstPtr cast(ExprAstPtr argument, DataType type) noexcept {
  auto result = expression_node(ExprAstKind::Cast, {}, {std::move(argument)});
  result.literal_type = type;
  return std::make_shared<const ExprAst>(std::move(result));
}
} // namespace eventflow
