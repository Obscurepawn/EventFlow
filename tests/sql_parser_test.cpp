#include "eventflow/sql_ast.hpp"
#include "status_test_support.hpp"
#include <atomic>
#include <gtest/gtest.h>
#include <thread>

namespace eventflow {
namespace {
SelectAst query(std::string_view sql) noexcept {
  auto statement = test::take(parse_sql(sql));
  auto* select = std::get_if<SelectAst>(&statement);
  if (!select) {
    ADD_FAILURE() << "Expected a SELECT statement";
    std::abort();
  }
  return std::move(*select);
}

void expect_same_expression(const ExprAstPtr& left, const ExprAstPtr& right) noexcept {
  ASSERT_NE(left, nullptr);
  ASSERT_NE(right, nullptr);
  EXPECT_EQ(left->kind, right->kind);
  EXPECT_EQ(left->text, right->text);
  EXPECT_EQ(left->qualifier, right->qualifier);
  EXPECT_EQ(left->literal, right->literal);
  EXPECT_EQ(left->literal_type, right->literal_type);
  ASSERT_EQ(left->args.size(), right->args.size());
  for (std::size_t index = 0; index < left->args.size(); ++index)
    expect_same_expression(left->args[index], right->args[index]);
}

TEST(SqlParser, ParsesOneSelectAndOwnsCanonicalIdentifiers) {
  auto result = parse_sql("SeLeCt Account FROM FiLLs;");
  ASSERT_TRUE(result.ok()) << result.status().message();
  const auto* query = std::get_if<SelectAst>(&result.value());
  ASSERT_NE(query, nullptr);
  EXPECT_EQ(query->from.name, "fills");
  ASSERT_EQ(query->projection.size(), 1U);
  EXPECT_EQ(query->projection[0].expression->kind, ExprAstKind::Column);
  EXPECT_EQ(query->projection[0].expression->text, "account");
}

TEST(SqlParser, OwnsStringsAfterInputAndAntlrObjectsAreDestroyed) {
  std::string sql = "SELECT 'it''s durable' AS message FROM FiLLs AS f";
  auto select = query(sql);
  sql.assign(sql.size(), 'x');
  EXPECT_EQ(select.from.name, "fills");
  EXPECT_EQ(select.from.alias, "f");
  EXPECT_EQ(select.projection[0].alias, "message");
  EXPECT_EQ(std::get<std::string>(select.projection[0].expression->literal), "it's durable");
}

TEST(SqlParser, PreservesQuotedIdentifiersAndUnicodeWithoutCaseFoldingThem) {
  auto select = query("SELECT \"\u8D26\u6237\".\"Fill\"\"ID\" AS \"\u663E\u793A\u540D\", "
                      "caf\u00E9, \U00010400\u5B57\u6BB5\n                           FROM "
                      "\"Account \u8868\" AS \"\u8D26\u6237\"");
  EXPECT_EQ(select.from.name, "Account \u8868");
  EXPECT_EQ(select.from.alias, "\u8D26\u6237");
  EXPECT_EQ(select.projection[0].expression->qualifier, "\u8D26\u6237");
  EXPECT_EQ(select.projection[0].expression->text, "Fill\"ID");
  EXPECT_EQ(select.projection[0].alias, "\u663E\u793A\u540D");
  EXPECT_EQ(select.projection[1].expression->text, "caf\u00E9");
  EXPECT_EQ(select.projection[2].expression->text, "\U00010400\u5B57\u6BB5");
}

TEST(SqlParser, HandlesCommentsEscapesEmptyAndMultilineStrings) {
  auto select = query("-- Before SQL\nSELECT '--not a comment' AS x, '' AS empty, "
                      "'a\nb''c' AS multiline FROM t; -- After SQL\n");
  EXPECT_EQ(std::get<std::string>(select.projection[0].expression->literal), "--not a comment");
  EXPECT_EQ(std::get<std::string>(select.projection[1].expression->literal), "");
  EXPECT_EQ(std::get<std::string>(select.projection[2].expression->literal), "a\nb'c");
}

TEST(SqlParser, CreatesTableWithSixTypesAndExplicitNullability) {
  auto statement = test::take(parse_sql(R"SQL(
    CREATE TABLE FiLLs (
      account STRING NOT NULL,
      fill_id INT64,
      quantity FLOAT64 NULL,
      accepted_at TIMESTAMP_MICROS NOT NULL,
      enabled BOOL,
      embedding FLOAT_VECTOR,
      PRIMARY KEY (account, fill_id)
    ) PARTITION BY (account);
  )SQL"));
  const auto* table = std::get_if<CreateTableAst>(&statement);
  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->name, "fills");
  ASSERT_EQ(table->columns.size(), 6U);
  EXPECT_EQ(table->columns[0].type, DataType::String);
  EXPECT_EQ(table->columns[1].type, DataType::Int64);
  EXPECT_EQ(table->columns[2].type, DataType::Float64);
  EXPECT_EQ(table->columns[3].type, DataType::TimestampMicros);
  EXPECT_EQ(table->columns[4].type, DataType::Bool);
  EXPECT_EQ(table->columns[5].type, DataType::FloatVector);
  EXPECT_EQ(table->columns[0].nullable, false);
  EXPECT_EQ(table->columns[1].nullable, std::nullopt);
  EXPECT_EQ(table->columns[2].nullable, true);
  EXPECT_EQ(table->primary_key, (std::vector<std::string>{"account", "fill_id"}));
  EXPECT_EQ(table->partition_key, (std::vector<std::string>{"account"}));
}

TEST(SqlParser, KeepsMissingPartitionConfigurationForTheBinder) {
  auto statement =
      test::take(parse_sql("CREATE TABLE x (PRIMARY KEY (id), id INT64 NULL, payload STRING)"));
  const auto* table = std::get_if<CreateTableAst>(&statement);
  ASSERT_NE(table, nullptr);
  EXPECT_TRUE(table->partition_key.empty());
  EXPECT_EQ(table->columns[0].nullable, true);
  auto middle_key = parse_sql("CREATE TABLE x (id INT64, PRIMARY KEY (id), payload STRING)");
  EXPECT_TRUE(middle_key.ok()) << middle_key.status().message();
}

TEST(SqlParser, CreatesMaterializedViewWithAggregatesAndGroupBy) {
  auto statement = test::take(parse_sql(R"SQL(
    CREATE MATERIALIZED VIEW Totals AS
    SELECT account, SUM(quantity) AS volume, COUNT(*) AS rows_seen,
           COUNT(quantity) AS nonnull_rows, AVG(quantity) AS mean_quantity
    FROM fills WHERE quantity IS NOT NULL GROUP BY account;
  )SQL"));
  const auto* view = std::get_if<CreateViewAst>(&statement);
  ASSERT_NE(view, nullptr);
  EXPECT_EQ(view->name, "totals");
  const auto& select = view->query;
  ASSERT_EQ(select.projection.size(), 5U);
  EXPECT_EQ(select.projection[1].expression->text, "sum");
  EXPECT_EQ(select.projection[2].expression->text, "count");
  EXPECT_EQ(select.projection[2].expression->args[0]->kind, ExprAstKind::Star);
  EXPECT_EQ(select.projection[3].expression->args[0]->kind, ExprAstKind::Column);
  EXPECT_EQ(select.projection[4].expression->text, "avg");
  ASSERT_EQ(select.group_by.size(), 1U);
  EXPECT_EQ(select.group_by[0]->text, "account");
  EXPECT_EQ(select.where->kind, ExprAstKind::IsNull);
  EXPECT_EQ(select.where->text, "is_not_null");
}

TEST(SqlParser, ParsesInnerJoinAndOptionalAsAliases) {
  for (const std::string inner : {"", "INNER "}) {
    auto select = query("SELECT t.account, t.volume, l.ceiling FROM totals t " + inner +
                        "JOIN limits AS l ON t.account = l.account AND t.region = l.region "
                        "WHERE t.volume > l.ceiling AND NOT (l.ceiling < 0)");
    EXPECT_EQ(select.from.alias, "t");
    ASSERT_TRUE(select.join.has_value());
    EXPECT_EQ(select.join->right.name, "limits");
    EXPECT_EQ(select.join->right.alias, "l");
    EXPECT_EQ(select.join->condition->text, "and");
    EXPECT_EQ(select.join->condition->args[0]->text, "=");
    EXPECT_EQ(select.join->condition->args[0]->args[0]->qualifier, "t");
    EXPECT_EQ(select.join->condition->args[0]->args[1]->qualifier, "l");
    EXPECT_EQ(select.where->args[1]->text, "not");
  }
}

TEST(SqlParser, KeepsMultipleGroupByExpressionsInSourceOrder) {
  auto select = query("SELECT region, account, COUNT(*) AS n FROM t GROUP BY region, account");
  EXPECT_EQ(select.group_by.size(), 2U);
  EXPECT_EQ(select.group_by[0]->text, "region");
  EXPECT_EQ(select.group_by[1]->text, "account");
  EXPECT_EQ(select.where, nullptr);
}

TEST(SqlParser, AppliesArithmeticPrecedenceAndLeftAssociativity) {
  auto select = query("SELECT -a * 2 + b / +3 - 4 AS x, 10 - 3 - 2 AS y FROM t");
  expect_same_expression(select.projection[0].expression,
                         binary("-",
                                binary("+", binary("*", unary("-", column("a")), number("2")),
                                       binary("/", column("b"), unary("+", number("3")))),
                                number("4")));
  expect_same_expression(select.projection[1].expression,
                         binary("-", binary("-", number("10"), number("3")), number("2")));
}

TEST(SqlParser, AppliesNotComparisonAndOrPrecedence) {
  auto select = query("SELECT a FROM t WHERE NOT a = 1 OR b <> 2 AND c IS NOT NULL");
  expect_same_expression(
      select.where,
      binary("or", unary("not", binary("=", column("a"), number("1"))),
             binary("and", binary("<>", column("b"), number("2")), is_null(column("c"), true))));
  auto grouped = query("SELECT a FROM t WHERE NOT (a = 1 OR b = 2) AND c IS NULL");
  EXPECT_EQ(grouped.where->text, "and");
  EXPECT_EQ(grouped.where->args[0]->text, "not");
  EXPECT_EQ(grouped.where->args[0]->args[0]->text, "or");
  EXPECT_EQ(grouped.where->args[1]->text, "is_null");
}

TEST(SqlParser, PreservesComparisonOperators) {
  for (const std::string operation : {"=", "<>", "!=", "<", "<=", ">", ">="}) {
    auto select = query("SELECT a FROM t WHERE a " + operation + " b");
    EXPECT_EQ(select.where->kind, ExprAstKind::Binary);
    EXPECT_EQ(select.where->text, operation);
  }
}

TEST(SqlParser, PreservesNumberLexemesForCheckedBindingIncludingInt64Minimum) {
  auto select = query("SELECT -9223372036854775808 AS low, 18446744073709551616 AS large, "
                      "1.25e-2 AS f, .5 AS half, 2. AS two, 1E+30 AS huge FROM t");
  EXPECT_EQ(select.projection[0].expression->kind, ExprAstKind::Unary);
  EXPECT_EQ(select.projection[0].expression->args[0]->kind, ExprAstKind::Number);
  EXPECT_EQ(select.projection[0].expression->args[0]->text, "9223372036854775808");
  EXPECT_EQ(select.projection[1].expression->text, "18446744073709551616");
  EXPECT_EQ(select.projection[2].expression->text, "1.25e-2");
  EXPECT_EQ(select.projection[3].expression->text, ".5");
  EXPECT_EQ(select.projection[4].expression->text, "2.");
  EXPECT_EQ(select.projection[5].expression->text, "1E+30");
}

TEST(SqlParser, ParsesNullAndBoolLiteralsAndCastTargetTypes) {
  auto select = query("SELECT NULL AS n, TRUE AS yes, FALSE AS no, "
                      "CAST(NULL AS FLOAT64) AS f, CAST(a AS INT64) AS i FROM t");
  EXPECT_TRUE(std::holds_alternative<std::monostate>(select.projection[0].expression->literal));
  EXPECT_EQ(select.projection[0].expression->literal_type, std::nullopt);
  EXPECT_EQ(select.projection[1].expression->literal_type, DataType::Bool);
  EXPECT_TRUE(std::get<bool>(select.projection[1].expression->literal));
  EXPECT_FALSE(std::get<bool>(select.projection[2].expression->literal));
  EXPECT_EQ(select.projection[3].expression->kind, ExprAstKind::Cast);
  EXPECT_EQ(select.projection[3].expression->literal_type, DataType::Float64);
  EXPECT_TRUE(select.projection[3].expression->text.empty());
  EXPECT_EQ(select.projection[4].expression->literal_type, DataType::Int64);
}

TEST(SqlParser, ParsesQualifiedAndUnqualifiedStarsAndLeavesFunctionBindingToBinder) {
  auto select = query("SELECT *, t.*, COUNT(*) AS n, Mystery(a, 2) AS m FROM t");
  EXPECT_EQ(select.projection[0].expression->kind, ExprAstKind::Star);
  EXPECT_TRUE(select.projection[0].expression->qualifier.empty());
  EXPECT_EQ(select.projection[1].expression->kind, ExprAstKind::Star);
  EXPECT_EQ(select.projection[1].expression->qualifier, "t");
  EXPECT_EQ(select.projection[2].expression->args[0]->kind, ExprAstKind::Star);
  EXPECT_EQ(select.projection[3].expression->kind, ExprAstKind::Call);
  EXPECT_EQ(select.projection[3].expression->text, "mystery");
  EXPECT_EQ(select.projection[3].expression->args.size(), 2U);
}

TEST(SqlParser, RejectsMalformedSyntaxAndTrailingStatements) {
  for (const std::string_view sql : {"",
                                     "-- only a comment",
                                     ";",
                                     "SELECT",
                                     "SELECT a",
                                     "SELECT FROM t",
                                     "SELECT a, FROM t",
                                     "SELECT a FROM",
                                     "SELECT a FROM t; SELECT b FROM t",
                                     "SELECT a FROM t;;",
                                     "SELECT a FROM t; garbage",
                                     "SELECT a FROM t @",
                                     "SELECT 'unclosed FROM t",
                                     "SELECT \"unclosed FROM t",
                                     "SELECT \"\" FROM t",
                                     "SELECT a FROM t /* comment */",
                                     "SELECT a FROM t WHERE",
                                     "SELECT a FROM t WHERE a < b < c",
                                     "SELECT a FROM t WHERE a IS NOT",
                                     "SELECT a FROM t GROUP BY",
                                     "SELECT 1e AS x FROM t",
                                     "SELECT 0x10 AS x FROM t",
                                     "SELECT a AS FROM t",
                                     "CREATE TABLE x (a INT64)",
                                     "CREATE TABLE x (a INTEGER, PRIMARY KEY (a))",
                                     "CREATE TABLE x (a INT64, PRIMARY KEY (a), PRIMARY KEY (a))",
                                     "CREATE TABLE x (a INT64 NOT, PRIMARY KEY (a))",
                                     "SELECT CAST(a AS DECIMAL) AS x FROM t",
                                     "SELECT (a FROM t",
                                     "SELECT a) FROM t"}) {
    SCOPED_TRACE(sql);
    auto result = parse_sql(sql);
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), ErrorCode::InvalidArgument);
  }
}

TEST(SqlParser, RejectsUnsupportedClausesIncludingBareTrailingKeywords) {
  for (const std::string_view sql :
       {"SELECT DISTINCT a FROM t",
        "SELECT a FROM t HAVING a = 1",
        "SELECT a FROM t ORDER BY a",
        "SELECT a FROM t ORDER",
        "SELECT a FROM t LIMIT 1",
        "SELECT a FROM t LIMIT",
        "SELECT a FROM t OFFSET",
        "SELECT a FROM t WITH",
        "SELECT a FROM t UNION SELECT a FROM u",
        "WITH t AS (SELECT a FROM u) SELECT a FROM t",
        "SELECT SUM(a) OVER () AS s FROM t",
        "SELECT a FROM (SELECT a FROM t) AS u",
        "SELECT a FROM t LEFT JOIN u ON t.a = u.a",
        "SELECT a FROM t CROSS JOIN u",
        "SELECT a FROM t JOIN u ON t.a = u.a JOIN v ON v.a = t.a",
        "SELECT a FROM t, u",
        "SELECT a FROM t NATURAL JOIN u",
        "SELECT a FROM t JOIN u USING (a)",
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET a = 1",
        "DELETE FROM t",
        "DROP TABLE t",
        "CREATE OR REPLACE MATERIALIZED VIEW v AS SELECT a FROM t",
        "CREATE TABLE t (a INT64, PRIMARY KEY (a)) WITH (unknown = 1)"}) {
    SCOPED_TRACE(sql);
    auto result = parse_sql(sql);
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), ErrorCode::InvalidArgument);
  }
}

TEST(SqlParser, RejectsInvalidUtf8AndNulBeforeCallingAntlr) {
  for (const std::string& bytes :
       {std::string("\xff", 1), std::string("\xc0\xaf", 2), std::string("\xed\xa0\x80", 3),
        std::string("\xf4\x90\x80\x80", 4), std::string("\0", 1)}) {
    auto result = parse_sql("SELECT '" + bytes + "' AS x FROM t");
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), ErrorCode::InvalidArgument);
  }
}

TEST(SqlParser, ReportsSyntaxLocationAndRetainsExpressionSourceLocation) {
  auto select = query("-- comment\nSELECT\n  a + b AS total\nFROM t");
  EXPECT_EQ(select.projection[0].expression->line, 3U);
  EXPECT_EQ(select.projection[0].expression->column, 2U);
  auto invalid = parse_sql("SELECT a\nFROM t\nWHERE )");
  ASSERT_FALSE(invalid.ok());
  EXPECT_NE(invalid.status().message().find("3:"), std::string_view::npos);
}

TEST(SqlParser, EnforcesByteAndTokenBudgetsWithoutTruncation) {
  auto too_many_bytes = parse_sql(std::string(256 * 1024 + 1, ' '));
  EXPECT_EQ(too_many_bytes.status().code(), ErrorCode::ResourceExhausted);
  std::string many_columns = "SELECT a";
  for (std::size_t count = 0; count < 2200; ++count)
    many_columns += ",a";
  many_columns += " FROM t";
  auto too_many_tokens = parse_sql(many_columns);
  EXPECT_EQ(too_many_tokens.status().code(), ErrorCode::ResourceExhausted);
}

TEST(SqlParser, BoundsParenthesisNestingBeforeRecursiveParsing) {
  const auto nested = [](std::size_t depth) noexcept {
    return "SELECT " + std::string(depth, '(') + "a" + std::string(depth, ')') + " FROM t";
  };
  auto permitted = parse_sql(nested(64));
  EXPECT_TRUE(permitted.ok()) << permitted.status().message();
  auto rejected = parse_sql(nested(65));
  EXPECT_EQ(rejected.status().code(), ErrorCode::ResourceExhausted);
  auto harmless =
      parse_sql("SELECT '" + std::string(200, '(') + "' AS x FROM t -- " + std::string(200, '('));
  EXPECT_TRUE(harmless.ok()) << harmless.status().message();
}

TEST(SqlParser, BoundsFlatUnaryAndBinaryAstDepth) {
  auto unary_chain = parse_sql("SELECT " + std::string(128, '+') + "a AS x FROM t");
  EXPECT_EQ(unary_chain.status().code(), ErrorCode::ResourceExhausted);
  auto permitted = parse_sql("SELECT " + std::string(127, '+') + "a AS x FROM t");
  EXPECT_TRUE(permitted.ok()) << permitted.status().message();
  std::string expression = "SELECT a";
  for (std::size_t index = 0; index < 150; ++index)
    expression += " + a";
  auto binary_chain = parse_sql(expression + " AS x FROM t");
  EXPECT_EQ(binary_chain.status().code(), ErrorCode::ResourceExhausted);
  expression = "SELECT a FROM t WHERE ";
  for (std::size_t index = 0; index < 150; ++index)
    expression += "NOT ";
  auto not_chain = parse_sql(expression + "TRUE");
  EXPECT_EQ(not_chain.status().code(), ErrorCode::ResourceExhausted);
}

TEST(SqlParser, NormalizesSdkNamesAndPreservesExplicitExactNames) {
  EXPECT_EQ(test::take(sql_identifier("AcCoUnT_12")), "account_12");
  EXPECT_EQ(test::take(sql_identifier("\u8D26\u6237A")), "\u8D26\u6237a");
  EXPECT_EQ(test::take(sql_identifier("Mixed Name\"", true)), "Mixed Name\"");
  for (const std::string_view name : {"", "12account", "a-b", "a.b", "a b", "\"quoted\""}) {
    auto result = sql_identifier(name);
    EXPECT_EQ(result.status().code(), ErrorCode::InvalidArgument);
  }
  EXPECT_EQ(sql_identifier(std::string("a\0b", 3), true).status().code(),
            ErrorCode::InvalidArgument);
  EXPECT_EQ(sql_identifier(std::string("\xff", 1), true).status().code(),
            ErrorCode::InvalidArgument);
}

TEST(SqlParser, SqlAndSdkBuildersProduceTheSameExpressionStructure) {
  auto select = query("SELECT t.a + CAST(2 AS FLOAT64) AS x, COUNT(*) AS n, "
                      "t.*, TRUE AS b, 'hello' AS s FROM t WHERE t.a IS NULL");
  expect_same_expression(select.projection[0].expression,
                         binary("+", column("a", "t"), cast(number("2"), DataType::Float64)));
  expect_same_expression(select.projection[1].expression, call("count", {star()}));
  expect_same_expression(select.projection[2].expression, star("t"));
  expect_same_expression(select.projection[3].expression, literal(true, DataType::Bool));
  expect_same_expression(select.projection[4].expression,
                         literal(std::string("hello"), DataType::String));
  expect_same_expression(select.where, is_null(column("a", "t")));
}

TEST(SqlParser, SeparateCallsCanRunConcurrently) {
  std::atomic<bool> all_valid = true;
  std::vector<std::thread> threads;
  for (int worker = 0; worker < 4; ++worker) {
    threads.emplace_back([&all_valid]() noexcept {
      for (int iteration = 0; iteration < 30; ++iteration) {
        auto result = parse_sql("SELECT account, COUNT(*) AS n FROM fills GROUP BY account");
        if (!result.ok())
          all_valid = false;
      }
    });
  }
  for (auto& thread : threads)
    thread.join();
  EXPECT_TRUE(all_valid);
}

} // namespace
} // namespace eventflow
