#include "eventflow/catalog.hpp"
#include <array>
#include <cmath>
#include <gtest/gtest.h>
#include <limits>

namespace eventflow {
namespace {
constexpr auto kMin = std::numeric_limits<std::int64_t>::min();
constexpr auto kMax = std::numeric_limits<std::int64_t>::max();

BoundExprPtr constant(Value value, DataType type) noexcept {
  auto expression = std::make_shared<BoundExpr>();
  expression->type = type;
  expression->nullable = std::holds_alternative<std::monostate>(value);
  expression->literal = std::move(value);
  return expression;
}

BoundExprPtr integer(std::int64_t value) noexcept {
  return constant(value, DataType::Int64);
}

BoundExprPtr real(double value) noexcept {
  return constant(value, DataType::Float64);
}

BoundExprPtr boolean(Value value) noexcept {
  return constant(std::move(value), DataType::Bool);
}

BoundExprPtr null(DataType type) noexcept {
  return constant(std::monostate{}, type);
}

BoundExprPtr input(std::size_t slot, DataType type, bool nullable = false) noexcept {
  auto expression = std::make_shared<BoundExpr>();
  expression->kind = BoundExprKind::Input;
  expression->type = type;
  expression->nullable = nullable;
  expression->slot = slot;
  return expression;
}

BoundExprPtr operation(BoundExprKind kind, std::string op, DataType type,
                       std::vector<BoundExprPtr> args, bool nullable = true) noexcept {
  auto expression = std::make_shared<BoundExpr>();
  expression->kind = kind;
  expression->op = std::move(op);
  expression->type = type;
  expression->nullable = nullable;
  expression->args = std::move(args);
  return expression;
}

BoundExprPtr binary_expr(std::string op, BoundExprPtr left, BoundExprPtr right,
                         DataType type = DataType::Int64) noexcept {
  return operation(BoundExprKind::Binary, std::move(op), type, {std::move(left), std::move(right)});
}

void expect_value(const BoundExprPtr& expression, const Value& expected,
                  const Row& row = {}) noexcept {
  auto result = evaluate(*expression, row);
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(*result, expected);
}

void expect_invalid(const BoundExprPtr& expression, const Row& row = {}) noexcept {
  auto result = evaluate(*expression, row);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), ErrorCode::InvalidArgument);
}

RelationDescriptor base_relation() noexcept {
  RelationDescriptor relation;
  relation.id = 10;
  relation.name = "measurements";
  relation.storage_schema = {10,
                             "measurements",
                             1,
                             {{1, "item", DataType::Int64, false},
                              {2, "amount", DataType::Float64, true},
                              {3, "tenant", DataType::String, false},
                              {4, "zone", DataType::Bool, false}},
                             {"item", "zone", "tenant"},
                             {"tenant", "zone"}};
  relation.visible_count = 4;
  relation.physical_key_slots = {2, 3, 0};
  relation.partition_slots = {2, 3};
  relation.partition_visible_slots = {2, 3};
  relation.identity_types = {DataType::String, DataType::Bool, DataType::Int64};
  relation.partition_types = {DataType::String, DataType::Bool};
  return relation;
}

RelationDescriptor view_relation() noexcept {
  RelationDescriptor relation;
  relation.kind = RelationKind::SqlView;
  relation.id = 11;
  relation.name = "amounts";
  relation.storage_schema = {11,
                             "amounts",
                             1,
                             {{1, "amount", DataType::Float64, true},
                              {2, "__ef_key_0", DataType::String, false},
                              {3, "__ef_key_1", DataType::Bool, false},
                              {4, "__ef_key_2", DataType::Int64, false}},
                             {"__ef_key_0", "__ef_key_1", "__ef_key_2"},
                             {"__ef_key_0", "__ef_key_1"}};
  relation.visible_count = 1;
  relation.physical_key_slots = {1, 2, 3};
  relation.partition_slots = {1, 2};
  relation.partition_visible_slots = {kNoSlot, kNoSlot};
  relation.identity_types = {DataType::String, DataType::Bool, DataType::Int64};
  relation.partition_types = {DataType::String, DataType::Bool};
  relation.plan = std::make_shared<BoundPlan>();
  return relation;
}

TEST(Expression, EvaluatesTypedLiteral) {
  BoundExpr expression;
  expression.type = DataType::Int64;
  expression.literal = std::int64_t{7};
  auto result = evaluate(expression, {});
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(*result, Value(std::int64_t{7}));
}

TEST(Expression, InputSlotsUseVisibleCombinedAndGroupedRows) {
  expect_value(input(2, DataType::String), std::string("right"),
               {std::int64_t{10}, true, std::string("right")});
  expect_value(input(1, DataType::Int64), std::int64_t{12},
               {std::string("group"), std::int64_t{12}, 3.0});
  expect_value(input(0, DataType::Float64, true), std::monostate{}, {std::monostate{}});
  expect_invalid(input(3, DataType::Int64), {std::int64_t{1}});
  expect_invalid(input(0, DataType::Int64), {std::string("wrong")});
  expect_invalid(input(0, DataType::Int64), {std::monostate{}});
}

TEST(Expression, RejectsMalformedTypedLiteral) {
  expect_invalid(constant(std::string("not an integer"), DataType::Int64));
  expect_invalid(real(std::numeric_limits<double>::infinity()));
  expect_invalid(real(std::numeric_limits<double>::quiet_NaN()));
  expect_invalid(constant(std::vector<double>{std::numeric_limits<double>::infinity()},
                          DataType::FloatVector));
  expect_invalid(constant(std::string("\xff"), DataType::String));
}

TEST(Expression, UntypedNullSupportsCountAndCastButCannotLeakIntoProjection) {
  auto untyped = std::make_shared<BoundExpr>();
  untyped->untyped_null = true;
  expect_value(untyped, std::monostate{});
  expect_value(operation(BoundExprKind::IsNull, "is_null", DataType::Bool, {untyped}, false), true);
  expect_value(operation(BoundExprKind::Cast, "", DataType::Float64, {untyped}), std::monostate{});
  BoundPlan plan;
  plan.projection = {untyped};
  plan.output_fields = {{1, "unknown", DataType::Int64, true}};
  EXPECT_FALSE(evaluate_projection(plan, {}).ok());
  untyped->literal = std::int64_t{1};
  expect_invalid(untyped);
  untyped->literal = std::monostate{};
  untyped->kind = BoundExprKind::Input;
  expect_invalid(untyped, {std::monostate{}});
}

TEST(Expression, ImplementsCompleteSqlBooleanTruthTables) {
  const std::array<Value, 3> values = {false, true, std::monostate{}};
  const std::array<std::array<Value, 3>, 3> and_table = {
      std::array<Value, 3>{false, false, false},
      std::array<Value, 3>{false, true, std::monostate{}},
      std::array<Value, 3>{false, std::monostate{}, std::monostate{}}};
  const std::array<std::array<Value, 3>, 3> or_table = {
      std::array<Value, 3>{false, true, std::monostate{}}, std::array<Value, 3>{true, true, true},
      std::array<Value, 3>{std::monostate{}, true, std::monostate{}}};
  for (std::size_t left = 0; left < values.size(); ++left) {
    for (std::size_t right = 0; right < values.size(); ++right) {
      SCOPED_TRACE(std::to_string(left) + "," + std::to_string(right));
      expect_value(
          binary_expr("and", boolean(values[left]), boolean(values[right]), DataType::Bool),
          and_table[left][right]);
      expect_value(binary_expr("or", boolean(values[left]), boolean(values[right]), DataType::Bool),
                   or_table[left][right]);
    }
  }
  expect_value(operation(BoundExprKind::Unary, "not", DataType::Bool, {boolean(false)}), true);
  expect_value(operation(BoundExprKind::Unary, "not", DataType::Bool, {boolean(true)}), false);
  expect_value(operation(BoundExprKind::Unary, "not", DataType::Bool, {null(DataType::Bool)}),
               std::monostate{});
}

TEST(Expression, BooleanEvaluationShortCircuitsOnlyDecisiveLeftOperands) {
  auto divide = binary_expr("/", integer(1), integer(0));
  auto failing = binary_expr("=", divide, integer(0), DataType::Bool);
  expect_value(binary_expr("and", boolean(false), failing, DataType::Bool), false);
  expect_value(binary_expr("or", boolean(true), failing, DataType::Bool), true);
  for (auto left : {boolean(true), null(DataType::Bool)})
    expect_invalid(binary_expr("and", left, failing, DataType::Bool));
  for (auto left : {boolean(false), null(DataType::Bool)})
    expect_invalid(binary_expr("or", left, failing, DataType::Bool));
  expect_invalid(binary_expr("and", failing, boolean(false), DataType::Bool));
  expect_invalid(binary_expr("or", failing, boolean(true), DataType::Bool));
  expect_invalid(binary_expr("+", null(DataType::Int64), divide));
}

TEST(Expression, FilterPassesOnlyTrueAndMissingFilterPasses) {
  auto missing = passes_filter({}, {});
  ASSERT_TRUE(missing.ok());
  EXPECT_TRUE(*missing);
  for (const Value& value : {Value(false), Value(true), Value(std::monostate{})}) {
    auto result = passes_filter(boolean(value), {});
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(*result, value == Value(true));
  }
  EXPECT_FALSE(passes_filter(integer(1), {}).ok());
}

TEST(Expression, Int64ArithmeticAndUnarySigns) {
  expect_value(binary_expr("+", integer(9), integer(4)), std::int64_t{13});
  expect_value(binary_expr("-", integer(9), integer(4)), std::int64_t{5});
  expect_value(binary_expr("*", integer(-9), integer(4)), std::int64_t{-36});
  expect_value(binary_expr("/", integer(-9), integer(4)), std::int64_t{-2});
  expect_value(binary_expr("/", integer(9), integer(-4)), std::int64_t{-2});
  expect_value(binary_expr("+", integer(kMax), integer(0)), kMax);
  expect_value(binary_expr("-", integer(kMin), integer(0)), kMin);
  expect_value(operation(BoundExprKind::Unary, "+", DataType::Int64, {integer(kMin)}), kMin);
  expect_value(operation(BoundExprKind::Unary, "-", DataType::Int64, {integer(kMax)}), -kMax);
}

TEST(Expression, RejectsAllIntegerOverflowBoundariesAndDivisionByZero) {
  expect_invalid(binary_expr("+", integer(kMax), integer(1)));
  expect_invalid(binary_expr("+", integer(kMin), integer(-1)));
  expect_invalid(binary_expr("-", integer(kMin), integer(1)));
  expect_invalid(binary_expr("-", integer(kMax), integer(-1)));
  expect_invalid(binary_expr("*", integer(kMax), integer(2)));
  expect_invalid(binary_expr("*", integer(kMin), integer(-1)));
  expect_invalid(binary_expr("/", integer(kMin), integer(-1)));
  expect_invalid(binary_expr("/", integer(1), integer(0)));
  expect_invalid(operation(BoundExprKind::Unary, "-", DataType::Int64, {integer(kMin)}));
}

TEST(Expression, FloatArithmeticRejectsNonfiniteResultsAndEitherSignedZeroDivisor) {
  expect_value(binary_expr("+", real(1.25), real(2.5), DataType::Float64), 3.75);
  expect_value(binary_expr("-", real(1.25), real(2.5), DataType::Float64), -1.25);
  expect_value(binary_expr("*", real(1.25), real(2.5), DataType::Float64), 3.125);
  expect_value(binary_expr("/", real(9), real(4), DataType::Float64), 2.25);
  expect_value(operation(BoundExprKind::Unary, "-", DataType::Float64, {real(1.5)}), -1.5);
  expect_invalid(
      binary_expr("*", real(std::numeric_limits<double>::max()), real(2), DataType::Float64));
  expect_invalid(binary_expr("/", real(1), real(0.0), DataType::Float64));
  expect_invalid(binary_expr("/", real(1), real(-0.0), DataType::Float64));
  const auto maximum = std::numeric_limits<double>::max();
  expect_invalid(binary_expr("+", real(maximum), real(maximum), DataType::Float64));
  expect_invalid(binary_expr("-", real(-maximum), real(maximum), DataType::Float64));
  expect_invalid(binary_expr("/", real(maximum), real(0.5), DataType::Float64));
}

TEST(Expression, NullArithmeticAndComparisonReturnNull) {
  for (const auto* op : {"+", "-", "*", "/"}) {
    expect_value(binary_expr(op, null(DataType::Int64), integer(1)), std::monostate{});
    expect_value(binary_expr(op, real(1), null(DataType::Float64), DataType::Float64),
                 std::monostate{});
  }
  for (const auto* op : {"=", "!=", "<>", "<", "<=", ">", ">="})
    expect_value(binary_expr(op, integer(1), null(DataType::Int64), DataType::Bool),
                 std::monostate{});
  expect_value(operation(BoundExprKind::Unary, "-", DataType::Int64, {null(DataType::Int64)}),
               std::monostate{});
}

TEST(Expression, ComparisonsPreserveTypesAndUtf8ByteOrdering) {
  const std::array<const char*, 7> operators = {"=", "!=", "<>", "<", "<=", ">", ">="};
  const std::array<bool, 7> expected = {false, true, true, true, true, false, false};
  for (std::size_t i = 0; i < operators.size(); ++i) {
    expect_value(binary_expr(operators[i], integer(kMin), integer(kMax), DataType::Bool),
                 expected[i]);
    expect_value(binary_expr(operators[i], real(-2.5), real(0.25), DataType::Bool), expected[i]);
    expect_value(binary_expr(operators[i], constant(std::string("z"), DataType::String),
                             constant(std::string("\u4E2D"), DataType::String), DataType::Bool),
                 expected[i]);
    expect_value(binary_expr(operators[i], constant(std::int64_t{100}, DataType::TimestampMicros),
                             constant(std::int64_t{200}, DataType::TimestampMicros),
                             DataType::Bool),
                 expected[i]);
  }
  expect_value(binary_expr("=", boolean(true), boolean(true), DataType::Bool), true);
  expect_value(binary_expr("<>", boolean(true), boolean(false), DataType::Bool), true);
  expect_value(binary_expr("=", real(-0.0), real(0.0), DataType::Bool), true);
}

TEST(Expression, RejectsImplicitNumericPromotionAndUnsupportedTypeOperations) {
  expect_invalid(binary_expr("+", integer(1), real(1)));
  expect_invalid(binary_expr("=", integer(1), real(1), DataType::Bool));
  expect_invalid(binary_expr("=", integer(1), constant(std::int64_t{1}, DataType::TimestampMicros),
                             DataType::Bool));
  expect_invalid(binary_expr("<", boolean(false), boolean(true), DataType::Bool));
  expect_invalid(binary_expr("+", constant(std::int64_t{1}, DataType::TimestampMicros),
                             constant(std::int64_t{2}, DataType::TimestampMicros),
                             DataType::TimestampMicros));
  auto vector = constant(std::vector<double>{1, 2}, DataType::FloatVector);
  expect_invalid(binary_expr("=", vector, vector, DataType::Bool));
  expect_invalid(binary_expr("+", vector, vector, DataType::FloatVector));
  expect_invalid(binary_expr("and", integer(1), boolean(true), DataType::Bool));
}

TEST(Expression, IsNullSupportsEveryTypeIncludingVectors) {
  const std::array<BoundExprPtr, 6> present = {
      boolean(false),
      integer(0),
      real(0),
      constant(std::string(), DataType::String),
      constant(std::int64_t{0}, DataType::TimestampMicros),
      constant(std::vector<double>{}, DataType::FloatVector)};
  for (const auto& value : present) {
    expect_value(operation(BoundExprKind::IsNull, "is_null", DataType::Bool, {value}, false),
                 false);
    expect_value(operation(BoundExprKind::IsNull, "is_not_null", DataType::Bool, {value}, false),
                 true);
    expect_value(
        operation(BoundExprKind::IsNull, "is_null", DataType::Bool, {null(value->type)}, false),
        true);
  }
}

TEST(Expression, ExplicitFloatCastRetainsOnlyExactlyRepresentableIntegers) {
  for (const auto value : {std::int64_t{0}, std::int64_t{1}, kMin, std::int64_t{1} << 53,
                           (std::int64_t{1} << 53) + 2, -(std::int64_t{1} << 60)})
    expect_value(operation(BoundExprKind::Cast, "", DataType::Float64, {integer(value)}),
                 static_cast<double>(value));
  for (const auto value : {kMax, (std::int64_t{1} << 53) + 1, -(std::int64_t{1} << 53) - 1})
    expect_invalid(operation(BoundExprKind::Cast, "", DataType::Float64, {integer(value)}));
  expect_value(operation(BoundExprKind::Cast, "", DataType::Float64, {real(1.25)}), 1.25);
  expect_value(operation(BoundExprKind::Cast, "", DataType::Float64, {null(DataType::Int64)}),
               std::monostate{});
  expect_invalid(operation(BoundExprKind::Cast, "", DataType::Int64, {real(1.25)}));
  expect_invalid(operation(BoundExprKind::Cast, "", DataType::Float64,
                           {constant(std::int64_t{1}, DataType::TimestampMicros)}));
  expect_invalid(operation(BoundExprKind::Cast, "", DataType::Float64,
                           {constant(std::string("1"), DataType::String)}));
}

TEST(Expression, RejectsMalformedExpressionShapeAndResultType) {
  expect_invalid(operation(BoundExprKind::Binary, "+", DataType::Int64, {integer(1)}));
  expect_invalid(operation(BoundExprKind::Binary, "+", DataType::Int64, {integer(1), {}}));
  expect_invalid(operation(BoundExprKind::Unary, "-", DataType::Int64, {}));
  expect_invalid(
      operation(BoundExprKind::Binary, "unsupported", DataType::Int64, {integer(1), integer(2)}));
  expect_invalid(binary_expr("+", integer(1), integer(2), DataType::Float64));
  expect_invalid(binary_expr("=", integer(1), integer(2), DataType::Int64));
  expect_invalid(operation(BoundExprKind::IsNull, "unknown", DataType::Bool, {integer(1)}));
  expect_invalid(operation(BoundExprKind::IsNull, "is_null", DataType::Int64, {integer(1)}));
  expect_invalid(operation(BoundExprKind::Binary, "+", DataType::Int64,
                           {null(DataType::Int64), integer(1)}, false));
  expect_invalid(operation(static_cast<BoundExprKind>(100), "", DataType::Int64, {}));
  expect_invalid(constant(std::int64_t{1}, static_cast<DataType>(100)));
}

TEST(Expression, ProjectionPreservesNullVectorsAndDoesNotRequireVisibleKeys) {
  BoundPlan plan;
  plan.projection = {input(2, DataType::FloatVector), null(DataType::Float64),
                     binary_expr("*", input(1, DataType::Int64), integer(2))};
  plan.output_fields = {{1, "embedding", DataType::FloatVector, false},
                        {2, "missing", DataType::Float64, true},
                        {3, "twice", DataType::Int64, true}};
  auto projected =
      evaluate_projection(plan, {std::string("key"), std::int64_t{3}, std::vector<double>{1, 2}});
  ASSERT_TRUE(projected.ok()) << projected.status().message();
  EXPECT_EQ(*projected, (Row{std::vector<double>{1, 2}, std::monostate{}, std::int64_t{6}}));
  plan.projection.push_back({});
  EXPECT_FALSE(evaluate_projection(plan, {}).ok());
}

TEST(Expression, ProjectionRejectsWrongSchemaAndPropagatesFailure) {
  BoundPlan plan;
  plan.projection = {integer(3)};
  plan.output_fields = {{1, "result", DataType::String, false}};
  EXPECT_FALSE(evaluate_projection(plan, {}).ok());
  plan.output_fields = {{1, "result", DataType::Int64, false}};
  plan.projection = {binary_expr("/", integer(1), integer(0))};
  EXPECT_FALSE(evaluate_projection(plan, {}).ok());
}

TEST(RelationRows, BaseLayoutNormalizesPartitionPrefixWithoutChangingVisibleOrder) {
  const auto relation = base_relation();
  const Row row = {std::int64_t{42}, 7.5, std::string("tenant-a"), true};
  const Row identity = {std::string("tenant-a"), true, std::int64_t{42}};
  auto stored = make_stored_row(relation, row, identity);
  ASSERT_TRUE(stored.ok()) << stored.status().message();
  EXPECT_EQ(*stored, row);
  auto visible = visible_row(relation, *stored);
  ASSERT_TRUE(visible.ok());
  EXPECT_EQ(*visible, row);
  auto physical = physical_identity(relation, *stored);
  ASSERT_TRUE(physical.ok());
  EXPECT_EQ(*physical, identity);
  auto partition = partition_values(relation, *stored);
  ASSERT_TRUE(partition.ok());
  EXPECT_EQ(*partition, (Row{std::string("tenant-a"), true}));
}

TEST(RelationRows, BaseRejectsMismatchedOrMalformedIdentityAndIncompleteRows) {
  const auto relation = base_relation();
  const Row row = {std::int64_t{42}, std::monostate{}, std::string("tenant-a"), true};
  EXPECT_FALSE(
      make_stored_row(relation, row, {std::string("tenant-a"), true, std::int64_t{43}}).ok());
  EXPECT_FALSE(make_stored_row(relation, row, {std::string("tenant-a"), true}).ok());
  EXPECT_FALSE(make_stored_row(relation, row, {std::string("tenant-a"), true, 42.0}).ok());
  EXPECT_FALSE(
      make_stored_row(relation, {7.5}, {std::string("tenant-a"), true, std::int64_t{42}}).ok());
  EXPECT_FALSE(visible_row(relation, {std::int64_t{42}}).ok());
  EXPECT_FALSE(physical_identity(relation, {std::int64_t{42}}).ok());
  EXPECT_FALSE(partition_values(relation, {std::int64_t{42}}).ok());
}

TEST(RelationRows, ViewAppendsHiddenIdentityAndRetainsDuplicateVisibleRows) {
  const auto relation = view_relation();
  const Row first_key = {std::string("a"), false, std::int64_t{1}};
  const Row second_key = {std::string("a"), false, std::int64_t{2}};
  auto first = make_stored_row(relation, {std::monostate{}}, first_key);
  auto second = make_stored_row(relation, {std::monostate{}}, second_key);
  ASSERT_TRUE(first.ok()) << first.status().message();
  ASSERT_TRUE(second.ok());
  EXPECT_NE(*first, *second);
  auto first_visible = visible_row(relation, *first);
  auto second_visible = visible_row(relation, *second);
  ASSERT_TRUE(first_visible.ok());
  ASSERT_TRUE(second_visible.ok());
  EXPECT_EQ(*first_visible, *second_visible);
  EXPECT_EQ(first_visible->size(), 1U);
  auto identity = physical_identity(relation, *first);
  ASSERT_TRUE(identity.ok());
  EXPECT_EQ(*identity, first_key);
  auto partition = partition_values(relation, *first);
  ASSERT_TRUE(partition.ok());
  EXPECT_EQ(*partition, (Row{std::string("a"), false}));
}

TEST(RelationRows, ViewRejectsNullKeysWrongTypesAndStorageShape) {
  const auto relation = view_relation();
  EXPECT_FALSE(make_stored_row(relation, {0.5}, {std::monostate{}, false, std::int64_t{1}}).ok());
  EXPECT_FALSE(make_stored_row(relation, {0.5}, {std::string("a"), false, 1.0}).ok());
  EXPECT_FALSE(
      make_stored_row(relation, {0.5, 1.0}, {std::string("a"), false, std::int64_t{1}}).ok());
  EXPECT_FALSE(
      make_stored_row(relation, {std::string("wrong")}, {std::string("a"), false, std::int64_t{1}})
          .ok());
}

TEST(RelationRows, VisiblePartitionProjectionMustAgreeWithHiddenIdentity) {
  auto relation = view_relation();
  relation.storage_schema.fields.insert(relation.storage_schema.fields.begin(),
                                        Field{5, "tenant", DataType::String, false});
  relation.visible_count = 2;
  relation.physical_key_slots = {2, 3, 4};
  relation.partition_slots = {2, 3};
  relation.partition_visible_slots = {0, kNoSlot};
  auto stored =
      make_stored_row(relation, {std::string("a"), 0.5}, {std::string("a"), true, std::int64_t{3}});
  ASSERT_TRUE(stored.ok()) << stored.status().message();
  EXPECT_FALSE(
      make_stored_row(relation, {std::string("b"), 0.5}, {std::string("a"), true, std::int64_t{3}})
          .ok());
  (*stored)[0] = std::string("b");
  EXPECT_FALSE(visible_row(relation, *stored).ok());
  EXPECT_FALSE(physical_identity(relation, *stored).ok());
  EXPECT_FALSE(partition_values(relation, *stored).ok());
}

TEST(RelationRows, RejectsMalformedSlotMetadataAndNonPrefixPartitionKeys) {
  const Row row = {1.0, std::string("a"), true, std::int64_t{1}};
  auto relation = view_relation();
  relation.visible_count = 5;
  EXPECT_FALSE(visible_row(relation, row).ok());
  relation = view_relation();
  relation.physical_key_slots = {1, 2, 9};
  EXPECT_FALSE(physical_identity(relation, row).ok());
  relation = view_relation();
  relation.physical_key_slots = {3, 1, 2};
  EXPECT_FALSE(physical_identity(relation, row).ok());
  relation = view_relation();
  relation.partition_slots = {2, 1};
  EXPECT_FALSE(partition_values(relation, row).ok());
  relation = view_relation();
  relation.identity_types[2] = DataType::Float64;
  EXPECT_FALSE(physical_identity(relation, row).ok());
  relation = view_relation();
  relation.partition_types[0] = DataType::Int64;
  EXPECT_FALSE(partition_values(relation, row).ok());
  relation = view_relation();
  relation.partition_visible_slots[0] = 7;
  EXPECT_FALSE(visible_row(relation, row).ok());
  relation = view_relation();
  relation.partition_slots.pop_back();
  EXPECT_FALSE(partition_values(relation, row).ok());
  relation = view_relation();
  relation.identity_types.pop_back();
  EXPECT_FALSE(physical_identity(relation, row).ok());
  relation = view_relation();
  relation.visible_count = 0;
  EXPECT_FALSE(visible_row(relation, row).ok());
  relation = view_relation();
  relation.visible_count = 2;
  EXPECT_FALSE(visible_row(relation, row).ok());
  relation = view_relation();
  relation.plan.reset();
  EXPECT_FALSE(visible_row(relation, row).ok());
  relation = view_relation();
  relation.storage_schema.fields.back().name = "user_key";
  relation.storage_schema.primary_key.back() = "user_key";
  EXPECT_FALSE(visible_row(relation, row).ok());
}
} // namespace
} // namespace eventflow
