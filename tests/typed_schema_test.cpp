#include "eventflow/typed_schema.hpp"
#include <algorithm>
#include <gtest/gtest.h>
#include <limits>

namespace eventflow {
namespace typed_schema_test {

struct Observation {
  std::string ticker;
  std::int64_t time_us = 0;
  bool ready = false;
  double score = 0;
  std::vector<double> embedding;
  std::optional<double> previous_score;
};

struct DuplicateNames {
  double first = 0;
  double second = 0;
};

struct WithoutTraits {
  double value = 0;
};

} // namespace typed_schema_test

template <> struct SchemaTraits<typed_schema_test::Observation> {
  static inline std::size_t declaration_calls = 0;

  static auto fields() noexcept {
    ++declaration_calls;
    using T = typed_schema_test::Observation;
    return std::tuple{
        field<&T::ticker>("ticker"),       field<&T::time_us>("time_us"),
        field<&T::ready>("ready"),         field<&T::score>("score"),
        field<&T::embedding>("embedding"), field<&T::previous_score>("previous_score")};
  }
};

template <> struct SchemaTraits<typed_schema_test::DuplicateNames> {
  static auto fields() noexcept {
    using T = typed_schema_test::DuplicateNames;
    return std::tuple{field<&T::first>("value"), field<&T::second>("value")};
  }
};

namespace typed_schema_test {
namespace {

static_assert(TypedSchema<Observation>);
static_assert(!TypedSchema<WithoutTraits>);
static_assert(noexcept(typed_fields<Observation>()));
static_assert(noexcept(encode_typed(Observation{})));
static_assert(noexcept(decode_typed<Observation>(Row{})));

TEST(TypedSchemaTest, ExplicitMemberMappingDescribesFieldsAndRoundTripsAllSupportedValues) {
  auto fields = typed_fields<Observation>();
  ASSERT_TRUE(fields.ok());
  ASSERT_EQ(fields->size(), 6U);
  EXPECT_EQ((*fields)[0].name, "ticker");
  EXPECT_EQ((*fields)[4].type, DataType::FloatVector);
  EXPECT_TRUE((*fields)[5].nullable);
  const Observation input{.ticker = "ABC",
                          .time_us = 7,
                          .ready = true,
                          .score = 0.8,
                          .embedding = {1.0, 2.0},
                          .previous_score = 0.5};
  auto encoded = encode_typed(input);
  ASSERT_TRUE(encoded.ok());
  auto decoded = decode_typed<Observation>(*encoded);
  ASSERT_TRUE(decoded.ok());
  EXPECT_EQ(decoded->ticker, input.ticker);
  EXPECT_EQ(decoded->time_us, input.time_us);
  EXPECT_EQ(decoded->ready, input.ready);
  EXPECT_EQ(decoded->score, input.score);
  EXPECT_EQ(decoded->embedding, input.embedding);
  EXPECT_EQ(decoded->previous_score, input.previous_score);
}

TEST(TypedSchemaTest, OptionalNullRoundTripsAndRequiredNullIsRejected) {
  auto encoded = encode_typed(Observation{.ticker = "ABC", .embedding = {}, .previous_score = {}});
  ASSERT_TRUE(encoded.ok());
  EXPECT_TRUE(std::holds_alternative<std::monostate>((*encoded)[5]));
  auto decoded = decode_typed<Observation>(*encoded);
  ASSERT_TRUE(decoded.ok());
  EXPECT_FALSE(decoded->previous_score);
  (*encoded)[0] = std::monostate{};
  EXPECT_FALSE(decode_typed<Observation>(*encoded).ok());
}

TEST(TypedSchemaTest, NamedBindingReordersFieldsAndValidatesExtraPayload) {
  auto fields = typed_fields<Observation>();
  ASSERT_TRUE(fields.ok());
  std::reverse(fields->begin(), fields->end());
  fields->push_back({.id = 99, .name = "extra", .type = DataType::Float64});
  auto bound = bind_typed_schema<Observation>(*fields);
  ASSERT_TRUE(bound.ok());
  Row input{std::monostate{}, std::vector<double>{1.0}, 0.8, true,
            std::int64_t(7),  std::string("ABC"),       5.0};
  auto decoded = bound->decode(input);
  ASSERT_TRUE(decoded.ok());
  EXPECT_EQ(decoded->ticker, "ABC");
  EXPECT_EQ(decoded->score, 0.8);
  input.back() = std::numeric_limits<double>::quiet_NaN();
  EXPECT_FALSE(bound->decode(input).ok());
  input.back() = std::string("wrong");
  EXPECT_FALSE(bound->decode(input).ok());
  input.pop_back();
  EXPECT_FALSE(bound->decode(input).ok());
}

TEST(TypedSchemaTest, BindingRejectsMissingWrongAndUnexpectedlyNullableFields) {
  auto fields = typed_fields<Observation>();
  ASSERT_TRUE(fields.ok());
  auto missing = *fields;
  missing.erase(missing.begin());
  EXPECT_FALSE(bind_typed_schema<Observation>(missing).ok());
  auto wrong = *fields;
  wrong[0].type = DataType::Float64;
  EXPECT_FALSE(bind_typed_schema<Observation>(wrong).ok());
  auto nullable = *fields;
  nullable[0].nullable = true;
  EXPECT_FALSE(bind_typed_schema<Observation>(nullable).ok());
  auto required_optional = *fields;
  required_optional[5].nullable = false;
  EXPECT_TRUE(bind_typed_schema<Observation>(required_optional).ok());
  auto timestamp = *fields;
  timestamp[1].type = DataType::TimestampMicros;
  EXPECT_TRUE(bind_typed_schema<Observation>(timestamp).ok());
}

TEST(TypedSchemaTest, BoundDecodingDoesNotRebindNamesOnEachEvent) {
  auto fields = typed_fields<Observation>();
  ASSERT_TRUE(fields.ok());
  auto binding = bind_typed_schema<Observation>(*fields);
  ASSERT_TRUE(binding.ok());
  const auto calls = SchemaTraits<Observation>::declaration_calls;
  const Row row{std::string("ABC"),    std::int64_t(7), true, 0.8,
                std::vector<double>{}, std::monostate{}};
  EXPECT_TRUE(binding->decode(row).ok());
  EXPECT_TRUE(binding->decode(row).ok());
  EXPECT_EQ(SchemaTraits<Observation>::declaration_calls, calls);
}

TEST(TypedSchemaTest, DuplicateMappingsAndInvalidValuesReturnStatus) {
  EXPECT_FALSE(typed_fields<DuplicateNames>().ok());
  auto input = Observation{.ticker = "ABC", .embedding = {}, .previous_score = {}};
  input.score = std::numeric_limits<double>::infinity();
  EXPECT_FALSE(encode_typed(input).ok());
  input.score = 0.0;
  input.embedding = {std::numeric_limits<double>::quiet_NaN()};
  EXPECT_FALSE(encode_typed(input).ok());
  input.embedding = {};
  input.ticker = std::string("\xff", 1);
  EXPECT_FALSE(encode_typed(input).ok());
}

} // namespace
} // namespace typed_schema_test
} // namespace eventflow
