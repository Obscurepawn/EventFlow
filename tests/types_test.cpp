#include <gtest/gtest.h>

#include <array>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <set>
#include <string>
#include <utility>

#include "eventflow/error.hpp"
#include "eventflow/key_codec.hpp"
#include "eventflow/types.hpp"

namespace eventflow {
namespace {

Schema example_schema() noexcept {
  return {17,
          "example",
          3,
          {{1, "active", DataType::Bool},
           {2, "id", DataType::Int64},
           {3, "price", DataType::Float64},
           {4, "symbol", DataType::String},
           {5, "time", DataType::TimestampMicros},
           {6, "embedding", DataType::FloatVector},
           {7, "note", DataType::String, true}},
          {"symbol", "id"},
          {"symbol"}};
}

Json example_json() noexcept {
  return {{"active", true},  {"id", -42},      {"price", 1.5},
          {"symbol", "ABC"}, {"time", 123456}, {"embedding", {1.0, -2.5, 0.0}},
          {"note", nullptr}};
}

template <class T> T take(StatusOr<T> result) noexcept {
  if (!result.ok()) {
    ADD_FAILURE() << result.status().message();
    std::abort();
  }
  return std::move(result).value();
}

template <class F> void expect_invalid(F&& fn) noexcept {
  auto result = fn();
  EXPECT_FALSE(result.ok());
  if constexpr (std::is_same_v<decltype(result), Status>) {
    EXPECT_EQ(result.code(), ErrorCode::InvalidArgument);
  } else {
    EXPECT_EQ(result.status().code(), ErrorCode::InvalidArgument);
  }
}

static_assert(noexcept(valid_utf8(std::string_view{})));
static_assert(noexcept(parse_json_exact(std::string_view{})));
static_assert(noexcept(std::declval<const Schema&>().validate_definition()));
static_assert(noexcept(std::declval<const Schema&>().from_json(std::declval<const Json&>())));
static_assert(noexcept(encode_row(std::declval<const Schema&>(), std::declval<const Row&>())));
static_assert(noexcept(decode_row(std::declval<const Schema&>(),
                                  std::declval<const std::string&>())));
static_assert(noexcept(encode_key_parts(std::declval<const Row&>())));
static_assert(noexcept(stable_hash(std::string_view{})));
static_assert(noexcept(hex_decode(std::string_view{})));
static_assert(noexcept(std::declval<const Schema&>().index_of(std::declval<const std::string&>())));
static_assert(noexcept(std::declval<const Schema&>().validate(std::declval<const Row&>())));
static_assert(noexcept(std::declval<const Schema&>().to_json(std::declval<const Row&>())));
static_assert(noexcept(std::declval<const Schema&>().project(
    std::declval<const Row&>(), std::declval<const std::vector<std::string>&>())));
static_assert(noexcept(data_type_name(DataType::Bool)));
static_assert(noexcept(parse_data_type(std::declval<const std::string&>())));
static_assert(noexcept(value_to_json(std::declval<const Value&>())));
static_assert(noexcept(value_from_json(std::declval<const Json&>(), DataType::Bool, false)));
static_assert(noexcept(schema_to_json(std::declval<const Schema&>())));
static_assert(noexcept(schema_from_json(std::declval<const Json&>())));
static_assert(noexcept(shard_for(std::declval<const Row&>(), 1)));
static_assert(noexcept(data_prefix(0)));
static_assert(noexcept(table_prefix(0, 'T', 0)));
static_assert(noexcept(row_key(0, 'T', 0, std::declval<const Row&>())));
static_assert(noexcept(key_in_shard(0, std::string_view{})));
static_assert(noexcept(prefix_end(std::string_view{})));
static_assert(noexcept(hex_encode(std::string_view{})));

TEST(Values, Utf8ValidationRejectsMalformedAndAcceptsBoundaries) {
  for (const auto& text :
       {std::string{}, std::string("A\0B", 3), std::string("\u4E2D\u6587"), std::string("\xc2\x80"),
        std::string("\xed\x9f\xbf"), std::string("\xf4\x8f\xbf\xbf")}) {
    EXPECT_TRUE(valid_utf8(text));
    EXPECT_TRUE(value_from_json(Json(text), DataType::String, false).ok());
  }
  for (const auto& text :
       {std::string("\x80"), std::string("\xc0\x80"), std::string("\xc2"),
        std::string("\xe0\x80\x80"), std::string("\xed\xa0\x80"), std::string("\xf0\x80\x80\x80"),
        std::string("\xf4\x90\x80\x80"), std::string("\xf5\x80\x80\x80")}) {
    EXPECT_FALSE(valid_utf8(text));
    expect_invalid([&]() noexcept { return value_from_json(Json(text), DataType::String, false); });
    expect_invalid([&]() noexcept { return value_to_json(Value{text}); });
    auto schema = example_schema();
    schema.name = text;
    expect_invalid([&]() noexcept { return schema.validate_definition(); });
    schema = example_schema();
    schema.fields[0].name = text;
    expect_invalid([&]() noexcept { return schema.validate_definition(); });
  }
}

TEST(JsonParsing, IntegerLexemesNeverFallBackToRoundedDouble) {
  for (const auto* literal :
       {"18446744073709551616", "18446744073709551617", "-9223372036854775809",
        "-18446744073709551617", "999999999999999999999999999999999999999999999999"}) {
    expect_invalid([&]() noexcept { return parse_json_exact(literal); });
    expect_invalid(
        [&]() noexcept { return parse_json_exact(std::string("{\"n\":") + literal + "}"); });
    expect_invalid([&]() noexcept { return parse_json_exact(std::string("[[") + literal + "]]"); });
  }
  EXPECT_EQ(take(parse_json_exact("-9223372036854775808")).get<std::int64_t>(),
            std::numeric_limits<std::int64_t>::min());
  EXPECT_EQ(take(parse_json_exact("18446744073709551615")).get<std::uint64_t>(),
            std::numeric_limits<std::uint64_t>::max());
  EXPECT_EQ(take(parse_json_exact("9007199254740993")).get<std::uint64_t>(), 9007199254740993ULL);
}

TEST(RowCodec, RawIntegerOverflowIsRejectedForScalarsAndVectors) {
  for (const auto type : {DataType::Float64, DataType::FloatVector}) {
    const Schema schema{1,      "probe", 1, {{1, "id", DataType::Int64}, {2, "number", type}},
                        {"id"}, {"id"}};
    for (const auto* literal : {"9007199254740993", "18446744073709551615", "18446744073709551617",
                                "-9223372036854775809"}) {
      const auto value =
          type == DataType::FloatVector ? std::string("[") + literal + "]" : std::string(literal);
      const auto encoded =
          std::string("{\"format\":1,\"schema_id\":1,\"schema_version\":1,\"values\":[0,") + value +
          "]}";
      expect_invalid([&]() noexcept { return decode_row(schema, encoded); });
    }
  }
}

TEST(JsonParsing, ExplicitFloatsAndDigitsInsideEscapedStringsRemainValid) {
  for (const auto* literal :
       {"18446744073709551617.0", "-9223372036854775809.0", "1.8446744073709551617e19",
        "-9.223372036854775809E+18", "1e-9", "1E+20"}) {
    const auto json = take(parse_json_exact(literal));
    EXPECT_TRUE(json.is_number_float());
    EXPECT_TRUE(value_from_json(json, DataType::Float64, false).ok());
  }
  const auto input =
      R"({"18446744073709551617":"-9223372036854775809", "escaped":"\"18446744073709551617\\", "n":1})";
  const auto json = take(parse_json_exact(input));
  EXPECT_EQ(json["18446744073709551617"], "-9223372036854775809");
  EXPECT_EQ(json["escaped"], "\"18446744073709551617\\");
  EXPECT_EQ(json["n"], 1);
  expect_invalid(
      []() noexcept { return parse_json_exact(R"(["escaped\"", 18446744073709551617])"); });
  for (const auto* invalid :
       {"", "01", "--1", "1e", "[1,]", "1e9999", "{\"n\":}", "\"unterminated"}) {
    expect_invalid([&]() noexcept { return parse_json_exact(invalid); });
  }
}

TEST(Schema, ObjectRoundTripPreservesTypesAndNullableFields) {
  const auto schema = example_schema();
  const auto row = take(schema.from_json(example_json()));
  ASSERT_EQ(row.size(), 7u);
  EXPECT_TRUE(std::get<bool>(row[0]));
  EXPECT_EQ(std::get<std::int64_t>(row[1]), -42);
  EXPECT_EQ(std::get<double>(row[2]), 1.5);
  EXPECT_EQ(std::get<std::string>(row[3]), "ABC");
  EXPECT_EQ(std::get<std::int64_t>(row[4]), 123456);
  EXPECT_EQ(std::get<std::vector<double>>(row[5]), (std::vector<double>{1.0, -2.5, 0.0}));
  EXPECT_TRUE(std::holds_alternative<std::monostate>(row[6]));
  EXPECT_EQ(take(schema.to_json(row)), example_json());
  auto missing_nullable = example_json();
  missing_nullable.erase("note");
  EXPECT_EQ(take(schema.from_json(missing_nullable)), row);
  EXPECT_EQ(take(schema.project(row, {"id", "symbol"})),
            (Row{std::int64_t{-42}, std::string{"ABC"}}));
  expect_invalid([&]() noexcept { return schema.project(row, {"missing"}); });
}

TEST(Schema, InvalidDefinitionsAreRejected) {
  auto schema = example_schema();
  EXPECT_TRUE(schema.validate_definition().ok());
  schema.fields.clear();
  expect_invalid([&]() noexcept { return schema.validate_definition(); });
  schema = example_schema();
  schema.fields[1].id = schema.fields[0].id;
  expect_invalid([&]() noexcept { return schema.validate_definition(); });
  schema = example_schema();
  schema.fields[1].name = schema.fields[0].name;
  expect_invalid([&]() noexcept { return schema.validate_definition(); });
  schema = example_schema();
  schema.fields[1].name.clear();
  expect_invalid([&]() noexcept { return schema.validate_definition(); });
  schema = example_schema();
  schema.fields[1].type = static_cast<DataType>(999);
  expect_invalid([&]() noexcept { return schema.validate_definition(); });
}

TEST(Schema, KeysAreNonemptyUniqueNonnullableAndContainPartition) {
  for (const auto& invalid_key : {std::vector<std::string>{}, {"missing"}, {"id", "id"}}) {
    auto schema = example_schema();
    schema.primary_key = invalid_key;
    expect_invalid([&]() noexcept { return schema.validate_definition(); });
    schema = example_schema();
    schema.partition_key = invalid_key;
    expect_invalid([&]() noexcept { return schema.validate_definition(); });
  }
  auto schema = example_schema();
  schema.partition_key = {"time"};
  expect_invalid([&]() noexcept { return schema.validate_definition(); });
  for (const auto type : {DataType::Float64, DataType::FloatVector}) {
    schema = example_schema();
    schema.fields[1].type = type;
    expect_invalid([&]() noexcept { return schema.validate_definition(); });
  }
  schema = example_schema();
  schema.fields[1].nullable = true;
  expect_invalid([&]() noexcept { return schema.validate_definition(); });
  schema = example_schema();
  schema.primary_key = {"active", "time", "symbol"};
  EXPECT_TRUE(schema.validate_definition().ok());
}

TEST(Schema, JsonBoundaryRejectsUnknownMissingAndWrongTypes) {
  const auto schema = example_schema();
  auto object = example_json();
  object["unknown"] = 1;
  expect_invalid([&]() noexcept { return schema.from_json(object); });
  object = example_json();
  object.erase("id");
  expect_invalid([&]() noexcept { return schema.from_json(object); });
  object = example_json();
  object["id"] = nullptr;
  expect_invalid([&]() noexcept { return schema.from_json(object); });
  expect_invalid([&]() noexcept { return schema.from_json(Json::array()); });
  const std::array<std::pair<const char*, Json>, 6> wrong_types = {
      {{"active", 1},
       {"id", 1.0},
       {"price", "1.0"},
       {"symbol", true},
       {"time", 12.0},
       {"embedding", Json::array({true})}}};
  for (const auto& [name, value] : wrong_types) {
    object = example_json();
    object[name] = value;
    expect_invalid([&]() noexcept { return schema.from_json(object); });
  }
}

TEST(Schema, InternalRowsAreValidatedBeforeProjectionOrSerialization) {
  const auto schema = example_schema();
  auto row = take(schema.from_json(example_json()));
  row.pop_back();
  expect_invalid([&]() noexcept { return schema.validate(row); });
  expect_invalid([&]() noexcept { return schema.project(row, {"id"}); });
  expect_invalid([&]() noexcept { return schema.to_json(row); });
  row = take(schema.from_json(example_json()));
  row[1] = 42.0;
  expect_invalid([&]() noexcept { return schema.validate(row); });
  row[1] = std::monostate{};
  expect_invalid([&]() noexcept { return schema.validate(row); });
  row = take(schema.from_json(example_json()));
  row[2] = std::int64_t{5};
  expect_invalid([&]() noexcept { return schema.validate(row); });
}

TEST(Values, Int64AndTimestampBoundsAreChecked) {
  constexpr auto lo = std::numeric_limits<std::int64_t>::min();
  constexpr auto hi = std::numeric_limits<std::int64_t>::max();
  for (const auto type : {DataType::Int64, DataType::TimestampMicros}) {
    EXPECT_EQ(std::get<std::int64_t>(take(value_from_json(Json(lo), type, false))), lo);
    EXPECT_EQ(std::get<std::int64_t>(take(value_from_json(Json(hi), type, false))), hi);
    EXPECT_EQ(std::get<std::int64_t>(take(value_from_json(Json(std::uint64_t{42}), type, false))),
              42);
    expect_invalid(
        [&]() noexcept { return value_from_json(Json(std::uint64_t{1} << 63), type, false); });
    expect_invalid([&]() noexcept {
      return value_from_json(Json::parse("-9223372036854775809"), type, false);
    });
  }
}

TEST(Values, IntegerToDoubleConversionMustBeExact) {
  for (const auto integer : {std::int64_t{0}, (std::int64_t{1} << 53), -(std::int64_t{1} << 53),
                             std::numeric_limits<std::int64_t>::min()}) {
    EXPECT_EQ(std::get<double>(take(value_from_json(Json(integer), DataType::Float64, false))),
              static_cast<double>(integer));
  }
  for (const auto integer : {(std::int64_t{1} << 53) + 1, -((std::int64_t{1} << 53) + 1),
                             std::numeric_limits<std::int64_t>::max()}) {
    expect_invalid(
        [&]() noexcept { return value_from_json(Json(integer), DataType::Float64, false); });
  }
  EXPECT_EQ(std::get<double>(
                take(value_from_json(Json(std::uint64_t{1} << 63), DataType::Float64, false))),
            std::ldexp(1.0, 63));
  expect_invalid([]() noexcept {
    return value_from_json(Json(std::numeric_limits<std::uint64_t>::max()), DataType::Float64,
                           false);
  });
}

TEST(Values, NonfiniteScalarsAndVectorElementsAreRejected) {
  for (const double number :
       {std::numeric_limits<double>::infinity(), -std::numeric_limits<double>::infinity(),
        std::numeric_limits<double>::quiet_NaN()}) {
    expect_invalid(
        [&]() noexcept { return value_from_json(Json(number), DataType::Float64, false); });
    expect_invalid([&]() noexcept {
      return value_from_json(Json::array({number}), DataType::FloatVector, false);
    });
    expect_invalid([&]() noexcept { return value_to_json(Value{number}); });
    expect_invalid([&]() noexcept { return value_to_json(Value{std::vector<double>{number}}); });
    auto row = take(example_schema().from_json(example_json()));
    row[2] = number;
    expect_invalid([&]() noexcept { return example_schema().validate(row); });
    row = take(example_schema().from_json(example_json()));
    row[5] = std::vector<double>{number};
    expect_invalid([&]() noexcept { return example_schema().validate(row); });
  }
  EXPECT_EQ(take(value_from_json(Json::array(), DataType::FloatVector, false)),
            Value{std::vector<double>{}});
  expect_invalid([]() noexcept {
    return value_from_json(Json::array({nullptr}), DataType::FloatVector, true);
  });
}

TEST(Types, TypeNamesAndSchemaMetadataRoundTrip) {
  for (const auto type : {DataType::Bool, DataType::Int64, DataType::Float64, DataType::String,
                          DataType::TimestampMicros, DataType::FloatVector}) {
    EXPECT_EQ(take(parse_data_type(std::string(data_type_name(type)))), type);
  }
  EXPECT_EQ(take(parse_data_type("int64")), DataType::Int64);
  expect_invalid([]() noexcept { return parse_data_type("DECIMAL"); });
  EXPECT_TRUE(data_type_name(static_cast<DataType>(999)).empty());
  const auto schema = example_schema();
  const auto restored = take(schema_from_json(take(schema_to_json(schema))));
  EXPECT_EQ(take(schema_to_json(restored)), take(schema_to_json(schema)));
  EXPECT_EQ(take(restored.from_json(example_json())), take(schema.from_json(example_json())));
  expect_invalid([]() noexcept { return schema_from_json(Json::array()); });
  for (const auto& bad_id : {Json(-1), Json(std::uint64_t{1} << 32), Json(1.0)}) {
    auto metadata = take(schema_to_json(schema));
    metadata["id"] = bad_id;
    expect_invalid([&]() noexcept { return schema_from_json(metadata); });
  }
}

TEST(RowCodec, RoundTripChecksSchemaAndFormatIdentity) {
  const auto schema = example_schema();
  auto row = take(schema.from_json(example_json()));
  // Persisted JSON strings must be valid UTF-8; NUL itself is valid and preserved.
  row[3] = std::string("A\0B", 3);
  const auto bytes = take(encode_row(schema, row));
  EXPECT_EQ(take(decode_row(schema, bytes)), row);
  auto different = schema;
  ++different.version;
  expect_invalid([&]() noexcept { return decode_row(different, bytes); });
  different = schema;
  ++different.id;
  expect_invalid([&]() noexcept { return decode_row(different, bytes); });
  expect_invalid([&]() noexcept { return decode_row(schema, "not-json"); });
  expect_invalid([&]() noexcept { return decode_row(schema, "{}"); });
  row[1] = std::numeric_limits<std::int64_t>::min();
  row[4] = std::numeric_limits<std::int64_t>::max();
  EXPECT_EQ(take(decode_row(schema, take(encode_row(schema, row)))), row);
}

TEST(RowCodec, MalformedEncodingReturnsStatusWithoutThrowing) {
  const auto schema = example_schema();
  const auto bytes = take(encode_row(schema, take(schema.from_json(example_json()))));
  const auto valid = Json::parse(bytes, nullptr, false);
  ASSERT_FALSE(valid.is_discarded());
  for (const auto& name : {"format", "schema_id", "schema_version", "values"}) {
    auto malformed = valid;
    malformed.erase(name);
    expect_invalid([&]() noexcept { return decode_row(schema, malformed.dump()); });
  }
  for (const auto& value :
       {Json(nullptr), Json(-1), Json(1.0), Json("1"), Json(std::uint64_t{1} << 32)}) {
    auto malformed = valid;
    malformed["format"] = value;
    expect_invalid([&]() noexcept { return decode_row(schema, malformed.dump()); });
  }
  auto malformed = valid;
  malformed["format"] = 2;
  expect_invalid([&]() noexcept { return decode_row(schema, malformed.dump()); });
  malformed = valid;
  malformed["values"] = Json::array();
  expect_invalid([&]() noexcept { return decode_row(schema, malformed.dump()); });
  malformed = valid;
  malformed["values"][1] = true;
  expect_invalid([&]() noexcept { return decode_row(schema, malformed.dump()); });
  expect_invalid([&]() noexcept { return decode_row(schema, "{\"values\":[1e9999]}"); });
  expect_invalid([&]() noexcept { return decode_row(schema, std::string("\xff", 1)); });
}

TEST(Types, MalformedMetadataReturnsStatusWithoutThrowing) {
  const auto valid = take(schema_to_json(example_schema()));
  for (const auto& name : {"id", "name", "version", "fields", "primary_key", "partition_key"}) {
    auto malformed = valid;
    malformed.erase(name);
    expect_invalid([&]() noexcept { return schema_from_json(malformed); });
  }
  for (const auto& name : {"id", "name", "type"}) {
    auto malformed = valid;
    malformed["fields"][0].erase(name);
    expect_invalid([&]() noexcept { return schema_from_json(malformed); });
  }
  auto malformed = valid;
  malformed["fields"][0]["nullable"] = 1;
  expect_invalid([&]() noexcept { return schema_from_json(malformed); });
  malformed = valid;
  malformed["fields"][0]["type"] = true;
  expect_invalid([&]() noexcept { return schema_from_json(malformed); });
  malformed = valid;
  malformed["primary_key"] = Json::array({1});
  expect_invalid([&]() noexcept { return schema_from_json(malformed); });
  malformed = valid;
  malformed[std::string("\xff", 1)] = true;
  expect_invalid([&]() noexcept { return schema_from_json(malformed); });
}

TEST(KeyCodec, PrefixLayoutAndNamespaceAreStable) {
  EXPECT_EQ(take(hex_encode(take(data_prefix(0x01020304)))), "4401020304");
  EXPECT_EQ(take(hex_encode(take(table_prefix(0x01020304, 'T', 0x05060708)))),
            "44010203045405060708");
  for (const char kind : {'T', 'S', 'I'}) {
    const auto prefix = take(table_prefix(7, kind, 9));
    EXPECT_TRUE(take(row_key(7, kind, 9, {std::int64_t{1}})).starts_with(prefix));
    EXPECT_TRUE(key_in_shard(7, prefix));
    EXPECT_FALSE(key_in_shard(8, prefix));
  }
  EXPECT_FALSE(key_in_shard(7, "D"));
  EXPECT_FALSE(key_in_shard(7, ""));
  expect_invalid([]() noexcept { return table_prefix(0, 'X', 1); });
  expect_invalid([]() noexcept { return row_key(0, 'T', 1, {}); });
}

TEST(KeyCodec, SignedIntegersSortAcrossTheCompleteDomain) {
  const std::array<std::int64_t, 7> values = {
      std::numeric_limits<std::int64_t>::min(), -1000, -1, 0, 1, 1000,
      std::numeric_limits<std::int64_t>::max()};
  for (std::size_t i = 1; i < values.size(); ++i) {
    EXPECT_LT(take(encode_key_parts({values[i - 1]})), take(encode_key_parts({values[i]})));
  }
  EXPECT_LT(take(encode_key_parts({false})), take(encode_key_parts({true})));
  EXPECT_NE(take(encode_key_parts({false})), take(encode_key_parts({std::int64_t{0}})));
  EXPECT_NE(take(encode_key_parts({std::int64_t{1}})), take(encode_key_parts({std::string{"1"}})));
}

TEST(KeyCodec, StringsEscapeNulAndPreserveCompositeBoundaries) {
  const std::array<std::string, 8> strings = {"",
                                              std::string("\0", 1),
                                              std::string("\0\0", 2),
                                              std::string("\0A", 2),
                                              "A",
                                              std::string("A\0", 2),
                                              "AA",
                                              std::string("\xff", 1)};
  for (std::size_t i = 1; i < strings.size(); ++i) {
    EXPECT_LT(take(encode_key_parts({strings[i - 1]})), take(encode_key_parts({strings[i]})));
  }
  EXPECT_NE(take(encode_key_parts({std::string{"a"}, std::string{"bc"}})),
            take(encode_key_parts({std::string{"ab"}, std::string{"c"}})));
  EXPECT_NE(take(encode_key_parts({std::string("a\0", 2), std::string{"b"}})),
            take(encode_key_parts({std::string{"a"}, std::string("\0b", 2)})));
  const auto partial = take(encode_key_parts({std::string("a\0b", 3)}));
  EXPECT_TRUE(
      take(encode_key_parts({std::string("a\0b", 3), std::int64_t{42}})).starts_with(partial));
  EXPECT_FALSE(take(encode_key_parts({std::string("a\0bc", 4)})).starts_with(partial));
  EXPECT_EQ(take(encode_key_parts({})), "");
}

TEST(KeyCodec, CompositeEncodingHasNoCollisionsAndSortsByParts) {
  std::set<std::string> encoded;
  const std::array<std::string, 5> strings = {"", "a", "aa", std::string("\0", 1),
                                              std::string("a\0", 2)};
  for (const auto& first : strings) {
    for (const auto& second : strings) {
      EXPECT_TRUE(encoded.insert(take(encode_key_parts({first, second}))).second);
      for (std::int64_t id = -2; id <= 2; ++id) {
        EXPECT_TRUE(encoded.insert(take(encode_key_parts({first, second, id}))).second);
      }
    }
  }
  EXPECT_LT(take(encode_key_parts({std::string{"a"}, std::int64_t{100}})),
            take(encode_key_parts({std::string{"b"}, std::int64_t{-100}})));
  EXPECT_LT(take(encode_key_parts({std::string{"a"}, std::int64_t{-100}})),
            take(encode_key_parts({std::string{"a"}, std::int64_t{100}})));
}

TEST(KeyCodec, InvalidKeyValuesAndShardCountsAreRejected) {
  for (const Value& value :
       {Value{std::monostate{}}, Value{1.0}, Value{std::vector<double>{1.0}}}) {
    expect_invalid([&]() noexcept { return encode_key_parts({value}); });
    expect_invalid([&]() noexcept { return shard_for({value}, 4); });
  }
  expect_invalid([]() noexcept { return shard_for({std::int64_t{1}}, 0); });
  expect_invalid([]() noexcept { return shard_for({}, 4); });
}

TEST(KeyCodec, HashAndPartitioningAreStable) {
  EXPECT_EQ(stable_hash(""), 0xcbf29ce484222325ULL);
  EXPECT_EQ(stable_hash("hello"), 0xa430d84680aabd0bULL);
  EXPECT_NE(stable_hash(std::string("a\0b", 3)), stable_hash("a"));
  const Row key = {std::string{"ABC"}, std::int64_t{-42}};
  for (const std::uint32_t count : {1u, 2u, 7u, 256u}) {
    EXPECT_EQ(take(shard_for(key, count)), stable_hash(take(encode_key_parts(key))) % count);
  }
}

TEST(KeyCodec, PrefixEndDefinesHalfOpenRanges) {
  EXPECT_EQ(take(prefix_end("ABC")), std::optional<std::string>{"ABD"});
  EXPECT_EQ(take(prefix_end(std::string("A\xff", 2))), std::optional<std::string>{"B"});
  EXPECT_EQ(take(prefix_end(std::string("A\0", 2))),
            std::optional<std::string>{std::string("A\1", 2)});
  EXPECT_FALSE(take(prefix_end("")).has_value());
  EXPECT_FALSE(take(prefix_end(std::string("\xff\xff", 2))).has_value());
  const auto prefix =
      take(table_prefix(9, 'T', 1)) + take(encode_key_parts({std::string("x\0y", 3)}));
  const auto end = take(prefix_end(prefix));
  ASSERT_TRUE(end.has_value());
  for (const std::int64_t id : {-1, 0, 1}) {
    const auto key = take(row_key(9, 'T', 1, {std::string("x\0y", 3), id}));
    EXPECT_GE(key, prefix);
    EXPECT_LT(key, *end);
  }
}

TEST(KeyCodec, HexRoundTripsAllBytesAndRejectsMalformedInput) {
  std::string all_bytes;
  for (int i = 0; i < 256; ++i)
    all_bytes.push_back(static_cast<char>(i));
  EXPECT_EQ(take(hex_decode(take(hex_encode(all_bytes)))), all_bytes);
  EXPECT_EQ(take(hex_encode(std::string("\0\xAB\xff", 3))), "00abff");
  EXPECT_EQ(take(hex_decode("00aBFF")), std::string("\0\xAB\xff", 3));
  EXPECT_EQ(take(hex_decode("")), "");
  for (const std::string malformed : {"0", "abc", "0g", " 0", "0x00", "-1", "zz"}) {
    expect_invalid([&]() noexcept { return hex_decode(malformed); });
  }
}

} // namespace
} // namespace eventflow
