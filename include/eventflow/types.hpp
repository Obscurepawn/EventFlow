#pragma once
#include "eventflow/status.hpp"
#include <cstdint>
#include <nlohmann/json.hpp>
#include <string>
#include <variant>
#include <vector>

namespace eventflow {
using Json = nlohmann::json;
using Value =
    std::variant<std::monostate, bool, std::int64_t, double, std::string, std::vector<double>>;
using Row = std::vector<Value>;
enum class DataType { Bool, Int64, Float64, String, TimestampMicros, FloatVector };

struct Field {
  std::uint32_t id;
  std::string name;
  DataType type;
  bool nullable = false;
};

struct Schema {
  std::uint32_t id = 0;
  std::string name;
  std::uint32_t version = 1;
  std::vector<Field> fields;
  std::vector<std::string> primary_key;
  std::vector<std::string> partition_key;
  Status validate_definition() const noexcept;
  StatusOr<std::size_t> index_of(const std::string& field) const noexcept;
  Status validate(const Row& row) const noexcept;
  // A typed DOM cannot recover original numeric lexemes; parse raw JSON with parse_json_exact.
  StatusOr<Row> from_json(const Json& object) const noexcept;
  StatusOr<Json> to_json(const Row& row) const noexcept;
  StatusOr<Row> project(const Row& row, const std::vector<std::string>& names) const noexcept;
};

bool valid_utf8(std::string_view text) noexcept;
// Check integer lexemes before the JSON library can round them to Float64.
StatusOr<Json> parse_json_exact(std::string_view text) noexcept;
std::string_view data_type_name(DataType type) noexcept;
StatusOr<DataType> parse_data_type(const std::string& name) noexcept;
StatusOr<Json> value_to_json(const Value& value) noexcept;
StatusOr<Value> value_from_json(const Json& value, DataType type, bool nullable) noexcept;
StatusOr<std::string> encode_row(const Schema& schema, const Row& row) noexcept;
StatusOr<Row> decode_row(const Schema& schema, const std::string& bytes) noexcept;
StatusOr<Json> schema_to_json(const Schema& schema) noexcept;
StatusOr<Schema> schema_from_json(const Json& json) noexcept;
} // namespace eventflow
