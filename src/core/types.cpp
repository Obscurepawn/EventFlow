#include "eventflow/types.hpp"

#include <algorithm>
#include <bit>
#include <charconv>
#include <cmath>
#include <limits>
#include <set>
#include <type_traits>

#include "eventflow/status.hpp"

namespace eventflow {
namespace {

bool is_key_type(DataType type) noexcept {
  return type == DataType::Bool || type == DataType::Int64 || type == DataType::TimestampMicros ||
         type == DataType::String;
}

Status finite(double value) noexcept {
  return check(std::isfinite(value), "Nonfinite floating-point value");
}

// A binary64 value can retain 53 significant integer bits. Test the discarded
// bits before conversion instead of using an out-of-range float-to-int cast.
bool exactly_representable(std::uint64_t magnitude) noexcept {
  const auto bits = std::bit_width(magnitude);
  return bits <= 53 || (magnitude & ((std::uint64_t{1} << (bits - 53)) - 1)) == 0;
}

StatusOr<double> read_double(const Json& json) noexcept {
  EF_RETURN_IF_ERROR(check(json.is_number(), "Expected a number"));
  if (json.is_number_unsigned()) {
    EF_RETURN_IF_ERROR(check(exactly_representable(json.get<std::uint64_t>()),
                             "Integer is not exactly representable as Float64"));
  } else if (json.is_number_integer()) {
    const auto integer = json.get<std::int64_t>();
    const auto magnitude = integer < 0 ? std::uint64_t{0} - static_cast<std::uint64_t>(integer)
                                       : static_cast<std::uint64_t>(integer);
    EF_RETURN_IF_ERROR(
        check(exactly_representable(magnitude), "Integer is not exactly representable as Float64"));
  }

  auto value = json.get<double>();
  EF_RETURN_IF_ERROR(finite(value));
  return value;
}

StatusOr<std::uint32_t> read_u32(const Json& json, const std::string& name) noexcept {
  EF_RETURN_IF_ERROR(check(json.is_number_integer(), name + " must be an unsigned 32-bit integer"));
  if (!json.is_number_unsigned()) {
    EF_RETURN_IF_ERROR(check(json.get<std::int64_t>() >= 0, name + " must be nonnegative"));
  }

  const auto value = json.get<std::uint64_t>();
  EF_RETURN_IF_ERROR(
      check(value <= std::numeric_limits<std::uint32_t>::max(), name + " exceeds uint32 range"));
  return static_cast<std::uint32_t>(value);
}

StatusOr<std::vector<std::string>> read_names(const Json& json, const std::string& name) noexcept {
  EF_RETURN_IF_ERROR(check(json.is_array(), name + " must be an array"));
  std::vector<std::string> names;
  names.reserve(json.size());
  for (const auto& item : json) {
    EF_RETURN_IF_ERROR(check(item.is_string(), name + " must contain field names"));
    EF_RETURN_IF_ERROR(
        check(valid_utf8(item.get_ref<const std::string&>()), "Invalid UTF-8 field name"));
    names.push_back(item.get<std::string>());
  }

  return names;
}

Status check_members(const Json& json, const std::set<std::string>& allowed,
                     const std::set<std::string>& optional = {}) noexcept {
  EF_RETURN_IF_ERROR(check(json.is_object(), "Expected a JSON object"));
  for (const auto& [name, value] : json.items()) {
    (void)value;
    EF_RETURN_IF_ERROR(check(valid_utf8(name), "Invalid UTF-8 metadata field name"));
    EF_RETURN_IF_ERROR(check(allowed.contains(name) || optional.contains(name),
                             "Unknown metadata field: " + name));
  }

  for (const auto& name : allowed) {
    EF_RETURN_IF_ERROR(check(json.contains(name), "Missing metadata field: " + name));
  }

  return {};
}

} // namespace

Status Schema::validate_definition() const noexcept {
  EF_RETURN_IF_ERROR(check(valid_utf8(name), "Invalid UTF-8 Schema name"));
  EF_RETURN_IF_ERROR(check(!fields.empty(), "Schema must have fields"));
  std::set<std::uint32_t> ids;
  std::set<std::string> names;

  for (const auto& field : fields) {
    EF_RETURN_IF_ERROR(check(!field.name.empty(), "Field name must not be empty"));
    EF_RETURN_IF_ERROR(check(valid_utf8(field.name), "Invalid UTF-8 field name"));
    EF_RETURN_IF_ERROR(
        check(ids.insert(field.id).second, "Duplicate field ID: " + std::to_string(field.id)));
    EF_RETURN_IF_ERROR(
        check(names.insert(field.name).second, "Duplicate field name: " + field.name));
    EF_RETURN_IF_ERROR(check(!data_type_name(field.type).empty(), "Unknown DataType"));
  }

  const auto check_key = [this](const std::vector<std::string>& key,
                                const std::string& label) noexcept -> Status {
    EF_RETURN_IF_ERROR(check(!key.empty(), label + " must not be empty"));
    std::set<std::string> unique;
    for (const auto& name : key) {
      EF_RETURN_IF_ERROR(check(valid_utf8(name), "Invalid UTF-8 key field name"));
      EF_RETURN_IF_ERROR(
          check(unique.insert(name).second, "Duplicate " + label + " field: " + name));
      EF_ASSIGN_OR_RETURN(const auto index, index_of(name));
      const auto& field = fields[index];
      EF_RETURN_IF_ERROR(check(!field.nullable, label + " field cannot be nullable: " + name));
      EF_RETURN_IF_ERROR(
          check(is_key_type(field.type), label + " field has unsupported type: " + name));
    }

    return {};
  };
  EF_RETURN_IF_ERROR(check_key(primary_key, "Primary key"));
  EF_RETURN_IF_ERROR(check_key(partition_key, "Partition key"));

  for (const auto& name : partition_key) {
    EF_RETURN_IF_ERROR(
        check(std::find(primary_key.begin(), primary_key.end(), name) != primary_key.end(),
              "Primary key must contain partition field: " + name));
  }

  return {};
}

StatusOr<std::size_t> Schema::index_of(const std::string& name) const noexcept {
  EF_RETURN_IF_ERROR(check(valid_utf8(name), "Invalid UTF-8 field name"));
  for (std::size_t i = 0; i < fields.size(); ++i) {
    if (fields[i].name == name)
      return i;
  }

  return Status(ErrorCode::InvalidArgument, "Unknown field: " + name);
}

Status Schema::validate(const Row& row) const noexcept {
  EF_RETURN_IF_ERROR(validate_definition());
  EF_RETURN_IF_ERROR(check(row.size() == fields.size(), "Row size does not match Schema"));
  for (std::size_t i = 0; i < fields.size(); ++i) {
    const auto& field = fields[i];
    const auto& value = row[i];
    if (std::holds_alternative<std::monostate>(value)) {
      EF_RETURN_IF_ERROR(check(field.nullable, "Nonnullable field is NULL: " + field.name));
      continue;
    }

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

    EF_RETURN_IF_ERROR(check(matches, "Value type does not match field: " + field.name));
    if (const auto* number = std::get_if<double>(&value)) {
      EF_RETURN_IF_ERROR(finite(*number));
    }

    if (const auto* vector = std::get_if<std::vector<double>>(&value)) {
      for (const auto number : *vector) {
        EF_RETURN_IF_ERROR(finite(number));
      }
    }

    if (const auto* text = std::get_if<std::string>(&value)) {
      EF_RETURN_IF_ERROR(check(valid_utf8(*text), "Invalid UTF-8 String"));
    }
  }

  return {};
}

StatusOr<Row> Schema::from_json(const Json& object) const noexcept {
  EF_RETURN_IF_ERROR(validate_definition());
  EF_RETURN_IF_ERROR(check(object.is_object(), "Row JSON must be an object"));
  for (const auto& [name, value] : object.items()) {
    (void)value;
    EF_ASSIGN_OR_RETURN(const auto index, index_of(name));
    (void)index;
  }

  Row row;
  row.reserve(fields.size());
  for (const auto& field : fields) {
    const auto it = object.find(field.name);
    if (it == object.end()) {
      EF_RETURN_IF_ERROR(check(field.nullable, "Missing required field: " + field.name));
      row.emplace_back(std::monostate{});
    } else {
      EF_ASSIGN_OR_RETURN(auto value, value_from_json(*it, field.type, field.nullable));
      row.push_back(std::move(value));
    }
  }

  return row;
}

StatusOr<Json> Schema::to_json(const Row& row) const noexcept {
  EF_RETURN_IF_ERROR(validate(row));
  auto object = Json::object();
  for (std::size_t i = 0; i < fields.size(); ++i) {
    EF_ASSIGN_OR_RETURN(auto value, value_to_json(row[i]));
    object[fields[i].name] = std::move(value);
  }

  return object;
}

StatusOr<Row> Schema::project(const Row& row,
                              const std::vector<std::string>& names) const noexcept {
  EF_RETURN_IF_ERROR(validate(row));
  Row result;
  result.reserve(names.size());

  for (const auto& name : names) {
    EF_ASSIGN_OR_RETURN(const auto index, index_of(name));
    result.push_back(row[index]);
  }

  return result;
}

std::string_view data_type_name(DataType type) noexcept {
  switch (type) {
  case DataType::Bool:
    return "BOOL";
  case DataType::Int64:
    return "INT64";
  case DataType::Float64:
    return "FLOAT64";
  case DataType::String:
    return "STRING";
  case DataType::TimestampMicros:
    return "TIMESTAMP_MICROS";
  case DataType::FloatVector:
    return "FLOAT_VECTOR";
  }

  return {};
}

StatusOr<DataType> parse_data_type(const std::string& name) noexcept {
  EF_RETURN_IF_ERROR(check(valid_utf8(name), "Invalid UTF-8 type name"));
  auto upper = name;
  for (char& c : upper) {
    if (c >= 'a' && c <= 'z')
      c = static_cast<char>(c - 'a' + 'A');
  }

  if (upper == "BOOL" || upper == "BOOLEAN")
    return DataType::Bool;
  if (upper == "INT64" || upper == "BIGINT" || upper == "INT" || upper == "INTEGER")
    return DataType::Int64;
  if (upper == "FLOAT64" || upper == "DOUBLE" || upper == "FLOAT")
    return DataType::Float64;
  if (upper == "STRING" || upper == "TEXT" || upper == "VARCHAR")
    return DataType::String;
  if (upper == "TIMESTAMP_MICROS" || upper == "TIMESTAMPMICROS" || upper == "TIMESTAMP")
    return DataType::TimestampMicros;
  if (upper == "FLOAT_VECTOR" || upper == "FLOATVECTOR" || upper == "VECTOR")
    return DataType::FloatVector;

  return Status(ErrorCode::InvalidArgument, "Unsupported type: " + name);
}

StatusOr<Json> value_to_json(const Value& value) noexcept {
  return std::visit(
      [](const auto& item) noexcept -> StatusOr<Json> {
        using T = std::decay_t<decltype(item)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          return Json(nullptr);
        } else {
          if constexpr (std::is_same_v<T, double>) {
            EF_RETURN_IF_ERROR(finite(item));
          }

          if constexpr (std::is_same_v<T, std::vector<double>>) {
            for (const auto number : item) {
              EF_RETURN_IF_ERROR(finite(number));
            }
          }

          if constexpr (std::is_same_v<T, std::string>) {
            EF_RETURN_IF_ERROR(check(valid_utf8(item), "Invalid UTF-8 String"));
          }

          return Json(item);
        }
      },
      value);
}

StatusOr<Value> value_from_json(const Json& value, DataType type, bool nullable) noexcept {
  EF_RETURN_IF_ERROR(check(!data_type_name(type).empty(), "Unknown DataType"));
  if (value.is_null()) {
    EF_RETURN_IF_ERROR(check(nullable, "NULL is not allowed"));
    return Value{std::monostate{}};
  }

  switch (type) {
  case DataType::Bool:
    EF_RETURN_IF_ERROR(check(value.is_boolean(), "Expected Bool"));
    return Value{value.get<bool>()};
  case DataType::Int64:
  case DataType::TimestampMicros:
    EF_RETURN_IF_ERROR(
        check(value.is_number_integer(), "Expected Int64 or TimestampMicros integer"));
    if (value.is_number_unsigned()) {
      EF_RETURN_IF_ERROR(
          check(value.get<std::uint64_t>() <=
                    static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()),
                "Integer exceeds Int64 range"));
    }

    return Value{value.get<std::int64_t>()};
  case DataType::Float64: {
    EF_ASSIGN_OR_RETURN(const auto number, read_double(value));
    return Value{number};
  }
  case DataType::String:
    EF_RETURN_IF_ERROR(check(value.is_string(), "Expected String"));
    EF_RETURN_IF_ERROR(
        check(valid_utf8(value.get_ref<const std::string&>()), "Invalid UTF-8 String"));
    return Value{value.get<std::string>()};
  case DataType::FloatVector: {
    EF_RETURN_IF_ERROR(check(value.is_array(), "Expected FloatVector array"));
    std::vector<double> vector;
    vector.reserve(value.size());
    for (const auto& item : value) {
      EF_ASSIGN_OR_RETURN(const auto number, read_double(item));
      vector.push_back(number);
    }

    return Value{std::move(vector)};
  }
  }

  return Status(ErrorCode::InvalidArgument, "Unknown DataType");
}

bool valid_utf8(std::string_view text) noexcept {
  std::size_t i = 0;
  while (i < text.size()) {
    const auto first = static_cast<unsigned char>(text[i++]);
    if (first <= 0x7f)
      continue;

    std::uint32_t codepoint = 0;
    std::uint32_t minimum = 0;
    std::size_t remaining = 0;
    if (first >= 0xc2 && first <= 0xdf) {
      codepoint = first & 0x1f;
      minimum = 0x80;
      remaining = 1;
    } else if (first >= 0xe0 && first <= 0xef) {
      codepoint = first & 0x0f;
      minimum = 0x800;
      remaining = 2;
    } else if (first >= 0xf0 && first <= 0xf4) {
      codepoint = first & 0x07;
      minimum = 0x10000;
      remaining = 3;
    } else {
      return false;
    }

    if (remaining > text.size() - i)
      return false;

    for (std::size_t j = 0; j < remaining; ++j) {
      const auto byte = static_cast<unsigned char>(text[i++]);
      if ((byte & 0xc0) != 0x80)
        return false;

      codepoint = (codepoint << 6) | (byte & 0x3f);
    }

    if (codepoint < minimum || codepoint > 0x10ffff || (codepoint >= 0xd800 && codepoint <= 0xdfff))
      return false;
  }

  return true;
}

StatusOr<Json> parse_json_exact(std::string_view text) noexcept {
  // Only guard integer ranges here; the library remains responsible for JSON
  // grammar, UTF-8, and explicitly floating-point numeric tokens.
  std::size_t i = 0;
  while (i < text.size()) {
    if (text[i] == '"') {
      ++i;
      while (i < text.size()) {
        const auto c = text[i++];
        if (c == '"')
          break;
        if (c == '\\' && i < text.size())
          ++i;
      }

      continue;
    }

    if (text[i] != '-' && (text[i] < '0' || text[i] > '9')) {
      ++i;
      continue;
    }

    const auto start = i;
    const auto end = text.find_first_not_of("0123456789+-.eE", start);
    i = end == std::string_view::npos ? text.size() : end;
    const auto token = text.substr(start, i - start);
    if (token.find_first_of(".eE") != std::string_view::npos)
      continue;

    std::from_chars_result parsed{};
    if (token.front() == '-') {
      std::int64_t integer = 0;
      parsed = std::from_chars(token.data(), token.data() + token.size(), integer);
    } else {
      std::uint64_t integer = 0;
      parsed = std::from_chars(token.data(), token.data() + token.size(), integer);
    }

    EF_RETURN_IF_ERROR(check(parsed.ec == std::errc{} && parsed.ptr == token.data() + token.size(),
                             "JSON integer is outside the supported Int64/UInt64 range"));
  }

  auto json = Json::parse(text, nullptr, false);
  EF_RETURN_IF_ERROR(check(!json.is_discarded(), "Invalid JSON"));
  return json;
}

StatusOr<std::string> encode_row(const Schema& schema, const Row& row) noexcept {
  EF_RETURN_IF_ERROR(schema.validate(row));
  auto values = Json::array();
  for (const auto& value : row) {
    EF_ASSIGN_OR_RETURN(auto json, value_to_json(value));
    values.push_back(std::move(json));
  }

  return Json{{"format", 1},
              {"schema_id", schema.id},
              {"schema_version", schema.version},
              {"values", std::move(values)}}
      .dump();
}

StatusOr<Row> decode_row(const Schema& schema, const std::string& bytes) noexcept {
  EF_RETURN_IF_ERROR(schema.validate_definition());
  EF_ASSIGN_OR_RETURN(const auto object, parse_json_exact(bytes));
  EF_RETURN_IF_ERROR(check_members(object, {"format", "schema_id", "schema_version", "values"}));
  EF_ASSIGN_OR_RETURN(const auto format, read_u32(object["format"], "format"));
  EF_ASSIGN_OR_RETURN(const auto schema_id, read_u32(object["schema_id"], "schema_id"));
  EF_ASSIGN_OR_RETURN(const auto version, read_u32(object["schema_version"], "schema_version"));
  EF_RETURN_IF_ERROR(check(format == 1, "Unsupported row format"));
  EF_RETURN_IF_ERROR(check(schema_id == schema.id, "Row Schema ID mismatch"));
  EF_RETURN_IF_ERROR(check(version == schema.version, "Row Schema version mismatch"));
  const auto& values = object["values"];
  EF_RETURN_IF_ERROR(check(values.is_array() && values.size() == schema.fields.size(),
                           "Invalid encoded row size"));
  Row row;
  row.reserve(values.size());

  for (std::size_t i = 0; i < values.size(); ++i) {
    EF_ASSIGN_OR_RETURN(
        auto value, value_from_json(values[i], schema.fields[i].type, schema.fields[i].nullable));
    row.push_back(std::move(value));
  }

  return row;
}

StatusOr<Json> schema_to_json(const Schema& schema) noexcept {
  EF_RETURN_IF_ERROR(schema.validate_definition());
  auto fields = Json::array();
  for (const auto& field : schema.fields) {
    fields.push_back({{"id", field.id},
                      {"name", field.name},
                      {"type", data_type_name(field.type)},
                      {"nullable", field.nullable}});
  }

  return Json{{"id", schema.id},
              {"name", schema.name},
              {"version", schema.version},
              {"fields", std::move(fields)},
              {"primary_key", schema.primary_key},
              {"partition_key", schema.partition_key}};
}

StatusOr<Schema> schema_from_json(const Json& json) noexcept {
  EF_RETURN_IF_ERROR(
      check_members(json, {"id", "name", "version", "fields", "primary_key", "partition_key"}));
  Schema schema;
  EF_ASSIGN_OR_RETURN(schema.id, read_u32(json["id"], "id"));
  EF_RETURN_IF_ERROR(check(json["name"].is_string(), "Schema name must be a string"));
  schema.name = json["name"].get<std::string>();
  EF_ASSIGN_OR_RETURN(schema.version, read_u32(json["version"], "version"));
  const auto& fields = json["fields"];
  EF_RETURN_IF_ERROR(check(fields.is_array(), "Schema fields must be an array"));

  for (const auto& item : fields) {
    EF_RETURN_IF_ERROR(check_members(item, {"id", "name", "type"}, {"nullable"}));
    Field field;
    EF_ASSIGN_OR_RETURN(field.id, read_u32(item["id"], "field id"));
    EF_RETURN_IF_ERROR(check(item["name"].is_string(), "Field name must be a string"));
    EF_RETURN_IF_ERROR(check(item["type"].is_string(), "Field type must be a string"));
    field.name = item["name"].get<std::string>();
    EF_ASSIGN_OR_RETURN(field.type, parse_data_type(item["type"].get<std::string>()));
    if (item.contains("nullable")) {
      EF_RETURN_IF_ERROR(check(item["nullable"].is_boolean(), "Nullable flag must be boolean"));
      field.nullable = item["nullable"].get<bool>();
    }

    schema.fields.push_back(std::move(field));
  }

  EF_ASSIGN_OR_RETURN(schema.primary_key, read_names(json["primary_key"], "primary_key"));
  EF_ASSIGN_OR_RETURN(schema.partition_key, read_names(json["partition_key"], "partition_key"));
  EF_RETURN_IF_ERROR(schema.validate_definition());
  return schema;
}

} // namespace eventflow
