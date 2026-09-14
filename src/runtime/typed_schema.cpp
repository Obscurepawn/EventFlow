#include "eventflow/typed_schema.hpp"
#include <algorithm>
#include <cmath>
#include <set>

namespace eventflow {
namespace {

Status validate_fields(const std::vector<Field>& fields) noexcept {
  EF_RETURN_IF_ERROR(check(!fields.empty(), "Typed schema must contain fields"));
  std::set<std::string> names;
  std::set<std::uint32_t> ids;
  for (const auto& field : fields) {
    EF_RETURN_IF_ERROR(check(!field.name.empty() && valid_utf8(field.name),
                             "Typed schema field name must be nonempty UTF-8"));
    EF_RETURN_IF_ERROR(check(names.insert(field.name).second,
                             "Typed schema has duplicate field name: " + field.name));
    EF_RETURN_IF_ERROR(
        check(ids.insert(field.id).second, "Typed schema has duplicate field ID: " + field.name));
    EF_RETURN_IF_ERROR(check(!data_type_name(field.type).empty(),
                             "Typed schema has unsupported field type: " + field.name));
  }

  return {};
}

Status validate_value(const Field& field, const Value& value) noexcept {
  if (std::holds_alternative<std::monostate>(value))
    return check(field.nullable, "Typed field is unexpectedly NULL: " + field.name);

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

  EF_RETURN_IF_ERROR(check(matches, "Typed value type does not match field: " + field.name));
  if (const auto* number = std::get_if<double>(&value))
    EF_RETURN_IF_ERROR(
        check(std::isfinite(*number), "Typed Float64 must be finite: " + field.name));
  if (const auto* vector = std::get_if<std::vector<double>>(&value)) {
    for (const auto number : *vector)
      EF_RETURN_IF_ERROR(
          check(std::isfinite(number), "Typed FloatVector must be finite: " + field.name));
  }

  if (const auto* text = std::get_if<std::string>(&value))
    EF_RETURN_IF_ERROR(check(valid_utf8(*text), "Typed String must be valid UTF-8: " + field.name));

  return {};
}

bool compatible_type(DataType expected, DataType actual) noexcept {
  const auto integer = [](DataType type) noexcept {
    return type == DataType::Int64 || type == DataType::TimestampMicros;
  };
  return expected == actual || (integer(expected) && integer(actual));
}

} // namespace

Status validate_row_fields(const std::vector<Field>& fields, const Row& row) noexcept {
  EF_RETURN_IF_ERROR(check(row.size() == fields.size(), "Typed row size does not match schema"));
  for (std::size_t i = 0; i < row.size(); ++i)
    EF_RETURN_IF_ERROR(validate_value(fields[i], row[i]));

  return {};
}

StatusOr<RowFieldBinding> bind_row_fields(const std::vector<Field>& expected,
                                          const std::vector<Field>& actual) noexcept {
  EF_RETURN_IF_ERROR(validate_fields(expected));
  EF_RETURN_IF_ERROR(validate_fields(actual));
  RowFieldBinding binding;
  binding.actual_ = actual;
  binding.expected_ = expected;

  for (const auto& field : expected) {
    const auto found =
        std::find_if(actual.begin(), actual.end(), [&field](const Field& candidate) noexcept {
          return candidate.name == field.name;
        });
    EF_RETURN_IF_ERROR(
        check(found != actual.end(), "Typed schema is missing field: " + field.name));
    EF_RETURN_IF_ERROR(check(compatible_type(field.type, found->type),
                             "Typed field " + field.name + " requires " +
                                 std::string(data_type_name(field.type))));
    EF_RETURN_IF_ERROR(check(field.nullable || !found->nullable,
                             "Nullable field requires an optional typed member: " + field.name));
    binding.indices_.push_back(static_cast<std::size_t>(std::distance(actual.begin(), found)));
  }

  return binding;
}

StatusOr<Row> RowFieldBinding::project(const Row& row) const noexcept {
  EF_RETURN_IF_ERROR(validate_row_fields(actual_, row));
  Row output;
  output.reserve(indices_.size());
  for (const auto index : indices_)
    output.push_back(row[index]);

  EF_RETURN_IF_ERROR(validate_row_fields(expected_, output));
  return output;
}

} // namespace eventflow
