#pragma once

#include "eventflow/types.hpp"
#include <optional>
#include <tuple>
#include <type_traits>

namespace eventflow {

// Specialize explicitly. C++20 does not reflect arbitrary struct members.
template <typename T> struct SchemaTraits;

template <auto Member> struct TypedField {
  static_assert(std::is_member_object_pointer_v<decltype(Member)>,
                "Schema fields must refer to data members");
  static constexpr auto member = Member;
  std::string name;
};

template <auto Member> TypedField<Member> field(std::string name) noexcept {
  return {.name = std::move(name)};
}

template <typename T>
concept TypedSchema = std::is_nothrow_default_constructible_v<T> &&
                      std::is_nothrow_move_constructible_v<T> && requires {
                        { SchemaTraits<T>::fields() } noexcept;
                        std::tuple_size<decltype(SchemaTraits<T>::fields())>::value;
                      };

Status validate_row_fields(const std::vector<Field>& fields, const Row& row) noexcept;

class RowFieldBinding {
public:
  // Every actual field is checked, including fields absent from the typed struct.
  StatusOr<Row> project(const Row& row) const noexcept;

private:
  RowFieldBinding() noexcept = default;
  std::vector<Field> actual_;
  std::vector<Field> expected_;
  std::vector<std::size_t> indices_;
  friend StatusOr<RowFieldBinding> bind_row_fields(const std::vector<Field>&,
                                                   const std::vector<Field>&) noexcept;
};

StatusOr<RowFieldBinding> bind_row_fields(const std::vector<Field>& expected,
                                          const std::vector<Field>& actual) noexcept;

namespace typed_detail {

template <typename T> struct MemberType;

template <typename Object, typename Member> struct MemberType<Member Object::*> {
  using object_type = Object;
  using value_type = Member;
};

template <typename T> struct OptionalType {
  using value_type = T;
  static constexpr bool nullable = false;
};

template <typename T> struct OptionalType<std::optional<T>> {
  using value_type = T;
  static constexpr bool nullable = true;
};

template <typename T>
inline constexpr bool supported_value =
    std::is_same_v<T, bool> || std::is_same_v<T, std::int64_t> || std::is_same_v<T, double> ||
    std::is_same_v<T, std::string> || std::is_same_v<T, std::vector<double>>;

template <typename T> constexpr DataType data_type() noexcept {
  static_assert(supported_value<T>,
                "Typed fields support bool, int64_t, double, string and vector<double>");
  if constexpr (std::is_same_v<T, bool>)
    return DataType::Bool;
  else if constexpr (std::is_same_v<T, std::int64_t>)
    return DataType::Int64;
  else if constexpr (std::is_same_v<T, double>)
    return DataType::Float64;
  else if constexpr (std::is_same_v<T, std::string>)
    return DataType::String;
  else
    return DataType::FloatVector;
}

template <typename T, typename Mapping>
void append_field(const Mapping& mapping, std::vector<Field>& fields) noexcept {
  using Member = MemberType<std::remove_cv_t<decltype(Mapping::member)>>;
  static_assert(std::is_same_v<typename Member::object_type, T>,
                "SchemaTraits fields must belong to the described type");
  using Optional = OptionalType<typename Member::value_type>;
  fields.push_back({.id = static_cast<std::uint32_t>(fields.size() + 1),
                    .name = mapping.name,
                    .type = data_type<typename Optional::value_type>(),
                    .nullable = Optional::nullable});
}

template <typename T> Value encode_member(const T& value) noexcept {
  if constexpr (OptionalType<T>::nullable)
    return value ? Value(*value) : Value(std::monostate{});
  else
    return Value(value);
}

template <typename T> Status assign_member(T& target, const Value& value) noexcept {
  using Optional = OptionalType<T>;
  if constexpr (Optional::nullable) {
    if (std::holds_alternative<std::monostate>(value)) {
      target.reset();
      return {};
    }
  }
  const auto* scalar = std::get_if<typename Optional::value_type>(&value);
  if (!scalar)
    return Status(ErrorCode::InvalidArgument, "Typed field has an incompatible value");
  target = *scalar;
  return {};
}

template <TypedSchema T, std::size_t... Indices>
Row encode_members(const T& value, std::index_sequence<Indices...>) noexcept {
  using Mappings = decltype(SchemaTraits<T>::fields());
  Row row;
  row.reserve(sizeof...(Indices));
  (row.push_back(encode_member(value.*std::tuple_element_t<Indices, Mappings>::member)), ...);
  return row;
}

template <TypedSchema T> Row encode_members(const T& value) noexcept {
  return encode_members(
      value, std::make_index_sequence<std::tuple_size_v<decltype(SchemaTraits<T>::fields())>>{});
}

template <TypedSchema T, std::size_t... Indices>
StatusOr<T> decode_members(const Row& row, std::index_sequence<Indices...>) noexcept {
  using Mappings = decltype(SchemaTraits<T>::fields());
  T value{};
  Status status;
  const auto assign = [&]<std::size_t Index>() noexcept {
    if (status.ok())
      status = assign_member(value.*std::tuple_element_t<Index, Mappings>::member, row[Index]);
  };
  (assign.template operator()<Indices>(), ...);
  EF_RETURN_IF_ERROR(status);
  return value;
}

template <TypedSchema T> StatusOr<T> decode_members(const Row& row) noexcept {
  return decode_members<T>(
      row, std::make_index_sequence<std::tuple_size_v<decltype(SchemaTraits<T>::fields())>>{});
}

} // namespace typed_detail

template <TypedSchema T> StatusOr<std::vector<Field>> typed_fields() noexcept {
  const auto mappings = SchemaTraits<T>::fields();
  std::vector<Field> fields;
  fields.reserve(std::tuple_size_v<decltype(mappings)>);
  std::apply(
      [&fields](const auto&... mapping) noexcept {
        (typed_detail::append_field<T>(mapping, fields), ...);
      },
      mappings);
  // Binding validates duplicate names, field types and empty definitions.
  EF_ASSIGN_OR_RETURN(auto validated, bind_row_fields(fields, fields));
  (void)validated;
  return fields;
}

template <TypedSchema T> StatusOr<Row> encode_typed(const T& value) noexcept {
  EF_ASSIGN_OR_RETURN(auto fields, typed_fields<T>());
  auto row = typed_detail::encode_members(value);
  EF_RETURN_IF_ERROR(validate_row_fields(fields, row));
  return row;
}

template <TypedSchema T> StatusOr<T> decode_typed(const Row& row) noexcept {
  EF_ASSIGN_OR_RETURN(auto fields, typed_fields<T>());
  EF_RETURN_IF_ERROR(validate_row_fields(fields, row));
  return typed_detail::decode_members<T>(row);
}

template <TypedSchema T> class TypedRowBinding {
public:
  StatusOr<T> decode(const Row& row) const noexcept {
    EF_ASSIGN_OR_RETURN(auto projected, binding_.project(row));
    return typed_detail::decode_members<T>(projected);
  }

private:
  explicit TypedRowBinding(RowFieldBinding binding) noexcept : binding_(std::move(binding)) {}

  RowFieldBinding binding_;
  template <TypedSchema U>
  friend StatusOr<TypedRowBinding<U>> bind_typed_schema(const std::vector<Field>&) noexcept;
};

template <TypedSchema T>
StatusOr<TypedRowBinding<T>> bind_typed_schema(const std::vector<Field>& actual) noexcept {
  EF_ASSIGN_OR_RETURN(auto expected, typed_fields<T>());
  EF_ASSIGN_OR_RETURN(auto binding, bind_row_fields(expected, actual));
  return TypedRowBinding<T>(std::move(binding));
}

} // namespace eventflow
