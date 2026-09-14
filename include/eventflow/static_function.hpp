#pragma once

#include "eventflow/typed_schema.hpp"
#include <functional>

namespace eventflow {

struct StaticFunctionDefinition {
  std::string id;
  std::uint64_t version = 0;
  std::string implementation_identity;
  std::vector<Field> input_fields;
  std::vector<Field> output_fields;
  std::function<StatusOr<Row>(const Row&)> invoke;
};

namespace typed_detail {

template <typename T, typename = void> struct StaticFunctionSignature {
  static constexpr bool valid = false;
};

template <typename In, typename Out>
struct StaticFunctionSignature<StatusOr<Out> (*)(const In&) noexcept> {
  static constexpr bool valid = TypedSchema<In> && TypedSchema<Out>;
  using input_type = In;
  using output_type = Out;
};

template <typename Owner, typename In, typename Out>
struct StaticFunctionSignature<StatusOr<Out> (Owner::*)(const In&) const noexcept>
    : StaticFunctionSignature<StatusOr<Out> (*)(const In&) noexcept> {};

template <typename Callable>
struct StaticFunctionSignature<Callable, std::void_t<decltype(&Callable::operator())>>
    : StaticFunctionSignature<decltype(&Callable::operator())> {
  static constexpr bool valid =
      std::is_empty_v<Callable> && StaticFunctionSignature<decltype(&Callable::operator())>::valid;
};

template <auto Fn> constexpr bool nonnull_static_callable() noexcept {
  if constexpr (std::is_pointer_v<std::remove_cv_t<decltype(Fn)>>)
    return Fn != nullptr;
  return true;
}

} // namespace typed_detail

template <auto Fn>
concept StaticFunctionCallable =
    typed_detail::StaticFunctionSignature<std::remove_cv_t<decltype(Fn)>>::valid &&
    typed_detail::nonnull_static_callable<Fn>();

template <auto Fn>
  requires StaticFunctionCallable<Fn>
struct Function {
  using input_type =
      typename typed_detail::StaticFunctionSignature<std::remove_cv_t<decltype(Fn)>>::input_type;
  using output_type =
      typename typed_detail::StaticFunctionSignature<std::remove_cv_t<decltype(Fn)>>::output_type;

  std::string id;
  std::uint64_t version = 0;
  std::string implementation_identity;

  StatusOr<StaticFunctionDefinition> definition() const noexcept {
    EF_RETURN_IF_ERROR(
        check(!id.empty() && valid_utf8(id), "Static function ID must be nonempty UTF-8"));
    EF_RETURN_IF_ERROR(check(version > 0, "Static function version must be positive"));
    EF_RETURN_IF_ERROR(
        check(!implementation_identity.empty() && valid_utf8(implementation_identity),
              "Static function requires a stable implementation identity"));
    EF_ASSIGN_OR_RETURN(auto input_fields, typed_fields<input_type>());
    EF_ASSIGN_OR_RETURN(auto output_fields, typed_fields<output_type>());
    EF_ASSIGN_OR_RETURN(auto input_binding, bind_typed_schema<input_type>(input_fields));
    auto invoke = [input_binding = std::move(input_binding),
                   output_fields](const Row& row) noexcept -> StatusOr<Row> {
      EF_ASSIGN_OR_RETURN(auto input, input_binding.decode(row));
      EF_ASSIGN_OR_RETURN(auto output, Fn(input));
      auto encoded = typed_detail::encode_members(output);
      EF_RETURN_IF_ERROR(validate_row_fields(output_fields, encoded));
      return encoded;
    };
    return StaticFunctionDefinition{.id = id,
                                    .version = version,
                                    .implementation_identity = implementation_identity,
                                    .input_fields = std::move(input_fields),
                                    .output_fields = std::move(output_fields),
                                    .invoke = std::move(invoke)};
  }
};

} // namespace eventflow
