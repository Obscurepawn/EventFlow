#pragma once

#include "eventflow/function_executor.hpp"

namespace eventflow {

inline constexpr std::size_t max_udf_bytes = 1024 * 1024;

// Host and plugin must share the SDK, compiler ABI and standard-library configuration.
struct UdfDefinition {
  FunctionSignature signature;
  StatusOr<Row> (*invoke)(const FunctionInputs&) noexcept = nullptr;
};

using UdfEntry = const UdfDefinition* (*)() noexcept;
using UdfBuildId = const char* (*)() noexcept;

struct FunctionRef {
  std::string id;
  std::uint64_t version = 0;
  std::string digest;
};

StatusOr<std::string> read_udf_file(const std::string& path) noexcept;
std::string udf_digest(std::string_view bytes) noexcept;

class UdfLibrary {
public:
  static StatusOr<std::shared_ptr<UdfLibrary>> open(std::string_view bytes) noexcept;
  ~UdfLibrary() noexcept;
  UdfLibrary(const UdfLibrary&) noexcept = delete;
  UdfLibrary& operator=(const UdfLibrary&) noexcept = delete;
  const UdfDefinition& definition() const noexcept;
  const std::string& digest() const noexcept;

private:
  struct Impl;
  explicit UdfLibrary(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;
};

// Pins the library in the callback and binds its byte digest into implementation identity.
Status register_udf(FunctionRegistry&, std::shared_ptr<UdfLibrary>) noexcept;

} // namespace eventflow

extern "C" const char* eventflow_udf_build_id() noexcept;
extern "C" const eventflow::UdfDefinition* eventflow_udf_entry() noexcept;
