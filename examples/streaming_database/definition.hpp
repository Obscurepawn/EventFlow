#pragma once

#include "eventflow/submitted_computation.hpp"

namespace eventflow::native_udf_example {

SubmittedComputation computation_definition(const FunctionRef& function) noexcept;

} // namespace eventflow::native_udf_example
