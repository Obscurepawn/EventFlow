#pragma once

#include "eventflow/computation_runtime.hpp"

namespace eventflow::streaming_example {

// Every database process links this same application-owned static definition.
ComputationModule computation_module() noexcept;

} // namespace eventflow::streaming_example
