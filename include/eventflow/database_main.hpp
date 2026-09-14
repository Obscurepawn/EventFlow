#pragma once

#include "eventflow/computation_runtime.hpp"

namespace eventflow {

// Static applications pass the same module to all roles of their database executable.
// The entry point owns process signals and blocks until the selected service stops.
int database_main(int argc, char** argv, const ComputationModule& module = {}) noexcept;

} // namespace eventflow
