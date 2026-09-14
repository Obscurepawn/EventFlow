#pragma once

#include "eventflow/catalog.hpp"
#include <cstddef>

namespace eventflow {

// TriggerSet::bind installs this protected relation automatically. Raw legacy
// ProcessorConfig instances keep relation=0 until an audit relation is bound.
struct TriggerAuditConfig {
  RelationId relation = 0;
  std::size_t max_rows_per_scope = 128;
  // Retained KV key plus encoded row bytes, excluding backend/index overhead.
  std::size_t max_bytes_per_scope = 256 * 1024;
};

} // namespace eventflow
