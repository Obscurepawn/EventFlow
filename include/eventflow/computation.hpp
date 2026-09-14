#pragma once

#include "eventflow/trigger.hpp"

namespace eventflow {

// A complete computation definition. Node addresses and runtime resource choices
// are separate deployment concerns; ProcessorConfig is the legacy owner container.
struct BoundComputation {
  CatalogSnapshot catalog;
  std::shared_ptr<FunctionRegistry> registry;
  ProcessorConfig definition;
  Json explanation;

  const Json& explain() const noexcept {
    return explanation;
  }
};

class TriggerSet {
public:
  // Work on private Catalog/Registry copies. Nothing is published on failure.
  // Generated captures, function views and requests share the same Catalog graph.
  static StatusOr<BoundComputation> bind(CatalogSnapshot catalog, ProcessorConfig definition,
                                         std::vector<Trigger> triggers,
                                         const EngineConfig& runtime = {}) noexcept;
};

} // namespace eventflow
