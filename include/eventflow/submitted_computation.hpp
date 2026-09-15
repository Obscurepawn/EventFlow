#pragma once

#include "eventflow/computation.hpp"
#include "eventflow/udf.hpp"

namespace eventflow {

struct ComputationCondition {
  std::string column;
  std::string comparison = "ge";
  Value value;
};

struct SubmittedTrigger {
  std::string name;
  FunctionRef function;
  std::string capture_relation;
  std::vector<std::string> capture_fields;
  std::vector<ComputationCondition> conditions;
  std::int64_t every_us = 0; // Zero observes source inputs; positive values define a Clock.
  bool once_per_session = true;
  std::int64_t pending_us = 1000000;
  std::int64_t lifetime_us = 10000000;
};

struct SubmittedComputation {
  std::string name;
  std::vector<std::string> ddl;
  std::string input_relation;
  std::int64_t session_lifetime_us = 1800000000;
  std::vector<SubmittedTrigger> triggers;
};

Json encode_function_ref(const FunctionRef&) noexcept;
StatusOr<FunctionRef> decode_function_ref(const Json&) noexcept;
StatusOr<Json> encode_computation(const SubmittedComputation&) noexcept;
StatusOr<SubmittedComputation> decode_computation(const Json&) noexcept;
StatusOr<std::string> udf_storage_key(std::uint32_t control_shard,
                                      const std::string& digest) noexcept;
StatusOr<BoundComputation>
bind_submitted_computation(const SubmittedComputation&,
                           const std::vector<std::shared_ptr<UdfLibrary>>&,
                           const EngineConfig&) noexcept;
StatusOr<BoundComputation> load_submitted_computation(IStateStore&, std::uint32_t control_shard,
                                                      const Json& definition,
                                                      const Json& registered,
                                                      const EngineConfig&) noexcept;

} // namespace eventflow
