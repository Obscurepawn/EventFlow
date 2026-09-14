#pragma once
#include "eventflow/cep.hpp"
#include "eventflow/observation_definition.hpp"

namespace eventflow {
using ObservationProgramDefinition = std::variant<FsmDefinition, CepDefinition>;
// Canonical declarations include callback presence, never callback addresses or bodies.
StatusOr<std::string> observation_program_fingerprint(const FsmDefinition& definition) noexcept;
StatusOr<std::string> observation_program_fingerprint(const CepDefinition& definition) noexcept;

struct ObservationProgramBounds {
  std::size_t max_state_bytes = 0; // Complete encoded wrapper state.
  std::size_t max_output_count = 0;
  std::size_t max_output_bytes = 0; // Encoded emissions/matches/timeouts, excluding host KV keys.
  std::size_t max_read_bytes = 0;   // Canonical typed read map, including aliases and missing rows.
  std::size_t max_event_bytes = 0;  // Largest declared canonical typed event envelope.
};

struct ObservationProgramStateInfo {
  std::int64_t time_us = 0;
  std::optional<std::int64_t> next_deadline_us;
};

struct ObservationProgramStep {
  std::string next_state;
  std::vector<FsmEmission> emissions;
  std::vector<CepMatch> matches;
  std::vector<CepTimeout> timeouts;
  std::int64_t time_us = 0;
  std::optional<std::int64_t> next_deadline_us;
};

// Pure compiled-recipe binding. The descriptor must originate from a validated Catalog;
// this wrapper checks its portable signatures, and validates all values before callbacks.
// Callback type assumptions/bodies remain the responsibility of the immutable implementation.
class ObservationProgram {
public:
  static StatusOr<std::unique_ptr<ObservationProgram>>
  bind(const ObservationRuleDescriptor& descriptor, std::string_view implementation_identity,
       ObservationProgramDefinition definition) noexcept;
  ~ObservationProgram() noexcept;
  StatusOr<std::string> initial_state(std::int64_t at_us) const noexcept;
  StatusOr<ObservationProgramStep> step(std::string_view encoded_state, std::string_view binding_id,
                                        const FsmEvent& event,
                                        const FsmReads& reads) const noexcept;
  StatusOr<ObservationProgramStep> advance(std::string_view encoded_state, std::int64_t now_us,
                                           const FsmReads& reads) const noexcept;
  StatusOr<ObservationProgramStateInfo>
  inspect_state(std::string_view encoded_state) const noexcept;
  const ObservationProgramBounds& bounds() const noexcept;

private:
  struct Impl;
  explicit ObservationProgram(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};
} // namespace eventflow
