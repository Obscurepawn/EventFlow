#pragma once

#include "eventflow/types.hpp"
#include <functional>
#include <map>
#include <memory>
#include <optional>

namespace eventflow {
struct FsmEvent {
  std::string kind;
  std::string occurrence_id;
  std::int64_t time_us = 0; // Actual processing time, including delayed timeout evaluation.
  Row values;
  std::optional<std::int64_t> scheduled_time_us = {}; // Original deadline, for audit only.
};

struct FsmState {
  std::string program_id;
  std::uint64_t program_version = 0;
  std::string state;
  std::uint64_t transition_sequence = 0;
  std::int64_t time_us = 0;
  std::int64_t entered_at_us = 0;
  std::optional<std::int64_t> deadline_us;
  Row registers;
};

using FsmReads = std::map<std::string, std::optional<Row>>;

struct FsmContext {
  const FsmState& state;
  const FsmEvent& event;
  const FsmReads& reads; // Stable, declared MV reads captured by the owning round.
};

using FsmGuard = std::function<StatusOr<bool>(const FsmContext&)>;
using FsmUpdate = std::function<StatusOr<Row>(const FsmContext&)>;

struct FsmStateDefinition {
  std::string name;
  std::optional<std::int64_t> timeout_us;
};

struct FsmTransition {
  std::string from;
  std::string event; // "*" matches data events, "$timeout" matches only internal timeout.
  std::string to;
  FsmGuard guard;   // Empty means true; first matching transition wins.
  FsmUpdate update; // Empty preserves registers.
  std::string emit; // Empty produces no downstream occurrence.
};

struct FsmDefinition {
  std::string id;
  std::uint64_t version = 1; // Bump whenever callback code or declarative semantics change.
  std::vector<FsmStateDefinition> states;
  std::string initial;
  std::vector<Field> register_fields;
  Row initial_registers;
  std::vector<std::string> reads;
  std::vector<FsmTransition> transitions;
  std::string reset_event;
  std::size_t max_steps = 64;
  std::size_t max_state_bytes = 1024 * 1024;
  // Immutable compiler identity. Empty preserves legacy snapshots and definitions.
  std::string definition_identity = {};
};

struct FsmEmission {
  std::string name;
  std::string cause;
  std::uint64_t transition_sequence = 0;
  std::int64_t time_us = 0;
  std::optional<std::int64_t> scheduled_time_us = {};
};

struct FsmStep {
  FsmState state;
  std::vector<FsmEmission> emissions;
};

// This pure operator never commits state or sends messages. Its result belongs in the same
// round as source progress and captured request inputs; only committed output is dispatched.
class FsmProgram {
public:
  static StatusOr<std::unique_ptr<FsmProgram>> create(FsmDefinition definition) noexcept;
  StatusOr<FsmState> initial_state(std::int64_t time_us = 0) const noexcept;
  StatusOr<FsmStep> step(const FsmState& state, const FsmEvent& event,
                         const FsmReads& reads = {}) const noexcept;
  StatusOr<FsmStep> advance(const FsmState& state, std::int64_t time_us,
                            const FsmReads& reads = {}) const noexcept;
  StatusOr<std::string> encode_state(const FsmState& state) const noexcept;
  StatusOr<FsmState> decode_state(std::string_view bytes) const noexcept;

private:
  explicit FsmProgram(FsmDefinition definition) noexcept;
  Status validate_state(const FsmState& state) const noexcept;
  Status validate_reads(const FsmReads& reads) const noexcept;
  Status transition(FsmStep& output, const FsmEvent& event, const FsmReads& reads,
                    std::size_t& steps) const noexcept;
  FsmDefinition definition_;
};

// Combinators preserve short-circuiting and propagate the first ordinary error.
FsmGuard all_guards(std::vector<FsmGuard> guards) noexcept;
FsmGuard any_guards(std::vector<FsmGuard> guards) noexcept;
FsmGuard not_guard(FsmGuard guard) noexcept;
FsmDefinition on_event(std::string id, std::string event, FsmGuard guard = {},
                       bool once = false) noexcept;
FsmDefinition crossing(std::string id, FsmGuard predicate) noexcept;
FsmDefinition followed_by(std::string id, std::string first, std::string second,
                          FsmGuard first_guard = {}, FsmGuard second_guard = {},
                          std::optional<std::int64_t> within_us = {},
                          bool strict_next = false) noexcept;
} // namespace eventflow
