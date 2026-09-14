#pragma once

#include "eventflow/fsm.hpp"
#include <memory>

namespace eventflow {

// Strict is raw-stream adjacency. NextOfKind ignores other kinds, but the first matching
// kind ends the path when its guard rejects. FollowedBy may skip guard-rejected events.
enum class CepContiguity { Strict, FollowedBy, FollowedByAny, NextOfKind };
enum class CepSkipStrategy { NoSkip, SkipToNext, SkipPastLastEvent };
enum class CepAction { Take, Ignore, Proceed };

struct CepEvent {
  std::string id; // Stable caller-provided identity; transport dedup belongs to the owner.
  std::string kind;
  std::int64_t time_us = 0;
  Row values;
};

struct CepCapture {
  std::string label;
  CepEvent event;
};

struct CepContext {
  const CepEvent& event;
  const Row& registers;
  const std::vector<CepCapture>& history; // Prior TAKEs in this path; excludes current event.
  const FsmReads& reads;                  // One immutable current-view snapshot for this step.
};

using CepGuard = std::function<StatusOr<bool>(const CepContext&)>;
using CepUpdate = std::function<StatusOr<Row>(const CepContext&)>;

struct CepPatternNode;

class CepPattern {
public:
  CepPattern() noexcept = default;
  static CepPattern event(std::string label, std::string kind, CepGuard guard = {},
                          CepUpdate update = {}) noexcept;
  CepPattern then(CepPattern next,
                  CepContiguity contiguity = CepContiguity::FollowedBy) const noexcept;
  CepPattern or_else(CepPattern alternative) const noexcept;
  CepPattern optional() const noexcept;
  // Repeated bodies must be non-nullable; a missing maximum is explicitly unsupported.
  CepPattern repeat(std::size_t minimum, std::optional<std::size_t> maximum,
                    CepContiguity inner = CepContiguity::FollowedBy) const noexcept;
  // These explicit declarations are rejected by create until their semantics are implemented.
  CepPattern greedy() const noexcept;
  CepPattern negate() const noexcept;

private:
  explicit CepPattern(std::shared_ptr<const CepPatternNode> node) noexcept;
  std::shared_ptr<const CepPatternNode> node_;
  friend class CepProgram;
};

// Count/byte limits cover retained state, staged sharing and returned output. Accepted states
// conservatively reserve enough work/output for a later complete timeout drain. Budgets are
// logical charges, not a process RSS guarantee; caller-retained prior snapshots are external.
struct CepLimits {
  std::size_t max_nodes = 512;
  std::size_t max_paths = 1024;
  std::size_t max_history_nodes = 4096;
  std::size_t max_events = 2048;
  std::size_t max_pending_matches = 1024;
  std::size_t max_matches_per_step = 256;
  std::size_t max_timeouts_per_step = 1024;
  std::size_t max_events_per_match = 64;
  std::size_t max_work_per_step = 65536;
  std::size_t max_event_bytes = 64 * 1024;
  std::size_t max_register_bytes = 16 * 1024;
  std::size_t max_read_bytes = 1024 * 1024;
  std::size_t max_output_bytes = 2 * 1024 * 1024;
  std::size_t max_state_bytes = 8 * 1024 * 1024;
};

struct CepDefinition {
  std::string id;
  std::uint64_t version = 1; // Change when callback behavior or declarative semantics change.
  CepPattern pattern;
  std::int64_t within_us = 0; // Required positive processing-time bound, [start, deadline).
  CepSkipStrategy skip = CepSkipStrategy::NoSkip;
  std::vector<std::string> reads;
  std::vector<Field> register_fields;
  Row initial_registers;
  CepLimits limits;
  // Definition metadata is shared by the program, never copied into path registers.
  std::string definition_identity = {};
};

struct CepStateData;

class CepState {
public:
  CepState() noexcept = default;

private:
  explicit CepState(std::shared_ptr<const CepStateData> data) noexcept;
  std::shared_ptr<const CepStateData> data_;
  friend class CepProgram;
};

struct CepMatch {
  std::string id;
  std::vector<CepCapture> events;
  Row registers;
  FsmReads reads;
  std::int64_t matched_at_us = 0;
  std::int64_t emitted_at_us = 0;
};

struct CepTimeout {
  std::string path_id;
  std::vector<CepCapture> events;
  std::int64_t scheduled_time_us = 0;
  std::int64_t time_us = 0;
};

struct CepStep {
  CepState state;
  std::vector<CepMatch> matches;
  std::vector<CepTimeout> timeouts;
};

struct CepPathSummary {
  std::string id;
  std::uint32_t node = 0;
  Row registers;
  std::vector<CepCapture> history;
  std::int64_t deadline_us = 0;
};

struct CepStateSummary {
  std::int64_t time_us = 0;
  std::uint64_t event_sequence = 0;
  std::optional<std::int64_t> next_deadline_us;
  std::size_t history_nodes = 0;
  std::size_t event_records = 0;
  std::size_t pending_matches = 0;
  std::vector<CepPathSummary> paths;
};

// Pure bounded E-NFA. All returned state and output remain staging until the owner commits.
// Failure never changes the input state; UNKNOWN handling and source ACK remain owner duties.
class CepProgram {
public:
  static StatusOr<std::unique_ptr<CepProgram>> create(CepDefinition definition) noexcept;
  ~CepProgram() noexcept;
  StatusOr<CepState> initial_state(std::int64_t time_us = 0) const noexcept;
  StatusOr<CepStep> step(const CepState& state, const CepEvent& event,
                         const FsmReads& reads = {}) const noexcept;
  StatusOr<CepStep> advance(const CepState& state, std::int64_t time_us) const noexcept;
  StatusOr<std::string> encode_state(const CepState& state) const noexcept;
  StatusOr<CepState> decode_state(std::string_view bytes) const noexcept;
  StatusOr<CepStateSummary> inspect(const CepState& state) const noexcept;
  std::string_view fingerprint() const noexcept;

private:
  struct Impl;
  explicit CepProgram(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace eventflow
