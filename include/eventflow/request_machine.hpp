#pragma once

#include "eventflow/engine.hpp"
#include "eventflow/function_executor.hpp"

namespace eventflow {

enum class RequestPhase { Accepted, Running, Success, Failed, Cancelled };
enum class RequestNodePhase { Waiting, Ready, Running, Succeeded, Failed, Skipped };
enum class CompletionDisposition { NotApplicable, Ignored, Accepted };
enum class RequestAttemptOutcome { Succeeded, Failed, Cancelled, TimedOut };

struct RequestAttemptSettlement {
  RelationId relation = 0;
  std::uint32_t attempt = 0;
  RequestAttemptOutcome outcome = RequestAttemptOutcome::Failed;
};

struct RequestManifest {
  Row scope;
  Row identity;
  std::string request_id;
  std::uint64_t admission_seq = 0;
  std::uint64_t candidate_epoch = 0;
  std::string owner_token;
  std::uint64_t owner_epoch = 0;
  std::int64_t admitted_at_us = 0;
  std::int64_t deadline_us = 0;
  // Source inputs are captured before the admission round writes R + 1.
  // Both zero means unspecified only for isolated, storage-free fixtures.
  std::uint64_t capture_revision = 0;
  std::uint64_t admission_revision = 0;
  // begin fills empty provenance/targets from the machine's immutable Catalog.
  std::string catalog_fingerprint;
  std::vector<RelationId> targets;
};

// Structural codecs only. The owner checks its trusted key/configuration;
// RequestMachine additionally validates Catalog, domain and configured limits.
StatusOr<Json> request_manifest_to_json(const RequestManifest& manifest) noexcept;
StatusOr<RequestManifest> request_manifest_from_json(const Json& value) noexcept;

struct RequestNodeState {
  RelationId relation = 0;
  RequestNodePhase phase = RequestNodePhase::Waiting;
  std::uint32_t attempt = 0;
  std::int64_t ready_at_us = 0;
  std::int64_t started_at_us = 0;
  std::int64_t attempt_deadline_us = 0;
  std::string task_id;
  FunctionInputs frozen_inputs;
  std::string frozen_input_identity; // Exact codec witness, not an authentication secret.
  bool has_output = false;           // Succeeded with false is complete, not Waiting.
  Status error;
};

struct RequestMachineState {
  RequestManifest manifest;
  RequestPhase phase = RequestPhase::Accepted;
  std::int64_t now_us = 0;
  std::vector<RequestNodeState> nodes; // Catalog RelationId order; no editable edges.
  Status error;
};

struct RequestMachineTransition {
  RequestMachineState state;
  std::vector<FunctionTask> tasks;
  // This value does not grant write permissions. The owner stages it through
  // its protected completion path, stabilizes SQL, then refreshes this state.
  std::optional<TableChange> output;
  std::vector<std::string> cancel_task_ids;
  CompletionDisposition disposition = CompletionDisposition::NotApplicable;
  // Trusted facts captured before work/error fields are cleared. At most one
  // settlement per running node in this transition; never supplied by a worker.
  std::vector<RequestAttemptSettlement> settlements = {};
};

struct RequestMachineConfig {
  std::size_t max_nodes = 128;
  std::size_t max_state_bytes = 4 * 1024 * 1024;
  std::size_t max_frozen_input_bytes = 1024 * 1024;
  std::size_t max_output_bytes = 256 * 1024;
  std::size_t max_identifier_bytes = 1024;
  std::size_t max_running_tasks = 16;
  std::uint32_t max_attempts = 3;
  std::int64_t attempt_timeout_us = 30'000'000;
  std::int64_t retry_backoff_us = 0;
  std::int64_t max_lifetime_us = 3'600'000'000;
};

using ReadRequestRow = std::function<StatusOr<std::optional<Row>>(RelationId, const Row& identity)>;

// A pure transition machine for one fixed Catalog/target selection. Callers
// publish candidate state and row/outbox changes only after one owner commit.
// No method commits storage, executes functions, or dispatches returned tasks.
class RequestMachine {
public:
  static StatusOr<std::unique_ptr<RequestMachine>>
  create(CatalogSnapshot catalog, std::shared_ptr<FunctionRegistry> registry,
         std::vector<RelationId> targets, const RequestMachineConfig& config = {}) noexcept;
  ~RequestMachine() noexcept;

  StatusOr<RequestMachineState> begin(RequestManifest manifest) const noexcept;
  // The reader observes staged rows after the owner has stabilized SQL.
  StatusOr<RequestMachineTransition> refresh(const RequestMachineState& state,
                                             const ReadRequestRow& read_staged_row) const noexcept;
  StatusOr<RequestMachineTransition> claim_ready(const RequestMachineState& state,
                                                 std::int64_t now_us,
                                                 std::size_t max_tasks = 16) const noexcept;
  StatusOr<RequestMachineTransition> prepare_completion(const RequestMachineState& state,
                                                        const FunctionCompletion& completion,
                                                        std::int64_t now_us) const noexcept;
  StatusOr<RequestMachineTransition> advance_time(const RequestMachineState& state,
                                                  std::int64_t now_us) const noexcept;
  StatusOr<RequestMachineTransition> cancel(const RequestMachineState& state,
                                            std::int64_t now_us) const noexcept;
  StatusOr<std::vector<FunctionTask>> outbox(const RequestMachineState& state) const noexcept;
  StatusOr<std::string> encode(const RequestMachineState& state) const noexcept;
  // expected_manifest comes from the owner's trusted request key/record.
  StatusOr<RequestMachineState> decode(std::string_view bytes,
                                       const RequestManifest& expected_manifest) const noexcept;

private:
  struct Impl;
  explicit RequestMachine(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace eventflow
