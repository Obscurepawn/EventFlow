#pragma once

#include "eventflow/cep.hpp"
#include "eventflow/engine.hpp"
#include "eventflow/fsm.hpp"
#include "eventflow/request_runtime.hpp"
#include "eventflow/source_identity.hpp"
#include "eventflow/trigger_audit.hpp"
#include <map>

namespace eventflow {

struct ProcessorEvent {
  SourceIdentity source;
  Row scope;
  std::string kind;
  Row values;
  std::vector<TableChange> changes;
  std::int64_t processing_time_us = 0;
};

struct ProcessedPosition {
  std::string source_id;
  std::string topic;
  std::int32_t partition = 0;
  // This shard's largest accepted offset + 1; not a global broker acknowledgement frontier.
  std::int64_t next_offset = 0;
};

struct TriggerOccurrence {
  std::string id;
  Row scope;
  std::string rule_id;
  std::uint64_t rule_version = 0;
  FsmEmission emission;
  FsmReads reads; // Immutable values observed by this transition, not a later lookup.
  std::optional<SourceIdentity> source;
  std::string event_kind;
  Row event_values;
  std::uint64_t skipped_slots = 0;
  std::int64_t evaluated_at_us = 0;
  // CEP output owns the immutable full match; the shared outbox uses the same ACK protocol.
  std::optional<CepMatch> cep_match = {};
};

struct OccurrenceSink {
  RelationId relation = 0;
  // Construct a full typed row in an independent base relation. Called inside the Engine hook;
  // do not call Engine public methods, mutate external state, or perform external I/O here.
  std::function<StatusOr<Row>(const TriggerOccurrence&)> make_row;
};

struct ProcessorRule {
  FsmDefinition definition;
  std::map<std::string, RelationId> reads;
  std::optional<OccurrenceSink> sink;
};

struct ProcessorCepRule {
  CepDefinition definition;
  std::map<std::string, RelationId> reads;
  std::optional<OccurrenceSink> sink;
  // Empty retains the full host event stream. Otherwise, only these kinds consume CEP input;
  // excluded inputs still advance processing time and release expired or pending matches.
  std::vector<std::string> input_kinds = {};
};

struct TtlPolicy {
  RelationId relation = 0;
  std::int64_t ttl_us = 0;
};

struct PeriodicPolicy {
  std::string id;
  std::string event_kind = "clock";
  std::int64_t interval_us = 0;
  std::int64_t phase_us = 0;
};

struct PeriodicPosition {
  std::string policy_id;
  Row scope;
  std::int64_t next_slot_us = 0;
  std::optional<std::int64_t> last_slot_us;
  std::uint64_t skipped_slots = 0;
};

struct ClockFiring {
  std::string policy_id;
  Row scope;
  std::int64_t slot_us = 0;
  std::uint64_t skipped_slots = 0;
};

struct CandidateActivation {
  std::int64_t expected_epoch = 0;
  Row payload;
};

struct CandidateExpiryPolicy {
  RelationId relation = 0;
  // A non-null Int64 field in Candidate payload, expressed in processing-time microseconds.
  std::string deadline_column;
  std::uint64_t version = 1;
  // Pure SDK policy: identify revisions/duplicates before requesting a new epoch. No I/O,
  // external side effects, exceptions or Engine calls are permitted inside owner callbacks.
  std::function<StatusOr<std::optional<CandidateActivation>>(RoundContext&, const ProcessorEvent&)>
      on_source;
};
enum class RequestTriggerMode { Wait, TryOnceAtFiring };
enum class RequestPendingReason {
  None,
  NotReady,
  NoPrerequisite,
  NoCapacity,
  Expired,
  Replaced,
  OnceConsumed
};

struct RequestBindingProgress {
  std::string binding_id;
  bool accepted = false;
  bool expired = false;
  std::uint64_t skipped_count = 0;
  std::optional<std::int64_t> last_slot_or_time;
  RequestPendingReason reason = RequestPendingReason::None;
};

struct CandidateProgress {
  Row scope;
  std::int64_t candidate_epoch = 0;
  bool active = false;
  std::int64_t started_at_us = 0;
  std::int64_t deadline_us = 0;
  std::vector<RequestBindingProgress> bindings;
};

struct RequestTriggerBinding {
  std::string id;
  std::uint64_t version = 1;
  std::string rule_id;
  std::string emission_name;
  std::string plan;
  bool once_per_candidate = false;
  std::int64_t pending_lifetime_us = 0;
  std::function<StatusOr<RequestAdmission>(RoundContext&, const TriggerOccurrence&)> make_admission;
  // Optional candidate-local once binding that must already have been admitted.
  std::string required_accepted_binding = {};
  RequestTriggerMode mode = RequestTriggerMode::Wait;
  // Optional absolute pending-intent cap from a non-null Int64 Candidate payload field.
  std::string deadline_column = {};
};

struct LevelRequestBinding {
  std::string id;
  std::uint64_t version = 1;
  std::string plan;
  std::int64_t pending_lifetime_us = 0;
};

struct RequestProcessorConfig {
  std::shared_ptr<FunctionRegistry> registry;
  std::vector<RequestPlanDefinition> plans;
  RequestRuntimeConfig runtime = []() noexcept {
    RequestRuntimeConfig value;
    value.max_record_bytes = 1024 * 1024;
    value.max_index_bytes = 128 * 1024;
    return value;
  }();
  std::vector<RequestTriggerBinding> trigger_bindings;
  std::vector<LevelRequestBinding> level_bindings;
  std::optional<CandidateExpiryPolicy> candidate;
  // Includes outbox admission intents and the separately reserved per-candidate level slots.
  std::size_t max_pending_intents = 1024;
};

struct RequestKey {
  Row scope;
  std::string request_id;
};
enum class ProcessorOperationKind {
  Source,
  Advance,
  Acknowledge,
  AdmitOccurrence,
  CompleteRequest,
  CancelRequest,
  ClaimTasks,
  Pump
};
enum class PendingCommitOwner { Lifecycle, Request };

struct PendingOperationInfo {
  ProcessorOperationKind kind = ProcessorOperationKind::Advance;
  PendingCommitOwner commit_owner = PendingCommitOwner::Lifecycle;
  std::int64_t fixed_now_us = 0;
  bool awaiting_commit = false;
};
enum class RequestDisposition { Accepted, Updated, Deferred, Duplicate, Ignored, Expired, Skipped };

struct RequestOperationResult {
  RequestKey key;
  RequestDisposition disposition = RequestDisposition::Ignored;
  std::optional<RequestRecord> record;
  Status reason = {};
};

struct ProcessorConfig {
  // A private extension-state object ID; must not collide with a Catalog aggregate state ID.
  std::uint32_t namespace_id = 0xffff0000U;
  std::vector<RelationId> input_relations;
  std::vector<ProcessorRule> rules;
  std::vector<TtlPolicy> ttl_policies;
  // Per observed scope. No backfill before scope activation; overdue slots coalesce to the
  // latest slot <= advance target. Clock event values contain the skipped-slot count (Int64).
  std::vector<PeriodicPolicy> periodic_policies;
  std::size_t max_keys = 1024;
  std::size_t max_timers = 4096;
  std::size_t max_source_partitions = 64;
  std::size_t max_dedup_entries = 1024;
  std::size_t max_dedup_bytes = 512 * 1024;
  std::size_t max_event_bytes = 64 * 1024;
  // Leave room for terminal request records, indices and relation updates in a remote batch.
  std::size_t max_state_bytes = 1024 * 1024;
  std::size_t max_occurrences = 1024;
  std::size_t max_occurrence_bytes = 16 * 1024;
  std::size_t max_outbox_bytes = 512 * 1024;
  std::size_t max_rounds_per_call = 1024;
  std::vector<ProcessorCepRule> cep_rules;
  std::optional<RequestProcessorConfig> requests = {};
  TriggerAuditConfig trigger_audit = {};
};

struct CommittedRound {
  std::uint64_t revision = 0;
  std::int64_t processing_time_us = 0;
  std::vector<RowChange> changes;
  std::vector<TriggerOccurrence> occurrences;
  std::vector<ClockFiring> clock_firings;
  std::vector<FunctionTask> tasks = {};
  std::vector<std::string> cancel_task_ids = {};
  std::vector<RequestOperationResult> requests = {};
};

struct ProcessResult {
  Status operation_status;
  CommitResult commit;
  bool input_committed = false;
  bool duplicate = false;
  // These subrounds are already durable even when the later operation is incomplete.
  // resolve_pending_commit returns only newly confirmed rounds, never repeats these entries.
  std::vector<CommittedRound> committed_rounds;
  std::optional<RequestOperationResult> request_result = {};
  std::optional<PendingOperationInfo> pending = {};
  bool work_remaining = false;
};

// One serialized owner per shard. This component owns its Engine and its private runtime state.
// Every timer subround is individually atomic; a process call spanning subrounds is not a global
// transaction. Source acknowledgement remains the caller's responsibility after input_committed.
class EventProcessor {
public:
  // Validate the processor's catalog/config contracts without opening an Engine or store.
  // Store capacity and RequestRuntime plan binding are still checked by open.
  static Status validate_definition(CatalogSnapshot catalog, EngineConfig engine_config,
                                    ProcessorConfig config) noexcept;
  static StatusOr<std::unique_ptr<EventProcessor>> open(std::shared_ptr<IStateStore> store,
                                                        CatalogSnapshot catalog,
                                                        EngineConfig engine_config,
                                                        ProcessorConfig config) noexcept;
  ~EventProcessor() noexcept;
  StatusOr<ProcessResult> process(const ProcessorEvent& event) noexcept;
  StatusOr<ProcessResult> advance(std::int64_t time_us) noexcept;
  StatusOr<ProcessResult> resolve_pending_commit() noexcept;
  StatusOr<ProcessResult> admit_occurrence(std::string_view occurrence_id,
                                           std::int64_t now_us) noexcept;
  StatusOr<ProcessResult> complete_request(const RequestKey& key,
                                           const FunctionCompletion& completion,
                                           std::int64_t now_us) noexcept;
  StatusOr<ProcessResult> cancel_request(const RequestKey& key, std::int64_t now_us) noexcept;
  StatusOr<ProcessResult> claim_tasks(const RequestKey& key, std::int64_t now_us,
                                      std::size_t limit = 16) noexcept;
  StatusOr<ProcessResult> pump(std::int64_t now_us) noexcept;
  StatusOr<std::optional<PendingOperationInfo>> pending_operation() const noexcept;
  StatusOr<RequestRecord> read_request(const RequestKey& key) noexcept;
  StatusOr<CandidateProgress> read_candidate(const Row& scope) const noexcept;
  StatusOr<std::vector<RequestRecord>> active_requests() const noexcept;
  StatusOr<std::vector<FunctionTask>> pending_tasks(std::size_t limit) const noexcept;
  StatusOr<std::vector<TriggerOccurrence>> pending_occurrences(std::size_t limit) const noexcept;
  // Consume only after downstream durable acceptance. UNKNOWN retains the same Engine batch.
  StatusOr<ProcessResult> acknowledge_occurrences(const std::vector<std::string>& ids) noexcept;
  StatusOr<std::vector<ProcessedPosition>> processed_positions() const noexcept;
  StatusOr<std::vector<PeriodicPosition>> periodic_positions() const noexcept;
  StatusOr<FsmState> read_fsm(const std::string& rule_id, const Row& scope) const noexcept;
  StatusOr<CepStateSummary> read_cep(const std::string& rule_id, const Row& scope) const noexcept;
  StatusOr<QueryResult> query_sql(std::string_view sql, const QueryContext& context = {}) noexcept;
  std::int64_t processing_time_us() const noexcept;
  const EngineConfig& engine_config() const noexcept;
  bool has_pending_commit() const noexcept;
  bool failed() const noexcept;

private:
  struct Impl;
  explicit EventProcessor(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace eventflow
