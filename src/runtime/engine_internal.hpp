#pragma once

// Private owner integration only. This header is not part of the installed/public SDK.
#include "eventflow/engine.hpp"
#include "eventflow/function_executor.hpp"
#include "eventflow/source_identity.hpp"

namespace eventflow {
using RuntimePrepare = std::function<StatusOr<std::vector<TableChange>>(RoundContext&)>;
using RuntimeFinalize =
    std::function<StatusOr<std::vector<TableChange>>(RoundContext&, const std::vector<RowChange>&)>;

// Host-produced facts only. Relation changes already live in the Engine round;
// these inputs describe source/clock and accepted request transitions without
// inventing broker identities for internal events.
struct RuntimeObservationInput {
  ObservationInputKind kind = ObservationInputKind::Source;
  std::string origin;
  std::optional<RelationId> relation;

  // Internal deadline advancement addresses a rule directly, without requiring
  // an application Clock binding or fabricating a source event.
  std::optional<std::uint32_t> advance_rule;
  Row scope;
  Row values;
  std::optional<SourceIdentity> source;
  std::optional<std::int64_t> scheduled_time_us;
};

struct RuntimeCause {
  std::vector<RuntimeObservationInput> inputs;
};

using RuntimeObserver = std::function<StatusOr<std::vector<TableChange>>(
    RoundContext&, const ObservationRuleDescriptor&, const RuntimeCause&,
    const std::vector<RowChange>&)>;

struct RequestManifest;
enum class RequestPhase;
enum class CandidateOperation { Activate, Deactivate };

struct EngineRuntimeAccess {
  // Only the private lifecycle hook may stage audit rows in its triggering commit.
  static Status write_trigger_audit(RoundContext&, const TableChange&) noexcept;

  // One private executor for the exact Catalog observation graph. Each rule is
  // called once in dependency order; its sink is stabilized before the next rule.
  static Status install_observer(Engine&, std::uint32_t namespace_id, RuntimeObserver) noexcept;
  static Status stage_observer_state(RoundContext&, Mutation) noexcept;
  static StatusOr<TableChange> make_request_status_write(RoundContext&, RelationId,
                                                         const RequestManifest&,
                                                         RequestPhase) noexcept;
  static StatusOr<TableChange> make_candidate_write(RoundContext&, RelationId, const Row&,
                                                    CandidateOperation, std::int64_t expected_epoch,
                                                    const Row& payload) noexcept;
  static const EngineConfig& config(const Engine& engine) noexcept;
  static StatusOr<std::size_t> metadata_bytes(const Engine& engine) noexcept;
  static std::int64_t time_us(const Engine& engine) noexcept;
  static std::shared_ptr<IStateStore> state_store(const Engine& engine) noexcept;
  static Status attach_request_owner(Engine& engine, std::uint32_t namespace_id = 0) noexcept;
  static void detach_request_owner(Engine& engine) noexcept;
  static Status stage_request_state(RoundContext& round, Mutation mutation) noexcept;
  static StatusOr<DispatchResult> resolve_request_commit(Engine& engine) noexcept;

  // CEP prepares on a disposable overlay. Only capacity errors before publication are
  // recoverable; public input/sink permissions and the ordinary fail-stop contract remain.
  static StatusOr<DispatchResult> apply_lifecycle(Engine& engine,
                                                  const std::vector<TableChange>& inputs,
                                                  const DispatchContext& context,
                                                  const RoundHook& prepare,
                                                  const RuntimePrepare& owned_prepare = {},
                                                  const RuntimeCause& cause = {}) noexcept;
  static StatusOr<std::unique_ptr<Engine>>
  open(std::shared_ptr<IStateStore> store, CatalogSnapshot catalog, const EngineConfig& config,
       std::shared_ptr<FunctionRegistry> registry) noexcept;

  // prepare observes revision R before owned writes; this frame commits at R+1. It may return
  // new RequestCapture/FunctionView rows and RuntimeStatus mutations. finalize observes the
  // first stabilized phase and may return RuntimeStatus mutations only. The owner verifies
  // request/attempt/terminal fences before using this bridge; no worker receives this capability.
  static StatusOr<DispatchResult> apply(Engine& engine, const DispatchContext& context,
                                        const RuntimePrepare& prepare,
                                        const RuntimeFinalize& finalize = {},
                                        const RuntimeCause& cause = {}) noexcept;
};
} // namespace eventflow
