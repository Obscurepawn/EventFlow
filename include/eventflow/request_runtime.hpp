#pragma once

#include "eventflow/request_machine.hpp"

namespace eventflow {

struct RequestAdmission {
  std::string plan;
  Row scope;
  std::string request_id;
  std::string trigger_id;
  std::uint64_t candidate_epoch = 0;
  std::int64_t lifetime_us = 0; // Zero selects the plan's default lifetime.
  Row arguments;
};

struct RequestCapture {
  bool ready = true;
  std::vector<TableChange> rows; // RequestCapture roots only, all at the manifest identity.
};

using CaptureRequest =
    std::function<StatusOr<RequestCapture>(RoundContext&, const RequestAdmission&)>;

struct RequestPlanDefinition {
  std::string name;
  std::uint64_t version = 1;
  std::vector<RelationId> targets;
  RequestMachineConfig machine;
  std::int64_t default_lifetime_us = 30'000'000;
  CaptureRequest capture; // Pure capture recipe, registered for this plan version.
  // Optional controlled lifecycle relation; all plans in one request domain share its binding.
  std::optional<RelationId> request_status_relation = {};
};

struct RequestRuntimeConfig {
  std::uint32_t namespace_id = 0xffff0001U;
  std::uint64_t owner_epoch = 1;
  std::size_t max_active_requests = 32;
  // Reserve each active machine's configured state bound plus its admission envelope.
  // Charging only the current encoding could admit work that can never be claimed.
  std::size_t max_active_bytes = 8 * 1024 * 1024;
  std::size_t max_record_bytes = 2 * 1024 * 1024;
  std::size_t max_index_bytes = 256 * 1024;
  std::size_t max_argument_bytes = 64 * 1024;
};

struct RequestRecord {
  RequestAdmission admission;
  RequestMachineState state;
};

struct RequestRuntimeResult {
  CommitResult commit;
  bool accepted = false;
  bool deferred = false;
  bool duplicate = false;
  std::optional<RequestRecord> record; // Only a confirmed result is exposed.
  std::vector<RowChange> changes;
  std::vector<FunctionTask> tasks;
  std::vector<std::string> cancel_task_ids;
};

// The owner can stage lifecycle/outbox/accepted-once changes in the same commit. It must
// publish its companion in-memory state only when this operation is confirmed.
using RequestCommitHook =
    std::function<StatusOr<std::vector<TableChange>>(RoundContext&, const RequestRecord&)>;

// A collaborator of one serialized Engine owner, not a second writer/Engine. The Engine must
// outlive this runtime; its store argument must be the same adapter owned by that Engine.
// Only active requests occupy the in-memory book; terminal history is fetched from KV on demand.
// Fixed request Catalogs require an explicit controlled migration; destroying this object
// does not release the versions needed by durable tasks or archived records.
class RequestRuntime {
public:
  static StatusOr<std::unique_ptr<RequestRuntime>>
  open(Engine& engine, std::shared_ptr<IStateStore> store,
       std::shared_ptr<FunctionRegistry> registry, std::vector<RequestPlanDefinition> plans,
       const RequestRuntimeConfig& config = {}) noexcept;
  ~RequestRuntime() noexcept;
  StatusOr<RequestRuntimeResult> admit(const RequestAdmission& admission, std::int64_t now_us,
                                       const RequestCommitHook& companion = {}) noexcept;
  StatusOr<RequestRuntimeResult> claim(const Row& scope, std::string_view request_id,
                                       std::int64_t now_us, std::size_t max_tasks = 16,
                                       const RequestCommitHook& companion = {}) noexcept;
  StatusOr<RequestRuntimeResult> complete(const Row& scope, std::string_view request_id,
                                          const FunctionCompletion& completion, std::int64_t now_us,
                                          const RequestCommitHook& companion = {}) noexcept;
  StatusOr<RequestRuntimeResult> advance(const Row& scope, std::string_view request_id,
                                         std::int64_t now_us,
                                         const RequestCommitHook& companion = {}) noexcept;
  StatusOr<RequestRuntimeResult> cancel(const Row& scope, std::string_view request_id,
                                        std::int64_t now_us,
                                        const RequestCommitHook& companion = {}) noexcept;
  StatusOr<RequestRuntimeResult> resolve_pending_commit() noexcept;
  StatusOr<RequestRecord> read(const Row& scope, std::string_view request_id) noexcept;
  StatusOr<std::vector<RequestRecord>> active_requests() const noexcept;
  StatusOr<std::vector<FunctionTask>> pending_tasks(std::size_t limit) const noexcept;
  bool has_pending_commit() const noexcept;

private:
  struct Impl;
  explicit RequestRuntime(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};
} // namespace eventflow
