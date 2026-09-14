#include "eventflow/computation_runtime.hpp"
#include <algorithm>
#include <chrono>

namespace eventflow {
namespace {

std::string request_key(const RequestKey& key) noexcept {
  Json scope = Json::array();
  for (const auto& value : key.scope) {
    // Request scopes have already passed Catalog validation before reaching this cache.
    auto encoded = value_to_json(value);
    if (encoded.ok())
      scope.push_back(std::move(encoded).value());
  }
  return Json::array({scope, key.request_id}).dump();
}

bool terminal(RequestPhase phase) noexcept {
  return phase == RequestPhase::Success || phase == RequestPhase::Failed ||
         phase == RequestPhase::Cancelled;
}

} // namespace

StatusOr<BoundComputation> bind_computation_module(const ComputationModule& module,
                                                   const std::vector<std::string>& ddl) noexcept {
  if (module.name.empty() || module.name.size() > 128 || module.version == 0 ||
      module.name.find('\0') != std::string::npos || !valid_utf8(module.name))
    return Status(ErrorCode::InvalidArgument, "invalid computation module identity");
  if (module.bind)
    return module.bind(ddl);
  if (module.name != "sql" || module.version != 1)
    return Status(ErrorCode::InvalidArgument, "a custom computation module needs a binder");
  EF_ASSIGN_OR_RETURN(auto catalog, Catalog::create());
  for (const auto& statement : ddl) {
    EF_ASSIGN_OR_RETURN(auto relation, catalog->define_sql(statement));
    (void)relation;
  }
  EF_ASSIGN_OR_RETURN(auto snapshot, catalog->snapshot());
  ProcessorConfig config;
  for (const auto& relation : snapshot.relations)
    if (relation->kind == RelationKind::BaseTable &&
        relation->base_write_role == BaseWriteRole::PublicInput)
      config.input_relations.push_back(relation->id);
  return BoundComputation{.catalog = std::move(snapshot),
                          .registry = nullptr,
                          .definition = std::move(config),
                          .explanation = Json::object()};
}

StatusOr<std::unique_ptr<ComputationRuntime>>
ComputationRuntime::open(std::shared_ptr<IStateStore> store, BoundComputation definition,
                         const EngineConfig& engine,
                         const ComputationRuntimeConfig& config) noexcept {
  if (!store || (definition.definition.requests && !definition.registry) ||
      config.executor.max_tasks == 0)
    return Status(ErrorCode::InvalidArgument, "invalid computation runtime configuration");
  auto result = std::unique_ptr<ComputationRuntime>(new ComputationRuntime);
  result->config_ = config;
  result->definition_ = std::move(definition);
  EF_ASSIGN_OR_RETURN(result->processor_,
                      EventProcessor::open(std::move(store), result->definition_.catalog, engine,
                                           result->definition_.definition));
  if (!result->definition_.definition.requests) {
    // SQL and occurrence-only triggers have no function work to schedule.
    return result;
  }
  if (config.threaded) {
    EF_ASSIGN_OR_RETURN(result->executor_, ThreadPoolFunctionExecutor::create(
                                               result->definition_.registry, config.executor));
  } else {
    EF_ASSIGN_OR_RETURN(auto executor, SimpleFunctionExecutor::create(result->definition_.registry,
                                                                      config.executor));
    result->simple_ = executor.get();
    result->executor_ = std::move(executor);
  }
  EF_RETURN_IF_ERROR(result->synchronize());
  return result;
}

ComputationRuntime::~ComputationRuntime() noexcept = default;

void ComputationRuntime::forget(const std::string& id) noexcept {
  task_owners_.erase(id);
  submitted_.erase(id);
}

Status ComputationRuntime::consume(ProcessResult result) noexcept {
  Status delivery_status;
  for (auto& round : result.committed_rounds) {
    for (auto& event : round.requests) {
      if (!event.record)
        continue;
      const auto key = request_key(event.key);
      for (const auto& node : event.record->state.nodes)
        if (!node.task_id.empty() && node.phase == RequestNodePhase::Running)
          task_owners_[node.task_id] = event.key;
      if (terminal(event.record->state.phase))
        active_.erase(key);
      else
        active_[key] = *event.record;
    }
    for (const auto& id : round.cancel_task_ids) {
      if (!submitted_.contains(id)) {
        forget(id);
        continue;
      }
      const auto status = executor_->cancel(id);
      if (!status.ok() && status.code() != ErrorCode::NotFound && delivery_status.ok())
        delivery_status = status;
    }
  }
  return result.operation_status.ok() ? delivery_status : result.operation_status;
}

Status ComputationRuntime::synchronize() noexcept {
  if (!definition_.definition.requests)
    return {};
  EF_ASSIGN_OR_RETURN(auto records, processor_->active_requests());
  active_.clear();
  std::set<std::string> running;
  for (auto& record : records) {
    RequestKey key{.scope = record.admission.scope, .request_id = record.admission.request_id};
    for (const auto& node : record.state.nodes)
      if (node.phase == RequestNodePhase::Running && !node.task_id.empty()) {
        task_owners_[node.task_id] = key;
        running.insert(node.task_id);
      }
    active_[request_key(key)] = std::move(record);
  }
  for (auto it = completions_.begin(); it != completions_.end();) {
    if (!running.contains(it->task_id)) {
      forget(it->task_id);
      it = completions_.erase(it);
    } else {
      ++it;
    }
  }
  for (const auto& id : submitted_)
    if (!running.contains(id)) {
      const auto status = executor_->cancel(id);
      if (!status.ok() && status.code() != ErrorCode::NotFound)
        return status;
    }
  // A claimed task can expire before executor capacity becomes available. It
  // will never produce a completion, so only still-submitted tasks retain owners.
  for (auto it = task_owners_.begin(); it != task_owners_.end();) {
    if (!running.contains(it->first) && !submitted_.contains(it->first))
      it = task_owners_.erase(it);
    else
      ++it;
  }
  return {};
}

Status ComputationRuntime::resolve_pending() noexcept {
  for (std::size_t step = 0; processor_->has_pending_commit() && step < 32; ++step) {
    EF_ASSIGN_OR_RETURN(auto resolved, processor_->resolve_pending_commit());
    const auto status = consume(std::move(resolved));
    if (!processor_->has_pending_commit())
      EF_RETURN_IF_ERROR(synchronize());
    EF_RETURN_IF_ERROR(status);
  }
  if (processor_->has_pending_commit())
    return Status(ErrorCode::UnknownCommit, "computation commit remains unconfirmed");
  return synchronize();
}

Status ComputationRuntime::claim_ready(std::int64_t now_us) noexcept {
  if (!definition_.definition.requests)
    return {};
  std::vector<RequestKey> keys;
  for (const auto& [id, record] : active_) {
    (void)id;
    if (std::any_of(record.state.nodes.begin(), record.state.nodes.end(),
                    [](const RequestNodeState& node) noexcept {
                      return node.phase == RequestNodePhase::Ready;
                    }))
      keys.push_back({.scope = record.admission.scope, .request_id = record.admission.request_id});
  }
  for (const auto& key : keys) {
    const auto active = executor_->metrics().active_tasks;
    if (active >= config_.executor.max_tasks || !completions_.empty())
      break;
    EF_ASSIGN_OR_RETURN(
        auto claimed,
        processor_->claim_tasks(key, now_us,
                                std::min<std::size_t>(16, config_.executor.max_tasks - active)));
    EF_RETURN_IF_ERROR(consume(std::move(claimed)));
  }
  EF_ASSIGN_OR_RETURN(auto pending, processor_->pending_tasks(config_.executor.max_tasks));
  for (const auto& task : pending) {
    if (submitted_.contains(task.task_id))
      continue;
    if (!task_owners_.contains(task.task_id))
      return Status(ErrorCode::FailedPrecondition, "task has no confirmed request owner");
    auto dispatched = task;
    dispatched.ready_at_us = 0;
    const auto submitted = executor_->submit(dispatched);
    if (submitted.code() == ErrorCode::ResourceExhausted)
      break;
    EF_RETURN_IF_ERROR(submitted);
    submitted_.insert(task.task_id);
  }
  return {};
}

StatusOr<RuntimeIngestResult> ComputationRuntime::ingest_source(std::string_view relation, Row row,
                                                                const SourceIdentity& source,
                                                                std::int64_t now_us) noexcept {
  EF_RETURN_IF_ERROR(resolve_pending());
  EF_ASSIGN_OR_RETURN(auto descriptor, definition_.catalog.find(std::string(relation)));
  const auto& inputs = definition_.definition.input_relations;
  if (std::find(inputs.begin(), inputs.end(), descriptor->id) == inputs.end())
    return Status(ErrorCode::InvalidArgument, "ingest requires a declared public input");
  EF_RETURN_IF_ERROR(descriptor->storage_schema.validate(row));
  EF_ASSIGN_OR_RETURN(auto scope, partition_values(*descriptor, row));
  EF_ASSIGN_OR_RETURN(auto identity, physical_identity(*descriptor, row));
  ProcessorEvent event{.source = source,
                       .scope = std::move(scope),
                       .kind = descriptor->name,
                       .values = row,
                       .changes = {{.relation = descriptor->id,
                                    .identity = std::move(identity),
                                    .row = std::move(row)}},
                       .processing_time_us = now_us};
  EF_ASSIGN_OR_RETURN(auto processed, processor_->process(event));
  RuntimeIngestResult result{.input_committed = processed.input_committed,
                             .duplicate = processed.duplicate,
                             .operation_status = {}};
  result.operation_status = consume(std::move(processed));
  if (result.operation_status.ok() && !result.duplicate)
    result.operation_status = pump(std::max(now_us, processor_->processing_time_us()));
  return result;
}

Status ComputationRuntime::pump(std::int64_t now_us) noexcept {
  EF_RETURN_IF_ERROR(resolve_pending());
  for (std::size_t round = 0; round < 32; ++round) {
    EF_ASSIGN_OR_RETURN(auto result, definition_.definition.requests ? processor_->pump(now_us)
                                                                     : processor_->advance(now_us));
    const bool progress = !result.committed_rounds.empty();
    const bool remaining = result.work_remaining;
    EF_RETURN_IF_ERROR(consume(std::move(result)));
    EF_RETURN_IF_ERROR(claim_ready(now_us));
    if (!remaining || !progress)
      return {};
  }
  return Status(ErrorCode::ResourceExhausted, "computation slice exhausted its progress budget");
}

StatusOr<std::size_t> ComputationRuntime::run_functions(std::int64_t now_us,
                                                        std::size_t limit) noexcept {
  const auto pumped = pump(now_us);
  if (!pumped.ok() &&
      (pumped.code() != ErrorCode::ResourceExhausted || processor_->has_pending_commit()))
    return pumped;
  if (!executor_)
    return std::size_t{0};
  std::size_t completed = 0;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(10);
  while (completed < limit) {
    if (!simple_ && std::chrono::steady_clock::now() >= deadline)
      break;
    if (completions_.empty()) {
      if (simple_) {
        EF_ASSIGN_OR_RETURN(auto ran, simple_->run_ready(now_us, limit - completed));
        (void)ran;
      }
      EF_ASSIGN_OR_RETURN(auto polled,
                          executor_->poll(std::min(config_.executor.max_tasks, limit - completed),
                                          simple_ ? 0 : 2));
      for (auto& completion : polled)
        completions_.push_back(std::move(completion));
      if (completions_.empty())
        break;
    }
    const auto& completion = completions_.front();
    const auto owner = task_owners_.find(completion.task_id);
    if (owner == task_owners_.end())
      return Status(ErrorCode::Internal, "function completion has no trusted request owner");
    EF_ASSIGN_OR_RETURN(auto result,
                        processor_->complete_request(owner->second, completion, now_us));
    const bool confirmed = result.request_result.has_value();
    const auto status = consume(std::move(result));
    if (confirmed) {
      const auto id = completion.task_id;
      completions_.pop_front();
      forget(id);
      ++completed;
    }
    EF_RETURN_IF_ERROR(status);
    if (!confirmed)
      return Status(ErrorCode::ResourceExhausted, "completion awaits owner progress");
    if (completions_.empty())
      EF_RETURN_IF_ERROR(pump(now_us));
  }
  return completed;
}

StatusOr<QueryResult> ComputationRuntime::query_sql(std::string_view sql) noexcept {
  return processor_->query_sql(sql);
}

StatusOr<RequestRecord> ComputationRuntime::read_request(const RequestKey& key) noexcept {
  return processor_->read_request(key);
}

const BoundComputation& ComputationRuntime::definition() const noexcept {
  return definition_;
}

EventProcessor& ComputationRuntime::processor() noexcept {
  return *processor_;
}

FunctionExecutorMetrics ComputationRuntime::executor_metrics() const noexcept {
  return executor_ ? executor_->metrics() : FunctionExecutorMetrics{};
}

const char* ComputationRuntime::executor_mode() const noexcept {
  return !executor_ ? "disabled" : simple_ ? "simple" : "thread_pool";
}

std::size_t ComputationRuntime::executor_worker_count() const noexcept {
  return !executor_ || simple_ ? 0 : config_.executor.worker_count;
}

} // namespace eventflow
