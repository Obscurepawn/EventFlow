#include "eventflow/function_executor.hpp"
#include "eventflow/time.hpp"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <condition_variable>
#include <deque>
#include <list>
#include <map>
#include <mutex>
#include <pthread.h>
#include <set>

namespace eventflow {
namespace {
Status invalid(std::string_view message) noexcept {
  return {ErrorCode::InvalidArgument, message};
}

Status exhausted(std::string_view message) noexcept {
  return {ErrorCode::ResourceExhausted, message};
}

bool valid_name(std::string_view text, std::size_t limit = 256) noexcept {
  return !text.empty() && text.size() <= limit && valid_utf8(text) &&
         text.find('\0') == std::string_view::npos;
}

Status validate_fields(const std::vector<Field>& fields) noexcept {
  if (fields.empty() || fields.size() > 256)
    return invalid("function fields must contain 1..256 entries");

  std::set<std::uint32_t> ids;
  std::set<std::string> names;
  for (const auto& field : fields)
    if (!valid_name(field.name) || !ids.insert(field.id).second ||
        !names.insert(field.name).second || data_type_name(field.type).empty())
      return invalid("invalid or duplicate function field");

  return {};
}

Json fields_json(const std::vector<Field>& fields) noexcept {
  Json result = Json::array();
  for (const auto& field : fields)
    result.push_back(
        Json::array({field.id, field.name, static_cast<int>(field.type), field.nullable}));

  return result;
}

StatusOr<std::string> signature_bytes(const FunctionSignature& signature) noexcept {
  if (!valid_name(signature.function_id) || signature.version == 0 ||
      !valid_name(signature.implementation_identity, 512) || signature.inputs.size() > 64)
    return invalid("invalid builtin function identity or input count");

  EF_RETURN_IF_ERROR(validate_fields(signature.output_fields));
  Json json{{"id", signature.function_id},
            {"version", signature.version},
            {"implementation", signature.implementation_identity},
            {"pure", signature.pure},
            {"inputs", Json::array()},
            {"output", fields_json(signature.output_fields)}};
  std::set<std::string> aliases;

  for (const auto& input : signature.inputs) {
    if (!valid_name(input.alias) || !aliases.insert(input.alias).second)
      return invalid("duplicate or invalid function input alias");

    EF_RETURN_IF_ERROR(validate_fields(input.fields));
    json["inputs"].push_back(Json::array({input.alias, input.required, fields_json(input.fields)}));
  }

  return json.dump();
}

std::string function_key(std::string_view id, std::uint64_t version) noexcept {
  return Json::array({id, version}).dump();
}
} // namespace

struct FunctionRegistry::Impl {
  FunctionRegistryConfig config;
  mutable std::mutex mutex;
  std::map<std::string, std::shared_ptr<const BuiltinFunction>, std::less<>> functions;
  std::size_t bytes = 0;

  explicit Impl(const FunctionRegistryConfig& value) noexcept : config(value) {}
};

FunctionRegistry::FunctionRegistry(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}

FunctionRegistry::~FunctionRegistry() noexcept = default;

StatusOr<std::shared_ptr<FunctionRegistry>>
FunctionRegistry::create(const FunctionRegistryConfig& config) noexcept {
  if (config.max_functions == 0 || config.max_signature_bytes == 0)
    return invalid("registry budgets must be positive");

  return std::shared_ptr<FunctionRegistry>(new FunctionRegistry(std::make_unique<Impl>(config)));
}

Status FunctionRegistry::add(BuiltinFunction function) noexcept {
  if (!function.callback)
    return invalid("builtin function needs an implementation");

  EF_ASSIGN_OR_RETURN(auto encoded, signature_bytes(function.signature));
  const auto key = function_key(function.signature.function_id, function.signature.version);
  std::lock_guard lock(impl_->mutex);
  if (impl_->functions.contains(key))
    return {ErrorCode::Conflict, "function version is immutable and already registered"};
  if (impl_->functions.size() == impl_->config.max_functions ||
      encoded.size() > impl_->config.max_signature_bytes - impl_->bytes)
    return exhausted("builtin registry capacity exceeded");

  impl_->bytes += encoded.size();
  impl_->functions.emplace(key, std::make_shared<const BuiltinFunction>(std::move(function)));
  return {};
}

StatusOr<FunctionSignature> FunctionRegistry::describe(std::string_view id,
                                                       std::uint64_t version) const noexcept {
  if (!valid_name(id) || version == 0)
    return invalid("invalid function lookup identity");

  std::lock_guard lock(impl_->mutex);
  const auto found = impl_->functions.find(function_key(id, version));
  if (found == impl_->functions.end())
    return Status(ErrorCode::NotFound, "function version not registered");

  return FunctionSignature{found->second->signature};
}

StatusOr<std::shared_ptr<FunctionRegistry>> FunctionRegistry::clone() const noexcept {
  std::lock_guard lock(impl_->mutex);
  auto copy = std::make_unique<Impl>(impl_->config);
  copy->functions = impl_->functions;
  copy->bytes = impl_->bytes;

  return std::shared_ptr<FunctionRegistry>(new FunctionRegistry(std::move(copy)));
}

namespace {
Status charge(std::size_t& bytes, std::size_t amount, std::size_t maximum) noexcept {
  if (bytes > maximum || amount > maximum - bytes)
    return exhausted("function byte budget exceeded");

  bytes += amount;
  return {};
}

StatusOr<std::size_t> validate_row(const Row& row, const std::vector<Field>& fields,
                                   std::size_t maximum, bool retained = false) noexcept {
  if (row.size() != fields.size())
    return invalid("function row arity does not match signature");

  std::size_t bytes = sizeof(Row);
  const auto slots = retained ? row.capacity() : row.size();
  if (maximum < bytes || slots > (maximum - bytes) / sizeof(Value))
    return exhausted("function row exceeds byte budget");

  bytes += slots * sizeof(Value);
  for (std::size_t i = 0; i < row.size(); ++i) {
    const auto& value = row[i];
    if (value.index() == 0) {
      if (!fields[i].nullable)
        return invalid("function nonnullable field is NULL");

      continue;
    }

    const auto type = fields[i].type;
    const auto expected = type == DataType::Bool                                           ? 1U
                          : (type == DataType::Int64 || type == DataType::TimestampMicros) ? 2U
                          : type == DataType::Float64                                      ? 3U
                          : type == DataType::String                                       ? 4U
                                                                                           : 5U;
    if (value.index() != expected)
      return invalid("function value type does not exactly match signature");
    if (const auto* text = std::get_if<std::string>(&value)) {
      EF_RETURN_IF_ERROR(charge(bytes, retained ? text->capacity() : text->size(), maximum));
      if (retained)
        EF_RETURN_IF_ERROR(charge(bytes, 1, maximum));
      if (!valid_utf8(*text))
        return invalid("function string is not valid UTF-8");
    } else if (const auto* vector = std::get_if<std::vector<double>>(&value)) {
      const auto slots = retained ? vector->capacity() : vector->size();
      if (slots > (maximum - bytes) / sizeof(double))
        return exhausted("function vector exceeds byte budget");

      bytes += slots * sizeof(double);
      for (auto item : *vector)
        if (!std::isfinite(item))
          return invalid("function vector contains a nonfinite value");
    } else if (const auto* number = std::get_if<double>(&value)) {
      if (!std::isfinite(*number))
        return invalid("function scalar is nonfinite");
    }
  }

  return bytes;
}

StatusOr<Json> inputs_json(const FunctionInputs& inputs) noexcept {
  Json json = Json::array();
  for (const auto& input : inputs) {
    if (!input) {
      json.push_back(nullptr);
      continue;
    }

    Json row = Json::array();
    for (const auto& value : *input) {
      EF_ASSIGN_OR_RETURN(auto encoded, value_to_json(value));
      row.push_back(Json::array({value.index(), std::move(encoded)}));
    }

    json.push_back(std::move(row));
  }

  return json;
}

Status validate_executor_config(const FunctionExecutorConfig& config) noexcept {
  if (config.max_tasks == 0 || config.max_input_bytes == 0 ||
      config.max_output_bytes_per_task == 0 ||
      config.max_completion_bytes < config.max_output_bytes_per_task)
    return invalid("invalid executor task or byte capacities");

  return {};
}
enum class TaskStage { Queued, Running, Completed };

struct TaskRecord {
  FunctionTask task;
  std::shared_ptr<const BuiltinFunction> function;
  std::string payload;
  std::string cache_key;
  std::size_t input_bytes = 0;
  std::size_t completion_reservation = 0;
  TaskStage stage = TaskStage::Queued;
  bool cancellation_requested = false;
  FunctionCompletion completion;
};

struct Receipt {
  std::string task_id;
  std::string payload;
  FunctionCompletion completion;
  std::size_t bytes = 0;
};

struct CacheEntry {
  std::string key;
  std::shared_ptr<const Row> output;
  std::size_t bytes = 0;
};

std::size_t completion_size(const FunctionCompletion& completion) noexcept {
  std::size_t bytes = sizeof(FunctionCompletion) + completion.task_id.size() +
                      completion.function_id.size() + completion.implementation_identity.size() +
                      completion.output.size() * sizeof(Value);
  for (const auto& value : completion.output) {
    if (const auto* text = std::get_if<std::string>(&value))
      bytes += text->size();
    if (const auto* vector = std::get_if<std::vector<double>>(&value))
      bytes += vector->size() * sizeof(double);
  }

  return bytes;
}

FunctionCompletion completion_for(const FunctionTask& task, Status status, Row output,
                                  bool cancelled, bool cached, std::int64_t now) noexcept {
  return {.task_id = task.task_id,
          .function_id = task.function_id,
          .function_version = task.function_version,
          .implementation_identity = task.implementation_identity,
          .status = status,
          .output = std::move(output),
          .cancelled = cancelled,
          .from_cache = cached,
          .completed_at_us = now};
}
} // namespace

struct FunctionExecutorState {
  std::shared_ptr<FunctionRegistry> registry;
  FunctionExecutorConfig config;
  mutable std::mutex mutex;
  std::condition_variable ready_changed;
  std::condition_variable completed_changed;
  std::map<std::string, std::shared_ptr<TaskRecord>, std::less<>> active;
  std::deque<std::shared_ptr<TaskRecord>> queued;
  std::deque<std::shared_ptr<TaskRecord>> completed;
  std::list<Receipt> receipts;
  std::map<std::string, std::list<Receipt>::iterator, std::less<>> receipt_index;
  std::list<CacheEntry> cache;
  std::map<std::string, std::list<CacheEntry>::iterator, std::less<>> cache_index;
  FunctionExecutorMetrics counters;
  std::optional<SystemClock> clock;
  std::int64_t now_us = 0;
  bool closing = false;

  FunctionExecutorState(std::shared_ptr<FunctionRegistry> value,
                        const FunctionExecutorConfig& limits) noexcept
      : registry(std::move(value)), config(limits) {}

  StatusOr<std::shared_ptr<const BuiltinFunction>> pin_function(const FunctionTask& task) noexcept {
    std::lock_guard lock(registry->impl_->mutex);
    const auto found =
        registry->impl_->functions.find(function_key(task.function_id, task.function_version));
    if (found == registry->impl_->functions.end())
      return Status(ErrorCode::NotFound, "function version not registered");
    if (found->second->signature.implementation_identity != task.implementation_identity)
      return Status(ErrorCode::Conflict, "function implementation identity mismatch");

    return std::shared_ptr<const BuiltinFunction>{found->second};
  }

  StatusOr<std::shared_ptr<TaskRecord>> prepare(const FunctionTask& task) noexcept {
    if (!valid_name(task.task_id, 4096) || !valid_name(task.function_id) ||
        task.function_version == 0 || !valid_name(task.implementation_identity, 512) ||
        task.ready_at_us < 0)
      return invalid("invalid frozen task identity or scheduling time");

    EF_ASSIGN_OR_RETURN(auto function, pin_function(task));
    if (task.inputs.size() != function->signature.inputs.size())
      return invalid("function input count mismatch");

    std::size_t bytes = sizeof(TaskRecord);
    EF_RETURN_IF_ERROR(charge(bytes,
                              2 * task.task_id.size() + task.function_id.size() +
                                  task.implementation_identity.size(),
                              config.max_input_bytes));
    EF_RETURN_IF_ERROR(
        charge(bytes, task.inputs.size() * sizeof(std::optional<Row>), config.max_input_bytes));
    for (std::size_t i = 0; i < task.inputs.size(); ++i) {
      const auto& input = function->signature.inputs[i];
      if (!task.inputs[i]) {
        if (input.required)
          return invalid("required function input is missing");

        continue;
      }

      EF_ASSIGN_OR_RETURN(
          auto size, validate_row(*task.inputs[i], input.fields, config.max_input_bytes - bytes));
      EF_RETURN_IF_ERROR(charge(bytes, size, config.max_input_bytes));
    }

    EF_ASSIGN_OR_RETURN(auto inputs, inputs_json(task.inputs));
    auto content = Json::array({task.function_id, task.function_version,
                                task.implementation_identity, std::move(inputs)})
                       .dump();
    EF_RETURN_IF_ERROR(charge(bytes, content.size(), config.max_input_bytes));
    auto payload = Json::array({content, task.ready_at_us}).dump();
    EF_RETURN_IF_ERROR(charge(bytes, payload.size(), config.max_input_bytes));
    std::size_t completion_reservation = sizeof(FunctionCompletion);
    EF_RETURN_IF_ERROR(
        charge(completion_reservation,
               task.task_id.size() + task.function_id.size() + task.implementation_identity.size(),
               config.max_completion_bytes));
    EF_RETURN_IF_ERROR(charge(completion_reservation, config.max_output_bytes_per_task,
                              config.max_completion_bytes));
    auto record = std::make_shared<TaskRecord>();
    record->task = task;
    record->function = std::move(function);
    record->payload = std::move(payload);
    record->cache_key = std::move(content);
    record->input_bytes = bytes;
    record->completion_reservation = completion_reservation;

    return record;
  }

  Status submit(const FunctionTask& task) noexcept {
    EF_ASSIGN_OR_RETURN(auto record, prepare(task));
    std::lock_guard lock(mutex);
    if (closing)
      return {ErrorCode::FailedPrecondition, "function executor is closed"};

    const auto existing = active.find(task.task_id);
    if (existing != active.end()) {
      if (existing->second->payload != record->payload)
        return {ErrorCode::Conflict, "task identity reused with different frozen content"};

      ++counters.idempotent_submissions;
      return {};
    }

    const auto receipt = receipt_index.find(task.task_id);
    if (receipt != receipt_index.end() && receipt->second->payload != record->payload)
      return {ErrorCode::Conflict, "retained task identity reused with different content"};
    if (active.size() >= config.max_tasks ||
        record->input_bytes > config.max_input_bytes - counters.active_input_bytes ||
        record->completion_reservation >
            config.max_completion_bytes - counters.reserved_completion_bytes)
      return exhausted("function task/completion capacity is reserved by outstanding work");

    active.emplace(task.task_id, record);
    counters.active_input_bytes += record->input_bytes;
    counters.reserved_completion_bytes += record->completion_reservation;
    ++counters.submitted;
    if (receipt != receipt_index.end()) {
      record->stage = TaskStage::Completed;
      record->completion = receipt->second->completion;
      receipts.splice(receipts.begin(), receipts, receipt->second);
      completed.push_back(record);
      ++counters.receipt_replays;
      completed_changed.notify_all();
    } else {
      queued.push_back(record);
      ready_changed.notify_all();
    }

    return {};
  }

  std::int64_t sample_time_locked() noexcept {
    if (clock) {
      auto sampled = clock->now_us();
      if (sampled.ok())
        now_us = std::max(now_us, sampled.value());
    }

    return now_us;
  }

  void finish_locked(const std::shared_ptr<TaskRecord>& record, Status status, Row output,
                     bool cached, std::int64_t at) noexcept {
    if (record->cancellation_requested) {
      status = {ErrorCode::FailedPrecondition, "function task cancelled"};
      Row{}.swap(output);
    }

    record->stage = TaskStage::Completed;
    record->completion = completion_for(record->task, status, std::move(output),
                                        record->cancellation_requested, cached, at);
    completed.push_back(record);
    completed_changed.notify_all();
  }

  Status cancel(std::string_view id) noexcept {
    std::lock_guard lock(mutex);
    const auto found = active.find(id);
    if (found == active.end())
      return {ErrorCode::NotFound, "task is not active; its owner must fence late completion"};

    auto& record = found->second;
    if (record->stage == TaskStage::Completed)
      return {ErrorCode::FailedPrecondition, "task already has a terminal completion"};

    record->cancellation_requested = true;
    if (record->stage == TaskStage::Queued) {
      std::erase(queued, record);
      finish_locked(record, {}, {}, false, sample_time_locked());
    }

    ready_changed.notify_all();
    return {};
  }

  std::shared_ptr<TaskRecord> take_ready_locked(std::int64_t at) noexcept {
    for (auto it = queued.begin(); it != queued.end(); ++it) {
      if ((*it)->task.ready_at_us > at)
        continue;

      auto record = *it;
      queued.erase(it);
      record->stage = TaskStage::Running;
      return record;
    }

    return {};
  }

  void insert_cache_locked(const std::string& key, const Row& output,
                           std::size_t output_bytes) noexcept {
    if (config.max_cache_entries == 0 || config.max_cache_bytes == 0)
      return;

    std::size_t bytes = sizeof(CacheEntry);
    if (!charge(bytes, key.size(), config.max_cache_bytes).ok() ||
        !charge(bytes, key.size(), config.max_cache_bytes).ok() ||
        !charge(bytes, output_bytes, config.max_cache_bytes).ok())
      return;

    const auto old = cache_index.find(key);
    if (old != cache_index.end()) {
      counters.cache_bytes -= old->second->bytes;
      cache.erase(old->second);
      cache_index.erase(old);
    }

    while (!cache.empty() && (cache.size() >= config.max_cache_entries ||
                              bytes > config.max_cache_bytes - counters.cache_bytes)) {
      counters.cache_bytes -= cache.back().bytes;
      cache_index.erase(cache.back().key);
      cache.pop_back();
      ++counters.cache_evictions;
    }

    cache.push_front({.key = key, .output = std::make_shared<const Row>(output), .bytes = bytes});
    cache_index.emplace(key, cache.begin());
    counters.cache_bytes += bytes;
  }

  void execute(const std::shared_ptr<TaskRecord>& record, std::int64_t at) noexcept {
    std::shared_ptr<const Row> cached;
    {
      std::lock_guard lock(mutex);
      if (record->cancellation_requested) {
        finish_locked(record, {}, {}, false, sample_time_locked());
        return;
      }

      if (record->function->signature.pure && config.max_cache_entries > 0 &&
          config.max_cache_bytes > 0) {
        const auto found = cache_index.find(record->cache_key);
        if (found != cache_index.end()) {
          cached = found->second->output;
          cache.splice(cache.begin(), cache, found->second);
          ++counters.cache_hits;
        } else
          ++counters.cache_misses;
      }

      if (!cached)
        ++counters.callback_calls;
    }

    auto output =
        cached ? StatusOr<Row>(Row{*cached}) : record->function->callback(record->task.inputs);
    Status status = output.status();
    Row row;
    std::size_t output_bytes = 0;
    if (output.ok()) {
      auto valid = validate_row(output.value(), record->function->signature.output_fields,
                                config.max_output_bytes_per_task);
      if (valid.ok()) {
        // Callback containers may carry arbitrarily large spare allocations. Copying
        // builds host-owned compact values; validate retained capacity before publication.
        row = output.value();
        auto retained = validate_row(row, record->function->signature.output_fields,
                                     config.max_output_bytes_per_task, true);
        if (retained.ok())
          output_bytes = retained.value();
        else {
          status = retained.status();
          Row{}.swap(row);
        }
      } else
        status = valid.status();
    }

    std::lock_guard lock(mutex);
    at = clock ? sample_time_locked() : at;
    if (!record->cancellation_requested && status.ok() && !cached &&
        record->function->signature.pure)
      insert_cache_locked(record->cache_key, row, output_bytes);

    finish_locked(record, status, std::move(row), static_cast<bool>(cached), at);
  }

  void retain_receipt_locked(const TaskRecord& record) noexcept {
    if (config.max_receipts == 0 || config.max_receipt_bytes == 0)
      return;

    std::size_t bytes = sizeof(Receipt);
    if (!charge(bytes, 2 * record.task.task_id.size(), config.max_receipt_bytes).ok() ||
        !charge(bytes, record.payload.size(), config.max_receipt_bytes).ok() ||
        !charge(bytes, completion_size(record.completion), config.max_receipt_bytes).ok())
      return;

    const auto old = receipt_index.find(record.task.task_id);
    if (old != receipt_index.end()) {
      counters.receipt_bytes -= old->second->bytes;
      receipts.erase(old->second);
      receipt_index.erase(old);
    }

    while (!receipts.empty() && (receipts.size() >= config.max_receipts ||
                                 bytes > config.max_receipt_bytes - counters.receipt_bytes)) {
      counters.receipt_bytes -= receipts.back().bytes;
      receipt_index.erase(receipts.back().task_id);
      receipts.pop_back();
    }

    receipts.push_front({.task_id = record.task.task_id,
                         .payload = record.payload,
                         .completion = record.completion,
                         .bytes = bytes});
    receipt_index.emplace(record.task.task_id, receipts.begin());
    counters.receipt_bytes += bytes;
  }

  StatusOr<std::vector<FunctionCompletion>> poll(std::size_t maximum, int timeout,
                                                 bool wait) noexcept {
    if (maximum == 0 || timeout < 0)
      return invalid("poll requires a positive count and nonnegative timeout");

    std::unique_lock lock(mutex);
    if (wait && completed.empty() && timeout > 0)
      completed_changed.wait_for(lock, std::chrono::milliseconds(timeout), [&]() noexcept {
        return !completed.empty() || (closing && active.empty());
      });

    std::vector<FunctionCompletion> result;
    while (!completed.empty() && result.size() < maximum) {
      auto record = completed.front();
      completed.pop_front();
      retain_receipt_locked(*record);
      result.push_back(std::move(record->completion));
      counters.active_input_bytes -= record->input_bytes;
      counters.reserved_completion_bytes -= record->completion_reservation;
      active.erase(record->task.task_id);
    }

    return result;
  }

  FunctionExecutorMetrics metrics() const noexcept {
    std::lock_guard lock(mutex);
    auto result = counters;
    result.active_tasks = active.size();
    result.queued_tasks = queued.size();
    result.completed_tasks = completed.size();
    result.running_tasks = active.size() - queued.size() - completed.size();
    result.receipt_entries = receipts.size();
    result.cache_entries = cache.size();

    return result;
  }

  void request_close() noexcept {
    std::lock_guard lock(mutex);
    if (closing)
      return;

    closing = true;
    const auto at = sample_time_locked();
    for (auto& [id, record] : active) {
      (void)id;
      if (record->stage == TaskStage::Completed)
        continue;

      record->cancellation_requested = true;
      if (record->stage == TaskStage::Queued)
        finish_locked(record, {}, {}, false, at);
    }

    queued.clear();
    ready_changed.notify_all();
    completed_changed.notify_all();
  }

  void worker() noexcept {
    for (;;) {
      std::shared_ptr<TaskRecord> record;
      std::int64_t at = 0;
      {
        std::unique_lock lock(mutex);
        for (;;) {
          if (closing && queued.empty())
            return;

          at = sample_time_locked();
          record = take_ready_locked(at);
          if (record)
            break;
          if (queued.empty())
            ready_changed.wait(lock);
          else {
            auto delay = std::int64_t{1000000};
            for (const auto& queued_record : queued)
              delay = std::min(delay, queued_record->task.ready_at_us - at);

            ready_changed.wait_for(lock,
                                   std::chrono::microseconds(std::max<std::int64_t>(delay, 1)));
          }
        }
      }

      execute(record, at);
    }
  }
};

struct SimpleFunctionExecutor::Impl {
  std::shared_ptr<FunctionExecutorState> state;
  std::mutex execution_mutex;

  explicit Impl(std::shared_ptr<FunctionExecutorState> value) noexcept : state(std::move(value)) {}
};

SimpleFunctionExecutor::SimpleFunctionExecutor(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

SimpleFunctionExecutor::~SimpleFunctionExecutor() noexcept {
  impl_->state->request_close();
}

StatusOr<std::unique_ptr<SimpleFunctionExecutor>>
SimpleFunctionExecutor::create(std::shared_ptr<FunctionRegistry> registry,
                               const FunctionExecutorConfig& config) noexcept {
  if (!registry)
    return invalid("function executor needs a registry");

  EF_RETURN_IF_ERROR(validate_executor_config(config));
  auto state = std::make_shared<FunctionExecutorState>(std::move(registry), config);
  return std::unique_ptr<SimpleFunctionExecutor>(
      new SimpleFunctionExecutor(std::make_unique<Impl>(std::move(state))));
}

StatusOr<std::size_t> SimpleFunctionExecutor::run_ready(std::int64_t now,
                                                        std::size_t maximum) noexcept {
  if (now < 0 || maximum == 0)
    return invalid("invalid deterministic execution time or count");

  std::unique_lock run_lock(impl_->execution_mutex, std::try_to_lock);
  if (!run_lock.owns_lock())
    return Status(ErrorCode::FailedPrecondition, "deterministic execution is already running");

  auto state = impl_->state;
  {
    std::lock_guard lock(state->mutex);
    if (now < state->now_us)
      return invalid("deterministic time cannot move backwards");

    state->now_us = now;
  }

  std::size_t count = 0;
  while (count < maximum) {
    std::shared_ptr<TaskRecord> record;
    {
      std::lock_guard lock(state->mutex);
      record = state->take_ready_locked(now);
    }

    if (!record)
      break;

    state->execute(record, now);
    ++count;
  }

  return count;
}

Status SimpleFunctionExecutor::submit(const FunctionTask& task) noexcept {
  return impl_->state->submit(task);
}

Status SimpleFunctionExecutor::cancel(std::string_view id) noexcept {
  return impl_->state->cancel(id);
}

StatusOr<std::vector<FunctionCompletion>> SimpleFunctionExecutor::poll(std::size_t maximum,
                                                                       int timeout) noexcept {
  return impl_->state->poll(maximum, timeout, false);
}

FunctionExecutorMetrics SimpleFunctionExecutor::metrics() const noexcept {
  return impl_->state->metrics();
}

Status SimpleFunctionExecutor::close() noexcept {
  impl_->state->request_close();
  return {};
}

struct ThreadPoolFunctionExecutor::Impl {
  std::shared_ptr<FunctionExecutorState> state;
  std::vector<pthread_t> threads;
  std::mutex close_mutex;
  std::atomic<bool> joined{false};

  explicit Impl(std::shared_ptr<FunctionExecutorState> value) noexcept : state(std::move(value)) {}

  static void* entry(void* opaque) noexcept {
    std::unique_ptr<std::shared_ptr<FunctionExecutorState>> state(
        static_cast<std::shared_ptr<FunctionExecutorState>*>(opaque));
    (*state)->worker();
    return nullptr;
  }

  Status close() noexcept {
    if (joined.load())
      return {};

    // Check before taking close_mutex: a caller may hold it while joining this callback.
    for (const auto& thread : threads)
      if (pthread_equal(pthread_self(), thread))
        return {ErrorCode::FailedPrecondition, "a function callback cannot join its own executor"};

    std::lock_guard lock(close_mutex);
    if (joined.load())
      return {};

    state->request_close();
    Status result;
    for (const auto& thread : threads) {
      const auto error = pthread_join(thread, nullptr);
      if (error != 0) {
        const auto ignored = pthread_detach(thread);
        (void)ignored;
        result = {ErrorCode::Unavailable, "cannot join function worker"};
      }
    }

    joined.store(true);
    return result;
  }
};

ThreadPoolFunctionExecutor::ThreadPoolFunctionExecutor(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

ThreadPoolFunctionExecutor::~ThreadPoolFunctionExecutor() noexcept {
  const auto ignored = impl_->close();
  (void)ignored;
}

StatusOr<std::unique_ptr<ThreadPoolFunctionExecutor>>
ThreadPoolFunctionExecutor::create(std::shared_ptr<FunctionRegistry> registry,
                                   const FunctionExecutorConfig& config) noexcept {
  if (!registry || config.worker_count == 0 || config.worker_count > 256)
    return invalid("threaded executor requires registry and 1..256 workers");

  EF_RETURN_IF_ERROR(validate_executor_config(config));
  auto state = std::make_shared<FunctionExecutorState>(std::move(registry), config);
  EF_ASSIGN_OR_RETURN(auto clock, SystemClock::create());
  state->clock = std::move(clock);
  state->sample_time_locked();
  auto impl = std::make_unique<Impl>(state);
  impl->threads.reserve(config.worker_count);

  for (std::size_t i = 0; i < config.worker_count; ++i) {
    auto argument = std::make_unique<std::shared_ptr<FunctionExecutorState>>(state);
    pthread_t thread;
    const auto error = pthread_create(&thread, nullptr, &Impl::entry, argument.get());
    if (error != 0) {
      const auto ignored = impl->close();
      (void)ignored;
      return Status(ErrorCode::Unavailable, "cannot create function worker");
    }

    argument.release();
    impl->threads.push_back(thread);
  }

  return std::unique_ptr<ThreadPoolFunctionExecutor>(
      new ThreadPoolFunctionExecutor(std::move(impl)));
}

Status ThreadPoolFunctionExecutor::submit(const FunctionTask& task) noexcept {
  return impl_->state->submit(task);
}

Status ThreadPoolFunctionExecutor::cancel(std::string_view id) noexcept {
  return impl_->state->cancel(id);
}

StatusOr<std::vector<FunctionCompletion>> ThreadPoolFunctionExecutor::poll(std::size_t maximum,
                                                                           int timeout) noexcept {
  return impl_->state->poll(maximum, timeout, true);
}

FunctionExecutorMetrics ThreadPoolFunctionExecutor::metrics() const noexcept {
  return impl_->state->metrics();
}

Status ThreadPoolFunctionExecutor::close() noexcept {
  return impl_->close();
}
} // namespace eventflow
