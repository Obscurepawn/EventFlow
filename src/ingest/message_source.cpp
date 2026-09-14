#include "eventflow/message_source.hpp"
#include "source_http_protocol.hpp"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <deque>
#include <httplib.h>
#include <librdkafka/rdkafka.h>
#include <limits>

namespace eventflow {
namespace {
std::atomic<std::uint64_t> next_instance_id{1};

bool valid_text(const std::string& value, std::size_t limit) noexcept {
  return !value.empty() && value.size() <= limit && value.find('\0') == std::string::npos;
}

std::size_t message_bytes(const SourceMessage& message) noexcept {
  return (message.key ? message.key->size() : 0) + (message.payload ? message.payload->size() : 0);
}

Status invalid(std::string_view message) noexcept {
  return {ErrorCode::InvalidArgument, message};
}

Status closed_status() noexcept {
  return {ErrorCode::FailedPrecondition, "message source is closed"};
}

struct Inflight {
  DeliveryToken token;
  std::size_t bytes = 0;
  bool completed = false;
};

struct PartitionState {
  SourcePartition identity;
  std::int64_t next_offset = 0;
  std::int64_t committed = -1;
  bool paused = false;
  std::deque<Inflight> inflight;
  std::optional<DeliveryToken> last_ack;
  std::optional<DeliveryToken> pending_ack;
};

struct SourceState {
  MessageSourceConfig config;
  std::vector<PartitionState> partitions;
  std::uint64_t instance_id = next_instance_id.fetch_add(1);
  std::uint64_t next_delivery_id = 1;
  std::size_t inflight_messages = 0;
  std::size_t inflight_bytes = 0;
  std::size_t round_robin = 0;
  bool closed = false;

  explicit SourceState(const MessageSourceConfig& value) noexcept : config(value) {
    for (const auto& assignment : config.assignments) {
      PartitionState state;
      state.identity = {.source_id = config.source_id,
                        .topic = assignment.topic,
                        .partition = assignment.partition};
      state.next_offset = assignment.next_offset.value_or(0);
      partitions.push_back(std::move(state));
    }
  }

  StatusOr<std::size_t> find(const SourcePartition& partition) const noexcept {
    if (closed)
      return closed_status();

    for (std::size_t i = 0; i < partitions.size(); ++i)
      if (partitions[i].identity == partition)
        return std::size_t{i};

    return invalid("source partition is not assigned to this source");
  }

  Status validate_poll(std::size_t count, std::size_t bytes, int timeout) const noexcept {
    if (closed)
      return closed_status();
    if (count == 0 || bytes == 0 || timeout < 0)
      return invalid("poll needs positive count/bytes and a nonnegative timeout");

    return {};
  }

  bool fits(std::size_t bytes) const noexcept {
    return inflight_messages < config.max_inflight_messages &&
           bytes <= config.max_inflight_bytes - inflight_bytes;
  }

  Status deliver(SourceMessage& message, PartitionState& partition) noexcept {
    if (next_delivery_id == std::numeric_limits<std::uint64_t>::max())
      return {ErrorCode::ResourceExhausted, "delivery identifier exhausted"};

    message.token.instance_id = instance_id;
    message.token.delivery_id = next_delivery_id++;
    const auto bytes = message_bytes(message);
    partition.inflight.push_back({.token = message.token, .bytes = bytes, .completed = false});
    partition.next_offset = message.token.offset + 1;
    ++inflight_messages;
    inflight_bytes += bytes;

    return {};
  }

  void revoke(PartitionState& partition, std::int64_t offset) noexcept {
    for (const auto& record : partition.inflight) {
      --inflight_messages;
      inflight_bytes -= record.bytes;
    }

    partition.inflight.clear();
    partition.last_ack.reset();
    partition.next_offset = offset;
  }
};

Status validate_config(const MessageSourceConfig& config) noexcept {
  if (!valid_text(config.source_id, 256) || config.assignments.empty() ||
      config.assignments.size() > 1024 || config.max_message_bytes == 0 ||
      config.max_inflight_messages == 0 || config.max_inflight_bytes < config.max_message_bytes)
    return invalid("invalid source identity, assignment, or capacity");

  for (std::size_t i = 0; i < config.assignments.size(); ++i) {
    const auto& current = config.assignments[i];
    if (!valid_text(current.topic, 249) || current.partition < 0 ||
        (current.next_offset && *current.next_offset < 0))
      return invalid("invalid topic, partition, or initial offset");

    for (std::size_t j = 0; j < i; ++j)
      if (current.topic == config.assignments[j].topic &&
          current.partition == config.assignments[j].partition)
        return invalid("duplicate source assignment");
  }

  return {};
}

// Completion is tracked in delivery order, not by assuming numeric offset adjacency.
template <typename Commit>
Status acknowledge(SourceState& state, const DeliveryToken& token, Commit commit) noexcept {
  EF_ASSIGN_OR_RETURN(const auto index, state.find(token.partition));
  auto& partition = state.partitions[index];
  if (token.instance_id != state.instance_id || token.delivery_id == 0)
    return invalid("delivery token does not belong to this source instance");
  if (partition.pending_ack && *partition.pending_ack != token)
    return {ErrorCode::FailedPrecondition, "retry the unresolved acknowledgement first"};
  if (partition.last_ack && *partition.last_ack == token)
    return {};

  auto record = std::find_if(partition.inflight.begin(), partition.inflight.end(),
                             [&](const Inflight& value) noexcept { return value.token == token; });
  if (record == partition.inflight.end())
    return {ErrorCode::FailedPrecondition, "unknown, revoked, or stale delivery token"};

  record->completed = true;

  // A later delivery may finish first. Only the completed prefix can advance the broker position.
  std::size_t prefix_count = 0;
  std::int64_t next_offset = partition.committed;
  for (const auto& delivered : partition.inflight) {
    if (!delivered.completed)
      break;

    ++prefix_count;
    next_offset = std::max(next_offset, delivered.token.offset + 1);
  }

  if (next_offset > partition.committed) {
    const auto status = commit(index, next_offset);
    if (!status.ok()) {
      // Keep the records and original token until this same acknowledgement is confirmed.
      partition.pending_ack = token;
      return status;
    }

    partition.committed = next_offset;
  }

  for (std::size_t i = 0; i < prefix_count; ++i) {
    --state.inflight_messages;
    state.inflight_bytes -= partition.inflight.front().bytes;
    partition.inflight.pop_front();
  }

  partition.pending_ack.reset();
  partition.last_ack = token;
  return {};
}
} // namespace

struct MemMockMessageSource::Impl {
  SourceState state;
  MemMockSourceConfig config;
  std::vector<std::vector<SourceMessage>> logs;
  std::size_t log_messages = 0;
  std::size_t log_bytes = 0;

  explicit Impl(const MemMockSourceConfig& value) noexcept
      : state(value), config(value), logs(value.assignments.size()) {}
};

MemMockMessageSource::MemMockMessageSource(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

MemMockMessageSource::~MemMockMessageSource() noexcept = default;

StatusOr<std::unique_ptr<MemMockMessageSource>>
MemMockMessageSource::open(const MemMockSourceConfig& config) noexcept {
  EF_RETURN_IF_ERROR(validate_config(config));
  if (config.max_log_messages == 0 || config.max_log_bytes < config.max_message_bytes)
    return invalid("invalid finite log capacity");

  return std::unique_ptr<MemMockMessageSource>(
      new MemMockMessageSource(std::make_unique<Impl>(config)));
}

Status MemMockMessageSource::append(const SourcePartition& partition, std::int64_t offset,
                                    std::optional<std::string> key,
                                    std::optional<std::string> payload,
                                    std::optional<std::int64_t> timestamp) noexcept {
  EF_ASSIGN_OR_RETURN(const auto index, impl_->state.find(partition));
  auto& log = impl_->logs[index];
  if (offset < 0 || offset == std::numeric_limits<std::int64_t>::max() ||
      (!log.empty() && offset <= log.back().token.offset))
    return invalid("log offsets must be increasing, nonnegative, and incrementable");
  if ((key && key->size() > impl_->config.max_message_bytes) ||
      (payload && payload->size() > impl_->config.max_message_bytes - (key ? key->size() : 0)))
    return {ErrorCode::ResourceExhausted, "message exceeds the source record budget"};

  SourceMessage message{
      .token = {.partition = partition, .offset = offset, .instance_id = 0, .delivery_id = 0},
      .key = std::move(key),
      .payload = std::move(payload),
      .timestamp_ms = timestamp};
  const auto bytes = message_bytes(message);
  if (impl_->log_messages >= impl_->config.max_log_messages ||
      bytes > impl_->config.max_log_bytes - impl_->log_bytes)
    return {ErrorCode::ResourceExhausted, "finite source log is full"};

  log.push_back(std::move(message));
  ++impl_->log_messages;
  impl_->log_bytes += bytes;
  return {};
}

StatusOr<std::vector<SourceMessage>>
MemMockMessageSource::poll(std::size_t count, std::size_t budget, int timeout) noexcept {
  auto& state = impl_->state;
  EF_RETURN_IF_ERROR(state.validate_poll(count, budget, timeout));
  std::vector<SourceMessage> result;
  std::size_t bytes = 0;
  bool oversized = false;

  while (result.size() < count) {
    bool delivered = false;
    for (std::size_t i = 0; i < state.partitions.size(); ++i) {
      const auto index = (state.round_robin + i) % state.partitions.size();
      auto& partition = state.partitions[index];
      if (partition.paused || partition.pending_ack)
        continue;

      const auto& log = impl_->logs[index];
      const auto next =
          std::lower_bound(log.begin(), log.end(), partition.next_offset,
                           [](const SourceMessage& value, std::int64_t offset) noexcept {
                             return value.token.offset < offset;
                           });
      if (next == log.end())
        continue;

      const auto size = message_bytes(*next);
      if (size > budget - bytes) {
        oversized = oversized || size > budget;
        continue;
      }

      if (!state.fits(size))
        continue;

      SourceMessage message = *next;
      EF_RETURN_IF_ERROR(state.deliver(message, partition));
      result.push_back(std::move(message));
      bytes += size;
      state.round_robin = (index + 1) % state.partitions.size();
      delivered = true;
      break;
    }

    if (!delivered)
      break;
  }

  if (result.empty() && oversized)
    return Status(ErrorCode::ResourceExhausted, "poll byte budget cannot fit the next record");

  return result;
}

Status MemMockMessageSource::ack(const DeliveryToken& token) noexcept {
  return acknowledge(impl_->state, token,
                     [](std::size_t, std::int64_t) noexcept { return Status{}; });
}

Status MemMockMessageSource::pause(const SourcePartition& partition) noexcept {
  EF_ASSIGN_OR_RETURN(const auto index, impl_->state.find(partition));
  impl_->state.partitions[index].paused = true;
  return {};
}

Status MemMockMessageSource::resume(const SourcePartition& partition) noexcept {
  EF_ASSIGN_OR_RETURN(const auto index, impl_->state.find(partition));
  impl_->state.partitions[index].paused = false;
  return {};
}

Status MemMockMessageSource::seek(const SourcePartition& partition, std::int64_t offset) noexcept {
  EF_ASSIGN_OR_RETURN(const auto index, impl_->state.find(partition));
  if (offset < 0)
    return invalid("seek offset must be nonnegative");

  impl_->state.revoke(impl_->state.partitions[index], offset);
  return {};
}

StatusOr<std::int64_t>
MemMockMessageSource::committed_position(const SourcePartition& partition) noexcept {
  EF_ASSIGN_OR_RETURN(const auto index, impl_->state.find(partition));
  return std::int64_t{impl_->state.partitions[index].committed};
}

Status MemMockMessageSource::close() noexcept {
  impl_->state.closed = true;
  impl_->logs.clear();
  impl_->state.partitions.clear();
  return {};
}

namespace {
Status kafka_status(rd_kafka_resp_err_t error) noexcept {
  return error == RD_KAFKA_RESP_ERR_NO_ERROR
             ? Status{}
             : Status(ErrorCode::Unavailable, rd_kafka_err2str(error));
}

struct KafkaListDeleter {
  void operator()(rd_kafka_topic_partition_list_t* list) const noexcept {
    rd_kafka_topic_partition_list_destroy(list);
  }
};

using KafkaList = std::unique_ptr<rd_kafka_topic_partition_list_t, KafkaListDeleter>;

KafkaList one_partition(const SourcePartition& partition, std::int64_t offset = 0) noexcept {
  KafkaList list(rd_kafka_topic_partition_list_new(1));
  if (list)
    rd_kafka_topic_partition_list_add(list.get(), partition.topic.c_str(), partition.partition)
        ->offset = offset;

  return list;
}

Status list_status(rd_kafka_resp_err_t error, const KafkaList& list) noexcept {
  if (error != RD_KAFKA_RESP_ERR_NO_ERROR)
    return kafka_status(error);

  for (int i = 0; i < list->cnt; ++i)
    if (list->elems[i].err != RD_KAFKA_RESP_ERR_NO_ERROR)
      return kafka_status(list->elems[i].err);

  return {};
}
} // namespace

struct KafkaMessageSource::Impl {
  SourceState state;
  KafkaSourceConfig config;
  rd_kafka_t* consumer = nullptr;
  std::vector<std::optional<SourceMessage>> buffered;
  std::vector<bool> fetch_paused;
  std::vector<Status> partition_errors;
  std::size_t buffered_messages = 0;
  std::size_t buffered_bytes = 0;
  Status deferred_error;

  explicit Impl(const KafkaSourceConfig& value) noexcept
      : state(value), config(value), buffered(value.assignments.size()),
        fetch_paused(value.assignments.size()), partition_errors(value.assignments.size()) {}

  ~Impl() noexcept {
    if (consumer)
      rd_kafka_destroy(consumer);
  }

  Status sync_fetch_pause(std::size_t index) noexcept {
    const bool paused = state.partitions[index].paused || buffered[index].has_value() ||
                        state.partitions[index].pending_ack.has_value() ||
                        !partition_errors[index].ok();
    if (fetch_paused[index] == paused)
      return {};

    auto list = one_partition(state.partitions[index].identity);
    if (!list)
      return {ErrorCode::ResourceExhausted, "cannot allocate Kafka partition list"};

    const auto error = paused ? rd_kafka_pause_partitions(consumer, list.get())
                              : rd_kafka_resume_partitions(consumer, list.get());
    EF_RETURN_IF_ERROR(list_status(error, list));
    fetch_paused[index] = paused;

    return {};
  }

  StatusOr<std::int64_t> fetch_committed(std::size_t index) noexcept {
    auto list = one_partition(state.partitions[index].identity);
    if (!list)
      return Status(ErrorCode::ResourceExhausted, "cannot allocate Kafka partition list");

    EF_RETURN_IF_ERROR(
        list_status(rd_kafka_committed(consumer, list.get(), config.operation_timeout_ms), list));
    return std::int64_t{list->elems[0].offset == RD_KAFKA_OFFSET_INVALID ? -1
                                                                         : list->elems[0].offset};
  }
};

KafkaMessageSource::KafkaMessageSource(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

KafkaMessageSource::~KafkaMessageSource() noexcept {
  const auto ignored = close();
  (void)ignored;
}

StatusOr<std::unique_ptr<KafkaMessageSource>>
KafkaMessageSource::open(const KafkaSourceConfig& config) noexcept {
  EF_RETURN_IF_ERROR(validate_config(config));
  if (!valid_text(config.bootstrap_servers, 4096) || !valid_text(config.group_id, 256) ||
      config.operation_timeout_ms <= 0 || config.prefetch_kbytes == 0 ||
      config.prefetch_kbytes > static_cast<std::size_t>(std::numeric_limits<int>::max()))
    return invalid("invalid Kafka connection or queue configuration");

  auto impl = std::make_unique<Impl>(config);
  auto* conf = rd_kafka_conf_new();
  if (!conf)
    return Status(ErrorCode::ResourceExhausted, "cannot allocate Kafka configuration");

  const std::pair<const char*, std::string> settings[] = {
      {"bootstrap.servers", config.bootstrap_servers},
      {"group.id", config.group_id},
      {"enable.auto.commit", "false"},
      {"enable.auto.offset.store", "false"},
      // Missing group commits start explicitly at BEGINNING below; an invalid recovery offset
      // must fail instead of silently skipping retained input or resetting the source cursor.
      {"auto.offset.reset", "error"},
      {"enable.partition.eof", "true"},
      {"isolation.level", "read_committed"},
      {"queued.max.messages.kbytes", std::to_string(config.prefetch_kbytes)},
      {"socket.timeout.ms", std::to_string(config.operation_timeout_ms)},
      {"fetch.wait.max.ms", "50"},
      {"log_level", "0"}};
  char error[512]{};

  for (const auto& [key, value] : settings) {
    if (rd_kafka_conf_set(conf, key, value.c_str(), error, sizeof(error)) != RD_KAFKA_CONF_OK) {
      rd_kafka_conf_destroy(conf);
      return invalid(error);
    }
  }

  impl->consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, error, sizeof(error));
  if (!impl->consumer) {
    rd_kafka_conf_destroy(conf);
    return Status(ErrorCode::Unavailable, error);
  }

  EF_RETURN_IF_ERROR(kafka_status(rd_kafka_poll_set_consumer(impl->consumer)));
  KafkaList assignment(
      rd_kafka_topic_partition_list_new(static_cast<int>(config.assignments.size())));
  if (!assignment)
    return Status(ErrorCode::ResourceExhausted, "cannot allocate Kafka assignment");

  for (std::size_t i = 0; i < config.assignments.size(); ++i) {
    const auto& part = config.assignments[i];
    EF_ASSIGN_OR_RETURN(const auto committed, impl->fetch_committed(i));
    impl->state.partitions[i].committed = committed;
    auto* entry =
        rd_kafka_topic_partition_list_add(assignment.get(), part.topic.c_str(), part.partition);
    entry->offset =
        part.next_offset.value_or(committed >= 0 ? committed : RD_KAFKA_OFFSET_BEGINNING);
    impl->state.partitions[i].next_offset = std::max<std::int64_t>(entry->offset, 0);
  }

  EF_RETURN_IF_ERROR(kafka_status(rd_kafka_assign(impl->consumer, assignment.get())));
  return std::unique_ptr<KafkaMessageSource>(new KafkaMessageSource(std::move(impl)));
}

StatusOr<std::vector<SourceMessage>> KafkaMessageSource::poll(std::size_t count, std::size_t budget,
                                                              int timeout) noexcept {
  auto& state = impl_->state;
  EF_RETURN_IF_ERROR(state.validate_poll(count, budget, timeout));
  if (!impl_->deferred_error.ok()) {
    const auto error = impl_->deferred_error;
    impl_->deferred_error = {};
    return error;
  }

  std::vector<SourceMessage> result;
  std::size_t bytes = 0;
  bool oversized = false;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout);
  const auto finish_error = [&](Status status) noexcept -> StatusOr<std::vector<SourceMessage>> {
    if (result.empty())
      return status;

    impl_->deferred_error = status;
    return std::move(result);
  };
  std::size_t empty_events = 0;

  while (result.size() < count) {
    bool delivered = false;
    bool has_fetchable = false;
    for (std::size_t i = 0; i < state.partitions.size(); ++i) {
      const auto index = (state.round_robin + i) % state.partitions.size();
      auto& partition = state.partitions[index];
      if (!impl_->partition_errors[index].ok())
        return finish_error(impl_->partition_errors[index]);
      if (partition.paused || partition.pending_ack)
        continue;

      auto& buffered = impl_->buffered[index];
      if (!buffered) {
        has_fetchable = true;
        continue;
      }

      const auto size = message_bytes(*buffered);
      if (size > budget - bytes) {
        oversized = oversized || size > budget;
        continue;
      }

      if (!state.fits(size))
        continue;

      const auto status = state.deliver(*buffered, partition);
      if (!status.ok())
        return finish_error(status);

      result.push_back(std::move(*buffered));
      buffered.reset();
      --impl_->buffered_messages;
      impl_->buffered_bytes -= size;
      bytes += size;
      state.round_robin = (index + 1) % state.partitions.size();
      const auto resumed = impl_->sync_fetch_pause(index);
      if (!resumed.ok())
        return finish_error(resumed);

      delivered = true;
      break;
    }

    if (delivered)
      continue;

    // Reserve one maximum-sized record before fetching from the external queue.
    if (!has_fetchable ||
        state.inflight_messages + impl_->buffered_messages >= state.config.max_inflight_messages ||
        state.config.max_message_bytes >
            state.config.max_inflight_bytes - state.inflight_bytes - impl_->buffered_bytes)
      break;

    const auto now = std::chrono::steady_clock::now();
    const auto remaining =
        now < deadline
            ? std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count()
            : 0;
    auto* raw =
        rd_kafka_consumer_poll(impl_->consumer, result.empty() ? static_cast<int>(remaining) : 0);
    if (!raw)
      break;
    if (raw->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      const auto error = raw->err;
      rd_kafka_message_destroy(raw);
      if (error == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
        if (++empty_events > state.partitions.size() * 2 ||
            std::chrono::steady_clock::now() >= deadline)
          break;

        continue;
      }

      return finish_error(kafka_status(error));
    }

    const SourcePartition partition{.source_id = state.config.source_id,
                                    .topic = rd_kafka_topic_name(raw->rkt),
                                    .partition = raw->partition};
    auto found = state.find(partition);
    if (!found.ok()) {
      rd_kafka_message_destroy(raw);
      return finish_error(found.status());
    }

    const auto index = found.value();
    if (raw->key_len > state.config.max_message_bytes ||
        raw->len > state.config.max_message_bytes - raw->key_len || raw->offset < 0 ||
        raw->offset == std::numeric_limits<std::int64_t>::max() || impl_->buffered[index]) {
      impl_->partition_errors[index] = {
          ErrorCode::ResourceExhausted,
          "unacceptable Kafka record; explicit seek or close required"};
      rd_kafka_message_destroy(raw);
      const auto ignored = impl_->sync_fetch_pause(index);
      (void)ignored;

      return finish_error(impl_->partition_errors[index]);
    }

    SourceMessage message;
    message.token.partition = partition;
    message.token.offset = raw->offset;
    if (raw->key)
      message.key = std::string(static_cast<const char*>(raw->key), raw->key_len);
    if (raw->payload)
      message.payload = std::string(static_cast<const char*>(raw->payload), raw->len);

    const auto timestamp = rd_kafka_message_timestamp(raw, nullptr);
    if (timestamp != -1)
      message.timestamp_ms = timestamp;

    rd_kafka_message_destroy(raw);
    impl_->buffered_bytes += message_bytes(message);
    ++impl_->buffered_messages;
    impl_->buffered[index] = std::move(message);
    const auto paused = impl_->sync_fetch_pause(index);
    if (!paused.ok())
      return finish_error(paused);
  }

  if (result.empty() && oversized)
    return Status(ErrorCode::ResourceExhausted, "poll byte budget cannot fit the next record");

  return result;
}

Status KafkaMessageSource::ack(const DeliveryToken& token) noexcept {
  auto status =
      acknowledge(impl_->state, token, [&](std::size_t index, std::int64_t offset) noexcept {
        auto list = one_partition(impl_->state.partitions[index].identity, offset);
        if (!list)
          return Status(ErrorCode::ResourceExhausted, "cannot allocate Kafka commit");

        const auto result = list_status(rd_kafka_commit(impl_->consumer, list.get(), 0), list);
        return result.ok() ? Status{} : Status(ErrorCode::UnknownCommit, result.message());
      });
  const auto index = impl_->state.find(token.partition);
  if (index.ok()) {
    const auto paused = impl_->sync_fetch_pause(index.value());
    if (status.ok())
      status = paused;
  }

  return status;
}

Status KafkaMessageSource::pause(const SourcePartition& partition) noexcept {
  EF_ASSIGN_OR_RETURN(const auto index, impl_->state.find(partition));
  impl_->state.partitions[index].paused = true;
  return impl_->sync_fetch_pause(index);
}

Status KafkaMessageSource::resume(const SourcePartition& partition) noexcept {
  EF_ASSIGN_OR_RETURN(const auto index, impl_->state.find(partition));
  impl_->state.partitions[index].paused = false;
  return impl_->sync_fetch_pause(index);
}

Status KafkaMessageSource::seek(const SourcePartition& partition, std::int64_t offset) noexcept {
  EF_ASSIGN_OR_RETURN(const auto index, impl_->state.find(partition));
  if (offset < 0)
    return invalid("seek offset must be nonnegative");

  auto& state = impl_->state.partitions[index];
  if (state.pending_ack)
    return {ErrorCode::FailedPrecondition, "resolve the pending acknowledgement before seek"};

  auto list = one_partition(partition, offset);
  if (!list)
    return {ErrorCode::ResourceExhausted, "cannot allocate Kafka seek"};

  impl_->state.revoke(state, offset);
  if (impl_->buffered[index]) {
    --impl_->buffered_messages;
    impl_->buffered_bytes -= message_bytes(*impl_->buffered[index]);
    impl_->buffered[index].reset();
  }

  auto* error =
      rd_kafka_seek_partitions(impl_->consumer, list.get(), impl_->config.operation_timeout_ms);
  Status result;
  if (error) {
    result = {ErrorCode::Unavailable, rd_kafka_error_string(error)};
    rd_kafka_error_destroy(error);
  } else
    result = list_status(RD_KAFKA_RESP_ERR_NO_ERROR, list);
  impl_->partition_errors[index] = result;
  const auto paused = impl_->sync_fetch_pause(index);

  return result.ok() ? paused : result;
}

StatusOr<std::int64_t>
KafkaMessageSource::committed_position(const SourcePartition& partition) noexcept {
  EF_ASSIGN_OR_RETURN(const auto index, impl_->state.find(partition));
  return impl_->fetch_committed(index);
}

Status KafkaMessageSource::close() noexcept {
  if (impl_->state.closed)
    return {};

  impl_->state.closed = true;
  const auto error = rd_kafka_consumer_close(impl_->consumer);
  rd_kafka_destroy(impl_->consumer);
  impl_->consumer = nullptr;
  impl_->buffered.clear();
  impl_->state.partitions.clear();

  return kafka_status(error);
}

struct HttpMessageSource::Impl {
  SourceState state;
  HttpSourceConfig config;
  std::unique_ptr<httplib::Client> client;
  Status deferred_error;

  explicit Impl(const HttpSourceConfig& value) noexcept : state(value), config(value) {}

  StatusOr<Json> post(const std::string& action, const Json& body) noexcept {
    std::string bytes;
    bool too_large = false;
    bool invalid_frame = false;
    httplib::Request request;
    request.method = "POST";
    request.path = "/v1/source/" + action;
    request.body = body.dump();
    request.set_header("Content-Type", "application/json");
    request.response_handler = [&](const httplib::Response& response) noexcept {
      if (response.has_header("Transfer-Encoding") ||
          response.get_header_value_count("Content-Length") != 1 ||
          (response.has_header("Content-Encoding") &&
           response.get_header_value("Content-Encoding") != "identity")) {
        invalid_frame = true;
        return false;
      }
      const auto text = response.get_header_value("Content-Length");
      std::size_t length = 0;
      const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), length);
      invalid_frame = error != std::errc{} || end != text.data() + text.size();
      too_large = length > config.max_response_bytes;
      return !invalid_frame && !too_large;
    };
    request.content_receiver = [&](const char* data, std::size_t size, std::uint64_t,
                                   std::uint64_t) noexcept {
      if (size > config.max_response_bytes - bytes.size()) {
        too_large = true;
        return false;
      }
      bytes.append(data, size);
      return true;
    };
    const auto response = client->send(request);
    if (too_large)
      return Status(ErrorCode::ResourceExhausted, "HTTP source response exceeds its byte limit");
    if (invalid_frame)
      return invalid("HTTP source returned an invalid response frame");
    if (!response)
      return Status(ErrorCode::Unavailable, "HTTP source request was not confirmed");
    EF_ASSIGN_OR_RETURN(auto json, parse_json_exact(bytes));
    if (!json.is_object())
      return invalid("HTTP source response must be an object");
    if (response->status != 200) {
      EF_RETURN_IF_ERROR(source_http::fields(json, {"error", "error_code"}));
      if (!json["error"].is_string() || !json["error_code"].is_number_integer())
        return invalid("HTTP source error response is malformed");
      const auto& code = json["error_code"];
      if ((code.is_number_unsigned() &&
           code.get<std::uint64_t>() > static_cast<std::uint64_t>(ErrorCode::UnknownCommit)) ||
          (!code.is_number_unsigned() && code.get<std::int64_t>() < 1))
        return invalid("HTTP source error code is out of range");
      const auto value = code.get<std::int64_t>();
      if (value < 1 || value > static_cast<int>(ErrorCode::UnknownCommit))
        return invalid("HTTP source error code is out of range");
      return Status(static_cast<ErrorCode>(value), json["error"].get_ref<const std::string&>());
    }
    return json;
  }

  StatusOr<std::int64_t> position(std::size_t index) noexcept {
    EF_ASSIGN_OR_RETURN(
        auto body, post("position", source_http::identity_json(state.partitions[index].identity)));
    EF_RETURN_IF_ERROR(source_http::fields(body, {"next_offset"}));
    return source_http::decimal(body["next_offset"], true);
  }
};

HttpMessageSource::HttpMessageSource(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

HttpMessageSource::~HttpMessageSource() noexcept {
  const auto ignored = close();
  (void)ignored;
}

StatusOr<std::unique_ptr<HttpMessageSource>>
HttpMessageSource::open(const HttpSourceConfig& config) noexcept {
  EF_RETURN_IF_ERROR(validate_config(config));
  if (!valid_text(config.endpoint, 1024) || !config.endpoint.starts_with("http://") ||
      config.operation_timeout_ms <= 0 || config.operation_timeout_ms > 60000 ||
      config.max_response_bytes == 0 || config.max_response_bytes > 64 * 1024 * 1024)
    return invalid("invalid HTTP source endpoint, timeout, or response capacity");
  auto impl = std::make_unique<Impl>(config);
  impl->client = std::make_unique<httplib::Client>(config.endpoint);
  if (!impl->client->is_valid())
    return invalid("HTTP source endpoint is invalid");
  const auto seconds = config.operation_timeout_ms / 1000;
  const auto micros = (config.operation_timeout_ms % 1000) * 1000;
  impl->client->set_connection_timeout(seconds, micros);
  impl->client->set_read_timeout(seconds, micros);
  impl->client->set_write_timeout(seconds, micros);
  for (std::size_t i = 0; i < impl->state.partitions.size(); ++i) {
    EF_ASSIGN_OR_RETURN(auto position, impl->position(i));
    impl->state.partitions[i].committed = position;
    impl->state.partitions[i].next_offset =
        config.assignments[i].next_offset.value_or(position >= 0 ? position : 0);
  }
  return std::unique_ptr<HttpMessageSource>(new HttpMessageSource(std::move(impl)));
}

StatusOr<std::vector<SourceMessage>> HttpMessageSource::poll(std::size_t count, std::size_t budget,
                                                             int timeout) noexcept {
  auto& state = impl_->state;
  EF_RETURN_IF_ERROR(state.validate_poll(count, budget, timeout));
  if (!impl_->deferred_error.ok()) {
    const auto error = impl_->deferred_error;
    impl_->deferred_error = {};
    return error;
  }
  std::vector<SourceMessage> result;
  std::size_t bytes = 0;
  bool oversized = false;
  const auto finish_error = [&](Status status) noexcept -> StatusOr<std::vector<SourceMessage>> {
    if (result.empty())
      return status;
    impl_->deferred_error = status;
    return std::move(result);
  };
  // HTTP poll requests are nonblocking at the server. The service owns pacing;
  // each RPC has a separate transport timeout, even when timeout_ms is zero.
  while (result.size() < count && state.inflight_messages < state.config.max_inflight_messages) {
    bool delivered = false;
    for (std::size_t i = 0; i < state.partitions.size(); ++i) {
      const auto index = (state.round_robin + i) % state.partitions.size();
      auto& partition = state.partitions[index];
      if (partition.paused || partition.pending_ack || bytes == budget)
        continue;
      Json request = source_http::identity_json(partition.identity);
      request["next_offset"] = std::to_string(partition.next_offset);
      request["max_messages"] = std::size_t{1};
      request["max_bytes"] = std::min(budget - bytes, state.config.max_message_bytes);
      request["timeout_ms"] = 0;
      auto response = impl_->post("poll", request);
      if (!response.ok()) {
        if (response.status().code() == ErrorCode::ResourceExhausted) {
          oversized = true;
          continue;
        }
        return finish_error(response.status());
      }
      const auto valid = source_http::fields(*response, {"messages"});
      if (!valid.ok())
        return finish_error(valid);
      const auto& messages = (*response)["messages"];
      if (!messages.is_array() || messages.size() > 1)
        return finish_error(invalid("HTTP source exceeded its requested message count"));
      if (messages.empty())
        continue;
      auto decoded = source_http::decode_message(messages[0], partition.identity,
                                                 state.config.max_message_bytes);
      if (!decoded.ok())
        return finish_error(decoded.status());
      auto message = std::move(decoded).value();
      if (message.token.offset < partition.next_offset)
        return finish_error(
            invalid("HTTP source returned an offset before the requested position"));
      const auto size = message_bytes(message);
      if (size > budget - bytes) {
        oversized = true;
        continue;
      }
      if (!state.fits(size))
        continue;
      const auto accepted = state.deliver(message, partition);
      if (!accepted.ok())
        return finish_error(accepted);
      result.push_back(std::move(message));
      bytes += size;
      state.round_robin = (index + 1) % state.partitions.size();
      delivered = true;
      break;
    }
    if (!delivered)
      break;
  }
  if (result.empty() && oversized)
    return Status(ErrorCode::ResourceExhausted, "poll budget cannot fit the next HTTP record");
  return result;
}

Status HttpMessageSource::ack(const DeliveryToken& token) noexcept {
  return acknowledge(impl_->state, token, [&](std::size_t index, std::int64_t next) noexcept {
    Json request = source_http::identity_json(impl_->state.partitions[index].identity);
    request["next_offset"] = std::to_string(next);
    auto result = impl_->post("ack", request);
    if (!result.ok())
      return Status(ErrorCode::UnknownCommit, result.status().message());
    auto valid = source_http::fields(*result, {"next_offset"});
    if (!valid.ok())
      return Status(ErrorCode::UnknownCommit, valid.message());
    auto confirmed = source_http::decimal((*result)["next_offset"]);
    if (!confirmed.ok() || *confirmed < next)
      return Status(ErrorCode::UnknownCommit, "HTTP source acknowledgement was not confirmed");
    return Status{};
  });
}

Status HttpMessageSource::pause(const SourcePartition& partition) noexcept {
  EF_ASSIGN_OR_RETURN(auto index, impl_->state.find(partition));
  impl_->state.partitions[index].paused = true;
  return {};
}

Status HttpMessageSource::resume(const SourcePartition& partition) noexcept {
  EF_ASSIGN_OR_RETURN(auto index, impl_->state.find(partition));
  impl_->state.partitions[index].paused = false;
  return {};
}

Status HttpMessageSource::seek(const SourcePartition& partition, std::int64_t next) noexcept {
  EF_ASSIGN_OR_RETURN(auto index, impl_->state.find(partition));
  if (next < 0)
    return invalid("HTTP source seek offset must be nonnegative");
  auto& state = impl_->state.partitions[index];
  if (state.pending_ack)
    return Status(ErrorCode::FailedPrecondition, "resolve the HTTP acknowledgement before seek");
  impl_->state.revoke(state, next);
  impl_->deferred_error = {};
  return {};
}

StatusOr<std::int64_t>
HttpMessageSource::committed_position(const SourcePartition& partition) noexcept {
  EF_ASSIGN_OR_RETURN(auto index, impl_->state.find(partition));
  return impl_->position(index);
}

Status HttpMessageSource::close() noexcept {
  if (impl_->state.closed)
    return {};
  impl_->state.closed = true;
  impl_->state.partitions.clear();
  impl_->client->stop();
  return {};
}

} // namespace eventflow
