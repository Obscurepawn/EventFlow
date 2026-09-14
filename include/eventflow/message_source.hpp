#pragma once

#include "eventflow/status.hpp"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace eventflow {

struct SourcePartition {
  std::string source_id;
  std::string topic;
  std::int32_t partition = 0;
  bool operator==(const SourcePartition&) const noexcept = default;
};

// A token is an instance-local acknowledgement capability. The durable event identity is only
// (source_id, topic, partition, offset); instance_id and delivery_id never enter dedup storage.
struct DeliveryToken {
  SourcePartition partition;
  std::int64_t offset = 0;
  std::uint64_t instance_id = 0;
  std::uint64_t delivery_id = 0;
  bool operator==(const DeliveryToken&) const noexcept = default;
};

struct SourceMessage {
  DeliveryToken token;
  // nullopt preserves a Kafka null key / tombstone; an empty string is a value.
  std::optional<std::string> key;
  std::optional<std::string> payload;
  std::optional<std::int64_t> timestamp_ms;
};

struct SourceAssignment {
  std::string topic;
  std::int32_t partition = 0;
  // Unspecified means resume the group commit, or the earliest retained record.
  std::optional<std::int64_t> next_offset;
};

struct MessageSourceConfig {
  std::string source_id;
  std::vector<SourceAssignment> assignments;
  // Both byte budgets count raw key+payload bytes. Partition/identifier limits and message
  // counts separately bound token/envelope overhead; caller-owned returned buffers are external.
  std::size_t max_message_bytes = 4 * 1024 * 1024;
  std::size_t max_inflight_messages = 1024;
  std::size_t max_inflight_bytes = 16 * 1024 * 1024;
};

struct MemMockSourceConfig : MessageSourceConfig {
  // Append fails when this finite retained log is full; it never evicts replayable input.
  std::size_t max_log_messages = 10000;
  std::size_t max_log_bytes = 64 * 1024 * 1024;
};

struct KafkaSourceConfig : MessageSourceConfig {
  std::string bootstrap_servers;
  std::string group_id;
  int operation_timeout_ms = 5000;
  // librdkafka's prefetch queue is separate from the application inflight budget.
  std::size_t prefetch_kbytes = 16384;
};

struct HttpSourceConfig : MessageSourceConfig {
  std::string endpoint;
  int operation_timeout_ms = 1000;
  std::size_t max_response_bytes = 16 * 1024 * 1024;
};

// All operations on an instance must be serialized by its caller. No schema or engine types
// cross this boundary. poll bounds raw key+payload bytes; count limits bound envelope overhead.
class IMessageSource {
public:
  virtual ~IMessageSource() noexcept = default;
  virtual StatusOr<std::vector<SourceMessage>> poll(std::size_t max_messages, std::size_t max_bytes,
                                                    int timeout_ms) noexcept = 0;
  // Call only after durable computation. Later completions cannot skip an earlier delivery.
  // The most recent successful ack per partition can be repeated; older retired tokens may
  // return FailedPrecondition. UnknownCommit requires retrying this exact token before another
  // ack/seek on that partition. Pause does not prevent ack.
  virtual Status ack(const DeliveryToken& token) noexcept = 0;
  virtual Status pause(const SourcePartition& partition) noexcept = 0;
  virtual Status resume(const SourcePartition& partition) noexcept = 0;
  // Revokes old inflight tokens, preserves pause state, and never changes group commits.
  // Replaying an older checkpoint never lowers the already confirmed broker position.
  virtual Status seek(const SourcePartition& partition, std::int64_t next_offset) noexcept = 0;
  // The next offset after the confirmed prefix; -1 means there is no committed position.
  virtual StatusOr<std::int64_t> committed_position(const SourcePartition& partition) noexcept = 0;
  // Does not acknowledge outstanding records. Idempotent.
  virtual Status close() noexcept = 0;
};

class MemMockMessageSource final : public IMessageSource {
public:
  static StatusOr<std::unique_ptr<MemMockMessageSource>>
  open(const MemMockSourceConfig& config) noexcept;
  ~MemMockMessageSource() noexcept override;
  Status append(const SourcePartition& partition, std::int64_t offset,
                std::optional<std::string> key, std::optional<std::string> payload,
                std::optional<std::int64_t> timestamp_ms = {}) noexcept;
  // Deterministic: timeout is validated but this finite-log adapter never sleeps.
  StatusOr<std::vector<SourceMessage>> poll(std::size_t, std::size_t, int) noexcept override;
  Status ack(const DeliveryToken&) noexcept override;
  Status pause(const SourcePartition&) noexcept override;
  Status resume(const SourcePartition&) noexcept override;
  Status seek(const SourcePartition&, std::int64_t) noexcept override;
  StatusOr<std::int64_t> committed_position(const SourcePartition&) noexcept override;
  Status close() noexcept override;

private:
  struct Impl;
  explicit MemMockMessageSource(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;
};

class KafkaMessageSource final : public IMessageSource {
public:
  // Static manual assignment only: no group rebalance/automatic ownership protocol. A missing
  // group commit starts at BEGINNING; invalid explicit recovery positions report an error.
  // Oversized records halt delivery until explicit seek/close, without acknowledging the record.
  static StatusOr<std::unique_ptr<KafkaMessageSource>>
  open(const KafkaSourceConfig& config) noexcept;
  ~KafkaMessageSource() noexcept override;
  StatusOr<std::vector<SourceMessage>> poll(std::size_t, std::size_t, int) noexcept override;
  Status ack(const DeliveryToken&) noexcept override;
  Status pause(const SourcePartition&) noexcept override;
  Status resume(const SourcePartition&) noexcept override;
  Status seek(const SourcePartition&, std::int64_t) noexcept override;
  StatusOr<std::int64_t> committed_position(const SourcePartition&) noexcept override;
  Status close() noexcept override;

private:
  struct Impl;
  explicit KafkaMessageSource(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;
};

// HTTP transports raw bytes and positions; table/schema decoding belongs to the database.
// The protocol uses /v1/source/poll, /position and idempotent /ack POST operations.
class HttpMessageSource final : public IMessageSource {
public:
  static StatusOr<std::unique_ptr<HttpMessageSource>> open(const HttpSourceConfig&) noexcept;
  ~HttpMessageSource() noexcept override;
  StatusOr<std::vector<SourceMessage>> poll(std::size_t, std::size_t, int) noexcept override;
  Status ack(const DeliveryToken&) noexcept override;
  Status pause(const SourcePartition&) noexcept override;
  Status resume(const SourcePartition&) noexcept override;
  Status seek(const SourcePartition&, std::int64_t) noexcept override;
  StatusOr<std::int64_t> committed_position(const SourcePartition&) noexcept override;
  Status close() noexcept override;

private:
  struct Impl;
  explicit HttpMessageSource(std::unique_ptr<Impl>) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace eventflow
