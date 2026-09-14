#pragma once

#include "eventflow/message_source.hpp"
#include <functional>

namespace eventflow {

struct SourceApplyResult {
  bool input_committed = false;
  Status operation_status;
};

using SourceApply = std::function<StatusOr<SourceApplyResult>(const SourceMessage&, std::int64_t)>;

struct SourcePollerConfig {
  std::size_t max_message_bytes = 1024 * 1024;
};

struct SourcePollerProgress {
  std::uint64_t polled = 0;
  std::uint64_t acknowledged = 0;
  bool paused = false;
  bool pending = false;
  bool input_committed = false;
  bool failed = false;
  std::optional<std::int64_t> pending_time_us;
  Status operation_status;
};

// The database service serializes step/pause/restore with its deployment gate.
// One retained delivery bounds memory and preserves its time and identity on retry.
class SourcePoller {
public:
  static StatusOr<std::unique_ptr<SourcePoller>>
  open(std::shared_ptr<IMessageSource> source, SourceApply apply,
       const SourcePollerConfig& config = {}) noexcept;
  ~SourcePoller() noexcept;

  StatusOr<SourcePollerProgress> step(std::int64_t now_us) noexcept;
  void pause() noexcept;
  void resume() noexcept;
  const SourcePollerProgress& progress() const noexcept;
  Status close() noexcept;

private:
  SourcePoller() noexcept = default;
  std::shared_ptr<IMessageSource> source_;
  SourceApply apply_;
  SourcePollerConfig config_;
  SourcePollerProgress progress_;
  std::optional<SourceMessage> pending_;
  std::int64_t pending_time_us_ = 0;
  bool closed_ = false;
};

} // namespace eventflow
