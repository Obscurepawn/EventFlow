#pragma once

#include "eventflow/status.hpp"
#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace eventflow {

class IClock {
public:
  virtual ~IClock() noexcept = default;
  virtual StatusOr<std::int64_t> now_us() noexcept = 0;
};

class ManualClock final : public IClock {
public:
  static StatusOr<ManualClock> create(std::int64_t initial_us = 0) noexcept;
  Status advance(std::int64_t time_us) noexcept;
  StatusOr<std::int64_t> now_us() noexcept override;

private:
  explicit ManualClock(std::int64_t time_us) noexcept : time_us_(time_us) {}

  std::int64_t time_us_;
};

// Wall time is sampled once. Later wall-clock corrections cannot rewind processing time.
class SystemClock final : public IClock {
public:
  static StatusOr<SystemClock> create(std::int64_t restored_us = 0) noexcept;
  StatusOr<std::int64_t> now_us() noexcept override;

private:
  SystemClock(std::int64_t anchor_us, std::chrono::steady_clock::time_point origin) noexcept;
  std::int64_t anchor_us_;
  std::chrono::steady_clock::time_point origin_;
};

struct TimerSpec {
  std::string id; // Stable owner/rule/key identity, encoded by the owning operator.
  std::uint64_t generation = 0;
  std::int64_t deadline_us = 0;
  std::int64_t period_us = 0; // Zero is a one-shot timer.
  bool operator==(const TimerSpec&) const noexcept = default;
};

struct TimerFiring {
  TimerSpec timer; // For periodic timers, deadline is the latest due slot.
  std::uint64_t skipped_slots = 0;
};

struct TimerLimits {
  std::size_t max_timers = 1024;
  std::size_t max_id_bytes = 1024 * 1024;
};

// Copy into round-local staging; publish the copy only after the owning KV batch commits.
class TimerQueue {
public:
  static StatusOr<TimerQueue> create(const TimerLimits& limits = {}) noexcept;
  Status schedule(const TimerSpec& timer) noexcept;
  Status cancel(std::string_view id, std::uint64_t generation) noexcept;
  StatusOr<std::vector<TimerFiring>> advance(std::int64_t time_us,
                                             std::size_t max_firings = 1024) noexcept;
  std::optional<std::int64_t> next_deadline() const noexcept;
  std::int64_t time_us() const noexcept;
  std::size_t size() const noexcept;
  StatusOr<std::string> encode() const noexcept;
  static StatusOr<TimerQueue> decode(std::string_view bytes,
                                     const TimerLimits& limits = {}) noexcept;

private:
  explicit TimerQueue(TimerLimits limits) noexcept : limits_(limits) {}

  TimerLimits limits_;
  std::int64_t time_us_ = 0;
  std::size_t id_bytes_ = 0;
  std::map<std::string, TimerSpec, std::less<>> timers_;
};

struct WatermarkUpdate {
  bool late = false; // Compared with the watermark before accepting this observation.
  std::optional<std::int64_t> watermark_us;
};

class WatermarkTracker {
public:
  static StatusOr<WatermarkTracker> create(const std::vector<std::string>& partitions,
                                           std::int64_t allowed_lateness_us = 0) noexcept;
  StatusOr<WatermarkUpdate> observe(std::string_view partition,
                                    std::int64_t event_time_us) noexcept;
  Status set_idle(std::string_view partition, bool idle) noexcept;
  std::optional<std::int64_t> watermark_us() const noexcept;
  StatusOr<std::string> encode() const noexcept;
  static StatusOr<WatermarkTracker> decode(std::string_view bytes) noexcept;

private:
  struct Progress {
    std::optional<std::int64_t> max_event_us;
    bool idle = false;
  };

  explicit WatermarkTracker(std::int64_t lateness_us) noexcept : lateness_us_(lateness_us) {}

  void recompute() noexcept;
  std::int64_t lateness_us_;
  std::optional<std::int64_t> watermark_us_;
  std::map<std::string, Progress, std::less<>> partitions_;
};

} // namespace eventflow
