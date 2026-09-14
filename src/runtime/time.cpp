#include "eventflow/time.hpp"
#include "eventflow/types.hpp"
#include <algorithm>
#include <limits>
#include <tuple>

namespace eventflow {
namespace {
constexpr std::size_t max_partitions = 4096;
constexpr std::size_t max_partition_bytes = 1024;

StatusOr<std::int64_t> integer(const Json& object, const char* field) noexcept {
  if (!object.contains(field) || !object[field].is_number_integer())
    return Status(ErrorCode::InvalidArgument, "missing integer in time snapshot");

  const auto& value = object[field];
  if (value.is_number_unsigned() && value.get<std::uint64_t>() > INT64_MAX)
    return Status(ErrorCode::InvalidArgument, "time snapshot integer overflow");

  return value.get<std::int64_t>();
}

StatusOr<std::uint64_t> unsigned_integer(const Json& object, const char* field) noexcept {
  if (!object.contains(field) || !object[field].is_number_integer())
    return Status(ErrorCode::InvalidArgument, "missing generation in timer snapshot");

  const auto& value = object[field];
  if (!value.is_number_unsigned() && value.get<std::int64_t>() < 0)
    return Status(ErrorCode::InvalidArgument, "negative timer generation");

  return value.get<std::uint64_t>();
}

StatusOr<Json> snapshot_json(std::string_view bytes, std::string_view kind,
                             std::size_t max_bytes) noexcept {
  EF_RETURN_IF_ERROR(check(bytes.size() <= max_bytes, "time snapshot exceeds byte budget"));
  EF_ASSIGN_OR_RETURN(auto json, parse_json_exact(bytes));
  if (!json.is_object() || !json.contains("format") || json["format"] != 1 ||
      !json.contains("kind") || !json["kind"].is_string() ||
      json["kind"].get_ref<const std::string&>() != kind)
    return Status(ErrorCode::InvalidArgument, "unsupported time snapshot format");

  return json;
}
} // namespace

StatusOr<ManualClock> ManualClock::create(std::int64_t initial_us) noexcept {
  EF_RETURN_IF_ERROR(check(initial_us >= 0, "negative processing time"));
  return ManualClock(initial_us);
}

Status ManualClock::advance(std::int64_t time_us) noexcept {
  EF_RETURN_IF_ERROR(check(time_us >= time_us_, "processing time cannot move backwards"));
  time_us_ = time_us;
  return {};
}

StatusOr<std::int64_t> ManualClock::now_us() noexcept {
  return std::int64_t(time_us_);
}

SystemClock::SystemClock(std::int64_t anchor_us,
                         std::chrono::steady_clock::time_point origin) noexcept
    : anchor_us_(anchor_us), origin_(origin) {}

StatusOr<SystemClock> SystemClock::create(std::int64_t restored_us) noexcept {
  EF_RETURN_IF_ERROR(check(restored_us >= 0, "negative restored processing time"));
  const auto wall_us = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
  EF_RETURN_IF_ERROR(check(wall_us >= 0, "wall clock is before Unix epoch"));

  return SystemClock(std::max(std::int64_t(wall_us), restored_us),
                     std::chrono::steady_clock::now());
}

StatusOr<std::int64_t> SystemClock::now_us() noexcept {
  const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - origin_)
                           .count();
  if (elapsed < 0 || elapsed > INT64_MAX - anchor_us_)
    return Status(ErrorCode::ResourceExhausted, "processing clock exhausted");

  return std::int64_t(anchor_us_ + elapsed);
}

StatusOr<TimerQueue> TimerQueue::create(const TimerLimits& limits) noexcept {
  EF_RETURN_IF_ERROR(check(limits.max_timers > 0 && limits.max_timers <= 1000000 &&
                               limits.max_id_bytes > 0 && limits.max_id_bytes <= 64 * 1024 * 1024,
                           "invalid timer limits"));
  return TimerQueue(limits);
}

Status TimerQueue::schedule(const TimerSpec& timer) noexcept {
  EF_RETURN_IF_ERROR(check(!timer.id.empty() && valid_utf8(timer.id), "invalid timer identity"));
  EF_RETURN_IF_ERROR(check(timer.deadline_us > time_us_ && timer.period_us >= 0,
                           "timer must be scheduled after current processing time"));
  const auto found = timers_.find(timer.id);
  if (found != timers_.end()) {
    if (found->second == timer)
      return {};
    if (timer.generation <= found->second.generation)
      return Status(ErrorCode::Conflict, "timer replacement requires a newer generation");

    found->second = timer;
    return {};
  }

  if (timers_.size() >= limits_.max_timers || timer.id.size() > limits_.max_id_bytes - id_bytes_)
    return Status(ErrorCode::ResourceExhausted, "timer queue budget exceeded");

  timers_.emplace(timer.id, timer);
  id_bytes_ += timer.id.size();
  return {};
}

Status TimerQueue::cancel(std::string_view id, std::uint64_t generation) noexcept {
  const auto found = timers_.find(id);
  if (found != timers_.end() && found->second.generation == generation) {
    id_bytes_ -= found->first.size();
    timers_.erase(found);
  }

  return {};
}

StatusOr<std::vector<TimerFiring>> TimerQueue::advance(std::int64_t time_us,
                                                       std::size_t max_firings) noexcept {
  EF_RETURN_IF_ERROR(check(time_us >= time_us_, "timer clock cannot move backwards"));
  EF_RETURN_IF_ERROR(check(max_firings > 0, "timer firing budget must be positive"));
  std::vector<TimerFiring> result;

  for (const auto& [id, timer] : timers_) {
    (void)id;
    if (timer.deadline_us > time_us)
      continue;
    if (result.size() == max_firings)
      return Status(ErrorCode::ResourceExhausted, "due timers exceed round firing budget");

    TimerFiring firing{.timer = timer, .skipped_slots = 0};
    if (timer.period_us > 0) {
      // Subtraction is safe: both times are nonnegative and deadline <= now.
      const auto skipped = (time_us - timer.deadline_us) / timer.period_us;
      firing.skipped_slots = static_cast<std::uint64_t>(skipped);
      firing.timer.deadline_us += skipped * timer.period_us;
      if (firing.timer.deadline_us > INT64_MAX - timer.period_us)
        return Status(ErrorCode::ResourceExhausted, "next periodic deadline would overflow");
    }

    result.push_back(std::move(firing));
  }

  std::sort(result.begin(), result.end(), [](const auto& left, const auto& right) noexcept {
    return std::tie(left.timer.deadline_us, left.timer.id) <
           std::tie(right.timer.deadline_us, right.timer.id);
  });

  // Publish only after all budget/overflow checks, preserving atomic failure semantics.
  for (const auto& firing : result) {
    if (firing.timer.period_us > 0)
      timers_.find(firing.timer.id)->second.deadline_us =
          firing.timer.deadline_us + firing.timer.period_us;
    else {
      id_bytes_ -= firing.timer.id.size();
      timers_.erase(firing.timer.id);
    }
  }

  time_us_ = time_us;
  return result;
}

std::optional<std::int64_t> TimerQueue::next_deadline() const noexcept {
  std::optional<std::int64_t> result;
  for (const auto& [id, timer] : timers_) {
    (void)id;
    if (!result || timer.deadline_us < *result)
      result = timer.deadline_us;
  }

  return result;
}

std::int64_t TimerQueue::time_us() const noexcept {
  return time_us_;
}

std::size_t TimerQueue::size() const noexcept {
  return timers_.size();
}

StatusOr<std::string> TimerQueue::encode() const noexcept {
  Json timers = Json::array();
  for (const auto& [id, timer] : timers_)
    timers.push_back({{"id", id},
                      {"generation", timer.generation},
                      {"deadline_us", timer.deadline_us},
                      {"period_us", timer.period_us}});

  return Json{
      {"kind", "timers"}, {"format", 1}, {"time_us", time_us_}, {"timers", std::move(timers)}}
      .dump();
}

StatusOr<TimerQueue> TimerQueue::decode(std::string_view bytes,
                                        const TimerLimits& limits) noexcept {
  EF_ASSIGN_OR_RETURN(auto result, create(limits));

  // UTF-8 identities may contain control characters escaped to six JSON bytes each.
  const auto max_bytes = limits.max_id_bytes * 6 + limits.max_timers * 256 + 256;
  EF_ASSIGN_OR_RETURN(auto json, snapshot_json(bytes, "timers", max_bytes));
  EF_ASSIGN_OR_RETURN(result.time_us_, integer(json, "time_us"));
  EF_RETURN_IF_ERROR(check(result.time_us_ >= 0 && json.contains("timers") &&
                               json["timers"].is_array() &&
                               json["timers"].size() <= limits.max_timers,
                           "invalid timer snapshot layout"));
  for (const auto& entry : json["timers"]) {
    if (!entry.is_object() || !entry.contains("id") || !entry["id"].is_string())
      return Status(ErrorCode::InvalidArgument, "invalid timer snapshot identity");

    TimerSpec timer;
    timer.id = entry["id"].get<std::string>();
    EF_ASSIGN_OR_RETURN(timer.generation, unsigned_integer(entry, "generation"));
    EF_ASSIGN_OR_RETURN(timer.deadline_us, integer(entry, "deadline_us"));
    EF_ASSIGN_OR_RETURN(timer.period_us, integer(entry, "period_us"));
    if (result.timers_.contains(timer.id))
      return Status(ErrorCode::InvalidArgument, "duplicate timer in snapshot");

    EF_RETURN_IF_ERROR(result.schedule(timer));
  }

  return result;
}

StatusOr<WatermarkTracker> WatermarkTracker::create(const std::vector<std::string>& partitions,
                                                    std::int64_t lateness) noexcept {
  EF_RETURN_IF_ERROR(
      check(!partitions.empty() && partitions.size() <= max_partitions && lateness >= 0,
            "invalid watermark configuration"));
  WatermarkTracker result(lateness);

  for (const auto& partition : partitions) {
    EF_RETURN_IF_ERROR(check(!partition.empty() && partition.size() <= max_partition_bytes &&
                                 valid_utf8(partition),
                             "invalid watermark partition identity"));
    if (!result.partitions_.emplace(partition, Progress{}).second)
      return Status(ErrorCode::InvalidArgument, "duplicate watermark partition");
  }

  return result;
}

void WatermarkTracker::recompute() noexcept {
  std::optional<std::int64_t> minimum;
  for (const auto& [id, progress] : partitions_) {
    (void)id;
    if (progress.idle)
      continue;
    if (!progress.max_event_us)
      return; // An active partition with unknown progress must not be skipped.
    const auto candidate = *progress.max_event_us - lateness_us_;
    if (!minimum || candidate < *minimum)
      minimum = candidate;
  }

  if (minimum && (!watermark_us_ || *minimum > *watermark_us_))
    watermark_us_ = minimum;
}

StatusOr<WatermarkUpdate> WatermarkTracker::observe(std::string_view partition,
                                                    std::int64_t event_time_us) noexcept {
  EF_RETURN_IF_ERROR(check(event_time_us >= 0, "negative event time"));
  const auto found = partitions_.find(partition);
  if (found == partitions_.end())
    return Status(ErrorCode::NotFound, "unknown watermark partition");

  const bool late = watermark_us_ && event_time_us <= *watermark_us_;
  auto& progress = found->second;
  progress.idle = false;
  if (!progress.max_event_us || event_time_us > *progress.max_event_us)
    progress.max_event_us = event_time_us;

  recompute();
  return WatermarkUpdate{.late = late, .watermark_us = watermark_us_};
}

Status WatermarkTracker::set_idle(std::string_view partition, bool idle) noexcept {
  const auto found = partitions_.find(partition);
  if (found == partitions_.end())
    return Status(ErrorCode::NotFound, "unknown watermark partition");

  found->second.idle = idle;
  recompute();
  return {};
}

std::optional<std::int64_t> WatermarkTracker::watermark_us() const noexcept {
  return watermark_us_;
}

StatusOr<std::string> WatermarkTracker::encode() const noexcept {
  Json partitions = Json::array();
  for (const auto& [id, progress] : partitions_) {
    Json value = nullptr;
    if (progress.max_event_us)
      value = *progress.max_event_us;

    partitions.push_back({{"id", id}, {"idle", progress.idle}, {"max_event_us", value}});
  }

  Json watermark = nullptr;
  if (watermark_us_)
    watermark = *watermark_us_;

  return Json{{"format", 1},
              {"kind", "watermarks"},
              {"lateness_us", lateness_us_},
              {"watermark_us", watermark},
              {"partitions", std::move(partitions)}}
      .dump();
}

StatusOr<WatermarkTracker> WatermarkTracker::decode(std::string_view bytes) noexcept {
  EF_ASSIGN_OR_RETURN(auto json, snapshot_json(bytes, "watermarks", 32 * 1024 * 1024));
  EF_ASSIGN_OR_RETURN(auto lateness, integer(json, "lateness_us"));
  if (!json.contains("partitions") || !json["partitions"].is_array() ||
      json["partitions"].size() > max_partitions || !json.contains("watermark_us"))
    return Status(ErrorCode::InvalidArgument, "invalid watermark snapshot layout");

  std::vector<std::string> ids;
  for (const auto& entry : json["partitions"]) {
    if (!entry.is_object() || !entry.contains("id") || !entry["id"].is_string() ||
        !entry.contains("idle") || !entry["idle"].is_boolean() || !entry.contains("max_event_us"))
      return Status(ErrorCode::InvalidArgument, "invalid watermark partition snapshot");

    ids.push_back(entry["id"].get<std::string>());
  }

  EF_ASSIGN_OR_RETURN(auto result, create(ids, lateness));
  std::optional<std::int64_t> upper_bound;
  for (const auto& entry : json["partitions"]) {
    auto& progress = result.partitions_.find(entry["id"].get_ref<const std::string&>())->second;
    progress.idle = entry["idle"].get<bool>();
    if (!entry["max_event_us"].is_null()) {
      EF_ASSIGN_OR_RETURN(auto value, integer(entry, "max_event_us"));
      EF_RETURN_IF_ERROR(check(value >= 0, "negative restored event time"));
      progress.max_event_us = value;
      const auto candidate = value - lateness;
      if (!upper_bound || candidate > *upper_bound)
        upper_bound = candidate;
    }
  }

  if (!json["watermark_us"].is_null()) {
    EF_ASSIGN_OR_RETURN(auto value, integer(json, "watermark_us"));
    EF_RETURN_IF_ERROR(check(upper_bound && value <= *upper_bound && value >= -lateness,
                             "restored watermark exceeds observed progress"));
    result.watermark_us_ = value;
  }

  // Detect snapshots that discard a watermark already implied by active progress.
  const auto saved = result.watermark_us_;
  result.recompute();
  EF_RETURN_IF_ERROR(check(saved == result.watermark_us_, "inconsistent saved watermark"));

  return result;
}
} // namespace eventflow
