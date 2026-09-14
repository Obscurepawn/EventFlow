#include "eventflow/time.hpp"
#include "eventflow/types.hpp"
#include "status_test_support.hpp"
#include <gtest/gtest.h>
#include <random>

namespace eventflow {
namespace {
using test::take;

TEST(ManualClock, RejectsBackwardsTimeWithoutMutation) {
  auto clock = take(ManualClock::create(10));
  ASSERT_TRUE(clock.advance(20).ok());
  EXPECT_FALSE(clock.advance(19).ok());
  EXPECT_EQ(take(clock.now_us()), 20);
  EXPECT_FALSE(ManualClock::create(-1).ok());
}

TEST(TimerQueue, EqualityExpiresBeforeDataAndPeriodicSlotsCoalesce) {
  auto queue = take(TimerQueue::create());
  ASSERT_TRUE(queue.schedule({"periodic", 1, 10, 10}).ok());
  ASSERT_TRUE(queue.schedule({"once", 2, 30, 0}).ok());
  EXPECT_TRUE(take(queue.advance(9)).empty());
  const auto fired = take(queue.advance(30));
  ASSERT_EQ(fired.size(), 2u);
  EXPECT_EQ(fired[0].timer.id, "once");
  EXPECT_EQ(fired[0].timer.deadline_us, 30);
  EXPECT_EQ(fired[1].timer.id, "periodic");
  EXPECT_EQ(fired[1].timer.deadline_us, 30);
  EXPECT_EQ(fired[1].skipped_slots, 2u);
  EXPECT_EQ(queue.next_deadline(), 40);
  EXPECT_TRUE(take(queue.advance(30)).empty());
}

TEST(WatermarkTracker, UnknownPartitionProgressBlocksAndIdleCanRelease) {
  auto tracker = take(WatermarkTracker::create({"a", "b"}, 10));
  EXPECT_FALSE(take(tracker.observe("a", 100)).watermark_us);
  ASSERT_TRUE(tracker.set_idle("b", true).ok());
  EXPECT_EQ(tracker.watermark_us(), 90);
  const auto late = take(tracker.observe("b", 90));
  EXPECT_TRUE(late.late);
  EXPECT_EQ(late.watermark_us, 90);
  EXPECT_EQ(take(tracker.observe("a", 200)).watermark_us, 90);
  EXPECT_EQ(take(tracker.observe("b", 250)).watermark_us, 190);
}

TEST(SystemClock, RespectsRestoredFloorAndDoesNotRewind) {
  const auto wall = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count();
  auto clock = take(SystemClock::create(wall + 1000000));
  auto previous = take(clock.now_us());
  EXPECT_GE(previous, wall + 1000000);
  for (int i = 0; i < 100; ++i) {
    const auto now = take(clock.now_us());
    EXPECT_GE(now, previous);
    previous = now;
  }
  EXPECT_FALSE(SystemClock::create(-1).ok());
}

TEST(TimerQueue, StaleCancellationCannotRemoveReplacedTimer) {
  auto queue = take(TimerQueue::create());
  ASSERT_TRUE(queue.schedule({"ttl", 1, 10, 0}).ok());
  ASSERT_TRUE(queue.schedule({"ttl", 2, 20, 0}).ok());
  ASSERT_TRUE(queue.cancel("ttl", 1).ok());
  EXPECT_TRUE(take(queue.advance(10)).empty());
  const auto fired = take(queue.advance(20));
  ASSERT_EQ(fired.size(), 1u);
  EXPECT_EQ(fired.front().timer.generation, 2u);
  EXPECT_EQ(queue.size(), 0u);
}

TEST(TimerQueue, ScheduleRetryIsIdempotentAndConflictDoesNotChangeState) {
  auto queue = take(TimerQueue::create());
  ASSERT_TRUE(queue.schedule({"once", UINT64_MAX, 10, 0}).ok());
  const auto before = take(queue.encode());
  ASSERT_TRUE(queue.schedule({"once", UINT64_MAX, 10, 0}).ok());
  EXPECT_EQ(queue.schedule({"once", UINT64_MAX, 11, 0}).code(), ErrorCode::Conflict);
  EXPECT_EQ(queue.schedule({"once", 0, 12, 0}).code(), ErrorCode::Conflict);
  EXPECT_EQ(take(queue.encode()), before);
}

TEST(TimerQueue, BudgetFailureDoesNotPartiallyFireOrAdvanceTime) {
  auto queue = take(TimerQueue::create());
  ASSERT_TRUE(queue.schedule({"a", 0, 10, 0}).ok());
  ASSERT_TRUE(queue.schedule({"b", 0, 20, 0}).ok());
  const auto before = take(queue.encode());
  EXPECT_EQ(queue.advance(30, 1).status().code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(take(queue.encode()), before);
  EXPECT_EQ(take(queue.advance(30, 2)).size(), 2u);
}

TEST(TimerQueue, CountAndByteBudgetsAreReleasedOnCancelAndExpiry) {
  auto queue = take(TimerQueue::create({2, 3}));
  ASSERT_TRUE(queue.schedule({"ab", 0, 10, 0}).ok());
  EXPECT_EQ(queue.schedule({"cd", 0, 10, 0}).code(), ErrorCode::ResourceExhausted);
  ASSERT_TRUE(queue.schedule({"c", 0, 10, 0}).ok());
  EXPECT_EQ(queue.schedule({"d", 0, 10, 0}).code(), ErrorCode::ResourceExhausted);
  ASSERT_TRUE(queue.cancel("ab", 0).ok());
  ASSERT_TRUE(queue.schedule({"de", 0, 20, 0}).ok());
  EXPECT_EQ(take(queue.advance(20)).size(), 2u);
  ASSERT_TRUE(queue.schedule({"abc", 0, 30, 0}).ok());
}

TEST(TimerQueue, InvalidTimesAndOverflowLeaveQueueUnchanged) {
  auto queue = take(TimerQueue::create());
  EXPECT_FALSE(queue.schedule({"", 0, 1, 0}).ok());
  EXPECT_FALSE(queue.schedule({"a", 0, 0, 0}).ok());
  EXPECT_FALSE(queue.schedule({"a", 0, 1, -1}).ok());
  ASSERT_TRUE(queue.schedule({"a", 0, INT64_MAX, 1}).ok());
  const auto before = take(queue.encode());
  EXPECT_EQ(queue.advance(INT64_MAX).status().code(), ErrorCode::ResourceExhausted);
  EXPECT_FALSE(queue.advance(-1).ok());
  EXPECT_FALSE(queue.advance(1, 0).ok());
  EXPECT_EQ(take(queue.encode()), before);
  ASSERT_TRUE(queue.cancel("a", 0).ok());
  ASSERT_TRUE(queue.schedule({"a", 1, INT64_MAX, 0}).ok());
  EXPECT_EQ(take(queue.advance(INT64_MAX)).size(), 1u);
}

TEST(TimerQueue, SnapshotRecreatesCurrentDeadlineAndNotOriginalPeriodicSlot) {
  auto queue = take(TimerQueue::create());
  ASSERT_TRUE(queue.schedule({"periodic", 5, 5, 5}).ok());
  ASSERT_TRUE(queue.schedule({std::string("ttl\0row", 7), UINT64_MAX, 21, 0}).ok());
  EXPECT_EQ(take(queue.advance(17)).front().skipped_slots, 2u);
  const auto bytes = take(queue.encode());
  auto restored = take(TimerQueue::decode(bytes));
  EXPECT_EQ(take(restored.encode()), bytes);
  EXPECT_EQ(restored.time_us(), 17);
  const auto fired = take(restored.advance(25));
  ASSERT_EQ(fired.size(), 2u);
  EXPECT_EQ(fired[0].timer.deadline_us, 21);
  EXPECT_EQ(fired[1].timer.deadline_us, 25);
  EXPECT_EQ(fired[1].skipped_slots, 1u);
  EXPECT_EQ(restored.next_deadline(), 30);
}

TEST(TimerQueue, RejectsCorruptAndOverBudgetSnapshots) {
  auto queue = take(TimerQueue::create());
  ASSERT_TRUE(queue.schedule({"timer", 5, 10, 1}).ok());
  const auto bytes = take(queue.encode());
  const auto valid = take(parse_json_exact(bytes));
  EXPECT_FALSE(TimerQueue::decode(bytes, {1, 2}).ok());
  EXPECT_FALSE(TimerQueue::decode("{").ok());
  for (const auto* field : {"generation", "deadline_us", "period_us"}) {
    auto json = valid;
    json["timers"][0][field] = -1;
    EXPECT_FALSE(TimerQueue::decode(json.dump()).ok()) << field;
    json["timers"][0][field] = "1";
    EXPECT_FALSE(TimerQueue::decode(json.dump()).ok()) << field;
  }
  auto duplicate = valid;
  duplicate["timers"].push_back(duplicate["timers"][0]);
  EXPECT_FALSE(TimerQueue::decode(duplicate.dump()).ok());
  auto expired = valid;
  expired["time_us"] = 10;
  EXPECT_FALSE(TimerQueue::decode(expired.dump()).ok());
}

TEST(TimerQueue, RandomOneShotsMatchIndependentDeadlineOrderingAfterRestore) {
  auto queue = take(TimerQueue::create());
  std::mt19937 random(42);
  std::vector<std::pair<std::int64_t, std::string>> expected;
  for (int i = 0; i < 200; ++i) {
    const auto deadline = std::int64_t(1 + random() % 100);
    const auto id = std::to_string(i);
    ASSERT_TRUE(queue.schedule({id, 0, deadline, 0}).ok());
    expected.emplace_back(deadline, id);
  }
  queue = take(TimerQueue::decode(take(queue.encode())));
  std::sort(expected.begin(), expected.end());
  std::vector<std::pair<std::int64_t, std::string>> actual;
  for (int time = 0; time <= 100; ++time)
    for (const auto& firing : take(queue.advance(time)))
      actual.emplace_back(firing.timer.deadline_us, firing.timer.id);
  EXPECT_EQ(actual, expected);
  EXPECT_EQ(queue.size(), 0u);
}

TEST(WatermarkTracker, UsesSlowestActivePartitionAndDoesNotRewindOnReordering) {
  auto tracker = take(WatermarkTracker::create({"a", "b"}, 5));
  EXPECT_FALSE(take(tracker.observe("b", 50)).watermark_us);
  EXPECT_EQ(take(tracker.observe("a", 100)).watermark_us, 45);
  const auto within_tolerance = take(tracker.observe("b", 46));
  EXPECT_FALSE(within_tolerance.late);
  EXPECT_EQ(within_tolerance.watermark_us, 45);
  EXPECT_TRUE(take(tracker.observe("b", 45)).late);
  EXPECT_EQ(take(tracker.observe("b", 200)).watermark_us, 95);
}

TEST(WatermarkTracker, AllIdleKeepsWatermarkAndResumingDoesNotRewind) {
  auto tracker = take(WatermarkTracker::create({"a", "b"}));
  ASSERT_TRUE(tracker.set_idle("b", true).ok());
  EXPECT_EQ(take(tracker.observe("a", 100)).watermark_us, 100);
  ASSERT_TRUE(tracker.set_idle("a", true).ok());
  EXPECT_EQ(tracker.watermark_us(), 100);
  ASSERT_TRUE(tracker.set_idle("b", false).ok());
  EXPECT_EQ(tracker.watermark_us(), 100);
  EXPECT_TRUE(take(tracker.observe("b", 1)).late);
  EXPECT_EQ(tracker.watermark_us(), 100);
  EXPECT_EQ(take(tracker.observe("b", 101)).watermark_us, 101);
}

TEST(WatermarkTracker, FirstObservationIsComparedToPriorWatermark) {
  auto tracker = take(WatermarkTracker::create({"a"}));
  EXPECT_FALSE(take(tracker.observe("a", 10)).late);
  EXPECT_TRUE(take(tracker.observe("a", 10)).late);
  EXPECT_FALSE(take(tracker.observe("a", 11)).late);
}

TEST(WatermarkTracker, SnapshotPreservesIdleProgressAndGlobalFloor) {
  auto tracker = take(WatermarkTracker::create({"a", "b"}, 10));
  ASSERT_TRUE(tracker.set_idle("b", true).ok());
  EXPECT_EQ(take(tracker.observe("a", 100)).watermark_us, 90);
  EXPECT_TRUE(take(tracker.observe("b", 20)).late);
  const auto bytes = take(tracker.encode());
  auto restored = take(WatermarkTracker::decode(bytes));
  EXPECT_EQ(take(restored.encode()), bytes);
  EXPECT_EQ(take(restored.observe("a", 200)).watermark_us, 90);
  EXPECT_EQ(take(restored.observe("b", 250)).watermark_us, 190);
}

TEST(WatermarkTracker, ExtremesAndInvalidCallsDoNotOverflowOrMutate) {
  auto tracker = take(WatermarkTracker::create({"a"}, INT64_MAX));
  EXPECT_EQ(take(tracker.observe("a", 0)).watermark_us, -INT64_MAX);
  EXPECT_EQ(take(tracker.observe("a", INT64_MAX)).watermark_us, 0);
  const auto before = take(tracker.encode());
  EXPECT_FALSE(tracker.observe("a", -1).ok());
  EXPECT_FALSE(tracker.observe("missing", 1).ok());
  EXPECT_FALSE(tracker.set_idle("missing", true).ok());
  EXPECT_EQ(take(tracker.encode()), before);
  EXPECT_FALSE(WatermarkTracker::create({}).ok());
  EXPECT_FALSE(WatermarkTracker::create({"a", "a"}).ok());
  EXPECT_FALSE(WatermarkTracker::create({"a"}, -1).ok());
}

TEST(WatermarkTracker, RejectsSnapshotThatInventsOrDiscardsProgress) {
  auto tracker = take(WatermarkTracker::create({"a"}, 10));
  ASSERT_TRUE(tracker.observe("a", 100).ok());
  const auto valid = take(parse_json_exact(take(tracker.encode())));
  for (const Json& invalid : {Json(nullptr), Json(91), Json(89), Json("90"), Json(UINT64_MAX)}) {
    auto json = valid;
    json["watermark_us"] = invalid;
    EXPECT_FALSE(WatermarkTracker::decode(json.dump()).ok());
  }
  auto duplicate = valid;
  duplicate["partitions"].push_back(duplicate["partitions"][0]);
  EXPECT_FALSE(WatermarkTracker::decode(duplicate.dump()).ok());
}
} // namespace
} // namespace eventflow
