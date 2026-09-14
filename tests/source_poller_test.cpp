#include "eventflow/source_poller.hpp"
#include "status_test_support.hpp"
#include <gtest/gtest.h>

namespace eventflow {
namespace {
using test::take;

std::shared_ptr<MemMockMessageSource> make_source() noexcept {
  MemMockSourceConfig config;
  config.source_id = "market_feed";
  config.assignments = {{.topic = "trades", .partition = 0, .next_offset = 0}};
  return std::shared_ptr<MemMockMessageSource>(take(MemMockMessageSource::open(config)));
}

TEST(SourcePoller, AcknowledgesOnlyAfterTheDatabaseConfirmsTheInput) {
  auto source = make_source();
  const SourcePartition partition{.source_id = "market_feed", .topic = "trades", .partition = 0};
  ASSERT_TRUE(source->append(partition, 0, "AAPL", "70").ok());
  std::size_t applications = 0;
  auto poller =
      take(SourcePoller::open(source,
                              [&](const SourceMessage& message,
                                  std::int64_t now_us) noexcept -> StatusOr<SourceApplyResult> {
                                // The source position remains unacknowledged while database work is
                                // running.
                                EXPECT_EQ(take(source->committed_position(partition)), -1);
                                EXPECT_EQ(message.token.offset, 0);
                                EXPECT_EQ(message.payload, "70");
                                EXPECT_EQ(now_us, 100);
                                ++applications;
                                return SourceApplyResult{.input_committed = true,
                                                         .operation_status = {}};
                              }));
  auto progress = poller->step(100);
  ASSERT_TRUE(progress.ok());
  EXPECT_EQ(applications, 1U);
  EXPECT_EQ(progress->acknowledged, 1U);
  EXPECT_EQ(take(source->committed_position(partition)), 1);
}

TEST(SourcePoller, UnknownInputRetainsTheOriginalPayloadTimeAndPositionAcrossPause) {
  auto source = make_source();
  const SourcePartition partition{.source_id = "market_feed", .topic = "trades", .partition = 0};
  ASSERT_TRUE(source->append(partition, 0, "AAPL", "70").ok());
  ASSERT_TRUE(source->append(partition, 1, "AAPL", "50").ok());
  std::size_t applications = 0;
  auto poller = take(SourcePoller::open(
      source,
      [&](const SourceMessage& message,
          std::int64_t now_us) noexcept -> StatusOr<SourceApplyResult> {
        EXPECT_EQ(message.token.offset, 0);
        EXPECT_EQ(message.payload, "70");
        EXPECT_EQ(now_us, 100);
        ++applications;
        if (applications == 1)
          return Status(ErrorCode::UnknownCommit, "database response was lost");
        return SourceApplyResult{.input_committed = true, .operation_status = {}};
      }));
  EXPECT_TRUE(take(poller->step(100)).pending);
  EXPECT_EQ(take(source->committed_position(partition)), -1);
  poller->pause();
  EXPECT_TRUE(take(poller->step(200)).paused);
  EXPECT_EQ(applications, 1U);
  poller->resume();
  EXPECT_EQ(take(poller->step(300)).acknowledged, 1U);
  EXPECT_EQ(applications, 2U);
  EXPECT_EQ(poller->progress().polled, 1U);
  EXPECT_EQ(take(source->committed_position(partition)), 1);
}

TEST(SourcePoller, AcknowledgesConfirmedInputEvenWhenLaterFunctionWorkIsBackpressured) {
  auto source = make_source();
  const SourcePartition partition{.source_id = "market_feed", .topic = "trades", .partition = 0};
  ASSERT_TRUE(source->append(partition, 0, "AAPL", "70").ok());
  auto poller = take(SourcePoller::open(
      source, [](const SourceMessage&, std::int64_t) noexcept -> StatusOr<SourceApplyResult> {
        return SourceApplyResult{
            .input_committed = true,
            .operation_status = Status(ErrorCode::ResourceExhausted, "function queue is full")};
      }));
  const auto progress = take(poller->step(100));
  EXPECT_EQ(progress.acknowledged, 1U);
  EXPECT_FALSE(progress.failed);
  EXPECT_EQ(take(source->committed_position(partition)), 1);
}

TEST(SourcePoller, PermanentInputErrorRemainsObservableWithoutSkippingTheMessage) {
  auto source = make_source();
  const SourcePartition partition{.source_id = "market_feed", .topic = "trades", .partition = 0};
  ASSERT_TRUE(source->append(partition, 0, "AAPL", "bad row").ok());
  std::size_t applications = 0;
  auto poller = take(SourcePoller::open(
      source, [&](const SourceMessage&, std::int64_t) noexcept -> StatusOr<SourceApplyResult> {
        ++applications;
        return Status(ErrorCode::InvalidArgument, "input schema is invalid");
      }));
  EXPECT_TRUE(take(poller->step(100)).failed);
  EXPECT_TRUE(take(poller->step(200)).failed);
  EXPECT_EQ(applications, 1U);
  EXPECT_TRUE(poller->progress().pending);
  EXPECT_EQ(take(source->committed_position(partition)), -1);
  ASSERT_TRUE(poller->close().ok());
  EXPECT_FALSE(poller->step(300).ok());
}

} // namespace
} // namespace eventflow
