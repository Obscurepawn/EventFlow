#include "eventflow/http_message_server.hpp"
#include "eventflow/message_source.hpp"
#include "status_test_support.hpp"
#include <gtest/gtest.h>

namespace eventflow::test {
namespace {

class HttpAndMemorySource : public ::testing::TestWithParam<bool> {
protected:
  MessageSourceConfig config;
  std::unique_ptr<IMessageSource> source;
  std::unique_ptr<HttpMessageServer> server;
  std::string endpoint;

  SourcePartition partition(std::int32_t number = 0) noexcept {
    return {.source_id = config.source_id, .topic = "trades", .partition = number};
  }

  Status open_source() noexcept {
    if (GetParam()) {
      HttpSourceConfig http;
      static_cast<MessageSourceConfig&>(http) = config;
      http.endpoint = endpoint;
      auto opened = HttpMessageSource::open(http);
      if (!opened.ok())
        return opened.status();
      source = std::move(opened).value();
    } else {
      MemMockSourceConfig memory;
      static_cast<MessageSourceConfig&>(memory) = config;
      auto opened = MemMockMessageSource::open(memory);
      if (!opened.ok())
        return opened.status();
      source = std::move(opened).value();
    }
    return {};
  }

  void SetUp() noexcept override {
    config.source_id = "contract_feed";
    config.assignments = {{.topic = "trades", .partition = 0, .next_offset = 0},
                          {.topic = "trades", .partition = 1, .next_offset = 0}};
    config.max_message_bytes = 64;
    config.max_inflight_bytes = 256;
    if (GetParam()) {
      server = take(HttpMessageServer::create({.partitions = config.assignments}));
      endpoint = "http://127.0.0.1:" + std::to_string(take(server->start("127.0.0.1", 0)));
    }
    const auto opened = open_source();
    ASSERT_TRUE(opened.ok()) << opened.message();
  }

  void TearDown() noexcept override {
    source.reset();
    server.reset();
  }

  Status append(std::int32_t number, std::int64_t offset, std::optional<std::string> key,
                std::optional<std::string> payload) noexcept {
    if (server)
      return server->append("trades", number, offset, std::move(key), std::move(payload), 123);
    return static_cast<MemMockMessageSource*>(source.get())
        ->append(partition(number), offset, std::move(key), std::move(payload), 123);
  }
};

TEST_P(HttpAndMemorySource, PollPreservesBytesAndAcknowledgesOnlyTheDeliveredPrefix) {
  ASSERT_TRUE(append(0, 10, std::string("a\0b", 3), std::string("x\0y", 3)).ok());
  ASSERT_TRUE(append(0, 14, std::nullopt, std::string{}).ok());
  auto messages = take(source->poll(2, 128, 0));
  ASSERT_EQ(messages.size(), 2U);
  EXPECT_EQ(messages[0].key, std::string("a\0b", 3));
  EXPECT_EQ(messages[0].payload, std::string("x\0y", 3));
  EXPECT_EQ(messages[0].timestamp_ms, 123);
  EXPECT_FALSE(messages[1].key.has_value());
  ASSERT_TRUE(messages[1].payload.has_value());
  EXPECT_TRUE(messages[1].payload->empty());
  // Later completion cannot acknowledge the earlier, unfinished delivery.
  ASSERT_TRUE(source->ack(messages[1].token).ok());
  EXPECT_EQ(take(source->committed_position(partition())), -1);
  ASSERT_TRUE(source->ack(messages[0].token).ok());
  EXPECT_EQ(take(source->committed_position(partition())), 15);
  ASSERT_TRUE(source->ack(messages[0].token).ok());
}

TEST_P(HttpAndMemorySource, PauseAndReplayPreservePositionsAndRevokeOldTokens) {
  ASSERT_TRUE(append(0, 0, "AAPL", "70").ok());
  ASSERT_TRUE(append(0, 1, "AAPL", "50").ok());
  auto messages = take(source->poll(2, 128, 0));
  ASSERT_EQ(messages.size(), 2U);
  ASSERT_TRUE(source->ack(messages[0].token).ok());
  ASSERT_TRUE(source->pause(partition()).ok());
  ASSERT_TRUE(source->seek(partition(), 0).ok());
  EXPECT_TRUE(take(source->poll(2, 128, 0)).empty());
  EXPECT_FALSE(source->ack(messages[1].token).ok());
  ASSERT_TRUE(source->resume(partition()).ok());
  auto replay = take(source->poll(2, 128, 0));
  ASSERT_EQ(replay.size(), 2U);
  EXPECT_EQ(replay[0].token.offset, 0);
  ASSERT_TRUE(source->ack(replay[0].token).ok());
  EXPECT_EQ(take(source->committed_position(partition())), 1);
  ASSERT_TRUE(source->ack(replay[1].token).ok());
  EXPECT_EQ(take(source->committed_position(partition())), 2);
}

TEST_P(HttpAndMemorySource, PollBudgetDoesNotSkipAnOversizedRecord) {
  ASSERT_TRUE(append(0, 0, "A", "123456").ok());
  auto too_small = source->poll(1, 3, 0);
  ASSERT_FALSE(too_small.ok());
  EXPECT_EQ(too_small.status().code(), ErrorCode::ResourceExhausted);
  auto messages = take(source->poll(1, 64, 0));
  ASSERT_EQ(messages.size(), 1U);
  EXPECT_EQ(messages[0].token.offset, 0);
  EXPECT_EQ(messages[0].payload, "123456");
}

TEST_P(HttpAndMemorySource, ClosingNeverAcknowledgesUnfinishedInput) {
  ASSERT_TRUE(append(0, 0, "A", "70").ok());
  auto messages = take(source->poll(1, 64, 0));
  ASSERT_EQ(messages.size(), 1U);
  ASSERT_TRUE(source->close().ok());
  EXPECT_FALSE(source->ack(messages[0].token).ok());
  EXPECT_FALSE(source->poll(1, 64, 0).ok());
  ASSERT_TRUE(source->close().ok());
}

INSTANTIATE_TEST_SUITE_P(LocalAndHttp, HttpAndMemorySource, ::testing::Bool());

} // namespace
} // namespace eventflow::test
