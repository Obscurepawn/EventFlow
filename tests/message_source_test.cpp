#include "eventflow/message_source.hpp"
#include "status_test_support.hpp"
#include <chrono>
#include <gtest/gtest.h>
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafka_mock.h>
#include <limits>

namespace eventflow::test {
namespace {
SourcePartition partition(std::int32_t id = 0) noexcept {
  return {"feed", "events", id};
}

MemMockSourceConfig mem_config() noexcept {
  MemMockSourceConfig config;
  config.source_id = "feed";
  config.assignments = {{"events", 0, {}}, {"events", 1, {}}};
  return config;
}

TEST(MemMessageSource, AcknowledgesDeliveredPrefixAcrossOffsetGaps) {
  auto source = take(MemMockMessageSource::open(mem_config()));
  ASSERT_TRUE(source->append(partition(), 10, "key", "first").ok());
  ASSERT_TRUE(source->append(partition(), 14, "key", "second").ok());
  auto messages = take(source->poll(10, 1000, 0));
  ASSERT_EQ(messages.size(), 2U);
  EXPECT_EQ(take(source->committed_position(partition())), -1);
  ASSERT_TRUE(source->ack(messages[1].token).ok());
  EXPECT_EQ(take(source->committed_position(partition())), -1);
  ASSERT_TRUE(source->ack(messages[0].token).ok());
  EXPECT_EQ(take(source->committed_position(partition())), 15);
}

TEST(MemMessageSource, RejectsInvalidConfigAndBoundedLogAppend) {
  auto config = mem_config();
  config.max_message_bytes = 8;
  config.max_log_bytes = 8;
  config.max_log_messages = 2;
  auto source = take(MemMockMessageSource::open(config));
  ASSERT_TRUE(source->append(partition(), 0, "k", "123").ok());
  ASSERT_TRUE(source->append(partition(), 4, "k", "123").ok());
  EXPECT_EQ(source->append(partition(), 5, {}, "1").code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(source->append(partition(), 4, {}, "1").code(), ErrorCode::InvalidArgument);
  EXPECT_EQ(source->append(partition(), -1, {}, "1").code(), ErrorCode::InvalidArgument);
  EXPECT_EQ(source->append(partition(), std::numeric_limits<std::int64_t>::max(), {}, "1").code(),
            ErrorCode::InvalidArgument);
  config.assignments.push_back(config.assignments.front());
  EXPECT_FALSE(MemMockMessageSource::open(config).ok());
  config = mem_config();
  config.source_id.clear();
  EXPECT_FALSE(MemMockMessageSource::open(config).ok());
}

class MessageSourceContract : public ::testing::TestWithParam<bool> {
protected:
  std::unique_ptr<IMessageSource> source;
  rd_kafka_t* producer = nullptr;
  rd_kafka_mock_cluster_t* cluster = nullptr;
  std::int64_t offsets[2]{};
  MessageSourceConfig config;

  void SetUp() noexcept override {
    config = mem_config();
    config.max_message_bytes = 64;
    config.max_inflight_bytes = 4096;
    if (GetParam()) {
      auto* conf = rd_kafka_conf_new();
      char error[256]{};
      ASSERT_EQ(rd_kafka_conf_set(conf, "test.mock.num.brokers", "1", error, sizeof(error)),
                RD_KAFKA_CONF_OK);
      ASSERT_EQ(rd_kafka_conf_set(conf, "log_level", "0", error, sizeof(error)), RD_KAFKA_CONF_OK);
      ASSERT_EQ(rd_kafka_conf_set(conf, "message.timeout.ms", "5000", error, sizeof(error)),
                RD_KAFKA_CONF_OK);
      producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error, sizeof(error));
      ASSERT_NE(producer, nullptr) << error;
      cluster = rd_kafka_handle_mock_cluster(producer);
      ASSERT_NE(cluster, nullptr);
      ASSERT_EQ(rd_kafka_mock_topic_create(cluster, "events", 2, 1), RD_KAFKA_RESP_ERR_NO_ERROR);
    }
    ASSERT_TRUE(open_source().ok());
  }

  void TearDown() noexcept override {
    source.reset();
    if (producer)
      rd_kafka_destroy(producer);
  }

  Status open_source() noexcept {
    source.reset();
    if (GetParam()) {
      KafkaSourceConfig kafka;
      static_cast<MessageSourceConfig&>(kafka) = config;
      kafka.group_id = "contract-group";
      kafka.bootstrap_servers = rd_kafka_mock_cluster_bootstraps(cluster);
      auto result = KafkaMessageSource::open(kafka);
      if (!result.ok())
        return result.status();
      source = std::move(result).value();
    } else {
      MemMockSourceConfig mem;
      static_cast<MessageSourceConfig&>(mem) = config;
      source = take(MemMockMessageSource::open(mem));
    }
    return {};
  }

  Status append(std::int32_t p, std::optional<std::string> key = "k",
                std::optional<std::string> payload = "data") noexcept {
    if (!GetParam())
      return static_cast<MemMockMessageSource*>(source.get())
          ->append(partition(p), offsets[p]++, std::move(key), std::move(payload), 1700000000000);
    rd_kafka_vu_t values[6]{};
    values[0].vtype = RD_KAFKA_VTYPE_TOPIC;
    values[0].u.cstr = "events";
    values[1].vtype = RD_KAFKA_VTYPE_PARTITION;
    values[1].u.i32 = p;
    values[2].vtype = RD_KAFKA_VTYPE_MSGFLAGS;
    values[2].u.i = RD_KAFKA_MSG_F_COPY;
    values[3].vtype = RD_KAFKA_VTYPE_KEY;
    values[3].u.mem = {key ? key->data() : nullptr, key ? key->size() : 0};
    values[4].vtype = RD_KAFKA_VTYPE_VALUE;
    values[4].u.mem = {payload ? payload->data() : nullptr, payload ? payload->size() : 0};
    values[5].vtype = RD_KAFKA_VTYPE_TIMESTAMP;
    values[5].u.i64 = 1700000000000;
    auto* error = rd_kafka_produceva(producer, values, 6);
    if (error) {
      Status status(ErrorCode::Unavailable, rd_kafka_error_string(error));
      rd_kafka_error_destroy(error);
      return status;
    }
    const auto flushed = rd_kafka_flush(producer, 5000);
    return flushed == RD_KAFKA_RESP_ERR_NO_ERROR
               ? Status{}
               : Status(ErrorCode::Unavailable, rd_kafka_err2str(flushed));
  }

  std::vector<SourceMessage> read(std::size_t count, std::size_t bytes = 4096) noexcept {
    std::vector<SourceMessage> messages;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
    do {
      auto result = source->poll(count - messages.size(), bytes, 100);
      if (!result.ok()) {
        ADD_FAILURE() << result.status().message();
        break;
      }
      for (auto& message : result.value())
        messages.push_back(std::move(message));
    } while (messages.size() < count && std::chrono::steady_clock::now() < deadline);
    EXPECT_EQ(messages.size(), count);
    return messages;
  }
};

TEST_P(MessageSourceContract, AdvancesOnlyCompletedDeliveryPrefix) {
  ASSERT_TRUE(append(0).ok());
  ASSERT_TRUE(append(0).ok());
  ASSERT_TRUE(append(0).ok());
  auto messages = read(3);
  ASSERT_EQ(messages.size(), 3U);
  ASSERT_TRUE(source->ack(messages[2].token).ok());
  EXPECT_EQ(take(source->committed_position(partition())), -1);
  ASSERT_TRUE(source->ack(messages[0].token).ok());
  EXPECT_EQ(take(source->committed_position(partition())), 1);
  ASSERT_TRUE(source->ack(messages[1].token).ok());
  EXPECT_EQ(take(source->committed_position(partition())), 3);
  EXPECT_TRUE(source->ack(messages[1].token).ok());
}

TEST_P(MessageSourceContract, PauseIsPerPartitionAndDoesNotBlockAcknowledgement) {
  ASSERT_TRUE(append(0).ok());
  auto first = read(1);
  ASSERT_EQ(first.size(), 1U);
  ASSERT_TRUE(source->pause(partition()).ok());
  ASSERT_TRUE(source->ack(first[0].token).ok());
  ASSERT_TRUE(append(0).ok());
  ASSERT_TRUE(append(1).ok());
  auto other = read(1);
  ASSERT_EQ(other.size(), 1U);
  EXPECT_EQ(other[0].token.partition.partition, 1);
  EXPECT_TRUE(take(source->poll(10, 100, 100)).empty());
  ASSERT_TRUE(source->resume(partition()).ok());
  auto resumed = read(1);
  ASSERT_EQ(resumed.size(), 1U);
  EXPECT_EQ(resumed[0].token.partition.partition, 0);
  EXPECT_EQ(resumed[0].token.offset, 1);
}

TEST_P(MessageSourceContract, SeekRevokesTokensAndReplaysWithoutRegressingCommits) {
  ASSERT_TRUE(append(0).ok());
  ASSERT_TRUE(append(0).ok());
  auto old = read(2);
  ASSERT_EQ(old.size(), 2U);
  ASSERT_TRUE(source->ack(old[0].token).ok());
  ASSERT_TRUE(source->seek(partition(), 0).ok());
  EXPECT_EQ(take(source->committed_position(partition())), 1);
  EXPECT_FALSE(source->ack(old[1].token).ok());
  auto replay = read(2);
  ASSERT_EQ(replay.size(), 2U);
  EXPECT_EQ(replay[0].token.offset, 0);
  EXPECT_NE(replay[0].token.delivery_id, old[0].token.delivery_id);
  EXPECT_FALSE(source->ack(old[0].token).ok());
  ASSERT_TRUE(source->ack(replay[0].token).ok());
  EXPECT_EQ(take(source->committed_position(partition())), 1);
  ASSERT_TRUE(source->ack(replay[1].token).ok());
  EXPECT_EQ(take(source->committed_position(partition())), 2);
}

TEST_P(MessageSourceContract, RejectsFabricatedAndCrossInstanceTokens) {
  ASSERT_TRUE(append(0).ok());
  auto messages = read(1);
  ASSERT_EQ(messages.size(), 1U);
  auto token = messages[0].token;
  ++token.offset;
  EXPECT_FALSE(source->ack(token).ok());
  token = messages[0].token;
  token.partition.source_id = "wrong-source";
  EXPECT_FALSE(source->ack(token).ok());
  token = messages[0].token;
  token.partition.partition = 1;
  EXPECT_FALSE(source->ack(token).ok());
  token = messages[0].token;
  ++token.instance_id;
  EXPECT_FALSE(source->ack(token).ok());
  token = messages[0].token;
  ++token.delivery_id;
  EXPECT_FALSE(source->ack(token).ok());
  EXPECT_EQ(take(source->committed_position(partition())), -1);
  EXPECT_TRUE(source->ack(messages[0].token).ok());
}

TEST_P(MessageSourceContract, PollBudgetsNeverDropTheUndeliveredRecord) {
  ASSERT_TRUE(append(0).ok());
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
  Status error;
  do {
    auto result = source->poll(10, 4, 100);
    if (!result.ok()) {
      error = result.status();
      break;
    }
    ASSERT_TRUE(result->empty());
  } while (std::chrono::steady_clock::now() < deadline);
  EXPECT_EQ(error.code(), ErrorCode::ResourceExhausted);
  auto messages = read(1, 5);
  ASSERT_EQ(messages.size(), 1U);
  EXPECT_EQ(messages[0].payload, "data");
  EXPECT_EQ(messages[0].token.offset, 0);
}

TEST_P(MessageSourceContract, InflightCapacityRequiresPrefixCompletion) {
  config.max_inflight_messages = 2;
  ASSERT_TRUE(open_source().ok());
  ASSERT_TRUE(append(0).ok());
  ASSERT_TRUE(append(0).ok());
  ASSERT_TRUE(append(0).ok());
  auto first = read(2);
  ASSERT_EQ(first.size(), 2U);
  EXPECT_TRUE(take(source->poll(10, 100, 100)).empty());
  ASSERT_TRUE(source->ack(first[1].token).ok());
  EXPECT_TRUE(take(source->poll(10, 100, 100)).empty());
  ASSERT_TRUE(source->ack(first[0].token).ok());
  auto next = read(1);
  ASSERT_EQ(next.size(), 1U);
  EXPECT_EQ(next[0].token.offset, 2);
}

TEST_P(MessageSourceContract, PreservesOwnedBinaryNullAndEmptyPayloads) {
  ASSERT_TRUE(append(0, std::string("a\0b", 3), std::string("c\0d", 3)).ok());
  ASSERT_TRUE(append(0, {}, {}).ok());
  ASSERT_TRUE(append(0, "", "").ok());
  auto values = read(3);
  ASSERT_EQ(values.size(), 3U);
  ASSERT_TRUE(source->close().ok());
  EXPECT_EQ(values[0].key, std::string("a\0b", 3));
  EXPECT_EQ(values[0].payload, std::string("c\0d", 3));
  EXPECT_EQ(values[0].timestamp_ms, 1700000000000);
  EXPECT_FALSE(values[1].key.has_value());
  EXPECT_FALSE(values[1].payload.has_value());
  ASSERT_TRUE(values[2].key.has_value());
  ASSERT_TRUE(values[2].payload.has_value());
  EXPECT_TRUE(values[2].payload->empty());
}

TEST_P(MessageSourceContract, ValidatesArgumentsAndCloseDoesNotCommit) {
  EXPECT_FALSE(source->poll(0, 1, 0).ok());
  EXPECT_FALSE(source->poll(1, 0, 0).ok());
  EXPECT_FALSE(source->poll(1, 1, -1).ok());
  EXPECT_FALSE(source->pause(partition(7)).ok());
  EXPECT_FALSE(source->resume(partition(7)).ok());
  EXPECT_FALSE(source->seek(partition(), -1).ok());
  ASSERT_TRUE(append(0).ok());
  auto messages = read(1);
  ASSERT_EQ(messages.size(), 1U);
  EXPECT_EQ(take(source->committed_position(partition())), -1);
  ASSERT_TRUE(source->close().ok());
  ASSERT_TRUE(source->close().ok());
  EXPECT_FALSE(source->ack(messages[0].token).ok());
  EXPECT_FALSE(source->poll(1, 1, 0).ok());
  EXPECT_FALSE(source->committed_position(partition()).ok());
  if (GetParam()) {
    ASSERT_TRUE(open_source().ok());
    EXPECT_EQ(take(source->committed_position(partition())), -1);
    auto replay = read(1);
    ASSERT_EQ(replay.size(), 1U);
    EXPECT_EQ(replay[0].token.offset, 0);
  }
}

TEST_P(MessageSourceContract, BufferedRecordRespectsPauseAndOtherPartitionsContinue) {
  ASSERT_TRUE(append(0).ok());
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
  Status error;
  do {
    auto result = source->poll(1, 4, 100);
    if (!result.ok()) {
      error = result.status();
      break;
    }
    ASSERT_TRUE(result->empty());
  } while (std::chrono::steady_clock::now() < deadline);
  ASSERT_EQ(error.code(), ErrorCode::ResourceExhausted);
  ASSERT_TRUE(source->pause(partition()).ok());
  ASSERT_TRUE(append(1, {}, "other").ok());
  auto other = read(1);
  ASSERT_EQ(other.size(), 1U);
  EXPECT_EQ(other[0].token.partition.partition, 1);
  ASSERT_TRUE(source->resume(partition()).ok());
  auto buffered = read(1);
  ASSERT_EQ(buffered.size(), 1U);
  EXPECT_EQ(buffered[0].token.partition.partition, 0);
  EXPECT_EQ(buffered[0].token.offset, 0);
}

TEST_P(MessageSourceContract, SeekPreservesPauseAndDoesNotCommitForwardSkip) {
  ASSERT_TRUE(append(0).ok());
  auto original = read(1);
  ASSERT_EQ(original.size(), 1U);
  ASSERT_TRUE(source->pause(partition()).ok());
  ASSERT_TRUE(source->seek(partition(), 0).ok());
  EXPECT_TRUE(take(source->poll(1, 100, 100)).empty());
  EXPECT_FALSE(source->ack(original[0].token).ok());
  EXPECT_EQ(take(source->committed_position(partition())), -1);
  ASSERT_TRUE(source->resume(partition()).ok());
  auto replay = read(1);
  ASSERT_EQ(replay.size(), 1U);
  ASSERT_TRUE(source->seek(partition(), 1).ok());
  EXPECT_EQ(take(source->committed_position(partition())), -1);
}

TEST_P(MessageSourceContract, KafkaRetriesOriginalAckAfterCommitError) {
  if (!GetParam())
    GTEST_SKIP() << "Kafka protocol error injection only";
  ASSERT_TRUE(append(0).ok());
  ASSERT_TRUE(append(0).ok());
  auto messages = read(2);
  ASSERT_EQ(messages.size(), 2U);
  rd_kafka_mock_push_request_errors(cluster, 8, 1, RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED);
  const auto failed = source->ack(messages[0].token);
  ASSERT_EQ(failed.code(), ErrorCode::UnknownCommit) << failed.message();
  EXPECT_FALSE(source->ack(messages[1].token).ok());
  EXPECT_FALSE(source->seek(partition(), 0).ok());
  EXPECT_EQ(take(source->committed_position(partition())), -1);
  ASSERT_TRUE(source->ack(messages[0].token).ok());
  EXPECT_EQ(take(source->committed_position(partition())), 1);
  ASSERT_TRUE(source->ack(messages[1].token).ok());
  EXPECT_EQ(take(source->committed_position(partition())), 2);
}

TEST_P(MessageSourceContract, KafkaReopensAtBrokerCommitAndCanReplayCheckpoint) {
  if (!GetParam())
    GTEST_SKIP() << "broker persistence only; MemMock is instance-local";
  ASSERT_TRUE(append(0).ok());
  ASSERT_TRUE(append(0).ok());
  ASSERT_TRUE(append(0).ok());
  auto messages = read(2);
  ASSERT_EQ(messages.size(), 2U);
  ASSERT_TRUE(source->ack(messages[0].token).ok());
  ASSERT_TRUE(open_source().ok());
  auto resumed = read(2);
  ASSERT_EQ(resumed.size(), 2U);
  EXPECT_EQ(resumed[0].token.offset, 1);
  EXPECT_EQ(resumed[1].token.offset, 2);
  EXPECT_FALSE(source->ack(messages[1].token).ok());
  ASSERT_TRUE(source->ack(resumed[0].token).ok());
  ASSERT_TRUE(source->ack(resumed[1].token).ok());
  config.assignments[0].next_offset = 0;
  ASSERT_TRUE(open_source().ok());
  auto checkpoint = read(1);
  ASSERT_EQ(checkpoint.size(), 1U);
  EXPECT_EQ(checkpoint[0].token.offset, 0);
  EXPECT_EQ(take(source->committed_position(partition())), 3);
}

TEST_P(MessageSourceContract, KafkaRejectsOutOfRangeRecoveryInsteadOfResetting) {
  if (!GetParam())
    GTEST_SKIP() << "Kafka broker offset range only";
  ASSERT_TRUE(append(0).ok());
  auto first = read(1);
  ASSERT_EQ(first.size(), 1U);
  ASSERT_TRUE(source->seek(partition(), 100).ok());
  bool rejected = false;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
  do {
    auto result = source->poll(1, 100, 100);
    if (!result.ok()) {
      rejected = true;
      break;
    }
    ASSERT_TRUE(result->empty())
        << "an invalid recovery position must not silently reset to earliest";
  } while (std::chrono::steady_clock::now() < deadline);
  EXPECT_TRUE(rejected);
  EXPECT_EQ(take(source->committed_position(partition())), -1);
}

TEST_P(MessageSourceContract, InflightByteBudgetIsReleasedByAcknowledgement) {
  config.max_message_bytes = 8;
  config.max_inflight_bytes = 8;
  ASSERT_TRUE(open_source().ok());
  ASSERT_TRUE(append(0, {}, "12345678").ok());
  ASSERT_TRUE(append(0, {}, "9").ok());
  auto first = read(1);
  ASSERT_EQ(first.size(), 1U);
  EXPECT_TRUE(take(source->poll(10, 100, 100)).empty());
  ASSERT_TRUE(source->ack(first[0].token).ok());
  auto next = read(1);
  ASSERT_EQ(next.size(), 1U);
  EXPECT_EQ(next[0].payload, "9");
}

TEST_P(MessageSourceContract, ByteLimitedBatchesKeepAllSubsequentRecords) {
  ASSERT_TRUE(append(0).ok());
  ASSERT_TRUE(append(0).ok());
  ASSERT_TRUE(append(0).ok());
  auto messages = read(3, 5);
  ASSERT_EQ(messages.size(), 3U);
  for (std::size_t i = 0; i < messages.size(); ++i) {
    EXPECT_EQ(messages[i].token.offset, static_cast<std::int64_t>(i));
    ASSERT_TRUE(source->ack(messages[i].token).ok());
  }
  EXPECT_EQ(take(source->committed_position(partition())), 3);
}

TEST_P(MessageSourceContract, KafkaOversizedRecordIsNotAcknowledgedOrSilentlySkipped) {
  if (!GetParam())
    GTEST_SKIP() << "MemMock rejects oversized records at append";
  ASSERT_TRUE(append(0, {}, "ok").ok());
  ASSERT_TRUE(append(0, {}, std::string(65, 'x')).ok());
  std::vector<SourceMessage> delivered;
  Status error;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
  do {
    auto result = source->poll(10, 1000, 100);
    if (!result.ok()) {
      error = result.status();
      break;
    }
    for (auto& message : result.value())
      delivered.push_back(std::move(message));
  } while (std::chrono::steady_clock::now() < deadline);
  EXPECT_EQ(error.code(), ErrorCode::ResourceExhausted);
  ASSERT_EQ(delivered.size(), 1U);
  EXPECT_EQ(delivered[0].payload, "ok");
  ASSERT_TRUE(source->ack(delivered[0].token).ok());
  EXPECT_EQ(take(source->committed_position(partition())), 1);
  EXPECT_EQ(source->poll(10, 1000, 0).status().code(), ErrorCode::ResourceExhausted);
  ASSERT_TRUE(source->seek(partition(), 2).ok());
  EXPECT_EQ(take(source->committed_position(partition())), 1);
}

INSTANTIATE_TEST_SUITE_P(MemAndKafkaWire, MessageSourceContract, ::testing::Bool());
} // namespace
} // namespace eventflow::test
