#include "eventflow/key_codec.hpp"
#include "legacy_feature_fixture.hpp"
#include "remote_harness.hpp"
#include "status_test_support.hpp"
#include <filesystem>
#include <gtest/gtest.h>

namespace eventflow::test {
namespace {
using example::FeatureComputationDemo;

class DemoObservedStore final : public IStateStore {
public:
  explicit DemoObservedStore(std::shared_ptr<IStateStore> store) noexcept
      : backend(std::move(store)) {}

  StatusOr<ReadResult> get_many(std::uint32_t shard,
                                const std::vector<std::string>& keys) noexcept override {
    return backend->get_many(shard, keys);
  }

  StatusOr<ScanResult> scan(std::uint32_t shard, const std::string& prefix,
                            const std::optional<std::string>& after,
                            std::size_t limit) noexcept override {
    if (!watched_prefix.empty() && prefix.starts_with(watched_prefix)) {
      ++watched_scans;
      if (deny_watched_scan)
        return Status(ErrorCode::ResourceExhausted,
                      "request history scan is outside recovery budget");
    }
    return backend->scan(shard, prefix, after, limit);
  }

  StatusOr<StoreLimits> limits() noexcept override {
    return backend->limits();
  }

  StatusOr<ShardStatus> status(std::uint32_t shard) noexcept override {
    return backend->status(shard);
  }

  StatusOr<CommitResult> commit(const CommitBatch& batch) noexcept override {
    EF_ASSIGN_OR_RETURN(auto result, backend->commit(batch));
    if (lose_next) {
      lose_next = false;
      return CommitResult{CommitCode::Unknown, batch.expected_revision, false,
                          "lost demo response"};
    }
    return result;
  }

  bool lose_next = false;
  std::shared_ptr<IStateStore> backend;
  std::string watched_prefix;
  std::size_t watched_scans = 0;
  bool deny_watched_scan = false;
};

class FeatureComputationDemoContract : public testing::TestWithParam<int> {
public:
  ~FeatureComputationDemoContract() noexcept override {
    demo.reset();
    store.reset();
    observed.reset();
    if (!path.empty()) {
      std::error_code error;
      std::filesystem::remove_all(path, error);
    }
  }

  Status start(const example::DemoConfig& config = {}) noexcept {
    StoreConfig storage;
    storage.shard_count = 1;
    std::unique_ptr<IStateStore> backend;
    if (GetParam() % 2 == 0) {
      EF_ASSIGN_OR_RETURN(backend, MemoryStateStore::open(storage));
    } else {
      char pattern[] = "/tmp/eventflow-feature-computation-demo-XXXXXX";
      const auto* made = ::mkdtemp(pattern);
      if (!made)
        return Status(ErrorCode::Unavailable, "temporary directory unavailable");
      path = made;
      EF_ASSIGN_OR_RETURN(backend, RocksStateStore::open(path + "/db", storage));
    }
    if (GetParam() >= 2) {
      EF_ASSIGN_OR_RETURN(backend, RemoteHarnessStore::open(std::move(backend)));
    }
    observed =
        std::make_shared<DemoObservedStore>(std::shared_ptr<IStateStore>(std::move(backend)));
    store = observed;
    EF_ASSIGN_OR_RETURN(demo, FeatureComputationDemo::open(store, config));
    return {};
  }

  Status news(std::int64_t time, std::string id = "n1", std::int64_t version = 1) noexcept {
    return demo->ingest(
        "news", {std::string("T"), std::move(id), version, std::string("earnings update"), 100.0},
        time);
  }

  Status quote(std::int64_t time, double bid = 100.1, double ask = 100.3) noexcept {
    return demo->ingest("quote", {std::string("T"), bid, ask, time}, time);
  }

  Status history(std::int64_t time, std::int64_t id, double price) noexcept {
    return demo->ingest("history", {std::string("T"), id, price, time}, time);
  }

  std::vector<RequestRecord> accepted(std::string_view plan) noexcept {
    std::vector<RequestRecord> result;
    for (const auto& event : demo->request_events())
      if (event.result.disposition == RequestDisposition::Accepted && event.result.record &&
          event.result.record->admission.plan == plan)
        result.push_back(*event.result.record);
    return result;
  }

  RequestBindingProgress progress(const std::string& binding) noexcept {
    const auto candidate = take(demo->processor().read_candidate({std::string("T")}));
    for (const auto& value : candidate.bindings)
      if (value.binding_id == binding)
        return value;
    ADD_FAILURE() << "missing candidate binding " << binding;
    return {};
  }

  std::string path;
  std::shared_ptr<IStateStore> store;
  std::shared_ptr<DemoObservedStore> observed;
  std::unique_ptr<FeatureComputationDemo> demo;
};

TEST_P(FeatureComputationDemoContract,
       EmbeddingCompletesBeforeMarketThenInitialPredictionUsesSharedGraph) {
  auto opened = start();
  ASSERT_TRUE(opened.ok()) << opened.message();
  ASSERT_TRUE(news(1).ok());
  ASSERT_EQ(accepted("A").size(), 1U);
  ASSERT_EQ(take(demo->run_functions(2)), 1U);
  EXPECT_TRUE(accepted("B").empty());
  ASSERT_TRUE(history(3, 1, 99.0).ok());
  ASSERT_TRUE(history(4, 2, 100.0).ok());
  ASSERT_TRUE(quote(5).ok());
  ASSERT_EQ(accepted("B").size(), 1U);
  EXPECT_EQ(take(demo->run_functions(6, 1)), 1U);
  EXPECT_EQ(take(demo->run_functions(6, 1)), 1U);
  EXPECT_TRUE(
      take(demo->query_sql("SELECT expected_return FROM latest_prediction WHERE ticker='T'"))
          .rows.empty());
  EXPECT_EQ(take(demo->run_functions(6, 1)), 1U);
  const auto prediction =
      take(demo->query_sql("SELECT expected_return FROM latest_prediction WHERE ticker='T'"));
  const auto risk = take(demo->query_sql("SELECT risk_score FROM latest_risk WHERE ticker='T'"));
  ASSERT_EQ(prediction.rows.size(), 1U);
  ASSERT_EQ(risk.rows.size(), 1U);
  EXPECT_EQ(demo->executor_metrics().callback_calls, 4U);
  EXPECT_EQ(take(demo->run_functions(7)), 0U);
}

TEST_P(FeatureComputationDemoContract, ConfirmationBeforeEmbeddingWaitsForInitialAdmission) {
  ASSERT_TRUE(start().ok());
  ASSERT_TRUE(news(1).ok());
  ASSERT_TRUE(history(100'000, 1, 99.0).ok());
  ASSERT_TRUE(history(200'000, 2, 100.0).ok());
  ASSERT_TRUE(
      demo->ingest("trade", {std::string("T"), std::int64_t{1}, std::int64_t{100}}, 300'000).ok());
  ASSERT_TRUE(quote(400'000).ok());
  EXPECT_TRUE(accepted("B").empty());
  EXPECT_TRUE(accepted("C").empty());
  const auto pending = take(demo->processor().pending_occurrences(16));
  ASSERT_EQ(pending.size(), 1U);
  EXPECT_EQ(pending[0].rule_id, "confirm_c");
  EXPECT_EQ(take(demo->run_functions(1'000'000, 1)), 1U);
  ASSERT_EQ(accepted("B").size(), 1U);
  ASSERT_EQ(accepted("C").size(), 1U);
  const auto b_admission = accepted("B")[0].admission;
  EXPECT_NE(take(demo->read_request({b_admission.scope, b_admission.request_id})).state.phase,
            RequestPhase::Success);
  EXPECT_EQ(take(demo->run_functions(1'000'000)), 6U);
  EXPECT_LT(accepted("B")[0].state.manifest.admission_seq,
            accepted("C")[0].state.manifest.admission_seq);
  ASSERT_TRUE(quote(1'100'000).ok());
  EXPECT_EQ(accepted("C").size(), 1U);
}

TEST_P(FeatureComputationDemoContract, MissedPeriodicSlotsNeverBackfillWhenMarketReturns) {
  ASSERT_TRUE(start().ok());
  ASSERT_TRUE(news(1).ok());
  ASSERT_TRUE(history(2, 1, 99.0).ok());
  ASSERT_TRUE(history(3, 2, 100.0).ok());
  ASSERT_TRUE(quote(4).ok());
  EXPECT_EQ(take(demo->run_functions(5)), 4U);
  ASSERT_EQ(accepted("B").size(), 1U);
  for (const auto now : {5'000'000, 10'000'000, 15'000'000})
    ASSERT_TRUE(demo->pump(now).ok());
  EXPECT_TRUE(accepted("D").empty());
  EXPECT_EQ(progress("d").skipped_count, 3U);
  EXPECT_EQ(progress("d").last_slot_or_time, 15'000'000);
  EXPECT_EQ(progress("d").reason, RequestPendingReason::NotReady);
  EXPECT_TRUE(take(demo->processor().pending_occurrences(16)).empty());
  demo.reset();
  demo = take(FeatureComputationDemo::open(store));
  EXPECT_EQ(progress("d").skipped_count, 3U);
  EXPECT_EQ(progress("d").reason, RequestPendingReason::NotReady);
  ASSERT_TRUE(quote(16'000'000).ok());
  EXPECT_TRUE(accepted("D").empty());
  ASSERT_TRUE(quote(19'500'000).ok());
  ASSERT_TRUE(demo->pump(20'000'000).ok());
  ASSERT_EQ(accepted("D").size(), 1U);
  EXPECT_EQ(accepted("D")[0].state.manifest.admitted_at_us, 20'000'000);
  EXPECT_EQ(take(demo->run_functions(20'000'001)), 3U);
  ASSERT_TRUE(demo->pump(40'000'000).ok());
  EXPECT_EQ(accepted("D").size(), 1U);
  ASSERT_FALSE(demo->clock_events().empty());
  EXPECT_EQ(demo->clock_events().back().skipped_slots, 3U);
  ASSERT_TRUE(quote(124'500'000).ok());
  ASSERT_TRUE(demo->pump(125'000'000).ok());
  EXPECT_EQ(accepted("D").size(), 1U);
  ASSERT_TRUE(history(125'000'001, 3, 101.0).ok());
  ASSERT_TRUE(history(125'000'002, 4, 102.0).ok());
  ASSERT_TRUE(quote(129'500'000).ok());
  ASSERT_TRUE(demo->pump(130'000'000).ok());
  EXPECT_EQ(accepted("D").size(), 2U);
  EXPECT_EQ(take(demo->run_functions(130'000'001)), 3U);
}

TEST_P(FeatureComputationDemoContract, RevisionFencesOldCompletionAndNewNewsReplacesSameTicker) {
  ASSERT_TRUE(start().ok());
  ASSERT_TRUE(news(1).ok());
  const auto old = accepted("A")[0];
  ASSERT_TRUE(news(2, "n1", 2).ok());
  ASSERT_EQ(accepted("A").size(), 2U);
  EXPECT_EQ(take(demo->run_functions(3, 1)), 1U);
  EXPECT_TRUE(
      take(demo->query_sql("SELECT vector FROM latest_embedding WHERE ticker='T'")).rows.empty());
  EXPECT_EQ(take(demo->read_request({old.admission.scope, old.admission.request_id})).state.phase,
            RequestPhase::Success);
  EXPECT_EQ(take(demo->run_functions(4, 1)), 1U);
  auto latest = take(
      demo->query_sql("SELECT __ef_latest_candidate_epoch FROM latest_embedding WHERE ticker='T'"));
  ASSERT_EQ(latest.rows.size(), 1U);
  EXPECT_EQ(std::get<std::int64_t>(latest.rows[0][0]), 2);
  ASSERT_TRUE(news(5, "n2", 1).ok());
  ASSERT_EQ(accepted("A").size(), 3U);
  EXPECT_TRUE(
      take(demo->query_sql("SELECT vector FROM latest_embedding WHERE ticker='T'")).rows.empty());
  ASSERT_TRUE(news(6, "n1", 1).ok());
  EXPECT_EQ(accepted("A").size(), 3U);
  ASSERT_TRUE(news(7, "n2", 1).ok());
  EXPECT_EQ(accepted("A").size(), 3U);
  EXPECT_EQ(take(demo->run_functions(8)), 1U);
  EXPECT_EQ(demo->executor_metrics().callback_calls, 1U);
  EXPECT_EQ(demo->executor_metrics().cache_hits, 2U);
  auto candidate =
      take(demo->query_sql("SELECT news_id, text_version FROM candidate WHERE ticker='T'"));
  ASSERT_EQ(candidate.rows.size(), 1U);
  EXPECT_EQ(std::get<std::string>(candidate.rows[0][0]), "n2");
}

TEST_P(FeatureComputationDemoContract, NoInputExpiresReadinessAndCandidateWithoutNewAdmission) {
  ASSERT_TRUE(start().ok());
  ASSERT_TRUE(news(1).ok());
  ASSERT_EQ(take(demo->run_functions(2)), 1U);
  ASSERT_TRUE(demo->pump(2'000'001).ok());
  EXPECT_TRUE(accepted("B").empty());
  EXPECT_TRUE(progress("b").expired);
  EXPECT_EQ(progress("b").reason, RequestPendingReason::Expired);
  ASSERT_TRUE(history(2'100'000, 1, 99.0).ok());
  ASSERT_TRUE(history(2'200'000, 2, 100.0).ok());
  ASSERT_TRUE(quote(2'300'000).ok());
  EXPECT_TRUE(accepted("B").empty());
  ASSERT_TRUE(demo->pump(300'000'001).ok());
  auto candidate = take(demo->query_sql("SELECT active FROM candidate WHERE ticker='T'"));
  ASSERT_EQ(candidate.rows.size(), 1U);
  EXPECT_FALSE(std::get<bool>(candidate.rows[0][0]));
  EXPECT_TRUE(
      take(demo->query_sql("SELECT vector FROM latest_embedding WHERE ticker='T'")).rows.empty());
  EXPECT_TRUE(accepted("D").empty());
}

TEST_P(FeatureComputationDemoContract, InvalidPriceSamplesAndInvalidQuoteDoNotSatisfyReady) {
  ASSERT_TRUE(start().ok());
  ASSERT_TRUE(news(1).ok());
  ASSERT_EQ(take(demo->run_functions(2)), 1U);
  ASSERT_TRUE(history(3, 1, -1.0).ok());
  ASSERT_TRUE(history(4, 2, 100.0).ok());
  ASSERT_TRUE(quote(5, 0.0, 100.0).ok());
  EXPECT_TRUE(accepted("B").empty());
  ASSERT_TRUE(history(6, 3, 101.0).ok());
  ASSERT_TRUE(quote(7, 101.0, 100.0).ok());
  EXPECT_TRUE(accepted("B").empty());
  ASSERT_TRUE(quote(8, 100.0, 100.0).ok());
  ASSERT_EQ(accepted("B").size(), 1U);
  EXPECT_EQ(take(demo->run_functions(9)), 3U);
}

TEST_P(FeatureComputationDemoContract, NewsHighWaterSurvivesBodyTtlAndDriverReopen) {
  ASSERT_TRUE(start().ok());
  ASSERT_TRUE(news(1, "n1", 3).ok());
  EXPECT_EQ(take(demo->run_functions(2)), 1U);
  ASSERT_TRUE(news(1'000'000, "n2", 1).ok());
  EXPECT_EQ(take(demo->run_functions(1'000'001)), 1U);
  ASSERT_TRUE(demo->pump(300'000'002).ok());
  const auto old_body =
      take(demo->query_sql("SELECT news_id FROM news WHERE ticker='T' AND news_id='n1'"));
  EXPECT_TRUE(old_body.rows.empty());
  demo.reset();
  demo = take(FeatureComputationDemo::open(store));
  ASSERT_TRUE(news(300'000'003, "n1", 2).ok());
  ASSERT_TRUE(news(300'000'004, "n1", 3).ok());
  EXPECT_TRUE(accepted("A").empty());
  auto candidate =
      take(demo->query_sql("SELECT news_id,candidate_epoch FROM candidate WHERE ticker='T'"));
  EXPECT_EQ(std::get<std::string>(candidate.rows[0][0]), "n2");
  EXPECT_EQ(std::get<std::int64_t>(candidate.rows[0][1]), 2);
  ASSERT_TRUE(news(300'000'005, "n1", 4).ok());
  ASSERT_EQ(accepted("A").size(), 1U);
  EXPECT_EQ(accepted("A")[0].state.manifest.candidate_epoch, 3U);
  EXPECT_EQ(take(demo->run_functions(300'000'006)), 1U);
}

TEST_P(FeatureComputationDemoContract, DurableTasksRouteAfterReopenWithoutAnApplicationDag) {
  ASSERT_TRUE(start().ok());
  ASSERT_TRUE(news(1).ok());
  const auto key = accepted("A")[0].admission;
  observed->watched_prefix =
      take(table_prefix(0, 'T', demo->definition().relations.request_status));
  observed->deny_watched_scan = true;
  demo.reset();
  auto restarted = FeatureComputationDemo::open(store);
  ASSERT_TRUE(restarted.ok()) << restarted.status().message();
  demo = std::move(*restarted);
  EXPECT_EQ(observed->watched_scans, 0U);
  observed->deny_watched_scan = false;
  EXPECT_EQ(take(demo->run_functions(2)), 1U);
  EXPECT_EQ(take(demo->read_request({key.scope, key.request_id})).state.phase,
            RequestPhase::Success);
  EXPECT_EQ(
      take(demo->query_sql("SELECT vector FROM latest_embedding WHERE ticker='T'")).rows.size(),
      1U);
  ASSERT_TRUE(news(3).ok());
  EXPECT_TRUE(accepted("A").empty());
}

TEST_P(FeatureComputationDemoContract,
       QuoteAgeBoundaryIsInclusiveAndMalformedTimestampCannotOverflow) {
  ASSERT_TRUE(start().ok());
  ASSERT_TRUE(news(1).ok());
  EXPECT_EQ(take(demo->run_functions(2)), 1U);
  ASSERT_TRUE(history(3, 1, 99.0).ok());
  ASSERT_TRUE(history(4, 2, 100.0).ok());
  ASSERT_TRUE(demo->ingest("quote", {std::string("T"), 100.0, 100.0, INT64_MIN}, 5).ok());
  EXPECT_TRUE(accepted("B").empty());
  ASSERT_TRUE(
      demo->ingest("quote", {std::string("T"), 100.0, 100.0, std::int64_t{0}}, 2'000'000).ok());
  EXPECT_EQ(accepted("B").size(), 1U);
}

TEST_P(FeatureComputationDemoContract, ConfirmationDeadlineAndTradeWindowUseApplicationPolicy) {
  example::DemoConfig config;
  config.confirmation_return = 0.01;
  config.confirmation_volume = 200;
  ASSERT_TRUE(start(config).ok());
  ASSERT_TRUE(news(1).ok());
  ASSERT_TRUE(history(2, 1, 99.0).ok());
  ASSERT_TRUE(history(3, 2, 100.0).ok());
  ASSERT_TRUE(quote(4).ok());
  EXPECT_EQ(take(demo->run_functions(5)), 4U);
  ASSERT_TRUE(
      demo->ingest("trade", {std::string("T"), std::int64_t{1}, std::int64_t{199}}, 1'000'000)
          .ok());
  ASSERT_TRUE(quote(1'100'000, 101.0, 101.0).ok());
  EXPECT_TRUE(accepted("C").empty());
  ASSERT_TRUE(demo->pump(11'000'000).ok());
  ASSERT_TRUE(
      demo->ingest("trade", {std::string("T"), std::int64_t{2}, std::int64_t{1}}, 11'000'001).ok());
  ASSERT_TRUE(quote(11'000'002, 101.0, 101.0).ok());
  EXPECT_TRUE(accepted("C").empty());
  ASSERT_TRUE(
      demo->ingest("trade", {std::string("T"), std::int64_t{3}, std::int64_t{200}}, 29'000'000)
          .ok());
  ASSERT_TRUE(quote(30'000'002, 102.0, 102.0).ok());
  EXPECT_TRUE(accepted("C").empty());
}

TEST_P(FeatureComputationDemoContract, PendingConfirmationExpiresAtCandidateAbsoluteDeadline) {
  ASSERT_TRUE(start().ok());
  ASSERT_TRUE(news(1).ok());
  ASSERT_TRUE(history(2, 1, 99.0).ok());
  ASSERT_TRUE(history(3, 2, 100.0).ok());
  ASSERT_TRUE(
      demo->ingest("trade", {std::string("T"), std::int64_t{1}, std::int64_t{100}}, 300'000).ok());
  ASSERT_TRUE(quote(400'000).ok());
  ASSERT_EQ(take(demo->processor().pending_occurrences(16)).size(), 1U);
  EXPECT_EQ(take(demo->run_functions(3'000'000)), 1U);
  EXPECT_TRUE(progress("b").expired);
  EXPECT_TRUE(accepted("C").empty());
  ASSERT_TRUE(demo->pump(30'000'001).ok());
  EXPECT_TRUE(take(demo->processor().pending_occurrences(16)).empty());
  EXPECT_TRUE(progress("c").expired);
  EXPECT_EQ(progress("c").reason, RequestPendingReason::Expired);
  demo.reset();
  demo = take(FeatureComputationDemo::open(store));
  EXPECT_TRUE(progress("b").expired);
  EXPECT_TRUE(progress("c").expired);
  EXPECT_EQ(progress("c").reason, RequestPendingReason::Expired);
}

TEST_P(FeatureComputationDemoContract, EncoderCacheAndNewsHighWaterStayWithinTickerScope) {
  ASSERT_TRUE(start().ok());
  ASSERT_TRUE(news(1, "n1", 3).ok());
  ASSERT_TRUE(demo->ingest("news",
                           {std::string("OTHER"), std::string("n1"), std::int64_t{1},
                            std::string("earnings update"), 100.0},
                           2)
                  .ok());
  EXPECT_EQ(take(demo->run_functions(3)), 2U);
  EXPECT_EQ(demo->executor_metrics().callback_calls, 2U);
  EXPECT_EQ(demo->executor_metrics().cache_hits, 0U);
  EXPECT_EQ(
      take(demo->query_sql("SELECT vector FROM latest_embedding WHERE ticker='OTHER'")).rows.size(),
      1U);
}

TEST_P(FeatureComputationDemoContract, TimeoutDrainsQueuedCancellationAndReleasesExecutorCapacity) {
  ASSERT_TRUE(start().ok());
  ASSERT_TRUE(news(1).ok());
  const auto request = accepted("A")[0].admission;
  EXPECT_EQ(take(demo->run_functions(30'000'001)), 1U);
  EXPECT_EQ(take(demo->read_request({request.scope, request.request_id})).state.phase,
            RequestPhase::Failed);
  EXPECT_EQ(demo->executor_metrics().active_tasks, 0U);
  EXPECT_EQ(demo->executor_metrics().callback_calls, 0U);
  EXPECT_EQ(take(demo->run_functions(30'000'002)), 0U);
}

TEST_P(FeatureComputationDemoContract,
       ConfirmationAtExactlyThirtySecondsIsExpiredBeforeObservation) {
  ASSERT_TRUE(start().ok());
  ASSERT_TRUE(news(1).ok());
  ASSERT_TRUE(history(2, 1, 99.0).ok());
  ASSERT_TRUE(history(3, 2, 100.0).ok());
  ASSERT_TRUE(quote(4, 100.0, 100.0).ok());
  EXPECT_EQ(take(demo->run_functions(5)), 4U);
  ASSERT_TRUE(
      demo->ingest("trade", {std::string("T"), std::int64_t{1}, std::int64_t{100}}, 29'000'000)
          .ok());
  ASSERT_TRUE(quote(30'000'001, 101.0, 101.0).ok());
  EXPECT_TRUE(accepted("C").empty());
  EXPECT_TRUE(take(demo->processor().pending_occurrences(16)).empty());
}

TEST(FeatureComputationDemoDefinition,
     RejectsCollidingHighWaterNamespaceAndFingerprintsPolicyValues) {
  example::DemoConfig config;
  config.news_version_namespace_id = 0xffff0000U;
  EXPECT_FALSE(example::DemoDefinition::create(config).ok());
  config.news_version_namespace_id = 1;
  EXPECT_FALSE(example::DemoDefinition::create(config).ok());
  config = {};
  const auto first = take(example::DemoDefinition::create(config));
  config.confirmation_volume = 200;
  const auto second = take(example::DemoDefinition::create(config));
  const auto& first_rule = first.processor.rules[0].definition;
  const auto& second_rule = second.processor.rules[0].definition;
  ASSERT_FALSE(first_rule.definition_identity.empty());
  EXPECT_NE(first_rule.definition_identity, second_rule.definition_identity);
  // Immutable declaration identity is separate from mutable business registers.
  EXPECT_TRUE(first_rule.initial_registers.empty());
  auto original = take(FsmProgram::create(first_rule));
  auto changed = take(FsmProgram::create(second_rule));
  const auto bytes = take(original->encode_state(take(original->initial_state())));
  EXPECT_FALSE(changed->decode_state(bytes).ok());
}

TEST(FeatureComputationDemoDefinition, FiniteInputOverflowFailsInsteadOfPublishingZeroVolatility) {
  const auto definition = take(example::DemoDefinition::create());
  const auto* relation = take(definition.catalog.find(definition.relations.features));
  const auto& function = relation->function->definition;
  auto executor = take(SimpleFunctionExecutor::create(definition.registry));
  FunctionTask task;
  task.task_id = "extreme_history";
  task.function_id = function.function_id;
  task.function_version = function.function_version;
  task.implementation_identity = function.implementation_identity;
  task.inputs = {Row{std::string("T"), 100.0, 0.1},
                 Row{std::string("T"), std::string("request"), std::vector<double>{1e-300, 1e300}}};
  ASSERT_TRUE(executor->submit(task).ok());
  EXPECT_EQ(take(executor->run_ready(1)), 1U);
  const auto completions = take(executor->poll(1));
  ASSERT_EQ(completions.size(), 1U);
  EXPECT_FALSE(completions[0].status.ok());
  EXPECT_TRUE(completions[0].output.empty());
}

TEST_P(FeatureComputationDemoContract, DriverResolvesSourceUnknownAndSynchronizesDurableOffsets) {
  ASSERT_TRUE(start().ok());
  observed->lose_next = true;
  EXPECT_EQ(news(1).code(), ErrorCode::UnknownCommit);
  ASSERT_TRUE(demo->processor().has_pending_commit());
  ASSERT_TRUE(demo->pump(2).ok());
  EXPECT_FALSE(demo->processor().has_pending_commit());
  EXPECT_TRUE(news(3, "n1", 2).ok());
  auto positions = take(demo->processor().processed_positions());
  ASSERT_EQ(positions.size(), 1U);
  EXPECT_EQ(positions[0].next_offset, 2);
}

TEST_P(FeatureComputationDemoContract, DirectOwnerConfirmationCannotLeaveDriverOffsetBehind) {
  ASSERT_TRUE(start().ok());
  observed->lose_next = true;
  EXPECT_EQ(news(1).code(), ErrorCode::UnknownCommit);
  EXPECT_TRUE(take(demo->processor().resolve_pending_commit()).input_committed);
  ASSERT_TRUE(demo->pump(2).ok());
  EXPECT_TRUE(news(3, "n1", 2).ok());
}

TEST_P(FeatureComputationDemoContract, BatchOfPolledCompletionsSurvivesTheFirstUnknownCommit) {
  ASSERT_TRUE(start().ok());
  ASSERT_TRUE(news(1).ok());
  ASSERT_TRUE(news(2, "n1", 2).ok());
  auto requests = accepted("A");
  ASSERT_EQ(requests.size(), 2U);
  observed->lose_next = true;
  EXPECT_EQ(demo->run_functions(2).status().code(), ErrorCode::UnknownCommit);
  ASSERT_TRUE(take(demo->processor().resolve_pending_commit()).operation_status.ok());
  ASSERT_TRUE(demo->pump(3).ok());
  EXPECT_EQ(take(demo->run_functions(3)), 1U);
  EXPECT_EQ(
      take(demo->read_request({requests[1].admission.scope, requests[1].admission.request_id}))
          .state.phase,
      RequestPhase::Success);
  EXPECT_EQ(demo->executor_metrics().active_tasks, 0U);
}

INSTANTIATE_TEST_SUITE_P(FourKvAdapters, FeatureComputationDemoContract,
                         testing::Values(0, 1, 2, 3));
} // namespace
} // namespace eventflow::test
