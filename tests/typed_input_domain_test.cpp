#include "eventflow/event_processor.hpp"
#include "eventflow/key_codec.hpp"
#include "status_test_support.hpp"
#include <gtest/gtest.h>

namespace eventflow::test {
namespace {
class TypedInputDomainTest : public ::testing::TestWithParam<bool> {
protected:
  std::shared_ptr<MemoryStateStore> store;
  std::unique_ptr<Catalog> catalog;
  std::unique_ptr<EventProcessor> processor;
  ProcessorConfig config;
  EngineConfig engine_config{.shard_count = 1};
  RelationId news = 0;

  void SetUp() noexcept override {
    store = take(MemoryStateStore::open({.shard_count = 1, .shard_owners = {}}));
    catalog = take(Catalog::create());
    news = take(catalog->define_sql(
        "CREATE TABLE news(ticker STRING NOT NULL, id INT64 NOT NULL, sentiment INT64 NOT NULL, "
        "PRIMARY KEY(ticker,id)) PARTITION BY(ticker)"));
    config.input_relations = {news};
    const auto positive = [](const CepContext& context) noexcept -> StatusOr<bool> {
      return std::get<std::int64_t>(context.event.values[0]) > 0;
    };
    config.cep_rules = {
        {.definition = {.id = "positive_pair",
                        .pattern = CepPattern::event("first", "news", positive)
                                       .then(CepPattern::event("second", "news", positive),
                                             CepContiguity::Strict),
                        .within_us = 10,
                        .reads = {},
                        .register_fields = {},
                        .initial_registers = {},
                        .limits = {.max_paths = 8,
                                   .max_matches_per_step = 4,
                                   .max_event_bytes = 4096,
                                   .max_register_bytes = 4096,
                                   .max_read_bytes = 4096,
                                   .max_state_bytes = 64 * 1024}},
         .reads = {},
         .sink = {},
         .input_kinds = {"news"}}};
    config.periodic_policies = {{.id = "quote_refresh", .event_kind = "clock", .interval_us = 5}};
    if (GetParam())
      configure_request_owner();
  }

  void configure_request_owner() noexcept {
    auto registry = take(FunctionRegistry::create());
    const auto candidate = take(catalog->define_candidate(
        {.name = "news_session",
         .scope_fields = {{.id = 1, .name = "ticker", .type = DataType::String, .nullable = false}},
         .payload_fields = {
             {.id = 2, .name = "deadline", .type = DataType::Int64, .nullable = false}}}));
    const auto capture = take(catalog->define_sql(
        "CREATE TABLE capture(ticker STRING NOT NULL, request_id STRING NOT NULL, "
        "PRIMARY KEY(ticker,request_id)) PARTITION BY(ticker)",
        {.partition_key = {},
         .base_write_role = BaseWriteRole::RequestCapture,
         .identity_domain = "news_request"}));
    const auto snapshot = take(catalog->snapshot());
    const auto capture_schema = take(snapshot.find(capture))->storage_schema.fields;
    const std::vector<Field> output = {
        {.id = 1, .name = "score", .type = DataType::Int64, .nullable = false}};
    const auto target = take(catalog->define_function(
        {.name = "news_score",
         .function_id = "score",
         .function_version = 1,
         .implementation_identity = "test.score.v1",
         .inputs =
             {{.alias = "input", .relation = capture, .required = true, .fields = capture_schema}},
         .output_fields = output}));
    ASSERT_TRUE(registry
                    ->add({.signature = {.function_id = "score",
                                         .version = 1,
                                         .implementation_identity = "test.score.v1",
                                         .inputs = {{.alias = "input",
                                                     .required = true,
                                                     .fields = capture_schema}},
                                         .output_fields = output,
                                         .pure = true},
                           .callback = [](const FunctionInputs&) noexcept -> StatusOr<Row> {
                             return Row{std::int64_t{1}};
                           }})
                    .ok());
    config.requests.emplace();
    config.requests->registry = std::move(registry);
    config.requests->plans = {
        {.name = "score_request",
         .targets = {target},
         .machine = {},
         .capture = [](RoundContext&, const RequestAdmission&) noexcept
             -> StatusOr<RequestCapture> { return RequestCapture{.ready = false, .rows = {}}; }}};
    config.requests->candidate =
        CandidateExpiryPolicy{.relation = candidate,
                              .deadline_column = "deadline",
                              .on_source = [](RoundContext&, const ProcessorEvent&) noexcept
                                  -> StatusOr<std::optional<CandidateActivation>> {
                                return std::optional<CandidateActivation>{};
                              }};
  }

  void open() noexcept {
    processor.reset();
    processor = take(EventProcessor::open(store, take(catalog->snapshot()), engine_config, config));
  }

  ProcessResult input(std::int64_t offset, std::string kind, std::int64_t at,
                      std::int64_t sentiment = 1) noexcept {
    auto result = take(processor->process(
        {.source = {.source_id = "source", .topic = "feed", .partition = 0, .offset = offset},
         .scope = {std::string{"ABC"}},
         .kind = std::move(kind),
         .values = {sentiment},
         .changes = {{.relation = news,
                      .identity = {std::string{"ABC"}, offset},
                      .row = Row{std::string{"ABC"}, offset, sentiment}}},
         .processing_time_us = at}));
    EXPECT_TRUE(result.operation_status.ok()) << result.operation_status.message();
    EXPECT_TRUE(result.input_committed);
    return result;
  }

  CepStateSummary state() noexcept {
    return take(processor->read_cep("positive_pair", {std::string{"ABC"}}));
  }

  std::vector<TriggerOccurrence> output() noexcept {
    return take(processor->pending_occurrences(16));
  }

  void snapshot_restore() noexcept {
    take(store->create_snapshot("input-domain"));
    auto restored = take(MemoryStateStore::from_snapshot(*store, "input-domain"));
    processor.reset();
    store = std::move(restored);
    open();
  }

  Json persisted_definition() noexcept {
    const auto key = take(row_key(0, 'S', config.namespace_id, {std::string{"lifecycle"}}));
    const auto loaded = take(store->get_many(0, {key}));
    const auto state = Json::parse(*loaded.values[0]);
    return Json::parse(state["fingerprint"].get<std::string>());
  }
};

TEST_P(TypedInputDomainTest, StrictNewsIgnoresQuoteAndClockButNegativeNewsConsumesTheNextPosition) {
  open();
  input(0, "news", 0);
  input(1, "quote", 1);
  EXPECT_EQ(state().event_sequence, 1U);
  EXPECT_EQ(state().time_us, 1);
  ASSERT_EQ(state().paths.size(), 1U);
  auto advanced = take(processor->advance(5));
  ASSERT_TRUE(advanced.operation_status.ok()) << advanced.operation_status.message();
  EXPECT_EQ(state().event_sequence, 1U);
  EXPECT_EQ(state().time_us, 5);
  ASSERT_EQ(state().paths.size(), 1U);
  input(2, "news", 6, -1);
  EXPECT_EQ(state().event_sequence, 2U);
  EXPECT_TRUE(state().paths.empty());
  input(3, "news", 7, 2);
  EXPECT_TRUE(output().empty());
  input(4, "news", 8, 3);
  const auto matches = output();
  ASSERT_EQ(matches.size(), 1U);
  ASSERT_TRUE(matches[0].cep_match);
  EXPECT_EQ(matches[0].cep_match->events[0].event.values, Row{std::int64_t{2}});
  EXPECT_EQ(matches[0].cep_match->events[1].event.values, Row{std::int64_t{3}});
}

TEST_P(TypedInputDomainTest, EmptyDomainRetainsRawStreamAdjacencyAndLegacyFingerprintShape) {
  config.cep_rules[0].input_kinds.clear();
  open();
  input(0, "news", 0);
  input(1, "quote", 1);
  EXPECT_TRUE(state().paths.empty());
  EXPECT_EQ(state().event_sequence, 2U);
  input(2, "news", 2);
  ASSERT_TRUE(take(processor->advance(5)).operation_status.ok());
  EXPECT_TRUE(state().paths.empty());
  EXPECT_EQ(state().event_sequence, 4U);
  const auto persisted = persisted_definition();
  ASSERT_EQ(persisted["cep"].size(), 1U);
  EXPECT_EQ(persisted["cep"][0].size(), 3U);
  const auto program = take(CepProgram::create(config.cep_rules[0].definition));
  EXPECT_EQ(persisted["cep"][0],
            Json::array({std::string{program->fingerprint()}, Json::object(), nullptr}));
  open();
  EXPECT_EQ(state().event_sequence, 4U);
}

TEST_P(TypedInputDomainTest, DomainAndLivePathSurviveReopenSnapshotAndExclusiveDeadline) {
  open();
  input(0, "news", 0);
  open();
  snapshot_restore();
  ASSERT_EQ(state().paths.size(), 1U);
  input(1, "quote", 10);
  EXPECT_TRUE(state().paths.empty());
  EXPECT_EQ(state().event_sequence, 1U);
  EXPECT_EQ(state().time_us, 10);
  input(2, "news", 11, 2);
  input(3, "news", 12, 3);
  ASSERT_EQ(output().size(), 1U);
  const auto original = output()[0].id;
  snapshot_restore();
  ASSERT_EQ(output().size(), 1U);
  EXPECT_EQ(output()[0].id, original);
  EXPECT_EQ(take(processor->processed_positions())[0].next_offset, 4);
}

TEST_P(TypedInputDomainTest, ExcludedInputReleasesPendingMatchWhenAnOlderPathExpires) {
  config.periodic_policies.clear();
  auto& definition = config.cep_rules[0].definition;
  definition.skip = CepSkipStrategy::SkipToNext;
  definition.pattern =
      CepPattern::event("first", "news")
          .then(CepPattern::event("second", "news",
                                  [](const CepContext& context) noexcept -> StatusOr<bool> {
                                    return std::get<std::int64_t>(context.event.values[0]) >
                                           std::get<std::int64_t>(
                                               context.history[0].event.values[0]);
                                  }),
                CepContiguity::FollowedByAny);
  open();
  input(0, "news", 0, 100);
  input(1, "news", 1, 1);
  input(2, "news", 2, 2);
  ASSERT_EQ(state().pending_matches, 1U);
  ASSERT_TRUE(output().empty());
  snapshot_restore();
  input(3, "quote", 10, 200);
  const auto matches = output();
  ASSERT_EQ(matches.size(), 1U);
  ASSERT_TRUE(matches[0].cep_match);
  EXPECT_EQ(matches[0].cep_match->events[0].event.values, Row{std::int64_t{1}});
  EXPECT_EQ(matches[0].cep_match->events[1].event.values, Row{std::int64_t{2}});
  EXPECT_EQ(matches[0].cep_match->matched_at_us, 2);
  EXPECT_EQ(matches[0].cep_match->emitted_at_us, 10);
  EXPECT_EQ(state().event_sequence, 3U);
  EXPECT_EQ(state().pending_matches, 0U);
}

TEST_P(TypedInputDomainTest, ChangingTheInputDomainRejectsPersistedRuntimeState) {
  open();
  input(0, "news", 0);
  processor.reset();
  for (const auto& replacement :
       std::vector<std::vector<std::string>>{{}, {"news", "quote"}, {"quote"}}) {
    auto changed = config;
    changed.cep_rules[0].input_kinds = replacement;
    const auto reopened =
        EventProcessor::open(store, take(catalog->snapshot()), engine_config, changed);
    EXPECT_FALSE(reopened.ok());
  }
  open();
  EXPECT_EQ(state().event_sequence, 1U);
}

TEST_P(TypedInputDomainTest, RejectsInvalidDomainsBeforeInitializingStorage) {
  std::vector<std::vector<std::string>> invalid = {{""},
                                                   {"*"},
                                                   {"$timeout"},
                                                   {"news", "news"},
                                                   {std::string(129, 'x')},
                                                   {std::string("new\0s", 5)},
                                                   {std::string("\xff", 1)}};
  std::vector<std::string> too_many;
  for (std::size_t index = 0; index < 65; ++index)
    too_many.push_back("input_" + std::to_string(index));
  invalid.push_back(std::move(too_many));
  for (const auto& kinds : invalid) {
    auto changed = config;
    changed.cep_rules[0].input_kinds = kinds;
    const auto rejected =
        EventProcessor::open(store, take(catalog->snapshot()), engine_config, changed);
    EXPECT_FALSE(rejected.ok());
    if (!rejected.ok())
      EXPECT_EQ(rejected.status().code(), ErrorCode::InvalidArgument);
    EXPECT_EQ(take(store->status(0)).revision, 0U);
  }
}

TEST_P(TypedInputDomainTest,
       DefinitionValidationIsIndependentOfStorageAndReportsConfigurationErrors) {
  const auto snapshot = take(catalog->snapshot());
  ASSERT_TRUE(EventProcessor::validate_definition(snapshot, engine_config, config).ok());
  EXPECT_EQ(take(store->status(0)).revision, 0U);

  auto invalid = config;
  invalid.cep_rules[0].input_kinds = {"news", "news"};
  EXPECT_EQ(EventProcessor::validate_definition(snapshot, engine_config, invalid).code(),
            ErrorCode::InvalidArgument);
  invalid = config;
  invalid.input_relations = {news + 1000};
  EXPECT_FALSE(EventProcessor::validate_definition(snapshot, engine_config, invalid).ok());
  EXPECT_EQ(take(store->status(0)).revision, 0U);

  open();
  input(0, "news", 0);
  const auto revision = take(store->status(0)).revision;
  ASSERT_TRUE(EventProcessor::validate_definition(snapshot, engine_config, config).ok());
  EXPECT_EQ(take(store->status(0)).revision, revision);
  EXPECT_EQ(state().event_sequence, 1U);
}

TEST_P(TypedInputDomainTest, AcceptsTheFullBoundedDomainWithoutBroadeningItsKinds) {
  auto& kinds = config.cep_rules[0].input_kinds;
  for (std::size_t index = 1; index < 64; ++index)
    kinds.push_back("input_" + std::to_string(index));
  open();
  input(0, "news", 0);
  input(1, "quote", 1);
  EXPECT_EQ(state().event_sequence, 1U);
  input(2, "news", 2);
  EXPECT_EQ(output().size(), 1U);
}

TEST_P(TypedInputDomainTest, OtherBusinessScopesDoNotConsumeOrTerminateTheNewsPath) {
  open();
  input(0, "news", 0);
  const auto unrelated = take(processor->process(
      {.source = {.source_id = "source", .topic = "feed", .partition = 0, .offset = 1},
       .scope = {std::string{"DEF"}},
       .kind = "quote",
       .values = {std::int64_t{100}},
       .changes = {{.relation = news,
                    .identity = {std::string{"DEF"}, std::int64_t{1}},
                    .row = Row{std::string{"DEF"}, std::int64_t{1}, std::int64_t{100}}}},
       .processing_time_us = 1}));
  ASSERT_TRUE(unrelated.operation_status.ok()) << unrelated.operation_status.message();
  ASSERT_TRUE(unrelated.input_committed);
  ASSERT_EQ(state().paths.size(), 1U);
  const auto other = take(processor->read_cep("positive_pair", {std::string{"DEF"}}));
  EXPECT_EQ(other.event_sequence, 0U);
  EXPECT_TRUE(other.paths.empty());
  input(2, "news", 2);
  const auto matches = output();
  ASSERT_EQ(matches.size(), 1U);
  EXPECT_EQ(matches[0].scope, Row{std::string{"ABC"}});
}

INSTANTIATE_TEST_SUITE_P(LifecycleAndRequestOwners, TypedInputDomainTest, ::testing::Bool());
} // namespace
} // namespace eventflow::test
