#include "eventflow/trigger_expression.hpp"
#include <gtest/gtest.h>
#include <limits>

namespace eventflow {
namespace {
using namespace std::chrono_literals;

class TriggerExpressionTest : public testing::Test {
protected:
  void SetUp() override {
    auto opened = Catalog::create();
    ASSERT_TRUE(opened.ok());
    catalog = std::move(*opened);
    auto source = catalog->define_sql(
        "CREATE TABLE news (ticker STRING, score FLOAT64, ready BOOL, quantity INT64, "
        "PRIMARY KEY(ticker))",
        {.partition_key = {"ticker"}});
    ASSERT_TRUE(source.ok()) << source.status().message();
    news = *source;
    auto projected =
        catalog->define_sql("CREATE MATERIALIZED VIEW readiness AS SELECT ready FROM news");
    ASSERT_TRUE(projected.ok()) << projected.status().message();
    readiness = *projected;
    auto state = catalog->snapshot();
    ASSERT_TRUE(state.ok());
    snapshot = std::move(*state);
  }

  EventRef news_event() const noexcept {
    return EventRef::source({.kind = "news", .payload_relation = news});
  }

  StatusOr<bool> run(const BoundTriggerPredicate& bound, Row values, FsmReads reads = {},
                     std::string kind = "news") const noexcept {
    FsmState state;
    const FsmEvent event{.kind = std::move(kind), .occurrence_id = {}, .values = std::move(values)};
    return bound.fsm({.state = state, .event = event, .reads = reads});
  }

  std::unique_ptr<Catalog> catalog;
  CatalogSnapshot snapshot;
  RelationId news = 0;
  RelationId readiness = 0;
};

TEST_F(TriggerExpressionTest, ResolvesColumnsByNameAndChecksWholeEventEvenForConstantFalse) {
  auto bound =
      bind_trigger_predicate(snapshot, news_event().column<double>("score") > 0.0, news_event());
  ASSERT_TRUE(bound.ok()) << bound.status().message();
  auto yes = run(*bound, {std::string("ABC"), 0.8, true, std::int64_t(7)});
  ASSERT_TRUE(yes.ok());
  EXPECT_TRUE(*yes);
  auto constant = bind_trigger_predicate(snapshot, Predicate::constant(false), news_event());
  ASSERT_TRUE(constant.ok());
  EXPECT_FALSE(run(*constant, {std::string("ABC"), 0.8, true}).ok());
  EXPECT_FALSE(run(*constant, {std::string("ABC"), 0.8, true, std::string("bad")}).ok());
}

TEST_F(TriggerExpressionTest, ColumnReorderingBindsTheNewLayout) {
  auto opened = Catalog::create();
  ASSERT_TRUE(opened.ok());
  auto relation = (*opened)->define_sql("CREATE TABLE news (quantity INT64, ready BOOL, ticker "
                                        "STRING, score FLOAT64, PRIMARY KEY(ticker))",
                                        {.partition_key = {"ticker"}});
  ASSERT_TRUE(relation.ok());
  auto state = (*opened)->snapshot();
  ASSERT_TRUE(state.ok());
  auto event = EventRef::source({.kind = "news", .payload_relation = *relation});
  auto bound = bind_trigger_predicate(*state, event.column<double>("score") > 0.0, event);
  ASSERT_TRUE(bound.ok());
  auto result = run(*bound, {std::int64_t(7), true, std::string("ABC"), 0.8});
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(*result);
}

TEST_F(TriggerExpressionTest, MissingNullAndWrongReadTypesRemainDistinct) {
  const auto ready = ViewRef(readiness).column<bool>("ready");
  auto bound = bind_trigger_predicate(snapshot, ready.is_true());
  ASSERT_TRUE(bound.ok());
  EXPECT_EQ(bound->reads.at("readiness"), readiness);
  auto missing = run(*bound, {}, {{"readiness", std::nullopt}});
  ASSERT_TRUE(missing.ok());
  EXPECT_FALSE(*missing);
  auto null = run(*bound, {}, {{"readiness", Row{std::monostate{}}}});
  ASSERT_TRUE(null.ok());
  EXPECT_FALSE(*null);
  EXPECT_FALSE(run(*bound, {}, {{"readiness", Row{std::int64_t(1)}}}).ok());
  EXPECT_FALSE(run(*bound, {}).ok());
  auto fallback = bind_trigger_predicate(snapshot, ready.value_or(true).is_true());
  ASSERT_TRUE(fallback.ok());
  auto accepted = run(*fallback, {}, {{"readiness", std::nullopt}});
  ASSERT_TRUE(accepted.ok());
  EXPECT_TRUE(*accepted);
}

TEST_F(TriggerExpressionTest, NullableComparisonsAndExplicitNullChecks) {
  auto negative =
      bind_trigger_predicate(snapshot, news_event().column<double>("score") != 0.0, news_event());
  ASSERT_TRUE(negative.ok());
  auto missing = run(*negative, {std::string("ABC"), std::monostate{}, true, std::int64_t(0)});
  ASSERT_TRUE(missing.ok());
  EXPECT_FALSE(*missing);
  auto null = bind_trigger_predicate(snapshot, news_event().column<double>("score").is_null(),
                                     news_event());
  ASSERT_TRUE(null.ok());
  auto accepted = run(*null, {std::string("ABC"), std::monostate{}, true, std::int64_t(0)});
  ASSERT_TRUE(accepted.ok());
  EXPECT_TRUE(*accepted);
}

TEST_F(TriggerExpressionTest, ProjectedViewsAcceptVisibleRowsAndValidateStoredHiddenFields) {
  auto bound = bind_trigger_predicate(snapshot, ViewRef(readiness).column<bool>("ready").is_true());
  ASSERT_TRUE(bound.ok());
  auto visible = run(*bound, {}, {{"readiness", Row{true}}});
  ASSERT_TRUE(visible.ok());
  EXPECT_TRUE(*visible);
  auto stored = run(*bound, {}, {{"readiness", Row{true, std::string("ABC")}}});
  ASSERT_TRUE(stored.ok());
  EXPECT_TRUE(*stored);
  EXPECT_FALSE(run(*bound, {}, {{"readiness", Row{true, std::int64_t(7)}}}).ok());
}

TEST_F(TriggerExpressionTest, ComparesColumnsAndPreservesNegationAndStringSemantics) {
  auto event = news_event();
  auto bound = bind_trigger_predicate(snapshot,
                                      event.column<double>("score") >=
                                              ViewRef(news).column<double>("score") &&
                                          !(event.column<std::string>("ticker") == "OTHER"),
                                      event);
  ASSERT_TRUE(bound.ok());
  auto result = run(*bound, {std::string("ABC"), 0.8, true, std::int64_t(7)},
                    {{"news", Row{std::string("ABC"), 0.7, true, std::int64_t(9)}}});
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(*result);
}

TEST_F(TriggerExpressionTest, CurrentEventReferencesCannotReadAnotherKindOrConflictingPayload) {
  auto wrong_kind = EventRef::source({.kind = "other", .payload_relation = news});
  EXPECT_FALSE(
      bind_trigger_predicate(snapshot, wrong_kind.column<double>("score") > 0.0, news_event())
          .ok());
  auto wrong_layout = EventRef::source({.kind = "news", .payload_relation = readiness});
  EXPECT_FALSE(
      bind_trigger_predicate(snapshot, wrong_layout.column<bool>("ready").is_true(), news_event())
          .ok());
  auto untyped = EventRef::source({.kind = "news"});
  EXPECT_FALSE(bind_trigger_predicate(snapshot, untyped.column<bool>("ready").is_true()).ok());
}

TEST_F(TriggerExpressionTest, ShortCircuitSkipsOpaqueFailuresButNeverPayloadValidation) {
  int calls = 0;
  auto failure = Predicate::guard(
      {},
      [&calls](const FsmContext&) noexcept -> StatusOr<bool> {
        ++calls;
        return Status(ErrorCode::Unavailable, "Expected guard failure");
      },
      "test.failure.v1");
  auto conjunction =
      bind_trigger_predicate(snapshot, Predicate::constant(false) && failure, news_event());
  auto disjunction =
      bind_trigger_predicate(snapshot, Predicate::constant(true) || failure, news_event());
  ASSERT_TRUE(conjunction.ok());
  ASSERT_TRUE(disjunction.ok());
  EXPECT_FALSE(conjunction->cep);
  auto no = run(*conjunction, {std::string("ABC"), 1.0, true, std::int64_t(1)});
  auto yes = run(*disjunction, {std::string("ABC"), 1.0, true, std::int64_t(1)});
  ASSERT_TRUE(no.ok());
  ASSERT_TRUE(yes.ok());
  EXPECT_FALSE(*no);
  EXPECT_TRUE(*yes);
  EXPECT_EQ(calls, 0);
  EXPECT_FALSE(run(*conjunction, {}).ok());
  auto reached = bind_trigger_predicate(snapshot, Predicate::constant(true) && failure);
  ASSERT_TRUE(reached.ok());
  EXPECT_EQ(run(*reached, {}).status().code(), ErrorCode::Unavailable);
  EXPECT_EQ(calls, 1);
}

TEST_F(TriggerExpressionTest, RejectsMissingWrongAndHiddenColumnDeclarations) {
  EXPECT_FALSE(
      bind_trigger_predicate(snapshot, news_event().column<bool>("missing").is_true()).ok());
  EXPECT_FALSE(bind_trigger_predicate(snapshot, news_event().column<bool>("score").is_true()).ok());
  auto relation = snapshot.find(readiness);
  ASSERT_TRUE(relation.ok());
  ASSERT_GT((*relation)->storage_schema.fields.size(), (*relation)->visible_count);
  const auto hidden = (*relation)->storage_schema.fields[(*relation)->visible_count].name;
  EXPECT_FALSE(
      bind_trigger_predicate(snapshot, ViewRef(readiness).column<std::string>(hidden) == "ABC")
          .ok());
}

TEST_F(TriggerExpressionTest, ReadAliasesDeduplicateAndConflictsFail) {
  auto repeated = bind_trigger_predicate(
      snapshot, ViewRef(news, "current").column<double>("score") > 0.0 &&
                    ViewRef(news, "current").column<std::int64_t>("quantity") >= 1);
  ASSERT_TRUE(repeated.ok());
  EXPECT_EQ(repeated->reads.size(), 1U);
  EXPECT_FALSE(bind_trigger_predicate(
                   snapshot, ViewRef(news, "shared").column<bool>("ready").is_true() &&
                                 ViewRef(readiness, "shared").column<bool>("ready").is_true())
                   .ok());
}

TEST_F(TriggerExpressionTest, RejectsNonUniqueViewsAndIncompatibleScopes) {
  auto many = catalog->define_sql(
      "CREATE TABLE articles (ticker STRING, id INT64, PRIMARY KEY(ticker, id))",
      {.partition_key = {"ticker"}});
  ASSERT_TRUE(many.ok());
  auto other = catalog->define_sql("CREATE TABLE other (id INT64, ready BOOL, PRIMARY KEY(id))",
                                   {.partition_key = {"id"}});
  ASSERT_TRUE(other.ok());
  auto state = catalog->snapshot();
  ASSERT_TRUE(state.ok());
  EXPECT_FALSE(bind_trigger_predicate(*state, ViewRef(*many).column<std::int64_t>("id") > 0).ok());
  EXPECT_FALSE(
      bind_trigger_predicate(*state, ViewRef(*other).column<bool>("ready").is_true(), news_event())
          .ok());
}

TEST_F(TriggerExpressionTest, OpaqueGuardRequiresStableIdentityAndValidatedReadDeclaration) {
  auto pass = [](const FsmContext&) noexcept -> StatusOr<bool> { return true; };
  EXPECT_FALSE(bind_trigger_predicate(snapshot, Predicate::guard({}, pass, "")).ok());
  EXPECT_FALSE(bind_trigger_predicate(snapshot, Predicate::guard({}, {}, "guard.v1")).ok());
  auto bound = bind_trigger_predicate(
      snapshot,
      Predicate::guard({{.alias = "readiness", .relation = readiness}}, pass, "guard.v1"));
  ASSERT_TRUE(bound.ok());
  EXPECT_EQ(bound->reads.size(), 1U);
  EXPECT_FALSE(run(*bound, {}, {{"readiness", Row{std::string("bad")}}}).ok());
}

TEST_F(TriggerExpressionTest, DeclarativePredicateWorksInCepWithoutSyntheticFsmState) {
  auto event = news_event();
  auto bound = bind_trigger_predicate(
      snapshot, event.column<bool>("ready").is_true() && event.column<std::int64_t>("quantity") > 1,
      event);
  ASSERT_TRUE(bound.ok());
  ASSERT_TRUE(bound->cep);
  const CepEvent incoming{.id = "article-1",
                          .kind = "news",
                          .time_us = 1,
                          .values = {std::string("ABC"), 0.8, true, std::int64_t(7)}};
  const Row registers;
  const std::vector<CepCapture> history;
  const FsmReads reads;
  auto accepted =
      bound->cep({.event = incoming, .registers = registers, .history = history, .reads = reads});
  ASSERT_TRUE(accepted.ok());
  EXPECT_TRUE(*accepted);
}

TEST_F(TriggerExpressionTest, SignatureIncludesConstantsSchemaAndOpaqueImplementationIdentity) {
  auto first =
      bind_trigger_predicate(snapshot, news_event().column<double>("score") > 0.0, news_event());
  auto equal =
      bind_trigger_predicate(snapshot, news_event().column<double>("score") > 0.0, news_event());
  auto changed =
      bind_trigger_predicate(snapshot, news_event().column<double>("score") > 1.0, news_event());
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(equal.ok());
  ASSERT_TRUE(changed.ok());
  EXPECT_EQ(first->signature, equal->signature);
  EXPECT_NE(first->signature, changed->signature);
  auto pass = [](const FsmContext&) noexcept -> StatusOr<bool> { return true; };
  auto opaque1 = bind_trigger_predicate(snapshot, Predicate::guard({}, pass, "guard.v1"));
  auto opaque2 = bind_trigger_predicate(snapshot, Predicate::guard({}, pass, "guard.v2"));
  ASSERT_TRUE(opaque1.ok());
  ASSERT_TRUE(opaque2.ok());
  EXPECT_NE(opaque1->signature, opaque2->signature);
}

TEST_F(TriggerExpressionTest, RejectsNonFiniteConstantsAndNonFinitePayloads) {
  EXPECT_FALSE(bind_trigger_predicate(snapshot, news_event().column<double>("score") >
                                                    std::numeric_limits<double>::infinity())
                   .ok());
  auto bound = bind_trigger_predicate(snapshot, Predicate{}, news_event());
  ASSERT_TRUE(bound.ok());
  EXPECT_FALSE(run(*bound, {std::string("ABC"), std::numeric_limits<double>::quiet_NaN(), true,
                            std::int64_t(1)})
                   .ok());
}

TEST_F(TriggerExpressionTest, TypedEventGuardProjectsByNameAndRunsOnFsmAndCep) {
  std::size_t calls = 0;
  const std::vector<Field> expected{
      {.id = 1, .name = "quantity", .type = DataType::Int64, .nullable = true},
      {.id = 2, .name = "score", .type = DataType::Float64, .nullable = true}};
  auto predicate = Predicate::event_guard(
      news_event(), expected,
      [&calls](const Row& row) noexcept -> StatusOr<bool> {
        ++calls;
        const auto* quantity = std::get_if<std::int64_t>(&row[0]);
        const auto* score = std::get_if<double>(&row[1]);
        return quantity && score && *quantity >= 7 && *score > 0.0;
      },
      "news.positive_quantity.v1");
  auto bound = bind_trigger_predicate(snapshot, predicate, news_event());
  ASSERT_TRUE(bound.ok()) << bound.status().message();
  auto result = run(*bound, {std::string("ABC"), 0.8, true, std::int64_t(7)});
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(*result);
  const CepEvent incoming{.id = "news-1",
                          .kind = "news",
                          .time_us = 1,
                          .values = {std::string("ABC"), 0.8, true, std::int64_t(7)}};
  const Row registers;
  const std::vector<CepCapture> history;
  const FsmReads reads;
  ASSERT_TRUE(bound->cep);
  auto cep =
      bound->cep({.event = incoming, .registers = registers, .history = history, .reads = reads});
  ASSERT_TRUE(cep.ok());
  EXPECT_TRUE(*cep);
  EXPECT_EQ(calls, 2U);
  EXPECT_FALSE(run(*bound, {std::string("ABC"), 0.8, std::string("wrong"), std::int64_t(7)}).ok());
  EXPECT_EQ(calls, 2U);
  auto short_circuit =
      bind_trigger_predicate(snapshot, Predicate::constant(false) && predicate, news_event());
  ASSERT_TRUE(short_circuit.ok());
  EXPECT_FALSE(run(*short_circuit, {std::string("ABC"), 0.8}).ok());
  EXPECT_EQ(calls, 2U);
}

TEST_F(TriggerExpressionTest, TypedEventGuardBindsExplicitSchemaAndImplementationIdentity) {
  const std::vector<Field> expected{
      {.id = 1, .name = "score", .type = DataType::Float64, .nullable = true}};
  auto pass = [](const Row&) noexcept -> StatusOr<bool> { return true; };
  auto first = bind_trigger_predicate(
      snapshot, Predicate::event_guard(news_event(), expected, pass, "guard.v1"));
  auto changed = bind_trigger_predicate(
      snapshot, Predicate::event_guard(news_event(), expected, pass, "guard.v2"));
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(changed.ok());
  EXPECT_NE(first->signature, changed->signature);
  EXPECT_FALSE(
      bind_trigger_predicate(snapshot, Predicate::event_guard(news_event(), expected, pass, ""))
          .ok());
  EXPECT_FALSE(bind_trigger_predicate(
                   snapshot, Predicate::event_guard(news_event(), expected, {}, "guard.v1"))
                   .ok());
  auto wrong = expected;
  wrong[0].type = DataType::Bool;
  EXPECT_FALSE(bind_trigger_predicate(snapshot,
                                      Predicate::event_guard(news_event(), wrong, pass, "guard.v1"))
                   .ok());
  wrong = expected;
  wrong[0].nullable = false;
  EXPECT_FALSE(bind_trigger_predicate(snapshot,
                                      Predicate::event_guard(news_event(), wrong, pass, "guard.v1"))
                   .ok());
}

TEST(TriggerDurationTest, ConvertsExactlyAndRejectsOverflowOrFractionalMicroseconds) {
  EXPECT_EQ(*TriggerDuration(2s).microseconds(), 2'000'000);
  EXPECT_EQ(*TriggerDuration(-3ms).microseconds(), -3'000);
  EXPECT_EQ(*TriggerDuration(2'000ns).microseconds(), 2);
  EXPECT_EQ(*TriggerDuration().microseconds(), 0);
  EXPECT_FALSE(TriggerDuration(1ns).microseconds().ok());
  EXPECT_FALSE(TriggerDuration(std::chrono::seconds(std::numeric_limits<std::int64_t>::max()))
                   .microseconds()
                   .ok());
  EXPECT_EQ(*TriggerDuration(std::chrono::microseconds(std::numeric_limits<std::int64_t>::min()))
                 .microseconds(),
            std::numeric_limits<std::int64_t>::min());
  using HugePeriod =
      std::chrono::duration<std::uint64_t, std::ratio<std::numeric_limits<std::int64_t>::max()>>;
  EXPECT_FALSE(
      TriggerDuration(HugePeriod(std::numeric_limits<std::uint64_t>::max())).microseconds().ok());
  using Fraction = std::chrono::duration<std::int64_t, std::ratio<1, 3>>;
  EXPECT_FALSE(TriggerDuration(Fraction(1)).microseconds().ok());
  EXPECT_EQ(*TriggerDuration(Fraction(3)).microseconds(), 1'000'000);
}
} // namespace
} // namespace eventflow
