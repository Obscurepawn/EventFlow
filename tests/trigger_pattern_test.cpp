#include "eventflow/trigger_pattern.hpp"
#include "status_test_support.hpp"
#include <chrono>
#include <gtest/gtest.h>
#include <limits>

namespace eventflow::test {
namespace {
using namespace std::chrono_literals;

struct PatternFixture {
  CatalogSnapshot catalog;
  EventRef first;
  EventRef second;
  RelationId ready = 0;
};

PatternFixture fixture(bool reordered = false) noexcept {
  auto catalog = take(Catalog::create());
  auto input = take(catalog->define_sql(
      reordered ? "CREATE TABLE input(score FLOAT64 NOT NULL,k STRING,PRIMARY KEY(k)) "
                  "PARTITION BY(k)"
                : "CREATE TABLE input(k STRING,score FLOAT64 NOT NULL,PRIMARY KEY(k)) "
                  "PARTITION BY(k)"));
  auto ready = take(catalog->define_sql(
      "CREATE MATERIALIZED VIEW ready AS SELECT k,score > 0.0 AS ready FROM input"));
  return {.catalog = take(catalog->snapshot()),
          .first = EventRef::source({.kind = "first", .payload_relation = input}),
          .second = EventRef::source({.kind = "second", .payload_relation = input}),
          .ready = ready};
}

StatusOr<std::unique_ptr<CepProgram>>
program(const CatalogSnapshot& catalog, const Pattern& pattern, CepLimits limits = {}) noexcept {
  EF_ASSIGN_OR_RETURN(auto bound, bind_trigger_pattern(catalog, pattern));
  std::vector<std::string> reads;
  for (const auto& [alias, relation] : bound.reads) {
    (void)relation;
    reads.push_back(alias);
  }
  return CepProgram::create({.id = "news_sequence",
                             .version = 1,
                             .pattern = std::move(bound.pattern),
                             .within_us = bound.within_us,
                             .skip = bound.skip,
                             .reads = std::move(reads),
                             .register_fields = {},
                             .initial_registers = {},
                             .limits = limits});
}

CepEvent input(std::string id, std::string kind, std::int64_t time_us, double score = 1.0,
               bool reordered = false) noexcept {
  return {.id = std::move(id),
          .kind = std::move(kind),
          .time_us = time_us,
          .values =
              reordered ? Row{score, std::string("ticker")} : Row{std::string("ticker"), score}};
}

TEST(TriggerPattern, OverlappingStartsKeepIndependentHistories) {
  auto data = fixture();
  auto pattern = Pattern::begin("start", data.first)
                     .followed_by_any("confirm", data.second)
                     .where(data.second.column<double>("score") > 0.0)
                     .within(100us);
  auto compiled = take(program(data.catalog, pattern));
  auto state = take(compiled->initial_state());
  state = take(compiled->step(state, input("first_one", "first", 1))).state;
  state = take(compiled->step(state, input("first_two", "first", 2))).state;
  auto result = take(compiled->step(state, input("second_one", "second", 3)));
  ASSERT_EQ(result.matches.size(), 2U);
  EXPECT_EQ(result.matches[0].events.front().event.id, "first_one");
  EXPECT_EQ(result.matches[1].events.front().event.id, "first_two");
}

TEST(TriggerPattern, StrictAndNextOfKindObserveDifferentInputDomains) {
  auto data = fixture();
  auto start = Pattern::begin("start", data.first);
  for (bool strict : {false, true}) {
    auto sequence =
        strict ? start.next("confirm", data.second) : start.next_of_kind("confirm", data.second);
    auto compiled = take(program(data.catalog, sequence.within(100us)));
    auto state = take(compiled->initial_state());
    state = take(compiled->step(state, input("first", "first", 1))).state;
    state = take(compiled->step(state, input("unrelated", "quote", 2))).state;
    auto result = take(compiled->step(state, input("second", "second", 3)));
    EXPECT_EQ(result.matches.size(), strict ? 0U : 1U);
  }
}

TEST(TriggerPattern, NextOfKindRejectsFirstNegativeWhileFollowedByCanWait) {
  auto data = fixture();
  auto start = Pattern::begin("start", data.first);
  for (bool next_kind : {false, true}) {
    auto sequence = next_kind ? start.next_of_kind("confirm", data.second)
                              : start.followed_by("confirm", data.second);
    sequence = sequence.where(data.second.column<double>("score") > 0.0).within(100us);
    auto compiled = take(program(data.catalog, sequence));
    auto state = take(compiled->initial_state());
    state = take(compiled->step(state, input("first", "first", 1))).state;
    state = take(compiled->step(state, input("negative", "second", 2, -1.0))).state;
    auto result = take(compiled->step(state, input("positive", "second", 3)));
    EXPECT_EQ(result.matches.size(), next_kind ? 0U : 1U);
  }
}

TEST(TriggerPattern, ARejectedConfirmationCanStartAnotherOverlappingPath) {
  auto data = fixture();
  auto compiled = take(program(data.catalog, Pattern::begin("start", data.first)
                                                 .next_of_kind("confirm", data.first)
                                                 .where(data.first.column<double>("score") > 0.0)
                                                 .within(100us)));
  auto state = take(compiled->initial_state());
  state = take(compiled->step(state, input("original", "first", 1))).state;
  auto rejected = take(compiled->step(state, input("replacement", "first", 2, -1.0)));
  EXPECT_TRUE(rejected.matches.empty());
  auto accepted = take(compiled->step(rejected.state, input("positive", "first", 3)));
  ASSERT_EQ(accepted.matches.size(), 1U);
  EXPECT_EQ(accepted.matches[0].events[0].event.id, "replacement");
  // The confirming event also starts a new candidate under NoSkip.
  auto remaining = take(compiled->inspect(accepted.state));
  ASSERT_EQ(remaining.paths.size(), 1U);
  EXPECT_EQ(remaining.paths[0].history[0].event.id, "positive");
}

TEST(TriggerPattern, FollowedByAnyRetainsSuccessfulSkipBranches) {
  auto data = fixture();
  for (bool retain_alternative : {false, true}) {
    auto start = Pattern::begin("start", data.first);
    auto pattern = retain_alternative ? start.followed_by_any("confirm", data.second)
                                      : start.followed_by("confirm", data.second);
    auto compiled = take(program(data.catalog, pattern.within(100us)));
    auto state = take(compiled->initial_state());
    state = take(compiled->step(state, input("start", "first", 1))).state;
    auto first = take(compiled->step(state, input("confirm_one", "second", 2)));
    auto second = take(compiled->step(first.state, input("confirm_two", "second", 3)));
    EXPECT_EQ(first.matches.size(), 1U);
    EXPECT_EQ(second.matches.size(), retain_alternative ? 1U : 0U);
  }
}

TEST(TriggerPattern, SkipStrategiesKeepExistingAdmissionOrderSemantics) {
  auto data = fixture();
  for (auto skip :
       {CepSkipStrategy::NoSkip, CepSkipStrategy::SkipToNext, CepSkipStrategy::SkipPastLastEvent}) {
    auto compiled = take(program(data.catalog, Pattern::begin("start", data.first)
                                                   .followed_by_any("confirm", data.second)
                                                   .within(100us)
                                                   .after_match(skip)));
    auto state = take(compiled->initial_state());
    state = take(compiled->step(state, input("first_one", "first", 1))).state;
    state = take(compiled->step(state, input("first_two", "first", 1))).state;
    auto first = take(compiled->step(state, input("second_one", "second", 1)));
    auto second = take(compiled->step(first.state, input("second_two", "second", 1)));
    const auto expected = skip == CepSkipStrategy::NoSkip       ? 4U
                          : skip == CepSkipStrategy::SkipToNext ? 2U
                                                                : 1U;
    EXPECT_EQ(first.matches.size() + second.matches.size(), expected);
  }
}

TEST(TriggerPattern, ExclusiveDeadlineExpiresBeforeMatchingAndRestores) {
  auto data = fixture();
  auto compiled = take(program(
      data.catalog,
      Pattern::begin("start", data.first).next_of_kind("confirm", data.second).within(10us)));
  auto state = take(compiled->initial_state());
  state = take(compiled->step(state, input("first", "first", 1))).state;
  state = take(compiled->decode_state(take(compiled->encode_state(state))));
  EXPECT_EQ(take(compiled->step(state, input("early", "second", 10))).matches.size(), 1U);
  auto expired = take(compiled->step(state, input("late", "second", 11)));
  EXPECT_TRUE(expired.matches.empty());
  ASSERT_EQ(expired.timeouts.size(), 1U);
  EXPECT_EQ(expired.timeouts[0].scheduled_time_us, 11);
  auto idle_expiry = take(compiled->advance(state, 11));
  EXPECT_EQ(idle_expiry.timeouts.size(), 1U);
}

TEST(TriggerPattern, FieldNamesSurviveSchemaReorderingAndValidateActualPayload) {
  auto data = fixture(true);
  auto compiled = take(program(data.catalog, Pattern::begin("positive", data.first)
                                                 .where(data.first.column<double>("score") > 0.0)
                                                 .within(10us)));
  auto state = take(compiled->initial_state());
  EXPECT_EQ(take(compiled->step(state, input("good", "first", 1, 2.0, true))).matches.size(), 1U);
  auto invalid = compiled->step(state, input("wrong_layout", "first", 1));
  EXPECT_FALSE(invalid.ok());
  EXPECT_EQ(take(compiled->encode_state(state)),
            take(compiled->encode_state(take(compiled->initial_state()))));
  // A missing explicit condition still validates the declared input schema.
  compiled = take(program(data.catalog, Pattern::begin("any", data.first).within(10us)));
  EXPECT_FALSE(compiled
                   ->step(take(compiled->initial_state()),
                          {.id = "short", .kind = "first", .time_us = 1, .values = {}})
                   .ok());
}

TEST(TriggerPattern, ConditionsReadCurrentViewsWithoutRetainingAnOldStartSnapshot) {
  auto data = fixture();
  auto pattern = Pattern::begin("start", data.first)
                     .next_of_kind("confirm", data.second)
                     .where(ViewRef(data.ready).column<bool>("ready").is_true())
                     .within(100us);
  auto bound = take(bind_trigger_pattern(data.catalog, pattern));
  ASSERT_EQ(bound.reads.size(), 1U);
  const auto alias = bound.reads.begin()->first;
  auto compiled = take(program(data.catalog, pattern));
  auto state = take(compiled->initial_state());
  state = take(compiled->step(state, input("start", "first", 1),
                              {{alias, Row{std::string("ticker"), false}}}))
              .state;
  auto accepted = take(compiled->step(state, input("ready", "second", 2),
                                      {{alias, Row{std::string("ticker"), true}}}));
  ASSERT_EQ(accepted.matches.size(), 1U);
  EXPECT_EQ(accepted.matches[0].reads.find(alias)->second,
            (std::optional<Row>{Row{std::string("ticker"), true}}));
  EXPECT_TRUE(take(compiled->step(state, input("missing", "second", 2), {{alias, std::nullopt}}))
                  .matches.empty());
}

TEST(TriggerPattern, OptionalGroupsAlternativesAndBoundedRepeatExecuteRealMatches) {
  auto data = fixture();
  auto alternative =
      Pattern::begin("optional_first", data.first)
          .optional()
          .then(Pattern::begin("required_second", data.second), CepContiguity::Strict);
  auto repeated = Pattern::begin("repeated_first", data.first).repeat(2, 3, CepContiguity::Strict);
  auto compiled = take(program(data.catalog, alternative.or_else(repeated).within(100us)));
  auto state = take(compiled->initial_state());
  EXPECT_EQ(take(compiled->step(state, input("second", "second", 1))).matches.size(), 1U);
  state = take(compiled->step(state, input("first_one", "first", 1))).state;
  auto pair = take(compiled->step(state, input("first_two", "first", 2)));
  ASSERT_EQ(pair.matches.size(), 1U);
  EXPECT_EQ(pair.matches[0].events.size(), 2U);
  auto triple = take(compiled->step(pair.state, input("first_three", "first", 3)));
  ASSERT_EQ(triple.matches.size(), 2U);
}

TEST(TriggerPattern, FluentBranchesAreImmutableAndRepeatedWhereCombinesPredicates) {
  auto data = fixture();
  auto start = Pattern::begin("start", data.first).where(data.first.column<double>("score") > 0.0);
  auto bounded = start.where(data.first.column<double>("score") < 10.0).within(100us);
  auto original = take(program(data.catalog, start.within(100us)));
  auto filtered = take(program(data.catalog, bounded));
  auto event = input("large", "first", 1, 20.0);
  EXPECT_EQ(take(original->step(take(original->initial_state()), event)).matches.size(), 1U);
  EXPECT_TRUE(take(filtered->step(take(filtered->initial_state()), event)).matches.empty());
}

TEST(TriggerPattern, InvalidSchemasWindowsLabelsAndRepetitionsReturnStatus) {
  auto data = fixture();
  auto start = Pattern::begin("start", data.first);
  for (const auto& invalid :
       {Pattern{}, start, start.within(0us), start.within(-1us), start.within(1ns),
        start.within(std::chrono::hours(std::numeric_limits<std::int64_t>::max())),
        start.next("start", data.second).within(1s), start.optional().within(1s),
        start.repeat(3, 2).within(1s), start.repeat(1, 65).within(1s),
        start.optional().repeat(1, 2).within(1s),
        start.where(data.first.column<bool>("score").is_true()).within(1s),
        start.where(data.first.column<double>("missing") > 0.0).within(1s),
        start.or_else(Pattern::begin("alternative", data.second))
            .where(data.first.column<double>("score") > 0.0)
            .within(1s),
        start.then(Pattern::begin("nested", data.second).within(1s)).within(2s)}) {
    EXPECT_FALSE(program(data.catalog, invalid).ok());
  }
}

TEST(TriggerPattern, MatchLengthLimitsAreAppliedByTheExistingMatcher) {
  auto data = fixture();
  auto pattern = Pattern::begin("start", data.first).next("confirm", data.second).within(1s);
  EXPECT_FALSE(program(data.catalog, pattern, {.max_events_per_match = 1}).ok());
  EXPECT_TRUE(program(data.catalog, pattern, {.max_events_per_match = 2}).ok());
}

TEST(TriggerPattern, ConflictingPayloadSchemasAndOpaqueFsmGuardsAreRejected) {
  auto data = fixture();
  auto conflict = EventRef::source({.kind = "first", .payload_relation = data.ready});
  EXPECT_FALSE(bind_trigger_pattern(
                   data.catalog,
                   Pattern::begin("first", data.first).followed_by("second", conflict).within(1s))
                   .ok());
  auto opaque = Predicate::guard(
      {}, [](const FsmContext&) noexcept -> StatusOr<bool> { return true; },
      "opaque_business_predicate.v1");
  EXPECT_FALSE(bind_trigger_pattern(data.catalog,
                                    Pattern::begin("first", data.first).where(opaque).within(1s))
                   .ok());
}

TEST(TriggerPattern, SignatureIncludesPredicateMeaningAndTimeBound) {
  auto data = fixture();
  auto start = Pattern::begin("start", data.first);
  auto first = take(bind_trigger_pattern(
      data.catalog, start.where(data.first.column<double>("score") > 1.0).within(1s)));
  auto second = take(bind_trigger_pattern(
      data.catalog, start.where(data.first.column<double>("score") > 2.0).within(1s)));
  auto longer = take(bind_trigger_pattern(
      data.catalog, start.where(data.first.column<double>("score") > 1.0).within(2s)));
  EXPECT_NE(first.signature, second.signature);
  EXPECT_NE(first.signature, longer.signature);
}
} // namespace
} // namespace eventflow::test
