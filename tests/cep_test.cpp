#include "eventflow/cep.hpp"
#include "status_test_support.hpp"
#include <algorithm>
#include <gtest/gtest.h>
#include <limits>

namespace eventflow::test {
namespace {
TEST(CepProgram, OverlappingStartsKeepBothHistories) {
  CepDefinition definition;
  definition.id = "ab";
  definition.within_us = 100;
  definition.pattern =
      CepPattern::event("a", "A").then(CepPattern::event("b", "B"), CepContiguity::FollowedByAny);
  auto program = take(CepProgram::create(std::move(definition)));
  auto state = take(program->initial_state());
  state = take(program->step(state, {"a1", "A", 1, {}})).state;
  state = take(program->step(state, {"a2", "A", 2, {}})).state;
  auto output = take(program->step(state, {"b", "B", 3, {}}));
  ASSERT_EQ(output.matches.size(), 2U);
  EXPECT_EQ(output.matches[0].events[0].event.id, "a1");
  EXPECT_EQ(output.matches[1].events[0].event.id, "a2");
}

CepDefinition pair_definition(CepSkipStrategy skip = CepSkipStrategy::NoSkip) noexcept {
  CepDefinition definition;
  definition.id = "pairs";
  definition.within_us = 100;
  definition.skip = skip;
  definition.pattern =
      CepPattern::event("a", "A").then(CepPattern::event("b", "B"), CepContiguity::FollowedByAny);
  return definition;
}

CepEvent input(std::string id, std::string kind, std::int64_t time,
               std::int64_t value = 0) noexcept {
  return {std::move(id), std::move(kind), time, Row{value}};
}

std::vector<std::string> normalized(const std::vector<CepMatch>& matches) noexcept {
  std::vector<std::string> result;
  for (const auto& match : matches) {
    std::string text;
    for (const auto& capture : match.events)
      text += capture.label + ":" + capture.event.id + ";";
    result.push_back(std::move(text));
  }
  std::sort(result.begin(), result.end());
  return result;
}

TEST(CepProgram, OptionalEpsilonNeverConsumesTheSameInputTwice) {
  auto definition = pair_definition();
  definition.pattern =
      CepPattern::event("a", "A")
          .then(CepPattern::event("optional_c", "C").optional(), CepContiguity::Strict)
          .then(CepPattern::event("final_c", "C"), CepContiguity::Strict);
  auto program = take(CepProgram::create(std::move(definition)));
  auto state = take(program->initial_state());
  state = take(program->step(state, input("a", "A", 1))).state;
  auto first = take(program->step(state, input("c1", "C", 2)));
  ASSERT_EQ(first.matches.size(), 1U);
  EXPECT_EQ(normalized(first.matches), (std::vector<std::string>{"a:a;final_c:c1;"}));
  auto next = take(program->step(first.state, input("c2", "C", 3)));
  ASSERT_EQ(next.matches.size(), 1U);
  EXPECT_EQ(normalized(next.matches), (std::vector<std::string>{"a:a;optional_c:c1;final_c:c2;"}));
  state = take(program->step(state, input("x", "X", 2))).state;
  EXPECT_TRUE(take(program->step(state, input("c", "C", 3))).matches.empty());
}

TEST(CepProgram, OuterStrictAppliesToTheFirstTakeOfANullableGroup) {
  auto definition = pair_definition();
  auto group = CepPattern::event("b", "B").optional().then(CepPattern::event("c", "C"));
  definition.pattern = CepPattern::event("a", "A").then(group, CepContiguity::Strict);
  auto program = take(CepProgram::create(definition));
  auto a = take(program->step(take(program->initial_state()), input("a", "A", 1))).state;
  auto skipped = take(program->step(a, input("x", "X", 2))).state;
  EXPECT_TRUE(take(program->step(skipped, input("c", "C", 3))).matches.empty());
  EXPECT_EQ(take(program->step(a, input("c", "C", 2))).matches.size(), 1U);
  auto began = take(program->step(a, input("b", "B", 2))).state;
  began = take(program->step(began, input("x", "X", 3))).state;
  EXPECT_EQ(take(program->step(began, input("c", "C", 4))).matches.size(), 1U);
}

TEST(CepProgram, OptionalStartCanSkipWithoutDuplicatingOneEvent) {
  auto definition = pair_definition();
  definition.pattern = CepPattern::event("optional", "A")
                           .optional()
                           .then(CepPattern::event("required", "A"), CepContiguity::Strict);
  auto program = take(CepProgram::create(std::move(definition)));
  auto first = take(program->step(take(program->initial_state()), input("a1", "A", 1)));
  EXPECT_EQ(normalized(first.matches), (std::vector<std::string>{"required:a1;"}));
  auto second = take(program->step(first.state, input("a2", "A", 2)));
  EXPECT_EQ(normalized(second.matches),
            (std::vector<std::string>{"optional:a1;required:a2;", "required:a2;"}));
}

TEST(CepProgram, HistoryAndCurrentReadsAreDistinctAndRetryPreservesState) {
  auto definition = pair_definition();
  definition.reads = {"total"};
  definition.register_fields = {{1, "saved", DataType::Int64, false}};
  definition.initial_registers = Row{std::int64_t{0}};
  auto remember = [](const CepContext& context) noexcept -> StatusOr<Row> {
    return Row{context.event.values[0]};
  };
  auto guard = [](const CepContext& context) noexcept -> StatusOr<bool> {
    EXPECT_EQ(context.history.size(), 1U);
    EXPECT_EQ(context.history[0].label, "a");
    EXPECT_EQ(context.history[0].event.values[0], context.registers[0]);
    EXPECT_EQ(std::get<std::int64_t>((*context.reads.at("total"))[0]), 1000);
    return std::get<std::int64_t>(context.event.values[0]) >
           std::get<std::int64_t>(context.history[0].event.values[0]);
  };
  definition.pattern = CepPattern::event("a", "A", {}, remember)
                           .then(CepPattern::event("b", "B", guard), CepContiguity::FollowedByAny);
  auto program = take(CepProgram::create(std::move(definition)));
  FsmReads reads{{"total", Row{std::int64_t{1000}}}};
  auto state = take(program->initial_state());
  state = take(program->step(state, input("a1", "A", 1, 10), reads)).state;
  state = take(program->step(state, input("a2", "A", 2, 20), reads)).state;
  const auto original = take(program->encode_state(state));
  auto copy = state;
  auto first = take(program->step(state, input("b", "B", 3, 15), reads));
  auto retry = take(program->step(copy, input("b", "B", 3, 15), reads));
  ASSERT_EQ(first.matches.size(), 1U);
  EXPECT_EQ(first.matches[0].events[0].event.id, "a1");
  EXPECT_EQ(first.matches[0].registers, Row{std::int64_t{10}});
  EXPECT_EQ(first.matches[0].id, retry.matches[0].id);
  EXPECT_EQ(take(program->encode_state(first.state)), take(program->encode_state(retry.state)));
  EXPECT_EQ(take(program->encode_state(state)), original);
}

TEST(CepProgram, AlternativeBranchesKeepRegistersAndLabeledHistorySeparate) {
  auto definition = pair_definition();
  definition.register_fields = {{1, "value", DataType::Int64, false}};
  definition.initial_registers = Row{std::int64_t{0}};
  auto left = CepPattern::event("left", "X", {}, [](const CepContext&) noexcept -> StatusOr<Row> {
    return Row{std::int64_t{11}};
  });
  auto right = CepPattern::event("right", "X", {}, [](const CepContext&) noexcept -> StatusOr<Row> {
    return Row{std::int64_t{21}};
  });
  definition.pattern = CepPattern::event("a", "A")
                           .then(left.or_else(right))
                           .then(CepPattern::event("d", "D"), CepContiguity::Strict);
  auto program = take(CepProgram::create(std::move(definition)));
  auto state = take(program->initial_state());
  state = take(program->step(state, input("a", "A", 1))).state;
  auto shared = take(program->inspect(state));
  EXPECT_EQ(shared.paths.size(), 2U);
  EXPECT_EQ(shared.history_nodes, 1U);
  EXPECT_EQ(shared.event_records, 1U);
  state = take(program->step(state, input("x", "X", 2))).state;
  EXPECT_EQ(take(program->inspect(state)).event_records, 2U);
  auto output = take(program->step(state, input("d", "D", 3)));
  ASSERT_EQ(output.matches.size(), 2U);
  EXPECT_NE(output.matches[0].id, output.matches[1].id);
  std::vector<std::int64_t> values;
  for (const auto& match : output.matches) {
    ASSERT_EQ(match.events.size(), 3U);
    values.push_back(std::get<std::int64_t>(match.registers[0]));
  }
  std::sort(values.begin(), values.end());
  EXPECT_EQ(values, (std::vector<std::int64_t>{11, 21}));
  EXPECT_EQ(take(program->inspect(output.state)).event_records, 0U);
  EXPECT_EQ(take(program->inspect(state)).paths.size(), 2U);
}

TEST(CepProgram, SharedPrefixLivesUntilTheLastBranchFinishes) {
  auto definition = pair_definition();
  definition.pattern = CepPattern::event("a", "A").then(
      CepPattern::event("b", "B").or_else(CepPattern::event("c", "C")));
  auto program = take(CepProgram::create(std::move(definition)));
  auto state = take(program->step(take(program->initial_state()), input("a", "A", 1))).state;
  EXPECT_EQ(take(program->inspect(state)).history_nodes, 1U);
  auto one = take(program->step(state, input("b", "B", 2)));
  ASSERT_EQ(one.matches.size(), 1U);
  EXPECT_EQ(take(program->inspect(one.state)).history_nodes, 1U);
  EXPECT_EQ(take(program->inspect(one.state)).event_records, 1U);
  auto two = take(program->step(one.state, input("c", "C", 3)));
  ASSERT_EQ(two.matches.size(), 1U);
  EXPECT_EQ(two.matches[0].events[0].event.id, "a");
  EXPECT_EQ(take(program->inspect(two.state)).history_nodes, 0U);
  EXPECT_EQ(take(program->inspect(two.state)).event_records, 0U);
}

TEST(CepProgram, SkipStrategiesUseAdmissionOrderEvenAtEqualTimestamps) {
  for (auto strategy :
       {CepSkipStrategy::NoSkip, CepSkipStrategy::SkipToNext, CepSkipStrategy::SkipPastLastEvent}) {
    auto program = take(CepProgram::create(pair_definition(strategy)));
    auto state = take(program->initial_state());
    state = take(program->step(state, input("a1", "A", 1))).state;
    state = take(program->step(state, input("a2", "A", 1))).state;
    auto one = take(program->step(state, input("b1", "B", 1)));
    auto two = take(program->step(one.state, input("b2", "B", 1)));
    const auto expected = strategy == CepSkipStrategy::NoSkip       ? 4U
                          : strategy == CepSkipStrategy::SkipToNext ? 2U
                                                                    : 1U;
    EXPECT_EQ(one.matches.size() + two.matches.size(), expected);
    if (strategy == CepSkipStrategy::SkipPastLastEvent)
      EXPECT_EQ(one.matches[0].events[0].event.id, "a1");
  }
}

TEST(CepProgram, PendingSkipMatchRestoresItsOriginalReadWitness) {
  auto definition = pair_definition(CepSkipStrategy::SkipToNext);
  definition.within_us = 10;
  definition.reads = {"feature"};
  definition.pattern = CepPattern::event("a", "A").then(
      CepPattern::event("b", "B",
                        [](const CepContext& context) noexcept -> StatusOr<bool> {
                          return std::get<std::int64_t>(context.event.values[0]) >
                                 std::get<std::int64_t>(context.history[0].event.values[0]);
                        }),
      CepContiguity::FollowedByAny);
  auto program = take(CepProgram::create(std::move(definition)));
  FsmReads before{{"feature", Row{std::int64_t{7}}}};
  auto state = take(program->initial_state());
  state = take(program->step(state, input("a1", "A", 0, 100), before)).state;
  state = take(program->step(state, input("a2", "A", 1, 1), before)).state;
  auto pending = take(program->step(state, input("b", "B", 2, 2), before));
  EXPECT_TRUE(pending.matches.empty());
  EXPECT_EQ(take(program->inspect(pending.state)).pending_matches, 1U);
  state = take(program->decode_state(take(program->encode_state(pending.state))));
  FsmReads after{{"feature", Row{std::int64_t{99}}}};
  auto output = take(program->step(state, input("x", "X", 10), after));
  ASSERT_EQ(output.matches.size(), 1U);
  EXPECT_EQ(output.matches[0].reads, before);
  EXPECT_EQ(output.matches[0].matched_at_us, 2);
  EXPECT_EQ(output.matches[0].emitted_at_us, 10);
  EXPECT_EQ(output.timeouts.size(), 1U);
  EXPECT_EQ(take(program->inspect(output.state)).pending_matches, 0U);
}

TEST(CepProgram, NoSkipDoesNotWaitForAnEarlierUnfinishedStart) {
  auto definition = pair_definition();
  definition.pattern = CepPattern::event("a", "A").then(
      CepPattern::event("b", "B",
                        [](const CepContext& context) noexcept -> StatusOr<bool> {
                          return context.history[0].event.id == "a2";
                        }),
      CepContiguity::FollowedByAny);
  auto program = take(CepProgram::create(std::move(definition)));
  auto state = take(program->initial_state());
  state = take(program->step(state, input("a1", "A", 1))).state;
  state = take(program->step(state, input("a2", "A", 2))).state;
  EXPECT_EQ(take(program->step(state, input("b", "B", 3))).matches.size(), 1U);
}

TEST(CepProgram, EqualityExpiresFirstAndIdleNeverRemovesTheFreshStart) {
  auto definition = pair_definition();
  definition.within_us = 10;
  auto program = take(CepProgram::create(std::move(definition)));
  auto state = take(program->initial_state());
  state = take(program->step(state, input("a", "A", 0))).state;
  EXPECT_EQ(take(program->step(state, input("b9", "B", 9))).matches.size(), 1U);
  auto equal = take(program->step(state, input("b10", "B", 10)));
  EXPECT_TRUE(equal.matches.empty());
  ASSERT_EQ(equal.timeouts.size(), 1U);
  EXPECT_EQ(equal.timeouts[0].scheduled_time_us, 10);
  EXPECT_EQ(equal.timeouts[0].time_us, 10);
  auto delayed = take(program->advance(state, 20));
  EXPECT_EQ(delayed.timeouts[0].scheduled_time_us, 10);
  EXPECT_EQ(delayed.timeouts[0].time_us, 20);
  state = take(program->advance(delayed.state, 1000)).state;
  state = take(program->step(state, input("later_a", "A", 1001))).state;
  EXPECT_EQ(take(program->step(state, input("later_b", "B", 1002))).matches.size(), 1U);
}

TEST(CepProgram, PathAndOutputLimitsRejectTheWholeStep) {
  auto definition = pair_definition();
  definition.limits.max_paths = 1;
  auto program = take(CepProgram::create(definition));
  auto state = take(program->step(take(program->initial_state()), input("a1", "A", 1))).state;
  auto before = take(program->encode_state(state));
  EXPECT_EQ(program->step(state, input("a2", "A", 2)).status().code(),
            ErrorCode::ResourceExhausted);
  EXPECT_EQ(take(program->encode_state(state)), before);
  definition.limits.max_paths = 10;
  definition.limits.max_matches_per_step = 1;
  program = take(CepProgram::create(definition));
  state = take(program->step(take(program->initial_state()), input("a1", "A", 1))).state;
  state = take(program->step(state, input("a2", "A", 2))).state;
  before = take(program->encode_state(state));
  EXPECT_EQ(program->step(state, input("b", "B", 3)).status().code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(take(program->encode_state(state)), before);
}

TEST(CepProgram, PeakHistoryAndEventBudgetsIncludeWorkingBranches) {
  for (bool events : {false, true}) {
    auto definition = pair_definition();
    definition.pattern = CepPattern::event("a", "A").then(CepPattern::event("b", "B"));
    if (events)
      definition.limits.max_events = 1;
    else
      definition.limits.max_history_nodes = 1;
    auto program = take(CepProgram::create(std::move(definition)));
    auto state = take(program->step(take(program->initial_state()), input("a", "A", 1))).state;
    const auto before = take(program->encode_state(state));
    EXPECT_EQ(program->step(state, input("b", "B", 2)).status().code(),
              ErrorCode::ResourceExhausted);
    EXPECT_EQ(take(program->encode_state(state)), before);
  }
}

TEST(CepProgram, AcceptedPathsAlwaysFitLaterAtomicTimeoutCleanup) {
  auto definition = pair_definition();
  definition.limits.max_timeouts_per_step = 1;
  auto program = take(CepProgram::create(std::move(definition)));
  auto state = take(program->step(take(program->initial_state()), input("a1", "A", 1))).state;
  EXPECT_EQ(program->step(state, input("a2", "A", 1)).status().code(),
            ErrorCode::ResourceExhausted);
  auto expired = take(program->advance(state, 101));
  EXPECT_EQ(expired.timeouts.size(), 1U);
  EXPECT_TRUE(take(program->inspect(expired.state)).paths.empty());
}

TEST(CepProgram, RegisterTypeAndCallbackFailuresLeaveInputStateUntouched) {
  auto definition = pair_definition();
  definition.register_fields = {{1, "r", DataType::Int64, false}};
  definition.initial_registers = Row{std::int64_t{0}};
  definition.pattern =
      CepPattern::event("a", "A", {}, [](const CepContext&) noexcept -> StatusOr<Row> {
        return Row{std::string("wrong")};
      });
  auto program = take(CepProgram::create(definition));
  auto state = take(program->initial_state());
  const auto before = take(program->encode_state(state));
  EXPECT_EQ(program->step(state, input("a", "A", 1)).status().code(), ErrorCode::InvalidArgument);
  EXPECT_EQ(take(program->encode_state(state)), before);
  definition.pattern =
      CepPattern::event("a", "A", [](const CepContext&) noexcept -> StatusOr<bool> {
        return Status(ErrorCode::Unavailable, "guard fault");
      });
  program = take(CepProgram::create(definition));
  state = take(program->initial_state());
  EXPECT_EQ(program->step(state, input("a", "A", 1)).status().code(), ErrorCode::Unavailable);
}

TEST(CepProgram, DefinitionRejectsUnsupportedOrInvalidPatternSyntax) {
  for (auto pattern :
       {CepPattern{}, CepPattern::event("a", "A").greedy(), CepPattern::event("a", "A").negate(),
        CepPattern::event("a", "A").repeat(1, std::nullopt),
        CepPattern::event("a", "A").repeat(3, 2), CepPattern::event("a", "A").optional(),
        CepPattern::event("a", "A").optional().repeat(1, 3),
        CepPattern::event("a", "A").then(CepPattern::event("a", "B"))}) {
    auto definition = pair_definition();
    definition.pattern = pattern;
    EXPECT_FALSE(CepProgram::create(std::move(definition)).ok());
  }
  auto definition = pair_definition();
  definition.within_us = 0;
  EXPECT_FALSE(CepProgram::create(definition).ok());
  definition = pair_definition();
  definition.limits.max_nodes = 4;
  EXPECT_EQ(CepProgram::create(definition).status().code(), ErrorCode::ResourceExhausted);
}

TEST(CepProgram, BudgetsAndSerializableDefinitionParticipateInFingerprint) {
  auto definition = pair_definition();
  auto first = take(CepProgram::create(definition));
  auto same = take(CepProgram::create(definition));
  EXPECT_EQ(first->fingerprint(), same->fingerprint());
  auto bytes = take(first->encode_state(take(first->initial_state())));
  definition.limits.max_paths += 1;
  auto changed = take(CepProgram::create(definition));
  EXPECT_NE(first->fingerprint(), changed->fingerprint());
  EXPECT_FALSE(changed->decode_state(bytes).ok());
  ++definition.version;
  changed = take(CepProgram::create(definition));
  EXPECT_FALSE(changed->decode_state(bytes).ok());
}

TEST(CepProgram, CodecRejectsCorruptReferencesTypesAndExpiredPaths) {
  auto definition = pair_definition();
  definition.register_fields = {{1, "r", DataType::Int64, false}};
  definition.initial_registers = Row{std::int64_t{0}};
  auto program = take(CepProgram::create(std::move(definition)));
  auto state = take(program->step(take(program->initial_state()), input("a", "A", 1))).state;
  const auto bytes = take(program->encode_state(state));
  const auto original = take(parse_json_exact(bytes));
  EXPECT_EQ(take(program->encode_state(take(program->decode_state(bytes)))), bytes);
  std::vector<Json> damaged;
  auto json = original;
  json["nodes"][0][2] = json["nodes"][0][0];
  damaged.push_back(json);
  json = original;
  json["nodes"][0][3] = "missing";
  damaged.push_back(json);
  json = original;
  json["nodes"].push_back(json["nodes"][0]);
  damaged.push_back(json);
  json = original;
  json["paths"][0][1] = 99999;
  damaged.push_back(json);
  json = original;
  json["paths"][0][0] = 99999;
  damaged.push_back(json);
  json = original;
  json["paths"].push_back(json["paths"][0]);
  damaged.push_back(json);
  json = original;
  json["paths"][0][2] = Json::array({Json::array({4, "bad"})});
  damaged.push_back(json);
  json = original;
  json["events"][0]["values"][0][0] = 7;
  damaged.push_back(json);
  json = original;
  json["events"].push_back(json["events"][0]);
  damaged.push_back(json);
  json = original;
  json["time"] = 101;
  damaged.push_back(json);
  json = original;
  json["event_sequence"] = -1;
  damaged.push_back(json);
  json = original;
  json["next_history_id"] = 0;
  damaged.push_back(json);
  json = original;
  json["format"] = 999;
  damaged.push_back(json);
  json = original;
  json["paths"] = Json::array();
  damaged.push_back(json);
  for (const auto& value : damaged)
    EXPECT_FALSE(program->decode_state(value.dump()).ok()) << value.dump();
  EXPECT_FALSE(program->decode_state(bytes + "x").ok());
  EXPECT_FALSE(program->decode_state("{").ok());
}

TEST(CepProgram, OverflowInvalidInputAndWorkLimitsAreAtomic) {
  auto definition = pair_definition();
  auto program = take(CepProgram::create(definition));
  auto state = take(program->initial_state());
  auto before = take(program->encode_state(state));
  EXPECT_EQ(program->step(state, input("a", "A", INT64_MAX)).status().code(),
            ErrorCode::ResourceExhausted);
  EXPECT_EQ(take(program->encode_state(state)), before);
  auto json = take(parse_json_exact(before));
  json["event_sequence"] = UINT64_MAX;
  json["last_event_id"] = "old";
  state = take(program->decode_state(json.dump()));
  EXPECT_EQ(program->step(state, input("a", "A", 1)).status().code(), ErrorCode::ResourceExhausted);
  json = take(parse_json_exact(before));
  json["next_history_id"] = UINT64_MAX;
  state = take(program->decode_state(json.dump()));
  EXPECT_EQ(program->step(state, input("a", "A", 1)).status().code(), ErrorCode::ResourceExhausted);
  state = take(program->initial_state());
  auto bad = input("bad", "A", 1);
  bad.values = Row{std::numeric_limits<double>::infinity()};
  EXPECT_FALSE(program->step(state, bad).ok());
  bad.id = std::string("\xff", 1);
  EXPECT_FALSE(program->step(state, bad).ok());
  definition.limits.max_work_per_step = 1;
  EXPECT_EQ(CepProgram::create(definition).status().code(), ErrorCode::ResourceExhausted);
  definition.limits.max_work_per_step = 8;
  program = take(CepProgram::create(definition));
  state = take(program->step(take(program->initial_state()), input("a1", "A", 1))).state;
  before = take(program->encode_state(state));
  EXPECT_EQ(program->step(state, input("a2", "A", 2)).status().code(),
            ErrorCode::ResourceExhausted);
  EXPECT_EQ(take(program->encode_state(state)), before);
}

TEST(CepProgram, NextOfKindConsumesFirstNewsEvenWhenItsGuardRejects) {
  auto definition = pair_definition();
  definition.pattern =
      CepPattern::event("arm", "Arm")
          .then(CepPattern::event("news", "News",
                                  [](const CepContext& context) noexcept -> StatusOr<bool> {
                                    return std::get<std::int64_t>(context.event.values[0]) > 0;
                                  }),
                CepContiguity::NextOfKind);
  auto program = take(CepProgram::create(definition));
  auto armed = take(program->step(take(program->initial_state()), input("arm", "Arm", 1))).state;
  armed = take(program->step(armed, input("trade", "Trade", 2))).state;
  EXPECT_EQ(take(program->step(armed, input("positive", "News", 3, 1))).matches.size(), 1U);
  auto rejected = take(program->step(armed, input("negative", "News", 3, -1))).state;
  EXPECT_TRUE(take(program->step(rejected, input("positive", "News", 4, 1))).matches.empty());
}

TEST(CepProgram, CodecRejectsImpossibleGlobalTimeOrderAndStrictInputGaps) {
  auto program = take(CepProgram::create(pair_definition()));
  auto state = take(program->step(take(program->initial_state()), input("a1", "A", 1))).state;
  state = take(program->step(state, input("a2", "A", 2))).state;
  auto json = take(parse_json_exact(take(program->encode_state(state))));
  json["events"][0]["time"] = 10;
  json["time"] = 10;
  EXPECT_FALSE(program->decode_state(json.dump()).ok());
  auto definition = pair_definition();
  definition.pattern =
      CepPattern::event("a", "A").then(CepPattern::event("b", "B"), CepContiguity::Strict);
  program = take(CepProgram::create(std::move(definition)));
  state = take(program->step(take(program->initial_state()), input("a", "A", 1))).state;
  json = take(parse_json_exact(take(program->encode_state(state))));
  json["event_sequence"] = 2;
  json["last_event_id"] = "unmatched";
  EXPECT_FALSE(program->decode_state(json.dump()).ok());
}

TEST(CepProgram, ReadBudgetCountsEscapedAliasBytes) {
  auto definition = pair_definition();
  const std::string alias(20, '\\');
  definition.reads = {alias};
  definition.limits.max_read_bytes = 40;
  auto program = take(CepProgram::create(std::move(definition)));
  auto state = take(program->initial_state());
  EXPECT_EQ(
      program->step(state, input("a", "A", 1), {{alias, Row{std::int64_t{7}}}}).status().code(),
      ErrorCode::ResourceExhausted);
}

std::vector<std::string> oracle(const std::string& kinds, std::size_t max_count,
                                CepContiguity inner, std::int64_t within) noexcept {
  std::vector<std::string> result;
  for (std::size_t last = 0; last < kinds.size(); ++last) {
    if (kinds[last] != 'B')
      continue;
    const auto bound = std::size_t{1} << last;
    for (std::size_t mask = 1; mask < bound; ++mask) {
      std::vector<std::size_t> indices;
      for (std::size_t i = 0; i < last; ++i)
        if (mask & (std::size_t{1} << i))
          indices.push_back(i);
      if (indices.size() > max_count || static_cast<std::int64_t>(last - indices[0]) >= within)
        continue;
      bool legal = true;
      for (std::size_t n = 0; n < indices.size(); ++n) {
        legal &= kinds[indices[n]] == 'A';
        if (n == 0)
          continue;
        if (inner == CepContiguity::Strict)
          legal &= indices[n] == indices[n - 1] + 1;
        if (inner == CepContiguity::FollowedBy)
          for (auto i = indices[n - 1] + 1; i < indices[n]; ++i)
            legal &= kinds[i] != 'A';
      }
      if (!legal)
        continue;
      std::string binding;
      for (auto i : indices)
        binding += "a:e" + std::to_string(i) + ";";
      binding += "b:e" + std::to_string(last) + ";";
      result.push_back(std::move(binding));
    }
  }
  std::sort(result.begin(), result.end());
  return result;
}

TEST(CepProgram, ExhaustiveSmallStreamsMatchIndependentSubsequenceEnumeration) {
  for (auto inner :
       {CepContiguity::Strict, CepContiguity::FollowedBy, CepContiguity::FollowedByAny}) {
    for (std::size_t repeats : {1U, 3U}) {
      for (std::int64_t within : {3, 100}) {
        auto definition = pair_definition();
        definition.within_us = within;
        definition.pattern = CepPattern::event("a", "A")
                                 .repeat(1, repeats, inner)
                                 .then(CepPattern::event("b", "B"), CepContiguity::FollowedByAny);
        auto program = take(CepProgram::create(std::move(definition)));
        const std::string alphabet = "ABX";
        for (std::size_t length = 0, combinations = 1; length <= 5; ++length, combinations *= 3) {
          for (std::size_t code = 0; code < combinations; ++code) {
            auto number = code;
            std::string kinds(length, 'X');
            for (auto& kind : kinds) {
              kind = alphabet[number % 3];
              number /= 3;
            }
            auto state = take(program->initial_state());
            std::vector<CepMatch> matches;
            for (std::size_t i = 0; i < length; ++i) {
              auto output =
                  take(program->step(state, input("e" + std::to_string(i), std::string(1, kinds[i]),
                                                  static_cast<std::int64_t>(i))));
              for (auto& match : output.matches) {
                EXPECT_EQ(match.matched_at_us, static_cast<std::int64_t>(i));
                EXPECT_EQ(match.emitted_at_us, static_cast<std::int64_t>(i));
                matches.push_back(std::move(match));
              }
              // Every prefix is a real encode/decode boundary, not just a final-state check.
              state = take(program->decode_state(take(program->encode_state(output.state))));
              EXPECT_EQ(normalized(matches), oracle(kinds.substr(0, i + 1), repeats, inner, within))
                  << "stream=" << kinds << " prefix=" << i << " repeats=" << repeats
                  << " inner=" << static_cast<int>(inner) << " within=" << within;
            }
          }
        }
      }
    }
  }
}
} // namespace
} // namespace eventflow::test
