#include "eventflow/fsm.hpp"
#include "status_test_support.hpp"
#include <gtest/gtest.h>

namespace eventflow {
namespace {
using test::take;

FsmGuard positive() noexcept {
  return [](const FsmContext& context) noexcept -> StatusOr<bool> {
    if (context.event.values.size() != 1 ||
        !std::holds_alternative<std::int64_t>(context.event.values[0]))
      return Status(ErrorCode::InvalidArgument, "sentiment must be an integer");
    return std::get<std::int64_t>(context.event.values[0]) > 0;
  };
}

TEST(FsmProgram, StrictNextConsumesFirstNewsAndIgnoresOtherEventKinds) {
  auto definition = followed_by("rule", "armed", "news", {}, positive(), 10, true);
  auto program = take(FsmProgram::create(std::move(definition)));
  auto state = take(program->initial_state());
  state = take(program->step(state, {"armed", "a", 1, {}})).state;
  state = take(program->step(state, {"quote", "q", 2, {}})).state;
  auto rejected = take(program->step(state, {"news", "n1", 3, {std::int64_t(-1)}}));
  EXPECT_TRUE(rejected.emissions.empty());
  EXPECT_TRUE(
      take(program->step(rejected.state, {"news", "n2", 4, {std::int64_t(1)}})).emissions.empty());
  auto accepted = take(program->step(state, {"news", "n3", 3, {std::int64_t(1)}}));
  ASSERT_EQ(accepted.emissions.size(), 1u);
  EXPECT_EQ(accepted.emissions[0].cause, "n3");
}

TEST(FsmProgram, TimeoutAtEqualityRunsBeforeData) {
  auto program =
      take(FsmProgram::create(followed_by("rule", "armed", "news", {}, positive(), 10, true)));
  auto state = take(program->initial_state());
  state = take(program->step(state, {"armed", "a", 1, {}})).state;
  EXPECT_TRUE(
      take(program->step(state, {"news", "late", 11, {std::int64_t(1)}})).emissions.empty());
  EXPECT_EQ(take(program->step(state, {"news", "on-time", 10, {std::int64_t(1)}})).emissions.size(),
            1u);
}

TEST(FsmProgram, CrossingReadsCurrentDeclaredViewAndOnlyEmitsOnFalseToTrue) {
  auto predicate = [](const FsmContext& context) noexcept -> StatusOr<bool> {
    const auto& row = context.reads.find("volume")->second;
    return row && std::get<std::int64_t>((*row)[0]) > 100;
  };
  auto definition = crossing("threshold", predicate);
  definition.reads = {"volume"};
  auto program = take(FsmProgram::create(std::move(definition)));
  auto state = take(program->initial_state());
  int emissions = 0;
  const std::vector<std::int64_t> volumes{0, 100, 101, 105, 99, 110};
  for (std::size_t i = 0; i < volumes.size(); ++i) {
    auto output = take(program->step(state, {"trade", std::to_string(i), std::int64_t(i), {}},
                                     {{"volume", Row{volumes[i]}}}));
    emissions += static_cast<int>(output.emissions.size());
    state = std::move(output.state);
  }
  EXPECT_EQ(emissions, 2);
  EXPECT_EQ(state.state, "above");
}

TEST(FsmProgram, FollowedBySkipsNonmatchingNewsWhileStrictNextConsumesIt) {
  auto program =
      take(FsmProgram::create(followed_by("sequence", "armed", "news", {}, positive(), 20)));
  auto state = take(program->initial_state());
  state = take(program->step(state, {"armed", "arm", 1, {}})).state;
  state = take(program->step(state, {"news", "negative", 2, {std::int64_t(-1)}})).state;
  auto output = take(program->step(state, {"news", "positive", 3, {std::int64_t(1)}}));
  EXPECT_EQ(output.emissions.size(), 1u);
  EXPECT_EQ(output.state.state, "accepted");
  EXPECT_FALSE(output.state.deadline_us);
}

TEST(FsmProgram, OnceAndResetKeepAmonotonicGeneration) {
  auto program = take(FsmProgram::create(on_event("once", "news", {}, true)));
  auto state = take(program->initial_state());
  auto first = take(program->step(state, {"news", "first", 1, {}}));
  ASSERT_EQ(first.emissions.size(), 1u);
  auto ignored = take(program->step(first.state, {"news", "second", 2, {}}));
  EXPECT_TRUE(ignored.emissions.empty());
  auto reset = take(program->step(ignored.state, {"reset", "reset-1", 3, {}}));
  auto next = take(program->step(reset.state, {"news", "third", 4, {}}));
  ASSERT_EQ(next.emissions.size(), 1u);
  EXPECT_GT(next.emissions[0].transition_sequence, first.emissions[0].transition_sequence);
}

TEST(FsmProgram, ProgrammableRegistersAreTypedAndRestorable) {
  auto definition = on_event("count", "news");
  definition.register_fields = {{0, "count", DataType::Int64, false},
                                {1, "text", DataType::String, true},
                                {2, "embedding", DataType::FloatVector, false}};
  definition.initial_registers = {std::int64_t(0), std::monostate{}, std::vector<double>{1, 2}};
  definition.transitions[0].update = [](const FsmContext& context) noexcept -> StatusOr<Row> {
    auto registers = context.state.registers;
    std::get<std::int64_t>(registers[0]) += 1;
    registers[1] = context.event.occurrence_id;
    return registers;
  };
  auto program = take(FsmProgram::create(std::move(definition)));
  auto state = take(program->initial_state());
  state = take(program->step(state, {"news", "first", 1, {}})).state;
  state = take(program->decode_state(take(program->encode_state(state))));
  state = take(program->step(state, {"news", "second", 2, {}})).state;
  EXPECT_EQ(std::get<std::int64_t>(state.registers[0]), 2);
  EXPECT_EQ(std::get<std::string>(state.registers[1]), "second");
  EXPECT_EQ(std::get<std::vector<double>>(state.registers[2]), (std::vector<double>{1, 2}));
}

TEST(FsmProgram, TypedActionFailureDoesNotMutateCallerState) {
  auto definition = on_event("bad-action", "news");
  definition.register_fields = {{0, "count", DataType::Int64, false}};
  definition.initial_registers = {std::int64_t(7)};
  definition.transitions[0].update = [](const FsmContext&) noexcept -> StatusOr<Row> {
    return Row{std::string("wrong-type")};
  };
  auto program = take(FsmProgram::create(std::move(definition)));
  auto state = take(program->initial_state());
  const auto before = take(program->encode_state(state));
  EXPECT_FALSE(program->step(state, {"news", "first", 1, {}}).ok());
  EXPECT_EQ(take(program->encode_state(state)), before);
}

TEST(FsmProgram, GuardCombinatorsShortCircuitAndPropagateStatus) {
  auto yes = [](const FsmContext&) noexcept -> StatusOr<bool> { return true; };
  auto no = [](const FsmContext&) noexcept -> StatusOr<bool> { return false; };
  auto error = [](const FsmContext&) noexcept -> StatusOr<bool> {
    return Status(ErrorCode::Unavailable, "view unavailable");
  };
  FsmState state;
  FsmEvent event;
  FsmReads reads;
  FsmContext context{state, event, reads};
  EXPECT_FALSE(take(all_guards({no, error})(context)));
  EXPECT_TRUE(take(any_guards({yes, error})(context)));
  EXPECT_EQ(all_guards({yes, error})(context).status().code(), ErrorCode::Unavailable);
  EXPECT_EQ(any_guards({no, error})(context).status().code(), ErrorCode::Unavailable);
  EXPECT_TRUE(take(all_guards({})(context)));
  EXPECT_FALSE(take(any_guards({})(context)));
  EXPECT_FALSE(take(not_guard(yes)(context)));
}

TEST(FsmProgram, GuardCannotReceiveUndeclaredOrMissingViewReads) {
  bool called = false;
  auto definition = on_event("readset", "news", [&](const FsmContext&) noexcept -> StatusOr<bool> {
    called = true;
    return true;
  });
  definition.reads = {"market"};
  auto program = take(FsmProgram::create(std::move(definition)));
  auto state = take(program->initial_state());
  EXPECT_FALSE(program->step(state, {"news", "n", 1, {}}).ok());
  EXPECT_FALSE(program->step(state, {"news", "n", 1, {}}, {{"wrong", Row{}}}).ok());
  EXPECT_FALSE(called);
  EXPECT_EQ(
      take(program->step(state, {"news", "n", 1, {}}, {{"market", std::nullopt}})).emissions.size(),
      1u);
  EXPECT_TRUE(called);
}

TEST(FsmProgram, NoInputAdvanceRestoresDeadlineAndResetCancelsIt) {
  auto program =
      take(FsmProgram::create(followed_by("timeout", "armed", "news", {}, {}, 10, true)));
  auto state = take(program->initial_state());
  state = take(program->step(state, {"armed", "a", 1, {}})).state;
  state = take(program->decode_state(take(program->encode_state(state))));
  EXPECT_EQ(state.deadline_us, 11);
  auto expired = take(program->advance(state, 100));
  EXPECT_EQ(expired.state.state, "waiting");
  EXPECT_FALSE(expired.state.deadline_us);
  auto reset = take(program->step(state, {"reset", "r", 2, {}}));
  EXPECT_FALSE(reset.state.deadline_us);
  EXPECT_EQ(take(program->advance(reset.state, 100)).state.state, "waiting");
}

TEST(FsmProgram, TimeoutAndDataShareOneBoundedTransitionBudget) {
  auto definition = followed_by("bounded", "armed", "news", {}, {}, 10, true);
  definition.max_steps = 1;
  auto program = take(FsmProgram::create(std::move(definition)));
  auto state = take(program->initial_state());
  state = take(program->step(state, {"armed", "a", 1, {}})).state;
  const auto before = take(program->encode_state(state));
  // Timeout returns to waiting; the same-time arm would require a second transition.
  EXPECT_EQ(program->step(state, {"armed", "a2", 11, {}}).status().code(),
            ErrorCode::ResourceExhausted);
  EXPECT_EQ(take(program->encode_state(state)), before);
}

TEST(FsmProgram, DelayedTimeoutCycleSchedulesItsNextWaitFromCurrentTime) {
  FsmDefinition definition;
  definition.id = "cycle";
  definition.states = {{"one", 1}};
  definition.initial = "one";
  definition.max_steps = 3;
  definition.transitions = {{"one", "$timeout", "one", {}, {}, "tick"}};
  auto program = take(FsmProgram::create(std::move(definition)));
  auto state = take(program->initial_state());
  const auto advanced = take(program->advance(state, 3));
  EXPECT_EQ(advanced.emissions.size(), 1u);
  EXPECT_EQ(advanced.state.entered_at_us, 3);
  EXPECT_EQ(advanced.state.deadline_us, 4);
  EXPECT_EQ(state.transition_sequence, 0u);
}

TEST(FsmProgram, TimeoutCallbackUsesNowAndDoesNotExpireANewRelativeWaitRetroactively) {
  std::int64_t observed = -1;
  FsmDefinition definition;
  definition.id = "delayed";
  definition.states = {{"armed", 5}, {"settling", 7}, {"done", {}}};
  definition.initial = "armed";
  definition.transitions = {{"armed",
                             "$timeout",
                             "settling",
                             {},
                             [&](const FsmContext& context) noexcept -> StatusOr<Row> {
                               observed = context.event.time_us;
                               return Row{};
                             },
                             "expired"},
                            {"settling", "$timeout", "done", {}, {}, "settled"}};
  auto program = take(FsmProgram::create(std::move(definition)));
  const auto output = take(program->advance(take(program->initial_state()), 20));
  EXPECT_EQ(observed, 20);
  EXPECT_EQ(output.state.state, "settling");
  EXPECT_EQ(output.state.entered_at_us, 20);
  EXPECT_EQ(output.state.deadline_us, 27);
  ASSERT_EQ(output.emissions.size(), 1u);
  EXPECT_EQ(output.emissions[0].time_us, 20);
  EXPECT_EQ(output.emissions[0].scheduled_time_us, 5);
}

TEST(FsmProgram, RejectsOverflowBackwardsTimeAndForgedTimeoutEvents) {
  auto program = take(FsmProgram::create(followed_by("times", "armed", "news", {}, {}, 10)));
  auto state = take(program->initial_state(10));
  EXPECT_FALSE(program->step(state, {"armed", "backwards", 9, {}}).ok());
  EXPECT_EQ(program->step(state, {"armed", "overflow", INT64_MAX, {}}).status().code(),
            ErrorCode::ResourceExhausted);
  EXPECT_FALSE(program->step(state, {"$timeout", "forged", 11, {}}).ok());
  state.transition_sequence = UINT64_MAX;
  EXPECT_EQ(program->step(state, {"armed", "exhausted", 11, {}}).status().code(),
            ErrorCode::ResourceExhausted);
}

TEST(FsmProgram, StateCodecRejectsWrongProgramVersionAndInvalidDeadline) {
  auto program = take(FsmProgram::create(followed_by("codec", "armed", "news", {}, {}, 10)));
  auto state = take(program->step(take(program->initial_state()), {"armed", "a", 1, {}})).state;
  const auto valid = take(parse_json_exact(take(program->encode_state(state))));
  for (const auto* field :
       {"program_id", "program_version", "state", "entered_at_us", "deadline_us"}) {
    auto json = valid;
    json[field] = nullptr;
    EXPECT_FALSE(program->decode_state(json.dump()).ok()) << field;
  }
  auto version = valid;
  version["program_version"] = 2;
  EXPECT_FALSE(program->decode_state(version.dump()).ok());
  auto deadline = valid;
  deadline["deadline_us"] = 12;
  EXPECT_FALSE(program->decode_state(deadline.dump()).ok());
  EXPECT_FALSE(program->decode_state("{").ok());
}

TEST(FsmProgram, InvalidDefinitionsAreRejectedBeforeAnyDispatch) {
  auto definition = on_event("valid", "news");
  auto unknown = definition;
  unknown.transitions[0].to = "missing";
  EXPECT_FALSE(FsmProgram::create(std::move(unknown)).ok());
  auto duplicate = definition;
  duplicate.states.push_back(duplicate.states[0]);
  EXPECT_FALSE(FsmProgram::create(std::move(duplicate)).ok());
  definition.states[0].timeout_us = 10;
  EXPECT_FALSE(FsmProgram::create(std::move(definition)).ok());
  EXPECT_FALSE(FsmProgram::create(followed_by("bad-timeout", "a", "b", {}, {}, 0)).ok());
}

TEST(FsmProgram, FinalTimeAndSequenceGrowthMustFitPersistedStateBudget) {
  auto definition = on_event("budget", "news");
  auto original = take(FsmProgram::create(definition));
  definition.max_state_bytes = take(original->encode_state(take(original->initial_state()))).size();
  auto program = take(FsmProgram::create(std::move(definition)));
  auto state = take(program->initial_state());
  EXPECT_EQ(program->advance(state, INT64_MAX).status().code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(program->step(state, {"ignored", "n", INT64_MAX, {}}).status().code(),
            ErrorCode::ResourceExhausted);
  for (int i = 0; i < 9; ++i)
    state = take(program->step(state, {"news", std::to_string(i), 0, {}})).state;
  EXPECT_EQ(state.transition_sequence, 9u);
  EXPECT_EQ(program->step(state, {"reset", "r", 0, {}}).status().code(),
            ErrorCode::ResourceExhausted);
  EXPECT_TRUE(program->encode_state(state).ok());
}
} // namespace
} // namespace eventflow
