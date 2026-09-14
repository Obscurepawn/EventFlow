#include "eventflow/catalog.hpp"
#include "eventflow/observation_program.hpp"
#include "status_test_support.hpp"
#include <gtest/gtest.h>

namespace eventflow::test {
namespace {
TEST(ObservationProgram, FsmFingerprintAndBindingMatchActualCompiledRecipe) {
  auto fsm = on_event("recipe", "news", {}, true);
  auto fingerprint = observation_program_fingerprint(fsm);
  ASSERT_TRUE(fingerprint.ok()) << fingerprint.status().message();
  auto catalog = take(Catalog::create());
  ObservationRuleDefinition declaration;
  declaration.name = "instance";
  declaration.recipe_id = fsm.id;
  declaration.recipe_version = fsm.version;
  declaration.implementation_identity = "builtin.once.v1";
  declaration.program_fingerprint = *fingerprint;
  declaration.scope_fields = {{1, "k", DataType::String, false}};
  ObservationInputDefinition input;
  input.id = "news_binding";
  input.event_kind = "news";
  input.source_kind = "news";
  declaration.inputs = {input};
  take(catalog->define_observation(declaration));
  auto snapshot = take(catalog->snapshot());
  auto bound = ObservationProgram::bind(snapshot.observations[0], "builtin.once.v1", fsm);
  EXPECT_TRUE(bound.ok()) << bound.status().message();
}

ObservationRuleDescriptor descriptor_for(const ObservationProgramDefinition& definition,
                                         const std::vector<std::string>& kinds,
                                         std::vector<Field> event_fields = {}) noexcept {
  auto catalog = take(Catalog::create());
  auto relation = take(catalog->define_sql(
      "CREATE TABLE input(k STRING,v INT64 NOT NULL,PRIMARY KEY(k)) PARTITION BY(k)"));
  auto snapshot = take(catalog->snapshot());
  ObservationRuleDefinition declared;
  declared.name = "instance";
  declared.version = 7;
  declared.implementation_identity = "builtin.observer.v1";
  declared.scope_fields = {{1, "k", DataType::String, false}};
  std::visit(
      [&](const auto& program) noexcept {
        declared.recipe_id = program.id;
        declared.recipe_version = program.version;
        declared.program_fingerprint = take(observation_program_fingerprint(program));
        for (const auto& alias : program.reads)
          declared.reads.push_back(
              {alias, relation, snapshot.relations[0]->storage_schema.fields, 512});
      },
      definition);
  declared.program_kind = std::holds_alternative<FsmDefinition>(definition)
                              ? ObservationProgramKind::Fsm
                              : ObservationProgramKind::Cep;
  for (const auto& kind : kinds) {
    ObservationInputDefinition input;
    input.id = kind;
    input.event_kind = kind;
    input.source_kind = kind;
    input.source_fields = event_fields;
    input.max_event_bytes = 1024;
    declared.inputs.push_back(std::move(input));
  }
  take(catalog->define_observation(declared));
  return take(catalog->snapshot()).observations[0];
}

std::unique_ptr<ObservationProgram> bind_program(const ObservationProgramDefinition& definition,
                                                 const std::vector<std::string>& kinds,
                                                 std::vector<Field> fields = {}) noexcept {
  auto descriptor = descriptor_for(definition, kinds, std::move(fields));
  return take(ObservationProgram::bind(descriptor, "builtin.observer.v1", definition));
}

TEST(ObservationProgram, CrossingActivelyConsumesCurrentTypedReadsAndRoundtripsState) {
  auto fsm = crossing("cross", [](const FsmContext& context) noexcept -> StatusOr<bool> {
    const auto& value = context.reads.at("view");
    return value && std::get<std::int64_t>((*value)[1]) > 100;
  });
  fsm.reads = {"view"};
  auto program = bind_program(fsm, {"change"});
  auto state = take(program->initial_state(0));
  std::size_t count = 0;
  const std::vector<std::int64_t> sequence{10, 120, 130, 10, 150};
  for (std::size_t i = 0; i < sequence.size(); ++i) {
    auto before = state;
    FsmReads reads{{"view", Row{std::string("a"), sequence[i]}}};
    auto result = take(
        program->step(state, "change",
                      {"change", std::to_string(i), static_cast<std::int64_t>(i + 1), {}}, reads));
    EXPECT_EQ(state, before);
    count += result.emissions.size();
    state = std::move(result.next_state);
    EXPECT_LE(state.size(), program->bounds().max_state_bytes);
    EXPECT_EQ(take(program->inspect_state(state)).time_us, static_cast<std::int64_t>(i + 1));
  }
  EXPECT_EQ(count, 2);
  auto result =
      take(program->step(state, "change", {"change", "missing", 6, {}}, {{"view", std::nullopt}}));
  EXPECT_TRUE(result.emissions.empty());
  EXPECT_FALSE(
      program
          ->step(result.next_state, "change", {"change", "past", 5, {}}, {{"view", std::nullopt}})
          .ok());
}

TEST(ObservationProgram, AllGuardsInOneStepShareAnOwnedReadSnapshot) {
  const FsmReads* seen = nullptr;
  int calls = 0;
  FsmDefinition fsm;
  fsm.id = "snapshot";
  fsm.states = {{"s", {}}};
  fsm.initial = "s";
  fsm.reads = {"view"};
  fsm.transitions = {{"s",
                      "data",
                      "s",
                      [&](const FsmContext& context) noexcept -> StatusOr<bool> {
                        seen = &context.reads;
                        ++calls;
                        return false;
                      },
                      {},
                      {}},
                     {"s",
                      "data",
                      "s",
                      [&](const FsmContext& context) noexcept -> StatusOr<bool> {
                        ++calls;
                        return seen == &context.reads &&
                               std::get<std::int64_t>(context.reads.at("view")->at(1)) == 120;
                      },
                      {},
                      "ready"}};
  auto program = bind_program(fsm, {"data"});
  FsmReads reads{{"view", Row{std::string("a"), std::int64_t(120)}}};
  auto result =
      take(program->step(take(program->initial_state(0)), "data", {"data", "first", 1, {}}, reads));
  EXPECT_EQ(calls, 2);
  EXPECT_NE(seen, &reads);
  ASSERT_EQ(result.emissions.size(), 1);
  EXPECT_EQ(result.emissions[0].name, "ready");
}

TEST(ObservationProgram, ExactDeadlineUsesActualAdvanceReadsBeforeDataAndDoesNotMutateOldState) {
  FsmDefinition fsm;
  fsm.id = "timed";
  fsm.states = {{"idle", {}}, {"waiting", 5}};
  fsm.initial = "idle";
  fsm.reads = {"view"};
  fsm.transitions = {{"idle", "arm", "waiting", {}, {}, {}},
                     {"waiting",
                      "$timeout",
                      "idle",
                      [](const FsmContext& context) noexcept -> StatusOr<bool> {
                        return std::get<std::int64_t>(context.reads.at("view")->at(1)) > 10;
                      },
                      {},
                      "expired"},
                     {"waiting", "$timeout", "idle", {}, {}, {}}};
  auto program = bind_program(fsm, {"arm", "data"});
  auto state = take(program->initial_state(0));
  state = take(program->step(state, "arm", {"arm", "a", 1, {}},
                             {{"view", Row{std::string("a"), std::int64_t(1)}}}))
              .next_state;
  EXPECT_EQ(take(program->inspect_state(state)).next_deadline_us, 6);
  const auto before = state;
  auto expired =
      take(program->advance(state, 6, {{"view", Row{std::string("a"), std::int64_t(20)}}}));
  ASSERT_EQ(expired.emissions.size(), 1);
  EXPECT_EQ(expired.emissions[0].scheduled_time_us, 6);
  EXPECT_EQ(expired.emissions[0].time_us, 6);
  EXPECT_EQ(state, before);
  auto by_data = take(program->step(state, "data", {"data", "b", 6, {}},
                                    {{"view", Row{std::string("a"), std::int64_t(20)}}}));
  EXPECT_EQ(by_data.emissions.size(), 1);
  EXPECT_FALSE(by_data.next_deadline_us);
  EXPECT_FALSE(program->advance(state, 0, {{"view", std::nullopt}}).ok());
}

TEST(ObservationProgram, RejectsTypesAliasesEventKindsAndByteBudgetsBeforeCallbacks) {
  int calls = 0;
  auto fsm = on_event("typed", "data", [&](const FsmContext&) noexcept -> StatusOr<bool> {
    ++calls;
    return true;
  });
  fsm.reads = {"view"};
  auto descriptor = descriptor_for(fsm, {"data"}, {{1, "value", DataType::Float64, false}});
  descriptor.definition.reads[0].max_row_bytes = 32;
  descriptor.definition.inputs[0].max_event_bytes = 128;
  auto program = take(ObservationProgram::bind(descriptor, "builtin.observer.v1", fsm));
  const auto state = take(program->initial_state(0));
  const FsmReads reads{{"view", Row{std::string("a"), std::int64_t(1)}}};
  EXPECT_FALSE(program->step(state, "data", {"data", "a", 1, {std::int64_t(1)}}, reads).ok());
  EXPECT_FALSE(program->step(state, "data", {"other", "a", 1, {1.0}}, reads).ok());
  EXPECT_FALSE(program->step(state, "missing", {"data", "a", 1, {1.0}}, reads).ok());
  EXPECT_FALSE(program->step(state, "data", {"data", "a", 1, {1.0}}, {}).ok());
  EXPECT_FALSE(
      program->step(state, "data", {"data", "a", 1, {1.0}}, {{"view", Row{std::string("a"), 1.0}}})
          .ok());
  EXPECT_EQ(program
                ->step(state, "data", {"data", "a", 1, {1.0}},
                       {{"view", Row{std::string(40, 'x'), std::int64_t(1)}}})
                .status()
                .code(),
            ErrorCode::ResourceExhausted);
  EXPECT_FALSE(program->step(state, "data", {"data", std::string(120, 'x'), 1, {1.0}}, reads).ok());
  EXPECT_EQ(calls, 0);
  EXPECT_TRUE(program->step(state, "data", {"data", "ok", 1, {1.0}}, reads).ok());
  EXPECT_EQ(calls, 1);
}

TEST(ObservationProgram, MismatchedRecipesCallbacksAndDeclaredReadSetsAreRejected) {
  auto fsm =
      on_event("recipe", "news", [](const FsmContext&) noexcept -> StatusOr<bool> { return true; });
  auto descriptor = descriptor_for(fsm, {"news"});
  EXPECT_FALSE(ObservationProgram::bind(descriptor, "wrong", fsm).ok());
  auto changed = fsm;
  changed.id = "other";
  EXPECT_FALSE(ObservationProgram::bind(descriptor, "builtin.observer.v1", changed).ok());
  changed = fsm;
  ++changed.version;
  EXPECT_FALSE(ObservationProgram::bind(descriptor, "builtin.observer.v1", changed).ok());
  changed = fsm;
  changed.transitions[0].guard = {};
  EXPECT_FALSE(ObservationProgram::bind(descriptor, "builtin.observer.v1", changed).ok());
  changed = fsm;
  changed.reads = {"view"};
  EXPECT_FALSE(ObservationProgram::bind(descriptor, "builtin.observer.v1", changed).ok());
  changed = fsm;
  changed.transitions[0].to = "done";
  EXPECT_FALSE(ObservationProgram::bind(descriptor, "builtin.observer.v1", changed).ok());
  auto invalid = descriptor;
  invalid.event_fields.push_back({});
  EXPECT_FALSE(ObservationProgram::bind(invalid, "builtin.observer.v1", fsm).ok());
  auto same_callbacks = fsm;
  EXPECT_EQ(take(observation_program_fingerprint(same_callbacks)),
            descriptor.definition.program_fingerprint);
}

TEST(ObservationProgram, CanonicalStateRejectsWrongProgramExtraFieldsAndBackwardsTime) {
  auto fsm = on_event("recipe", "news", {}, true);
  auto descriptor = descriptor_for(fsm, {"news"});
  auto program = take(ObservationProgram::bind(descriptor, "builtin.observer.v1", fsm));
  auto state = take(program->initial_state(10));
  auto encoded = take(parse_json_exact(state));
  EXPECT_EQ(encoded["program_fingerprint"], descriptor.definition.program_fingerprint);
  auto wrong = encoded;
  wrong["program_fingerprint"] = "{}";
  EXPECT_FALSE(program->inspect_state(wrong.dump()).ok());
  wrong = encoded;
  wrong["extra"] = true;
  EXPECT_FALSE(program->inspect_state(wrong.dump()).ok());
  wrong = encoded;
  auto inner = take(parse_json_exact(wrong["state"].get<std::string>()));
  inner["time_us"] = -1;
  wrong["state"] = inner.dump();
  EXPECT_FALSE(program->inspect_state(wrong.dump()).ok());
  EXPECT_FALSE(program->step(state, "news", {"news", "old", 9, {}}, {}).ok());
  EXPECT_FALSE(program->initial_state(-1).ok());
  auto once = take(program->step(state, "news", {"news", "one", 11, {}}, {}));
  auto twice = take(program->step(once.next_state, "news", {"news", "two", 12, {}}, {}));
  EXPECT_EQ(once.emissions.size(), 1);
  EXPECT_TRUE(twice.emissions.empty());
}

CepDefinition pair_program() noexcept {
  CepDefinition result;
  result.id = "pair";
  result.within_us = 10;
  result.reads = {"view"};
  result.pattern = CepPattern::event("a", "A").then(
      CepPattern::event("b", "B",
                        [](const CepContext& context) noexcept -> StatusOr<bool> {
                          return std::get<std::int64_t>(context.reads.at("view")->at(1)) > 100;
                        }),
      CepContiguity::FollowedByAny);
  result.limits.max_state_bytes = 64 * 1024;
  result.limits.max_event_bytes = 1024;
  result.limits.max_register_bytes = 1024;
  result.limits.max_read_bytes = 2048;
  result.limits.max_output_bytes = 8192;
  return result;
}

TEST(ObservationProgram, CepUsesActualProcessingTimeAndReturnsOwnedMatchesAndTimeouts) {
  auto definition = pair_program();
  auto program = bind_program(definition, {"A", "B"});
  const FsmReads low{{"view", Row{std::string("a"), std::int64_t(10)}}};
  FsmReads high{{"view", Row{std::string("a"), std::int64_t(120)}}};
  auto initial = take(program->initial_state(0));
  auto first = take(program->step(initial, "A", {"A", "a1", 1, {}}, low));
  EXPECT_EQ(first.next_deadline_us, 11);
  auto match = take(program->step(first.next_state, "B", {"B", "b1", 10, {}}, high));
  ASSERT_EQ(match.matches.size(), 1);
  EXPECT_EQ(match.matches[0].matched_at_us, 10);
  high["view"] = Row{std::string("a"), std::int64_t(0)};
  EXPECT_EQ(std::get<std::int64_t>(match.matches[0].reads.at("view")->at(1)), 120);
  auto expired = take(program->advance(first.next_state, 11, low));
  EXPECT_EQ(expired.timeouts.size(), 1);
  EXPECT_TRUE(expired.matches.empty());
  EXPECT_FALSE(expired.next_deadline_us);
  EXPECT_EQ(expired.timeouts[0].scheduled_time_us, 11);
  EXPECT_LE(match.next_state.size(), program->bounds().max_state_bytes);
  EXPECT_LE(match.matches.size() + match.timeouts.size(), program->bounds().max_output_count);
}

TEST(ObservationProgram, CepRestoredHistoryStillHasToMatchCatalogEventTypes) {
  auto definition = pair_program();
  auto program = bind_program(definition, {"A", "B"}, {{1, "value", DataType::Int64, false}});
  const FsmReads reads{{"view", Row{std::string("a"), std::int64_t(120)}}};
  auto first = take(program->step(take(program->initial_state(0)), "A",
                                  {"A", "a1", 1, {std::int64_t(7)}}, reads));
  auto envelope = take(parse_json_exact(first.next_state));
  auto inner = take(parse_json_exact(envelope["state"].get<std::string>()));
  inner["events"][0]["values"][0] = Json::array({3, 7.0});
  envelope["state"] = inner.dump();
  EXPECT_FALSE(program->inspect_state(envelope.dump()).ok());
}
} // namespace
} // namespace eventflow::test
