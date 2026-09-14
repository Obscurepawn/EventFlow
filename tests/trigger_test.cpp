#include "eventflow/trigger.hpp"
#include <gtest/gtest.h>

namespace eventflow {
namespace trigger_test {

struct Observation {
  std::string ticker;
  double score = 0;
};

struct Features {
  double feature = 0;
};

struct Prediction {
  double result = 0;
};

} // namespace trigger_test

template <> struct SchemaTraits<trigger_test::Observation> {
  static auto fields() noexcept {
    using T = trigger_test::Observation;
    return std::tuple{field<&T::ticker>("ticker"), field<&T::score>("score")};
  }
};

template <> struct SchemaTraits<trigger_test::Features> {
  static auto fields() noexcept {
    return std::tuple{field<&trigger_test::Features::feature>("feature")};
  }
};

template <> struct SchemaTraits<trigger_test::Prediction> {
  static auto fields() noexcept {
    return std::tuple{field<&trigger_test::Prediction::result>("result")};
  }
};

namespace trigger_test {
namespace {
using namespace std::chrono_literals;

StatusOr<Features> current_features(const Observation& observation) noexcept {
  return Features{.feature = observation.score};
}

StatusOr<Features> pair_features(const Observation& first, const Observation& confirm) noexcept {
  return Features{.feature = (first.score + confirm.score) * 0.5};
}

StatusOr<Prediction> predict(const Features& features) noexcept {
  return Prediction{.result = features.feature * 2.0};
}

class TriggerTest : public testing::Test {
protected:
  void SetUp() override {
    auto created = Catalog::create();
    ASSERT_TRUE(created.ok());
    catalog = std::move(*created);
    auto defined = catalog->define_sql("CREATE TABLE news(ticker STRING,score FLOAT64 NOT "
                                       "NULL,PRIMARY KEY(ticker)) PARTITION BY(ticker)");
    ASSERT_TRUE(defined.ok()) << defined.status().message();
    news = *defined;
    auto view = catalog->define_sql(
        "CREATE MATERIALIZED VIEW ready AS SELECT ticker,score>0.0 AS ready FROM news");
    ASSERT_TRUE(view.ok());
    ready = *view;
    auto state = catalog->snapshot();
    ASSERT_TRUE(state.ok());
    snapshot = std::move(*state);
  }

  EventRef source() const noexcept {
    return EventRef::source({.kind = "news", .payload_relation = news});
  }

  Trigger confirmation(NextPolicy policy) const noexcept {
    return Trigger::on_event("confirmation", source())
        .when(source().column<double>("score") > 0.0)
        .then_next(source(), policy)
        .when(source().column<double>("score") > 0.0)
        .within(2s)
        .rearm_on_next_event()
        .emit("confirmed");
  }

  StatusOr<std::unique_ptr<FsmProgram>> program(const Trigger& trigger) const noexcept {
    EF_ASSIGN_OR_RETURN(auto bound, bind_trigger(snapshot, trigger));
    return FsmProgram::create(bound.rule->definition);
  }

  FsmEvent input(std::int64_t time, double score = 1.0, std::string kind = "news") const noexcept {
    return {.kind = std::move(kind),
            .occurrence_id = std::to_string(time),
            .time_us = time,
            .values = {std::string("ABC"), score}};
  }

  std::unique_ptr<Catalog> catalog;
  CatalogSnapshot snapshot;
  RelationId news = 0;
  RelationId ready = 0;
};

TEST_F(TriggerTest, EventConditionEmitsRepeatedlyAndPreservesTheOriginalBuilder) {
  const auto original = Trigger::on_event("sample_news", source());
  const auto positive = original.when(source().column<double>("score") > 0.0).emit("positive");
  auto condition = program(positive);
  ASSERT_TRUE(condition.ok()) << condition.status().message();
  auto state = (*condition)->initial_state();
  ASSERT_TRUE(state.ok());
  auto rejected = (*condition)->step(*state, input(1, -1.0));
  ASSERT_TRUE(rejected.ok());
  EXPECT_TRUE(rejected->emissions.empty());
  auto accepted = (*condition)->step(rejected->state, input(2));
  ASSERT_TRUE(accepted.ok());
  ASSERT_EQ(accepted->emissions.size(), 1U);
  EXPECT_EQ(accepted->emissions[0].name, "positive");
  auto again = (*condition)->step(accepted->state, input(3));
  ASSERT_TRUE(again.ok());
  EXPECT_EQ(again->emissions.size(), 1U);
  auto plain = program(original);
  ASSERT_TRUE(plain.ok());
  auto plain_state = (*plain)->initial_state();
  ASSERT_TRUE(plain_state.ok());
  auto negative = (*plain)->step(*plain_state, input(1, -1.0));
  ASSERT_TRUE(negative.ok());
  EXPECT_EQ(negative->emissions.size(), 1U);
}

TEST_F(TriggerTest, OfKindIgnoresQuoteWhileStrictRejectsIt) {
  for (const auto policy : {NextPolicy::OfKind, NextPolicy::Strict}) {
    auto matcher = program(confirmation(policy));
    ASSERT_TRUE(matcher.ok()) << matcher.status().message();
    auto state = (*matcher)->initial_state();
    ASSERT_TRUE(state.ok());
    auto armed = (*matcher)->step(*state, input(1));
    ASSERT_TRUE(armed.ok());
    EXPECT_EQ(armed->state.state, "armed");
    auto quote = (*matcher)->step(armed->state, input(2, 1.0, "quote"));
    ASSERT_TRUE(quote.ok());
    auto news_input = (*matcher)->step(quote->state, input(3));
    ASSERT_TRUE(news_input.ok());
    EXPECT_EQ(news_input->emissions.size(), policy == NextPolicy::OfKind ? 1U : 0U);
    EXPECT_EQ(news_input->state.state, policy == NextPolicy::OfKind ? "idle" : "armed");
  }
}

TEST_F(TriggerTest, FailedConfirmationCannotAlsoBecomeANewStart) {
  auto matcher = program(confirmation(NextPolicy::OfKind));
  ASSERT_TRUE(matcher.ok());
  auto state = (*matcher)->initial_state();
  ASSERT_TRUE(state.ok());
  auto armed = (*matcher)->step(*state, input(1));
  ASSERT_TRUE(armed.ok());
  auto negative = (*matcher)->step(armed->state, input(2, -1.0));
  ASSERT_TRUE(negative.ok());
  EXPECT_EQ(negative->state.state, "idle");
  auto restarted = (*matcher)->step(negative->state, input(3));
  ASSERT_TRUE(restarted.ok());
  EXPECT_TRUE(restarted->emissions.empty());
  EXPECT_EQ(restarted->state.state, "armed");
  auto confirmed = (*matcher)->step(restarted->state, input(4));
  ASSERT_TRUE(confirmed.ok());
  EXPECT_EQ(confirmed->emissions.size(), 1U);
}

TEST_F(TriggerTest, ExactDeadlineExpiresBeforeTheNextDataTransition) {
  auto matcher = program(confirmation(NextPolicy::OfKind));
  ASSERT_TRUE(matcher.ok());
  auto state = (*matcher)->initial_state();
  ASSERT_TRUE(state.ok());
  auto armed = (*matcher)->step(*state, input(100));
  ASSERT_TRUE(armed.ok());
  auto boundary = (*matcher)->step(armed->state, input(2'000'100));
  ASSERT_TRUE(boundary.ok());
  EXPECT_TRUE(boundary->emissions.empty());
  EXPECT_EQ(boundary->state.state, "armed");
  EXPECT_EQ(boundary->state.entered_at_us, 2'000'100);
  auto expired = (*matcher)->advance(boundary->state, 4'000'100);
  ASSERT_TRUE(expired.ok());
  EXPECT_EQ(expired->state.state, "idle");
}

TEST_F(TriggerTest, GuardFailureDoesNotMutateTheCallerState) {
  auto declaration = Trigger::on_event("guarded", source())
                         .when(Predicate::guard(
                             {},
                             [](const FsmContext&) noexcept -> StatusOr<bool> {
                               return Status(ErrorCode::Unavailable, "Expected predicate failure");
                             },
                             "failure.v1"));
  auto matcher = program(declaration);
  ASSERT_TRUE(matcher.ok());
  auto state = (*matcher)->initial_state();
  ASSERT_TRUE(state.ok());
  auto result = (*matcher)->step(*state, input(1));
  EXPECT_EQ(result.status().code(), ErrorCode::Unavailable);
  EXPECT_EQ(state->transition_sequence, 0U);
  EXPECT_EQ(state->time_us, 0);
  auto malformed = input(2);
  malformed.values.pop_back();
  EXPECT_EQ((*matcher)->step(*state, malformed).status().code(), ErrorCode::InvalidArgument);
}

TEST_F(TriggerTest, RawWildcardSamplesDeclaredViewsOnSourcesAndClocks) {
  auto declaration = Trigger::on_event("sample_all_inputs", std::string("*"))
                         .when(Predicate::guard(
                             {{.alias = "ready", .relation = ready}},
                             [](const FsmContext& context) noexcept -> StatusOr<bool> {
                               const auto found = context.reads.find("ready");
                               if (found == context.reads.end() || !found->second)
                                 return false;
                               const auto* ready_now = std::get_if<bool>(&(*found->second)[1]);
                               return ready_now && *ready_now && context.event.time_us >= 1;
                             },
                             "sample.current_ready.v1"));
  auto matcher = program(declaration);
  ASSERT_TRUE(matcher.ok()) << matcher.status().message();
  auto state = (*matcher)->initial_state();
  ASSERT_TRUE(state.ok());
  const FsmReads reads{{"ready", Row{std::string("ABC"), true}}};
  auto source_step = (*matcher)->step(*state, input(1), reads);
  ASSERT_TRUE(source_step.ok());
  EXPECT_EQ(source_step->emissions.size(), 1U);
  auto clock_step = (*matcher)->step(source_step->state, input(2, 1.0, "clock"), reads);
  ASSERT_TRUE(clock_step.ok());
  EXPECT_EQ(clock_step->emissions.size(), 1U);
  auto expiry = (*matcher)->step(clock_step->state, input(3, 1.0, "expiry"), reads);
  ASSERT_TRUE(expiry.ok());
  EXPECT_EQ(expiry->emissions.size(), 1U);
  EXPECT_FALSE(
      bind_trigger(snapshot,
                   Trigger::on_event("typed_wildcard",
                                     EventRef::source({.kind = "*", .payload_relation = news})))
          .ok());
}

TEST_F(TriggerTest, InvalidWaitAndRequestDeclarationsFailWithoutRepairingEarlierErrors) {
  const auto first = Trigger::on_event("confirmation", source());
  EXPECT_FALSE(bind_trigger(snapshot, first.then_next(source(), NextPolicy::OfKind)).ok());
  EXPECT_FALSE(
      bind_trigger(snapshot, first.then_next(source(), NextPolicy::OfKind).within(1s)).ok());
  EXPECT_FALSE(bind_trigger(snapshot, first.within(1s)).ok());
  EXPECT_FALSE(bind_trigger(snapshot, first.version(0).version(1)).ok());
  EXPECT_FALSE(bind_trigger(snapshot, first.then_next(source(), NextPolicy::OfKind)
                                          .then_next(source(), NextPolicy::OfKind)
                                          .within(1s)
                                          .rearm_on_next_event())
                   .ok());
  EXPECT_FALSE(
      bind_trigger(snapshot, first.request({.id = "request", .plan = "plan", .pending_for = 1s}))
          .ok());
  EXPECT_FALSE(bind_trigger(snapshot, first.request({.id = "request",
                                                     .plan = "plan",
                                                     .mode = RequestTriggerMode::Wait,
                                                     .pending_for = 0s}))
                   .ok());
  EXPECT_TRUE(bind_trigger(snapshot, first.request({.id = "request",
                                                    .plan = "plan",
                                                    .mode = RequestTriggerMode::Wait,
                                                    .pending_for = 1s}))
                  .ok());
}

TEST_F(TriggerTest, ClockUsesStableKindsAndCheckedMicrosecondDurations) {
  auto clock = Trigger::clock(
      "sample", {.policy_id = "five_seconds", .every = 5s, .phase = 1s, .event_kind = {}});
  auto bound = bind_trigger(snapshot, clock);
  ASSERT_TRUE(bound.ok()) << bound.status().message();
  ASSERT_TRUE(bound->clock);
  EXPECT_EQ(bound->clock->event_kind, "__trigger_clock_sample");
  EXPECT_EQ(bound->clock->interval_us, 5'000'000);
  EXPECT_EQ(bound->clock->phase_us, 1'000'000);
  auto matcher = FsmProgram::create(bound->rule->definition);
  ASSERT_TRUE(matcher.ok());
  auto state = (*matcher)->initial_state();
  ASSERT_TRUE(state.ok());
  auto fired = (*matcher)->step(*state, {.kind = bound->clock->event_kind,
                                         .occurrence_id = "slot-1",
                                         .time_us = 6'000'000,
                                         .values = {}});
  ASSERT_TRUE(fired.ok());
  EXPECT_EQ(fired->emissions.size(), 1U);
  EXPECT_FALSE(
      bind_trigger(snapshot,
                   Trigger::clock(
                       "bad", {.policy_id = "policy", .every = 1ns, .phase = 0s, .event_kind = {}}))
          .ok());
  EXPECT_FALSE(
      bind_trigger(snapshot,
                   Trigger::clock(
                       "bad", {.policy_id = "policy", .every = 1s, .phase = -1s, .event_kind = {}}))
          .ok());
  EXPECT_FALSE(
      bind_trigger(snapshot,
                   Trigger::clock(
                       "bad", {.policy_id = "policy", .every = 1s, .phase = 0s, .event_kind = "*"}))
          .ok());
}

TEST_F(TriggerTest, PatternUsesTheBoundedEnfaAndFixedMatchEmission) {
  const auto pair =
      Pattern::begin("first", source()).followed_by_any("confirm", source()).within(2s);
  auto bound = bind_trigger(snapshot, Trigger::pattern("pair", pair));
  ASSERT_TRUE(bound.ok()) << bound.status().message();
  ASSERT_TRUE(bound->pattern);
  EXPECT_EQ(bound->pattern->definition.limits.max_paths, 32U);
  auto matcher = CepProgram::create(bound->pattern->definition);
  ASSERT_TRUE(matcher.ok());
  auto state = (*matcher)->initial_state();
  ASSERT_TRUE(state.ok());
  auto first = (*matcher)->step(
      *state, {.id = "a", .kind = "news", .time_us = 1, .values = {std::string("ABC"), 0.5}});
  ASSERT_TRUE(first.ok());
  auto second = (*matcher)->step(
      first->state, {.id = "b", .kind = "news", .time_us = 2, .values = {std::string("ABC"), 0.8}});
  ASSERT_TRUE(second.ok()) << second.status().message();
  EXPECT_EQ(second->matches.size(), 1U);
  EXPECT_FALSE(bind_trigger(snapshot, Trigger::pattern("pair", pair).emit("other")).ok());
  EXPECT_FALSE(bind_trigger(snapshot, Trigger::pattern("pair", pair).when(Predicate{})).ok());
  auto limits = CepLimits{};
  limits.max_paths = 0;
  EXPECT_FALSE(bind_trigger(snapshot, Trigger::pattern("pair", pair).limits(limits)).ok());
}

TEST_F(TriggerTest, PrivateDefinitionChangesWithConditionsWindowsAndFunctionIdentity) {
  auto first = bind_trigger(snapshot, confirmation(NextPolicy::OfKind));
  auto changed = bind_trigger(snapshot, confirmation(NextPolicy::OfKind).within(3s));
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(changed.ok());
  EXPECT_NE(first->rule->definition.definition_identity,
            changed->rule->definition.definition_identity);
  EXPECT_TRUE(first->rule->definition.register_fields.empty());
  EXPECT_TRUE(first->rule->definition.initial_registers.empty());
  auto condition = bind_trigger(
      snapshot,
      Trigger::on_event("condition", source()).when(source().column<double>("score") > 0.0));
  auto condition2 = bind_trigger(
      snapshot,
      Trigger::on_event("condition", source()).when(source().column<double>("score") > 1.0));
  ASSERT_TRUE(condition.ok());
  ASSERT_TRUE(condition2.ok());
  EXPECT_NE(condition->rule->definition.definition_identity,
            condition2->rule->definition.definition_identity);
}

TEST_F(TriggerTest, CurrentCaptureBindsOneViewAndStaticFunctionWithoutUnrelatedCatalogMetadata) {
  const auto make = [&](std::string identity) noexcept {
    return Trigger::on_event("predict_current", source())
        .capture_current<&current_features>(news)
        .request(Function<&predict>{.id = "prediction",
                                    .version = 1,
                                    .implementation_identity = std::move(identity)},
                 {.mode = RequestTriggerMode::Wait, .pending_for = 1s, .lifetime = 10s});
  };
  auto declaration = make("prediction.v1");
  auto bound = bind_trigger(snapshot, declaration);
  ASSERT_TRUE(bound.ok()) << bound.status().message();
  ASSERT_TRUE(bound->capture);
  ASSERT_TRUE(bound->function);
  EXPECT_EQ(bound->capture->reads, std::vector<RelationId>{news});
  auto result = bound->function->invoke({0.8});
  ASSERT_TRUE(result.ok());
  EXPECT_DOUBLE_EQ(*std::get_if<double>(&(*result)[0]), 1.6);
  auto another =
      catalog->define_sql("CREATE TABLE unrelated(k STRING,PRIMARY KEY(k)) PARTITION BY(k)");
  ASSERT_TRUE(another.ok());
  auto next = catalog->snapshot();
  ASSERT_TRUE(next.ok());
  auto same = bind_trigger(*next, declaration);
  ASSERT_TRUE(same.ok());
  EXPECT_EQ(bound->rule->definition.definition_identity,
            same->rule->definition.definition_identity);
  auto changed = bind_trigger(snapshot, make("prediction.v2"));
  ASSERT_TRUE(changed.ok());
  EXPECT_NE(bound->rule->definition.definition_identity,
            changed->rule->definition.definition_identity);
  EXPECT_FALSE(bind_trigger(snapshot, declaration.request({.id = "legacy",
                                                           .plan = "legacy",
                                                           .mode = RequestTriggerMode::Wait,
                                                           .pending_for = 1s}))
                   .ok());
}

TEST_F(TriggerTest, MatchCaptureFeedsTheStaticFunctionAndRejectsOrdinaryEventTriggers) {
  const Function<&predict> function{
      .id = "prediction", .version = 1, .implementation_identity = "prediction.v1"};
  const auto pair = Pattern::begin("first", source()).next_of_kind("confirm", source()).within(2s);
  auto declaration =
      Trigger::pattern("pair_prediction", pair)
          .capture_match<&pair_features>({"first", "confirm"})
          .request(function,
                   {.mode = RequestTriggerMode::Wait, .pending_for = 1s, .lifetime = 10s});
  auto bound = bind_trigger(snapshot, declaration);
  ASSERT_TRUE(bound.ok()) << bound.status().message();
  ASSERT_TRUE(bound->capture);
  EXPECT_EQ(bound->capture->source, CaptureSource::Match);
  auto matcher = CepProgram::create(bound->pattern->definition);
  ASSERT_TRUE(matcher.ok());
  auto state = (*matcher)->initial_state();
  ASSERT_TRUE(state.ok());
  auto first = (*matcher)->step(
      *state, {.id = "first", .kind = "news", .time_us = 1, .values = {std::string("ABC"), 0.4}});
  ASSERT_TRUE(first.ok());
  auto second = (*matcher)->step(
      first->state,
      {.id = "confirm", .kind = "news", .time_us = 2, .values = {std::string("ABC"), 0.8}});
  ASSERT_TRUE(second.ok()) << second.status().message();
  ASSERT_EQ(second->matches.size(), 1U);
  TriggerOccurrence occurrence;
  occurrence.cep_match = second->matches[0];
  auto captured = bound->capture->match(occurrence);
  ASSERT_TRUE(captured.ok()) << captured.status().message();
  auto result = bound->function->invoke(*captured);
  ASSERT_TRUE(result.ok());
  EXPECT_DOUBLE_EQ(*std::get_if<double>(&(*result)[0]), 1.2);
  auto invalid = Trigger::on_event("wrong_capture", source())
                     .capture_match<&pair_features>({"first", "confirm"})
                     .request(function, {.mode = RequestTriggerMode::Wait, .pending_for = 1s});
  EXPECT_FALSE(bind_trigger(snapshot, invalid).ok());
}

TEST_F(TriggerTest, SessionMustReferenceACandidateRelation) {
  EXPECT_FALSE(bind_trigger(snapshot, Trigger::on_event("wrong_session", source())
                                          .in_session(SessionRef::candidate(news)))
                   .ok());
  auto candidate = catalog->define_candidate(
      {.name = "news_session",
       .scope_fields = {{.id = 1, .name = "ticker", .type = DataType::String}},
       .payload_fields = {}});
  ASSERT_TRUE(candidate.ok());
  auto next = catalog->snapshot();
  ASSERT_TRUE(next.ok());
  EXPECT_TRUE(bind_trigger(*next, Trigger::on_event("session", source())
                                      .in_session(SessionRef::candidate(*candidate)))
                  .ok());
}

TEST_F(TriggerTest, DefinitionMetadataDoesNotConsumePatternRegisters) {
  auto limits = CepLimits{};
  limits.max_register_bytes = 2;
  const auto pair = Pattern::begin("first", source()).next_of_kind("confirm", source()).within(2s);
  auto bound = bind_trigger(snapshot, Trigger::pattern("small_registers", pair).limits(limits));
  ASSERT_TRUE(bound.ok()) << bound.status().message();
  EXPECT_TRUE(bound->pattern->definition.initial_registers.empty());
  auto matcher = CepProgram::create(bound->pattern->definition);
  ASSERT_TRUE(matcher.ok());
  auto state = (*matcher)->initial_state();
  ASSERT_TRUE(state.ok());
  auto first = (*matcher)->step(
      *state, {.id = "first", .kind = "news", .time_us = 1, .values = {std::string("ABC"), 0.4}});
  ASSERT_TRUE(first.ok()) << first.status().message();
  auto second = (*matcher)->step(
      first->state,
      {.id = "confirm", .kind = "news", .time_us = 2, .values = {std::string("ABC"), 0.8}});
  ASSERT_TRUE(second.ok()) << second.status().message();
  ASSERT_EQ(second->matches.size(), 1U);
  EXPECT_TRUE(second->matches[0].registers.empty());
}

TEST_F(TriggerTest, ChangedDefinitionIdentityRejectsFsmAndCepRestoration) {
  auto plain = on_event("identity", "news");
  auto legacy = FsmProgram::create(plain);
  ASSERT_TRUE(legacy.ok());
  auto initial = (*legacy)->initial_state();
  ASSERT_TRUE(initial.ok());
  auto legacy_bytes = (*legacy)->encode_state(*initial);
  ASSERT_TRUE(legacy_bytes.ok());
  EXPECT_EQ(legacy_bytes->find("definition_identity"), std::string::npos);
  EXPECT_EQ(
      *legacy_bytes,
      R"({"deadline_us":null,"entered_at_us":0,"format":1,"program_id":"identity","program_version":1,"registers":[],"state":"waiting","time_us":0,"transition_sequence":0})");
  plain.definition_identity = "identity-v1";
  auto original = FsmProgram::create(plain);
  ASSERT_TRUE(original.ok());
  auto bytes = (*original)->encode_state(*initial);
  ASSERT_TRUE(bytes.ok());
  EXPECT_NE(bytes->find("definition_identity"), std::string::npos);
  EXPECT_TRUE((*original)->decode_state(*bytes).ok());
  EXPECT_FALSE((*legacy)->decode_state(*bytes).ok());
  EXPECT_FALSE((*original)->decode_state(*legacy_bytes).ok());
  plain.definition_identity = "identity-v2";
  auto changed = FsmProgram::create(plain);
  ASSERT_TRUE(changed.ok());
  EXPECT_FALSE((*changed)->decode_state(*bytes).ok());

  CepDefinition cep;
  cep.id = "identity";
  cep.pattern = CepPattern::event("news", "news");
  cep.within_us = 1'000'000;
  auto legacy_cep = CepProgram::create(cep);
  ASSERT_TRUE(legacy_cep.ok());
  EXPECT_EQ((*legacy_cep)->fingerprint().find("definition_identity"), std::string::npos);
  EXPECT_EQ(
      (*legacy_cep)->fingerprint(),
      R"({"fields":[],"format":1,"id":"identity","limits":[512,1024,4096,2048,1024,256,1024,64,65536,65536,16384,1048576,2097152,8388608],"pattern":["event","news","news",false,false],"reads":[],"registers":[],"skip":0,"version":1,"within":1000000})");
  cep.definition_identity = "identity-v1";
  auto first_cep = CepProgram::create(cep);
  ASSERT_TRUE(first_cep.ok());
  auto cep_state = (*first_cep)->initial_state();
  ASSERT_TRUE(cep_state.ok());
  auto cep_bytes = (*first_cep)->encode_state(*cep_state);
  ASSERT_TRUE(cep_bytes.ok());
  EXPECT_TRUE((*first_cep)->decode_state(*cep_bytes).ok());
  EXPECT_FALSE((*legacy_cep)->decode_state(*cep_bytes).ok());
  cep.definition_identity = "identity-v2";
  auto second_cep = CepProgram::create(cep);
  ASSERT_TRUE(second_cep.ok());
  EXPECT_NE((*first_cep)->fingerprint(), (*second_cep)->fingerprint());
  EXPECT_FALSE((*second_cep)->decode_state(*cep_bytes).ok());
}

} // namespace
} // namespace trigger_test
} // namespace eventflow
