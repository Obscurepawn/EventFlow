#include "eventflow/typed_trigger.hpp"
#include "status_test_support.hpp"
#include <gtest/gtest.h>

namespace eventflow {
struct TypedNews {
  std::string ticker;
  double sentiment = 0.0;
};

struct SentimentPair {
  double first = 0.0;
  double second = 0.0;
};

template <> struct SchemaTraits<TypedNews> {
  static auto fields() noexcept {
    return std::make_tuple(field<&TypedNews::ticker>("ticker"),
                           field<&TypedNews::sentiment>("sentiment"));
  }
};

template <> struct SchemaTraits<SentimentPair> {
  static auto fields() noexcept {
    return std::make_tuple(field<&SentimentPair::first>("first"),
                           field<&SentimentPair::second>("second"));
  }
};

namespace test {
namespace {
using namespace std::chrono_literals;

StatusOr<bool> positive(const TypedNews& news) noexcept {
  return news.sentiment > 0.0;
}

StatusOr<SentimentPair> make_pair(const TypedNews& first, const TypedNews& second) noexcept {
  return SentimentPair{.first = first.sentiment, .second = second.sentiment};
}

struct Fixture {
  CatalogSnapshot catalog;
  RelationId news = 0;
};

Fixture fixture() noexcept {
  auto catalog = take(Catalog::create());
  auto news = take(catalog->define_sql(
      "CREATE TABLE news(sentiment FLOAT64 NOT NULL,ticker STRING,PRIMARY KEY(ticker)) "
      "PARTITION BY(ticker)"));
  return {.catalog = take(catalog->snapshot()), .news = news};
}

TEST(TypedTrigger, TypedSourceValidatesNamedKeyAndSchemaBeforeOpeningTheOwner) {
  auto data = fixture();
  auto source = EventRef::source<TypedNews>({.kind = "news", .payload_relation = data.news});
  EXPECT_FALSE(source.bind(data.catalog).ok());
  EXPECT_TRUE(source.key_by(&TypedNews::ticker).bind(data.catalog).ok());
  EXPECT_FALSE(source.key_by(&TypedNews::sentiment).bind(data.catalog).ok());
  EXPECT_FALSE(source.key_by(&TypedNews::ticker, &TypedNews::ticker).bind(data.catalog).ok());
}

TEST(TypedTrigger, RuntimeNullGuardRejectsBindingAndKeepsTheBuilderError) {
  auto data = fixture();
  auto source = EventRef::source<TypedNews>({.kind = "news", .payload_relation = data.news})
                    .key_by(&TypedNews::ticker);
  using Guard = StatusOr<bool> (*)(const TypedNews&) noexcept;
  const Guard absent = nullptr;
  const auto start = Pattern::begin<TypedNews>("first");
  const auto invalid = start.where(absent, "missing_guard.v1");
  auto rejected = invalid.within(1s).bind(data.catalog, source);
  ASSERT_FALSE(rejected.ok());
  EXPECT_EQ(rejected.status().code(), ErrorCode::InvalidArgument);
  EXPECT_NE(rejected.status().message().find("null"), std::string_view::npos);
  auto still_rejected =
      invalid.where(&positive, "positive.v1").next("confirm").within(1s).bind(data.catalog, source);
  EXPECT_FALSE(still_rejected.ok());
  EXPECT_EQ(still_rejected.status().code(), ErrorCode::InvalidArgument);

  // A rejected derived declaration does not poison its immutable source.
  auto bound = take(start.where(&positive, "positive.v1").within(1s).bind(data.catalog, source));
  auto matcher = take(CepProgram::create({.id = "valid_pointer",
                                          .pattern = std::move(bound.pattern),
                                          .within_us = bound.within_us,
                                          .reads = {},
                                          .register_fields = {},
                                          .initial_registers = {},
                                          .limits = {}}));
  auto matched = take(matcher->step(
      take(matcher->initial_state()),
      {.id = "positive", .kind = "news", .time_us = 1, .values = {0.5, std::string("ticker")}}));
  EXPECT_EQ(matched.matches.size(), 1U);
}

TEST(TypedTrigger, TypedPatternAndProjectionKeepMatchedInputsAfterLaterNewsArrives) {
  auto data = fixture();
  auto source = EventRef::source<TypedNews>({.kind = "news", .payload_relation = data.news})
                    .key_by(&TypedNews::ticker);
  auto declaration = Pattern::begin<TypedNews>("first")
                         .where<&positive>("positive.v1")
                         .next("confirm")
                         .where<&positive>("positive.v1")
                         .within(2s);
  auto bound = take(declaration.bind(data.catalog, source));
  EXPECT_EQ(bound.input_kinds, (std::vector<std::string>{"news"}));
  auto projection = take(bind_match_projection<&make_pair>(data.catalog, bound.labels,
                                                           {"first", "confirm"}, "pair.v1"));
  auto matcher = take(CepProgram::create({.id = "pair",
                                          .pattern = std::move(bound.pattern),
                                          .within_us = bound.within_us,
                                          .reads = {},
                                          .register_fields = {},
                                          .initial_registers = {},
                                          .limits = {}}));
  auto state = take(matcher->initial_state());
  state =
      take(matcher->step(
               state,
               {.id = "A", .kind = "news", .time_us = 1, .values = {0.2, std::string("ticker")}}))
          .state;
  auto matched = take(matcher->step(
      state, {.id = "B", .kind = "news", .time_us = 2, .values = {0.6, std::string("ticker")}}));
  ASSERT_EQ(matched.matches.size(), 1U);
  TriggerOccurrence occurrence;
  occurrence.cep_match = matched.matches[0];
  occurrence.event_values = Row{99.0, std::string("later_C")};
  auto result = take(decode_typed<SentimentPair>(take(projection.project(occurrence))));
  EXPECT_DOUBLE_EQ(result.first, 0.2);
  EXPECT_DOUBLE_EQ(result.second, 0.6);
  EXPECT_FALSE(
      bind_match_projection<&make_pair>(data.catalog, bound.labels, {"first", "missing"}, "pair.v1")
          .ok());
  EXPECT_FALSE(
      bind_match_projection<&make_pair>(data.catalog, bound.labels, {"first"}, "pair.v1").ok());
}

StatusOr<SentimentPair> optional_pair(const std::optional<TypedNews>& first,
                                      const TypedNews& second) noexcept {
  return SentimentPair{.first = first ? first->sentiment : -1.0, .second = second.sentiment};
}

StatusOr<SentimentPair> repeated_pair(const std::vector<TypedNews>& items) noexcept {
  double total = 0;
  for (const auto& item : items)
    total += item.sentiment;
  return SentimentPair{.first = total, .second = static_cast<double>(items.size())};
}

StatusOr<SentimentPair> one_event(const TypedNews& event) noexcept {
  return SentimentPair{.first = event.sentiment, .second = event.sentiment};
}

StatusOr<SentimentPair> rejected_projection(const TypedNews&) noexcept {
  return Status(ErrorCode::FailedPrecondition, "Business projection rejected this match");
}

TriggerOccurrence occurrence(std::vector<CepCapture> captures) noexcept {
  TriggerOccurrence result;
  result.cep_match = CepMatch{};
  result.cep_match->events = std::move(captures);
  return result;
}

CepCapture capture(std::string label, double sentiment) noexcept {
  return {.label = std::move(label),
          .event = {.id = "event",
                    .kind = "news",
                    .time_us = 1,
                    .values = {sentiment, std::string("ticker")}}};
}

TEST(TypedTrigger, OptionalAndRepeatedLabelsRequireExplicitArgumentShapes) {
  auto data = fixture();
  auto source = EventRef::source<TypedNews>({.kind = "news", .payload_relation = data.news})
                    .key_by(&TypedNews::ticker);
  auto optional = take(Pattern::begin<TypedNews>("first")
                           .optional()
                           .then(Pattern::begin<TypedNews>("second"))
                           .within(1s)
                           .bind(data.catalog, source));
  auto projection = take(bind_match_projection<&optional_pair>(data.catalog, optional.labels,
                                                               {"first", "second"}, "optional.v1"));
  auto absent = take(
      decode_typed<SentimentPair>(take(projection.project(occurrence({capture("second", 0.5)})))));
  EXPECT_DOUBLE_EQ(absent.first, -1.0);
  EXPECT_DOUBLE_EQ(absent.second, 0.5);
  auto present = take(decode_typed<SentimentPair>(
      take(projection.project(occurrence({capture("first", 0.2), capture("second", 0.5)})))));
  EXPECT_DOUBLE_EQ(present.first, 0.2);
  EXPECT_FALSE(bind_match_projection<&make_pair>(data.catalog, optional.labels, {"first", "second"},
                                                 "bad.v1")
                   .ok());
  auto repeated =
      take(Pattern::begin<TypedNews>("items").repeat(2, 3).within(1s).bind(data.catalog, source));
  auto many = take(
      bind_match_projection<&repeated_pair>(data.catalog, repeated.labels, {"items"}, "many.v1"));
  auto result = take(decode_typed<SentimentPair>(
      take(many.project(occurrence({capture("items", 0.2), capture("items", 0.5)})))));
  EXPECT_DOUBLE_EQ(result.first, 0.7);
  EXPECT_DOUBLE_EQ(result.second, 2.0);
  EXPECT_FALSE(
      bind_match_projection<&one_event>(data.catalog, repeated.labels, {"items"}, "bad.v1").ok());
  EXPECT_FALSE(many.project(occurrence({capture("items", 0.1)})).ok());
  EXPECT_FALSE(many.project(occurrence({capture("items", 0.1), capture("items", 0.2),
                                        capture("items", 0.3), capture("items", 0.4)}))
                   .ok());
}

TEST(TypedTrigger, MalformedMatchesAndBusinessErrorsReturnStatus) {
  auto data = fixture();
  auto source = EventRef::source<TypedNews>({.kind = "news", .payload_relation = data.news})
                    .key_by(&TypedNews::ticker);
  auto bound = take(Pattern::begin<TypedNews>("first").within(1s).bind(data.catalog, source));
  auto project =
      take(bind_match_projection<&one_event>(data.catalog, bound.labels, {"first"}, "one.v1"));
  EXPECT_FALSE(project.project(TriggerOccurrence{}).ok());
  EXPECT_FALSE(project.project(occurrence({})).ok());
  auto wrong = capture("first", 1.0);
  wrong.event.kind = "quote";
  EXPECT_FALSE(project.project(occurrence({wrong})).ok());
  wrong = capture("first", 1.0);
  wrong.event.values = Row{std::string("wrong"), std::string("ticker")};
  EXPECT_FALSE(project.project(occurrence({wrong})).ok());
  auto rejected = take(bind_match_projection<&rejected_projection>(data.catalog, bound.labels,
                                                                   {"first"}, "reject.v1"));
  EXPECT_EQ(rejected.project(occurrence({capture("first", 1.0)})).status().code(),
            ErrorCode::FailedPrecondition);
  EXPECT_FALSE(bind_match_projection<&one_event>(data.catalog, bound.labels, {"first"}, "").ok());
}

TEST(TypedTrigger, ProjectionSignaturesIncludeExplicitIdentityAndParameterOrder) {
  auto data = fixture();
  auto source = EventRef::source<TypedNews>({.kind = "news", .payload_relation = data.news})
                    .key_by(&TypedNews::ticker);
  auto bound =
      take(Pattern::begin<TypedNews>("first").next("second").within(1s).bind(data.catalog, source));
  auto forward = take(bind_match_projection<&make_pair>(data.catalog, bound.labels,
                                                        {"first", "second"}, "pair.v1"));
  auto reverse = take(bind_match_projection<&make_pair>(data.catalog, bound.labels,
                                                        {"second", "first"}, "pair.v1"));
  auto revised = take(bind_match_projection<&make_pair>(data.catalog, bound.labels,
                                                        {"first", "second"}, "pair.v2"));
  EXPECT_NE(forward.signature, reverse.signature);
  EXPECT_NE(forward.signature, revised.signature);
}

TEST(TypedTrigger, TypedGuardPropagatesErrorsAndRejectsIncorrectPayloadBeforeCallingUserCode) {
  auto data = fixture();
  auto source = EventRef::source<TypedNews>({.kind = "news", .payload_relation = data.news})
                    .key_by(&TypedNews::ticker);
  int calls = 0;
  auto rule =
      Pattern::begin<TypedNews>("first")
          .where(
              [&calls](const TypedNews&) noexcept -> StatusOr<bool> {
                ++calls;
                return Status(ErrorCode::FailedPrecondition, "Business condition rejected input");
              },
              "rejected.v1")
          .within(1s);
  auto bound = take(rule.bind(data.catalog, source));
  auto matcher = take(CepProgram::create({.id = "errors",
                                          .pattern = std::move(bound.pattern),
                                          .within_us = bound.within_us,
                                          .reads = {},
                                          .register_fields = {},
                                          .initial_registers = {},
                                          .limits = {}}));
  auto state = take(matcher->initial_state());
  EXPECT_FALSE(matcher
                   ->step(state, {.id = "bad",
                                  .kind = "news",
                                  .time_us = 1,
                                  .values = {std::string("bad"), std::string("ticker")}})
                   .ok());
  EXPECT_EQ(calls, 0);
  auto result = matcher->step(
      state,
      {.id = "business", .kind = "news", .time_us = 1, .values = {0.1, std::string("ticker")}});
  EXPECT_EQ(result.status().code(), ErrorCode::FailedPrecondition);
  EXPECT_EQ(calls, 1);
}

StatusOr<SentimentPair> wrong_input(const SentimentPair& input) noexcept {
  return SentimentPair{.first = input.first, .second = input.second};
}

TEST(TypedTrigger, SourceNamesAndProjectionInputSchemasAreValidatedAtBinding) {
  auto data = fixture();
  auto source = EventRef::source<TypedNews>({.kind = "news", .payload_relation = data.news})
                    .key_by(&TypedNews::ticker);
  auto bound = take(Pattern::begin<TypedNews>("first").within(1s).bind(data.catalog, source));
  EXPECT_FALSE(
      bind_match_projection<&wrong_input>(data.catalog, bound.labels, {"first"}, "wrong.v1").ok());
  for (const auto& kind :
       {std::string{}, std::string("*"), std::string("$timeout"), std::string("$expire"),
        std::string(129, 'n'), std::string("bad\0kind", 8)}) {
    EXPECT_FALSE(EventRef::source<TypedNews>({.kind = kind, .payload_relation = data.news})
                     .key_by(&TypedNews::ticker)
                     .bind(data.catalog)
                     .ok());
  }
}

TEST(TypedTrigger, CapturelessStaticProjectionUsesTheSameCheckedAdapter) {
  auto data = fixture();
  auto source = EventRef::source<TypedNews>({.kind = "news", .payload_relation = data.news})
                    .key_by(&TypedNews::ticker);
  auto bound = take(Pattern::begin<TypedNews>("first").within(1s).bind(data.catalog, source));
  constexpr auto mapper = [](const TypedNews& news) noexcept -> StatusOr<SentimentPair> {
    return SentimentPair{.first = news.sentiment, .second = news.sentiment * 2.0};
  };
  auto projected =
      take(bind_match_projection<mapper>(data.catalog, bound.labels, {"first"}, "lambda.v1"));
  auto result = take(
      decode_typed<SentimentPair>(take(projected.project(occurrence({capture("first", 0.25)})))));
  EXPECT_DOUBLE_EQ(result.first, 0.25);
  EXPECT_DOUBLE_EQ(result.second, 0.5);
}
} // namespace
} // namespace test
} // namespace eventflow
