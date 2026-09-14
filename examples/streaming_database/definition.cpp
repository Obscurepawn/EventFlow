#include "definition.hpp"
#include <chrono>
#include <cmath>
#include <limits>

namespace eventflow::streaming_example {

struct CurrentNewsFeatures {
  std::int64_t news_count = 0;
  std::optional<double> average_sentiment;
  std::optional<double> sentiment_price_sum;
};

struct NewsFeatureSnapshot {
  std::int64_t news_count = 0;
  double average_sentiment = 0.0;
  double sentiment_price_sum = 0.0;
};

struct NewsImpact {
  double impact_score = 0.0;
};

} // namespace eventflow::streaming_example

namespace eventflow {

template <> struct SchemaTraits<streaming_example::CurrentNewsFeatures> {
  static auto fields() noexcept {
    using streaming_example::CurrentNewsFeatures;
    return std::make_tuple(field<&CurrentNewsFeatures::news_count>("news_count"),
                           field<&CurrentNewsFeatures::average_sentiment>("average_sentiment"),
                           field<&CurrentNewsFeatures::sentiment_price_sum>("sentiment_price_sum"));
  }
};

template <> struct SchemaTraits<streaming_example::NewsFeatureSnapshot> {
  static auto fields() noexcept {
    using streaming_example::NewsFeatureSnapshot;
    return std::make_tuple(field<&NewsFeatureSnapshot::news_count>("news_count"),
                           field<&NewsFeatureSnapshot::average_sentiment>("average_sentiment"),
                           field<&NewsFeatureSnapshot::sentiment_price_sum>("sentiment_price_sum"));
  }
};

template <> struct SchemaTraits<streaming_example::NewsImpact> {
  static auto fields() noexcept {
    return std::make_tuple(field<&streaming_example::NewsImpact::impact_score>("impact_score"));
  }
};

} // namespace eventflow

namespace eventflow::streaming_example {
namespace {

StatusOr<NewsFeatureSnapshot> capture_news_features(const CurrentNewsFeatures& current) noexcept {
  // SQL aggregate-derived columns are nullable; a usable news group needs values.
  if (current.news_count <= 0 || !current.average_sentiment || !current.sentiment_price_sum)
    return Status(ErrorCode::FailedPrecondition, "news aggregate has no usable features");
  return NewsFeatureSnapshot{.news_count = current.news_count,
                             .average_sentiment = *current.average_sentiment,
                             .sentiment_price_sum = *current.sentiment_price_sum};
}

StatusOr<NewsImpact> calculate_news_impact(const NewsFeatureSnapshot& input) noexcept {
  // Use each article's pre-assigned sentiment and contemporaneous market price.
  // This deterministic indicator demonstrates static computation, not text inference.
  if (input.news_count <= 0)
    return Status(ErrorCode::InvalidArgument, "news impact requires a positive news count");
  const double impact = input.sentiment_price_sum / static_cast<double>(input.news_count);
  if (!std::isfinite(impact))
    return Status(ErrorCode::InvalidArgument, "news impact is not finite");
  return NewsImpact{.impact_score = impact};
}

StatusOr<BoundComputation> bind_definition(const std::vector<std::string>& staged_ddl) noexcept {
  using namespace std::chrono_literals;
  EF_ASSIGN_OR_RETURN(auto catalog, Catalog::create());
  // This example identifies an article by (ticker, timestamp_us); repeated keys
  // update that article. The payload keeps full text and microsecond news time.
  EF_ASSIGN_OR_RETURN(auto news, catalog->define_sql(R"sql(
    CREATE TABLE news_updates (
      ticker STRING NOT NULL,
      timestamp_us INT64 NOT NULL,
      headline STRING NOT NULL,
      text_body STRING NOT NULL,
      sentiment_score FLOAT64 NOT NULL,
      market_price FLOAT64 NOT NULL,
      PRIMARY KEY (ticker, timestamp_us)
    ) PARTITION BY (ticker)
  )sql"));
  // First MV: aggregate sentiment and retain the sentiment/price pairing per article.
  EF_ASSIGN_OR_RETURN(auto stats, catalog->define_sql(R"sql(
    CREATE MATERIALIZED VIEW news_stats AS
      SELECT ticker, COUNT(*) AS news_count, SUM(sentiment_score) AS total_sentiment,
             SUM(sentiment_score * market_price) AS sentiment_price_sum
      FROM news_updates GROUP BY ticker
  )sql"));
  (void)stats;
  // Second MV: derive mean sentiment from the current aggregate. COUNT is Int64,
  // so division explicitly casts it to Float64 instead of relying on coercion.
  EF_ASSIGN_OR_RETURN(auto features, catalog->define_sql(R"sql(
    CREATE MATERIALIZED VIEW news_features AS
      SELECT ticker, news_count,
             total_sentiment / CAST(news_count AS FLOAT64) AS average_sentiment,
             sentiment_price_sum
      FROM news_stats
  )sql"));
  for (const auto& statement : staged_ddl) {
    EF_ASSIGN_OR_RETURN(auto relation, catalog->define_sql(statement));
    (void)relation;
  }
  EF_ASSIGN_OR_RETURN(
      auto session,
      catalog->define_candidate(
          {.name = "news_session",
           .scope_fields =
               {{.id = 1, .name = "ticker", .type = DataType::String, .nullable = false}},
           .payload_fields = {
               {.id = 2, .name = "deadline_us", .type = DataType::Int64, .nullable = false}}}));

  ProcessorConfig definition;
  definition.input_relations = {news};
  definition.requests.emplace();
  // The first news update opens one session per ticker. This finite example deliberately
  // keeps it active until the Int64 deadline; a real application defines session rollover.
  definition.requests->candidate = CandidateExpiryPolicy{
      .relation = session,
      .deadline_column = "deadline_us",
      .version = 1,
      .on_source = [session](RoundContext& round, const ProcessorEvent& event) noexcept
          -> StatusOr<std::optional<CandidateActivation>> {
        EF_ASSIGN_OR_RETURN(auto existing, round.read_relation(session, event.scope));
        if (existing)
          return std::optional<CandidateActivation>{};
        return std::optional<CandidateActivation>(CandidateActivation{
            .expected_epoch = 0, .payload = {std::numeric_limits<std::int64_t>::max()}});
      }};

  // At least two articles and mean sentiment >= 0.5 admit one request per session.
  // Later news cannot replace the input already captured by that request.
  const auto threshold =
      Trigger::on_event("sentiment_threshold",
                        EventRef::source({.kind = "news_updates", .payload_relation = news}))
          .implementation_identity("streaming.sentiment_threshold.v1")
          .when((ViewRef(features).column<std::int64_t>("news_count") >= std::int64_t{2}) &&
                (ViewRef(features).column<double>("average_sentiment") >= 0.5))
          .in_session(SessionRef::candidate(session))
          .capture_current<&capture_news_features>(features, "streaming.capture_news_features.v1")
          .request(Function<&calculate_news_impact>{.id = "news_impact",
                                                    .version = 1,
                                                    .implementation_identity =
                                                        "streaming.news_impact.v1"},
                   {.id = "threshold_impact_request",
                    .mode = RequestTriggerMode::Wait,
                    .pending_for = 1s,
                    .lifetime = 10s,
                    .once_per_candidate = true});

  // Database time drives one clock per observed ticker. Each ready 200 ms slot
  // captures the current cascaded MV and produces a separate news impact request.
  // Overdue slots may be coalesced; this declaration does not promise a wall-time SLA.
  const auto periodic =
      Trigger::clock("periodic_impact", {.policy_id = "news_impact_clock",
                                         .every = 200ms,
                                         .phase = 0ms,
                                         .event_kind = "news_clock"})
          .implementation_identity("streaming.periodic_impact.v1")
          .in_session(SessionRef::candidate(session))
          .capture_current<&capture_news_features>(features, "streaming.capture_news_features.v1")
          .request(Function<&calculate_news_impact>{.id = "news_impact",
                                                    .version = 1,
                                                    .implementation_identity =
                                                        "streaming.news_impact.v1"},
                   {.id = "periodic_impact_request",
                    .mode = RequestTriggerMode::TryOnceAtFiring,
                    .pending_for = 1s,
                    .lifetime = 10s,
                    .once_per_candidate = false});

  EF_ASSIGN_OR_RETURN(auto snapshot, catalog->snapshot());
  // Bind SQL, captures, function views and requests into one Catalog before deployment.
  return TriggerSet::bind(std::move(snapshot), std::move(definition), {threshold, periodic},
                          {.shard_count = 2});
}

} // namespace

ComputationModule computation_module() noexcept {
  return {.name = "streaming_news", .version = 1, .bind = &bind_definition};
}

} // namespace eventflow::streaming_example
