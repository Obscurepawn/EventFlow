#include "definition.hpp"

namespace eventflow::native_udf_example {

SubmittedComputation computation_definition(const FunctionRef& function) noexcept {
  // These declarations travel over the SDK. The generic database binary has no business graph.
  SubmittedComputation definition{
      .name = "news_scoring",
      .ddl =
          {"CREATE TABLE news_updates(ticker STRING NOT NULL,timestamp_us INT64 NOT NULL,"
           "headline STRING NOT NULL,text_body STRING NOT NULL,sentiment_score FLOAT64 NOT NULL,"
           "market_price FLOAT64 NOT NULL,PRIMARY KEY(ticker,timestamp_us)) PARTITION BY(ticker)",
           "CREATE MATERIALIZED VIEW news_stats AS SELECT ticker,COUNT(*) AS news_count,"
           "SUM(sentiment_score) AS total_sentiment,SUM(sentiment_score*market_price) AS "
           "sentiment_price_sum "
           "FROM news_updates GROUP BY ticker",
           "CREATE MATERIALIZED VIEW news_features AS SELECT ticker,news_count,"
           "total_sentiment/CAST(news_count AS FLOAT64) AS average_sentiment,sentiment_price_sum "
           "FROM news_stats"},
      .input_relation = "news_updates",
      .session_lifetime_us = 1800000000,
      .triggers = {
          {.name = "sentiment_threshold",
           .function = function,
           .capture_relation = "news_features",
           .capture_fields = {"ticker", "news_count", "sentiment_price_sum"},
           .conditions = {{.column = "news_count", .comparison = "ge", .value = std::int64_t{2}},
                          {.column = "average_sentiment", .comparison = "ge", .value = 0.5}},
           .every_us = 0,
           .once_per_session = true},
          {.name = "periodic_impact",
           .function = function,
           .capture_relation = "news_features",
           .capture_fields = {"ticker", "news_count", "sentiment_price_sum"},
           .conditions = {},
           .every_us = 200000,
           .once_per_session = false}}};
  return definition;
}

} // namespace eventflow::native_udf_example
