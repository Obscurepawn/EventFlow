#include "definition.hpp"
#include "eventflow/cluster_client.hpp"
#include "eventflow/key_codec.hpp"
#include <algorithm>
#include <charconv>
#include <chrono>
#include <httplib.h>
#include <iostream>
#include <set>
#include <string_view>
#include <thread>

namespace eventflow::streaming_example {
namespace {

struct NewsUpdate {
  std::string ticker;
  std::int64_t timestamp_us = 0;
  std::string headline;
  std::string text_body;
  double sentiment_score = 0.0;
  double market_price = 0.0;
};

Status require(bool condition, std::string_view message) noexcept {
  return condition ? Status{} : Status(ErrorCode::FailedPrecondition, message);
}

StatusOr<std::int64_t> decimal(const Json& value) noexcept {
  if (!value.is_string())
    return Status(ErrorCode::InvalidArgument, "expected a decimal string");
  const auto& text = value.get_ref<const std::string&>();
  std::int64_t result = 0;
  const auto parsed = std::from_chars(text.data(), text.data() + text.size(), result);
  if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size())
    return Status(ErrorCode::InvalidArgument, "invalid decimal string");
  return std::int64_t(result);
}

StatusOr<Json> source_request(const std::string& endpoint, const std::string& operation,
                              const Json& body) noexcept {
  httplib::Client client(endpoint);
  client.set_connection_timeout(5);
  client.set_read_timeout(5);
  client.set_write_timeout(5);
  const auto result = client.Post("/v1/source/" + operation, body.dump(), "application/json");
  if (!result)
    return Status(ErrorCode::Unavailable, "HTTP message service request failed");
  if (result->status != 200)
    return Status(ErrorCode::FailedPrecondition,
                  "HTTP message service rejected " + operation + ": " + result->body);
  if (result->body.size() > 65536)
    return Status(ErrorCode::ResourceExhausted, "HTTP producer response exceeds its limit");
  return parse_json_exact(result->body);
}

Json source_identity(std::int32_t partition) noexcept {
  return Json{{"source_id", "news_feed"}, {"topic", "news_updates"}, {"partition", partition}};
}

StatusOr<std::int64_t> source_position(const std::string& endpoint,
                                       std::int32_t partition) noexcept {
  EF_ASSIGN_OR_RETURN(auto reply, source_request(endpoint, "position", source_identity(partition)));
  if (!reply.contains("next_offset"))
    return Status(ErrorCode::InvalidArgument, "source position response has no next_offset");
  return decimal(reply["next_offset"]);
}

Status publish(const std::string& endpoint, const NewsUpdate& news, std::int32_t partition,
               std::int64_t expected_offset) noexcept {
  // The producer writes business JSON bytes to the independent message service.
  // It knows no database ingest envelope and never executes database work.
  const Json payload{{"ticker", news.ticker},
                     {"timestamp_us", news.timestamp_us},
                     {"headline", news.headline},
                     {"text_body", news.text_body},
                     {"sentiment_score", news.sentiment_score},
                     {"market_price", news.market_price}};
  EF_ASSIGN_OR_RETURN(auto key_hex, hex_encode(news.ticker));
  EF_ASSIGN_OR_RETURN(auto payload_hex, hex_encode(payload.dump()));
  auto body = source_identity(partition);
  body["key_hex"] = std::move(key_hex);
  body["payload_hex"] = std::move(payload_hex);
  // The payload retains microseconds; the broker envelope carries milliseconds.
  body["timestamp_ms"] = std::to_string(news.timestamp_us / 1000);
  EF_ASSIGN_OR_RETURN(auto reply, source_request(endpoint, "publish", body));
  EF_ASSIGN_OR_RETURN(auto offset, decimal(reply["offset"]));
  EF_RETURN_IF_ERROR(require(offset == expected_offset, "producer received an unexpected offset"));
  std::cout << "[producer] partition=" << partition << " offset=" << offset
            << " payload=" << payload.dump() << '\n';
  return {};
}

StatusOr<std::vector<std::string>> business_keys() noexcept {
  std::vector<std::string> keys(2);
  for (std::size_t index = 0; index < 1000 && (keys[0].empty() || keys[1].empty()); ++index) {
    const auto candidate = "TICKER_" + std::to_string(index);
    EF_ASSIGN_OR_RETURN(auto shard, shard_for({candidate}, 2));
    if (keys[shard].empty())
      keys[shard] = candidate;
  }
  EF_RETURN_IF_ERROR(require(!keys[0].empty() && !keys[1].empty(),
                             "could not select keys for both compute shards"));
  return keys;
}

struct TriggerRelations {
  std::string capture;
  std::string result;
  std::string latest;
  std::string capture_columns;
};

StatusOr<TriggerRelations> relations(const BoundComputation& bound,
                                     std::string_view trigger_id) noexcept {
  // Generated names are discovered from the binding, never guessed from suffixes.
  for (const auto& trigger : bound.explain()["triggers"]) {
    if (trigger["id"].get<std::string>() != trigger_id)
      continue;
    const auto& generated = trigger["generated"];
    EF_ASSIGN_OR_RETURN(auto capture,
                        bound.catalog.find(generated["capture_relation"].get<RelationId>()));
    EF_ASSIGN_OR_RETURN(auto result,
                        bound.catalog.find(generated["function_relation"].get<RelationId>()));
    EF_ASSIGN_OR_RETURN(auto latest,
                        bound.catalog.find(generated["latest_relation"].get<RelationId>()));
    EF_RETURN_IF_ERROR(require(capture->visible_count == 5,
                               "capture must have ticker, request identity and three features"));
    const auto& fields = capture->storage_schema.fields;
    return TriggerRelations{.capture = capture->name,
                            .result = result->name,
                            .latest = latest->name,
                            .capture_columns = fields[2].name + " AS news_count," + fields[3].name +
                                               " AS average_sentiment," + fields[4].name +
                                               " AS sentiment_price_sum,request_id"};
  }
  return Status(ErrorCode::NotFound, "trigger is absent from the bound definition");
}

StatusOr<ClusterQueryResult> query(ClusterClient& client, const std::string& ticker,
                                   const std::string& columns,
                                   const std::string& relation) noexcept {
  // Generated ticker keys contain only ASCII letters, underscores and digits.
  return client.query("SELECT " + columns + " FROM " + relation + " WHERE ticker='" + ticker + "'",
                      {ticker}, 1024, 1024 * 1024);
}

Status display(const std::string& label, const ClusterQueryResult& result) noexcept {
  std::cout << "[SQL] " << label << " node=" << result.node_id << " shard=" << result.shard << '\n';
  for (const auto& row : result.rows) {
    Json encoded = Json::object();
    for (std::size_t index = 0; index < row.size(); ++index) {
      EF_ASSIGN_OR_RETURN(auto value, value_to_json(row[index]));
      encoded[result.fields[index].name] = std::move(value);
    }
    std::cout << "  " << encoded.dump() << '\n';
  }
  return {};
}

template <typename Predicate>
Status wait_for(std::string_view description, Predicate predicate) noexcept {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
  do {
    EF_ASSIGN_OR_RETURN(auto ready, predicate());
    if (ready)
      return {};
    // Waiting never calls ingest or advances database time: Coordinator owns progress.
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
  } while (std::chrono::steady_clock::now() < deadline);
  return Status(ErrorCode::FailedPrecondition, "timed out waiting for " + std::string(description));
}

Status wait_features(ClusterClient& client, const std::string& ticker, std::int64_t count,
                     double average_sentiment, double sentiment_price_sum) noexcept {
  return wait_for("cascaded news features", [&]() noexcept -> StatusOr<bool> {
    EF_ASSIGN_OR_RETURN(
        auto current,
        query(client, ticker, "news_count,average_sentiment,sentiment_price_sum", "news_features"));
    return current.rows == std::vector<Row>{{count, average_sentiment, sentiment_price_sum}};
  });
}

Status wait_score(ClusterClient& client, const std::string& ticker, const TriggerRelations& trigger,
                  double score) noexcept {
  return wait_for("successful news impact score", [&]() noexcept -> StatusOr<bool> {
    EF_ASSIGN_OR_RETURN(auto current, query(client, ticker, "impact_score", trigger.latest));
    return current.rows == std::vector<Row>{{score}};
  });
}

Status wait_position(const std::string& endpoint, std::int32_t partition,
                     std::int64_t expected) noexcept {
  return wait_for("confirmed source offset", [&]() noexcept -> StatusOr<bool> {
    EF_ASSIGN_OR_RETURN(auto position, source_position(endpoint, partition));
    return position == expected;
  });
}

Status verify_features(ClusterClient& client, const std::string& ticker,
                       std::uint32_t expected_shard, std::int64_t count, double total_sentiment,
                       double sentiment_price_sum) noexcept {
  EF_ASSIGN_OR_RETURN(
      auto totals,
      query(client, ticker, "news_count,total_sentiment,sentiment_price_sum", "news_stats"));
  EF_ASSIGN_OR_RETURN(
      auto features,
      query(client, ticker, "news_count,average_sentiment,sentiment_price_sum", "news_features"));
  EF_RETURN_IF_ERROR(require(
      totals.rows == std::vector<Row>{{count, total_sentiment, sentiment_price_sum}} &&
          features.rows == std::vector<Row>{{count, total_sentiment / static_cast<double>(count),
                                             sentiment_price_sum}},
      "both cascading MVs must match the committed news set"));
  EF_RETURN_IF_ERROR(require(features.shard == expected_shard,
                             "business key was not served by its expected compute shard"));
  EF_RETURN_IF_ERROR(display(ticker + " news_stats", totals));
  return display(ticker + " news_features", features);
}

Status verify_news(ClusterClient& client, const std::string& ticker,
                   const std::vector<NewsUpdate>& expected) noexcept {
  EF_ASSIGN_OR_RETURN(auto current,
                      query(client, ticker,
                            "ticker,timestamp_us,headline,text_body,sentiment_score,market_price",
                            "news_updates"));
  EF_RETURN_IF_ERROR(require(current.rows.size() == expected.size(),
                             "news row count differs from the published updates"));
  // Compare all six fields without relying on SQL row order. Text includes quotes
  // and a newline so the HTTP/JSON/storage round trip must preserve the full body.
  for (const auto& news : expected) {
    const Row row{news.ticker,    news.timestamp_us,    news.headline,
                  news.text_body, news.sentiment_score, news.market_price};
    EF_RETURN_IF_ERROR(
        require(std::find(current.rows.begin(), current.rows.end(), row) != current.rows.end(),
                "SQL did not preserve all six news fields"));
  }
  return display(ticker + " original news", current);
}

StatusOr<std::vector<Row>> verify_threshold(ClusterClient& client, const std::string& ticker,
                                            const TriggerRelations& trigger,
                                            double average_sentiment, double sentiment_price_sum,
                                            double score) noexcept {
  EF_ASSIGN_OR_RETURN(auto result,
                      query(client, ticker, "impact_score,request_id", trigger.result));
  EF_ASSIGN_OR_RETURN(auto capture,
                      query(client, ticker, trigger.capture_columns, trigger.capture));
  EF_RETURN_IF_ERROR(require(result.rows.size() == 1 && result.rows[0].size() == 2 &&
                                 result.rows[0][0] == Value(score),
                             "once-per-session threshold must produce exactly one expected score"));
  EF_RETURN_IF_ERROR(require(capture.rows.size() == 1 &&
                                 capture.rows[0] == Row{std::int64_t{2}, average_sentiment,
                                                        sentiment_price_sum, result.rows[0][1]},
                             "threshold request must retain its admission-time feature snapshot"));
  EF_RETURN_IF_ERROR(display(ticker + " threshold result", result));
  EF_RETURN_IF_ERROR(display(ticker + " threshold frozen input", capture));
  return std::move(capture.rows);
}

Status run(const std::string& meta_endpoint, const std::string& source_endpoint) noexcept {
  EF_ASSIGN_OR_RETURN(auto bound, computation_module().bind({}));
  EF_ASSIGN_OR_RETURN(auto news_relation, bound.catalog.find("news_updates"));
  EF_RETURN_IF_ERROR(
      require(news_relation->visible_count == 6, "news schema must expose exactly six fields"));
  EF_ASSIGN_OR_RETURN(auto threshold, relations(bound, "sentiment_threshold"));
  EF_ASSIGN_OR_RETURN(auto periodic, relations(bound, "periodic_impact"));
  EF_ASSIGN_OR_RETURN(auto keys, business_keys());
  EF_ASSIGN_OR_RETURN(auto client, ClusterClient::open(meta_endpoint, {.timeout_seconds = 10}));
  EF_ASSIGN_OR_RETURN(auto discovered, client->discover());
  EF_RETURN_IF_ERROR(require(discovered.value("state", std::string{}) == "STAGING",
                             "example needs a fresh STAGING cluster"));
  std::cout << "Definition: news_updates -> news_stats -> news_features\n"
               "Threshold: news_count >= 2 AND average_sentiment >= 0.5, once per ticker session.\n"
               "Clock: every 200 ms, capture current features for a separate impact request.\n"
               "Impact score: sentiment_price_sum / news_count.\n"
            << "Routes: " << keys[0] << " -> shard 0, " << keys[1] << " -> shard 1\n";

  const NewsUpdate first{
      .ticker = keys[0],
      .timestamp_us = 1'700'000'000'000'001,
      .headline = "Product outlook improves",
      .text_body = "Management said \"demand is improving\".\nThe report covers the next quarter.",
      .sentiment_score = 0.25,
      .market_price = 100.0};
  const NewsUpdate second{.ticker = keys[0],
                          .timestamp_us = 1'700'000'001'000'002,
                          .headline = "Earnings exceed expectations",
                          .text_body = "Revenue and operating profit exceed the previous outlook.",
                          .sentiment_score = 0.75,
                          .market_price = 104.0};
  const NewsUpdate third{.ticker = keys[0],
                         .timestamp_us = 1'700'000'002'000'003,
                         .headline = "New customer contract announced",
                         .text_body = "A new customer signed a multi-year supply agreement.",
                         .sentiment_score = 0.5,
                         .market_price = 112.0};
  const NewsUpdate other_first{.ticker = keys[1],
                               .timestamp_us = first.timestamp_us,
                               .headline = "Expansion plan published",
                               .text_body =
                                   "The company plans to expand its regional distribution network.",
                               .sentiment_score = 0.5,
                               .market_price = 200.0};
  const NewsUpdate other_second{.ticker = keys[1],
                                .timestamp_us = second.timestamp_us,
                                .headline = "Demand forecast raised",
                                .text_body =
                                    "The quarterly demand forecast increased after new orders.",
                                .sentiment_score = 0.75,
                                .market_price = 208.0};

  // Publish while STAGING: a running listener alone must not consume or acknowledge input.
  EF_RETURN_IF_ERROR(publish(source_endpoint, first, 0, 0));
  EF_RETURN_IF_ERROR(publish(source_endpoint, other_first, 1, 0));
  EF_RETURN_IF_ERROR(publish(source_endpoint, second, 0, 1));
  EF_RETURN_IF_ERROR(publish(source_endpoint, other_second, 1, 1));
  std::this_thread::sleep_for(std::chrono::milliseconds(250));
  for (const auto partition : {0, 1}) {
    EF_ASSIGN_OR_RETURN(auto before, source_position(source_endpoint, partition));
    EF_RETURN_IF_ERROR(require(before == -1, "STAGING deployment acknowledged source input"));
  }
  std::cout << "[staging] Both source partitions remain unacknowledged.\n";
  EF_ASSIGN_OR_RETURN(auto deployed, client->deploy());
  EF_RETURN_IF_ERROR(require(deployed.value("state", std::string{}) == "RUNNING",
                             "deployment did not reach RUNNING"));
  std::cout << "[deploy] RUNNING; database now consumes HTTP messages.\n";

  EF_RETURN_IF_ERROR(wait_features(*client, keys[0], 2, 0.5, 103.0));
  EF_RETURN_IF_ERROR(wait_features(*client, keys[1], 2, 0.625, 256.0));
  EF_RETURN_IF_ERROR(wait_score(*client, keys[0], threshold, 51.5));
  EF_RETURN_IF_ERROR(wait_score(*client, keys[1], threshold, 128.0));
  EF_RETURN_IF_ERROR(wait_score(*client, keys[0], periodic, 51.5));
  EF_RETURN_IF_ERROR(wait_score(*client, keys[1], periodic, 128.0));
  EF_RETURN_IF_ERROR(wait_position(source_endpoint, 0, 2));
  EF_RETURN_IF_ERROR(wait_position(source_endpoint, 1, 2));
  EF_RETURN_IF_ERROR(verify_news(*client, keys[0], {first, second}));
  EF_RETURN_IF_ERROR(verify_news(*client, keys[1], {other_first, other_second}));
  EF_RETURN_IF_ERROR(verify_features(*client, keys[0], 0, 2, 1.0, 103.0));
  EF_RETURN_IF_ERROR(verify_features(*client, keys[1], 1, 2, 1.25, 256.0));
  EF_ASSIGN_OR_RETURN(auto frozen, verify_threshold(*client, keys[0], threshold, 0.5, 103.0, 51.5));
  EF_ASSIGN_OR_RETURN(auto other_frozen,
                      verify_threshold(*client, keys[1], threshold, 0.625, 256.0, 128.0));
  (void)other_frozen;

  // Clock progress requires no new message and must preserve the source confirmation prefix.
  EF_ASSIGN_OR_RETURN(auto earlier, query(*client, keys[0], "request_id", periodic.result));
  std::set<std::string> earlier_ids;
  for (const auto& row : earlier.rows)
    earlier_ids.insert(std::get<std::string>(row[0]));
  EF_RETURN_IF_ERROR(
      wait_for("another autonomous periodic request", [&]() noexcept -> StatusOr<bool> {
        EF_ASSIGN_OR_RETURN(auto current,
                            query(*client, keys[0], "impact_score,request_id", periodic.result));
        for (const auto& row : current.rows)
          if (row[0] == Value(51.5) && !earlier_ids.contains(std::get<std::string>(row[1])))
            return true;
        return false;
      }));
  for (const auto partition : {0, 1}) {
    EF_ASSIGN_OR_RETURN(auto position, source_position(source_endpoint, partition));
    EF_RETURN_IF_ERROR(require(position == 2, "clock-only work changed source confirmation"));
  }
  std::cout << "[clock] A new successful request appeared with no additional source input.\n";

  EF_ASSIGN_OR_RETURN(auto paused, client->pause());
  EF_RETURN_IF_ERROR(require(paused.value("state", std::string{}) == "PAUSED", "pause failed"));
  EF_RETURN_IF_ERROR(publish(source_endpoint, third, 0, 2));
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  EF_ASSIGN_OR_RETURN(auto paused_position, source_position(source_endpoint, 0));
  EF_RETURN_IF_ERROR(
      require(paused_position == 2, "paused database acknowledged the new news update"));
  EF_RETURN_IF_ERROR(verify_features(*client, keys[0], 0, 2, 1.0, 103.0));
  std::cout << "[pause] New news stays in the message log; SQL and ACK remain unchanged.\n";
  EF_ASSIGN_OR_RETURN(auto resumed, client->resume());
  EF_RETURN_IF_ERROR(require(resumed.value("state", std::string{}) == "RUNNING", "resume failed"));
  EF_RETURN_IF_ERROR(wait_features(*client, keys[0], 3, 0.5, 159.0));
  EF_RETURN_IF_ERROR(wait_score(*client, keys[0], periodic, 53.0));
  EF_RETURN_IF_ERROR(wait_position(source_endpoint, 0, 3));
  EF_RETURN_IF_ERROR(verify_news(*client, keys[0], {first, second, third}));
  EF_RETURN_IF_ERROR(verify_features(*client, keys[0], 0, 3, 1.5, 159.0));
  EF_ASSIGN_OR_RETURN(auto still_frozen,
                      verify_threshold(*client, keys[0], threshold, 0.5, 103.0, 51.5));
  EF_RETURN_IF_ERROR(
      require(still_frozen == frozen, "later news changed the admitted threshold request input"));
  EF_ASSIGN_OR_RETURN(auto latest,
                      query(*client, keys[0], "impact_score,request_id", periodic.latest));
  EF_RETURN_IF_ERROR(display(keys[0] + " latest periodic impact", latest));

  // Pause fixes database time before checkpoint. The checkpoint binds source positions,
  // Catalog and runtime state; a raw RocksDB snapshot would not prove this cut.
  EF_ASSIGN_OR_RETURN(auto checkpoint_pause, client->pause());
  (void)checkpoint_pause;
  EF_ASSIGN_OR_RETURN(auto status, client->status());
  EF_ASSIGN_OR_RETURN(auto checkpoint_time, decimal(status["now_us"]));
  EF_ASSIGN_OR_RETURN(auto checkpoint, client->checkpoint("streaming-cut", checkpoint_time));
  EF_RETURN_IF_ERROR(require(checkpoint.value("complete", false), "checkpoint is incomplete"));
  const Json checkpoint_summary{{"checkpoint_id", checkpoint["checkpoint_id"]},
                                {"now_us", checkpoint["now_us"]},
                                {"complete", checkpoint["complete"]},
                                {"source_progress", checkpoint["source_progress"]},
                                {"nodes", checkpoint["nodes"].size()}};
  std::cout << "[checkpoint] " << checkpoint_summary.dump() << '\n';
  EF_ASSIGN_OR_RETURN(auto running, client->resume());
  EF_RETURN_IF_ERROR(require(running.value("state", std::string{}) == "RUNNING",
                             "resume after checkpoint failed"));
  std::cout << "STREAMING_DATABASE_OK shards=2 news_counts=3,2 threshold_impacts=51.5,128 "
               "periodic_impact=53 source_next_offsets=3,2\n";
  return {};
}

} // namespace
} // namespace eventflow::streaming_example

int main(int argc, char** argv) noexcept {
  if (argc != 3) {
    std::cerr << "Usage: eventflow_streaming_verify META_ENDPOINT MESSAGE_ENDPOINT\n";
    return 2;
  }
  const auto result = eventflow::streaming_example::run(argv[1], argv[2]);
  if (!result.ok()) {
    std::cerr << "Streaming database verification failed: " << result.message() << '\n';
    return 1;
  }
  return 0;
}
