#include "definition.hpp"
#include <chrono>
#include <limits>

namespace eventflow::streaming_example {

struct CurrentFeatures {
  std::optional<std::int64_t> total_volume;
  std::int64_t trade_count = 0;
  std::optional<std::int64_t> doubled_volume;
};

struct FeatureSnapshot {
  std::int64_t total_volume = 0;
  std::int64_t trade_count = 0;
  std::int64_t doubled_volume = 0;
};

struct Score {
  std::int64_t score = 0;
};

} // namespace eventflow::streaming_example

namespace eventflow {

template <> struct SchemaTraits<streaming_example::CurrentFeatures> {
  static auto fields() noexcept {
    using streaming_example::CurrentFeatures;
    return std::make_tuple(field<&CurrentFeatures::total_volume>("total_volume"),
                           field<&CurrentFeatures::trade_count>("trade_count"),
                           field<&CurrentFeatures::doubled_volume>("doubled_volume"));
  }
};

template <> struct SchemaTraits<streaming_example::FeatureSnapshot> {
  static auto fields() noexcept {
    using streaming_example::FeatureSnapshot;
    return std::make_tuple(field<&FeatureSnapshot::total_volume>("total_volume"),
                           field<&FeatureSnapshot::trade_count>("trade_count"),
                           field<&FeatureSnapshot::doubled_volume>("doubled_volume"));
  }
};

template <> struct SchemaTraits<streaming_example::Score> {
  static auto fields() noexcept {
    return std::make_tuple(field<&streaming_example::Score::score>("score"));
  }
};

} // namespace eventflow

namespace eventflow::streaming_example {
namespace {

StatusOr<FeatureSnapshot> capture_features(const CurrentFeatures& current) noexcept {
  // SQL SUM is nullable in its schema; this trade group must have concrete values.
  if (!current.total_volume || !current.doubled_volume)
    return Status(ErrorCode::FailedPrecondition, "trade aggregate has no volume");
  return FeatureSnapshot{.total_volume = *current.total_volume,
                         .trade_count = current.trade_count,
                         .doubled_volume = *current.doubled_volume};
}

StatusOr<Score> calculate_score(const FeatureSnapshot& input) noexcept {
  // A small deterministic business function makes every captured result auditable.
  std::int64_t score = 0;
  if (__builtin_add_overflow(input.doubled_volume, input.trade_count, &score))
    return Status(ErrorCode::InvalidArgument, "trade score overflows Int64");
  return Score{.score = score};
}

StatusOr<BoundComputation> bind_definition(const std::vector<std::string>& staged_ddl) noexcept {
  using namespace std::chrono_literals;
  EF_ASSIGN_OR_RETURN(auto catalog, Catalog::create());
  // trade_id preserves individual trades; ticker owns their partition and aggregate.
  EF_ASSIGN_OR_RETURN(auto trades, catalog->define_sql(R"sql(
    CREATE TABLE trades (
      ticker STRING NOT NULL,
      trade_id INT64 NOT NULL,
      volume INT64 NOT NULL,
      PRIMARY KEY (ticker, trade_id)
    ) PARTITION BY (ticker)
  )sql"));
  // First MV: maintain each ticker's total and count as trades are committed.
  EF_ASSIGN_OR_RETURN(auto totals, catalog->define_sql(R"sql(
    CREATE MATERIALIZED VIEW trade_totals AS
      SELECT ticker, SUM(volume) AS total_volume, COUNT(*) AS trade_count
      FROM trades GROUP BY ticker
  )sql"));
  (void)totals;
  // Second MV depends on the first: one input commit also updates doubled_volume.
  EF_ASSIGN_OR_RETURN(auto features, catalog->define_sql(R"sql(
    CREATE MATERIALIZED VIEW market_features AS
      SELECT ticker, total_volume, trade_count, total_volume * 2 AS doubled_volume
      FROM trade_totals
  )sql"));
  for (const auto& statement : staged_ddl) {
    EF_ASSIGN_OR_RETURN(auto relation, catalog->define_sql(statement));
    (void)relation;
  }
  EF_ASSIGN_OR_RETURN(
      auto session,
      catalog->define_candidate(
          {.name = "trading_session",
           .scope_fields =
               {{.id = 1, .name = "ticker", .type = DataType::String, .nullable = false}},
           .payload_fields = {
               {.id = 2, .name = "deadline_us", .type = DataType::Int64, .nullable = false}}}));

  ProcessorConfig definition;
  definition.input_relations = {trades};
  definition.requests.emplace();
  // The first trade opens one session per ticker. This finite example deliberately
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

  // Evaluate the committed aggregate after a trade. The first total >= 100 admits
  // one request in this session; later trades cannot replace its frozen input.
  const auto threshold =
      Trigger::on_event("volume_threshold",
                        EventRef::source({.kind = "trades", .payload_relation = trades}))
          .implementation_identity("streaming.volume_threshold.v1")
          .when(ViewRef(features).column<std::int64_t>("total_volume") >= std::int64_t{100})
          .in_session(SessionRef::candidate(session))
          .capture_current<&capture_features>(features, "streaming.capture_features.v1")
          .request(
              Function<&calculate_score>{.id = "trade_score",
                                         .version = 1,
                                         .implementation_identity = "streaming.trade_score.v1"},
              {.id = "threshold_score_request",
               .mode = RequestTriggerMode::Wait,
               .pending_for = 1s,
               .lifetime = 10s,
               .once_per_candidate = true});

  // Database time drives one clock per observed ticker. Each ready 200 ms slot
  // captures the current cascaded MV and produces a separate score request.
  // Overdue slots may be coalesced; this declaration does not promise a wall-time SLA.
  const auto periodic =
      Trigger::clock("periodic_score", {.policy_id = "market_score_clock",
                                        .every = 200ms,
                                        .phase = 0ms,
                                        .event_kind = "market_clock"})
          .implementation_identity("streaming.periodic_score.v1")
          .in_session(SessionRef::candidate(session))
          .capture_current<&capture_features>(features, "streaming.capture_features.v1")
          .request(
              Function<&calculate_score>{.id = "trade_score",
                                         .version = 1,
                                         .implementation_identity = "streaming.trade_score.v1"},
              {.id = "periodic_score_request",
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
  return {.name = "streaming_market", .version = 1, .bind = &bind_definition};
}

} // namespace eventflow::streaming_example
