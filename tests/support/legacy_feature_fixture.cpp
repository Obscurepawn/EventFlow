#include "legacy_feature_fixture.hpp"
#include "eventflow/computation.hpp"
#include "eventflow/key_codec.hpp"
#include <algorithm>
#include <charconv>
#include <chrono>
#include <cmath>
#include <limits>

// This legacy application policy is linked only into regression test targets.
namespace eventflow::example {
namespace {
Status invalid(std::string_view text) noexcept {
  return Status(ErrorCode::InvalidArgument, text);
}

Status validate_config(const DemoConfig& c) noexcept {
  for (const auto value : {c.candidate_ttl_us, c.ready_wait_us, c.quote_max_age_us,
                           c.confirmation_window_us, c.trade_window_us, c.periodic_interval_us,
                           c.history_window_us, c.request_lifetime_us, c.encoder_version})
    if (value <= 0 || value > 3'600'000'000)
      return invalid("demo time/version values must be positive and bounded");
  if (!std::isfinite(c.confirmation_return) || c.confirmation_return < 0.0 ||
      c.confirmation_return > 1.0 || c.confirmation_volume <= 0 || c.history_max_rows < 2 ||
      c.history_max_rows > 1024 || c.audit_limit == 0 || c.audit_limit > 8192 ||
      c.news_version_namespace_id == 0)
    return invalid("invalid demo threshold or memory budget");
  return {};
}

std::string policy_json(const DemoConfig& c) noexcept {
  return Json::array({c.candidate_ttl_us, c.ready_wait_us, c.quote_max_age_us,
                      c.confirmation_window_us, c.confirmation_return, c.trade_window_us,
                      c.confirmation_volume, c.periodic_interval_us, c.history_window_us,
                      c.request_lifetime_us, c.encoder_version, c.history_max_rows,
                      c.news_version_namespace_id})
      .dump();
}

bool quote_ready(const std::optional<Row>& quote, std::int64_t now,
                 const DemoConfig& config) noexcept {
  if (!quote)
    return false;
  const auto* bid = std::get_if<double>(&(*quote)[1]);
  const auto* ask = std::get_if<double>(&(*quote)[2]);
  const auto observed = std::get<std::int64_t>((*quote)[3]);
  return bid && ask && std::isfinite(*bid) && std::isfinite(*ask) && *bid > 0.0 && *ask >= *bid &&
         observed >= 0 && observed <= now && now - observed <= config.quote_max_age_us;
}

bool candidate_matches(const std::optional<Row>& candidate,
                       const RequestAdmission& admission) noexcept {
  return candidate && std::get<bool>((*candidate)[2]) &&
         std::get<std::int64_t>((*candidate)[1]) > 0 &&
         static_cast<std::uint64_t>(std::get<std::int64_t>((*candidate)[1])) ==
             admission.candidate_epoch;
}

Row request_identity(const RequestAdmission& admission) noexcept {
  auto identity = admission.scope;
  identity.emplace_back(admission.request_id);
  return identity;
}

TableChange captured(RelationId relation, const Row& identity, Row payload) noexcept {
  Row row = identity;
  row.insert(row.end(), std::make_move_iterator(payload.begin()),
             std::make_move_iterator(payload.end()));
  return {.relation = relation, .identity = identity, .row = std::move(row)};
}

StatusOr<RelationId> define_capture(Catalog& catalog, const std::string& name,
                                    const std::string& payload) noexcept {
  DefinitionOptions options;
  options.base_write_role = BaseWriteRole::RequestCapture;
  options.identity_domain = "demo_requests";
  return catalog.define_sql("CREATE TABLE " + name +
                                "(ticker STRING NOT NULL,request_id "
                                "STRING NOT NULL," +
                                payload + ",PRIMARY KEY(ticker,request_id)) PARTITION BY(ticker)",
                            options);
}

StatusOr<RelationId> define_function(Catalog& catalog, FunctionRegistry& registry, std::string name,
                                     const std::vector<RelationId>& inputs,
                                     std::vector<Field> output,
                                     BuiltinFunctionCallback callback) noexcept {
  FunctionViewDefinition function;
  function.name = name;
  function.function_id = "demo_" + name;
  function.function_version = 1;
  function.implementation_identity = "eventflow.demo." + name + ".v1";
  function.output_fields = std::move(output);
  FunctionSignature signature;
  signature.function_id = function.function_id;
  signature.version = function.function_version;
  signature.implementation_identity = function.implementation_identity;
  signature.output_fields = function.output_fields;
  EF_ASSIGN_OR_RETURN(auto snapshot, catalog.snapshot());
  for (std::size_t i = 0; i < inputs.size(); ++i) {
    EF_ASSIGN_OR_RETURN(auto source, snapshot.find(inputs[i]));
    std::vector<Field> fields(source->storage_schema.fields.begin(),
                              source->storage_schema.fields.begin() + source->visible_count);
    const auto alias = "input_" + std::to_string(i);
    function.inputs.push_back(
        {.alias = alias, .relation = inputs[i], .required = true, .fields = fields});
    signature.inputs.push_back({.alias = alias, .required = true, .fields = std::move(fields)});
  }
  EF_RETURN_IF_ERROR(
      registry.add({.signature = std::move(signature), .callback = std::move(callback)}));
  return catalog.define_function(function);
}

StatusOr<Row> encode_text(const FunctionInputs& inputs) noexcept {
  const auto& text = std::get<std::string>((*inputs[0])[1]);
  const auto version = std::get<std::int64_t>((*inputs[0])[2]);
  std::vector<double> vector(4, 0.0);
  for (const unsigned char byte : text)
    vector[byte % vector.size()] += 1.0;
  const auto scale = static_cast<double>(std::max<std::size_t>(1, text.size()));
  for (auto& value : vector)
    value = value / scale + static_cast<double>(version) * 0.001;
  return Row{std::move(vector)};
}

StatusOr<Row> compute_features(const FunctionInputs& inputs) noexcept {
  const double mid = std::get<double>((*inputs[0])[1]);
  const double spread = std::get<double>((*inputs[0])[2]);
  const auto& prices = std::get<std::vector<double>>((*inputs[1])[2]);
  if (prices.size() < 2 || !std::isfinite(mid) || mid <= 0.0)
    return invalid("features require a positive mid and two prices");
  double mean = 0.0;
  double squares = 0.0;
  for (std::size_t i = 1; i < prices.size(); ++i) {
    const double change = prices[i] / prices[i - 1] - 1.0;
    const double delta = change - mean;
    mean += delta / static_cast<double>(i);
    squares += delta * (change - mean);
    if (!std::isfinite(change) || !std::isfinite(mean) || !std::isfinite(squares))
      return invalid("demo history arithmetic is not finite");
  }
  const double change = mid / prices.back() - 1.0;
  const double volatility =
      std::sqrt(std::max(0.0, squares / static_cast<double>(prices.size() - 1)));
  if (!std::isfinite(change) || !std::isfinite(volatility) || !std::isfinite(spread))
    return invalid("demo features overflowed finite numeric semantics");
  return Row{change, volatility, spread / mid};
}

StatusOr<Row> predict(const FunctionInputs& inputs) noexcept {
  const double change = std::get<double>((*inputs[0])[2]);
  const auto& vector = std::get<std::vector<double>>((*inputs[1])[2]);
  if (vector.size() != 4)
    return invalid("demo encoder vector shape differs");
  const double value = change * 0.5 + (vector[0] - vector[1]) * 0.001;
  if (!std::isfinite(value))
    return invalid("demo prediction is not finite");
  return Row{value};
}

StatusOr<Row> evaluate_risk(const FunctionInputs& inputs) noexcept {
  const auto& features = *inputs[0];
  const double score = std::abs(std::get<double>(features[2])) + std::get<double>(features[3]) +
                       std::get<double>(features[4]) +
                       std::abs(std::get<double>((*inputs[1])[2])) * 0.1;
  if (!std::isfinite(score))
    return invalid("demo risk is not finite");
  return Row{std::min(1.0, score)};
}

std::string request_key(const RequestKey& key) noexcept {
  Json scope = Json::array();
  for (const auto& value : key.scope)
    scope.push_back(std::get<std::string>(value));
  return Json::array({scope, key.request_id}).dump();
}

bool terminal(RequestPhase phase) noexcept {
  return phase == RequestPhase::Success || phase == RequestPhase::Failed ||
         phase == RequestPhase::Cancelled;
}
} // namespace

StatusOr<DemoDefinition> DemoDefinition::create(const DemoConfig& config,
                                                const std::vector<std::string>& ddl) noexcept {
  EF_RETURN_IF_ERROR(validate_config(config));
  DemoDefinition result;
  EF_ASSIGN_OR_RETURN(auto catalog, Catalog::create());
  EF_ASSIGN_OR_RETURN(result.registry, FunctionRegistry::create());
  auto& r = result.relations;
  EF_ASSIGN_OR_RETURN(
      r.news,
      catalog->define_sql(
          "CREATE TABLE news(ticker STRING NOT NULL,news_id STRING NOT NULL,text_version INT64 NOT "
          "NULL,text STRING NOT NULL,reference_price FLOAT64 NOT NULL,PRIMARY "
          "KEY(ticker,news_id,text_version)) PARTITION BY(ticker)"));
  EF_ASSIGN_OR_RETURN(
      r.quote,
      catalog->define_sql(
          "CREATE TABLE quote(ticker STRING NOT NULL,bid FLOAT64,ask FLOAT64,observed_us INT64 NOT "
          "NULL,PRIMARY KEY(ticker)) PARTITION BY(ticker)"));
  EF_ASSIGN_OR_RETURN(
      r.history,
      catalog->define_sql(
          "CREATE TABLE history(ticker STRING NOT NULL,sample_id INT64 NOT NULL,price FLOAT64,"
          "observed_us INT64 NOT NULL,PRIMARY KEY(ticker,sample_id)) PARTITION BY(ticker)"));
  EF_ASSIGN_OR_RETURN(
      r.trade,
      catalog->define_sql(
          "CREATE TABLE trade(ticker STRING NOT NULL,trade_id INT64 NOT NULL,volume INT64 NOT NULL,"
          "PRIMARY KEY(ticker,trade_id)) PARTITION BY(ticker)"));
  EF_ASSIGN_OR_RETURN(
      auto volume,
      catalog->define_sql(
          "CREATE MATERIALIZED VIEW trade_volume AS SELECT ticker,SUM(volume) AS volume "
          "FROM trade WHERE volume>0 GROUP BY ticker"));
  // The condition consumes a cascaded MV, so threshold maintenance stays in the
  // same relational graph as aggregation rather than repeating it in the guard.
  EF_ASSIGN_OR_RETURN(
      volume, catalog->define_sql(
                  "CREATE MATERIALIZED VIEW trade_condition AS SELECT ticker,volume,volume>=" +
                  std::to_string(config.confirmation_volume) + " AS ready FROM trade_volume"));
  EF_ASSIGN_OR_RETURN(
      r.candidate,
      catalog->define_candidate(
          {.name = "candidate",
           .scope_fields =
               {{.id = 1, .name = "ticker", .type = DataType::String, .nullable = false}},
           .payload_fields = {
               {.id = 2, .name = "deadline_us", .type = DataType::Int64, .nullable = false},
               {.id = 3, .name = "news_id", .type = DataType::String, .nullable = false},
               {.id = 4, .name = "text_version", .type = DataType::Int64, .nullable = false},
               {.id = 5, .name = "text", .type = DataType::String, .nullable = false},
               {.id = 6, .name = "encoder_version", .type = DataType::Int64, .nullable = false},
               {.id = 7, .name = "admitted_us", .type = DataType::Int64, .nullable = false},
               {.id = 8, .name = "reference_price", .type = DataType::Float64, .nullable = false},
               {.id = 9,
                .name = "confirmation_deadline_us",
                .type = DataType::Int64,
                .nullable = false}}}));
  EF_ASSIGN_OR_RETURN(r.text_capture,
                      define_capture(*catalog, "text_capture",
                                     "text STRING NOT NULL,encoder_version INT64 NOT NULL"));
  EF_ASSIGN_OR_RETURN(r.text_sql,
                      catalog->define_sql("CREATE MATERIALIZED VIEW text_sql AS SELECT "
                                          "ticker,text,encoder_version FROM text_capture"));
  EF_ASSIGN_OR_RETURN(
      r.embedding,
      define_function(
          *catalog, *result.registry, "embedding", {r.text_sql},
          {{.id = 1, .name = "vector", .type = DataType::FloatVector, .nullable = false}},
          encode_text));
  EF_ASSIGN_OR_RETURN(auto initial, catalog->snapshot());
  EF_ASSIGN_OR_RETURN(auto root, initial.find(r.text_capture));
  EF_ASSIGN_OR_RETURN(r.request_status,
                      catalog->define_request_status({.name = "request_status",
                                                      .request_domain = "demo_requests",
                                                      .identity_fields = root->identity_fields,
                                                      .candidate_relation = r.candidate}));
  EF_ASSIGN_OR_RETURN(r.latest_embedding,
                      catalog->define_latest({.name = "latest_embedding",
                                              .result_relation = r.embedding,
                                              .request_status_relation = r.request_status,
                                              .candidate_relation = r.candidate}));
  EF_ASSIGN_OR_RETURN(r.quote_capture, define_capture(*catalog, "quote_capture",
                                                      "bid FLOAT64 NOT NULL,ask FLOAT64 NOT NULL"));
  EF_ASSIGN_OR_RETURN(r.history_capture,
                      define_capture(*catalog, "history_capture", "prices FLOAT_VECTOR NOT NULL"));
  EF_ASSIGN_OR_RETURN(r.embedding_capture, define_capture(*catalog, "embedding_capture",
                                                          "vector FLOAT_VECTOR NOT NULL"));
  EF_ASSIGN_OR_RETURN(
      r.market_sql,
      catalog->define_sql(
          "CREATE MATERIALIZED VIEW market_sql AS SELECT ticker,bid/2.0+ask/2.0 AS mid,"
          "ask-bid AS spread FROM quote_capture"));
  EF_ASSIGN_OR_RETURN(
      r.features,
      define_function(
          *catalog, *result.registry, "features", {r.market_sql, r.history_capture},
          {{.id = 1, .name = "change", .type = DataType::Float64, .nullable = false},
           {.id = 2, .name = "volatility", .type = DataType::Float64, .nullable = false},
           {.id = 3, .name = "spread_ratio", .type = DataType::Float64, .nullable = false}},
          compute_features));
  EF_ASSIGN_OR_RETURN(
      r.prediction,
      define_function(
          *catalog, *result.registry, "prediction", {r.features, r.embedding_capture},
          {{.id = 1, .name = "expected_return", .type = DataType::Float64, .nullable = false}},
          predict));
  EF_ASSIGN_OR_RETURN(
      r.risk, define_function(
                  *catalog, *result.registry, "risk", {r.features, r.prediction},
                  {{.id = 1, .name = "risk_score", .type = DataType::Float64, .nullable = false}},
                  evaluate_risk));
  EF_ASSIGN_OR_RETURN(r.latest_prediction,
                      catalog->define_latest({.name = "latest_prediction",
                                              .result_relation = r.prediction,
                                              .request_status_relation = r.request_status,
                                              .candidate_relation = r.candidate}));
  EF_ASSIGN_OR_RETURN(r.latest_risk,
                      catalog->define_latest({.name = "latest_risk",
                                              .result_relation = r.risk,
                                              .request_status_relation = r.request_status,
                                              .candidate_relation = r.candidate}));

  auto& p = result.processor;
  p.input_relations = {r.news, r.quote, r.history, r.trade};
  // Quote age is inclusive at the configured boundary; other windows expire at their deadline.
  p.ttl_policies = {{.relation = r.news, .ttl_us = config.candidate_ttl_us},
                    {.relation = r.quote, .ttl_us = config.quote_max_age_us + 1},
                    {.relation = r.history, .ttl_us = config.history_window_us},
                    {.relation = r.trade, .ttl_us = config.trade_window_us}};
  p.max_keys = 16;
  p.max_timers = 2048;
  // Leave space for request terminal records and SQL closure in the default remote KV batch.
  p.max_state_bytes = 512 * 1024;
  p.max_occurrences = 64;
  p.max_event_bytes = 16 * 1024;
  p.requests.emplace();
  auto& requests = *p.requests;
  requests.registry = result.registry;
  requests.max_pending_intents = 128;
  requests.runtime.max_active_requests = 16;
  requests.runtime.max_active_bytes = 16 * 1024 * 1024;
  requests.runtime.max_record_bytes = 1024 * 1024;
  EF_ASSIGN_OR_RETURN(auto news_relation, initial.find(r.news));
  const auto news_schema = news_relation->storage_schema;
  requests.candidate = CandidateExpiryPolicy{
      .relation = r.candidate,
      .deadline_column = "deadline_us",
      .version = 1,
      .on_source = [r, config, news_schema](RoundContext& round,
                                            const ProcessorEvent& event) noexcept
          -> StatusOr<std::optional<CandidateActivation>> {
        if (event.kind != "news")
          return std::optional<CandidateActivation>{};
        EF_RETURN_IF_ERROR(news_schema.validate(event.values));
        const auto& row = event.values;
        const auto& id = std::get<std::string>(row[1]);
        const auto version = std::get<std::int64_t>(row[2]);
        const auto reference = std::get<double>(row[4]);
        if (id.empty() || version <= 0 || std::get<std::string>(row[3]).empty() ||
            !std::isfinite(reference) || reference <= 0.0)
          return std::optional<CandidateActivation>{};
        EF_ASSIGN_OR_RETURN(auto old, round.read_relation(r.candidate, event.scope));
        const auto epoch = old ? std::get<std::int64_t>((*old)[1]) : 0;
        if (old && std::get<std::string>((*old)[4]) == id &&
            std::get<std::int64_t>((*old)[5]) >= version)
          return std::optional<CandidateActivation>{};
        Row version_identity = event.scope;
        version_identity.emplace_back(id);
        EF_ASSIGN_OR_RETURN(
            auto version_key,
            row_key(round.shard(), 'S', config.news_version_namespace_id, version_identity));
        EF_ASSIGN_OR_RETURN(auto high_water, round.read(version_key));
        if (high_water) {
          std::int64_t stored_version = 0;
          const auto parsed = std::from_chars(
              high_water->data(), high_water->data() + high_water->size(), stored_version);
          if (parsed.ec != std::errc{} || parsed.ptr != high_water->data() + high_water->size() ||
              stored_version <= 0)
            return Status(ErrorCode::FailedPrecondition, "invalid durable news version high water");
          if (stored_version >= version)
            return std::optional<CandidateActivation>{};
        }
        EF_RETURN_IF_ERROR(
            round.stage({.key = std::move(version_key), .value = std::to_string(version)}));
        if (event.processing_time_us > INT64_MAX - config.candidate_ttl_us ||
            event.processing_time_us > INT64_MAX - config.confirmation_window_us)
          return invalid("candidate deadline overflow");
        return std::optional<CandidateActivation>{
            {.expected_epoch = epoch,
             .payload = {event.processing_time_us + config.candidate_ttl_us, id, version, row[3],
                         config.encoder_version, event.processing_time_us, reference,
                         event.processing_time_us + config.confirmation_window_us}}};
      }};

  // Stable plan IDs: A encodes news text; B is the first ready prediction;
  // C confirms price/volume conditions; D recomputes at a periodic clock slot.
  // Bindings use the corresponding lowercase IDs in durable request identities.
  RequestPlanDefinition text_encoding;
  text_encoding.name = "A";
  text_encoding.targets = {r.embedding};
  text_encoding.request_status_relation = r.request_status;
  text_encoding.default_lifetime_us = config.request_lifetime_us;
  text_encoding.machine.max_nodes = 16;
  text_encoding.machine.max_state_bytes = 256 * 1024;
  text_encoding.machine.max_frozen_input_bytes = 64 * 1024;
  text_encoding.machine.max_output_bytes = 16 * 1024;
  text_encoding.machine.max_attempts = 1;
  text_encoding.machine.attempt_timeout_us = config.request_lifetime_us;
  text_encoding.capture =
      [r](RoundContext& round,
          const RequestAdmission& admission) noexcept -> StatusOr<RequestCapture> {
    EF_ASSIGN_OR_RETURN(auto candidate, round.read_relation(r.candidate, admission.scope));
    if (!candidate_matches(candidate, admission))
      return RequestCapture{.ready = false, .rows = {}};
    const auto identity = request_identity(admission);
    return RequestCapture{
        .ready = true,
        .rows = {captured(r.text_capture, identity, {(*candidate)[6], (*candidate)[7]})}};
  };
  requests.plans.push_back(text_encoding);
  for (const auto* name : {"B", "C", "D"}) {
    auto plan = text_encoding;
    plan.name = name;
    plan.targets = {r.prediction, r.risk};
    plan.capture =
        [r, config](RoundContext& round,
                    const RequestAdmission& admission) noexcept -> StatusOr<RequestCapture> {
      EF_ASSIGN_OR_RETURN(auto candidate, round.read_relation(r.candidate, admission.scope));
      if (!candidate_matches(candidate, admission))
        return RequestCapture{.ready = false, .rows = {}};
      const auto now = round.processing_time_us();
      if (admission.plan == "C" &&
          now - std::get<std::int64_t>((*candidate)[8]) >= config.confirmation_window_us)
        return RequestCapture{.ready = false, .rows = {}};
      EF_ASSIGN_OR_RETURN(auto quote, round.read_relation(r.quote, admission.scope));
      EF_ASSIGN_OR_RETURN(auto embedding, round.read_relation(r.latest_embedding, admission.scope));
      if (!quote_ready(quote, now, config) || !embedding ||
          std::get<std::int64_t>((*embedding)[3]) != std::get<std::int64_t>((*candidate)[1]))
        return RequestCapture{.ready = false, .rows = {}};
      EF_ASSIGN_OR_RETURN(auto history,
                          round.scan_relation(r.history, admission.scope, StateImage::Staged,
                                              config.history_max_rows, 1024 * 1024));
      std::sort(history.begin(), history.end(), [](const Row& lhs, const Row& rhs) noexcept {
        if (std::get<std::int64_t>(lhs[3]) != std::get<std::int64_t>(rhs[3]))
          return std::get<std::int64_t>(lhs[3]) < std::get<std::int64_t>(rhs[3]);
        return std::get<std::int64_t>(lhs[1]) < std::get<std::int64_t>(rhs[1]);
      });
      std::vector<double> prices;
      for (const auto& sample : history) {
        const auto* price = std::get_if<double>(&sample[2]);
        const auto observed = std::get<std::int64_t>(sample[3]);
        if (price && std::isfinite(*price) && *price > 0.0 && observed >= 0 && observed <= now &&
            now - observed < config.history_window_us)
          prices.push_back(*price);
      }
      if (prices.size() < 2)
        return RequestCapture{.ready = false, .rows = {}};
      const auto identity = request_identity(admission);
      return RequestCapture{
          .ready = true,
          .rows = {captured(r.quote_capture, identity, {(*quote)[1], (*quote)[2]}),
                   captured(r.history_capture, identity, {std::move(prices)}),
                   captured(r.embedding_capture, identity, {(*embedding)[2]})}};
    };
    requests.plans.push_back(std::move(plan));
  }
  using namespace std::chrono_literals;
  const auto session = SessionRef::candidate(r.candidate);
  const auto policy = policy_json(config);
  // Stable binding IDs: a encodes text, c confirms a market move, and d scores
  // the current periodic slot. Their request plans keep the same identities.
  const auto news_trigger =
      Trigger::on_event("embed_a", EventRef::source({.kind = "news", .payload_relation = r.news}))
          .implementation_identity(policy)
          .in_session(session)
          .request({.id = "a",
                    .version = 1,
                    .plan = "A",
                    .mode = RequestTriggerMode::Wait,
                    .pending_for = TriggerDuration::from_microseconds(config.ready_wait_us),
                    .lifetime = {},
                    .once_per_candidate = true,
                    .required_accepted_binding = {},
                    .deadline_column = {},
                    .make_admission = {},
                    .machine = {}});
  // This pure guard needs processing-time age and several current views. Declare
  // each read once; the binder validates layouts and assembles the owner read set.
  const auto confirmation_condition = Predicate::guard(
      {{.alias = "candidate", .relation = r.candidate},
       {.alias = "quote", .relation = r.quote},
       {.alias = "volume", .relation = volume}},
      [config](const FsmContext& context) noexcept -> StatusOr<bool> {
        const auto& candidate = context.reads.at("candidate");
        const auto& quote = context.reads.at("quote");
        const auto& volume_row = context.reads.at("volume");
        if (!candidate || !std::get<bool>((*candidate)[2]) || !volume_row ||
            !quote_ready(quote, context.event.time_us, config))
          return false;
        const auto elapsed = context.event.time_us - std::get<std::int64_t>((*candidate)[8]);
        if (elapsed < 0 || elapsed >= config.confirmation_window_us)
          return false;
        const auto mid = std::get<double>((*quote)[1]) / 2.0 + std::get<double>((*quote)[2]) / 2.0;
        const auto reference = std::get<double>((*candidate)[9]);
        return mid >= reference * (1.0 + config.confirmation_return) &&
               std::get<bool>((*volume_row)[2]);
      },
      "feature.confirmation.v2:" + policy);
  const auto confirmation_trigger =
      Trigger::on_event("confirm_c", "*")
          .implementation_identity(policy)
          .when(confirmation_condition)
          .in_session(session)
          .request(
              {.id = "c",
               .version = 1,
               .plan = "C",
               .mode = RequestTriggerMode::Wait,
               .pending_for = TriggerDuration::from_microseconds(config.confirmation_window_us),
               .lifetime = {},
               .once_per_candidate = true,
               .required_accepted_binding = "b",
               .deadline_column = "confirmation_deadline_us",
               .make_admission = {},
               .machine = {}});
  // A firing tries its current slot once. It never backfills missed clock slots,
  // and it requires first prediction admission, not first prediction success.
  const auto periodic_trigger =
      Trigger::clock("predict_d",
                     {.policy_id = "periodic_d",
                      .every = TriggerDuration::from_microseconds(config.periodic_interval_us),
                      .phase = 0us,
                      .event_kind = "tick_d"})
          .implementation_identity(policy)
          .in_session(session)
          .request({.id = "d",
                    .version = 1,
                    .plan = "D",
                    .mode = RequestTriggerMode::TryOnceAtFiring,
                    .pending_for = TriggerDuration::from_microseconds(config.periodic_interval_us),
                    .lifetime = {},
                    .once_per_candidate = false,
                    .required_accepted_binding = "b",
                    .deadline_column = {},
                    .make_admission = {},
                    .machine = {}});
  requests.level_bindings = {
      {.id = "b", .version = 1, .plan = "B", .pending_lifetime_us = config.ready_wait_us}};
  for (const auto& statement : ddl) {
    EF_ASSIGN_OR_RETURN(auto id, catalog->define_sql(statement));
    (void)id;
  }
  EF_ASSIGN_OR_RETURN(result.catalog, catalog->snapshot());
  for (const auto& relation : result.catalog.relations)
    if (relation->kind == RelationKind::BaseTable &&
        relation->base_write_role == BaseWriteRole::PublicInput &&
        std::find(p.input_relations.begin(), p.input_relations.end(), relation->id) ==
            p.input_relations.end())
      p.input_relations.push_back(relation->id);
  if (config.news_version_namespace_id == p.namespace_id ||
      config.news_version_namespace_id == requests.runtime.namespace_id)
    return invalid("news version namespace collides with an owner namespace");
  for (const auto& relation : result.catalog.relations)
    if (relation->id == config.news_version_namespace_id ||
        relation->state_id == config.news_version_namespace_id)
      return invalid("news version namespace collides with Catalog state");
  EF_ASSIGN_OR_RETURN(auto bound,
                      TriggerSet::bind(result.catalog, std::move(result.processor),
                                       {news_trigger, confirmation_trigger, periodic_trigger}));
  result.catalog = std::move(bound.catalog);
  result.registry = std::move(bound.registry);
  result.processor = std::move(bound.definition);
  return result;
}

StatusOr<std::unique_ptr<FeatureComputationDemo>>
FeatureComputationDemo::open(std::shared_ptr<IStateStore> store, const DemoConfig& config,
                             const EngineConfig& engine, const std::vector<std::string>& ddl,
                             bool threaded) noexcept {
  auto demo = std::unique_ptr<FeatureComputationDemo>(new FeatureComputationDemo);
  demo->config_ = config;
  EF_ASSIGN_OR_RETURN(demo->definition_, DemoDefinition::create(config, ddl));
  EF_ASSIGN_OR_RETURN(demo->processor_,
                      EventProcessor::open(std::move(store), demo->definition_.catalog, engine,
                                           demo->definition_.processor));
  if (threaded) {
    FunctionExecutorConfig executor_config;
    EF_ASSIGN_OR_RETURN(demo->executor_, ThreadPoolFunctionExecutor::create(
                                             demo->definition_.registry, executor_config));
    demo->executor_workers_ = executor_config.worker_count;
  } else {
    EF_ASSIGN_OR_RETURN(auto executor, SimpleFunctionExecutor::create(demo->definition_.registry));
    demo->simple_executor_ = executor.get();
    demo->executor_ = std::move(executor);
  }
  EF_ASSIGN_OR_RETURN(auto positions, demo->processor_->processed_positions());
  for (const auto& position : positions)
    if (position.source_id == "demo" && position.topic == "input" && position.partition == 0)
      demo->next_offset_ = position.next_offset;
  EF_ASSIGN_OR_RETURN(auto active, demo->processor_->active_requests());
  for (auto& record : active) {
    RequestKey key{.scope = record.admission.scope, .request_id = record.admission.request_id};
    for (const auto& node : record.state.nodes)
      if (!node.task_id.empty() && node.phase == RequestNodePhase::Running)
        demo->task_owners_[node.task_id] = key;
    demo->active_[request_key(key)] = std::move(record);
  }
  return demo;
}

FeatureComputationDemo::~FeatureComputationDemo() noexcept = default;

Status FeatureComputationDemo::consume(ProcessResult result) noexcept {
  Status delivery_status;
  for (auto& round : result.committed_rounds) {
    for (auto& event : round.requests) {
      if (event.record) {
        const auto key = request_key(event.key);
        for (const auto& node : event.record->state.nodes)
          if (!node.task_id.empty() && node.phase == RequestNodePhase::Running)
            task_owners_[node.task_id] = event.key;
        if (terminal(event.record->state.phase))
          active_.erase(key);
        else
          active_[key] = *event.record;
      }
      if (request_events_.size() == config_.audit_limit)
        request_events_.pop_front();
      request_events_.push_back({.revision = round.revision,
                                 .time_us = round.processing_time_us,
                                 .result = std::move(event)});
    }
    for (const auto& id : round.cancel_task_ids) {
      if (submitted_tasks_.contains(id)) {
        auto status = executor_->cancel(id);
        if (!status.ok() && status.code() != ErrorCode::NotFound && delivery_status.ok())
          delivery_status = status;
      }
    }
    for (const auto& task : round.tasks) {
      if (!task_owners_.contains(task.task_id))
        return Status(ErrorCode::Internal, "committed task has no trusted request record");
      // Dispatch after consuming every confirmed round. The durable outbox survives
      // a temporarily full executor without losing later request or clock effects.
    }
    for (auto& clock : round.clock_firings) {
      if (clock_events_.size() == config_.audit_limit)
        clock_events_.pop_front();
      clock_events_.push_back(std::move(clock));
    }
  }
  return result.operation_status.ok() ? delivery_status : result.operation_status;
}

void FeatureComputationDemo::forget_completion(const std::string& id) noexcept {
  task_owners_.erase(id);
  submitted_tasks_.erase(id);
}

Status FeatureComputationDemo::synchronize_confirmed() noexcept {
  EF_ASSIGN_OR_RETURN(auto positions, processor_->processed_positions());
  for (const auto& position : positions)
    if (position.source_id == "demo" && position.topic == "input" && position.partition == 0)
      next_offset_ = std::max(next_offset_, position.next_offset);
  EF_ASSIGN_OR_RETURN(auto active, processor_->active_requests());
  active_.clear();
  std::set<std::string> running;
  for (auto& record : active) {
    RequestKey key{.scope = record.admission.scope, .request_id = record.admission.request_id};
    for (const auto& node : record.state.nodes)
      if (node.phase == RequestNodePhase::Running && !node.task_id.empty()) {
        task_owners_[node.task_id] = key;
        running.insert(node.task_id);
      }
    active_[request_key(key)] = std::move(record);
  }
  // A caller may have confirmed the exposed processor directly. Consult bounded
  // authoritative state before resending a polled completion or choosing an offset.
  for (auto it = completions_.begin(); it != completions_.end();) {
    if (!running.contains(it->task_id)) {
      forget_completion(it->task_id);
      it = completions_.erase(it);
    } else
      ++it;
  }
  for (const auto& id : submitted_tasks_)
    if (!running.contains(id)) {
      const auto status = executor_->cancel(id);
      if (!status.ok() && status.code() != ErrorCode::NotFound)
        return status;
    }
  return {};
}

Status FeatureComputationDemo::resolve_pending() noexcept {
  for (std::size_t step = 0; processor_->has_pending_commit() && step < 32; ++step) {
    EF_ASSIGN_OR_RETURN(auto resolved, processor_->resolve_pending_commit());
    auto status = consume(std::move(resolved));
    if (!processor_->has_pending_commit()) {
      EF_RETURN_IF_ERROR(synchronize_confirmed());
    }
    if (!status.ok())
      return status;
  }
  if (processor_->has_pending_commit())
    return Status(ErrorCode::UnknownCommit, "demo owner commit remains unconfirmed");
  return synchronize_confirmed();
}

Status FeatureComputationDemo::claim_ready(std::int64_t now_us) noexcept {
  std::vector<RequestKey> keys;
  for (const auto& [id, record] : active_) {
    (void)id;
    if (std::any_of(record.state.nodes.begin(), record.state.nodes.end(),
                    [](const RequestNodeState& node) noexcept {
                      return node.phase == RequestNodePhase::Ready;
                    }))
      keys.push_back({.scope = record.admission.scope, .request_id = record.admission.request_id});
  }
  for (const auto& key : keys) {
    const auto active = executor_->metrics().active_tasks;
    if (active >= 64 || !completions_.empty())
      break;
    EF_ASSIGN_OR_RETURN(
        auto result, processor_->claim_tasks(key, now_us, std::min<std::size_t>(16, 64 - active)));
    EF_RETURN_IF_ERROR(consume(std::move(result)));
  }
  // Durable task outbox is authoritative; local routing derives only from trusted records.
  EF_ASSIGN_OR_RETURN(auto pending, processor_->pending_tasks(64));
  for (const auto& task : pending) {
    if (submitted_tasks_.contains(task.task_id))
      continue;
    if (!task_owners_.contains(task.task_id))
      return Status(ErrorCode::FailedPrecondition, "task recovery requires its request record");
    // The owner already claimed this Ready task at its logical clock. The
    // executor's wall clock only schedules execution; it must not reinterpret
    // that logical timestamp, including after recovery of the same opaque ID.
    auto dispatched = task;
    dispatched.ready_at_us = 0;
    const auto submitted = executor_->submit(dispatched);
    if (submitted.code() == ErrorCode::ResourceExhausted)
      break;
    EF_RETURN_IF_ERROR(submitted);
    submitted_tasks_.insert(task.task_id);
  }
  return {};
}

Status FeatureComputationDemo::ingest(std::string_view relation, Row row,
                                      std::int64_t now_us) noexcept {
  EF_RETURN_IF_ERROR(resolve_pending());
  if (next_offset_ == INT64_MAX)
    return invalid("demo source offset is exhausted");
  const auto offset = next_offset_;
  EF_ASSIGN_OR_RETURN(
      auto result,
      ingest_source(relation, std::move(row),
                    {.source_id = "demo", .topic = "input", .partition = 0, .offset = offset},
                    now_us));
  if (result.input_committed || result.duplicate)
    next_offset_ = std::max(next_offset_, offset + 1);
  if (result.operation_status.ok() && !result.input_committed && !result.duplicate)
    return Status(ErrorCode::ResourceExhausted, "demo input awaits owner progress");
  return result.operation_status;
}

StatusOr<DemoIngestResult> FeatureComputationDemo::ingest_source(std::string_view relation, Row row,
                                                                 const SourceIdentity& source,
                                                                 std::int64_t now_us) noexcept {
  EF_RETURN_IF_ERROR(resolve_pending());
  EF_ASSIGN_OR_RETURN(auto descriptor, definition_.catalog.find(std::string(relation)));
  if (std::find(definition_.processor.input_relations.begin(),
                definition_.processor.input_relations.end(),
                descriptor->id) == definition_.processor.input_relations.end())
    return invalid("ingest requires a declared public input");
  EF_RETURN_IF_ERROR(descriptor->storage_schema.validate(row));
  EF_ASSIGN_OR_RETURN(auto scope, partition_values(*descriptor, row));
  EF_ASSIGN_OR_RETURN(auto identity, physical_identity(*descriptor, row));
  if (now_us < 0 || source.offset == INT64_MAX)
    return invalid("invalid demo time or exhausted source offset");
  ProcessorEvent event{.source = source,
                       .scope = std::move(scope),
                       .kind = descriptor->name,
                       .values = row,
                       .changes = {{.relation = descriptor->id,
                                    .identity = std::move(identity),
                                    .row = std::move(row)}},
                       .processing_time_us = now_us};
  EF_ASSIGN_OR_RETURN(auto result, processor_->process(event));
  DemoIngestResult outcome{.input_committed = result.input_committed,
                           .duplicate = result.duplicate,
                           .operation_status = {}};
  outcome.operation_status = consume(std::move(result));
  // An exact delivery retry must not rewind an owner whose timers have since
  // advanced. Background/explicit pump can continue any remaining work.
  if (!outcome.operation_status.ok() || outcome.duplicate)
    return outcome;
  outcome.operation_status = pump(std::max(now_us, processor_->processing_time_us()));
  return outcome;
}

Status FeatureComputationDemo::pump(std::int64_t now_us) noexcept {
  EF_RETURN_IF_ERROR(resolve_pending());
  for (std::size_t round = 0; round < 32; ++round) {
    EF_ASSIGN_OR_RETURN(auto result, processor_->pump(now_us));
    const bool progress = !result.committed_rounds.empty();
    const bool remaining = result.work_remaining;
    EF_RETURN_IF_ERROR(consume(std::move(result)));
    EF_RETURN_IF_ERROR(claim_ready(now_us));
    if (!remaining || !progress)
      return {};
  }
  return Status(ErrorCode::ResourceExhausted, "demo dispatch exhausted its progress budget");
}

StatusOr<std::size_t> FeatureComputationDemo::run_functions(std::int64_t now_us,
                                                            std::size_t limit) noexcept {
  std::size_t completed = 0;
  EF_RETURN_IF_ERROR(pump(now_us));
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(10);
  while (completed < limit) {
    if (!simple_executor_ && std::chrono::steady_clock::now() >= deadline)
      break;
    if (completions_.empty()) {
      std::size_t ran = 0;
      if (simple_executor_) {
        EF_ASSIGN_OR_RETURN(ran, simple_executor_->run_ready(now_us, limit - completed));
      }
      EF_ASSIGN_OR_RETURN(auto polled, executor_->poll(std::min<std::size_t>(64, limit - completed),
                                                       simple_executor_ ? 0 : 2));
      for (auto& result : polled)
        completions_.push_back(std::move(result));
      if (completions_.empty()) {
        if (ran != 0)
          return Status(ErrorCode::Internal, "executed demo task has no completion");
        break;
      }
    }
    const auto& completion = completions_.front();
    auto found = task_owners_.find(completion.task_id);
    if (found == task_owners_.end())
      return Status(ErrorCode::Internal, "completion has no trusted request route");
    EF_ASSIGN_OR_RETURN(auto result,
                        processor_->complete_request(found->second, completion, now_us));
    const bool confirmed = result.request_result.has_value();
    const auto status = consume(std::move(result));
    if (confirmed) {
      const auto id = completion.task_id;
      completions_.pop_front();
      forget_completion(id);
      ++completed;
    }
    if (!status.ok())
      return status;
    if (!confirmed)
      return Status(ErrorCode::ResourceExhausted, "demo completion awaits further owner progress");
    if (completions_.empty())
      EF_RETURN_IF_ERROR(pump(now_us));
  }
  return completed;
}

StatusOr<QueryResult> FeatureComputationDemo::query_sql(std::string_view sql) noexcept {
  return processor_->query_sql(sql);
}

StatusOr<RequestRecord> FeatureComputationDemo::read_request(const RequestKey& key) noexcept {
  return processor_->read_request(key);
}

const std::deque<DemoRequestEvent>& FeatureComputationDemo::request_events() const noexcept {
  return request_events_;
}

const std::deque<ClockFiring>& FeatureComputationDemo::clock_events() const noexcept {
  return clock_events_;
}

const DemoDefinition& FeatureComputationDemo::definition() const noexcept {
  return definition_;
}

EventProcessor& FeatureComputationDemo::processor() noexcept {
  return *processor_;
}

FunctionExecutorMetrics FeatureComputationDemo::executor_metrics() const noexcept {
  return executor_->metrics();
}

const char* FeatureComputationDemo::executor_mode() const noexcept {
  return simple_executor_ ? "simple" : "thread_pool";
}

std::size_t FeatureComputationDemo::executor_worker_count() const noexcept {
  return executor_workers_;
}
} // namespace eventflow::example

namespace eventflow::test_support {
namespace {

StatusOr<BoundComputation> bind_legacy_feature(const std::vector<std::string>& ddl) noexcept {
  EF_ASSIGN_OR_RETURN(auto legacy, example::DemoDefinition::create({}, ddl));
  return BoundComputation{.catalog = std::move(legacy.catalog),
                          .registry = std::move(legacy.registry),
                          .definition = std::move(legacy.processor),
                          .explanation = Json::object()};
}

} // namespace

ComputationModule legacy_feature_module() noexcept {
  return {.name = "feature_computation", .version = 1, .bind = bind_legacy_feature};
}

} // namespace eventflow::test_support
