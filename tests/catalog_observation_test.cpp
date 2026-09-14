#include "eventflow/catalog_codec.hpp"
#include "status_test_support.hpp"
#include <gtest/gtest.h>

namespace eventflow::test {
namespace {
ObservationRuleDefinition rule(const CatalogSnapshot& catalog, RelationId relation) noexcept {
  ObservationRuleDefinition result;
  result.name = "crossing";
  result.recipe_id = "crossing_recipe";
  result.implementation_identity = "builtin.crossing.v1";
  result.program_fingerprint = R"({"guard":"v>100","states":["low","high"]})";
  result.scope_fields = {{1, "k", DataType::String, false}};
  ObservationInputDefinition input;
  input.id = "value_change";
  input.kind = ObservationInputKind::RelationChange;
  input.event_kind = "value_changed";
  input.relation = relation;
  result.inputs.push_back(input);
  auto descriptor = take(catalog.find(relation));
  result.reads.push_back(
      {"current", relation,
       std::vector<Field>(descriptor->storage_schema.fields.begin(),
                          descriptor->storage_schema.fields.begin() + descriptor->visible_count)});
  return result;
}

TEST(CatalogObservation, DeclarationParticipatesInSnapshotAndCanonicalWire) {
  auto catalog = take(Catalog::create());
  auto relation = take(catalog->define_sql(
      "CREATE TABLE values_by_key(k STRING,v INT64,PRIMARY KEY(k)) PARTITION BY(k)"));
  auto before = take(catalog->snapshot());
  auto old_query = take(catalog->bind_query_sql("SELECT v FROM values_by_key WHERE k='a'"));
  auto defined = catalog->define_observation(rule(before, relation));
  ASSERT_TRUE(defined.ok()) << defined.status().message();
  auto after = take(catalog->snapshot());
  EXPECT_EQ(after.version, before.version + 1);
  ASSERT_EQ(after.observations.size(), 1);
  EXPECT_NE(after.fingerprint, before.fingerprint);
  EXPECT_FALSE(validate_bound_plan(old_query, after).ok());
  auto decoded = decode_catalog(take(encode_catalog(after)));
  ASSERT_TRUE(decoded.ok()) << decoded.status().message();
  EXPECT_EQ(decoded->fingerprint, after.fingerprint);
}

std::unique_ptr<Catalog> input_catalog() noexcept {
  auto result = take(Catalog::create());
  take(result->define_sql(
      "CREATE TABLE values_by_key(k STRING,v INT64,PRIMARY KEY(k)) PARTITION BY(k)"));
  return result;
}

TEST(CatalogObservation, NoDeclarationsKeepOriginalWireAndSnapshotsOwnTheirMetadata) {
  auto catalog = input_catalog();
  auto before = take(catalog->snapshot());
  EXPECT_FALSE(take(parse_json_exact(before.fingerprint)).contains("observations"));
  EXPECT_EQ(take(encode_catalog(before)), before.fingerprint);
  auto definition = rule(before, 1);
  take(catalog->define_observation(definition));
  definition.inputs[0].event_kind = "changed_after_define";
  auto copy = take(catalog->snapshot());
  copy.observations[0].definition.reads[0].fields[1].name = "changed_copy";
  auto current = take(catalog->snapshot());
  EXPECT_EQ(current.observations[0].definition.inputs[0].event_kind, "value_changed");
  EXPECT_EQ(current.observations[0].definition.reads[0].fields[1].name, "v");
  EXPECT_FALSE(Catalog::from_snapshot(copy).ok());
  EXPECT_TRUE(before.observations.empty());
}

TEST(CatalogObservation, InterleavedRelationsReconstructBindingPrefixesAndQueryProvenance) {
  auto catalog = input_catalog();
  take(catalog->define_sql("CREATE TABLE sink(k STRING,v INT64,PRIMARY KEY(k)) PARTITION BY(k)"));
  auto first = rule(take(catalog->snapshot()), 1);
  first.sink_relation = 2;
  first.max_sink_row_bytes = 4096;
  take(catalog->define_observation(first));
  auto child =
      take(catalog->define_sql("CREATE MATERIALIZED VIEW child AS SELECT k,v+1 AS v FROM sink"));
  auto second = rule(take(catalog->snapshot()), child);
  second.name = "after_sink";
  take(catalog->define_observation(second));
  take(catalog->define_sql("CREATE MATERIALIZED VIEW later AS SELECT k,v+1 AS v FROM child"));
  auto snapshot = take(catalog->snapshot());
  EXPECT_EQ(snapshot.version, 6);
  EXPECT_EQ(snapshot.observations[0].after_relation_count, 2);
  EXPECT_EQ(snapshot.observations[1].after_relation_count, 3);
  auto decoded = take(decode_catalog(take(encode_catalog(snapshot))));
  EXPECT_EQ(decoded.fingerprint, snapshot.fingerprint);
  auto restored = take(Catalog::from_snapshot(decoded));
  auto query = take(restored->bind_query_sql("SELECT v FROM later WHERE k='a'"));
  EXPECT_TRUE(validate_bound_plan(query, decoded).ok());
  EXPECT_EQ(query.catalog_version, decoded.version);
  auto graph = take(observation_graph(decoded));
  EXPECT_EQ(graph.rule_order, (std::vector<std::uint32_t>{1, 2}));
}

TEST(CatalogObservation, InvalidScopeReadsSelectorsAndBoundsRejectWithoutChangingCatalog) {
  auto catalog = input_catalog();
  take(catalog->define_sql(
      "CREATE TABLE history(k STRING,id INT64,v INT64,PRIMARY KEY(k,id)) PARTITION BY(k)"));
  auto snapshot = take(catalog->snapshot());
  const auto original = rule(snapshot, 1);
  std::vector<ObservationRuleDefinition> invalid;
  auto changed = original;
  changed.scope_fields[0].nullable = true;
  invalid.push_back(changed);
  changed = original;
  changed.scope_fields[0].name = "other";
  invalid.push_back(changed);
  changed = original;
  changed.reads[0].fields[1].type = DataType::Float64;
  invalid.push_back(changed);
  changed = original;
  changed.reads[0].relation = 2;
  changed.reads[0].fields = snapshot.relations[1]->storage_schema.fields;
  invalid.push_back(changed);
  changed = original;
  changed.inputs[0].kind = static_cast<ObservationInputKind>(99);
  invalid.push_back(changed);
  changed = original;
  changed.inputs[0].source_fields = {{1, "fake", DataType::Int64, false}};
  invalid.push_back(changed);
  changed = original;
  changed.inputs[0].change_mask = 0;
  invalid.push_back(changed);
  changed = original;
  changed.inputs[0].max_event_bytes = 0;
  invalid.push_back(changed);
  changed = original;
  changed.inputs[0].relation = 999;
  invalid.push_back(changed);
  changed = original;
  changed.reads[0].max_row_bytes = 0;
  invalid.push_back(changed);
  changed = original;
  changed.program_fingerprint = "{ \"a\":1}";
  invalid.push_back(changed);
  changed = original;
  changed.program_kind = static_cast<ObservationProgramKind>(99);
  invalid.push_back(changed);
  changed = original;
  changed.version = 0;
  invalid.push_back(changed);
  changed = original;
  changed.inputs.push_back(changed.inputs[0]);
  invalid.push_back(changed);
  changed = original;
  changed.sink_relation = 2;
  invalid.push_back(changed);
  changed = original;
  changed.activation_targets = {1};
  invalid.push_back(changed);
  for (std::size_t i = 0; i < invalid.size(); ++i) {
    SCOPED_TRACE(i);
    EXPECT_FALSE(catalog->define_observation(invalid[i]).ok());
    EXPECT_EQ(take(catalog->snapshot()).fingerprint, snapshot.fingerprint);
  }
}

TEST(CatalogObservation, ReadDependenciesOrderRulesWithoutSubscribingToReadChanges) {
  auto catalog = input_catalog();
  take(catalog->define_sql("CREATE TABLE sink(k STRING,v INT64,PRIMARY KEY(k)) PARTITION BY(k)"));
  auto late = rule(take(catalog->snapshot()), 2);
  late.name = "registered_first";
  late.inputs[0].kind = ObservationInputKind::Source;
  late.inputs[0].relation.reset();
  late.inputs[0].source_kind = "news";
  late.inputs[0].source_fields = {{1, "v", DataType::Int64, false}};
  late.recipe_id = "source_recipe";
  take(catalog->define_observation(late));
  auto early = rule(take(catalog->snapshot()), 1);
  early.sink_relation = 2;
  early.max_sink_row_bytes = 4096;
  take(catalog->define_observation(early));
  auto graph = take(observation_graph(take(catalog->snapshot())));
  EXPECT_EQ(graph.rule_order, (std::vector<std::uint32_t>{2, 1}));
  EXPECT_FALSE(graph.subscribers_by_relation.contains(2));
  EXPECT_EQ(graph.subscribers_by_relation.at(1), (std::vector<std::uint32_t>{2}));
}

TEST(CatalogObservation, DirectAndIndirectSinkFeedbackAreRejected) {
  auto catalog = input_catalog();
  auto self = rule(take(catalog->snapshot()), 1);
  self.sink_relation = 1;
  self.max_sink_row_bytes = 4096;
  EXPECT_FALSE(catalog->define_observation(self).ok());
  take(catalog->define_sql("CREATE TABLE other(k STRING,v INT64,PRIMARY KEY(k)) PARTITION BY(k)"));
  auto first = rule(take(catalog->snapshot()), 1);
  first.sink_relation = 2;
  first.max_sink_row_bytes = 4096;
  take(catalog->define_observation(first));
  auto second = rule(take(catalog->snapshot()), 2);
  second.name = "back_edge";
  second.sink_relation = 1;
  second.max_sink_row_bytes = 4096;
  EXPECT_FALSE(catalog->define_observation(second).ok());
  EXPECT_EQ(take(catalog->snapshot()).observations.size(), 1);
}

RelationId add_function(Catalog& catalog) noexcept {
  DefinitionOptions options;
  options.base_write_role = BaseWriteRole::RequestCapture;
  options.identity_domain = "request_domain";
  auto capture = take(catalog.define_sql("CREATE TABLE capture(k STRING,request_id STRING,v "
                                         "INT64,PRIMARY KEY(k,request_id)) PARTITION BY(k)",
                                         options));
  auto snapshot = take(catalog.snapshot());
  const auto& fields = take(snapshot.find(capture))->storage_schema.fields;
  return take(catalog.define_function({"result",
                                       "fn",
                                       1,
                                       "builtin.fn",
                                       {{"input", capture, true, fields}},
                                       {{1, "answer", DataType::Int64, false}}}));
}

TEST(CatalogObservation, HostEventLayoutsAndActivationEdgesRoundtripForEveryInputKind) {
  auto catalog = input_catalog();
  const auto target = add_function(*catalog);
  auto definition = rule(take(catalog->snapshot()), 1);
  definition.program_kind = ObservationProgramKind::Cep;
  definition.version = UINT64_MAX;
  definition.recipe_version = UINT64_MAX;
  auto source = ObservationInputDefinition{};
  source.id = "source";
  source.event_kind = "source";
  source.source_kind = "news";
  source.source_fields = {{1, "payload", DataType::String, false}};
  definition.inputs.push_back(source);
  auto completion = ObservationInputDefinition{};
  completion.id = "settled";
  completion.event_kind = "settled";
  completion.kind = ObservationInputKind::AttemptSettled;
  completion.plan = "plan";
  completion.completion_targets = {target};
  definition.inputs.push_back(completion);
  completion.id = "terminal";
  completion.event_kind = "terminal";
  completion.kind = ObservationInputKind::RequestTerminal;
  definition.inputs.push_back(completion);
  auto clock = ObservationInputDefinition{};
  clock.id = "clock";
  clock.event_kind = "clock";
  clock.kind = ObservationInputKind::Clock;
  clock.policy_id = "periodic";
  definition.inputs.push_back(clock);
  auto expiry = ObservationInputDefinition{};
  expiry.id = "expiry";
  expiry.event_kind = "expiry";
  expiry.kind = ObservationInputKind::ExpiryNotice;
  expiry.relation = 1;
  definition.inputs.push_back(expiry);
  take(catalog->define_observation(definition));
  auto snapshot = take(decode_catalog(take(encode_catalog(take(catalog->snapshot())))));
  const auto& fields = snapshot.observations[0].event_fields;
  ASSERT_EQ(fields.size(), 6);
  EXPECT_EQ(fields[0].size(), 7);
  EXPECT_EQ(fields[1].size(), 1);
  EXPECT_EQ(fields[2].size(), 6);
  EXPECT_EQ(fields[3].size(), 4);
  EXPECT_EQ(fields[4].size(), 3);
  EXPECT_EQ(fields[5].size(), 2);
  EXPECT_EQ(fields[2].back().name, "outcome");
  EXPECT_TRUE(fields[0][3].nullable);
  auto cycle = definition;
  cycle.name = "recursive";
  cycle.activation_targets = {target};
  EXPECT_FALSE(catalog->define_observation(cycle).ok());
}

TEST(CatalogObservation, RecipeIdentityCannotBeReusedForDifferentProgramOrSignature) {
  auto catalog = input_catalog();
  auto definition = rule(take(catalog->snapshot()), 1);
  take(catalog->define_observation(definition));
  auto conflicting = definition;
  conflicting.name = "other";
  conflicting.implementation_identity = "different";
  EXPECT_FALSE(catalog->define_observation(conflicting).ok());
  conflicting = definition;
  conflicting.name = "other";
  conflicting.program_fingerprint = "{}";
  EXPECT_FALSE(catalog->define_observation(conflicting).ok());
  conflicting = definition;
  conflicting.name = "other";
  conflicting.recipe_version = 2;
  EXPECT_TRUE(catalog->define_observation(conflicting).ok());
}

TEST(CatalogObservation, UnknownFieldsAndForgedLayoutsCannotDisappearInCodec) {
  auto catalog = input_catalog();
  take(catalog->define_observation(rule(take(catalog->snapshot()), 1)));
  auto original = take(parse_json_exact(take(encode_catalog(take(catalog->snapshot())))));
  std::vector<Json> invalid;
  auto changed = original;
  changed["observations"][0]["extra"] = true;
  invalid.push_back(changed);
  changed = original;
  changed["observations"][0]["definition"]["extra"] = true;
  invalid.push_back(changed);
  changed = original;
  changed["observations"][0]["definition"]["inputs"][0]["extra"] = true;
  invalid.push_back(changed);
  changed = original;
  changed["observations"][0]["definition"]["reads"][0]["extra"] = true;
  invalid.push_back(changed);
  changed = original;
  changed["observations"][0]["event_fields"][0][0]["type"] = "int64";
  invalid.push_back(changed);
  changed = original;
  changed["observations"][0]["after_relation_count"] = 0;
  invalid.push_back(changed);
  changed = original;
  changed["observations"][0]["id"] = 2;
  invalid.push_back(changed);
  changed = original;
  changed["observations"][0]["definition"]["inputs"][0]["kind"] = 6;
  invalid.push_back(changed);
  changed = original;
  changed["observations"][0]["definition"]["inputs"][0]["max_event_bytes"] = -1;
  invalid.push_back(changed);
  for (std::size_t i = 0; i < invalid.size(); ++i) {
    SCOPED_TRACE(i);
    EXPECT_FALSE(decode_catalog(invalid[i].dump()).ok());
  }
  auto bytes = original.dump();
  CatalogCodecConfig config;
  config.max_bytes = bytes.size();
  EXPECT_TRUE(decode_catalog(bytes, config).ok());
  --config.max_bytes;
  EXPECT_EQ(decode_catalog(bytes, config).status().code(), ErrorCode::ResourceExhausted);
}

TEST(CatalogObservation, HiddenPartitionProjectionAndAggregateKeepBusinessScope) {
  auto catalog = input_catalog();
  auto projection = take(catalog->define_sql(
      "CREATE MATERIALIZED VIEW dropped_key AS SELECT v*2 AS x FROM values_by_key"));
  auto projected = rule(take(catalog->snapshot()), projection);
  take(catalog->define_observation(projected));
  auto aggregate = take(catalog->define_sql(
      "CREATE MATERIALIZED VIEW grouped AS SELECT SUM(v) AS total FROM values_by_key GROUP BY k"));
  auto grouped = rule(take(catalog->snapshot()), aggregate);
  grouped.name = "group_observer";
  grouped.recipe_id = "group_recipe";
  EXPECT_TRUE(catalog->define_observation(grouped).ok());
  auto snapshot = take(catalog->snapshot());
  EXPECT_EQ(snapshot.relations[projection - 1]->partition_visible_slots[0], kNoSlot);
  EXPECT_EQ(snapshot.relations[aggregate - 1]->partition_visible_slots[0], kNoSlot);
  EXPECT_TRUE(decode_catalog(take(encode_catalog(snapshot))).ok());
}

TEST(CatalogObservation, ActivationIncludesCaptureAncestorsInFeedbackDetection) {
  auto catalog = input_catalog();
  auto target = add_function(*catalog);
  auto definition = rule(take(catalog->snapshot()), target - 1);
  definition.reads.clear();
  definition.activation_targets = {target};
  EXPECT_FALSE(catalog->define_observation(definition).ok());
  EXPECT_TRUE(take(catalog->snapshot()).observations.empty());
}

TEST(CatalogObservation, WideRelationGeneratesBothEventSidesWithinCodecFieldBudgets) {
  auto catalog = take(Catalog::create());
  CreateTableAst table;
  table.name = "wide";
  table.primary_key = {"k"};
  table.partition_key = {"k"};
  table.columns.push_back({"k", DataType::String, false});
  for (std::size_t i = 1; i < 2048; ++i)
    table.columns.push_back({"v" + std::to_string(i), DataType::Int64, false});
  auto relation = take(catalog->define(table));
  auto defined = catalog->define_observation(rule(take(catalog->snapshot()), relation));
  ASSERT_TRUE(defined.ok()) << defined.status().message();
  auto snapshot = take(catalog->snapshot());
  ASSERT_EQ(snapshot.observations[0].event_fields[0].size(), 4099);
  EXPECT_TRUE(encode_catalog(snapshot).ok());
  CatalogCodecConfig config;
  config.max_fields_per_relation = 2048;
  EXPECT_TRUE(encode_catalog(snapshot, config).ok());
  --config.max_fields_per_relation;
  EXPECT_EQ(encode_catalog(snapshot, config).status().code(), ErrorCode::ResourceExhausted);
}

TEST(CatalogObservation, WholeRequestSqlTargetsAreValidButAttemptTargetsAreFunctions) {
  auto catalog = input_catalog();
  add_function(*catalog);
  auto target = take(
      catalog->define_sql("CREATE MATERIALIZED VIEW final_result AS SELECT answer FROM result"));
  auto activation = rule(take(catalog->snapshot()), 1);
  activation.activation_targets = {target};
  EXPECT_TRUE(catalog->define_observation(activation).ok());
  auto terminal = rule(take(catalog->snapshot()), 1);
  terminal.name = "terminal_observer";
  terminal.recipe_id = "terminal_recipe";
  terminal.inputs.clear();
  ObservationInputDefinition input;
  input.id = "terminal";
  input.kind = ObservationInputKind::RequestTerminal;
  input.event_kind = "terminal";
  input.plan = "sql_target_plan";
  input.completion_targets = {target};
  terminal.inputs.push_back(input);
  EXPECT_TRUE(catalog->define_observation(terminal).ok());
  auto wrong_attempt = terminal;
  wrong_attempt.name = "wrong_attempt";
  wrong_attempt.recipe_id = "attempt_recipe";
  wrong_attempt.inputs[0].kind = ObservationInputKind::AttemptSettled;
  EXPECT_FALSE(catalog->define_observation(wrong_attempt).ok());
  EXPECT_TRUE(decode_catalog(take(encode_catalog(take(catalog->snapshot())))).ok());
}
} // namespace
} // namespace eventflow::test
