#include "eventflow/catalog.hpp"
#include "status_test_support.hpp"
#include <gtest/gtest.h>

namespace eventflow {
namespace {
using test::take;

DefinitionOptions capture_options(std::string domain = "prediction_requests") noexcept {
  DefinitionOptions options;
  options.base_write_role = BaseWriteRole::RequestCapture;
  options.identity_domain = std::move(domain);
  return options;
}

RelationId define_capture(Catalog& catalog, const std::string& name,
                          const std::string& payload = "text STRING") noexcept {
  return take(catalog.define_sql("CREATE TABLE " + name + " (account STRING, request_id STRING, " +
                                     payload +
                                     ", PRIMARY KEY(request_id,account)) PARTITION BY(account)",
                                 capture_options()));
}

FunctionViewDefinition function_definition(const CatalogSnapshot& snapshot, const std::string& name,
                                           const std::vector<RelationId>& inputs) noexcept {
  FunctionViewDefinition definition;
  definition.name = name;
  definition.function_id = "builtin." + name;
  definition.function_version = 1;
  definition.implementation_identity = "builtin." + name + ".v1";
  for (std::size_t i = 0; i < inputs.size(); ++i) {
    const auto* input = take(snapshot.find(inputs[i]));
    definition.inputs.push_back(
        {"input_" + std::to_string(i), inputs[i], true,
         std::vector<Field>(input->storage_schema.fields.begin(),
                            input->storage_schema.fields.begin() + input->visible_count)});
  }
  definition.output_fields = {{1, "score", DataType::Float64, true}};
  return definition;
}

TEST(FunctionCatalog, CaptureRoleOwnsTheFullRequestIdentityDomain) {
  auto catalog = take(Catalog::create());
  const auto id = define_capture(*catalog, "capture");
  const auto snapshot = take(catalog->snapshot());
  const auto* relation = take(snapshot.find(id));
  EXPECT_EQ(relation->kind, RelationKind::BaseTable);
  EXPECT_EQ(relation->base_write_role, BaseWriteRole::RequestCapture);
  EXPECT_EQ(relation->identity_domain, "prediction_requests");
  ASSERT_EQ(relation->identity_fields.size(), 2U);
  EXPECT_EQ(relation->identity_fields[0].name, "account");
  EXPECT_EQ(relation->identity_fields[1].name, "request_id");
  EXPECT_EQ(relation->partition_types.size(), 1U);
  EXPECT_EQ(relation->identity_types.size(), 2U);
  EXPECT_FALSE(relation->unique_per_partition);
  EXPECT_FALSE(relation->plan);
  EXPECT_FALSE(relation->function);
}

TEST(FunctionCatalog, FunctionVisibleIdentityIsHostOwnedAndPayloadSignatureIsSeparate) {
  auto catalog = take(Catalog::create());
  const auto capture = define_capture(*catalog, "capture");
  auto definition = function_definition(take(catalog->snapshot()), "embedding", {capture});
  const auto id = take(catalog->define_function(definition));
  const auto snapshot = take(catalog->snapshot());
  const auto* relation = take(snapshot.find(id));
  ASSERT_EQ(relation->kind, RelationKind::FunctionView);
  ASSERT_TRUE(relation->function);
  EXPECT_FALSE(relation->plan);
  EXPECT_EQ(relation->visible_count, 3U);
  EXPECT_EQ(relation->storage_schema.fields.size(), 5U);
  EXPECT_EQ(relation->storage_schema.fields[0].name, "account");
  EXPECT_EQ(relation->storage_schema.fields[1].name, "request_id");
  EXPECT_EQ(relation->storage_schema.fields[2].name, "score");
  EXPECT_FALSE(relation->storage_schema.fields[0].nullable);
  EXPECT_FALSE(relation->storage_schema.fields[1].nullable);
  EXPECT_EQ(relation->physical_key_slots, (std::vector<std::size_t>{3, 4}));
  EXPECT_EQ(relation->partition_slots, (std::vector<std::size_t>{3}));
  EXPECT_EQ(relation->partition_visible_slots, (std::vector<std::size_t>{0}));
  EXPECT_EQ(relation->function->definition.output_fields.size(), 1U);
  EXPECT_EQ(relation->identity_domain, "prediction_requests");
}

TEST(FunctionCatalog, StoredFunctionRowCannotForgeVisibleRequestIdentity) {
  auto catalog = take(Catalog::create());
  const auto capture = define_capture(*catalog, "capture");
  const auto id = take(catalog->define_function(
      function_definition(take(catalog->snapshot()), "embedding", {capture})));
  const auto snapshot = take(catalog->snapshot());
  const auto* relation = take(snapshot.find(id));
  const Row identity{std::string("a"), std::string("request-1")};
  const Row visible{std::string("a"), std::string("request-1"), 0.5};
  auto stored = take(make_stored_row(*relation, visible, identity));
  EXPECT_EQ(take(visible_row(*relation, stored)), visible);
  EXPECT_FALSE(
      make_stored_row(*relation, visible, Row{std::string("a"), std::string("request-2")}).ok());
  stored[1] = std::string("request-2");
  EXPECT_FALSE(visible_row(*relation, stored).ok());
}

TEST(FunctionCatalog, OrdinarySqlRetainsLegacyCanonicalMetadataShape) {
  auto catalog = take(Catalog::create());
  take(catalog->define_sql(
      "CREATE TABLE ordinary(k STRING, v INT64, PRIMARY KEY(k)) PARTITION BY(k)"));
  take(catalog->define_sql("CREATE MATERIALIZED VIEW projected AS SELECT v FROM ordinary"));
  const auto snapshot = take(catalog->snapshot());
  const auto json = take(parse_json_exact(snapshot.fingerprint));
  EXPECT_EQ(snapshot.relations[0]->kind, RelationKind::BaseTable);
  EXPECT_EQ(snapshot.relations[1]->kind, RelationKind::SqlView);
  for (const auto& relation : json["relations"]) {
    EXPECT_FALSE(relation.contains("kind"));
    EXPECT_FALSE(relation.contains("base_write_role"));
    EXPECT_FALSE(relation.contains("identity_domain"));
    EXPECT_FALSE(relation.contains("function"));
  }
  EXPECT_FALSE(json["relations"][1]["plan"].contains("identity_domain"));
  EXPECT_TRUE(Catalog::from_snapshot(snapshot).ok());
}

TEST(FunctionCatalog, CaptureRequiresAnExplicitDomainAndExactlyOneNonpartitionStringId) {
  auto catalog = take(Catalog::create());
  const auto before = take(catalog->snapshot()).fingerprint;
  for (const auto* ddl :
       {"CREATE TABLE bad(k STRING, PRIMARY KEY(k)) PARTITION BY(k)",
        "CREATE TABLE bad(k STRING, request_id INT64, PRIMARY KEY(k,request_id)) PARTITION BY(k)",
        "CREATE TABLE bad(k STRING, request_id STRING, extra INT64, PRIMARY "
        "KEY(k,request_id,extra)) PARTITION BY(k)",
        "CREATE TABLE bad(k STRING, request_id STRING, PRIMARY KEY(k,request_id)) PARTITION "
        "BY(k,request_id)"})
    EXPECT_FALSE(catalog->define_sql(ddl, capture_options()).ok()) << ddl;
  const std::string valid =
      "CREATE TABLE good(k STRING, request_id STRING, PRIMARY KEY(k,request_id)) PARTITION BY(k)";
  EXPECT_FALSE(catalog->define_sql(valid, capture_options("")).ok());
  DefinitionOptions options;
  options.identity_domain = "forged";
  EXPECT_FALSE(catalog->define_sql(valid, options).ok());
  options.base_write_role = BaseWriteRole::RuntimeStatus;
  EXPECT_FALSE(catalog->define_sql(valid, options).ok());
  options.identity_domain.clear();
  options.base_write_role = static_cast<BaseWriteRole>(99);
  EXPECT_FALSE(catalog->define_sql(valid, options).ok());
  EXPECT_EQ(take(catalog->snapshot()).fingerprint, before);
  EXPECT_EQ(define_capture(*catalog, "capture"), 1U);
}

TEST(FunctionCatalog, SameDomainCannotDisguiseDifferentIdentityNamesOrPartitionOrder) {
  auto catalog = take(Catalog::create());
  define_capture(*catalog, "capture");
  EXPECT_FALSE(catalog
                   ->define_sql("CREATE TABLE news(account STRING, news_id STRING, PRIMARY "
                                "KEY(account,news_id)) PARTITION BY(account)",
                                capture_options())
                   .ok());
  EXPECT_FALSE(catalog
                   ->define_sql("CREATE TABLE wrong(account INT64, request_id STRING, PRIMARY "
                                "KEY(account,request_id)) PARTITION BY(account)",
                                capture_options())
                   .ok());
  auto options = capture_options("composite_requests");
  take(catalog->define_sql("CREATE TABLE composite(region INT64, subject STRING, request STRING, "
                           "PRIMARY KEY(request,subject,region)) PARTITION BY(subject,region)",
                           options));
  EXPECT_FALSE(
      catalog
          ->define_sql("CREATE TABLE reordered(region INT64, subject STRING, request STRING, "
                       "PRIMARY KEY(request,subject,region)) PARTITION BY(region,subject)",
                       options)
          .ok());
}

TEST(FunctionCatalog, CaptureSqlFunctionSqlRemainOneGraphAndPreserveHiddenRequestIdentity) {
  auto catalog = take(Catalog::create());
  const auto capture = define_capture(*catalog, "capture");
  const auto market = define_capture(*catalog, "market", "price FLOAT64");
  const auto filtered = take(catalog->define_sql(
      "CREATE MATERIALIZED VIEW filtered AS SELECT text FROM capture WHERE text IS NOT NULL"));
  auto descriptor = take(catalog->snapshot()).relations.back();
  EXPECT_EQ(descriptor->identity_domain, "prediction_requests");
  EXPECT_EQ(descriptor->partition_visible_slots, (std::vector<std::size_t>{kNoSlot}));
  EXPECT_EQ(descriptor->identity_fields.size(), 2U);
  auto embedding = function_definition(take(catalog->snapshot()), "embedding", {filtered});
  embedding.output_fields = {{1, "vector", DataType::FloatVector, false}};
  const auto embedding_id = take(catalog->define_function(embedding));
  const auto projected = take(
      catalog->define_sql("CREATE MATERIALIZED VIEW vector_only AS SELECT vector FROM embedding"));
  const auto score = take(catalog->define_function(
      function_definition(take(catalog->snapshot()), "score", {projected, market})));
  const auto final = take(catalog->define_sql("CREATE MATERIALIZED VIEW doubled AS SELECT account, "
                                              "request_id, score*2.0 AS value FROM score"));
  const auto snapshot = take(catalog->snapshot());
  EXPECT_EQ(take(dependencies(*take(snapshot.find(capture)))), (std::vector<RelationId>{}));
  EXPECT_EQ(take(dependencies(*take(snapshot.find(filtered)))), (std::vector<RelationId>{capture}));
  EXPECT_EQ(take(dependencies(*take(snapshot.find(embedding_id)))),
            (std::vector<RelationId>{filtered}));
  EXPECT_EQ(take(dependencies(*take(snapshot.find(score)))),
            (std::vector<RelationId>{projected, market}));
  EXPECT_EQ(take(dependencies(*take(snapshot.find(final)))), (std::vector<RelationId>{score}));
  const auto plan = take(catalog->bind_query_sql("SELECT value FROM doubled WHERE account='desk'"));
  EXPECT_EQ(plan.identity_domain, "prediction_requests");
  ASSERT_TRUE(plan.inferred_scope);
  EXPECT_EQ(*plan.inferred_scope, (Row{std::string("desk")}));
  EXPECT_TRUE(Catalog::from_snapshot(snapshot).ok());
  const auto explanation = take(catalog->explain(score));
  EXPECT_NE(explanation.find("FunctionView"), std::string::npos);
  EXPECT_NE(explanation.find("builtin.score.v1"), std::string::npos);
}

TEST(FunctionCatalog, GroupingDropsRequestDomainAndCannotFeedRequestFunction) {
  auto catalog = take(Catalog::create());
  define_capture(*catalog, "capture");
  const auto grouped =
      take(catalog->define_sql("CREATE MATERIALIZED VIEW counts AS SELECT account, COUNT(*) AS "
                               "count FROM capture GROUP BY account"));
  const auto snapshot = take(catalog->snapshot());
  const auto* relation = take(snapshot.find(grouped));
  EXPECT_TRUE(relation->identity_domain.empty());
  EXPECT_TRUE(relation->identity_fields.empty());
  EXPECT_FALSE(catalog->define_function(function_definition(snapshot, "bad", {grouped})).ok());
}

TEST(FunctionCatalog, FunctionRejectsPublicRowsAndDifferentRequestDomainsDespiteEqualTypes) {
  auto catalog = take(Catalog::create());
  const auto capture = define_capture(*catalog, "capture");
  const auto news =
      take(catalog->define_sql("CREATE TABLE news(account STRING, news_id STRING, text STRING, "
                               "PRIMARY KEY(account,news_id)) PARTITION BY(account)"));
  const auto other =
      take(catalog->define_sql("CREATE TABLE other(account STRING, request_id STRING, text STRING, "
                               "PRIMARY KEY(account,request_id)) PARTITION BY(account)",
                               capture_options("other_requests")));
  const auto snapshot = take(catalog->snapshot());
  EXPECT_EQ(take(snapshot.find(capture))->identity_types,
            take(snapshot.find(news))->identity_types);
  EXPECT_FALSE(
      catalog->define_function(function_definition(snapshot, "public_input", {news})).ok());
  EXPECT_FALSE(
      catalog->define_function(function_definition(snapshot, "mixed", {capture, other})).ok());
  EXPECT_FALSE(
      catalog->define_function(function_definition(snapshot, "public_mixed", {capture, news}))
          .ok());
}

TEST(FunctionCatalog, InputAliasesAndExactSignaturesAreValidatedAtomically) {
  auto catalog = take(Catalog::create());
  const auto capture = define_capture(*catalog, "capture");
  const auto snapshot = take(catalog->snapshot());
  const auto valid = function_definition(snapshot, "function", {capture, capture});
  for (int mutation = 0; mutation < 7; ++mutation) {
    auto definition = valid;
    if (mutation == 0)
      definition.inputs[1].alias = definition.inputs[0].alias;
    if (mutation == 1)
      definition.inputs[0].fields[0].type = DataType::Int64;
    if (mutation == 2)
      definition.inputs[0].fields[0].nullable = true;
    if (mutation == 3)
      definition.inputs[0].fields[0].name = "other";
    if (mutation == 4)
      definition.inputs[0].fields[0].id = 99;
    if (mutation == 5)
      definition.inputs[0].fields.pop_back();
    if (mutation == 6)
      definition.inputs[0].relation = 999;
    EXPECT_FALSE(catalog->define_function(definition).ok()) << mutation;
    EXPECT_EQ(take(catalog->snapshot()).fingerprint, snapshot.fingerprint);
  }
  auto optional = valid;
  optional.inputs[1].required = false;
  const auto id = take(catalog->define_function(optional));
  EXPECT_EQ(take(dependencies(*take(take(catalog->snapshot()).find(id)))),
            (std::vector<RelationId>{capture}));
}

TEST(FunctionCatalog, OutputPayloadCannotForgeIdentityOrInvalidSchema) {
  auto catalog = take(Catalog::create());
  const auto capture = define_capture(*catalog, "capture");
  const auto valid = function_definition(take(catalog->snapshot()), "function", {capture});
  for (int mutation = 0; mutation < 7; ++mutation) {
    auto definition = valid;
    if (mutation == 0)
      definition.output_fields[0].name = "account";
    if (mutation == 1)
      definition.output_fields[0].name = "request_id";
    if (mutation == 2)
      definition.output_fields[0].name = "__ef_hidden";
    if (mutation == 3)
      definition.output_fields[0].id = 2;
    if (mutation == 4)
      definition.output_fields[0].type = static_cast<DataType>(99);
    if (mutation == 5)
      definition.output_fields.push_back({2, "score", DataType::Float64, true});
    if (mutation == 6)
      definition.output_fields.clear();
    EXPECT_FALSE(catalog->define_function(definition).ok()) << mutation;
  }
}

TEST(FunctionCatalog, FunctionVersionIdentityAndMetadataBoundsAreExplicit) {
  auto catalog = take(Catalog::create());
  const auto capture = define_capture(*catalog, "capture");
  const auto valid = function_definition(take(catalog->snapshot()), "function", {capture});
  for (int mutation = 0; mutation < 6; ++mutation) {
    auto definition = valid;
    if (mutation == 0)
      definition.function_id.clear();
    if (mutation == 1)
      definition.function_version = 0;
    if (mutation == 2)
      definition.implementation_identity.clear();
    if (mutation == 3)
      definition.inputs.clear();
    if (mutation == 4)
      definition.inputs.resize(33, definition.inputs[0]);
    if (mutation == 5)
      definition.output_fields.resize(1025, definition.output_fields[0]);
    EXPECT_FALSE(catalog->define_function(definition).ok()) << mutation;
  }
  take(catalog->define_function(valid));
  EXPECT_EQ(catalog->define_function(valid).status().code(), ErrorCode::Conflict);
  auto changed = valid;
  changed.name = "function_v2";
  changed.function_version = 2;
  changed.implementation_identity = "builtin.function.v2";
  take(catalog->define_function(changed));
  const auto snapshot = take(catalog->snapshot());
  EXPECT_TRUE(Catalog::from_snapshot(snapshot).ok());
  EXPECT_EQ(snapshot.relations[1]->function->definition.function_version, 1U);
  EXPECT_EQ(snapshot.relations[2]->function->definition.function_version, 2U);
}

TEST(FunctionCatalog, OneFunctionVersionHasOneSignatureRegardlessOfInputRelationIds) {
  auto catalog = take(Catalog::create());
  const auto first = define_capture(*catalog, "first");
  const auto second = define_capture(*catalog, "second");
  const auto numeric = define_capture(*catalog, "numeric", "text FLOAT64");
  const auto valid = function_definition(take(catalog->snapshot()), "function", {first});
  take(catalog->define_function(valid));
  auto reused = valid;
  reused.name = "compatible";
  reused.inputs[0].relation = second;
  EXPECT_TRUE(catalog->define_function(reused).ok());
  const auto before = take(catalog->snapshot());
  for (int mutation = 0; mutation < 5; ++mutation) {
    auto conflicting = valid;
    conflicting.name = "conflict_" + std::to_string(mutation);
    if (mutation == 0)
      conflicting.implementation_identity = "different-implementation";
    if (mutation == 1)
      conflicting.inputs[0].alias = "different_alias";
    if (mutation == 2)
      conflicting.inputs[0].required = false;
    if (mutation == 3)
      conflicting.output_fields[0].type = DataType::String;
    if (mutation == 4) {
      conflicting.inputs[0].relation = numeric;
      const auto* source = take(before.find(numeric));
      conflicting.inputs[0].fields = source->storage_schema.fields;
    }
    EXPECT_FALSE(catalog->define_function(conflicting).ok()) << mutation;
    EXPECT_TRUE(take(catalog->snapshot()).fingerprint == before.fingerprint);
  }
  EXPECT_TRUE(Catalog::from_snapshot(take(catalog->snapshot())).ok());
}

TEST(FunctionCatalog, SnapshotRejectsContradictoryKindsDomainsAndFunctionProvenance) {
  auto catalog = take(Catalog::create());
  const auto capture = define_capture(*catalog, "capture");
  take(catalog->define_function(
      function_definition(take(catalog->snapshot()), "function", {capture})));
  const auto original = take(catalog->snapshot());
  for (int mutation = 0; mutation < 7; ++mutation) {
    auto snapshot = original;
    auto relation = std::make_shared<RelationDescriptor>(*snapshot.relations[1]);
    auto function = std::make_shared<FunctionViewPlan>(*relation->function);
    relation->function = function;
    snapshot.relations[1] = relation;
    if (mutation == 0)
      relation->kind = RelationKind::BaseTable;
    if (mutation == 1)
      relation->kind = RelationKind::SqlView;
    if (mutation == 2)
      relation->plan = std::make_shared<BoundPlan>();
    if (mutation == 3)
      relation->identity_domain = "forged";
    if (mutation == 4)
      function->catalog_fingerprint += "forged";
    if (mutation == 5)
      function->definition.inputs[0].relation = relation->id;
    if (mutation == 6)
      relation->base_write_role = BaseWriteRole::RequestCapture;
    EXPECT_FALSE(Catalog::from_snapshot(snapshot).ok()) << mutation;
  }
  EXPECT_TRUE(Catalog::from_snapshot(original).ok());
}

TEST(FunctionCatalog, RestoredFunctionOwnsNestedDefinitionAndPublicPlanDomainIsValidated) {
  auto catalog = take(Catalog::create());
  const auto capture = define_capture(*catalog, "capture");
  take(catalog->define_function(
      function_definition(take(catalog->snapshot()), "function", {capture})));
  auto snapshot = take(catalog->snapshot());
  auto relation = std::make_shared<RelationDescriptor>(*snapshot.relations[1]);
  auto function = std::make_shared<FunctionViewPlan>(*relation->function);
  relation->function = function;
  snapshot.relations[1] = relation;
  auto restored = take(Catalog::from_snapshot(snapshot));
  function->definition.inputs[0].fields[0].name = "mutated";
  function->definition.output_fields[0].name = "mutated";
  const auto owned = take(restored->snapshot());
  EXPECT_EQ(owned.relations[1]->function->definition.inputs[0].fields[0].name, "account");
  EXPECT_EQ(owned.relations[1]->function->definition.output_fields[0].name, "score");
  auto plan = take(restored->bind_query_sql("SELECT score FROM function WHERE account='a'"));
  EXPECT_TRUE(validate_bound_plan(plan, owned).ok());
  plan.identity_domain = "forged";
  EXPECT_FALSE(validate_bound_plan(plan, owned).ok());
}

TEST(FunctionCatalog, ForgedMatchingFingerprintCannotChangeTheBoundInputSignature) {
  auto catalog = take(Catalog::create());
  const auto capture = define_capture(*catalog, "capture");
  take(catalog->define_function(
      function_definition(take(catalog->snapshot()), "function", {capture})));
  auto snapshot = take(catalog->snapshot());
  auto relation = std::make_shared<RelationDescriptor>(*snapshot.relations[1]);
  auto function = std::make_shared<FunctionViewPlan>(*relation->function);
  function->definition.inputs[0].fields[2].type = DataType::Int64;
  relation->function = function;
  snapshot.relations[1] = relation;
  auto fingerprint = take(parse_json_exact(snapshot.fingerprint));
  fingerprint["relations"][1]["function"]["definition"]["inputs"][0]["fields"][2]["type"] =
      data_type_name(DataType::Int64);
  snapshot.fingerprint = fingerprint.dump();
  const auto restored = Catalog::from_snapshot(snapshot);
  ASSERT_FALSE(restored.ok());
  EXPECT_NE(restored.status().message().find("signature"), std::string_view::npos);
}

TEST(FunctionCatalog, CompositeBusinessKeyNamesAreDerivedAndRequestIdDoesNotBecomePartition) {
  auto catalog = take(Catalog::create());
  const auto capture = take(catalog->define_sql(
      "CREATE TABLE events(region INT64, subject STRING, request STRING, payload FLOAT_VECTOR, "
      "PRIMARY KEY(request,region,subject)) PARTITION BY(subject,region)",
      capture_options("composite_requests")));
  const auto function = take(catalog->define_function(
      function_definition(take(catalog->snapshot()), "function", {capture})));
  const auto snapshot = take(catalog->snapshot());
  const auto* relation = take(snapshot.find(function));
  ASSERT_EQ(relation->identity_fields.size(), 3U);
  EXPECT_EQ(relation->identity_fields[0].name, "subject");
  EXPECT_EQ(relation->identity_fields[1].name, "region");
  EXPECT_EQ(relation->identity_fields[2].name, "request");
  EXPECT_EQ(relation->partition_types, (std::vector<DataType>{DataType::String, DataType::Int64}));
  EXPECT_EQ(relation->physical_key_slots.size(), 3U);
  EXPECT_EQ(relation->partition_visible_slots, (std::vector<std::size_t>{0, 1}));
  auto plan = take(catalog->bind_query_sql(
      "SELECT request, score FROM function WHERE subject='sub' AND region=7"));
  EXPECT_EQ(*plan.inferred_scope, (Row{std::string("sub"), std::int64_t{7}}));
  EXPECT_TRUE(Catalog::from_snapshot(snapshot).ok());
}

TEST(FunctionCatalog, ExistingRequestDomainCannotBeAttachedToOrdinarySqlViaOptions) {
  auto catalog = take(Catalog::create());
  define_capture(*catalog, "capture");
  EXPECT_FALSE(catalog
                   ->define_sql("CREATE MATERIALIZED VIEW bad AS SELECT text FROM capture",
                                capture_options())
                   .ok());
  DefinitionOptions status;
  status.base_write_role = BaseWriteRole::RuntimeStatus;
  const auto id = take(catalog->define_sql(
      "CREATE TABLE runtime(k STRING, state INT64, PRIMARY KEY(k)) PARTITION BY(k)", status));
  const auto snapshot = take(catalog->snapshot());
  EXPECT_TRUE(take(snapshot.find(id))->identity_domain.empty());
  EXPECT_TRUE(Catalog::from_snapshot(snapshot).ok());
}

} // namespace
} // namespace eventflow
