#include "eventflow/catalog.hpp"
#include "eventflow/request_machine.hpp"
#include "status_test_support.hpp"
#include <gtest/gtest.h>

namespace eventflow {
namespace {
using test::take;

TEST(LatestCatalog, CandidateCanonicalizesScopeAndPayloadWithRuntimeOwnership) {
  auto catalog = take(Catalog::create());
  auto defined = catalog->define_candidate({"candidate",
                                            {{17, "account", DataType::String, false}},
                                            {{42, "news", DataType::String, true}}});
  ASSERT_TRUE(defined.ok()) << defined.status().message();
  const auto snapshot = take(catalog->snapshot());
  const auto* relation = take(snapshot.find(*defined));
  EXPECT_EQ(relation->kind, RelationKind::BaseTable);
  EXPECT_EQ(relation->base_write_role, BaseWriteRole::RuntimeStatus);
  ASSERT_TRUE(relation->candidate);
  EXPECT_EQ(relation->candidate->format, 1U);
  EXPECT_TRUE(relation->identity_domain.empty());
  EXPECT_TRUE(relation->identity_fields.empty());
  EXPECT_TRUE(relation->unique_per_partition);
  EXPECT_EQ(relation->physical_key_slots, (std::vector<std::size_t>{0}));
  EXPECT_EQ(relation->partition_slots, (std::vector<std::size_t>{0}));
  EXPECT_EQ(relation->visible_count, 4U);
  EXPECT_EQ(relation->storage_schema.fields[1].name, "candidate_epoch");
  EXPECT_EQ(relation->storage_schema.fields[2].name, "active");
  EXPECT_EQ(relation->storage_schema.fields[3].id, 4U);
  EXPECT_EQ(relation->candidate->definition.scope_fields[0].id, 1U);
  EXPECT_EQ(relation->candidate->definition.payload_fields[0].id, 4U);
  EXPECT_TRUE(Catalog::from_snapshot(snapshot).ok());
}

struct LatestFixture {
  std::unique_ptr<Catalog> catalog = take(Catalog::create());
  RelationId candidate = 0;
  RelationId status = 0;
  RelationId capture = 0;
  RelationId result = 0;

  LatestFixture() noexcept {
    candidate = take(catalog->define_candidate({"candidate",
                                                {{7, "account", DataType::String, false}},
                                                {{8, "news", DataType::String, true}}}));
    status = take(catalog->define_request_status(
        {"request_status",
         "requests",
         {{11, "account", DataType::String, false}, {13, "request_id", DataType::String, false}},
         candidate}));
    DefinitionOptions options;
    options.base_write_role = BaseWriteRole::RequestCapture;
    options.identity_domain = "requests";
    capture = take(
        catalog->define_sql("CREATE TABLE capture(account STRING, request_id STRING, text STRING, "
                            "PRIMARY KEY(account,request_id)) PARTITION BY(account)",
                            options));
    const auto snapshot = take(catalog->snapshot());
    const auto* input = take(snapshot.find(capture));
    FunctionViewDefinition definition;
    definition.name = "result";
    definition.function_id = "builtin.score";
    definition.function_version = 1;
    definition.implementation_identity = "builtin.score.v1";
    definition.inputs = {{"captured",
                          capture,
                          true,
                          {input->storage_schema.fields.begin(),
                           input->storage_schema.fields.begin() + input->visible_count}}};
    definition.output_fields = {{1, "score", DataType::Float64, true}};
    result = take(catalog->define_function(definition));
  }

  RelationId latest(std::string name = "latest") noexcept {
    return take(catalog->define_latest({std::move(name), result, status, candidate}));
  }
};

TEST(LatestCatalog, RequestStatusHasCanonicalCompleteIdentityWithoutRequestLineage) {
  LatestFixture fixture;
  const auto snapshot = take(fixture.catalog->snapshot());
  const auto* relation = take(snapshot.find(fixture.status));
  ASSERT_TRUE(relation->request_status);
  EXPECT_EQ(relation->base_write_role, BaseWriteRole::RuntimeStatus);
  EXPECT_EQ(relation->request_status->definition.request_domain, "requests");
  EXPECT_TRUE(relation->identity_domain.empty());
  EXPECT_TRUE(relation->identity_fields.empty());
  EXPECT_FALSE(relation->unique_per_partition);
  EXPECT_EQ(relation->physical_key_slots, (std::vector<std::size_t>{0, 1}));
  EXPECT_EQ(relation->partition_slots, (std::vector<std::size_t>{0}));
  EXPECT_EQ(relation->visible_count, 5U);
  EXPECT_EQ(relation->storage_schema.fields[2].name, "candidate_epoch");
  EXPECT_EQ(relation->storage_schema.fields[3].name, "admission_seq");
  EXPECT_EQ(relation->storage_schema.fields[4].name, "phase");
  EXPECT_EQ(take(dependencies(*relation)), (std::vector<RelationId>{fixture.candidate}));
  const auto query = take(fixture.catalog->bind_query_sql("SELECT * FROM request_status"));
  EXPECT_TRUE(query.identity_domain.empty());
}

TEST(LatestCatalog, LatestCopiesVisibleResultAndUsesOnlyScopeAsPhysicalIdentity) {
  LatestFixture fixture;
  const auto id = fixture.latest();
  const auto snapshot = take(fixture.catalog->snapshot());
  const auto* latest = take(snapshot.find(id));
  EXPECT_EQ(latest->kind, RelationKind::LatestView);
  ASSERT_TRUE(latest->latest);
  EXPECT_FALSE(latest->plan);
  EXPECT_FALSE(latest->function);
  EXPECT_EQ(latest->visible_count, 5U);
  EXPECT_EQ(latest->storage_schema.fields.size(), 6U);
  EXPECT_EQ(latest->storage_schema.fields[1].name, "request_id");
  EXPECT_TRUE(latest->storage_schema.fields[2].nullable);
  EXPECT_EQ(latest->physical_key_slots, (std::vector<std::size_t>{5}));
  EXPECT_EQ(latest->partition_slots, latest->physical_key_slots);
  EXPECT_EQ(latest->identity_types, latest->partition_types);
  EXPECT_EQ(latest->state_id, id);
  EXPECT_TRUE(latest->unique_per_partition);
  EXPECT_TRUE(latest->identity_domain.empty());
  EXPECT_TRUE(latest->identity_fields.empty());
  EXPECT_EQ(take(dependencies(*latest)),
            (std::vector<RelationId>{fixture.result, fixture.status, fixture.candidate}));
  const Row visible{std::string("a"), std::string("r"), 0.5, std::int64_t(7), std::int64_t(11)};
  const auto stored = take(make_stored_row(*latest, visible, Row{std::string("a")}));
  EXPECT_EQ(take(visible_row(*latest, stored)), visible);
  EXPECT_EQ(take(physical_identity(*latest, stored)), (Row{std::string("a")}));
  EXPECT_FALSE(make_stored_row(*latest, visible, Row{std::string("b")}).ok());
  EXPECT_FALSE(make_stored_row(*latest, visible, Row{std::string("a"), std::string("r")}).ok());
}

TEST(LatestCatalog, HiddenRequestIdentitySurvivesSourceSqlButLatestDoesNotPropagateItsDomain) {
  LatestFixture fixture;
  const auto projected = take(fixture.catalog->define_sql(
      "CREATE MATERIALIZED VIEW projected AS SELECT score FROM result"));
  const auto id = take(
      fixture.catalog->define_latest({"latest", projected, fixture.status, fixture.candidate}));
  const auto downstream = take(fixture.catalog->define_sql(
      "CREATE MATERIALIZED VIEW downstream AS SELECT score, __ef_latest_candidate_epoch, "
      "__ef_latest_admission_seq FROM latest WHERE __ef_latest_admission_seq > 1"));
  const auto snapshot = take(fixture.catalog->snapshot());
  const auto* latest = take(snapshot.find(id));
  EXPECT_EQ(latest->partition_visible_slots, (std::vector<std::size_t>{kNoSlot}));
  EXPECT_EQ(latest->visible_count, 3U);
  EXPECT_EQ(take(make_stored_row(*latest, Row{0.7, std::int64_t(3), std::int64_t(12)},
                                 Row{std::string("a")}))
                .size(),
            4U);
  const auto* derived = take(snapshot.find(downstream));
  EXPECT_TRUE(derived->identity_domain.empty());
  EXPECT_EQ(derived->identity_types, latest->partition_types);
  EXPECT_TRUE(Catalog::from_snapshot(snapshot).ok());
}

TEST(LatestCatalog, GeneratedProvenanceSupportsStarAliasesFiltersAndChainedSql) {
  LatestFixture fixture;
  fixture.latest();
  take(fixture.catalog->define_sql("CREATE MATERIALIZED VIEW copy AS SELECT * FROM latest"));
  const auto plan = take(fixture.catalog->bind_query_sql(
      "SELECT __ef_latest_candidate_epoch AS epoch, __ef_latest_admission_seq AS seq "
      "FROM copy WHERE account='a'"));
  EXPECT_EQ(plan.output_fields[0].name, "epoch");
  EXPECT_TRUE(plan.inferred_scope);
  EXPECT_TRUE(validate_bound_plan(plan, take(fixture.catalog->snapshot())).ok());
  EXPECT_FALSE(fixture.catalog->bind_query_sql("SELECT __ef_key_0 FROM latest").ok());
  EXPECT_FALSE(
      fixture.catalog->bind_query_sql("SELECT score AS __ef_latest_admission_seq FROM latest")
          .ok());
  EXPECT_FALSE(
      fixture.catalog->bind_query_sql("SELECT 1 AS __ef_latest_candidate_epoch FROM latest").ok());
  EXPECT_FALSE(fixture.catalog
                   ->define_sql("CREATE TABLE forged(k STRING, __ef_latest_admission_seq INT64, "
                                "PRIMARY KEY(k)) PARTITION BY(k)")
                   .ok());
}

TEST(LatestCatalog, ScopeLookupJoinCanReadLatestAndStillHasNoRequestLineage) {
  LatestFixture fixture;
  fixture.latest();
  const auto query = take(fixture.catalog->bind_query_sql(
      "SELECT l.account, l.score, c.news FROM latest l JOIN candidate c ON l.account=c.account"));
  EXPECT_TRUE(query.right);
  EXPECT_TRUE(query.unique_per_partition);
  EXPECT_TRUE(query.identity_domain.empty());
  EXPECT_TRUE(validate_bound_plan(query, take(fixture.catalog->snapshot())).ok());
}

TEST(LatestCatalog, CandidateRejectsInvalidScopeNamesIdsAndReservedPayload) {
  auto catalog = take(Catalog::create());
  const auto before = take(catalog->snapshot()).fingerprint;
  const std::vector<CandidateDefinition> invalid{
      {"c", {}, {}},
      {"c", {{1, "account", DataType::String, true}}, {}},
      {"c", {{1, "account", DataType::FloatVector, false}}, {}},
      {"c", {{1, "account", DataType::String, false}}, {{1, "text", DataType::String, true}}},
      {"c", {{1, "account", DataType::String, false}}, {{2, "account", DataType::String, true}}},
      {"c", {{1, "candidate_epoch", DataType::Int64, false}}, {}},
      {"c", {{1, "account", DataType::String, false}}, {{2, "active", DataType::Bool, false}}},
      {"c",
       {{1, "account", DataType::String, false}},
       {{2, "__EF_forged", DataType::Int64, false}}},
      {"c",
       {{1, "account", DataType::String, false}},
       {{2, "x", static_cast<DataType>(90), false}}}};
  for (const auto& definition : invalid)
    EXPECT_FALSE(catalog->define_candidate(definition).ok());
  EXPECT_EQ(take(catalog->snapshot()).fingerprint, before);
}

TEST(LatestCatalog, StatusRejectsScopeOrIdentityChangesAndDoesNotConsumeIds) {
  LatestFixture fixture;
  const auto before = take(fixture.catalog->snapshot());
  RequestStatusDefinition valid{
      "other_status",
      "requests",
      {{1, "account", DataType::String, false}, {2, "request_id", DataType::String, false}},
      fixture.candidate};
  for (int mutation = 0; mutation < 9; ++mutation) {
    auto definition = valid;
    switch (mutation) {
    case 0:
      definition.identity_fields.pop_back();
      break;
    case 1:
      definition.identity_fields[0].name = "desk";
      break;
    case 2:
      definition.identity_fields[0].nullable = true;
      break;
    case 3:
      definition.identity_fields[1].type = DataType::Int64;
      break;
    case 4:
      definition.identity_fields[1].nullable = true;
      break;
    case 5:
      definition.identity_fields[1].name = "different_request_id";
      break;
    case 6:
      definition.identity_fields[1].id = 1;
      break;
    case 7:
      definition.request_domain.clear();
      break;
    case 8:
      definition.candidate_relation = fixture.capture;
      break;
    }
    EXPECT_FALSE(fixture.catalog->define_request_status(definition).ok()) << mutation;
  }
  EXPECT_EQ(take(fixture.catalog->snapshot()).fingerprint, before.fingerprint);
  EXPECT_EQ(fixture.latest(), before.version + 1);
}

TEST(LatestCatalog, DomainAgreementIsEnforcedWhenStatusPrecedesCapture) {
  auto catalog = take(Catalog::create());
  const auto candidate =
      take(catalog->define_candidate({"candidate", {{1, "account", DataType::String, false}}, {}}));
  take(catalog->define_request_status(
      {"status",
       "requests",
       {{1, "account", DataType::String, false}, {2, "request_id", DataType::String, false}},
       candidate}));
  DefinitionOptions options;
  options.base_write_role = BaseWriteRole::RequestCapture;
  options.identity_domain = "requests";
  EXPECT_FALSE(catalog
                   ->define_sql("CREATE TABLE capture(account STRING, news_id STRING, PRIMARY "
                                "KEY(account,news_id)) PARTITION BY(account)",
                                options)
                   .ok());
}

TEST(LatestCatalog, LatestRejectsPublicCaptureWrongStatusCandidateAndDifferentDomain) {
  LatestFixture fixture;
  const auto other_candidate = take(fixture.catalog->define_candidate(
      {"other_candidate", {{1, "account", DataType::String, false}}, {}}));
  const auto other_status = take(fixture.catalog->define_request_status(
      {"other_status",
       "other_requests",
       {{1, "account", DataType::String, false}, {2, "request_id", DataType::String, false}},
       fixture.candidate}));
  const auto before = take(fixture.catalog->snapshot()).fingerprint;
  const std::vector<LatestViewDefinition> invalid{
      {"bad", fixture.capture, fixture.status, fixture.candidate},
      {"bad", fixture.candidate, fixture.status, fixture.candidate},
      {"bad", fixture.result, fixture.capture, fixture.candidate},
      {"bad", fixture.result, fixture.status, other_candidate},
      {"bad", fixture.result, other_status, fixture.candidate},
      {"bad", fixture.result, fixture.status, fixture.capture},
      {"bad", 999, fixture.status, fixture.candidate}};
  for (const auto& definition : invalid)
    EXPECT_FALSE(fixture.catalog->define_latest(definition).ok());
  EXPECT_EQ(take(fixture.catalog->snapshot()).fingerprint, before);
}

TEST(LatestCatalog, FunctionAndRequestMachineRejectLifecycleAndLatestInputs) {
  LatestFixture fixture;
  const auto latest = fixture.latest();
  const auto sql =
      take(fixture.catalog->define_sql("CREATE MATERIALIZED VIEW derived AS SELECT * FROM latest"));
  const auto snapshot = take(fixture.catalog->snapshot());
  auto registry = take(FunctionRegistry::create());
  for (const auto id : {fixture.candidate, fixture.status, latest, sql}) {
    const auto* input = take(snapshot.find(id));
    auto definition = take(snapshot.find(fixture.result))->function->definition;
    definition.name = "rejected";
    definition.function_id = "builtin.rejected";
    definition.inputs[0].relation = id;
    definition.inputs[0].fields.assign(input->storage_schema.fields.begin(),
                                       input->storage_schema.fields.begin() + input->visible_count);
    EXPECT_FALSE(fixture.catalog->define_function(definition).ok());
    EXPECT_FALSE(RequestMachine::create(snapshot, registry, {id}).ok());
  }
}

TEST(LatestCatalog, SnapshotDeepCloneOwnsAllThreeMetadataPayloads) {
  LatestFixture fixture;
  const auto id = fixture.latest();
  auto snapshot = take(fixture.catalog->snapshot());
  auto candidate =
      std::make_shared<CandidateAnnotation>(*snapshot.relations[fixture.candidate - 1]->candidate);
  auto status = std::make_shared<RequestStatusAnnotation>(
      *snapshot.relations[fixture.status - 1]->request_status);
  auto latest = std::make_shared<LatestViewPlan>(*snapshot.relations[id - 1]->latest);
  auto candidate_relation =
      std::make_shared<RelationDescriptor>(*snapshot.relations[fixture.candidate - 1]);
  auto status_relation =
      std::make_shared<RelationDescriptor>(*snapshot.relations[fixture.status - 1]);
  auto latest_relation = std::make_shared<RelationDescriptor>(*snapshot.relations[id - 1]);
  candidate_relation->candidate = candidate;
  status_relation->request_status = status;
  latest_relation->latest = latest;
  snapshot.relations[fixture.candidate - 1] = candidate_relation;
  snapshot.relations[fixture.status - 1] = status_relation;
  snapshot.relations[id - 1] = latest_relation;
  auto restored = take(Catalog::from_snapshot(snapshot));
  candidate->definition.payload_fields[0].name = "mutated";
  status->definition.request_domain = "mutated";
  latest->definition.result_relation = 0;
  const auto owned = take(restored->snapshot());
  EXPECT_EQ(owned.fingerprint, snapshot.fingerprint);
  EXPECT_EQ(owned.relations[fixture.candidate - 1]->candidate->definition.payload_fields[0].name,
            "news");
  EXPECT_EQ(owned.relations[fixture.status - 1]->request_status->definition.request_domain,
            "requests");
  EXPECT_EQ(owned.relations[id - 1]->latest->definition.result_relation, fixture.result);
  EXPECT_TRUE(Catalog::from_snapshot(owned).ok());
  EXPECT_FALSE(Catalog::from_snapshot(snapshot).ok());
}

TEST(LatestCatalog, SnapshotRejectsKindPointerRoleFormatDomainAndStateTampering) {
  LatestFixture fixture;
  const auto latest_id = fixture.latest();
  const auto clean = take(fixture.catalog->snapshot());
  for (int mutation = 0; mutation < 12; ++mutation) {
    auto snapshot = clean;
    const auto id = mutation < 4 ? fixture.candidate : (mutation < 7 ? fixture.status : latest_id);
    auto relation = std::make_shared<RelationDescriptor>(*snapshot.relations[id - 1]);
    snapshot.relations[id - 1] = relation;
    switch (mutation) {
    case 0:
      relation->base_write_role = BaseWriteRole::PublicInput;
      break;
    case 1:
      relation->request_status = clean.relations[fixture.status - 1]->request_status;
      break;
    case 2: {
      auto metadata = std::make_shared<CandidateAnnotation>(*relation->candidate);
      metadata->format = 2;
      relation->candidate = metadata;
      break;
    }
    case 3:
      relation->candidate.reset();
      break;
    case 4:
      relation->identity_domain = "requests";
      break;
    case 5: {
      auto metadata = std::make_shared<RequestStatusAnnotation>(*relation->request_status);
      metadata->format = 99;
      relation->request_status = metadata;
      break;
    }
    case 6:
      relation->storage_schema.fields.back().nullable = true;
      break;
    case 7:
      relation->state_id = fixture.result;
      break;
    case 8:
      relation->function = clean.relations[fixture.result - 1]->function;
      break;
    case 9:
      relation->kind = RelationKind::SqlView;
      break;
    case 10: {
      auto metadata = std::make_shared<LatestViewPlan>(*relation->latest);
      metadata->format = 2;
      relation->latest = metadata;
      break;
    }
    case 11:
      relation->identity_domain = "requests";
      break;
    }
    EXPECT_FALSE(Catalog::from_snapshot(snapshot).ok()) << mutation;
  }
}

TEST(LatestCatalog, SnapshotRejectsLatestBindingProvenanceAndDependencyTampering) {
  LatestFixture fixture;
  const auto id = fixture.latest();
  const auto clean = take(fixture.catalog->snapshot());
  for (int mutation = 0; mutation < 5; ++mutation) {
    auto snapshot = clean;
    auto relation = std::make_shared<RelationDescriptor>(*snapshot.relations[id - 1]);
    auto plan = std::make_shared<LatestViewPlan>(*relation->latest);
    relation->latest = plan;
    snapshot.relations[id - 1] = relation;
    switch (mutation) {
    case 0:
      plan->catalog_version += 1;
      break;
    case 1:
      plan->catalog_fingerprint += "x";
      break;
    case 2:
      plan->definition.result_relation = id;
      break;
    case 3:
      plan->definition.candidate_relation = fixture.status;
      break;
    case 4:
      plan->definition.name = "different";
      break;
    }
    EXPECT_FALSE(Catalog::from_snapshot(snapshot).ok()) << mutation;
  }
}

TEST(LatestCatalog, LifecycleRowsValidatePositiveEpochSequenceAndFixedPhase) {
  LatestFixture fixture;
  const auto id = fixture.latest();
  const auto snapshot = take(fixture.catalog->snapshot());
  const auto* candidate = take(snapshot.find(fixture.candidate));
  const auto* status = take(snapshot.find(fixture.status));
  const auto* latest = take(snapshot.find(id));
  EXPECT_TRUE(make_stored_row(*candidate,
                              Row{std::string("a"), std::int64_t(1), false, std::monostate{}},
                              Row{std::string("a")})
                  .ok());
  EXPECT_FALSE(make_stored_row(*candidate,
                               Row{std::string("a"), std::int64_t(0), true, std::monostate{}},
                               Row{std::string("a")})
                   .ok());
  for (const auto* phase : {"ACCEPTED", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED"})
    EXPECT_TRUE(make_stored_row(*status,
                                Row{std::string("a"), std::string("r"), std::int64_t(1),
                                    std::int64_t(9), std::string(phase)},
                                Row{std::string("a"), std::string("r")})
                    .ok());
  EXPECT_FALSE(make_stored_row(*status,
                               Row{std::string("a"), std::string("r"), std::int64_t(1),
                                   std::int64_t(9), std::string("completed")},
                               Row{std::string("a"), std::string("r")})
                   .ok());
  EXPECT_FALSE(make_stored_row(
                   *latest,
                   Row{std::string("a"), std::string("r"), 0.3, std::int64_t(1), std::int64_t(-1)},
                   Row{std::string("a")})
                   .ok());
  EXPECT_FALSE(make_stored_row(*latest,
                               Row{std::string("a"), std::string("r"), 0.3, true, std::int64_t(9)},
                               Row{std::string("a")})
                   .ok());
}

TEST(LatestCatalog, RowHelpersRejectForgedLifecycleMetadata) {
  LatestFixture fixture;
  const auto id = fixture.latest();
  const auto snapshot = take(fixture.catalog->snapshot());
  auto candidate = *take(snapshot.find(fixture.candidate));
  candidate.storage_schema.fields[1].type = DataType::String;
  EXPECT_FALSE(make_stored_row(candidate,
                               Row{std::string("a"), std::string("1"), true, std::monostate{}},
                               Row{std::string("a")})
                   .ok());
  auto latest = *take(snapshot.find(id));
  latest.storage_schema.fields[3].type = DataType::Bool;
  EXPECT_FALSE(make_stored_row(latest,
                               Row{std::string("a"), std::string("r"), 0.3, true, std::int64_t(9)},
                               Row{std::string("a")})
                   .ok());
  latest = *take(snapshot.find(id));
  latest.request_status = take(snapshot.find(fixture.status))->request_status;
  EXPECT_FALSE(dependencies(latest).ok());
}

TEST(LatestCatalog, ExplainAndFingerprintIncludeLifecycleDefinitionsAndSelectorInputs) {
  LatestFixture fixture;
  const auto id = fixture.latest();
  const auto json = take(parse_json_exact(take(fixture.catalog->snapshot()).fingerprint));
  EXPECT_EQ(json["relations"][0]["candidate"]["format"], 1);
  EXPECT_EQ(json["relations"][1]["request_status"]["request_domain"], "requests");
  EXPECT_EQ(json["relations"][id - 1]["kind"], "latest_view");
  EXPECT_EQ(json["relations"][id - 1]["latest"]["definition"]["result_relation"], fixture.result);
  EXPECT_FALSE(json["relations"][fixture.result - 1].contains("latest"));
  EXPECT_FALSE(json["relations"][fixture.result - 1].contains("candidate"));
  const auto explain = take(parse_json_exact(take(fixture.catalog->explain(id))));
  EXPECT_TRUE(explain["has_internal_state"].get<bool>());
  EXPECT_EQ(explain["operators"].back(), "LatestSelector");
  EXPECT_EQ(explain["dependencies"],
            (Json::array({fixture.result, fixture.status, fixture.candidate})));
  EXPECT_EQ(take(parse_json_exact(take(fixture.catalog->explain(fixture.status))))["dependencies"],
            Json::array({fixture.candidate}));
}

TEST(LatestCatalog, LatestPreservesEveryVisibleScopeAliasForLookupJoins) {
  LatestFixture fixture;
  const auto source =
      take(fixture.catalog->define_sql("CREATE MATERIALIZED VIEW aliased AS SELECT account AS "
                                       "first_scope, account AS second_scope, score FROM result"));
  take(fixture.catalog->define_latest({"latest", source, fixture.status, fixture.candidate}));
  auto joined = fixture.catalog->bind_query_sql(
      "SELECT l.score, c.news FROM latest l JOIN candidate c ON l.second_scope=c.account");
  ASSERT_TRUE(joined.ok()) << joined.status().message();
  EXPECT_TRUE(validate_bound_plan(*joined, take(fixture.catalog->snapshot())).ok());
  const auto inferred =
      take(fixture.catalog->bind_query_sql("SELECT score FROM latest WHERE second_scope='a'"));
  EXPECT_EQ(inferred.inferred_scope, std::optional<Row>(Row{std::string("a")}));
}
} // namespace
} // namespace eventflow
