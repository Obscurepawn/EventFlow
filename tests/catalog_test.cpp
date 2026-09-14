#include "eventflow/catalog.hpp"
#include <gtest/gtest.h>
#include <unistd.h>

namespace eventflow {
namespace {
CreateTableAst fills() {
  return {"fills",
          {{"account", DataType::String, false},
           {"id", DataType::Int64, {}},
           {"quantity", DataType::Int64, {}},
           {"price", DataType::Float64, {}},
           {"stamp", DataType::TimestampMicros, false}},
          {"id", "account"},
          {"account"}};
}

TEST(Catalog, BaseTableKeepsDeclaredSchemaButPlacesPartitionFirstInPhysicalKey) {
  auto catalog = Catalog::create();
  ASSERT_TRUE(catalog.ok());
  auto id = (*catalog)->define(StatementAst(fills()));
  ASSERT_TRUE(id.ok()) << id.status().message();
  auto snapshot = (*catalog)->snapshot();
  ASSERT_TRUE(snapshot.ok());
  EXPECT_EQ(snapshot->version, 1u);
  auto relation = snapshot->find(*id);
  ASSERT_TRUE(relation.ok());
  EXPECT_EQ((*relation)->physical_key_slots, (std::vector<std::size_t>{0, 1}));
  EXPECT_EQ((*relation)->storage_schema.primary_key, (std::vector<std::string>{"id", "account"}));
  EXPECT_FALSE((*relation)->storage_schema.fields[1].nullable);
  EXPECT_TRUE((*relation)->storage_schema.fields[2].nullable);
  EXPECT_FALSE((*relation)->unique_per_partition);
}

TEST(Catalog, DefinitionFailureIsAtomicAndOptionsCannotBeSilentlyIgnored) {
  auto catalog = Catalog::create();
  ASSERT_TRUE(catalog.ok());
  auto before = (*catalog)->snapshot();
  ASSERT_TRUE(before.ok());
  auto invalid = fills();
  invalid.partition_key.clear();
  EXPECT_FALSE((*catalog)->define(StatementAst(invalid)).ok());
  invalid = fills();
  invalid.columns[1].nullable = true;
  EXPECT_FALSE((*catalog)->define(StatementAst(invalid)).ok());
  invalid = fills();
  invalid.columns[0].name = "__ef_secret";
  EXPECT_FALSE((*catalog)->define(StatementAst(invalid)).ok());
  invalid = fills();
  invalid.name = std::string("bad\xff", 4);
  EXPECT_FALSE((*catalog)->define(StatementAst(invalid)).ok());
  EXPECT_FALSE((*catalog)->define(StatementAst(fills()), DefinitionOptions{{"id"}}).ok());
  auto after = (*catalog)->snapshot();
  ASSERT_TRUE(after.ok());
  EXPECT_EQ(before->fingerprint, after->fingerprint);
  invalid = fills();
  invalid.partition_key.clear();
  auto id = (*catalog)->define(StatementAst(invalid), DefinitionOptions{{"account"}});
  ASSERT_TRUE(id.ok());
  EXPECT_EQ(*id, 1u);
  auto duplicate = (*catalog)->define(StatementAst(fills()));
  ASSERT_FALSE(duplicate.ok());
  EXPECT_EQ(duplicate.status().code(), ErrorCode::Conflict);
}

TEST(Catalog, ProjectionHasHiddenIdentityAndStarOnlyExposesVisibleColumns) {
  auto catalog = Catalog::create();
  ASSERT_TRUE(catalog.ok());
  ASSERT_TRUE((*catalog)->define(StatementAst(fills())).ok());
  SelectAst query;
  query.from.name = "fills";
  query.projection = {{column("quantity"), "quantity"}};
  auto view = (*catalog)->define(StatementAst(CreateViewAst{"quantities", query}));
  ASSERT_TRUE(view.ok()) << view.status().message();
  auto snapshot = (*catalog)->snapshot();
  ASSERT_TRUE(snapshot.ok());
  auto found = snapshot->find(*view);
  ASSERT_TRUE(found.ok());
  const auto& relation = **found;
  EXPECT_EQ(relation.visible_count, 1u);
  EXPECT_EQ(relation.storage_schema.fields.size(), 3u);
  EXPECT_EQ(relation.physical_key_slots, (std::vector<std::size_t>{1, 2}));
  EXPECT_EQ(relation.partition_slots, (std::vector<std::size_t>{1}));
  EXPECT_EQ(relation.partition_visible_slots, (std::vector<std::size_t>{kNoSlot}));
  EXPECT_EQ(relation.identity_types, (std::vector<DataType>{DataType::String, DataType::Int64}));
  EXPECT_EQ(relation.state_id, *view);
  auto select_star = (*catalog)->bind_query_sql("SELECT * FROM quantities");
  ASSERT_TRUE(select_star.ok()) << select_star.status().message();
  EXPECT_EQ(select_star->output_fields.size(), 1u);
  EXPECT_FALSE((*catalog)->bind_query_sql("SELECT __ef_key_0 FROM quantities").ok());
  EXPECT_FALSE((*catalog)->bind_query_sql("SELECT quantity AS __EF_SECRET FROM quantities").ok());
  EXPECT_FALSE((*catalog)->bind_query_sql("SELECT quantity, quantity FROM quantities").ok());
}

TEST(Catalog, SqlAndSdkDefinitionsProduceTheSameCanonicalCatalog) {
  auto sql = Catalog::create();
  auto sdk = Catalog::create();
  ASSERT_TRUE(sql.ok());
  ASSERT_TRUE(sdk.ok());
  ASSERT_TRUE((*sql)
                  ->define_sql("CREATE TABLE fills (account STRING NOT NULL, id INT64, quantity "
                               "INT64, price FLOAT64, stamp TIMESTAMP_MICROS NOT NULL, PRIMARY "
                               "KEY(id, account)) PARTITION BY(account)")
                  .ok());
  ASSERT_TRUE((*sdk)->define(StatementAst(fills())).ok());
  SelectAst query;
  query.from.name = "fills";
  query.group_by = {column("account")};
  query.projection = {{column("account"), {}},
                      {call("sum", {column("quantity")}), "volume"},
                      {call("count", {star()}), "rows_seen"}};
  ASSERT_TRUE((*sdk)->define(StatementAst(CreateViewAst{"totals", query})).ok());
  auto view = (*sql)->define_sql("CREATE MATERIALIZED VIEW totals AS SELECT account, SUM(quantity) "
                                 "AS volume, COUNT(*) AS rows_seen FROM fills GROUP BY account");
  ASSERT_TRUE(view.ok()) << view.status().message();
  auto a = (*sql)->snapshot();
  auto b = (*sdk)->snapshot();
  ASSERT_TRUE(a.ok());
  ASSERT_TRUE(b.ok());
  EXPECT_EQ(a->fingerprint, b->fingerprint);
  auto explanation = (*sql)->explain(*view);
  ASSERT_TRUE(explanation.ok());
  EXPECT_NE(explanation->find("Aggregate"), std::string::npos);
  EXPECT_NE(explanation->find("has_internal_state"), std::string::npos);
}

TEST(Binder, TypeAndNullRulesApplyToSdkAndSql) {
  auto catalog = Catalog::create();
  ASSERT_TRUE(catalog.ok());
  ASSERT_TRUE((*catalog)->define(StatementAst(fills())).ok());
  for (const auto* sql :
       {"SELECT quantity + price AS bad FROM fills", "SELECT stamp + quantity AS bad FROM fills",
        "SELECT stamp = quantity AS bad FROM fills", "SELECT TRUE < FALSE AS bad FROM fills",
        "SELECT quantity FROM fills WHERE quantity", "SELECT NULL AS bad FROM fills",
        "SELECT quantity + 1 FROM fills", "SELECT SUM(quantity) AS bad FROM fills",
        "SELECT SUM(quantity) AS bad FROM fills WHERE SUM(quantity) > 0 GROUP BY account",
        "SELECT SUM(SUM(quantity)) AS bad FROM fills GROUP BY account",
        "SELECT quantity FROM fills GROUP BY account"}) {
    auto result = (*catalog)->bind_query_sql(sql);
    EXPECT_FALSE(result.ok()) << sql;
  }
  for (const auto* sql :
       {"SELECT CAST(NULL AS FLOAT64) AS x FROM fills",
        "SELECT CAST(quantity AS FLOAT64) + price AS x FROM fills",
        "SELECT quantity + NULL AS x FROM fills", "SELECT NULL = quantity AS x FROM fills",
        "SELECT NULL IS NULL AS x FROM fills", "SELECT -9223372036854775808 AS x FROM fills",
        "SELECT COUNT(NULL) AS x FROM fills GROUP BY account",
        "SELECT COUNT(quantity) AS x, AVG(quantity) AS y FROM fills GROUP BY account"}) {
    auto result = (*catalog)->bind_query_sql(sql);
    EXPECT_TRUE(result.ok()) << sql << ": " << result.status().message();
  }
  SelectAst sdk;
  sdk.from.name = "fills";
  sdk.projection = {{binary("+", column("stamp"), number("1")), "x"}};
  EXPECT_FALSE((*catalog)->bind_query(sdk).ok());
  sdk.projection = {{literal(Value(std::int64_t{1}), DataType::Float64), "x"}};
  EXPECT_FALSE((*catalog)->bind_query(sdk).ok());
  sdk.projection = {{column("quantity"), {}}};
  sdk.where = literal(Value{});
  auto unknown = (*catalog)->bind_query(sdk);
  ASSERT_TRUE(unknown.ok());
  EXPECT_EQ(unknown->filter->type, DataType::Bool);
  EXPECT_TRUE(unknown->filter->nullable);
}

TEST(Binder, RejectsMalformedSdkTreeShapes) {
  auto catalog = Catalog::create();
  ASSERT_TRUE(catalog.ok());
  ASSERT_TRUE((*catalog)->define(StatementAst(fills())).ok());
  auto malformed_star = std::make_shared<ExprAst>();
  malformed_star->kind = ExprAstKind::Star;
  malformed_star->args = {column("missing")};
  SelectAst query;
  query.from.name = "fills";
  query.group_by = {column("account")};
  query.projection = {{call("count", {malformed_star}), "n"}};
  EXPECT_FALSE((*catalog)->bind_query(query).ok());
  auto malformed_number = std::make_shared<ExprAst>();
  malformed_number->kind = ExprAstKind::Number;
  malformed_number->text = "9223372036854775808";
  malformed_number->args = {column("missing")};
  query.group_by.clear();
  query.projection = {{unary("-", malformed_number), "n"}};
  EXPECT_FALSE((*catalog)->bind_query(query).ok());
}

TEST(Binder, FloatVectorAndTimestampKeepTheirDistinctTypeContracts) {
  auto catalog = Catalog::create();
  ASSERT_TRUE(catalog.ok());
  ASSERT_TRUE((*catalog)
                  ->define_sql("CREATE TABLE typed (id INT64, vec FLOAT_VECTOR, stamp "
                               "TIMESTAMP_MICROS, PRIMARY KEY(id)) PARTITION BY(id)")
                  .ok());
  for (const auto* sql : {"SELECT vec FROM typed", "SELECT vec IS NULL AS absent FROM typed",
                          "SELECT COUNT(vec) AS n FROM typed GROUP BY id",
                          "SELECT stamp = stamp AS same FROM typed"}) {
    auto plan = (*catalog)->bind_query_sql(sql);
    EXPECT_TRUE(plan.ok()) << sql << ": " << plan.status().message();
  }
  for (const auto* sql :
       {"SELECT vec = vec AS same FROM typed", "SELECT vec + vec AS x FROM typed",
        "SELECT SUM(vec) AS x FROM typed GROUP BY id",
        "SELECT CAST(vec AS FLOAT64) AS x FROM typed", "SELECT stamp + stamp AS x FROM typed",
        "SELECT CAST(stamp AS FLOAT64) AS x FROM typed"}) {
    auto plan = (*catalog)->bind_query_sql(sql);
    EXPECT_FALSE(plan.ok()) << sql;
  }
  SelectAst query;
  query.from.name = "typed";
  query.projection = {
      {binary("=", column("stamp"), literal(Value(std::int64_t{1}), DataType::TimestampMicros)),
       "same"}};
  EXPECT_TRUE((*catalog)->bind_query(query).ok());
}

TEST(Binder, UntypedNullArithmeticCannotBecomeBoolOrString) {
  auto catalog = Catalog::create();
  ASSERT_TRUE(catalog.ok());
  ASSERT_TRUE((*catalog)->define(StatementAst(fills())).ok());
  for (const auto* sql :
       {"SELECT quantity FROM fills WHERE NULL + NULL",
        "SELECT (NULL + NULL) AND TRUE AS x FROM fills",
        "SELECT (NULL + NULL) = account AS x FROM fills", "SELECT NULL + NULL AS x FROM fills"}) {
    auto plan = (*catalog)->bind_query_sql(sql);
    EXPECT_FALSE(plan.ok()) << sql;
  }
  auto plan = (*catalog)->bind_query_sql("SELECT (NULL + NULL) + price AS x FROM fills");
  ASSERT_TRUE(plan.ok()) << plan.status().message();
  EXPECT_EQ(plan->output_fields[0].type, DataType::Float64);
}

TEST(Binder, NormalizesCompositeGroupOrderAndRequiresTheCompleteBusinessKey) {
  auto catalog = Catalog::create();
  ASSERT_TRUE(catalog.ok());
  ASSERT_TRUE(
      (*catalog)
          ->define_sql("CREATE TABLE events (region STRING, account INT64, id INT64, q INT64, "
                       "PRIMARY KEY (id, account, region)) PARTITION BY (region, account)")
          .ok());
  auto plan = (*catalog)->bind_query_sql(
      "SELECT account, region, SUM(q) + account AS total FROM events GROUP BY account, region");
  ASSERT_TRUE(plan.ok()) << plan.status().message();
  ASSERT_EQ(plan->group_by.size(), 2u);
  EXPECT_EQ(plan->group_by[0]->slot, 0u);
  EXPECT_EQ(plan->group_by[1]->slot, 1u);
  EXPECT_EQ(plan->partition_visible_slots, (std::vector<std::size_t>{1, 0}));
  EXPECT_EQ(plan->projection[0]->slot, 1u);
  EXPECT_EQ(plan->aggregates.size(), 1u);
  EXPECT_TRUE(plan->unique_per_partition);
  EXPECT_FALSE((*catalog)->bind_query_sql("SELECT account FROM events GROUP BY account").ok());
  EXPECT_FALSE(
      (*catalog)->bind_query_sql("SELECT region FROM events GROUP BY region, account, id").ok());
  EXPECT_FALSE(
      (*catalog)->bind_query_sql("SELECT region FROM events GROUP BY region, region").ok());
}

TEST(Binder, JoinRequiresFullCorrespondingKeyAndSchemaProofOfUniqueness) {
  auto catalog = Catalog::create();
  ASSERT_TRUE(catalog.ok());
  ASSERT_TRUE((*catalog)->define(StatementAst(fills())).ok());
  ASSERT_TRUE((*catalog)
                  ->define_sql("CREATE MATERIALIZED VIEW totals AS SELECT account, SUM(quantity) "
                               "AS volume FROM fills GROUP BY account")
                  .ok());
  ASSERT_TRUE((*catalog)
                  ->define_sql("CREATE TABLE limits (account STRING, ceiling INT64, PRIMARY "
                               "KEY(account)) PARTITION BY(account)")
                  .ok());
  auto join =
      (*catalog)->bind_query_sql("SELECT t.account, t.volume, l.ceiling FROM totals t JOIN limits "
                                 "l ON l.account = t.account WHERE t.volume > l.ceiling");
  ASSERT_TRUE(join.ok()) << join.status().message();
  EXPECT_TRUE(join->unique_per_partition);
  EXPECT_EQ(join->identity_types, (std::vector<DataType>{DataType::String}));
  EXPECT_EQ(join->projection[2]->slot, 3u);
  for (const auto* sql :
       {"SELECT f.quantity FROM fills f JOIN limits l ON f.account = l.account",
        "SELECT t.volume FROM totals t JOIN limits l ON t.volume = l.ceiling",
        "SELECT t.volume FROM totals t JOIN limits l ON t.account = l.account AND t.volume = "
        "l.ceiling",
        "SELECT account FROM totals t JOIN limits l ON t.account = l.account",
        "SELECT totals.volume FROM totals t JOIN limits l ON t.account = l.account",
        "SELECT t.volume FROM totals t JOIN limits l ON t.account = l.account OR t.account = "
        "l.account"}) {
    auto result = (*catalog)->bind_query_sql(sql);
    EXPECT_FALSE(result.ok()) << sql;
  }
  ASSERT_TRUE(
      (*catalog)
          ->define_sql("CREATE TABLE lefts (a INT64, b INT64, PRIMARY KEY(a,b)) PARTITION BY(a,b)")
          .ok());
  ASSERT_TRUE(
      (*catalog)
          ->define_sql("CREATE TABLE rights (x INT64, y INT64, PRIMARY KEY(x,y)) PARTITION BY(x,y)")
          .ok());
  EXPECT_TRUE((*catalog)
                  ->bind_query_sql("SELECT l.a FROM lefts l JOIN rights r ON l.b=r.y AND r.x=l.a")
                  .ok());
  EXPECT_FALSE((*catalog)
                   ->bind_query_sql("SELECT l.a FROM lefts l JOIN rights r ON l.a=r.y AND l.b=r.x")
                   .ok());
  EXPECT_FALSE((*catalog)->bind_query_sql("SELECT l.a FROM lefts l JOIN rights r ON l.a=r.x").ok());
  EXPECT_FALSE((*catalog)
                   ->bind_query_sql("SELECT l.a FROM lefts l JOIN rights r ON l.a=r.x AND l.a=r.x")
                   .ok());
}

TEST(Binder, RetainsPartitionLineageAcrossRepeatedDirectProjectionAliases) {
  auto catalog = Catalog::create();
  ASSERT_TRUE(catalog.ok());
  ASSERT_TRUE((*catalog)->define(StatementAst(fills())).ok());
  ASSERT_TRUE((*catalog)
                  ->define_sql("CREATE MATERIALIZED VIEW aliases AS SELECT account AS a, account "
                               "AS b, quantity FROM fills")
                  .ok());
  auto grouped = (*catalog)->bind_query_sql("SELECT b, SUM(quantity) AS q FROM aliases GROUP BY b");
  ASSERT_TRUE(grouped.ok()) << grouped.status().message();
  auto scoped = (*catalog)->bind_query_sql("SELECT quantity FROM aliases WHERE b='desk'");
  ASSERT_TRUE(scoped.ok());
  ASSERT_TRUE(scoped->inferred_scope.has_value());
  EXPECT_EQ(*scoped->inferred_scope, (Row{std::string("desk")}));
  ASSERT_TRUE((*catalog)
                  ->define_sql("CREATE MATERIALIZED VIEW totals AS SELECT b AS c, SUM(quantity) AS "
                               "q FROM aliases GROUP BY b")
                  .ok());
  ASSERT_TRUE(
      (*catalog)
          ->define_sql(
              "CREATE TABLE limits (account STRING, PRIMARY KEY(account)) PARTITION BY(account)")
          .ok());
  ASSERT_TRUE((*catalog)
                  ->define_sql("CREATE MATERIALIZED VIEW joined AS SELECT t.c AS x, l.account AS y "
                               "FROM totals t JOIN limits l ON t.c=l.account")
                  .ok());
  auto result = (*catalog)->bind_query_sql("SELECT y FROM joined WHERE y='desk' GROUP BY y");
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_TRUE(result->inferred_scope.has_value());
  auto snapshot = (*catalog)->snapshot();
  ASSERT_TRUE(snapshot.ok());
  EXPECT_TRUE(Catalog::from_snapshot(*snapshot).ok());
}

TEST(Binder, ScopeInferenceOnlyUsesCompleteAndConjoinedPartitionEqualities) {
  auto catalog = Catalog::create();
  ASSERT_TRUE(catalog.ok());
  ASSERT_TRUE((*catalog)
                  ->define_sql("CREATE TABLE events (a STRING, b INT64, id INT64, PRIMARY "
                               "KEY(id,a,b)) PARTITION BY(a,b)")
                  .ok());
  auto full = (*catalog)->bind_query_sql("SELECT id FROM events WHERE b=2 AND a='desk' AND id>0");
  ASSERT_TRUE(full.ok()) << full.status().message();
  ASSERT_TRUE(full->inferred_scope.has_value());
  EXPECT_EQ(*full->inferred_scope, (Row{std::string("desk"), std::int64_t{2}}));
  for (const auto* sql :
       {"SELECT id FROM events WHERE a='desk'", "SELECT id FROM events WHERE a='desk' OR b=2",
        "SELECT id FROM events WHERE (a='desk' AND b=2) OR (a='other' AND b=3)",
        "SELECT id FROM events WHERE a=NULL AND b=2"}) {
    auto plan = (*catalog)->bind_query_sql(sql);
    ASSERT_TRUE(plan.ok()) << sql;
    EXPECT_FALSE(plan->inferred_scope.has_value()) << sql;
  }
}

TEST(Catalog, RejectsScopeForgeryEvenWhenFingerprintMatches) {
  auto catalog = Catalog::create();
  ASSERT_TRUE(catalog.ok());
  ASSERT_TRUE((*catalog)->define(StatementAst(fills())).ok());
  ASSERT_TRUE((*catalog)
                  ->define_sql("CREATE MATERIALIZED VIEW projected AS SELECT quantity FROM fills "
                               "WHERE account='desk'")
                  .ok());
  auto snapshot = (*catalog)->snapshot();
  ASSERT_TRUE(snapshot.ok());
  auto descriptor = std::make_shared<RelationDescriptor>(*snapshot->relations[1]);
  auto plan = std::make_shared<BoundPlan>(*descriptor->plan);
  plan->inferred_scope = Row{std::string("forged")};
  descriptor->plan = plan;
  snapshot->relations[1] = descriptor;
  auto json = Json::parse(snapshot->fingerprint);
  json["relations"][1]["plan"]["inferred_scope"] = Json::array({"forged"});
  snapshot->fingerprint = json.dump();
  EXPECT_FALSE(Catalog::from_snapshot(*snapshot).ok());
}

TEST(Catalog, RestoredCatalogOwnsItsPlanExpressions) {
  auto catalog = Catalog::create();
  ASSERT_TRUE(catalog.ok());
  ASSERT_TRUE((*catalog)->define(StatementAst(fills())).ok());
  ASSERT_TRUE((*catalog)
                  ->define_sql("CREATE MATERIALIZED VIEW projected AS SELECT quantity FROM fills")
                  .ok());
  auto snapshot = (*catalog)->snapshot();
  ASSERT_TRUE(snapshot.ok());
  auto descriptor = std::make_shared<RelationDescriptor>(*snapshot->relations[1]);
  auto plan = std::make_shared<BoundPlan>(*descriptor->plan);
  auto expression = std::make_shared<BoundExpr>(*plan->projection[0]);
  plan->projection[0] = expression;
  descriptor->plan = plan;
  snapshot->relations[1] = descriptor;
  auto restored = Catalog::from_snapshot(*snapshot);
  ASSERT_TRUE(restored.ok()) << restored.status().message();
  expression->slot = 12345;
  auto owned = (*restored)->snapshot();
  ASSERT_TRUE(owned.ok());
  EXPECT_EQ(owned->relations[1]->plan->projection[0]->slot, 2u);
}

TEST(Catalog, RestoresOnlyConsistentSnapshotsAndKeepsStableIds) {
  auto catalog = Catalog::create();
  ASSERT_TRUE(catalog.ok());
  ASSERT_TRUE((*catalog)->define(StatementAst(fills())).ok());
  ASSERT_TRUE((*catalog)
                  ->define_sql("CREATE MATERIALIZED VIEW totals AS SELECT account, SUM(quantity) "
                               "AS volume FROM fills GROUP BY account")
                  .ok());
  ASSERT_TRUE((*catalog)
                  ->define_sql("CREATE MATERIALIZED VIEW projected AS SELECT volume FROM totals")
                  .ok());
  auto snapshot = (*catalog)->snapshot();
  ASSERT_TRUE(snapshot.ok());
  auto restored = Catalog::from_snapshot(*snapshot);
  ASSERT_TRUE(restored.ok()) << restored.status().message();
  auto next =
      (*restored)->define_sql("CREATE MATERIALIZED VIEW again AS SELECT volume FROM projected");
  ASSERT_TRUE(next.ok()) << next.status().message();
  EXPECT_EQ(*next, 4u);
  CatalogSnapshot corrupt = *snapshot;
  corrupt.fingerprint += "x";
  EXPECT_FALSE(Catalog::from_snapshot(corrupt).ok());
  corrupt = *snapshot;
  ++corrupt.version;
  EXPECT_FALSE(Catalog::from_snapshot(corrupt).ok());
  corrupt = *snapshot;
  std::swap(corrupt.relations[0], corrupt.relations[1]);
  EXPECT_FALSE(Catalog::from_snapshot(corrupt).ok());
  corrupt = *snapshot;
  auto descriptor = std::make_shared<RelationDescriptor>(*corrupt.relations[1]);
  descriptor->partition_slots[0] = 0;
  corrupt.relations[1] = descriptor;
  EXPECT_FALSE(Catalog::from_snapshot(corrupt).ok());
  corrupt = *snapshot;
  descriptor = std::make_shared<RelationDescriptor>(*corrupt.relations[1]);
  auto plan = std::make_shared<BoundPlan>(*descriptor->plan);
  plan->left = 3;
  descriptor->plan = plan;
  corrupt.relations[1] = descriptor;
  EXPECT_FALSE(Catalog::from_snapshot(corrupt).ok());
}

TEST(Binder, PublicSnapshotEntryRejectsInvalidVisibleCountWithoutAborting) {
  auto catalog = Catalog::create();
  ASSERT_TRUE(catalog.ok());
  ASSERT_TRUE((*catalog)->define(StatementAst(fills())).ok());
  auto snapshot = (*catalog)->snapshot();
  ASSERT_TRUE(snapshot.ok());
  auto relation = std::make_shared<RelationDescriptor>(*snapshot->relations[0]);
  auto valid_plan = (*catalog)->bind_query_sql("SELECT quantity FROM fills");
  ASSERT_TRUE(valid_plan.ok());
  relation->visible_count = 1000000;
  snapshot->relations[0] = relation;
  EXPECT_FALSE(validate_bound_plan(*valid_plan, *snapshot).ok());
  SelectAst query;
  query.from.name = "fills";
  query.projection = {{column("quantity"), {}}};
  EXPECT_EXIT(
      {
        alarm(5);
        const auto result = bind_select(query, *snapshot);
        std::_Exit(result.ok() ? 1 : 0);
      },
      ::testing::ExitedWithCode(0), "");
}

TEST(Binder, PublicPlanValidatorRejectsMalformedAggregateAndMetadata) {
  auto catalog = Catalog::create();
  ASSERT_TRUE(catalog.ok());
  ASSERT_TRUE((*catalog)->define(StatementAst(fills())).ok());
  auto snapshot = (*catalog)->snapshot();
  ASSERT_TRUE(snapshot.ok());
  auto bound = (*catalog)->bind_query_sql(
      "SELECT COUNT(*) AS n FROM fills WHERE account='desk' GROUP BY account");
  ASSERT_TRUE(bound.ok());
  EXPECT_TRUE(validate_bound_plan(*bound, *snapshot).ok());
  auto malformed = *bound;
  malformed.aggregates[0].kind = AggregateKind::Sum;
  EXPECT_FALSE(validate_bound_plan(malformed, *snapshot).ok());
  malformed = *bound;
  malformed.aggregates[0].kind = static_cast<AggregateKind>(999);
  EXPECT_FALSE(validate_bound_plan(malformed, *snapshot).ok());
  malformed = *bound;
  malformed.aggregates[0].result_type = DataType::Float64;
  EXPECT_FALSE(validate_bound_plan(malformed, *snapshot).ok());
  malformed = *bound;
  malformed.projection[0] = {};
  EXPECT_FALSE(validate_bound_plan(malformed, *snapshot).ok());
  malformed = *bound;
  auto expression = std::make_shared<BoundExpr>(*malformed.projection[0]);
  expression->slot = 1000000;
  malformed.projection[0] = expression;
  EXPECT_FALSE(validate_bound_plan(malformed, *snapshot).ok());
  malformed = *bound;
  malformed.group_by[0] = {};
  EXPECT_FALSE(validate_bound_plan(malformed, *snapshot).ok());
  malformed = *bound;
  malformed.inferred_scope = Row{std::string("forged")};
  EXPECT_FALSE(validate_bound_plan(malformed, *snapshot).ok());
  malformed = *bound;
  malformed.unique_per_partition = false;
  EXPECT_FALSE(validate_bound_plan(malformed, *snapshot).ok());
  malformed = *bound;
  malformed.right = malformed.left;
  EXPECT_FALSE(validate_bound_plan(malformed, *snapshot).ok());
  malformed = *bound;
  malformed.left = 999;
  EXPECT_FALSE(validate_bound_plan(malformed, *snapshot).ok());
}

TEST(Catalog, ViewProvenanceKeepsItsDefinitionCatalogWithoutRecursiveFingerprintJson) {
  auto catalog = Catalog::create();
  ASSERT_TRUE(catalog.ok());
  ASSERT_TRUE((*catalog)->define(StatementAst(fills())).ok());
  auto before = (*catalog)->snapshot();
  ASSERT_TRUE(before.ok());
  ASSERT_TRUE((*catalog)
                  ->define_sql("CREATE MATERIALIZED VIEW projected AS SELECT quantity FROM fills")
                  .ok());
  auto snapshot = (*catalog)->snapshot();
  ASSERT_TRUE(snapshot.ok());
  const auto& view_plan = *snapshot->relations[1]->plan;
  EXPECT_EQ(view_plan.catalog_fingerprint, before->fingerprint);
  EXPECT_TRUE(validate_bound_plan(view_plan, *before).ok());
  EXPECT_FALSE(validate_bound_plan(view_plan, *snapshot).ok());
  auto json = bound_plan_json(view_plan);
  ASSERT_TRUE(json.ok());
  EXPECT_FALSE(json->contains("catalog_fingerprint"));
  EXPECT_TRUE(Catalog::from_snapshot(*snapshot).ok());
  auto descriptor = std::make_shared<RelationDescriptor>(*snapshot->relations[1]);
  auto forged = std::make_shared<BoundPlan>(*descriptor->plan);
  forged->catalog_fingerprint = snapshot->fingerprint;
  descriptor->plan = forged;
  snapshot->relations[1] = descriptor;
  EXPECT_FALSE(Catalog::from_snapshot(*snapshot).ok());
}

TEST(Binder, QueryPlanProvenanceRejectsSameVersionDifferentCatalog) {
  auto first = Catalog::create();
  auto second = Catalog::create();
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  ASSERT_TRUE((*first)->define(StatementAst(fills())).ok());
  auto other = fills();
  other.name = "unrelated";
  ASSERT_TRUE((*second)->define(StatementAst(other)).ok());
  auto plan = (*first)->bind_query_sql("SELECT quantity FROM fills");
  ASSERT_TRUE(plan.ok());
  auto own = (*first)->snapshot();
  auto foreign = (*second)->snapshot();
  ASSERT_TRUE(own.ok());
  ASSERT_TRUE(foreign.ok());
  ASSERT_EQ(own->version, foreign->version);
  EXPECT_TRUE(validate_bound_plan(*plan, *own).ok());
  EXPECT_FALSE(validate_bound_plan(*plan, *foreign).ok());
}
} // namespace
} // namespace eventflow
