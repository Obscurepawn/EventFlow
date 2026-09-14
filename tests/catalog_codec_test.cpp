#include "eventflow/catalog_codec.hpp"
#include "status_test_support.hpp"
#include <gtest/gtest.h>

namespace eventflow::test {
namespace {
TEST(CatalogCodec, OrdinarySqlRoundtripPreservesCanonicalDefinitionsAndQueryBinding) {
  auto catalog = take(Catalog::create());
  take(catalog->define_sql("CREATE TABLE input(k STRING,v INT64,PRIMARY KEY(k)) PARTITION BY(k)"));
  take(catalog->define_sql("CREATE MATERIALIZED VIEW filtered AS SELECT k,v+1 AS result FROM input "
                           "WHERE v IS NOT NULL AND v>0"));
  auto snapshot = take(catalog->snapshot());
  auto encoded = encode_catalog(snapshot);
  ASSERT_TRUE(encoded.ok()) << encoded.status().message();
  EXPECT_EQ(encoded.value(), snapshot.fingerprint);
  auto decoded = decode_catalog(encoded.value());
  ASSERT_TRUE(decoded.ok()) << decoded.status().message();
  EXPECT_EQ(decoded->fingerprint, snapshot.fingerprint);
  auto restored = take(Catalog::from_snapshot(*decoded));
  EXPECT_TRUE(restored->bind_query_sql("SELECT result FROM filtered WHERE k='a'").ok());
}

std::unique_ptr<Catalog> full_catalog() noexcept {
  auto catalog = take(Catalog::create());
  auto candidate = take(catalog->define_candidate({"candidate",
                                                   {{1, "k", DataType::String, false}},
                                                   {{2, "deadline", DataType::Int64, false}}}));
  DefinitionOptions options;
  options.base_write_role = BaseWriteRole::RequestCapture;
  options.identity_domain = "requests";
  auto capture = take(catalog->define_sql("CREATE TABLE capture(k STRING,request_id STRING,x "
                                          "FLOAT64,PRIMARY KEY(k,request_id)) PARTITION BY(k)",
                                          options));
  auto input = take(
      catalog->define_sql("CREATE MATERIALIZED VIEW input AS SELECT k,request_id,(x+1.0)*2.0 AS x "
                          "FROM capture WHERE x IS NOT NULL"));
  auto snapshot = take(catalog->snapshot());
  auto source = take(snapshot.find(input));
  auto function = take(catalog->define_function(
      {"function_result",
       "model",
       7,
       "builtin.model.v7",
       {{"data", input, true,
         std::vector<Field>(source->storage_schema.fields.begin(),
                            source->storage_schema.fields.begin() + source->visible_count)}},
       {{1, "score", DataType::Float64, false}}}));
  auto identity = take(snapshot.find(capture))->identity_fields;
  auto status =
      take(catalog->define_request_status({"request_status", "requests", identity, candidate}));
  take(catalog->define_latest({"latest", function, status, candidate}));
  take(catalog->define_sql("CREATE MATERIALIZED VIEW latest_values AS SELECT "
                           "k,score,__ef_latest_admission_seq FROM latest"));
  take(catalog->define_sql(
      "CREATE TABLE trades(k STRING,id INT64,v INT64,PRIMARY KEY(k,id)) PARTITION BY(k)"));
  take(catalog->define_sql("CREATE MATERIALIZED VIEW totals AS SELECT k,SUM(v) AS total,AVG(v) AS "
                           "mean,COUNT(*) AS n,COUNT(v) AS present FROM trades GROUP BY k"));
  take(
      catalog->define_sql("CREATE TABLE quote(k STRING,p FLOAT64,PRIMARY KEY(k)) PARTITION BY(k)"));
  take(catalog->define_sql("CREATE MATERIALIZED VIEW joined AS SELECT a.k,a.mean+b.p AS score FROM "
                           "totals a JOIN quote b ON a.k=b.k WHERE b.p IS NOT NULL"));
  return catalog;
}

TEST(CatalogCodec, AllRelationKindsAndBindingProvenanceSurviveTheWire) {
  auto snapshot = take(full_catalog()->snapshot());
  auto encoded = take(encode_catalog(snapshot));
  auto decoded = take(decode_catalog(encoded));
  EXPECT_EQ(decoded.fingerprint, snapshot.fingerprint);
  EXPECT_EQ(decoded.version, snapshot.version);
  ASSERT_EQ(decoded.relations.size(), snapshot.relations.size());
  for (std::size_t i = 0; i < decoded.relations.size(); ++i) {
    EXPECT_NE(decoded.relations[i].get(), snapshot.relations[i].get());
    EXPECT_EQ(take(dependencies(*decoded.relations[i])),
              take(dependencies(*snapshot.relations[i])));
    EXPECT_EQ(decoded.relations[i]->storage_schema.fields.size(),
              snapshot.relations[i]->storage_schema.fields.size());
  }
  auto restored = take(Catalog::from_snapshot(decoded));
  EXPECT_TRUE(restored->bind_query_sql("SELECT score FROM joined WHERE k='a'").ok());
  EXPECT_TRUE(
      restored->bind_query_sql("SELECT __ef_latest_admission_seq FROM latest_values WHERE k='a'")
          .ok());
}

TEST(CatalogCodec, InvalidAndUnknownMetadataCannotDisappearDuringReconstruction) {
  auto original = take(parse_json_exact(take(encode_catalog(take(full_catalog()->snapshot())))));
  std::vector<Json> invalid;
  auto changed = original;
  changed["unknown"] = 1;
  invalid.push_back(changed);
  changed = original;
  changed["layout_version"] = 2;
  invalid.push_back(changed);
  changed = original;
  changed["catalog_version"] = 999;
  invalid.push_back(changed);
  changed = original;
  changed["relations"][0]["candidate"]["format"] = 2;
  invalid.push_back(changed);
  changed = original;
  changed["relations"][0]["candidate"]["extra"] = false;
  invalid.push_back(changed);
  changed = original;
  changed["relations"][1]["identity_fields"][1]["name"] = "different";
  invalid.push_back(changed);
  changed = original;
  changed["relations"][2]["plan"]["projection"][2]["args"][0]["slot"] = 900;
  invalid.push_back(changed);
  changed = original;
  changed["relations"][2]["plan"]["operators"] = Json::array({"silently_skipped"});
  invalid.push_back(changed);
  changed = original;
  changed["relations"][3]["function"]["definition"]["function_version"] = -1;
  invalid.push_back(changed);
  changed = original;
  changed["relations"][3]["function"]["definition"]["inputs"][0]["relation"] = 999;
  invalid.push_back(changed);
  changed = original;
  changed["relations"][4]["request_status"]["candidate_relation"] = 2;
  invalid.push_back(changed);
  changed = original;
  changed["relations"][5]["kind"] = "unrecognized";
  invalid.push_back(changed);
  changed = original;
  changed["relations"][5]["latest"]["definition"]["candidate_relation"] = 999;
  invalid.push_back(changed);
  changed = original;
  changed["relations"][8]["plan"]["aggregates"][0]["state_encoding_version"] = 99;
  invalid.push_back(changed);
  changed = original;
  changed["relations"][8]["plan"]["has_internal_state"] = false;
  invalid.push_back(changed);
  changed = original;
  changed["relations"][1]["storage_schema"]["fields"][0]["nullable"] = true;
  invalid.push_back(changed);
  for (std::size_t i = 0; i < invalid.size(); ++i)
    EXPECT_FALSE(decode_catalog(invalid[i].dump()).ok()) << i;
}

TEST(CatalogCodec, MissingFieldsAndWrongPrimitiveTypesReturnErrors) {
  auto original = take(parse_json_exact(take(encode_catalog(take(full_catalog()->snapshot())))));
  for (const auto* name :
       {"id", "name", "storage_schema", "plan", "partition_slots", "unique_per_partition"}) {
    auto value = original;
    value["relations"][0].erase(name);
    EXPECT_FALSE(decode_catalog(value.dump()).ok()) << name;
  }
  for (auto value : {Json(nullptr), Json(true), Json("bad"), Json::array()}) {
    auto bad = original;
    bad["relations"][2]["plan"]["projection"][0] = value;
    EXPECT_FALSE(decode_catalog(bad.dump()).ok());
  }
  EXPECT_FALSE(decode_catalog("{\"catalog_version\":18446744073709551616}").ok());
}

TEST(CatalogCodec, ConfiguredRelationExpressionAndByteLimitsAreEnforced) {
  auto snapshot = take(full_catalog()->snapshot());
  auto bytes = take(encode_catalog(snapshot));
  CatalogCodecConfig config;
  config.max_bytes = bytes.size() - 1;
  EXPECT_EQ(decode_catalog(bytes, config).status().code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(encode_catalog(snapshot, config).status().code(), ErrorCode::ResourceExhausted);
  config = {};
  config.max_relations = 1;
  EXPECT_EQ(decode_catalog(bytes, config).status().code(), ErrorCode::ResourceExhausted);
  config = {};
  config.max_expression_nodes = 1;
  EXPECT_EQ(decode_catalog(bytes, config).status().code(), ErrorCode::ResourceExhausted);
  config = {};
  config.max_provenance_bytes = 1;
  EXPECT_EQ(decode_catalog(bytes, config).status().code(), ErrorCode::ResourceExhausted);
  config = {};
  config.max_expression_depth = 1;
  EXPECT_EQ(decode_catalog(bytes, config).status().code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(decode_catalog(std::string(513, '[') + std::string(513, ']')).status().code(),
            ErrorCode::ResourceExhausted);
}

TEST(CatalogCodec, EmptyCatalogAndQuotedBracketsRemainValid) {
  auto empty = take(Catalog::create());
  auto snapshot = take(empty->snapshot());
  EXPECT_EQ(take(decode_catalog(take(encode_catalog(snapshot)))).fingerprint, snapshot.fingerprint);
  take(empty->define_sql("CREATE TABLE input(k STRING,PRIMARY KEY(k)) PARTITION BY(k)"));
  take(empty->define_sql(
      "CREATE MATERIALIZED VIEW text_value AS SELECT k,'[\\\"{}]' AS value FROM input"));
  snapshot = take(empty->snapshot());
  EXPECT_EQ(take(decode_catalog(take(encode_catalog(snapshot)))).fingerprint, snapshot.fingerprint);
}

TEST(CatalogCodec, ExplicitFieldBudgetAllowsWideLegalSchemas) {
  auto catalog = take(Catalog::create());
  std::string sql = "CREATE TABLE wide(k STRING";
  for (int i = 0; i < 1100; ++i)
    sql += ",c" + std::to_string(i) + " INT64";
  sql += ",PRIMARY KEY(k)) PARTITION BY(k)";
  take(catalog->define_sql(sql));
  auto snapshot = take(catalog->snapshot());
  auto encoded = encode_catalog(snapshot);
  ASSERT_TRUE(encoded.ok()) << encoded.status().message();
  EXPECT_EQ(take(decode_catalog(*encoded)).fingerprint, snapshot.fingerprint);
  CatalogCodecConfig limited;
  limited.max_fields_per_relation = 1024;
  EXPECT_EQ(decode_catalog(*encoded, limited).status().code(), ErrorCode::ResourceExhausted);
}

TEST(CatalogCodec, AggregateArraysRespectTheExplicitFieldBudget) {
  auto catalog = take(Catalog::create());
  take(catalog->define_sql("CREATE TABLE input(k STRING,PRIMARY KEY(k)) PARTITION BY(k)"));
  SelectAst query;
  query.from = {"input", {}};
  query.group_by = {column("k")};
  query.projection.push_back({column("k"), "k"});
  for (int i = 0; i < 1025; ++i)
    query.projection.push_back({call("count", {star()}), "n" + std::to_string(i)});
  take(catalog->define(CreateViewAst{"counts", query}));
  auto snapshot = take(catalog->snapshot());
  EXPECT_EQ(take(decode_catalog(take(encode_catalog(snapshot)))).fingerprint, snapshot.fingerprint);
  auto small = take(parse_json_exact(take(full_catalog()->snapshot()).fingerprint));
  auto& aggregates = small["relations"][8]["plan"]["aggregates"];
  while (aggregates.size() < 7)
    aggregates.push_back(aggregates[0]);
  CatalogCodecConfig limits;
  limits.max_fields_per_relation = 6;
  EXPECT_EQ(decode_catalog(small.dump(), limits).status().code(), ErrorCode::ResourceExhausted);
}
} // namespace
} // namespace eventflow::test
