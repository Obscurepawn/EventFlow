#include "eventflow/catalog_codec.hpp"
#include "status_test_support.hpp"
#include <gtest/gtest.h>

namespace eventflow::test {
namespace {
constexpr std::string_view audit_table_sql =
    "CREATE TABLE audit_rows(scope_key STRING,occurrence_id STRING,emitted_at INT64,"
    "PRIMARY KEY(scope_key,occurrence_id)) PARTITION BY(scope_key)";

StatusOr<CatalogSnapshot> audit_snapshot() noexcept {
  EF_ASSIGN_OR_RETURN(auto catalog, Catalog::create());
  EF_ASSIGN_OR_RETURN(
      const auto id,
      catalog->define_sql(audit_table_sql, {.partition_key = {},
                                            .base_write_role = BaseWriteRole::TriggerAudit,
                                            .identity_domain = {}}));
  (void)id;
  return catalog->snapshot();
}

TEST(TriggerAuditCatalog, DefinesProtectedBaseTableWithoutRequestIdentityOrAnnotations) {
  auto created = audit_snapshot();
  ASSERT_TRUE(created.ok()) << created.status().message();
  const auto& snapshot = *created;
  ASSERT_EQ(snapshot.relations.size(), 1U);
  const auto& relation = *snapshot.relations.front();
  EXPECT_EQ(relation.kind, RelationKind::BaseTable);
  EXPECT_EQ(relation.base_write_role, BaseWriteRole::TriggerAudit);
  EXPECT_TRUE(relation.identity_domain.empty());
  EXPECT_TRUE(relation.identity_fields.empty());
  EXPECT_FALSE(relation.candidate);
  EXPECT_FALSE(relation.request_status);
  EXPECT_EQ(relation.physical_key_slots, (std::vector<std::size_t>{0, 1}));
  EXPECT_EQ(relation.partition_slots, (std::vector<std::size_t>{0}));
  EXPECT_FALSE(relation.unique_per_partition);
  auto restored = Catalog::from_snapshot(snapshot);
  ASSERT_TRUE(restored.ok()) << restored.status().message();
  auto query = (*restored)->bind_query_sql(
      "SELECT occurrence_id,emitted_at FROM audit_rows WHERE scope_key='account_a'");
  ASSERT_TRUE(query.ok()) << query.status().message();
  EXPECT_TRUE(query->identity_domain.empty());
  EXPECT_TRUE(validate_bound_plan(*query, snapshot).ok());
}

TEST(TriggerAuditCatalog, CanonicalWireAndSnapshotRestorePreserveAuditOwnership) {
  auto created = audit_snapshot();
  ASSERT_TRUE(created.ok()) << created.status().message();
  auto encoded = encode_catalog(*created);
  ASSERT_TRUE(encoded.ok()) << encoded.status().message();
  EXPECT_EQ(*encoded, created->fingerprint);
  const auto wire = take(parse_json_exact(*encoded));
  EXPECT_EQ(wire["relations"][0]["base_write_role"], "trigger_audit");
  EXPECT_FALSE(wire["relations"][0].contains("identity_domain"));
  EXPECT_FALSE(wire["relations"][0].contains("identity_fields"));
  auto decoded = decode_catalog(*encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status().message();
  EXPECT_EQ(decoded->fingerprint, created->fingerprint);
  EXPECT_EQ(decoded->relations.front()->base_write_role, BaseWriteRole::TriggerAudit);
  EXPECT_NE(decoded->relations.front().get(), created->relations.front().get());
  auto restored = take(Catalog::from_snapshot(*decoded));
  auto snapshot = take(restored->snapshot());
  EXPECT_EQ(snapshot.relations.front()->base_write_role, BaseWriteRole::TriggerAudit);
  EXPECT_EQ(take(encode_catalog(snapshot)), *encoded);
}

TEST(TriggerAuditCatalog, InvalidDefinitionRolesAndDomainsLeaveCatalogUnchanged) {
  auto catalog = take(Catalog::create());
  const auto before = take(catalog->snapshot());
  const std::vector<DefinitionOptions> invalid{{.partition_key = {},
                                                .base_write_role = static_cast<BaseWriteRole>(99),
                                                .identity_domain = {}},
                                               {.partition_key = {},
                                                .base_write_role = BaseWriteRole::TriggerAudit,
                                                .identity_domain = "requests"},
                                               {.partition_key = {},
                                                .base_write_role = BaseWriteRole::PublicInput,
                                                .identity_domain = "requests"},
                                               {.partition_key = {},
                                                .base_write_role = BaseWriteRole::RuntimeStatus,
                                                .identity_domain = "requests"},
                                               {.partition_key = {},
                                                .base_write_role = BaseWriteRole::RequestCapture,
                                                .identity_domain = {}}};
  for (const auto& options : invalid) {
    EXPECT_EQ(catalog->define_sql(audit_table_sql, options).status().code(),
              ErrorCode::InvalidArgument);
    EXPECT_EQ(take(catalog->snapshot()).fingerprint, before.fingerprint);
  }
  take(catalog->define_sql(audit_table_sql));
  const auto with_input = take(catalog->snapshot());
  EXPECT_EQ(catalog
                ->define_sql("CREATE MATERIALIZED VIEW audit_view AS SELECT * FROM audit_rows",
                             {.partition_key = {},
                              .base_write_role = BaseWriteRole::TriggerAudit,
                              .identity_domain = {}})
                .status()
                .code(),
            ErrorCode::InvalidArgument);
  EXPECT_EQ(take(catalog->snapshot()).fingerprint, with_input.fingerprint);
}

TEST(TriggerAuditCatalog, WireRejectsMalformedRolesDomainsAndPhysicalTypes) {
  auto created = audit_snapshot();
  ASSERT_TRUE(created.ok()) << created.status().message();
  const auto original = take(parse_json_exact(take(encode_catalog(*created))));
  std::vector<Json> invalid;
  for (const auto& role : {Json("unknown_role"), Json("public_input"), Json(3), Json(nullptr)}) {
    auto changed = original;
    changed["relations"][0]["base_write_role"] = role;
    invalid.push_back(std::move(changed));
  }
  auto changed = original;
  changed["relations"][0]["identity_domain"] = "requests";
  changed["relations"][0]["identity_fields"] = Json::array();
  invalid.push_back(changed);
  changed = original;
  changed["relations"][0]["identity_domain"] = "";
  changed["relations"][0]["identity_fields"] = Json::array();
  invalid.push_back(changed);
  changed = original;
  changed["relations"][0]["identity_fields"] = Json::array();
  invalid.push_back(changed);
  changed = original;
  changed["relations"][0]["identity_types"][1] = "INT64";
  invalid.push_back(changed);
  changed = original;
  changed["relations"][0]["partition_types"][0] = "INT64";
  invalid.push_back(changed);
  changed = original;
  changed["relations"][0]["storage_schema"]["fields"][1]["type"] = "INT64";
  invalid.push_back(changed);
  for (std::size_t i = 0; i < invalid.size(); ++i) {
    SCOPED_TRACE(i);
    EXPECT_EQ(decode_catalog(invalid[i].dump()).status().code(), ErrorCode::InvalidArgument);
  }
}

TEST(TriggerAuditCatalog, WireRejectsAuditRoleOnViewsAndLifecycleAnnotations) {
  auto catalog = take(Catalog::create());
  take(catalog->define_sql(audit_table_sql));
  take(catalog->define_sql("CREATE MATERIALIZED VIEW audit_view AS SELECT * FROM audit_rows"));
  take(catalog->define_candidate(
      {.name = "candidate",
       .scope_fields =
           {{.id = 1, .name = "scope_key", .type = DataType::String, .nullable = false}},
       .payload_fields = {}}));
  const auto original = take(parse_json_exact(take(encode_catalog(take(catalog->snapshot())))));
  for (const auto index : {1U, 2U}) {
    auto changed = original;
    changed["relations"][index]["base_write_role"] = "trigger_audit";
    EXPECT_EQ(decode_catalog(changed.dump()).status().code(), ErrorCode::InvalidArgument);
  }
}

TEST(TriggerAuditCatalog, ExistingRoleEncodingRetainsOriginalFieldPresenceAndSpelling) {
  const std::vector<DefinitionOptions> options{{},
                                               {.partition_key = {},
                                                .base_write_role = BaseWriteRole::RequestCapture,
                                                .identity_domain = "requests"},
                                               {.partition_key = {},
                                                .base_write_role = BaseWriteRole::RuntimeStatus,
                                                .identity_domain = {}}};
  std::vector<Json> wires;
  for (const auto& option : options) {
    auto catalog = take(Catalog::create());
    take(catalog->define_sql(audit_table_sql, option));
    const auto snapshot = take(catalog->snapshot());
    auto encoded = take(encode_catalog(snapshot));
    EXPECT_EQ(encoded, snapshot.fingerprint);
    wires.push_back(take(parse_json_exact(encoded)));
  }
  EXPECT_FALSE(wires[0]["relations"][0].contains("base_write_role"));
  EXPECT_EQ(wires[1]["relations"][0]["base_write_role"], "request_capture");
  EXPECT_EQ(wires[2]["relations"][0]["base_write_role"], "runtime_status");
  auto expected_status = wires[0];
  expected_status["relations"][0]["base_write_role"] = "runtime_status";
  EXPECT_EQ(wires[2], expected_status);
}
} // namespace
} // namespace eventflow::test
