#include "engine_internal.hpp"
#include "eventflow/key_codec.hpp"
#include "trigger_audit_internal.hpp"
#include <algorithm>
#include <limits>
#include <map>

namespace eventflow {

CreateTableAst trigger_audit_table(const std::vector<DataType>& scope_types) noexcept {
  CreateTableAst table{.name = std::string(trigger_audit_table_name),
                       .columns = {},
                       .primary_key = {},
                       .partition_key = {}};
  for (std::size_t i = 0; i < scope_types.size(); ++i) {
    auto name = "scope_key_" + std::to_string(i);
    table.columns.push_back({.name = name, .type = scope_types[i], .nullable = false});
    table.primary_key.push_back(name);
    table.partition_key.push_back(std::move(name));
  }

  const std::vector<ColumnAst> metadata{
      {.name = "occurrence_id", .type = DataType::String, .nullable = false},
      {.name = "rule_id", .type = DataType::String, .nullable = false},
      {.name = "rule_version", .type = DataType::String, .nullable = false},
      {.name = "emission", .type = DataType::String, .nullable = false},
      {.name = "event_kind", .type = DataType::String, .nullable = false},
      {.name = "occurred_at_us", .type = DataType::Int64, .nullable = false},
      {.name = "evaluated_at_us", .type = DataType::Int64, .nullable = false},
      {.name = "scheduled_at_us", .type = DataType::Int64, .nullable = true},
      {.name = "skipped_slots", .type = DataType::String, .nullable = false},
      {.name = "firing_revision", .type = DataType::String, .nullable = false},
      {.name = "request_id", .type = DataType::String, .nullable = true},
      {.name = "binding_id", .type = DataType::String, .nullable = true},
      {.name = "plan", .type = DataType::String, .nullable = true},
      {.name = "candidate_epoch", .type = DataType::String, .nullable = true},
      {.name = "request_mode", .type = DataType::String, .nullable = true},
      {.name = "intent_deadline_us", .type = DataType::Int64, .nullable = true},
      {.name = "source_id", .type = DataType::String, .nullable = true},
      {.name = "source_topic", .type = DataType::String, .nullable = true},
      {.name = "source_partition", .type = DataType::Int64, .nullable = true},
      {.name = "source_offset", .type = DataType::Int64, .nullable = true}};
  table.columns.insert(table.columns.end(), metadata.begin(), metadata.end());
  table.primary_key.push_back("occurrence_id");

  return table;
}

StatusOr<RelationId>
define_trigger_audit_relation(Catalog& catalog, const std::vector<DataType>& scope_types) noexcept {
  return catalog.define(
      StatementAst(trigger_audit_table(scope_types)),
      {.partition_key = {}, .base_write_role = BaseWriteRole::TriggerAudit, .identity_domain = {}});
}

Status validate_trigger_audit_relation(const CatalogSnapshot& catalog,
                                       const TriggerAuditConfig& config,
                                       const std::vector<DataType>& scope_types) noexcept {
  if (!config.relation)
    return {};
  if (!config.max_rows_per_scope || !config.max_bytes_per_scope)
    return Status(ErrorCode::InvalidArgument, "trigger audit retention limits must be positive");

  EF_ASSIGN_OR_RETURN(auto relation, catalog.find(config.relation));
  const auto expected = trigger_audit_table(scope_types);
  if (relation->name != expected.name || relation->kind != RelationKind::BaseTable ||
      relation->base_write_role != BaseWriteRole::TriggerAudit ||
      !relation->identity_domain.empty() || relation->candidate || relation->request_status ||
      relation->function || relation->latest || relation->plan ||
      relation->partition_types != scope_types ||
      relation->visible_count != expected.columns.size() ||
      relation->storage_schema.fields.size() != expected.columns.size() ||
      relation->storage_schema.primary_key != expected.primary_key ||
      relation->storage_schema.partition_key != expected.partition_key)
    return Status(ErrorCode::InvalidArgument,
                  "trigger audit relation does not match its builtin schema");

  for (std::size_t i = 0; i < expected.columns.size(); ++i) {
    const auto& actual = relation->storage_schema.fields[i];
    const auto& field = expected.columns[i];
    if (actual.name != field.name || actual.type != field.type || actual.nullable != field.nullable)
      return Status(ErrorCode::InvalidArgument,
                    "trigger audit column differs from its builtin schema");
  }

  return {};
}

namespace {

struct AuditRow {
  Row row;
  Row identity;
  std::size_t bytes = 0;
  bool existed = false;
};

struct AuditScope {
  Row scope;
  std::vector<Row> incoming;
};

StatusOr<AuditRow> audit_row(RoundContext& round, const RelationDescriptor& relation,
                             const Row& row, bool existed) noexcept {
  EF_ASSIGN_OR_RETURN(auto identity, physical_identity(relation, row));
  EF_ASSIGN_OR_RETURN(auto key, row_key(round.shard(), 'T', relation.id, identity));
  EF_ASSIGN_OR_RETURN(auto encoded, encode_row(relation.storage_schema, row));
  if (encoded.size() > std::numeric_limits<std::size_t>::max() - key.size())
    return Status(ErrorCode::ResourceExhausted, "trigger audit row size overflow");

  return AuditRow{.row = row,
                  .identity = std::move(identity),
                  .bytes = encoded.size() + key.size(),
                  .existed = existed};
}

} // namespace

Status write_trigger_audit_rows(RoundContext& round, const RelationDescriptor& relation,
                                const TriggerAuditConfig& config, const std::vector<Row>& rows,
                                std::size_t max_record_bytes) noexcept {
  std::map<std::string, AuditScope> scopes;
  for (const auto& row : rows) {
    EF_RETURN_IF_ERROR(relation.storage_schema.validate(row));
    EF_ASSIGN_OR_RETURN(auto scope, partition_values(relation, row));
    EF_ASSIGN_OR_RETURN(auto key, encode_key_parts(scope));
    const auto [found, inserted] =
        scopes.try_emplace(key, AuditScope{.scope = scope, .incoming = {}});
    (void)inserted;
    found->second.incoming.push_back(row);
  }

  const auto id_column =
      relation.partition_types.size() + static_cast<std::size_t>(TriggerAuditColumn::OccurrenceId);
  const auto time_column =
      relation.partition_types.size() + static_cast<std::size_t>(TriggerAuditColumn::EvaluatedAtUs);
  for (const auto& [key, scope] : scopes) {
    (void)key;

    // One bounded scan per scope, even when several rules emit in the same round.
    EF_ASSIGN_OR_RETURN(auto previous,
                        round.scan_relation(relation.id, scope.scope, StateImage::Staged,
                                            config.max_rows_per_scope, config.max_bytes_per_scope));
    std::map<std::string, AuditRow> retained;
    std::size_t bytes = 0;

    for (const auto& row : previous) {
      EF_ASSIGN_OR_RETURN(auto item, audit_row(round, relation, row, true));
      bytes += item.bytes; // The successful scan already bounded their combined charge.
      retained.emplace(std::get<std::string>(row[id_column]), std::move(item));
    }

    for (const auto& row : scope.incoming) {
      EF_ASSIGN_OR_RETURN(auto item, audit_row(round, relation, row, false));
      if (item.bytes > max_record_bytes || item.bytes > config.max_bytes_per_scope ||
          item.bytes > std::numeric_limits<std::size_t>::max() - bytes)
        return Status(ErrorCode::ResourceExhausted,
                      "one trigger audit row exceeds its byte budget");

      bytes += item.bytes;
      if (!retained.emplace(std::get<std::string>(row[id_column]), std::move(item)).second)
        return Status(ErrorCode::Conflict, "trigger occurrence already has an audit row");
    }

    std::vector<decltype(retained)::iterator> ordered;
    for (auto it = retained.begin(); it != retained.end(); ++it)
      ordered.push_back(it);

    std::sort(ordered.begin(), ordered.end(), [&](const auto& left, const auto& right) noexcept {
      const auto left_time = std::get<std::int64_t>(left->second.row[time_column]);
      const auto right_time = std::get<std::int64_t>(right->second.row[time_column]);
      return left_time == right_time ? left->first < right->first : left_time < right_time;
    });

    for (auto it : ordered) {
      if (retained.size() <= config.max_rows_per_scope && bytes <= config.max_bytes_per_scope)
        break;

      bytes -= it->second.bytes;
      if (it->second.existed)
        EF_RETURN_IF_ERROR(EngineRuntimeAccess::write_trigger_audit(
            round,
            {.relation = relation.id, .identity = it->second.identity, .row = std::nullopt}));

      retained.erase(it);
    }

    for (const auto& [id, item] : retained) {
      (void)id;
      if (!item.existed)
        EF_RETURN_IF_ERROR(EngineRuntimeAccess::write_trigger_audit(
            round, {.relation = relation.id, .identity = item.identity, .row = item.row}));
    }
  }

  return {};
}

} // namespace eventflow
