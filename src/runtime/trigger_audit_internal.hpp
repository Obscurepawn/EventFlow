#pragma once

#include "eventflow/engine.hpp"
#include "eventflow/trigger_audit.hpp"

namespace eventflow {

inline constexpr std::string_view trigger_audit_table_name = "trigger_audit";

// Column offsets after the canonical scope_key_N columns. Keep construction and
// validation together so persisted rows cannot silently change interpretation.
enum class TriggerAuditColumn : std::size_t {
  OccurrenceId,
  RuleId,
  RuleVersion,
  Emission,
  EventKind,
  OccurredAtUs,
  EvaluatedAtUs,
  ScheduledAtUs,
  SkippedSlots,
  FiringRevision,
  RequestId,
  BindingId,
  Plan,
  CandidateEpoch,
  RequestMode,
  IntentDeadlineUs,
  SourceId,
  SourceTopic,
  SourcePartition,
  SourceOffset
};

CreateTableAst trigger_audit_table(const std::vector<DataType>& scope_types) noexcept;
StatusOr<RelationId>
define_trigger_audit_relation(Catalog& catalog, const std::vector<DataType>& scope_types) noexcept;
Status validate_trigger_audit_relation(const CatalogSnapshot& catalog,
                                       const TriggerAuditConfig& config,
                                       const std::vector<DataType>& scope_types) noexcept;

Status write_trigger_audit_rows(RoundContext& round, const RelationDescriptor& relation,
                                const TriggerAuditConfig& config, const std::vector<Row>& rows,
                                std::size_t max_record_bytes) noexcept;

} // namespace eventflow
