#pragma once
#include "eventflow/observation_definition.hpp"
#include "eventflow/sql_ast.hpp"
#include <limits>

namespace eventflow {
using RelationId = std::uint32_t;
inline constexpr std::size_t kNoSlot = std::numeric_limits<std::size_t>::max();
enum class RelationKind { BaseTable, SqlView, FunctionView, LatestView };
enum class BaseWriteRole { PublicInput, RequestCapture, RuntimeStatus, TriggerAudit };
enum class BoundExprKind { Input, Literal, Unary, Binary, IsNull, Cast };
struct BoundExpr;
using BoundExprPtr = std::shared_ptr<const BoundExpr>;

struct BoundExpr {
  BoundExprKind kind = BoundExprKind::Literal;
  DataType type = DataType::Int64;
  bool nullable = false;
  bool untyped_null = false;
  std::size_t slot = 0;
  Value literal;
  std::string op;
  std::vector<BoundExprPtr> args;
};
enum class AggregateKind { CountAll, Count, Sum, Average };

struct AggregateSpec {
  AggregateKind kind;
  BoundExprPtr argument;
  DataType result_type = DataType::Int64;
};

struct BoundPlan {
  std::uint64_t catalog_version = 0;
  std::string catalog_fingerprint; // Exact binding provenance, excluded from definition JSON.
  RelationId left = 0;
  std::optional<RelationId> right;
  BoundExprPtr filter; // Bound to visible left row concatenated with visible right row.
  std::vector<BoundExprPtr> group_by; // Canonical partition order, before aggregation.
  std::vector<AggregateSpec> aggregates;
  // Bound to the joined row, or [group columns, aggregate outputs] when grouped.
  std::vector<BoundExprPtr> projection;
  std::vector<Field> output_fields;
  std::vector<DataType> identity_types;
  std::vector<DataType> partition_types;
  std::vector<std::size_t> partition_visible_slots; // kNoSlot when projected away.
  bool unique_per_partition = false;
  std::optional<Row> inferred_scope;
  // Only an unchanged full request identity retains this domain through SQL.
  std::string identity_domain = {};
  std::vector<Field> identity_fields = {};
};

struct FunctionInput {
  std::string alias;
  RelationId relation = 0;
  bool required = true;
  std::vector<Field> fields; // Exact visible input signature, in declared order.
};

struct FunctionViewDefinition {
  std::string name;
  std::string function_id;
  std::uint64_t function_version = 0;
  std::string implementation_identity;
  std::vector<FunctionInput> inputs;
  std::vector<Field> output_fields; // Worker payload only; identity is owned by the host.
};

struct FunctionViewPlan {
  std::uint64_t catalog_version = 0;
  std::string catalog_fingerprint; // Binding provenance, excluded from definition JSON.
  FunctionViewDefinition definition;
};

struct CandidateDefinition {
  std::string name;
  std::vector<Field> scope_fields;
  std::vector<Field> payload_fields;
};

struct CandidateAnnotation {
  std::uint32_t format = 1;
  CandidateDefinition definition;
};

struct RequestStatusDefinition {
  std::string name;
  std::string request_domain;
  std::vector<Field> identity_fields;
  RelationId candidate_relation = 0;
};

struct RequestStatusAnnotation {
  std::uint32_t format = 1;
  RequestStatusDefinition definition;
};

struct LatestViewDefinition {
  std::string name;
  RelationId result_relation = 0;
  RelationId request_status_relation = 0;
  RelationId candidate_relation = 0;
};

struct LatestViewPlan {
  std::uint64_t catalog_version = 0;
  std::string catalog_fingerprint;
  std::uint32_t format = 1;
  LatestViewDefinition definition;
};

struct RelationDescriptor {
  RelationId id = 0;
  std::string name;
  Schema storage_schema; // Visible columns first; MV hidden identity columns follow.
  std::size_t visible_count = 0;
  std::vector<std::size_t> physical_key_slots; // Partition fields first.
  std::vector<std::size_t> partition_slots;
  std::vector<std::size_t> partition_visible_slots;
  std::vector<DataType> identity_types;
  std::vector<DataType> partition_types;
  bool unique_per_partition = false;
  std::uint32_t state_id = 0;
  std::shared_ptr<const BoundPlan> plan; // Present only for SqlView.
  RelationKind kind = RelationKind::BaseTable;
  BaseWriteRole base_write_role = BaseWriteRole::PublicInput; // Only meaningful for BaseTable.
  std::string identity_domain = {};        // Nonempty only for full request-identity relations.
  std::vector<Field> identity_fields = {}; // Canonical physical identity names and types.
  std::shared_ptr<const FunctionViewPlan> function = {}; // Present only for FunctionView.
  std::shared_ptr<const CandidateAnnotation> candidate = {};
  std::shared_ptr<const RequestStatusAnnotation> request_status = {};
  std::shared_ptr<const LatestViewPlan> latest = {}; // Present only for LatestView.
};

struct CatalogSnapshot {
  std::uint64_t version = 0;
  std::vector<std::shared_ptr<const RelationDescriptor>> relations; // ID/dependency order.
  std::string fingerprint; // Canonical definitions, not a lossy hash.
  std::vector<ObservationRuleDescriptor> observations = {};
  StatusOr<const RelationDescriptor*> find(RelationId id) const noexcept;
  StatusOr<const RelationDescriptor*> find(const std::string& name) const noexcept;
  StatusOr<const ObservationRuleDescriptor*> find_observation(std::string_view name) const noexcept;
};

struct DefinitionOptions {
  std::vector<std::string> partition_key;
  BaseWriteRole base_write_role = BaseWriteRole::PublicInput;
  std::string identity_domain = {}; // Required only for RequestCapture definitions.
};

// One dependency graph for SQL and functions; repeated inputs are returned once.
StatusOr<std::vector<RelationId>> dependencies(const RelationDescriptor& relation) noexcept;
StatusOr<ObservationGraph> observation_graph(const CatalogSnapshot& catalog) noexcept;
StatusOr<Json> observation_json(const ObservationRuleDescriptor& observation) noexcept;

class Catalog {
public:
  static StatusOr<std::unique_ptr<Catalog>> create() noexcept;
  static StatusOr<std::unique_ptr<Catalog>> from_snapshot(const CatalogSnapshot& snapshot) noexcept;
  StatusOr<CatalogSnapshot> snapshot() const noexcept;
  // Defines metadata only. Runtime activation and initialization belong to Engine.
  StatusOr<RelationId> define(const StatementAst& statement,
                              const DefinitionOptions& options = {}) noexcept;
  StatusOr<RelationId> define_sql(std::string_view sql,
                                  const DefinitionOptions& options = {}) noexcept;
  // Metadata only. Function execution and activation belong to the shard owner.
  StatusOr<RelationId> define_function(const FunctionViewDefinition& definition) noexcept;
  StatusOr<RelationId> define_candidate(const CandidateDefinition& definition) noexcept;
  StatusOr<RelationId> define_request_status(const RequestStatusDefinition& definition) noexcept;
  StatusOr<RelationId> define_latest(const LatestViewDefinition& definition) noexcept;
  StatusOr<std::uint32_t> define_observation(const ObservationRuleDefinition& definition) noexcept;
  StatusOr<BoundPlan> bind_query(const SelectAst& query) const noexcept;
  StatusOr<BoundPlan> bind_query_sql(std::string_view sql) const noexcept;
  StatusOr<std::string> explain(RelationId relation) const noexcept;

private:
  Catalog() noexcept = default;
  CatalogSnapshot state_;
};

StatusOr<BoundPlan> bind_select(const SelectAst& query, const CatalogSnapshot& catalog) noexcept;
// External query gate: validate structure and provenance against the current catalog.
Status validate_bound_plan(const BoundPlan& plan, const CatalogSnapshot& catalog) noexcept;
StatusOr<Value> evaluate(const BoundExpr& expression, const Row& input) noexcept;
StatusOr<bool> passes_filter(const BoundExprPtr& expression, const Row& input) noexcept;
StatusOr<Row> evaluate_projection(const BoundPlan& plan, const Row& input) noexcept;
StatusOr<Row> visible_row(const RelationDescriptor& relation, const Row& stored) noexcept;
StatusOr<Row> physical_identity(const RelationDescriptor& relation, const Row& stored) noexcept;
StatusOr<Row> partition_values(const RelationDescriptor& relation, const Row& stored) noexcept;
StatusOr<Row> make_stored_row(const RelationDescriptor& relation, const Row& visible,
                              const Row& identity) noexcept;
StatusOr<Json> bound_plan_json(const BoundPlan& plan) noexcept;
} // namespace eventflow
