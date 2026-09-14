#pragma once
#include "eventflow/catalog.hpp"

namespace eventflow::sql_internal {
inline bool same_fields(const std::vector<Field>& left, const std::vector<Field>& right) noexcept {
  if (left.size() != right.size())
    return false;

  for (std::size_t i = 0; i < left.size(); ++i)
    if (left[i].id != right[i].id || left[i].name != right[i].name ||
        left[i].type != right[i].type || left[i].nullable != right[i].nullable)
      return false;

  return true;
}

inline Json fields_json(const std::vector<Field>& fields) noexcept {
  Json result = Json::array();
  for (const auto& field : fields)
    result.push_back({{"id", field.id},
                      {"name", field.name},
                      {"type", data_type_name(field.type)},
                      {"nullable", field.nullable}});

  return result;
}

inline bool latest_provenance_name(std::string_view name) noexcept {
  return name == "__ef_latest_candidate_epoch" || name == "__ef_latest_admission_seq";
}

inline bool generated_projection(const std::string& name, const BoundExprPtr& expression,
                                 const std::vector<Field>& inputs) noexcept {
  return latest_provenance_name(name) && expression && expression->kind == BoundExprKind::Input &&
         expression->slot < inputs.size() && inputs[expression->slot].name == name &&
         inputs[expression->slot].type == DataType::Int64 && !inputs[expression->slot].nullable;
}

Status validate_relation_metadata(const RelationDescriptor& relation) noexcept;
// Only Catalog calls this after owning and validating its immutable snapshot.
StatusOr<BoundPlan> bind_select_trusted(const SelectAst& query,
                                        const CatalogSnapshot& catalog) noexcept;

// Follow direct projections through the immutable dependency DAG. A partition
// can have several visible aliases; the descriptor stores only the first one.
inline std::optional<std::size_t> partition_lineage(const RelationDescriptor& source,
                                                    std::size_t visible_slot,
                                                    const CatalogSnapshot& catalog) noexcept {
  const auto* relation = &source;
  for (std::size_t remaining = catalog.relations.size() + 1; remaining > 0; --remaining) {
    if (visible_slot >= relation->visible_count)
      return {};
    if (relation->kind == RelationKind::LatestView) {
      if (!relation->latest)
        return {};

      auto source = catalog.find(relation->latest->definition.result_relation);
      if (!source.ok() || visible_slot >= (*source)->visible_count)
        return {};

      relation = *source;
      continue;
    }

    if (relation->kind != RelationKind::SqlView) {
      for (std::size_t key = 0; key < relation->partition_visible_slots.size(); ++key)
        if (relation->partition_visible_slots[key] == visible_slot)
          return key;

      return {};
    }

    if (!relation->plan)
      return {};

    const auto& plan = *relation->plan;
    if (visible_slot >= plan.projection.size() || !plan.projection[visible_slot] ||
        plan.projection[visible_slot]->kind != BoundExprKind::Input)
      return {};

    const auto slot = plan.projection[visible_slot]->slot;
    if (!plan.group_by.empty())
      return slot < plan.group_by.size() ? std::optional<std::size_t>(slot) : std::nullopt;

    auto left = catalog.find(plan.left);
    if (!left.ok())
      return {};
    if (slot < (*left)->visible_count) {
      relation = *left;
      visible_slot = slot;
    } else {
      if (!plan.right)
        return {};

      auto right = catalog.find(*plan.right);
      if (!right.ok())
        return {};

      visible_slot = slot - (*left)->visible_count;
      relation = *right;
    }
  }

  return {};
}
} // namespace eventflow::sql_internal
