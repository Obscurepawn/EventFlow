#include "eventflow/typed_trigger.hpp"

namespace eventflow {
StatusOr<std::vector<Field>> trigger_event_fields(const CatalogSnapshot& catalog,
                                                  const EventSource& source) noexcept {
  EF_ASSIGN_OR_RETURN(auto relation, catalog.find(source.payload_relation));
  if (relation->visible_count > relation->storage_schema.fields.size())
    return Status(ErrorCode::InvalidArgument, "Typed event relation has an invalid visible schema");

  return std::vector<Field>(relation->storage_schema.fields.begin(),
                            relation->storage_schema.fields.begin() + relation->visible_count);
}

Status validate_typed_event_key(const CatalogSnapshot& catalog, const EventSource& source,
                                const std::vector<std::string>& keys) noexcept {
  if (source.kind.empty() || source.kind.size() > 128 || !valid_utf8(source.kind) ||
      source.kind.find('\0') != std::string::npos || source.kind == "*" ||
      source.kind == "$timeout" || source.kind == "$expire")
    return Status(ErrorCode::InvalidArgument, "Typed source requires a concrete event kind");

  EF_ASSIGN_OR_RETURN(auto relation, catalog.find(source.payload_relation));
  if (keys.empty() || keys.size() != relation->partition_visible_slots.size())
    return Status(ErrorCode::InvalidArgument,
                  "Typed key_by must explicitly match the Catalog partition key");

  std::set<std::string> unique;
  for (std::size_t i = 0; i < keys.size(); ++i) {
    const auto slot = relation->partition_visible_slots[i];
    if (slot >= relation->visible_count || slot >= relation->storage_schema.fields.size() ||
        keys[i] != relation->storage_schema.fields[slot].name || !unique.insert(keys[i]).second)
      return Status(ErrorCode::InvalidArgument,
                    "Typed key_by differs from the Catalog partition key");
  }

  return {};
}

namespace typed_trigger_detail {
StatusOr<PatternLabelInfo> select_label(const std::vector<PatternLabelInfo>& labels,
                                        std::string_view name) noexcept {
  const PatternLabelInfo* found = nullptr;
  for (const auto& label : labels)
    if (label.label == name) {
      if (found)
        return Status(ErrorCode::InvalidArgument, "Match projection label is ambiguous");

      found = &label;
    }
  if (!found)
    return Status(ErrorCode::InvalidArgument, "Match projection references an unknown label");

  return PatternLabelInfo(*found);
}

StatusOr<std::vector<const CepEvent*>> matched_events(const TriggerOccurrence& occurrence,
                                                      const PatternLabelInfo& label) noexcept {
  if (!occurrence.cep_match)
    return Status(ErrorCode::InvalidArgument, "Match projection requires a CEP occurrence");

  std::vector<const CepEvent*> result;
  for (const auto& capture : occurrence.cep_match->events)
    if (capture.label == label.label) {
      if (capture.event.kind != label.event.kind)
        return Status(ErrorCode::InvalidArgument,
                      "Match capture kind differs from its declared label");

      result.push_back(&capture.event);
      if (result.size() > label.maximum)
        return Status(ErrorCode::InvalidArgument,
                      "Match capture exceeds declared label cardinality");
    }
  if (result.size() < label.minimum)
    return Status(ErrorCode::InvalidArgument, "Match capture is missing a required label");

  return result;
}

StatusOr<std::string> projection_signature(const std::vector<std::string>& labels,
                                           const std::vector<PatternLabelInfo>& metadata,
                                           const std::vector<Field>& output,
                                           const std::vector<std::vector<Field>>& inputs,
                                           std::string_view identity) noexcept {
  if (identity.empty() || !valid_utf8(identity) || identity.size() > 256 ||
      identity.find('\0') != std::string_view::npos)
    return Status(ErrorCode::InvalidArgument,
                  "Match projection requires an explicit implementation identity");

  Json bound = Json::array();
  for (const auto& name : labels) {
    EF_ASSIGN_OR_RETURN(auto label, select_label(metadata, name));
    bound.push_back(Json::array(
        {name, label.event.kind, label.event.payload_relation, label.minimum, label.maximum}));
  }

  Json fields = Json::array();
  for (const auto& field : output)
    fields.push_back(Json::array({field.name, static_cast<int>(field.type), field.nullable}));

  Json input_fields = Json::array();
  for (const auto& fields : inputs) {
    Json encoded = Json::array();
    for (const auto& field : fields)
      encoded.push_back(Json::array({field.name, static_cast<int>(field.type), field.nullable}));

    input_fields.push_back(std::move(encoded));
  }

  return Json{{"format", "match_projection.v1"},
              {"identity", identity},
              {"labels", std::move(bound)},
              {"inputs", std::move(input_fields)},
              {"output", std::move(fields)}}
      .dump();
}
} // namespace typed_trigger_detail
} // namespace eventflow
