#include "eventflow/engine.hpp"
#include "engine_internal.hpp"
#include "eventflow/exact_sum.hpp"
#include "eventflow/key_codec.hpp"
#include "eventflow/request_machine.hpp"
#include <algorithm>
#include <bit>
#include <iterator>
#include <limits>
#include <list>
#include <map>
#include <set>

namespace eventflow {
namespace {
using Changes = std::map<RelationId, std::map<std::string, RowChange>>;

// A hook may inspect its RoundContext, but cannot start a nested Engine operation.
struct OperationGuard {
  explicit OperationGuard(bool& active) noexcept : flag(active) {
    flag = true;
  }

  ~OperationGuard() noexcept {
    flag = false;
  }

  bool& flag;
};

StatusOr<std::set<RelationId>> reachable(const CatalogSnapshot& catalog,
                                         std::set<RelationId> roots) noexcept {
  for (const auto& relation : catalog.relations) {
    EF_ASSIGN_OR_RETURN(auto inputs, dependencies(*relation));
    for (auto id : inputs)
      if (roots.contains(id)) {
        roots.insert(relation->id);
        break;
      }
  }

  return roots;
}

StatusOr<std::uint64_t> count_field(const Json& json, const char* name) noexcept {
  if (!json.is_object() || !json.contains(name) || !json[name].is_number_integer())
    return Status(ErrorCode::FailedPrecondition, "invalid aggregate count");

  const auto& value = json[name];
  if (!value.is_number_unsigned() && value.get<std::int64_t>() < 0)
    return Status(ErrorCode::FailedPrecondition, "negative aggregate count");

  const auto count = value.get<std::uint64_t>();
  if (count > static_cast<std::uint64_t>(INT64_MAX))
    return Status(ErrorCode::FailedPrecondition, "aggregate count overflow");

  return std::uint64_t(count);
}

Status increment(std::uint64_t& value, bool add) noexcept {
  if (add) {
    if (value == static_cast<std::uint64_t>(INT64_MAX))
      return Status(ErrorCode::ResourceExhausted, "aggregate count exhausted");

    ++value;
  } else {
    if (value == 0)
      return Status(ErrorCode::FailedPrecondition, "aggregate count underflow");

    --value;
  }

  return {};
}

struct Accumulator {
  std::uint64_t count = 0;
  ExactSum sum;
};

struct GroupState {
  std::uint64_t rows = 0;
  std::vector<Accumulator> values;
};

StatusOr<GroupState> decode_group(const BoundPlan& plan,
                                  const std::optional<std::string>& bytes) noexcept {
  GroupState result;
  result.values.resize(plan.aggregates.size());
  if (!bytes)
    return result;

  auto parsed = parse_json_exact(*bytes);
  if (!parsed.ok())
    return Status(ErrorCode::FailedPrecondition, "invalid aggregate state JSON");

  const auto& json = *parsed;
  if (!json.is_object() || !json.contains("format") || json["format"] != 1 ||
      !json.contains("values") || !json["values"].is_array() ||
      json["values"].size() != result.values.size())
    return Status(ErrorCode::FailedPrecondition, "aggregate state layout mismatch");

  EF_ASSIGN_OR_RETURN(result.rows, count_field(json, "rows"));
  if (result.rows == 0)
    return Status(ErrorCode::FailedPrecondition, "empty group must not be stored");

  for (std::size_t i = 0; i < result.values.size(); ++i) {
    const auto& value = json["values"][i];
    EF_ASSIGN_OR_RETURN(result.values[i].count, count_field(value, "count"));
    if (result.values[i].count > result.rows || !value.contains("sum") || !value["sum"].is_string())
      return Status(ErrorCode::FailedPrecondition, "inconsistent aggregate state");

    auto encoded = hex_decode(value["sum"].get_ref<const std::string&>());
    if (!encoded.ok())
      return Status(ErrorCode::FailedPrecondition, "invalid sum bytes");

    auto sum = ExactSum::decode(*encoded);
    if (!sum.ok())
      return Status(ErrorCode::FailedPrecondition, "invalid sum state");

    result.values[i].sum = std::move(sum).value();
    const auto kind = plan.aggregates[i].kind;
    if ((result.values[i].count == 0 || kind == AggregateKind::Count ||
         kind == AggregateKind::CountAll) &&
        !result.values[i].sum.empty())
      return Status(ErrorCode::FailedPrecondition, "invalid count-only accumulator");
    if (kind == AggregateKind::CountAll && result.values[i].count != result.rows)
      return Status(ErrorCode::FailedPrecondition, "COUNT(*) state mismatch");
  }

  return result;
}

StatusOr<std::string> encode_group(const GroupState& group) noexcept {
  Json values = Json::array();
  for (const auto& value : group.values) {
    EF_ASSIGN_OR_RETURN(auto raw, value.sum.encode());
    EF_ASSIGN_OR_RETURN(auto sum, hex_encode(raw));
    values.push_back({{"count", value.count}, {"sum", std::move(sum)}});
  }

  return Json{{"format", 1}, {"rows", group.rows}, {"values", std::move(values)}}.dump();
}

Status contribute(GroupState& group, const BoundPlan& plan, const Row& row, bool add) noexcept {
  EF_RETURN_IF_ERROR(increment(group.rows, add));
  for (std::size_t i = 0; i < plan.aggregates.size(); ++i) {
    const auto& spec = plan.aggregates[i];
    auto& accumulator = group.values[i];
    if (spec.kind == AggregateKind::CountAll) {
      EF_RETURN_IF_ERROR(increment(accumulator.count, add));
      continue;
    }

    EF_ASSIGN_OR_RETURN(auto value, evaluate(*spec.argument, row));
    if (std::holds_alternative<std::monostate>(value))
      continue;

    EF_RETURN_IF_ERROR(increment(accumulator.count, add));
    if (spec.kind == AggregateKind::Count)
      continue;
    if (const auto* integer = std::get_if<std::int64_t>(&value))
      EF_RETURN_IF_ERROR(add ? accumulator.sum.add_int(*integer)
                             : accumulator.sum.remove_int(*integer));
    else if (const auto* real = std::get_if<double>(&value))
      EF_RETURN_IF_ERROR(add ? accumulator.sum.add(*real) : accumulator.sum.remove(*real));
    else
      return Status(ErrorCode::Internal, "non-numeric aggregate argument");
  }

  return {};
}

StatusOr<Row> aggregate_row(const GroupState& group, const BoundPlan& plan,
                            const Row& key) noexcept {
  Row result = key;
  for (std::size_t i = 0; i < plan.aggregates.size(); ++i) {
    const auto& spec = plan.aggregates[i];
    const auto& value = group.values[i];
    if (spec.kind == AggregateKind::Count || spec.kind == AggregateKind::CountAll)
      result.emplace_back(static_cast<std::int64_t>(value.count));
    else if (value.count == 0)
      result.emplace_back(std::monostate{});
    else if (spec.kind == AggregateKind::Average) {
      EF_ASSIGN_OR_RETURN(auto average, value.sum.to_double(value.count));
      result.emplace_back(average);
    } else if (spec.result_type == DataType::Int64) {
      EF_ASSIGN_OR_RETURN(auto sum, value.sum.to_int64());
      result.emplace_back(sum);
    } else {
      EF_ASSIGN_OR_RETURN(auto sum, value.sum.to_double());
      result.emplace_back(sum);
    }
  }

  return result;
}

bool same_images(const std::optional<Row>& left, const std::optional<Row>& right) noexcept {
  if (!left || !right)
    return left.has_value() == right.has_value();
  if (left->size() != right->size())
    return false;

  for (std::size_t i = 0; i < left->size(); ++i) {
    const auto& a = (*left)[i];
    const auto& b = (*right)[i];
    if (a.index() != b.index())
      return false;
    if (const auto* scalar = std::get_if<double>(&a)) {
      if (std::bit_cast<std::uint64_t>(*scalar) !=
          std::bit_cast<std::uint64_t>(std::get<double>(b)))
        return false;
    } else if (const auto* vector = std::get_if<std::vector<double>>(&a)) {
      const auto& other = std::get<std::vector<double>>(b);
      if (vector->size() != other.size())
        return false;

      for (std::size_t j = 0; j < vector->size(); ++j)
        if (std::bit_cast<std::uint64_t>((*vector)[j]) != std::bit_cast<std::uint64_t>(other[j]))
          return false;
    } else if (a != b)
      return false;
  }

  return true;
}

std::vector<RowChange> flatten(const Changes& changes) noexcept {
  std::vector<RowChange> result;
  for (const auto& [relation, rows] : changes) {
    (void)relation;
    for (const auto& [key, row] : rows) {
      (void)key;
      if (!same_images(row.old_row, row.new_row))
        result.push_back(row);
    }
  }

  return result;
}

Status validate_scope(const Row& scope, const std::vector<DataType>& types) noexcept {
  if (scope.size() != types.size() || types.empty())
    return Status(ErrorCode::InvalidArgument, "query requires complete partition scope");

  for (std::size_t i = 0; i < scope.size(); ++i) {
    const auto& value = scope[i];
    const bool valid =
        (types[i] == DataType::String && std::holds_alternative<std::string>(value)) ||
        (types[i] == DataType::Bool && std::holds_alternative<bool>(value)) ||
        ((types[i] == DataType::Int64 || types[i] == DataType::TimestampMicros) &&
         std::holds_alternative<std::int64_t>(value));
    if (!valid)
      return Status(ErrorCode::InvalidArgument, "partition scope type mismatch");
    if (const auto* text = std::get_if<std::string>(&value); text && !valid_utf8(*text))
      return Status(ErrorCode::InvalidArgument, "invalid UTF-8 key");
  }

  return {};
}

struct LatestState {
  std::int64_t epoch = 0;
  std::int64_t sequence = 0;
  Row winner;
};

StatusOr<std::string> encode_latest(const LatestState& state) noexcept {
  Json identity = Json::array();
  for (const auto& value : state.winner) {
    EF_ASSIGN_OR_RETURN(auto encoded, value_to_json(value));
    identity.push_back(std::move(encoded));
  }

  return Json{{"format", 1},
              {"epoch", state.epoch},
              {"sequence", state.sequence},
              {"winner", std::move(identity)}}
      .dump();
}

StatusOr<LatestState> decode_latest(std::string_view bytes, const RelationDescriptor& source,
                                    const Row& scope) noexcept {
  EF_ASSIGN_OR_RETURN(auto json, parse_json_exact(bytes));
  if (!json.is_object() || json.size() != 4 || !json.contains("format") || json["format"] != 1 ||
      !json.contains("winner") || !json["winner"].is_array())
    return Status(ErrorCode::FailedPrecondition, "invalid Latest state encoding");

  EF_ASSIGN_OR_RETURN(auto epoch, count_field(json, "epoch"));
  EF_ASSIGN_OR_RETURN(auto sequence, count_field(json, "sequence"));
  if (epoch == 0 || (sequence == 0 && !json["winner"].empty()) ||
      (sequence != 0 && json["winner"].size() != source.identity_types.size()))
    return Status(ErrorCode::FailedPrecondition, "invalid Latest winner identity");

  LatestState state{.epoch = static_cast<std::int64_t>(epoch),
                    .sequence = static_cast<std::int64_t>(sequence),
                    .winner = {}};
  for (std::size_t i = 0; i < json["winner"].size(); ++i) {
    EF_ASSIGN_OR_RETURN(auto value,
                        value_from_json(json["winner"][i], source.identity_types[i], false));
    state.winner.push_back(std::move(value));
  }

  if (!state.winner.empty() && !std::equal(scope.begin(), scope.end(), state.winner.begin()))
    return Status(ErrorCode::FailedPrecondition, "Latest winner belongs to another scope");

  return state;
}

StatusOr<std::string> request_phase_name(RequestPhase phase) noexcept {
  switch (phase) {
  case RequestPhase::Accepted:
    return std::string("ACCEPTED");
  case RequestPhase::Running:
    return std::string("RUNNING");
  case RequestPhase::Success:
    return std::string("SUCCEEDED");
  case RequestPhase::Failed:
    return std::string("FAILED");
  case RequestPhase::Cancelled:
    return std::string("CANCELLED");
  }

  return Status(ErrorCode::InvalidArgument, "invalid request phase");
}

bool request_terminal(std::string_view phase) noexcept {
  return phase == "SUCCEEDED" || phase == "FAILED" || phase == "CANCELLED";
}

StatusOr<std::string> metadata_key(std::uint32_t shard) noexcept {
  return row_key(shard, 'S', 0, {std::string("engine")});
}
} // namespace

struct RoundState {
  std::shared_ptr<IStateStore> store;
  const CatalogSnapshot* catalog = nullptr;
  EngineConfig config;
  std::uint64_t revision = 0;
  std::int64_t time = 0;
  std::map<std::string, std::optional<std::string>> before;
  std::map<std::string, std::optional<std::string>> overlay;
  Changes changes;
  std::set<RelationId> emitted;
  std::size_t read_bytes = 0;
  std::size_t staged_bytes = 0;
  std::size_t input_rows = 0;
  bool runtime_frame = false;
  bool lifecycle_frame = false;
  bool metadata_frame = false;
  bool observer_frame = false;
  std::uint32_t observer_namespace = 0;
  bool deriving_latest = false;
  std::map<std::string, std::string> metadata_authorizations;
  std::size_t metadata_bytes = 0;

  Status authorize_metadata(const TableChange& write) noexcept {
    if ((!runtime_frame && !metadata_frame) || !write.row)
      return Status(ErrorCode::InvalidArgument, "metadata write needs an owner frame and a row");

    EF_ASSIGN_OR_RETURN(auto relation, catalog->find(write.relation));
    EF_ASSIGN_OR_RETURN(auto key, row_key(config.shard, 'T', write.relation, write.identity));
    EF_ASSIGN_OR_RETURN(auto bytes, encode_row(relation->storage_schema, *write.row));
    if (metadata_authorizations.contains(key))
      return Status(ErrorCode::Conflict, "metadata identity already has a pending write");
    if (metadata_authorizations.size() >= config.max_rows ||
        key.size() > config.max_bytes - metadata_bytes ||
        bytes.size() > config.max_bytes - metadata_bytes - key.size())
      return Status(ErrorCode::ResourceExhausted, "metadata intent budget exceeded");

    metadata_bytes += key.size() + bytes.size();
    metadata_authorizations.emplace(std::move(key), std::move(bytes));
    return {};
  }

  Status consume_metadata(const TableChange& write) noexcept {
    if (!write.row)
      return Status(ErrorCode::InvalidArgument, "controlled metadata cannot be deleted");

    EF_ASSIGN_OR_RETURN(auto relation, catalog->find(write.relation));
    EF_ASSIGN_OR_RETURN(auto key, row_key(config.shard, 'T', write.relation, write.identity));
    EF_ASSIGN_OR_RETURN(auto bytes, encode_row(relation->storage_schema, *write.row));
    auto found = metadata_authorizations.find(key);
    if (found == metadata_authorizations.end() || found->second != bytes)
      return Status(ErrorCode::InvalidArgument, "metadata write lacks exact host authorization");

    metadata_bytes -= found->first.size() + found->second.size();
    metadata_authorizations.erase(found);
    return {};
  }

  bool request_state_write = false;
  const std::set<std::uint32_t>* reserved_namespaces = nullptr;

  StatusOr<std::optional<std::string>> read(const std::string& key, StateImage image) noexcept {
    if (image != StateImage::Before && image != StateImage::Staged)
      return Status(ErrorCode::InvalidArgument, "invalid round state image");
    if (!key_in_shard(config.shard, key))
      return Status(ErrorCode::InvalidArgument, "round access crosses shard");
    if (image == StateImage::Staged) {
      auto staged = overlay.find(key);
      if (staged != overlay.end())
        return std::optional<std::string>(staged->second);
    }

    auto existing = before.find(key);
    if (existing != before.end())
      return std::optional<std::string>(existing->second);
    if (key.size() > config.max_bytes - read_bytes)
      return Status(ErrorCode::ResourceExhausted, "round read key exceeds remaining budget");

    EF_ASSIGN_OR_RETURN(auto fetched, store->get_many(config.shard, {key}));
    if (fetched.revision != revision || fetched.values.size() != 1)
      return Status(ErrorCode::Conflict, "round read does not match owner revision");

    const auto bytes = key.size() + (fetched.values[0] ? fetched.values[0]->size() : 0);
    if (bytes > config.max_bytes - read_bytes)
      return Status(ErrorCode::ResourceExhausted, "round read budget exceeded");

    read_bytes += bytes;
    before.emplace(key, fetched.values[0]);
    return std::move(fetched.values[0]);
  }

  Status stage(Mutation mutation) noexcept {
    if (!key_in_shard(config.shard, mutation.key))
      return Status(ErrorCode::InvalidArgument, "round mutation crosses shard");

    const auto previous = overlay.find(mutation.key);
    if (previous == overlay.end() && overlay.size() == config.max_mutations)
      return Status(ErrorCode::ResourceExhausted, "round mutation count exceeded");

    const auto old_size =
        previous == overlay.end()
            ? 0
            : previous->first.size() + (previous->second ? previous->second->size() : 0);
    const auto new_size = mutation.key.size() + (mutation.value ? mutation.value->size() : 0);
    if (new_size > config.max_bytes - (staged_bytes - old_size))
      return Status(ErrorCode::ResourceExhausted, "round mutation bytes exceeded");

    staged_bytes = staged_bytes - old_size + new_size;
    overlay.insert_or_assign(std::move(mutation.key), std::move(mutation.value));
    return {};
  }

  Status validate_latest(const RelationDescriptor& output, const Row& scope,
                         const std::optional<Row>& row, StateImage image) noexcept {
    const auto& definition = output.latest->definition;
    EF_ASSIGN_OR_RETURN(auto source, catalog->find(definition.result_relation));
    EF_ASSIGN_OR_RETURN(auto key, row_key(config.shard, 'S', output.state_id, scope));
    EF_ASSIGN_OR_RETURN(auto bytes, read(key, image));
    EF_ASSIGN_OR_RETURN(auto candidate, read_row(definition.candidate_relation, scope, image));
    if (!bytes) {
      if (row || (candidate && std::get<bool>((*candidate)[scope.size() + 1])))
        return Status(ErrorCode::FailedPrecondition, "Latest selector state is missing");

      return {};
    }

    EF_ASSIGN_OR_RETURN(auto selected, decode_latest(*bytes, *source, scope));
    if (!candidate || selected.epoch != std::get<std::int64_t>((*candidate)[scope.size()]) ||
        (selected.sequence > 0) != row.has_value())
      return Status(ErrorCode::FailedPrecondition, "Latest row/state/candidate disagree");
    if (!row)
      return {};
    if (!std::get<bool>((*candidate)[scope.size() + 1]) ||
        std::get<std::int64_t>((*row)[output.visible_count - 2]) != selected.epoch ||
        std::get<std::int64_t>((*row)[output.visible_count - 1]) != selected.sequence)
      return Status(ErrorCode::FailedPrecondition, "Latest row provenance is inconsistent");

    EF_ASSIGN_OR_RETURN(auto value, read_row(source->id, selected.winner, image));
    EF_ASSIGN_OR_RETURN(auto metadata,
                        read_row(definition.request_status_relation, selected.winner, image));
    const auto base = selected.winner.size();
    if (!value || !metadata || std::get<std::string>((*metadata)[base + 2]) != "SUCCEEDED" ||
        std::get<std::int64_t>((*metadata)[base]) != selected.epoch ||
        std::get<std::int64_t>((*metadata)[base + 1]) != selected.sequence)
      return Status(ErrorCode::FailedPrecondition, "Latest winner is not a successful request");

    EF_ASSIGN_OR_RETURN(auto visible, visible_row(*source, *value));
    visible.emplace_back(selected.epoch);
    visible.emplace_back(selected.sequence);
    EF_ASSIGN_OR_RETURN(auto expected, make_stored_row(output, visible, scope));
    if (!same_images(row, std::optional<Row>{std::move(expected)}))
      return Status(ErrorCode::FailedPrecondition,
                    "Latest payload differs from its winning result");

    return {};
  }

  StatusOr<std::optional<Row>> read_row(RelationId id, const Row& identity,
                                        StateImage image) noexcept {
    EF_ASSIGN_OR_RETURN(auto relation, catalog->find(id));
    EF_RETURN_IF_ERROR(validate_scope(identity, relation->identity_types));
    Row scope(identity.begin(), identity.begin() + relation->partition_types.size());
    EF_ASSIGN_OR_RETURN(auto owner_shard, shard_for(scope, config.shard_count));
    if (owner_shard != config.shard)
      return Status(ErrorCode::InvalidArgument, "identity belongs to another shard");

    EF_ASSIGN_OR_RETURN(auto key, row_key(config.shard, 'T', id, identity));
    EF_ASSIGN_OR_RETURN(auto bytes, read(key, image));
    if (!bytes) {
      if (relation->latest && !deriving_latest)
        EF_RETURN_IF_ERROR(validate_latest(*relation, identity, {}, image));

      return std::optional<Row>{};
    }

    EF_ASSIGN_OR_RETURN(auto row, decode_row(relation->storage_schema, *bytes));
    EF_ASSIGN_OR_RETURN(auto actual, physical_identity(*relation, row));
    if (actual != identity)
      return Status(ErrorCode::FailedPrecondition, "stored identity mismatch");
    if (relation->latest && !deriving_latest)
      EF_RETURN_IF_ERROR(validate_latest(*relation, identity, std::optional<Row>{row}, image));

    return std::optional<Row>(std::move(row));
  }

  Status put_row(RelationId id, const Row& identity, const std::optional<Row>& row) noexcept {
    EF_ASSIGN_OR_RETURN(auto relation, catalog->find(id));
    EF_RETURN_IF_ERROR(validate_scope(identity, relation->identity_types));
    Row scope(identity.begin(), identity.begin() + relation->partition_types.size());
    EF_ASSIGN_OR_RETURN(auto owner_shard, shard_for(scope, config.shard_count));
    if (owner_shard != config.shard)
      return Status(ErrorCode::InvalidArgument, "identity belongs to another shard");

    EF_ASSIGN_OR_RETURN(auto key, row_key(config.shard, 'T', id, identity));
    EF_ASSIGN_OR_RETURN(auto prior_bytes, read(key, StateImage::Before));
    std::optional<Row> prior;
    if (prior_bytes) {
      EF_ASSIGN_OR_RETURN(auto value, decode_row(relation->storage_schema, *prior_bytes));
      prior = std::move(value);
    }

    std::optional<std::string> next;
    if (row) {
      EF_RETURN_IF_ERROR(relation->storage_schema.validate(*row));
      EF_ASSIGN_OR_RETURN(auto actual, physical_identity(*relation, *row));
      if (actual != identity)
        return Status(ErrorCode::InvalidArgument, "row identity changed");

      EF_ASSIGN_OR_RETURN(auto encoded, encode_row(relation->storage_schema, *row));
      next = std::move(encoded);
    }

    // A later phase can undo an earlier staged value. Keep its original/final images in the
    // internal touched set even when the net delta is zero: descendants still need resetting.
    // Only the public flattened change list omits exact net-zero images.
    EF_ASSIGN_OR_RETURN(auto current_bytes, read(key, StateImage::Staged));
    if (current_bytes == next)
      return {};

    EF_RETURN_IF_ERROR(stage({.key = key, .value = std::move(next)}));
    changes[id].insert_or_assign(key, RowChange{.relation = id,
                                                .identity = identity,
                                                .old_row = std::move(prior),
                                                .new_row = row});
    return {};
  }

  StatusOr<std::vector<Row>> scan_rows(RelationId id, const std::optional<Row>& scope,
                                       std::size_t row_budget, std::size_t byte_budget,
                                       StateImage image = StateImage::Staged) noexcept {
    if (image != StateImage::Before && image != StateImage::Staged)
      return Status(ErrorCode::InvalidArgument, "invalid scan state image");
    if (row_budget == 0 || byte_budget == 0)
      return Status(ErrorCode::InvalidArgument, "scan requires positive budgets");

    EF_ASSIGN_OR_RETURN(auto relation, catalog->find(id));
    if (relation->latest && scope) {
      // Scoped Latest scans are singleton reads, including the meaningful absent-row
      // case. Validate the selector rather than treating a missing T key as empty data.
      EF_ASSIGN_OR_RETURN(auto row, read_row(id, *scope, image));
      if (!row)
        return std::vector<Row>{};

      EF_ASSIGN_OR_RETURN(auto key, row_key(config.shard, 'T', id, *scope));
      EF_ASSIGN_OR_RETURN(auto bytes, encode_row(relation->storage_schema, *row));
      if (key.size() > byte_budget || bytes.size() > byte_budget - key.size())
        return Status(ErrorCode::ResourceExhausted, "scoped Latest scan exceeds byte budget");

      return std::vector<Row>{std::move(*row)};
    }

    EF_ASSIGN_OR_RETURN(auto prefix, table_prefix(config.shard, 'T', id));
    if (scope) {
      EF_ASSIGN_OR_RETURN(auto suffix, encode_key_parts(*scope));
      prefix += suffix;
    }

    if (prefix.size() > config.max_bytes)
      return Status(ErrorCode::ResourceExhausted, "scan prefix exceeds effective read budget");

    std::map<std::string, std::string> rows;
    std::optional<std::string> cursor;
    std::size_t bytes = 0;
    while (true) {
      EF_ASSIGN_OR_RETURN(auto page, store->scan(config.shard, prefix, cursor, config.page_rows));
      if (page.revision != revision)
        return Status(ErrorCode::Conflict, "scan revision changed");
      if (page.rows.size() > config.page_rows)
        return Status(ErrorCode::FailedPrecondition, "scan returned too many rows");

      auto previous = cursor;
      for (const auto& [key, value] : page.rows) {
        if (!key.starts_with(prefix) || (previous && key <= *previous))
          return Status(ErrorCode::FailedPrecondition, "scan page violates prefix or ordering");

        previous = key;
        if (key.size() + value.size() > byte_budget - bytes || rows.size() == row_budget)
          return Status(ErrorCode::ResourceExhausted, "capture budget exceeded");

        const auto read_size = key.size() + value.size();
        if (read_size > config.max_bytes - read_bytes)
          return Status(ErrorCode::ResourceExhausted, "total round scan read budget exceeded");

        read_bytes += read_size;
        const auto seen = before.find(key);
        if (seen != before.end() && seen->second != std::optional<std::string>{value})
          return Status(ErrorCode::Conflict, "scan disagrees with the original round image");

        before.emplace(key, value);
        bytes += key.size() + value.size();
        rows.emplace(key, value);
      }

      if (page.rows.size() < config.page_rows)
        break;

      cursor = page.rows.back().first;
    }

    // Initialization may consume upstream results created earlier in this same batch.
    for (const auto& [key, value] : overlay)
      if (image == StateImage::Staged && key.starts_with(prefix)) {
        auto old = rows.find(key);
        if (old != rows.end()) {
          bytes -= old->first.size() + old->second.size();
          rows.erase(old);
        }

        if (value) {
          if (key.size() + value->size() > byte_budget - bytes || rows.size() == row_budget)
            return Status(ErrorCode::ResourceExhausted, "staged capture budget exceeded");

          bytes += key.size() + value->size();
          rows.emplace(key, *value);
        }
      }
    std::vector<Row> result;

    for (const auto& [key, value] : rows) {
      EF_ASSIGN_OR_RETURN(auto row, decode_row(relation->storage_schema, value));
      EF_ASSIGN_OR_RETURN(auto identity, physical_identity(*relation, row));
      EF_ASSIGN_OR_RETURN(auto expected, row_key(config.shard, 'T', id, identity));
      if (expected != key)
        return Status(ErrorCode::FailedPrecondition, "scan identity mismatch");
      if (relation->latest && !deriving_latest)
        EF_RETURN_IF_ERROR(validate_latest(*relation, identity, std::optional<Row>{row}, image));

      result.push_back(std::move(row));
    }

    return result;
  }
};

RoundContext::RoundContext(std::unique_ptr<RoundState> state) noexcept : state_(std::move(state)) {}

RoundContext::~RoundContext() noexcept = default;

std::int64_t RoundContext::processing_time_us() const noexcept {
  return state_->time;
}

std::uint32_t RoundContext::shard() const noexcept {
  return state_->config.shard;
}

std::uint64_t RoundContext::revision() const noexcept {
  return state_->revision;
}

StatusOr<std::vector<Row>> RoundContext::scan_relation(RelationId id, const Row& scope,
                                                       StateImage image, std::size_t rows,
                                                       std::size_t bytes) noexcept {
  EF_ASSIGN_OR_RETURN(auto relation, state_->catalog->find(id));
  EF_RETURN_IF_ERROR(validate_scope(scope, relation->partition_types));
  EF_ASSIGN_OR_RETURN(auto shard, shard_for(scope, state_->config.shard_count));
  if (shard != state_->config.shard)
    return Status(ErrorCode::InvalidArgument, "scan scope belongs to another shard");

  return state_->scan_rows(id, scope, std::min(rows, state_->config.max_rows),
                           std::min(bytes, state_->config.max_bytes), image);
}

StatusOr<std::optional<std::string>> RoundContext::read(const std::string& key,
                                                        StateImage image) noexcept {
  return state_->read(key, image);
}

Status RoundContext::stage(Mutation mutation) noexcept {
  const auto& key = mutation.key;
  if (!key_in_shard(state_->config.shard, key) || key.size() < 10 ||
      (key[5] != 'S' && key[5] != 'I'))
    return Status(ErrorCode::InvalidArgument, "raw stage is limited to extension state/index keys");

  std::uint32_t object = 0;
  for (std::size_t i = 6; i < 10; ++i)
    object = (object << 8) | static_cast<unsigned char>(key[i]);

  if (object == 0)
    return Status(ErrorCode::InvalidArgument, "engine state is reserved");
  if (object == state_->observer_namespace)
    return Status(ErrorCode::InvalidArgument, "observer state is reserved");
  if (!state_->request_state_write && state_->reserved_namespaces &&
      state_->reserved_namespaces->contains(object))
    return Status(ErrorCode::InvalidArgument, "request runtime state is reserved");

  for (const auto& relation : state_->catalog->relations)
    if (relation->state_id == object && ((key[5] == 'S' && relation->plan) || relation->latest))
      return Status(ErrorCode::InvalidArgument, "relation internal state is reserved");

  return state_->stage(std::move(mutation));
}

StatusOr<std::optional<Row>> RoundContext::read_relation(RelationId relation, const Row& identity,
                                                         StateImage image) noexcept {
  return state_->read_row(relation, identity, image);
}

Status RoundContext::emit(RowChange change) noexcept {
  if (state_->runtime_frame || state_->observer_frame)
    return Status(ErrorCode::InvalidArgument,
                  "private runtime frames return owned writes through their callbacks");
  if (state_->input_rows == state_->config.max_rows)
    return Status(ErrorCode::ResourceExhausted, "round input and emit row budget exceeded");

  EF_ASSIGN_OR_RETURN(auto relation, state_->catalog->find(change.relation));
  if (relation->kind != RelationKind::BaseTable ||
      relation->base_write_role != BaseWriteRole::PublicInput)
    return Status(ErrorCode::InvalidArgument, "hook cannot write a runtime-owned relation");
  if (change.new_row) {
    EF_ASSIGN_OR_RETURN(auto scope, partition_values(*relation, *change.new_row));
    EF_ASSIGN_OR_RETURN(auto shard, shard_for(scope, state_->config.shard_count));
    if (shard != state_->config.shard)
      return Status(ErrorCode::InvalidArgument, "hook emit crosses shard");
  }

  EF_RETURN_IF_ERROR(state_->put_row(change.relation, change.identity, change.new_row));
  ++state_->input_rows;
  state_->emitted.insert(change.relation);
  return {};
}

struct EngineState {
  struct QueryEntry {
    std::shared_ptr<const BoundPlan> plan;
    std::size_t charge = 0;
    std::list<std::string>::iterator lru_position;
  };

  std::shared_ptr<IStateStore> store;
  CatalogSnapshot catalog;
  std::unique_ptr<Catalog> query_catalog;
  std::map<std::string, QueryEntry, std::less<>> cached_queries;
  std::list<std::string> query_lru;
  QueryCacheMetrics query_counters;
  EngineConfig config;
  std::uint64_t revision = 0;
  std::int64_t time = 0;
  bool failed = false;
  bool active = false;
  std::optional<CommitBatch> pending;
  std::vector<RowChange> pending_changes;
  std::optional<CatalogSnapshot> pending_catalog;
  std::int64_t pending_time = 0;
  bool pending_for_request = false;
  std::shared_ptr<FunctionRegistry> function_registry;
  bool request_owner_attached = false;
  bool has_latest = false;
  std::set<std::uint32_t> reserved_namespaces;
  RuntimeObserver observer;
  std::uint32_t observer_namespace = 0;
  std::vector<std::uint32_t> observer_order;

  void activate_catalog(CatalogSnapshot definitions) noexcept {
    catalog = std::move(definitions);
    cached_queries.clear();
    query_lru.clear();
    query_counters.bytes = 0;
    query_catalog.reset();
  }

  StatusOr<std::shared_ptr<const BoundPlan>> prepare_query(std::string_view sql) noexcept {
    const bool enabled = config.max_cached_queries != 0 && config.max_query_cache_bytes != 0;
    if (enabled) {
      const auto found = cached_queries.find(sql);
      if (found != cached_queries.end()) {
        ++query_counters.hits;
        query_lru.splice(query_lru.end(), query_lru, found->second.lru_position);
        return std::shared_ptr<const BoundPlan>(found->second.plan);
      }
    }

    ++query_counters.misses;
    if (!query_catalog) {
      EF_ASSIGN_OR_RETURN(query_catalog, Catalog::from_snapshot(catalog));
    }

    // This Catalog owns validated immutable definitions. Its binder returns a
    // fresh internal plan; external BoundPlan objects never enter this cache.
    EF_ASSIGN_OR_RETURN(auto bound, query_catalog->bind_query_sql(sql));
    auto plan = std::make_shared<const BoundPlan>(std::move(bound));
    if (!enabled)
      return plan;

    // Charge the two retained SQL keys, canonical plan representation, exact
    // provenance, and fixed entry metadata. This is a logical budget, not RSS.
    EF_ASSIGN_OR_RETURN(auto encoded, bound_plan_json(*plan));
    const auto canonical = encoded.dump();
    constexpr auto fixed_charge =
        sizeof(QueryEntry) + sizeof(BoundPlan) + 2 * sizeof(std::string) + 8 * sizeof(void*);
    std::size_t charge = 0;
    const auto add_charge = [this, &charge](std::size_t bytes) noexcept {
      if (bytes > config.max_query_cache_bytes - charge)
        return false;

      charge += bytes;
      return true;
    };
    if (!add_charge(fixed_charge) || !add_charge(sql.size()) || !add_charge(sql.size()) ||
        !add_charge(canonical.size()) || !add_charge(plan->catalog_fingerprint.size()))
      return plan;

    while (!cached_queries.empty() &&
           (cached_queries.size() >= config.max_cached_queries ||
            charge > config.max_query_cache_bytes - query_counters.bytes)) {
      const auto oldest = cached_queries.find(query_lru.front());
      query_counters.bytes -= oldest->second.charge;
      cached_queries.erase(oldest);
      query_lru.pop_front();
      ++query_counters.evictions;
    }

    query_lru.emplace_back(sql);
    cached_queries.emplace(
        std::string(sql),
        QueryEntry{.plan = plan, .charge = charge, .lru_position = std::prev(query_lru.end())});
    query_counters.bytes += charge;

    return plan;
  }

  Status ready() const noexcept {
    if (active)
      return Status(ErrorCode::FailedPrecondition, "engine operation is not reentrant");
    if (failed)
      return Status(ErrorCode::FailedPrecondition, "shard is failed");
    if (pending)
      return Status(ErrorCode::FailedPrecondition, "shard has an unconfirmed commit");
    if (!catalog.observations.empty() && !observer)
      return Status(ErrorCode::FailedPrecondition, "Catalog observer executor is not installed");

    return {};
  }

  std::unique_ptr<RoundState> round(const CatalogSnapshot& definitions, std::int64_t at) noexcept {
    auto result = std::make_unique<RoundState>();
    result->store = store;
    result->catalog = &definitions;
    result->config = config;
    result->revision = revision;
    result->time = at;
    result->reserved_namespaces = &reserved_namespaces;
    result->observer_namespace = observer_namespace;

    return result;
  }

  StatusOr<Mutation> metadata(const CatalogSnapshot& definitions,
                              std::int64_t processing_time_us) const noexcept {
    EF_ASSIGN_OR_RETURN(auto key, metadata_key(config.shard));
    Json metadata{{"format", 1},
                  {"catalog", definitions.fingerprint},
                  {"processing_time_us", processing_time_us},
                  {"shard_count", config.shard_count}};
    if (has_latest)
      metadata["latest_initialized"] = 1;

    return Mutation{.key = std::move(key), .value = metadata.dump()};
  }

  Status stage_meta(RoundState& round, const CatalogSnapshot& definitions) noexcept {
    EF_ASSIGN_OR_RETURN(auto mutation, metadata(definitions, round.time));
    return round.stage(std::move(mutation));
  }

  Status observe(RoundContext& context, const RuntimeCause& cause) noexcept {
    if (!observer)
      return {};

    auto& round = *context.state_;
    if (cause.inputs.size() > config.max_rows)
      return Status(ErrorCode::ResourceExhausted, "too many observer causes in one round");

    for (const auto id : observer_order) {
      const auto found = std::find_if(catalog.observations.begin(), catalog.observations.end(),
                                      [&](const auto& rule) noexcept { return rule.id == id; });
      if (found == catalog.observations.end())
        return Status(ErrorCode::Internal, "installed observer order differs from Catalog");

      const auto& definition = found->definition;
      std::vector<DataType> scope_types;
      for (const auto& field : definition.scope_fields)
        scope_types.push_back(field.type);

      std::set<std::string> scopes;
      const auto add_scope = [&](const Row& scope) noexcept -> Status {
        EF_RETURN_IF_ERROR(validate_scope(scope, scope_types));
        EF_ASSIGN_OR_RETURN(auto shard, shard_for(scope, config.shard_count));
        if (shard != config.shard)
          return Status(ErrorCode::InvalidArgument, "observer cause crosses its owner shard");

        EF_ASSIGN_OR_RETURN(auto encoded, encode_key_parts(scope));
        scopes.insert(std::move(encoded));
        return {};
      };
      const auto changes = flatten(round.changes);

      for (const auto& binding : definition.inputs) {
        if (binding.kind == ObservationInputKind::RelationChange) {
          for (const auto& change : changes) {
            if (binding.relation != change.relation)
              continue;

            const auto mask = !change.old_row ? 1U : !change.new_row ? 4U : 2U;
            if ((binding.change_mask & mask) == 0)
              continue;

            Row scope(change.identity.begin(), change.identity.begin() + scope_types.size());
            EF_RETURN_IF_ERROR(add_scope(scope));
          }

          continue;
        }

        for (const auto& input : cause.inputs) {
          if (input.kind != binding.kind || input.advance_rule)
            continue;

          const auto& origin = binding.kind == ObservationInputKind::Source  ? binding.source_kind
                               : binding.kind == ObservationInputKind::Clock ? binding.policy_id
                                                                             : binding.plan;
          if (binding.kind == ObservationInputKind::ExpiryNotice) {
            if (input.relation != binding.relation)
              continue;
          } else if (input.origin != origin)
            continue;
          if (binding.kind == ObservationInputKind::AttemptSettled &&
              (!input.relation ||
               std::find(binding.completion_targets.begin(), binding.completion_targets.end(),
                         *input.relation) == binding.completion_targets.end()))
            continue;

          EF_RETURN_IF_ERROR(add_scope(input.scope));
        }
      }

      for (const auto& input : cause.inputs)
        if (input.kind == ObservationInputKind::Clock && input.advance_rule == id)
          EF_RETURN_IF_ERROR(add_scope(input.scope));

      if (scopes.empty())
        continue;

      // The graph includes reads and sink edges, so all earlier producers of this
      // rule's inputs have stabilized. The callback advances the FSM/CEP now.
      auto output = [&]() noexcept -> StatusOr<std::vector<TableChange>> {
        OperationGuard permission(round.observer_frame);
        return observer(context, *found, cause, changes);
      }();
      if (!output.ok())
        return output.status();
      if (output->size() > config.max_rows - round.input_rows)
        return Status(ErrorCode::ResourceExhausted, "observer sink row budget exceeded");

      std::set<RelationId> roots;
      for (const auto& write : *output) {
        if (definition.sink_relation != write.relation || !write.row)
          return Status(ErrorCode::InvalidArgument, "observer may only upsert its declared sink");

        EF_ASSIGN_OR_RETURN(auto relation, catalog.find(write.relation));
        if (relation->kind != RelationKind::BaseTable ||
            relation->base_write_role != BaseWriteRole::PublicInput)
          return Status(ErrorCode::InvalidArgument, "observer sink must be public input");

        EF_ASSIGN_OR_RETURN(auto scope, partition_values(*relation, *write.row));
        EF_ASSIGN_OR_RETURN(auto encoded_scope, encode_key_parts(scope));
        if (!scopes.contains(encoded_scope))
          return Status(ErrorCode::InvalidArgument, "observer sink changes an unrelated scope");

        EF_ASSIGN_OR_RETURN(auto encoded_row, encode_row(relation->storage_schema, *write.row));
        if (encoded_row.size() > definition.max_sink_row_bytes)
          return Status(ErrorCode::ResourceExhausted, "observer sink exceeds its declared bytes");

        EF_RETURN_IF_ERROR(round.put_row(write.relation, write.identity, write.row));
        ++round.input_rows;
        roots.insert(write.relation);
      }

      EF_RETURN_IF_ERROR(stabilize(round, roots));
    }

    return {};
  }

  StatusOr<CommitResult> publish(RoundState& round,
                                 std::optional<CatalogSnapshot> candidate = {}) noexcept {
    if (round.revision != revision) {
      failed = true;
      return Status(ErrorCode::Conflict, "round revision changed before publication");
    }

    EF_ASSIGN_OR_RETURN(auto id, make_commit_id(config.owner, config.shard, round.revision));
    CommitBatch batch{.shard = config.shard,
                      .owner = config.owner,
                      .expected_revision = round.revision,
                      .commit_id = std::move(id),
                      .mutations = {}};
    for (auto& [key, value] : round.overlay)
      batch.mutations.push_back({.key = key, .value = value});

    // Keep the prepared batch and outputs if confirmation is lost. Recomputing could produce a
    // different write even though this batch may already be durable.
    auto outcome = store->commit(batch);
    if (!outcome.ok()) {
      if (outcome.status().code() == ErrorCode::Unavailable ||
          outcome.status().code() == ErrorCode::UnknownCommit) {
        pending = std::move(batch);
        pending_changes = flatten(round.changes);
        pending_catalog = std::move(candidate);
        pending_time = round.time;
        pending_for_request = round.runtime_frame && request_owner_attached;
      } else
        failed = true;
      return outcome.status();
    }

    if (outcome->code == CommitCode::Committed) {
      if (outcome->revision != revision + 1) {
        pending = std::move(batch);
        pending_changes = flatten(round.changes);
        pending_catalog = std::move(candidate);
        pending_time = round.time;
        pending_for_request = round.runtime_frame && request_owner_attached;

        return CommitResult{.code = CommitCode::Unknown,
                            .revision = revision,
                            .duplicate = false,
                            .message = "invalid store commit revision"};
      }

      revision = outcome->revision;
      time = round.time;
      if (candidate)
        activate_catalog(std::move(*candidate));
    } else if (outcome->code == CommitCode::Unknown) {
      pending = std::move(batch);
      pending_changes = flatten(round.changes);
      pending_catalog = std::move(candidate);
      pending_time = round.time;
      pending_for_request = round.runtime_frame && request_owner_attached;
    } else
      failed = true;
    return std::move(outcome).value();
  }

  StatusOr<std::optional<Row>> input_row(RoundState& round, const BoundPlan& plan, const Row& scope,
                                         StateImage image) noexcept {
    EF_ASSIGN_OR_RETURN(auto left, round.read_row(plan.left, scope, image));
    if (!left)
      return std::optional<Row>{};

    EF_ASSIGN_OR_RETURN(auto left_desc, round.catalog->find(plan.left));
    EF_ASSIGN_OR_RETURN(auto visible, visible_row(*left_desc, *left));
    if (plan.right) {
      EF_ASSIGN_OR_RETURN(auto right, round.read_row(*plan.right, scope, image));
      if (!right)
        return std::optional<Row>{};

      EF_ASSIGN_OR_RETURN(auto right_desc, round.catalog->find(*plan.right));
      EF_ASSIGN_OR_RETURN(auto right_visible, visible_row(*right_desc, *right));
      visible.insert(visible.end(), right_visible.begin(), right_visible.end());
    }

    return std::optional<Row>(std::move(visible));
  }

  StatusOr<std::optional<Row>> filtered(const BoundPlan& plan,
                                        const std::optional<Row>& row) noexcept {
    if (!row)
      return std::optional<Row>{};

    EF_ASSIGN_OR_RETURN(auto pass, passes_filter(plan.filter, *row));
    return pass ? std::optional<Row>(*row) : std::optional<Row>{};
  }

  Status view(RoundState& round, const RelationDescriptor& output, bool initialize) noexcept {
    const auto& plan = *output.plan;
    EF_ASSIGN_OR_RETURN(auto left, round.catalog->find(plan.left));
    std::vector<RowChange> inputs;
    if (initialize) {
      EF_ASSIGN_OR_RETURN(
          auto rows, round.scan_rows(plan.left, std::nullopt, config.max_rows, config.max_bytes));
      for (auto& row : rows) {
        EF_ASSIGN_OR_RETURN(auto identity, physical_identity(*left, row));
        inputs.push_back({.relation = plan.left,
                          .identity = std::move(identity),
                          .old_row = {},
                          .new_row = std::move(row)});
      }
    } else {
      auto found = round.changes.find(plan.left);
      if (found != round.changes.end())
        for (const auto& [key, change] : found->second) {
          (void)key;
          inputs.push_back(change);
        }
    }

    if (plan.right) {
      std::map<std::string, Row> scopes;
      for (const auto& change : inputs) {
        Row scope(change.identity.begin(), change.identity.begin() + left->partition_types.size());
        EF_ASSIGN_OR_RETURN(auto key, encode_key_parts(scope));
        scopes.emplace(std::move(key), std::move(scope));
      }

      if (!initialize) {
        EF_ASSIGN_OR_RETURN(auto right, round.catalog->find(*plan.right));
        auto found = round.changes.find(*plan.right);
        if (found != round.changes.end())
          for (const auto& [key, change] : found->second) {
            (void)key;
            Row scope(change.identity.begin(),
                      change.identity.begin() + right->partition_types.size());
            EF_ASSIGN_OR_RETURN(auto encoded, encode_key_parts(scope));
            scopes.emplace(std::move(encoded), std::move(scope));
          }
      }

      inputs.clear();
      for (const auto& [key, scope] : scopes) {
        (void)key;
        std::optional<Row> before;
        if (!initialize) {
          EF_ASSIGN_OR_RETURN(before, input_row(round, plan, scope, StateImage::Before));
        }

        EF_ASSIGN_OR_RETURN(auto after, input_row(round, plan, scope, StateImage::Staged));
        inputs.push_back({.relation = plan.left,
                          .identity = scope,
                          .old_row = std::move(before),
                          .new_row = std::move(after)});
      }
    } else {
      for (auto& change : inputs) {
        if (change.old_row) {
          EF_ASSIGN_OR_RETURN(change.old_row, visible_row(*left, *change.old_row));
        }

        if (change.new_row) {
          EF_ASSIGN_OR_RETURN(change.new_row, visible_row(*left, *change.new_row));
        }
      }
    }

    if (plan.group_by.empty()) {
      for (const auto& change : inputs) {
        EF_ASSIGN_OR_RETURN(auto after, filtered(plan, change.new_row));
        std::optional<Row> result;
        if (after) {
          EF_ASSIGN_OR_RETURN(auto visible, evaluate_projection(plan, *after));
          EF_ASSIGN_OR_RETURN(result, make_stored_row(output, visible, change.identity));
        }

        EF_RETURN_IF_ERROR(round.put_row(output.id, change.identity, result));
      }

      return {};
    }

    struct GroupDelta {
      Row key;
      std::vector<Row> removed;
      std::vector<Row> added;
    };

    std::map<std::string, GroupDelta> groups;
    for (const auto& change : inputs) {
      // GROUP BY is the canonical business partition. Even if filtering makes both original
      // and final images absent, reset any aggregate state created by an earlier phase.
      Row scope(change.identity.begin(), change.identity.begin() + plan.partition_types.size());
      EF_ASSIGN_OR_RETURN(auto scope_key, encode_key_parts(scope));
      groups[scope_key].key = std::move(scope);
      EF_ASSIGN_OR_RETURN(auto before, filtered(plan, change.old_row));
      EF_ASSIGN_OR_RETURN(auto after, filtered(plan, change.new_row));

      for (int side = 0; side < 2; ++side) {
        const auto& row = side == 0 ? before : after;
        if (!row)
          continue;

        Row group;
        for (const auto& expression : plan.group_by) {
          EF_ASSIGN_OR_RETURN(auto value, evaluate(*expression, *row));
          group.push_back(std::move(value));
        }

        EF_ASSIGN_OR_RETURN(auto key, encode_key_parts(group));
        auto& delta = groups[key];
        delta.key = std::move(group);
        (side == 0 ? delta.removed : delta.added).push_back(*row);
      }
    }

    for (const auto& [key, delta] : groups) {
      (void)key;
      EF_ASSIGN_OR_RETURN(auto state_key, row_key(config.shard, 'S', output.state_id, delta.key));
      std::optional<std::string> bytes;
      if (!initialize) {
        EF_ASSIGN_OR_RETURN(bytes, round.read(state_key, StateImage::Before));
      }

      EF_ASSIGN_OR_RETURN(auto state, decode_group(plan, bytes));

      // Merge all retractions and insertions before checking the final visible sum.
      for (const auto& row : delta.removed)
        EF_RETURN_IF_ERROR(contribute(state, plan, row, false));

      for (const auto& row : delta.added)
        EF_RETURN_IF_ERROR(contribute(state, plan, row, true));

      std::optional<Row> result;
      if (state.rows > 0) {
        EF_ASSIGN_OR_RETURN(auto values, aggregate_row(state, plan, delta.key));
        EF_ASSIGN_OR_RETURN(auto visible, evaluate_projection(plan, values));
        EF_ASSIGN_OR_RETURN(result, make_stored_row(output, visible, delta.key));
        EF_ASSIGN_OR_RETURN(auto encoded, encode_group(state));
        EF_RETURN_IF_ERROR(round.stage({.key = state_key, .value = std::move(encoded)}));
      } else
        EF_RETURN_IF_ERROR(round.stage({.key = state_key, .value = std::nullopt}));
      EF_RETURN_IF_ERROR(round.put_row(output.id, delta.key, result));
    }

    return {};
  }

  Status latest_view(RoundState& round, const RelationDescriptor& output) noexcept {
    // A candidate/status update precedes its derived Latest row within this frame.
    // Read validation resumes after the selector has atomically rebuilt both images.
    OperationGuard deriving(round.deriving_latest);
    const auto& definition = output.latest->definition;
    EF_ASSIGN_OR_RETURN(auto source, round.catalog->find(definition.result_relation));
    EF_ASSIGN_OR_RETURN(auto metadata, round.catalog->find(definition.request_status_relation));
    std::map<std::string, Row> scopes;
    std::map<std::string, std::map<std::string, Row>> identities;

    for (auto input : {definition.candidate_relation, definition.result_relation,
                       definition.request_status_relation}) {
      auto changes = round.changes.find(input);
      if (changes == round.changes.end())
        continue;

      for (const auto& [key, change] : changes->second) {
        (void)key;
        Row scope(change.identity.begin(), change.identity.begin() + output.partition_types.size());
        EF_ASSIGN_OR_RETURN(auto encoded, encode_key_parts(scope));
        scopes.emplace(encoded, scope);
        if (input != definition.candidate_relation) {
          EF_ASSIGN_OR_RETURN(auto identity, encode_key_parts(change.identity));
          identities[encoded].emplace(std::move(identity), change.identity);
        }
      }
    }

    for (const auto& [key, scope] : scopes) {
      EF_ASSIGN_OR_RETURN(auto candidate,
                          round.read_row(definition.candidate_relation, scope, StateImage::Staged));
      EF_ASSIGN_OR_RETURN(auto state_key, row_key(config.shard, 'S', output.state_id, scope));
      EF_ASSIGN_OR_RETURN(auto bytes, round.read(state_key, StateImage::Staged));
      EF_ASSIGN_OR_RETURN(auto current, round.read_row(output.id, scope, StateImage::Staged));
      LatestState selected;
      if (bytes) {
        EF_ASSIGN_OR_RETURN(selected, decode_latest(*bytes, *source, scope));
        if ((selected.sequence > 0) != current.has_value())
          return Status(ErrorCode::FailedPrecondition, "Latest row/state presence mismatch");
        if (current &&
            (std::get<std::int64_t>((*current)[output.visible_count - 2]) != selected.epoch ||
             std::get<std::int64_t>((*current)[output.visible_count - 1]) != selected.sequence))
          return Status(ErrorCode::FailedPrecondition, "Latest row/state provenance mismatch");
      } else if (current)
        return Status(ErrorCode::FailedPrecondition, "Latest row has no selector state");
      if (!candidate) {
        if (bytes)
          return Status(ErrorCode::FailedPrecondition, "candidate tombstone is missing");

        continue;
      }

      const auto epoch = std::get<std::int64_t>((*candidate)[scope.size()]);
      const auto active = std::get<bool>((*candidate)[scope.size() + 1]);
      if (epoch < selected.epoch)
        return Status(ErrorCode::FailedPrecondition, "candidate epoch regressed");
      if (epoch != selected.epoch || !active) {
        selected = {.epoch = epoch, .sequence = 0, .winner = {}};
        current.reset();
      }

      if (active) {
        for (const auto& [encoded, identity] : identities[key]) {
          (void)encoded;
          EF_ASSIGN_OR_RETURN(auto value, round.read_row(source->id, identity, StateImage::Staged));
          EF_ASSIGN_OR_RETURN(auto status,
                              round.read_row(metadata->id, identity, StateImage::Staged));
          if (!value || !status)
            continue;

          const auto base = identity.size();
          if (std::get<std::int64_t>((*status)[base]) != epoch ||
              std::get<std::string>((*status)[base + 2]) != "SUCCEEDED")
            continue;

          // Latest follows admission order within this candidate epoch, not completion order.
          // An older request finishing late must not replace a newer successful result.
          const auto sequence = std::get<std::int64_t>((*status)[base + 1]);
          if (sequence < selected.sequence)
            continue;

          EF_ASSIGN_OR_RETURN(auto visible, visible_row(*source, *value));
          visible.emplace_back(epoch);
          visible.emplace_back(sequence);
          EF_ASSIGN_OR_RETURN(auto row, make_stored_row(output, visible, scope));
          if (sequence == selected.sequence &&
              (selected.winner != identity || !same_images(current, std::optional<Row>{row})))
            return Status(ErrorCode::Conflict, "Latest sequence is bound to conflicting output");

          selected = {.epoch = epoch, .sequence = sequence, .winner = identity};
          current = std::move(row);
        }
      }

      EF_ASSIGN_OR_RETURN(auto encoded, encode_latest(selected));
      EF_RETURN_IF_ERROR(round.stage({.key = state_key, .value = std::move(encoded)}));
      EF_RETURN_IF_ERROR(round.put_row(output.id, scope, current));
    }

    return {};
  }

  Status stabilize(RoundState& round, const std::set<RelationId>& roots,
                   RelationId initialize_from = 0) noexcept {
    std::set<RelationId> affected = roots;
    // Catalog order places dependencies first. Each view sees all of its inputs settled for this
    // round, so a shared downstream view never observes only one side of an upstream change.
    for (const auto& relation : round.catalog->relations) {
      if (!relation->plan && !relation->latest)
        continue;

      EF_ASSIGN_OR_RETURN(auto inputs, dependencies(*relation));
      const bool init = initialize_from && relation->id >= initialize_from;
      const bool changed = std::any_of(inputs.begin(), inputs.end(), [&](RelationId id) noexcept {
        return affected.contains(id);
      });
      if (!init && !changed)
        continue;
      if (relation->latest) {
        if (init)
          return Status(ErrorCode::FailedPrecondition,
                        "Latest requires an empty admission baseline");

        EF_RETURN_IF_ERROR(latest_view(round, *relation));
      } else {
        EF_RETURN_IF_ERROR(view(round, *relation, init));
      }

      if (round.changes.contains(relation->id) && !round.changes[relation->id].empty())
        affected.insert(relation->id);
    }

    return {};
  }
};

Engine::Engine(std::unique_ptr<EngineState> state) noexcept : state_(std::move(state)) {}

Engine::~Engine() noexcept = default;

StatusOr<EngineConfig> resolve_engine_config(IStateStore& store,
                                             const EngineConfig& requested) noexcept {
  EF_RETURN_IF_ERROR(check(requested.shard_count > 0 && requested.shard < requested.shard_count &&
                               requested.max_rows > 0 && requested.max_bytes > 0 &&
                               requested.max_mutations > 0 && requested.page_rows > 0,
                           "invalid engine configuration"));
  CommitBatch largest_identity;
  largest_identity.shard = requested.shard;
  largest_identity.owner = requested.owner;
  largest_identity.expected_revision = UINT64_MAX;
  EF_ASSIGN_OR_RETURN(largest_identity.commit_id,
                      make_commit_id(requested.owner, requested.shard, UINT64_MAX));
  EF_ASSIGN_OR_RETURN(auto header, canonical_batch(largest_identity));
  EF_ASSIGN_OR_RETURN(auto limits, store.limits());
  if (limits.max_batch_bytes == 0 || limits.max_mutations == 0 || limits.max_read_keys == 0 ||
      limits.max_read_bytes == 0 || limits.max_scan_rows == 0)
    return Status(ErrorCode::FailedPrecondition, "store reports invalid zero capacity");

  auto effective = requested;
  effective.max_mutations = std::min(requested.max_mutations, limits.max_mutations);
  effective.page_rows = std::min(requested.page_rows, limits.max_scan_rows);

  // canonical_batch charges two uint64 lengths plus a presence byte for every put.
  // Reserving the full count also covers deletes and avoids coupling admission
  // proofs to the number of mutations an individual future transition will use.
  constexpr std::size_t put_framing = 17;
  if (header.size() >= limits.max_batch_bytes ||
      effective.max_mutations > (limits.max_batch_bytes - header.size() - 1) / put_framing)
    return Status(ErrorCode::ResourceExhausted,
                  "store cannot fit the configured mutation framing; reduce max_mutations");

  const auto available =
      limits.max_batch_bytes - header.size() - put_framing * effective.max_mutations;
  effective.max_bytes = std::min({requested.max_bytes, limits.max_read_bytes, available});
  return effective;
}

const EngineConfig& Engine::config() const noexcept {
  return state_->config;
}

StatusOr<std::unique_ptr<Engine>> Engine::open(std::shared_ptr<IStateStore> store,
                                               CatalogSnapshot catalog,
                                               const EngineConfig& config) noexcept {
  return open_runtime(std::move(store), std::move(catalog), config, {});
}

StatusOr<std::unique_ptr<Engine>>
Engine::open_runtime(std::shared_ptr<IStateStore> store, CatalogSnapshot catalog,
                     const EngineConfig& requested_config,
                     std::shared_ptr<FunctionRegistry> registry) noexcept {
  EF_RETURN_IF_ERROR(check(store != nullptr, "engine requires a state store"));
  EF_ASSIGN_OR_RETURN(auto config, resolve_engine_config(*store, requested_config));
  EF_ASSIGN_OR_RETURN(auto verified, Catalog::from_snapshot(catalog));
  EF_ASSIGN_OR_RETURN(catalog, verified->snapshot());
  const auto same_fields = [](const std::vector<Field>& left,
                              const std::vector<Field>& right) noexcept {
    if (left.size() != right.size())
      return false;

    for (std::size_t i = 0; i < left.size(); ++i)
      if (left[i].id != right[i].id || left[i].name != right[i].name ||
          left[i].type != right[i].type || left[i].nullable != right[i].nullable)
        return false;

    return true;
  };
  for (const auto& relation : catalog.relations) {
    if ((relation->candidate || relation->request_status || relation->latest) && !registry)
      return Status(ErrorCode::FailedPrecondition,
                    "controlled metadata and Latest require the private runtime");
    if (relation->kind != RelationKind::FunctionView)
      continue;
    if (!registry)
      return Status(ErrorCode::FailedPrecondition,
                    "function views require the private request runtime");

    const auto& definition = relation->function->definition;
    EF_ASSIGN_OR_RETURN(auto signature,
                        registry->describe(definition.function_id, definition.function_version));
    if (signature.function_id != definition.function_id ||
        signature.version != definition.function_version ||
        signature.implementation_identity != definition.implementation_identity ||
        signature.inputs.size() != definition.inputs.size() ||
        !same_fields(signature.output_fields, definition.output_fields))
      return Status(ErrorCode::Conflict,
                    "function registry identity/output signature differs from Catalog");

    for (std::size_t i = 0; i < signature.inputs.size(); ++i)
      if (signature.inputs[i].alias != definition.inputs[i].alias ||
          signature.inputs[i].required != definition.inputs[i].required ||
          !same_fields(signature.inputs[i].fields, definition.inputs[i].fields))
        return Status(ErrorCode::Conflict,
                      "function registry input signature differs from Catalog");
  }

  auto state = std::make_unique<EngineState>();
  state->store = std::move(store);
  state->catalog = std::move(catalog);
  state->has_latest =
      std::any_of(state->catalog.relations.begin(), state->catalog.relations.end(),
                  [](const auto& relation) noexcept { return relation->latest != nullptr; });
  state->query_catalog = std::move(verified);
  state->config = config;
  state->function_registry = std::move(registry);

  EF_ASSIGN_OR_RETURN(auto status, state->store->status(config.shard));
  state->revision = status.revision;

  EF_ASSIGN_OR_RETURN(auto key, metadata_key(config.shard));
  EF_ASSIGN_OR_RETURN(auto data, state->store->get_many(config.shard, {key}));
  if (data.revision != state->revision || data.values.size() != 1)
    return Status(ErrorCode::Conflict, "engine open revision mismatch");
  if (key.size() > config.max_bytes ||
      (data.values[0] && data.values[0]->size() > config.max_bytes - key.size()))
    return Status(ErrorCode::ResourceExhausted, "stored catalog exceeds effective read budget");

  if (data.values[0]) {
    auto parsed = parse_json_exact(*data.values[0]);
    if (!parsed.ok() || !parsed->is_object() || !parsed->contains("format") ||
        (*parsed)["format"] != 1 || !parsed->contains("catalog") ||
        (*parsed)["catalog"] != state->catalog.fingerprint || !parsed->contains("shard_count") ||
        (*parsed)["shard_count"] != config.shard_count)
      return Status(ErrorCode::FailedPrecondition, "engine metadata or catalog mismatch");
    if (state->has_latest &&
        (!parsed->contains("latest_initialized") || (*parsed)["latest_initialized"] != 1))
      return Status(ErrorCode::FailedPrecondition, "Latest initialization baseline is missing");

    EF_ASSIGN_OR_RETURN(auto time, count_field(*parsed, "processing_time_us"));
    state->time = static_cast<std::int64_t>(time);
  } else {
    EF_ASSIGN_OR_RETURN(auto prefix, data_prefix(config.shard));
    EF_ASSIGN_OR_RETURN(auto existing, state->store->scan(config.shard, prefix, std::nullopt, 1));
    if (existing.revision != state->revision || !existing.rows.empty())
      return Status(ErrorCode::FailedPrecondition, "unowned data exists without engine metadata");

    auto round = state->round(state->catalog, 0);
    EF_RETURN_IF_ERROR(state->stage_meta(*round, state->catalog));
    EF_ASSIGN_OR_RETURN(auto committed, state->publish(*round));
    if (committed.code != CommitCode::Committed)
      return Status(committed.code == CommitCode::Unknown ? ErrorCode::UnknownCommit
                                                          : ErrorCode::Conflict,
                    "engine initialization not committed");
  }

  return std::unique_ptr<Engine>(new Engine(std::move(state)));
}

StatusOr<DispatchResult> Engine::apply(const TableChange& change, const DispatchContext& context,
                                       const RoundHook& hook) noexcept {
  return apply_many({change}, context, hook);
}

StatusOr<DispatchResult> Engine::apply_many(const std::vector<TableChange>& inputs,
                                            const DispatchContext& context,
                                            const RoundHook& hook) noexcept {
  return apply_input_round(inputs, context, hook, false);
}

StatusOr<DispatchResult> Engine::apply_input_round(const std::vector<TableChange>& inputs,
                                                   const DispatchContext& context,
                                                   const RoundHook& hook, bool recover_capacity,
                                                   const RuntimePrepare& owned_prepare,
                                                   const RuntimeCause* cause) noexcept {
  EF_RETURN_IF_ERROR(state_->ready());
  OperationGuard operation(state_->active);
  if (context.processing_time_us < state_->time)
    return Status(ErrorCode::InvalidArgument, "processing time cannot move backwards");
  if (inputs.size() > state_->config.max_rows)
    return Status(ErrorCode::ResourceExhausted, "too many input rows in round");

  // Admission errors have no side effects and do not fail a healthy shard.
  for (const auto& input : inputs) {
    EF_ASSIGN_OR_RETURN(auto relation, state_->catalog.find(input.relation));
    if (relation->kind != RelationKind::BaseTable ||
        relation->base_write_role != BaseWriteRole::PublicInput)
      return Status(ErrorCode::InvalidArgument, "cannot mutate a runtime-owned relation");
    if (input.identity.size() != relation->identity_types.size())
      return Status(ErrorCode::InvalidArgument, "incomplete table identity");

    EF_RETURN_IF_ERROR(validate_scope(input.identity, relation->identity_types));
    Row scope(input.identity.begin(), input.identity.begin() + relation->partition_types.size());
    EF_RETURN_IF_ERROR(validate_scope(scope, relation->partition_types));
    EF_ASSIGN_OR_RETURN(auto shard, shard_for(scope, state_->config.shard_count));
    if (shard != state_->config.shard)
      return Status(ErrorCode::InvalidArgument, "input belongs to another shard");
    if (input.row) {
      EF_RETURN_IF_ERROR(relation->storage_schema.validate(*input.row));
      EF_ASSIGN_OR_RETURN(auto actual, physical_identity(*relation, *input.row));
      if (actual != input.identity)
        return Status(ErrorCode::InvalidArgument, "upsert identity mismatch");
    }
  }

  RoundContext round(state_->round(state_->catalog, context.processing_time_us));
  round.state_->input_rows = inputs.size();
  std::set<RelationId> roots;
  for (const auto& input : inputs) {
    const auto status = round.state_->put_row(input.relation, input.identity, input.row);
    if (!status.ok()) {
      state_->failed = !recover_capacity || status.code() != ErrorCode::ResourceExhausted;
      return status;
    }

    roots.insert(input.relation);
  }

  auto status = state_->stabilize(*round.state_, roots);
  if (status.ok() && owned_prepare) {
    // Candidate changes are a separate owner capability. They do not turn a source
    // batch into a RequestRuntime batch or grant request-state write permissions.
    auto prepared = [&]() noexcept -> StatusOr<std::vector<TableChange>> {
      OperationGuard permission(round.state_->metadata_frame);
      return owned_prepare(round);
    }();
    if (!prepared.ok())
      status = prepared.status();
    else {
      auto apply_candidates = [&]() noexcept -> Status {
        if (prepared->size() > state_->config.max_rows - round.state_->input_rows)
          return Status(ErrorCode::ResourceExhausted, "lifecycle candidate row budget exceeded");

        std::set<RelationId> candidate_roots;
        for (const auto& write : *prepared) {
          EF_ASSIGN_OR_RETURN(auto relation, state_->catalog.find(write.relation));
          if (!relation->candidate)
            return Status(ErrorCode::InvalidArgument,
                          "lifecycle prepare may only write typed Candidate metadata");

          EF_RETURN_IF_ERROR(round.state_->consume_metadata(write));
          EF_RETURN_IF_ERROR(round.state_->put_row(write.relation, write.identity, write.row));
          ++round.state_->input_rows;
          candidate_roots.insert(write.relation);
          roots.insert(write.relation);
        }

        if (!round.state_->metadata_authorizations.empty())
          return Status(ErrorCode::InvalidArgument, "unapplied lifecycle candidate intents");

        return state_->stabilize(*round.state_, candidate_roots);
      };
      status = apply_candidates();
    }
  }

  if (status.ok() && hook) {
    if (recover_capacity) {
      // Only the private lifecycle entry enables capacity recovery and this hook capability.
      OperationGuard permission(round.state_->lifecycle_frame);
      status = hook(round, flatten(round.state_->changes));
    } else {
      status = hook(round, flatten(round.state_->changes));
    }

    if (status.ok() && !round.state_->emitted.empty()) {
      // Hook emissions must not feed back into any already stabilized relation.
      EF_ASSIGN_OR_RETURN(auto existing, reachable(state_->catalog, roots));
      EF_ASSIGN_OR_RETURN(auto appended, reachable(state_->catalog, round.state_->emitted));
      for (auto id : appended)
        if (existing.contains(id)) {
          status = Status(ErrorCode::InvalidArgument,
                          "hook emission graph intersects the current input graph");
          break;
        }
      if (status.ok())
        status = state_->stabilize(*round.state_, round.state_->emitted);
    }
  }

  if (!status.ok()) {
    state_->failed = !recover_capacity || status.code() != ErrorCode::ResourceExhausted;
    return status;
  }

  status = state_->observe(round, cause ? *cause : RuntimeCause{});
  if (!status.ok()) {
    state_->failed = !recover_capacity || status.code() != ErrorCode::ResourceExhausted;
    return status;
  }

  status = state_->stage_meta(*round.state_, state_->catalog);
  if (!status.ok()) {
    state_->failed = !recover_capacity || status.code() != ErrorCode::ResourceExhausted;
    return status;
  }

  auto changes = flatten(round.state_->changes);
  EF_ASSIGN_OR_RETURN(auto committed, state_->publish(*round.state_));
  if (committed.code != CommitCode::Committed)
    changes.clear();

  return DispatchResult{.commit = std::move(committed), .changes = std::move(changes)};
}

StatusOr<DispatchResult> Engine::resolve_pending_commit() noexcept {
  return resolve_commit(false);
}

StatusOr<DispatchResult> Engine::resolve_commit(bool request_owner) noexcept {
  if (state_->active)
    return Status(ErrorCode::FailedPrecondition, "engine operation is not reentrant");
  if (state_->pending_for_request && state_->request_owner_attached && !request_owner)
    return Status(ErrorCode::FailedPrecondition,
                  "the request owner must confirm its own pending commit");

  OperationGuard operation(state_->active);
  if (state_->failed)
    return Status(ErrorCode::FailedPrecondition, "shard is failed");
  if (!state_->pending)
    return Status(ErrorCode::FailedPrecondition, "no pending commit");

  // Retry the exact batch; do not evaluate the event or its callbacks again.
  EF_ASSIGN_OR_RETURN(auto result, state_->store->commit(*state_->pending));
  std::vector<RowChange> published;
  if (result.code == CommitCode::Committed) {
    if (result.revision != state_->revision + 1)
      return DispatchResult{.commit = CommitResult{.code = CommitCode::Unknown,
                                                   .revision = state_->revision,
                                                   .duplicate = false,
                                                   .message = "invalid retry revision"},
                            .changes = {}};

    state_->revision = result.revision;
    state_->time = state_->pending_time;
    if (state_->pending_catalog)
      state_->activate_catalog(std::move(*state_->pending_catalog));

    published = std::move(state_->pending_changes);
    state_->pending.reset();
    state_->pending_for_request = false;
    state_->pending_catalog.reset();
    state_->pending_changes.clear();
  } else if (result.code == CommitCode::Rejected)
    state_->failed = true;
  return DispatchResult{.commit = std::move(result), .changes = std::move(published)};
}

StatusOr<InstallResult> Engine::install(const StatementAst& definition,
                                        const DefinitionOptions& options) noexcept {
  EF_RETURN_IF_ERROR(state_->ready());
  if (!state_->catalog.observations.empty())
    return Status(ErrorCode::FailedPrecondition,
                  "observer Catalog activation requires a controlled rebind");
  if (state_->request_owner_attached)
    return Status(ErrorCode::FailedPrecondition,
                  "request Catalog activation requires an explicit controlled migration");

  for (const auto& relation : state_->catalog.relations)
    if (!relation->identity_domain.empty() || relation->candidate || relation->request_status ||
        relation->latest)
      return Status(ErrorCode::FailedPrecondition,
                    "fixed request Catalog requires an explicit controlled migration");

  OperationGuard operation(state_->active);
  EF_ASSIGN_OR_RETURN(auto catalog, Catalog::from_snapshot(state_->catalog));
  EF_ASSIGN_OR_RETURN(auto id, catalog->define(definition, options));
  EF_ASSIGN_OR_RETURN(auto candidate, catalog->snapshot());
  RoundContext round(state_->round(candidate, state_->time));
  auto initialized = state_->stabilize(*round.state_, {}, id);
  if (!initialized.ok())
    return initialized; // New IDs were only staged; existing application remains valid.
  EF_RETURN_IF_ERROR(state_->stage_meta(*round.state_, candidate));
  EF_ASSIGN_OR_RETURN(auto result, state_->publish(*round.state_, std::move(candidate)));

  return InstallResult{.relation = id, .commit = std::move(result)};
}

StatusOr<InstallResult> Engine::install_sql(std::string_view sql,
                                            const DefinitionOptions& options) noexcept {
  EF_ASSIGN_OR_RETURN(auto definition, parse_sql(sql));
  return install(definition, options);
}

StatusOr<QueryResult> Engine::query(const BoundPlan& plan, const QueryContext& context) noexcept {
  EF_RETURN_IF_ERROR(state_->ready());
  OperationGuard operation(state_->active);
  EF_RETURN_IF_ERROR(validate_bound_plan(plan, state_->catalog));
  return query_trusted(plan, context);
}

StatusOr<QueryResult> Engine::query_trusted(const BoundPlan& plan,
                                            const QueryContext& context) noexcept {
  if (!state_->active)
    return Status(ErrorCode::Internal, "trusted query requires an active operation guard");

  const auto& selected = context.scope ? context.scope : plan.inferred_scope;
  if (!selected)
    return Status(ErrorCode::InvalidArgument, "query requires partition scope");
  if (context.scope && plan.inferred_scope && *context.scope != *plan.inferred_scope)
    return Status(ErrorCode::InvalidArgument, "explicit scope conflicts with query predicate");

  EF_RETURN_IF_ERROR(validate_scope(*selected, plan.partition_types));
  EF_ASSIGN_OR_RETURN(auto shard, shard_for(*selected, state_->config.shard_count));
  if (shard != state_->config.shard)
    return Status(ErrorCode::InvalidArgument, "query belongs to another shard");

  const auto max_rows = std::min(context.max_rows, state_->config.max_rows);
  const auto max_bytes = std::min(context.max_bytes, state_->config.max_bytes);
  EF_RETURN_IF_ERROR(check(max_rows > 0 && max_bytes > 0, "invalid query budget"));

  auto round = state_->round(state_->catalog, state_->time);
  round->config.max_bytes = max_bytes;
  round->config.max_rows = max_rows;

  EF_ASSIGN_OR_RETURN(auto left, state_->catalog.find(plan.left));
  std::vector<Row> inputs;

  if (plan.right) {
    EF_ASSIGN_OR_RETURN(auto joined,
                        state_->input_row(*round, plan, *selected, StateImage::Before));
    if (joined)
      inputs.push_back(std::move(*joined));
  } else if (left->latest) {
    // A missing row is meaningful for a singleton selector: consult its state as well,
    // instead of letting an empty T scan conceal a persisted winner.
    EF_ASSIGN_OR_RETURN(auto row, round->read_row(plan.left, *selected, StateImage::Before));
    if (row) {
      EF_ASSIGN_OR_RETURN(auto visible, visible_row(*left, *row));
      inputs.push_back(std::move(visible));
    }
  } else {
    EF_ASSIGN_OR_RETURN(auto rows, round->scan_rows(plan.left, *selected, max_rows, max_bytes));
    for (const auto& row : rows) {
      EF_ASSIGN_OR_RETURN(auto visible, visible_row(*left, row));
      inputs.push_back(std::move(visible));
    }
  }

  QueryResult result{.fields = plan.output_fields,
                     .rows = {},
                     .revision = state_->revision,
                     .catalog_version = state_->catalog.version};
  if (!plan.group_by.empty()) {
    GroupState group;
    group.values.resize(plan.aggregates.size());
    for (const auto& row : inputs) {
      EF_ASSIGN_OR_RETURN(auto pass, passes_filter(plan.filter, row));
      if (pass)
        EF_RETURN_IF_ERROR(contribute(group, plan, row, true));
    }

    if (group.rows > 0) {
      EF_ASSIGN_OR_RETURN(auto values, aggregate_row(group, plan, *selected));
      EF_ASSIGN_OR_RETURN(auto output, evaluate_projection(plan, values));
      result.rows.push_back(std::move(output));
    }
  } else {
    for (const auto& row : inputs) {
      EF_ASSIGN_OR_RETURN(auto pass, passes_filter(plan.filter, row));
      if (!pass)
        continue;

      EF_ASSIGN_OR_RETURN(auto output, evaluate_projection(plan, row));
      result.rows.push_back(std::move(output));
    }
  }

  std::size_t output_bytes = 0;
  for (const auto& row : result.rows)
    for (const auto& value : row) {
      EF_ASSIGN_OR_RETURN(auto encoded, value_to_json(value));
      const auto size = encoded.dump().size();
      if (size > max_bytes - output_bytes)
        return Status(ErrorCode::ResourceExhausted, "query output exceeds budget");

      output_bytes += size;
    }
  return result;
}

StatusOr<QueryResult> Engine::query_sql(std::string_view sql,
                                        const QueryContext& context) noexcept {
  EF_RETURN_IF_ERROR(state_->ready());
  OperationGuard operation(state_->active);
  EF_ASSIGN_OR_RETURN(auto plan, state_->prepare_query(sql));

  return query_trusted(*plan, context);
}

StatusOr<CatalogSnapshot> Engine::snapshot() const noexcept {
  return CatalogSnapshot(state_->catalog);
}

std::uint64_t Engine::revision() const noexcept {
  return state_->revision;
}

bool Engine::has_pending_commit() const noexcept {
  return state_->pending.has_value();
}

bool Engine::failed() const noexcept {
  return state_->failed;
}

QueryCacheMetrics Engine::query_cache_metrics() const noexcept {
  auto result = state_->query_counters;
  result.entries = state_->cached_queries.size();
  return result;
}

StatusOr<std::unique_ptr<Engine>>
EngineRuntimeAccess::open(std::shared_ptr<IStateStore> store, CatalogSnapshot catalog,
                          const EngineConfig& config,
                          std::shared_ptr<FunctionRegistry> registry) noexcept {
  return Engine::open_runtime(std::move(store), std::move(catalog), config, std::move(registry));
}

const EngineConfig& EngineRuntimeAccess::config(const Engine& engine) noexcept {
  return engine.state_->config;
}

StatusOr<std::size_t> EngineRuntimeAccess::metadata_bytes(const Engine& engine) noexcept {
  EF_ASSIGN_OR_RETURN(auto mutation, engine.state_->metadata(engine.state_->catalog, INT64_MAX));
  return mutation.key.size() + mutation.value->size();
}

std::int64_t EngineRuntimeAccess::time_us(const Engine& engine) noexcept {
  return engine.state_->time;
}

std::shared_ptr<IStateStore> EngineRuntimeAccess::state_store(const Engine& engine) noexcept {
  return engine.state_->store;
}

StatusOr<DispatchResult>
EngineRuntimeAccess::apply_lifecycle(Engine& engine, const std::vector<TableChange>& inputs,
                                     const DispatchContext& context, const RoundHook& prepare,
                                     const RuntimePrepare& owned_prepare,
                                     const RuntimeCause& cause) noexcept {
  return engine.apply_input_round(inputs, context, prepare, true, owned_prepare, &cause);
}

Status EngineRuntimeAccess::write_trigger_audit(RoundContext& context,
                                                const TableChange& write) noexcept {
  auto& round = *context.state_;
  if (!round.lifecycle_frame || round.runtime_frame || round.metadata_frame || round.observer_frame)
    return Status(ErrorCode::FailedPrecondition, "trigger audit requires a lifecycle frame");

  EF_ASSIGN_OR_RETURN(auto relation, round.catalog->find(write.relation));
  if (relation->kind != RelationKind::BaseTable ||
      relation->base_write_role != BaseWriteRole::TriggerAudit || relation->candidate ||
      relation->request_status)
    return Status(ErrorCode::InvalidArgument, "trigger audit requires its dedicated base relation");
  if (round.input_rows >= round.config.max_rows)
    return Status(ErrorCode::ResourceExhausted, "round input and audit row budget exceeded");

  // put_row validates identities and owner scopes for inserts and deletes and charges
  // the same read, staged-byte and mutation budgets as ordinary table changes.
  EF_RETURN_IF_ERROR(round.put_row(write.relation, write.identity, write.row));
  ++round.input_rows;
  round.emitted.insert(write.relation);

  return {};
}

Status EngineRuntimeAccess::install_observer(Engine& engine, std::uint32_t namespace_id,
                                             RuntimeObserver observer) noexcept {
  auto& state = *engine.state_;
  if (state.active || state.failed || state.pending || state.observer)
    return Status(ErrorCode::FailedPrecondition, "observer installation needs an idle owner");
  if (!observer || namespace_id == 0 || state.catalog.observations.empty() ||
      state.reserved_namespaces.contains(namespace_id))
    return Status(ErrorCode::InvalidArgument, "invalid observer callback or state namespace");

  for (const auto& relation : state.catalog.relations)
    if (relation->state_id == namespace_id)
      return Status(ErrorCode::InvalidArgument, "observer namespace collides with Catalog state");

  EF_ASSIGN_OR_RETURN(auto graph, observation_graph(state.catalog));
  state.observer_order = std::move(graph.rule_order);
  state.observer_namespace = namespace_id;
  state.observer = std::move(observer);

  return {};
}

Status EngineRuntimeAccess::stage_observer_state(RoundContext& round, Mutation mutation) noexcept {
  const auto& key = mutation.key;
  if (!round.state_->observer_frame || round.state_->observer_namespace == 0 ||
      !key_in_shard(round.state_->config.shard, key) || key.size() < 10 ||
      (key[5] != 'S' && key[5] != 'I'))
    return Status(ErrorCode::InvalidArgument, "observer state requires its private frame");

  std::uint32_t object = 0;
  for (std::size_t i = 6; i < 10; ++i)
    object = (object << 8) | static_cast<unsigned char>(key[i]);

  if (object != round.state_->observer_namespace)
    return Status(ErrorCode::InvalidArgument, "observer state write crosses its namespace");

  return round.state_->stage(std::move(mutation));
}

Status EngineRuntimeAccess::attach_request_owner(Engine& engine,
                                                 std::uint32_t namespace_id) noexcept {
  EF_RETURN_IF_ERROR(engine.state_->ready());
  if (engine.state_->request_owner_attached)
    return Status(ErrorCode::Conflict, "an Engine may have only one request owner");
  if (namespace_id != 0) {
    if (namespace_id == engine.state_->observer_namespace)
      return Status(ErrorCode::InvalidArgument, "request namespace collides with observer state");

    for (const auto& relation : engine.state_->catalog.relations)
      if (relation->state_id == namespace_id)
        return Status(ErrorCode::InvalidArgument, "request namespace collides with Catalog state");

    auto& reserved = engine.state_->reserved_namespaces;
    if (!reserved.contains(namespace_id) && reserved.size() >= 128)
      return Status(ErrorCode::ResourceExhausted, "too many registered runtime namespaces");

    // Retain protection after detach for the lifetime of this Engine; archived records
    // remain owned state. Reopened runtimes register before admitting public operations.
    reserved.insert(namespace_id);
  }

  engine.state_->request_owner_attached = true;
  return {};
}

void EngineRuntimeAccess::detach_request_owner(Engine& engine) noexcept {
  engine.state_->request_owner_attached = false;
}

Status EngineRuntimeAccess::stage_request_state(RoundContext& round, Mutation mutation) noexcept {
  if (!round.state_->runtime_frame || round.state_->observer_frame)
    return Status(ErrorCode::FailedPrecondition, "request state requires an owner frame");

  OperationGuard permission(round.state_->request_state_write);
  return round.stage(std::move(mutation));
}

StatusOr<DispatchResult> EngineRuntimeAccess::resolve_request_commit(Engine& engine) noexcept {
  return engine.resolve_commit(true);
}

StatusOr<DispatchResult> EngineRuntimeAccess::apply(Engine& engine, const DispatchContext& context,
                                                    const RuntimePrepare& prepare,
                                                    const RuntimeFinalize& finalize,
                                                    const RuntimeCause& cause) noexcept {
  auto& state = *engine.state_;
  EF_RETURN_IF_ERROR(state.ready());
  OperationGuard operation(state.active);
  if (context.processing_time_us < state.time)
    return Status(ErrorCode::InvalidArgument, "processing time cannot move backwards");

  EF_ASSIGN_OR_RETURN(auto current, state.store->status(state.config.shard));
  if (current.revision != state.revision)
    return Status(ErrorCode::Conflict, "runtime owner revision is stale");

  RoundContext round(state.round(state.catalog, context.processing_time_us));
  round.state_->runtime_frame = true;
  const auto apply_owned = [&](const std::vector<TableChange>& writes,
                               bool final) noexcept -> StatusOr<std::set<RelationId>> {
    if (writes.size() > state.config.max_rows - round.state_->input_rows)
      return Status(ErrorCode::ResourceExhausted, "runtime frame input row budget exceeded");

    std::set<RelationId> roots;
    for (const auto& write : writes) {
      EF_ASSIGN_OR_RETURN(auto relation, state.catalog.find(write.relation));
      const bool status = relation->kind == RelationKind::BaseTable &&
                          relation->base_write_role == BaseWriteRole::RuntimeStatus;
      const bool capture = relation->kind == RelationKind::BaseTable &&
                           relation->base_write_role == BaseWriteRole::RequestCapture;
      const bool function = relation->kind == RelationKind::FunctionView;
      if ((final && !status) || (!final && !status && !capture && !function))
        return Status(ErrorCode::InvalidArgument,
                      "runtime callback has no write permission for relation");
      if (final) {
        EF_ASSIGN_OR_RETURN(auto affected, reachable(state.catalog, {relation->id}));
        for (auto id : affected) {
          EF_ASSIGN_OR_RETURN(auto descendant, state.catalog.find(id));
          if (!descendant->identity_domain.empty())
            return Status(ErrorCode::InvalidArgument,
                          "final status writes may not modify a request identity domain");
        }
      }

      if (relation->candidate || relation->request_status)
        EF_RETURN_IF_ERROR(round.state_->consume_metadata(write));
      if (capture || function) {
        if (!write.row)
          return Status(ErrorCode::InvalidArgument,
                        "captured/function rows are immutable and cannot be deleted");

        EF_ASSIGN_OR_RETURN(auto existing, round.state_->read_row(write.relation, write.identity,
                                                                  StateImage::Staged));
        if (existing)
          return Status(ErrorCode::Conflict,
                        "captured/function identity already exists; owner must handle duplicates");
      }

      EF_RETURN_IF_ERROR(round.state_->put_row(write.relation, write.identity, write.row));
      ++round.state_->input_rows;
      roots.insert(write.relation);
    }

    return roots;
  };
  std::vector<TableChange> initial;
  if (prepare) {
    EF_ASSIGN_OR_RETURN(initial, prepare(round));
  }

  EF_ASSIGN_OR_RETURN(auto roots, apply_owned(initial, false));
  EF_RETURN_IF_ERROR(state.stabilize(*round.state_, roots));
  if (finalize) {
    EF_ASSIGN_OR_RETURN(auto writes, finalize(round, flatten(round.state_->changes)));
    EF_ASSIGN_OR_RETURN(auto final_roots, apply_owned(writes, true));
    EF_RETURN_IF_ERROR(state.stabilize(*round.state_, final_roots));
  }

  if (!round.state_->metadata_authorizations.empty())
    return Status(ErrorCode::InvalidArgument, "unapplied control metadata intents");

  EF_RETURN_IF_ERROR(state.observe(round, cause));
  bool modified = false;
  for (const auto& [key, value] : round.state_->overlay) {
    EF_ASSIGN_OR_RETURN(auto before, round.state_->read(key, StateImage::Before));
    modified |= before != value;
  }

  if (!modified && context.processing_time_us == state.time)
    return DispatchResult{.commit = CommitResult{.code = CommitCode::Committed,
                                                 .revision = state.revision,
                                                 .duplicate = true,
                                                 .message = "runtime frame is unchanged"},
                          .changes = {}};

  EF_RETURN_IF_ERROR(state.stage_meta(*round.state_, state.catalog));
  auto changes = flatten(round.state_->changes);
  EF_ASSIGN_OR_RETURN(auto committed, state.publish(*round.state_));
  if (committed.code != CommitCode::Committed)
    changes.clear();

  return DispatchResult{.commit = std::move(committed), .changes = std::move(changes)};
}

StatusOr<TableChange>
EngineRuntimeAccess::make_request_status_write(RoundContext& round, RelationId id,
                                               const RequestManifest& manifest,
                                               RequestPhase phase) noexcept {
  EF_ASSIGN_OR_RETURN(auto relation, round.state_->catalog->find(id));
  if (!round.state_->runtime_frame || !relation->request_status)
    return Status(ErrorCode::InvalidArgument,
                  "request status factory needs an annotated owner relation");
  if (manifest.candidate_epoch == 0 || manifest.candidate_epoch > INT64_MAX ||
      manifest.admission_seq == 0 || manifest.admission_seq > INT64_MAX)
    return Status(ErrorCode::ResourceExhausted, "request metadata integer range exhausted");

  Row identity = manifest.scope;
  identity.emplace_back(manifest.request_id);
  if (identity != manifest.identity ||
      manifest.catalog_fingerprint != round.state_->catalog->fingerprint ||
      manifest.targets.empty())
    return Status(ErrorCode::InvalidArgument, "request metadata manifest provenance mismatch");

  for (auto target : manifest.targets) {
    EF_ASSIGN_OR_RETURN(auto desc, round.state_->catalog->find(target));
    if (desc->identity_domain != relation->request_status->definition.request_domain)
      return Status(ErrorCode::InvalidArgument, "request target does not belong to status domain");
  }

  EF_ASSIGN_OR_RETURN(auto name, request_phase_name(phase));
  Row row = identity;
  row.emplace_back(static_cast<std::int64_t>(manifest.candidate_epoch));
  row.emplace_back(static_cast<std::int64_t>(manifest.admission_seq));
  row.emplace_back(name);
  EF_ASSIGN_OR_RETURN(row, make_stored_row(*relation, row, identity));
  EF_ASSIGN_OR_RETURN(auto existing, round.read_relation(id, identity));
  const auto base = identity.size();
  if (existing) {
    if ((*existing)[base] != row[base] || (*existing)[base + 1] != row[base + 1])
      return Status(ErrorCode::Conflict, "request metadata identity and ordering are immutable");

    const auto& old = std::get<std::string>((*existing)[base + 2]);
    if ((request_terminal(old) && old != name) || (old == "RUNNING" && name == "ACCEPTED"))
      return Status(ErrorCode::Conflict, "request metadata phase cannot regress");
  } else {
    EF_ASSIGN_OR_RETURN(auto candidate,
                        round.read_relation(relation->request_status->definition.candidate_relation,
                                            manifest.scope));
    if (!candidate || !std::get<bool>((*candidate)[manifest.scope.size() + 1]) ||
        std::get<std::int64_t>((*candidate)[manifest.scope.size()]) !=
            static_cast<std::int64_t>(manifest.candidate_epoch))
      return Status(ErrorCode::FailedPrecondition,
                    "new request does not match an active candidate");
  }

  TableChange write{.relation = id, .identity = std::move(identity), .row = std::move(row)};
  EF_RETURN_IF_ERROR(round.state_->authorize_metadata(write));
  return write;
}

StatusOr<TableChange> EngineRuntimeAccess::make_candidate_write(RoundContext& round, RelationId id,
                                                                const Row& scope,
                                                                CandidateOperation operation,
                                                                std::int64_t expected_epoch,
                                                                const Row& payload) noexcept {
  EF_ASSIGN_OR_RETURN(auto relation, round.state_->catalog->find(id));
  if ((!round.state_->runtime_frame && !round.state_->metadata_frame) || !relation->candidate ||
      expected_epoch < 0)
    return Status(ErrorCode::InvalidArgument,
                  "candidate factory needs an annotated owner relation");

  EF_ASSIGN_OR_RETURN(auto existing, round.read_relation(id, scope));
  const auto old_epoch = existing ? std::get<std::int64_t>((*existing)[scope.size()]) : 0;
  if (old_epoch != expected_epoch)
    return Status(ErrorCode::Conflict, "candidate expected epoch is stale");

  Row row;
  if (operation == CandidateOperation::Activate) {
    if (expected_epoch == INT64_MAX)
      return Status(ErrorCode::ResourceExhausted, "candidate epoch exhausted");

    row = scope;
    row.emplace_back(expected_epoch + 1);
    row.emplace_back(true);
    row.insert(row.end(), payload.begin(), payload.end());
  } else if (operation == CandidateOperation::Deactivate) {
    if (!existing || !payload.empty())
      return Status(ErrorCode::InvalidArgument,
                    "deactivation requires an existing candidate and no new payload");

    row = *existing;
    row[scope.size() + 1] = false;
  } else
    return Status(ErrorCode::InvalidArgument, "unknown candidate operation");
  EF_ASSIGN_OR_RETURN(row, make_stored_row(*relation, row, scope));
  TableChange write{.relation = id, .identity = scope, .row = std::move(row)};
  EF_RETURN_IF_ERROR(round.state_->authorize_metadata(write));

  return write;
}
} // namespace eventflow
