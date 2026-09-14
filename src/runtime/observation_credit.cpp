#include "eventflow/observation_credit.hpp"
#include <array>
#include <limits>
#include <set>

namespace eventflow {
namespace {
using Axis = std::size_t ObservationCredit::*;
constexpr std::array<Axis, 8> axes = {
    &ObservationCredit::events,          &ObservationCredit::bytes,
    &ObservationCredit::read_rows,       &ObservationCredit::read_bytes,
    &ObservationCredit::mutations,       &ObservationCredit::mutation_bytes,
    &ObservationCredit::canonical_bytes, &ObservationCredit::wire_bytes};
using Entries = std::map<std::string, ObservationCreditEntry>;

Status invalid(std::string_view message) noexcept {
  return {ErrorCode::InvalidArgument, message};
}

Status full(std::string_view message) noexcept {
  return {ErrorCode::ResourceExhausted, message};
}

bool empty(const ObservationCredit& credit) noexcept {
  return credit == ObservationCredit{};
}

Status add(ObservationCredit& total, const ObservationCredit& increment,
           const ObservationCredit& capacity) noexcept {
  for (const auto axis : axes)
    if (total.*axis > capacity.*axis || increment.*axis > capacity.*axis - total.*axis)
      return full("observation credit capacity exceeded");

  for (const auto axis : axes)
    total.*axis += increment.*axis;

  return {};
}

Status validate_limits(const ObservationCreditLimits& limits) noexcept {
  if (limits.max_owners == 0 || limits.max_identifier_bytes == 0 || limits.max_encoded_bytes == 0)
    return invalid("observation credit codec bounds must be positive");

  return {};
}

Status validate_owner(std::string_view owner, const ObservationCreditLimits& limits) noexcept {
  if (owner.empty() || !valid_utf8(owner))
    return invalid("observation credit owner must be nonempty UTF-8");
  if (owner.size() > limits.max_identifier_bytes)
    return full("observation credit owner exceeds identifier bound");

  return {};
}

Status validate_entry(const ObservationCreditEntry& entry,
                      const ObservationCreditLimits& limits) noexcept {
  EF_RETURN_IF_ERROR(validate_owner(entry.owner, limits));
  if (empty(entry.occupied) && empty(entry.reserved_future))
    return invalid("empty observation credit owners must be removed");

  return {};
}

Status validate_entries(const Entries& entries, const ObservationCreditLimits& limits) noexcept {
  EF_RETURN_IF_ERROR(validate_limits(limits));
  if (entries.size() > limits.max_owners)
    return full("observation credit owner count exceeded");

  ObservationCredit total;
  for (const auto& [owner, entry] : entries) {
    if (owner != entry.owner)
      return invalid("observation credit owner identity mismatch");

    EF_RETURN_IF_ERROR(validate_entry(entry, limits));
    EF_RETURN_IF_ERROR(add(total, entry.occupied, limits.capacity));
    EF_RETURN_IF_ERROR(add(total, entry.reserved_future, limits.capacity));
  }

  return {};
}

Json credit_json(const ObservationCredit& credit) noexcept {
  Json result = Json::array();
  for (const auto axis : axes)
    result.push_back(credit.*axis);

  return result;
}

StatusOr<std::string> encode_entries(const Entries& entries,
                                     const ObservationCreditLimits& limits) noexcept {
  EF_RETURN_IF_ERROR(validate_entries(entries, limits));
  Json owners = Json::array();
  for (const auto& [owner, entry] : entries)
    owners.push_back({{"owner", owner},
                      {"occupied", credit_json(entry.occupied)},
                      {"reserved_future", credit_json(entry.reserved_future)}});

  auto encoded = Json{{"format", 1}, {"owners", std::move(owners)}}.dump();
  if (encoded.size() > limits.max_encoded_bytes)
    return full("observation credit snapshot exceeds byte bound");

  return encoded;
}

bool members(const Json& json, std::initializer_list<const char*> fields) noexcept {
  if (!json.is_object() || json.size() != fields.size())
    return false;

  for (const auto* field : fields)
    if (!json.contains(field))
      return false;

  return true;
}

StatusOr<std::size_t> size_value(const Json& json) noexcept {
  if (!json.is_number_integer() || (!json.is_number_unsigned() && json.get<std::int64_t>() < 0))
    return invalid("observation credit must be an unsigned size_t integer");

  const auto value = json.get<std::uint64_t>();
  if (value > std::numeric_limits<std::size_t>::max())
    return invalid("observation credit integer exceeds size_t");

  return static_cast<std::size_t>(value);
}

StatusOr<ObservationCredit> parse_credit(const Json& json) noexcept {
  if (!json.is_array() || json.size() != axes.size())
    return invalid("observation credit requires exactly eight resource axes");

  ObservationCredit credit;
  for (std::size_t i = 0; i < axes.size(); ++i) {
    EF_ASSIGN_OR_RETURN(auto amount, size_value(json[i]));
    credit.*axes[i] = amount;
  }

  return credit;
}

StatusOr<Json> parse_snapshot(std::string_view bytes,
                              const ObservationCreditLimits& limits) noexcept {
  EF_RETURN_IF_ERROR(validate_limits(limits));
  if (bytes.size() > limits.max_encoded_bytes)
    return full("observation credit snapshot exceeds byte bound");

  // Bound nesting before the JSON parser allocates its container stack. Strings
  // may contain brackets and escaped quotes; those are not structural tokens.
  std::size_t depth = 0;
  bool quoted = false, escaped = false;

  for (const auto c : bytes) {
    if (quoted) {
      if (escaped)
        escaped = false;
      else if (c == '\\')
        escaped = true;
      else if (c == '"')
        quoted = false;
    } else if (c == '"') {
      quoted = true;
    } else if (c == '{' || c == '[') {
      if (++depth > 16)
        return invalid("observation credit snapshot is nested too deeply");
    } else if (c == '}' || c == ']') {
      if (depth == 0)
        return invalid("unbalanced observation credit snapshot");

      --depth;
    }
  }

  bool duplicate = false;
  std::array<std::set<std::string>, 16> keys;
  auto json = Json::parse(
      bytes,
      [&](int level, Json::parse_event_t event, Json& value) noexcept {
        if (event == Json::parse_event_t::object_start)
          keys[static_cast<std::size_t>(level)].clear();
        else if (event == Json::parse_event_t::key)
          duplicate |=
              !keys[static_cast<std::size_t>(level - 1)].insert(value.get<std::string>()).second;
        return true;
      },
      false);
  if (json.is_discarded() || duplicate)
    return invalid("malformed or duplicate observation credit snapshot fields");

  return json;
}
} // namespace

ObservationCreditLedger::ObservationCreditLedger(ObservationCreditLimits limits) noexcept
    : limits_(std::move(limits)) {}

StatusOr<ObservationCreditLedger>
ObservationCreditLedger::create(const ObservationCreditLimits& limits) noexcept {
  EF_RETURN_IF_ERROR(validate_limits(limits));
  ObservationCreditLedger ledger(limits);
  EF_ASSIGN_OR_RETURN(auto encoded, ledger.encode());
  (void)encoded;

  return ledger;
}

StatusOr<ObservationCreditLedger>
ObservationCreditLedger::from_snapshot(std::string_view bytes,
                                       const ObservationCreditLimits& limits) noexcept {
  EF_ASSIGN_OR_RETURN(auto json, parse_snapshot(bytes, limits));
  if (!members(json, {"format", "owners"}) || !json["owners"].is_array())
    return invalid("invalid observation credit snapshot layout");

  EF_ASSIGN_OR_RETURN(auto format, size_value(json["format"]));
  if (format != 1)
    return invalid("unsupported observation credit snapshot version");
  if (json["owners"].size() > limits.max_owners)
    return full("observation credit owner count exceeded");

  ObservationCreditLedger ledger(limits);
  for (const auto& value : json["owners"]) {
    if (!members(value, {"owner", "occupied", "reserved_future"}) || !value["owner"].is_string())
      return invalid("invalid observation credit owner layout");

    ObservationCreditEntry entry;
    entry.owner = value["owner"].get<std::string>();
    EF_ASSIGN_OR_RETURN(entry.occupied, parse_credit(value["occupied"]));
    EF_ASSIGN_OR_RETURN(entry.reserved_future, parse_credit(value["reserved_future"]));
    EF_RETURN_IF_ERROR(validate_entry(entry, limits));
    if (!ledger.entries_.emplace(entry.owner, entry).second)
      return invalid("duplicate observation credit owner");
  }

  EF_ASSIGN_OR_RETURN(auto encoded, ledger.encode());
  (void)encoded;
  return ledger;
}

Status
ObservationCreditLedger::reserve(std::span<const ObservationCreditEntry> additions) noexcept {
  if (additions.size() > limits_.max_owners - entries_.size())
    return full("observation credit owner count exceeded");

  auto next = entries_;
  for (const auto& entry : additions) {
    EF_RETURN_IF_ERROR(validate_entry(entry, limits_));
    if (!empty(entry.occupied))
      return invalid("new observation credit must reserve future resources");
    if (!next.emplace(entry.owner, entry).second)
      return {ErrorCode::Conflict, "observation credit owner already exists"};
  }

  EF_ASSIGN_OR_RETURN(auto encoded, encode_entries(next, limits_));
  (void)encoded;
  entries_.swap(next);
  return {};
}

Status
ObservationCreditLedger::rewrite(std::span<const std::string> consumed_owners,
                                 std::span<const ObservationCreditEntry> replacements) noexcept {
  if (consumed_owners.size() > entries_.size() || replacements.size() > limits_.max_owners)
    return full("observation credit rewrite exceeds owner count bound");

  std::set<std::string> consumed;
  ObservationCredit available;
  for (const auto& owner : consumed_owners) {
    EF_RETURN_IF_ERROR(validate_owner(owner, limits_));
    if (!consumed.insert(owner).second)
      return invalid("observation credit owner consumed twice");

    const auto found = entries_.find(owner);
    if (found == entries_.end())
      return {ErrorCode::NotFound, "observation credit owner does not exist"};

    EF_RETURN_IF_ERROR(add(available, found->second.occupied, limits_.capacity));
    EF_RETURN_IF_ERROR(add(available, found->second.reserved_future, limits_.capacity));
  }

  if (replacements.size() > limits_.max_owners - (entries_.size() - consumed.size()))
    return full("observation credit owner count exceeded");

  auto next = entries_;
  for (const auto& owner : consumed)
    next.erase(owner);

  ObservationCredit transferred;
  for (const auto& entry : replacements) {
    EF_RETURN_IF_ERROR(validate_entry(entry, limits_));
    EF_RETURN_IF_ERROR(add(transferred, entry.occupied, available));
    EF_RETURN_IF_ERROR(add(transferred, entry.reserved_future, available));
    if (!next.emplace(entry.owner, entry).second)
      return {ErrorCode::Conflict, "observation credit replacement owner already exists"};
  }

  EF_ASSIGN_OR_RETURN(auto encoded, encode_entries(next, limits_));
  (void)encoded;
  entries_.swap(next);
  return {};
}

const std::map<std::string, ObservationCreditEntry>&
ObservationCreditLedger::entries() const noexcept {
  return entries_;
}

ObservationCredit ObservationCreditLedger::occupied() const noexcept {
  ObservationCredit total;
  for (const auto& [owner, entry] : entries_) {
    (void)owner;
    for (const auto axis : axes)
      total.*axis += entry.occupied.*axis;
  }

  return total;
}

ObservationCredit ObservationCreditLedger::reserved_future() const noexcept {
  ObservationCredit total;
  for (const auto& [owner, entry] : entries_) {
    (void)owner;
    for (const auto axis : axes)
      total.*axis += entry.reserved_future.*axis;
  }

  return total;
}

StatusOr<std::string> ObservationCreditLedger::encode() const noexcept {
  return encode_entries(entries_, limits_);
}
} // namespace eventflow
