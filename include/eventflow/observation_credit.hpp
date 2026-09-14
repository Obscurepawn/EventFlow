#pragma once

#include "eventflow/types.hpp"
#include <map>
#include <span>

namespace eventflow {
// Logical reservations, not an RSS or execution-latency guarantee. Unused axes may be zero.
// Snapshot format 1 stores resource axes in this declaration order.
struct ObservationCredit {
  std::size_t events = 0;
  std::size_t bytes = 0;
  std::size_t read_rows = 0;
  std::size_t read_bytes = 0;
  std::size_t mutations = 0;
  std::size_t mutation_bytes = 0;
  std::size_t canonical_bytes = 0;
  std::size_t wire_bytes = 0;
  bool operator==(const ObservationCredit&) const noexcept = default;
};

struct ObservationCreditEntry {
  std::string owner;
  ObservationCredit occupied;
  ObservationCredit reserved_future;
  bool operator==(const ObservationCreditEntry&) const noexcept = default;
};

struct ObservationCreditLimits {
  ObservationCredit capacity;
  std::size_t max_owners = 1024;
  std::size_t max_identifier_bytes = 1024;
  std::size_t max_encoded_bytes = 1024 * 1024;
};

class ObservationCreditLedger {
public:
  static StatusOr<ObservationCreditLedger> create(const ObservationCreditLimits&) noexcept;
  static StatusOr<ObservationCreditLedger> from_snapshot(std::string_view,
                                                         const ObservationCreditLimits&) noexcept;
  // Updates affect this candidate only; its owner publishes it after the containing
  // storage transaction is confirmed. No method performs persistence or acknowledges work.
  // Each new owner has a unique identity; occupied must be zero at reservation.
  Status reserve(std::span<const ObservationCreditEntry> additions) noexcept;
  // Atomically replace all consumed owners. Each resulting resource axis must be
  // covered by their combined old credit; omitted credit is released. New owners
  // may reuse consumed IDs, but may not overwrite any unrelated existing owner.
  Status rewrite(std::span<const std::string> consumed_owners,
                 std::span<const ObservationCreditEntry> replacements) noexcept;
  const std::map<std::string, ObservationCreditEntry>& entries() const noexcept;
  ObservationCredit occupied() const noexcept;
  ObservationCredit reserved_future() const noexcept;
  StatusOr<std::string> encode() const noexcept;

private:
  explicit ObservationCreditLedger(ObservationCreditLimits limits) noexcept;
  ObservationCreditLimits limits_;
  std::map<std::string, ObservationCreditEntry> entries_;
};
} // namespace eventflow
