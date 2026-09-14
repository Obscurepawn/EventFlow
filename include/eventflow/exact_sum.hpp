#pragma once

#include "eventflow/status.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace eventflow {

// A signed integer measured in binary64's smallest subnormal unit, 2^-1074.
// The fixed capacity covers INT64_MAX finite binary64 contributions.
class ExactSum {
public:
  static constexpr std::size_t kLimbCount = 34;
  static constexpr std::size_t kEncodedSize = 8 + kLimbCount * 8;

  ExactSum() noexcept = default;
  Status add(double value) noexcept;
  Status remove(double value) noexcept;
  Status add_int(std::int64_t value) noexcept;
  Status remove_int(std::int64_t value) noexcept;

  // Round the exact quotient once using binary64 round-to-nearest, ties-to-even.
  // Overflow and a zero divisor fail. A negative underflow may produce -0.0.
  StatusOr<double> to_double(std::uint64_t divisor = 1) const noexcept;
  // A fractional or out-of-range result fails instead of truncating/wrapping.
  StatusOr<std::int64_t> to_int64() const noexcept;

  // Version 1: "EFS1", byte 1 (little endian), sign byte, two zero reserved
  // bytes, then 34 little-endian uint64 limbs from least to most significant.
  StatusOr<std::string> encode() const noexcept;
  static StatusOr<ExactSum> decode(std::string_view bytes) noexcept;

  // Tests algebraic zero; this object does not track the contribution count.
  bool empty() const noexcept;

private:
  Status apply_double(double value, bool retract) noexcept;
  Status apply_int(std::int64_t value, bool retract) noexcept;
  Status accumulate(std::uint64_t significand, unsigned shift, bool negative) noexcept;

  std::array<std::uint64_t, kLimbCount> limbs_{};
  bool negative_ = false;
};

} // namespace eventflow
