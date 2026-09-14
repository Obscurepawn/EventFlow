#include "eventflow/exact_sum.hpp"

#include <bit>
#include <climits>
#include <cstring>
#include <limits>

namespace eventflow {
namespace {

static_assert(CHAR_BIT == 8 && sizeof(double) == 8 && std::numeric_limits<double>::is_iec559 &&
              std::numeric_limits<double>::digits == 53 &&
              std::numeric_limits<double>::max_exponent == 1024);

// The packaged Linux GCC/Clang platform provides this integer intermediate.
__extension__ using Wide = unsigned __int128;
using Magnitude = std::array<std::uint64_t, ExactSum::kLimbCount>;
constexpr std::uint64_t kFractionMask = (std::uint64_t{1} << 52) - 1;
constexpr std::uint64_t kSignBit = std::uint64_t{1} << 63;

// Accumulate in units of 2^-1074, the smallest positive binary64 value. Removing an input can then
// undo its contribution exactly; rounding is deferred until a result is read.
constexpr unsigned kIntegerShift = 1074;

unsigned bit_length(const Magnitude& value) noexcept {
  for (std::size_t i = value.size(); i > 0; --i) {
    if (value[i - 1] != 0)
      return static_cast<unsigned>(64 * (i - 1)) + 64u -
             static_cast<unsigned>(std::countl_zero(value[i - 1]));
  }

  return 0;
}

bool any_below(const Magnitude& value, unsigned bits) noexcept {
  const unsigned whole = bits / 64;
  for (unsigned i = 0; i < whole; ++i) {
    if (value[i] != 0)
      return true;
  }

  const unsigned tail = bits % 64;
  return tail != 0 && (value[whole] & ((std::uint64_t{1} << tail) - 1)) != 0;
}

std::uint64_t shifted_word(const Magnitude& value, unsigned shift) noexcept {
  const unsigned word = shift / 64;
  const unsigned bit = shift % 64;
  std::uint64_t result = value[word] >> bit;
  if (bit != 0 && word + 1 < value.size())
    result |= value[word + 1] << (64 - bit);

  return result;
}

int compare(const Magnitude& left, const Magnitude& right) noexcept {
  for (std::size_t i = left.size(); i > 0; --i) {
    if (left[i - 1] != right[i - 1])
      return left[i - 1] < right[i - 1] ? -1 : 1;
  }

  return 0;
}

// Precondition: left >= right. Unsigned subtraction is defined modulo 2^64.
Magnitude subtract(const Magnitude& left, const Magnitude& right) noexcept {
  Magnitude result{};
  bool borrow = false;
  for (std::size_t i = 0; i < left.size(); ++i) {
    result[i] = left[i] - right[i] - static_cast<std::uint64_t>(borrow);
    borrow = left[i] < right[i] || (borrow && left[i] == right[i]);
  }

  return result;
}

} // namespace

Status ExactSum::add(double value) noexcept {
  return apply_double(value, false);
}

Status ExactSum::remove(double value) noexcept {
  return apply_double(value, true);
}

Status ExactSum::add_int(std::int64_t value) noexcept {
  return apply_int(value, false);
}

Status ExactSum::remove_int(std::int64_t value) noexcept {
  return apply_int(value, true);
}

Status ExactSum::apply_double(double value, bool retract) noexcept {
  const auto bits = std::bit_cast<std::uint64_t>(value);
  const auto exponent = static_cast<unsigned>((bits >> 52) & 0x7ff);
  if (exponent == 0x7ff)
    return Status(ErrorCode::InvalidArgument, "exact sum requires finite binary64 input");

  const std::uint64_t significand =
      (bits & kFractionMask) | (exponent == 0 ? 0 : std::uint64_t{1} << 52);
  return accumulate(significand, exponent == 0 ? 0 : exponent - 1,
                    ((bits & kSignBit) != 0) != retract);
}

Status ExactSum::apply_int(std::int64_t value, bool retract) noexcept {
  const bool negative = value < 0;
  const auto unsigned_value = static_cast<std::uint64_t>(value);
  const std::uint64_t magnitude = negative ? 0 - unsigned_value : unsigned_value;
  return accumulate(magnitude, kIntegerShift, negative != retract);
}

Status ExactSum::accumulate(std::uint64_t significand, unsigned shift, bool negative) noexcept {
  if (significand == 0)
    return {};

  Magnitude incoming{};
  const unsigned word = shift / 64;
  const unsigned bit = shift % 64;
  incoming[word] = significand << bit;
  if (bit != 0)
    incoming[word + 1] = significand >> (64 - bit);

  if (negative_ == negative) {
    Magnitude candidate{};
    Wide carry = 0;
    for (std::size_t i = 0; i < limbs_.size(); ++i) {
      const Wide sum = static_cast<Wide>(limbs_[i]) + incoming[i] + carry;
      candidate[i] = static_cast<std::uint64_t>(sum);
      carry = sum >> 64;
    }

    if (carry != 0)
      return Status(ErrorCode::ResourceExhausted, "exact sum limb capacity exceeded");

    limbs_ = candidate;
  } else {
    const int order = compare(limbs_, incoming);
    if (order >= 0) {
      limbs_ = subtract(limbs_, incoming);
      if (order == 0)
        negative_ = false;
    } else {
      limbs_ = subtract(incoming, limbs_);
      negative_ = negative;
    }
  }

  return {};
}

StatusOr<double> ExactSum::to_double(std::uint64_t divisor) const noexcept {
  if (divisor == 0)
    return Status(ErrorCode::InvalidArgument, "exact sum divisor must be positive");

  // N / D = Q + r / D. Keep both pieces until the final rounding decision.
  Magnitude quotient{};
  std::uint64_t remainder = 0;
  for (std::size_t i = limbs_.size(); i > 0; --i) {
    const Wide dividend = (static_cast<Wide>(remainder) << 64) | limbs_[i - 1];
    quotient[i - 1] = static_cast<std::uint64_t>(dividend / divisor);
    remainder = static_cast<std::uint64_t>(dividend % divisor);
  }

  const unsigned length = bit_length(quotient);
  unsigned shift = length > 53 ? length - 53 : 0;
  if (shift > 2045)
    return Status(ErrorCode::InvalidArgument, "exact sum binary64 result overflows");

  std::uint64_t significand = shifted_word(quotient, shift);
  bool round_up = false;
  if (shift == 0) {
    // Comparing r with D-r avoids overflowing a uint64 by computing 2*r.
    round_up = remainder > divisor - remainder ||
               (remainder == divisor - remainder && (significand & 1) != 0);
  } else {
    const bool guard = ((quotient[(shift - 1) / 64] >> ((shift - 1) % 64)) & 1) != 0;
    const bool sticky = any_below(quotient, shift - 1) || remainder != 0;
    round_up = guard && (sticky || (significand & 1) != 0);
  }

  significand += static_cast<std::uint64_t>(round_up);
  if (significand == (std::uint64_t{1} << 53)) {
    significand >>= 1;
    ++shift;
  }

  const std::uint64_t exponent = significand < (std::uint64_t{1} << 52) ? 0 : shift + 1;
  if (exponent >= 0x7ff)
    return Status(ErrorCode::InvalidArgument, "exact sum binary64 result overflows");

  const std::uint64_t bits =
      (negative_ ? kSignBit : 0) | (exponent << 52) | (significand & kFractionMask);
  return std::bit_cast<double>(bits);
}

StatusOr<std::int64_t> ExactSum::to_int64() const noexcept {
  if (any_below(limbs_, kIntegerShift))
    return Status(ErrorCode::InvalidArgument, "exact sum result is not an integer");
  if (bit_length(limbs_) > kIntegerShift + 64)
    return Status(ErrorCode::InvalidArgument, "exact sum Int64 result overflows");

  const std::uint64_t magnitude = shifted_word(limbs_, kIntegerShift);
  if ((!negative_ && magnitude > static_cast<std::uint64_t>(INT64_MAX)) ||
      (negative_ && magnitude > kSignBit))
    return Status(ErrorCode::InvalidArgument, "exact sum Int64 result overflows");
  if (negative_ && magnitude == kSignBit)
    return std::int64_t{INT64_MIN};

  const auto signed_magnitude = static_cast<std::int64_t>(magnitude);
  return negative_ ? -signed_magnitude : signed_magnitude;
}

StatusOr<std::string> ExactSum::encode() const noexcept {
  std::string bytes(kEncodedSize, '\0');
  std::memcpy(bytes.data(), "EFS1", 4);
  bytes[4] = 1;
  bytes[5] = negative_ ? 1 : 0;

  for (std::size_t i = 0; i < limbs_.size(); ++i) {
    for (unsigned byte = 0; byte < 8; ++byte)
      bytes[8 + i * 8 + byte] = static_cast<char>((limbs_[i] >> (byte * 8)) & 0xff);
  }

  return bytes;
}

StatusOr<ExactSum> ExactSum::decode(std::string_view bytes) noexcept {
  if (bytes.size() != kEncodedSize || bytes.substr(0, 4) != "EFS1" || bytes[4] != 1 ||
      (bytes[5] != 0 && bytes[5] != 1) || bytes[6] != 0 || bytes[7] != 0)
    return Status(ErrorCode::InvalidArgument, "invalid exact sum state encoding");

  ExactSum result;
  result.negative_ = bytes[5] == 1;
  for (std::size_t i = 0; i < result.limbs_.size(); ++i) {
    for (unsigned byte = 0; byte < 8; ++byte)
      result.limbs_[i] |=
          static_cast<std::uint64_t>(static_cast<unsigned char>(bytes[8 + i * 8 + byte]))
          << (byte * 8);
  }

  if (result.negative_ && result.empty())
    return Status(ErrorCode::InvalidArgument, "exact sum state has noncanonical negative zero");

  return result;
}

bool ExactSum::empty() const noexcept {
  return bit_length(limbs_) == 0;
}

} // namespace eventflow
