#include "eventflow/exact_sum.hpp"
#include "status_test_support.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cfenv>
#include <cmath>
#include <cstdint>
#include <limits>
#include <random>
#include <string>
#include <vector>

namespace eventflow {
namespace {
using test::take;

constexpr double kTiny = std::numeric_limits<double>::denorm_min();
constexpr double kMax = std::numeric_limits<double>::max();

void expect_bits(double actual, double expected) noexcept {
  EXPECT_EQ(std::bit_cast<std::uint64_t>(actual), std::bit_cast<std::uint64_t>(expected));
}

static_assert(noexcept(std::declval<ExactSum&>().add(0.0)));
static_assert(noexcept(std::declval<ExactSum&>().remove(0.0)));
static_assert(noexcept(std::declval<ExactSum&>().add_int(0)));
static_assert(noexcept(std::declval<ExactSum&>().remove_int(0)));
static_assert(noexcept(std::declval<const ExactSum&>().to_double()));
static_assert(noexcept(std::declval<const ExactSum&>().to_int64()));
static_assert(noexcept(std::declval<const ExactSum&>().encode()));
static_assert(noexcept(ExactSum::decode(std::string_view{})));
static_assert(noexcept(std::declval<const ExactSum&>().empty()));

TEST(ExactSum, RetractionPreservesPreviouslyRoundedAwayContribution) {
  ExactSum sum;
  ASSERT_TRUE(sum.add(1e16).ok());
  ASSERT_TRUE(sum.add(1.0).ok());
  ASSERT_TRUE(sum.remove(1e16).ok());
  EXPECT_EQ(take(sum.to_double()), 1.0);
}

TEST(ExactSum, EveryBinary64ExponentAndItsBoundarySignificandsRoundTrip) {
  for (std::uint64_t exponent = 0; exponent < 2047; ++exponent) {
    for (const std::uint64_t fraction :
         {std::uint64_t{0}, std::uint64_t{1}, (std::uint64_t{1} << 52) - 1}) {
      for (const std::uint64_t sign : {std::uint64_t{0}, std::uint64_t{1} << 63}) {
        const auto bits = sign | (exponent << 52) | fraction;
        const double value = std::bit_cast<double>(bits);
        ExactSum sum;
        ASSERT_TRUE(sum.add(value).ok());
        expect_bits(take(sum.to_double()), value == 0 ? 0.0 : value);
      }
    }
  }
}

TEST(ExactSum, AverageCanBeFiniteWhenTheUnscaledSumOverflows) {
  for (const double sign : {1.0, -1.0}) {
    ExactSum sum;
    ASSERT_TRUE(sum.add(sign * kMax).ok());
    ASSERT_TRUE(sum.add(sign * kMax).ok());
    EXPECT_EQ(sum.to_double().status().code(), ErrorCode::InvalidArgument);
    expect_bits(take(sum.to_double(2)), sign * kMax);
    ASSERT_TRUE(sum.remove(sign * kMax).ok());
    expect_bits(take(sum.to_double()), sign * kMax);
  }
}

TEST(ExactSum, NormalTiesRoundToEvenAndTinyStickyBitsBreakTheTie) {
  ExactSum even;
  ASSERT_TRUE(even.add(1.0).ok());
  ASSERT_TRUE(even.add(0x1p-53).ok());
  expect_bits(take(even.to_double()), 1.0);
  ASSERT_TRUE(even.add(kTiny).ok());
  expect_bits(take(even.to_double()), 0x1.0000000000001p0);
  ASSERT_TRUE(even.remove(kTiny).ok());
  expect_bits(take(even.to_double()), 1.0);

  ExactSum odd;
  ASSERT_TRUE(odd.add(1.0).ok());
  ASSERT_TRUE(odd.add(0x1.8p-52).ok());
  expect_bits(take(odd.to_double()), 0x1.0000000000002p0);
  ASSERT_TRUE(odd.remove(kTiny).ok());
  expect_bits(take(odd.to_double()), 0x1.0000000000001p0);
}

TEST(ExactSum, SubnormalTiesPreserveEvenAndNegativeUnderflowSign) {
  ExactSum sum;
  ASSERT_TRUE(sum.add(kTiny).ok());
  expect_bits(take(sum.to_double(2)), 0.0);
  ASSERT_TRUE(sum.add(2 * kTiny).ok());
  expect_bits(take(sum.to_double(2)), 2 * kTiny);
  ASSERT_TRUE(sum.remove(4 * kTiny).ok());
  expect_bits(take(sum.to_double(2)), -0.0);
  EXPECT_FALSE(sum.empty());
  ASSERT_TRUE(sum.remove(2 * kTiny).ok());
  expect_bits(take(sum.to_double(2)), -2 * kTiny);
}

TEST(ExactSum, SubnormalToNormalMidpointRoundsToMinimumNormal) {
  ExactSum sum;
  ASSERT_TRUE(sum.add(std::numeric_limits<double>::min()).ok());
  ASSERT_TRUE(sum.add(std::nextafter(std::numeric_limits<double>::min(), 0.0)).ok());
  expect_bits(take(sum.to_double(2)), std::numeric_limits<double>::min());
}

TEST(ExactSum, RoundingCarryRenormalizesAndDetectsTheOverflowMidpoint) {
  ExactSum carry;
  ASSERT_TRUE(carry.add(0x1.fffffffffffffp0).ok());
  ASSERT_TRUE(carry.add(0x1p-53).ok());
  expect_bits(take(carry.to_double()), 2.0);

  ExactSum sum;
  ASSERT_TRUE(sum.add(kMax).ok());
  ASSERT_TRUE(sum.add(0x1p969).ok());
  expect_bits(take(sum.to_double()), kMax);
  ASSERT_TRUE(sum.add(0x1p969).ok());
  EXPECT_EQ(sum.to_double().status().code(), ErrorCode::InvalidArgument);
}

TEST(ExactSum, DivisionUsesItsRemainderForRounding) {
  ExactSum sum;
  ASSERT_TRUE(sum.add(1.0).ok());
  expect_bits(take(sum.to_double(3)), 0x1.5555555555555p-2);
  expect_bits(take(sum.to_double(10)), 0x1.999999999999ap-4);
  ASSERT_TRUE(sum.remove(2.0).ok());
  expect_bits(take(sum.to_double(10)), -0x1.999999999999ap-4);

  ExactSum tiny;
  ASSERT_TRUE(tiny.add(2 * kTiny).ok());
  expect_bits(take(tiny.to_double(3)), kTiny);

  // Q has an even retained significand, guard 1 and no lower set bits. Only
  // the nonzero division remainder distinguishes this from an exact tie.
  ExactSum remainder_sticky;
  ASSERT_TRUE(remainder_sticky.add(0x1.8000000000001p-1020).ok());
  expect_bits(take(remainder_sticky.to_double(3)), 0x1.0000000000001p-1021);
}

TEST(ExactSum, LargestDivisorDoesNotOverflowRemainderArithmetic) {
  ExactSum sum;
  ASSERT_TRUE(sum.add_int(INT64_MAX).ok());
  expect_bits(take(sum.to_double(UINT64_MAX)), 0.5);
  ExactSum tiny;
  ASSERT_TRUE(tiny.add(kTiny).ok());
  expect_bits(take(tiny.to_double(UINT64_MAX)), 0.0);

  ExactSum large_remainder;
  ASSERT_TRUE(large_remainder.add(0x1p-1011).ok());
  // With Q=0, remainder=2^63 is just over half of UINT64_MAX.
  expect_bits(take(large_remainder.to_double(UINT64_MAX)), kTiny);
  EXPECT_EQ(sum.to_double(0).status().code(), ErrorCode::InvalidArgument);
  EXPECT_EQ(ExactSum{}.to_double(0).status().code(), ErrorCode::InvalidArgument);
}

TEST(ExactSum, Int64ExtremesAreExactAndIntermediateOverflowCanBeRetracted) {
  ExactSum sum;
  ASSERT_TRUE(sum.add_int(INT64_MAX).ok());
  EXPECT_EQ(take(sum.to_int64()), INT64_MAX);
  ASSERT_TRUE(sum.add_int(INT64_MAX).ok());
  EXPECT_EQ(sum.to_int64().status().code(), ErrorCode::InvalidArgument);
  expect_bits(take(sum.to_double(2)), 0x1p63);
  ASSERT_TRUE(sum.remove_int(INT64_MAX).ok());
  EXPECT_EQ(take(sum.to_int64()), INT64_MAX);
  ASSERT_TRUE(sum.add_int(INT64_MIN).ok());
  EXPECT_EQ(take(sum.to_int64()), -1);
  ASSERT_TRUE(sum.remove_int(INT64_MAX).ok());
  EXPECT_EQ(take(sum.to_int64()), INT64_MIN);
  ASSERT_TRUE(sum.add_int(-1).ok());
  EXPECT_EQ(sum.to_int64().status().code(), ErrorCode::InvalidArgument);
  ASSERT_TRUE(sum.remove_int(-1).ok());
  ASSERT_TRUE(sum.remove_int(INT64_MIN).ok());
  EXPECT_TRUE(sum.empty());
}

TEST(ExactSum, MixedIntegerAndFloatArithmeticSharesTheExactScale) {
  ExactSum sum;
  ASSERT_TRUE(sum.add_int(INT64_MAX).ok());
  ASSERT_TRUE(sum.remove(0x1p63).ok());
  EXPECT_EQ(take(sum.to_int64()), -1);
  ASSERT_TRUE(sum.add(0.5).ok());
  EXPECT_EQ(sum.to_int64().status().code(), ErrorCode::InvalidArgument);
  expect_bits(take(sum.to_double()), -0.5);
}

TEST(ExactSum, NonfiniteInputsFailWithoutChangingState) {
  ExactSum sum;
  ASSERT_TRUE(sum.add_int(123).ok());
  const auto before = take(sum.encode());
  for (double value :
       {std::numeric_limits<double>::infinity(), -std::numeric_limits<double>::infinity(),
        std::numeric_limits<double>::quiet_NaN(),
        std::bit_cast<double>(std::uint64_t{0x7ff0000000000001})}) {
    EXPECT_EQ(sum.add(value).code(), ErrorCode::InvalidArgument);
    EXPECT_EQ(sum.remove(value).code(), ErrorCode::InvalidArgument);
    EXPECT_EQ(take(sum.encode()), before);
  }
}

TEST(ExactSum, ZeroIsCanonicalAfterCancellationAndSignedZeroInputs) {
  ExactSum sum;
  const auto zero = take(sum.encode());
  ASSERT_TRUE(sum.add(-0.0).ok());
  ASSERT_TRUE(sum.remove(0.0).ok());
  ASSERT_TRUE(sum.remove(kTiny).ok());
  ASSERT_TRUE(sum.add(kTiny).ok());
  EXPECT_TRUE(sum.empty());
  expect_bits(take(sum.to_double()), 0.0);
  EXPECT_EQ(take(sum.to_int64()), 0);
  EXPECT_EQ(take(sum.encode()), zero);
}

TEST(ExactSum, EncodingHasStableVersionEndianSignAndMagnitudeBytes) {
  ExactSum sum;
  ASSERT_TRUE(sum.add(-kTiny).ok());
  std::string expected(ExactSum::kEncodedSize, '\0');
  expected.replace(0, 4, "EFS1");
  expected[4] = 1;
  expected[5] = 1;
  expected[8] = 1;
  EXPECT_EQ(take(sum.encode()), expected);
  auto restored = take(ExactSum::decode(expected));
  EXPECT_EQ(take(restored.encode()), expected);
  expect_bits(take(restored.to_double()), -kTiny);
  ASSERT_TRUE(restored.add(kTiny).ok());
  EXPECT_TRUE(restored.empty());

  ASSERT_TRUE(sum.add(kMax).ok());
  ASSERT_TRUE(sum.add_int(INT64_MIN).ok());
  const auto complex = take(sum.encode());
  restored = take(ExactSum::decode(complex));
  EXPECT_EQ(take(restored.encode()), complex);
  ASSERT_TRUE(restored.remove(kMax).ok());
  ASSERT_TRUE(restored.remove_int(INT64_MIN).ok());
  expect_bits(take(restored.to_double()), -kTiny);
}

TEST(ExactSum, CorruptEncodingAndNegativeZeroAreRejected) {
  const auto valid = take(ExactSum{}.encode());
  for (const auto length : {std::size_t{0}, std::size_t{4}, ExactSum::kEncodedSize - 1})
    EXPECT_EQ(ExactSum::decode(valid.substr(0, length)).status().code(),
              ErrorCode::InvalidArgument);
  EXPECT_EQ(ExactSum::decode(valid + '\0').status().code(), ErrorCode::InvalidArgument);
  for (const std::size_t offset : {0u, 1u, 2u, 3u, 4u, 5u, 6u, 7u}) {
    auto corrupt = valid;
    corrupt[offset] = static_cast<char>(0xff);
    EXPECT_EQ(ExactSum::decode(corrupt).status().code(), ErrorCode::InvalidArgument);
  }
  auto negative_zero = valid;
  negative_zero[5] = 1;
  EXPECT_EQ(ExactSum::decode(negative_zero).status().code(), ErrorCode::InvalidArgument);
}

TEST(ExactSum, CapacityOverflowIsAtomicAndRetractionStillWorks) {
  auto bytes = take(ExactSum{}.encode());
  std::fill(bytes.begin() + 8, bytes.end(), static_cast<char>(0xff));
  for (const char sign : {char{0}, char{1}}) {
    bytes[5] = sign;
    auto sum = take(ExactSum::decode(bytes));
    const double value = sign == 0 ? kTiny : -kTiny;
    EXPECT_EQ(sum.add(value).code(), ErrorCode::ResourceExhausted);
    EXPECT_EQ(take(sum.encode()), bytes);
    ASSERT_TRUE(sum.remove(value).ok());
    ASSERT_TRUE(sum.add(value).ok());
    EXPECT_EQ(take(sum.encode()), bytes);
    EXPECT_EQ(sum.to_double().status().code(), ErrorCode::InvalidArgument);
    EXPECT_EQ(sum.to_int64().status().code(), ErrorCode::InvalidArgument);
  }
}

TEST(ExactSum, RandomPermutationAndRetractionsRestoreExactState) {
  std::mt19937_64 random(0xe17f10);
  std::vector<double> values;
  ExactSum forward;
  for (unsigned i = 0; i < 2000; ++i) {
    std::uint64_t bits = random();
    if (((bits >> 52) & 0x7ff) == 0x7ff)
      bits ^= std::uint64_t{1} << 52;
    values.push_back(std::bit_cast<double>(bits));
    ASSERT_TRUE(forward.add(values.back()).ok());
  }
  const auto before = take(forward.encode());
  std::shuffle(values.begin(), values.end(), random);
  ExactSum permuted;
  for (double value : values)
    ASSERT_TRUE(permuted.add(value).ok());
  EXPECT_EQ(take(permuted.encode()), before);
  for (double value : values)
    ASSERT_TRUE(forward.remove(value).ok());
  EXPECT_TRUE(forward.empty());
  EXPECT_EQ(take(forward.encode()), take(ExactSum{}.encode()));
}

TEST(ExactSum, OutputRoundingIsIndependentOfTheFloatingPointEnvironment) {
  ExactSum sum;
  ASSERT_TRUE(sum.add(1.0).ok());
  ASSERT_TRUE(sum.add(0x1p-53).ok());
  const int original = std::fegetround();
  for (const int mode : {FE_DOWNWARD, FE_UPWARD, FE_TOWARDZERO, FE_TONEAREST}) {
    EXPECT_EQ(std::fesetround(mode), 0);
    expect_bits(take(sum.to_double()), 1.0);
  }
  EXPECT_EQ(std::fesetround(original), 0);
}

} // namespace
} // namespace eventflow
