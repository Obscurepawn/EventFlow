#include "eventflow/observation_credit.hpp"
#include "status_test_support.hpp"
#include <array>
#include <gtest/gtest.h>

namespace eventflow {
namespace {
using test::take;

TEST(ObservationCredit, ReservesNewOwnerBeforePublishingWork) {
  ObservationCreditLimits limits;
  limits.capacity = {2, 100};
  auto ledger = take(ObservationCreditLedger::create(limits));
  std::vector<ObservationCreditEntry> additions = {{"request:r1", {}, {1, 60}}};
  ASSERT_TRUE(ledger.reserve(additions).ok());
  ASSERT_EQ(ledger.entries().size(), 1U);
  EXPECT_EQ(ledger.entries().at("request:r1").reserved_future.bytes, 60U);
}

constexpr std::array<std::size_t ObservationCredit::*, 8> test_axes = {
    &ObservationCredit::events,          &ObservationCredit::bytes,
    &ObservationCredit::read_rows,       &ObservationCredit::read_bytes,
    &ObservationCredit::mutations,       &ObservationCredit::mutation_bytes,
    &ObservationCredit::canonical_bytes, &ObservationCredit::wire_bytes};

ObservationCreditLimits limits_for(ObservationCredit capacity) noexcept {
  ObservationCreditLimits limits;
  limits.capacity = capacity;
  return limits;
}

Status reserve_one(ObservationCreditLedger& ledger, std::string owner,
                   ObservationCredit amount) noexcept {
  std::array<ObservationCreditEntry, 1> entries = {{{std::move(owner), {}, amount}}};
  return ledger.reserve(entries);
}

Status replace_one(ObservationCreditLedger& ledger, const std::string& owner,
                   std::vector<ObservationCreditEntry> entries) noexcept {
  std::array<std::string, 1> consumed = {owner};
  return ledger.rewrite(consumed, entries);
}

TEST(ObservationCredit, EachResourceAxisHasAnExactBoundaryAndRejectsOneMoreAtomically) {
  for (auto axis : test_axes) {
    ObservationCredit capacity, amount;
    capacity.*axis = 100;
    amount.*axis = 100;
    auto ledger = take(ObservationCreditLedger::create(limits_for(capacity)));
    ASSERT_TRUE(reserve_one(ledger, "one", amount).ok());
    const auto before = take(ledger.encode());
    amount.*axis = 1;
    EXPECT_EQ(reserve_one(ledger, "two", amount).code(), ErrorCode::ResourceExhausted);
    EXPECT_EQ(take(ledger.encode()), before);
    EXPECT_EQ(ledger.reserved_future(), capacity);
    EXPECT_EQ(ledger.occupied(), ObservationCredit{});
  }
}

TEST(ObservationCredit, DuplicateReservationsRollbackEveryOwner) {
  auto ledger = take(ObservationCreditLedger::create(limits_for({5, 100})));
  ASSERT_TRUE(reserve_one(ledger, "old", {1, 10}).ok());
  const auto before = take(ledger.encode());
  std::vector<ObservationCreditEntry> additions = {{"new", {}, {1, 10}}, {"old", {}, {1, 10}}};
  EXPECT_EQ(ledger.reserve(additions).code(), ErrorCode::Conflict);
  EXPECT_EQ(take(ledger.encode()), before);
  additions[1].owner = "new";
  EXPECT_EQ(ledger.reserve(additions).code(), ErrorCode::Conflict);
  EXPECT_EQ(take(ledger.encode()), before);
}

TEST(ObservationCredit, CannotCreateOccupiedOrEmptyOwnersWithoutReservedResources) {
  auto ledger = take(ObservationCreditLedger::create(limits_for({5, 100})));
  std::array<ObservationCreditEntry, 1> additions = {{{"new", {1, 10}, {}}}};
  EXPECT_FALSE(ledger.reserve(additions).ok());
  EXPECT_FALSE(reserve_one(ledger, "empty", {}).ok());
  EXPECT_TRUE(ledger.entries().empty());
}

TEST(ObservationCredit, RequestTerminalTransfersCreditUntilOccurrenceAcknowledgement) {
  auto ledger = take(ObservationCreditLedger::create(limits_for({1, 100})));
  ASSERT_TRUE(reserve_one(ledger, "request:A", {1, 100}).ok());
  ASSERT_TRUE(replace_one(ledger, "request:A", {{"occurrence:A", {1, 100}, {}}}).ok());
  EXPECT_FALSE(ledger.entries().contains("request:A"));
  EXPECT_EQ(ledger.occupied(), (ObservationCredit{1, 100}));
  EXPECT_EQ(ledger.reserved_future(), ObservationCredit{});
  EXPECT_EQ(reserve_one(ledger, "request:B", {1, 1}).code(), ErrorCode::ResourceExhausted);
  ASSERT_TRUE(replace_one(ledger, "occurrence:A", {}).ok());
  EXPECT_TRUE(reserve_one(ledger, "request:B", {1, 100}).ok());
}

TEST(ObservationCredit, MultiownerSettlementSplitsOccupiedAndFutureWithoutReacquiringCapacity) {
  auto ledger = take(ObservationCreditLedger::create(limits_for({4, 100})));
  ASSERT_TRUE(reserve_one(ledger, "request", {1, 40}).ok());
  ASSERT_TRUE(reserve_one(ledger, "attempt", {3, 60}).ok());
  std::array<std::string, 2> consumed = {"request", "attempt"};
  std::vector<ObservationCreditEntry> split = {{"occurrence:1", {1, 20}, {1, 20}},
                                               {"occurrence:2", {2, 60}, {}}};
  ASSERT_TRUE(ledger.rewrite(consumed, split).ok());
  EXPECT_EQ(ledger.occupied(), (ObservationCredit{3, 80}));
  EXPECT_EQ(ledger.reserved_future(), (ObservationCredit{1, 20}));
  ASSERT_TRUE(replace_one(ledger, "occurrence:1", {{"child", {1, 30}, {}}}).ok());
  EXPECT_EQ(ledger.occupied(), (ObservationCredit{3, 90}));
  EXPECT_EQ(ledger.reserved_future(), ObservationCredit{});
  EXPECT_TRUE(reserve_one(ledger, "freed", {1, 10}).ok());
}

TEST(ObservationCredit, CannotMintCreditDuringTransferEvenWhenGlobalCapacityIsAvailable) {
  for (auto axis : test_axes) {
    ObservationCredit capacity, reserved, replacement;
    capacity.*axis = 100;
    reserved.*axis = 10;
    replacement.*axis = 11;
    auto ledger = take(ObservationCreditLedger::create(limits_for(capacity)));
    ASSERT_TRUE(reserve_one(ledger, "before", reserved).ok());
    const auto before = take(ledger.encode());
    EXPECT_FALSE(replace_one(ledger, "before", {{"after", replacement, {}}}).ok());
    EXPECT_EQ(take(ledger.encode()), before);
  }
}

TEST(ObservationCredit, CannotConsumeTwiceOverwriteUnrelatedOwnerOrDuplicateReplacement) {
  auto ledger = take(ObservationCreditLedger::create(limits_for({4, 100})));
  ASSERT_TRUE(reserve_one(ledger, "a", {2, 50}).ok());
  ASSERT_TRUE(reserve_one(ledger, "b", {2, 50}).ok());
  const auto before = take(ledger.encode());
  std::array<std::string, 2> duplicate = {"a", "a"};
  EXPECT_FALSE(ledger.rewrite(duplicate, {}).ok());
  EXPECT_FALSE(replace_one(ledger, "a", {{"b", {1, 10}, {}}}).ok());
  EXPECT_FALSE(replace_one(ledger, "a", {{"x", {1, 10}, {}}, {"x", {1, 10}, {}}}).ok());
  EXPECT_FALSE(replace_one(ledger, "missing", {}).ok());
  EXPECT_EQ(take(ledger.encode()), before);
  EXPECT_TRUE(replace_one(ledger, "a", {{"a", {1, 10}, {1, 30}}}).ok());
}

TEST(ObservationCredit, MaximumIntegersCannotOverflowCombinedOccupiedAndReserved) {
  for (const auto axis : test_axes) {
    ObservationCredit capacity, reserved, occupied;
    capacity.*axis = SIZE_MAX;
    reserved.*axis = SIZE_MAX;
    auto limits = limits_for(capacity);
    auto ledger = take(ObservationCreditLedger::create(limits));
    ASSERT_TRUE(reserve_one(ledger, "a", reserved).ok());
    EXPECT_EQ(take(ObservationCreditLedger::from_snapshot(take(ledger.encode()), limits))
                  .reserved_future(),
              capacity);
    occupied.*axis = 1;
    const auto before = take(ledger.encode());
    EXPECT_FALSE(replace_one(ledger, "a", {{"a", occupied, reserved}}).ok());
    EXPECT_FALSE(reserve_one(ledger, "b", occupied).ok());
    EXPECT_EQ(take(ledger.encode()), before);
  }
}

TEST(ObservationCredit, SnapshotUsesCanonicalOwnerOrderAndRestoresTerminalOccupancy) {
  auto limits = limits_for({3, 100});
  auto ledger = take(ObservationCreditLedger::create(limits));
  ASSERT_TRUE(reserve_one(ledger, "z", {1, 30}).ok());
  ASSERT_TRUE(reserve_one(ledger, "a", {2, 70}).ok());
  ASSERT_TRUE(replace_one(ledger, "z", {{"terminal", {1, 30}, {}}}).ok());
  const auto bytes = take(ledger.encode());
  auto restored = take(ObservationCreditLedger::from_snapshot(bytes, limits));
  EXPECT_EQ(take(restored.encode()), bytes);
  EXPECT_EQ(restored.entries(), ledger.entries());
  EXPECT_EQ(restored.occupied(), (ObservationCredit{1, 30}));
  EXPECT_FALSE(reserve_one(restored, "next", {1, 1}).ok());
  auto reordered = take(parse_json_exact(bytes));
  std::swap(reordered["owners"][0], reordered["owners"][1]);
  EXPECT_EQ(take(take(ObservationCreditLedger::from_snapshot(reordered.dump(), limits)).encode()),
            bytes);
}

TEST(ObservationCredit, SnapshotRestorationValidatesCurrentCapacityAndOwnerLimits) {
  auto limits = limits_for({2, 100});
  auto ledger = take(ObservationCreditLedger::create(limits));
  ASSERT_TRUE(reserve_one(ledger, "first", {1, 40}).ok());
  ASSERT_TRUE(reserve_one(ledger, "second", {1, 60}).ok());
  const auto bytes = take(ledger.encode());
  limits.capacity.bytes = 99;
  EXPECT_FALSE(ObservationCreditLedger::from_snapshot(bytes, limits).ok());
  limits.capacity.bytes = 100;
  limits.max_owners = 1;
  EXPECT_FALSE(ObservationCreditLedger::from_snapshot(bytes, limits).ok());
  limits.max_owners = 2;
  limits.max_identifier_bytes = 5;
  EXPECT_FALSE(ObservationCreditLedger::from_snapshot(bytes, limits).ok());
  limits.max_identifier_bytes = 6;
  EXPECT_TRUE(ObservationCreditLedger::from_snapshot(bytes, limits).ok());
}

TEST(ObservationCredit, EncodedByteBoundaryAppliesBeforeParsingAndToAtomicUpdates) {
  auto limits = limits_for({2, 100});
  auto ledger = take(ObservationCreditLedger::create(limits));
  ASSERT_TRUE(reserve_one(ledger, "a", {1, 10}).ok());
  const auto bytes = take(ledger.encode());
  limits.max_encoded_bytes = bytes.size();
  auto exact = take(ObservationCreditLedger::from_snapshot(bytes, limits));
  const auto before = take(exact.encode());
  EXPECT_FALSE(reserve_one(exact, "b", {1, 10}).ok());
  EXPECT_FALSE(replace_one(exact, "a", {{"longer", {1, 10}, {}}}).ok());
  EXPECT_EQ(take(exact.encode()), before);
  limits.max_encoded_bytes = bytes.size() - 1;
  EXPECT_EQ(ObservationCreditLedger::from_snapshot(bytes, limits).status().code(),
            ErrorCode::ResourceExhausted);
}

TEST(ObservationCredit, InvalidBoundsNamesAndZeroCapacityAreExplicit) {
  ObservationCreditLimits limits;
  EXPECT_TRUE(ObservationCreditLedger::create(limits).ok());
  auto zero = take(ObservationCreditLedger::create(limits));
  EXPECT_FALSE(reserve_one(zero, "a", {1}).ok());
  limits.capacity.events = 5;
  limits.max_identifier_bytes = 4;
  auto ledger = take(ObservationCreditLedger::create(limits));
  EXPECT_FALSE(reserve_one(ledger, "", {1}).ok());
  EXPECT_FALSE(reserve_one(ledger, std::string("\xff"), {1}).ok());
  EXPECT_FALSE(reserve_one(ledger, "abcde", {1}).ok());
  EXPECT_TRUE(reserve_one(ledger, "a[\"]", {1}).ok());
  EXPECT_TRUE(ObservationCreditLedger::from_snapshot(take(ledger.encode()), limits).ok());
  for (const auto member :
       {&ObservationCreditLimits::max_owners, &ObservationCreditLimits::max_identifier_bytes,
        &ObservationCreditLimits::max_encoded_bytes}) {
    auto bad = limits;
    bad.*member = 0;
    EXPECT_FALSE(ObservationCreditLedger::create(bad).ok());
  }
  limits.max_encoded_bytes = 1;
  EXPECT_FALSE(ObservationCreditLedger::create(limits).ok());
}

TEST(ObservationCredit, MalformedSnapshotsRejectUnknownDuplicateAndNonIntegerFields) {
  const auto limits = limits_for({2, 100});
  auto ledger = take(ObservationCreditLedger::create(limits));
  ASSERT_TRUE(reserve_one(ledger, "a", {1, 10}).ok());
  const auto bytes = take(ledger.encode());
  const auto good = take(parse_json_exact(bytes));
  std::vector<std::string> bad = {"", "null", "[]", "{}", "{", std::string(17, '[')};
  auto value = good;
  value["extra"] = 1;
  bad.push_back(value.dump());
  value = good;
  value.erase("format");
  bad.push_back(value.dump());
  for (const auto& wrong : std::vector<Json>{-1, 1.0, true, "1", nullptr, Json::array(), 2}) {
    value = good;
    value["format"] = wrong;
    bad.push_back(value.dump());
  }
  for (const auto& wrong : std::vector<Json>{-1, 1.0, true, "1", nullptr, Json::array()}) {
    value = good;
    value["owners"][0]["occupied"][0] = wrong;
    bad.push_back(value.dump());
  }
  value = good;
  value["owners"].push_back(value["owners"][0]);
  bad.push_back(value.dump());
  value = good;
  value["owners"][0]["occupied"].push_back(0);
  bad.push_back(value.dump());
  value = good;
  value["owners"][0]["extra"] = 0;
  bad.push_back(value.dump());
  value = good;
  value["owners"][0]["reserved_future"] = Json::array({0, 0, 0, 0, 0, 0, 0, 0});
  bad.push_back(value.dump());
  bad.push_back("{\"format\":1," + bytes.substr(1));
  auto nested = bytes;
  const auto owner = nested.find("\"owner\":\"a\"");
  ASSERT_NE(owner, std::string::npos);
  nested.insert(owner, "\"owner\":\"b\",");
  bad.push_back(nested);
  auto overflow = bytes;
  const auto number = overflow.find("[1,10");
  ASSERT_NE(number, std::string::npos);
  overflow.replace(number + 1, 1, "18446744073709551616");
  bad.push_back(overflow);
  for (const auto& input : bad) {
    SCOPED_TRACE(input);
    EXPECT_FALSE(ObservationCreditLedger::from_snapshot(input, limits).ok());
  }
}

TEST(ObservationCredit, FailedAtomicMultiownerUpdateNeverPartiallyConsumesFutureCredit) {
  auto ledger = take(ObservationCreditLedger::create(limits_for({3, 100})));
  ASSERT_TRUE(reserve_one(ledger, "a", {1, 30}).ok());
  ASSERT_TRUE(reserve_one(ledger, "b", {2, 70}).ok());
  const auto before = take(ledger.encode());
  std::array<std::string, 2> consumed = {"a", "b"};
  std::vector<ObservationCreditEntry> replacements = {{"ok", {1, 30}, {}},
                                                      {"too_much", {2, 71}, {}}};
  EXPECT_FALSE(ledger.rewrite(consumed, replacements).ok());
  EXPECT_EQ(take(ledger.encode()), before);
}

TEST(ObservationCredit, OwnerCountIncludesSplitResultsAndRetainedFutureReservations) {
  auto limits = limits_for({3, 100});
  limits.max_owners = 1;
  auto ledger = take(ObservationCreditLedger::create(limits));
  ASSERT_TRUE(reserve_one(ledger, "a", {3, 100}).ok());
  const auto before = take(ledger.encode());
  EXPECT_FALSE(replace_one(ledger, "a", {{"one", {1, 20}, {}}, {"two", {}, {2, 80}}}).ok());
  EXPECT_EQ(take(ledger.encode()), before);
  EXPECT_TRUE(replace_one(ledger, "a", {{"same_slot", {1, 20}, {2, 80}}}).ok());
}
} // namespace
} // namespace eventflow
