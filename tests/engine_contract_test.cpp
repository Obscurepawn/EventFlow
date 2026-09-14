#include "eventflow/engine.hpp"
#include "eventflow/key_codec.hpp"
#include "remote_harness.hpp"
#include "status_test_support.hpp"

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <map>
#include <random>
#include <set>
#include <unistd.h>

namespace eventflow {
namespace {
using test::take;

class TemporaryDirectory {
public:
  TemporaryDirectory() noexcept {
    char path[] = "/tmp/eventflow-engine-contract-XXXXXX";
    const auto* created = ::mkdtemp(path);
    if (!created) {
      ADD_FAILURE() << "mkdtemp failed";
      std::abort();
    }
    path_ = created;
  }

  ~TemporaryDirectory() noexcept {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  const std::string& path() const noexcept {
    return path_;
  }

private:
  std::string path_;
};

// Observe the real adapter and inject only explicit boundary outcomes.
class ObservedStore final : public IStateStore {
public:
  explicit ObservedStore(std::shared_ptr<IStateStore> delegate) noexcept
      : delegate_(std::move(delegate)) {}

  StatusOr<ReadResult> get_many(std::uint32_t shard,
                                const std::vector<std::string>& keys) noexcept override {
    ++read_calls;
    if (fail_next_read) {
      fail_next_read = false;
      return Status(ErrorCode::Unavailable, "injected point read failure");
    }
    return delegate_->get_many(shard, keys);
  }

  StatusOr<ScanResult> scan(std::uint32_t shard, const std::string& prefix,
                            const std::optional<std::string>& after,
                            std::size_t limit) noexcept override {
    ++scan_calls;
    EF_ASSIGN_OR_RETURN(auto result, delegate_->scan(shard, prefix, after, limit));
    if (change_revision_on_scan != 0 && scan_calls == change_revision_on_scan) {
      change_revision_on_scan = 0;
      ++result.revision;
    }
    return result;
  }

  StatusOr<CommitResult> commit(const CommitBatch& batch) noexcept override {
    batches.push_back(batch);
    if (reject_next_commit) {
      reject_next_commit = false;
      EF_ASSIGN_OR_RETURN(auto current, delegate_->status(batch.shard));
      return CommitResult{CommitCode::Rejected, current.revision, false,
                          "injected owner/revision rejection"};
    }
    std::vector<std::string> keys;
    for (const auto& mutation : batch.mutations)
      keys.push_back(mutation.key);
    EF_ASSIGN_OR_RETURN(before_commit, delegate_->get_many(batch.shard, keys));
    EF_ASSIGN_OR_RETURN(auto result, delegate_->commit(batch));
    EF_ASSIGN_OR_RETURN(after_commit, delegate_->get_many(batch.shard, keys));
    if (unknown_after_next_commit && result.code == CommitCode::Committed) {
      unknown_after_next_commit = false;
      return CommitResult{CommitCode::Unknown, batch.expected_revision, false,
                          "committed response was lost"};
    }
    return result;
  }

  StatusOr<StoreLimits> limits() noexcept override {
    return delegate_->limits();
  }

  StatusOr<ShardStatus> status(std::uint32_t shard) noexcept override {
    return delegate_->status(shard);
  }

  void reset_observation() noexcept {
    scan_calls = 0;
    read_calls = 0;
    batches.clear();
  }

  std::size_t scan_calls = 0;
  std::size_t read_calls = 0;
  std::vector<CommitBatch> batches;
  ReadResult before_commit;
  ReadResult after_commit;
  bool unknown_after_next_commit = false;
  bool reject_next_commit = false;
  bool fail_next_read = false;
  std::size_t change_revision_on_scan = 0;

private:
  std::shared_ptr<IStateStore> delegate_;
};

using ChangeKey = std::pair<RelationId, std::string>;

struct ReferenceRow {
  Row identity;
  Row stored;
};

using ReferenceRelations = std::map<ChangeKey, ReferenceRow>;
using BaseRows = std::map<std::pair<std::string, std::int64_t>, Row>;

void reference_insert(ReferenceRelations& output, RelationId relation, Row identity, Row visible,
                      bool materialized) noexcept {
  Row stored = visible;
  if (materialized)
    stored.insert(stored.end(), identity.begin(), identity.end());
  auto encoded = take(encode_key_parts(identity));
  output.emplace(ChangeKey{relation, std::move(encoded)},
                 ReferenceRow{std::move(identity), std::move(stored)});
}

std::map<ChangeKey, RowChange> reference_changes(const ReferenceRelations& before,
                                                 const ReferenceRelations& after) noexcept {
  std::map<ChangeKey, RowChange> changes;
  for (const auto& [key, row] : before) {
    const auto found = after.find(key);
    if (found == after.end())
      changes.emplace(key, RowChange{key.first, row.identity, row.stored, std::nullopt});
    else if (row.stored != found->second.stored)
      changes.emplace(key, RowChange{key.first, row.identity, row.stored, found->second.stored});
  }
  for (const auto& [key, row] : after)
    if (!before.contains(key))
      changes.emplace(key, RowChange{key.first, row.identity, std::nullopt, row.stored});
  return changes;
}

void expect_changes(const std::vector<RowChange>& actual, const ReferenceRelations& before,
                    const ReferenceRelations& after) noexcept {
  auto expected = reference_changes(before, after);
  EXPECT_EQ(actual.size(), expected.size());
  std::set<ChangeKey> seen;
  for (const auto& change : actual) {
    const ChangeKey key{change.relation, take(encode_key_parts(change.identity))};
    EXPECT_TRUE(seen.insert(key).second) << "a round must publish one final change per row";
    const auto found = expected.find(key);
    ASSERT_NE(found, expected.end()) << "unexpected relation " << change.relation;
    EXPECT_EQ(change.identity, found->second.identity);
    EXPECT_EQ(change.old_row, found->second.old_row);
    EXPECT_EQ(change.new_row, found->second.new_row);
  }
}

void sort_rows(std::vector<Row>& rows) noexcept {
  std::sort(rows.begin(), rows.end(), [](const Row& a, const Row& b) noexcept { return a < b; });
}

QueryContext scope(const std::string& account) noexcept {
  QueryContext context;
  context.scope = Row{account};
  return context;
}

TableChange fill_change(RelationId table, const std::string& account, std::int64_t id,
                        std::optional<Value> quantity) noexcept {
  return {table, Row{account, id},
          quantity ? std::optional<Row>(Row{account, id, *quantity}) : std::nullopt};
}

class EngineContract : public testing::TestWithParam<int> {
protected:
  void SetUp() noexcept override {
    StoreConfig config;
    config.shard_count = 1;
    std::unique_ptr<IStateStore> backend;
    if (GetParam() == 1)
      backend = take(RocksStateStore::open(directory_.path() + "/db", config));
    else
      backend = take(MemoryStateStore::open(config));
    if (GetParam() == 2)
      backend = take(test::RemoteHarnessStore::open(std::move(backend)));
    store_ = std::make_shared<ObservedStore>(std::shared_ptr<IStateStore>(std::move(backend)));
    catalog_ = take(Catalog::create());
    config_.shard_count = 1;
    config_.shard = 0;
    config_.page_rows = 2;
  }

  RelationId define(std::string_view sql) noexcept {
    return take(catalog_->define_sql(sql));
  }

  RelationId define_fills() noexcept {
    return define("CREATE TABLE fills (account STRING NOT NULL, id INT64 NOT NULL, "
                  "quantity INT64 NULL, PRIMARY KEY(id,account)) PARTITION BY(account)");
  }

  void open() noexcept {
    engine_ = take(Engine::open(store_, take(catalog_->snapshot()), config_));
    store_->reset_observation();
  }

  DispatchResult apply(TableChange change) noexcept {
    auto result = take(engine_->apply(change));
    EXPECT_EQ(result.commit.code, CommitCode::Committed);
    return result;
  }

  QueryResult query(std::string_view sql, const QueryContext& context = {}) noexcept {
    return take(engine_->query_sql(sql, context));
  }

  void expect_query(std::string_view sql, std::vector<Row> expected,
                    const QueryContext& context = {}) noexcept {
    auto actual = query(sql, context);
    sort_rows(actual.rows);
    sort_rows(expected);
    EXPECT_EQ(actual.rows, expected) << sql;
    EXPECT_EQ(actual.revision, engine_->revision());
  }

  void expect_durable(const ReferenceRelations& expected) noexcept {
    for (const auto& [key, row] : expected) {
      const auto snapshot = take(engine_->snapshot());
      const auto* relation = take(snapshot.find(key.first));
      auto data = take(store_->get_many(0, {take(row_key(0, 'T', key.first, row.identity))}));
      ASSERT_EQ(data.values.size(), 1U);
      ASSERT_TRUE(data.values[0]);
      EXPECT_EQ(take(decode_row(relation->storage_schema, *data.values[0])), row.stored);
    }
  }

  TemporaryDirectory directory_;
  std::shared_ptr<ObservedStore> store_;
  std::unique_ptr<Catalog> catalog_;
  EngineConfig config_;
  std::unique_ptr<Engine> engine_;
};

TEST_P(EngineContract, CascadeMatchesIndependentFullReferenceAcrossRandomRetractions) {
  const auto fills = define_fills();
  const auto filtered =
      define("CREATE MATERIALIZED VIEW filtered AS SELECT account, id, quantity FROM fills "
             "WHERE quantity IS NULL OR quantity >= 0");
  const auto projected =
      define("CREATE MATERIALIZED VIEW projected AS SELECT account, quantity FROM filtered");
  const auto totals =
      define("CREATE MATERIALIZED VIEW totals AS SELECT account, SUM(quantity) AS total, "
             "COUNT(*) AS rows_seen, COUNT(quantity) AS nonnull_rows, AVG(quantity) AS mean "
             "FROM projected GROUP BY account");
  const auto features =
      define("CREATE MATERIALIZED VIEW features AS SELECT account, total * 2 AS doubled, "
             "rows_seen, nonnull_rows, mean FROM totals");
  open();
  BaseRows input;
  // This reference recomputes from all base rows, without using engine expressions or state.
  const auto reference = [&]() noexcept {
    ReferenceRelations output;
    struct Group {
      std::int64_t count = 0;
      std::int64_t nonnull = 0;
      std::int64_t sum = 0;
    };
    std::map<std::string, Group> groups;
    for (const auto& [key, row] : input) {
      const Row identity{key.first, key.second};
      reference_insert(output, fills, identity, row, false);
      const auto* value = std::get_if<std::int64_t>(&row[2]);
      if (value && *value < 0)
        continue;
      reference_insert(output, filtered, identity, row, true);
      reference_insert(output, projected, identity, Row{key.first, row[2]}, true);
      auto& group = groups[key.first];
      ++group.count;
      if (value) {
        ++group.nonnull;
        group.sum += *value;
      }
    }
    for (const auto& [account, group] : groups) {
      const Value total = group.nonnull ? Value(group.sum) : Value{};
      const Value mean =
          group.nonnull ? Value(static_cast<double>(group.sum) / static_cast<double>(group.nonnull))
                        : Value{};
      reference_insert(output, totals, Row{account},
                       Row{account, total, group.count, group.nonnull, mean}, true);
      reference_insert(output, features, Row{account},
                       Row{account, group.nonnull ? Value(group.sum * 2) : Value{}, group.count,
                           group.nonnull, mean},
                       true);
    }
    return output;
  };
  std::mt19937 random(0x7e57U);
  ReferenceRelations before;
  for (std::size_t step = 0; step < 60; ++step) {
    SCOPED_TRACE(step);
    const std::string account = "desk-" + std::to_string(random() % 3);
    const std::int64_t id = static_cast<std::int64_t>(random() % 5);
    const auto operation = random() % 5;
    std::optional<Value> value;
    if (operation != 0)
      value = operation == 1 ? Value{} : Value(static_cast<std::int64_t>(random() % 15) - 4);
    const auto change = fill_change(fills, account, id, value);
    if (change.row)
      input[{account, id}] = *change.row;
    else
      input.erase({account, id});
    auto after = reference();
    store_->reset_observation();
    auto result = apply(change);
    EXPECT_EQ(store_->scan_calls, 0U) << "steady-state maintenance must use point reads";
    expect_changes(result.changes, before, after);
    for (const auto& relation : take(engine_->snapshot()).relations) {
      for (int partition = 0; partition < 3; ++partition) {
        const auto key = "desk-" + std::to_string(partition);
        std::vector<Row> expected;
        for (const auto& [identity, row] : after)
          if (identity.first == relation->id && row.identity[0] == Value(key))
            expected.emplace_back(row.stored.begin(), row.stored.begin() + relation->visible_count);
        expect_query("SELECT * FROM " + relation->name, std::move(expected), scope(key));
      }
    }
    before = std::move(after);
  }
  expect_durable(before);
}

TEST_P(EngineContract, HiddenIdentityKeepsDuplicateProjectionRowsAndTheirRetractions) {
  const auto fills = define_fills();
  const auto values = define("CREATE MATERIALIZED VIEW values_only AS SELECT quantity FROM fills");
  open();
  apply(fill_change(fills, "a", 1, Value(std::int64_t{7})));
  auto inserted = apply(fill_change(fills, "a", 2, Value(std::int64_t{7})));
  const auto result = query("SELECT * FROM values_only", scope("a"));
  ASSERT_EQ(result.fields.size(), 1U);
  EXPECT_EQ(result.fields[0].name, "quantity");
  EXPECT_EQ(result.rows, (std::vector<Row>{{std::int64_t{7}}, {std::int64_t{7}}}));
  std::size_t matched = 0;
  for (const auto& change : inserted.changes) {
    if (change.relation != values)
      continue;
    ++matched;
    EXPECT_EQ(change.identity, (Row{std::string("a"), std::int64_t{2}}));
    EXPECT_EQ(change.new_row,
              (std::optional<Row>(Row{std::int64_t{7}, std::string("a"), std::int64_t{2}})));
  }
  EXPECT_EQ(matched, 1U);
  apply(fill_change(fills, "a", 1, std::nullopt));
  expect_query("SELECT * FROM values_only", {{std::int64_t{7}}}, scope("a"));
  EXPECT_TRUE(apply(fill_change(fills, "a", 2, Value(std::int64_t{7}))).changes.empty());
  apply(fill_change(fills, "a", 2, std::nullopt));
  expect_query("SELECT * FROM values_only", {}, scope("a"));
  EXPECT_TRUE(apply(fill_change(fills, "a", 2, std::nullopt)).changes.empty());
}

TEST_P(EngineContract, NullOnlyGroupDisappearsWithItsLastInputAndState) {
  const auto fills = define_fills();
  const auto totals = define(
      "CREATE MATERIALIZED VIEW totals AS SELECT account, SUM(quantity) AS total, "
      "COUNT(*) AS n, COUNT(quantity) AS nn, AVG(quantity) AS mean FROM fills GROUP BY account");
  open();
  apply(fill_change(fills, "a", 1, Value{}));
  expect_query("SELECT * FROM totals WHERE account = 'a'",
               {{std::string("a"), Value{}, std::int64_t{1}, std::int64_t{0}, Value{}}});
  auto result = apply(fill_change(fills, "a", 1, std::nullopt));
  std::size_t deleted = 0;
  for (const auto& change : result.changes)
    if (change.relation == totals) {
      ++deleted;
      EXPECT_TRUE(change.old_row);
      EXPECT_FALSE(change.new_row);
    }
  EXPECT_EQ(deleted, 1U);
  expect_query("SELECT * FROM totals WHERE account = 'a'", {});
  const auto snapshot = take(engine_->snapshot());
  const auto* relation = take(snapshot.find(totals));
  EXPECT_TRUE(
      take(store_->scan(0, take(table_prefix(0, 'S', relation->state_id)), {}, 20)).rows.empty());
  apply(fill_change(fills, "a", 2, Value(std::int64_t{9})));
  expect_query("SELECT * FROM totals WHERE account = 'a'",
               {{std::string("a"), std::int64_t{9}, std::int64_t{1}, std::int64_t{1}, 9.0}});
}

TEST_P(EngineContract, ExactFloatingRetractionSurvivesEngineReopen) {
  const auto fills = define("CREATE TABLE samples (account STRING, id INT64, value FLOAT64, "
                            "PRIMARY KEY(account,id)) PARTITION BY(account)");
  define("CREATE MATERIALIZED VIEW totals AS SELECT account, SUM(value) AS total, "
         "AVG(value) AS mean, COUNT(*) AS n FROM samples GROUP BY account");
  open();
  apply({fills, Row{std::string("a"), std::int64_t{1}},
         Row{std::string("a"), std::int64_t{1}, 1e16}});
  apply(
      {fills, Row{std::string("a"), std::int64_t{2}}, Row{std::string("a"), std::int64_t{2}, 1.0}});
  const auto snapshot = take(engine_->snapshot());
  engine_.reset();
  engine_ = take(Engine::open(store_, snapshot, config_));
  apply({fills, Row{std::string("a"), std::int64_t{1}}, std::nullopt});
  expect_query("SELECT * FROM totals WHERE account = 'a'",
               {{std::string("a"), 1.0, 1.0, std::int64_t{1}}});
}

TEST_P(EngineContract, GroupChecksOnlyTheFinalSumAfterAllChangesInTheRound) {
  const auto fills = define_fills();
  define("CREATE MATERIALIZED VIEW totals AS SELECT account, SUM(quantity) AS total, "
         "COUNT(*) AS n FROM fills GROUP BY account");
  open();
  apply(fill_change(fills, "a", 1, Value(std::numeric_limits<std::int64_t>::max())));
  store_->reset_observation();
  auto replaced = engine_->apply_many({fill_change(fills, "a", 2, Value(std::int64_t{1})),
                                       fill_change(fills, "a", 1, std::nullopt)});
  ASSERT_TRUE(replaced.ok()) << replaced.status().message();
  EXPECT_EQ(replaced->commit.code, CommitCode::Committed);
  EXPECT_FALSE(engine_->failed());
  EXPECT_EQ(store_->scan_calls, 0U);
  EXPECT_EQ(store_->batches.size(), 1U);
  expect_query("SELECT * FROM totals WHERE account = 'a'",
               {{std::string("a"), std::int64_t{1}, std::int64_t{1}}});
}

TEST_P(EngineContract, JoinHandlesBothArrivalOrdersUpdatesDeletesAndReinsertions) {
  const auto left = define("CREATE TABLE positions (account STRING, q INT64, "
                           "PRIMARY KEY(account)) PARTITION BY(account)");
  const auto right = define("CREATE TABLE limits (account STRING, ceiling INT64, "
                            "PRIMARY KEY(account)) PARTITION BY(account)");
  const auto joined =
      define("CREATE MATERIALIZED VIEW active AS SELECT p.account, p.q, l.ceiling "
             "FROM positions p JOIN limits l ON p.account = l.account WHERE p.q > l.ceiling");
  open();
  std::map<std::string, Row> positions;
  std::map<std::string, Row> limits;
  const auto reference = [&]() noexcept {
    ReferenceRelations result;
    for (const auto& [key, row] : positions)
      reference_insert(result, left, Row{key}, row, false);
    for (const auto& [key, row] : limits)
      reference_insert(result, right, Row{key}, row, false);
    for (const auto& [key, row] : positions) {
      const auto other = limits.find(key);
      if (other != limits.end() &&
          std::get<std::int64_t>(row[1]) > std::get<std::int64_t>(other->second[1]))
        reference_insert(result, joined, Row{key}, Row{key, row[1], other->second[1]}, true);
    }
    return result;
  };
  const std::vector<TableChange> changes{
      {left, {std::string("a")}, Row{std::string("a"), std::int64_t{10}}},
      {right, {std::string("a")}, Row{std::string("a"), std::int64_t{3}}},
      {right, {std::string("b")}, Row{std::string("b"), std::int64_t{3}}},
      {left, {std::string("b")}, Row{std::string("b"), std::int64_t{10}}},
      {right, {std::string("a")}, Row{std::string("a"), std::int64_t{12}}},
      {left, {std::string("a")}, Row{std::string("a"), std::int64_t{15}}},
      {left, {std::string("b")}, std::nullopt},
      {left, {std::string("b")}, Row{std::string("b"), std::int64_t{20}}},
      {right, {std::string("a")}, std::nullopt},
      {right, {std::string("a")}, Row{std::string("a"), std::int64_t{1}}}};
  ReferenceRelations before;
  for (const auto& change : changes) {
    auto& relation = change.relation == left ? positions : limits;
    const auto& key = std::get<std::string>(change.identity[0]);
    if (change.row)
      relation[key] = *change.row;
    else
      relation.erase(key);
    const auto after = reference();
    store_->reset_observation();
    const auto result = apply(change);
    EXPECT_EQ(store_->scan_calls, 0U);
    expect_changes(result.changes, before, after);
    before = after;
  }
  expect_query("SELECT * FROM active WHERE account = 'a'",
               {{std::string("a"), std::int64_t{15}, std::int64_t{1}}});
  expect_query("SELECT * FROM active WHERE account = 'b'",
               {{std::string("b"), std::int64_t{20}, std::int64_t{3}}});
}

TEST_P(EngineContract, DiamondPublishesOnlyTheFinalPairOncePerRound) {
  const auto source = define("CREATE TABLE source (account STRING, q INT64, "
                             "PRIMARY KEY(account)) PARTITION BY(account)");
  define("CREATE MATERIALIZED VIEW left_branch AS SELECT account, q + 1 AS x FROM source");
  define("CREATE MATERIALIZED VIEW right_branch AS SELECT account, q * 2 AS y FROM source");
  const auto joined =
      define("CREATE MATERIALIZED VIEW combined AS SELECT l.account, l.x, r.y FROM left_branch l "
             "JOIN right_branch r ON l.account = r.account");
  open();
  apply({source, {std::string("a")}, Row{std::string("a"), std::int64_t{3}}});
  store_->reset_observation();
  auto result = take(
      engine_->apply_many({{source, {std::string("a")}, Row{std::string("a"), std::int64_t{4}}},
                           {source, {std::string("a")}, Row{std::string("a"), std::int64_t{5}}}}));
  EXPECT_EQ(result.commit.code, CommitCode::Committed);
  EXPECT_EQ(store_->scan_calls, 0U);
  EXPECT_EQ(store_->batches.size(), 1U);
  ASSERT_EQ(result.changes.size(), 4U);
  std::size_t matches = 0;
  for (const auto& change : result.changes)
    if (change.relation == joined) {
      ++matches;
      EXPECT_EQ(change.old_row, (std::optional<Row>(Row{std::string("a"), std::int64_t{4},
                                                        std::int64_t{6}, std::string("a")})));
      EXPECT_EQ(change.new_row, (std::optional<Row>(Row{std::string("a"), std::int64_t{6},
                                                        std::int64_t{10}, std::string("a")})));
    }
  EXPECT_EQ(matches, 1U);
  expect_query("SELECT * FROM combined WHERE account = 'a'",
               {{std::string("a"), std::int64_t{6}, std::int64_t{10}}});
}

TEST_P(EngineContract, ScopedQueriesEnforceCaptureBudgetsWithoutTruncation) {
  const auto fills = define_fills();
  open();
  for (std::int64_t id = 0; id < 5; ++id)
    apply(fill_change(fills, "a", id, Value(id)));
  apply(fill_change(fills, "b", 1, Value(std::int64_t{99})));
  const auto revision = engine_->revision();
  auto inferred = query("SELECT quantity FROM fills WHERE 'a' = account AND quantity >= 0");
  ASSERT_EQ(inferred.rows.size(), 5U);
  EXPECT_EQ(inferred.revision, revision);
  auto explicit_scope = query("SELECT quantity FROM fills", scope("a"));
  sort_rows(inferred.rows);
  sort_rows(explicit_scope.rows);
  EXPECT_EQ(explicit_scope.rows, inferred.rows);
  auto missing = engine_->query_sql("SELECT quantity FROM fills");
  ASSERT_FALSE(missing.ok());
  EXPECT_EQ(missing.status().code(), ErrorCode::InvalidArgument);
  auto conflict = engine_->query_sql("SELECT quantity FROM fills WHERE account = 'a'", scope("b"));
  ASSERT_FALSE(conflict.ok());
  EXPECT_EQ(conflict.status().code(), ErrorCode::InvalidArgument);
  auto limited = scope("a");
  limited.max_rows = 4;
  auto rows = engine_->query_sql("SELECT * FROM fills", limited);
  ASSERT_FALSE(rows.ok());
  EXPECT_EQ(rows.status().code(), ErrorCode::ResourceExhausted);
  limited.max_rows = 5;
  EXPECT_EQ(query("SELECT * FROM fills", limited).rows.size(), 5U);
  limited.max_bytes = 1;
  auto bytes = engine_->query_sql("SELECT * FROM fills", limited);
  ASSERT_FALSE(bytes.ok());
  EXPECT_EQ(bytes.status().code(), ErrorCode::ResourceExhausted);
  auto bad_expression = engine_->query_sql("SELECT quantity / 0 AS bad FROM fills", scope("a"));
  ASSERT_FALSE(bad_expression.ok());
  EXPECT_EQ(bad_expression.status().code(), ErrorCode::InvalidArgument);
  EXPECT_FALSE(engine_->failed());
  EXPECT_EQ(engine_->revision(), revision);
  EXPECT_EQ(query("SELECT * FROM fills", scope("a")).rows.size(), 5U);
}

TEST_P(EngineContract, QueryCannotCombinePagesFromDifferentRevisions) {
  const auto fills = define_fills();
  open();
  for (std::int64_t id = 0; id < 3; ++id)
    apply(fill_change(fills, "a", id, Value(id)));
  store_->reset_observation();
  store_->change_revision_on_scan = 2;
  auto result = engine_->query_sql("SELECT * FROM fills", scope("a"));
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(store_->scan_calls, 2U);
}

TEST_P(EngineContract, BaseAggregateCascadeAndHookStateCommitAsOneBatch) {
  const auto fills = define_fills();
  const auto totals = define("CREATE MATERIALIZED VIEW totals AS SELECT account, "
                             "SUM(quantity) AS total FROM fills GROUP BY account");
  const auto cascade =
      define("CREATE MATERIALIZED VIEW doubled AS SELECT account, total * 2 AS value FROM totals");
  open();
  const auto revision = engine_->revision();
  const auto custom_key = take(row_key(0, 'S', 900, Row{std::string("hook")}));
  const auto initial = take(store_->scan(0, take(data_prefix(0)), {}, 10000));
  ASSERT_EQ(initial.revision, revision);
  const std::map<std::string, std::string> baseline(initial.rows.begin(), initial.rows.end());
  std::size_t hook_calls = 0;
  auto dispatched = engine_->apply(
      fill_change(fills, "a", 1, Value(std::int64_t{7})), DispatchContext{1234567},
      [&](RoundContext& round, const std::vector<RowChange>& changes) noexcept {
        ++hook_calls;
        EXPECT_EQ(round.processing_time_us(), 1234567);
        EXPECT_EQ(changes.size(), 3U);
        EF_ASSIGN_OR_RETURN(
            auto before,
            round.read_relation(fills, Row{std::string("a"), std::int64_t{1}}, StateImage::Before));
        EXPECT_FALSE(before);
        EF_ASSIGN_OR_RETURN(auto staged,
                            round.read_relation(fills, Row{std::string("a"), std::int64_t{1}}));
        EXPECT_EQ(staged,
                  (std::optional<Row>(Row{std::string("a"), std::int64_t{1}, std::int64_t{7}})));
        EF_ASSIGN_OR_RETURN(
            auto durable,
            store_->get_many(
                0, {take(row_key(0, 'T', fills, Row{std::string("a"), std::int64_t{1}}))}));
        EXPECT_FALSE(durable.values[0]);
        EF_RETURN_IF_ERROR(round.stage({custom_key, std::string("first")}));
        EF_RETURN_IF_ERROR(round.stage({custom_key, std::string("final")}));
        EF_ASSIGN_OR_RETURN(auto old_custom, round.read(custom_key, StateImage::Before));
        EXPECT_FALSE(old_custom);
        EF_ASSIGN_OR_RETURN(auto custom, round.read(custom_key));
        EXPECT_EQ(custom, std::optional<std::string>("final"));
        return Status{};
      });
  ASSERT_TRUE(dispatched.ok()) << dispatched.status().message();
  EXPECT_EQ(dispatched->commit.code, CommitCode::Committed);
  EXPECT_EQ(hook_calls, 1U);
  ASSERT_EQ(store_->batches.size(), 1U);
  const auto& batch = store_->batches[0];
  EXPECT_EQ(batch.expected_revision, revision);
  EXPECT_EQ(store_->before_commit.revision, revision);
  EXPECT_EQ(store_->after_commit.revision, revision + 1);
  ASSERT_EQ(store_->before_commit.values.size(), batch.mutations.size());
  ASSERT_EQ(store_->after_commit.values.size(), batch.mutations.size());
  std::set<std::string> keys;
  bool aggregate_state = false;
  const auto state_prefix = take(table_prefix(0, 'S', totals));
  for (std::size_t i = 0; i < batch.mutations.size(); ++i) {
    const auto& mutation = batch.mutations[i];
    EXPECT_TRUE(keys.insert(mutation.key).second);
    const auto before = baseline.find(mutation.key);
    const auto expected_before =
        before == baseline.end() ? std::nullopt : std::optional<std::string>(before->second);
    EXPECT_EQ(store_->before_commit.values[i], expected_before);
    EXPECT_EQ(store_->after_commit.values[i], mutation.value);
    aggregate_state |= mutation.key.starts_with(state_prefix);
  }
  EXPECT_TRUE(aggregate_state);
  EXPECT_TRUE(keys.contains(take(row_key(0, 'T', fills, Row{std::string("a"), std::int64_t{1}}))));
  EXPECT_TRUE(keys.contains(take(row_key(0, 'T', totals, Row{std::string("a")}))));
  EXPECT_TRUE(keys.contains(take(row_key(0, 'T', cascade, Row{std::string("a")}))));
  EXPECT_TRUE(keys.contains(custom_key));
}

TEST_P(EngineContract, UnknownCommitBlocksOwnerAndRetriesTheIdenticalBatchOnce) {
  const auto fills = define_fills();
  const auto totals =
      define("CREATE MATERIALIZED VIEW totals AS SELECT account, SUM(quantity) AS total, "
             "COUNT(*) AS n FROM fills GROUP BY account");
  open();
  const auto revision = engine_->revision();
  store_->unknown_after_next_commit = true;
  std::size_t hooks = 0;
  auto result = take(engine_->apply(fill_change(fills, "a", 1, Value(std::int64_t{7})), {},
                                    [&](RoundContext&, const std::vector<RowChange>&) noexcept {
                                      ++hooks;
                                      return Status{};
                                    }));
  EXPECT_EQ(result.commit.code, CommitCode::Unknown);
  EXPECT_TRUE(result.changes.empty()) << "unconfirmed deltas must not be published";
  EXPECT_TRUE(engine_->has_pending_commit());
  EXPECT_EQ(engine_->revision(), revision);
  EXPECT_EQ(take(store_->status(0)).revision, revision + 1);
  ASSERT_EQ(store_->batches.size(), 1U);
  const auto original = take(canonical_batch(store_->batches.front()));
  auto blocked_query = engine_->query_sql("SELECT * FROM totals WHERE account = 'a'");
  ASSERT_FALSE(blocked_query.ok());
  EXPECT_EQ(blocked_query.status().code(), ErrorCode::FailedPrecondition);
  auto blocked_apply = engine_->apply(fill_change(fills, "a", 2, Value(std::int64_t{9})));
  ASSERT_FALSE(blocked_apply.ok());
  EXPECT_EQ(blocked_apply.status().code(), ErrorCode::FailedPrecondition);
  EXPECT_EQ(store_->batches.size(), 1U);
  auto resolved = take(engine_->resolve_pending_commit());
  EXPECT_EQ(resolved.commit.code, CommitCode::Committed);
  EXPECT_EQ(resolved.commit.revision, revision + 1);
  EXPECT_EQ(engine_->revision(), revision + 1);
  ReferenceRelations expected;
  reference_insert(expected, fills, Row{std::string("a"), std::int64_t{1}},
                   Row{std::string("a"), std::int64_t{1}, std::int64_t{7}}, false);
  reference_insert(expected, totals, Row{std::string("a")},
                   Row{std::string("a"), std::int64_t{7}, std::int64_t{1}}, true);
  expect_changes(resolved.changes, {}, expected);
  EXPECT_EQ(take(store_->status(0)).revision, revision + 1);
  EXPECT_FALSE(engine_->has_pending_commit());
  EXPECT_EQ(hooks, 1U);
  for (const auto& batch : store_->batches)
    EXPECT_EQ(take(canonical_batch(batch)), original);
  const auto resolved_batches = store_->batches.size();
  auto repeated = engine_->resolve_pending_commit();
  ASSERT_FALSE(repeated.ok());
  EXPECT_EQ(repeated.status().code(), ErrorCode::FailedPrecondition);
  EXPECT_EQ(store_->batches.size(), resolved_batches);
  EXPECT_EQ(engine_->revision(), revision + 1);
  EXPECT_EQ(hooks, 1U);
  expect_query("SELECT * FROM totals WHERE account = 'a'",
               {{std::string("a"), std::int64_t{7}, std::int64_t{1}}});
  apply(fill_change(fills, "a", 2, Value(std::int64_t{9})));
  expect_query("SELECT * FROM totals WHERE account = 'a'",
               {{std::string("a"), std::int64_t{16}, std::int64_t{2}}});
}

TEST_P(EngineContract, RejectedCommitPublishesNothingAndStopsTheOwner) {
  const auto fills = define_fills();
  const auto totals = define("CREATE MATERIALIZED VIEW totals AS SELECT account, "
                             "SUM(quantity) AS total FROM fills GROUP BY account");
  open();
  const auto revision = engine_->revision();
  store_->reject_next_commit = true;
  auto result = engine_->apply(fill_change(fills, "a", 1, Value(std::int64_t{7})));
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(result->commit.code, CommitCode::Rejected);
  EXPECT_TRUE(result->changes.empty());
  EXPECT_TRUE(engine_->failed());
  EXPECT_EQ(take(store_->status(0)).revision, revision);
  auto raw = take(
      store_->get_many(0, {take(row_key(0, 'T', fills, Row{std::string("a"), std::int64_t{1}})),
                           take(row_key(0, 'T', totals, Row{std::string("a")}))}));
  EXPECT_EQ(raw.values, (std::vector<std::optional<std::string>>{std::nullopt, std::nullopt}));
  EXPECT_FALSE(engine_->query_sql("SELECT * FROM fills", scope("a")).ok());
  EXPECT_FALSE(engine_->apply(fill_change(fills, "a", 2, Value(std::int64_t{8}))).ok());
}

TEST_P(EngineContract, InputValidationRejectsWithoutPoisoningButDispatchArithmeticFailsClosed) {
  const auto fills = define_fills();
  const auto division =
      define("CREATE MATERIALIZED VIEW divisions AS SELECT account, 10 / quantity AS v FROM fills");
  open();
  const auto revision = engine_->revision();
  auto malformed = engine_->apply({fills, Row{std::string("wrong"), std::int64_t{1}},
                                   Row{std::string("a"), std::int64_t{1}, std::int64_t{2}}});
  ASSERT_FALSE(malformed.ok());
  EXPECT_EQ(malformed.status().code(), ErrorCode::InvalidArgument);
  EXPECT_FALSE(engine_->failed());
  EXPECT_EQ(engine_->revision(), revision);
  apply(fill_change(fills, "a", 1, Value(std::int64_t{2})));
  store_->reset_observation();
  const auto committed_revision = engine_->revision();
  auto divide_zero = engine_->apply(fill_change(fills, "a", 1, Value(std::int64_t{0})));
  ASSERT_FALSE(divide_zero.ok());
  EXPECT_EQ(divide_zero.status().code(), ErrorCode::InvalidArgument);
  EXPECT_TRUE(engine_->failed());
  EXPECT_TRUE(store_->batches.empty());
  EXPECT_EQ(take(store_->status(0)).revision, committed_revision);
  auto stored = take(store_->get_many(
      0, {take(row_key(0, 'T', fills, Row{std::string("a"), std::int64_t{1}})),
          take(row_key(0, 'T', division, Row{std::string("a"), std::int64_t{1}}))}));
  const auto snapshot = take(catalog_->snapshot());
  ASSERT_TRUE(stored.values[0]);
  ASSERT_TRUE(stored.values[1]);
  EXPECT_EQ(take(decode_row(take(snapshot.find(fills))->storage_schema, *stored.values[0])),
            (Row{std::string("a"), std::int64_t{1}, std::int64_t{2}}));
  EXPECT_EQ(take(decode_row(take(snapshot.find(division))->storage_schema, *stored.values[1])),
            (Row{std::string("a"), std::int64_t{5}, std::string("a"), std::int64_t{1}}));
}

TEST_P(EngineContract, MutationBudgetAndPointReadFailureLeaveNoPartialState) {
  const auto fills = define_fills();
  const auto totals = define("CREATE MATERIALIZED VIEW totals AS SELECT account, "
                             "SUM(quantity) AS total FROM fills GROUP BY account");
  config_.max_mutations = 2;
  open();
  const auto revision = engine_->revision();
  auto result = engine_->apply(fill_change(fills, "a", 1, Value(std::int64_t{3})));
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), ErrorCode::ResourceExhausted);
  EXPECT_TRUE(store_->batches.empty());
  EXPECT_EQ(take(store_->status(0)).revision, revision);
  auto rows = take(
      store_->get_many(0, {take(row_key(0, 'T', fills, Row{std::string("a"), std::int64_t{1}})),
                           take(row_key(0, 'T', totals, Row{std::string("a")}))}));
  for (const auto& row : rows.values)
    EXPECT_FALSE(row);

  engine_.reset();
  config_.max_mutations = 100;
  engine_ = take(Engine::open(store_, take(catalog_->snapshot()), config_));
  store_->reset_observation();
  store_->fail_next_read = true;
  auto read_failed = engine_->apply(fill_change(fills, "a", 1, Value(std::int64_t{3})));
  ASSERT_FALSE(read_failed.ok());
  EXPECT_EQ(read_failed.status().code(), ErrorCode::Unavailable);
  EXPECT_TRUE(store_->batches.empty());
  EXPECT_EQ(take(store_->status(0)).revision, revision);
}

TEST_P(EngineContract, ExistingFiniteDataInstallsAtomicallyThenMaintainsIncrementally) {
  const auto fills = define_fills();
  open();
  apply(fill_change(fills, "a", 1, Value(std::int64_t{3})));
  apply(fill_change(fills, "a", 2, Value(std::int64_t{5})));
  apply(fill_change(fills, "b", 1, Value{}));
  const auto old_snapshot = take(engine_->snapshot());
  auto old_catalog = take(Catalog::from_snapshot(old_snapshot));
  auto old_plan = take(old_catalog->bind_query_sql("SELECT * FROM fills WHERE account = 'a'"));
  const auto revision = engine_->revision();
  store_->reset_observation();
  auto installed = take(engine_->install_sql(
      "CREATE MATERIALIZED VIEW totals AS SELECT account, SUM(quantity) AS total, "
      "COUNT(*) AS n, AVG(quantity) AS mean FROM fills GROUP BY account"));
  EXPECT_EQ(installed.commit.code, CommitCode::Committed);
  EXPECT_EQ(engine_->revision(), revision + 1);
  EXPECT_EQ(store_->batches.size(), 1U);
  EXPECT_GT(store_->scan_calls, 0U);
  expect_query("SELECT * FROM totals WHERE account = 'a'",
               {{std::string("a"), std::int64_t{8}, std::int64_t{2}, 4.0}});
  expect_query("SELECT * FROM totals WHERE account = 'b'",
               {{std::string("b"), Value{}, std::int64_t{1}, Value{}}});
  auto stale = engine_->query(old_plan);
  ASSERT_FALSE(stale.ok());
  EXPECT_EQ(stale.status().code(), ErrorCode::Conflict);
  store_->reset_observation();
  apply(fill_change(fills, "a", 1, Value(std::int64_t{7})));
  EXPECT_EQ(store_->scan_calls, 0U);
  expect_query("SELECT * FROM totals WHERE account = 'a'",
               {{std::string("a"), std::int64_t{12}, std::int64_t{2}, 6.0}});
  const auto installed_snapshot = take(engine_->snapshot());
  engine_.reset();
  engine_ = take(Engine::open(store_, installed_snapshot, config_));
  apply(fill_change(fills, "a", 2, std::nullopt));
  expect_query("SELECT * FROM totals WHERE account = 'a'",
               {{std::string("a"), std::int64_t{7}, std::int64_t{1}, 7.0}});
}

TEST_P(EngineContract, InitializationBudgetFailureDoesNotActivateOrPersistHalfAView) {
  const auto fills = define_fills();
  config_.max_rows = 4;
  open();
  for (std::int64_t id = 0; id < 8; ++id)
    apply(fill_change(fills, "a", id, Value(id)));
  const auto before = take(engine_->snapshot());
  const auto revision = engine_->revision();
  store_->reset_observation();
  auto installed =
      engine_->install_sql("CREATE MATERIALIZED VIEW copied AS SELECT quantity FROM fills");
  ASSERT_FALSE(installed.ok());
  EXPECT_EQ(installed.status().code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(take(engine_->snapshot()).fingerprint, before.fingerprint);
  EXPECT_EQ(take(store_->status(0)).revision, revision);
  EXPECT_TRUE(store_->batches.empty());
  EXPECT_FALSE(take(engine_->snapshot()).find("copied").ok());
  EXPECT_TRUE(take(store_->scan(0, take(table_prefix(0, 'T', fills + 1)), {}, 20)).rows.empty());
  engine_.reset();
  config_.max_rows = 100;
  engine_ = take(Engine::open(store_, before, config_));
  std::vector<Row> unchanged;
  for (std::int64_t id = 0; id < 8; ++id)
    unchanged.push_back(Row{std::string("a"), id, id});
  expect_query("SELECT * FROM fills", std::move(unchanged), scope("a"));
}

INSTANTIATE_TEST_SUITE_P(Adapters, EngineContract, testing::Values(0, 1, 2),
                         [](const testing::TestParamInfo<int>& info) noexcept {
                           if (info.param == 0)
                             return std::string("Memory");
                           if (info.param == 1)
                             return std::string("Rocks");
                           return std::string("RemoteMemory");
                         });
} // namespace
} // namespace eventflow
