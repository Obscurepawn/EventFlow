#include "eventflow/engine.hpp"
#include "eventflow/key_codec.hpp"
#include "remote_harness.hpp"
#include "status_test_support.hpp"

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <unistd.h>

namespace eventflow {
namespace {
using test::take;

class ReviewDirectory {
public:
  ReviewDirectory() noexcept {
    char path[] = "/tmp/eventflow-engine-review-XXXXXX";
    const auto* created = ::mkdtemp(path);
    if (!created) {
      ADD_FAILURE() << "mkdtemp failed";
      std::abort();
    }
    path_ = created;
  }

  ~ReviewDirectory() noexcept {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  const std::string& path() const noexcept {
    return path_;
  }

private:
  std::string path_;
};

class ReviewStore final : public IStateStore {
public:
  explicit ReviewStore(std::unique_ptr<IStateStore> delegate) noexcept
      : delegate_(std::move(delegate)) {}

  StatusOr<ReadResult> get_many(std::uint32_t shard,
                                const std::vector<std::string>& keys) noexcept override {
    return delegate_->get_many(shard, keys);
  }

  StatusOr<ScanResult> scan(std::uint32_t shard, const std::string& prefix,
                            const std::optional<std::string>& after,
                            std::size_t limit) noexcept override {
    return delegate_->scan(shard, prefix, after, limit);
  }

  StatusOr<CommitResult> commit(const CommitBatch& batch) noexcept override {
    batches.push_back(batch);
    return delegate_->commit(batch);
  }

  StatusOr<StoreLimits> limits() noexcept override {
    return delegate_->limits();
  }

  StatusOr<ShardStatus> status(std::uint32_t shard) noexcept override {
    return delegate_->status(shard);
  }

  std::vector<CommitBatch> batches;

private:
  std::unique_ptr<IStateStore> delegate_;
};

TableChange fact(RelationId relation, std::int64_t id, std::int64_t value) noexcept {
  return {relation, Row{std::string("a"), id}, Row{std::string("a"), id, value}};
}

RowChange emission(RelationId relation, std::int64_t id, std::int64_t value) noexcept {
  return {relation, Row{std::string("a"), id}, std::nullopt, Row{std::string("a"), id, value}};
}

class EngineReview : public testing::TestWithParam<int> {
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
    store_ = std::make_shared<ReviewStore>(std::move(backend));
    catalog_ = take(Catalog::create());
    config_.shard_count = 1;
    config_.page_rows = 2;
  }

  RelationId define(std::string_view sql) noexcept {
    return take(catalog_->define_sql(sql));
  }

  RelationId define_facts() noexcept {
    return define("CREATE TABLE facts (k STRING NOT NULL, id INT64 NOT NULL, v INT64 NOT NULL, "
                  "PRIMARY KEY(k,id)) PARTITION BY(k)");
  }

  RelationId define_requests() noexcept {
    return define("CREATE TABLE request_inputs (k STRING NOT NULL, id INT64 NOT NULL, "
                  "v INT64 NOT NULL, PRIMARY KEY(k,id)) PARTITION BY(k)");
  }

  RelationId define_totals() noexcept {
    return define("CREATE MATERIALIZED VIEW totals AS SELECT k, SUM(v) AS s "
                  "FROM facts GROUP BY k");
  }

  void open() noexcept {
    engine_ = take(Engine::open(store_, take(catalog_->snapshot()), config_));
    store_->batches.clear();
  }

  void apply(const TableChange& change) noexcept {
    auto result = engine_->apply(change);
    ASSERT_TRUE(result.ok()) << result.status().message();
    ASSERT_EQ(result->commit.code, CommitCode::Committed);
  }

  ScanResult durable() noexcept {
    return take(store_->scan(0, take(data_prefix(0)), {}, 10000));
  }

  void expect_atomic_rejection(const StatusOr<DispatchResult>& result,
                               const ScanResult& before) noexcept {
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(engine_->revision(), before.revision);
    const auto after = durable();
    EXPECT_EQ(after.revision, before.revision);
    EXPECT_EQ(after.rows, before.rows);
    EXPECT_TRUE(store_->batches.empty());
  }

  void expect_query(std::string_view sql, std::vector<Row> expected) noexcept {
    auto result = engine_->query_sql(sql);
    ASSERT_TRUE(result.ok()) << result.status().message();
    std::sort(result->rows.begin(), result->rows.end());
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(result->rows, expected);
    EXPECT_EQ(result->revision, engine_->revision());
  }

  void reject_stage(const Mutation& mutation, RelationId facts) noexcept {
    const auto before = durable();
    store_->batches.clear();
    const auto result = engine_->apply(
        fact(facts, 1, 1), {}, [&](RoundContext& round, const std::vector<RowChange>&) noexcept {
          return round.stage(mutation);
        });
    expect_atomic_rejection(result, before);
  }

  ReviewDirectory directory_;
  std::shared_ptr<ReviewStore> store_;
  std::unique_ptr<Catalog> catalog_;
  EngineConfig config_;
  std::unique_ptr<Engine> engine_;
};

TEST_P(EngineReview, ReentrantApplyIsRejectedAndOuterRoundCommitsOneConsistentRevision) {
  const auto facts = define_facts();
  define_totals();
  open();
  const auto revision = engine_->revision();
  std::size_t hooks = 0;
  const auto result = engine_->apply(
      fact(facts, 1, 1), {}, [&](RoundContext& round, const std::vector<RowChange>&) noexcept {
        ++hooks;
        const auto nested = engine_->apply(fact(facts, 2, 2));
        EXPECT_FALSE(nested.ok());
        if (!nested.ok())
          EXPECT_EQ(nested.status().code(), ErrorCode::FailedPrecondition);
        EXPECT_EQ(engine_->revision(), revision);
        EF_ASSIGN_OR_RETURN(auto staged,
                            round.read_relation(facts, Row{std::string("a"), std::int64_t{1}}));
        EXPECT_EQ(staged,
                  (std::optional<Row>(Row{std::string("a"), std::int64_t{1}, std::int64_t{1}})));
        return Status{};
      });
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(result->commit.code, CommitCode::Committed);
  EXPECT_EQ(hooks, 1U);
  ASSERT_EQ(store_->batches.size(), 1U);
  EXPECT_EQ(store_->batches.front().expected_revision, revision);
  EXPECT_EQ(result->commit.revision, revision + 1);
  EXPECT_EQ(engine_->revision(), revision + 1);
  EXPECT_EQ(take(store_->status(0)).revision, revision + 1);
  EXPECT_EQ(result->changes.size(), 2U);
  expect_query("SELECT * FROM facts WHERE k = 'a'",
               {{std::string("a"), std::int64_t{1}, std::int64_t{1}}});
  expect_query("SELECT * FROM totals WHERE k = 'a'", {{std::string("a"), std::int64_t{1}}});
}

TEST_P(EngineReview, ReturningReentrantFailureAbortsTheEntireOuterRound) {
  const auto facts = define_facts();
  define_totals();
  open();
  const auto before = durable();
  const auto result = engine_->apply(fact(facts, 1, 1), {},
                                     [&](RoundContext&, const std::vector<RowChange>&) noexcept {
                                       const auto nested = engine_->apply(fact(facts, 2, 2));
                                       EXPECT_FALSE(nested.ok());
                                       return nested.status();
                                     });
  expect_atomic_rejection(result, before);
}

TEST_P(EngineReview, EveryEngineDataEntryRejectsReentryDuringAHook) {
  const auto facts = define_facts();
  define_totals();
  open();
  const auto before = take(catalog_->snapshot());
  const auto plan = take(catalog_->bind_query_sql("SELECT * FROM facts WHERE k = 'a'"));
  const auto definition = take(parse_sql("CREATE MATERIALIZED VIEW nested_ast AS "
                                         "SELECT k,v FROM facts"));
  std::size_t checked = 0;
  const auto expect_blocked = [&](const auto& operation) noexcept {
    ++checked;
    EXPECT_FALSE(operation.ok());
    if (!operation.ok())
      EXPECT_EQ(operation.status().code(), ErrorCode::FailedPrecondition);
  };
  const auto revision = engine_->revision();
  const auto result = engine_->apply(
      fact(facts, 1, 1), {}, [&](RoundContext&, const std::vector<RowChange>&) noexcept {
        expect_blocked(engine_->apply_many({fact(facts, 2, 2)}));
        expect_blocked(engine_->query(plan));
        expect_blocked(engine_->query_sql("SELECT * FROM facts WHERE k = 'a'"));
        expect_blocked(engine_->install(definition));
        expect_blocked(engine_->install_sql("CREATE MATERIALIZED VIEW nested_sql AS "
                                            "SELECT k,v FROM facts"));
        expect_blocked(engine_->resolve_pending_commit());
        return Status{};
      });
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(result->commit.code, CommitCode::Committed);
  EXPECT_EQ(checked, 6U);
  EXPECT_EQ(engine_->revision(), revision + 1);
  EXPECT_EQ(store_->batches.size(), 1U);
  const auto after = take(engine_->snapshot());
  EXPECT_EQ(after.version, before.version);
  EXPECT_EQ(after.fingerprint, before.fingerprint);
  expect_query("SELECT * FROM totals WHERE k = 'a'", {{std::string("a"), std::int64_t{1}}});
}

TEST_P(EngineReview, EmittedRootWithSharedJoinDescendantsRejectsTheEntireRound) {
  const auto left = define("CREATE TABLE a (k STRING NOT NULL, v INT64 NOT NULL, "
                           "PRIMARY KEY(k)) PARTITION BY(k)");
  const auto right = define("CREATE TABLE b (k STRING NOT NULL, v INT64 NOT NULL, "
                            "PRIMARY KEY(k)) PARTITION BY(k)");
  define("CREATE MATERIALIZED VIEW j AS SELECT a.k, a.v * b.v AS product "
         "FROM a JOIN b ON a.k = b.k");
  define("CREATE MATERIALIZED VIEW d AS SELECT k, product AS p FROM j");
  open();
  apply({left, Row{std::string("a")}, Row{std::string("a"), std::int64_t{1}}});
  apply({right, Row{std::string("a")}, Row{std::string("a"), std::int64_t{4}}});
  expect_query("SELECT * FROM j WHERE k = 'a'", {{std::string("a"), std::int64_t{4}}});
  expect_query("SELECT * FROM d WHERE k = 'a'", {{std::string("a"), std::int64_t{4}}});
  const auto before = durable();
  store_->batches.clear();
  const auto result = engine_->apply(
      {left, Row{std::string("a")}, Row{std::string("a"), std::int64_t{2}}}, {},
      [&](RoundContext& round, const std::vector<RowChange>&) noexcept {
        return round.emit({right, Row{std::string("a")}, Row{std::string("a"), std::int64_t{4}},
                           Row{std::string("a"), std::int64_t{2}}});
      });
  expect_atomic_rejection(result, before);
}

TEST_P(EngineReview, ReemittingAnOriginalRootRejectsEvenWhenTheInputWasANoop) {
  const auto facts = define_facts();
  open();
  apply(fact(facts, 1, 1));
  const auto before = durable();
  store_->batches.clear();
  const auto result = engine_->apply(
      fact(facts, 1, 1), {}, [&](RoundContext& round, const std::vector<RowChange>&) noexcept {
        return round.emit(emission(facts, 2, 2));
      });
  expect_atomic_rejection(result, before);
}

TEST_P(EngineReview, EmittedGraphMustBeDisjointEvenWhenTheOriginalInputWasANoop) {
  const auto left = define("CREATE TABLE a (k STRING NOT NULL, v INT64 NOT NULL, "
                           "PRIMARY KEY(k)) PARTITION BY(k)");
  const auto right = define("CREATE TABLE b (k STRING NOT NULL, v INT64 NOT NULL, "
                            "PRIMARY KEY(k)) PARTITION BY(k)");
  define("CREATE MATERIALIZED VIEW j AS SELECT a.k, a.v * b.v AS product "
         "FROM a JOIN b ON a.k = b.k");
  open();
  const TableChange unchanged{left, Row{std::string("a")}, Row{std::string("a"), std::int64_t{1}}};
  apply(unchanged);
  apply({right, Row{std::string("a")}, Row{std::string("a"), std::int64_t{4}}});
  const auto before = durable();
  store_->batches.clear();
  const auto result = engine_->apply(
      unchanged, {}, [&](RoundContext& round, const std::vector<RowChange>& changes) noexcept {
        EXPECT_TRUE(changes.empty());
        return round.emit({right, Row{std::string("a")}, Row{std::string("a"), std::int64_t{4}},
                           Row{std::string("a"), std::int64_t{2}}});
      });
  expect_atomic_rejection(result, before);
}

TEST_P(EngineReview, IndependentRequestInputsAndDerivedViewsCommitWithCustomState) {
  const auto facts = define_facts();
  define_totals();
  const auto requests = define_requests();
  define("CREATE MATERIALIZED VIEW request_features AS SELECT k,id,v * 2 AS feature "
         "FROM request_inputs");
  open();
  const auto revision = engine_->revision();
  const auto state_key = take(row_key(0, 'S', 900, Row{std::string("hook")}));
  const auto index_key = take(row_key(0, 'I', 901, Row{std::string("hook")}));
  const auto result =
      engine_->apply(fact(facts, 1, 3), {},
                     [&](RoundContext& round, const std::vector<RowChange>& changes) noexcept {
                       EXPECT_EQ(changes.size(), 2U);
                       EF_RETURN_IF_ERROR(round.stage({state_key, std::string("state")}));
                       EF_RETURN_IF_ERROR(round.stage({index_key, std::string("index")}));
                       EF_RETURN_IF_ERROR(round.emit(emission(requests, 7, 3)));
                       EF_ASSIGN_OR_RETURN(auto before, round.read(state_key, StateImage::Before));
                       EF_ASSIGN_OR_RETURN(auto staged, round.read(state_key));
                       EXPECT_FALSE(before);
                       EXPECT_EQ(staged, std::optional<std::string>("state"));
                       return Status{};
                     });
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(result->commit.code, CommitCode::Committed);
  EXPECT_EQ(result->changes.size(), 4U);
  ASSERT_EQ(store_->batches.size(), 1U);
  EXPECT_EQ(store_->batches.front().expected_revision, revision);
  EXPECT_EQ(engine_->revision(), revision + 1);
  expect_query("SELECT * FROM totals WHERE k = 'a'", {{std::string("a"), std::int64_t{3}}});
  expect_query("SELECT * FROM request_inputs WHERE k = 'a'",
               {{std::string("a"), std::int64_t{7}, std::int64_t{3}}});
  expect_query("SELECT * FROM request_features WHERE k = 'a'",
               {{std::string("a"), std::int64_t{7}, std::int64_t{6}}});
  EXPECT_EQ(take(store_->get_many(0, {state_key, index_key})).values,
            (std::vector<std::optional<std::string>>{std::string("state"), std::string("index")}));
}

TEST_P(EngineReview, RawStageCannotOverwriteABaseRelationRow) {
  const auto facts = define_facts();
  define_totals();
  open();
  const auto snapshot = take(catalog_->snapshot());
  const auto* relation = take(snapshot.find(facts));
  const auto key = take(row_key(0, 'T', facts, Row{std::string("a"), std::int64_t{1}}));
  const auto value = take(encode_row(relation->storage_schema,
                                     Row{std::string("a"), std::int64_t{1}, std::int64_t{100}}));
  reject_stage({key, value}, facts);
}

TEST_P(EngineReview, RawStageCannotDeleteAMaterializedViewRow) {
  const auto facts = define_facts();
  const auto totals = define_totals();
  open();
  const auto key = take(row_key(0, 'T', totals, Row{std::string("a")}));
  reject_stage({key, std::nullopt}, facts);
}

TEST_P(EngineReview, RawStageCannotWriteTheEngineMetadataNamespace) {
  const auto facts = define_facts();
  open();
  const auto key = take(row_key(0, 'S', 0, Row{std::string("engine")}));
  reject_stage({key, std::string("corrupted")}, facts);
}

TEST_P(EngineReview, RawStageCannotWriteOtherKeysInReservedStateZero) {
  const auto facts = define_facts();
  open();
  const auto key = take(row_key(0, 'S', 0, Row{std::string("extension")}));
  reject_stage({key, std::string("custom")}, facts);
}

TEST_P(EngineReview, RawStageCannotOverwriteCatalogAggregateState) {
  const auto facts = define_facts();
  const auto totals = define_totals();
  open();
  const auto snapshot = take(catalog_->snapshot());
  const auto* relation = take(snapshot.find(totals));
  const auto key = take(row_key(0, 'S', relation->state_id, Row{std::string("a")}));
  reject_stage({key, std::string("corrupted")}, facts);
}

TEST_P(EngineReview, QueryRejectsAPlanFromAnotherCatalogAtTheSameVersion) {
  const auto prices = define("CREATE TABLE prices (k STRING NOT NULL, price INT64 NOT NULL, "
                             "PRIMARY KEY(k)) PARTITION BY(k)");
  open();
  apply({prices, Row{std::string("a")}, Row{std::string("a"), std::int64_t{123}}});
  auto other = take(Catalog::create());
  take(other->define_sql("CREATE TABLE credits (k STRING NOT NULL, credit INT64 NOT NULL, "
                         "PRIMARY KEY(k)) PARTITION BY(k)"));
  const auto foreign = take(other->bind_query_sql("SELECT credit FROM credits WHERE k = 'a'"));
  ASSERT_EQ(foreign.catalog_version, take(engine_->snapshot()).version);
  const auto before = durable();
  store_->batches.clear();
  const auto result = engine_->query(foreign);
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().code() == ErrorCode::Conflict ||
              result.status().code() == ErrorCode::InvalidArgument);
  EXPECT_EQ(engine_->revision(), before.revision);
  EXPECT_EQ(durable().rows, before.rows);
  EXPECT_TRUE(store_->batches.empty());
  expect_query("SELECT price FROM prices WHERE k = 'a'", {{std::int64_t{123}}});
}

TEST_P(EngineReview, QueryRejectsAnAggregatePlanWhoseRequiredArgumentWasRemoved) {
  const auto facts = define_facts();
  open();
  apply(fact(facts, 1, 3));
  auto plan = take(catalog_->bind_query_sql("SELECT k, COUNT(*) AS n FROM facts "
                                            "WHERE k = 'a' GROUP BY k"));
  ASSERT_EQ(plan.aggregates.size(), 1U);
  ASSERT_FALSE(plan.aggregates.front().argument);
  const auto valid = engine_->query(plan);
  ASSERT_TRUE(valid.ok()) << valid.status().message();
  EXPECT_EQ(valid->rows, (std::vector<Row>{{std::string("a"), std::int64_t{1}}}));
  // COUNT(*) has no argument; changing only its kind creates an invalid executable plan.
  plan.aggregates.front().kind = AggregateKind::Sum;
  const auto before = durable();
  store_->batches.clear();
  const auto result = engine_->query(plan);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), ErrorCode::InvalidArgument);
  EXPECT_EQ(engine_->revision(), before.revision);
  EXPECT_EQ(durable().rows, before.rows);
  EXPECT_TRUE(store_->batches.empty());
  expect_query("SELECT * FROM facts WHERE k = 'a'",
               {{std::string("a"), std::int64_t{1}, std::int64_t{3}}});
}

TEST_P(EngineReview, EmitCountsAgainstTheRowBudgetAndLeavesNoPartialRows) {
  const auto requests = define_requests();
  config_.max_rows = 1;
  open();
  const auto before = durable();
  const auto result =
      engine_->apply_many({}, {}, [&](RoundContext& round, const std::vector<RowChange>&) noexcept {
        EF_RETURN_IF_ERROR(round.emit(emission(requests, 1, 1)));
        return round.emit(emission(requests, 2, 2));
      });
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), ErrorCode::ResourceExhausted);
  expect_atomic_rejection(result, before);
  EXPECT_TRUE(take(store_->scan(0, take(table_prefix(0, 'T', requests)), {}, 10)).rows.empty());
}

TEST_P(EngineReview, RepeatedEmissionOfOneIdentityStillConsumesTheRowBudget) {
  const auto requests = define_requests();
  config_.max_rows = 1;
  open();
  const auto before = durable();
  const auto result =
      engine_->apply_many({}, {}, [&](RoundContext& round, const std::vector<RowChange>&) noexcept {
        EF_RETURN_IF_ERROR(round.emit(emission(requests, 1, 1)));
        return round.emit(emission(requests, 1, 1));
      });
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), ErrorCode::ResourceExhausted);
  expect_atomic_rejection(result, before);
}

TEST_P(EngineReview, OriginalInputsAndHookEmissionsShareTheRowBudget) {
  const auto facts = define_facts();
  const auto requests = define_requests();
  config_.max_rows = 1;
  open();
  const auto before = durable();
  const auto result = engine_->apply(
      fact(facts, 1, 1), {}, [&](RoundContext& round, const std::vector<RowChange>&) noexcept {
        return round.emit(emission(requests, 1, 1));
      });
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), ErrorCode::ResourceExhausted);
  expect_atomic_rejection(result, before);
}

TEST_P(EngineReview, OneEmissionAtTheExactRowLimitCommitsNormally) {
  const auto requests = define_requests();
  config_.max_rows = 1;
  open();
  const auto revision = engine_->revision();
  const auto result =
      engine_->apply_many({}, {}, [&](RoundContext& round, const std::vector<RowChange>&) noexcept {
        return round.emit(emission(requests, 1, 7));
      });
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(result->commit.code, CommitCode::Committed);
  EXPECT_EQ(result->changes.size(), 1U);
  EXPECT_EQ(engine_->revision(), revision + 1);
  EXPECT_EQ(store_->batches.size(), 1U);
  expect_query("SELECT * FROM request_inputs WHERE k = 'a'",
               {{std::string("a"), std::int64_t{1}, std::int64_t{7}}});
}

INSTANTIATE_TEST_SUITE_P(Adapters, EngineReview, testing::Values(0, 1, 2),
                         [](const testing::TestParamInfo<int>& info) noexcept {
                           if (info.param == 0)
                             return std::string("Memory");
                           if (info.param == 1)
                             return std::string("Rocks");
                           return std::string("RemoteMemory");
                         });
} // namespace
} // namespace eventflow
