#include "../src/runtime/engine_internal.hpp"
#include "eventflow/key_codec.hpp"
#include "eventflow/request_runtime.hpp"
#include "remote_harness.hpp"
#include "status_test_support.hpp"
#include <filesystem>
#include <gtest/gtest.h>

namespace eventflow {
namespace {
using test::take;

class LatestRequestStore final : public IStateStore {
public:
  explicit LatestRequestStore(std::shared_ptr<IStateStore> store) noexcept
      : backend(std::move(store)) {}

  StatusOr<ReadResult> get_many(std::uint32_t shard,
                                const std::vector<std::string>& keys) noexcept override {
    return backend->get_many(shard, keys);
  }

  StatusOr<ScanResult> scan(std::uint32_t shard, const std::string& prefix,
                            const std::optional<std::string>& after,
                            std::size_t limit) noexcept override {
    return backend->scan(shard, prefix, after, limit);
  }

  StatusOr<StoreLimits> limits() noexcept override {
    return backend->limits();
  }

  StatusOr<ShardStatus> status(std::uint32_t shard) noexcept override {
    return backend->status(shard);
  }

  StatusOr<CommitResult> commit(const CommitBatch& batch) noexcept override {
    batches.push_back(batch);
    EF_ASSIGN_OR_RETURN(auto result, backend->commit(batch));
    if (unknown_next) {
      unknown_next = false;
      return CommitResult{CommitCode::Unknown, batch.expected_revision, false, "lost reply"};
    }
    return result;
  }

  std::shared_ptr<IStateStore> backend;
  std::vector<CommitBatch> batches;
  bool unknown_next = false;
};

struct LatestRequestFixture {
  std::string path;
  std::shared_ptr<LatestRequestStore> store;
  std::shared_ptr<FunctionRegistry> registry;
  std::unique_ptr<Catalog> catalog;
  std::unique_ptr<Engine> engine;
  std::unique_ptr<RequestRuntime> runtime;
  RequestRuntimeConfig config;
  EngineConfig engine_config;
  std::vector<RequestPlanDefinition> plans;
  RelationId candidate = 0;
  RelationId capture = 0;
  RelationId target = 0;
  RelationId result = 0;
  RelationId status = 0;
  RelationId latest = 0;
  RelationId child = 0;
  RelationId other_target = 0;
  bool two_targets = false;
  bool filter_result = false;
  std::size_t captures = 0;
  bool capture_ready = true;

  ~LatestRequestFixture() noexcept {
    runtime.reset();
    engine.reset();
    store.reset();
    if (!path.empty()) {
      std::error_code error;
      std::filesystem::remove_all(path, error);
    }
  }

  Status define() noexcept {
    EF_ASSIGN_OR_RETURN(catalog, Catalog::create());
    EF_ASSIGN_OR_RETURN(registry, FunctionRegistry::create());
    EF_ASSIGN_OR_RETURN(candidate,
                        catalog->define_candidate({"candidate",
                                                   {{1, "k", DataType::String, false}},
                                                   {{2, "text", DataType::String, false}}}));
    DefinitionOptions options;
    options.base_write_role = BaseWriteRole::RequestCapture;
    options.identity_domain = "requests";
    EF_ASSIGN_OR_RETURN(
        capture, catalog->define_sql("CREATE TABLE capture(k STRING,request_id STRING,x INT64 "
                                     "NOT NULL,PRIMARY KEY(k,request_id)) PARTITION BY(k)",
                                     options));
    EF_ASSIGN_OR_RETURN(auto snapshot, catalog->snapshot());
    EF_ASSIGN_OR_RETURN(auto input, snapshot.find(capture));
    const auto fields = input->storage_schema.fields;
    FunctionViewDefinition function;
    function.name = "computed";
    function.function_id = "twice";
    function.function_version = 1;
    function.implementation_identity = "builtin.twice.v1";
    function.inputs = {{"value", capture, true, fields}};
    function.output_fields = {{1, "answer", DataType::Int64, false}};
    EF_ASSIGN_OR_RETURN(target, catalog->define_function(function));
    EF_RETURN_IF_ERROR(registry->add(
        {{"twice", 1, "builtin.twice.v1", {{"value", true, fields}}, function.output_fields, true},
         [](const FunctionInputs& inputs) noexcept -> StatusOr<Row> {
           return Row{std::get<std::int64_t>((*inputs[0])[2]) * 2};
         }}));
    if (two_targets) {
      function.name = "other_computed";
      EF_ASSIGN_OR_RETURN(other_target, catalog->define_function(function));
    }
    EF_ASSIGN_OR_RETURN(
        result,
        catalog->define_sql(filter_result ? "CREATE MATERIALIZED VIEW result AS SELECT answer FROM "
                                            "computed WHERE answer > 100"
                                          : "CREATE MATERIALIZED VIEW result AS SELECT answer FROM "
                                            "computed"));
    EF_ASSIGN_OR_RETURN(status,
                        catalog->define_request_status(
                            {"request_meta", "requests", input->identity_fields, candidate}));
    EF_ASSIGN_OR_RETURN(latest, catalog->define_latest({"latest", result, status, candidate}));
    EF_ASSIGN_OR_RETURN(
        child, catalog->define_sql("CREATE MATERIALIZED VIEW plus_one AS SELECT answer+1 AS value "
                                   "FROM latest"));
    RequestPlanDefinition plan;
    plan.name = "prediction";
    plan.targets = {result};
    if (two_targets) {
      plan.targets.push_back(other_target);
      plan.machine.max_attempts = 1;
    }
    plan.request_status_relation = status;
    plan.capture = [this](RoundContext&,
                          const RequestAdmission& admission) noexcept -> StatusOr<RequestCapture> {
      ++captures;
      if (!capture_ready)
        return RequestCapture{false, {}};
      Row identity = admission.scope;
      identity.emplace_back(admission.request_id);
      Row row = identity;
      row.emplace_back(std::get<std::int64_t>(admission.arguments[0]));
      return RequestCapture{true, {{capture, std::move(identity), std::move(row)}}};
    };
    plans = {std::move(plan)};
    engine_config.shard_count = 1;
    return {};
  }

  Status start(int backend_kind = 0) noexcept {
    StoreConfig storage;
    storage.shard_count = 1;
    std::unique_ptr<IStateStore> backend;
    if (backend_kind % 2 == 0) {
      EF_ASSIGN_OR_RETURN(backend, MemoryStateStore::open(storage));
    } else {
      char pattern[] = "/tmp/eventflow-request-latest-XXXXXX";
      const auto* created = ::mkdtemp(pattern);
      if (!created)
        return Status(ErrorCode::Unavailable, "temporary directory unavailable");
      path = created;
      EF_ASSIGN_OR_RETURN(backend, RocksStateStore::open(path + "/db", storage));
    }
    if (backend_kind >= 2) {
      EF_ASSIGN_OR_RETURN(backend, test::RemoteHarnessStore::open(std::move(backend)));
    }
    store = std::make_shared<LatestRequestStore>(std::shared_ptr<IStateStore>(std::move(backend)));
    EF_ASSIGN_OR_RETURN(engine, EngineRuntimeAccess::open(store, take(catalog->snapshot()),
                                                          engine_config, registry));
    return {};
  }

  Status open(int backend_kind = 0) noexcept {
    EF_RETURN_IF_ERROR(define());
    EF_RETURN_IF_ERROR(start(backend_kind));
    EF_RETURN_IF_ERROR(activate());
    EF_ASSIGN_OR_RETURN(runtime, RequestRuntime::open(*engine, store, registry, plans, config));
    return {};
  }

  Status activate(std::int64_t expected_epoch = 0, const std::string& scope = "a") noexcept {
    EF_ASSIGN_OR_RETURN(
        auto committed,
        EngineRuntimeAccess::apply(
            *engine, {EngineRuntimeAccess::time_us(*engine)},
            [&](RoundContext& context) noexcept -> StatusOr<std::vector<TableChange>> {
              EF_ASSIGN_OR_RETURN(auto row,
                                  EngineRuntimeAccess::make_candidate_write(
                                      context, candidate, {scope}, CandidateOperation::Activate,
                                      expected_epoch, {std::string("news")}));
              return std::vector<TableChange>{std::move(row)};
            }));
    return check(committed.commit.code == CommitCode::Committed, "candidate activation failed");
  }

  RequestAdmission admission(const std::string& id = "r1", std::int64_t value = 7,
                             std::uint64_t epoch = 1,
                             const std::string& scope = "a") const noexcept {
    return {"prediction", {scope}, id, {}, epoch, 0, {value}};
  }

  Status reopen() noexcept {
    runtime.reset();
    engine.reset();
    EF_ASSIGN_OR_RETURN(engine, EngineRuntimeAccess::open(store, take(catalog->snapshot()),
                                                          engine_config, registry));
    EF_ASSIGN_OR_RETURN(runtime, RequestRuntime::open(*engine, store, registry, plans, config));
    return {};
  }

  FunctionCompletion run(const FunctionTask& task, std::int64_t now = 3) noexcept {
    auto executor = take(SimpleFunctionExecutor::create(registry));
    EXPECT_TRUE(executor->submit(task).ok());
    EXPECT_EQ(take(executor->run_ready(now)), 1U);
    auto results = take(executor->poll(1));
    EXPECT_EQ(results.size(), 1U);
    return results.empty() ? FunctionCompletion{} : std::move(results[0]);
  }

  std::vector<Row> query(const std::string& sql) noexcept {
    QueryContext context;
    context.scope = Row{std::string("a")};
    return take(engine->query_sql(sql, context)).rows;
  }
};

class RequestLatestContract : public testing::TestWithParam<int> {};

TEST_P(RequestLatestContract, AdmissionClaimAndCompletionPublishWholeRequestStatus) {
  LatestRequestFixture f;
  ASSERT_TRUE(f.open(GetParam()).ok());
  auto admitted = take(f.runtime->admit(f.admission(), 1));
  ASSERT_TRUE(admitted.record);
  EXPECT_EQ(f.query("SELECT candidate_epoch,admission_seq,phase FROM request_meta WHERE k='a'"),
            (std::vector<Row>{{std::int64_t{1}, std::int64_t{1}, std::string("ACCEPTED")}}));
  EXPECT_TRUE(f.query("SELECT answer FROM latest").empty());
  auto claimed = take(f.runtime->claim({std::string("a")}, "r1", 2));
  ASSERT_EQ(claimed.tasks.size(), 1U);
  EXPECT_EQ(f.query("SELECT phase FROM request_meta WHERE k='a'"),
            (std::vector<Row>{{std::string("RUNNING")}}));
  auto completed = take(f.runtime->complete({std::string("a")}, "r1", f.run(claimed.tasks[0]), 3));
  ASSERT_TRUE(completed.record);
  EXPECT_EQ(completed.record->state.phase, RequestPhase::Success);
  EXPECT_EQ(f.query("SELECT phase FROM request_meta WHERE k='a'"),
            (std::vector<Row>{{std::string("SUCCEEDED")}}));
  EXPECT_EQ(f.query("SELECT answer,__ef_latest_admission_seq FROM latest"),
            (std::vector<Row>{{std::int64_t{14}, std::int64_t{1}}}));
  EXPECT_EQ(f.query("SELECT value FROM plus_one"), (std::vector<Row>{{std::int64_t{15}}}));
}

TEST_P(RequestLatestContract, AdmissionOrderWinsAcrossReverseCompletionAndLaterCancellation) {
  LatestRequestFixture f;
  ASSERT_TRUE(f.open(GetParam()).ok());
  take(f.runtime->admit(f.admission("old", 1), 1));
  take(f.runtime->admit(f.admission("new", 2), 2));
  auto old = take(f.runtime->claim({std::string("a")}, "old", 3));
  auto newer = take(f.runtime->claim({std::string("a")}, "new", 4));
  take(f.runtime->complete({std::string("a")}, "new", f.run(newer.tasks[0], 5), 5));
  take(f.runtime->complete({std::string("a")}, "old", f.run(old.tasks[0], 6), 6));
  EXPECT_EQ(f.query("SELECT answer,__ef_latest_admission_seq FROM latest"),
            (std::vector<Row>{{std::int64_t{4}, std::int64_t{2}}}));
  take(f.runtime->admit(f.admission("cancelled", 3), 7));
  take(f.runtime->cancel({std::string("a")}, "cancelled", 8));
  EXPECT_EQ(f.query("SELECT phase FROM request_meta WHERE request_id='cancelled'"),
            (std::vector<Row>{{std::string("CANCELLED")}}));
  EXPECT_EQ(f.query("SELECT answer,__ef_latest_admission_seq FROM latest"),
            (std::vector<Row>{{std::int64_t{4}, std::int64_t{2}}}));
  EXPECT_EQ(f.query("SELECT answer FROM computed").size(), 2U);
}

TEST_P(RequestLatestContract, OneSuccessfulFunctionCannotPublishLatestForAFailedWholeRequest) {
  LatestRequestFixture f;
  f.two_targets = true;
  ASSERT_TRUE(f.open(GetParam()).ok());
  take(f.runtime->admit(f.admission(), 1));
  auto claimed = take(f.runtime->claim({std::string("a")}, "r1", 2));
  ASSERT_EQ(claimed.tasks.size(), 2U);
  take(f.runtime->complete({std::string("a")}, "r1", f.run(claimed.tasks[0]), 3));
  EXPECT_EQ(f.query("SELECT answer FROM result"), (std::vector<Row>{{std::int64_t{14}}}));
  EXPECT_TRUE(f.query("SELECT answer FROM latest").empty());
  EXPECT_EQ(f.query("SELECT phase FROM request_meta"),
            (std::vector<Row>{{std::string("RUNNING")}}));
  auto failure = f.run(claimed.tasks[1], 4);
  failure.output.clear();
  failure.status = {ErrorCode::Internal, "deterministic failure"};
  auto completed = take(f.runtime->complete({std::string("a")}, "r1", failure, 4));
  EXPECT_EQ(completed.record->state.phase, RequestPhase::Failed);
  EXPECT_EQ(f.query("SELECT phase FROM request_meta"), (std::vector<Row>{{std::string("FAILED")}}));
  EXPECT_TRUE(f.query("SELECT answer FROM latest").empty());
}

TEST_P(RequestLatestContract, ReplacementAllowsOldTerminalHistoryWithoutReactivatingOldLatest) {
  LatestRequestFixture f;
  ASSERT_TRUE(f.open(GetParam()).ok());
  take(f.runtime->admit(f.admission("old", 1), 1));
  auto old = take(f.runtime->claim({std::string("a")}, "old", 2));
  ASSERT_TRUE(f.activate(1).ok());
  take(f.runtime->admit(f.admission("current", 2, 2), 3));
  auto current = take(f.runtime->claim({std::string("a")}, "current", 4));
  take(f.runtime->complete({std::string("a")}, "current", f.run(current.tasks[0], 5), 5));
  ASSERT_TRUE(f.reopen().ok());
  ASSERT_EQ(take(f.runtime->pending_tasks(4)).size(), 1U);
  auto completed = take(f.runtime->complete({std::string("a")}, "old", f.run(old.tasks[0], 6), 6));
  EXPECT_EQ(completed.record->state.phase, RequestPhase::Success);
  EXPECT_EQ(f.query("SELECT candidate_epoch,phase FROM request_meta WHERE request_id='old'"),
            (std::vector<Row>{{std::int64_t{1}, std::string("SUCCEEDED")}}));
  EXPECT_EQ(
      f.query("SELECT answer,__ef_latest_candidate_epoch,__ef_latest_admission_seq FROM latest"),
      (std::vector<Row>{{std::int64_t{4}, std::int64_t{2}, std::int64_t{2}}}));
  ASSERT_TRUE(f.reopen().ok());
  EXPECT_EQ(take(f.runtime->read({std::string("a")}, "old")).state.phase, RequestPhase::Success);
  EXPECT_TRUE(take(f.runtime->active_requests()).empty());
  EXPECT_EQ(f.query("SELECT answer FROM latest"), (std::vector<Row>{{std::int64_t{4}}}));
}

TEST_P(RequestLatestContract, UnknownAdmissionClaimCompletionResolveTheOriginalMetadataBatch) {
  LatestRequestFixture f;
  ASSERT_TRUE(f.open(GetParam()).ok());
  std::size_t companions = 0;
  RequestCommitHook companion =
      [&](RoundContext&, const RequestRecord&) noexcept -> StatusOr<std::vector<TableChange>> {
    ++companions;
    return std::vector<TableChange>{};
  };
  f.store->unknown_next = true;
  auto uncertain = take(f.runtime->admit(f.admission(), 1, companion));
  EXPECT_EQ(uncertain.commit.code, CommitCode::Unknown);
  EXPECT_FALSE(uncertain.record);
  EXPECT_EQ(f.captures, 1U);
  const auto admission_batch = f.store->batches.back();
  auto admitted = take(f.runtime->resolve_pending_commit());
  EXPECT_TRUE(admitted.accepted);
  EXPECT_EQ(companions, 1U);
  EXPECT_EQ(f.captures, 1U);
  EXPECT_EQ(f.store->batches.back().commit_id, admission_batch.commit_id);
  ASSERT_EQ(f.store->batches.back().mutations.size(), admission_batch.mutations.size());
  for (std::size_t i = 0; i < admission_batch.mutations.size(); ++i) {
    EXPECT_EQ(f.store->batches.back().mutations[i].key, admission_batch.mutations[i].key);
    EXPECT_EQ(f.store->batches.back().mutations[i].value, admission_batch.mutations[i].value);
  }
  f.store->unknown_next = true;
  auto claim = take(f.runtime->claim({std::string("a")}, "r1", 2, 16, companion));
  EXPECT_EQ(claim.commit.code, CommitCode::Unknown);
  EXPECT_TRUE(claim.tasks.empty());
  auto claimed = take(f.runtime->resolve_pending_commit());
  ASSERT_EQ(claimed.tasks.size(), 1U);
  EXPECT_EQ(companions, 2U);
  auto completion = f.run(claimed.tasks[0]);
  f.store->unknown_next = true;
  auto complete = take(f.runtime->complete({std::string("a")}, "r1", completion, 3, companion));
  EXPECT_EQ(complete.commit.code, CommitCode::Unknown);
  EXPECT_FALSE(complete.record);
  auto completed = take(f.runtime->resolve_pending_commit());
  ASSERT_TRUE(completed.record);
  EXPECT_EQ(completed.record->state.phase, RequestPhase::Success);
  EXPECT_EQ(companions, 3U);
  EXPECT_EQ(f.query("SELECT answer FROM latest"), (std::vector<Row>{{std::int64_t{14}}}));
  const auto revision = f.engine->revision();
  EXPECT_TRUE(
      take(f.runtime->complete({std::string("a")}, "r1", completion, 4, companion)).duplicate);
  EXPECT_TRUE(take(f.runtime->admit(f.admission(), 4, companion)).duplicate);
  EXPECT_EQ(f.engine->revision(), revision);
  EXPECT_EQ(companions, 3U);
}

TEST_P(RequestLatestContract, ForgedCompanionStatusRollsBackCaptureAndDoesNotConsumeSequence) {
  LatestRequestFixture f;
  ASSERT_TRUE(f.open(GetParam()).ok());
  const auto revision = f.engine->revision();
  auto rejected = f.runtime->admit(
      f.admission(), 1,
      [&](RoundContext&,
          const RequestRecord& record) noexcept -> StatusOr<std::vector<TableChange>> {
        return std::vector<TableChange>{{f.status, record.state.manifest.identity,
                                         Row{std::string("a"), std::string("r1"), std::int64_t{1},
                                             std::int64_t{1}, std::string("SUCCEEDED")}}};
      });
  EXPECT_FALSE(rejected.ok());
  EXPECT_EQ(f.engine->revision(), revision);
  EXPECT_FALSE(f.engine->failed());
  EXPECT_TRUE(take(f.runtime->active_requests()).empty());
  EXPECT_TRUE(f.query("SELECT x FROM capture").empty());
  EXPECT_TRUE(f.query("SELECT phase FROM request_meta").empty());
  auto admitted = take(f.runtime->admit(f.admission(), 1));
  EXPECT_EQ(admitted.record->state.manifest.admission_seq, 1U);
}

TEST_P(RequestLatestContract, InvalidEpochAndStaleCandidateDoNotCaptureOrConsumeSequence) {
  LatestRequestFixture f;
  ASSERT_TRUE(f.open(GetParam()).ok());
  const auto revision = f.engine->revision();
  EXPECT_EQ(f.runtime->admit(f.admission("zero", 1, 0), 1).status().code(),
            ErrorCode::ResourceExhausted);
  EXPECT_EQ(
      f.runtime->admit(f.admission("overflow", 1, std::uint64_t{INT64_MAX} + 1), 1).status().code(),
      ErrorCode::ResourceExhausted);
  EXPECT_EQ(f.runtime->admit(f.admission("future", 1, 2), 1).status().code(),
            ErrorCode::FailedPrecondition);
  EXPECT_EQ(f.runtime->admit(f.admission("missing", 1, 1, "b"), 1).status().code(),
            ErrorCode::FailedPrecondition);
  EXPECT_EQ(f.captures, 0U);
  EXPECT_EQ(f.engine->revision(), revision);
  auto admitted = take(f.runtime->admit(f.admission(), 1));
  EXPECT_EQ(admitted.record->state.manifest.admission_seq, 1U);
}

TEST_P(RequestLatestContract, DeferredCaptureDoesNotPublishStatusOrConsumeSequence) {
  LatestRequestFixture f;
  ASSERT_TRUE(f.open(GetParam()).ok());
  const auto revision = f.engine->revision();
  f.capture_ready = false;
  auto deferred = take(f.runtime->admit(f.admission(), 1));
  EXPECT_TRUE(deferred.deferred);
  EXPECT_EQ(f.engine->revision(), revision);
  EXPECT_TRUE(f.query("SELECT phase FROM request_meta").empty());
  f.capture_ready = true;
  EXPECT_EQ(take(f.runtime->admit(f.admission(), 1)).record->state.manifest.admission_seq, 1U);
}

TEST_P(RequestLatestContract, WorkerPayloadCannotForgeIdentityOrWholeRequestPhase) {
  LatestRequestFixture f;
  ASSERT_TRUE(f.open(GetParam()).ok());
  take(f.runtime->admit(f.admission(), 1));
  auto claim = take(f.runtime->claim({std::string("a")}, "r1", 2));
  auto completion = f.run(claim.tasks[0]);
  const auto revision = f.engine->revision();
  ++completion.function_version;
  EXPECT_TRUE(take(f.runtime->complete({std::string("a")}, "r1", completion, 3)).duplicate);
  EXPECT_EQ(f.engine->revision(), revision);
  --completion.function_version;
  completion.output = {std::string("other"), std::string("forged"), std::int64_t{999},
                       std::string("SUCCEEDED")};
  EXPECT_FALSE(f.runtime->complete({std::string("a")}, "r1", completion, 3).ok());
  EXPECT_EQ(f.engine->revision(), revision);
  EXPECT_EQ(f.query("SELECT request_id,phase FROM request_meta"),
            (std::vector<Row>{{std::string("r1"), std::string("RUNNING")}}));
  EXPECT_TRUE(f.query("SELECT answer FROM latest").empty());
  take(f.runtime->complete({std::string("a")}, "r1", f.run(claim.tasks[0]), 3));
  EXPECT_EQ(f.query("SELECT k,request_id,answer FROM computed"),
            (std::vector<Row>{{std::string("a"), std::string("r1"), std::int64_t{14}}}));
}

TEST_P(RequestLatestContract, TimedOutRequestPublishesFailedAndLateCompletionIsIgnored) {
  LatestRequestFixture f;
  ASSERT_TRUE(f.open(GetParam()).ok());
  auto admission = f.admission();
  admission.lifetime_us = 5;
  take(f.runtime->admit(admission, 1));
  auto claim = take(f.runtime->claim({std::string("a")}, "r1", 2));
  auto timed_out = take(f.runtime->advance({std::string("a")}, "r1", 6));
  EXPECT_EQ(timed_out.record->state.phase, RequestPhase::Failed);
  EXPECT_EQ(f.query("SELECT phase FROM request_meta"), (std::vector<Row>{{std::string("FAILED")}}));
  const auto revision = f.engine->revision();
  EXPECT_TRUE(
      take(f.runtime->complete({std::string("a")}, "r1", f.run(claim.tasks[0], 7), 7)).duplicate);
  EXPECT_EQ(f.engine->revision(), revision);
  EXPECT_TRUE(f.query("SELECT answer FROM latest").empty());
}

TEST_P(RequestLatestContract, SuccessfulRequestWithoutSourceRowsDoesNotInventLatest) {
  LatestRequestFixture f;
  f.filter_result = true;
  ASSERT_TRUE(f.open(GetParam()).ok());
  take(f.runtime->admit(f.admission(), 1));
  auto claimed = take(f.runtime->claim({std::string("a")}, "r1", 2));
  auto completed = take(f.runtime->complete({std::string("a")}, "r1", f.run(claimed.tasks[0]), 3));
  EXPECT_EQ(completed.record->state.phase, RequestPhase::Success);
  EXPECT_EQ(f.query("SELECT phase FROM request_meta"),
            (std::vector<Row>{{std::string("SUCCEEDED")}}));
  EXPECT_TRUE(f.query("SELECT answer FROM result").empty());
  EXPECT_TRUE(f.query("SELECT answer FROM latest").empty());
  EXPECT_TRUE(take(f.runtime->active_requests()).empty());
}

TEST_P(RequestLatestContract, UnknownCancellationConfirmsStatusBeforeReleasingCapacity) {
  LatestRequestFixture f;
  f.config.max_active_requests = 1;
  ASSERT_TRUE(f.open(GetParam()).ok());
  take(f.runtime->admit(f.admission(), 1));
  auto claimed = take(f.runtime->claim({std::string("a")}, "r1", 2));
  f.store->unknown_next = true;
  auto uncertain = take(f.runtime->cancel({std::string("a")}, "r1", 3));
  EXPECT_EQ(uncertain.commit.code, CommitCode::Unknown);
  EXPECT_FALSE(uncertain.record);
  EXPECT_TRUE(uncertain.cancel_task_ids.empty());
  EXPECT_EQ(f.runtime->admit(f.admission("next"), 4).status().code(),
            ErrorCode::FailedPrecondition);
  auto confirmed = take(f.runtime->resolve_pending_commit());
  EXPECT_EQ(confirmed.record->state.phase, RequestPhase::Cancelled);
  EXPECT_EQ(confirmed.cancel_task_ids, std::vector<std::string>{claimed.tasks[0].task_id});
  EXPECT_EQ(f.query("SELECT phase FROM request_meta"),
            (std::vector<Row>{{std::string("CANCELLED")}}));
  ASSERT_TRUE(f.reopen().ok());
  EXPECT_TRUE(
      take(f.runtime->complete({std::string("a")}, "r1", f.run(claimed.tasks[0], 4), 4)).duplicate);
  EXPECT_TRUE(take(f.runtime->admit(f.admission("next"), 4)).accepted);
  EXPECT_TRUE(f.query("SELECT answer FROM latest").empty());
}

TEST(RequestLatest, LatestDomainRequiresItsBoundStatusAndExactIdentityFieldNames) {
  for (int variant = 0; variant < 4; ++variant) {
    SCOPED_TRACE(variant);
    LatestRequestFixture f;
    ASSERT_TRUE(f.define().ok());
    if (variant == 0) {
      f.plans[0].request_status_relation.reset();
    } else if (variant == 1) {
      f.plans[0].request_status_relation = f.candidate;
    } else {
      const auto scope_name = variant == 2 ? "account" : "k";
      const auto domain = variant == 2 ? "requests" : "other_requests";
      auto candidate = take(f.catalog->define_candidate(
          {"other_candidate", {{1, scope_name, DataType::String, false}}, {}}));
      auto status = f.catalog->define_request_status(
          {"other_meta",
           domain,
           {{1, scope_name, DataType::String, false}, {2, "request_id", DataType::String, false}},
           candidate});
      if (variant == 2) {
        EXPECT_EQ(status.status().code(), ErrorCode::InvalidArgument);
        continue;
      }
      f.plans[0].request_status_relation = take(std::move(status));
    }
    ASSERT_TRUE(f.start().ok());
    const auto revision = f.engine->revision();
    auto opened = RequestRuntime::open(*f.engine, f.store, f.registry, f.plans, f.config);
    EXPECT_EQ(opened.status().code(), ErrorCode::InvalidArgument);
    EXPECT_EQ(f.engine->revision(), revision);
  }
}

TEST(RequestLatest, SameDomainPlansShareStatusButRejectConflictingProducers) {
  LatestRequestFixture f;
  ASSERT_TRUE(f.define().ok());
  auto second = f.plans[0];
  second.name = "second_plan";
  f.plans.push_back(second);
  const auto fields = take(take(f.catalog->snapshot()).find(f.capture))->identity_fields;
  const auto conflicting =
      take(f.catalog->define_request_status({"conflicting_meta", "requests", fields, f.candidate}));
  ASSERT_TRUE(f.start().ok());
  ASSERT_TRUE(f.activate().ok());
  auto bad = f.plans;
  bad[1].request_status_relation = conflicting;
  auto rejected = RequestRuntime::open(*f.engine, f.store, f.registry, bad, f.config);
  EXPECT_EQ(rejected.status().code(), ErrorCode::InvalidArgument);
  EXPECT_NE(rejected.status().message().find("conflicting"), std::string::npos);
  f.runtime = take(RequestRuntime::open(*f.engine, f.store, f.registry, f.plans, f.config));
  EXPECT_EQ(take(f.runtime->admit(f.admission(), 1)).record->state.manifest.admission_seq, 1U);
  auto admission = f.admission("second");
  admission.plan = "second_plan";
  EXPECT_EQ(take(f.runtime->admit(admission, 2)).record->state.manifest.admission_seq, 2U);
  EXPECT_EQ(f.query("SELECT phase FROM request_meta").size(), 2U);
  auto changed = f.admission();
  changed.plan = "second_plan";
  EXPECT_EQ(f.runtime->admit(changed, 2).status().code(), ErrorCode::Conflict);
  ASSERT_TRUE(f.reopen().ok());
  EXPECT_EQ(take(f.runtime->active_requests()).size(), 2U);
}

TEST(RequestLatest, MaximumSignedSequenceCanFinishAndTheNextAdmissionDoesNotCapture) {
  LatestRequestFixture f;
  ASSERT_TRUE(f.open().ok());
  f.runtime.reset();
  f.engine.reset();
  const auto index_key = take(row_key(0, 'S', f.config.namespace_id, {std::string("index")}));
  auto stored = take(f.store->get_many(0, {index_key}));
  auto index = take(parse_json_exact(*stored.values[0]));
  index["next_sequence"] = std::uint64_t{INT64_MAX};
  auto changed =
      take(f.store->commit({0,
                            f.engine_config.owner,
                            stored.revision,
                            take(make_commit_id(f.engine_config.owner, 0, stored.revision)),
                            {{index_key, index.dump()}}}));
  ASSERT_EQ(changed.code, CommitCode::Committed);
  ASSERT_TRUE(f.reopen().ok());
  auto admitted = take(f.runtime->admit(f.admission(), 1));
  EXPECT_EQ(admitted.record->state.manifest.admission_seq, std::uint64_t{INT64_MAX});
  EXPECT_EQ(f.query("SELECT admission_seq FROM request_meta"),
            (std::vector<Row>{{std::int64_t{INT64_MAX}}}));
  ASSERT_TRUE(f.reopen().ok());
  const auto revision = f.engine->revision();
  EXPECT_EQ(f.runtime->admit(f.admission("exhausted"), 2).status().code(),
            ErrorCode::ResourceExhausted);
  EXPECT_EQ(f.engine->revision(), revision);
  EXPECT_EQ(f.captures, 1U);
  EXPECT_TRUE(take(f.runtime->admit(f.admission(), 2)).duplicate);
  auto cancelled = take(f.runtime->cancel({std::string("a")}, "r1", 2));
  EXPECT_EQ(cancelled.record->state.phase, RequestPhase::Cancelled);
  EXPECT_EQ(f.query("SELECT phase FROM request_meta"),
            (std::vector<Row>{{std::string("CANCELLED")}}));
}

TEST(RequestLatest, MetadataReservationRejectsBeforeCaptureWhenItsConfiguredBoundsCannotFit) {
  for (const bool bytes : {true, false}) {
    SCOPED_TRACE(bytes);
    LatestRequestFixture f;
    ASSERT_TRUE(f.define().ok());
    if (bytes)
      f.engine_config.max_bytes = f.config.max_record_bytes;
    else
      f.engine_config.max_mutations = 3;
    ASSERT_TRUE(f.start().ok());
    ASSERT_TRUE(f.activate().ok());
    f.runtime = take(RequestRuntime::open(*f.engine, f.store, f.registry, f.plans, f.config));
    const auto revision = f.engine->revision();
    auto rejected = f.runtime->admit(f.admission(), 1);
    EXPECT_EQ(rejected.status().code(), ErrorCode::ResourceExhausted);
    EXPECT_EQ(f.captures, 0U);
    EXPECT_EQ(f.engine->revision(), revision);
    EXPECT_TRUE(take(f.runtime->active_requests()).empty());
  }
}

TEST(RequestLatest, ReopenRejectsMissingOrConflictingStatusForAnActiveRequest) {
  for (const bool missing : {true, false}) {
    SCOPED_TRACE(missing);
    LatestRequestFixture f;
    ASSERT_TRUE(f.open().ok());
    take(f.runtime->admit(f.admission(), 1));
    f.runtime.reset();
    f.engine.reset();
    const auto key = take(row_key(0, 'T', f.status, {std::string("a"), std::string("r1")}));
    auto stored = take(f.store->get_many(0, {key}));
    std::optional<std::string> value;
    if (!missing) {
      auto snapshot = take(f.catalog->snapshot());
      const auto* relation = take(snapshot.find(f.status));
      value = take(encode_row(relation->storage_schema,
                              {std::string("a"), std::string("r1"), std::int64_t{1},
                               std::int64_t{1}, std::string("RUNNING")}));
    }
    auto changed =
        take(f.store->commit({0,
                              f.engine_config.owner,
                              stored.revision,
                              take(make_commit_id(f.engine_config.owner, 0, stored.revision)),
                              {{key, std::move(value)}}}));
    ASSERT_EQ(changed.code, CommitCode::Committed);
    EXPECT_EQ(f.reopen().code(), ErrorCode::FailedPrecondition);
  }
}

INSTANTIATE_TEST_SUITE_P(AllStores, RequestLatestContract, testing::Values(0, 1, 2, 3));

TEST(LatestRuntimeRegression, MissingWinnerRowCannotLookLikeAnEmptyLatest) {
  LatestRequestFixture f;
  ASSERT_TRUE(f.open().ok());
  take(f.runtime->admit(f.admission(), 1));
  auto claimed = take(f.runtime->claim({std::string("a")}, "r1", 2));
  take(f.runtime->complete({std::string("a")}, "r1", f.run(claimed.tasks[0]), 3));
  ASSERT_EQ(f.query("SELECT answer FROM latest").size(), 1U);
  f.runtime.reset();
  f.engine.reset();
  auto key = take(row_key(0, 'T', f.latest, {std::string("a")}));
  auto revision = take(f.store->status(0)).revision;
  ASSERT_EQ(take(f.store->commit({0,
                                  f.engine_config.owner,
                                  revision,
                                  take(make_commit_id(f.engine_config.owner, 0, revision)),
                                  {{key, std::nullopt}}}))
                .code,
            CommitCode::Committed);
  ASSERT_TRUE(f.reopen().ok());
  auto queried =
      f.engine->query_sql("SELECT answer FROM latest", QueryContext{Row{std::string("a")}});
  EXPECT_EQ(queried.status().code(), ErrorCode::FailedPrecondition)
      << "A persisted winner remains, but an absent Latest T row was reported as "
      << (queried.ok() ? std::to_string(queried->rows.size()) + " ordinary rows"
                       : queried.status().message());
}

TEST(LatestRuntimeRegression, AdmittedSingleRequestCanCommitItsSuccessfulCompletion) {
  LatestRequestFixture f;
  ASSERT_TRUE(f.define().ok());
  f.engine_config.max_mutations = 7;
  f.config.max_active_requests = 1;
  ASSERT_TRUE(f.start().ok());
  ASSERT_TRUE(f.activate().ok());
  f.runtime = take(RequestRuntime::open(*f.engine, f.store, f.registry, f.plans, f.config));
  auto admitted = f.runtime->admit(f.admission(), 1);
  if (!admitted.ok()) {
    EXPECT_EQ(admitted.status().code(), ErrorCode::ResourceExhausted);
    EXPECT_TRUE(take(f.runtime->active_requests()).empty());
    EXPECT_EQ(f.captures, 0U);
    return;
  }
  ASSERT_TRUE(admitted->accepted);
  auto claimed = take(f.runtime->claim({std::string("a")}, "r1", 2));
  ASSERT_EQ(claimed.tasks.size(), 1U);
  auto completion = f.run(claimed.tasks[0]);
  auto revision = f.engine->revision();
  auto finished = f.runtime->complete({std::string("a")}, "r1", completion, 3);
  EXPECT_TRUE(finished.ok()) << finished.status().message();
  if (!finished.ok()) {
    EXPECT_EQ(finished.status().code(), ErrorCode::ResourceExhausted);
    EXPECT_EQ(f.engine->revision(), revision);
    EXPECT_EQ(take(f.runtime->read({std::string("a")}, "r1")).state.phase, RequestPhase::Running);
    EXPECT_EQ(f.runtime->complete({std::string("a")}, "r1", completion, 3).status().code(),
              ErrorCode::ResourceExhausted);
    EXPECT_EQ(f.runtime->admit(f.admission("r2"), 3).status().code(), ErrorCode::ResourceExhausted);
  }
}

TEST(LatestRuntimeRegression, GroupByOnlyDescendantsAreIncludedInMutationReservation) {
  LatestRequestFixture f;
  ASSERT_TRUE(f.define().ok());
  auto latest = take(f.catalog->define_latest({"latest_visible", f.target, f.status, f.candidate}));
  (void)latest;
  take(f.catalog->define_sql(
      "CREATE MATERIALIZED VIEW distinct_one AS SELECT k FROM latest_visible GROUP BY k"));
  take(f.catalog->define_sql(
      "CREATE MATERIALIZED VIEW distinct_two AS SELECT k FROM latest_visible GROUP BY k"));
  f.engine_config.max_mutations = 14;
  f.config.max_active_requests = 1;
  ASSERT_TRUE(f.start().ok());
  ASSERT_TRUE(f.activate().ok());
  f.runtime = take(RequestRuntime::open(*f.engine, f.store, f.registry, f.plans, f.config));
  auto admitted = f.runtime->admit(f.admission(), 1);
  ASSERT_EQ(admitted.status().code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(f.captures, 0U);
  EXPECT_TRUE(take(f.runtime->active_requests()).empty());
}

TEST(LatestRuntimeRegression, HostScopedScanAlsoRejectsAStoredWinnerWithNoResultRow) {
  LatestRequestFixture f;
  ASSERT_TRUE(f.open().ok());
  take(f.runtime->admit(f.admission(), 1));
  auto task = take(f.runtime->claim({std::string("a")}, "r1", 2)).tasks[0];
  take(f.runtime->complete({std::string("a")}, "r1", f.run(task), 3));
  f.runtime.reset();
  f.engine.reset();
  auto key = take(row_key(0, 'T', f.latest, {std::string("a")}));
  auto revision = take(f.store->status(0)).revision;
  take(f.store->commit({0,
                        f.engine_config.owner,
                        revision,
                        take(make_commit_id(f.engine_config.owner, 0, revision)),
                        {{key, std::nullopt}}}));
  ASSERT_TRUE(f.reopen().ok());
  auto result = EngineRuntimeAccess::apply(
      *f.engine, {3}, [&](RoundContext& context) noexcept -> StatusOr<std::vector<TableChange>> {
        EF_ASSIGN_OR_RETURN(auto rows, context.scan_relation(f.latest, {std::string("a")}));
        (void)rows;
        return std::vector<TableChange>{};
      });
  EXPECT_EQ(result.status().code(), ErrorCode::FailedPrecondition);
}
} // namespace
} // namespace eventflow
