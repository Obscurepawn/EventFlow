#include "../src/runtime/engine_internal.hpp"
#include "eventflow/key_codec.hpp"
#include "eventflow/request_runtime.hpp"
#include "remote_harness.hpp"
#include "status_test_support.hpp"
#include <filesystem>
#include <gtest/gtest.h>
#include <map>

namespace eventflow {
namespace {
using test::take;

class RequestStore final : public IStateStore {
public:
  explicit RequestStore(std::shared_ptr<IStateStore> backend) noexcept
      : backend(std::move(backend)) {}

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
  bool unknown_next = false;
  std::vector<CommitBatch> batches;
};

struct RequestFixture {
  std::shared_ptr<RequestStore> store;
  std::string path;
  std::vector<RequestPlanDefinition> plans;
  RequestRuntimeConfig config;
  std::size_t captures = 0;
  std::int64_t attempt_timeout_us = 30'000'000;
  std::map<std::string, std::size_t> graph_calls;
  RelationId status_relation = 0;
  RelationId final_relation = 0;
  std::shared_ptr<FunctionRegistry> registry;
  std::unique_ptr<Catalog> catalog;
  std::unique_ptr<Engine> engine;
  std::unique_ptr<RequestRuntime> runtime;
  RelationId quote = 0;
  RelationId capture = 0;
  RelationId target = 0;
  std::size_t calls = 0;

  ~RequestFixture() noexcept {
    runtime.reset();
    engine.reset();
    store.reset();
    if (!path.empty()) {
      std::error_code error;
      std::filesystem::remove_all(path, error);
    }
  }

  RelationId add_function(const std::string& name, const std::vector<RelationId>& inputs,
                          BuiltinFunctionCallback callback) noexcept {
    auto current = take(catalog->snapshot());
    FunctionViewDefinition definition;
    definition.name = name;
    definition.function_id = name;
    definition.function_version = 1;
    definition.implementation_identity = "builtin." + name + ".1";
    definition.output_fields = {{1, "value", DataType::Int64, false}};
    FunctionSignature signature{
        name, 1, definition.implementation_identity, {}, definition.output_fields, true};
    for (std::size_t i = 0; i < inputs.size(); ++i) {
      auto input = take(current.find(inputs[i]));
      std::vector<Field> fields(input->storage_schema.fields.begin(),
                                input->storage_schema.fields.begin() + input->visible_count);
      auto alias = "in_" + std::to_string(i);
      definition.inputs.push_back({alias, inputs[i], true, fields});
      signature.inputs.push_back({alias, true, fields});
    }
    auto wrapped = [this, name, callback](const FunctionInputs& values) noexcept -> StatusOr<Row> {
      ++graph_calls[name];
      return callback(values);
    };
    EXPECT_TRUE(registry->add({signature, wrapped}).ok());
    return take(catalog->define_function(definition));
  }

  Status open(int backend_kind = 0, bool diamond = false) noexcept {
    EF_ASSIGN_OR_RETURN(catalog, Catalog::create());
    EF_ASSIGN_OR_RETURN(
        quote,
        catalog->define_sql(
            "CREATE TABLE quote(k STRING, price INT64 NOT NULL, PRIMARY KEY(k)) PARTITION BY(k)"));
    DefinitionOptions options;
    options.base_write_role = BaseWriteRole::RequestCapture;
    options.identity_domain = "requests";
    EF_ASSIGN_OR_RETURN(
        capture,
        catalog->define_sql("CREATE TABLE captured(k STRING, request_id STRING, x INT64 NOT NULL, "
                            "PRIMARY KEY(k,request_id)) PARTITION BY(k)",
                            options));
    EF_ASSIGN_OR_RETURN(
        auto projected,
        catalog->define_sql("CREATE MATERIALIZED VIEW inputs AS SELECT x FROM captured"));
    EF_ASSIGN_OR_RETURN(auto snapshot, catalog->snapshot());
    EF_ASSIGN_OR_RETURN(auto input, snapshot.find(projected));
    std::vector<Field> fields(input->storage_schema.fields.begin(),
                              input->storage_schema.fields.begin() + input->visible_count);
    FunctionViewDefinition function;
    function.name = "computed";
    function.function_id = "twice";
    function.function_version = 1;
    function.implementation_identity = "builtin.twice.v1";
    function.inputs = {{"value", projected, true, fields}};
    function.output_fields = {{1, "answer", DataType::Int64, false}};
    EF_ASSIGN_OR_RETURN(target, catalog->define_function(function));
    EF_ASSIGN_OR_RETURN(registry, FunctionRegistry::create());
    EF_RETURN_IF_ERROR(registry->add(
        {{"twice", 1, "builtin.twice.v1", {{"value", true, fields}}, function.output_fields, true},
         [this](const FunctionInputs& inputs) noexcept -> StatusOr<Row> {
           ++calls;
           return Row{std::get<std::int64_t>((*inputs[0])[0]) * 2};
         }}));
    if (diamond) {
      auto branch = take(
          catalog->define_sql("CREATE MATERIALIZED VIEW branch AS SELECT answer FROM computed"));
      auto left = add_function("left_branch", {branch},
                               [](const FunctionInputs& input) noexcept -> StatusOr<Row> {
                                 return Row{std::get<std::int64_t>((*input[0])[0]) + 1};
                               });
      auto right = add_function("right_branch", {branch},
                                [](const FunctionInputs& input) noexcept -> StatusOr<Row> {
                                  return Row{std::get<std::int64_t>((*input[0])[0]) + 2};
                                });
      add_function("combined", {left, right},
                   [](const FunctionInputs& input) noexcept -> StatusOr<Row> {
                     return Row{std::get<std::int64_t>((*input[0])[2]) +
                                std::get<std::int64_t>((*input[1])[2])};
                   });
      final_relation = take(catalog->define_sql(
          "CREATE MATERIALIZED VIEW final_result AS SELECT value FROM combined"));
    }
    DefinitionOptions status_options;
    status_options.base_write_role = BaseWriteRole::RuntimeStatus;
    status_relation = take(catalog->define_sql(
        "CREATE TABLE request_status(k STRING,v INT64,PRIMARY KEY(k)) PARTITION BY(k)",
        status_options));
    StoreConfig store_config;
    store_config.shard_count = 1;
    std::unique_ptr<IStateStore> backend;
    if (backend_kind % 2 == 0) {
      EF_ASSIGN_OR_RETURN(backend, MemoryStateStore::open(store_config));
    } else {
      char pattern[] = "/tmp/eventflow-request-owner-XXXXXX";
      auto created = ::mkdtemp(pattern);
      if (!created)
        return Status(ErrorCode::Unavailable, "temporary directory unavailable");
      path = created;
      EF_ASSIGN_OR_RETURN(backend, RocksStateStore::open(path + "/db", store_config));
    }
    if (backend_kind >= 2) {
      EF_ASSIGN_OR_RETURN(backend, test::RemoteHarnessStore::open(std::move(backend)));
    }
    store = std::make_shared<RequestStore>(std::shared_ptr<IStateStore>(std::move(backend)));
    EngineConfig engine_config;
    engine_config.shard_count = 1;
    EF_ASSIGN_OR_RETURN(snapshot, catalog->snapshot());
    EF_ASSIGN_OR_RETURN(engine,
                        EngineRuntimeAccess::open(store, snapshot, engine_config, registry));
    EF_ASSIGN_OR_RETURN(auto inserted, engine->apply({quote, Row{std::string("a")},
                                                      Row{std::string("a"), std::int64_t(10)}}));
    (void)inserted;
    RequestPlanDefinition plan;
    plan.name = "prediction";
    plan.machine.attempt_timeout_us = attempt_timeout_us;
    plan.targets = {diamond ? final_relation : target};
    plan.capture = [this](RoundContext& context,
                          const RequestAdmission& admission) noexcept -> StatusOr<RequestCapture> {
      ++captures;
      EF_ASSIGN_OR_RETURN(auto row, context.read_relation(quote, admission.scope));
      if (!row)
        return RequestCapture{false, {}};
      Row identity = admission.scope;
      identity.emplace_back(admission.request_id);
      Row value{admission.scope[0], admission.request_id, (*row)[1]};
      return RequestCapture{true, {{capture, std::move(identity), std::move(value)}}};
    };
    plans = {plan};
    EF_ASSIGN_OR_RETURN(runtime, RequestRuntime::open(*engine, store, registry, plans, config));
    return {};
  }

  RequestAdmission admission(const std::string& id = "r1") const noexcept {
    RequestAdmission result;
    result.plan = "prediction";
    result.scope = {std::string("a")};
    result.request_id = id;
    return result;
  }

  Status reopen() noexcept {
    runtime.reset();
    engine.reset();
    EngineConfig engine_config;
    engine_config.shard_count = 1;
    EF_ASSIGN_OR_RETURN(auto snapshot, catalog->snapshot());
    EF_ASSIGN_OR_RETURN(engine,
                        EngineRuntimeAccess::open(store, snapshot, engine_config, registry));
    EF_ASSIGN_OR_RETURN(runtime, RequestRuntime::open(*engine, store, registry, plans, config));
    return {};
  }

  FunctionCompletion run_one(const FunctionTask& task, std::int64_t now = 2) noexcept {
    auto executor = take(SimpleFunctionExecutor::create(registry));
    EXPECT_TRUE(executor->submit(task).ok());
    EXPECT_EQ(take(executor->run_ready(now)), 1U);
    auto results = take(executor->poll(1));
    EXPECT_EQ(results.size(), 1U);
    return results.empty() ? FunctionCompletion{} : std::move(results[0]);
  }
};

TEST(RequestRuntime, AdmissionCapturesCurrentRowsAndDefersFunctionExecutionUntilClaim) {
  RequestFixture fixture;
  ASSERT_TRUE(fixture.open().ok());
  const auto capture_revision = fixture.engine->revision();
  RequestAdmission admission;
  admission.plan = "prediction";
  admission.scope = {std::string("a")};
  admission.request_id = "r1";
  auto result = fixture.runtime->admit(admission, 1);
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_TRUE(result->accepted);
  ASSERT_TRUE(result->record);
  EXPECT_EQ(result->record->state.manifest.capture_revision, capture_revision);
  EXPECT_EQ(result->record->state.manifest.admission_revision, capture_revision + 1);
  EXPECT_EQ(fixture.calls, 0u);
  EXPECT_TRUE(result->tasks.empty());
  const auto captured = take(fixture.engine->query_sql("SELECT x FROM captured WHERE k='a'"));
  ASSERT_EQ(captured.rows.size(), 1u);
  EXPECT_EQ(std::get<std::int64_t>(captured.rows[0][0]), 10);
}

class RequestRuntimeContract : public testing::TestWithParam<int> {};

TEST_P(RequestRuntimeContract, FrozenInputsSurviveSourceUpdateAndDiamondRunsSharedAncestorOnce) {
  RequestFixture f;
  ASSERT_TRUE(f.open(GetParam(), true).ok());
  auto admitted = take(f.runtime->admit(f.admission(), 1));
  ASSERT_TRUE(admitted.accepted);
  take(f.engine->apply({f.quote, Row{std::string("a")}, Row{std::string("a"), std::int64_t{99}}},
                       {1}));
  auto executor = take(SimpleFunctionExecutor::create(f.registry));
  auto phase = admitted.record->state.phase;
  for (std::int64_t now = 2; now < 10 && phase != RequestPhase::Success; ++now) {
    auto claimed = take(f.runtime->claim({std::string("a")}, "r1", now));
    ASSERT_FALSE(claimed.tasks.empty());
    for (auto& task : claimed.tasks)
      ASSERT_TRUE(executor->submit(task).ok());
    EXPECT_EQ(take(executor->run_ready(now)), claimed.tasks.size());
    auto completions = take(executor->poll(16));
    for (auto& completion : completions) {
      auto result = take(f.runtime->complete({std::string("a")}, "r1", completion, now));
      ASSERT_TRUE(result.record);
      phase = result.record->state.phase;
    }
  }
  EXPECT_EQ(phase, RequestPhase::Success);
  EXPECT_EQ(f.calls, 1U);
  EXPECT_EQ(f.graph_calls["left_branch"], 1U);
  EXPECT_EQ(f.graph_calls["right_branch"], 1U);
  EXPECT_EQ(f.graph_calls["combined"], 1U);
  auto result =
      take(f.engine->query_sql("SELECT value FROM final_result", {Row{std::string("a")}}));
  ASSERT_EQ(result.rows.size(), 1U);
  EXPECT_EQ(result.rows[0], Row{std::int64_t{43}});
  EXPECT_TRUE(take(f.runtime->active_requests()).empty());
}

TEST_P(RequestRuntimeContract,
       UnknownAdmissionResolvesExactBatchWithoutRecaptureOrEarlyPublication) {
  RequestFixture f;
  ASSERT_TRUE(f.open(GetParam()).ok());
  std::size_t companion_calls = 0;
  auto hook = [&](RoundContext&,
                  const RequestRecord&) noexcept -> StatusOr<std::vector<TableChange>> {
    ++companion_calls;
    return std::vector<TableChange>{
        {f.status_relation, {std::string("a")}, Row{std::string("a"), std::int64_t{1}}}};
  };
  f.store->unknown_next = true;
  auto uncertain = take(f.runtime->admit(f.admission(), 1, hook));
  EXPECT_EQ(uncertain.commit.code, CommitCode::Unknown);
  EXPECT_FALSE(uncertain.accepted);
  EXPECT_FALSE(uncertain.record);
  EXPECT_TRUE(uncertain.tasks.empty());
  EXPECT_TRUE(f.runtime->has_pending_commit());
  EXPECT_FALSE(f.runtime->pending_tasks(1).ok());
  const auto original = take(canonical_batch(f.store->batches.back()));
  auto confirmed = take(f.runtime->resolve_pending_commit());
  EXPECT_EQ(confirmed.commit.code, CommitCode::Committed);
  EXPECT_TRUE(confirmed.accepted);
  EXPECT_EQ(f.captures, 1U);
  EXPECT_EQ(companion_calls, 1U);
  EXPECT_EQ(take(canonical_batch(f.store->batches.back())), original);
  EXPECT_EQ(take(f.engine->query_sql("SELECT v FROM request_status WHERE k='a'")).rows[0],
            Row{std::int64_t{1}});
  auto again = take(f.runtime->admit(f.admission(), 2, hook));
  EXPECT_TRUE(again.duplicate);
  EXPECT_EQ(f.captures, 1U);
  EXPECT_EQ(companion_calls, 1U);
}

TEST_P(RequestRuntimeContract, UnknownClaimAndCompletionExposeTasksAndResultsOnlyOnceConfirmed) {
  RequestFixture f;
  ASSERT_TRUE(f.open(GetParam()).ok());
  take(f.runtime->admit(f.admission(), 1));
  f.store->unknown_next = true;
  auto claim = take(f.runtime->claim({std::string("a")}, "r1", 2));
  EXPECT_EQ(claim.commit.code, CommitCode::Unknown);
  EXPECT_TRUE(claim.tasks.empty());
  auto confirmed = take(f.runtime->resolve_pending_commit());
  ASSERT_EQ(confirmed.tasks.size(), 1U);
  auto completion = f.run_one(confirmed.tasks[0]);
  f.store->unknown_next = true;
  auto done = take(f.runtime->complete({std::string("a")}, "r1", completion, 3));
  EXPECT_EQ(done.commit.code, CommitCode::Unknown);
  EXPECT_FALSE(done.record);
  auto restored = take(f.runtime->resolve_pending_commit());
  ASSERT_TRUE(restored.record);
  EXPECT_EQ(restored.record->state.phase, RequestPhase::Success);
  auto revision = f.engine->revision();
  EXPECT_TRUE(take(f.runtime->complete({std::string("a")}, "r1", completion, 4)).duplicate);
  EXPECT_EQ(f.engine->revision(), revision);
  EXPECT_EQ(f.calls, 1U);
}

TEST_P(RequestRuntimeContract, ReopenReconstructsRunningTaskAndPreservesTerminalArchive) {
  RequestFixture f;
  ASSERT_TRUE(f.open(GetParam()).ok());
  take(f.runtime->admit(f.admission(), 1));
  auto first = take(f.runtime->claim({std::string("a")}, "r1", 2));
  ASSERT_EQ(first.tasks.size(), 1U);
  ASSERT_TRUE(f.reopen().ok());
  auto recovered = take(f.runtime->pending_tasks(16));
  ASSERT_EQ(recovered.size(), 1U);
  EXPECT_EQ(first.tasks[0].task_id, recovered[0].task_id);
  EXPECT_EQ(first.tasks[0].inputs, recovered[0].inputs);
  auto completion = f.run_one(recovered[0]);
  EXPECT_EQ(take(f.runtime->complete({std::string("a")}, "r1", completion, 3)).record->state.phase,
            RequestPhase::Success);
  ASSERT_TRUE(f.reopen().ok());
  EXPECT_TRUE(take(f.runtime->pending_tasks(16)).empty());
  EXPECT_TRUE(take(f.runtime->active_requests()).empty());
  EXPECT_EQ(take(f.runtime->read({std::string("a")}, "r1")).state.phase, RequestPhase::Success);
  EXPECT_TRUE(take(f.runtime->admit(f.admission(), 4)).duplicate);
}

TEST_P(RequestRuntimeContract, CapacityOneAllowsCancellationAndRejectsLateCompletion) {
  RequestFixture f;
  f.config.max_active_requests = 1;
  ASSERT_TRUE(f.open(GetParam()).ok());
  take(f.runtime->admit(f.admission(), 1));
  auto first = take(f.runtime->claim({std::string("a")}, "r1", 2));
  EXPECT_EQ(f.runtime->admit(f.admission("r2"), 3).status().code(), ErrorCode::ResourceExhausted);
  auto cancel = take(f.runtime->cancel({std::string("a")}, "r1", 4));
  EXPECT_EQ(cancel.record->state.phase, RequestPhase::Cancelled);
  ASSERT_EQ(cancel.cancel_task_ids.size(), 1U);
  EXPECT_TRUE(take(f.runtime->admit(f.admission("r2"), 5)).accepted);
  auto completion = f.run_one(first.tasks[0]);
  auto late = take(f.runtime->complete({std::string("a")}, "r1", completion, 6));
  EXPECT_TRUE(late.duplicate);
  EXPECT_EQ(late.record->state.phase, RequestPhase::Cancelled);
  EXPECT_TRUE(take(f.engine->query_sql("SELECT answer FROM computed WHERE k='a'")).rows.empty());
}

TEST_P(RequestRuntimeContract, CompanionFailureRollsBackCaptureAndRetainsOwnerUsability) {
  RequestFixture f;
  ASSERT_TRUE(f.open(GetParam()).ok());
  const auto revision = f.engine->revision();
  auto failure = f.runtime->admit(
      f.admission(), 1,
      [](RoundContext&, const RequestRecord&) noexcept -> StatusOr<std::vector<TableChange>> {
        return Status(ErrorCode::ResourceExhausted, "owner status full");
      });
  EXPECT_EQ(failure.status().code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(f.engine->revision(), revision);
  EXPECT_FALSE(f.engine->failed());
  EXPECT_TRUE(take(f.engine->query_sql("SELECT x FROM captured WHERE k='a'")).rows.empty());
  EXPECT_TRUE(take(f.runtime->active_requests()).empty());
  auto retry = take(f.runtime->admit(f.admission(), 1));
  EXPECT_TRUE(retry.accepted);
  EXPECT_EQ(retry.record->state.manifest.admission_seq, 1U);
}

TEST_P(RequestRuntimeContract, NotReadyCaptureLeavesNoRecordAndCanBeRetriedLater) {
  RequestFixture f;
  ASSERT_TRUE(f.open(GetParam()).ok());
  auto admission = f.admission();
  admission.scope = {std::string("b")};
  auto revision = f.engine->revision();
  auto deferred = take(f.runtime->admit(admission, 1));
  EXPECT_TRUE(deferred.deferred);
  EXPECT_FALSE(deferred.accepted);
  EXPECT_EQ(f.engine->revision(), revision);
  EXPECT_EQ(f.runtime->read(admission.scope, "r1").status().code(), ErrorCode::NotFound);
  take(f.engine->apply({f.quote, admission.scope, Row{std::string("b"), std::int64_t{5}}}));
  auto accepted = take(f.runtime->admit(admission, 2));
  EXPECT_TRUE(accepted.accepted);
  EXPECT_EQ(accepted.record->state.manifest.admission_seq, 1U);
}

TEST_P(RequestRuntimeContract, AdmissionIdentityIncludesExactArgumentTypesAndSignedZero) {
  RequestFixture f;
  ASSERT_TRUE(f.open(GetParam()).ok());
  auto original = f.admission();
  original.arguments = {std::int64_t{1}, -0.0};
  take(f.runtime->admit(original, 1));
  auto conflicting = original;
  conflicting.arguments[0] = 1.0;
  EXPECT_EQ(f.runtime->admit(conflicting, 2).status().code(), ErrorCode::Conflict);
  conflicting = original;
  conflicting.arguments[1] = 0.0;
  EXPECT_EQ(f.runtime->admit(conflicting, 2).status().code(), ErrorCode::Conflict);
  EXPECT_TRUE(take(f.runtime->admit(original, 2)).duplicate);
}

TEST_P(RequestRuntimeContract, ASecondOwnerIsRejectedAndLeaseReleasedAtDestruction) {
  RequestFixture f;
  ASSERT_TRUE(f.open(GetParam()).ok());
  auto second = RequestRuntime::open(*f.engine, f.store, f.registry, f.plans, f.config);
  EXPECT_EQ(second.status().code(), ErrorCode::Conflict);
  ASSERT_TRUE(f.reopen().ok());
  EXPECT_TRUE(take(f.runtime->admit(f.admission(), 1)).accepted);
}

TEST_P(RequestRuntimeContract, RetryKeepsFrozenInputsAndRejectsOldAttemptBeforeWritingOutput) {
  RequestFixture f;
  f.attempt_timeout_us = 5;
  ASSERT_TRUE(f.open(GetParam()).ok());
  take(f.runtime->admit(f.admission(), 1));
  auto original = take(f.runtime->claim({std::string("a")}, "r1", 2));
  ASSERT_EQ(original.tasks.size(), 1U);
  take(f.engine->apply({f.quote, {std::string("a")}, Row{std::string("a"), std::int64_t{999}}},
                       {3}));
  auto timeout = take(f.runtime->advance({std::string("a")}, "r1", 7));
  EXPECT_EQ(timeout.cancel_task_ids, std::vector<std::string>{original.tasks[0].task_id});
  auto retry = take(f.runtime->claim({std::string("a")}, "r1", 7));
  ASSERT_EQ(retry.tasks.size(), 1U);
  EXPECT_NE(retry.tasks[0].task_id, original.tasks[0].task_id);
  EXPECT_EQ(retry.tasks[0].inputs, original.tasks[0].inputs);
  auto old_completion = f.run_one(original.tasks[0], 2);
  take(f.runtime->complete({std::string("a")}, "r1", old_completion, 8));
  EXPECT_TRUE(take(f.engine->query_sql("SELECT answer FROM computed WHERE k='a'")).rows.empty());
  auto valid = f.run_one(retry.tasks[0], 8);
  auto done = take(f.runtime->complete({std::string("a")}, "r1", valid, 9));
  EXPECT_EQ(done.record->state.phase, RequestPhase::Success);
  EXPECT_EQ(take(f.engine->query_sql("SELECT answer FROM computed WHERE k='a'")).rows[0],
            Row{std::int64_t{20}});
}

TEST_P(RequestRuntimeContract, DeclaredLifetimeExpiresAndFreesAdmissionCapacity) {
  RequestFixture f;
  f.config.max_active_requests = 1;
  ASSERT_TRUE(f.open(GetParam()).ok());
  auto admission = f.admission();
  admission.lifetime_us = 5;
  take(f.runtime->admit(admission, 1));
  auto claimed = take(f.runtime->claim(admission.scope, "r1", 2));
  ASSERT_EQ(claimed.tasks.size(), 1U);
  auto expired = take(f.runtime->advance(admission.scope, "r1", 6));
  EXPECT_EQ(expired.record->state.phase, RequestPhase::Failed);
  EXPECT_EQ(expired.cancel_task_ids, std::vector<std::string>{claimed.tasks[0].task_id});
  EXPECT_TRUE(take(f.runtime->admit(f.admission("r2"), 6)).accepted);
  EXPECT_TRUE(take(f.runtime->complete(admission.scope, "r1", f.run_one(claimed.tasks[0], 2), 7))
                  .duplicate);
}

TEST_P(RequestRuntimeContract, SameRequestIdInDifferentScopesHasSeparateImmutableCapture) {
  RequestFixture f;
  ASSERT_TRUE(f.open(GetParam()).ok());
  take(f.engine->apply({f.quote, {std::string("b")}, Row{std::string("b"), std::int64_t{11}}}));
  auto a = f.admission();
  auto b = a;
  b.scope = {std::string("b")};
  take(f.runtime->admit(a, 1));
  take(f.runtime->admit(b, 1));
  auto ta = take(f.runtime->claim(a.scope, "r1", 2)).tasks;
  auto tb = take(f.runtime->claim(b.scope, "r1", 2)).tasks;
  ASSERT_EQ(ta.size(), 1U);
  ASSERT_EQ(tb.size(), 1U);
  EXPECT_NE(ta[0].task_id, tb[0].task_id);
  auto ca = f.run_one(ta[0]);
  take(f.runtime->complete(b.scope, "r1", ca, 3));
  EXPECT_EQ(take(f.runtime->read(b.scope, "r1")).state.phase, RequestPhase::Running);
  take(f.runtime->complete(a.scope, "r1", ca, 3));
  take(f.runtime->complete(b.scope, "r1", f.run_one(tb[0]), 3));
  EXPECT_EQ(take(f.engine->query_sql("SELECT answer FROM computed WHERE k='a'")).rows[0],
            Row{std::int64_t{20}});
  EXPECT_EQ(take(f.engine->query_sql("SELECT answer FROM computed WHERE k='b'")).rows[0],
            Row{std::int64_t{22}});
}

TEST(RequestRuntime, AStoreWithMatchingRevisionCannotReplaceTheEngineBackend) {
  RequestFixture f;
  ASSERT_TRUE(f.open().ok());
  StoreConfig config;
  config.shard_count = 1;
  auto other = std::shared_ptr<IStateStore>(take(MemoryStateStore::open(config)));
  for (const auto& batch : f.store->batches)
    EXPECT_EQ(take(other->commit(batch)).code, CommitCode::Committed);
  f.runtime.reset();
  auto bad = RequestRuntime::open(*f.engine, other, f.registry, f.plans, f.config);
  EXPECT_EQ(bad.status().code(), ErrorCode::InvalidArgument);
}

TEST(RequestRuntime, AttachedOwnerPreventsCatalogActivationUntilExplicitDetach) {
  RequestFixture f;
  ASSERT_TRUE(f.open().ok());
  take(f.runtime->admit(f.admission(), 1));
  auto result =
      f.engine->install_sql("CREATE TABLE later(k STRING,v INT64,PRIMARY KEY(k)) PARTITION BY(k)");
  EXPECT_EQ(result.status().code(), ErrorCode::FailedPrecondition);
  EXPECT_FALSE(f.engine->failed());
  EXPECT_TRUE(take(f.runtime->read({std::string("a")}, "r1")).state.manifest.admission_seq == 1);
}

TEST(RequestRuntime, OrdinaryHooksCannotOverwriteTheRegisteredRequestNamespace) {
  RequestFixture f;
  ASSERT_TRUE(f.open().ok());
  take(f.runtime->admit(f.admission(), 1));
  auto key = take(row_key(0, 'S', f.config.namespace_id, {std::string("index")}));
  auto original = take(f.store->get_many(0, {key}));
  auto result = f.engine->apply_many(
      {}, {2}, [&](RoundContext& context, const std::vector<RowChange>&) noexcept -> Status {
        return context.stage({key, std::string("corrupt")});
      });
  EXPECT_EQ(result.status().code(), ErrorCode::InvalidArgument);
  auto after = take(f.store->get_many(0, {key}));
  EXPECT_EQ(after.values, original.values);
  EXPECT_EQ(after.revision, original.revision);
}

TEST(RequestOwnerFresh, PublicEngineResolveMustNotStrandRuntime) {
  RequestFixture f;
  ASSERT_TRUE(f.open().ok());
  f.store->unknown_next = true;
  ASSERT_EQ(take(f.runtime->admit(f.admission(), 1)).commit.code, CommitCode::Unknown);
  auto direct = f.engine->resolve_pending_commit();
  EXPECT_FALSE(direct.ok()) << "Public Engine resolver stole the Runtime-owned confirmation";
  auto resolved = f.runtime->resolve_pending_commit();
  EXPECT_TRUE(resolved.ok()) << resolved.status().message();
  EXPECT_TRUE(f.runtime->active_requests().ok());
}

TEST(RequestOwnerFresh, CompanionCannotCorruptRuntimeIndex) {
  RequestFixture f;
  ASSERT_TRUE(f.open().ok());
  auto key = take(row_key(0, 'S', f.config.namespace_id, {std::string("index")}));
  auto before = f.engine->revision();
  auto admitted =
      f.runtime->admit(f.admission(), 1,
                       [&](RoundContext& context,
                           const RequestRecord&) noexcept -> StatusOr<std::vector<TableChange>> {
                         EF_RETURN_IF_ERROR(context.stage({key, std::string("corrupt")}));
                         return std::vector<TableChange>{};
                       });
  EXPECT_FALSE(admitted.ok()) << "Companion replaced the request index after Runtime stage";
  EXPECT_EQ(f.engine->revision(), before);
  auto reopened = f.reopen();
  EXPECT_TRUE(reopened.ok()) << reopened.message();
}

TEST(RequestOwnerFresh, ActiveByteLimitReservesProgressCapacity) {
  RequestFixture baseline;
  ASSERT_TRUE(baseline.open().ok());
  ASSERT_TRUE(baseline.runtime->admit(baseline.admission(), 1).ok());
  auto prefix = take(table_prefix(0, 'S', baseline.config.namespace_id));
  auto entries = take(baseline.store->scan(0, prefix, {}, 100));
  std::size_t active_bytes = 0;
  for (const auto& entry : entries.rows) {
    auto json = take(parse_json_exact(entry.second));
    if (json.contains("state"))
      active_bytes = entry.second.size();
  }
  ASSERT_GT(active_bytes, 0U);
  RequestFixture f;
  f.config.max_active_bytes = active_bytes;
  ASSERT_TRUE(f.open().ok());
  auto admitted = f.runtime->admit(f.admission(), 1);
  if (!admitted.ok()) {
    EXPECT_EQ(admitted.status().code(), ErrorCode::ResourceExhausted);
    EXPECT_TRUE(take(f.runtime->active_requests()).empty());
    return;
  }
  ASSERT_TRUE(admitted->accepted);
  auto claimed = f.runtime->claim({std::string("a")}, "r1", 2);
  EXPECT_TRUE(claimed.ok()) << "Accepted at " << active_bytes
                            << " bytes but cannot claim: " << claimed.status().message();
  EXPECT_TRUE(f.runtime->cancel({std::string("a")}, "r1", 3).ok());
}

TEST(RequestOwnerFresh, DetachMustNotPermitCatalogToOrphanAcceptedRequests) {
  RequestFixture f;
  ASSERT_TRUE(f.open().ok());
  take(f.runtime->admit(f.admission(), 1));
  ASSERT_EQ(take(f.runtime->claim({std::string("a")}, "r1", 2)).tasks.size(), 1U);
  f.runtime.reset();
  auto installed =
      f.engine->install_sql("CREATE TABLE later(k STRING,v INT64,PRIMARY KEY(k)) PARTITION BY(k)");
  EXPECT_FALSE(installed.ok())
      << "Catalog activation succeeded while durable running request exists";
  auto reopened = RequestRuntime::open(*f.engine, f.store, f.registry, f.plans, f.config);
  EXPECT_TRUE(reopened.ok()) << reopened.status().message();
}

TEST(RequestRuntime, OrdinaryInputUnknownCanStillBeResolvedWithRequestOwnerAttached) {
  RequestFixture f;
  ASSERT_TRUE(f.open().ok());
  f.store->unknown_next = true;
  auto input = take(
      f.engine->apply({f.quote, {std::string("a")}, Row{std::string("a"), std::int64_t{12}}}, {1}));
  EXPECT_EQ(input.commit.code, CommitCode::Unknown);
  EXPECT_EQ(take(f.engine->resolve_pending_commit()).commit.code, CommitCode::Committed);
  auto admitted = take(f.runtime->admit(f.admission(), 1));
  EXPECT_TRUE(admitted.accepted);
  EXPECT_EQ(take(f.engine->query_sql("SELECT x FROM captured WHERE k='a'")).rows[0],
            Row{std::int64_t{12}});
}

TEST(RequestRuntime, ReconstructedEngineStillCannotDiscardFixedRequestCatalog) {
  RequestFixture f;
  ASSERT_TRUE(f.open().ok());
  take(f.runtime->admit(f.admission(), 1));
  auto task = take(f.runtime->claim({std::string("a")}, "r1", 2)).tasks[0];
  f.runtime.reset();
  f.engine.reset();
  EngineConfig config;
  config.shard_count = 1;
  f.engine =
      take(EngineRuntimeAccess::open(f.store, take(f.catalog->snapshot()), config, f.registry));
  auto ddl =
      f.engine->install_sql("CREATE TABLE later(k STRING,v INT64,PRIMARY KEY(k)) PARTITION BY(k)");
  EXPECT_EQ(ddl.status().code(), ErrorCode::FailedPrecondition);
  f.runtime = take(RequestRuntime::open(*f.engine, f.store, f.registry, f.plans, f.config));
  EXPECT_EQ(take(f.runtime->pending_tasks(16))[0].task_id, task.task_id);
}

TEST_P(RequestRuntimeContract, RecoveryRejectsInsufficientEffectiveMutationBudgetWithoutWrites) {
  RequestFixture f;
  ASSERT_TRUE(f.open(GetParam()).ok());
  ASSERT_TRUE(take(f.runtime->admit(f.admission(), 1)).accepted);
  f.runtime.reset();
  f.engine.reset();
  EngineConfig config;
  config.shard_count = 1;
  config.max_mutations = 2;
  f.engine =
      take(EngineRuntimeAccess::open(f.store, take(f.catalog->snapshot()), config, f.registry));
  const auto revision = f.engine->revision();
  const auto commits = f.store->batches.size();
  auto opened = RequestRuntime::open(*f.engine, f.store, f.registry, f.plans, f.config);
  ASSERT_FALSE(opened.ok());
  EXPECT_EQ(opened.status().code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(f.store->batches.size(), commits);
  EXPECT_EQ(take(f.store->status(0)).revision, revision);
  ASSERT_TRUE(f.reopen().ok());
  EXPECT_EQ(take(f.runtime->cancel({std::string("a")}, "r1", 2)).record->state.phase,
            RequestPhase::Cancelled);
}

TEST_P(RequestRuntimeContract, OwnedRecordBudgetAppliesWithoutStatusRelationBeforeCapture) {
  RequestFixture f;
  ASSERT_TRUE(f.open(GetParam()).ok());
  f.runtime.reset();
  f.engine.reset();
  EngineConfig config;
  config.shard_count = 1;
  config.max_bytes = 1024 * 1024;
  f.engine =
      take(EngineRuntimeAccess::open(f.store, take(f.catalog->snapshot()), config, f.registry));
  f.runtime = take(RequestRuntime::open(*f.engine, f.store, f.registry, f.plans, f.config));
  const auto revision = f.engine->revision();
  const auto commits = f.store->batches.size();
  auto admitted = f.runtime->admit(f.admission(), 1);
  ASSERT_FALSE(admitted.ok());
  EXPECT_EQ(admitted.status().code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(f.captures, 0U);
  EXPECT_EQ(f.store->batches.size(), commits);
  EXPECT_EQ(take(f.store->status(0)).revision, revision);
}

INSTANTIATE_TEST_SUITE_P(AllStores, RequestRuntimeContract, testing::Values(0, 1, 2, 3));
} // namespace
} // namespace eventflow
