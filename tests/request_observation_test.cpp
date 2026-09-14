#include "../src/runtime/engine_internal.hpp"
#include "eventflow/key_codec.hpp"
#include "eventflow/request_runtime.hpp"
#include "remote_harness.hpp"
#include "status_test_support.hpp"
#include <algorithm>
#include <filesystem>
#include <gtest/gtest.h>

namespace eventflow::test {
namespace {
class ObservationStore final : public IStateStore {
public:
  explicit ObservationStore(std::shared_ptr<IStateStore> value) noexcept
      : backend(std::move(value)) {}

  StatusOr<StoreLimits> limits() noexcept override {
    return backend->limits();
  }

  StatusOr<ReadResult> get_many(std::uint32_t shard,
                                const std::vector<std::string>& keys) noexcept override {
    return backend->get_many(shard, keys);
  }

  StatusOr<ScanResult> scan(std::uint32_t shard, const std::string& prefix,
                            const std::optional<std::string>& after,
                            std::size_t limit) noexcept override {
    return backend->scan(shard, prefix, after, limit);
  }

  StatusOr<ShardStatus> status(std::uint32_t shard) noexcept override {
    return backend->status(shard);
  }

  StatusOr<CommitResult> commit(const CommitBatch& batch) noexcept override {
    EF_ASSIGN_OR_RETURN(auto result, backend->commit(batch));
    if (unknown_next) {
      unknown_next = false;
      return CommitResult{CommitCode::Unknown, batch.expected_revision, false,
                          "lost observation commit response"};
    }
    return result;
  }

  std::shared_ptr<IStateStore> backend;
  bool unknown_next = false;
};

struct ObservationFrame {
  RuntimeCause cause;
  std::vector<RowChange> changes;
  std::optional<Row> latest;
  std::optional<Row> child;
  std::uint64_t revision = 0;
  std::int64_t now = 0;
};

struct ObservationFixture {
  std::shared_ptr<ObservationStore> store;
  std::shared_ptr<FunctionRegistry> registry;
  std::unique_ptr<Catalog> catalog;
  std::unique_ptr<Engine> engine;
  std::unique_ptr<RequestRuntime> runtime;
  std::vector<RequestPlanDefinition> plans;
  std::vector<ObservationFrame> frames;
  RelationId capture = 0, target = 0, second_target = 0, sql = 0, candidate = 0, status = 0,
             latest = 0, child = 0, dependent = 0;
  bool typed_status = true;
  bool immediate_sql = false;
  bool two_targets = false;
  bool empty_capture = false;
  bool filtered_sql = false;
  bool require_filtered_result = false;
  std::uint32_t max_attempts = 2;
  std::int64_t timeout_us = 10;
  std::string path;
  std::string observation_key;

  ~ObservationFixture() noexcept {
    runtime.reset();
    engine.reset();
    store.reset();
    if (!path.empty()) {
      std::error_code ignored;
      std::filesystem::remove_all(path, ignored);
    }
  }

  Status open(int backend_kind) noexcept {
    EF_ASSIGN_OR_RETURN(catalog, Catalog::create());
    EF_ASSIGN_OR_RETURN(registry, FunctionRegistry::create());
    DefinitionOptions options;
    options.base_write_role = BaseWriteRole::RequestCapture;
    options.identity_domain = "observation_requests";
    EF_ASSIGN_OR_RETURN(
        capture, catalog->define_sql("CREATE TABLE capture(k STRING,request_id STRING,x INT64 NOT "
                                     "NULL,PRIMARY KEY(k,request_id)) PARTITION BY(k)",
                                     options));
    EF_ASSIGN_OR_RETURN(auto snapshot, catalog->snapshot());
    EF_ASSIGN_OR_RETURN(auto input, snapshot.find(capture));
    FunctionViewDefinition function;
    function.name = "computed";
    function.function_id = "twice";
    function.function_version = 1;
    function.implementation_identity = "builtin.twice.v1";
    function.inputs = {{"input", capture, true, input->storage_schema.fields}};
    function.output_fields = {{1, "answer", DataType::Int64, false}};
    EF_ASSIGN_OR_RETURN(target, catalog->define_function(function));
    EF_RETURN_IF_ERROR(registry->add({{"twice",
                                       1,
                                       "builtin.twice.v1",
                                       {{"input", true, input->storage_schema.fields}},
                                       function.output_fields,
                                       true},
                                      [](const FunctionInputs& values) noexcept -> StatusOr<Row> {
                                        return Row{std::get<std::int64_t>((*values[0])[2]) * 2};
                                      }}));
    if (two_targets) {
      function.name = "second";
      EF_ASSIGN_OR_RETURN(second_target, catalog->define_function(function));
    }
    EF_ASSIGN_OR_RETURN(
        sql, catalog->define_sql(
                 immediate_sql
                     ? "CREATE MATERIALIZED VIEW result AS SELECT x AS answer FROM capture"
                     : (filtered_sql
                            ? "CREATE MATERIALIZED VIEW result AS SELECT answer FROM computed "
                              "WHERE answer>100"
                            : "CREATE MATERIALIZED VIEW result AS SELECT answer FROM computed")));
    if (require_filtered_result) {
      EF_ASSIGN_OR_RETURN(snapshot, catalog->snapshot());
      EF_ASSIGN_OR_RETURN(auto relation, snapshot.find(sql));
      FunctionViewDefinition next;
      next.name = "after_filter";
      next.function_id = "after_filter";
      next.function_version = 1;
      next.implementation_identity = "builtin.after_filter.v1";
      std::vector<Field> fields(relation->storage_schema.fields.begin(),
                                relation->storage_schema.fields.begin() + relation->visible_count);
      next.inputs = {{"input", sql, true, fields}};
      next.output_fields = {{1, "answer", DataType::Int64, false}};
      EF_ASSIGN_OR_RETURN(dependent, catalog->define_function(next));
      EF_RETURN_IF_ERROR(registry->add(
          {{"after_filter",
            1,
            "builtin.after_filter.v1",
            {{"input", true, fields}},
            next.output_fields,
            true},
           [](const FunctionInputs&) noexcept -> StatusOr<Row> { return Row{std::int64_t{14}}; }}));
    }
    if (typed_status) {
      EF_ASSIGN_OR_RETURN(candidate,
                          catalog->define_candidate({"candidate",
                                                     {{1, "k", DataType::String, false}},
                                                     {{2, "payload", DataType::Int64, false}}}));
      EF_ASSIGN_OR_RETURN(status,
                          catalog->define_request_status({"request_status", "observation_requests",
                                                          input->identity_fields, candidate}));
      EF_ASSIGN_OR_RETURN(latest, catalog->define_latest({"latest", sql, status, candidate}));
      EF_ASSIGN_OR_RETURN(
          child, catalog->define_sql(
                     "CREATE MATERIALIZED VIEW plus_one AS SELECT answer+1 AS value FROM latest"));
    }
    RequestPlanDefinition plan;
    plan.name = "prediction";
    plan.targets = {dependent ? dependent : sql};
    if (two_targets)
      plan.targets.push_back(second_target);
    if (typed_status)
      plan.request_status_relation = status;
    plan.default_lifetime_us = 100;
    plan.machine.max_attempts = max_attempts;
    plan.machine.attempt_timeout_us = timeout_us;
    plan.capture = [this](RoundContext&,
                          const RequestAdmission& admission) noexcept -> StatusOr<RequestCapture> {
      if (empty_capture)
        return RequestCapture{true, {}};
      Row identity = admission.scope;
      identity.emplace_back(admission.request_id);
      Row row = identity;
      row.emplace_back(std::int64_t{7});
      return RequestCapture{true, {{capture, identity, row}}};
    };
    plans = {plan};
    ObservationRuleDefinition rule;
    rule.name = "request_observer";
    rule.recipe_id = "request_observer";
    rule.implementation_identity = "builtin.request_observer.v1";
    rule.program_fingerprint = "{}";
    rule.scope_fields = {{1, "k", DataType::String, false}};
    if (!immediate_sql) {
      ObservationInputDefinition attempt;
      attempt.id = "attempt";
      attempt.kind = ObservationInputKind::AttemptSettled;
      attempt.event_kind = "attempt_settled";
      attempt.plan = plan.name;
      attempt.completion_targets = {target};
      rule.inputs.push_back(attempt);
      if (two_targets) {
        attempt.id = "second_attempt";
        attempt.completion_targets = {second_target};
        rule.inputs.push_back(attempt);
      }
    }
    ObservationInputDefinition terminal;
    terminal.id = "terminal";
    terminal.kind = ObservationInputKind::RequestTerminal;
    terminal.event_kind = "request_terminal";
    terminal.plan = plan.name;
    terminal.completion_targets = plan.targets;
    rule.inputs.push_back(terminal);
    if (typed_status) {
      EF_ASSIGN_OR_RETURN(snapshot, catalog->snapshot());
      for (auto id : {latest, child}) {
        EF_ASSIGN_OR_RETURN(auto relation, snapshot.find(id));
        rule.reads.push_back({id == latest ? "latest" : "child",
                              id,
                              {relation->storage_schema.fields.begin(),
                               relation->storage_schema.fields.begin() + relation->visible_count}});
      }
    }
    EF_ASSIGN_OR_RETURN(auto observation, catalog->define_observation(rule));
    (void)observation;
    StoreConfig config;
    config.shard_count = 1;
    std::unique_ptr<IStateStore> backend;
    if (backend_kind % 2 == 0) {
      EF_ASSIGN_OR_RETURN(backend, MemoryStateStore::open(config));
    } else {
      char pattern[] = "/tmp/eventflow-request-observation-XXXXXX";
      const auto* created = ::mkdtemp(pattern);
      if (!created)
        return Status(ErrorCode::Unavailable, "temporary directory failed");
      path = created;
      EF_ASSIGN_OR_RETURN(backend, RocksStateStore::open(path + "/db", config));
    }
    if (backend_kind >= 2) {
      EF_ASSIGN_OR_RETURN(backend, RemoteHarnessStore::open(std::move(backend)));
    }
    store = std::make_shared<ObservationStore>(std::move(backend));
    EngineConfig engine_config;
    engine_config.shard_count = 1;
    EF_ASSIGN_OR_RETURN(engine, EngineRuntimeAccess::open(store, take(catalog->snapshot()),
                                                          engine_config, registry));
    EF_ASSIGN_OR_RETURN(observation_key, row_key(0, 'S', 42001, {std::string("observed")}));
    EF_RETURN_IF_ERROR(EngineRuntimeAccess::install_observer(
        *engine, 42001,
        [this](
            RoundContext& round, const ObservationRuleDescriptor&, const RuntimeCause& cause,
            const std::vector<RowChange>& changes) noexcept -> StatusOr<std::vector<TableChange>> {
          ObservationFrame frame;
          frame.cause = cause;
          frame.changes = changes;
          frame.revision = round.revision() + 1;
          frame.now = round.processing_time_us();
          if (typed_status) {
            EF_ASSIGN_OR_RETURN(frame.latest, round.read_relation(latest, {std::string("a")}));
            EF_ASSIGN_OR_RETURN(frame.child, round.read_relation(child, {std::string("a")}));
          }
          frames.push_back(frame);
          EF_RETURN_IF_ERROR(EngineRuntimeAccess::stage_observer_state(
              round, {observation_key, std::to_string(frames.size())}));
          return std::vector<TableChange>{};
        }));
    if (typed_status) {
      EF_ASSIGN_OR_RETURN(
          auto activated,
          EngineRuntimeAccess::apply(
              *engine, {0},
              [&](RoundContext& round) noexcept -> StatusOr<std::vector<TableChange>> {
                EF_ASSIGN_OR_RETURN(auto write,
                                    EngineRuntimeAccess::make_candidate_write(
                                        round, candidate, {std::string("a")},
                                        CandidateOperation::Activate, 0, {std::int64_t{1}}));
                return std::vector<TableChange>{write};
              }));
      if (activated.commit.code != CommitCode::Committed)
        return Status(ErrorCode::UnknownCommit, "activation failed");
    }
    EF_ASSIGN_OR_RETURN(runtime, RequestRuntime::open(*engine, store, registry, plans));
    return {};
  }

  RequestAdmission admission() const noexcept {
    return {"prediction", {std::string("a")}, "r1", {}, typed_status ? 1U : UINT64_MAX, 0, {}};
  }

  FunctionCompletion completion(const FunctionTask& task, Status status_value = {}) const noexcept {
    return {task.task_id,
            task.function_id,
            task.function_version,
            task.implementation_identity,
            status_value,
            Row{std::int64_t{14}},
            false,
            false,
            0};
  }

  RequestRuntimeResult admit() noexcept {
    return take(runtime->admit(admission(), 1));
  }

  std::vector<FunctionTask> claim(std::int64_t now = 2) noexcept {
    return take(runtime->claim({std::string("a")}, "r1", now)).tasks;
  }
};

class RequestObservationContract : public testing::TestWithParam<int> {};

TEST_P(RequestObservationContract,
       AcceptedCompletionObservesFinalLatestAndSqlChildInTheSameRevision) {
  ObservationFixture f;
  ASSERT_TRUE(f.open(GetParam()).ok());
  ASSERT_TRUE(f.admit().accepted);
  const auto tasks = f.claim();
  ASSERT_EQ(tasks.size(), 1U);
  auto result = take(f.runtime->complete({std::string("a")}, "r1", f.completion(tasks[0]), 3));
  ASSERT_EQ(f.frames.size(), 1U);
  const auto& frame = f.frames.back();
  ASSERT_EQ(frame.cause.inputs.size(), 2U);
  EXPECT_EQ(frame.cause.inputs[0].kind, ObservationInputKind::AttemptSettled);
  EXPECT_EQ(frame.cause.inputs[0].values,
            (Row{std::string("prediction"), std::string("r1"), std::string("1"),
                 std::int64_t{f.target}, std::int64_t{1}, std::string("SUCCEEDED")}));
  EXPECT_EQ(frame.cause.inputs[1].kind, ObservationInputKind::RequestTerminal);
  EXPECT_EQ(frame.cause.inputs[1].values, (Row{std::string("prediction"), std::string("r1"),
                                               std::string("1"), std::string("SUCCEEDED")}));
  ASSERT_TRUE(frame.latest);
  ASSERT_TRUE(frame.child);
  EXPECT_EQ(std::get<std::int64_t>((*frame.latest)[0]), 14);
  EXPECT_EQ(std::get<std::int64_t>((*frame.child)[0]), 15);
  EXPECT_EQ(frame.revision, result.commit.revision);
  const auto stored = take(f.store->get_many(0, {f.observation_key}));
  EXPECT_EQ(stored.revision, result.commit.revision);
  EXPECT_EQ(stored.values[0], "1");
  for (const auto& input : frame.cause.inputs) {
    EXPECT_FALSE(input.source);
    EXPECT_EQ(input.origin, "prediction");
    EXPECT_EQ(input.scope, Row{std::string("a")});
  }
  EXPECT_TRUE(
      take(f.runtime->complete({std::string("a")}, "r1", f.completion(tasks[0]), 4)).duplicate);
  EXPECT_EQ(f.frames.size(), 1U);
}

const RuntimeObservationInput& attempt_input(const ObservationFrame& frame,
                                             std::size_t at = 0) noexcept {
  const auto& input = frame.cause.inputs.at(at);
  EXPECT_EQ(input.kind, ObservationInputKind::AttemptSettled);
  EXPECT_EQ(input.values.size(), 6U);
  EXPECT_FALSE(input.source);
  EXPECT_FALSE(input.scheduled_time_us);
  EXPECT_FALSE(input.advance_rule);
  return input;
}

const RuntimeObservationInput& terminal_input(const ObservationFrame& frame) noexcept {
  const auto& input = frame.cause.inputs.back();
  EXPECT_EQ(input.kind, ObservationInputKind::RequestTerminal);
  EXPECT_FALSE(input.relation);
  EXPECT_FALSE(input.source);
  EXPECT_EQ(input.values.size(), 4U);
  return input;
}

TEST_P(RequestObservationContract, PermanentFailureProducesFactsWithoutAnyRelationChange) {
  ObservationFixture f;
  f.typed_status = false;
  f.max_attempts = 1;
  ASSERT_TRUE(f.open(GetParam()).ok());
  f.admit();
  const auto tasks = f.claim();
  ASSERT_EQ(tasks.size(), 1U);
  const auto result = take(
      f.runtime->complete({std::string("a")}, "r1",
                          f.completion(tasks[0], {ErrorCode::InvalidArgument, "bad input"}), 3));
  ASSERT_EQ(f.frames.size(), 1U);
  const auto& frame = f.frames[0];
  EXPECT_TRUE(frame.changes.empty());
  ASSERT_EQ(frame.cause.inputs.size(), 2U);
  EXPECT_EQ(std::get<std::string>(attempt_input(frame).values.back()), "FAILED");
  EXPECT_EQ(std::get<std::string>(terminal_input(frame).values.back()), "FAILED");
  EXPECT_EQ(std::get<std::string>(terminal_input(frame).values[2]), std::to_string(UINT64_MAX));
  EXPECT_EQ(result.record->state.phase, RequestPhase::Failed);
}

TEST_P(RequestObservationContract, RetryFailureAndNewClaimPreserveTheOriginalAttemptIdentity) {
  ObservationFixture f;
  f.typed_status = false;
  ASSERT_TRUE(f.open(GetParam()).ok());
  f.admit();
  const auto tasks = f.claim();
  ASSERT_EQ(tasks.size(), 1U);
  take(f.runtime->complete({std::string("a")}, "r1",
                           f.completion(tasks[0], {ErrorCode::Unavailable, "retry"}), 3));
  ASSERT_EQ(f.frames.size(), 1U);
  ASSERT_EQ(f.frames[0].cause.inputs.size(), 1U);
  EXPECT_TRUE(f.frames[0].changes.empty());
  EXPECT_EQ(std::get<std::string>(attempt_input(f.frames[0]).values.back()), "FAILED");
  auto next = f.claim(3);
  ASSERT_EQ(next.size(), 1U);
  EXPECT_NE(next[0].task_id, tasks[0].task_id);
  EXPECT_EQ(f.frames.size(), 1U);
  EXPECT_TRUE(
      take(f.runtime->complete({std::string("a")}, "r1", f.completion(tasks[0]), 4)).duplicate);
  EXPECT_EQ(f.frames.size(), 1U);
  take(f.runtime->complete({std::string("a")}, "r1", f.completion(next[0]), 5));
  ASSERT_EQ(f.frames.size(), 2U);
  EXPECT_EQ(std::get<std::int64_t>(attempt_input(f.frames[1]).values[4]), 2);
  EXPECT_EQ(std::get<std::string>(attempt_input(f.frames[1]).values.back()), "SUCCEEDED");
  EXPECT_EQ(std::get<std::string>(terminal_input(f.frames[1]).values.back()), "SUCCEEDED");
}

TEST_P(RequestObservationContract,
       ClaimCanTimeOutOldAttemptAndStartNewOneWithoutErasingSettlement) {
  ObservationFixture f;
  f.typed_status = false;
  ASSERT_TRUE(f.open(GetParam()).ok());
  f.admit();
  const auto first = f.claim();
  ASSERT_EQ(first.size(), 1U);
  const auto next = f.claim(12);
  ASSERT_EQ(next.size(), 1U);
  ASSERT_EQ(f.frames.size(), 1U);
  ASSERT_EQ(f.frames[0].cause.inputs.size(), 1U);
  const auto& event = attempt_input(f.frames[0]);
  EXPECT_EQ(std::get<std::int64_t>(event.values[4]), 1);
  EXPECT_EQ(std::get<std::string>(event.values.back()), "TIMED_OUT");
  const auto state = take(f.runtime->read({std::string("a")}, "r1")).state;
  auto current = std::find_if(state.nodes.begin(), state.nodes.end(),
                              [&](const auto& node) noexcept { return node.relation == f.target; });
  ASSERT_NE(current, state.nodes.end());
  EXPECT_EQ(current->attempt, 2U);
  EXPECT_TRUE(current->error.ok());
  EXPECT_TRUE(
      take(f.runtime->complete({std::string("a")}, "r1", f.completion(first[0]), 13)).duplicate);
  EXPECT_EQ(f.frames.size(), 1U);
  take(f.runtime->complete({std::string("a")}, "r1", f.completion(next[0]), 14));
  EXPECT_EQ(f.frames.size(), 2U);
}

TEST_P(RequestObservationContract, MatchingLateCompletionObservesTimeoutInsteadOfWorkerSuccess) {
  ObservationFixture f;
  f.typed_status = false;
  ASSERT_TRUE(f.open(GetParam()).ok());
  f.admit();
  const auto tasks = f.claim();
  ASSERT_EQ(tasks.size(), 1U);
  auto wrong = f.completion(tasks[0]);
  wrong.function_id = "untrusted";
  EXPECT_TRUE(take(f.runtime->complete({std::string("a")}, "r1", wrong, 12)).duplicate);
  EXPECT_TRUE(f.frames.empty());
  take(f.runtime->complete({std::string("a")}, "r1", f.completion(tasks[0]), 12));
  ASSERT_EQ(f.frames.size(), 1U);
  ASSERT_EQ(f.frames[0].cause.inputs.size(), 1U);
  EXPECT_EQ(std::get<std::string>(attempt_input(f.frames[0]).values.back()), "TIMED_OUT");
  EXPECT_TRUE(f.frames[0].changes.empty());
}

TEST_P(RequestObservationContract, CancellationBeforeClaimProducesOnlyOneRequestTerminal) {
  ObservationFixture f;
  f.typed_status = false;
  ASSERT_TRUE(f.open(GetParam()).ok());
  f.admit();
  take(f.runtime->cancel({std::string("a")}, "r1", 2));
  ASSERT_EQ(f.frames.size(), 1U);
  ASSERT_EQ(f.frames[0].cause.inputs.size(), 1U);
  EXPECT_EQ(std::get<std::string>(terminal_input(f.frames[0]).values.back()), "CANCELLED");
  EXPECT_TRUE(f.frames[0].changes.empty());
  EXPECT_TRUE(take(f.runtime->cancel({std::string("a")}, "r1", 3)).duplicate);
  EXPECT_TRUE(take(f.runtime->admit(f.admission(), 4)).duplicate);
  EXPECT_EQ(f.frames.size(), 1U);
}

TEST_P(RequestObservationContract, CancellingMultipleRunningNodesReportsEachAcceptedAttemptOnce) {
  ObservationFixture f;
  f.two_targets = true;
  ASSERT_TRUE(f.open(GetParam()).ok());
  f.admit();
  const auto tasks = f.claim();
  ASSERT_EQ(tasks.size(), 2U);
  take(f.runtime->cancel({std::string("a")}, "r1", 3));
  ASSERT_EQ(f.frames.size(), 1U);
  const auto& frame = f.frames[0];
  ASSERT_EQ(frame.cause.inputs.size(), 3U);
  EXPECT_EQ(attempt_input(frame, 0).relation, f.target);
  EXPECT_EQ(attempt_input(frame, 1).relation, f.second_target);
  EXPECT_EQ(std::get<std::string>(attempt_input(frame, 0).values.back()), "CANCELLED");
  EXPECT_EQ(std::get<std::string>(attempt_input(frame, 1).values.back()), "CANCELLED");
  EXPECT_EQ(std::get<std::string>(terminal_input(frame).values.back()), "CANCELLED");
  for (const auto& task : tasks)
    EXPECT_TRUE(
        take(f.runtime->complete({std::string("a")}, "r1", f.completion(task), 4)).duplicate);
  EXPECT_EQ(f.frames.size(), 1U);
}

TEST_P(RequestObservationContract, FailureCancelsSiblingAndOrdersSettlementsByNodeIdentity) {
  ObservationFixture f;
  f.two_targets = true;
  f.max_attempts = 1;
  ASSERT_TRUE(f.open(GetParam()).ok());
  f.admit();
  const auto tasks = f.claim();
  ASSERT_EQ(tasks.size(), 2U);
  take(f.runtime->complete({std::string("a")}, "r1",
                           f.completion(tasks[1], {ErrorCode::InvalidArgument, "failed second"}),
                           3));
  ASSERT_EQ(f.frames.size(), 1U);
  const auto& frame = f.frames[0];
  ASSERT_EQ(frame.cause.inputs.size(), 3U);
  EXPECT_EQ(attempt_input(frame, 0).relation, f.target);
  EXPECT_EQ(attempt_input(frame, 1).relation, f.second_target);
  EXPECT_EQ(std::get<std::string>(attempt_input(frame, 0).values.back()), "CANCELLED");
  EXPECT_EQ(std::get<std::string>(attempt_input(frame, 1).values.back()), "FAILED");
  EXPECT_EQ(std::get<std::string>(terminal_input(frame).values.back()), "FAILED");
  EXPECT_FALSE(frame.latest);
  EXPECT_FALSE(frame.child);
}

TEST_P(RequestObservationContract, WholeRequestDeadlineSettlesAllRunningAttemptsAsTimedOut) {
  ObservationFixture f;
  f.two_targets = true;
  ASSERT_TRUE(f.open(GetParam()).ok());
  f.admit();
  ASSERT_EQ(f.claim().size(), 2U);
  take(f.runtime->advance({std::string("a")}, "r1", 101));
  ASSERT_EQ(f.frames.size(), 1U);
  const auto& frame = f.frames[0];
  ASSERT_EQ(frame.cause.inputs.size(), 3U);
  EXPECT_EQ(std::get<std::string>(attempt_input(frame, 0).values.back()), "TIMED_OUT");
  EXPECT_EQ(std::get<std::string>(attempt_input(frame, 1).values.back()), "TIMED_OUT");
  EXPECT_EQ(std::get<std::string>(terminal_input(frame).values.back()), "FAILED");
  EXPECT_TRUE(take(f.runtime->advance({std::string("a")}, "r1", 102)).duplicate);
  EXPECT_EQ(f.frames.size(), 1U);
}

TEST_P(RequestObservationContract, AdmissionCanFinishWithoutFunctionAttemptsOrAnyRowChange) {
  ObservationFixture f;
  f.typed_status = false;
  f.immediate_sql = true;
  f.empty_capture = true;
  ASSERT_TRUE(f.open(GetParam()).ok());
  auto result = f.admit();
  ASSERT_TRUE(result.accepted);
  ASSERT_EQ(result.record->state.phase, RequestPhase::Success);
  ASSERT_EQ(f.frames.size(), 1U);
  const auto& frame = f.frames[0];
  ASSERT_EQ(frame.cause.inputs.size(), 1U);
  EXPECT_TRUE(frame.changes.empty());
  EXPECT_EQ(std::get<std::string>(terminal_input(frame).values.back()), "SUCCEEDED");
}

TEST_P(RequestObservationContract, MissingRequiredCaptureCanFailDuringAdmissionWithoutFakeAttempt) {
  ObservationFixture f;
  f.typed_status = false;
  f.empty_capture = true;
  ASSERT_TRUE(f.open(GetParam()).ok());
  auto result = f.admit();
  ASSERT_TRUE(result.accepted);
  ASSERT_EQ(result.record->state.phase, RequestPhase::Failed);
  ASSERT_EQ(f.frames.size(), 1U);
  ASSERT_EQ(f.frames[0].cause.inputs.size(), 1U);
  EXPECT_TRUE(f.frames[0].changes.empty());
  EXPECT_EQ(std::get<std::string>(terminal_input(f.frames[0]).values.back()), "FAILED");
}

TEST_P(RequestObservationContract, UnknownCommitResolvesOriginalFactsWithoutInvokingObserverAgain) {
  ObservationFixture f;
  ASSERT_TRUE(f.open(GetParam()).ok());
  f.admit();
  const auto tasks = f.claim();
  ASSERT_EQ(tasks.size(), 1U);
  f.store->unknown_next = true;
  const auto uncertain =
      take(f.runtime->complete({std::string("a")}, "r1", f.completion(tasks[0]), 3));
  ASSERT_EQ(uncertain.commit.code, CommitCode::Unknown);
  EXPECT_FALSE(uncertain.record);
  ASSERT_EQ(f.frames.size(), 1U);
  const auto final = take(f.runtime->resolve_pending_commit());
  EXPECT_EQ(final.commit.code, CommitCode::Committed);
  EXPECT_EQ(f.frames.size(), 1U);
  const auto stored = take(f.store->get_many(0, {f.observation_key}));
  EXPECT_EQ(stored.values[0], "1");
  EXPECT_EQ(stored.revision, final.commit.revision);
  EXPECT_TRUE(
      take(f.runtime->complete({std::string("a")}, "r1", f.completion(tasks[0]), 4)).duplicate);
  EXPECT_EQ(f.frames.size(), 1U);
}

TEST_P(RequestObservationContract, InvalidPayloadDoesNotPublishFactsAndCacheSuccessUsesNormalPath) {
  ObservationFixture f;
  ASSERT_TRUE(f.open(GetParam()).ok());
  f.admit();
  const auto tasks = f.claim();
  ASSERT_EQ(tasks.size(), 1U);
  auto malformed = f.completion(tasks[0]);
  malformed.output = {std::string("wrong type")};
  const auto before = f.engine->revision();
  EXPECT_FALSE(f.runtime->complete({std::string("a")}, "r1", malformed, 3).ok());
  EXPECT_EQ(f.engine->revision(), before);
  EXPECT_TRUE(f.frames.empty());
  auto cached = f.completion(tasks[0]);
  cached.from_cache = true;
  cached.completed_at_us = INT64_MAX;
  take(f.runtime->complete({std::string("a")}, "r1", cached, 3));
  ASSERT_EQ(f.frames.size(), 1U);
  EXPECT_EQ(f.frames[0].now, 3);
  EXPECT_EQ(std::get<std::string>(attempt_input(f.frames[0]).values.back()), "SUCCEEDED");
}

TEST_P(RequestObservationContract,
       SuccessfulNodeDoesNotImplyWholeRequestSuccessOrLatestPublication) {
  ObservationFixture f;
  f.two_targets = true;
  f.max_attempts = 1;
  ASSERT_TRUE(f.open(GetParam()).ok());
  f.admit();
  const auto tasks = f.claim();
  ASSERT_EQ(tasks.size(), 2U);
  take(f.runtime->complete({std::string("a")}, "r1", f.completion(tasks[0]), 3));
  ASSERT_EQ(f.frames.size(), 1U);
  ASSERT_EQ(f.frames[0].cause.inputs.size(), 1U);
  EXPECT_EQ(std::get<std::string>(attempt_input(f.frames[0]).values.back()), "SUCCEEDED");
  EXPECT_FALSE(f.frames[0].latest);
  EXPECT_FALSE(f.frames[0].child);
  take(f.runtime->complete({std::string("a")}, "r1",
                           f.completion(tasks[1], {ErrorCode::InvalidArgument, "bad second"}), 4));
  ASSERT_EQ(f.frames.size(), 2U);
  ASSERT_EQ(f.frames[1].cause.inputs.size(), 2U);
  EXPECT_EQ(std::get<std::string>(terminal_input(f.frames[1]).values.back()), "FAILED");
  EXPECT_FALSE(f.frames[1].latest);
  EXPECT_FALSE(f.frames[1].child);
}

TEST_P(RequestObservationContract,
       WorkerCancellationIsAnAcceptedCancelledAttemptWithFailedRequest) {
  ObservationFixture f;
  ASSERT_TRUE(f.open(GetParam()).ok());
  f.admit();
  const auto tasks = f.claim();
  ASSERT_EQ(tasks.size(), 1U);
  auto cancelled = f.completion(tasks[0]);
  cancelled.cancelled = true;
  take(f.runtime->complete({std::string("a")}, "r1", cancelled, 3));
  ASSERT_EQ(f.frames.size(), 1U);
  ASSERT_EQ(f.frames[0].cause.inputs.size(), 2U);
  EXPECT_EQ(std::get<std::string>(attempt_input(f.frames[0]).values.back()), "CANCELLED");
  EXPECT_EQ(std::get<std::string>(terminal_input(f.frames[0]).values.back()), "FAILED");
}

TEST_P(RequestObservationContract,
       SuccessfulFilteredTargetKeepsSuccessOutcomeWithoutPublishingLatest) {
  ObservationFixture f;
  f.filtered_sql = true;
  ASSERT_TRUE(f.open(GetParam()).ok());
  f.admit();
  const auto tasks = f.claim();
  ASSERT_EQ(tasks.size(), 1U);
  const auto result =
      take(f.runtime->complete({std::string("a")}, "r1", f.completion(tasks[0]), 3));
  ASSERT_EQ(result.record->state.phase, RequestPhase::Success);
  ASSERT_EQ(f.frames.size(), 1U);
  ASSERT_EQ(f.frames[0].cause.inputs.size(), 2U);
  EXPECT_EQ(std::get<std::string>(attempt_input(f.frames[0]).values.back()), "SUCCEEDED");
  EXPECT_EQ(std::get<std::string>(terminal_input(f.frames[0]).values.back()), "SUCCEEDED");
  EXPECT_FALSE(f.frames[0].latest);
  EXPECT_FALSE(f.frames[0].child);
}

TEST_P(RequestObservationContract, RefreshFailureMergesSiblingCancellationAfterAcceptedSuccess) {
  ObservationFixture f;
  f.two_targets = true;
  f.filtered_sql = true;
  f.require_filtered_result = true;
  ASSERT_TRUE(f.open(GetParam()).ok());
  f.admit();
  const auto tasks = f.claim();
  ASSERT_EQ(tasks.size(), 2U);
  const auto result =
      take(f.runtime->complete({std::string("a")}, "r1", f.completion(tasks[0]), 3));
  ASSERT_EQ(result.record->state.phase, RequestPhase::Failed);
  ASSERT_EQ(f.frames.size(), 1U);
  ASSERT_EQ(f.frames[0].cause.inputs.size(), 3U);
  EXPECT_EQ(std::get<std::string>(attempt_input(f.frames[0], 0).values.back()), "SUCCEEDED");
  EXPECT_EQ(attempt_input(f.frames[0], 1).relation, f.second_target);
  EXPECT_EQ(std::get<std::string>(attempt_input(f.frames[0], 1).values.back()), "CANCELLED");
  EXPECT_EQ(std::get<std::string>(terminal_input(f.frames[0]).values.back()), "FAILED");
  EXPECT_FALSE(f.frames[0].latest);
  EXPECT_FALSE(f.frames[0].child);
}

INSTANTIATE_TEST_SUITE_P(FourKvAdapters, RequestObservationContract, testing::Values(0, 1, 2, 3));
} // namespace
} // namespace eventflow::test
