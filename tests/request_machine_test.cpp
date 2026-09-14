#include "eventflow/request_machine.hpp"
#include "status_test_support.hpp"
#include <gtest/gtest.h>
#include <map>

namespace eventflow {
namespace {
using test::take;

class RequestMachineTest : public testing::Test {
protected:
  void SetUp() noexcept override {
    catalog = take(Catalog::create());
    registry = take(FunctionRegistry::create());
    DefinitionOptions options;
    options.base_write_role = BaseWriteRole::RequestCapture;
    options.identity_domain = "requests";
    capture = take(catalog->define_sql("CREATE TABLE capture(k STRING, request_id STRING, x INT64 "
                                       "NOT NULL, PRIMARY KEY(k,request_id)) PARTITION BY(k)",
                                       options));
    projected = take(catalog->define_sql(
        "CREATE MATERIALIZED VIEW projected AS SELECT x FROM capture WHERE x >= 0"));
    shared = add_function("shared", {projected}, "value",
                          [this](const FunctionInputs& inputs) noexcept -> StatusOr<Row> {
                            ++calls[shared];
                            return Row{*std::get_if<std::int64_t>(&(*inputs[0])[0]) + 1};
                          });
    branch = take(catalog->define_sql(
        "CREATE MATERIALIZED VIEW branch AS SELECT value AS y FROM shared WHERE value > 0"));
    left = add_function("left_branch", {branch}, "left_value",
                        [this](const FunctionInputs& inputs) noexcept -> StatusOr<Row> {
                          ++calls[left];
                          return Row{*std::get_if<std::int64_t>(&(*inputs[0])[0]) + 10};
                        });
    right = add_function("right_branch", {branch}, "right_value",
                         [this](const FunctionInputs& inputs) noexcept -> StatusOr<Row> {
                           ++calls[right];
                           return Row{*std::get_if<std::int64_t>(&(*inputs[0])[0]) + 20};
                         });
    combined = add_function("combined", {left, right}, "total",
                            [this](const FunctionInputs& inputs) noexcept -> StatusOr<Row> {
                              ++calls[combined];
                              return Row{*std::get_if<std::int64_t>(&(*inputs[0])[2]) +
                                         *std::get_if<std::int64_t>(&(*inputs[1])[2])};
                            });
    target =
        take(catalog->define_sql("CREATE MATERIALIZED VIEW final AS SELECT total FROM combined"));
    snapshot = take(catalog->snapshot());
    machine = take(RequestMachine::create(snapshot, registry, {target}));
    rows[capture] = Row{std::string("a"), std::string("req-1"), std::int64_t{3}};
    stabilize_sql();
  }

  RelationId add_function(const std::string& name, const std::vector<RelationId>& inputs,
                          const std::string& output, BuiltinFunctionCallback callback,
                          bool required = true) noexcept {
    const auto current = take(catalog->snapshot());
    FunctionViewDefinition definition;
    definition.name = name;
    definition.function_id = name;
    definition.function_version = 1;
    definition.implementation_identity = "builtin:" + name + ":1";
    definition.output_fields = {{1, output, DataType::Int64, false}};
    FunctionSignature signature;
    signature.function_id = name;
    signature.version = 1;
    signature.implementation_identity = definition.implementation_identity;
    signature.output_fields = definition.output_fields;
    for (std::size_t i = 0; i < inputs.size(); ++i) {
      const auto* source = take(current.find(inputs[i]));
      const std::vector<Field> fields(source->storage_schema.fields.begin(),
                                      source->storage_schema.fields.begin() +
                                          source->visible_count);
      const auto alias = "input_" + std::to_string(i);
      definition.inputs.push_back({alias, inputs[i], required, fields});
      signature.inputs.push_back({alias, required, fields});
    }
    EXPECT_TRUE(registry->add({std::move(signature), std::move(callback)}).ok());
    return take(catalog->define_function(definition));
  }

  RequestManifest manifest() const noexcept {
    RequestManifest result;
    result.scope = {std::string("a")};
    result.identity = {std::string("a"), std::string("req-1")};
    result.request_id = "req-1";
    result.admission_seq = 1;
    result.candidate_epoch = 4;
    result.owner_token = "owner-7";
    result.owner_epoch = 7;
    result.admitted_at_us = 10;
    result.deadline_us = 1'000'000;
    return result;
  }

  ReadRequestRow reader() noexcept {
    return [this](RelationId relation, const Row&) noexcept -> StatusOr<std::optional<Row>> {
      const auto found = rows.find(relation);
      return found == rows.end() ? std::optional<Row>{} : std::optional<Row>(found->second);
    };
  }

  void stabilize_sql() noexcept {
    const auto current = take(catalog->snapshot());
    for (const auto& relation : current.relations) {
      if (relation->kind != RelationKind::SqlView)
        continue;
      const auto parent = rows.find(relation->plan->left);
      if (parent == rows.end()) {
        rows.erase(relation->id);
        continue;
      }
      const auto* source = take(current.find(relation->plan->left));
      const auto visible = take(visible_row(*source, parent->second));
      if (!take(passes_filter(relation->plan->filter, visible))) {
        rows.erase(relation->id);
        continue;
      }
      const auto projected_row = take(evaluate_projection(*relation->plan, visible));
      const auto identity = take(physical_identity(*source, parent->second));
      rows[relation->id] = take(make_stored_row(*relation, projected_row, identity));
    }
  }

  const RequestNodeState& node(const RequestMachineState& state, RelationId id) const noexcept {
    for (const auto& value : state.nodes)
      if (value.relation == id)
        return value;
    std::abort();
  }

  RequestMachineState ready_state() noexcept {
    const auto initial = take(machine->begin(manifest()));
    return take(machine->refresh(initial, reader())).state;
  }

  static FunctionCompletion completion(const FunctionTask& task, std::int64_t value = 4) noexcept {
    FunctionCompletion result;
    result.task_id = task.task_id;
    result.function_id = task.function_id;
    result.function_version = task.function_version;
    result.implementation_identity = task.implementation_identity;
    result.output = {value};
    return result;
  }

  RequestMachineState stage_and_refresh(RequestMachineTransition transition) noexcept {
    if (transition.output) {
      EXPECT_TRUE(transition.output->row.has_value());
      rows[transition.output->relation] = *transition.output->row;
    }
    stabilize_sql();
    return take(machine->refresh(transition.state, reader())).state;
  }

  std::unique_ptr<Catalog> catalog;
  std::shared_ptr<FunctionRegistry> registry;
  std::unique_ptr<RequestMachine> machine;
  CatalogSnapshot snapshot;
  RelationId capture = 0, projected = 0, shared = 0, branch = 0, left = 0, right = 0, combined = 0,
             target = 0;
  std::map<RelationId, Row> rows;
  std::map<RelationId, std::size_t> calls;
};

TEST_F(RequestMachineTest, BeginDerivesOnlyTheCatalogAncestorNodesAndFreezesManifest) {
  auto state = machine->begin(manifest());
  ASSERT_TRUE(state.ok()) << state.status().message();
  EXPECT_EQ(state->phase, RequestPhase::Accepted);
  EXPECT_EQ(state->manifest.catalog_fingerprint, snapshot.fingerprint);
  EXPECT_EQ(state->manifest.targets, (std::vector<RelationId>{target}));
  EXPECT_EQ(state->now_us, 10);
  ASSERT_EQ(state->nodes.size(), snapshot.relations.size());
  for (const auto& current : state->nodes) {
    EXPECT_EQ(current.phase, RequestNodePhase::Waiting);
    EXPECT_EQ(current.attempt, 0U);
    EXPECT_FALSE(current.has_output);
  }
}

TEST_F(RequestMachineTest, CatalogDiamondExecutesSharedAncestorOnceThroughActualSimpleExecutor) {
  auto state = ready_state();
  EXPECT_EQ(node(state, capture).phase, RequestNodePhase::Succeeded);
  EXPECT_EQ(node(state, projected).phase, RequestNodePhase::Succeeded);
  EXPECT_EQ(node(state, shared).phase, RequestNodePhase::Ready);
  auto executor = take(SimpleFunctionExecutor::create(registry));
  std::int64_t now = 10;
  for (int round = 0; round < 8 && state.phase != RequestPhase::Success; ++round) {
    auto claimed = take(machine->claim_ready(state, ++now));
    state = std::move(claimed.state);
    ASSERT_FALSE(claimed.tasks.empty());
    for (const auto& task : claimed.tasks) {
      const auto submitted = executor->submit(task);
      ASSERT_TRUE(submitted.ok()) << submitted.message();
    }
    EXPECT_EQ(take(executor->run_ready(now)), claimed.tasks.size());
    auto completed = take(executor->poll(16));
    for (const auto& result : completed) {
      auto prepared = take(machine->prepare_completion(state, result, now));
      ASSERT_EQ(prepared.disposition, CompletionDisposition::Accepted);
      ASSERT_TRUE(prepared.output);
      EXPECT_EQ(prepared.output->identity, manifest().identity);
      EXPECT_EQ((*prepared.output->row)[0], Value(std::string("a")));
      EXPECT_EQ((*prepared.output->row)[1], Value(std::string("req-1")));
      state = stage_and_refresh(std::move(prepared));
    }
  }
  EXPECT_EQ(state.phase, RequestPhase::Success);
  EXPECT_EQ(calls[shared], 1U);
  EXPECT_EQ(calls[left], 1U);
  EXPECT_EQ(calls[right], 1U);
  EXPECT_EQ(calls[combined], 1U);
  EXPECT_EQ(rows[target][0], Value(std::int64_t{38}));
  EXPECT_TRUE(take(machine->outbox(state)).empty());
  const auto frozen = take(machine->encode(state));
  EXPECT_EQ(take(machine->encode(take(machine->decode(frozen, manifest())))), frozen);
}

TEST_F(RequestMachineTest, RefreshDistinguishesCompleteSqlWithoutOutputFromWaiting) {
  rows[capture][2] = std::int64_t{-1};
  stabilize_sql();
  const auto state = ready_state();
  EXPECT_EQ(node(state, projected).phase, RequestNodePhase::Succeeded);
  EXPECT_FALSE(node(state, projected).has_output);
  EXPECT_EQ(node(state, shared).phase, RequestNodePhase::Skipped);
  EXPECT_EQ(node(state, target).phase, RequestNodePhase::Skipped);
  EXPECT_EQ(state.phase, RequestPhase::Failed);
  EXPECT_TRUE(take(machine->outbox(state)).empty());
}

TEST_F(RequestMachineTest, OptionalSuccessfulAbsenceIsFrozenAsNullInput) {
  const auto optional = add_function(
      "optional", {projected}, "value",
      [](const FunctionInputs& inputs) noexcept -> StatusOr<Row> {
        return Row{std::int64_t(inputs[0] ? 1 : 0)};
      },
      false);
  snapshot = take(catalog->snapshot());
  machine = take(RequestMachine::create(snapshot, registry, {optional}));
  rows[capture][2] = std::int64_t{-1};
  stabilize_sql();
  auto claimed = take(machine->claim_ready(ready_state(), 11));
  ASSERT_EQ(claimed.tasks.size(), 1U);
  ASSERT_EQ(claimed.tasks[0].inputs.size(), 1U);
  EXPECT_FALSE(claimed.tasks[0].inputs[0]);
  const auto done =
      take(machine->prepare_completion(claimed.state, completion(claimed.tasks[0], 0), 12));
  EXPECT_EQ(done.state.phase, RequestPhase::Success);
}

TEST_F(RequestMachineTest, RefreshReadFailureAndWrongIdentityLeaveInputStateUnchanged) {
  const auto initial = take(machine->begin(manifest()));
  const auto before = take(machine->encode(initial));
  std::size_t reads = 0;
  ReadRequestRow failing =
      [this, &reads](RelationId id, const Row& identity) noexcept -> StatusOr<std::optional<Row>> {
    if (++reads == 2)
      return Status(ErrorCode::Unavailable, "injected read error");
    return reader()(id, identity);
  };
  EXPECT_EQ(machine->refresh(initial, failing).status().code(), ErrorCode::Unavailable);
  EXPECT_EQ(take(machine->encode(initial)), before);
  rows[capture][0] = std::string("other-scope");
  EXPECT_FALSE(machine->refresh(initial, reader()).ok());
  EXPECT_EQ(take(machine->encode(initial)), before);
}

TEST_F(RequestMachineTest, CompletionOnlyPreparesOwnedRowAndRequiresSqlRefreshForDescendants) {
  const auto initial = ready_state();
  const auto claimed = take(machine->claim_ready(initial, 11));
  const auto before = take(machine->encode(claimed.state));
  const auto prepared =
      take(machine->prepare_completion(claimed.state, completion(claimed.tasks[0]), 12));
  ASSERT_TRUE(prepared.output);
  EXPECT_FALSE(rows.contains(shared));
  EXPECT_EQ(node(prepared.state, shared).phase, RequestNodePhase::Succeeded);
  EXPECT_EQ(node(prepared.state, branch).phase, RequestNodePhase::Waiting);
  EXPECT_TRUE(take(machine->claim_ready(prepared.state, 12)).tasks.empty());
  EXPECT_FALSE(machine->refresh(prepared.state, reader()).ok());
  EXPECT_EQ(take(machine->encode(claimed.state)), before);
  const auto refreshed = stage_and_refresh(prepared);
  EXPECT_EQ(node(refreshed, branch).phase, RequestNodePhase::Succeeded);
  EXPECT_EQ(node(refreshed, left).phase, RequestNodePhase::Ready);
  EXPECT_EQ(node(refreshed, right).phase, RequestNodePhase::Ready);
}

TEST_F(RequestMachineTest, RetryKeepsFrozenInputsAndFencesThePreviousAttempt) {
  RequestMachineConfig config;
  config.attempt_timeout_us = 20;
  config.retry_backoff_us = 5;
  config.max_attempts = 2;
  machine = take(RequestMachine::create(snapshot, registry, {target}, config));
  const auto first = take(machine->claim_ready(ready_state(), 11));
  auto forged = first.state;
  forged.now_us = 31;
  EXPECT_FALSE(machine->encode(forged).ok());
  forged = first.state;
  ++forged.nodes[2].attempt_deadline_us;
  EXPECT_FALSE(machine->encode(forged).ok());
  rows[projected][0] = std::int64_t{999};
  const auto expired = take(machine->advance_time(first.state, 31));
  EXPECT_EQ(node(expired.state, shared).phase, RequestNodePhase::Ready);
  EXPECT_EQ(expired.cancel_task_ids, (std::vector<std::string>{first.tasks[0].task_id}));
  EXPECT_TRUE(take(machine->claim_ready(expired.state, 35)).tasks.empty());
  auto late = take(machine->prepare_completion(expired.state, completion(first.tasks[0]), 32));
  EXPECT_EQ(late.disposition, CompletionDisposition::Ignored);
  EXPECT_EQ(take(machine->encode(late.state)), take(machine->encode(expired.state)));
  const auto second = take(machine->claim_ready(expired.state, 36));
  ASSERT_EQ(second.tasks.size(), 1U);
  EXPECT_NE(second.tasks[0].task_id, first.tasks[0].task_id);
  EXPECT_EQ(second.tasks[0].inputs, first.tasks[0].inputs);
  EXPECT_EQ(second.tasks[0].ready_at_us, 36);
  EXPECT_EQ(node(second.state, shared).attempt, 2U);
  late = take(machine->prepare_completion(second.state, completion(first.tasks[0]), 37));
  EXPECT_EQ(late.disposition, CompletionDisposition::Ignored);
  EXPECT_FALSE(late.output);
  const auto accepted =
      take(machine->prepare_completion(second.state, completion(second.tasks[0]), 37));
  EXPECT_EQ(accepted.disposition, CompletionDisposition::Accepted);
  EXPECT_EQ((*accepted.output->row)[2], Value(std::int64_t{4}));
}

TEST_F(RequestMachineTest, DeadlineEqualityUsesOwnerClockBeforeWorkerSuccess) {
  auto bounded = manifest();
  bounded.deadline_us = 31;
  const auto ready = take(machine->refresh(take(machine->begin(bounded)), reader())).state;
  const auto claimed = take(machine->claim_ready(ready, 11));
  auto response = completion(claimed.tasks[0]);
  response.completed_at_us = 12;
  const auto expired = take(machine->prepare_completion(claimed.state, response, 31));
  EXPECT_EQ(expired.disposition, CompletionDisposition::Ignored);
  EXPECT_EQ(expired.state.phase, RequestPhase::Failed);
  EXPECT_FALSE(expired.output);
  EXPECT_TRUE(take(machine->outbox(expired.state)).empty());
  response.completed_at_us = 9'999'999;
  const auto accepted = take(machine->prepare_completion(claimed.state, response, 12));
  EXPECT_EQ(accepted.disposition, CompletionDisposition::Accepted);
  EXPECT_EQ(accepted.state.now_us, 12);
}

TEST_F(RequestMachineTest, MaxAttemptsTerminatesAndReleasesTheRequestOutbox) {
  RequestMachineConfig config;
  config.attempt_timeout_us = 20;
  config.max_attempts = 1;
  machine = take(RequestMachine::create(snapshot, registry, {target}, config));
  const auto claimed = take(machine->claim_ready(ready_state(), 11));
  const auto failed = take(machine->advance_time(claimed.state, 31));
  EXPECT_EQ(failed.state.phase, RequestPhase::Failed);
  EXPECT_EQ(node(failed.state, shared).phase, RequestNodePhase::Failed);
  EXPECT_EQ(node(failed.state, target).phase, RequestNodePhase::Skipped);
  EXPECT_TRUE(take(machine->outbox(failed.state)).empty());
  EXPECT_EQ(failed.cancel_task_ids.size(), 1U);
  EXPECT_TRUE(take(machine->claim_ready(failed.state, 32)).tasks.empty());
}

TEST_F(RequestMachineTest, PermanentBranchFailureCancelsOtherRunningBranches) {
  auto claimed = take(machine->claim_ready(ready_state(), 11));
  auto state = stage_and_refresh(
      take(machine->prepare_completion(claimed.state, completion(claimed.tasks[0]), 12)));
  claimed = take(machine->claim_ready(state, 13));
  ASSERT_EQ(claimed.tasks.size(), 2U);
  auto failure = completion(claimed.tasks[0]);
  failure.status = Status(ErrorCode::InvalidArgument, "permanent function error");
  const auto failed = take(machine->prepare_completion(claimed.state, failure, 14));
  EXPECT_EQ(failed.disposition, CompletionDisposition::Accepted);
  EXPECT_EQ(failed.state.phase, RequestPhase::Failed);
  EXPECT_FALSE(failed.output);
  EXPECT_EQ(failed.cancel_task_ids, (std::vector<std::string>{claimed.tasks[1].task_id}));
  EXPECT_TRUE(take(machine->outbox(failed.state)).empty());
}

TEST_F(RequestMachineTest, CancellationAndDuplicateOrWrongVersionCompletionsHaveNoWrites) {
  const auto claimed = take(machine->claim_ready(ready_state(), 11));
  const auto before = take(machine->encode(claimed.state));
  for (int field = 0; field < 3; ++field) {
    auto wrong = completion(claimed.tasks[0]);
    if (field == 0)
      wrong.task_id += "forged";
    if (field == 1)
      ++wrong.function_version;
    if (field == 2)
      wrong.implementation_identity += "forged";
    const auto ignored = take(machine->prepare_completion(claimed.state, wrong, 12));
    EXPECT_EQ(ignored.disposition, CompletionDisposition::Ignored);
    EXPECT_FALSE(ignored.output);
    EXPECT_EQ(take(machine->encode(ignored.state)), before);
  }
  const auto cancelled = take(machine->cancel(claimed.state, 12));
  EXPECT_EQ(cancelled.state.phase, RequestPhase::Cancelled);
  EXPECT_EQ(cancelled.cancel_task_ids.size(), 1U);
  EXPECT_TRUE(take(machine->outbox(cancelled.state)).empty());
  const auto ignored =
      take(machine->prepare_completion(cancelled.state, completion(claimed.tasks[0]), 13));
  EXPECT_EQ(ignored.disposition, CompletionDisposition::Ignored);
  EXPECT_FALSE(ignored.output);
  const auto accepted =
      take(machine->prepare_completion(claimed.state, completion(claimed.tasks[0]), 12));
  const auto duplicate =
      take(machine->prepare_completion(accepted.state, completion(claimed.tasks[0]), 13));
  EXPECT_EQ(duplicate.disposition, CompletionDisposition::Ignored);
  EXPECT_EQ(take(machine->encode(duplicate.state)), take(machine->encode(accepted.state)));
}

TEST_F(RequestMachineTest, ClaimBudgetKeepsUnclaimedReadyNodesAndDoesNotDuplicateRunningTasks) {
  RequestMachineConfig config;
  config.max_running_tasks = 1;
  machine = take(RequestMachine::create(snapshot, registry, {target}, config));
  auto first = take(machine->claim_ready(ready_state(), 11));
  auto state = stage_and_refresh(
      take(machine->prepare_completion(first.state, completion(first.tasks[0]), 12)));
  EXPECT_TRUE(take(machine->claim_ready(state, 13, 0)).tasks.empty());
  auto one = take(machine->claim_ready(state, 13, 64));
  ASSERT_EQ(one.tasks.size(), 1U);
  EXPECT_EQ(node(one.state, left).phase, RequestNodePhase::Running);
  EXPECT_EQ(node(one.state, right).phase, RequestNodePhase::Ready);
  EXPECT_TRUE(take(machine->claim_ready(one.state, 14, 64)).tasks.empty());
  EXPECT_EQ(take(machine->outbox(one.state)).size(), 1U);
}

TEST_F(RequestMachineTest, CodecRebuildsExactRunningTasksAndRejectsStateAndManifestForgery) {
  const auto claimed = take(machine->claim_ready(ready_state(), 11));
  const auto encoded = take(machine->encode(claimed.state));
  const auto restored = take(machine->decode(encoded, manifest()));
  const auto tasks = take(machine->outbox(restored));
  ASSERT_EQ(tasks.size(), 1U);
  EXPECT_EQ(tasks[0].task_id, claimed.tasks[0].task_id);
  EXPECT_EQ(tasks[0].inputs, claimed.tasks[0].inputs);
  EXPECT_EQ(tasks[0].ready_at_us, claimed.tasks[0].ready_at_us);
  auto wrong_manifest = manifest();
  wrong_manifest.owner_epoch = 8;
  EXPECT_FALSE(machine->decode(encoded, wrong_manifest).ok());
  wrong_manifest = manifest();
  wrong_manifest.request_id = "other";
  wrong_manifest.identity.back() = std::string("other");
  EXPECT_FALSE(machine->decode(encoded, wrong_manifest).ok());
  for (int mutation = 0; mutation < 8; ++mutation) {
    auto json = take(parse_json_exact(encoded));
    if (mutation == 0)
      json["format"] = 2;
    if (mutation == 1)
      json["phase"] = static_cast<int>(RequestPhase::Success);
    if (mutation == 2)
      json["nodes"][2]["attempt"] = 2;
    if (mutation == 3)
      json["nodes"][2]["task_id"] = "forged";
    if (mutation == 4)
      json["nodes"][2]["frozen_inputs"][0][0] = 999;
    if (mutation == 5)
      json["nodes"][0]["relation"] = 999;
    if (mutation == 6)
      json["extra"] = true;
    if (mutation == 7)
      json["nodes"][2]["frozen_inputs"][0][0] = "wrong-type";
    EXPECT_FALSE(machine->decode(json.dump(), manifest()).ok()) << mutation;
  }
  EXPECT_FALSE(machine->decode(" " + encoded, manifest()).ok());
  EXPECT_FALSE(machine->decode(std::string(100, '[') + std::string(100, ']'), manifest()).ok());
}

TEST_F(RequestMachineTest, SourceCaptureRevisionAndAdmissionCommitRemainDistinctInCodecAndTask) {
  auto request = manifest();
  request.capture_revision = 41;
  request.admission_revision = 42;
  const auto ready = take(machine->refresh(take(machine->begin(request)), reader())).state;
  const auto claimed = take(machine->claim_ready(ready, 11));
  const auto task_json = take(parse_json_exact(claimed.tasks[0].task_id));
  EXPECT_EQ(task_json["manifest"]["capture_revision"], 41U);
  EXPECT_EQ(task_json["manifest"]["admission_revision"], 42U);
  const auto encoded = take(machine->encode(claimed.state));
  EXPECT_TRUE(machine->decode(encoded, request).ok());
  request.capture_revision = 42;
  request.admission_revision = 43;
  EXPECT_FALSE(machine->decode(encoded, request).ok());
  request.admission_revision = 42;
  EXPECT_FALSE(machine->begin(request).ok());
}

TEST_F(RequestMachineTest, TypeInputOutputAndStateBudgetsFailAtomically) {
  const auto initial = take(machine->begin(manifest()));
  const auto before = take(machine->encode(initial));
  RequestMachineConfig config;
  config.max_frozen_input_bytes = 3;
  auto small_input = take(RequestMachine::create(snapshot, registry, {target}, config));
  EXPECT_EQ(small_input->refresh(initial, reader()).status().code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(take(machine->encode(initial)), before);
  auto ready = ready_state();
  config = {};
  config.max_state_bytes = take(machine->encode(ready)).size() + 8;
  auto small_state = take(RequestMachine::create(snapshot, registry, {target}, config));
  EXPECT_EQ(small_state->claim_ready(ready, 11).status().code(), ErrorCode::ResourceExhausted);
  config = {};
  config.max_output_bytes = 2;
  auto small_output = take(RequestMachine::create(snapshot, registry, {target}, config));
  const auto claimed = take(machine->claim_ready(ready, 11));
  EXPECT_EQ(small_output->prepare_completion(claimed.state, completion(claimed.tasks[0]), 12)
                .status()
                .code(),
            ErrorCode::ResourceExhausted);
  auto malformed = completion(claimed.tasks[0]);
  malformed.output = {std::string("bad")};
  EXPECT_EQ(machine->prepare_completion(claimed.state, malformed, 12).status().code(),
            ErrorCode::InvalidArgument);
  EXPECT_EQ(node(claimed.state, shared).phase, RequestNodePhase::Running);
}

TEST_F(RequestMachineTest, CreateChecksRegistrySignatureGraphDomainAndNodeBudget) {
  auto empty = take(FunctionRegistry::create());
  EXPECT_FALSE(RequestMachine::create(snapshot, empty, {shared}).ok());
  auto wrong = take(FunctionRegistry::create());
  auto signature = take(registry->describe("shared", 1));
  signature.implementation_identity = "wrong-version";
  ASSERT_TRUE(wrong
                  ->add({signature,
                         [](const FunctionInputs&) noexcept -> StatusOr<Row> {
                           return Row{std::int64_t{1}};
                         }})
                  .ok());
  EXPECT_FALSE(RequestMachine::create(snapshot, wrong, {shared}).ok());
  EXPECT_FALSE(RequestMachine::create(snapshot, registry, {target, target}).ok());
  EXPECT_FALSE(RequestMachine::create(snapshot, registry, {999}).ok());
  RequestMachineConfig config;
  config.max_nodes = 3;
  EXPECT_EQ(RequestMachine::create(snapshot, registry, {target}, config).status().code(),
            ErrorCode::ResourceExhausted);
  const auto ordinary =
      take(catalog->define_sql("CREATE TABLE ordinary(k STRING, request_id STRING, PRIMARY "
                               "KEY(k,request_id)) PARTITION BY(k)"));
  EXPECT_FALSE(RequestMachine::create(take(catalog->snapshot()), registry, {ordinary}).ok());
  const auto encoded = take(machine->encode(take(machine->begin(manifest()))));
  EXPECT_TRUE(machine->decode(encoded, manifest()).ok());
}

TEST_F(RequestMachineTest, ManifestLifetimeIdentityAndMonotonicTimeAreValidated) {
  for (int mutation = 0; mutation < 6; ++mutation) {
    auto invalid = manifest();
    if (mutation == 0)
      invalid.scope[0] = std::string("other");
    if (mutation == 1)
      invalid.request_id = "other";
    if (mutation == 2)
      invalid.owner_token.clear();
    if (mutation == 3)
      invalid.admission_seq = 0;
    if (mutation == 4)
      invalid.deadline_us = invalid.admitted_at_us;
    if (mutation == 5)
      invalid.deadline_us = 4'000'000'000;
    EXPECT_FALSE(machine->begin(invalid).ok()) << mutation;
  }
  const auto state = ready_state();
  EXPECT_FALSE(machine->advance_time(state, 9).ok());
  EXPECT_FALSE(machine->claim_ready(state, 9).ok());
  EXPECT_FALSE(machine->cancel(state, 9).ok());
}

TEST_F(RequestMachineTest, BeginRejectsIdentityThatCouldNeverFitAnExecutorTask) {
  auto request = manifest();
  request.request_id = std::string(1024, 'r');
  request.owner_token = std::string(1024, 'o');
  request.scope = {std::string(1024, 's')};
  request.identity = {std::string(1024, 's'), request.request_id};
  EXPECT_EQ(machine->begin(request).status().code(), ErrorCode::ResourceExhausted);
}

TEST_F(RequestMachineTest, PublicManifestCodecSupportsOwnerArchivesAndRejectsMalformedFields) {
  auto request = take(machine->begin(manifest())).manifest;
  request.capture_revision = 41;
  request.admission_revision = 42;
  auto json = request_manifest_to_json(request);
  ASSERT_TRUE(json.ok()) << json.status().message();
  auto restored = request_manifest_from_json(*json);
  ASSERT_TRUE(restored.ok()) << restored.status().message();
  EXPECT_EQ(restored->identity, request.identity);
  EXPECT_EQ(restored->targets, request.targets);
  EXPECT_EQ(restored->catalog_fingerprint, request.catalog_fingerprint);
  EXPECT_EQ(restored->capture_revision, 41U);
  EXPECT_EQ(restored->admission_revision, 42U);
  auto malformed = *json;
  malformed["owner_epoch"] = "not-an-integer";
  EXPECT_FALSE(request_manifest_from_json(malformed).ok());
  malformed = *json;
  malformed["identity"][0] = "different";
  EXPECT_FALSE(request_manifest_from_json(malformed).ok());
  malformed = *json;
  malformed["capture_revision"] = 42;
  EXPECT_FALSE(request_manifest_from_json(malformed).ok());
  malformed = *json;
  malformed["extra"] = true;
  EXPECT_FALSE(request_manifest_from_json(malformed).ok());
}

TEST_F(RequestMachineTest, AdmissionReservesEnoughStateBytesForCancellationAndTimeout) {
  const auto admitted = take(machine->begin(manifest()));
  const auto initial_bytes = take(machine->encode(admitted)).size();
  RequestMachineConfig config;
  config.max_state_bytes = initial_bytes;
  auto tight = take(RequestMachine::create(snapshot, registry, {target}, config));
  EXPECT_EQ(tight->begin(manifest()).status().code(), ErrorCode::ResourceExhausted);
  EXPECT_FALSE(tight->decode(take(machine->encode(admitted)), manifest()).ok());

  std::size_t lower = initial_bytes, upper = RequestMachineConfig{}.max_state_bytes;
  while (lower < upper) {
    const auto middle = lower + (upper - lower) / 2;
    config.max_state_bytes = middle;
    auto candidate = take(RequestMachine::create(snapshot, registry, {target}, config));
    if (candidate->begin(manifest()).ok())
      upper = middle;
    else
      lower = middle + 1;
  }
  config.max_state_bytes = lower;
  auto minimum = take(RequestMachine::create(snapshot, registry, {target}, config));
  const auto accepted = take(minimum->begin(manifest()));
  auto cancelled = minimum->cancel(accepted, 11);
  ASSERT_TRUE(cancelled.ok()) << cancelled.status().message();
  EXPECT_EQ(cancelled->state.phase, RequestPhase::Cancelled);
  EXPECT_LE(take(minimum->encode(cancelled->state)).size(), config.max_state_bytes);
  auto expired = minimum->advance_time(accepted, accepted.manifest.deadline_us);
  ASSERT_TRUE(expired.ok()) << expired.status().message();
  EXPECT_EQ(expired->state.phase, RequestPhase::Failed);
  EXPECT_LE(take(minimum->encode(expired->state)).size(), config.max_state_bytes);
}

TEST_F(RequestMachineTest, ReadyStateRejectsImpossibleAttemptAndErrorCombinations) {
  auto ready = ready_state();
  for (const auto code :
       {ErrorCode::InvalidArgument, ErrorCode::Unavailable, ErrorCode::ResourceExhausted}) {
    auto forged = ready;
    forged.nodes[2].error = Status(code, "error before first attempt");
    EXPECT_FALSE(machine->encode(forged).ok());
  }
  RequestMachineConfig config;
  config.attempt_timeout_us = 20;
  machine = take(RequestMachine::create(snapshot, registry, {target}, config));
  const auto running = take(machine->claim_ready(ready, 11));
  const auto retried = take(machine->advance_time(running.state, 31));
  auto forged = retried.state;
  forged.nodes[2].error = {};
  EXPECT_FALSE(machine->encode(forged).ok());
  forged.nodes[2].error = Status(ErrorCode::InvalidArgument, "permanent error cannot retry");
  EXPECT_FALSE(machine->encode(forged).ok());
  const auto encoded = take(machine->encode(retried.state));
  EXPECT_TRUE(machine->decode(encoded, manifest()).ok());
}

} // namespace
} // namespace eventflow
