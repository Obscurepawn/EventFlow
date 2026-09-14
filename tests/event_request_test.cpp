#include "eventflow/event_processor.hpp"
#include "remote_harness.hpp"
#include "status_test_support.hpp"
#include <filesystem>
#include <gtest/gtest.h>

namespace eventflow::test {
namespace {
class ObservedRequestStore final : public IStateStore {
public:
  explicit ObservedRequestStore(std::shared_ptr<IStateStore> value) noexcept : backend(value) {}

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
    ++commits;
    EF_ASSIGN_OR_RETURN(auto result, backend->commit(batch));
    if (unknown_at == commits)
      return CommitResult{CommitCode::Unknown, batch.expected_revision, false, "lost reply"};
    return result;
  }

  std::shared_ptr<IStateStore> backend;
  std::size_t commits = 0;
  std::size_t unknown_at = 0;
};

struct RequestFixture {
  std::string path;
  std::shared_ptr<ObservedRequestStore> store;
  std::shared_ptr<FunctionRegistry> registry;
  std::unique_ptr<Catalog> catalog;
  std::unique_ptr<EventProcessor> processor;
  ProcessorConfig config;
  EngineConfig engine_config;
  StoreConfig storage_config;
  RelationId market = 0, candidate = 0, capture = 0, target = 0, meta = 0, latest = 0;
  std::size_t captures = 0;
  bool capture_ready = true;
  std::size_t string_output_bytes = 0;

  ~RequestFixture() noexcept {
    processor.reset();
    store.reset();
    if (!path.empty()) {
      std::error_code error;
      std::filesystem::remove_all(path, error);
    }
  }

  Status define() noexcept {
    EF_ASSIGN_OR_RETURN(catalog, Catalog::create());
    EF_ASSIGN_OR_RETURN(registry, FunctionRegistry::create());
    EF_ASSIGN_OR_RETURN(market, catalog->define_sql("CREATE TABLE market(k STRING NOT NULL,x INT64 "
                                                    "NOT NULL,PRIMARY KEY(k)) PARTITION BY(k)"));
    EF_ASSIGN_OR_RETURN(candidate,
                        catalog->define_candidate({"candidate",
                                                   {{1, "k", DataType::String, false}},
                                                   {{2, "deadline", DataType::Int64, false},
                                                    {3, "version", DataType::Int64, false}}}));
    DefinitionOptions options;
    options.base_write_role = BaseWriteRole::RequestCapture;
    options.identity_domain = "request";
    EF_ASSIGN_OR_RETURN(capture,
                        catalog->define_sql("CREATE TABLE capture(k STRING,request_id STRING,x "
                                            "INT64 NOT NULL,PRIMARY KEY(k,request_id)) "
                                            "PARTITION BY(k)",
                                            options));
    EF_ASSIGN_OR_RETURN(auto snapshot, catalog->snapshot());
    EF_ASSIGN_OR_RETURN(auto relation, snapshot.find(capture));
    auto fields = relation->storage_schema.fields;
    FunctionViewDefinition function;
    function.name = "computed";
    function.function_id = "twice";
    function.function_version = 1;
    function.implementation_identity = "builtin.twice.v1";
    function.inputs = {{"input", capture, true, fields}};
    function.output_fields = {
        {1, "answer", string_output_bytes ? DataType::String : DataType::Int64, false}};
    EF_ASSIGN_OR_RETURN(target, catalog->define_function(function));
    EF_RETURN_IF_ERROR(registry->add(
        {{"twice", 1, "builtin.twice.v1", {{"input", true, fields}}, function.output_fields, true},
         [bytes = string_output_bytes](const FunctionInputs& inputs) noexcept -> StatusOr<Row> {
           if (bytes) {
             const auto overhead =
                 Json::array({std::get<std::string>((*inputs[0])[0]),
                              std::get<std::string>((*inputs[0])[1]), std::string{},
                              std::get<std::string>((*inputs[0])[0]),
                              std::get<std::string>((*inputs[0])[1])})
                     .dump()
                     .size();
             return Row{std::string(bytes - overhead, 'x')};
           }
           return Row{std::get<std::int64_t>((*inputs[0])[2]) * 2};
         }}));
    EF_ASSIGN_OR_RETURN(meta,
                        catalog->define_request_status(
                            {"request_meta", "request", relation->identity_fields, candidate}));
    EF_ASSIGN_OR_RETURN(latest, catalog->define_latest({"latest", target, meta, candidate}));
    RequestPlanDefinition plan;
    plan.name = "A";
    plan.targets = {target};
    plan.request_status_relation = meta;
    plan.default_lifetime_us = 100;
    plan.machine.attempt_timeout_us = 20;
    plan.machine.max_attempts = 1;
    if (string_output_bytes)
      plan.machine.max_output_bytes = string_output_bytes;
    plan.capture = [this](RoundContext& round,
                          const RequestAdmission& admission) noexcept -> StatusOr<RequestCapture> {
      ++captures;
      if (!capture_ready)
        return RequestCapture{false, {}};
      EF_ASSIGN_OR_RETURN(auto current, round.read_relation(market, admission.scope));
      if (!current)
        return RequestCapture{false, {}};
      Row identity = admission.scope;
      identity.emplace_back(admission.request_id);
      Row row = identity;
      row.push_back((*current)[1]);
      return RequestCapture{true, {{capture, identity, row}}};
    };
    config.input_relations = {market};
    config.requests.emplace();
    config.requests->registry = registry;
    config.requests->plans = {plan};
    config.requests->candidate = CandidateExpiryPolicy{
        candidate, "deadline", 1,
        [this](RoundContext& round, const ProcessorEvent& event) noexcept
            -> StatusOr<std::optional<CandidateActivation>> {
          if (event.kind != "news")
            return std::optional<CandidateActivation>{};
          EF_ASSIGN_OR_RETURN(auto old, round.read_relation(candidate, event.scope));
          std::int64_t epoch = 0;
          if (old) {
            epoch = std::get<std::int64_t>((*old)[1]);
            if (std::get<std::int64_t>((*old)[4]) >= std::get<std::int64_t>(event.values[0]))
              return std::optional<CandidateActivation>{};
          }
          return std::optional<CandidateActivation>{
              {epoch, {event.processing_time_us + 50, event.values[0]}}};
        }};
    ProcessorRule rule;
    rule.definition = on_event("news_rule", "news");
    config.rules = {rule};
    config.requests->trigger_bindings = {
        {"a", 1, "news_rule", "trigger", "A", true, 40,
         [](RoundContext&, const TriggerOccurrence&) noexcept -> StatusOr<RequestAdmission> {
           RequestAdmission admission;
           return admission;
         }}};
    engine_config.shard_count = 1;
    return {};
  }

  Status start(int backend_kind) noexcept {
    auto storage = storage_config;
    storage.shard_count = 1;
    std::unique_ptr<IStateStore> backend;
    if (backend_kind % 2 == 0) {
      EF_ASSIGN_OR_RETURN(backend, MemoryStateStore::open(storage));
    } else {
      char pattern[] = "/tmp/eventflow-event-request-XXXXXX";
      const auto* created = ::mkdtemp(pattern);
      if (!created)
        return Status(ErrorCode::Unavailable, "temporary directory unavailable");
      path = created;
      EF_ASSIGN_OR_RETURN(backend, RocksStateStore::open(path + "/db", storage));
    }
    if (backend_kind >= 2) {
      EF_ASSIGN_OR_RETURN(backend, RemoteHarnessStore::open(std::move(backend)));
    }
    store =
        std::make_shared<ObservedRequestStore>(std::shared_ptr<IStateStore>(std::move(backend)));
    return reopen();
  }

  Status reopen() noexcept {
    processor.reset();
    EF_ASSIGN_OR_RETURN(
        processor, EventProcessor::open(store, take(catalog->snapshot()), engine_config, config));
    return {};
  }

  ProcessorEvent event(std::int64_t offset = 0, std::int64_t now = 1, const std::string& key = "a",
                       std::string kind = "news") noexcept {
    return {{"source", "topic", 0, offset},
            {key},
            std::move(kind),
            {offset + 1},
            {{market, {key}, Row{key, std::int64_t{7}}}},
            now};
  }
};

class EventRequestContract : public testing::TestWithParam<int> {};

TEST_P(EventRequestContract, OpensOneOwnerWithFunctionCatalog) {
  RequestFixture f;
  ASSERT_TRUE(f.define().ok());
  auto status = f.start(GetParam());
  ASSERT_TRUE(status.ok()) << status.message();
}

TEST_P(EventRequestContract, SourceAdmissionClaimAndCompletionShareOneDurableOwner) {
  RequestFixture f;
  ASSERT_TRUE(f.define().ok());
  ASSERT_TRUE(f.start(GetParam()).ok());
  auto source = take(f.processor->process(f.event()));
  ASSERT_TRUE(source.operation_status.ok()) << source.operation_status.message();
  ASSERT_TRUE(source.input_committed);
  EXPECT_TRUE(source.work_remaining);
  auto pending = take(f.processor->pending_occurrences(16));
  ASSERT_EQ(pending.size(), 1U);
  EXPECT_EQ(f.captures, 0U);
  EXPECT_EQ(f.processor->acknowledge_occurrences({pending[0].id}).status().code(),
            ErrorCode::FailedPrecondition);
  const auto before = source.commit.revision;
  auto admission = take(f.processor->admit_occurrence(pending[0].id, 1));
  ASSERT_TRUE(admission.operation_status.ok()) << admission.operation_status.message();
  ASSERT_TRUE(admission.request_result);
  ASSERT_TRUE(admission.request_result->record);
  const auto record = *admission.request_result->record;
  EXPECT_EQ(record.state.manifest.capture_revision, before);
  EXPECT_EQ(record.state.manifest.admission_revision, before + 1);
  EXPECT_EQ(record.state.manifest.candidate_epoch, 1U);
  EXPECT_TRUE(take(f.processor->pending_occurrences(16)).empty());
  const RequestKey key{record.admission.scope, record.admission.request_id};
  auto claimed = take(f.processor->claim_tasks(key, 2));
  ASSERT_TRUE(claimed.operation_status.ok()) << claimed.operation_status.message();
  auto tasks = take(f.processor->pending_tasks(16));
  ASSERT_EQ(tasks.size(), 1U);
  auto executor = take(SimpleFunctionExecutor::create(f.registry));
  ASSERT_TRUE(executor->submit(tasks[0]).ok());
  ASSERT_EQ(take(executor->run_ready(3)), 1U);
  auto completions = take(executor->poll(1));
  auto completed = take(f.processor->complete_request(key, completions[0], 3));
  ASSERT_TRUE(completed.operation_status.ok()) << completed.operation_status.message();
  EXPECT_FALSE(completed.input_committed);
  EXPECT_FALSE(completed.work_remaining);
  ASSERT_TRUE(completed.request_result && completed.request_result->record);
  EXPECT_EQ(completed.request_result->record->state.phase, RequestPhase::Success);
  auto latest = take(f.processor->query_sql("SELECT answer FROM latest WHERE k='a'"));
  ASSERT_EQ(latest.rows.size(), 1U);
  EXPECT_EQ(std::get<std::int64_t>(latest.rows[0][0]), 14);
  const auto revision = completed.commit.revision;
  auto duplicate = take(f.processor->complete_request(key, completions[0], 3));
  EXPECT_TRUE(duplicate.committed_rounds.empty());
  EXPECT_EQ(duplicate.commit.revision, revision);
  ASSERT_TRUE(f.reopen().ok());
  EXPECT_EQ(take(f.processor->read_request(key)).state.phase, RequestPhase::Success);
  EXPECT_EQ(take(f.processor->processed_positions())[0].next_offset, 1);
}

TEST_P(EventRequestContract, AdmissionUnknownResolvesOriginalCaptureAndConsumptionExactlyOnce) {
  RequestFixture f;
  ASSERT_TRUE(f.define().ok());
  const auto expiring = take(f.catalog->define_sql(
      "CREATE TABLE expiring(k STRING NOT NULL,x INT64 NOT NULL,PRIMARY KEY(k)) PARTITION BY(k)"));
  f.config.input_relations.push_back(expiring);
  f.config.ttl_policies = {{expiring, 1}};
  ASSERT_TRUE(f.start(GetParam()).ok());
  auto event = f.event();
  event.changes.push_back({expiring, {std::string("a")}, Row{std::string("a"), std::int64_t{2}}});
  ASSERT_TRUE(take(f.processor->process(event)).input_committed);
  auto occurrence = take(f.processor->pending_occurrences(16))[0];
  const auto before = take(f.store->status(0)).revision;
  f.store->unknown_at = f.store->commits + 2;
  auto unknown = take(f.processor->admit_occurrence(occurrence.id, 2));
  ASSERT_EQ(unknown.operation_status.code(), ErrorCode::UnknownCommit);
  ASSERT_EQ(unknown.committed_rounds.size(), 1U);
  EXPECT_EQ(unknown.committed_rounds[0].revision, before + 1);
  ASSERT_TRUE(unknown.pending);
  EXPECT_EQ(unknown.pending->commit_owner, PendingCommitOwner::Request);
  EXPECT_EQ(unknown.pending->fixed_now_us, 2);
  EXPECT_FALSE(unknown.request_result);
  EXPECT_FALSE(f.processor->pending_occurrences(16).ok());
  EXPECT_FALSE(f.processor->pending_tasks(16).ok());
  EXPECT_FALSE(f.processor->active_requests().ok());
  auto pending = take(f.processor->pending_operation());
  ASSERT_TRUE(pending);
  EXPECT_EQ(pending->kind, ProcessorOperationKind::AdmitOccurrence);
  EXPECT_EQ(f.captures, 1U);
  auto resolved = take(f.processor->resolve_pending_commit());
  ASSERT_TRUE(resolved.operation_status.ok()) << resolved.operation_status.message();
  ASSERT_EQ(resolved.committed_rounds.size(), 2U);
  EXPECT_EQ(resolved.committed_rounds[0].revision, before + 2);
  EXPECT_EQ(f.captures, 1U);
  ASSERT_TRUE(resolved.request_result && resolved.request_result->record);
  EXPECT_TRUE(take(f.processor->pending_occurrences(16)).empty());
  EXPECT_FALSE(take(f.processor->pending_operation()));
}

TEST_P(EventRequestContract, GuardFreeFenceExpiresEveryScopeBeforeDeferredCapture) {
  RequestFixture f;
  ASSERT_TRUE(f.define().ok());
  f.config.ttl_policies = {{f.market, 5}};
  f.capture_ready = false;
  ASSERT_TRUE(f.start(GetParam()).ok());
  ASSERT_TRUE(take(f.processor->process(f.event(0, 1, "a"))).input_committed);
  ASSERT_TRUE(take(f.processor->process(f.event(1, 1, "b"))).input_committed);
  auto pending = take(f.processor->pending_occurrences(16));
  ASSERT_EQ(pending.size(), 2U);
  f.captures = 0;
  auto result = take(f.processor->admit_occurrence(pending[0].id, 6));
  ASSERT_TRUE(result.operation_status.ok()) << result.operation_status.message();
  ASSERT_TRUE(result.request_result);
  EXPECT_EQ(result.request_result->disposition, RequestDisposition::Deferred);
  ASSERT_FALSE(result.committed_rounds.empty());
  std::size_t deletions = 0;
  for (const auto& change : result.committed_rounds.front().changes)
    deletions += change.relation == f.market && !change.new_row;
  EXPECT_EQ(deletions, 2U);
  EXPECT_TRUE(take(f.processor->query_sql("SELECT x FROM market WHERE k='a'")).rows.empty());
  EXPECT_TRUE(take(f.processor->query_sql("SELECT x FROM market WHERE k='b'")).rows.empty());
  EXPECT_EQ(take(f.processor->pending_occurrences(16)).size(), 2U);
  EXPECT_EQ(f.captures, 2U);
  ASSERT_TRUE(f.reopen().ok());
}

TEST_P(EventRequestContract, FullActiveAndOutboxStillAllowCompletionThenAdmissionThenTimeout) {
  RequestFixture f;
  ASSERT_TRUE(f.define().ok());
  f.config.max_occurrences = 1;
  f.config.requests->runtime.max_active_requests = 1;
  f.config.ttl_policies = {{f.market, 5}};
  ProcessorRule timer;
  timer.definition.id = "timer";
  timer.definition.initial = "idle";
  timer.definition.states = {{"idle", {}}, {"armed", 5}};
  timer.definition.transitions = {{"idle", "news", "armed", {}, {}, {}},
                                  {"armed", "$timeout", "idle", {}, {}, "alarm"}};
  f.config.rules.push_back(timer);
  ASSERT_TRUE(f.start(GetParam()).ok());
  ASSERT_TRUE(take(f.processor->process(f.event())).input_committed);
  auto first = take(f.processor->pending_occurrences(1))[0];
  auto admission = take(f.processor->admit_occurrence(first.id, 1));
  ASSERT_TRUE(admission.request_result && admission.request_result->record);
  const auto record = *admission.request_result->record;
  const RequestKey key{record.admission.scope, record.admission.request_id};
  ASSERT_TRUE(take(f.processor->claim_tasks(key, 1)).operation_status.ok());
  auto task = take(f.processor->pending_tasks(1))[0];
  ASSERT_TRUE(take(f.processor->process(f.event(1, 2, "b"))).input_committed);
  ASSERT_EQ(take(f.processor->pending_occurrences(1)).size(), 1U);
  auto executor = take(SimpleFunctionExecutor::create(f.registry));
  ASSERT_TRUE(executor->submit(task).ok());
  ASSERT_EQ(take(executor->run_ready(6)), 1U);
  auto completion = take(executor->poll(1))[0];
  auto result = take(f.processor->complete_request(key, completion, 6));
  ASSERT_TRUE(result.operation_status.ok()) << result.operation_status.message();
  ASSERT_TRUE(result.request_result && result.request_result->record);
  EXPECT_EQ(result.request_result->record->state.phase, RequestPhase::Success);
  ASSERT_GE(result.committed_rounds.size(), 4U);
  std::optional<std::uint64_t> completed_revision, admitted_revision, timer_revision;
  for (const auto& round : result.committed_rounds) {
    for (const auto& request : round.requests) {
      if (request.key.scope == Row{std::string("a")})
        completed_revision = round.revision;
      if (request.key.scope == Row{std::string("b")})
        admitted_revision = round.revision;
    }
    if (!round.occurrences.empty())
      timer_revision = round.revision;
  }
  ASSERT_TRUE(completed_revision && admitted_revision && timer_revision);
  EXPECT_LT(*completed_revision, *admitted_revision);
  EXPECT_LT(*admitted_revision, *timer_revision);
  auto pending = take(f.processor->pending_occurrences(1));
  ASSERT_EQ(pending.size(), 1U);
  EXPECT_EQ(pending[0].rule_id, "timer");
  auto blocked = take(f.processor->pump(7));
  EXPECT_EQ(blocked.operation_status.code(), ErrorCode::ResourceExhausted);
  EXPECT_FALSE(blocked.committed_rounds.empty());
  EXPECT_FALSE(f.processor->failed());
  ASSERT_TRUE(f.reopen().ok());
  ASSERT_TRUE(take(f.processor->acknowledge_occurrences({pending[0].id})).operation_status.ok());
  auto resumed = take(f.processor->pump(8));
  ASSERT_TRUE(resumed.operation_status.ok()) << resumed.operation_status.message();
  auto second = take(f.processor->pending_occurrences(1));
  ASSERT_EQ(second.size(), 1U);
  EXPECT_EQ(second[0].scope, Row{std::string("b")});
  EXPECT_EQ(second[0].evaluated_at_us, 8);
  EXPECT_EQ(second[0].emission.scheduled_time_us, 7);
}

TEST_P(EventRequestContract, CandidateExpiryFencesLatestBeforeLegalHistoricalCompletion) {
  RequestFixture f;
  ASSERT_TRUE(f.define().ok());
  ASSERT_TRUE(f.start(GetParam()).ok());
  ASSERT_TRUE(take(f.processor->process(f.event())).input_committed);
  auto occurrence = take(f.processor->pending_occurrences(16))[0];
  auto admitted = take(f.processor->admit_occurrence(occurrence.id, 1));
  ASSERT_TRUE(admitted.request_result && admitted.request_result->record)
      << (admitted.request_result ? admitted.request_result->reason.message()
                                  : admitted.operation_status.message());
  const RequestKey key = admitted.request_result->key;
  ASSERT_TRUE(take(f.processor->claim_tasks(key, 45)).operation_status.ok());
  auto tasks = take(f.processor->pending_tasks(1));
  ASSERT_EQ(tasks.size(), 1U);
  auto executor = take(SimpleFunctionExecutor::create(f.registry));
  ASSERT_TRUE(executor->submit(tasks[0]).ok());
  ASSERT_EQ(take(executor->run_ready(51)), 1U);
  auto completion = take(executor->poll(1))[0];
  auto result = take(f.processor->complete_request(key, completion, 51));
  ASSERT_TRUE(result.operation_status.ok()) << result.operation_status.message();
  EXPECT_EQ(take(f.processor->read_request(key)).state.phase, RequestPhase::Success);
  EXPECT_TRUE(take(f.processor->query_sql("SELECT answer FROM latest WHERE k='a'")).rows.empty());
  EXPECT_EQ(take(f.processor->query_sql("SELECT answer FROM computed WHERE k='a'")).rows.size(),
            1U);
  auto candidates = take(f.processor->query_sql("SELECT active FROM candidate WHERE k='a'"));
  ASSERT_EQ(candidates.rows.size(), 1U);
  EXPECT_FALSE(std::get<bool>(candidates.rows[0][0]));
  ASSERT_TRUE(f.reopen().ok());
}

TEST_P(EventRequestContract, DurableLevelAdmissionBypassesPendingCAndSurvivesCompletionRestart) {
  RequestFixture f;
  ASSERT_TRUE(f.define().ok());
  f.config.max_occurrences = 1;
  f.config.requests->runtime.max_active_requests = 1;
  auto b = f.config.requests->plans[0];
  b.name = "B";
  b.capture = [&f](RoundContext& round,
                   const RequestAdmission& admission) noexcept -> StatusOr<RequestCapture> {
    EF_ASSIGN_OR_RETURN(auto latest, round.read_relation(f.latest, admission.scope));
    if (!latest)
      return RequestCapture{false, {}};
    Row identity = admission.scope;
    identity.emplace_back(admission.request_id);
    Row row = identity;
    row.emplace_back(std::int64_t{9});
    return RequestCapture{true, {{f.capture, identity, row}}};
  };
  auto c = f.config.requests->plans[0];
  c.name = "C";
  f.config.requests->plans.push_back(b);
  f.config.requests->plans.push_back(c);
  f.config.requests->level_bindings = {{"b", 1, "B", 40}};
  ProcessorRule confirm;
  confirm.definition = on_event("confirm", "confirm");
  f.config.rules.push_back(confirm);
  auto c_binding = f.config.requests->trigger_bindings[0];
  c_binding.id = "c";
  c_binding.rule_id = "confirm";
  c_binding.plan = "C";
  c_binding.required_accepted_binding = "b";
  f.config.requests->trigger_bindings.push_back(c_binding);
  ASSERT_TRUE(f.start(GetParam()).ok());
  ASSERT_TRUE(take(f.processor->process(f.event())).input_committed);
  auto first = take(f.processor->pending_occurrences(1))[0];
  auto admitted = take(f.processor->admit_occurrence(first.id, 1));
  ASSERT_TRUE(admitted.request_result && admitted.request_result->record)
      << (admitted.request_result ? admitted.request_result->reason.message()
                                  : admitted.operation_status.message());
  const auto a_key = admitted.request_result->key;
  ASSERT_TRUE(take(f.processor->claim_tasks(a_key, 1)).operation_status.ok());
  auto task = take(f.processor->pending_tasks(1))[0];
  ASSERT_TRUE(take(f.processor->process(f.event(1, 2, "a", "confirm"))).input_committed);
  auto pending_c = take(f.processor->pending_occurrences(1));
  ASSERT_EQ(pending_c.size(), 1U);
  EXPECT_EQ(pending_c[0].rule_id, "confirm");
  auto executor = take(SimpleFunctionExecutor::create(f.registry));
  ASSERT_TRUE(executor->submit(task).ok());
  ASSERT_EQ(take(executor->run_ready(3)), 1U);
  auto completion = take(executor->poll(1))[0];
  // Stop immediately after completion, before a level recheck can execute.
  f.config.max_rounds_per_call = 1;
  ASSERT_TRUE(f.reopen().ok());
  auto completed = take(f.processor->complete_request(a_key, completion, 3));
  ASSERT_TRUE(completed.request_result && completed.request_result->record);
  EXPECT_EQ(completed.request_result->record->state.phase, RequestPhase::Success);
  EXPECT_EQ(completed.operation_status.code(), ErrorCode::ResourceExhausted);
  ASSERT_EQ(completed.committed_rounds.size(), 1U);
  f.config.max_rounds_per_call = 1024;
  ASSERT_TRUE(f.reopen().ok());
  auto resumed = take(f.processor->pump(3));
  ASSERT_TRUE(resumed.operation_status.ok()) << resumed.operation_status.message();
  std::optional<RequestKey> b_key;
  for (const auto& round : resumed.committed_rounds)
    for (const auto& request : round.requests)
      if (request.record && request.record->admission.plan == "B")
        b_key = request.key;
  ASSERT_TRUE(b_key);
  EXPECT_EQ(take(f.processor->pending_occurrences(1))[0].id, pending_c[0].id);
  auto cancelled = take(f.processor->cancel_request(*b_key, 4));
  ASSERT_TRUE(cancelled.operation_status.ok()) << cancelled.operation_status.message();
  std::size_t c_admissions = 0;
  for (const auto& round : cancelled.committed_rounds)
    for (const auto& request : round.requests)
      c_admissions += request.record && request.record->admission.plan == "C";
  EXPECT_EQ(c_admissions, 1U);
  EXPECT_TRUE(take(f.processor->pending_occurrences(1)).empty());
  auto again = take(f.processor->pump(4));
  EXPECT_TRUE(again.committed_rounds.empty());
}

TEST_P(EventRequestContract, SourceUnknownPublishesTypedCandidateAndPositionOnlyOnResolve) {
  RequestFixture f;
  ASSERT_TRUE(f.define().ok());
  ASSERT_TRUE(f.start(GetParam()).ok());
  f.store->unknown_at = f.store->commits + 1;
  auto unknown = take(f.processor->process(f.event()));
  ASSERT_EQ(unknown.operation_status.code(), ErrorCode::UnknownCommit);
  EXPECT_FALSE(unknown.input_committed);
  EXPECT_TRUE(unknown.committed_rounds.empty());
  ASSERT_TRUE(unknown.pending);
  EXPECT_EQ(unknown.pending->commit_owner, PendingCommitOwner::Lifecycle);
  auto resolved = take(f.processor->resolve_pending_commit());
  ASSERT_TRUE(resolved.operation_status.ok()) << resolved.operation_status.message();
  EXPECT_TRUE(resolved.input_committed);
  ASSERT_EQ(resolved.committed_rounds.size(), 1U);
  ASSERT_EQ(resolved.committed_rounds[0].occurrences.size(), 1U);
  EXPECT_EQ(take(f.processor->processed_positions())[0].next_offset, 1);
  auto duplicate = take(f.processor->process(f.event(0, 99)));
  EXPECT_TRUE(duplicate.duplicate);
  EXPECT_EQ(f.processor->processing_time_us(), 1);
  auto candidates =
      take(f.processor->query_sql("SELECT candidate_epoch FROM candidate WHERE k='a'"));
  EXPECT_EQ(std::get<std::int64_t>(candidates.rows[0][0]), 1);
}

TEST_P(EventRequestContract, UnknownOrdinaryAckDoesNotCommitASecondBatchOnResolve) {
  RequestFixture f;
  ASSERT_TRUE(f.define().ok());
  f.config.requests->trigger_bindings.clear();
  ASSERT_TRUE(f.start(GetParam()).ok());
  ASSERT_TRUE(take(f.processor->process(f.event())).input_committed);
  const auto occurrence = take(f.processor->pending_occurrences(16))[0];
  const auto before = take(f.store->status(0)).revision;
  f.store->unknown_at = f.store->commits + 1;
  auto unknown = take(f.processor->acknowledge_occurrences({occurrence.id}));
  ASSERT_EQ(unknown.operation_status.code(), ErrorCode::UnknownCommit);
  EXPECT_TRUE(unknown.committed_rounds.empty());
  auto resolved = take(f.processor->resolve_pending_commit());
  ASSERT_TRUE(resolved.operation_status.ok()) << resolved.operation_status.message();
  ASSERT_EQ(resolved.committed_rounds.size(), 1U);
  EXPECT_EQ(resolved.commit.revision, before + 1);
  EXPECT_FALSE(f.processor->has_pending_commit());
  EXPECT_TRUE(take(f.processor->pending_occurrences(16)).empty());
}

TEST_P(EventRequestContract, OversizedGeneratedTriggerAndLevelIdsRejectSourceBeforePosition) {
  for (bool level : {false, true}) {
    RequestFixture f;
    ASSERT_TRUE(f.define().ok());
    if (level) {
      f.config.requests->trigger_bindings.clear();
      f.config.requests->level_bindings = {{"b", 1, "A", 40}};
    }
    ASSERT_TRUE(f.start(GetParam()).ok());
    auto result = take(f.processor->process(f.event(0, 1, std::string(600, 'x'))));
    EXPECT_EQ(result.operation_status.code(), ErrorCode::ResourceExhausted);
    EXPECT_FALSE(result.input_committed);
    EXPECT_FALSE(f.processor->failed());
    EXPECT_TRUE(take(f.processor->processed_positions()).empty());
    EXPECT_TRUE(take(f.processor->pending_occurrences(16)).empty());
    ASSERT_TRUE(f.reopen().ok());
  }
}

TEST_P(EventRequestContract, TerminalBudgetRejectsAdmissionBeforeCaptureAndPreservesIntent) {
  RequestFixture f;
  ASSERT_TRUE(f.define().ok());
  f.config.max_state_bytes = 2 * 1024 * 1024;
  f.engine_config.max_bytes = 2 * 1024 * 1024 + 600 * 1024;
  ASSERT_TRUE(f.start(GetParam()).ok());
  ASSERT_TRUE(take(f.processor->process(f.event())).input_committed);
  auto pending = take(f.processor->pending_occurrences(16))[0];
  auto result = take(f.processor->admit_occurrence(pending.id, 1));
  ASSERT_TRUE(result.operation_status.ok()) << result.operation_status.message();
  ASSERT_TRUE(result.request_result);
  EXPECT_EQ(result.request_result->disposition, RequestDisposition::Deferred);
  EXPECT_FALSE(result.request_result->record);
  EXPECT_EQ(f.captures, 0U);
  ASSERT_EQ(result.committed_rounds.size(), 1U);
  EXPECT_TRUE(result.committed_rounds[0].changes.empty());
  auto retained = take(f.processor->pending_occurrences(16));
  ASSERT_EQ(retained.size(), 1U);
  EXPECT_EQ(retained[0].id, pending.id);
  EXPECT_FALSE(f.processor->failed());
}

TEST_P(EventRequestContract, ActualStoreLimitDefersBeforeCaptureAndPreservesOnce) {
  RequestFixture f;
  ASSERT_TRUE(f.define().ok());
  f.storage_config.max_batch_bytes = 1024 * 1024;
  ASSERT_TRUE(f.start(GetParam()).ok());
  ASSERT_TRUE(take(f.processor->process(f.event())).input_committed);
  auto occurrences = take(f.processor->pending_occurrences(16));
  ASSERT_EQ(occurrences.size(), 1U);
  auto result = take(f.processor->admit_occurrence(occurrences[0].id, 1));
  ASSERT_TRUE(result.operation_status.ok()) << result.operation_status.message();
  ASSERT_TRUE(result.request_result);
  EXPECT_EQ(result.request_result->disposition, RequestDisposition::Deferred);
  EXPECT_EQ(result.request_result->reason.code(), ErrorCode::ResourceExhausted);
  EXPECT_FALSE(result.request_result->record);
  EXPECT_EQ(f.captures, 0U);
  EXPECT_TRUE(take(f.processor->active_requests()).empty());
  auto retained = take(f.processor->pending_occurrences(16));
  ASSERT_EQ(retained.size(), 1U);
  EXPECT_EQ(retained[0].id, occurrences[0].id);
  EXPECT_FALSE(f.processor->failed());
  EXPECT_TRUE(f.reopen().ok());
}

TEST_P(EventRequestContract, SmallerEffectiveBudgetRejectsRecoveryOfAcceptedWorkWithoutWrites) {
  RequestFixture f;
  ASSERT_TRUE(f.define().ok());
  ASSERT_TRUE(f.start(GetParam()).ok());
  ASSERT_TRUE(take(f.processor->process(f.event())).input_committed);
  auto occurrences = take(f.processor->pending_occurrences(16));
  ASSERT_EQ(occurrences.size(), 1U);
  auto admitted = take(f.processor->admit_occurrence(occurrences[0].id, 1));
  ASSERT_TRUE(admitted.operation_status.ok());
  ASSERT_TRUE(admitted.request_result && admitted.request_result->record);
  const auto commits = f.store->commits;
  const auto revision = take(f.store->status(0)).revision;
  f.engine_config.max_bytes = 1024 * 1024;
  auto restored = f.reopen();
  EXPECT_EQ(restored.code(), ErrorCode::ResourceExhausted);
  EXPECT_EQ(f.store->commits, commits);
  EXPECT_EQ(take(f.store->status(0)).revision, revision);
  f.engine_config.max_bytes = 4 * 1024 * 1024;
  ASSERT_TRUE(f.reopen().ok());
  EXPECT_EQ(take(f.processor->active_requests()).size(), 1U);
}

TEST_P(EventRequestContract, OrderedExpiryNoticesSurviveSeparateSweepsAndExecuteAtCurrentTime) {
  RequestFixture f;
  ASSERT_TRUE(f.define().ok());
  const auto other = take(f.catalog->define_sql(
      "CREATE TABLE other(k STRING NOT NULL,x INT64 NOT NULL,PRIMARY KEY(k)) PARTITION BY(k)"));
  f.config.input_relations.push_back(other);
  f.config.ttl_policies = {{f.market, 5}, {other, 7}};
  f.config.max_occurrences = 1;
  f.config.requests->trigger_bindings.clear();
  f.config.rules[0].definition = on_event("news_rule", "news");
  ProcessorRule rule;
  rule.definition.id = "expiry";
  rule.definition.initial = "idle";
  rule.definition.states = {{"idle", {}}, {"waiting", 3}};
  rule.definition.register_fields = {{1, "count", DataType::Int64, false},
                                     {2, "processed", DataType::Int64, false}};
  rule.definition.initial_registers = {std::int64_t{0}, std::int64_t{0}};
  rule.definition.max_state_bytes = 1024;
  const auto update = [](const FsmContext& context) noexcept -> StatusOr<Row> {
    return Row{std::get<std::int64_t>(context.state.registers[0]) + 1, context.event.time_us};
  };
  rule.definition.transitions = {{"idle", "$expire", "waiting", {}, update, "expired"},
                                 {"waiting", "$expire", "waiting", {}, update, "expired"},
                                 {"waiting", "$timeout", "idle", {}, {}, {}}};
  f.config.rules.push_back(rule);
  ASSERT_TRUE(f.start(GetParam()).ok());
  auto event = f.event();
  event.changes.push_back({other, {std::string("a")}, Row{std::string("a"), std::int64_t{9}}});
  ASSERT_TRUE(take(f.processor->process(event)).input_committed);
  const auto blocker = take(f.processor->pending_occurrences(1))[0];
  auto first_sweep = take(f.processor->pump(6));
  EXPECT_EQ(first_sweep.operation_status.code(), ErrorCode::ResourceExhausted);
  auto second_sweep = take(f.processor->pump(8));
  EXPECT_EQ(second_sweep.operation_status.code(), ErrorCode::ResourceExhausted);
  ASSERT_TRUE(f.reopen().ok());
  ASSERT_TRUE(take(f.processor->acknowledge_occurrences({blocker.id})).operation_status.ok());
  auto first = take(f.processor->pump(9));
  EXPECT_EQ(first.operation_status.code(), ErrorCode::ResourceExhausted);
  auto notices = take(f.processor->pending_occurrences(1));
  ASSERT_EQ(notices.size(), 1U);
  EXPECT_EQ(notices[0].emission.cause, "expiry:1");
  EXPECT_EQ(notices[0].emission.scheduled_time_us, 6);
  EXPECT_EQ(notices[0].evaluated_at_us, 9);
  auto state = take(f.processor->read_fsm("expiry", {std::string("a")}));
  EXPECT_EQ(state.deadline_us, 12);
  EXPECT_EQ(std::get<std::int64_t>(state.registers[0]), 1);
  ASSERT_TRUE(f.reopen().ok());
  ASSERT_TRUE(take(f.processor->acknowledge_occurrences({notices[0].id})).operation_status.ok());
  auto second = take(f.processor->pump(10));
  ASSERT_TRUE(second.operation_status.ok()) << second.operation_status.message();
  notices = take(f.processor->pending_occurrences(1));
  ASSERT_EQ(notices.size(), 1U);
  EXPECT_EQ(notices[0].emission.cause, "expiry:2");
  EXPECT_EQ(notices[0].emission.scheduled_time_us, 8);
  EXPECT_EQ(notices[0].evaluated_at_us, 10);
  state = take(f.processor->read_fsm("expiry", {std::string("a")}));
  EXPECT_EQ(std::get<std::int64_t>(state.registers[0]), 2);
  EXPECT_EQ(std::get<std::int64_t>(state.registers[1]), 10);
}

TEST_P(EventRequestContract, SingleRoundBudgetStillCompletesAtIncreasingDispatchTimes) {
  RequestFixture f;
  ASSERT_TRUE(f.define().ok());
  ASSERT_TRUE(f.start(GetParam()).ok());
  ASSERT_TRUE(take(f.processor->process(f.event())).input_committed);
  auto id = take(f.processor->pending_occurrences(16))[0].id;
  auto admission = take(f.processor->admit_occurrence(id, 2));
  ASSERT_TRUE(admission.request_result && admission.request_result->record);
  const auto key = admission.request_result->key;
  ASSERT_TRUE(take(f.processor->claim_tasks(key, 3)).operation_status.ok());
  auto executor = take(SimpleFunctionExecutor::create(f.registry));
  ASSERT_TRUE(executor->submit(take(f.processor->pending_tasks(1))[0]).ok());
  ASSERT_EQ(take(executor->run_ready(4)), 1U);
  const auto completion = take(executor->poll(1))[0];
  f.config.max_rounds_per_call = 1;
  ASSERT_TRUE(f.reopen().ok());
  auto completed = take(f.processor->complete_request(key, completion, 4));
  ASSERT_TRUE(completed.request_result && completed.request_result->record);
  EXPECT_EQ(completed.request_result->record->state.phase, RequestPhase::Success);
  ASSERT_EQ(completed.committed_rounds.size(), 1U);
  EXPECT_EQ(f.processor->processing_time_us(), 4);
  auto next = take(f.processor->pump(5));
  EXPECT_EQ(next.committed_rounds.size(), 1U);
  EXPECT_EQ(f.processor->processing_time_us(), 5);
}

TEST_P(EventRequestContract, ReservedFullLifecycleAllowsMaximumResultAfterLaterSourceGrowth) {
  RequestFixture f;
  f.string_output_bytes = 32 * 1024;
  ASSERT_TRUE(f.define().ok());
  f.engine_config.max_bytes = 3 * 1024 * 1024;
  f.config.max_dedup_bytes = 1536 * 1024;
  ASSERT_TRUE(f.start(GetParam()).ok());
  ASSERT_TRUE(take(f.processor->process(f.event())).input_committed);
  auto id = take(f.processor->pending_occurrences(16))[0].id;
  auto admitted = take(f.processor->admit_occurrence(id, 1));
  ASSERT_TRUE(admitted.request_result && admitted.request_result->record)
      << (admitted.request_result ? admitted.request_result->reason.message()
                                  : admitted.operation_status.message());
  const auto key = admitted.request_result->key;
  ASSERT_TRUE(take(f.processor->claim_tasks(key, 2)).operation_status.ok());
  auto task = take(f.processor->pending_tasks(1))[0];
  for (std::int64_t offset = 1; offset <= 12; ++offset) {
    auto event = f.event(offset, 2, "a", "trade");
    event.values = {std::string(40 * 1024, static_cast<char>('a' + offset))};
    event.changes.clear();
    auto result = take(f.processor->process(event));
    ASSERT_TRUE(result.operation_status.ok()) << result.operation_status.message();
    ASSERT_TRUE(result.input_committed);
  }
  auto executor = take(SimpleFunctionExecutor::create(f.registry));
  ASSERT_TRUE(executor->submit(task).ok());
  ASSERT_EQ(take(executor->run_ready(3)), 1U);
  auto completion = take(executor->poll(1))[0];
  auto result = take(f.processor->complete_request(key, completion, 3));
  ASSERT_TRUE(result.operation_status.ok()) << result.operation_status.message();
  ASSERT_TRUE(result.request_result && result.request_result->record);
  EXPECT_EQ(result.request_result->record->state.phase, RequestPhase::Success);
  auto latest = take(f.processor->query_sql("SELECT answer FROM latest WHERE k='a'"));
  ASSERT_EQ(latest.rows.size(), 1U);
  const auto expected_overhead =
      Json::array({std::get<std::string>(key.scope[0]), key.request_id, std::string{},
                   std::get<std::string>(key.scope[0]), key.request_id})
          .dump()
          .size();
  EXPECT_EQ(std::get<std::string>(latest.rows[0][0]).size(),
            f.string_output_bytes - expected_overhead);
  ASSERT_TRUE(f.reopen().ok());
}

TEST_P(EventRequestContract, NegativeFrozenLifetimeCannotCommitAnUnrecoverableIntent) {
  RequestFixture f;
  ASSERT_TRUE(f.define().ok());
  f.config.requests->trigger_bindings[0].make_admission =
      [](RoundContext&, const TriggerOccurrence&) noexcept -> StatusOr<RequestAdmission> {
    RequestAdmission admission;
    admission.lifetime_us = -1;
    return admission;
  };
  ASSERT_TRUE(f.start(GetParam()).ok());
  auto result = take(f.processor->process(f.event()));
  EXPECT_EQ(result.operation_status.code(), ErrorCode::InvalidArgument);
  EXPECT_FALSE(result.input_committed);
  EXPECT_TRUE(result.committed_rounds.empty());
  ASSERT_TRUE(f.reopen().ok());
  EXPECT_TRUE(take(f.processor->pending_occurrences(16)).empty());
}

TEST_P(EventRequestContract,
       ExpiryClosureBudgetRejectsNewScopesBeforeBlockingExistingCancellation) {
  RequestFixture f;
  ASSERT_TRUE(f.define().ok());
  const auto child = take(f.catalog->define_sql(
      "CREATE MATERIALIZED VIEW expired_payload AS SELECT k,'" + std::string(8192, 'x') +
      "' AS payload FROM candidate WHERE active=false"));
  (void)child;
  f.engine_config.max_bytes = 384 * 1024;
  f.config.max_state_bytes = 64 * 1024;
  f.config.requests->runtime.max_record_bytes = 64 * 1024;
  f.config.requests->runtime.max_index_bytes = 64 * 1024;
  f.config.requests->plans[0].machine.max_state_bytes = 32 * 1024;
  auto opened = f.start(GetParam());
  ASSERT_TRUE(opened.ok()) << opened.message();
  ASSERT_TRUE(take(f.processor->process(f.event())).input_committed);
  auto id = take(f.processor->pending_occurrences(16))[0].id;
  auto admitted = take(f.processor->admit_occurrence(id, 1));
  ASSERT_TRUE(admitted.request_result && admitted.request_result->record)
      << (admitted.request_result ? admitted.request_result->reason.message()
                                  : admitted.operation_status.message());
  const auto key = admitted.request_result->key;
  bool rejected = false;
  for (std::int64_t offset = 1; offset <= 40; ++offset) {
    auto result = take(f.processor->process(f.event(offset, 1, "scope" + std::to_string(offset))));
    if (result.operation_status.code() == ErrorCode::ResourceExhausted) {
      EXPECT_FALSE(result.input_committed);
      rejected = true;
      break;
    }
    ASSERT_TRUE(result.operation_status.ok()) << result.operation_status.message();
  }
  ASSERT_TRUE(rejected);
  EXPECT_FALSE(f.processor->failed());
  ASSERT_TRUE(take(f.processor->claim_tasks(key, 45)).operation_status.ok());
  auto result = take(f.processor->cancel_request(key, 51));
  ASSERT_TRUE(result.operation_status.ok()) << result.operation_status.message();
  ASSERT_TRUE(result.request_result && result.request_result->record);
  EXPECT_EQ(result.request_result->record->state.phase, RequestPhase::Cancelled);
  EXPECT_TRUE(take(f.processor->pending_tasks(16)).empty());
}

TEST_P(EventRequestContract, PeriodicEventReservesCombinedOutputBytesAtConfiguration) {
  RequestFixture f;
  ASSERT_TRUE(f.define().ok());
  f.config.requests->trigger_bindings.clear();
  ProcessorRule first, second;
  first.definition = on_event("p1", "tick");
  second.definition = on_event("p2", "tick");
  f.config.rules = {first, second};
  f.config.periodic_policies = {{"clock", "tick", 10, 0}};
  f.config.max_occurrences = 2;
  f.config.max_outbox_bytes = f.config.max_occurrence_bytes;
  EXPECT_EQ(f.start(GetParam()).code(), ErrorCode::ResourceExhausted);
}

TEST_P(EventRequestContract, FullQueuesCannotBlockCancellationTimeoutOrDeadlineCompletionFence) {
  for (int control = 0; control < 3; ++control) {
    RequestFixture f;
    ASSERT_TRUE(f.define().ok());
    f.config.max_occurrences = 1;
    f.config.requests->runtime.max_active_requests = 1;
    ProcessorRule timer;
    timer.definition.id = "timer";
    timer.definition.initial = "idle";
    timer.definition.states = {{"idle", {}}, {"armed", 5}};
    timer.definition.transitions = {{"idle", "news", "armed", {}, {}, {}},
                                    {"armed", "$timeout", "idle", {}, {}, "alarm"}};
    f.config.rules.push_back(timer);
    ASSERT_TRUE(f.start(GetParam()).ok());
    ASSERT_TRUE(take(f.processor->process(f.event())).input_committed);
    auto id = take(f.processor->pending_occurrences(1))[0].id;
    auto admitted = take(f.processor->admit_occurrence(id, 1));
    ASSERT_TRUE(admitted.request_result && admitted.request_result->record)
        << (admitted.request_result ? admitted.request_result->reason.message()
                                    : admitted.operation_status.message());
    const auto key = admitted.request_result->key;
    ASSERT_TRUE(take(f.processor->claim_tasks(key, 2)).operation_status.ok());
    auto executor = take(SimpleFunctionExecutor::create(f.registry));
    ASSERT_TRUE(executor->submit(take(f.processor->pending_tasks(1))[0]).ok());
    ASSERT_EQ(take(executor->run_ready(3)), 1U);
    auto completion = take(executor->poll(1))[0];
    ASSERT_TRUE(take(f.processor->process(f.event(1, 3, "b"))).input_committed);
    ASSERT_EQ(take(f.processor->pending_occurrences(1)).size(), 1U);
    auto result = take(control == 0   ? f.processor->cancel_request(key, 6)
                       : control == 1 ? f.processor->pump(22)
                                      : f.processor->complete_request(key, completion, 22));
    EXPECT_FALSE(result.input_committed);
    EXPECT_FALSE(result.committed_rounds.empty());
    EXPECT_FALSE(f.processor->failed());
    EXPECT_EQ(take(f.processor->read_request(key)).state.phase,
              control == 0 ? RequestPhase::Cancelled : RequestPhase::Failed);
    std::size_t cancellations = 0;
    for (const auto& round : result.committed_rounds)
      cancellations += round.cancel_task_ids.size();
    EXPECT_EQ(cancellations, 1U);
    EXPECT_TRUE(take(f.processor->query_sql("SELECT answer FROM latest WHERE k='a'")).rows.empty());
  }
}

TEST_P(EventRequestContract, TryOnceMissAtSourceFiringCommitsAuditAndNeverRecapturesAfterUnknown) {
  RequestFixture f;
  ASSERT_TRUE(f.define().ok());
  f.capture_ready = false;
  f.config.requests->trigger_bindings[0].mode = RequestTriggerMode::TryOnceAtFiring;
  ASSERT_TRUE(f.start(GetParam()).ok());
  f.store->unknown_at = f.store->commits + 2;
  auto result = take(f.processor->process(f.event()));
  ASSERT_EQ(result.operation_status.code(), ErrorCode::UnknownCommit);
  EXPECT_TRUE(result.input_committed);
  ASSERT_EQ(result.committed_rounds.size(), 1U);
  EXPECT_EQ(f.captures, 1U);
  EXPECT_FALSE(f.processor->read_candidate({std::string("a")}).ok());
  auto resolved = take(f.processor->resolve_pending_commit());
  ASSERT_TRUE(resolved.operation_status.ok()) << resolved.operation_status.message();
  ASSERT_EQ(resolved.committed_rounds.size(), 1U);
  ASSERT_EQ(resolved.committed_rounds[0].requests.size(), 1U);
  EXPECT_EQ(resolved.committed_rounds[0].requests[0].disposition, RequestDisposition::Skipped);
  auto candidate = take(f.processor->read_candidate({std::string("a")}));
  ASSERT_EQ(candidate.bindings.size(), 1U);
  EXPECT_EQ(candidate.started_at_us, 1);
  EXPECT_EQ(candidate.bindings[0].skipped_count, 1U);
  EXPECT_EQ(candidate.bindings[0].last_slot_or_time, 1);
  EXPECT_EQ(candidate.bindings[0].reason, RequestPendingReason::NotReady);
  EXPECT_FALSE(candidate.bindings[0].accepted);
  EXPECT_TRUE(take(f.processor->pending_occurrences(16)).empty());
  f.capture_ready = true;
  ASSERT_TRUE(f.reopen().ok());
  auto later = take(f.processor->pump(2));
  ASSERT_TRUE(later.operation_status.ok());
  EXPECT_EQ(f.captures, 1U);
  EXPECT_EQ(take(f.processor->read_candidate({std::string("a")})).bindings[0].skipped_count, 1U);
}

TEST_P(EventRequestContract, PeriodicTryOnceSeparatesCoalescedSlotsAndCurrentNotReadyAudit) {
  RequestFixture f;
  ASSERT_TRUE(f.define().ok());
  f.capture_ready = false;
  auto binding = f.config.requests->trigger_bindings[0];
  binding.id = "d";
  binding.rule_id = "d";
  binding.once_per_candidate = false;
  binding.mode = RequestTriggerMode::TryOnceAtFiring;
  binding.pending_lifetime_us = 100;
  f.config.requests->trigger_bindings = {binding};
  ProcessorRule rule;
  rule.definition = on_event("d", "tick");
  f.config.rules = {rule};
  f.config.periodic_policies = {{"clock", "tick", 5, 0}};
  ASSERT_TRUE(f.start(GetParam()).ok());
  ASSERT_TRUE(take(f.processor->process(f.event())).input_committed);
  auto result = take(f.processor->pump(16));
  ASSERT_TRUE(result.operation_status.ok()) << result.operation_status.message();
  ASSERT_EQ(result.committed_rounds.size(), 2U);
  ASSERT_EQ(result.committed_rounds[0].occurrences.size(), 1U);
  const auto& occurrence = result.committed_rounds[0].occurrences[0];
  EXPECT_EQ(occurrence.skipped_slots, 2U);
  EXPECT_EQ(occurrence.emission.scheduled_time_us, 15);
  auto candidate = take(f.processor->read_candidate({std::string("a")}));
  EXPECT_EQ(candidate.bindings[0].skipped_count, 1U);
  EXPECT_EQ(candidate.bindings[0].last_slot_or_time, 15);
  EXPECT_EQ(candidate.bindings[0].reason, RequestPendingReason::NotReady);
  EXPECT_TRUE(take(f.processor->pending_occurrences(16)).empty());
  ASSERT_TRUE(f.reopen().ok());
  f.capture_ready = true;
  ASSERT_TRUE(take(f.processor->pump(17)).operation_status.ok());
  EXPECT_EQ(f.captures, 1U);
  auto ready = take(f.processor->pump(20));
  ASSERT_TRUE(ready.operation_status.ok()) << ready.operation_status.message();
  candidate = take(f.processor->read_candidate({std::string("a")}));
  EXPECT_TRUE(candidate.bindings[0].accepted);
  EXPECT_EQ(candidate.bindings[0].skipped_count, 1U);
  EXPECT_EQ(candidate.bindings[0].last_slot_or_time, 20);
  EXPECT_EQ(candidate.bindings[0].reason, RequestPendingReason::None);
  EXPECT_EQ(f.captures, 2U);
}

TEST_P(EventRequestContract, AbsoluteCandidateDeadlineCapExpiresPendingIntentAtExactBoundary) {
  RequestFixture f;
  ASSERT_TRUE(f.define().ok());
  f.capture_ready = false;
  f.config.requests->trigger_bindings[0].deadline_column = "version";
  ASSERT_TRUE(f.start(GetParam()).ok());
  ASSERT_TRUE(take(f.processor->process(f.event(4, 1))).input_committed);
  auto id = take(f.processor->pending_occurrences(16))[0].id;
  auto deferred = take(f.processor->admit_occurrence(id, 4));
  ASSERT_TRUE(deferred.request_result);
  EXPECT_EQ(deferred.request_result->disposition, RequestDisposition::Deferred);
  ASSERT_TRUE(f.reopen().ok());
  auto expired = take(f.processor->pump(5));
  ASSERT_TRUE(expired.operation_status.ok()) << expired.operation_status.message();
  EXPECT_TRUE(take(f.processor->pending_occurrences(16)).empty());
  auto candidate = take(f.processor->read_candidate({std::string("a")}));
  EXPECT_TRUE(candidate.active);
  EXPECT_EQ(candidate.deadline_us, 51);
  EXPECT_TRUE(candidate.bindings[0].expired);
  EXPECT_FALSE(candidate.bindings[0].accepted);
  EXPECT_EQ(candidate.bindings[0].reason, RequestPendingReason::Expired);
  EXPECT_EQ(f.captures, 1U);
  ASSERT_TRUE(f.reopen().ok());
}

TEST_P(EventRequestContract, ActiveRequestRecoveryIsBoundedIndependentlyOfTerminalHistory) {
  RequestFixture f;
  ASSERT_TRUE(f.define().ok());
  f.config.requests->runtime.max_active_requests = 1;
  ASSERT_TRUE(f.start(GetParam()).ok());
  RequestKey active;
  for (std::int64_t offset = 0; offset < 5; ++offset) {
    ASSERT_TRUE(take(f.processor->process(f.event(offset, 1))).input_committed);
    auto id = take(f.processor->pending_occurrences(16))[0].id;
    auto admitted = take(f.processor->admit_occurrence(id, 1));
    ASSERT_TRUE(admitted.request_result && admitted.request_result->record);
    active = admitted.request_result->key;
    if (offset < 4)
      ASSERT_TRUE(take(f.processor->cancel_request(active, 1)).operation_status.ok());
  }
  QueryContext tiny;
  tiny.max_rows = 1;
  EXPECT_EQ(
      f.processor->query_sql("SELECT phase FROM request_meta WHERE k='a'", tiny).status().code(),
      ErrorCode::ResourceExhausted);
  ASSERT_TRUE(f.reopen().ok());
  auto requests = take(f.processor->active_requests());
  ASSERT_EQ(requests.size(), 1U);
  EXPECT_EQ(requests[0].admission.request_id, active.request_id);
  EXPECT_EQ(requests[0].state.phase, RequestPhase::Accepted);
}

INSTANTIATE_TEST_SUITE_P(FourStores, EventRequestContract, testing::Values(0, 1, 2, 3));
} // namespace
} // namespace eventflow::test
