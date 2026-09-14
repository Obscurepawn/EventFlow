# Relational computation and execution API

Use these APIs for a custom C++ application embedded around EventFlow. Use [cluster and operations](cluster-and-operations.md) for the precompiled distributed profile. Methods below are project-owned `noexcept` methods; check every returned `Status` or `StatusOr<T>`. This reference intentionally omits internal codecs and storage implementation helpers.

## Error and value types

Sources: [status.hpp](../../../include/eventflow/status.hpp), [types.hpp](../../../include/eventflow/types.hpp).

| API/type | Contract |
| --- | --- |
| `Status{}` | Success. A failure is `Status(ErrorCode::InvalidArgument, "message")`, or another appropriate code. |
| `Status::ok()`, `code()`, `message()` | Inspect a result without exceptions. Codes include `NotFound`, `Conflict`, `Unavailable`, `ResourceExhausted`, `FailedPrecondition`, `Internal`, and `UnknownCommit`. |
| `StatusOr<T>::ok()`, `status()`, `get()` | Move-only result. `get()` is null on failure. `status()` is OK on success. |
| `StatusOr<T>::value()`, `operator*`, `operator->` | Access only after a success check; unchecked failure aborts. These are not recovery APIs. Move ownership out with `std::move(result).value()`. |
| `EF_RETURN_IF_ERROR(expression)` | Return a failed `Status` from the current status-returning function. |
| `EF_ASSIGN_OR_RETURN(auto value, expression)` | Propagate failure or move the successful value into the supplied declaration/variable. Put macro uses inside braces when used in conditional bodies. |
| `Value`, `Row`, `Field`, `Schema` | A row is a schema-ordered vector of values. SQL types are `BOOL`, `INT64`, `FLOAT64`, `STRING`, `TIMESTAMP_MICROS`, and `FLOAT_VECTOR`; null is `std::monostate`. Use `std::int64_t`, not an arbitrary integer alternative, in rows. Timestamp storage uses Int64 microseconds. |
| `Schema::validate_definition()`, `validate(row)`, `index_of(name)` | Check keys, field types/nullability and row shape; resolve a visible field name. Prefer typed schema adapters when writing application callbacks. |

An outer `StatusOr` success is not enough for a mutation: inspect its commit/result fields as well. An UNKNOWN commit is unresolved, not rejected. Preserve the same operation and use the owning object's `resolve_pending_commit()`; do not acknowledge input or recreate work with a new identity.

## Catalog: describe one dependency graph

Source: [catalog.hpp](../../../include/eventflow/catalog.hpp).

| API | Arguments, effect and important checks |
| --- | --- |
| `Catalog::create()` | Returns an empty `unique_ptr<Catalog>`. No store or execution starts. |
| `Catalog::from_snapshot(snapshot)` | Reconstructs and validates a private Catalog from a snapshot. |
| `define_sql(sql, options = {})` | Defines one `CREATE TABLE` or `CREATE MATERIALIZED VIEW`; returns `RelationId`. Metadata only. Dependencies must already exist; no forward reference, execution or cluster deployment. |
| `define(statement_ast, options = {})` | Same operation using a parsed `StatementAst`. `parse_sql(sql)` is declared in [sql_ast.hpp](../../../include/eventflow/sql_ast.hpp). |
| `snapshot()` | Returns `CatalogSnapshot` with version, canonical fingerprint and relation descriptors. Take the snapshot after all intended definitions. A later Catalog edit does not update an already-open owner. |
| `CatalogSnapshot::find(id)` / `find(name)` | Returns a pointer to a descriptor owned by that snapshot; `NotFound` for an unknown relation. Keep the snapshot alive while using the pointer. |
| `bind_query_sql(sql)` / `bind_query(select_ast)` | Validates and binds a SELECT against this exact Catalog. Returns `BoundPlan`; does not query storage or activate a task. |
| `explain(relation_id)` | Returns a textual description of the defined relation/plan. |
| `dependencies(descriptor)` | Returns direct relation dependencies, deduplicating repeated inputs. |
| `define_candidate({.name, .scope_fields, .payload_fields})` | Defines controlled session metadata. The owner adds/manages `candidate_epoch` and `active`; application input must not write them directly. See [sessions](trigger-api.md#explicit-sessions). |
| `define_function(FunctionViewDefinition)` | Advanced: describes a compiled function node, with function ID/version/implementation identity, named inputs and output fields. The matching immutable callback must be in the registry. Definition alone never executes it. Prefer typed `Function` plus capture for new simple requests. |
| `define_request_status(RequestStatusDefinition)` | Advanced: controlled request lifecycle relation, tied to a request identity domain and candidate. The request owner writes it. |
| `define_latest(LatestViewDefinition)` | Advanced: selects a successful result using its status relation and current candidate. Selection follows admission order among eligible successes, not completion arrival order; failed/cancelled work cannot replace the winner. |
| `define_observation(ObservationRuleDefinition)` | Advanced metadata interface. Generic application observation-host integration is not part of the current usage path; its presence is not a general CDC subscription API. |

`DefinitionOptions.partition_key` can specify the partition key outside SQL. If SQL also declares `PARTITION BY`, both declarations must agree. The default `base_write_role = PublicInput` is appropriate for ingestion. `RequestCapture`, `RuntimeStatus` and `TriggerAudit` are protected owner-write roles; a request capture additionally requires an identity domain. Do not expose these as ordinary user tables.

Primary and partition keys are nonempty, non-null and use supported key types. The primary key must contain every partition field. Physical identity is ordered as partition fields first, then remaining primary-key fields, even if the SQL primary-key declaration used another order. A base table is unique per partition when its primary key contains exactly its partition fields.

## Supported SQL subset

Sources: [grammar](../../../src/sql/EventFlowSql.g4), [binder](../../../src/sql/binder.cpp), [expression evaluation](../../../src/sql/expression.cpp).

| Construct | Supported behavior / boundary |
| --- | --- |
| Tables | `CREATE TABLE ... PRIMARY KEY (...) ... PARTITION BY (...)`. Supply the partition key through SQL or `DefinitionOptions`; it does not default to the primary key. |
| Cascaded materialized views | `CREATE MATERIALIZED VIEW next AS SELECT ... FROM previous`. SQL dependencies form the feature DAG. Updates and deletes propagate through affected views. |
| Projection/filter | Explicit fields, `*`, aliases, literals, arithmetic, comparisons, boolean operations and `IS [NOT] NULL`. SQL identifiers follow the parser's rules, not an external database dialect. |
| Aggregate | `COUNT(*)`, `COUNT(x)`, `SUM(x)`, `AVG(x)` with `GROUP BY` covering the complete partition key, using simple key columns. No global aggregate or arbitrary cross-key group. |
| Join | At most one inner lookup join per SELECT; both inputs must be provably unique per partition. `ON` must equate every corresponding partition-key column. Aggregate raw events per key first, then join the aggregate views. |
| Numeric expressions | Checked arithmetic, finite floating-point values and supported type combinations. `CAST(... AS FLOAT64)` is the supported explicit cast; arbitrary SQL function calls/casts are not implemented. |
| Unsupported | SQL `INSERT/UPDATE/DELETE`, `ALTER/DROP`, outer/cross joins, subqueries, CTEs, `UNION`, `DISTINCT`, `HAVING`, `ORDER BY`, `LIMIT/OFFSET`, analytic/window clauses and dynamic SQL UDF families. Ingest through row/event APIs. |

Every read query needs **one complete business scope**, either `QueryContext{.scope = Row{...}}` or equality predicates from which all partition values can be inferred. Explicit and inferred scopes must agree. There is no distributed all-key SELECT or historical MVCC query. Row/byte limits are safety budgets, not SQL pagination or `LIMIT` semantics.

## Engine: SQL-only owner

Source: [engine.hpp](../../../include/eventflow/engine.hpp).

| API/type | Contract |
| --- | --- |
| `Engine::open(store, catalog, engine_config = {})` | Opens one serialized shard owner, validates persisted metadata against the exact Catalog, and resolves effective store budgets. Supply matching shard count and owner identity. Public `Engine::open` is for relational-only graphs; controlled candidate/status/Latest or function nodes require `EventProcessor`. |
| `EngineConfig` | `shard`, `shard_count`, `owner`; bounded rows/bytes/mutations/page size; bounded query-plan cache. A Compute node must divide node memory among its owners. |
| `apply(change, context = {}, hook = {})` / `apply_many(changes, context = {}, hook = {})` | One atomic relational round for the supplied public-base mutations and affected MV DAG. Inputs must belong to this owner; all changes in one batch share one round. `DispatchContext.processing_time_us` is the explicit processing-time axis. |
| `TableChange{.relation, .identity, .row}` | Full-row upsert when `.row` exists; delete when it is `std::nullopt`. Identity uses the descriptor's physical order; the row must agree with it. Do not mutate an MV or protected relation directly. |
| `DispatchResult` | `commit` records outcome/revision; `changes` are confirmed changes only. Inspect `commit.code == CommitCode::Committed`. |
| `install_sql(sql, options = {})` / `install(ast, options = {})` | Activate a supported new SQL definition in this Engine and initialize its state under the same commit boundary. This is distinct from Catalog-only definition and from distributed DDL. Fixed request Catalogs need controlled migration; do not hot-edit them. |
| `query_sql(sql, context = {})` / `query(bound_plan, context = {})` | Current committed per-scope read. Bound plans must match this Catalog's structure and provenance. Returns `QueryResult{.fields, .rows, .revision, .catalog_version}`. |
| `resolve_pending_commit()` | Retry/resolve this owner's retained UNKNOWN batch. Do not execute unrelated mutations while pending. |
| `snapshot()`, `revision()`, `config()` | Current Catalog metadata, confirmed revision and effective owner configuration. `snapshot()` is not a data checkpoint. |
| `has_pending_commit()`, `failed()`, `query_cache_metrics()` | Inspect uncertainty, fail-stop state and query-cache usage. Do not blindly retry a failed owner. |

Advanced `RoundHook` callbacks receive `RoundContext` after relational stabilization. `read_relation(id, identity, image)` and `scan_relation(id, scope, image, max_rows, max_bytes)` read bounded staged/before images of this round. These are not historical snapshots. Hooks and capture/guard callbacks must be pure, bounded and non-reentrant: no external I/O, no calls back into public Engine methods and no external side effects. Applications normally use `ViewRef` and typed capture instead.

## EventProcessor: source, time and request lifecycle

Source: [event_processor.hpp](../../../include/eventflow/event_processor.hpp).

| API | Contract |
| --- | --- |
| `validate_definition(catalog, engine_config, processor_config)` | Pure structural owner validation. Does not open storage; `open` still checks storage capacity and request-plan binding. |
| `open(store, catalog, engine_config, processor_config)` | Opens one serialized owner of both Engine and runtime state. For declarative triggers pass all parts of the `BoundComputation` returned by `TriggerSet::bind`. |
| `process(ProcessorEvent)` | Applies an accepted source envelope, deduplication, MV maintenance, session handling and trigger evaluation. The envelope carries source identity, scope, kind, typed/schema-ordered values, public table changes and processing time. Construct values, row identity and scope consistently; `key_by` does not construct or route this envelope. |
| `advance(time_us)` | Advances processing-time timers/TTL/Clock without introducing a source message. |
| `pump(now_us)` | Bounded progress across timers, pending trigger admissions and request tasks. It does not run function callbacks or create a background thread. Revisit while work remains; also service the executor. |
| `resolve_pending_commit()` | Resolve the pending operation using its original time and retained batch. Confirmed subrounds already returned are not repeated. |
| `admit_occurrence(occurrence_id, now_us)` | Explicitly attempt a durable trigger occurrence's request admission. Normal application drivers use `pump`. |
| `claim_tasks(request_key, now_us, limit = 16)` | Durably claim ready tasks for a request before executor submission. |
| `complete_request(request_key, completion, now_us)` | Validate attempt/function/owner identity and publish completion through the request owner. Executor output alone is not a committed feature. |
| `cancel_request(request_key, now_us)` | Request cancellation and publish the owner transition; does not force-kill an executing C++ callback. |
| `pending_operation()`, `has_pending_commit()`, `failed()` | Inspect operation ownership/uncertainty and fail-stop state. |
| `read_request(key)`, `active_requests()`, `pending_tasks(limit)` | Audit request manifest/phase/frozen input, enumerate active requests, or recover claimed work. A `RequestKey` is `{.scope, .request_id}`. |
| `read_candidate(scope)` | Current epoch, active interval and per-binding acceptance/skip/pending reasons. |
| `pending_occurrences(limit)`, `acknowledge_occurrences(ids)` | Durable outbox access for externally consumed occurrences. Acknowledge only after downstream durable acceptance. Request-bound occurrences follow their owner admission protocol. |
| `processed_positions()` | This shard's largest accepted offset plus one for each source partition; it is not the global broker acknowledgement frontier. |
| `periodic_positions()` | Next/last Clock slots and skipped counts per observed scope/policy. |
| `read_fsm(rule_id, scope)`, `read_cep(rule_id, scope)` | Inspect the compiled simple machine or bounded E-NFA paths/history/deadlines. Prefer these for debugging, not direct state mutation. |
| `query_sql(sql, context = {})`, `processing_time_us()`, `engine_config()` | Query through this owner, inspect its time and effective Engine limits. Querying does not trigger requests. |

`ProcessResult` has multiple meaningful fields: check the outer result, `operation_status`, `commit`, `pending` and `work_remaining`. A call may contain several individually committed timer/request subrounds; the entire call is not one global transaction. Acknowledge source delivery only when `input_committed` confirms acceptance, even when other work remains. `duplicate` records deduplicated source handling. Retain every confirmed `committed_rounds` result exactly once.

## Function execution: owner and worker are separate

Sources: [function_executor.hpp](../../../include/eventflow/function_executor.hpp), [request_runtime.hpp](../../../include/eventflow/request_runtime.hpp). For the shared owner/executor loop, read [ComputationRuntime](../../../src/runtime/computation_runtime.cpp). The complete cluster example uses this implementation through Compute and needs no client progress loop.

| API | Contract |
| --- | --- |
| `FunctionRegistry::create(config = {})` | Bounded registry for compiled callbacks. |
| `add(BuiltinFunction)` | Registers one exact ID/version/implementation identity and input/output signature. Pure deterministic callbacks may reuse cached results. No dynamic library upload is implied. |
| `describe(id, version)`, `clone()` | Inspect a signature or copy immutable entries into a private registry. `TriggerSet::bind` clones before adding generated definitions. |
| `SimpleFunctionExecutor::create(registry, config = {})` | Deterministic executor for embedding/tests. No worker thread. |
| `ThreadPoolFunctionExecutor::create(registry, config = {})` | Same contract with bounded worker threads for compiled callbacks. |
| `submit(task)` | Nonblocking bounded admission; returns `ResourceExhausted` when full. The task already contains frozen input and an opaque owner-generated attempt identity. Preserve and retry that same task when appropriate. |
| `SimpleFunctionExecutor::run_ready(now_us, max_tasks = 64)` | Explicit callback execution for the simple adapter. Its `poll` never executes work or sleeps. |
| `poll(max_completions, timeout_ms = 0)` | Transfers completed results and releases executor capacity. Keep completions until the owning `complete_request` commit is confirmed, including UNKNOWN handling. |
| `cancel(task_id)` | Queued work gets a cancelled completion; running code is not force-killed and retains capacity until it returns. |
| `metrics()`, `close()` | Inspect bounded task/cache usage; reject new work and cancel/drain shutdown work. Thread-pool close joins callbacks. |

The loop is: owner `pump/claim_tasks` commits task identity -> `submit` -> simple `run_ready` or thread-pool execution -> `poll` -> owner `complete_request` -> query committed results. Keep mapping each opaque task ID to its request key. Do not write worker output directly to KV. Duplicate physical execution may occur after bounded receipt eviction; owner fences protect publication. Do not claim generic exactly-once external side effects.

Application queries can also read the automatically installed [trigger_audit](trigger-audit.md) relation. Its dedicated role only permits private lifecycle writes; do not use `apply`, a public hook or SQL DDL to populate it.
