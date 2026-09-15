---
name: use-eventflow
description: Build applications with EventFlow's C++ feature-computation database, including SQL materialized views, SDK-uploaded C++ UDFs, submitted Trigger rules, database-managed message consumption, cluster deployment and SQL queries, and checkpoint recovery. Use when a task consumes EventFlow APIs or adapts its examples; kernel development and general streaming research are outside this skill's focus.
---

# Use EventFlow

Turn a feature-computation requirement into a runnable application using the APIs in the available EventFlow checkout. Preserve the requested business logic and deployment mode. This skill describes current implementation, not a promise that every full-product design is implemented.

## Locate the source and choose the entry point

In this repository, the checkout root is two directories above this file. If the skill was copied elsewhere, locate the actual checkout containing `include/eventflow/`, `examples/`, and `CMakeLists.txt`; resolve repository paths against that checkout. Do not use machine-specific paths from another session. A binary-only runtime package can run its examples but cannot compile a new C++ application.

| User needs | Entry point | Read next |
| --- | --- | --- |
| Custom schemas and continuously maintained features | `SubmittedComputation.ddl`, SQL materialized views | [Relational and execution APIs](references/relational-api.md) |
| Uploaded functions with source conditions or Clock | `register_udf`, `SubmittedTrigger`, `FunctionRef` | [Uploaded UDF example](../../examples/streaming_database/README.md) |
| Embedded C++ or advanced Pattern APIs | `Trigger`, `Pattern`, `TriggerSet::bind`, `EventProcessor` | [Trigger and typed-function APIs](references/trigger-api.md) |
| Operate a running cluster from C++ or HTTP | `ClusterClient`, Coordinator/Meta | [Cluster and operations](references/cluster-and-operations.md) |
| Implement a complete streaming database application | Adapt `examples/streaming_database` | [Feature recipes](references/recipes.md) |
| List historical trigger names, timestamps and request identities | Built-in `trigger_audit`, queried with SQL | [Trigger audit](references/trigger-audit.md) |

Load only the relevant references. Public declarations under `include/eventflow/` and the linked example source settle version-sensitive details; if a declaration differs, follow the checkout and describe the difference.

## Build the application

1. Identify business key, base schema, row identity, source delivery identity, MV dependencies, trigger, and desired result. Ask only for business choices that cannot be inferred.
2. For the normal cluster workflow, compile a C++ UDF into a shared library using the same SDK/toolchain as the server. Start generic `eventflow` processes with native UDF upload enabled, call `ClusterClient::register_udf`, construct a `SubmittedComputation` referencing the returned function version, then call `submit_computation` and `deploy`. This applies before the first deployment. Use `examples/static_streaming` only when the user requests the compiled-module or embedded API route.
3. Define base tables before dependent MVs. Submitted definitions specify session_lifetime_us, source conditions or every_us, capture_relation/capture_fields and the function reference. The server validates and builds the Catalog and request state together. For advanced embedded APIs, use BoundComputation's returned Catalog, definition and registry together.
4. For the normal cluster workflow, configure a `SourceSubscription`: Coordinator owns SourcePoller, durable-input ACK, time advancement and function completion. Producers publish raw messages; clients deploy and query. If using an embedded owner directly, supply its execution loop explicitly. A builder or SQL read alone does not execute a model.
5. Query materialized results with a business-key scope, and inspect persistent request records when verifying admission, fixed inputs or Latest selection. Start from the complete `examples/streaming_database/run.sh` and its independent definition. Use the consumer CMake template only when a separate application directory is required.

## Contracts that change implementation choices

- One business key is handled by one shard owner. Shared KV does not add cross-key joins, cross-shard transactions or automatic resharding.
- SQL MVs update with accepted base changes. Trigger predicates read stable current relations when evaluated; they do not subscribe to arbitrary MV CDC merely because a `ViewRef` appears in a predicate.
- `capture_match` freezes a projection of the matched events at the occurrence; `capture_current` reads the declared current relation at actual admission. Both preserve admitted inputs across later updates.
- Requests require an installed, active Session. Clock scheduling, request readiness, one-per-candidate consumption and whole-request success are separate conditions.
- A Clock repeats for an observed key while database time is driven. The cluster example uses the Coordinator background loop; the client never pumps it. `drain(now_us)` settles work at one time and returns; it does not wait for the next period. Missed slots coalesce. Repeated requests need `once_per_candidate=false` and an active, ready session.
- Treat `input_committed` separately from the operation status and model completion. Acknowledge upstream only after confirmed input; preserve the original delivery bytes/identity on retries. An ambiguous commit is not evidence of rejection.
- `TriggerSet::bind` automatically installs `trigger_audit`. Each emitted occurrence has one historical row, independent of later request success; query it per key without creating a table. Default retention is 128 rows / 256 KiB per scope.
- Latest selects successful requests by candidate and admission order, not wall-clock completion order. Current query revision is not the request's captured revision.
- Source replay positions, Catalog/runtime state and KV snapshot must form one coordinated checkpoint. A raw KV flush alone does not provide cluster recovery; RocksDB WAL remains enabled.

## Code and execution conventions

Use C++20, Clang and CMake/Make; dependencies are headers/prebuilt libraries under `cpp3rdlib/`. Application examples use `Status` / `StatusOr<T>`, `noexcept`, snake_case functions, English ASCII code/comments and designated aggregate fields. Keep row values in their declared schema order. Do not use exceptions as business control flow.

Development runs use ASan/UBSan/LSan; Release uses `-O3`, `NDEBUG`, supported LTO and no sanitizers/fast-math. `run.sh` handles build details. When changing code, build the relevant target and verify the actual result; when only explaining APIs, a source check suffices.

Verify the business-specific outcome, not just process startup: include a positive case, an unmet-condition or timing boundary where relevant, and the expected MV/request/Latest result. For a cluster example, include real Coordinator, KV and multiple Compute processes. Do not represent a MemMock or protocol mock run as real Kafka evidence.

## Current capability boundary

Native C++ UDF upload and structured computation submission are the default example workflow; see examples/streaming_database/README.md and specs/native-udf-delivery.md. Upload and submission are limited to STAGING before the first deployment. Functions run in the Compute process with a matching C++ ABI; only trusted, deterministic, bounded code is supported. Shared KV stores the original library bytes and registration metadata, and Compute reads them during deployment or recovery. Compiled modules and embedded APIs remain separate reference routes. Full UDTF/UDAF families, Process Function, arbitrary remote job lifecycle, online DDL/graph migration, HA, MVCC and cross-key computation are not delivered interfaces. MemMock, HTTP and Kafka implement the same IMessageSource contract used by SourcePoller. The CLI supports HTTP/Kafka subscriptions; the complete example uses an independent HTTP message server. Kafka retains static manual assignment, and real broker/Compose validation remains separate. Offer an implemented route or state the missing capability instead of inventing an API.

Return the chosen integration mode, concrete code/run command, relevant semantic assumptions and actual verification outcome. Keep deployment addresses and runtime budgets separate from business computation definitions.
