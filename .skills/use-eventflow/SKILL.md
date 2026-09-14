---
name: use-eventflow
description: Build applications with EventFlow's C++ feature-computation database, including SQL materialized views, Trigger and Pattern rules, static functions, database-managed message consumption, cluster deployment and SQL queries, and checkpoint recovery. Use when a task consumes EventFlow APIs or adapts its examples; kernel development and general streaming research are outside this skill's focus.
---

# Use EventFlow

Turn a feature-computation requirement into a runnable application using the APIs in the available EventFlow checkout. Preserve the requested business logic and deployment mode. This skill describes current implementation, not a promise that every full-product design is implemented.

## Locate the source and choose the entry point

In this repository, the checkout root is two directories above this file. If the skill was copied elsewhere, locate the actual checkout containing `include/eventflow/`, `examples/`, and `CMakeLists.txt`; resolve repository paths against that checkout. Do not use machine-specific paths from another session. A binary-only runtime package can run its examples but cannot compile a new C++ application.

| User needs | Entry point | Read next |
| --- | --- | --- |
| Custom schemas and continuously maintained features | `Catalog`, SQL, `Engine`, `ComputationModule` | [Relational and execution APIs](references/relational-api.md) |
| Custom event/condition/Clock/CEP logic and calculations | `Trigger`, `Pattern`, static `Function`, `TriggerSet::bind`, `EventProcessor` | [Trigger and typed-function APIs](references/trigger-api.md) |
| Operate a running cluster from C++ or HTTP | `ClusterClient`, Coordinator/Meta | [Cluster and operations](references/cluster-and-operations.md) |
| Implement a complete streaming database application | Adapt `examples/streaming_database` | [Feature recipes](references/recipes.md) |
| List historical trigger names, timestamps and request identities | Built-in `trigger_audit`, queried with SQL | [Trigger audit](references/trigger-audit.md) |

Load only the relevant references. Public declarations under `include/eventflow/` and the linked example source settle version-sensitive details; if a declaration differs, follow the checkout and describe the difference.

## Build the application

1. Identify business key, base schema, row identity, source delivery identity, MV dependencies, trigger, and desired result. Ask only for business choices that cannot be inferred.
2. For a custom cluster definition, provide a static `ComputationModule` with stable name/version and a `bind` factory returning `BoundComputation`; pass it to `database_main` in an application executable linked by all service roles. Embedded Engine/EventProcessor remains available. The SDK controls deployment and startup DDL; it does not upload C++ callbacks.
3. Define base tables before dependent MVs. For computed requests, install an explicit candidate/session policy and bind the triggers atomically. Use `BoundComputation`'s returned Catalog, definition and registry together.
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

Static typed functions are compiled into the application. Dynamic UDF/UDTF/UDAF upload, Process Function, arbitrary remote job submission/unregister, online DDL/graph migration, HA, MVCC and cross-key computation are not delivered interfaces. MemMock, HTTP and Kafka implement the same IMessageSource contract used by SourcePoller. The CLI supports HTTP/Kafka subscriptions; the complete example uses an independent HTTP message server. Kafka retains static manual assignment, and real broker/Compose validation remains separate. Offer an implemented route or state the missing capability instead of inventing an API.

Return the chosen integration mode, concrete code/run command, relevant semantic assumptions and actual verification outcome. Keep deployment addresses and runtime budgets separate from business computation definitions.
