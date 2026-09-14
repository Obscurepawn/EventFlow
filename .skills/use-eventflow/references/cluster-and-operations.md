# Cluster, input and operating APIs

Use this reference for a static database module, remote client, database-managed source consumption, checkpoint or consumer build. The complete application is [streaming_database](../../../examples/streaming_database/README.md). For custom C++ computation definitions read [Trigger APIs](trigger-api.md); for full examples read [recipes](recipes.md).

Contents: SDK methods; wire protocol; source delivery; time; storage/checkpoints; build and diagnostics.

## ClusterClient: one Meta address

Source: [cluster_client.hpp](../../../include/eventflow/cluster_client.hpp), [implementation](../../../src/client/cluster_client.cpp). All methods are synchronous and `noexcept`. Fallible methods return `StatusOr<T>`; do not unwrap an error. The SDK proxies through Meta rather than opening direct Compute connections after discovery.

| Method | Semantics |
| --- | --- |
| `ClusterClient::open(endpoint, config={})` | Return a client for a valid HTTP endpoint. Construction does not establish that a deployed computation is RUNNING; call discovery. |
| `discover()` | Fetch deployment state/version, current time and every shard route; validate route shape. Use logical node/shard identity rather than guessing a key's node. |
| `catalog()` | Fetch current Catalog/schema and computation metadata. Inspect this before constructing a schema-ordered `Row`. |
| `ddl(sql)` | Submit one supported table/MV definition while STAGING. DDL changes the computation definition; it is not an arbitrary runtime setting. Online DDL after deployment is rejected. |
| `deploy()` | Install the same Catalog and statically linked ComputationModule on the configured workers; RUNNING is published after their confirmations. It takes no arbitrary C++ module or job argument. |
| `ingest(relation, row, source, now_us, event_time_us={})` | Route by the relation's business key. Preserve `SourceIdentity`. Return `ClusterIngestResult`, including `input_committed`, `duplicate`, separate `operation_status`, node/shard and optional confirmed revision. |
| `query(sql, scope, max_rows=10000, max_bytes=4*1024*1024)` | Read materialized state for one explicit key. Return typed fields/rows plus actual query revision, Catalog version and node/shard. Does not initiate model computation; this also queries built-in [trigger history](trigger-audit.md). |
| `read_request(scope, request_id)` | Return the stored request audit: identity, phase and actual capture/admission versions and time. Use the returned ID rather than constructing an internal encoded ID. |
| `pump(now_us, max_functions=4096)` | Perform one bounded progress call across Compute owners. Includes timers, expiration and function progress; inspect `work_remaining` and pending-input barriers. Does not consume a source offset. |
| `drain(now_us, max_rounds=64, max_functions=4096)` | Repeat successful pump calls at the same time until no runnable work remains. Return FailedPrecondition if an unresolved input blocks time, ResourceExhausted if the round budget expires, or the original error. Does not wait for future clock slots. |
| `pause()` | Pause cluster input admission and Coordinator-driven time advancement. Retain state and installed definition. It is not job deletion. |
| `resume()` | Resume a paused/quiesced installed deployment after the lifecycle checks. It is not a way to replace its schema. |
| `checkpoint(checkpoint_id, now_us)` | Coordinate a consistent source/runtime/Catalog/storage cut. Success leaves the deployment QUIESCED; explicitly resume if continuing. See the recovery section below. |
| `status()` | Inspect control state, confirmed source progress, watermark and worker reports. Use this to distinguish input progress from timer-only work. |

`ClusterClientConfig` has timeout and request/response byte budgets. `ingest_attempts` defaults to 1; when increased, only eligible unconfirmed ingestion failures retry, using identical serialized bytes. Other mutations are sent once. A transport failure or UnknownCommit is ambiguous: reconcile the original operation; changing its ID can duplicate effects.

The application executable supplies a `ComputationModule` with `name`, `version` and `bind` to `database_main`. Its factory returns a complete `BoundComputation` and handles staged DDL. Coordinator and Compute bind the same application definition; SDK `deploy()` does not upload it. The generic default module is SQL-only; the standalone streaming executable installs its own `streaming_market` module.

The complete example defines `trades(ticker: String, trade_id: Int64, volume: Int64)`, followed by `trade_totals` and `market_features`. Read the deployed Catalog instead of assuming a fixture schema. There is no `submit_job()` / `unregister()` API, remote callback upload or online graph migration. Startup DDL can add supported MVs, but cannot dynamically register a C++ guard.

## HTTP/JSON equivalent

See [running the cluster](../../../docs/core_delivery.md) and [coordinator_service.cpp](../../../src/service/coordinator_service.cpp). Use `POST /v1/meta/<operation>` with JSON. SDK methods encode and validate this protocol; prefer them from C++.

| Operations | Request members |
| --- | --- |
| `discover`, `catalog`, `deploy`, `pause`, `resume`, `status` | `{}` |
| `ddl` | `sql` |
| `ingest` | `relation`, schema-ordered `row`, `source`, `now_us`, optional `event_time_us` |
| `query` | `sql`, `scope`, optional row/byte limits |
| `pump` | `now_us`, `max_functions` |
| `read_request` | `scope`, `request_id` |
| `checkpoint` | `checkpoint_id`, `now_us` |

`drain` is a client-side loop over `pump`, not a `/v1/meta/drain` endpoint. Wire metadata such as offsets, revisions, versions and microsecond times uses canonical decimal strings; row values retain their schema types. Inspect the SDK encoders when implementing another language. HTTP success alone is insufficient to confirm an input.

Read-only example for an already running local deployment:

```sh
curl --fail-with-body -sS http://127.0.0.1:8080/v1/meta/discover \
  -H 'Content-Type: application/json' --data '{}'
```

Service wrappers expose status-returning `create(...)`, `start(host, port)`, `is_running()` and `stop()`. Ordinary integrations should use the executable roles and existing scripts rather than reimplement service startup. Sources: [Coordinator](../../../include/eventflow/coordinator_service.hpp), [Compute](../../../include/eventflow/compute_service.hpp), [KV](../../../include/eventflow/kv_server.hpp).

## IMessageSource: delivery is separate from computation

Source: [message_source.hpp](../../../include/eventflow/message_source.hpp), [adapter implementation](../../../src/ingest/message_source.cpp). Calls on one instance must be serialized. The interface transports raw optional key/payload bytes; Coordinator decodes the configured subscription payload into a table row. Embedded integrations provide their own decoder. Kafka null payload and an empty string are distinct; choose the table-deletion semantics explicitly.

| API | Contract |
| --- | --- |
| `MemMockMessageSource::open(config)` | Finite retained in-memory log for deterministic tests; no broker. `append(partition, offset, key, payload, timestamp_ms={})` supplies test records and fails on retained-log capacity. |
| `HttpMessageSource::open(config)` | Remote adapter for the independent HTTP message server; bounded raw bytes, poll/position/ack, and the same delivery contract. The example server has process-local retained logs. |
| `KafkaMessageSource::open(config)` | librdkafka adapter with explicit assignments, bootstrap servers and group ID. Static manual assignment; no rebalance/automatic ownership protocol. |
| `poll(max_messages, max_bytes, timeout_ms)` | Return a bounded batch with instance-local `DeliveryToken`s. MemMock validates timeout but never sleeps. |
| `ack(token)` | Confirm the delivered prefix after durable input computation. A later completion cannot skip an earlier delivery. Repeat the most recent successful token if necessary; old retired tokens may be rejected. |
| `pause(partition)` / `resume(partition)` | Control delivery for the partition; pause still permits acknowledgements. |
| `seek(partition, next_offset)` | Revoke old inflight tokens and change the next delivery position; preserve pause, do not change group commits or lower the confirmed broker prefix. |
| `committed_position(partition)` | Return the next offset after the confirmed prefix; -1 means no committed position. |
| `close()` | Idempotently close without acknowledging outstanding records. |

Durable identity is `(source_id, topic, partition, offset)`. Delivery token `instance_id` and `delivery_id` are local acknowledgement capabilities and must not enter the engine's dedup key. `SourceMessage.timestamp_ms` is milliseconds; EventFlow processing/event time fields are microseconds. Convert with overflow checks rather than passing milliseconds directly.

For each input, preserve the original row, identity and timestamps until its outcome is resolved. If `input_committed=true`, acknowledge the source even when the returned `operation_status` reports unfinished later work. That flag confirms input state, not completion of every derived model. If the call fails before confirming input, do not acknowledge it.

An `ack` returning UnknownCommit requires retrying that exact token before another ack or seek on the same partition. Do not turn a new poll/token or a new source offset into an error-recovery shortcut. Respect the adapter's inflight and retained-log budgets.

`SourceSubscription` injects an `IMessageSource`, assignments, target relation and polling interval into Coordinator. [SourcePoller](../../../include/eventflow/source_poller.hpp) retains one original message and fixed processing time, calls the existing routed ingestion path, and acknowledges only confirmed input. UNKNOWN retries the original delivery/token. Permanent errors retain the original position and appear in `source_reader` / `background_status`; they are not skipped.

The CLI [source parser](../../../src/service/database_main.cpp) accepts `source.type=http` with `endpoint`, or `source.type=kafka` with `bootstrap_servers` and `group_id`. Both require `source_id`, `relation` and `assignments`; assignment `next_offset` uses a decimal string. The subscription accepts a schema-shaped JSON object or a schema-ordered JSON array; null payload is rejected rather than implicitly deleting a row. `poll_interval_ms`, `max_message_bytes` and adapter limits bound work and memory.

The complete example uses HTTP and needs no broker. Kafka uses the same SourcePoller path through its adapter, but real Kafka broker/Compose integration remains separately unverified. Do not replace an actual end-to-end integration claim with a protocol mock.

## Database time and explicit embedded progress

Source: [time.hpp](../../../include/eventflow/time.hpp).

| API | Contract |
| --- | --- |
| `ManualClock::create(initial_us=0)` | Create deterministic processing time. `advance(time_us)` sets a nondecreasing absolute time; it does not add a duration. `now_us()` reads it. |
| `SystemClock::create(restored_us=0)` | Anchor to Unix microseconds (at least restored time), then measure elapsed steady-clock time. Wall-clock corrections do not rewind processing time. |
| `IClock::now_us()` | Read a status-returning microsecond timestamp. Sample once for a logical owner operation rather than changing time inside its callbacks. |

The normal database workflow uses Coordinator's background loop. The example sets `tick_interval_ms=20` and `source.poll_interval_ms=10`; the client publishes, deploys and queries. No client pump/drain is needed to consume messages or settle function results. A positive tick interval advances Clock even when no new source record arrives.

The C++ Coordinator config defaults `tick_interval_ms` to 0; the executable's omitted field defaults to 100 ms. Zero disables automatic idle-time growth, but a configured source still starts the background consumer and completion progress. Fully manual tests should also omit the automatic source. Keep all times on the same axis: do not mix synthetic 5-second times with Unix microseconds. An unconfirmed input limits progress to its original processing time.

An embedded integration can call [ComputationRuntime](../../../include/eventflow/computation_runtime.hpp) for owner/executor progress, or drive EventProcessor and an executor explicitly. `Trigger::clock` itself creates no thread. SDK pump/drain remain bounded explicit control operations, not the recommended application consumption loop.

Event-time observations update source watermark/late metadata; current Trigger/CEP windows use processing time. Timer work may progress with no new source message. No observed business key means no per-key Clock activation.

## Storage and recovery boundaries

For application authors, storage is an injected `IStateStore`. Sources: [state_store.hpp](../../../include/eventflow/state_store.hpp), [cached_store.hpp](../../../include/eventflow/cached_store.hpp). Use public engine APIs for relation changes; manually writing its KV keys bypasses invariants.

| API | Meaning |
| --- | --- |
| `MemoryStateStore::open(config)` | Deterministic process-local store. Its snapshots survive only while the source object is retained. |
| `RocksStateStore::open(path, config)` | Persistent local backend used by KV Service. |
| `RemoteStateStore::open(endpoint, timeout_seconds=5)` | KV Service adapter for separated Compute/storage processes. Preserve identity across ambiguous commits. |
| `get_many(shard, keys)` | Bounded values with actual shard revision. |
| `scan(shard, prefix, after, limit)` | Bounded prefix page after an optional key; not an arbitrary cross-key SQL query. |
| `commit(batch)` | Owner/revision-checked atomic shard batch. Inspect Committed/Rejected/Unknown in `CommitResult`, independently of StatusOr transport success. This is a runtime/adaptor API, not a way to install application rows. |
| `status(shard)` / `limits()` | Confirmed shard revision/last commit identity; actual adapter capacity. |
| `create_snapshot(id)` / `snapshot_status(id)` | Immutable storage snapshot and its status. IDs use 1..128 ASCII letters/digits, '-' or '_'. This is only the storage boundary. |
| `MemoryStateStore::from_snapshot(source, id)` | Independent store cloned from a snapshot retained by the source object. |
| `RocksStateStore::from_snapshot(source_path, id, restore_path, config)` | Restore into a new, nonexistent active directory; never open the snapshot writable. |

Compute caches are bounded and respect owner/confirmed revision and ambiguous-commit handling. Do not add application caches that treat an unconfirmed write as visible committed state.

A cluster checkpoint freezes source positions, Catalog, runtime state and confirmed shard receipts with the KV snapshot. Pause database admission, obtain its fixed `status.now_us`, and call `checkpoint`, then either `resume()` or stop the processes and restore to a fresh Rocks directory with the matching checkpoint identity. Preserve logical owner/node/shard assignments and the definition; endpoints can change. The [run manual](../../../docs/core_delivery.md) explains process restoration; [core_cluster_test.cpp](../../../tests/core_cluster_test.cpp) is the real-process recovery regression. The complete example exercises normal checkpoint creation only, not process reconstruction or fault injection. On recovery, Coordinator seeks the source to saved checkpoint positions; a newer source-side commit must not skip replay. The source must retain the corresponding messages. There is no SDK `restore()` call.

Reliable source replay can replace an application redo log only with a verified replay protocol. This checkout retains RocksDB WAL. Do not claim automatic failover, MVCC, cross-key transactions or exactly-once external side effects from a snapshot test.

## Build an application against the source checkout

The complete starting point is `bash examples/streaming_database/run.sh`, with application-owned `definition.cpp`, database `main.cpp`, independent message server and verifier. To keep a consumer outside the repository, the following complete `CMakeLists.txt` links the same public targets; provide your own `main.cpp` and set `EVENTFLOW_ROOT` at configure time. It requires the full source checkout and its `cpp3rdlib` bundle.

```cmake
cmake_minimum_required(VERSION 3.20)
project(my_features LANGUAGES CXX)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT DEFINED EVENTFLOW_ROOT)
  message(FATAL_ERROR "Set EVENTFLOW_ROOT to the EventFlow source checkout")
endif()
add_subdirectory("${EVENTFLOW_ROOT}" "${CMAKE_BINARY_DIR}/eventflow" EXCLUDE_FROM_ALL)
add_executable(my_features main.cpp)
target_link_libraries(my_features PRIVATE ef_computation)
# The consumer output directory differs from EventFlow's build/bin layout.
target_link_options(my_features PRIVATE "LINKER:--disable-new-dtags")
set_target_properties(my_features PROPERTIES
  BUILD_RPATH "${EVENTFLOW_ROOT}/cpp3rdlib/lib")
if(CMAKE_BUILD_TYPE STREQUAL "Release")
  include(CheckIPOSupported)
  check_ipo_supported(RESULT consumer_ipo_supported LANGUAGES CXX)
  set_property(TARGET my_features PROPERTY
    INTERPROCEDURAL_OPTIMIZATION "${consumer_ipo_supported}")
endif()
```

Configure/build from the consumer directory:

```sh
cmake -S . -B build -G "Unix Makefiles" \
  -DEVENTFLOW_ROOT=/path/to/EventFlow -DCMAKE_CXX_COMPILER=clang++ \
  -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=OFF
cmake --build build --target my_features --parallel 4
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 LSAN_OPTIONS=exitcode=23 \
  ./build/my_features
```

The template links an embedded computation consumer. For the static cluster executable, follow the current streaming target in CMakeLists and link `ef_database_main` with your module source. Use `ef_cluster_client` instead for an SDK-only program, or `ef_engine` for a SQL-only embedded program. Link `ef_message_source` when adapting a broker. The public targets propagate include directories, C++20 and applicable sanitizer flags; GoogleTest remains test-only. This local build RPATH refers to the checkout; packaging a standalone consumer requires copying its dependencies and choosing an appropriate relative runtime path.

For Release use a fresh directory, `-DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF`. Keep `-O3`, `NDEBUG`, supported LTO and no sanitizer/fast-math. Do not download/build third-party sources or switch to GCC to work around an integration issue. For clangd point the consumer to its generated compile database; the EventFlow checkout also supports `make compdb` with Bear.

## Diagnose the layer that failed

| Observation | Check |
| --- | --- |
| `query requires partition scope` | Supply `QueryContext.scope` or a supported equality predicate from which scope can be inferred; SDK queries take explicit scope. |
| No request after a Clock slot | Observed key, driver time, Session activity, Ready/when predicate, once flag, prerequisite admission and capacity. |
| Current MV changed but model output did not | Query does not launch models; admitted input is immutable. Inspect a new trigger/request, not an old result. |
| Input transport returned an error | Confirm the original outcome/identity; never acknowledge an unconfirmed input or retry it with a different offset. |
| Automatic consumption stops | Inspect RUNNING/paused state, source_reader.operation_status, pending original input, configured assignment and payload Schema; do not skip or ACK a failed input. |
| `drain` reports pending-input barrier | Resolve the original unconfirmed input before advancing cluster time. |
| DDL rejected | Check deployment state and supported grammar; no online migration exists. |
| No rows in Latest | Require whole-request success in the current candidate, then inspect admission order and candidate expiration. |
| Binary cannot load a bundled library | Check actual RPATH/layout and `ldd`; source include paths do not establish runtime dependency resolution. |
