# Built-in trigger history

`TriggerSet::bind` automatically installs the protected `trigger_audit` relation. The standalone streaming module uses that binder too. Do not ask the application to create this table, attach an OccurrenceSink, or write audit rows. An existing object with the reserved name makes binding fail atomically.

## Query it

Use the actual ticker printed by [streaming_database](../../../examples/streaming_database/README.md), for example `TICKER_0`:

```sql
SELECT rule_id, event_kind, occurred_at_us, evaluated_at_us,
       scheduled_at_us, request_id, plan, firing_revision
FROM trigger_audit
WHERE scope_key_0 = 'TICKER_0';
```

With an already open SDK client, supply the key separately:

```cpp
EF_ASSIGN_OR_RETURN(auto history, client->query(
    "SELECT rule_id, evaluated_at_us, request_id FROM trigger_audit",
    Row{std::string{"TICKER_0"}}));
```

Run `bash examples/streaming_database/run.sh` for the complete database deployment. While it is RUNNING, query trigger history through Meta using the actual ticker and endpoint. The SQL above is an additional read-only inspection, not a claim that the verifier prints those rows. See [recipes](recipes.md) for setup; the example creates a normal checkpoint but does not validate process reconstruction.

Queries require one complete scope, like other EventFlow queries; no all-shard SELECT, ORDER BY or SQL LIMIT is added. Scope columns are `scope_key_0`, `scope_key_1`, etc., in partition order and with the original types. The current cluster takes one String ticker; embedded owners can use supported composite keys. `bound.explain()["trigger_audit"]` reports source-to-audit column mapping, relation ID and retention settings.

## Schema and meaning

All time fields below use processing-time microseconds, not the news payload timestamp or Kafka event time.

| Columns | Type and meaning |
| --- | --- |
| `scope_key_N` | Non-null original key types. Together with occurrence_id they form the primary key; partitioning uses only the scope columns. |
| `occurrence_id` | Non-null STRING. Stable identity of the emitted occurrence. |
| `rule_id`, `rule_version` | Non-null STRING. The name supplied to Trigger/Pattern binding and its version. |
| `emission`, `event_kind` | Non-null STRING. Emission name (Pattern uses match) and the driving event kind. |
| `occurred_at_us` | Non-null INT64. FSM emission time or CEP match completion time. |
| `evaluated_at_us` | Non-null INT64. Time the owner actually evaluated/emitted this occurrence; delayed CEP release can be later than match completion. |
| `scheduled_at_us` | Nullable INT64. Original Clock slot where available; a late 5-second slot processed at 7 seconds keeps both timestamps. |
| `skipped_slots` | Non-null STRING. Number of Clock slots coalesced in this firing. |
| `firing_revision` | Non-null STRING. Confirmed shard revision containing both the occurrence state and this row. Not a later capture/admission revision. |
| `request_id`, `binding_id`, `plan` | Nullable STRING. Proposed request identity and its binding/plan, when a request intent exists. |
| `candidate_epoch`, `request_mode` | Nullable STRING. Candidate epoch and WAIT or TRY_ONCE_AT_FIRING. |
| `intent_deadline_us` | Nullable INT64. Absolute pre-admission deadline. |
| `source_id`, `source_topic` | Nullable STRING. Source driving this owner evaluation. |
| `source_partition`, `source_offset` | Nullable INT64. Source delivery identity; Clock does not fabricate source positions. |

UInt64 versions/revisions/counters are canonical decimal strings so SQL's signed Int64 and JSON floating-point consumers cannot truncate them. Payloads, complete match history and guard-read images are not copied into the audit table. It is not a full persisted TriggerWitness.

## Emission is not request success

Write the row when the occurrence is actually retained/emitted, before waiting for model success. Wait/NotReady and a later TryOnce skip keep the historical row. The proposed `request_id` is stable, but `read_request(scope, id)` may still be NotFound until admission, or remain absent after a skip. Read request status and capture/result relations to determine admission and completion.

False conditions, inactive sessions and consumed once bindings can suppress emission entirely; they do not create a row. Consuming/acknowledging the occurrence outbox does not remove historical audit rows. Legacy level bindings and direct request admissions have no declarative occurrence; inspect their existing request records rather than inventing a trigger firing.

## Retention, atomicity and compatibility

`ProcessorConfig.trigger_audit` uses [TriggerAuditConfig](../../../include/eventflow/trigger_audit.hpp):

- `relation` is assigned by `TriggerSet::bind`; application code does not supply an audit DDL. Raw legacy ProcessorConfig with relation=0 remains unconnected to auditing.
- `max_rows_per_scope` defaults to 128.
- `max_bytes_per_scope` defaults to 256 KiB of KV key plus encoded row bytes, excluding backend overhead.

Set retention bounds before binding if the application needs different limits. The owner validates that they fit its scan budgets. Retention keeps the newest `(evaluated_at_us, occurrence_id)` entries; ties use stable identity order. Query order itself is not a time-order promise. Insertions and evictions commit together; a single oversized row or insufficient round budget returns ResourceExhausted and rolls back that round, without silently losing a required audit entry. Rows also honor `max_occurrence_bytes`.

The private lifecycle writer atomically commits audit rows, trigger state and the relevant input outcome. Public inputs/hooks, generic request-status writes and observers cannot forge the protected table. UNKNOWN confirmation retries the original batch; exact source replay does not create another row. Coordinated checkpoints include audit rows, so restoration retains history and the same dedup boundary. Logs are bounded, not permanent archives.

Table schema/role and retention policy participate in Catalog/lifecycle fingerprints. An old deployment lacking this table is not automatically migrated by a new binary. Use a fresh deployment or an explicitly implemented migration; retain RocksDB WAL.

Implementation sources: [binding](../../../src/runtime/computation.cpp), [schema and retention](../../../src/runtime/trigger_audit.cpp), [owner emission](../../../src/runtime/event_processor.cpp). Reader-facing detail: [implementation guide](../../../docs/implementation_guide.md).
