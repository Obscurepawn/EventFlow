# Trigger, Pattern and typed function API

Triggers decide **when to request** computation; SQL MVs express **what data depends on what**. Querying an MV does not create a trigger. These C++ declarations are bound before the owner starts and compiled into its executable. The current SDK can drive installed triggers; it cannot upload arbitrary C++ trigger/model callbacks. Dynamic UDF, UDTF, UDAF and Process Function families are deferred.

## Typed schemas, events and predicates

Sources: [typed_schema.hpp](../../../include/eventflow/typed_schema.hpp), [static_function.hpp](../../../include/eventflow/static_function.hpp), [trigger_expression.hpp](../../../include/eventflow/trigger_expression.hpp), [typed_trigger.hpp](../../../include/eventflow/typed_trigger.hpp).

| API | Meaning and requirements |
| --- | --- |
| `SchemaTraits<T>::fields() noexcept` | Explicit specialization returning a tuple of `field<&T::member>("column_name")`. C++20 reflection is not assumed. The struct needs nonthrowing default/move construction. |
| `field<&T::member>(name)` | Maps a real member to one named field. Supported members: `bool`, `std::int64_t`, `double`, `std::string`, `std::vector<double>`, and their `std::optional` forms. |
| `typed_fields<T>()` | Validated ordered field signature; rejects duplicate names/invalid declarations. |
| `encode_typed(value)` / `decode_typed<T>(row)` | Convert using exactly the mapped field order and types. Nullable input needs an optional member. |
| `bind_typed_schema<T>(actual_fields)` -> `binding.decode(row)` | Bind by field name and compatible type/nullability, then decode an actual row. Reordered/superset relation fields are supported; every actual field is still validated. Int64 members can bind `TIMESTAMP_MICROS`; there is no unchecked numeric conversion. |
| `Function<&fn>{.id, .version, .implementation_identity}` | A compiled callable `StatusOr<Output> fn(const Input&) noexcept`, with both types described by `SchemaTraits`. Also supports an appropriate empty/captureless callable. ID is nonempty, version positive and implementation identity stable/nonempty. |
| `Function::definition()` | Returns a checked `StaticFunctionDefinition`; does not run the model. Changing implementation behavior requires an appropriate identity/version update, never a pointer-address identity. |
| `EventRef::source({.kind, .payload_relation})` | Declares event-kind selection and the relation whose visible schema describes `ProcessorEvent.values`. Does not create a source, emit CDC or perform ingestion. |
| `EventRef::source<T>({.kind, .payload_relation}).key_by(&T::key, ...)` | Declares one typed source and validates the existing Catalog partition key. Does not repartition, filter a particular key value, construct an envelope or route it to Compute. `.bind(snapshot)` returns the untyped reference after checks. |
| `event.column<T>("field")` | Column of the currently evaluated event, checked against its declared payload schema. Scalar `T` is `bool`, `int64_t`, `double` or `std::string`. |
| `ViewRef(relation_id, alias = {}).column<T>("field")` | Declares a read from a current same-scope, unique-per-partition relation/MV. The owner supplies one consistent read image for this evaluation. Put aggregates such as `SUM` in an MV, then test its column. |
| `column ==/!=/</<=/>/>= literal_or_same_typed_column` | Produces a Predicate. Missing rows and NULL comparisons are false. Use `is_null()` or `value_or(fallback)` to make absence explicit. `is_true()` is available for bool columns. |
| `Predicate::constant(bool)`, `!p`, `p && q`, `p || q` | Boolean expressions; AND/OR short-circuit. |
| `Predicate::guard(read_bindings, fsm_guard, implementation_identity)` | Advanced pure bounded guard with explicit read aliases; not every opaque FSM guard has a safe CEP adapter. Prefer typed fields or typed Pattern guards. |
| `Predicate::event_guard(event, expected_fields, row_callback, implementation_identity)` | Advanced validated event-row callback. Typed Pattern `.where` supplies this adapter automatically. |
| `TriggerDuration(chrono_duration)`, `from_microseconds(int64)`, `microseconds()` | Exact checked integral conversion to microseconds. Sub-microsecond fractions and overflow fail at binding. Use `std::chrono_literals`, e.g. `5s`, `100ms`. Time is processing time. |

Source events are the observation points. After a source round updates its base tables and cascaded MVs, the trigger evaluates its declared current reads. A predicate referring to an MV does not install a general subscription to every possible MV change. Async completion visibility and subsequent admission are progressed by the owner. Event-time watermarks are not Pattern or Clock deadlines.

## Ordinary Trigger methods

Source: [trigger.hpp](../../../include/eventflow/trigger.hpp), [compiler](../../../src/runtime/trigger.cpp).

Fluent methods return new immutable declarations. Keep the returned value or chain the call. Invalid combinations are reported by `bind_trigger`/`TriggerSet::bind`, not thrown during chaining.

| Method | Semantics |
| --- | --- |
| `Trigger::on_event(id, EventRef)` | Evaluate on this event kind. A single-stage true predicate emits on every qualifying event; it is not an automatic false-to-true edge detector. |
| `Trigger::on_event(id, event_kind_string)` | Kind-only form without a typed payload declaration. `"*"` is the explicit wildcard form. |
| `Trigger::clock(id, ClockSchedule)` | Periodic source for each observed business scope; details below. |
| `Trigger::pattern(id, Pattern)` | Compile an untyped sequence/group into bounded E-NFA. |
| `Trigger::pattern(id, TypedEventRef<T>, TypedPattern<T>)` | Same bounded E-NFA using one typed input kind. |
| `.version(positive_version)` | Stable declaration version, default 1. |
| `.implementation_identity(identity)` | Stable nonempty callback/definition identity. Required for a custom admission callback; include identities wherever application callback behavior affects persistence. |
| `.when(predicate)` | AND into the current stage's condition. Calls before `then_next` belong to stage one; calls after it belong to stage two. Pattern conditions use `.where`, not this method. |
| `.then_next(event, NextPolicy::OfKind)` | One armed wait per scope. Ignore unrelated kinds; consume the first event of the requested kind. If its predicate fails, end that wait without emission. |
| `.then_next(event, NextPolicy::Strict)` | The very next owner event must be the requested kind and satisfy its condition; another event ends the wait. |
| `.within(duration)` | Required positive bound for `then_next`; the deadline is exclusive. For E-NFA use `Pattern::within` instead. |
| `.rearm_on_next_event()` | Required explicit policy for `then_next`: completion/rejection/timeout returns to idle; a later event may arm a new wait. The event that ends the wait is not reused to arm it. |
| `.emit(name)` | Ordinary trigger emission name, default `"trigger"`. Pattern emission is fixed to `"match"`. Emitting alone does not run a model. |
| `.limits(CepLimits)` | Pattern-only count/byte/work bounds; too-small limits reject/exhaust work rather than retaining unlimited candidates. Ordinary triggers do not accept CEP limits. |
| `.in_session(SessionRef::candidate(id))` | Explicit request lifecycle scope. Must agree with the installed candidate activation policy. Does not create/activate a session on its own. |
| `.request(TriggerRequest)` | Bind to an already-defined request plan; requires explicit mode, positive pending bound, valid ID/plan and installed session. |
| `.capture_match<&project>({labels...}, identity = {})` | Typed immutable match projection, then `.request(Function<&model>{...}, options)`. Pattern-only. |
| `.capture_current<&project>(view_id, identity = {})` | Typed projection of one current per-scope row at admission, then a matching typed Function request. |

`then_next` supports exactly two stages, and requires both `within` and `rearm_on_next_event`. These latter methods without `then_next` fail. Use Pattern for more stages, overlapping starts, optional/repeated groups or alternatives. With typed capture, call ordinary Trigger modifiers before `capture_*`; the returned `CapturedTrigger<Input>` exposes the final typed `request` method.

## Clock scheduling and its two drivers

`ClockSchedule` fields are `.policy_id`, `.every`, `.phase` (default zero), `.event_kind` (optional; compiler generates a private kind when omitted). `every` must be positive and `phase` nonnegative. Policy IDs and Clock kinds must be distinct in the computation; a Clock kind cannot collide with a declared source kind.

Slots are aligned to `phase + n * every`. The first accepted event for a scope creates its clock, beginning at the first slot strictly after that event's processing time. An unseen key has no timer. After a firing the schedule advances again; there is no one-shot limit or `.repeat()` call. Overdue slots coalesce into the latest due slot, and skipped slots are audited rather than replayed as an unbounded burst.

| Driver | How to use it |
| --- | --- |
| Database-managed Clock | Compile `Trigger::clock(...)` in the application's ComputationModule, bind it and enable Coordinator time advancement. [streaming_database](../../../examples/streaming_database/definition.cpp) defines a 200 ms Clock; the service loop drives it without client pump/drain. The declaration itself starts no thread. |
| Embedded or explicit progress | An embedded owner must service time and its executor; [ComputationRuntime](../../../src/runtime/computation_runtime.cpp) provides the common implementation. SDK `pump/drain` are bounded explicit control operations, with a supplied logical time, not dynamic rule upload. |

A Coordinator can instead drive installed clocks with its automatic ticker when configured and running. Avoid mixing incompatible synthetic timestamps with wall-clock ticking. Continuing Clock firings do not guarantee continuing successful model requests: candidate expiry, readiness, `.once_per_candidate`, admission prerequisites, executor capacity and retained-state budgets still apply. An expired candidate does not itself delete the key's Clock schedule.

## Pattern operators and matching

Sources: [trigger_pattern.hpp](../../../include/eventflow/trigger_pattern.hpp), [typed_trigger.hpp](../../../include/eventflow/typed_trigger.hpp), [CEP semantics](../../../include/eventflow/cep.hpp).

| Untyped API | Typed form / meaning |
| --- | --- |
| `Pattern::begin(label, EventRef)` | `Pattern::begin<T>(label)` starts a typed pattern; source is supplied at `Trigger::pattern`. Labels identify captured events. |
| `.where(Predicate)` | Both forms accept predicates. Typed form also accepts `.where<&guard>("guard.v1")` or `.where(callable, "guard.v1")`, where the callable returns exactly `StatusOr<bool>(const T&) noexcept`. Null function pointers fail validation. Repeated conditions AND together. |
| `.next(label, event)` | Typed `.next(label)`. Strict adjacency within the stream delivered to this pattern. |
| `.next_of_kind(label, event)` | Typed `.next_of_kind(label)`. Ignore other kinds; the first same-kind guard rejection kills that path. |
| `.followed_by(label, event)` | Typed `.followed_by(label)`. May skip rejected events, then takes the first acceptable continuation for each path. |
| `.followed_by_any(label, event)` | Typed `.followed_by_any(label)`. Can both take and retain an ignore branch even for a matching event, allowing additional combinations. Bound its state carefully. |
| `.then(other_pattern, contiguity = CepContiguity::FollowedBy)` | Compose a whole group with explicit adjacency. Typed groups must use the same typed source. |
| `.or_else(other_pattern)` | Alternative group. Labels remain subject to compiler uniqueness/cardinality validation. |
| `.optional()` | Makes the **whole current expression** optional. To make only the last event optional, build that event separately and attach it with `.then(...)`. |
| `.repeat(minimum, maximum, inner = CepContiguity::FollowedBy)` | Repeats the **whole current expression** with an explicit finite maximum; repeated bodies cannot be nullable. No unbounded `one_or_more()` API. |
| `.within(duration)` | Mandatory positive processing-time window on the final pattern: `[first_take_time, first_take_time + duration)`. An event at the deadline is too late. |
| `.after_match(CepSkipStrategy)` | Final-pattern pruning policy, default `NoSkip`; options below. |

Apply `.where` while the current expression identifies one event; after `optional`, `repeat` or `or_else` it is a group and cannot accept an ambiguous event condition. Nested right-hand groups must not carry their own `within`/`after_match`; set final options after composition. Greedy, negative and event-time CEP operators are not implemented.

Typed Pattern receives every event of its declared kind, including same-kind guard failures; unrelated kinds are filtered out before adjacency, but still advance processing time/deadlines. Thus typed `.next` means the next declared-source event. Untyped Pattern sees the owner's full input stream, so unrelated kinds can break `.next`.

After-match policy is separate from adjacency:

| Strategy | Remaining paths/matches |
| --- | --- |
| `NoSkip` | Preserve overlapping starts and additional matches. |
| `SkipToNext` | Prune other paths/pending matches with the same start as the emitted match. |
| `SkipPastLastEvent` | Prune paths/pending matches whose start is at or before the emitted match's last event. |

Completed matches can be retained until earlier-start paths resolve so output/pruning order is deterministic. A `CepMatch` records `matched_at_us` and `emitted_at_us` separately. Do not equate callback delivery time with the event that completed the match.

## Explicit sessions

Sources: [candidate policy](../../../include/eventflow/event_processor.hpp), [module validation](../../../src/runtime/computation.cpp).

For any request-producing trigger:

1. Create a Candidate with `Catalog::define_candidate`, business-key `scope_fields`, and payload including a non-null `INT64` deadline field.
2. Set `ProcessorConfig.input_relations` and initialize `.requests`.
3. Install `.requests->candidate = CandidateExpiryPolicy{.relation, .deadline_column, .version, .on_source}`.
4. In the pure `on_source(RoundContext&, const ProcessorEvent&) noexcept` callback, return no activation for an ordinary event, or `CandidateActivation{.expected_epoch, .payload}` to open/replace the session. The expected epoch is zero initially or the current epoch; the new deadline must be in the future. Replacement advances the epoch. The owner writes the candidate row.
5. Attach `.in_session(SessionRef::candidate(candidate_id))` to each request trigger.

The installed policy, session, input and capture relations must share a compatible business-key shape. The current high-level API does not automatically construct candidate policies or support request creation without a candidate. A business revision is not a source offset: your activation callback decides which source changes begin a new candidate epoch. See the complete policy in the [streaming definition](../../../examples/streaming_database/definition.cpp).

## Request options and frozen input

Source: [TriggerRequest](../../../include/eventflow/trigger.hpp), [owner admission implementation](../../../src/runtime/event_processor.cpp).

| `TriggerRequest` field | Semantics |
| --- | --- |
| `.id` | Binding identity, not a concrete request ID. Required for an existing plan; generated typed requests default to the trigger ID. The owner derives concrete request identities from binding version and occurrence. |
| `.version` | Positive binding version, default 1. |
| `.plan` | Required predeclared request-plan name for ordinary `.request(options)`. Leave empty for `.request(Function, options)`; the binder generates its plan. |
| `.mode` | Required explicitly: `Wait` or `TryOnceAtFiring`; no implicit mode. |
| `.pending_for` | Required positive bound on the pre-admission intent. Capped by candidate expiry and optional `.deadline_column`. It is not the function's runtime timeout. |
| `.lifetime` | Optional accepted-request lifetime for a generated typed plan (default 30 seconds). Existing plans own their lifetime; this override is rejected for them. |
| `.once_per_candidate` | Default false. True permits one admitted request for this binding/epoch, not one execution ever. Pending once intents are deduplicated. A skipped admission does not consume success; a later model failure after admission does not reset the consumed binding. Expiry can terminally close that binding's pending opportunity. |
| `.required_accepted_binding` | Candidate-local prerequisite: another once binding or level binding must already be **accepted**, not necessarily successful/completed. Must refer to a distinct supported binding. |
| `.deadline_column` | Optional non-null Int64 Candidate payload field giving an additional absolute pre-admission deadline. |
| `.make_admission` | Advanced pure callback for existing plans; can supply bounded arguments and optional lifetime. Owner controls scope/plan/epoch/IDs. Requires a stable Trigger implementation identity. Cannot override a typed capture recipe. |
| `.machine` | Optional bounded request-machine settings for a generated typed plan, such as attempt/state/input/output limits. Existing plans own these settings and reject this override. |

`Wait` retains a bounded intent and retries when owner progress makes prerequisites/readiness/capacity available, until its deadline or candidate replacement/expiry. `TryOnceAtFiring` makes one logical admission attempt at that firing; absent prerequisites/readiness/capacity produce an audited skip, not a retry on a later input. A later Clock slot or new occurrence is a new opportunity. Commit-uncertainty resolution is still required and is not a new logical firing.

| Capture API | Time and shape |
| --- | --- |
| `capture_match<&project>({"a", "b"})` | Projects the immutable events owned by the matched occurrence when its intent is frozen. Values persist while admission waits and are not replaced by later MV values or recaptured on UNKNOWN resolution. |
| Match projection arguments | Required singleton label: `const Event&`; optional singleton: `std::optional<Event>`; repeated label: `std::vector<Event>` (const references to optional/vector are also supported). Parameters correspond to the declared label list and cardinalities must agree. Return `StatusOr<Input> noexcept`; `Input` requires SchemaTraits. |
| `capture_current<&project>(view_id)` | At request admission, read the declared unique-per-partition relation in the owner's current consistent image, decode named fields, and call `StatusOr<Input> project(const ViewRow&) noexcept`. Missing row means not ready. If `Wait` retries, the eventual admitted request can capture newer values than the original trigger saw. |
| `.request(Function<&model>{...}, options)` | Requires the capture output type to equal the function input type at compile time; binding also checks exact named field shape. Once accepted, worker tasks use frozen input even as live MVs change. |

Current capture records the actual admission source revision; it is not a historical read at the trigger's original revision. This gives the latest consistent owner snapshot satisfying trigger causality. The manifest distinguishes capture revision and admission revision. Keep pure callbacks bounded and free of external side effects.

## Bind and inspect the complete computation

Source: [computation.hpp](../../../include/eventflow/computation.hpp).

`TriggerSet::bind(catalog_snapshot, processor_definition, triggers, engine_config)` accepts 1-64 declarations, clones Catalog/Registry privately, binds schemas/read sets/sessions, checks IDs and Clock collisions, generates capture/function/status/Latest relations for typed requests and validates owner budgets/plans. Failure publishes nothing into the input Catalog or Registry.

It returns `BoundComputation`:

| Member | Use |
| --- | --- |
| `.catalog` | Complete single dependency graph, including generated relations. Pass to `EventProcessor::open`. |
| `.registry` | Compiled immutable callbacks; pass to the executor. May be absent for trigger-only definitions without requests. |
| `.definition` | Complete ProcessorConfig, including compiled rules/periodic policies and request plans; pass with the same Catalog to the owner. |
| `.explain()` / `.explanation` | Structured JSON with trigger semantics/read sets and generated relation/plan names. Use it to locate generated outputs, not hardcoded internal name encodings. |

`bind_trigger(snapshot, declaration)` compiles one declaration without module publication; it does not prove the complete session/plan/store contract. `TriggerSet::bind` is the normal application entry point. Store capacity and persisted metadata compatibility are still checked by `EventProcessor::open`.

For generated requests, inspect `bound.explain()["triggers"][i]["generated"]` for `capture_name`, `function_name`, `latest_name`, their relation IDs and `plan`. Query the generated Latest relation with a complete business scope. The recipe examples show full setup; copying only the Trigger chain omits required Catalog, session, input and execution setup.

## Built-in firing history

Every module bound with `TriggerSet::bind` automatically includes protected `trigger_audit`; no user DDL or sink registration. SQL can list rule_id, evaluated_at_us, scheduled_at_us and the proposed request_id by business scope. The row represents an emitted occurrence, including later waiting/skipped requests; it does not assert computation success. Outbox consumption preserves history, subject to bounded retention. See [audit schema and semantics](trigger-audit.md).
