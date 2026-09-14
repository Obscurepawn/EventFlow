# EventFlow development

- This is a C++20 feature-computation database. Preserve the user-approved scope in specs/.
- Keep third-party headers and prebuilt libraries under cpp3rdlib/. Do not add third-party implementation source or build downloads. Use CMake/Makefile only for project builds.
- Unit tests use GoogleTest from cpp3rdlib, linked only by test targets when BUILD_TESTING is enabled.
- External dependencies must have interfaces and deterministic MemOnly/simple implementations for tests. Run shared contract tests against real and simple adapters where applicable.
- Final delivery additionally requires multiple independent fresh-context global reviewers/challengers against original requirements, architecture/interface contracts, implementation and test evidence. Run them in parallel in one global round. Passing tests alone is not completion.
- Each completed milestone gets one independent fresh-context review round (user acceleration decision, 2026-09-14). Fix findings and run targeted verification; do not automatically start repeated fresh-review rounds or separate reviews for every component. Record findings, disposition, evidence and remaining gaps in specs/reviews/. Do not mark milestones complete on intent alone.
- Use English code comments and Chinese design/reader documentation. Do not publish or upload the take-home PDF or private documents.
- All project-owned production, example and test code, comments, messages and logs use English and ASCII symbols. Example README prose uses Chinese for readability; code blocks and executable commands remain English and ASCII. Keep intentional Unicode test data as escaped bytes/code points; do not rewrite third-party headers.
- Do not edit .reference-repos/; those checkouts are read-only reference material.

<!-- develop-with-specs:start -->
## Durable specifications

For work represented under specs/, read specs/INDEX.md and the relevant spec before editing. Keep task status, decisions, current state, changed files, verification evidence and next steps current at meaningful transitions and before handoff. Do not rely on chat as the only project memory.
<!-- develop-with-specs:end -->


## Error handling

All project-owned production functions use noexcept. Normal failures return Status or StatusOr<T>; no throw/catch for business control flow. Fallible initialization uses status-returning factories. Convert third-party returned errors, use non-throwing parsing APIs, and do not add special exception wrappers for third-party libraries. Third-party/generated library signatures and GoogleTest-generated test bodies are not rewritten.

## Toolchain and quality gates

Use Clang/clang++ for all project builds. Development/test builds enable AddressSanitizer, UndefinedBehaviorSanitizer, and LeakSanitizer with failing diagnostics. Release uses -O3 and supported LTO, NDEBUG, and no sanitizer instrumentation; preserve defined numeric semantics (no fast-math). CMake/Make remain the only build systems.

- Upstream delivery adapters must implement a broker-neutral interface with MemMock and Kafka backends, manual commit-after-compute acknowledgement, pause/resume and replay positions.
- Compute caches must be bounded by node memory budgets and honor owner/epoch/confirmed-state invalidation; cached data cannot bypass UNKNOWN commit handling.

- Watermarks and consistent checkpoints are required despite no HA. KV snapshot flush is only the storage boundary; checkpoints must bind source positions, catalog and runtime state. Reliable message replay can replace an application redo log, not the consistency/replay protocol; retain the existing RocksDB WAL until such a change is explicitly implemented and verified.

## Naming

Separate function, struct and class definitions with blank lines; enforced by clang-format.

Project-owned functions and variables use snake_case, including factories open/create/from_snapshot. Types use PascalCase. Constructors/destructors, required third-party/framework overrides and generated APIs retain required names. Do not rename third-party interfaces.

All project-owned aggregate struct initializers name supplied fields with C++20 designated initializers (`.field = value`), including production and examples. Do not use positional aggregate values. Empty default initialization, constructor calls, arrays and schema-ordered Row/container payloads retain their applicable C++ syntax.

Use business terminology in code, comments, diagnostics and examples. Avoid milestone or specification IDs such as C8 in reader-facing names; explain stable request IDs next to their business meaning.

- End-to-end tests deploy real Coordinator, KV and multiple Compute processes. Fast default tests use MemMock. The full product retains a separate Kafka integration through Docker Compose, pinned to apache/kafka:4.3.1, KRaft broker/controller, no ZooKeeper; the user deferred that integration beyond the five-hour core delivery. Never commit Kafka server distributions or container images. Protocol mock tests do not replace a real Kafka integration claim.

- Finish the core system before implementing user-extension families. UDF, UDTF and UDAF remain full-product follow-up scope, explicitly deferred beyond the user's five-hour core delivery. The user also deferred Process Function on 2026-09-14; its implementation and dedicated research are outside the current delivery and must not remain a completion gate. A plugin loader, built-in aggregates or FSM callbacks alone do not prove those extensions. Preserve the single Catalog graph and atomic lifecycle when adding them later.

## Module guidance

Read the applicable nested AGENTS.md before editing a directory. These files add local responsibilities and verification entry points; all root requirements still apply. For changes spanning public headers, implementations and tests, consult each affected directory's guidance.

- [Production sources](src/AGENTS.md): core, storage, SQL, runtime, ingest, services and client; SQL generated files have additional regeneration guidance.
- [Public headers](include/eventflow/AGENTS.md) and [tests](tests/AGENTS.md).
- [Examples](examples/AGENTS.md), with a local guide in each demo directory.
- [Build helpers](cmake/AGENTS.md) and [packaged dependencies](cpp3rdlib/AGENTS.md).
- [Reader documentation](docs/AGENTS.md) and [specifications and review evidence](specs/AGENTS.md).

Current API scope and implementation status come from the latest authorized record in [Trigger API ergonomics](specs/trigger-api-ergonomics.md) and the live public headers. Statically compiled typed functions do not establish delivery of the deferred dynamic UDF/UDTF/UDAF families. Historical core test counts do not validate later changes.
