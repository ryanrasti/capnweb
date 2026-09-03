# Rust counterpart parity audit

This audit records the result of comparing every Rust runtime counterpart with its JavaScript
source. “Partial” is not treated as parity: the missing methods and behavior remain required unless
they are explicitly deferred by the current streams/Blob/HTTP scope.

## Runtime files

- **`src/index.ts` → `rust/src/lib.rs` — Partial.** Export the complete non-deferred public
  surface and generated typed equivalents.
- **`src/core.ts` → `rust/src/core.rs` — Partial.** Complete dynamic function targets,
  getter/property semantics, exact payload copy/disposal bookkeeping, promise adoption,
  cancellation, brokenness, and all unwrap helpers.
- **`src/serialize.ts` → `rust/src/serialize.rs` — Partial.** Complete typed-array brands and byte
  order, rich errors, circular-reference diagnostics, structured-clone collections, rollback, and
  exact malformed-expression checks. Headers, Request/Response, and Blob are deferred.
- **`src/rpc.ts` → `rust/src/rpc.rs` — Partial.** Complete abort/send-error options, callback
  transfer, stats/drain, auto-release and rollback, duplicate-ID checks, exact reference counts,
  and all non-stream protocol validation. Stream messages are deferred.
- **`src/map.ts` → `rust/src/map.rs` — Not at parity.** Replace pull-then-map with `MapBuilder`,
  `MapVariableHook`, `MapApplicator`, scoped interception, captures, and map wire expressions.
- **`src/streams.ts` → `rust/src/streams.rs` — Deferred.** Entire module is deferred by scope.
- **`src/websocket.ts` → `rust/src/websocket.rs` and `rust/src/transport/websocket.rs` — Partial.**
  Add session constructors, close/error propagation, close-reason truncation, and lifecycle parity.
- **`src/messageport.ts` → `rust/src/messageport.rs` — Not at parity.** Add a Rust message-channel
  transport with equivalent clone/encoding semantics.
- **`src/batch.ts` → `rust/src/batch.rs` — Deferred.** Entire HTTP batch module is deferred.
- **`src/types.d.ts` → generated Rust API and `rust/rust-types.md` — Partial.** Complete `.d.ts`
  code generation, getters, callback capabilities, unions, and compile tests.

## Rust-only boundary files

- **`rust/macros/src/lib.rs`:** Replaces JS Proxy calls and prototype reflection. Method dispatch
  works; generated getters, richer types, overload diagnostics, and descriptor conformance remain.
- **`rust/src/transport/mod.rs`:** Mechanical split of the JS transport interface. String framing
  works; custom encoding levels and abort hooks remain.
- **`rust/src/transport/memory.rs`:** Rust test transport. Basic ordered delivery works; fencing,
  injected failures, logs, and pending-count controls remain.
- **`rust/src/transport/io.rs`:** Rust-specific newline framing with no direct JS counterpart.

## Core class/method audit

### `StubHook`

Implemented: `call`, `get`, `pull`, `dup`, `ignoreUnhandledRejections`, `dispose`, and `onBroken`.
The stream method is deferred. The map method is not at parity because the map instruction protocol
is not yet translated. Rust argument destruction replaces part of explicit `RpcPayload.dispose()`,
but all observable disposal timing still has to match JS.

### `ErrorStubHook`

Call/get/pull/dup and immediate brokenness notification are implemented. Owned-argument disposal is
provided by Rust drops. Exact identity reuse (`return this`) differs because Rust creates independent
`Arc` hooks where ownership requires it.

### `PayloadStubHook` and `TargetStubHook`

Basic calls, property traversal, cloning, and forwarding are implemented. Missing-property behavior,
prototype exposure restrictions, getters, callable values, deep-copy source tracking, and nested
brokenness/disposal are not yet fully equivalent.

### `PromiseStubHook`

Call/get ordering now receives synchronous sequence tickets before futures run, preserving e-order
across a pending resolution. Pull, dup, disposal forwarding, rejection forwarding, and brokenness
callbacks are present. Exact cancellation races and all argument/capture disposal tests still need
to be enabled and matched.

### `RpcStub` and `RpcPromise`

Explicit idempotent disposal, post-disposal errors, `dup`, `onRpcBroken`, calls, properties, pulls,
and typed Rust `Future` integration are present. Arbitrary thenable adoption has no direct Rust
analogue; adoption of Rust futures and ownership transfer still needs an explicit counterpart.
The current map API is not wire-equivalent.

### `ImportTableEntry`, `RpcImportHook`, and `RpcMainHook`

Resolution, pulling, release leases, main-stub shutdown, callback registration, and session abort
notification are implemented. Global callback registration preserves disconnect callback order.
Resolution callback transfer, cancellation races, exact local/remote refcounts, and duplicate
promise-ID validation still require parity tests.

### `RpcSession`

Push/pull/resolve/reject/release/abort, resource limits, promise-chain elision, and import/export
tables are implemented. Session options, `onSendError`, `getStats`, `drain`, custom encoding,
complete rollback, and exact clean shutdown remain.

## Test counterpart audit

Every one of the 45 JavaScript/TypeScript test files has a separately maintained `.test.rs` file.
Each unported runtime declaration has an explicit `#[ignore = "reason"]`; type-only files have an
explicit ignored compile-test placeholder. `rust/scripts/check-js-test-parity.mjs` only checks this
coverage and does not generate Rust source.
