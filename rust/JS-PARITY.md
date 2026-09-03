# JavaScript-to-Rust parity contract

## Objective

The Rust implementation is a source-structured port of the JavaScript implementation, not an
independent implementation of the wire protocol.

For every in-scope JavaScript runtime file, the Rust tree must contain a corresponding Rust file.
Within each file, declarations, fields, methods, state transitions, control flow, and explanatory
comments should appear in the same order as the JavaScript source. A reviewer should be able to
read the files side by side and identify the Rust counterpart of each JavaScript operation.

“Equivalent outcome in current tests” is not sufficient when the implementation uses a different
state machine. Compatibility fallbacks and condensed replacement abstractions are not parity.

## Required interpretation of 1:1

“Line by line” means a structural and behavioral translation in source order. It does not require
identical physical line numbers or deliberately unidiomatic syntax. Differences are allowed only
where required by the languages or the deliberately excluded scope.

Allowed mechanical translations include:

| JavaScript                               | Rust                                                                 |
| ---------------------------------------- | -------------------------------------------------------------------- |
| object reference                         | `Arc<T>` or `Weak<T>`                                                |
| explicit local lifetime counter          | `Arc`, `Clone`, and `Drop` when observably equivalent                |
| abstract class                           | object-safe trait plus concrete structs                              |
| `T \| Promise<T>`                        | an explicit immediate-or-pending enum                                |
| eager `Promise<T>`                       | a future that is spawned or polled eagerly at construction           |
| thrown exception                         | `Result<T, Error>` or an error hook at the equivalent boundary       |
| `undefined` in the simplified Rust API   | `null`/`None`, with the lossy mapping documented                     |
| JavaScript `Proxy` method interception   | generated typed Rust stub methods                                    |
| prototype reflection                     | generated target dispatch and getter tables                          |
| dynamic `instanceof` at an internal seam | explicit enum matching or narrowly scoped downcasting                |
| microtask ordering                       | an explicit queue/state machine preserving the same observable order |

The following are not acceptable mechanical translations:

- replacing a JavaScript class hierarchy with a smaller unrelated provider model;
- skipping methods because Rust ownership makes the common case appear safe;
- replacing wire-level map instructions with pull-then-map;
- eagerly resolving a promise that JavaScript keeps lazy;
- relying on Tokio scheduling where JavaScript promise registration order is observable;
- using a generated list of ignored tests instead of translating test bodies;
- declaring a file a counterpart when it contains only a compatibility slice without a parity note.

Every unavoidable behavioral difference must have a `Difference from JS:` comment on the affected
Rust method. A module-level note is not enough when only one method differs.

## Scope

### Runtime files that must be ported

These files are in scope and require source-ordered counterparts:

| JavaScript                       | Rust                                        |
| -------------------------------- | ------------------------------------------- |
| `src/core.ts`                    | `rust/src/core.rs`                          |
| `src/serialize.ts`               | `rust/src/serialize.rs`                     |
| `src/rpc.ts`                     | `rust/src/rpc.rs`                           |
| `src/map.ts`                     | `rust/src/map.rs`                           |
| `src/websocket.ts`               | `rust/src/websocket.rs`                     |
| `src/messageport.ts`             | `rust/src/messageport.rs`                   |
| public exports in `src/index.ts` | `rust/src/lib.rs`                           |
| proxy/reflection boundaries      | `rust/macros/src/lib.rs` and generated code |

Transport splitting, pinning, synchronization, and generated dispatch may use Rust-only support
files, but those files must be identified as mechanical support rather than JavaScript
counterparts.

### Deliberately deferred runtime areas

The following are outside this rewrite and may remain absent rather than represented by fake
implementations:

- `src/streams.ts`;
- readable/writable stream hooks and pipe protocol messages;
- Blob serialization and Blob-over-stream behavior;
- Headers, Request, and Response integration;
- HTTP batch RPC in `src/batch.ts`;
- Bun-only APIs;
- workerd-native RPC and Durable Object integration.

A deferred branch in an otherwise in-scope method should remain visibly localized at the same
position as the JavaScript branch. It should return a clear unsupported error and link to the
excluded feature. The surrounding method must still be translated.

### JavaScript-specific built-ins

Tests whose purpose is JavaScript object identity, prototype identity, or a JavaScript-only built-in
are not copied into the Rust suite. Examples include exact prototypes for `ArrayBufferView`
subclasses, JavaScript `Map`, `Set`, `RegExp`, and platform-native classes with no selected Rust wire
type.

This does not permit removing protocol-neutral behavior merely because JavaScript expresses it with
a built-in. Rust equivalents are still required for:

- null, booleans, numbers, strings, arrays, and records;
- NaN and infinities;
- arbitrary-size integers;
- byte strings;
- errors and nested error causes in the supported Rust error representation;
- capabilities and promises nested in values;
- resource limits and malformed-wire rejection.

A Rust-native representation can be added later for an excluded built-in, but the rewrite does not
carry a permanently ignored JavaScript-prototype test for it.

### Validation package

`packages/capnweb-validate` is not part of the Rust runtime port. Its test files must not be copied
into `rust/tests` and do not count against runtime parity.

The Rust code generator has its own tests for generated Rust APIs. Those tests should derive their
contracts from `.d.ts` input, but they are not translations of the validation plugin's transformer
or runtime-validation tests.

TypeScript-only compile-time tests are likewise not copied verbatim. Any public surface they reveal
must instead have an appropriate Rust compile-pass, compile-fail, or generated-code test.

## Test-surface rule

Every in-scope surface used by a JavaScript runtime test must have a Rust equivalent. This includes
internal test seams, not only the published API. Examples include:

- controllable in-memory transports with fencing, logs, pending counts, and injected failures;
- session statistics and drain operations;
- explicit disposal and brokenness registration;
- raw hook construction where JavaScript tests construct hooks directly;
- promise resolvers used to test calls before resolution;
- target fixtures, counters, disposal spies, and error targets;
- inspection of emitted protocol messages when a JavaScript test asserts wire behavior.

The Rust equivalent need not imitate JavaScript test syntax. It must expose the same observable
control and make the same assertion possible without weakening the test.

Tests must not be enabled based only on a similar broad E2E test. Each translated test retains the
original test's scenario and assertions, adjusted only for mechanical Rust differences.

## Clean-rewrite plan

The current condensed runtime should be removed rather than incrementally repaired. Preserve design
documents, manifests as dependency references, `.d.ts` fixtures, interoperability fixtures, and
unrelated code-generator work. Replace the runtime and its copied test inventory in the following
order.

### Phase 0: establish the source ledger

1. Record the exact JavaScript revision being translated.
2. List every top-level declaration from each in-scope source file in source order.
3. For each declaration, record its Rust name and one of:
   - translated;
   - mechanically different, with the method-level note location;
   - deferred because it belongs to the explicit excluded scope.
4. Do not use “partial” as a completion state for a finished phase.

### Phase 1: translate `core.ts` line by line

Translate in source order, including:

1. property-path and RPC type classification;
2. provider/importer/exporter interfaces used by core;
3. every `StubHook` method and its ownership contract;
4. `ErrorStubHook`;
5. call interception and scoped restoration;
6. the generated replacement for Proxy behavior;
7. `RpcStub` and `RpcPromise`, including adoption and neutering rules;
8. every unwrap helper and its ownership semantics;
9. promise pulling and application delivery;
10. `RpcPayload`, source tracking, deep-copy state, disposal, and rollback;
11. value, payload, target, and promise hooks in their original order;
12. brokenness forwarding, cancellation, and e-order behavior.

Do not implement map with a fallback. Keep the interception seam exact and connect it only when
`map.ts` is translated.

Completion gate: every non-deferred declaration and method in `core.ts` appears in the ledger as
translated or mechanically different. Core unit tests and local-stub tests pass without broad
substitute assertions.

### Phase 2: translate `serialize.ts` line by line

Translate constants, limits, helper functions, interfaces, null contexts, `Devaluator`, standalone
serialization, `Evaluator`, and standalone deserialization in source order.

For each JavaScript built-in branch:

- translate it when a protocol-neutral Rust representation is selected;
- leave a localized unsupported branch when it is deferred;
- omit JavaScript-prototype-only test expectations rather than creating permanent ignored tests.

Preserve recursion accounting, transactional export rollback, property filtering, special-number
encoding, malformed-input checks, and error serialization behavior.

Completion gate: all in-scope serialization and limits tests pass, including hostile-input and
rollback tests.

### Phase 3: translate `rpc.ts` line by line

Translate transport interfaces, message types, table entries, import hooks, main hooks, session
state, and public session methods in source order. Preserve:

- positive and negative ID allocation;
- local and remote reference counts;
- promise-ID uniqueness;
- lazy pulls and eager exported-promise resolution;
- root promise-chain elision;
- release timing and auto-release;
- callback registration order and transfer on resolution;
- all abort, disconnect, and send-failure paths;
- e-order and message ordering;
- stats and drain behavior;
- serialization rollback on failed sends.

Stream-specific fields and message branches remain localized deferred sections.

Completion gate: all non-stream RPC, capability-passing, lifecycle, e-order, brokenness, and
transport-failure tests pass with their original assertions.

### Phase 4: translate `map.ts` line by line

Translate `MapBuilder`, `MapVariableHook`, `MapApplicator`, scoped call interception, capture
ownership, instruction evaluation, and RPC map messages in source order.

Completion gate: local map, remote map, nested map, wire-message, rejection, and disposal tests pass
without pulling the subject prematurely.

### Phase 5: translate WebSocket and MessagePort support

Translate non-Bun WebSocket construction and lifecycle behavior, including close-code handling,
close-reason truncation, abort propagation, and bidirectional sessions. Implement a Rust
message-channel equivalent exposing the same observable transport semantics as MessagePort tests.

HTTP, Blob, stream, Bun, and workerd branches remain excluded.

### Phase 6: rebuild the generated public boundary

Once runtime hooks are stable:

1. generate stub methods and getters from `.d.ts`;
2. generate target dispatch tables instead of reflecting prototypes;
3. support every non-excluded value and capability type used by runtime tests;
4. add Rust compile-pass and compile-fail tests for generated surfaces;
5. verify generated code calls the same hook methods at the same points as the JS Proxy.

### Phase 7: translate and run tests

1. Create one handwritten Rust test file for each in-scope JavaScript runtime test file.
2. Translate test fixtures and test cases in source order.
3. Do not copy `capnweb-validate`, Bun, or workerd test files.
4. Remove tests dedicated solely to excluded JavaScript built-ins.
5. Mark only clearly deferred stream, Blob, Headers/Request/Response, and HTTP cases ignored.
6. Every ignored test must name the exact excluded feature; generic “parity slice” reasons are not
   allowed.
7. Run the entire Rust test suite.
8. Treat every other ignored test as a failure of the rewrite plan.

## Final acceptance criteria

The rewrite is complete when:

- each in-scope JavaScript declaration has a source-ordered Rust counterpart;
- no non-deferred method is represented by a fallback or default no-op;
- every method-level Rust difference is documented inline;
- every in-scope JavaScript test scenario has a handwritten Rust equivalent;
- only explicitly excluded tests are absent or ignored;
- no `capnweb-validate`, Bun, workerd, or JavaScript-prototype-only inventories remain in Rust tests;
- all enabled Rust tests, clippy with warnings denied, formatting, and documentation lint pass;
- Rust↔Rust and JavaScript↔Rust interoperability tests pass;
- the source ledger contains no unexplained partial entries.
