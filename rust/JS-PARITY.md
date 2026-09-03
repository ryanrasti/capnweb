# JavaScript-to-Rust parity contract

## Rule

The Rust runtime is a mechanical, line-by-line port of the JavaScript runtime.

For every in-scope JavaScript/TypeScript source file:

1. Create the corresponding Rust file.
2. Copy every source line into the Rust file as an exact `// JS:` comment.
3. Put the Rust equivalent immediately below the source line or smallest group of source lines that
   forms a translatable syntactic unit.
4. Preserve declaration order, field order, method order, control flow, state transitions, and
   explanatory comments.
5. Do not condense, redesign, or replace the implementation merely because another implementation
   could produce the same result.

A reviewer must be able to read the Rust file from top to bottom beside the JavaScript file and see
where every source line was translated.

## Source baseline

The rewrite is pinned to repository commit
`2b2dc89049c08ed6bf01f5955b8cdfc606e45896`. The corresponding `src/` Git tree is
`185594ec5a9b8ba0d02cb0b6dfe6483038417fc1`.

The source-line checker must fail if the current JavaScript source differs from that baseline until
the change is intentionally reviewed and the Rust comments are resynchronized. Updating the
baseline is a deliberate parity change and must be committed together with the corresponding Rust
translation updates.

## Required source format

A single-line translation looks like this:

```rust
// JS: export class ErrorStubHook extends StubHook {
pub struct ErrorStubHook {
```

Multiline syntax is preserved line by line and translated as one syntactic unit:

```rust
// JS:   call(path: PropertyPath,
// JS:        args: RpcPayload): StubHook {
fn call(
    &self,
    path: PropertyPath,
    args: RpcPayload,
) -> StubHook {
```

JavaScript comments and blank lines are also preserved:

```rust
// JS:

// JS:   // Create a clone which can be disposed independently.
// RUST-DIFF(local refcount): ordinary `Clone` and `Drop` provide the Rust lifetime semantics.
```

The text following `// JS:` must match the original source line exactly. The source lines must occur
in exactly the same order as the original file.

One JavaScript line may require multiple Rust lines. A group of JavaScript lines may be followed by
one Rust construct when Rust syntax cannot be interleaved more finely. The translation must still be
immediately adjacent; source comments may not be collected into a separate header or ledger.

## The only translation exceptions

Everything is translated mechanically except the following three categories.

### 1. JavaScript built-ins

A branch that depends on a JavaScript-only or currently deferred Web API keeps its exact `// JS:`
source lines, but its Rust equivalent may be a localized unsupported branch.

This currently includes:

- ReadableStream and WritableStream integration;
- Blob;
- Headers, Request, and Response;
- HTTP/fetch integration;
- JavaScript prototype-identity checks;
- JavaScript collection or typed-view identity when no Rust wire representation has been selected;
- Bun and workerd platform objects.

The unsupported Rust branch must remain at the same location as the JavaScript branch. It must say
which built-in is deferred. The rest of the containing method is still translated line by line.

Protocol-neutral values are not excluded merely because JavaScript represents them with a built-in.
Rust equivalents remain required for null, booleans, numbers, strings, arrays, records, arbitrary
integers, byte strings, errors, capabilities, promises, limits, and malformed-wire handling.

### `undefined`, `null`, and `()`

At a typed Rust API boundary, TypeScript `void` and `undefined` map to Rust `()`. A nullable `T` maps
to `Option<T>`. Dynamic nullish values use `CapnwebValue::Null`.

The wire and dynamic representations intentionally make no distinction between JavaScript
`undefined` and `null`:

- JSON `null` decodes as `CapnwebValue::Null`;
- the wire expression `["undefined"]` also decodes as `CapnwebValue::Null`;
- `CapnwebValue::Null` always encodes as JSON `null`;
- converting `()` into a dynamic value produces `CapnwebValue::Null`;
- converting `CapnwebValue::Null` to `()` succeeds;
- `Option::None` also encodes as JSON `null`;
- there is no internal `Undefined` or `Unit` dynamic variant.

The original nullish representation cannot be recovered after decoding. This unconditional,
intentionally lossy alias is an explicit language mapping rather than a deferred JavaScript
built-in.

### 2. Local JavaScript reference counting

Reference counting used only to manage local JavaScript object lifetime is translated to Rust's
ownership system:

- `Arc` and `Weak`;
- `Clone`;
- `Drop`;
- owned moves and borrows.

JavaScript `dup()`, explicit hook `dispose()`, disposed sentinels, and local integer refcounts are
removed when they exist only to emulate ownership. Rust uses ordinary `Clone`, moves, and `Drop`:

- `Arc::clone()` creates another local handle to the same logical capability;
- dropping the last local handle performs cancellation or release;
- payload ownership recursively drops contained handles;
- promise adoption consumes the source value;
- use after disposal or adoption is prevented by the type system;
- explicit early disposal uses `drop(value)` or a consuming convenience method.

The JavaScript behavior where `RpcPromise.dup()` changes a promise into a non-promise stub is not
carried into the Rust API. Any required internal type conversion is explicit and consuming.

This intentionally trades a small local-lifetime divergence for substantially less state-machine
code and an idiomatic Rust API. Wire-visible reference accounting is not covered by this exception.
Import/export counts, introduction counts, `release` messages, remote reference counts, and
serialization of repeated references are translated exactly because they are part of Cap'n Web
behavior.

#### Experimental owned promise results

The initial implementation will test an ownership-oriented promise API before treating it as a
permanent compatibility rule:

- `RpcPromise<T>` resolves to an owned `T` and awaiting consumes that promise handle;
- callers clone the promise before awaiting when they need another handle;
- the runtime stores one `CapnwebValue` resolution and each awaiting handle decodes a recursive
  clone of that dynamic value;
- this does not require application return type `T` itself to implement `Clone`;
- generated pass-by-value values may still implement `Clone` for normal Rust ergonomics;
- cloned capabilities share their `Arc`;
- dynamic arrays and objects clone recursively;
- pass-by-value results do not preserve JavaScript object identity.

This experiment succeeds only if pipelining remains lazy, calls remain eager, e-order is preserved,
wire reference counts remain exact, and retained tests do not require shared mutable result identity.
If it forces pervasive `Arc<T>` into application APIs, weakens cancellation, or makes ordinary
non-`Clone` Rust return values impractical, revisit it before declaring `RpcPromise` complete.

### 3. JavaScript Proxy and reflection

JavaScript Proxy call/property interception is replaced by generated Rust methods. JavaScript
prototype reflection is replaced by generated target dispatch and getter tables.

The generated target boundary uses an object-safe runtime trait equivalent to:

```rust
trait RpcTargetInner {
    fn call(self: Arc<Self>, path: PropertyPath, args: RpcPayload) -> Result<StubHook>;
    fn get(self: Arc<Self>, path: PropertyPath) -> Result<StubHook>;
}
```

`#[rpc_target]` implements this boundary for application targets. Dynamic target values hold an
`Arc<dyn RpcTargetInner>`. Generated dispatch invokes synchronous application methods synchronously
and eagerly starts asynchronous methods before returning their promise hook.

The original Proxy/reflection lines remain as `// JS:` comments in the corresponding Rust source.
Their Rust equivalent points to or invokes the generated macro boundary at the same logical point.
The generated code must perform the same hook calls, path construction, argument ownership, and
return wrapping as the JavaScript Proxy.

## Mechanical language mappings

These are syntax translations, not additional exceptions. They must preserve the JavaScript state
machine and ordering.

| JavaScript                         | Rust                                                       |
| ---------------------------------- | ---------------------------------------------------------- |
| class                              | struct plus inherent implementations                       |
| abstract class                     | object-safe trait plus concrete structs                    |
| object reference                   | `Arc<T>` or an owned value                                 |
| weak reference                     | `Weak<T>`                                                  |
| `T \| Promise<T>`                  | explicit immediate-or-pending enum                         |
| eager `Promise<T>`                 | future eagerly started at the equivalent construction line |
| thrown exception                   | `Result<T, Error>` or an error hook at the same boundary   |
| `try` / `catch` / `finally`        | equivalent result handling and cleanup guards              |
| discriminated union                | enum                                                       |
| nullable value                     | `Option<T>`                                                |
| dynamic RPC value                  | `CapnwebValue`                                             |
| microtask ordering                 | explicit ordered state or queue                            |
| callback registered before resolve | callback registered before resolution in Rust as well      |
| synchronous fast path              | immediate enum branch, not an always-async future          |

Rust futures are lazy while JavaScript promises are eager. Therefore constructing the Rust
counterpart must start or poll a future at the exact point where JavaScript constructs the promise.
Execution may not be deferred until `pull()` unless the JavaScript implementation also defers it.

The runtime uses an eager task-backed promise representation. Construction polls the future once
synchronously, matching execution of a JavaScript async function through its first `await`. If the
future is pending, subsequent polls are scheduled immediately on Tokio. Resolution callbacks are
stored and invoked in registration order rather than relying on incidental Tokio task order. An
immediately-ready result remains available through the synchronous fast path. Creating pending work
without an active Tokio runtime produces a rejected RPC promise instead of silently leaving a lazy
future dormant.

## In-scope source files

The rewrite covers the runtime implementation, including:

- `src/core.ts` → `rust/src/core.rs`;
- `src/serialize.ts` → `rust/src/serialize.rs`;
- `src/rpc.ts` → `rust/src/rpc.rs`;
- `src/map.ts` → `rust/src/map.rs`;
- `src/websocket.ts` → `rust/src/websocket.rs`;
- `src/messageport.ts` → `rust/src/messageport.rs`;
- public runtime exports from `src/index.ts` → `rust/src/lib.rs`;
- Proxy and reflection replacements → Rust macros and generated code.

Files dominated by deferred built-ins, such as streams and HTTP batch support, may initially contain
the full `// JS:` source with localized deferred Rust markers. They are not silently represented by
a different implementation.

`packages/capnweb-validate` is a separate TypeScript validation/transformer project and is not copied
into the Rust runtime or Rust test tree. The Rust `.d.ts` generator has its own Rust-facing tests.

## Tests

Every in-scope behavior exposed by the JavaScript runtime tests must have a Rust equivalent,
including internal test-only surfaces such as:

- raw hook construction;
- controllable transports and injected failures;
- transport logs and wire-message inspection;
- fences and pending-message counts;
- deterministic release through `Drop` and explicit `drop(value)`;
- brokenness callbacks;
- session statistics and draining;
- promise resolvers;
- counters, disposal spies, and target fixtures.

Tests are handwritten Rust counterparts, kept in the same source order as the JavaScript tests. A
broad E2E test does not replace a more specific JavaScript test.

Do not copy:

- `capnweb-validate` tests;
- Bun or workerd tests;
- tests whose only assertion is JavaScript built-in or prototype identity.

A test may be ignored only when it directly depends on a deferred JavaScript built-in. Its ignore
reason must name that built-in. Generic reasons such as “parity slice”, “not implemented”, or
“platform behavior” are not accepted.

All other tests must be translated and enabled. A failing translated test indicates missing runtime
parity and must be fixed in the corresponding source translation.

## Rewrite procedure

### 1. Remove the condensed implementation

Delete the current Rust runtime and copied ignored-test inventory. Preserve manifests as dependency
references, design documents, `.d.ts` fixtures, interoperability fixtures, and unrelated codegen
work.

### 2. Copy source files before implementing

For each in-scope JavaScript file, create its Rust counterpart by copying every original line with a
`// JS:` prefix. Do not begin the next source file until the current file contains every source line
in order.

### 3. Translate from top to bottom

Starting at the first source line, place the Rust equivalent directly beneath each source unit. Do
not skip ahead to a test-relevant method. Do not introduce a condensed abstraction to avoid
translating later methods.

When a line falls under one of the three exceptions, mark it locally:

```rust
// JS: if (value instanceof Blob) {
// RUST-DEFERRED(JS built-in: Blob): preserve this branch position.
return Err(Error::UnsupportedBuiltin("Blob"));
```

When local JS refcounting maps to ownership, note it locally:

```rust
// JS: ++this.localRefcount;
// RUST-DIFF(local refcount): Arc ownership increments through Clone.
```

When Proxy behavior maps to generation, note the generated entry point:

```rust
// JS: return doCall(target.hook, path, RpcPayload.fromAppParams(argumentsList));
// RUST-DIFF(JS Proxy): #[rpc_interface] emits this call at each generated method.
```

No other `RUST-DIFF` category is allowed without updating this contract first.

### 4. Verify source coverage mechanically

Add a checker that:

1. reads each original source file;
2. extracts the text following every `// JS:` marker in its Rust counterpart;
3. verifies exact text equality and ordering;
4. reports missing, duplicated, reordered, or stale source lines;
5. never generates or modifies Rust source.

The checker proves source coverage, not behavioral correctness.

### 5. Translate tests

After an implementation file is complete, translate its relevant tests directly. Remove copied
validation and JavaScript-built-in-only inventories instead of leaving them ignored.

Run the complete Rust suite. Ignore only tests tied directly to the named deferred built-ins.

### 6. Completion gate

A source file is complete only when:

- the source-line checker reports every original line present and ordered;
- every non-built-in source unit has an adjacent Rust equivalent;
- only the three documented translation exceptions appear;
- all corresponding non-deferred tests are enabled and pass;
- method-level ownership, eager execution, cleanup, and ordering match JavaScript;
- formatting and clippy with warnings denied pass.

## Final acceptance criteria

The port is complete when:

- every in-scope JavaScript source line is preserved in its Rust counterpart;
- every preserved line has an adjacent mechanical Rust translation or one of the three explicit
  exception markers;
- no compatibility fallback or condensed replacement runtime remains;
- wire reference accounting matches exactly;
- generated Proxy/dispatch boundaries invoke the same hook operations;
- every non-deferred runtime test has a handwritten, enabled Rust equivalent;
- no copied `capnweb-validate` or JavaScript-built-in-only test inventory remains;
- the source checker, all Rust tests, interoperability tests, formatting, clippy, and documentation
  lint pass.
