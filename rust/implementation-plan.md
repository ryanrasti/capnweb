# Rust implementation plan

> **Implementation update:** The runtime is now being translated structurally from the JavaScript
> implementation rather than built from new provider abstractions. See
> [`JS-PARITY.md`](JS-PARITY.md) for the authoritative class/module mapping and current parity
> boundary. The generated public API remains idiomatic Rust; internal runtime names and control flow
> follow JavaScript.

## Objective

Implement an idiomatic Rust peer for the existing Cap'n Web wire protocol. The first milestone is a
small interoperable runtime, not a complete translation of TypeScript's type system or every browser
value.

The design relies on four ideas:

1. generated Rust stub methods replace JavaScript proxy interception;
2. generated methods emit calls immediately and return path-bearing promises;
3. `Arc`, moves, and `Drop` manage local ownership, leaving explicit counts only at export
   boundaries;
4. serialization and evaluation operate with access to the session's import and export tables.

## Initial scope

The first interoperable version supports:

- WebSocket and in-memory transports;
- calls, properties, results, errors, and promise pipelining;
- local and remote targets;
- functions and callbacks passed by reference;
- primitive values, arrays, dynamic objects, and capabilities nested within them;
- typed readable and writable streams;
- generated client methods and server dispatch from a constrained `.d.ts` API;
- recorded `.map()` after ordinary calls and streams work.

Initially excluded or deliberately simplified:

- `undefined` is preserved as `CapnwebValue::Undefined`, distinct from `null`;
- `T | null` maps to `Option<T>`;
- discriminated unions and ordinary object declarations map to dynamic objects;
- unions other than `T | null` map to `CapnwebValue`;
- `Blob`, `Request`, and `Response` are deferred;
- arbitrary TypeScript conditional, mapped, intersection, and generic types are rejected;
- schema negotiation is not added to the protocol.

## Repository layout

Begin with one crate and split it only when useful:

```text
rust/
  Cargo.toml
  src/
    lib.rs
    value.rs
    protocol.rs
    codec.rs
    session.rs
    target.rs
    stub.rs
    map.rs
    stream.rs
    transport/
      mod.rs
      memory.rs
      websocket.rs
  examples/
  tests/
```

Code generation and procedural macros can later become workspace crates:

```text
rust/crates/capnweb-codegen/
rust/crates/capnweb-macros/
```

Keeping the runtime in one crate initially will make it easier to change internal boundaries.

## Dynamic value model

Use one recursive value enum rather than generating exact Rust representations for every
TypeScript object and union:

```rust
pub enum CapnwebValue {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    BigInt(BigInt),
    Bytes(Bytes),

    Array(Vec<CapnwebValue>),
    Object(BTreeMap<String, CapnwebValue>),

    Target(UntypedStub),
    Readable(RpcReadStream<CapnwebValue>),
    Writable(RpcWriteSink<CapnwebValue>),

    Date(Date),
    Url(Url),
    Headers(Headers),
    Error(RemoteError),
}
```

This is conceptually `serde_json::Value` extended with Cap'n Web capabilities and special values.
Pure `serde_json::Value` is insufficient because it cannot contain targets, streams, promises, or
byte-container metadata.

Safe unknown-value dispatch uses ordinary exhaustive matching:

```rust
match value {
    CapnwebValue::String(value) => use_string(value),
    CapnwebValue::Object(fields) => use_object(fields),
    CapnwebValue::Target(target) => use_target(target),
    other => return Err(TypeError::expected("string, object, or target", other)),
}
```

`std::any::Any` is not used for remote values. A remote target has no Rust concrete type ID on the
wire. An `UntypedStub` can be dynamically called by member name or treated as a generated typed
stub based on the local schema; an incorrect assumption fails when the method is invoked.

Typed records and discriminated Rust enums can be added later as a derive/codegen layer converting
to and from `CapnwebValue::Object`. They are not required by the protocol runtime.

## Wire protocol model

Define explicit message and expression enums rather than manipulating unvalidated JSON arrays
throughout the runtime:

```rust
enum Message {
    Push(Expression),
    Pull(ImportId),
    Resolve(ExportId, Expression),
    Reject(ExportId, Expression),
    Release(ImportId, u64),
    Stream(Expression),
    Pipe,
    Abort(Expression),
}

enum Expression {
    Value(CapnwebValue),
    Import {
        id: ImportId,
        path: PropertyPath,
        args: Option<Vec<Expression>>,
    },
    Pipeline {
        id: ImportId,
        path: PropertyPath,
        args: Option<Vec<Expression>>,
    },
    Export(ExportId),
    Promise(ExportId),
    Writable(ExportId),
    Readable(ImportId),
    Remap {
        id: ImportId,
        path: PropertyPath,
        captures: Vec<Capture>,
        instructions: Vec<MapInstruction>,
    },
}
```

The exact representation may separate literal values from evaluated runtime values. Incoming JSON
must be validated while parsing, with limits on depth, collection sizes, paths, IDs, and map
instructions.

## Session core

Each session contains mutable protocol state and an outbound queue:

```rust
struct SessionState {
    imports: ImportTable,
    exports: ExportTable,
    next_import: ImportId,
    next_export: ExportId,
    outbound: OutboundSender,
    status: SessionStatus,
}
```

Start with `Arc<Mutex<SessionState>>`. No mutex guard may be held across `.await`. Generated method
calls need to allocate the next implicit push result ID and enqueue the corresponding message in one
synchronous critical section; otherwise concurrent calls could disagree with wire message order.

A dedicated transport task drains the outbound queue and receives frames. Incoming messages are
processed serially. Application calls may run concurrently after their arguments have been fully
decoded and their required promise substitutions have resolved.

An actor-based core can be considered later, but it should not force every generated stub method to
be async merely to allocate an import ID.

## Import ownership

Separate shared import state from each wire introduction:

```rust
struct ImportState {
    session: Weak<Session>,
    id: ImportId,
    resolution: ImportResolution,
}

struct ImportLease {
    state: Arc<ImportState>,
}
```

Each received `["export", id]` creates one `Arc<ImportLease>` referring to the shared state for that
ID. Local clones and derived property paths clone that lease. They do not represent additional wire
introductions.

When the final clone of a lease drops, its `Drop` implementation enqueues:

```text
release(id, 1)
```

If one ID is introduced three times, three independent leases eventually send three unit releases.
Combining them into one `release(id, 3)` is an optional optimization, not a correctness requirement.

Keep diagnostic counters for:

- import-table entries;
- introductions received;
- active leases;
- releases sent;
- unresolved, resolved, and canceled imports;
- approximate local strong counts when debugging.

These counters are observability tools and must not control ownership correctness.

## Export ownership

The export table holds boundary capabilities:

```rust
struct ExportEntry {
    value: Arc<dyn DynRpcTarget>,
    remote_references: u64,
}
```

The explicit count is required because remote references cannot be represented by a local `Arc`.
Increment it whenever the export ID is introduced or reintroduced. Decrement it by the count in a
received release message and remove the entry at zero.

Export identity belongs to the capability wrapper at the RPC boundary, not necessarily to the
underlying application object. Cloning one wrapper preserves one export identity. Independently
wrapping the same `Arc<T>` may create two independently releasable capabilities.

All ownership behind the boundary uses ordinary `Arc`, `Weak`, moves, and `Drop`.

## Payload ownership

Do not reproduce JavaScript's payload hook lists or manual local refcounts. Decoded Rust values own
their nested imports directly. If a callback argument is not retained, it drops with the method
arguments. To retain it, application code moves or clones it:

```rust
self.listener = Some(listener);
// or
self.listener = Some(listener.clone());
```

There is no public `dup()` operation. `Clone` is the Rust equivalent.

Return values similarly transfer ownership into the encoder. Encoding still traverses nested values
to construct expressions, but a separate ownership graph is unnecessary.

## Contextual encoding and evaluation

Encoding and evaluation require session context:

```rust
struct Encoder<'a> {
    session: &'a mut SessionState,
}

struct Evaluator<'a> {
    session: &'a mut SessionState,
}
```

Encoding may:

- serialize ordinary values;
- encode a same-session stub as an import;
- encode an unresolved result as a pipeline expression;
- insert a local target into the export table;
- create a pipe for a readable stream;
- export a foreign-session stub as a forwarding target, if forwarding is supported.

Evaluation may:

- create an import lease from an export expression;
- look up one of this side's exports for an import expression;
- create unresolved promise state;
- resolve property paths and deliver calls;
- consume a pipe's readable end;
- collect map captures and evaluate map instructions.

Error paths must roll back newly created exports or otherwise leave the session in a state that can
be safely aborted.

## Generated interfaces and methods

The `.d.ts` frontend produces a constrained API IR. Rust generation then emits:

- interface descriptor/marker types;
- typed resolved stubs;
- path-bearing pending providers;
- target adapters and dispatch metadata;
- callback descriptors;
- argument conversion implementations.

Application implementations are ordinary inherent Rust methods. They are not required to implement
a generated trait whose signatures expose runtime-specific result types.

A generated method is a thin wrapper over one untyped call primitive:

```rust
impl KitchenSinkApiStub {
    pub fn echo_string(
        &self,
        value: impl RpcArg<String>,
    ) -> RpcPromise<String> {
        self.inner.call("echoString", (value,))
    }
}
```

`RemoteRef::call()` synchronously:

1. acquires the session state;
2. lowers arguments with the contextual encoder;
3. allocates the next import ID;
4. enqueues a `push` containing an import or pipeline call;
5. returns a typed path-bearing promise.

Generated methods on unresolved target results call the same primitive. Generated getters append a
property path. Rust does not need a general monadic builder or closure recorder for ordinary
pipelining.

Method arguments use sealed conversion traits. They accept concrete values, compatible unresolved
results, remote stubs, and local targets as appropriate. Capability parameters get generated
interface-specific conversion traits so unrelated target types cannot be passed accidentally.

## Target dispatch and macros

Codegen emits an interface descriptor and typed stub API. Application code exposes ordinary
inherent methods by annotating an implementation with the generated descriptor:

```rust
struct CounterImpl {
    value: f64,
}

#[capnweb::target(Counter)]
impl CounterImpl {
    fn value(&self) -> f64 {
        self.value
    }

    async fn increment(&self, by: Option<f64>) -> f64 {
        // ...
    }

    async fn load(&self, id: String) -> Result<User, DatabaseError> {
        // ...
    }
}
```

The methods remain idiomatic local Rust:

- they may be synchronous or asynchronous;
- infallible methods return `T` directly;
- fallible methods return `Result<T, E>` where `E` can become a remote error;
- runtime and transport errors do not appear in application signatures.

The target macro validates the implementation against generated method metadata and generates an
object-safe dynamic adapter:

```rust
trait DynRpcTarget: Send + Sync + 'static {
    fn dispatch(
        &self,
        path: &[PropertyKey],
        args: Option<Vec<CapnwebValue>>,
    ) -> BoxFuture<'static, RpcResult<CapnwebValue>>;
}
```

Dispatch matches the exposed member, decodes arguments, invokes the implementation, awaits only
when necessary, converts an application `Result` error into a rejection, and encodes the successful
return value. Unknown members fail without Rust reflection. Codegen may emit a small return-adapter
trait per method so both its exact `T` and `Result<T, E>` are accepted without overlapping blanket
implementations.

Naming should distinguish the two sides:

- *target* means a local implementation exported by reference;
- *stub* means a typed reference used to call a target.

Also support local wrapping through the same stub API:

```rust
let counter = CounterStub::new_local(CounterImpl { value: 0.0 });
let result = counter.increment(Some(1.0)).await?;
```

This is useful for forwarding, tests, and ensuring that local and remote dispatch have the same
observable behavior.

## Promise pipelining

`RpcPromise<T>` is both:

- a `Future<Output = RpcResult<T>>`; and
- a provider containing an import ID and property path.

Generated calls emit immediately:

```rust
let authed = api.authenticate(token); // push; result import 1
let ids = authed.friend_ids();         // push using pipeline import 1
let users = api.get_users(ids);        // ids encoded as pipeline import 2
let result = users.await?;             // pull import 3
```

Awaiting initiates a pull if one has not already been sent. A promise used only for pipelining does
not need to be pulled. Dropping its final lease releases or cancels it according to current protocol
state.

Embedded promises are resolved and substituted before arguments are delivered to application code.
The annotated inherent method therefore receives concrete values rather than promise wrappers.

## Recorded `.map()`

`.map()` is the only operation that needs scoped call interception. Its closure runs synchronously
once with a symbolic input. Calls made during that run are recorded as instructions rather than
sent.

JavaScript uses a global current builder. Rust can use a scoped thread-local recorder stack:

```rust
thread_local! {
    static ACTIVE_RECORDERS: RefCell<Vec<Rc<RefCell<MapRecorder>>>> =
        const { RefCell::new(Vec::new()) };
}
```

Every generated method already reaches `RemoteRef::call()`. That primitive checks for a current
recorder:

```rust
fn call(&self, path: PropertyPath, args: EncodedArgs) -> UntypedRpcPromise {
    if let Some(recorder) = active_recorder() {
        recorder.record_call(self, path, args)
    } else {
        self.session.emit_call(self, path, args)
    }
}
```

The public operation has a synchronous closure signature:

```rust
pub fn map<U>(
    &self,
    mapper: impl for<'m> FnOnce(MapValue<'m, T>) -> MapExpr<'m, U>,
) -> RpcPromise<Vec<U>>;
```

The recording scope never awaits, so async executor task migration does not invalidate the
thread-local approach. An RAII guard must restore the previous recorder on normal return or panic,
and a stack supports nested maps. An async closure does not satisfy the required return type and is
rejected at compile time.

During recording:

- mapper input is variable zero;
- calls produce positive instruction variables;
- external stubs become negative capture indexes;
- values and arguments are encoded through a recording encoder;
- local targets, callbacks, and readable streams created inside the mapper are rejected initially;
- the final expression is appended as the final instruction;
- the runtime emits one `remap` operation.

`RpcPromise<T>` may internally distinguish live and recorded providers. Attempting to await a
recorded provider is an error. Generated methods work for both because they route through the same
central call primitive.

The receiving side evaluates the instruction sequence once per array element, once for a non-null
scalar, and zero times for null. Captures are held for the duration of replay and then released.

## Transport layer

Use a framed transport abstraction. One possible shape is:

```rust
trait Transport {
    async fn send(&mut self, message: String) -> Result<(), TransportError>;
    async fn receive(&mut self) -> Result<Option<String>, TransportError>;
}
```

Splitting it into read/write halves or using `Sink<String>` plus `Stream<Item = Result<String, _>>`
may fit the session task better. The protocol layer should not depend directly on a WebSocket
library.

### In-memory transport

Implement this first as a pair of bounded async channels:

```rust
fn memory_transport_pair() -> (MemoryTransport, MemoryTransport);
```

Each endpoint's outgoing sender is connected to the other's incoming receiver. Initially, messages
should still pass through JSON serialization so tests exercise the real wire codec. A structured
message mode may later be useful for focused unit tests.

The in-memory transport supports deterministic tests for calls, bidirectional capabilities,
release, disconnects, pipelining, streams, and `.map()` without network setup.

### WebSocket transport

Adapt WebSocket text frames directly to protocol messages. Tokio plus `tokio-tungstenite` is the
simplest initial choice. Keep the core transport interface sufficiently narrow that alternate async
runtimes can be added without changing session semantics.

The adapter must enforce frame-size limits before JSON parsing and propagate socket close or errors
to every pending import and stream.

## Streams

Implement streams after ordinary target calls and pipelining, but include them in the first useful
release.

A writable stream is an exported target with three operations:

```text
write(chunk)
close()
abort(reason)
```

Writes use the optimized `stream` top-level message, which is automatically pulled and released.
Rust exposes arbitrary typed chunks through `Sink<T>`:

```rust
pub struct RpcWriteSink<T>;
```

A readable stream is sent by:

1. emitting `pipe` and allocating its implicit import ID;
2. creating a remote writable sink for that import;
3. immediately spawning a task that pumps the local source into the sink;
4. encoding `readable(importId)` in the enclosing value.

The receiver backs a pipe with a bounded channel. Its sender is exported as the writable target and
its receiver is stored until consumed exactly once by a readable expression:

```rust
pub struct RpcReadStream<T>; // implements Stream<Item = RpcResult<T>>
```

For `T = Bytes`, adapters may additionally implement `AsyncRead` and `AsyncWrite`.

The first implementation uses a fixed encoded-byte window. Resolved stream writes act as
acknowledgements and free window capacity. Later, port the JavaScript bandwidth-delay-product
estimator without changing protocol semantics.

Required stream behavior:

- writes are delivered in order;
- close waits behind prior writes and produces clean EOF;
- release without close aborts the writable side;
- explicit abort terminates pending and future writes;
- dropping the readable side cancels upstream pumping;
- session failure terminates all streams;
- independent streams remain multiplexed with ordinary RPC messages.

## Testing strategy

### Unit tests

- message and expression JSON encoding;
- malformed tag and shape rejection;
- primitive/container `CapnwebValue` conversion;
- ID allocation and message ordering;
- import lease drop behavior;
- export increment/release behavior;
- path accumulation;
- map instruction recording and replay;
- stream ordering, close, abort, and flow-window accounting.

### In-memory integration tests

- Rust client to Rust server;
- bidirectional calls;
- callbacks moved or cloned beyond the receiving call;
- nested capabilities;
- pipelined calls before resolution;
- unresolved results forwarded as arguments;
- multiple introductions of one export ID;
- disconnect with live imports and promises;
- concurrent calls preserving implicit ID order;
- multiple concurrent streams.

### JavaScript interoperability tests

- Rust client to JavaScript server;
- JavaScript client to Rust server;
- exact golden JSON for representative operations;
- callbacks and targets in both directions;
- promise pipelining and forwarding;
- import release and export reintroduction races;
- readable and writable streams;
- recorded and nested `.map()`.

## Implementation order

### Milestone 1: values and protocol

1. Add `CapnwebValue` for initial primitive and container types.
2. Define validated message and expression representations.
3. Implement JSON encoding and decoding without capabilities.
4. Add protocol resource limits and malformed-message tests.

### Milestone 2: transport and session skeleton

1. Define the framed transport abstraction.
2. Implement paired in-memory transports.
3. Add the outbound queue and serial incoming-message loop.
4. Implement abort and disconnect propagation.

### Milestone 3: calls and references

1. Implement import and export tables.
2. Add per-introduction import leases and export boundary counts.
3. Implement push, pull, resolve, reject, and release.
4. Add contextual capability encoding and evaluation.
5. Hand-write one target dispatcher and one typed stub.
6. Verify Rust-to-Rust calls over the in-memory transport.

### Milestone 4: pipelining

1. Make promises path-bearing providers.
2. Emit pipeline calls from unresolved targets.
3. Accept unresolved expressions as typed method arguments.
4. Resolve embedded promises before application dispatch.
5. Test multi-stage pipelines without intermediate awaits.

### Milestone 5: generated API surface

1. Define the constrained TypeScript API IR.
2. Hand-validate generated Rust shapes against the kitchen-sink fixture.
3. Implement `.d.ts` extraction using the TypeScript compiler API.
4. Generate interface descriptors, stubs, pending providers, and dispatch metadata.
5. Implement `#[capnweb::target(Interface)]` for ordinary inherent implementations.
6. Add golden generated-source tests.

### Milestone 6: WebSocket interoperability

1. Implement a Tokio WebSocket transport.
2. Connect a Rust client to a JavaScript server.
3. Connect a JavaScript client to a Rust server.
4. Add bidirectional target and callback tests.
5. Add release, malformed-peer, and disconnect tests.

### Milestone 7: streams

1. Implement writable targets and optimized stream calls.
2. Implement pipe allocation and one-shot readable consumption.
3. Add background pumping and cancellation.
4. Add fixed-window backpressure.
5. Test against JavaScript streams in both directions.
6. Add byte-oriented async I/O adapters.

### Milestone 8: recorded `.map()`

1. Add the scoped recorder stack and panic-safe guard.
2. Make the central call primitive recording-aware.
3. Record mapper variables, captures, calls, and nested maps.
4. Encode and send remap expressions.
5. Implement receiving-side instruction replay.
6. Add JavaScript interoperability and abuse-limit tests.

### Milestone 9: expansion and hardening

1. Add remaining selected value types.
2. Port dynamic BDP stream flow control if needed.
3. Add HTTP batch transport.
4. Add fuzzing for protocol decoding and state transitions.
5. Benchmark calls, pipelining, large payloads, and concurrent streams.

## First implementation checkpoint

Before building TypeScript codegen, prove the runtime with one manually written interface
descriptor, one stub, and an idiomatic local implementation:

```rust
struct CounterImpl {
    value: f64,
}

impl CounterImpl {
    fn value(&self) -> f64 {
        self.value
    }

    async fn increment(&self, by: Option<f64>) -> f64 {
        // ...
    }
}
```

Hand-write its dynamic target adapter and stub methods, then demonstrate:

```rust
let counter = api.make_counter(10.0);
let value = counter.increment(Some(2.0));
let echoed = api.echo_number(value);
assert_eq!(echoed.await?, 12.0);
```

Run this first over the in-memory transport, then with one JavaScript peer over WebSocket. This
checkpoint validates the central design before investing in macros or broad type generation:

- generated-style methods can synchronously emit correctly ordered operations;
- path-bearing promises provide the intended pipelining API;
- contextual serialization correctly converts targets, imports, and promises;
- Arc-backed import leases release references correctly;
- only export boundaries require explicit remote-reference counts.
