# Proposal: Idiomatic Rust implementation of Cap'n Web

## Summary

Build a Rust implementation of the Cap'n Web protocol that interoperates with the existing
JavaScript implementation while presenting an idiomatic, statically typed Rust API.

The implementation would mirror the JavaScript runtime internally—expressions, import/export
tables, promise pipelining, and reference accounting—but use generated Rust traits and typed stub
wrappers at the application boundary. TypeScript declarations would remain the source of truth for
the RPC contract.

This is not a new wire protocol and does not add schemas to Cap'n Web itself. The TypeScript input
is a build-time description used to generate Rust bindings; compatibility on the wire remains based
on method/property names and Cap'n Web values.

## Goals

- Interoperate directly with JavaScript Cap'n Web peers.
- Support Rust as either side of a bidirectional RPC session.
- Generate idiomatic Rust types, interface descriptors, target adapters, and client stubs from
  TypeScript declarations.
- Preserve Cap'n Web's important semantics:
  - passing targets and callbacks by reference;
  - bidirectional calls;
  - promise pipelining;
  - capabilities nested inside arguments and results;
  - deterministic release of remote references;
  - eventually, recorded `.map()` operations.
- Use Rust ownership and `Arc` to simplify local lifetime management.
- Keep the untyped protocol machinery behind generated typed APIs.

## Non-goals

At least initially, the port would not attempt to:

- implement the whole TypeScript type system;
- expose arbitrary Rust objects through runtime reflection;
- reproduce JavaScript prototype traversal for generated targets;
- add schema negotiation or numeric method IDs to the protocol;
- transmit executable Rust or JavaScript closures;
- support every web-platform value and transport in the first release.

## Contract source and code generation

A `.d.ts` file should be the canonical cross-language contract. A project may generate it from
`.ts`, but the Rust generator should consume declarations rather than implementation details.

The same declaration can generate bindings for both roles because Cap'n Web is bidirectional:

- a **client wrapper** for calling an interface remotely;
- an **interface descriptor and target adapter** for exposing an ordinary Rust implementation;
- dynamic value mappings for records and non-nullable unions in v0;
- typed callback wrappers for callable interfaces.

For example:

```ts
interface User {
  id: string;
  displayName: string;
}

interface Listener extends RpcTarget {
  update(user: User): void;
}

interface Api extends RpcTarget {
  getUser(id: string): Promise<User | null>;
  subscribe(listener: Listener): Promise<void>;
}
```

could generate an API resembling:

```rust
#[capnweb::interface]
pub enum Api {}

#[derive(Clone)]
pub struct ApiStub {
    inner: RpcStub<Api>,
}

impl ApiStub {
    pub fn get_user(
        &self,
        id: impl RpcEncodeArg<String>,
    ) -> RpcPromise<Option<CapnwebValue>>;

    pub fn subscribe(&self, listener: impl IntoListenerArg) -> RpcPromise<()>;
}
```

An application exposes idiomatic inherent methods with `#[capnweb::target(Api)]`; it is not forced
to return runtime-specific result types. Generated APIs should avoid requiring application code to
manipulate untyped protocol values except where the v0 mapping intentionally uses `CapnwebValue`
for objects and general unions.

### TypeScript frontend

The recommended pipeline is:

```text
TypeScript compiler API -> language-neutral API IR -> Rust source generator
```

Using the TypeScript compiler API avoids reimplementing alias resolution, inheritance, declaration
merging, and generic substitution. The existing `capnweb-validate` type introspection code may
provide reusable concepts or implementation pieces.

The intermediate representation should identify:

- interfaces exposed as pass-by-reference targets;
- callable interfaces and callback signatures;
- methods, getters, parameters, and return types;
- by-value records, tuples, arrays, and discriminated unions;
- optional and nullable values;
- promises, stubs, streams, and other special Cap'n Web types.

The first version should support a documented TypeScript subset and reject unsupported constructs
with source-located diagnostics. Conditional types, unconstrained generics, arbitrary
intersections, ambiguous overloads, and similar constructs should not silently degrade to dynamic
values.

The hand-written fixture in [`examples/kitchen-sink/`](examples/kitchen-sink/) pairs a broad `.d.ts`
contract with the intended generated Rust surface. It should become a golden codegen test once the
generator exists.

## Runtime architecture

The implementation can be split into a small set of crates:

- `capnweb-protocol`: wire messages, expressions, encoding, decoding, and limits;
- `capnweb-runtime`: sessions, imports, exports, stubs, promises, targets, and dispatch;
- `capnweb-transport`: WebSocket and HTTP batch adapters;
- `capnweb-codegen`: TypeScript IR and Rust generation;
- `capnweb-macros`: generated interface expansion and application target adapters.

A single crate can be used initially and separated once the boundaries stabilize.

### Protocol values

`serde_json::Value` is suitable for the final JSON representation but not for the entire runtime.
The evaluator needs to represent capabilities, promises, streams, and other non-JSON values:

```rust
enum RpcValue {
    Undefined,
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<RpcValue>),
    Object(BTreeMap<String, RpcValue>),
    Bytes(Bytes),
    Target(Arc<dyn RpcTarget>),
    Stub(UntypedStub),
    Promise(UntypedRpcPromise),
    Error(RpcErrorValue),
}
```

Additional variants can be introduced as support for dates, big integers, streams, requests,
responses, and other protocol values is implemented.

Generated `RpcEncode` and `RpcDecode` implementations bridge typed Rust values and this internal
representation. Argument encoding must accept unresolved `RpcPromise<T>` values as expressions so
that promises can be forwarded without first awaiting them.

### Session and tables

Each session owns:

- an import table for remote values referenced locally;
- an export table for local values referenced remotely;
- monotonically increasing positive and negative ID allocators;
- an outbound message queue;
- session cancellation/broken-state notification;
- configured resource limits.

The table and expression behavior should follow the documented protocol directly. There is no need
to persist table state or reuse IDs.

## Target dispatch

Generated targets do not need JavaScript-style reflection. A target adapter can match the first
property-path component and decode the expected argument tuple:

```rust
match (member, args) {
    ("getUser", Some(args)) => {
        let (id,): (String,) = RpcDecode::decode(args)?;
        let result = self.inner.get_user(id).await?;
        RpcEncode::encode(result)
    }
    ("subscribe", Some(args)) => {
        let (listener,): (ListenerStub,) = RpcDecode::decode(args)?;
        self.inner.subscribe(listener).await?;
        RpcEncode::encode(())
    }
    _ => Err(RpcError::NoSuchMember(member.to_owned())),
}
```

A minimal object-safe runtime interface might be:

```rust
trait RpcTarget: Send + Sync + 'static {
    fn dispatch(
        &self,
        path: &[PropertyKey],
        args: Option<Vec<RpcValue>>,
    ) -> BoxFuture<'static, Result<RpcValue, RpcError>>;
}
```

### Interface and target macros

The generated Rust source should separate the remote interface from its local implementation. The
TypeScript generator emits an interface descriptor, typed stub methods, argument/result codecs, and
dispatch metadata.

Application code exposes ordinary inherent methods through an implementation attribute:

```rust
struct MyApi;

#[capnweb::target(Api)]
impl MyApi {
    async fn get_user(&self, id: String) -> Option<User> {
        // ...
    }

    async fn update_user(&self, user: User) -> Result<(), DatabaseError> {
        // ...
    }
}
```

Local methods may be synchronous or asynchronous. Infallible methods return `T`; genuinely fallible
methods return `Result<T, E>`. The macro wraps both forms in the internal RPC result, awaits when
necessary, and converts application errors into remote rejections. Transport-specific `RpcResult`
types do not appear in application method signatures.

The generated interface machinery supplies:

- `ApiStub`, the resolved remote reference;
- the pending/path-bearing provider returned by methods whose result is an `Api`;
- typed client methods and property accessors;
- a local target wrapper implementing `dyn RpcTarget`;
- argument/result codecs and dispatch metadata.

`#[capnweb::target(Api)]` validates and connects the inherent implementation to that generated
metadata, producing the member-name dispatcher and target conversion.

The term *target* should be used for the local implementation and *stub* for a typed reference to a
target. This avoids overloading `#[stub]` to mean both sides of the boundary.

A local implementation should also be wrappable behind the generated stub API, corresponding to
JavaScript's `new RpcStub(target)`:

```rust
let api = ApiStub::new_local(MyApi);
let user = api.get_user("alice".into()).await?;
```

This is useful for tests and forwarding. It ensures that local and remote calls use the same typed
provider methods and target dispatch semantics.

Property paths after the first target member can be evaluated generically against the returned
`RpcValue`. Getters are emitted as explicit generated dispatch arms and typed client accessor
methods.

## Typed stubs and promise pipelining

A typed stub is a generated newtype around an untyped remote reference:

```rust
struct RpcStub<T> {
    lease: Arc<ImportLease>,
    path: PropertyPath,
    marker: PhantomData<fn() -> T>,
}
```

Generated methods emit operations immediately: they append or select a member path, encode
arguments, send a `push` containing an `import` or `pipeline` expression, allocate the next result
import ID, and return another typed promise. No general closure recorder or explicit monadic
`do`-notation is needed for ordinary pipelining; code generation replaces JavaScript's dynamic
`Proxy` interception.

`RpcPromise<T>` has two roles:

1. it implements `Future<Output = Result<T, RpcError>>` so the result can be pulled and awaited;
2. it retains the import and property path so generated methods and getters can pipeline operations
   before the value resolves.

Conceptually:

```rust
let authed = api.authenticate(token);       // no await
let ids = authed.get_friend_ids();           // pipelined
let profiles = ids.map(|id| api.get_user(id));
let profiles = profiles.await?;              // one round trip
```

Rust cannot intercept arbitrary property access, so generated getter methods should be used where a
TypeScript API exposes properties:

```rust
let name: RpcPromise<String> = user.display_name();
```

Method arguments should accept either concrete values or compatible remote promises. This may use a
sealed conversion trait such as `RpcEncodeArg<T>` to preserve static checking without exposing the
untyped expression representation.

## Passing targets and callbacks by reference

### Local targets

A local implementation is wrapped in `Arc` and adapted to `Arc<dyn RpcTarget>`. When serialized, it
is inserted into the export table and represented by an `export` expression. Deserialization on the
other side produces a typed stub.

Generated helper functions can make the conversion explicit:

```rust
let api = ApiTarget::new(MyApi::new());
serve_websocket(socket, api).await?;
```

### Closures

An ordinary Rust closure can be exposed as a callable target:

```rust
let listener = RpcCallback::new(move |event: Event| async move {
    process(event).await?;
    Ok(())
});

api.subscribe(listener).await?;
```

The closure itself is not serialized. It remains in the local export table, and the peer receives a
callable stub. Closures will generally need to be `move`, `Send`, `Sync`, and `'static` because they
may outlive the call that exported them.

A callback received from the peer is represented by a generated typed callback stub. Invoking it
sends an ordinary RPC call and returns an `RpcPromise<T>`.

### Callback retention

JavaScript requires an explicit `dup()` when retaining an argument capability because it cannot
express transfer or deterministic ownership. Rust can use normal ownership instead. A decoded
callback argument owns an `Arc`-backed import lease; moving it into application state retains it,
and cloning it creates another local owner of the same lease:

```rust
self.listener = Some(listener);
```

If it is neither moved nor cloned, it drops naturally when the call returns. This gives the same
wire lifetime behavior without a special callback-retention rule. The Rust API does not need a
public `dup()` operation: ordinary `Clone` is the local duplication operation.

## Reference management

Rust ownership removes much of the local manual bookkeeping, but does not completely replace
protocol-level reference accounting.

### Imports

Import state and wire introductions should be represented separately:

```rust
struct ImportState {
    session: Weak<Session>,
    id: ImportId,
    // Promise resolution and broken-state information.
}

struct ImportLease {
    state: Arc<ImportState>,
}
```

Each occurrence of `["export", id]` on the wire creates one `Arc<ImportLease>` referencing the
shared `ImportState`. Cloning a local stub or deriving a property path clones that same lease; it
does not create a new wire introduction. When the final local clone of one lease drops,
`ImportLease::drop()` enqueues `release(id, 1)` on the session's outbound task.

This means imports do not need a manual introduction count for correctness. If an ID is introduced
three times, three independent leases eventually send three unit releases. Aggregating these into
one `release(id, 3)` is an optional optimization.

The runtime should nevertheless maintain diagnostic counters such as introductions received,
active leases, releases sent, unresolved imports, and total import-table entries. These are useful
for leak detection and debugging but do not control ownership. `Arc::strong_count()` may be shown as
approximate debugging information but should not be used as a protocol count.

An explicit `dispose()` may also be provided for deterministic release and error reporting. `Drop`
itself only enqueues work and never performs asynchronous I/O.

### Exports

The export table holds strong references to local targets:

```rust
struct ExportEntry {
    value: Arc<dyn RpcTarget>,
    remote_introductions: u64,
}
```

`Arc` tracks references inside the Rust process. It cannot determine how many references have been
introduced to the remote peer. The runtime must therefore retain the small wire-level counter
required by the protocol:

- increment it whenever the export ID is introduced or reintroduced;
- subtract the count in a received `release` message;
- remove the export table entry when it reaches zero.

This counter also handles release/re-export races. Export deduplication should use the identity of
the capability wrapper at the RPC boundary, not necessarily the identity of the underlying
application object. Cloning one wrapper preserves one export identity; independently wrapping the
same `Arc<T>` may intentionally create two separately releasable capabilities.

Most of the more detailed local hook lifetime machinery in JavaScript should collapse naturally
into `Arc`, `Weak`, RAII, and an outbound queue.

### Payload ownership

No JS-style payload hook list or manual disposal walk should be required. Decoded Rust arguments own
all nested stub leases directly. Dropping an argument record drops its nested capabilities; moving
or cloning a capability out retains it. Return values similarly transfer their owned targets,
stubs, promises, and streams into the encoded result.

Encoding may still need a traversal to lower nested values into protocol expressions, but ownership
is expressed by moves, `Arc`, and RAII rather than a parallel refcounting system. This behavior
should be covered by tests involving capabilities nested in records, arrays, and error paths.

## Recorded `map()`

Cap'n Web's special `.map()` operation does not send executable closure code. The callback is run
once locally against recording placeholders, producing a sequence of protocol instructions and a
list of captured stubs.

Rust can expose the same idiom:

```rust
let names = ids.map(|id| api.get_user_name(id));
```

While the closure executes:

- `id` is a recording `RpcPromise<T>`;
- generated calls append instructions rather than sending messages;
- referenced external stubs are recorded as captures;
- the returned expression becomes the final instruction;
- the runtime sends a `remap` expression.

The placeholder type should only expose RPC traversal, generated calls, forwarding as arguments,
and nested `map()`. It should not implement arithmetic, ordering, iteration, or other operations
that imply access to the unresolved value.

Recorded `map()` can be implemented after ordinary calls and pipelining. It is the one operation
that requires a recording closure; ordinary generated methods emit their operations on demand.

## Streams

Streams should be part of the first useful implementation rather than treated as a separate final
subsystem. Cap'n Web already builds them from ordinary capability operations plus two small protocol
special cases.

A remote writable stream is effectively a target exposing `write(chunk)`, `close()`, and
`abort(reason)`. Rust should represent arbitrary typed chunks with `Sink<T>` and use the optimized
`stream` message for writes. A byte-stream adapter can additionally implement `AsyncWrite`.

A readable stream is transferred by sending `["pipe"]`, immediately pumping the local source into
the resulting remote writable, and placing `["readable", importId]` in the enclosing value. On the
receiver, a pipe can be backed by a bounded asynchronous channel: its sender is exported as the
writable target and its receiver is consumed once to produce an `RpcReadStream<T>`. The fundamental
Rust types should resemble:

```rust
struct RpcReadStream<T> { /* implements Stream<Item = Result<T, RpcError>> */ }
struct RpcWriteSink<T> { /* implements Sink<T, Error = RpcError> */ }
```

For byte chunks, adapters may implement `AsyncRead` and `AsyncWrite`.

The stream implementation must preserve ordered writes, clean EOF after `close()`, abnormal
termination after release without close, explicit abort, cancellation when the readable side is
dropped, and session-shutdown propagation. A correct first version can use a fixed encoded-byte
window for backpressure. The JavaScript implementation's bandwidth-delay-product estimator can be
ported later without changing the wire protocol.

Stream references and pump-task lifetimes should use the same `Arc` leases and export-boundary
accounting as other capabilities. Only open/closed/aborted state and pending acknowledgements are
stream-specific state.

## Transports

The runtime should separate message framing from protocol evaluation.

Suggested implementation order:

1. WebSocket, because it provides natural bidirectional message framing.
2. HTTP batch using newline-delimited messages.
3. Additional Rust channel or message-port-like transports as demand appears.

The transport interface should send complete JSON messages or structured protocol messages and
report incoming messages, closure, and errors to the session task.

## Initial type support

A useful first version should support:

- `null`, `undefined`, booleans, strings, and finite/non-finite numbers;
- Rust numeric types with checked conversion to and from JavaScript numbers;
- arrays and tuples;
- plain records;
- optional and nullable fields;
- discriminated unions;
- byte arrays;
- errors;
- promises;
- generated targets and callbacks;
- capabilities nested within records and arrays;
- readable and writable typed streams with fixed-window backpressure.

Support can subsequently be added for:

- `bigint`;
- dates and URLs;
- headers, requests, and responses;
- dynamic BDP-based stream flow control;
- blobs;
- maps and sets when supported by the wire implementation.

The generator must define exact mappings for missing fields, `undefined`, and `null`. These should
not all collapse to `Option<T>` when the distinction is observable in the TypeScript contract.

## Security and limits

An interoperable implementation must treat its peer as hostile. The runtime should expose limits
for at least:

- encoded message size;
- expression and object nesting depth;
- array and object size;
- property-path length;
- bigint digit count;
- import/export count;
- recorded `map()` instruction and capture count;
- total work performed while evaluating one message.

Generated dispatch must reject unknown members, malformed argument lists, invalid type conversions,
and attempts to access non-exposed members. Protocol errors that invalidate session state should
abort the session rather than leave partially updated tables.

## Testing strategy

Interoperability should be tested against the JavaScript implementation rather than only against a
second Rust endpoint.

The test matrix should include:

- Rust client to JavaScript server;
- JavaScript client to Rust server;
- Rust and JavaScript targets passed in both directions;
- callbacks invoked during and after the original call;
- nested capabilities in arguments and results;
- calls made on unresolved results;
- unresolved promises forwarded as arguments;
- duplicate and release behavior;
- disconnects with live imports, exports, and calls;
- malformed messages and configured limits;
- eventually, recorded and nested `map()` operations;
- stream ordering, half-close/EOF, backpressure, abort, drop cancellation, and disconnects.

Golden wire-message tests can validate exact JSON compatibility. Table-state tests should exercise
release/re-export races and verify that local targets are dropped exactly when the remote reference
count reaches zero.

## Delivery plan

### Phase 1: protocol proof of concept

- Message and expression types.
- JSON encoding and decoding.
- WebSocket transport.
- Import/export tables.
- Untyped calls, resolution, rejection, pull, and release.
- JavaScript interoperability test harness.

### Phase 2: typed client and server

- TypeScript-to-IR frontend for a constrained declaration subset.
- Rust value-type generation.
- Generated `#[capnweb::interface]` traits and typed client stubs.
- `#[capnweb::target]` implementation adapters and local-stub wrapping.
- Typed `RpcPromise<T>` and basic promise pipelining.
- Local targets and ordinary callbacks.
- Per-introduction import leases, export counts, and lifetime diagnostics.
- Typed readable/writable streams with cancellation and fixed-window backpressure.

### Phase 3: protocol completeness

- Capabilities nested in arbitrary values.
- Full pipeline property paths and promise forwarding.
- HTTP batch transport.
- Recorded `.map()` and captures.
- Resource limits and malformed-peer hardening.
- Broader error and special-value support.

### Phase 4: web values and stream optimization

- Dynamic bandwidth-delay-product stream flow control.
- Blob support over the pipe machinery.
- Request, response, and headers mappings.
- Remaining transports and conformance cases.

## Expected effort

With a deliberately constrained TypeScript input and an initial focus on ordinary values, calls,
callbacks, and pipelining:

- a protocol proof of concept should be achievable in roughly one to two weeks;
- a useful generated client/server implementation should be achievable in roughly three to six
  weeks;
- robust interoperability, conformance tests, limits, and broader protocol coverage would likely
  take one to three months.

Complete parity with every JavaScript value, transport, stream behavior, and TypeScript type-system
edge case would be a larger project, but it is not necessary to validate or ship the core design.

## Open questions

1. Should `.d.ts` be the only accepted source, or should the tool also discover exported API types
   directly from `.ts` projects?
2. How are RPC roots marked for generation: `extends RpcTarget`, explicit configuration, JSDoc, or
   all three?
3. How should `#[capnweb::target(Interface)]` report schema mismatches for inherent sync and async
   methods, including aliases around `Result<T, E>`?
4. Should generated methods accept a sealed `RpcEncodeArg<T>` abstraction, or provide separate
   methods for concrete and pipelined arguments?
5. Should `.d.ts` codegen emit fully expanded Rust, or emit `#[capnweb::interface]` traits and let a
   procedural macro generate the repetitive provider and dispatch surface?
6. Should the explicit generated `ApiTarget::new(implementation)` adapter also be available when an
   application does not want to annotate its inherent implementation?
7. Which transport and async runtime should be supported first: Tokio, runtime-neutral traits, or a
   small core with runtime-specific adapters?
8. How should JavaScript `number`, `undefined`, and structurally typed unions map into Rust without
   surprising lossy conversions?
9. Should initial stream flow control use a fixed global window, a configurable per-stream window,
   or immediately port the JavaScript BDP estimator?

## Recommendation

Proceed with a narrow interoperability prototype. Implement the untyped session and table model,
then hand-write one generated-style `ApiStub` and target dispatcher before building the TypeScript
frontend. This will validate the two important assumptions early:

1. Per-introduction `Arc` leases and ordinary Rust ownership eliminate import and payload
   refcounting, leaving only the remote count on each export boundary.
2. Generated, path-bearing stub and `RpcPromise<T>` methods provide the JavaScript proxy behavior
   without a general expression-builder API.
3. `#[capnweb::interface]` and `#[capnweb::target]` can cleanly connect generated contracts to typed
   remote stubs and local implementations.
4. Typed streams can reuse ordinary capability calls and the same ownership model, with only pipe
   setup, ordered writes, cancellation, and flow control added.

If those pieces work cleanly against the JavaScript test peer, the remaining work is primarily
code generation, coverage, and incremental support for additional protocol value types.
