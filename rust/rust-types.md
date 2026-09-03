# Proposal: TypeScript-to-Rust type mapping

## Summary

Generate an idiomatic, typed Rust API from a TypeScript declaration surface while preserving the
existing Cap'n Web wire protocol. The generator may consume either `.ts` or `.d.ts`; in both cases,
it should use the TypeScript compiler API and type checker rather than parse syntax in isolation.

The generated API has three parts:

- resolved Rust values used by application code;
- pending/provider values used for calls and promise pipelining;
- interface descriptors used by typed stubs and `#[capnweb::target(...)]` dispatch.

The initial mapping should be useful without attempting to represent all of TypeScript. Constructs
that cannot be mapped unambiguously either become `CapnwebValue` or produce a clear codegen error.

## Guiding rules

1. Preserve exact TypeScript wire names in generated metadata.
2. Use ordinary Rust primitives and containers where the mapping is unambiguous.
3. Use generated structs for pass-by-value objects with known keys.
4. Use typed capability handles for `RpcTarget`s and functions.
5. Hide promise materialization and wire errors from local target implementation signatures.
6. Generate enums only for unions that can be decoded unambiguously.
7. Fall back to `CapnwebValue` for genuinely dynamic values.
8. Preserve `undefined` as `CapnwebValue::Undefined`; map typed `void` and exact `undefined` to `()`.
9. Do not expose Rust implementation fields merely because they are public.

## Source discovery

The generator should accept a TypeScript project and one or more exported root API types. It should
use the TypeScript compiler API to resolve:

- imports and aliases;
- interfaces and classes;
- `RpcTarget` inheritance;
- methods and getters;
- function signatures;
- optional parameters and properties;
- promises;
- arrays, tuples, records, and supported unions;
- recursive references;
- source names and documentation comments.

A `.d.ts` remains the clearest cross-language contract, but requiring users to emit one manually is
unnecessary. The same semantic frontend can read exported declarations directly from `.ts`.

## Primitive values

| TypeScript       | Rust                             | Notes                                    |
| ---------------- | -------------------------------- | ---------------------------------------- |
| `string`         | `String`                         | UTF-8 locally                            |
| `boolean`        | `bool`                           |                                          |
| `number`         | `f64`                            | Includes NaN and infinities              |
| `bigint`         | `BigInt`                         | Narrower mappings require annotations    |
| `null`           | `None` in a nullable type        |                                          |
| `undefined`      | `()` / `CapnwebValue::Undefined` | Depends on typed vs. dynamic context     |
| `void`           | `()`                             | Encodes as Cap'n Web `undefined`         |
| string literal   | generated unit enum variant      | Usually part of a literal union          |
| boolean literal  | `bool`                           | Validation may enforce the literal       |
| number literal   | `f64`                            | Validation may enforce the literal       |

TypeScript `number` does not imply an integer width. The default must therefore be `f64`. Branded
numbers or explicit codegen annotations may add `i32`, `u64`, and similar mappings later.

## Nullability and optional values

A single nullish alternative maps to `Option<T>`, with generated encoding determined by the source
declaration:

```ts
value: T | null;       // None encodes as null
value: T | undefined;  // None encodes as undefined
value?: T;             // an absent value encodes as undefined
```

Dynamic decoding preserves the distinction as `CapnwebValue::Null` and
`CapnwebValue::Undefined`. A declaration such as `T | null | undefined` needs a generated enum or
other explicit wrapper capable of representing all three states; plain `Option<T>` is insufficient.

## Arrays, tuples, and string-keyed objects

| TypeScript                       | Rust                       |
| -------------------------------- | -------------------------- |
| `T[]`                            | `Vec<T>`                   |
| `readonly T[]`                   | `Vec<T>`                   |
| `[A, B]`                         | `(A, B)`                   |
| `readonly [A, B]`                | `(A, B)`                   |
| `[A, B?]`                        | `(A, Option<B>)`           |
| `{ [key: string]: V }`           | `BTreeMap<String, V>`      |
| `Record<string, V>`              | `BTreeMap<String, V>`      |
| statically known object keys     | generated Rust `struct`    |

`BTreeMap` is the default for deterministic serialization and debugging. This mapping represents a
plain JavaScript object with unknown string keys; it does not add support for JavaScript `Map`,
which Cap'n Web does not currently serialize.

## Pass-by-value objects

A plain interface with known properties becomes a Rust struct:

```ts
export interface User {
  id: string;
  displayName: string;
  nickname?: string;
}
```

```rust
#[derive(Clone, Debug, RpcValue)]
pub struct User {
    pub id: String,

    #[capnweb(rename = "displayName")]
    pub display_name: String,

    pub nickname: Option<String>,
}
```

These fields are pass-by-value data and are remotely traversable. The generated metadata always
retains the original TypeScript key.

Initially, Serde may be used after the RPC runtime has materialized all promises in the payload.
Serde itself should not be responsible for awaiting providers.

## Promise mapping

A remote method call always returns a pending value in Rust, regardless of whether its TypeScript
return declaration explicitly contains `Promise<T>`:

```ts
getName(): string;
getUser(): Promise<User>;
```

Conceptually:

```rust
fn get_name(&self) -> RpcPromise<String>;
fn get_user(&self) -> PendingUser;
```

Local target implementation methods remain ordinary Rust:

```rust
#[capnweb::target(Api)]
impl ApiImpl {
    fn get_name(&self) -> String {
        self.name.clone()
    }

    async fn get_user(&self) -> Result<User, DatabaseError> {
        // ...
    }
}
```

The generated dispatch wrapper:

- invokes synchronous methods synchronously;
- eagerly drives asynchronous Rust futures;
- converts application `Result<T, E>` errors into RPC rejections;
- materializes all promise-valued arguments before invoking the method;
- does not force local methods to return a runtime-specific `RpcResult<T>`.

When a pending value is passed as another call's argument, the generated method emits a pipeline
expression rather than awaiting it locally.

## Pending views of known objects

A generic `RpcPromise<User>` cannot expose Rust fields. For a known pass-by-value object, codegen
should generate a pending view:

```rust
pub struct PendingUser {
    root: RpcPromise<User>,

    pub id: RpcPromise<String>,
    pub display_name: RpcPromise<String>,
    pub nickname: RpcPromise<Option<String>>,
}
```

`PendingUser` implements `Future<Output = Result<User, RpcError>>`. Constructing it creates property
paths but does not emit network calls.

This permits actual field syntax while pipelining:

```rust
let name = api.get_user().display_name;
let name: String = name.await?;
```

It also permits forwarding a field without awaiting:

```rust
let greeting = api.greet(api.get_user().display_name);
```

After awaiting the whole value, fields are ordinary Rust values:

```rust
let user: User = api.get_user().await?;
let name: String = user.display_name;
```

Pending structs should not implement `Drop`; doing so would prohibit moving individual fields out.
Their fields and root provider hold the required Arc-backed ownership independently.

### Nested objects

A direct nested POJO can initially remain an `RpcPromise<Nested>` field. Supporting arbitrary
chains of field syntax through recursive object graphs requires lazy recursive views and is not a v0
requirement.

Capability and function fields are more useful and can expose their generated pending providers
directly, as described below.

## `RpcTarget` capabilities

A TypeScript class or interface identified as an `RpcTarget` generates a typed capability API:

```ts
export declare class Counter extends RpcTarget {
  get value(): number;
  increment(by: number): number;
}
```

The principal generated types are conceptually:

```rust
pub enum CounterInterface {}

pub struct Counter {
    hook: StubHook,
}

pub struct PendingCounter {
    hook: StubHook,
    path: PropertyPath,
}
```

`Counter` is a resolved typed capability handle. Its hook may point to:

- a remote import;
- a local Rust target;
- a target originally exported by this process and passed back by the peer.

Application code should see the same `Counter` type regardless of provenance. A target passed back
to its owner may use an entirely local hook, but it is not automatically downcast to the concrete
`CounterImpl`. Such a downcast would make behavior depend on provenance and cannot be guaranteed by
the wire schema.

`PendingCounter` represents an unresolved capability and exposes the same callable methods for
pipelining. It may additionally implement `Future<Output = Result<Counter, RpcError>>`.

A local implementation can be wrapped explicitly:

```rust
let counter = Counter::new_local(CounterImpl::new());
```

The target macro makes only schema-declared methods and getters reachable.

### Capability fields in objects

Given:

```ts
interface User {
  name: string;
  counter: Counter;
}
```

codegen produces:

```rust
pub struct User {
    pub name: String,
    pub counter: Counter,
}

pub struct PendingUser {
    root: RpcPromise<User>,
    pub name: RpcPromise<String>,
    pub counter: PendingCounter,
}
```

This supports direct pipelined calls through an actual field:

```rust
let value = api
    .get_user()
    .counter
    .increment(1.0)
    .await?;
```

Awaiting the whole user produces a resolved `Counter` field backed by the appropriate local or
remote hook.

### Nullable capabilities

A nullable capability resolves to `Option<Counter>`:

```ts
counter: Counter | null;
```

```rust
pub counter: Option<Counter>;
```

Ordinary Cap'n Web path traversal does not implicitly provide optional chaining. Calling through a
null capability rejects. Cap'n Web's `.map()` operation supplies null/undefined propagation.

The faithful pending representation is initially:

```rust
pub counter: RpcPromise<Option<Counter>>;
```

A future generated nullable provider could expose convenience methods implemented through `remap`,
but that would be generated sugar rather than ordinary pipeline traversal.

## Functions and callbacks

A TypeScript function is a callable capability and should behave like a function in Rust:

```ts
export type Formatter = (
  value: string,
  uppercase?: boolean,
) => string | Promise<string>;
```

The resolved Rust representation should support call syntax:

```rust
let result = formatter("hello".into(), Some(true)).await?;
```

Stable Rust does not permit directly implementing `Fn`, `FnMut`, or `FnOnce` for a custom struct.
The generated wrapper can instead implement `Deref` to a stored callable trait object:

```rust
pub struct Formatter {
    hook: StubHook,
    callable: Arc<
        dyn Fn(String, Option<bool>) -> RpcPromise<String> + Send + Sync,
    >,
}

impl Deref for Formatter {
    type Target =
        dyn Fn(String, Option<bool>) -> RpcPromise<String> + Send + Sync;

    fn deref(&self) -> &Self::Target {
        &*self.callable
    }
}
```

The wrapper retains its hook so re-encoding it preserves capability identity rather than creating a
new forwarding target.

Generated call arguments should also accept ordinary local closures:

```rust
api.format(|value: String, uppercase: Option<bool>| async move {
    if uppercase.unwrap_or(false) {
        value.to_uppercase()
    } else {
        value
    }
});
```

The runtime wraps the local closure in a callable target hook. Received callbacks become the
callable generated wrapper. A pending function field or return value similarly uses a
`PendingFormatter` that is callable and pipelines through its promise hook.

## Target getters and implementation fields

Rust has no language-level getter syntax. A target getter maps to an idiomatic zero-argument Rust
accessor method:

```ts
get value(): number;
```

```rust
let value = counter.value().await?;
```

The generated accessor emits a property-get operation, not a zero-argument method call. This wire
distinction must be preserved.

On the implementation side:

```rust
struct CounterImpl {
    value: f64,
    secret: String,
}

#[capnweb::target(CounterInterface)]
impl CounterImpl {
    fn value(&self) -> f64 {
        self.value
    }

    fn increment(&self, by: f64) -> f64 {
        // ...
    }
}
```

Neither `value` nor `secret` is traversable merely because it is a Rust struct field. Only methods
and getters present in the generated target descriptor are exported. This corresponds to the
JavaScript rule that an `RpcTarget` exposes prototype methods/getters but not own instance fields.

For source contracts, properties of pass-by-value interfaces are data fields. Properties of an
`RpcTarget` should be remotely exposed only when the TypeScript declaration identifies them as
getters. The generator should diagnose target instance-field declarations that would not be
reachable from a JavaScript implementation.

A getter result can itself be traversed. Generated target dispatch evaluates the getter for the
first path component and delegates the remainder of the property path to the resulting hook or
payload, matching JavaScript `followPath()` behavior.

## Errors and rejections

Incoming JavaScript errors should use a stable wire-facing representation rather than attempt to
construct arbitrary Rust error types:

```rust
pub struct RemoteError {
    pub kind: ErrorKind,
    pub message: String,
    pub stack: Option<String>,
    pub properties: BTreeMap<String, CapnwebValue>,
}

pub enum ErrorKind {
    Error,
    TypeError,
    RangeError,
    ReferenceError,
    SyntaxError,
    AggregateError,
    Other(String),
}
```

This preserves the protocol's well-known error whitelist while retaining unknown names safely.
Extra properties, `cause`, and aggregate errors can use `CapnwebValue` recursively.

An error transported as a value maps to `RemoteError`. A rejected operation maps to
`RpcError::Remote(RemoteError)`.

An ordinary application error:

```rust
async fn load(&self) -> Result<User, DatabaseError>;
```

is converted by generated dispatch to a generic wire `Error` by default. Applications can implement
an `IntoRemoteError` trait when they need a specific well-known kind or extra properties. Stack
traces remain redacted by default.

## Union types

Rust enum variants always have Rust names, but their payloads can be unit, tuple-like, or
struct-like. Variant names need not appear on the wire.

### Literal unions

```ts
type Status = "pending" | "ready" | "failed";
```

```rust
pub enum Status {
    Pending,
    Ready,
    Failed,
}
```

### Discriminated object unions

```ts
type Event =
  | { kind: "created"; user: User }
  | { kind: "deleted"; id: string };
```

```rust
pub enum Event {
    Created { user: User },
    Deleted { id: String },
}
```

The encoded value remains the original plain object containing `kind`; Rust variant names are local
codegen details.

### Untagged primitive unions

Wire-distinguishable primitive alternatives can use tuple variants:

```ts
type Identifier = string | number;
```

```rust
pub enum Identifier {
    String(String),
    Number(f64),
}
```

### Ambiguous unions

Structurally overlapping unions fall back to `CapnwebValue`:

```ts
type Result = { name: string } | { id: string };
```

A value may satisfy both branches, so selecting a Rust enum variant would require arbitrary ordering.

For v0, generate enums only for:

- literal unions;
- discriminated object unions;
- unions of clearly wire-distinguishable primitive categories;
- nullable unions, which map to `Option<T>`.

All other unions map to `CapnwebValue`.

## Special values

Initial or planned mappings are:

| TypeScript          | Rust                                      |
| ------------------- | ----------------------------------------- |
| `Uint8Array`        | `Bytes` or `Vec<u8>`                      |
| `Int8Array`         | `Vec<i8>`                                 |
| `Uint16Array`       | `Vec<u16>`                                |
| `Int16Array`        | `Vec<i16>`                                |
| `Uint32Array`       | `Vec<u32>`                                |
| `Int32Array`        | `Vec<i32>`                                |
| `BigUint64Array`    | `Vec<u64>`                                |
| `BigInt64Array`     | `Vec<i64>`                                |
| `Float32Array`      | `Vec<f32>`                                |
| `Float64Array`      | `Vec<f64>`                                |
| `ArrayBuffer`       | `ArrayBuffer` or `Bytes` wrapper          |
| `DataView`          | `DataView` wrapper                        |
| `Date`              | Cap'n Web `Date` wrapper initially        |
| `URL`               | `url::Url`                                |
| `Headers`           | Cap'n Web `Headers` wrapper               |
| `Error`             | `RemoteError`                             |
| `ReadableStream<T>` | `RpcReadStream<T>`                        |
| `WritableStream<T>` | `RpcWriteSink<T>`                         |

`Blob`, `Request`, and `Response` are deferred from v0. JavaScript `Map`, `Set`, `RegExp`, arbitrary
application-defined by-value classes, and cyclic value graphs remain unsupported while the wire
runtime does not support them.

## Dynamic fallback

Values that are intentionally dynamic or cannot be represented safely use one recursive enum:

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
    Error(RemoteError),
    Date(Date),
    Url(Url),
    Headers(Headers),
    Target(UntypedCapability),
    Function(UntypedFunction),
    Readable(RpcReadStream<CapnwebValue>),
    Writable(RpcWriteSink<CapnwebValue>),
}
```

Unknown-value handling uses exhaustive Rust matching. `std::any::Any` is not useful for remote
values because the peer does not transmit Rust `TypeId`s.

An untyped capability permits dynamic member calls by wire name. It can be treated as a generated
typed capability based on the local schema, but that cast cannot be verified until a call is made;
Cap'n Web carries no runtime interface identifier.

## Name generation

Convert TypeScript names to idiomatic snake case, but never reconstruct wire names from Rust names.
Generated code stores the original name explicitly:

```rust
#[capnweb(rename = "displayName")]
pub display_name: String;
```

```rust
pub fn get_user_profile(&self, id: String) -> PendingUser {
    self.call("getUserProfile", (id,))
}
```

The naming pass must account for:

- acronyms such as `userID`;
- digits;
- Rust keywords such as `type`, `match`, and `ref`;
- names that collide after snake-case conversion;
- methods and getters sharing the same source namespace.

Use raw identifiers or stable suffixes for Rust collisions while preserving exact wire metadata.

## Generated target API

For each `RpcTarget` interface, codegen emits:

- an interface descriptor containing exact methods, getters, wire names, and types;
- a resolved typed capability handle;
- a pending typed capability provider;
- generated client methods/getters;
- argument and result conversion metadata consumed by `#[capnweb::target(Interface)]`.

Application code exposes inherent methods rather than implementing a transport-shaped trait:

```rust
#[capnweb::target(Api)]
impl ApiImpl {
    fn version(&self) -> String {
        self.version.clone()
    }

    async fn get_user(&self, id: String) -> Result<User, DatabaseError> {
        // ...
    }
}
```

The target macro verifies method names and parameter/result compatibility against the generated
descriptor. Runtime-specific hooks, payloads, and errors stay behind generated dispatch.

## V0 support boundary

The v0 generator should support:

- scalar values;
- nullability and optional properties;
- arrays and readonly arrays;
- finite tuples;
- known pass-by-value objects;
- string-indexed plain objects;
- aliases;
- target interfaces;
- function callbacks;
- methods and explicit target getters;
- promises at method boundaries;
- capability fields in known objects;
- literal, discriminated, and simple primitive unions;
- dynamic fallback for unsupported unions and `unknown`;
- typed streams and selected special values as their runtime support lands.

It should initially reject or dynamically lower:

- conditional and mapped types;
- arbitrary intersections;
- ambiguous overloads;
- unconstrained generics;
- symbol-keyed fields;
- target instance fields presented as remotely traversable;
- unsupported web-platform values;
- structurally ambiguous unions.

The generator should emit source-located diagnostics explaining both the unsupported TypeScript
construct and the available fallback, where one exists.

## Recommendation

Use exact generated types for the common path and `CapnwebValue` as an explicit escape hatch:

- atoms become Rust atoms;
- arrays become `Vec`;
- unknown-key objects become `BTreeMap`;
- known-key objects become structs;
- known targets become uniform typed capability handles;
- known functions become callable wrappers and accept ordinary Rust closures;
- known POJO results get pending field views;
- capability fields get pending typed capability providers;
- target getters become accessor methods;
- target implementation fields remain inaccessible;
- unambiguous unions become enums;
- general unions and unknown values become `CapnwebValue`.

This provides strong typing and natural Rust syntax where the wire shape supports it without trying
to reproduce TypeScript's entire structural type system.
