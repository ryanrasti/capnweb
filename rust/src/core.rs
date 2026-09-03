// JS: // Copyright (c) 2025 Cloudflare, Inc.
// Copyright (c) 2025 Cloudflare, Inc.
// JS: // Licensed under the MIT license found in the LICENSE.txt file or at:
// Licensed under the MIT license found in the LICENSE.txt file or at:
// JS: //     https://opensource.org/license/mit
//     https://opensource.org/license/mit
// JS:

// JS: import type { RpcTargetBranded, __RPC_TARGET_BRAND } from "./types.js";
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use num_bigint::BigInt;
use thiserror::Error as ThisError;

/// An error raised at an RPC boundary.
#[derive(Clone, Debug, ThisError)]
pub enum Error {
    /// A generic application or protocol error.
    #[error("{0}")]
    Message(String),
    /// A value did not have the required type.
    #[error("{0}")]
    Type(String),
    /// A deliberately deferred JavaScript built-in was requested.
    #[error("JavaScript built-in `{0}` is not supported by the Rust runtime")]
    UnsupportedBuiltin(&'static str),
    /// An operation used a disposed RPC reference.
    #[error("Attempted to use RPC stub after it has been disposed.")]
    Disposed,
}

/// The result type used by the RPC runtime.
pub type Result<T> = std::result::Result<T, Error>;

/// An owned dynamic value accepted by the Rust RPC boundary.
pub enum CapnwebValue {
    /// JavaScript `null` or `undefined`.
    Null,
    /// A boolean.
    Bool(bool),
    /// A JavaScript number.
    Number(f64),
    /// A string.
    String(String),
    /// An arbitrary-size integer.
    BigInt(BigInt),
    /// A byte string.
    Bytes(Vec<u8>),
    /// An array.
    Array(Vec<Self>),
    /// A plain string-keyed object.
    Object(BTreeMap<String, Self>),
    /// A serializable function capability.
    Function(Arc<dyn RpcTarget>),
    /// An RPC stub.
    Stub(RpcStub),
    /// An unresolved RPC value.
    RpcPromise(RpcPromise),
    /// A local RPC target.
    Target(Arc<dyn RpcTarget>),
    /// A serialized application error.
    Error(Error),
}

impl std::fmt::Debug for CapnwebValue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => formatter.write_str("Null"),
            Self::Bool(value) => formatter.debug_tuple("Bool").field(value).finish(),
            Self::Number(value) => formatter.debug_tuple("Number").field(value).finish(),
            Self::String(value) => formatter.debug_tuple("String").field(value).finish(),
            Self::BigInt(value) => formatter.debug_tuple("BigInt").field(value).finish(),
            Self::Bytes(value) => formatter.debug_tuple("Bytes").field(value).finish(),
            Self::Array(value) => formatter.debug_tuple("Array").field(value).finish(),
            Self::Object(value) => formatter.debug_tuple("Object").field(value).finish(),
            Self::Function(_) => formatter.write_str("Function(..)"),
            Self::Stub(_) => formatter.write_str("Stub(..)"),
            Self::RpcPromise(_) => formatter.write_str("RpcPromise(..)"),
            Self::Target(_) => formatter.write_str("Target(..)"),
            Self::Error(value) => formatter.debug_tuple("Error").field(value).finish(),
        }
    }
}

/// A value that is either available synchronously or will become available asynchronously.
pub enum MaybePromise<T> {
    /// The synchronous fast path.
    Immediate(T),
    /// An eagerly-started asynchronous operation.
    Pending(Pin<Box<dyn Future<Output = Result<T>> + Send + 'static>>),
}
// JS: import { WORKERS_MODULE_SYMBOL } from "./symbols.js"
// RUST-DEFERRED(JS built-in: workers module Symbol): no Rust global-symbol lookup is performed.
// JS:

// JS: // Polyfill Symbol.dispose for browsers that don't support it yet
// Polyfill Symbol.dispose for browsers that don't support it yet.
// JS: if (!Symbol.dispose) {
// JS:   (Symbol as any).dispose = Symbol.for('dispose');
// JS: }
// JS: if (!Symbol.asyncDispose) {
// JS:   (Symbol as any).asyncDispose = Symbol.for('asyncDispose');
// JS: }
// RUST-DEFERRED(JS built-in: Symbol.dispose): Rust uses `Drop` for local disposal.
// JS:

// JS: // Polyfill Promise.withResolvers() for old Safari versions (ugh), Hermes (React Native), and
// Polyfill Promise.withResolvers() for old Safari versions (ugh), Hermes (React Native), and
// JS: // maybe others.
// maybe others.
// JS: if (!Promise.withResolvers) {
// JS:   Promise.withResolvers = function<T>(): PromiseWithResolvers<T> {
// JS:     let resolve: (value: T | PromiseLike<T>) => void;
// JS:     let reject: (reason?: any) => void;
// JS:     const promise = new Promise<T>((res, rej) => {
// JS:       resolve = res;
// JS:       reject = rej;
// JS:     });
// JS:     return { promise, resolve: resolve!, reject: reject! };
// JS:   };
// JS: }
// RUST-DEFERRED(JS built-in: Promise.withResolvers): Rust resolver state is defined where used.
// JS:

// JS: let workersModule: any = (globalThis as any)[WORKERS_MODULE_SYMBOL];
// RUST-DEFERRED(JS built-in: workers module global): native Workers RPC objects are unsupported.
// JS:

// JS: export interface RpcTarget {
/// Marker implemented by values exposed as local RPC targets.
pub trait RpcTarget: Send + Sync + 'static {
    // JS:   [__RPC_TARGET_BRAND]: never;
    // RUST-DIFF(JS Proxy): generated target implementations provide the unforgeable marker.
    // JS: };
}
// JS:

// JS: export let RpcTarget = workersModule ? workersModule.RpcTarget : class {};
// RUST-DIFF(JS Proxy): `#[rpc_target]` replaces the runtime target constructor and reflection.
// JS:

// JS: export type PropertyPath = (string | number)[];
/// A sequence of string or numeric property keys traversed by an RPC operation.
pub type PropertyPath = Vec<PropertyKey>;

/// One component of an RPC property path.
#[derive(Clone, Debug, PartialEq)]
pub enum PropertyKey {
    /// A named object property.
    String(String),
    /// A numeric array property.
    Number(f64),
}
// JS:

// JS: type TypeForRpc = "unsupported" | "primitive" | "object" | "function" | "array" | "date" |
// JS:     "bigint" | "bytes" | "blob" | "stub" | "rpc-promise" | "rpc-target" | "rpc-thenable" |
// JS:     "error" | "undefined" | "writable" | "readable" | "url" | "headers" | "request" | "response";
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TypeForRpc {
    Unsupported,
    Primitive,
    Object,
    Function,
    Array,
    Date,
    Bigint,
    Bytes,
    Blob,
    Stub,
    RpcPromise,
    RpcTarget,
    RpcThenable,
    Error,
    Undefined,
    Writable,
    Readable,
    Url,
    Headers,
    Request,
    Response,
}
// JS:

// JS: const AsyncFunction = (async function () {}).constructor;
// RUST-DEFERRED(JS built-in: Function prototypes): callable values have an explicit enum variant.
// JS:

// JS: // Buffer.prototype for Node.js environments, where Buffer is a Uint8Array subclass that we want
// Buffer.prototype for Node.js environments, where Buffer is a Uint8Array subclass that we want
// JS: // to accept as "bytes". In browsers, this is undefined, which won't match any prototype.
// to accept as "bytes". In browsers, this is undefined, which won't match any prototype.
// JS: let BUFFER_PROTOTYPE: object | undefined =
// JS:     typeof Buffer !== "undefined" ? Buffer.prototype : undefined;
// RUST-DEFERRED(JS built-in: typed-array prototypes): all selected Rust byte containers become Bytes.
// JS:

// JS: export function typeForRpc(value: unknown): TypeForRpc {
pub(crate) fn type_for_rpc(value: &CapnwebValue) -> TypeForRpc {
    // JS:   switch (typeof value) {
    match value {
        // JS:     case "boolean":
        // JS:     case "number":
        // JS:     case "string":
        // JS:       return "primitive";
        CapnwebValue::Bool(_) | CapnwebValue::Number(_) | CapnwebValue::String(_) => {
            TypeForRpc::Primitive
        }
        // JS:

        // JS:     case "undefined":
        // JS:       return "undefined";
        // Rust aliases `undefined` to `Null`, which is classified with the other primitive values below.
        // JS:

        // JS:     case "object":
        // JS:     case "function":
        // JS:       // Test by prototype, below.
        // JS:       break;
        // Object and function variants are classified by the exhaustive enum match below.
        // JS:

        // JS:     case "bigint":
        // JS:       return "bigint";
        CapnwebValue::BigInt(_) => TypeForRpc::Bigint,
        // JS:

        // JS:     default:
        // JS:       return "unsupported";
        // Rust's dynamic enum cannot contain an unclassified application value.
        // JS:   }
        // JS:

        // JS:   // Ugh JavaScript, why is `typeof null` equal to "object" but null isn't otherwise anything like
        // Ugh JavaScript, why is `typeof null` equal to "object" but null isn't otherwise anything like
        // JS:   // an object?
        // an object?
        // JS:   if (value === null) {
        // JS:     return "primitive";
        CapnwebValue::Null => TypeForRpc::Primitive,
        // JS:   }
        // JS:

        // JS:   // Aside from RpcTarget, we generally don't support serializing *subclasses* of serializable
        // Aside from RpcTarget, we generally don't support serializing *subclasses* of serializable
        // JS:   // types, so we switch on the exact prototype rather than use `instanceof` here.
        // types, so we switch on the exact prototype rather than use `instanceof` here.
        // JS:   let prototype = Object.getPrototypeOf(value);
        // Rust's exhaustive dynamic-value enum mechanically replaces prototype inspection.
        // JS:   switch (prototype) {
        // JS:     case Object.prototype:
        // JS:       return "object";
        CapnwebValue::Object(_) => TypeForRpc::Object,
        // JS:

        // JS:     case Function.prototype:
        // JS:     case AsyncFunction.prototype:
        // JS:       return "function";
        CapnwebValue::Function(_) => TypeForRpc::Function,
        // JS:

        // JS:     case Array.prototype:
        // JS:       return "array";
        CapnwebValue::Array(_) => TypeForRpc::Array,
        // JS:

        // JS:     case Date.prototype:
        // JS:       return "date";
        // RUST-DEFERRED(JS built-in: Date): there is no dynamic Date variant.
        // JS:

        // JS:     case Uint8Array.prototype:
        // JS:     case BUFFER_PROTOTYPE:
        // JS:     case ArrayBuffer.prototype:
        // JS:     case DataView.prototype:
        // JS:     case Int8Array.prototype:
        // JS:     case Uint8ClampedArray.prototype:
        // JS:     case Int16Array.prototype:
        // JS:     case Uint16Array.prototype:
        // JS:     case Int32Array.prototype:
        // JS:     case Uint32Array.prototype:
        // JS:     case BigInt64Array.prototype:
        // JS:     case BigUint64Array.prototype:
        // JS:     case Float32Array.prototype:
        // JS:     case Float64Array.prototype:
        // JS:       return "bytes";
        CapnwebValue::Bytes(_) => TypeForRpc::Bytes,
        // JS:

        // JS:     case WritableStream.prototype:
        // JS:       return "writable";
        // RUST-DEFERRED(JS built-in: WritableStream): there is no dynamic writable-stream variant.
        // JS:

        // JS:     case ReadableStream.prototype:
        // JS:       return "readable";
        // RUST-DEFERRED(JS built-in: ReadableStream): there is no dynamic readable-stream variant.
        // JS:

        // JS:     case URL.prototype:
        // JS:       return "url";
        // RUST-DEFERRED(JS built-in: URL): there is no dynamic URL variant.
        // JS:

        // JS:     case Headers.prototype:
        // JS:       return "headers";
        // RUST-DEFERRED(JS built-in: Headers): there is no dynamic Headers variant.
        // JS:

        // JS:     case Request.prototype:
        // JS:       return "request";
        // RUST-DEFERRED(JS built-in: Request): there is no dynamic Request variant.
        // JS:

        // JS:     case Response.prototype:
        // JS:       return "response";
        // RUST-DEFERRED(JS built-in: Response): there is no dynamic Response variant.
        // JS:

        // JS:     case Blob.prototype:
        // JS:       return "blob";
        // RUST-DEFERRED(JS built-in: Blob): there is no dynamic Blob variant.
        // JS:

        // JS:     // TODO: Other RPC-compatible pass-by-value types.
        // TODO: Other RPC-compatible pass-by-value types.
        // JS:

        // JS:     case RpcStub.prototype:
        // JS:       return "stub";
        CapnwebValue::Stub(_) => TypeForRpc::Stub,
        // JS:

        // JS:     case RpcPromise.prototype:
        // JS:       return "rpc-promise";
        CapnwebValue::RpcPromise(_) => TypeForRpc::RpcPromise,
        // JS:

        // JS:     // TODO: Promise<T> or thenable
        // TODO: Promise<T> or thenable.
        // JS:

        // JS:     default:
        // JS:       if (workersModule) {
        // JS:         // TODO: We also need to match `RpcPromise` and `RpcProperty`, but they currently aren't
        // JS:         //   exported by cloudflare:workers.
        // JS:         if (prototype == workersModule.RpcStub.prototype ||
        // JS:             value instanceof workersModule.ServiceStub) {
        // JS:           return "rpc-target";
        // JS:         } else if (prototype == workersModule.RpcPromise.prototype ||
        // JS:                    prototype == workersModule.RpcProperty.prototype) {
        // JS:           // Like rpc-target, but should be wrapped in RpcPromise, so that it can be pull()ed,
        // JS:           // which will await the thenable.
        // JS:           return "rpc-thenable";
        // JS:         }
        // JS:       }
        // RUST-DEFERRED(JS built-in: Workers RPC prototypes): native Workers capabilities are unsupported.
        // JS:

        // JS:       if (value instanceof RpcTarget) {
        // JS:         return "rpc-target";
        CapnwebValue::Target(_) => TypeForRpc::RpcTarget,
        // JS:       }
        // JS:

        // JS:       if (value instanceof Error) {
        // JS:         return "error";
        CapnwebValue::Error(_) => TypeForRpc::Error,
        // JS:       }
        // JS:

        // JS:       return "unsupported";
        // The exhaustive Rust value enum makes this branch unrepresentable.
        // JS:   }
    }
    // JS: }
}
// JS:
// JS: function mapNotLoaded(): never {
fn map_not_loaded<T>() -> Result<T> {
    // JS:   throw new Error("RPC map() implementation was not loaded.");
    Err(Error::Message(
        "RPC map() implementation was not loaded.".to_owned(),
    ))
    // JS: }
}
// JS:

// JS: // map() is implemented in `map.ts`. We can't import it here because it would create an import
// map() is implemented in `map.ts`. We can't import it here because it would create an import
// JS: // cycle, so instead we define two hook functions that map.ts will overwrite when it is imported.
// cycle, so instead we define two hook functions that map.ts will install when it is initialized.
// JS: export let mapImpl: MapImpl = { applyMap: mapNotLoaded, sendMap: mapNotLoaded };
static MAP_IMPL: std::sync::OnceLock<Arc<dyn MapImpl>> = std::sync::OnceLock::new();

pub(crate) fn install_map_impl(implementation: Arc<dyn MapImpl>) -> Result<()> {
    MAP_IMPL
        .set(implementation)
        .map_err(|_| Error::Message("RPC map() implementation was already loaded.".to_owned()))
}

fn loaded_map_impl() -> Result<&'static dyn MapImpl> {
    MAP_IMPL
        .get()
        .map(Arc::as_ref)
        .ok_or_else(|| Error::Message("RPC map() implementation was not loaded.".to_owned()))
}
// JS:

// JS: type MapImpl = {
pub(crate) trait MapImpl: Send + Sync {
    // JS:   // Applies a map function to an input value (usually an array).
    // Applies a map function to an input value (usually an array).
    // JS:   //
    //
    // JS:   // Takes ownership of `captures`, including when it throws synchronously, per the contract of
    // Takes ownership of `captures`, including when it throws synchronously, per the contract of
    // JS:   // StubHook.map().
    // StubHook.map().
    // JS:   applyMap(input: unknown, parent: object | undefined, owner: RpcPayload | null,
    // JS:            captures: StubHook[], instructions: unknown[])
    // JS:           : StubHook;
    fn apply_map(
        &self,
        input: CapnwebValue,
        parent: Option<&CapnwebValue>,
        owner: Option<&RpcPayload>,
        captures: Vec<StubHook>,
        instructions: Vec<CapnwebValue>,
    ) -> Result<StubHook>;
    // JS:

    // JS:   // Implements the .map() method of RpcStub.
    // Implements the .map() method of RpcStub.
    // JS:   sendMap(hook: StubHook, path: PropertyPath, func: (value: RpcPromise) => unknown)
    // JS:          : RpcPromise;
    fn send_map(
        &self,
        hook: StubHook,
        path: PropertyPath,
        function: Box<dyn FnOnce(RpcPromise) -> Result<CapnwebValue> + Send>,
    ) -> Result<RpcPromise>;
    // JS: }
}
// JS:
// JS: function streamNotLoaded(): never {
// JS:   throw new Error("Stream implementation was not loaded.");
// JS: }
fn stream_not_loaded<T>() -> Result<T> {
    Err(Error::UnsupportedBuiltin("ReadableStream/WritableStream"))
}
// JS:
// JS: // Stream support is implemented in `streams.ts`. We can't import it here because it would create
// JS: // an import cycle, so instead we define hook functions that streams.ts will overwrite.
// JS: export let streamImpl: StreamImpl = {
// JS:   createWritableStreamHook: streamNotLoaded,
// JS:   createWritableStreamFromHook: streamNotLoaded,
// JS:   createReadableStreamHook: streamNotLoaded
// JS: };
// JS:
// JS: export type StreamImpl = {
// JS:   // Creates a StubHook wrapping a local WritableStream for export.
// JS:   // The hook will call getWriter() on the stream, locking it.
// JS:   createWritableStreamHook(stream: WritableStream): StubHook;
// JS:
// JS:   // Creates a proxy WritableStream that forwards writes to a remote hook.
// JS:   createWritableStreamFromHook(hook: StubHook): WritableStream;
// JS:
// JS:   // Creates a minimal StubHook wrapping a local ReadableStream for disposal tracking.
// JS:   // The hook's dispose() will cancel the stream.
// JS:   createReadableStreamHook(stream: ReadableStream): StubHook;
// JS: }
// RUST-DEFERRED(JS built-in: ReadableStream/WritableStream): no StreamImpl trait is exposed yet.
// JS:
// JS: // Inner interface backing an RpcStub or RpcPromise.
// JS: //
// JS: // A hook may eventually resolve to a "payload".
// JS: //
// JS: // Declared as `abstract class` to allow `instanceof StubHook`, used by `RpcStub` constructor.
// JS: //
// JS: // This is conceptually similar to the Cap'n Proto C++ class `ClientHook`.
// JS: export abstract class StubHook {
/// A shared reference to the hook backing an [`RpcStub`] or [`RpcPromise`].
pub type StubHook = Arc<dyn StubHookInner>;

/// Completion and serialized-size information for a streaming call.
pub struct StreamCall {
    /// Resolves when the call completes.
    pub promise: Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>,
    /// Serialized request size for a remote call, or `None` for a local call.
    pub size: Option<usize>,
}

/// Inner interface backing an [`RpcStub`] or [`RpcPromise`].
pub trait StubHookInner: Send + Sync {
    // JS:   // Call a function at the given property path with the given arguments. Returns a hook for the
    // JS:   // promise for the result.
    // JS:   //
    // JS:   // call() takes ownership of `args`, including when it throws synchronously. Every
    // JS:   // implementation must arrange for `args` to be disposed on all failure paths as well as on
    // JS:   // success. Correspondingly, callers must never dispose `args` after invoking call(), even if
    // JS:   // it threw. Note that RpcPayload.dispose() is idempotent, so an implementation that delegates
    // JS:   // to another hook's call() may still dispose `args` defensively in its own catch block.
    // JS:   abstract call(path: PropertyPath, args: RpcPayload): StubHook;
    /// Calls a function at `path`, taking ownership of `args` even on failure.
    fn call(&self, path: PropertyPath, args: RpcPayload) -> Result<StubHook>;
    // JS:
    // JS:   // Like call(), but designed for streaming calls (e.g. WritableStream writes). Returns:
    // JS:   // - promise: A Promise<void> for the completion of the call.
    // JS:   // - size: If the call was remote, the byte size of the serialized message. For local calls,
    // JS:   //   undefined is returned, indicating the caller should await the promise to serialize writes
    // JS:   //   (no overlapping).
    // JS:   //
    // JS:   // stream() takes ownership of `args` under the same rules as call().
    // JS:   stream(path: PropertyPath, args: RpcPayload): {promise: Promise<void>, size?: number} {
    // JS:     // Default implementation: delegate to call() + pull(). No size is returned, so the caller
    // JS:     // knows this is a local call and should await the promise directly.
    // JS:     let hook = this.call(path, args);
    // JS:     let pulled: RpcPayload | Promise<RpcPayload>;
    // JS:     try {
    // JS:       pulled = hook.pull();
    // JS:     } catch (err) {
    // JS:       hook.dispose();
    // JS:       throw err;
    // JS:     }
    // JS:     let promise: Promise<void>;
    // JS:     if (pulled instanceof Promise) {
    // JS:       promise = pulled.then(p => { p.dispose(); });
    // JS:     } else {
    // JS:       pulled.dispose();
    // JS:       promise = Promise.resolve();
    // JS:     }
    // JS:     return { promise };
    // JS:   }
    fn stream(&self, path: PropertyPath, args: RpcPayload) -> Result<StreamCall> {
        let hook = self.call(path, args)?;
        let pulled = match hook.pull() {
            Ok(pulled) => pulled,
            Err(error) => {
                hook.dispose();
                return Err(error);
            }
        };

        let promise: Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> = match pulled {
            MaybePromise::Pending(pending) => Box::pin(async move {
                let payload = pending.await?;
                payload.dispose();
                Ok(())
            }),
            MaybePromise::Immediate(payload) => {
                payload.dispose();
                Box::pin(async { Ok(()) })
            }
        };

        Ok(StreamCall {
            promise,
            size: None,
        })
    }
    // JS:
    // JS:   // Apply a map operation.
    // JS:   //
    // JS:   // `captures` is a list of external stubs which are used as part of the mapper function.
    // JS:   // NOTE: The callee takes ownership of `captures`, including when map() throws synchronously;
    // JS:   // callers must not dispose them afterwards. StubHook.dispose() is safe to call multiple times,
    // JS:   // so an implementation that delegates to another hook's map() may still dispose the captures
    // JS:   // defensively.
    // JS:   //
    // JS:   // `instructions` is a JSON-serializable value describing the mapper function as a series of
    // JS:   // steps. Each step is an expression to evaluate, in the usual RPC expression format. The last
    // JS:   // instruction is the return value.
    // JS:   //
    // JS:   // Each instruction can refer to the results of any of the instructions before it, as well as to
    // JS:   // the captures, as if they were imports on the import table. In particular:
    // JS:   // * The value 0 is the input to the mapper function (e.g. one element of the array being mapped).
    // JS:   // * Positive values are 1-based indexes into the instruction table, representing the results of
    // JS:   //   previous instructions.
    // JS:   // * Negative values are -1-based indexes into the capture list.
    // JS:   abstract map(path: PropertyPath, captures: StubHook[], instructions: unknown[]): StubHook;
    /// Applies wire-level map instructions, taking ownership of every capture.
    fn map(
        &self,
        path: PropertyPath,
        captures: Vec<StubHook>,
        instructions: Vec<CapnwebValue>,
    ) -> Result<StubHook>;
    // JS:
    // JS:   // Read the property at the given path. Returns a StubHook representing a promise for that
    // JS:   // property. This behaves very similarly to call(), except that no actual function is invoked
    // JS:   // on the remote end, the property is simply returned. (Well, if the property has a getter, then
    // JS:   // that will be invoked...)
    // JS:   //
    // JS:   // (In the case that this stub is a promise with a resolution payload, get() implies cloning
    // JS:   // a branch of the payload, making a deep copy of any pass-by-value content.)
    // JS:   abstract get(path: PropertyPath): StubHook;
    /// Reads the value at `path`.
    fn get(&self, path: PropertyPath) -> Result<StubHook>;
    // JS:
    // JS:   // Create a clone of this StubHook, which can be disposed independently.
    // JS:   //
    // JS:   // The returned hook is NOT considered a promise, so will not resolve to a payload (you can use
    // JS:   // `get([])` to get a promise for a cloned payload).
    // JS:   abstract dup(): StubHook;
    /// Creates an independently disposable logical RPC reference.
    fn dup(&self) -> StubHook;
    // JS:
    // JS:   // Requests resolution of a StubHook that represents a promise, and eventually produces the
    // JS:   // payload.
    // JS:   //
    // JS:   // pull() should not be called on capabilities that aren't promises. It may never resolve or it
    // JS:   // may throw an exception.
    // JS:   //
    // JS:   // If pull() is never called (on a remote promise), the RPC system will not transmit the
    // JS:   // resolution at all. This allows a promise to be used strictly for pipelining.
    // JS:   //
    // JS:   // If the payload is already available, pull() returns it immediately, instead of returning a
    // JS:   // promise. This allows the caller to skip the microtask queue which is sometimes necessary to
    // JS:   // maintain e-order guarantees.
    // JS:   //
    // JS:   // The returned RpcPayload is the same one backing the StubHook itself. If the caller delivers
    // JS:   // or disposes the payload directly, then it should not call dispose() on the hook. If the caller
    // JS:   // does not intend to consume the StubHook, the caller must take responsibility for cloning the
    // JS:   // payload.
    // JS:   //
    // JS:   // You can call pull() multiple times, but it will return the same RpcPayload every time, and
    // JS:   // that payload should only be disposed once.
    // JS:   //
    // JS:   // If pull() returns a promise which rejects, the StubHook does not need to be disposed.
    // JS:   abstract pull(): RpcPayload | Promise<RpcPayload>;
    /// Requests this promise hook's resolution while preserving the synchronous fast path.
    fn pull(&self) -> Result<MaybePromise<RpcPayload>>;
    // JS:
    // JS:   // Called to prevent this stub from generating unhandled rejection events if it throws without
    // JS:   // having been pulled. Without this, if a client "push"es a call that immediately throws before
    // JS:   // the client manages to "pull" it or use it in a pipeline, this may be treated by the system as
    // JS:   // an unhandled rejection. Unfortunately, this unhandled rejection would be reported in the
    // JS:   // callee rather than the caller, possibly causing the callee to crash or log spurious errors,
    // JS:   // even though it's really up to the caller to deal with the exception!
    // JS:   abstract ignoreUnhandledRejections(): void;
    /// Suppresses reporting of a rejection that the remote caller is responsible for observing.
    fn ignore_unhandled_rejections(&self);
    // JS:
    // JS:   // Attempts to cancel any outstanding promise backing this hook, and disposes the payload that
    // JS:   // pull() would return (if any). If a pull() promise is outstanding, it may still resolve (with
    // JS:   // a disposed payload) or it may reject. It's safe to call dispose() multiple times.
    // JS:   abstract dispose(): void;
    /// Cancels outstanding work and releases this logical reference idempotently.
    fn dispose(&self);
    // JS:
    // JS:   abstract onBroken(callback: (error: any) => void): void;
    /// Invokes `callback` when this hook becomes permanently broken.
    fn on_broken(&self, callback: Box<dyn FnOnce(Error) + Send + 'static>);
    // JS: }
}
// JS:
// JS: export class ErrorStubHook extends StubHook {
/// A permanently broken hook which forwards the same error through every operation.
#[derive(Clone, Debug)]
pub struct ErrorStubHook {
    error: Error,
}
// JS:   constructor(private error: any) { super(); }
impl ErrorStubHook {
    /// Creates a permanently broken hook.
    pub fn new(error: Error) -> Self {
        Self { error }
    }

    fn new_hook(&self) -> StubHook {
        Arc::new(self.clone())
    }
}
// JS:
// JS:   // Per the StubHook contract, call() and map() take ownership of `args` / `captures` even on
// JS:   // failure. No callee sits behind this hook to consume them, so dispose them immediately.
impl StubHookInner for ErrorStubHook {
    // JS:   call(path: PropertyPath, args: RpcPayload): StubHook { args.dispose(); return this; }
    fn call(&self, _path: PropertyPath, args: RpcPayload) -> Result<StubHook> {
        args.dispose();
        Ok(self.new_hook())
    }
    // JS:   map(path: PropertyPath, captures: StubHook[], instructions: unknown[]): StubHook {
    fn map(
        &self,
        _path: PropertyPath,
        captures: Vec<StubHook>,
        _instructions: Vec<CapnwebValue>,
    ) -> Result<StubHook> {
        // JS:     for (let cap of captures) {
        for capture in captures {
            // JS:       cap.dispose();
            capture.dispose();
            // JS:     }
        }
        // JS:     return this;
        Ok(self.new_hook())
        // JS:   }
    }
    // JS:   get(path: PropertyPath): StubHook { return this; }
    fn get(&self, _path: PropertyPath) -> Result<StubHook> {
        Ok(self.new_hook())
    }
    // JS:   dup(): StubHook { return this; }
    fn dup(&self) -> StubHook {
        self.new_hook()
    }
    // JS:   pull(): RpcPayload | Promise<RpcPayload> { return Promise.reject(this.error); }
    fn pull(&self) -> Result<MaybePromise<RpcPayload>> {
        let error = self.error.clone();
        Ok(MaybePromise::Pending(Box::pin(async move { Err(error) })))
    }
    // JS:   ignoreUnhandledRejections(): void {}
    fn ignore_unhandled_rejections(&self) {}
    // JS:   dispose(): void {}
    fn dispose(&self) {}
    // JS:   onBroken(callback: (error: any) => void): void {
    fn on_broken(&self, callback: Box<dyn FnOnce(Error) + Send + 'static>) {
        // JS:     try {
        let error = self.error.clone();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // JS:       callback(this.error);
            callback(error);
            // JS:     } catch (err) {
        }));
        // JS:       // Don't throw back into the RPC system. Treat this as an unhandled rejection.
        // Don't unwind back into the RPC system.
        // JS:       Promise.resolve(err);
        // A caught callback panic is intentionally discarded like the squelched rejected promise.
        // JS:     }
        // JS:   }
    }
    // JS: };
}
// JS:
// JS: const DISPOSED_HOOK: StubHook = new ErrorStubHook(
// JS:     new Error("Attempted to use RPC stub after it has been disposed."));
static DISPOSED_HOOK: std::sync::LazyLock<StubHook> =
    std::sync::LazyLock::new(|| Arc::new(ErrorStubHook::new(Error::Disposed)));
// JS:
// JS: // A call interceptor can be used to intercept all RPC stub invocations within some synchronous
// JS: // scope. This is used to implement record/replay
// JS: type CallInterceptor = (hook: StubHook, path: PropertyPath, params: RpcPayload) => StubHook;
type CallInterceptor =
    Arc<dyn Fn(StubHook, PropertyPath, RpcPayload) -> Result<StubHook> + Send + Sync + 'static>;
// JS: let doCall: CallInterceptor = (hook: StubHook, path: PropertyPath, params: RpcPayload) => {
std::thread_local! {
    static DO_CALL: std::cell::RefCell<CallInterceptor> = std::cell::RefCell::new(
        Arc::new(|hook, path, params| hook.call(path, params))
    );
}

fn do_call(hook: StubHook, path: PropertyPath, params: RpcPayload) -> Result<StubHook> {
    DO_CALL.with(|interceptor| interceptor.borrow()(hook, path, params))
}
// JS:   return hook.call(path, params);
// The default closure above delegates directly to `StubHookInner::call`.
// JS: }
// JS:
// JS: export function withCallInterceptor<T>(interceptor: CallInterceptor, callback: () => T): T {
pub(crate) fn with_call_interceptor<T>(
    interceptor: CallInterceptor,
    callback: impl FnOnce() -> T,
) -> T {
    // JS:   let oldValue = doCall;
    let old_value = DO_CALL.with(|slot| slot.replace(interceptor));
    // JS:   doCall = interceptor;
    struct RestoreInterceptor(Option<CallInterceptor>);

    impl Drop for RestoreInterceptor {
        fn drop(&mut self) {
            if let Some(old_value) = self.0.take() {
                DO_CALL.with(|slot| {
                    slot.replace(old_value);
                });
            }
        }
    }

    let restore = RestoreInterceptor(Some(old_value));
    // JS:   try {
    // JS:     return callback();
    let result = callback();
    // JS:   } finally {
    // JS:     doCall = oldValue;
    drop(restore);
    // JS:   }
    // JS: }
    result
}
// JS:
// JS: // Private symbol which may be used to unwrap the real stub through the Proxy.
// JS: let RAW_STUB = Symbol("realStub");
// JS:
// JS: export interface RpcStub extends Disposable {
// JS:   // Declare magic `RAW_STUB` key that unwraps the proxy.
// JS:   [RAW_STUB]: this;
// JS: }
// JS:
// JS: const PROXY_HANDLERS: ProxyHandler<{raw: RpcStub}> = {
// JS:   apply(target: {raw: RpcStub}, thisArg: any, argumentsList: any[]) {
// JS:     let stub = target.raw;
// JS:     return new RpcPromise(doCall(stub.hook,
// JS:         stub.pathIfPromise || [], RpcPayload.fromAppParams(argumentsList)), []);
// JS:   },
// JS:
// JS:   get(target: {raw: RpcStub}, prop: string | symbol, receiver: any) {
// JS:     let stub = target.raw;
// JS:     if (prop === RAW_STUB) {
// JS:       return stub;
// JS:     } else if (prop in RpcPromise.prototype) {
// JS:       // Any method or property declared on RpcPromise (including inherited from RpcStub or
// JS:       // Object) should pass through to the target object, as trying to turn these into RPCs will
// JS:       // likely be problematic.
// JS:       //
// JS:       // Note we don't just check `prop in target` because we intentionally want to hide the
// JS:       // properties `hook` and `path`.
// JS:       return (<any>stub)[prop];
// JS:     } else if (typeof prop === "string") {
// JS:       // Return promise for property.
// JS:       return new RpcPromise(stub.hook,
// JS:           stub.pathIfPromise ? [...stub.pathIfPromise, prop] : [prop]);
// JS:     } else if (prop === Symbol.dispose &&
// JS:           (!stub.pathIfPromise || stub.pathIfPromise.length == 0)) {
// JS:       // We only advertise Symbol.dispose on stubs and root promises, not properties.
// JS:       return () => {
// JS:         stub.hook.dispose();
// JS:         stub.hook = DISPOSED_HOOK;
// JS:       };
// JS:     } else {
// JS:       return undefined;
// JS:     }
// JS:   },
// JS:
// JS:   has(target: {raw: RpcStub}, prop: string | symbol) {
// JS:     let stub = target.raw;
// JS:     if (prop === RAW_STUB) {
// JS:       return true;
// JS:     } else if (prop in RpcPromise.prototype) {
// JS:       return prop in stub;
// JS:     } else if (typeof prop === "string") {
// JS:       return true;
// JS:     } else if (prop === Symbol.dispose &&
// JS:           (!stub.pathIfPromise || stub.pathIfPromise.length == 0)) {
// JS:       return true;
// JS:     } else {
// JS:       return false;
// JS:     }
// JS:   },
// JS:
// JS:   construct(target: {raw: RpcStub}, args: any) {
// JS:     throw new Error("An RPC stub cannot be used as a constructor.");
// JS:   },
// JS:
// JS:   defineProperty(target: {raw: RpcStub}, property: string | symbol, attributes: PropertyDescriptor)
// JS:       : boolean {
// JS:     throw new Error("Can't define properties on RPC stubs.");
// JS:   },
// JS:
// JS:   deleteProperty(target: {raw: RpcStub}, p: string | symbol): boolean {
// JS:     throw new Error("Can't delete properties on RPC stubs.");
// JS:   },
// JS:
// JS:   getOwnPropertyDescriptor(target: {raw: RpcStub}, p: string | symbol): PropertyDescriptor | undefined {
// JS:     // Treat all properties as prototype properties. That's probably fine?
// JS:     return undefined;
// JS:   },
// JS:
// JS:   getPrototypeOf(target: {raw: RpcStub}): object | null {
// JS:     return Object.getPrototypeOf(target.raw);
// JS:   },
// JS:
// JS:   isExtensible(target: {raw: RpcStub}): boolean {
// JS:     return false;
// JS:   },
// JS:
// JS:   ownKeys(target: {raw: RpcStub}): ArrayLike<string | symbol> {
// JS:     return [];
// JS:   },
// JS:
// JS:   preventExtensions(target: {raw: RpcStub}): boolean {
// JS:     // Extensions are not possible anyway.
// JS:     return true;
// JS:   },
// JS:
// JS:   set(target: {raw: RpcStub}, p: string | symbol, newValue: any, receiver: any): boolean {
// JS:     throw new Error("Can't assign properties on RPC stubs.");
// JS:   },
// JS:
// JS:   setPrototypeOf(target: {raw: RpcStub}, v: object | null): boolean {
// JS:     throw new Error("Can't override prototype of RPC stubs.");
// JS:   },
// JS: };
// JS:
// JS: // Implementation of RpcStub.
// JS: //
// JS: // Note that the in the public API, we override the type of RpcStub to reflect the interface
// JS: // exposed by the proxy. That happens in index.ts. But for internal purposes, it's easier to just
// JS: // omit the type parameter.
// JS: export class RpcStub extends RpcTarget {
// JS:   // Although `hook` and `path` are declared `public` here, they are effectively hidden by the
// JS:   // proxy.
// JS:   constructor(hook: StubHook, pathIfPromise?: PropertyPath) {
// JS:     super();
// JS:
// JS:     if (!(hook instanceof StubHook)) {
// JS:       // Application invoked the constructor to explicitly construct a stub backed by some value
// JS:       // (usually an RpcTarget). (Note we override the types as seen by the app, which is why
// JS:       // the app can pass something that isn't a StubHook -- within the implementation, though,
// JS:       // we always pass StubHook.)
// JS:       let value = <any>hook;
// JS:       if (value instanceof RpcTarget || value instanceof Function) {
// JS:         hook = TargetStubHook.create(value, undefined);
// JS:       } else {
// JS:         // We adopt the value with "return" semantics since we want to take ownership of any stubs
// JS:         // within.
// JS:         hook = new PayloadStubHook(RpcPayload.fromAppReturn(value));
// JS:       }
// JS:
// JS:       // Don't let app set this.
// JS:       if (pathIfPromise) {
// JS:         throw new TypeError("RpcStub constructor expected one argument, received two.");
// JS:       }
// JS:     }
// JS:
// JS:     this.hook = hook;
// JS:     this.pathIfPromise = pathIfPromise;
// JS:
// JS:     // Proxy has an unfortunate rule that it will only be considered callable if the underlying
// JS:     // `target` is callable, i.e. a function. So our target *must* be callable. So we use a
// JS:     // dummy function.
// JS:     let func: any = () => {};
// JS:     func.raw = this;
// JS:     return new Proxy(func, PROXY_HANDLERS);
// JS:   }
// JS:
// JS:   public hook: StubHook;
// JS:   public pathIfPromise?: PropertyPath;
// JS:
// JS:   dup(): RpcStub {
// JS:     // Unfortunately the method will be invoked with `this` being the Proxy, not the `RpcPromise`
// JS:     // itself, so we have to unwrap it.
// JS:
// JS:     // Note dup() intentionally resets the path to empty and turns the result into a stub.
// JS:     // TODO: Maybe it should actually return the same type? But I think that's not what it does
// JS:     //   in Workers RPC today? (Need to check.) Alternatively, should there be an optional
// JS:     //   parameter to specify promise vs. stub?
// JS:     let target = this[RAW_STUB];
// JS:     if (target.pathIfPromise) {
// JS:       return new RpcStub(target.hook.get(target.pathIfPromise));
// JS:     } else {
// JS:       return new RpcStub(target.hook.dup());
// JS:     }
// JS:   }
// JS:
// JS:   onRpcBroken(callback: (error: any) => void) {
// JS:     this[RAW_STUB].hook.onBroken(callback);
// JS:   }
// JS:
// JS:   map(func: (value: RpcPromise) => unknown): RpcPromise {
// JS:     let {hook, pathIfPromise} = this[RAW_STUB];
// JS:     return mapImpl.sendMap(hook, pathIfPromise || [], func);
// JS:   }
// JS:
// JS:   toString() {
// JS:     return "[object RpcStub]";
// JS:   }
// JS: }
// JS:
// JS: export class RpcPromise extends RpcStub {
// JS:   // Internally, an `RpcPromise` is constructed from a `StubHook` plus a property path. The
// JS:   // application may instead pass a promise for the eventual resolution; calls made before it
// JS:   // settles are queued and delivered, in order, once it does.
// JS:   constructor(hook: StubHook | PromiseLike<unknown>, pathIfPromise?: PropertyPath) {
// JS:     if (hook instanceof StubHook) {
// JS:       super(hook, pathIfPromise ?? []);
// JS:     } else {
// JS:       if (pathIfPromise !== undefined) {
// JS:         throw new TypeError("RpcPromise constructor expected one argument, received two.");
// JS:       }
// JS:
// JS:       let kind = typeForRpc(hook);
// JS:       if (kind === "rpc-promise") {
// JS:         // Adopt an existing `RpcPromise` directly, transferring ownership of its hook: the source
// JS:         // promise is neutered, as if disposed, and must not be used afterwards. In particular,
// JS:         // adoption keeps the promise lazy -- assimilating it as a thenable would instead force its
// JS:         // resolution to be pulled -- and preserves hook-local behavior such as brokenness. This
// JS:         // applies only to promises, not bare stubs: a non-promise hook may not implement pull(),
// JS:         // so a bare stub takes the generic path below, which adopts the stub into the resolution
// JS:         // payload.
// JS:         let raw = unwrapStubAndPath(<RpcStub><unknown>hook);
// JS:         if (raw.pathIfPromise!.length > 0) {
// JS:           // Property promise: share the source's hook and path, exactly like the source promise.
// JS:           // The get() producing an independent hook happens lazily on first use, and properties
// JS:           // have no disposer, so there is nothing to neuter.
// JS:           super(raw.hook, raw.pathIfPromise);
// JS:         } else {
// JS:           let adopted = raw.hook;
// JS:           raw.hook = DISPOSED_HOOK;
// JS:           super(adopted, []);
// JS:         }
// JS:       } else if (kind === "rpc-thenable") {
// JS:         // Workerd-native RpcPromise/RpcProperty: wrap in a TargetStubHook, which pipelines calls
// JS:         // directly on the thenable and awaits it only on pull().
// JS:         super(TargetStubHook.create(<RpcTarget><unknown>hook, undefined), []);
// JS:       } else {
// JS:         // `Promise.resolve()` natively handles the hazards of assimilating an arbitrary thenable
// JS:         // (`then` getters with side effects, self-resolution, cross-realm thenables), so the
// JS:         // resolution callback below only ever sees settled, non-thenable values.
// JS:         //
// JS:         // The resolution is adopted with "return" semantics, taking ownership of any stubs
// JS:         // within (including a stub as the root value). This is the same representation used for
// JS:         // the resolution of a local async call: pull() delivers the value, pipelined calls
// JS:         // forward through the payload without forcing a pull, and a single-stub payload forwards
// JS:         // onBroken(), preserving brokenness.
// JS:         //
// JS:         // A rejection is left on the backing promise, like the rejection of a local async call:
// JS:         // PromiseStubHook disposes the arguments of queued calls and surfaces the error through
// JS:         // pull() and onBroken(). The ignoreUnhandledRejections() call keeps a never-used promise
// JS:         // from firing an unhandled rejection event; a pipelined call whose result is neither
// JS:         // awaited nor disposed still fires one, matching local async calls.
// JS:         let promiseHook = new PromiseStubHook(Promise.resolve(hook).then(
// JS:             value => new PayloadStubHook(RpcPayload.fromAppReturn(value))));
// JS:         promiseHook.ignoreUnhandledRejections();
// JS:         super(promiseHook, []);
// JS:       }
// JS:     }
// JS:   }
// JS:
// JS:   then(onfulfilled?: ((value: unknown) => unknown) | undefined | null,
// JS:        onrejected?: ((reason: any) => unknown) | undefined | null)
// JS:        : Promise<unknown> {
// JS:     return pullPromise(this).then(...arguments);
// JS:   }
// JS:
// JS:   catch(onrejected?: ((reason: any) => unknown) | undefined | null): Promise<unknown> {
// JS:     return pullPromise(this).catch(...arguments);
// JS:   }
// JS:
// JS:   finally(onfinally?: (() => void) | undefined | null): Promise<unknown> {
// JS:     return pullPromise(this).finally(...arguments);
// JS:   }
// JS:
// JS:   toString() {
// JS:     return "[object RpcPromise]";
// JS:   }
// JS: }
// JS:
// JS: // Given a stub (still wrapped in a Proxy), extract the underlying `StubHook`.
// JS: //
// JS: // The caller takes ownership, meaning it's expected that the original stub will never be disposed
// JS: // itself, but the caller is responsible for calling `dispose()` on the returned hook.
// JS: //
// JS: // However, if the stub points to a property of some other stub or promise, then no ownership is
// JS: // "transferred" because properties do not actually have disposers. However, the returned hook is
// JS: // a new hook that aliases that property, but does actually need to be disposed.
// JS: //
// JS: // The result is a promise (i.e. can be pull()ed) if and only if the input is a promise.
// JS: export function unwrapStubTakingOwnership(stub: RpcStub): StubHook {
// JS:   let {hook, pathIfPromise} = stub[RAW_STUB];
// JS:
// JS:   if (pathIfPromise && pathIfPromise.length > 0) {
// JS:     return hook.get(pathIfPromise);
// JS:   } else {
// JS:     return hook;
// JS:   }
// JS: }
// JS:
// JS: // Given a stub (still wrapped in a Proxy), extract the underlying `StubHook`, and duplicate it,
// JS: // returning the duplicate.
// JS: //
// JS: // The caller is responsible for disposing the returned hook, but the original stub also still
// JS: // needs to be disposed by its owner (unless it is a property, which never needs disposal).
// JS: //
// JS: // The result is a promise (i.e. can be pull()ed) if and only if the input is a promise. Note that
// JS: // this differs from the semantics of the actual `dup()` method.
// JS: export function unwrapStubAndDup(stub: RpcStub): StubHook {
// JS:   let {hook, pathIfPromise} = stub[RAW_STUB];
// JS:
// JS:   if (pathIfPromise) {
// JS:     return hook.get(pathIfPromise);
// JS:   } else {
// JS:     return hook.dup();
// JS:   }
// JS: }
// JS:
// JS: // Unwrap a stub returning the underlying `StubHook`, returning `undefined` if it is a property
// JS: // stub.
// JS: //
// JS: // This function is agnostic to ownership transfer. Exactly one of `stub` or the return `hook` must
// JS: // eventually be disposed (unless `undefined` is returned, in which case neither need to be
// JS: // disposed, as properties are not normally disposable).
// JS: export function unwrapStubNoProperties(stub: RpcStub): StubHook | undefined {
// JS:   let {hook, pathIfPromise} = stub[RAW_STUB];
// JS:
// JS:   if (pathIfPromise && pathIfPromise.length > 0) {
// JS:     return undefined;
// JS:   }
// JS:
// JS:   return hook;
// JS: }
// JS:
// JS: // Unwrap a stub returning the underlying `StubHook`. If it's a property, return the `StubHook`
// JS: // representing the stub or promise of which is is a property.
// JS: //
// JS: // This function is agnostic to ownership transfer. Exactly one of `stub` or the return `hook` must
// JS: // eventually be disposed.
// JS: export function unwrapStubOrParent(stub: RpcStub): StubHook {
// JS:   return stub[RAW_STUB].hook;
// JS: }
// JS:
// JS: // Given a stub (still wrapped in a Proxy), extract the `hook` and `pathIfPromise` properties.
// JS: //
// JS: // This function is agnostic to ownership transfer. Exactly one of `stub` or the return `hook` must
// JS: // eventually be disposed.
// JS: export function unwrapStubAndPath(stub: RpcStub): {hook: StubHook, pathIfPromise?: PropertyPath} {
// JS:   return stub[RAW_STUB];
// JS: }
// JS:
// JS: // Given a promise stub (still wrapped in a Proxy), pull the remote promise and deliver the
// JS: // payload. This is a helper used to implement the then/catch/finally methods of RpcPromise.
// JS: async function pullPromise(promise: RpcPromise): Promise<unknown> {
// JS:   let {hook, pathIfPromise} = promise[RAW_STUB];
// JS:   if (pathIfPromise!.length > 0) {
// JS:     // If this isn't the root promise, we have to clone it and pull the clone. This is a little
// JS:     // weird in terms of disposal: There's no way for the app to dispose/cancel the promise while
// JS:     // waiting because it never actually got a direct disposable reference. It has to dispose
// JS:     // the result.
// JS:     hook = hook.get(pathIfPromise!);
// JS:   }
// JS:   let payload = await hook.pull();
// JS:   return payload.deliverResolve();
// JS: }
// JS:
// JS: // =======================================================================================
// JS: // RpcPayload
// JS:
// JS: export type LocatedPromise = {parent: object, property: string | number, promise: RpcPromise};
// JS:
// JS: // Represents the params to an RPC call, or the resolution of an RPC promise, as it passes
// JS: // through the system.
// JS: //
// JS: // `RpcPayload` is a linear type -- it is passed to or returned from a call, ownership is being
// JS: // transferred. The payload in turn owns all the stubs within it. Disposing the payload disposes
// JS: // the stubs.
// JS: //
// JS: // Hypothetically, when an `RpcPayload` is first constructed from a message structure passed from
// JS: // the app, it ought to be deep-copied, for a few reasons:
// JS: // - To ensure subsequent modifications of the data structure by the app aren't reflected in the
// JS: //   already-sent message.
// JS: // - To find all stubs in the message tree, to take ownership of them.
// JS: // - To find all RpcTargets in the message tree, to wrap them in stubs.
// JS: //
// JS: // However, most payloads are immediately serialized to send across the wire. Said serialization
// JS: // *also* has to make a deep copy, and takes ownership of all stubs found within. In the case that
// JS: // the payload is immediately serialized, then making a deep copy first is wasteful.
// JS: //
// JS: // So, as an optimization, RpcPayload does not necessarily make a copy right away. Instead, it
// JS: // keeps track of whether it's still pointing at the message structure received directly from the
// JS: // app. In that case, the serializer can operate on the original structure directly, making it
// JS: // more efficient.
// JS: //
// JS: // On the receiving end, when an RpcPayload is deserialized from the wire, the payload can safely
// JS: // be delivered directly to the app without a copy. However, if the app makes a loopback call to
// JS: // itself, the payload may never cross the wire. In this case, a deep copy must be made before
// JS: // delivering the final message to the app. There are really two reasons for this copy:
// JS: // - We obviously don't want the caller and callee sharing in-memory mutable data structures, as
// JS: //   this would lead to vasty different behavior than what you'd see when doing RPC across a
// JS: //   network connection.
// JS: // - Before delivering the message to the application, all promises embedded in the message must
// JS: //   be resolved. This is what makes pipelining possible: the sender of a message can place
// JS: //   `RpcPromise`s in it that refer back to values in the recipient's process. These will be filled
// JS: //   in just before delivering the message to the recipient, so that there's no need to transmit
// JS: //   these values back and forth across the wire. It would be unreasonable to expect the
// JS: //   application itself to check the message for promises and resolve them all, so instead the
// JS: //   system automatically resolves all promises upfront, replacing them with their resolutions.
// JS: //   This modifies the payload in-place -- but this of course requires that the payload is
// JS: //   operating on a copy of the message, not the original provided from the sending app.
// JS: //
// JS: // For both the purposes of disposal and substituting promises with their resolutions, it is
// JS: // necessary at some point to make a list of all the stubs (including promise stubs) present in
// JS: // the message. Again, `RpcPayload` tries to minimize the number of times that the whole message
// JS: // needs to be walked, so it implements the following policy:
// JS: // * When constructing a payload from an app-provided message object, the message is not walked
// JS: //   upfront. We do not know yet what stubs it contains.
// JS: // * When deserializing a payload from the wire, we build a list of stubs as part of the
// JS: //   deserialization process.
// JS: // * If we need to deep-copy an app-provided message, we make a list of stubs then.
// JS: // * Hence, we have a list of stubs if and only if the message structure was NOT provided directly
// JS: //   by the application.
// JS: // * If an app-provided payload is serialized, the serializer finds the stubs. (It also typically
// JS: //   takes ownership of the stubs, effectively consuming the payload, so there's no need to build
// JS: //   a list of the stubs.)
// JS: // * If an app-provided payload is disposed, then we have to walk the message at that time to
// JS: //   dispose all stubs within. But, note that when a payload is serialized -- with the serializer
// JS: //   taking ownership of stubs -- then the payload will NOT be disposed explicitly, so this step
// JS: //   will not be needed.
// JS: export class RpcPayload {
// JS:   // Create a payload from a value passed as params to an RPC from the app.
// JS:   //
// JS:   // The payload does NOT take ownership of any stubs in `value`, and but promises not to modify
// JS:   // `value`. If the payload is delivered locally, `value` will be deep-copied first, so as not
// JS:   // to have the sender and recipient end up sharing the same mutable object. `value` will not be
// JS:   // touched again after the call returns synchronously (returns a promise) -- by that point,
// JS:   // the value has either been copied or serialized to the wire.
// JS:   public static fromAppParams(value: unknown): RpcPayload {
// JS:     return new RpcPayload(value, "params");
// JS:   }
// JS:
// JS:   // Create a payload from a value return from an RPC implementation by the app.
// JS:   //
// JS:   // Unlike fromAppParams(), in this case the payload takes ownership of all stubs in `value`, and
// JS:   // may hold onto `value` for an arbitrarily long time (e.g. to serve pipelined requests). It
// JS:   // will still avoid modifying `value` and will make a deep copy if it is delivered locally.
// JS:   public static fromAppReturn(value: unknown): RpcPayload {
// JS:     return new RpcPayload(value, "return");
// JS:   }
// JS:
// JS:   // Combine an array of payloads into a single payload whose value is an array. Ownership of all
// JS:   // stubs is transferred from the inputs to the outputs, hence if the output is disposed, the
// JS:   // inputs should not be. (In case of exception, nothing is disposed, though.)
// JS:   public static fromArray(array: RpcPayload[]): RpcPayload {
// JS:     let hooks: StubHook[] = [];
// JS:     let promises: LocatedPromise[] = [];
// JS:
// JS:     let resultArray: unknown[] = [];
// JS:
// JS:     for (let payload of array) {
// JS:       payload.ensureDeepCopied();
// JS:       for (let hook of payload.hooks!) {
// JS:         hooks.push(hook);
// JS:       }
// JS:       for (let promise of payload.promises!) {
// JS:         if (promise.parent === payload) {
// JS:           // This promise is the root of the source payload. We need to reparent it to its proper
// JS:           // location in the result array.
// JS:           promise = {
// JS:             parent: resultArray,
// JS:             property: resultArray.length,
// JS:             promise: promise.promise
// JS:           };
// JS:         }
// JS:         promises.push(promise);
// JS:       }
// JS:       resultArray.push(payload.value);
// JS:     }
// JS:
// JS:     return new RpcPayload(resultArray, "owned", hooks, promises);
// JS:   }
// JS:
// JS:   // Create a payload from a value parsed off the wire using Evaluator.evaluate().
// JS:   //
// JS:   // A payload is constructed with a null value and the given hooks and promises arrays. The value
// JS:   // is expected to be filled in by the evaluator, and the hooks and promises arrays are expected
// JS:   // to be extended with stubs found during parsing. (This weird usage model is necessary so that
// JS:   // if the root value turns out to be a promise, its `parent` in `promises` can be the payload
// JS:   // object itself.)
// JS:   //
// JS:   // When done, the payload takes ownership of the final value and all the stubs within. It may
// JS:   // modify the value in preparation for delivery, and may deliver the value directly to the app
// JS:   // without copying.
// JS:   public static forEvaluate(hooks: StubHook[], promises: LocatedPromise[]) {
// JS:     return new RpcPayload(null, "owned", hooks, promises);
// JS:   }
// JS:
// JS:   // Deep-copy the given value, including dup()ing all stubs.
// JS:   //
// JS:   // If `value` is a function, it should be bound to `oldParent` as its `this`.
// JS:   //
// JS:   // If deep-copying from a branch of some other RpcPayload, it must be provided, to make sure
// JS:   // RpcTargets found within don't get duplicate stubs.
// JS:   public static deepCopyFrom(
// JS:       value: unknown, oldParent: object | undefined, owner: RpcPayload | null): RpcPayload {
// JS:     let result = new RpcPayload(null, "owned", [], []);
// JS:     result.value = result.deepCopy(value, oldParent, "value", result, /*dupStubs=*/true, owner);
// JS:     return result;
// JS:   }
// JS:
// JS:   // Private constructor; use factory functions above to construct.
// JS:   private constructor(
// JS:     // The payload value.
// JS:     public value: unknown,
// JS:
// JS:     // What is the provenance of `value`?
// JS:     // "params": It came from the app, in params to a call. We must dupe any stubs within.
// JS:     // "return": It came from the app, returned from a call. We take ownership of all stubs within.
// JS:     // "owned": This value belongs fully to us, either because it was deserialized from the wire
// JS:     //   or because we deep-copied a value from the app.
// JS:     private source: "params" | "return" | "owned",
// JS:
// JS:     // `hooks` and `promises` are filled in only if `value` belongs to us (`source` is "owned") and
// JS:     // so can safely be delivered to the app. If `value` came from then app in the first place,
// JS:     // then it cannot be delivered back to the app nor modified by us without first deep-copying
// JS:     // it. `hooks` and `promises` will be computed as part of the deep-copy.
// JS:
// JS:     // All non-promise stubs found in `value`. This list is needed only for the purpose of being
// JS:     // able to dispose them when desired. This intentionally doesn't inculde promises because they
// JS:     // are already covered by `promises`, below.
// JS:     private hooks?: StubHook[],
// JS:
// JS:     // All promises found in `value`. The locations of each promise are provided to allow
// JS:     // substitutions later.
// JS:     private promises?: LocatedPromise[]
// JS:   ) {}
// JS:
// JS:   // For `source === "return"` payloads only, this tracks any StubHooks created around RpcTargets
// JS:   // or WritableStreams found in the payload at the time that it is serialized (or deep-copied) for
// JS:   // return, so that we can make sure they are not disposed before the pipeline ends.
// JS:   //
// JS:   // This is initialized on first use.
// JS:   private rpcTargets?: Map<RpcTarget | Function | WritableStream | ReadableStream, StubHook>;
// JS:
// JS:   // Get the StubHook representing the given RpcTarget found inside this payload.
// JS:   public getHookForRpcTarget(target: RpcTarget | Function, parent: object | undefined,
// JS:                              dupStubs: boolean = true): StubHook {
// JS:     if (this.source === "params") {
// JS:       if (dupStubs) {
// JS:         // We aren't supposed to take ownership of stubs appearing in params -- we're supposed to
// JS:         // dupe them. But an RpcTarget isn't a stub. If we create a stub around it, the stub takes
// JS:         // ownership.
// JS:         //
// JS:         // Usually, people passing raw RpcTargets into functions actually want the call to take
// JS:         // ownership -- that is, they want to have the disposer called later.
// JS:         //
// JS:         // But, if the RpcTarget happens to implement a `dup()` method, we will go ahead and call
// JS:         // that method, and wrap whatever it returns instead. This method wouldn't actually be
// JS:         // available over RPC anyway (since calling `dup()` on the client-side stub just dupes the
// JS:         // stub), so if an `RpcTarget` implements this, it must intend for us to use it.
// JS:         //
// JS:         // This is particularly important for the case of workerd-native RpcStubs, that is, stubs
// JS:         // from the built-in RPC system, rather than the pure-JS implementation of Cap'n Web.
// JS:         // We treat those stubs as RpcTargets. But, we do need to dup() them, just like we would
// JS:         // our own stubs.
// JS:
// JS:         let dupable = target as any;
// JS:         if (typeof dupable.dup === "function") {
// JS:           target = dupable.dup();
// JS:         }
// JS:       }
// JS:
// JS:       return TargetStubHook.create(target, parent);
// JS:     } else if (this.source === "return") {
// JS:       // If dupStubs is true, we want to both make sure the map contains the stub, and also return
// JS:       // a dup of that stub.
// JS:       //
// JS:       // If dupStubs is false, then we are being called as part of ensureDeepCopy(), i.e. replacing
// JS:       // ourselves with a deep copy. In this case we actually want the copy to end up owning all
// JS:       // the hooks, and the map to be left empty. So what we do in this case is:
// JS:       // * If the target is not in the map, we just create it, but don't populate the map.
// JS:       // * If the target *is* in the map, we *remove* the hook from the map, and return it.
// JS:
// JS:       let hook = this.rpcTargets?.get(target);
// JS:       if (hook) {
// JS:         if (dupStubs) {
// JS:           return hook.dup();
// JS:         } else {
// JS:           this.rpcTargets?.delete(target);
// JS:           return hook;
// JS:         }
// JS:       } else {
// JS:         hook = TargetStubHook.create(target, parent);
// JS:         if (dupStubs) {
// JS:           if (!this.rpcTargets) {
// JS:             this.rpcTargets = new Map;
// JS:           }
// JS:           this.rpcTargets.set(target, hook);
// JS:           return hook.dup();
// JS:         } else {
// JS:           return hook;
// JS:         }
// JS:       }
// JS:     } else {
// JS:       throw new Error("owned payload shouldn't contain raw RpcTargets");
// JS:     }
// JS:   }
// JS:
// JS:   // Get the StubHook representing the given WritableStream found inside this payload.
// JS:   public getHookForWritableStream(stream: WritableStream, parent: object | undefined,
// JS:                                   dupStubs: boolean = true): StubHook {
// JS:     if (this.source === "params") {
// JS:       // For params, we always create a new hook. WritableStreams don't have a dup() method,
// JS:       // and it wouldn't really make sense anyway since we're locking the stream by calling
// JS:       // getWriter().
// JS:       return streamImpl.createWritableStreamHook(stream);
// JS:     } else if (this.source === "return") {
// JS:       // Similar logic to getHookForRpcTarget().
// JS:       let hook = this.rpcTargets?.get(stream);
// JS:       if (hook) {
// JS:         if (dupStubs) {
// JS:           return hook.dup();
// JS:         } else {
// JS:           this.rpcTargets?.delete(stream);
// JS:           return hook;
// JS:         }
// JS:       } else {
// JS:         hook = streamImpl.createWritableStreamHook(stream);
// JS:         if (dupStubs) {
// JS:           if (!this.rpcTargets) {
// JS:             this.rpcTargets = new Map;
// JS:           }
// JS:           this.rpcTargets.set(stream, hook);
// JS:           return hook.dup();
// JS:         } else {
// JS:           return hook;
// JS:         }
// JS:       }
// JS:     } else {
// JS:       throw new Error("owned payload shouldn't contain raw WritableStreams");
// JS:     }
// JS:   }
// JS:
// JS:   // Get the StubHook representing the given ReadableStream found inside this payload.
// JS:   public getHookForReadableStream(stream: ReadableStream, parent: object | undefined,
// JS:                                   dupStubs: boolean = true): StubHook {
// JS:     if (this.source === "params") {
// JS:       return streamImpl.createReadableStreamHook(stream);
// JS:     } else if (this.source === "return") {
// JS:       let hook = this.rpcTargets?.get(stream);
// JS:       if (hook) {
// JS:         if (dupStubs) {
// JS:           return hook.dup();
// JS:         } else {
// JS:           this.rpcTargets?.delete(stream);
// JS:           return hook;
// JS:         }
// JS:       } else {
// JS:         hook = streamImpl.createReadableStreamHook(stream);
// JS:         if (dupStubs) {
// JS:           if (!this.rpcTargets) {
// JS:             this.rpcTargets = new Map;
// JS:           }
// JS:           this.rpcTargets.set(stream, hook);
// JS:           return hook.dup();
// JS:         } else {
// JS:           return hook;
// JS:         }
// JS:       }
// JS:     } else {
// JS:       throw new Error("owned payload shouldn't contain raw ReadableStreams");
// JS:     }
// JS:   }
// JS:
// JS:   private deepCopy(
// JS:       value: unknown, oldParent: object | undefined, property: string | number, parent: object,
// JS:       dupStubs: boolean, owner: RpcPayload | null): unknown {
// JS:     let kind = typeForRpc(value);
// JS:     switch (kind) {
// JS:       case "unsupported":
// JS:         // This will throw later on when someone tries to do something with it.
// JS:         return value;
// JS:
// JS:       case "primitive":
// JS:       case "bigint":
// JS:       case "date":
// JS:       case "bytes":
// JS:       case "blob":
// JS:       case "url":
// JS:       case "error":
// JS:       case "undefined":
// JS:         // immutable, no need to copy
// JS:         // TODO: Should errors be copied if they have own properties?
// JS:         return value;
// JS:
// JS:       case "array": {
// JS:         // We have to construct the new array first, then fill it in, so we can pass it as the
// JS:         // parent.
// JS:         let array = <Array<unknown>>value;
// JS:         let len = array.length;
// JS:         let result = new Array(len);
// JS:         for (let i = 0; i < len; i++) {
// JS:           result[i] = this.deepCopy(array[i], array, i, result, dupStubs, owner);
// JS:         }
// JS:         return result;
// JS:       }
// JS:
// JS:       case "object": {
// JS:         // Plain object. Unfortunately there's no way to pre-allocate the right shape.
// JS:         let result: Record<string, unknown> = {};
// JS:         let object = <Record<string, unknown>>value;
// JS:         for (let i in object) {
// JS:           result[i] = this.deepCopy(object[i], object, i, result, dupStubs, owner);
// JS:         }
// JS:         return result;
// JS:       }
// JS:
// JS:       case "stub":
// JS:       case "rpc-promise": {
// JS:         let stub = <RpcStub>value;
// JS:         let hook: StubHook;
// JS:         if (dupStubs) {
// JS:           hook = unwrapStubAndDup(stub);
// JS:         } else {
// JS:           hook = unwrapStubTakingOwnership(stub);
// JS:         }
// JS:         if (stub instanceof RpcPromise) {
// JS:           let promise = new RpcPromise(hook, []);
// JS:           this.promises!.push({parent, property, promise});
// JS:           return promise;
// JS:         } else {
// JS:           this.hooks!.push(hook);
// JS:           return new RpcStub(hook);
// JS:         }
// JS:       }
// JS:
// JS:       case "function":
// JS:       case "rpc-target": {
// JS:         let target = <RpcTarget | Function>value;
// JS:         let hook: StubHook;
// JS:         if (owner) {
// JS:           hook = owner.getHookForRpcTarget(target, oldParent, dupStubs);
// JS:         } else {
// JS:           hook = TargetStubHook.create(target, oldParent);
// JS:         }
// JS:         this.hooks!.push(hook);
// JS:         return new RpcStub(hook);
// JS:       }
// JS:
// JS:       case "rpc-thenable": {
// JS:         let target = <RpcTarget>value;
// JS:         let promise: RpcPromise;
// JS:         if (owner) {
// JS:           promise = new RpcPromise(owner.getHookForRpcTarget(target, oldParent, dupStubs), []);
// JS:         } else {
// JS:           promise = new RpcPromise(TargetStubHook.create(target, oldParent), []);
// JS:         }
// JS:         this.promises!.push({parent, property, promise});
// JS:         return promise;
// JS:       }
// JS:
// JS:       case "writable": {
// JS:         let stream = <WritableStream>value;
// JS:         let hook: StubHook;
// JS:         if (owner) {
// JS:           hook = owner.getHookForWritableStream(stream, oldParent, dupStubs);
// JS:         } else {
// JS:           hook = streamImpl.createWritableStreamHook(stream);
// JS:         }
// JS:         this.hooks!.push(hook);
// JS:         return stream;
// JS:       }
// JS:
// JS:       case "readable": {
// JS:         // Note that we don't use tee() here because we treat streams as reference types -- we
// JS:         // actually want to share the same body. tee()ing the stream would force the runtime to
// JS:         // buffer a copy of the whole body which would usually never be read.
// JS:         let stream = <ReadableStream>value;
// JS:         let hook: StubHook;
// JS:         if (owner) {
// JS:           hook = owner.getHookForReadableStream(stream, oldParent, dupStubs);
// JS:         } else {
// JS:           hook = streamImpl.createReadableStreamHook(stream);
// JS:         }
// JS:         this.hooks!.push(hook);
// JS:         return stream;
// JS:       }
// JS:
// JS:       case "headers":
// JS:         return new Headers(<Headers>value);
// JS:
// JS:       case "request": {
// JS:         let req = <Request>value;
// JS:         if (req.body) {
// JS:           // Note "deep-copy" of a ReadableStream always returns the same stream, but we still
// JS:           // need to run it in order to handle refcounting / disposal properly.
// JS:           this.deepCopy(req.body, req, "body", req, dupStubs, owner);
// JS:         }
// JS:
// JS:         // Make an actual copy of the object, e.g. so the headers are copied.
// JS:         // Note that it would be incorrect to use clone() here since that would tee() the body
// JS:         // stream.
// JS:         return new Request(req);
// JS:       }
// JS:
// JS:       case "response": {
// JS:         let resp = <Response>value;
// JS:         if (resp.body) {
// JS:           // Note "deep-copy" of a ReadableStream always returns the same stream, but we still
// JS:           // need to run it in order to handle refcounting / disposal properly.
// JS:           this.deepCopy(resp.body, resp, "body", resp, dupStubs, owner);
// JS:         }
// JS:
// JS:         // Make an actual copy of the object, e.g. so the headers are copied.
// JS:         // Note that it would be incorrect to use clone() here since that would tee() the body
// JS:         // stream.
// JS:         return new Response(resp.body, resp);
// JS:       }
// JS:
// JS:       default:
// JS:         kind satisfies never;
// JS:         throw new Error("unreachable");
// JS:     }
// JS:   }
// JS:
// JS:   // Ensures that if the value originally came from an unowned source, we have replaced it with a
// JS:   // deep copy.
// JS:   public ensureDeepCopied() {
// JS:     if (this.source !== "owned") {
// JS:       // If we came from call params, we need to dupe any stubs. Otherwise (we came from a return),
// JS:       // we take ownership of all stubs.
// JS:       let dupStubs = this.source === "params";
// JS:
// JS:       this.hooks = [];
// JS:       this.promises = [];
// JS:
// JS:       // Deep-copy the value.
// JS:       try {
// JS:         this.value = this.deepCopy(this.value, undefined, "value", this, dupStubs, this);
// JS:       } catch (err) {
// JS:         // Roll back the change.
// JS:         this.hooks = undefined;
// JS:         this.promises = undefined;
// JS:         throw err;
// JS:       }
// JS:
// JS:       // We now own the value.
// JS:       this.source = "owned";
// JS:
// JS:       // `rpcTargets` should have been left empty. We can throw it out.
// JS:       if (this.rpcTargets && this.rpcTargets.size > 0) {
// JS:         throw new Error("Not all rpcTargets were accounted for in deep-copy?");
// JS:       }
// JS:       this.rpcTargets = undefined;
// JS:     }
// JS:   }
// JS:
// JS:   // Resolve all promises in this payload and then assign the final value into `parent[property]`.
// JS:   private deliverTo(parent: object, property: string | number, promises: Promise<any>[]): void {
// JS:     this.ensureDeepCopied();
// JS:
// JS:     if (this.value instanceof RpcPromise) {
// JS:       RpcPayload.deliverRpcPromiseTo(this.value, parent, property, promises);
// JS:     } else {
// JS:       (<any>parent)[property] = this.value;
// JS:
// JS:       for (let record of this.promises!) {
// JS:         // Note that because we already did ensureDeepCopied(), replacing each promise with its
// JS:         // resolution does not interfere with disposal later on -- disposal will be based on the
// JS:         // `promises` list, so will still properly dispose each promise, which in turn disposes
// JS:         // the promise's eventual payload.
// JS:         RpcPayload.deliverRpcPromiseTo(record.promise, record.parent, record.property, promises);
// JS:       }
// JS:     }
// JS:   }
// JS:
// JS:   private static deliverRpcPromiseTo(
// JS:       promise: RpcPromise, parent: object, property: string | number,
// JS:       promises: Promise<unknown>[]) {
// JS:     // deepCopy() should have replaced any property stubs with normal promise stubs.
// JS:     let hook = unwrapStubNoProperties(promise);
// JS:     if (!hook) {
// JS:       throw new Error("property promises should have been resolved earlier");
// JS:     }
// JS:
// JS:     let inner = hook.pull();
// JS:     if (inner instanceof RpcPayload) {
// JS:       // Immediately resolved to payload.
// JS:       inner.deliverTo(parent, property, promises);
// JS:     } else {
// JS:       // It's a promise.
// JS:       promises.push(inner.then(payload => {
// JS:         let subPromises: Promise<unknown>[] = [];
// JS:         payload.deliverTo(parent, property, subPromises);
// JS:         if (subPromises.length > 0) {
// JS:           return Promise.all(subPromises);
// JS:         }
// JS:       }));
// JS:     }
// JS:   }
// JS:
// JS:   // Call the given function with the payload as an argument. The call is made synchronously if
// JS:   // possible, in order to maintain e-order. However, if any RpcPromises exist in the payload,
// JS:   // they are awaited and substituted before calling the function. The result of the call is
// JS:   // wrapped into another payload.
// JS:   //
// JS:   // The payload is automatically disposed after the call completes. The caller should not call
// JS:   // dispose().
// JS:   public async deliverCall(func: Function, thisArg: object | undefined): Promise<RpcPayload> {
// JS:     try {
// JS:       let promises: Promise<void>[] = [];
// JS:       this.deliverTo(this, "value", promises);
// JS:
// JS:       // WARNING: It is critical that if the promises list is empty, we do not await anything, so
// JS:       //   that the function is called immediately and synchronously. Otherwise, we might violate
// JS:       //   e-order.
// JS:       if (promises.length > 0) {
// JS:         await Promise.all(promises);
// JS:       }
// JS:
// JS:       // Call the function.
// JS:       let result = Function.prototype.apply.call(func, thisArg, this.value);
// JS:
// JS:       if (result instanceof RpcPromise) {
// JS:         // Special case: If the function immediately returns RpcPromise, we don't want to await it,
// JS:         // since that will actually wait for the promise. Instead we want to construct a payload
// JS:         // around it directly.
// JS:         return RpcPayload.fromAppReturn(result);
// JS:       } else {
// JS:         // In all other cases, await the result (which may or may not be a promise, but `await`
// JS:         // will just pass through non-promises).
// JS:         return RpcPayload.fromAppReturn(await result);
// JS:       }
// JS:     } finally {
// JS:       this.dispose();
// JS:     }
// JS:   }
// JS:
// JS:   // Deliver this payload as the argument list to `WritableStreamDefaultWriter.write(chunk)`.
// JS:   //
// JS:   // Unlike deliverCall(), stream chunks are enqueued and read asynchronously. Disposing the
// JS:   // payload when write() returns would free any stubs nested in the chunk before the reader
// JS:   // dequeues them. A chunk only needs its disposal deferred if it owns capabilities the
// JS:   // reader will use after write() resolves. Pure data (primitives, bytes, Date, plain
// JS:   // objects) carries no capabilities, so dispose it immediately and leave the chunk
// JS:   // untouched. Bare RpcStubs already implement Symbol.dispose, so their own disposer owns
// JS:   // the capability; only attach a payload disposer otherwise.
// JS:   //
// JS:   // Expects `this.value` to be a one-element argument array `[chunk]`. Returns a payload
// JS:   // wrapping the (void) write result.
// JS:   public async deliverStreamWrite(
// JS:       writer: { write(chunk: unknown): Promise<void> }): Promise<RpcPayload> {
// JS:     try {
// JS:       let promises: Promise<void>[] = [];
// JS:       this.deliverTo(this, "value", promises);
// JS:
// JS:       // Same e-order constraint as deliverCall: if there are no nested promises, call write
// JS:       // synchronously so concurrent writes stay ordered.
// JS:       if (promises.length > 0) {
// JS:         await Promise.all(promises);
// JS:       }
// JS:
// JS:       let chunk = (this.value as unknown[])[0];
// JS:
// JS:       // deliverTo → ensureDeepCopied, so hooks/promises are populated.
// JS:       const ownsCapabilities = this.hooks!.length > 0 || this.promises!.length > 0;
// JS:
// JS:       if (ownsCapabilities && chunk instanceof Object) {
// JS:         // Keep the payload alive until the reader disposes the chunk. A bare RpcStub already
// JS:         // implements Symbol.dispose (its own disposer owns the hook); only attach otherwise.
// JS:         if (!(Symbol.dispose in chunk)) {
// JS:           Object.defineProperty(chunk, Symbol.dispose, {
// JS:             value: () => this.dispose(),
// JS:             writable: true,
// JS:             enumerable: false,
// JS:             configurable: true,
// JS:           });
// JS:         }
// JS:         await writer.write(chunk);
// JS:         return RpcPayload.fromAppReturn(undefined);
// JS:       }
// JS:
// JS:       // No capabilities (or non-object chunk): nothing to keep alive beyond the write.
// JS:       await writer.write(chunk);
// JS:       this.dispose();
// JS:       return RpcPayload.fromAppReturn(undefined);
// JS:     } catch (err) {
// JS:       // deliverTo / write failed: free any capabilities we still own.
// JS:       this.dispose();
// JS:       throw err;
// JS:     }
// JS:   }
// JS:
// JS:   // Produce a promise for this payload for return to the application. Any RpcPromises in the
// JS:   // payload are awaited and substituted with their results first.
// JS:   //
// JS:   // The returned object will have a disposer which disposes the payload. The caller should not
// JS:   // separately dispose it.
// JS:   public async deliverResolve(): Promise<unknown> {
// JS:     try {
// JS:       let promises: Promise<void>[] = [];
// JS:       this.deliverTo(this, "value", promises);
// JS:
// JS:       if (promises.length > 0) {
// JS:         await Promise.all(promises);
// JS:       }
// JS:
// JS:       let result = this.value;
// JS:
// JS:       // Add disposer to result.
// JS:       if (result instanceof Object) {
// JS:         if (!(Symbol.dispose in result)) {
// JS:           // We want the disposer to be non-enumerable as otherwise it gets in the way of things
// JS:           // like unit tests trying to deep-compare the result to an object.
// JS:           Object.defineProperty(result, Symbol.dispose, {
// JS:             // NOTE: Using `this.dispose.bind(this)` here causes Playwright's build of
// JS:             //   Chromium 140.0.7339.16 to fail when the object is assigned to a `using` variable,
// JS:             //   with the error:
// JS:             //       TypeError: Symbol(Symbol.dispose) is not a function
// JS:             //   I cannot reproduce this problem in Chrome 140.0.7339.127 nor in Node or workerd,
// JS:             //   so maybe it was a short-lived V8 bug or something. To be safe, though, we use
// JS:             //   `() => this.dispose()`, which seems to always work.
// JS:             value: () => this.dispose(),
// JS:             writable: true,
// JS:             enumerable: false,
// JS:             configurable: true,
// JS:           });
// JS:         }
// JS:       }
// JS:
// JS:       return result;
// JS:     } catch (err) {
// JS:       // Automatically dispose since the application will never receive the disposable...
// JS:       this.dispose();
// JS:       throw err;
// JS:     }
// JS:   }
// JS:
// JS:   public dispose() {
// JS:     if (this.source === "owned") {
// JS:       // Oh good, we can just run through them.
// JS:       this.hooks!.forEach(hook => hook.dispose());
// JS:       this.promises!.forEach(promise => promise.promise[Symbol.dispose]());
// JS:     } else if (this.source === "return") {
// JS:       // Value received directly from app as a return value. We take ownership of all stubs, so we
// JS:       // must recursively scan it for things to dispose.
// JS:       this.disposeImpl(this.value, undefined);
// JS:       if (this.rpcTargets && this.rpcTargets.size > 0) {
// JS:         throw new Error("Not all rpcTargets were accounted for in disposeImpl()?");
// JS:       }
// JS:     } else {
// JS:       // this.source is "params". We don't own the stubs within.
// JS:     }
// JS:
// JS:     // Make dispose() idempotent.
// JS:     this.source = "owned";
// JS:     this.hooks = [];
// JS:     this.promises = [];
// JS:   }
// JS:
// JS:   // Recursive dispose, called only when `source` is "return".
// JS:   private disposeImpl(value: unknown, parent: object | undefined) {
// JS:     let kind = typeForRpc(value);
// JS:     switch (kind) {
// JS:       case "unsupported":
// JS:       case "primitive":
// JS:       case "bigint":
// JS:       case "bytes":
// JS:       case "blob":
// JS:       case "date":
// JS:       case "url":
// JS:       case "error":
// JS:       case "undefined":
// JS:         return;
// JS:
// JS:       case "array": {
// JS:         let array = <Array<unknown>>value;
// JS:         let len = array.length;
// JS:         for (let i = 0; i < len; i++) {
// JS:           this.disposeImpl(array[i], array);
// JS:         }
// JS:         return;
// JS:       }
// JS:
// JS:       case "object": {
// JS:         let object = <Record<string, unknown>>value;
// JS:         for (let i in object) {
// JS:           this.disposeImpl(object[i], object);
// JS:         }
// JS:         return;
// JS:       }
// JS:
// JS:       case "stub":
// JS:       case "rpc-promise": {
// JS:         let stub = <RpcStub>value;
// JS:         let hook = unwrapStubNoProperties(stub);
// JS:         if (hook) {
// JS:           hook.dispose();
// JS:         }
// JS:         return;
// JS:       }
// JS:
// JS:       case "function":
// JS:       case "rpc-target": {
// JS:         let target = <RpcTarget | Function>value;
// JS:         let hook = this.rpcTargets?.get(target);
// JS:         if (hook) {
// JS:           // We created a hook around this target earlier. Dispose it now.
// JS:           hook.dispose();
// JS:           this.rpcTargets!.delete(target);
// JS:         } else {
// JS:           // There never was a stub pointing at this target. This could be because:
// JS:           // * The call was used only for promise pipelining, so the result was never serialized,
// JS:           //   so it never got added to `rpcTargets`.
// JS:           // * The same RpcTarget appears in the results twice, and we already disposed the hook
// JS:           //   when we saw it earlier. Note that it's intentional that we should call the disposer
// JS:           //   twice if the same object appears twice.
// JS:           disposeRpcTarget(target);
// JS:         }
// JS:         return;
// JS:       }
// JS:
// JS:       case "rpc-thenable":
// JS:         // Since thenables are promises, we don't own them, so we don't dispose them.
// JS:         return;
// JS:
// JS:       case "headers":
// JS:         // Headers have no owned resources to dispose.
// JS:         return;
// JS:
// JS:       case "request": {
// JS:         // The body may be a ReadableStream that has an associated hook in rpcTargets.
// JS:         let req = <Request>value;
// JS:         if (req.body) this.disposeImpl(req.body, req);
// JS:         // TODO: When we support AbortSignal, we may need to dispose request.signal here?
// JS:         return;
// JS:       }
// JS:
// JS:       case "response": {
// JS:         // The body may be a ReadableStream that has an associated hook in rpcTargets.
// JS:         let resp = <Response>value;
// JS:         if (resp.body) this.disposeImpl(resp.body, resp);
// JS:         // TODO: When we support WebSocket, we may need to dispose response.webSocket here?
// JS:         return;
// JS:       }
// JS:
// JS:       case "writable": {
// JS:         let stream = <WritableStream>value;
// JS:         let hook = this.rpcTargets?.get(stream);
// JS:         if (hook) {
// JS:           this.rpcTargets!.delete(stream);
// JS:         } else {
// JS:           // Create a hook just so we can call its disposer for consistent behavior, which will
// JS:           // abort the stream.
// JS:           hook = streamImpl.createWritableStreamHook(stream);
// JS:         }
// JS:
// JS:         hook.dispose();
// JS:
// JS:         return;
// JS:       }
// JS:
// JS:       case "readable": {
// JS:         let stream = <ReadableStream>value;
// JS:         let hook = this.rpcTargets?.get(stream);
// JS:         if (hook) {
// JS:           this.rpcTargets!.delete(stream);
// JS:         } else {
// JS:           // Create a hook just so we can call its disposer for consistent behavior, which will
// JS:           // cancel the stream.
// JS:           hook = streamImpl.createReadableStreamHook(stream);
// JS:         }
// JS:
// JS:         hook.dispose();
// JS:
// JS:         return;
// JS:       }
// JS:
// JS:       default:
// JS:         kind satisfies never;
// JS:         return;
// JS:     }
// JS:   }
// JS:
// JS:   // Ignore unhandled rejections in all promises in this payload -- that is, all promises that
// JS:   // *would* be awaited if this payload were to be delivered. See the similarly-named method of
// JS:   // StubHook for explanation.
// JS:   ignoreUnhandledRejections(): void {
// JS:     if (this.hooks) {
// JS:       // Propagate to all stubs and promises.
// JS:       this.hooks.forEach(hook => {
// JS:         hook.ignoreUnhandledRejections();
// JS:       });
// JS:       this.promises!.forEach(
// JS:           promise => unwrapStubOrParent(promise.promise).ignoreUnhandledRejections());
// JS:     } else {
// JS:       // Ugh we have to walk the tree.
// JS:       this.ignoreUnhandledRejectionsImpl(this.value);
// JS:     }
// JS:   }
// JS:
// JS:   private ignoreUnhandledRejectionsImpl(value: unknown) {
// JS:     let kind = typeForRpc(value);
// JS:     switch (kind) {
// JS:       case "unsupported":
// JS:       case "primitive":
// JS:       case "bigint":
// JS:       case "bytes":
// JS:       case "blob":
// JS:       case "date":
// JS:       case "error":
// JS:       case "undefined":
// JS:       case "function":
// JS:       case "rpc-target":
// JS:       case "writable":
// JS:       case "readable":
// JS:       case "url":
// JS:       case "headers":
// JS:       case "request":
// JS:       case "response":
// JS:         return;
// JS:
// JS:       case "array": {
// JS:         let array = <Array<unknown>>value;
// JS:         let len = array.length;
// JS:         for (let i = 0; i < len; i++) {
// JS:           this.ignoreUnhandledRejectionsImpl(array[i]);
// JS:         }
// JS:         return;
// JS:       }
// JS:
// JS:       case "object": {
// JS:         let object = <Record<string, unknown>>value;
// JS:         for (let i in object) {
// JS:           this.ignoreUnhandledRejectionsImpl(object[i]);
// JS:         }
// JS:         return;
// JS:       }
// JS:
// JS:       case "stub":
// JS:       case "rpc-promise":
// JS:         unwrapStubOrParent(<RpcStub>value).ignoreUnhandledRejections();
// JS:         return;
// JS:
// JS:       case "rpc-thenable":
// JS:         (<any>value).then((_: any) => {}, (_: any) => {});
// JS:         return;
// JS:
// JS:       default:
// JS:         kind satisfies never;
// JS:         return;
// JS:     }
// JS:   }
// JS: };
// JS:
// JS: // =======================================================================================
// JS: // Local StubHook implementations
// JS:
// JS: // Result of followPath().
// JS: type FollowPathResult = {
// JS:   // Path led to a regular value.
// JS:
// JS:   value: unknown,              // the value
// JS:   parent: object | undefined,  // the immediate parent (useful as `this` if making a call)
// JS:   owner: RpcPayload | null,    // RpcPayload that owns the value, if any
// JS:
// JS:   hook?: never,
// JS:   remainingPath?: never,
// JS: } | {
// JS:   // Path leads into another stub, which needs to be called recursively.
// JS:
// JS:   hook: StubHook,               // StubHook of the inner stub.
// JS:   remainingPath: PropertyPath,  // Path to pass to `hook` when recursing.
// JS:
// JS:   value?: never,
// JS:   parent?: never,
// JS:   owner?: never,
// JS: };
// JS:
// JS: function followPath(value: unknown, parent: object | undefined,
// JS:                     path: PropertyPath, owner: RpcPayload | null): FollowPathResult {
// JS:   for (let i = 0; i < path.length; i++) {
// JS:     parent = <object>value;
// JS:
// JS:     let part = path[i];
// JS:     if (part in Object.prototype) {
// JS:       // Don't allow messing with Object.prototype properties over RPC. We block these even if
// JS:       // the specific object has overridden them for consistency with the deserialization code,
// JS:       // which will refuse to deserialize an object containing such properties. Anyway, it's
// JS:       // impossible for a normal client to even request these because accessing Object prototype
// JS:       // properties on a stub will resolve to the local prototype property, not making an RPC at
// JS:       // all.
// JS:       value = undefined;
// JS:       continue;
// JS:     }
// JS:
// JS:     let kind = typeForRpc(value);
// JS:     switch (kind) {
// JS:       case "object":
// JS:       case "function":
// JS:         // Must be own property, NOT inherited from a prototype.
// JS:         if (Object.hasOwn(<object>value, part)) {
// JS:           value = (<any>value)[part];
// JS:         } else {
// JS:           value = undefined;
// JS:         }
// JS:         break;
// JS:
// JS:       case "array":
// JS:         // For arrays, restrict specifically to numeric indexes, to be consistent with
// JS:         // serialization, which only sends a flat list.
// JS:         if (Number.isInteger(part) && <number>part >= 0) {
// JS:           value = (<any>value)[part];
// JS:         } else {
// JS:           value = undefined;
// JS:         }
// JS:         break;
// JS:
// JS:       case "rpc-target":
// JS:       case "rpc-thenable": {
// JS:         // Must be prototype property, and must NOT be inherited from `Object`.
// JS:         if (Object.hasOwn(<object>value, part)) {
// JS:           // We throw an error in this case, rather than return undefined, because otherwise
// JS:           // people tend to get confused about this. If you don't want it to be possible to
// JS:           // probe the existence of your instance properties, make them properly private (prefix
// JS:           // with #).
// JS:           throw new TypeError(
// JS:               `Attempted to access property '${part}', which is an instance property of the ` +
// JS:               `RpcTarget. To avoid leaking private internals, instance properties cannot be ` +
// JS:               `accessed over RPC. If you want to make this property available over RPC, define ` +
// JS:               `it as a method or getter on the class, instead of an instance property.`);
// JS:         } else {
// JS:           value = (<any>value)[part];
// JS:         }
// JS:
// JS:         // Since we're descending into the RpcTarget, the rest of the path is not "owned" by any
// JS:         // RpcPayload.
// JS:         owner = null;
// JS:         break;
// JS:       }
// JS:
// JS:       case "stub":
// JS:       case "rpc-promise": {
// JS:         let {hook: hook, pathIfPromise} = unwrapStubAndPath(<RpcStub>value);
// JS:         return { hook, remainingPath:
// JS:             pathIfPromise ? pathIfPromise.concat(path.slice(i)) : path.slice(i) };
// JS:       }
// JS:
// JS:       case "writable":
// JS:         // TODO: How do we pipeline on WritableStream? We can't expose the literal WritableStream
// JS:         //   interface because the caller would call getWriter() which would conflict with the
// JS:         //   RPC system calling it later. Perhaps the caller needs to somehow indicate, on the
// JS:         //   client side, "this pipelined property is expected to be a WritableStream", and then
// JS:         //   we can give them a WritableStream, and somehow this correctly pipelines... idk.
// JS:         value = undefined;
// JS:         break;
// JS:
// JS:       case "readable":
// JS:         // TODO: Do we want to support pipelining on ReadableStream at all? It doesn't seem like
// JS:         //   it really makes sense... you might as well just wait for the promise for the
// JS:         //   ReadableStream to resolve, and then read it, because you'll get bytes just as fast.
// JS:         value = undefined;
// JS:         break;
// JS:
// JS:       case "primitive":
// JS:       case "bigint":
// JS:       case "bytes":
// JS:       case "blob":
// JS:       case "date":
// JS:       case "error":
// JS:       case "url":
// JS:       case "headers":
// JS:       case "request":
// JS:       case "response":
// JS:         // These have no properties that can be accessed remotely.
// JS:         value = undefined;
// JS:         break;
// JS:
// JS:       case "undefined":
// JS:         // Intentionally produce TypeError.
// JS:         value = (value as any)[part];
// JS:         break;
// JS:
// JS:       case "unsupported": {
// JS:         if (i === 0) {
// JS:           throw new TypeError(`RPC stub points at a non-serializable type.`);
// JS:         } else {
// JS:           let prefix = path.slice(0, i).join(".");
// JS:           let remainder = path.slice(0, i).join(".");
// JS:           throw new TypeError(
// JS:               `'${prefix}' is not a serializable type, so property ${remainder} cannot ` +
// JS:               `be accessed.`);
// JS:         }
// JS:       }
// JS:
// JS:       default:
// JS:         kind satisfies never;
// JS:         throw new TypeError("unreachable");
// JS:     }
// JS:   }
// JS:
// JS:   // If we reached a promise, we actually want the caller to forward to the promise, not return
// JS:   // the promise itself.
// JS:   if (value instanceof RpcPromise) {
// JS:     let {hook: hook, pathIfPromise} = unwrapStubAndPath(<RpcStub>value);
// JS:     return { hook, remainingPath: pathIfPromise || [] };
// JS:   }
// JS:
// JS:   // We don't validate the final value itself because we don't know the intended use yet. If it's
// JS:   // for a call, any callable is valid. If it's for get(), then any serializable value is valid.
// JS:   return {
// JS:     value,
// JS:     parent,
// JS:     owner,
// JS:   };
// JS: }
// JS:
// JS: // Shared base class for PayloadStubHook and TargetStubHook.
// JS: abstract class ValueStubHook extends StubHook {
// JS:   protected abstract getValue(): {value: unknown, owner: RpcPayload | null};
// JS:
// JS:   call(path: PropertyPath, args: RpcPayload): StubHook {
// JS:     let followResult: FollowPathResult;
// JS:     try {
// JS:       let {value, owner} = this.getValue();
// JS:       followResult = followPath(value, undefined, path, owner);
// JS:     } catch (err) {
// JS:       // We took ownership of `args`, and there is no callee left to consume them.
// JS:       args.dispose();
// JS:       return new ErrorStubHook(err);
// JS:     }
// JS:
// JS:     if (followResult.hook) {
// JS:       // The delegate's call() takes ownership of `args`, per the StubHook contract.
// JS:       return followResult.hook.call(followResult.remainingPath, args);
// JS:     }
// JS:
// JS:     // It's a local function.
// JS:     if (typeof followResult.value != "function") {
// JS:       args.dispose();
// JS:       return new ErrorStubHook(new TypeError(`'${path.join('.')}' is not a function.`));
// JS:     }
// JS:
// JS:     // deliverCall() is async and disposes `args` itself when the call completes, so it never
// JS:     // needs a guard here.
// JS:     let promise = args.deliverCall(followResult.value, followResult.parent);
// JS:     return new PromiseStubHook(promise.then(payload => {
// JS:       return new PayloadStubHook(payload);
// JS:     }));
// JS:   }
// JS:
// JS:   map(path: PropertyPath, captures: StubHook[], instructions: unknown[]): StubHook {
// JS:     try {
// JS:       let followResult: FollowPathResult;
// JS:       try {
// JS:         let {value, owner} = this.getValue();
// JS:         followResult = followPath(value, undefined, path, owner);
// JS:       } catch (err) {
// JS:         // We took ownership of the captures, and there is no callee left to consume them.
// JS:         for (let cap of captures) {
// JS:           cap.dispose();
// JS:         }
// JS:         throw err;
// JS:       }
// JS:
// JS:       if (followResult.hook) {
// JS:         return followResult.hook.map(followResult.remainingPath, captures, instructions);
// JS:       }
// JS:
// JS:       return mapImpl.applyMap(
// JS:           followResult.value, followResult.parent, followResult.owner, captures, instructions);
// JS:     } catch (err) {
// JS:       // Past the inner catch, ownership of the captures has been transferred to the delegate's
// JS:       // map() or to applyMap(), so it would be incorrect to dispose them here.
// JS:       return new ErrorStubHook(err);
// JS:     }
// JS:   }
// JS:
// JS:   get(path: PropertyPath): StubHook {
// JS:     try {
// JS:       let {value, owner} = this.getValue();
// JS:
// JS:       if (path.length === 0 && owner === null) {
// JS:         if (value instanceof Object && "then" in value) {
// JS:           // The hook wraps a thenable (e.g. a workerd-native RpcPromise or RpcProperty), so it
// JS:           // really does back a promise. get([]) asks for an independent hook aliasing the same
// JS:           // promise, which is exactly dup().
// JS:           return this.dup();
// JS:         }
// JS:
// JS:         // The only way this happens is if someone sends "pipeline" and references a
// JS:         // TargetStubHook wrapping a non-thenable, but they shouldn't do that, because such a
// JS:         // hook never backs a promise, and a non-promise cannot be converted to a promise.
// JS:         throw new Error("Can't dup an RpcTarget stub as a promise.");
// JS:       }
// JS:
// JS:       let followResult = followPath(value, undefined, path, owner);
// JS:
// JS:       if (followResult.hook) {
// JS:         return followResult.hook.get(followResult.remainingPath);
// JS:       }
// JS:
// JS:       // Note that if `followResult.owner` is null, then we've descended into the contents of an
// JS:       // RpcTarget. In that case, if this deep copy discovers an RpcTarget embedded in the result,
// JS:       // it will create a new stub for it. If that RpcTarget has a disposer, it'll be disposed when
// JS:       // that stub is disposed. If the same RpcTarget is returned in *another* get(), it create
// JS:       // *another* stub, which calls the disposer *another* time. This can be quite weird -- the
// JS:       // disposer may be called any number of times, including zero if the property is never read
// JS:       // at all. Unfortunately, that's just the way it is. The application can avoid this problem by
// JS:       // wrapping the RpcTarget in an RpcStub itself, proactively, and using that as the property --
// JS:       // then, each time the property is get()ed, a dup() of that stub is returned.
// JS:       return new PayloadStubHook(RpcPayload.deepCopyFrom(
// JS:           followResult.value, followResult.parent, followResult.owner));
// JS:     } catch (err) {
// JS:       return new ErrorStubHook(err);
// JS:     }
// JS:   }
// JS: }
// JS:
// JS: // StubHook wrapping an RpcPayload in local memory.
// JS: //
// JS: // This is used for:
// JS: // - Resolution of a promise.
// JS: //   - Initially on the server side, where it can be pull()ed and used in pipelining.
// JS: //   - On the client side, after pull() has transmitted the payload.
// JS: // - Implementing RpcTargets, on the server side.
// JS: //   - Since the payload's root is an RpcTarget, pull()ing it will just duplicate the stub.
// JS: export class PayloadStubHook extends ValueStubHook {
// JS:   constructor(payload: RpcPayload) {
// JS:     super();
// JS:     this.payload = payload;
// JS:   }
// JS:
// JS:   private payload?: RpcPayload;  // cleared when disposed
// JS:
// JS:   private getPayload(): RpcPayload {
// JS:     if (this.payload) {
// JS:       return this.payload;
// JS:     } else {
// JS:       throw new Error("Attempted to use an RPC StubHook after it was disposed.");
// JS:     }
// JS:   }
// JS:
// JS:   protected getValue() {
// JS:     let payload = this.getPayload();
// JS:     return {value: payload.value, owner: payload};
// JS:   }
// JS:
// JS:   dup(): StubHook {
// JS:     // Although dup() is documented as not copying the payload, what this really means is that
// JS:     // you aren't expected to be able to pull() from a dup()ed hook if it is remote. However,
// JS:     // PayloadStubHook already has the value locally, and there's nothing we can do except clone
// JS:     // it here.
// JS:     //
// JS:     // TODO: Should we prohibit pull()ing from the clone? The fact that it'll be wrapped as
// JS:     //   RpcStub instead of RpcPromise should already prevent this...
// JS:     let thisPayload = this.getPayload();
// JS:     return new PayloadStubHook(RpcPayload.deepCopyFrom(
// JS:         thisPayload.value, undefined, thisPayload));
// JS:   }
// JS:
// JS:   pull(): RpcPayload | Promise<RpcPayload> {
// JS:     // Reminder: pull() intentionally returns the hook's own payload and not a clone. The caller
// JS:     // only needs to dispose one of the hook or the payload. It is the caller's responsibility
// JS:     // to not dispose the payload if they intend to keep the hook around.
// JS:     return this.getPayload();
// JS:   }
// JS:
// JS:   ignoreUnhandledRejections(): void {
// JS:     if (this.payload) {
// JS:       this.payload.ignoreUnhandledRejections();
// JS:     }
// JS:   }
// JS:
// JS:   dispose(): void {
// JS:     if (this.payload) {
// JS:       this.payload.dispose();
// JS:       this.payload = undefined;
// JS:     }
// JS:   }
// JS:
// JS:   onBroken(callback: (error: any) => void): void {
// JS:     if (this.payload) {
// JS:       if (this.payload.value instanceof RpcStub) {
// JS:         // Payload is a single stub, we should forward onRpcBroken to it.
// JS:         // TODO: Consider prohibiting PayloadStubHook created around a single stub; should always
// JS:         //   use the underlying stub's hook instead?
// JS:         this.payload.value.onRpcBroken(callback);
// JS:       }
// JS:
// JS:       // TODO: Should native stubs be able to implement onRpcBroken?
// JS:     }
// JS:   }
// JS: }
// JS:
// JS: function disposeRpcTarget(target: RpcTarget | Function) {
// JS:   if (Symbol.dispose in target) {
// JS:     try {
// JS:       ((<Disposable><any>target)[Symbol.dispose])();
// JS:     } catch (err) {
// JS:       // We don't actually want to throw from dispose() as this will create trouble for
// JS:       // the RPC state machine. Instead, treat the application's error as an unhandled
// JS:       // rejection.
// JS:       Promise.reject(err);
// JS:     }
// JS:   }
// JS: }
// JS:
// JS: // Many TargetStubHooks could point at the same RpcTarget. We store a refcount in a separate
// JS: // object that they all share.
// JS: //
// JS: // We can't store the refcount on the RpcTarget itself because if the application chooses to pass
// JS: // the same RpcTarget into the RPC system multiple times, we need to call this disposer multiple
// JS: // times for consistency.
// JS: type BoxedRefcount = { count: number };
// JS:
// JS: // StubHook which wraps an RpcTarget. This has similarities to PayloadStubHook (especially when
// JS: // the root of the payload happens to be an RpcTarget), but there can only be one RpcPayload
// JS: // pointing at an RpcTarget whereas there can be several TargetStubHooks pointing at it. Also,
// JS: // TargetStubHook cannot be pull()ed, because it always backs an RpcStub, not an RpcPromise.
// JS: class TargetStubHook extends ValueStubHook {
// JS:   // Constructs a TargetStubHook that is not duplicated from an existing hook.
// JS:   //
// JS:   // If `value` is a function, `parent` is bound as its "this".
// JS:   static create(value: RpcTarget | Function, parent: object | undefined) {
// JS:     if (typeof value !== "function") {
// JS:       // If the target isn't callable, we don't need to pass a `this` to it, so drop `parent`.
// JS:       // NOTE: `typeof value === "function"` checks if the value is callable. This technically
// JS:       //   works even for `RpcTarget` implementations that are callable, not just plain functions.
// JS:       parent = undefined;
// JS:     }
// JS:     return new TargetStubHook(value, parent);
// JS:   }
// JS:
// JS:   private constructor(target: RpcTarget | Function,
// JS:                       parent?: object | undefined,
// JS:                       dupFrom?: TargetStubHook) {
// JS:     super();
// JS:     this.target = target;
// JS:     this.parent = parent;
// JS:     if (dupFrom) {
// JS:       if (dupFrom.refcount) {
// JS:         this.refcount = dupFrom.refcount;
// JS:         ++this.refcount.count;
// JS:       }
// JS:     } else if (Symbol.dispose in target) {
// JS:       // Disposer present, so we need to refcount.
// JS:       this.refcount = {count: 1};
// JS:     }
// JS:   }
// JS:
// JS:   private target?: RpcTarget | Function;  // cleared when disposed
// JS:   private parent?: object | undefined;  // `this` parameter when calling `target`
// JS:   private refcount?: BoxedRefcount;  // undefined if not needed (because target has no disposer)
// JS:
// JS:   private getTarget(): RpcTarget | Function {
// JS:     if (this.target) {
// JS:       return this.target;
// JS:     } else {
// JS:       throw new Error("Attempted to use an RPC StubHook after it was disposed.");
// JS:     }
// JS:   }
// JS:
// JS:   protected getValue() {
// JS:     return {value: this.getTarget(), owner: null};
// JS:   }
// JS:
// JS:   dup(): StubHook {
// JS:     return new TargetStubHook(this.getTarget(), this.parent, this);
// JS:   }
// JS:
// JS:   pull(): RpcPayload | Promise<RpcPayload> {
// JS:     let target = this.getTarget();
// JS:     if ("then" in target) {
// JS:       // If the target is itself thenable, we allow it to be treated as a promise. This is used
// JS:       // in particular to support wrapping a workerd-native RpcPromise or RpcProperty.
// JS:       return Promise.resolve(target).then(resolution => {
// JS:         return RpcPayload.fromAppReturn(resolution);
// JS:       });
// JS:     } else {
// JS:       // This shouldn't be called since RpcTarget always becomes RpcStub, not RpcPromise, and you
// JS:       // can only pull a promise.
// JS:       return Promise.reject(new Error("Tried to resolve a non-promise stub."));
// JS:     }
// JS:   }
// JS:
// JS:   ignoreUnhandledRejections(): void {
// JS:     // Nothing to do.
// JS:   }
// JS:
// JS:   dispose(): void {
// JS:     if (this.target) {
// JS:       if (this.refcount) {
// JS:         if (--this.refcount.count == 0) {
// JS:           disposeRpcTarget(this.target);
// JS:         }
// JS:       }
// JS:
// JS:       this.target = undefined;
// JS:     }
// JS:   }
// JS:
// JS:   onBroken(callback: (error: any) => void): void {
// JS:     let target = this.target;
// JS:     if (target && "then" in target) {
// JS:       // The target is thenable (e.g. a workerd-native RpcPromise), so it backs a promise, which
// JS:       // becomes broken if it rejects.
// JS:       Promise.resolve(target).then(() => {}, callback);
// JS:     }
// JS:     // TODO: Should non-thenable RpcTargets be able to implement onRpcBroken?
// JS:   }
// JS: }
// JS:
// JS: // StubHook derived from a Promise for some other StubHook. Waits for the promise and then
// JS: // forward calls, being careful to honor e-order.
// JS: export class PromiseStubHook extends StubHook {
// JS:   private promise: Promise<StubHook>;
// JS:   private resolution: StubHook | undefined;
// JS:
// JS:   constructor(promise: Promise<StubHook>) {
// JS:     super();
// JS:
// JS:     this.promise = promise.then(res => { this.resolution = res; return res; });
// JS:   }
// JS:
// JS:   call(path: PropertyPath, args: RpcPayload): StubHook {
// JS:     // Note: We can't use `resolution` even if it's available because it could technically break
// JS:     //   e-order: A call() that arrives just after the resolution could be delivered faster than
// JS:     //   a call() that arrives just before. Keeping the promise around and always waiting on it
// JS:     //   avoids the problem.
// JS:     // TODO: Is there a way around this?
// JS:
// JS:     // Once call() returns (synchronously), we can no longer touch the original args. Since we
// JS:     // can't serialize them yet, we have to deep-copy them now.
// JS:     args.ensureDeepCopied();
// JS:
// JS:     return new PromiseStubHook(this.promise.then(
// JS:         hook => hook.call(path, args),
// JS:         err => {
// JS:           // The backing promise rejected, so no callee ever existed to take ownership of `args`.
// JS:           args.dispose();
// JS:           throw err;
// JS:         }));
// JS:   }
// JS:
// JS:   stream(path: PropertyPath, args: RpcPayload): {promise: Promise<void>, size?: number} {
// JS:     // Not yet resolved — we don't know if this will be local or remote. Deep-copy args and wait.
// JS:     // No size is returned because we can't know yet; this means the caller will await the promise,
// JS:     // which is the safe default (serialized writes).
// JS:     args.ensureDeepCopied();
// JS:     let promise = this.promise.then(
// JS:         hook => hook.stream(path, args).promise,
// JS:         err => {
// JS:           // The backing promise rejected, so no callee ever existed to take ownership of `args`.
// JS:           args.dispose();
// JS:           throw err;
// JS:         });
// JS:     return { promise };
// JS:   }
// JS:
// JS:   map(path: PropertyPath, captures: StubHook[], instructions: unknown[]): StubHook {
// JS:     return new PromiseStubHook(this.promise.then(
// JS:         hook => hook.map(path, captures, instructions),
// JS:         err => {
// JS:           // The backing promise rejected, so no callee ever existed to take ownership of the
// JS:           // captures.
// JS:           for (let cap of captures) {
// JS:             cap.dispose();
// JS:           }
// JS:           throw err;
// JS:         }));
// JS:   }
// JS:
// JS:   get(path: PropertyPath): StubHook {
// JS:     // Note: e-order matters for get(), just like call(), in case the property has a getter.
// JS:     return new PromiseStubHook(this.promise.then(hook => hook.get(path)));
// JS:   }
// JS:
// JS:   dup(): StubHook {
// JS:     if (this.resolution) {
// JS:       return this.resolution.dup();
// JS:     } else {
// JS:       return new PromiseStubHook(this.promise.then(hook => hook.dup()));
// JS:     }
// JS:   }
// JS:
// JS:   pull(): RpcPayload | Promise<RpcPayload> {
// JS:     // Luckily, resolutions are not subject to e-order, so it's safe to use `this.resolution`
// JS:     // here. In fact, it is required to maintain e-order elsewhere: If this promise is being used
// JS:     // as the input to some other local call (via promise pipelining), we need to make sure that
// JS:     // other call is not delayed at all when this promise is already resolved.
// JS:     if (this.resolution) {
// JS:       return this.resolution.pull();
// JS:     } else {
// JS:       return this.promise.then(hook => hook.pull());
// JS:     }
// JS:   }
// JS:
// JS:   ignoreUnhandledRejections(): void {
// JS:     if (this.resolution) {
// JS:       this.resolution.ignoreUnhandledRejections();
// JS:     } else {
// JS:       this.promise.then(res => {
// JS:         res.ignoreUnhandledRejections();
// JS:       }, err => {
// JS:         // Ignore the error!
// JS:       });
// JS:     }
// JS:   }
// JS:
// JS:   dispose(): void {
// JS:     // We can't take the fast path here when this.resolution is available because if the resolution
// JS:     // was just recently delivered, promise waiters may not have had a chance to consume it yet,
// JS:     // and disposing it would preempt them. If we always wait on `this.promise` then we'll
// JS:     // run after anything that was queued before us.
// JS:     this.promise.then(hook => hook.dispose(), () => {});
// JS:   }
// JS:
// JS:   onBroken(callback: (error: any) => void): void {
// JS:     if (this.resolution) {
// JS:       this.resolution.onBroken(callback);
// JS:     } else {
// JS:       this.promise.then(hook => {
// JS:         hook.onBroken(callback);
// JS:       }, callback);
// JS:     }
// JS:   }
// JS: }
