// JS: // Copyright (c) 2025 Cloudflare, Inc.
// JS: // Licensed under the MIT license found in the LICENSE.txt file or at:
// JS: //     https://opensource.org/license/mit
// JS:
// JS: import { StubHook, RpcPayload, RpcStub, PropertyPath, PayloadStubHook, ErrorStubHook, RpcTarget, unwrapStubAndPath, streamImpl } from "./core.js";
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::core::{
    CapnwebError, CapnwebValue, Error, ErrorStubHook, MapInstructions, MaybePromise,
    PayloadStubHook, PropertyPath, Result, RpcPayload, RpcStub, StreamCall, StubHook,
    StubHookInner, unwrap_stub_and_path,
};
// JS: import { Devaluator, Evaluator, ExportId, ImportId, Exporter, Importer, serialize, EncodingLevel, RpcLimits, DEFAULT_LIMITS } from "./serialize.js";
use crate::eager_promise::{EagerPromise, PromiseWithResolvers};
use crate::serialize::{
    DEFAULT_LIMITS, Devaluator, EncodingLevel, Evaluator, ExportId, Exporter, ImportId, Importer,
    RpcLimits, capnweb_to_json, json_to_capnweb,
};
// JS:
// JS: /**
// JS:  * Interface for a string-based RPC transport. This is the default transport type — no
// JS:  * `encodingLevel` field is needed. Messages are JSON strings. Implement this interface if the
// JS:  * built-in transports (e.g. for HTTP batch and WebSocket) don't meet your needs.
// JS:  */
// JS: export interface RpcTransport {
// JS:   /**
// JS:    * The encoding level this transport works with. For this interface it is always "string";
// JS:    * it may be omitted. (See `RpcTransportWithCustomEncoding` for the other levels.)
// JS:    */
// JS:   readonly encodingLevel?: "string";
// JS:
// JS:   /**
// JS:    * Sends a message to the other end. May optionally return a promise; if the promise rejects,
// JS:    * the session is aborted.
// JS:    */
// JS:   send(message: string): void | Promise<void>;
// JS:
// JS:   /**
// JS:    * Receives a message sent by the other end.
// JS:    *
// JS:    * If and when the transport becomes disconnected, this will reject. The thrown error will be
// JS:    * propagated to all outstanding calls and future calls on any stubs associated with the session.
// JS:    * If there are no outstanding calls (and none are made in the future), then the error does not
// JS:    * propagate anywhere -- this is considered a "clean" shutdown.
// JS:    */
// JS:   receive(): Promise<string>;
// JS:
// JS:   /**
// JS:    * Indicates that the RPC system has suffered an error that prevents the session from continuing.
// JS:    * The transport should ideally try to send any queued messages if it can, and then close the
// JS:    * connection. (It's not strictly necessary to deliver queued messages, but the last message sent
// JS:    * before abort() is called is often an "abort" message, which communicates the error to the
// JS:    * peer, so if that is dropped, the peer may have less information about what happened.)
// JS:    */
// JS:   abort?(reason: any): void;
// JS: }
// JS:
// JS: /**
// JS:  * Interface for a transport that receives partially encoded JS values instead of JSON strings.
// JS:  * The selected `encodingLevel` describes what the transport can assume about message values.
// JS:  */
// JS: export interface RpcTransportWithCustomEncoding {
// JS:   /**
// JS:    * The encoding level this transport works with.
// JS:    *
// JS:    * - "jsonCompatible": JSON-compatible JS value tree; transport handles final serialization.
// JS:    * - "jsonCompatibleWithBytes": Like "jsonCompatible" but Uint8Array values are left raw.
// JS:    * - "structuredClonable": Structured-clonable native values pass through where possible.
// JS:    */
// JS:   readonly encodingLevel: "jsonCompatible" | "jsonCompatibleWithBytes" | "structuredClonable";
// JS:
// JS:   /**
// JS:    * Encodes and sends a message to the other end. Returns the encoded byte size if known.
// JS:    * If the size is unavailable, return void; Cap'n Web will estimate stream message sizes for
// JS:    * flow control. Send errors should be propagated via `receive()` rejecting.
// JS:    */
// JS:   send(message: unknown): number | void;
// JS:
// JS:   /**
// JS:    * Receives and decodes a message sent by the other end.
// JS:    *
// JS:    * If and when the transport becomes disconnected, this will reject. The thrown error will be
// JS:    * propagated to all outstanding calls and future calls on any stubs associated with the session.
// JS:    * If there are no outstanding calls (and none are made in the future), then the error does not
// JS:    * propagate anywhere -- this is considered a "clean" shutdown.
// JS:    */
// JS:   receive(): Promise<unknown>;
// JS:
// JS:   /**
// JS:    * Indicates that the RPC system has suffered an error that prevents the session from continuing.
// JS:    * The transport should ideally try to send any queued messages if it can, and then close the
// JS:    * connection. (It's not strictly necessary to deliver queued messages, but the last message sent
// JS:    * before abort() is called is often an "abort" message, which communicates the error to the
// JS:    * peer, so if that is dropped, the peer may have less information about what happened.)
// JS:    */
// JS:   abort?(reason: any): void;
// JS: }
// JS:
// JS: /** Any supported transport type. */
// JS: export type AnyRpcTransport = RpcTransport | RpcTransportWithCustomEncoding;
/// A message at the boundary selected by the transport's encoding level.
pub enum TransportMessage {
    /// Fully JSON-stringified transport data.
    Text(String),
    /// A partially encoded Cap'n Web value tree.
    Value(CapnwebValue),
}

/// Result of starting a transport send.
pub enum TransportSend {
    /// The send completed synchronously; the optional value is the encoded byte size.
    Done(Option<usize>),
    /// The send continues asynchronously. String transports may use this form.
    Pending(Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>),
}

/// Object-safe union of the string and custom-encoding transport interfaces.
pub trait AnyRpcTransport: Send + Sync + 'static {
    /// Returns this transport's encoding level.
    fn encoding_level(&self) -> EncodingLevel {
        EncodingLevel::String
    }

    /// Sends one message and optionally reports its encoded byte size.
    fn send(&self, message: TransportMessage) -> Result<TransportSend>;

    /// Receives one message.
    fn receive(
        self: Arc<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<TransportMessage>> + Send + 'static>>;

    /// Aborts the underlying transport.
    fn abort(&self, _reason: Error) {}
}
// JS:
// JS: const ESTIMATED_OBJECT_OVERHEAD = 16;
const ESTIMATED_OBJECT_OVERHEAD: usize = 16;
// JS: const ESTIMATED_ENTRY_OVERHEAD = 8;
const ESTIMATED_ENTRY_OVERHEAD: usize = 8;
// JS: const ESTIMATED_BINARY_OVERHEAD = 16;
const ESTIMATED_BINARY_OVERHEAD: usize = 16;
// JS: const MAX_ESTIMATE_DEPTH = 64;
const MAX_ESTIMATE_DEPTH: usize = 64;

fn path_to_value(path: PropertyPath) -> CapnwebValue {
    CapnwebValue::Array(
        path.into_iter()
            .map(|part| match part {
                crate::core::PropertyKey::String(value) => CapnwebValue::String(value),
                crate::core::PropertyKey::Number(value) => CapnwebValue::Number(value),
            })
            .collect(),
    )
}

fn wire_integer(value: &CapnwebValue) -> Option<i64> {
    let CapnwebValue::Number(value) = value else {
        return None;
    };
    if value.is_finite()
        && value.fract() == 0.0
        && *value >= i64::MIN as f64
        && *value <= i64::MAX as f64
    {
        Some(*value as i64)
    } else {
        None
    }
}

fn value_to_error(value: CapnwebValue) -> Error {
    match value {
        CapnwebValue::Error(error) => Error::Message(format!("{}: {}", error.name, error.message)),
        CapnwebValue::String(message) => Error::Message(message),
        value => Error::Message(format!("RPC rejection: {value:?}")),
    }
}

fn tagged(tag: &str, values: impl IntoIterator<Item = CapnwebValue>) -> CapnwebValue {
    let mut result = vec![CapnwebValue::String(tag.to_owned())];
    result.extend(values);
    CapnwebValue::Array(result)
}
// JS:
// JS: function estimateStringSize(value: string): number {
fn estimate_string_size(value: &str) -> usize {
    // JS:   // Bias high. UTF-8 uses up to 3 bytes for BMP code points, and surrogate pairs are 4 bytes for
    // JS:   // 2 UTF-16 code units.
    // JS:   return 2 + value.length * 3;
    2 + value.encode_utf16().count() * 3
    // JS: }
}
// JS:
// JS: function estimateEncodedSize(value: unknown, seen?: WeakSet<object>, depth: number = 0): number {
fn estimate_encoded_size(value: &CapnwebValue, depth: usize) -> usize {
    // JS:   if (depth >= MAX_ESTIMATE_DEPTH) return ESTIMATED_ENTRY_OVERHEAD;
    if depth >= MAX_ESTIMATE_DEPTH {
        return ESTIMATED_ENTRY_OVERHEAD;
    }
    // JS:
    // JS:   switch (typeof value) {
    match value {
        // JS:     case "string":
        // JS:       return estimateStringSize(value);
        CapnwebValue::String(value) => estimate_string_size(value),
        // JS:     case "number":
        // JS:       return 16;
        CapnwebValue::Number(_) => 16,
        // JS:     case "bigint":
        // JS:       return 16;
        CapnwebValue::BigInt(_) => 16,
        // JS:     case "boolean":
        // JS:       return 8;
        CapnwebValue::Bool(_) => 8,
        // JS:     case "undefined":
        // JS:       return 16;
        CapnwebValue::Undefined => 16,
        // JS:     case "object": {
        // JS:       if (value === null) return 8;
        CapnwebValue::Null => 8,
        // JS:       if (ArrayBuffer.isView(value)) return ESTIMATED_BINARY_OVERHEAD + value.byteLength;
        CapnwebValue::Bytes(value) => ESTIMATED_BINARY_OVERHEAD + value.len(),
        // JS:       if (value instanceof ArrayBuffer) return ESTIMATED_BINARY_OVERHEAD + value.byteLength;
        // JS:       if (typeof Blob !== "undefined" && value instanceof Blob) {
        // JS:         return ESTIMATED_BINARY_OVERHEAD + value.size;
        // JS:       }
        // JS:       if (value instanceof Date) return 16;
        // JS:
        // JS:       // `seen` is only ever added to, never removed, so it dedupes by object identity across the
        // JS:       // entire traversal rather than just along the current path. This is intentional: it keeps the
        // JS:       // estimate safe against cyclic graphs (which would otherwise recurse forever). The trade-off
        // JS:       // is that a value reachable via two different paths (shared but acyclic) is counted in full
        // JS:       // the first time and only as ESTIMATED_ENTRY_OVERHEAD afterward, so shared substructure
        // JS:       // under-counts slightly. That's acceptable here — this is a flow-control estimate, not an
        // JS:       // exact serialized size, and it otherwise biases high.
        // JS:       seen ??= new WeakSet();
        // JS:       if (seen.has(value)) return ESTIMATED_ENTRY_OVERHEAD;
        // JS:       seen.add(value);
        // JS:
        // JS:       if (value instanceof Array) {
        CapnwebValue::Array(value) => {
            // JS:         let size = ESTIMATED_OBJECT_OVERHEAD;
            let mut size = ESTIMATED_OBJECT_OVERHEAD;
            // JS:         for (let item of value) {
            for item in value {
                // JS:           size += ESTIMATED_ENTRY_OVERHEAD + estimateEncodedSize(item, seen, depth + 1);
                size += ESTIMATED_ENTRY_OVERHEAD + estimate_encoded_size(item, depth + 1);
                // JS:         }
            }
            // JS:         return size;
            size
            // JS:       }
        }
        // JS:
        // JS:       if (value instanceof Error) {
        CapnwebValue::Error(value) => {
            // JS:         let size = ESTIMATED_OBJECT_OVERHEAD + estimateStringSize(value.name) +
            // JS:             estimateStringSize(value.message) + estimateStringSize(value.stack ?? "");
            let mut size = ESTIMATED_OBJECT_OVERHEAD
                + estimate_string_size(&value.name)
                + estimate_string_size(&value.message)
                + estimate_string_size(value.stack.as_deref().unwrap_or(""));
            // JS:         for (let key of Object.keys(value)) {
            for (key, property) in &value.properties {
                // JS:           size += ESTIMATED_ENTRY_OVERHEAD + estimateStringSize(key) +
                // JS:               estimateEncodedSize((value as any)[key], seen, depth + 1);
                size += ESTIMATED_ENTRY_OVERHEAD
                    + estimate_string_size(key)
                    + estimate_encoded_size(property, depth + 1);
                // JS:         }
            }
            // JS:         return size;
            size
            // JS:       }
        }
        // JS:
        // JS:       let size = ESTIMATED_OBJECT_OVERHEAD;
        CapnwebValue::Object(value) => {
            let mut size = ESTIMATED_OBJECT_OVERHEAD;
            // JS:       for (let key of Object.keys(value)) {
            // JS:         size += ESTIMATED_ENTRY_OVERHEAD + estimateStringSize(key) +
            // JS:             estimateEncodedSize((value as Record<string, unknown>)[key], seen, depth + 1);
            for (key, property) in value {
                size += ESTIMATED_ENTRY_OVERHEAD
                    + estimate_string_size(key)
                    + estimate_encoded_size(property, depth + 1);
                // JS:       }
            }
            // JS:       return size;
            size
        }
        // JS:     }
        // JS:     default:
        // JS:       return 16;
        CapnwebValue::Stub(_)
        | CapnwebValue::RpcPromise(_)
        | CapnwebValue::Target(_)
        | CapnwebValue::Function(_) => 16,
        // JS:   }
    }
    // JS: }
}
// JS:
// JS: // Entry on the exports table.
// JS: type ExportTableEntry = {
struct ExportTableEntry {
    // JS:   hook: StubHook,
    hook: StubHook,
    // JS:   refcount: number,
    refcount: usize,
    // JS:   pull?: Promise<void>,
    pulling: bool,
    // JS:
    // JS:   // If true, the export should be automatically released (with refcount 1) after its "resolve"
    // JS:   // or "reject" message is sent. This is set for exports created by ["stream"] messages.
    // JS:   autoRelease?: boolean,
    auto_release: bool,
    // JS:
    // JS:   // If this export was created by a ["pipe"] message, this holds the ReadableStream end of the
    // JS:   // pipe. It is consumed (and set to undefined) when a ["readable", importId] expression
    // JS:   // references this export.
    // JS:   pipeReadable?: ReadableStream
    // JS: };
}
// JS:
// JS: // Entry on the imports table.
// JS: class ImportTableEntry {
type BrokenCallback = Arc<Mutex<Option<Box<dyn FnOnce(Error) + Send + 'static>>>>;

fn invoke_broken_callback(callback: BrokenCallback, error: Error) {
    if let Some(callback) = callback
        .lock()
        .expect("broken callback mutex poisoned")
        .take()
    {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback(error)));
    }
}

struct ImportTableEntry {
    session: Weak<Mutex<RpcSessionState>>,
    session_id: u64,
    import_id: ImportId,
    local_refcount: usize,
    remote_refcount: usize,
    active_pull: Option<PromiseWithResolvers<()>>,
    resolution: Option<StubHook>,
    on_broken_registrations: Vec<usize>,
}

impl ImportTableEntry {
    // JS:   constructor(public session: RpcSessionImpl, public importId: number, pulling: boolean) {
    fn new(
        session: &Arc<Mutex<RpcSessionState>>,
        session_id: u64,
        import_id: ImportId,
        pulling: bool,
    ) -> Self {
        Self {
            session: Arc::downgrade(session),
            session_id,
            import_id,
            local_refcount: 0,
            remote_refcount: 1,
            active_pull: pulling.then(PromiseWithResolvers::new),
            resolution: None,
            on_broken_registrations: Vec::new(),
        }
        // JS:     if (pulling) {
        // JS:       this.activePull = Promise.withResolvers<void>();
        // JS:     }
        // JS:   }
    }
    // JS:
    // JS:   public localRefcount: number = 0;
    // Stored in `local_refcount`; `Arc` clones of one hook share one local entry reference.
    // JS:   public remoteRefcount: number = 1;
    // Stored in `remote_refcount`.
    // JS:
    // JS:   private activePull?: PromiseWithResolvers<void>;
    // `active_pull` stores the Rust `PromiseWithResolvers<()>` equivalent.
    // JS:   public resolution?: StubHook;
    // Stored in `resolution`.
    // JS:
    // JS:   // List of integer indexes into session.onBrokenCallbacks which are callbacks registered on
    // JS:   // this import. Initialized on first use (so `undefined` is the same as an empty list).
    // JS:   private onBrokenRegistrations?: number[];
    // Stored in `on_broken_registrations`.
    // JS:
    // JS:   resolve(resolution: StubHook) {
    fn resolve(entry: &Arc<Mutex<Self>>, resolution: StubHook) -> Result<()> {
        // JS:     // TODO: Need embargo handling here? PayloadStubHook needs to be wrapped in a
        // JS:     // PromiseStubHook awaiting the embargo I suppose. Previous notes on embargoes:
        // JS:     // - Resolve message specifies last call that was received before the resolve. The introducer is
        // JS:     //   responsible for any embargoes up to that point.
        // JS:     // - Any further calls forwarded by the introducer after that point MUST immediately resolve to
        // JS:     //   a forwarded call. The caller is responsible for ensuring the last of these is handed off
        // JS:     //   before direct calls can be delivered.
        // JS:
        // JS:     if (this.localRefcount == 0) {
        let mut state = entry.lock().expect("import entry mutex poisoned");
        if state.local_refcount == 0 {
            // JS:       // Already disposed (canceled), so ignore the resolution and don't send a redundant release.
            // JS:       resolution.dispose();
            drop(resolution);
            // JS:       return;
            return Ok(());
            // JS:     }
        }
        // JS:
        // JS:     this.resolution = resolution;
        state.resolution = Some(Arc::clone(&resolution));
        // JS:     this.sendRelease();
        let release = std::mem::take(&mut state.remote_refcount);
        let import_id = state.import_id;
        let session = state.session.upgrade();
        let registrations = std::mem::take(&mut state.on_broken_registrations);
        let active_pull = state.active_pull.take();
        drop(state);
        if release > 0 {
            if let Some(session) = &session {
                RpcSessionImpl::send_release(session, import_id, release)?;
            }
        }
        // JS:
        // JS:     if (this.onBrokenRegistrations) {
        // JS:       // Delete all our callback registrations from this session and re-register them on the
        // JS:       // target stub.
        // JS:       for (let i of this.onBrokenRegistrations) {
        for index in registrations {
            // JS:         let callback = this.session.onBrokenCallbacks[i];
            let callback = session.as_ref().and_then(|session| {
                session
                    .lock()
                    .expect("RPC session mutex poisoned")
                    .on_broken_callbacks
                    .get(index)
                    .and_then(Clone::clone)
            });
            let Some(callback) = callback else { continue };
            // JS:         let endIndex = this.session.onBrokenCallbacks.length;
            let end_index = session.as_ref().map_or(0, |session| {
                session
                    .lock()
                    .expect("RPC session mutex poisoned")
                    .on_broken_callbacks
                    .len()
            });
            // JS:         resolution.onBroken(callback);
            let forwarded = Arc::clone(&callback);
            resolution.on_broken(Box::new(move |error| {
                invoke_broken_callback(forwarded, error)
            }));
            // JS:         if (this.session.onBrokenCallbacks[endIndex] === callback) {
            // JS:           // Oh, calling onBroken() just registered the callback back on this connection again.
            // JS:           // But when the connection dies, we want all the callbacks to be called in the order in
            // JS:           // which they were registered. So we don't want this one pushed to the back of the line
            // JS:           // here. So, let's remove the newly-added registration and keep the original.
            // JS:           // TODO: This is quite hacky, think about whether this is really the right answer.
            // JS:           delete this.session.onBrokenCallbacks[endIndex];
            if let Some(session) = &session {
                let mut session = session.lock().expect("RPC session mutex poisoned");
                let registered_back = session
                    .on_broken_callbacks
                    .get(end_index)
                    .and_then(Option::as_ref)
                    .is_some_and(|candidate| Arc::ptr_eq(candidate, &callback));
                if registered_back {
                    session.on_broken_callbacks[end_index] = None;
                } else if let Some(slot) = session.on_broken_callbacks.get_mut(index) {
                    *slot = None;
                }
            }
            // JS:         } else {
            // JS:           // The callback is now registered elsewhere, so delete it from our session.
            // JS:           delete this.session.onBrokenCallbacks[i];
            // handled by the branch above.
            // JS:         }
            // JS:       }
        }
        // JS:       this.onBrokenRegistrations = undefined;
        // JS:     }
        // JS:
        // JS:     if (this.activePull) {
        // JS:       this.activePull.resolve();
        if let Some(active_pull) = active_pull {
            active_pull.resolve(());
        }
        // JS:       this.activePull = undefined;
        // represented by `take()` above.
        // JS:     }
        // JS:   }
        Ok(())
    }
    // JS:
    // JS:   async awaitResolution(): Promise<RpcPayload> {
    async fn await_resolution(entry: Arc<Mutex<Self>>) -> Result<RpcPayload> {
        // JS:     if (!this.activePull) {
        let (send_pull, session, import_id, promise) = {
            let mut state = entry.lock().expect("import entry mutex poisoned");
            let send_pull = state.active_pull.is_none();
            if send_pull {
                state.active_pull = Some(PromiseWithResolvers::new());
            }
            (
                send_pull,
                state.session.upgrade(),
                state.import_id,
                state
                    .active_pull
                    .as_ref()
                    .expect("active pull initialized")
                    .promise(),
            )
        };
        // JS:       this.session.sendPull(this.importId);
        if send_pull {
            let session =
                session.ok_or_else(|| Error::Message("RPC session was dropped".to_owned()))?;
            if let Err(error) = RpcSessionImpl::send_pull(&session, import_id) {
                if let Some(active_pull) = entry
                    .lock()
                    .expect("import entry mutex poisoned")
                    .active_pull
                    .take()
                {
                    active_pull.reject(error.clone());
                }
                return Err(error);
            }
            // JS:       this.activePull = Promise.withResolvers<void>();
            // set before sending to preserve e-order.
            // JS:     }
        }
        // JS:     await this.activePull.promise;
        promise.into_future().await?;
        let resolution = entry
            .lock()
            .expect("import entry mutex poisoned")
            .resolution
            .clone()
            .ok_or_else(|| {
                Error::Message("pull resolved without an import resolution".to_owned())
            })?;
        // JS:     return this.resolution!.pull();
        match resolution.pull()? {
            MaybePromise::Immediate(payload) => Ok(payload),
            MaybePromise::Pending(payload) => payload.await,
        }
        // JS:   }
    }
    // JS:
    // JS:   dispose() {
    fn dispose(entry: &Arc<Mutex<Self>>) -> Result<()> {
        // JS:     if (this.resolution) {
        let resolution = entry
            .lock()
            .expect("import entry mutex poisoned")
            .resolution
            .clone();
        if let Some(resolution) = resolution {
            // JS:       this.resolution.dispose();
            drop(resolution);
        // JS:     } else {
        } else {
            // JS:       this.abort(new Error("RPC was canceled because the RpcPromise was disposed."));
            Self::abort(
                entry,
                Error::Message("RPC was canceled because the RpcPromise was disposed.".to_owned()),
            );
            // JS:       this.sendRelease();
            Self::send_release(entry)?;
            // JS:     }
        }
        // JS:   }
        Ok(())
    }
    // JS:
    // JS:   abort(error: any) {
    fn abort(entry: &Arc<Mutex<Self>>, error: Error) {
        // JS:     if (!this.resolution) {
        let mut state = entry.lock().expect("import entry mutex poisoned");
        if state.resolution.is_none() {
            // JS:       this.resolution = new ErrorStubHook(error);
            state.resolution = Some(Arc::new(ErrorStubHook::new(error.clone())));
            // JS:
            // JS:       if (this.activePull) {
            // JS:         this.activePull.reject(error);
            let active_pull = state.active_pull.take();
            // JS:         this.activePull = undefined;
            // represented by `take()` above.
            // JS:       }
            // JS:
            // JS:       // The RpcSession itself will have called all our callbacks so we don't need to track the
            // JS:       // registrations anymore.
            // JS:       this.onBrokenRegistrations = undefined;
            state.on_broken_registrations.clear();
            drop(state);
            if let Some(active_pull) = active_pull {
                active_pull.reject(error);
            }
            // JS:     }
        }
        // JS:   }
    }
    // JS:
    // JS:   onBroken(callback: (error: any) => void): void {
    fn on_broken(entry: &Arc<Mutex<Self>>, callback: Box<dyn FnOnce(Error) + Send + 'static>) {
        // JS:     if (this.resolution) {
        let resolution = entry
            .lock()
            .expect("import entry mutex poisoned")
            .resolution
            .clone();
        if let Some(resolution) = resolution {
            // JS:       this.resolution.onBroken(callback);
            resolution.on_broken(callback);
        // JS:     } else {
        } else {
            // JS:       let index = this.session.onBrokenCallbacks.length;
            let callback = Arc::new(Mutex::new(Some(callback)));
            let session = entry
                .lock()
                .expect("import entry mutex poisoned")
                .session
                .upgrade();
            let Some(session) = session else { return };
            let mut session = session.lock().expect("RPC session mutex poisoned");
            let index = session.on_broken_callbacks.len();
            // JS:       this.session.onBrokenCallbacks.push(callback);
            session.on_broken_callbacks.push(Some(callback));
            drop(session);
            // JS:
            // JS:       if (!this.onBrokenRegistrations) this.onBrokenRegistrations = [];
            // JS:       this.onBrokenRegistrations.push(index);
            entry
                .lock()
                .expect("import entry mutex poisoned")
                .on_broken_registrations
                .push(index);
            // JS:     }
        }
        // JS:   }
    }
    // JS:
    // JS:   private sendRelease() {
    fn send_release(entry: &Arc<Mutex<Self>>) -> Result<()> {
        // JS:     if (this.remoteRefcount > 0) {
        let (session, import_id, remote_refcount) = {
            let mut state = entry.lock().expect("import entry mutex poisoned");
            (
                state.session.upgrade(),
                state.import_id,
                std::mem::take(&mut state.remote_refcount),
            )
        };
        if remote_refcount > 0 {
            // JS:       this.session.sendRelease(this.importId, this.remoteRefcount);
            if let Some(session) = session {
                RpcSessionImpl::send_release(&session, import_id, remote_refcount)?;
            }
            // JS:       this.remoteRefcount = 0;
            // taken above.
            // JS:     }
        }
        // JS:   }
        Ok(())
    }
    // JS: };
}
// JS:
// JS: class RpcImportHook extends StubHook {
struct RpcImportHook {
    is_promise: bool,
    entry: Arc<Mutex<ImportTableEntry>>,
    shutdown_on_drop: bool,
}
// JS:   public entry?: ImportTableEntry;  // undefined when we're disposed
// Rust ownership keeps the entry present until the final hook `Arc` is dropped.
// JS:
// JS:   // `pulling` is true if we already expect that this import is going to be resolved later, and
// JS:   // null if this import is not allowed to be pulled (i.e. it's a stub not a promise).
// JS:   constructor(public isPromise: boolean, entry: ImportTableEntry) {
impl RpcImportHook {
    fn new(is_promise: bool, entry: Arc<Mutex<ImportTableEntry>>) -> Self {
        // JS:     super();
        // JS:     ++entry.localRefcount;
        entry
            .lock()
            .expect("import entry mutex poisoned")
            .local_refcount += 1;
        // JS:     this.entry = entry;
        Self {
            is_promise,
            entry,
            shutdown_on_drop: false,
        }
        // JS:   }
    }
    // JS:
    // JS:   collectPath(path: PropertyPath): RpcImportHook {
    // JS:     return this;
    // JS:   }
    // JS:
    // JS:   getEntry(): ImportTableEntry {
    fn get_entry(&self) -> Arc<Mutex<ImportTableEntry>> {
        // JS:     if (this.entry) {
        // JS:       return this.entry;
        Arc::clone(&self.entry)
        // JS:     } else {
        // JS:       // Shouldn't get here in practice since the holding stub should have replaced the hook when
        // JS:       // disposed.
        // JS:       throw new Error("This RpcImportHook was already disposed.");
        // JS:     }
        // JS:   }
    }
}
// JS:
// JS:   // Like getEntry(), but for use in methods that take ownership of args or captures: if the
// JS:   // entry is gone, there is no callee left to consume them, so dispose them before throwing.
// JS:   private getEntryTakingOwnership(disposeOwned: () => void): ImportTableEntry {
// JS:     try {
// JS:       return this.getEntry();
// JS:     } catch (err) {
// JS:       disposeOwned();
// JS:       throw err;
// JS:     }
// JS:   }
// JS:
// JS:   // -------------------------------------------------------------------------------------
// JS:   // implements StubHook
// JS:
impl StubHookInner for RpcImportHook {
    fn rpc_import(&self) -> Option<(u64, i64)> {
        let entry = self.entry.lock().expect("import entry mutex poisoned");
        Some((entry.session_id, entry.import_id))
    }

    // JS:   call(path: PropertyPath, args: RpcPayload): StubHook {
    fn call(&self, path: PropertyPath, args: RpcPayload) -> Result<StubHook> {
        // JS:     let entry = this.getEntryTakingOwnership(() => args.dispose());
        let entry = self.get_entry();
        // JS:     if (entry.resolution) {
        let (resolution, session, import_id) = {
            let entry = entry.lock().expect("import entry mutex poisoned");
            (
                entry.resolution.clone(),
                entry.session.upgrade(),
                entry.import_id,
            )
        };
        if let Some(resolution) = resolution {
            // JS:       return entry.resolution.call(path, args);
            resolution.call(path, args)
        // JS:     } else {
        } else {
            // JS:       return entry.session.sendCall(entry.importId, path, args);
            let Some(session) = session else {
                drop(args);
                return Err(Error::Message("RPC session was dropped".to_owned()));
            };
            RpcSessionImpl::send_call(&session, import_id, path, Some(args))
            // JS:     }
        }
        // JS:   }
    }
    // JS:
    // JS:   stream(path: PropertyPath, args: RpcPayload): {promise: Promise<void>, size?: number} {
    fn stream(&self, path: PropertyPath, args: RpcPayload) -> Result<StreamCall> {
        // JS:     let entry = this.getEntryTakingOwnership(() => args.dispose());
        let entry = self.get_entry();
        // JS:     if (entry.resolution) {
        let (resolution, session, import_id) = {
            let entry = entry.lock().expect("import entry mutex poisoned");
            (
                entry.resolution.clone(),
                entry.session.upgrade(),
                entry.import_id,
            )
        };
        if let Some(resolution) = resolution {
            // JS:       return entry.resolution.stream(path, args);
            resolution.stream(path, args)
        // JS:     } else {
        } else {
            // JS:       return entry.session.sendStream(entry.importId, path, args);
            let Some(session) = session else {
                drop(args);
                return Err(Error::Message("RPC session was dropped".to_owned()));
            };
            RpcSessionImpl::send_stream(&session, import_id, path, args)
            // JS:     }
        }
        // JS:   }
    }
    // JS:
    // JS:   map(path: PropertyPath, captures: StubHook[], instructions: unknown[]): StubHook {
    fn map(
        &self,
        path: PropertyPath,
        captures: Vec<StubHook>,
        instructions: MapInstructions,
    ) -> Result<StubHook> {
        // JS:     let entry = this.getEntryTakingOwnership(() => {
        let entry = self.get_entry();
        // JS:       for (let cap of captures) {
        // JS:         cap.dispose();
        // JS:       }
        // JS:     });
        // JS:     if (entry.resolution) {
        let (resolution, session, import_id) = {
            let entry = entry.lock().expect("import entry mutex poisoned");
            (
                entry.resolution.clone(),
                entry.session.upgrade(),
                entry.import_id,
            )
        };
        if let Some(resolution) = resolution {
            // JS:       return entry.resolution.map(path, captures, instructions);
            resolution.map(path, captures, instructions)
        // JS:     } else {
        } else {
            // JS:       return entry.session.sendMap(entry.importId, path, captures, instructions);
            let Some(session) = session else {
                drop(captures);
                return Err(Error::Message("RPC session was dropped".to_owned()));
            };
            RpcSessionImpl::send_map(&session, import_id, path, captures, instructions)
            // JS:     }
        }
        // JS:   }
    }
    // JS:
    // JS:   get(path: PropertyPath): StubHook {
    fn get(&self, path: PropertyPath) -> Result<StubHook> {
        // JS:     let entry = this.getEntry();
        let entry = self.get_entry();
        // JS:     if (entry.resolution) {
        let (resolution, session, import_id) = {
            let entry = entry.lock().expect("import entry mutex poisoned");
            (
                entry.resolution.clone(),
                entry.session.upgrade(),
                entry.import_id,
            )
        };
        if let Some(resolution) = resolution {
            // JS:       return entry.resolution.get(path);
            resolution.get(path)
        // JS:     } else {
        } else {
            // JS:       return entry.session.sendCall(entry.importId, path);
            let session =
                session.ok_or_else(|| Error::Message("RPC session was dropped".to_owned()))?;
            RpcSessionImpl::send_call(&session, import_id, path, None)
            // JS:     }
        }
        // JS:   }
    }
    // JS:
    // JS:   dup(): RpcImportHook {
    // JS:     return new RpcImportHook(false, this.getEntry());
    // JS:   }
    // JS:
    // JS:   pull(): RpcPayload | Promise<RpcPayload> {
    fn pull(&self) -> Result<MaybePromise<RpcPayload>> {
        // JS:     let entry = this.getEntry();
        let entry = self.get_entry();
        // JS:
        // JS:     if (!this.isPromise) {
        if !self.is_promise {
            // JS:       throw new Error("Can't pull this hook because it's not a promise hook.");
            return Err(Error::Message(
                "Can't pull this hook because it's not a promise hook.".to_owned(),
            ));
            // JS:     }
        }
        // JS:
        // JS:     if (entry.resolution) {
        if let Some(resolution) = entry
            .lock()
            .expect("import entry mutex poisoned")
            .resolution
            .clone()
        {
            // JS:       return entry.resolution.pull();
            return resolution.pull();
            // JS:     }
        }
        // JS:
        // JS:     return entry.awaitResolution();
        Ok(MaybePromise::Pending(Box::pin(
            ImportTableEntry::await_resolution(entry),
        )))
        // JS:   }
    }
    // JS:
    // JS:   ignoreUnhandledRejections(): void {
    fn ignore_unhandled_rejections(&self) {
        // JS:     // We don't actually have to do anything here because this method only has to ignore rejections
        // JS:     // if pull() is *not* called, and if pull() is not called then we won't generate any rejections
        // JS:     // anyway.
        // JS:   }
    }
    // JS:
    // JS:   dispose(): void {
    // JS:     let entry = this.entry;
    // JS:     this.entry = undefined;
    // JS:     if (entry) {
    // JS:       if (--entry.localRefcount === 0) {
    // JS:         entry.dispose();
    // JS:       }
    // JS:     }
    // JS:   }
    // JS:
    // JS:   onBroken(callback: (error: any) => void): void {
    fn on_broken(&self, callback: Box<dyn FnOnce(Error) + Send + 'static>) {
        // JS:     if (this.entry) {
        // JS:       this.entry.onBroken(callback);
        ImportTableEntry::on_broken(&self.entry, callback);
        // JS:     }
        // JS:   }
    }
    // JS: }
}

impl Drop for RpcImportHook {
    fn drop(&mut self) {
        if self.shutdown_on_drop {
            let session = {
                self.entry
                    .lock()
                    .expect("import entry mutex poisoned")
                    .session
                    .upgrade()
            };
            if let Some(session) = session {
                RpcSessionImpl::shutdown_state(&session);
            }
            return;
        }
        let should_dispose = {
            let mut entry = self.entry.lock().expect("import entry mutex poisoned");
            entry.local_refcount -= 1;
            entry.local_refcount == 0
        };
        if should_dispose {
            let _ = ImportTableEntry::dispose(&self.entry);
        }
    }
}
// JS:
// JS: class RpcMainHook extends RpcImportHook {
// JS:   private session?: RpcSessionImpl;
// JS:
// JS:   constructor(entry: ImportTableEntry) {
// JS:     super(false, entry);
// JS:     this.session = entry.session;
// JS:   }
// JS:
// JS:   dispose(): void {
// JS:     if (this.session) {
// JS:       let session = this.session;
// JS:       this.session = undefined;
// JS:       session.shutdown();
// JS:     }
// JS:   }
// JS: }
// JS:
// JS: /**
// JS:  * Options to customize behavior of an RPC session. All functions which start a session should
// JS:  * optionally accept this.
// JS:  */
// JS: export type RpcSessionOptions = {
/// Receiver limits and error-rewriting behavior for one RPC session.
#[derive(Clone)]
pub struct RpcSessionOptions {
    /// Optional outgoing serialized-error filter.
    pub on_send_error: Option<Arc<dyn Fn(CapnwebError) -> Option<CapnwebError> + Send + Sync>>,
    /// Resolved receiver-side limits.
    pub limits: RpcLimits,
}

impl Default for RpcSessionOptions {
    fn default() -> Self {
        Self {
            on_send_error: None,
            limits: DEFAULT_LIMITS,
        }
    }
}
// JS:   /**
// JS:    * If provided, this function will be called whenever an `Error` object is serialized (for any
// JS:    * reason, not just because it was thrown). This can be used to log errors, and also to redact
// JS:    * them.
// JS:    *
// JS:    * If `onSendError` returns an Error object, than object will be substituted in place of the
// JS:    * original. If it has a stack property, the stack will be sent to the client.
// JS:    *
// JS:    * If `onSendError` doesn't return anything (or is not provided at all), the default behavior is
// JS:    * to serialize the error with the stack omitted.
// JS:    */
// JS:   onSendError?: (error: Error) => Error | void;
// JS:
// JS:   /**
// JS:    * Overrides for the resource limits enforced while deserializing messages from the peer. Any
// JS:    * field left unset falls back to `DEFAULT_LIMITS`. These guard against resource-exhaustion
// JS:    * attacks from untrusted peers; see `RpcLimits` for the meaning and defaults of each field.
// JS:    *
// JS:    * Limits are a purely local, receiver-side decision -- the protocol has no negotiation step, so
// JS:    * the peer never learns these values. A message that exceeds a limit is rejected, aborting the
// JS:    * session.
// JS:    */
// JS:   limits?: Partial<RpcLimits>;
// JS: };
// JS:
// JS: class RpcSessionImpl implements Importer, Exporter {
static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

/// Shared implementation backing an RPC session and all of its import hooks.
#[derive(Clone)]
pub struct RpcSessionImpl {
    state: Arc<Mutex<RpcSessionState>>,
}

struct RpcSessionState {
    id: u64,
    transport: Arc<dyn AnyRpcTransport>,
    options: RpcSessionOptions,
    // JS:   private exports: Array<ExportTableEntry> = [];
    exports: HashMap<ExportId, ExportTableEntry>,
    // JS:   private reverseExports: Map<StubHook, ExportId> = new Map();
    reverse_exports: Vec<(StubHook, ExportId)>,
    // JS:   private imports: Array<ImportTableEntry> = [];
    imports: HashMap<ImportId, Arc<Mutex<ImportTableEntry>>>,
    next_import_id: ImportId,
    // JS:   private abortReason?: any;
    abort_reason: Option<Error>,
    // JS:   private cancelReadLoop?: (error: any) => void;
    read_loop_abort: Option<tokio::task::AbortHandle>,
    // JS:
    // JS:   // We assign positive numbers to imports we initiate, and negative numbers to exports we
    // JS:   // initiate. So the next import ID is just `imports.length`, but the next export ID needs
    // JS:   // to be tracked explicitly.
    // JS:   private nextExportId = -1;
    next_export_id: ExportId,
    next_incoming_export_id: ExportId,
    // JS:
    // JS:   // If set, call this when all incoming calls are complete.
    // JS:   private onBatchDone?: Omit<PromiseWithResolvers<void>, "promise">;
    // JS:
    // JS:   // How many promises is our peer expecting us to resolve?
    // JS:   private pullCount = 0;
    pull_count: usize,
    on_batch_done: Option<PromiseWithResolvers<()>>,
    // JS:
    // JS:   // Sparse array of onBrokenCallback registrations. Items are strictly appended to the end but
    // JS:   // may be deleted from the middle (hence leaving the array sparse).
    // JS:   onBrokenCallbacks: ((error: any) => void)[] = [];
    on_broken_callbacks: Vec<Option<BrokenCallback>>,
    // JS:
    // JS:   // Encoding level from the transport (defaults to "string")
    // JS:   private encodingLevel: EncodingLevel;
    encoding_level: EncodingLevel,
    // JS:
    // JS:   // Resource limits enforced on incoming messages, resolved once from the defaults plus any
    // JS:   // per-session overrides.
    // JS:   private limits: RpcLimits;
    limits: RpcLimits,
}
// JS:
// JS:   constructor(private transport: AnyRpcTransport, mainHook: StubHook,
// JS:       private options: RpcSessionOptions) {
impl RpcSessionImpl {
    /// Starts a session around `transport`, exporting `main_hook` as bootstrap ID zero.
    pub fn new(
        transport: Arc<dyn AnyRpcTransport>,
        main_hook: StubHook,
        options: RpcSessionOptions,
    ) -> Result<Self> {
        let _ = crate::map::force_init_map();
        // JS:     // `RpcTransport` has no `encodingLevel` field, so its presence is what marks a custom-encoding
        // JS:     // transport. Read it defensively: treat a present-but-`undefined` value (e.g. an uninitialized
        // JS:     // class field) as the default string level rather than mis-routing it down the custom-encoding
        // JS:     // path, and reject any other unrecognized value (e.g. a stale pre-rename level name) loudly
        // JS:     // instead of silently corrupting the wire.
        // JS:     let level: EncodingLevel = "string";
        let level = transport.encoding_level();
        // JS:     if ('encodingLevel' in transport) {
        // JS:       let raw = transport.encodingLevel as unknown;
        // JS:       if (raw !== undefined) {
        // JS:         if (raw !== "string" && raw !== "jsonCompatible" &&
        // JS:             raw !== "jsonCompatibleWithBytes" && raw !== "structuredClonable") {
        // JS:           throw new TypeError(`Unknown transport encodingLevel: ${String(raw)}`);
        // JS:         }
        // JS:         level = raw;
        // JS:       }
        // JS:     }
        // JS:     this.encodingLevel = level;
        // The Rust transport returns the closed `EncodingLevel` enum, so invalid names are unrepresentable.
        // JS:
        // JS:     this.limits = { ...DEFAULT_LIMITS, ...options.limits };
        let limits = options.limits;
        let mut exports = HashMap::new();
        // JS:
        // JS:     // Export zero is automatically the bootstrap object.
        // JS:     this.exports.push({hook: mainHook, refcount: 1});
        exports.insert(
            0,
            ExportTableEntry {
                hook: main_hook,
                refcount: 1,
                pulling: false,
                auto_release: false,
            },
        );
        let state = Arc::new(Mutex::new(RpcSessionState {
            id: NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed),
            transport,
            options,
            exports,
            reverse_exports: Vec::new(),
            imports: HashMap::new(),
            next_import_id: 1,
            abort_reason: None,
            read_loop_abort: None,
            next_export_id: -1,
            next_incoming_export_id: 1,
            pull_count: 0,
            on_batch_done: None,
            on_broken_callbacks: Vec::new(),
            encoding_level: level,
            limits,
        }));
        // JS:
        // JS:     // Import zero is the other side's bootstrap object.
        // JS:     this.imports.push(new ImportTableEntry(this, 0, false));
        let main_import = Arc::new(Mutex::new(ImportTableEntry::new(
            &state,
            state.lock().expect("RPC session mutex poisoned").id,
            0,
            false,
        )));
        state
            .lock()
            .expect("RPC session mutex poisoned")
            .imports
            .insert(0, main_import);
        // JS:
        // JS:     this.readLoop().catch(err => this.abort(err));
        RpcSessionImpl::start_read_loop(&state);
        Ok(Self { state })
        // JS:   }
    }
    // JS:
    // JS:   // Should only be called once immediately after construction.
    // JS:   getMainImport(): RpcImportHook {
    pub fn get_main_import(&self) -> Result<RpcStub> {
        // JS:     return new RpcMainHook(this.imports[0]);
        let entry = self
            .state
            .lock()
            .expect("RPC session mutex poisoned")
            .imports
            .get(&0)
            .cloned()
            .ok_or_else(|| Error::Message("missing main import".to_owned()))?;
        let mut hook = RpcImportHook::new(false, entry);
        hook.shutdown_on_drop = true;
        Ok(RpcStub::from_hook(Arc::new(hook), None))
        // JS:   }
    }
    // JS:
    // JS:   shutdown(): void {
    pub fn shutdown(&self) {
        // JS:     // TODO(someday): Should we add some sort of "clean shutdown" mechanism? This gets the job
        // JS:     //   done just fine for the moment.
        // JS:     this.abort(new Error("RPC session was shut down by disposing the main stub"), false);
        RpcSessionImpl::shutdown_state(&self.state);
        // JS:   }
    }
    // JS:
    // JS:   exportStub(hook: StubHook): ExportId {
    fn export_stub_impl(&mut self, hook: StubHook) -> Result<ExportId> {
        // JS:     if (this.abortReason) throw this.abortReason;
        let mut state = self.state.lock().expect("RPC session mutex poisoned");
        if let Some(error) = &state.abort_reason {
            return Err(error.clone());
        }
        // JS:
        // JS:     let existingExportId = this.reverseExports.get(hook);
        let existing_export_id = state
            .reverse_exports
            .iter()
            .find(|(known, _)| Arc::ptr_eq(known, &hook))
            .map(|(_, id)| *id);
        // JS:     if (existingExportId !== undefined) {
        if let Some(export_id) = existing_export_id {
            // JS:       ++this.exports[existingExportId].refcount;
            state
                .exports
                .get_mut(&export_id)
                .expect("reverse export missing entry")
                .refcount += 1;
            // JS:       return existingExportId;
            Ok(export_id)
        // JS:     } else {
        } else {
            // JS:       let exportId = this.nextExportId--;
            let export_id = state.next_export_id;
            state.next_export_id -= 1;
            // JS:       this.exports[exportId] = { hook, refcount: 1 };
            state.exports.insert(
                export_id,
                ExportTableEntry {
                    hook: Arc::clone(&hook),
                    refcount: 1,
                    pulling: false,
                    auto_release: false,
                },
            );
            // JS:       this.reverseExports.set(hook, exportId);
            state.reverse_exports.push((hook, export_id));
            // JS:       // TODO: Use onBroken().
            // JS:       return exportId;
            Ok(export_id)
            // JS:     }
        }
        // JS:   }
    }
    // JS:
    // JS:   exportPromise(hook: StubHook): ExportId {
    fn export_promise_impl(&mut self, hook: StubHook) -> Result<ExportId> {
        // JS:     if (this.abortReason) throw this.abortReason;
        let export_id = {
            let mut state = self.state.lock().expect("RPC session mutex poisoned");
            if let Some(error) = &state.abort_reason {
                return Err(error.clone());
            }
            // JS:
            // JS:     // Promises always use a new ID because otherwise the recipient could miss the resolution.
            // JS:     let exportId = this.nextExportId--;
            let export_id = state.next_export_id;
            state.next_export_id -= 1;
            // JS:     this.exports[exportId] = { hook, refcount: 1 };
            state.exports.insert(
                export_id,
                ExportTableEntry {
                    hook: Arc::clone(&hook),
                    refcount: 1,
                    pulling: false,
                    auto_release: false,
                },
            );
            // JS:     this.reverseExports.set(hook, exportId);
            state.reverse_exports.push((hook, export_id));
            export_id
        };
        // JS:
        // JS:     // Automatically start resolving any promises we send.
        // JS:     this.ensureResolvingExport(exportId);
        RpcSessionImpl::ensure_resolving_export(&self.state, export_id)?;
        // JS:     return exportId;
        Ok(export_id)
        // JS:   }
    }
    // JS:
    // JS:   unexport(ids: Array<ExportId>): void {
    fn unexport_impl(&mut self, ids: Vec<ExportId>) -> Result<()> {
        // JS:     for (let id of ids) {
        for id in ids {
            // JS:       this.releaseExport(id, 1);
            Self::release_export(&self.state, id, 1)?;
            // JS:     }
        }
        // JS:   }
        Ok(())
    }
    // JS:
    // JS:   private releaseExport(exportId: ExportId, refcount: number) {
    // Implemented as a state helper because Rust import hooks hold a weak session reference.
    fn release_export(
        state: &Arc<Mutex<RpcSessionState>>,
        export_id: ExportId,
        refcount: usize,
    ) -> Result<()> {
        // JS:     let entry = this.exports[exportId];
        let mut state = state.lock().expect("RPC session mutex poisoned");
        // JS:     if (!entry) {
        if !state.exports.contains_key(&export_id) {
            // JS:       throw new Error(`no such export ID: ${exportId}`);
            return Err(Error::Message(format!("no such export ID: {export_id}")));
            // JS:     }
        }
        // JS:     if (entry.refcount < refcount) {
        let old_refcount = state.exports[&export_id].refcount;
        if old_refcount < refcount {
            // JS:       throw new Error(`refcount would go negative: ${entry.refcount} < ${refcount}`);
            return Err(Error::Message(format!(
                "refcount would go negative: {old_refcount} < {refcount}"
            )));
            // JS:     }
        }
        // JS:     entry.refcount -= refcount;
        state
            .exports
            .get_mut(&export_id)
            .expect("checked above")
            .refcount -= refcount;
        // JS:     if (entry.refcount === 0) {
        if state.exports[&export_id].refcount == 0 {
            // JS:       delete this.exports[exportId];
            let entry = state.exports.remove(&export_id).expect("checked above");
            // JS:       this.reverseExports.delete(entry.hook);
            state
                .reverse_exports
                .retain(|(hook, _)| !Arc::ptr_eq(hook, &entry.hook));
            // JS:       entry.hook.dispose();
            drop(entry);
            // JS:     }
        }
        // JS:   }
        Ok(())
    }
    // JS:
    // JS:   onSendError(error: Error): Error | void {
    fn on_send_error_impl(&mut self, error: CapnwebError) -> Option<CapnwebError> {
        // JS:     if (this.options.onSendError) {
        let callback = self
            .state
            .lock()
            .expect("RPC session mutex poisoned")
            .options
            .on_send_error
            .clone();
        if let Some(callback) = callback {
            // JS:       return this.options.onSendError(error);
            callback(error)
        // JS:     }
        } else {
            None
        }
        // JS:   }
    }
    // JS:
    // JS:   private ensureResolvingExport(exportId: ExportId) {
    fn ensure_resolving_export(
        state: &Arc<Mutex<RpcSessionState>>,
        export_id: ExportId,
    ) -> Result<()> {
        // JS:     let exp = this.exports[exportId];
        let (hook, auto_release) = {
            let mut session = state.lock().expect("RPC session mutex poisoned");
            // JS:     if (!exp) {
            // JS:       throw new Error(`no such export ID: ${exportId}`);
            let export = session
                .exports
                .get_mut(&export_id)
                .ok_or_else(|| Error::Message(format!("no such export ID: {export_id}")))?;
            // JS:     }
            // JS:     if (!exp.pull) {
            if export.pulling {
                return Ok(());
            }
            export.pulling = true;
            (Arc::clone(&export.hook), export.auto_release)
        };
        // JS:       let resolve = async () => {
        // JS:         let hook = exp.hook;
        // JS:         for (;;) {
        // JS:           let payload = await hook.pull();
        // JS:           if (payload.value instanceof RpcStub) {
        // JS:             let {hook: inner, pathIfPromise} = unwrapStubAndPath(payload.value);
        // JS:             if (pathIfPromise && pathIfPromise.length == 0) {
        // JS:               if (this.getImport(hook) === undefined) {
        // JS:                 // Optimization: The resolution is just another promise, and it is not a promise
        // JS:                 // pointing back to the peer. So if we send a resolve message, it's just going to
        // JS:                 // resolve to another new promise export, which is just going to have to wait for
        // JS:                 // another resolve message later. This intermediate resolve message gives the peer
        // JS:                 // no useful information, so let's skip it and just wait for the chained
        // JS:                 // resolution.
        // JS:                 hook = inner;
        // JS:                 continue;
        // JS:               }
        // JS:             }
        // JS:           }
        // JS:
        // JS:           return payload;
        // JS:         }
        // JS:       };
        // JS:
        // JS:       let autoRelease = exp.autoRelease;
        // JS:
        // JS:       ++this.pullCount;
        state.lock().expect("RPC session mutex poisoned").pull_count += 1;
        let weak_state = Arc::downgrade(state);
        let _pull = EagerPromise::new(async move {
            let pulled = async {
                let mut hook = hook;
                loop {
                    let payload = match hook.pull()? {
                        MaybePromise::Immediate(payload) => payload,
                        MaybePromise::Pending(payload) => payload.await?,
                    };
                    if let CapnwebValue::RpcPromise(promise) = &payload.value {
                        let (inner, path) = unwrap_stub_and_path(promise.as_stub());
                        let points_back = weak_state.upgrade().is_some_and(|state| {
                            let session_id = state.lock().expect("RPC session mutex poisoned").id;
                            inner
                                .rpc_import()
                                .is_some_and(|(owner, _)| owner == session_id)
                        });
                        if path.as_ref().is_some_and(Vec::is_empty) && !points_back {
                            hook = inner;
                            continue;
                        }
                    }
                    return Ok(payload);
                }
            }
            .await;
            let Some(state) = weak_state.upgrade() else {
                return Ok(());
            };
            // JS:       exp.pull = resolve().then(
            match pulled {
                // JS:         payload => {
                Ok(payload) => {
                    // JS:           // We don't transfer ownership of stubs in the payload since the payload
                    // JS:           // belongs to the hook which sticks around to handle pipelined requests.
                    // JS:           let value = Devaluator.devaluate(payload.value, undefined, this, payload, this.encodingLevel);
                    let encoding = state
                        .lock()
                        .expect("RPC session mutex poisoned")
                        .encoding_level;
                    let mut session = RpcSessionImpl {
                        state: Arc::clone(&state),
                    };
                    match Devaluator::devaluate(
                        &payload.value,
                        &mut session,
                        Some(&payload),
                        encoding,
                    ) {
                        Ok(value) => {
                            let _ = Self::send(
                                &state,
                                tagged("resolve", [CapnwebValue::Number(export_id as f64), value]),
                            );
                        }
                        Err(error) => Self::send_rejection(&state, export_id, error),
                    }
                    // JS:           this.send(["resolve", exportId, value]);
                    // JS:           if (autoRelease) this.releaseExport(exportId, 1);
                    if auto_release {
                        let _ = Self::release_export(&state, export_id, 1);
                    }
                    // JS:         },
                }
                // JS:         error => {
                Err(error) => {
                    // JS:           this.send(["reject", exportId, Devaluator.devaluate(error, undefined, this, undefined, this.encodingLevel)]);
                    Self::send_rejection(&state, export_id, error);
                    // JS:           if (autoRelease) this.releaseExport(exportId, 1);
                    if auto_release {
                        let _ = Self::release_export(&state, export_id, 1);
                    }
                    // JS:         }
                }
            }
            // JS:       ).catch(
            // JS:         error => {
            // JS:           // If serialization failed, report the serialization error, which should
            // JS:           // itself always be serializable.
            // JS:           try {
            // JS:             this.send(["reject", exportId, Devaluator.devaluate(error, undefined, this, undefined, this.encodingLevel)]);
            // JS:             if (autoRelease) this.releaseExport(exportId, 1);
            // JS:           } catch (error2) {
            // JS:             // TODO: Shouldn't happen, now what?
            // JS:             this.abort(error2);
            // JS:           }
            // JS:         }
            // JS:       ).finally(() => {
            // JS:         if (--this.pullCount === 0) {
            let on_batch_done = {
                let mut session = state.lock().expect("RPC session mutex poisoned");
                session.pull_count -= 1;
                if session.pull_count == 0 {
                    session.on_batch_done.take()
                } else {
                    None
                }
            };
            if let Some(on_batch_done) = on_batch_done {
                on_batch_done.resolve(());
                // JS:           if (this.onBatchDone) {
                // JS:             this.onBatchDone.resolve();
                // JS:           }
                // JS:         }
            }
            Ok(())
            // JS:       });
        });
        // JS:     }
        // JS:   }
        Ok(())
    }

    fn send_rejection(state: &Arc<Mutex<RpcSessionState>>, export_id: ExportId, error: Error) {
        let value = CapnwebValue::Error(error.into());
        let encoding = state
            .lock()
            .expect("RPC session mutex poisoned")
            .encoding_level;
        let mut session = RpcSessionImpl {
            state: Arc::clone(state),
        };
        if let Ok(value) = Devaluator::devaluate(&value, &mut session, None, encoding) {
            let _ = Self::send(
                state,
                tagged("reject", [CapnwebValue::Number(export_id as f64), value]),
            );
        }
    }
    // JS:
    // JS:   getImport(hook: StubHook): ImportId | undefined {
    fn get_import_impl(&mut self, hook: &StubHook) -> Option<ImportId> {
        // JS:     if (hook instanceof RpcImportHook && hook.entry && hook.entry.session === this) {
        let session_id = self.state.lock().expect("RPC session mutex poisoned").id;
        // JS:       return hook.entry.importId;
        hook.rpc_import()
            .and_then(|(owner, import_id)| (owner == session_id).then_some(import_id))
        // JS:     } else {
        // JS:       return undefined;
        // represented by `None` above.
        // JS:     }
        // JS:   }
    }
    // JS:
    // JS:   importStub(idx: ImportId): RpcImportHook {
    fn import_stub_impl(&mut self, index: ImportId) -> Result<StubHook> {
        // JS:     if (this.abortReason) throw this.abortReason;
        let mut state = self.state.lock().expect("RPC session mutex poisoned");
        if let Some(error) = &state.abort_reason {
            return Err(error.clone());
        }
        // JS:
        // JS:     let entry = this.imports[idx];
        let entry = state.imports.get(&index).cloned();
        // JS:     if (!entry) {
        let entry = if let Some(entry) = entry {
            entry
        } else {
            // JS:       entry = new ImportTableEntry(this, idx, false);
            let entry = Arc::new(Mutex::new(ImportTableEntry::new(
                &self.state,
                state.id,
                index,
                false,
            )));
            // JS:       this.imports[idx] = entry;
            state.imports.insert(index, Arc::clone(&entry));
            entry
            // JS:     }
        };
        // JS:     return new RpcImportHook(/*isPromise=*/false, entry);
        Ok(Arc::new(RpcImportHook::new(false, entry)))
        // JS:   }
    }
    // JS:
    // JS:   importPromise(idx: ImportId): StubHook {
    fn import_promise_impl(&mut self, index: ImportId) -> Result<StubHook> {
        // JS:     if (this.abortReason) throw this.abortReason;
        let mut state = self.state.lock().expect("RPC session mutex poisoned");
        if let Some(error) = &state.abort_reason {
            return Err(error.clone());
        }
        // JS:
        // JS:     if (this.imports[idx]) {
        if state.imports.contains_key(&index) {
            // JS:       // Can't reuse an existing ID for a promise!
            // JS:       return new ErrorStubHook(new Error(
            // JS:           "Bug in RPC system: The peer sent a promise reusing an existing export ID."));
            return Ok(Arc::new(ErrorStubHook::new(Error::Message(
                "Bug in RPC system: The peer sent a promise reusing an existing export ID."
                    .to_owned(),
            ))));
            // JS:     }
        }
        // JS:
        // JS:     // Create an already-pulling hook.
        // JS:     let entry = new ImportTableEntry(this, idx, true);
        let entry = Arc::new(Mutex::new(ImportTableEntry::new(
            &self.state,
            state.id,
            index,
            true,
        )));
        // JS:     this.imports[idx] = entry;
        state.imports.insert(index, Arc::clone(&entry));
        // JS:     return new RpcImportHook(/*isPromise=*/true, entry);
        Ok(Arc::new(RpcImportHook::new(true, entry)))
        // JS:   }
    }
    // JS:
    // JS:   getExport(idx: ExportId): StubHook | undefined {
    fn get_export_impl(&mut self, index: ExportId) -> Option<StubHook> {
        // JS:     return this.exports[idx]?.hook;
        self.state
            .lock()
            .expect("RPC session mutex poisoned")
            .exports
            .get(&index)
            .map(|entry| Arc::clone(&entry.hook))
        // JS:   }
    }
    // JS:
    // JS:   getPipeReadable(exportId: ExportId): ReadableStream {
    // JS:     let entry = this.exports[exportId];
    // JS:     if (!entry || !entry.pipeReadable) {
    // JS:       throw new Error(`Export ${exportId} is not a pipe or its readable end was already consumed.`);
    // JS:     }
    // JS:     let readable = entry.pipeReadable;
    // JS:     entry.pipeReadable = undefined;
    // JS:     return readable;
    // JS:   }
    // JS:
    // JS:   getLimits(): RpcLimits {
    fn get_limits_impl(&self) -> RpcLimits {
        // JS:     return this.limits;
        self.state
            .lock()
            .expect("RPC session mutex poisoned")
            .limits
        // JS:   }
    }
    // JS:
    // JS:   createPipe(readable: ReadableStream, readableHook: StubHook): ImportId {
    // JS:     if (this.abortReason) throw this.abortReason;
    // JS:
    // JS:     this.send(["pipe"]);
    // JS:
    // JS:     let importId = this.imports.length;
    // JS:     // The pipe import is not a promise -- it's immediately usable as a writable stream.
    // JS:     let entry = new ImportTableEntry(this, importId, false);
    // JS:     this.imports.push(entry);
    // JS:
    // JS:     // Create a proxy WritableStream from the import hook and pump the ReadableStream into it.
    // JS:     let hook = new RpcImportHook(/*isPromise=*/false, entry);
    // JS:     let writable = streamImpl.createWritableStreamFromHook(hook);
    // JS:     readable.pipeTo(writable).catch(() => {
    // JS:       // Errors are handled by the writable stream's error handling -- either the write fails
    // JS:       // and the writable side reports it, or the readable side errors and pipeTo aborts the
    // JS:       // writable side. Either way, the hook's disposal will handle cleanup.
    // JS:     }).finally(() => readableHook.dispose());
    // JS:
    // JS:     return importId;
    // JS:   }
    // JS:
    // JS:   // Serializes and sends a message. Returns the byte length reported by the transport, or
    // JS:   // undefined if the transport doesn't report size.
    // JS:   private send(msg: any): number | undefined {
    fn observe_send(
        state: &Arc<Mutex<RpcSessionState>>,
        pending: Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>,
    ) {
        let weak_state = Arc::downgrade(state);
        let promise = EagerPromise::new(pending);
        let _reaction = promise.then(move |result| {
            if let Err(error) = result {
                if let Some(state) = weak_state.upgrade() {
                    Self::abort(&state, error, false);
                }
            }
            Ok(())
        });
    }

    fn defer_abort(state: &Arc<Mutex<RpcSessionState>>, error: Error) {
        Self::observe_send(
            state,
            Box::pin(async move {
                tokio::task::yield_now().await;
                Err(error)
            }),
        );
    }

    fn send(state: &Arc<Mutex<RpcSessionState>>, message: CapnwebValue) -> Result<Option<usize>> {
        // JS:     if (this.abortReason !== undefined) {
        let (transport, encoding_level, aborted) = {
            let state = state.lock().expect("RPC session mutex poisoned");
            (
                Arc::clone(&state.transport),
                state.encoding_level,
                state.abort_reason.is_some(),
            )
        };
        if aborted {
            // JS:       // Ignore sends after we've aborted.
            // JS:       return 0;
            return Ok(Some(0));
            // JS:     }
        }
        // JS:
        // JS:     if (this.encodingLevel === "string") {
        if encoding_level == EncodingLevel::String {
            // JS:       let msgText: string;
            // JS:       try {
            // JS:         msgText = JSON.stringify(msg);
            let message_text = (|| {
                let json = capnweb_to_json(&message)?;
                serde_json::to_string(&json).map_err(|error| {
                    Error::Message(format!("failed to encode RPC message: {error}"))
                })
            })();
            let message_text = match message_text {
                Ok(message_text) => message_text,
                Err(error) => {
                    Self::abort(state, error.clone(), true);
                    return Err(error);
                }
            };
            // JS:       } catch (err) {
            // JS:         // If JSON stringification failed, there's something wrong with the devaluator, as it
            // JS:         // should not allow non-JSONable values to be injected in the first place.
            // JS:         try { this.abort(err); } catch (err2) {}
            // JS:         throw err;
            // JS:       }
            // JS:
            // JS:       try {
            // JS:         let sent = (this.transport as RpcTransport).send(msgText) as Promise<void> | undefined;
            match transport.send(TransportMessage::Text(message_text.clone())) {
                Ok(TransportSend::Done(_)) => {}
                Ok(TransportSend::Pending(pending)) => Self::observe_send(state, pending),
                Err(error) => Self::defer_abort(state, error),
            }
            // JS:         if (sent !== undefined && typeof sent.catch === "function") {
            // JS:           // If send fails, abort the connection, but don't try to send an abort message since
            // JS:           // that'll probably also fail.
            // JS:           sent.catch(err => this.abort(err, false));
            // JS:         }
            // JS:       } catch (err) {
            // JS:         // The transport threw synchronously. Treat it like an async send failure: abort the
            // JS:         // session (without trying to send an abort message over the broken transport), but
            // JS:         // defer to a microtask so the caller finishes its own bookkeeping first, matching the
            // JS:         // timing of a rejected promise from an async transport.
            // JS:         queueMicrotask(() => this.abort(err, false));
            // JS:       }
            // JS:       return msgText.length;
            Ok(Some(message_text.encode_utf16().count()))
        // JS:     } else {
        } else {
            // JS:       // Custom encoding transport encodes and returns the actual encoded size, or void if size
            // JS:       // is unavailable (e.g. structured clone).
            // JS:       try {
            // JS:         let size = (this.transport as RpcTransportWithCustomEncoding).send(msg);
            let size = match transport.send(TransportMessage::Value(message)) {
                Ok(TransportSend::Done(size)) => size,
                Ok(TransportSend::Pending(pending)) => {
                    Self::observe_send(state, pending);
                    None
                }
                Err(error) => {
                    Self::defer_abort(state, error);
                    None
                }
            };
            // JS:         if (typeof size === "number") {
            // JS:           return size;
            // JS:         }
            // JS:         // Defend against transports that return something other than a number, e.g. an
            // JS:         // accidentally-async `send()` returning a promise: treat the size as unknown, and
            // JS:         // observe any returned thenable so a rejection aborts the session rather than going
            // JS:         // unhandled. (The documented contract is to report errors via `receive()`.)
            // JS:         let thenable = size as unknown;
            // JS:         if (thenable && typeof (thenable as PromiseLike<unknown>).then === "function") {
            // JS:           Promise.resolve(thenable).catch(err => this.abort(err, false));
            // JS:         }
            // JS:         return undefined;
            // JS:       } catch (err) {
            // JS:         // Same as the synchronous failure case above.
            // JS:         queueMicrotask(() => this.abort(err, false));
            // JS:         return undefined;
            // JS:       }
            // JS:     }
            Ok(size)
        }
        // JS:   }
    }
    // JS:
    // JS:   sendCall(id: ImportId, path: PropertyPath, args?: RpcPayload): RpcImportHook {
    fn send_call(
        state: &Arc<Mutex<RpcSessionState>>,
        id: ImportId,
        path: PropertyPath,
        args: Option<RpcPayload>,
    ) -> Result<StubHook> {
        // JS:     if (this.abortReason) {
        if let Some(error) = state
            .lock()
            .expect("RPC session mutex poisoned")
            .abort_reason
            .clone()
        {
            // JS:       args?.dispose();
            drop(args);
            // JS:       throw this.abortReason;
            return Err(error);
            // JS:     }
        }
        // JS:
        // JS:     let value: Array<any> = ["pipeline", id, path];
        let mut value = vec![
            CapnwebValue::String("pipeline".to_owned()),
            CapnwebValue::Number(id as f64),
            path_to_value(path),
        ];
        // JS:     if (args) {
        if let Some(args) = args {
            // JS:       let devalue: unknown;
            // JS:       try {
            // JS:         devalue = Devaluator.devaluate(args.value, undefined, this, args, this.encodingLevel);
            let encoding_level = state
                .lock()
                .expect("RPC session mutex poisoned")
                .encoding_level;
            let mut session = RpcSessionImpl {
                state: Arc::clone(state),
            };
            let devalued =
                Devaluator::devaluate(&args.value, &mut session, Some(&args), encoding_level)?;
            // JS:       } catch (err) {
            // JS:         args.dispose();
            // JS:         throw err;
            // JS:       }
            // JS:
            // JS:       // HACK: Since the args is an array, devaluator will wrap in a second array. Need to unwrap.
            // JS:       // TODO: Clean this up somehow.
            // JS:       value.push((<Array<unknown>>devalue)[0]);
            let CapnwebValue::Array(mut wrapper) = devalued else {
                return Err(Error::Type(
                    "RPC argument payload was not an array".to_owned(),
                ));
            };
            value.push(wrapper.remove(0));
            // JS:
            // JS:       // Serializing the payload takes ownership of all stubs within, so the payload itself does
            // JS:       // not need to be disposed on success. If serialization threw partway through, disposing
            // JS:       // the payload above is safe: any hooks already exported were dups of the payload's hooks,
            // JS:       // and the Devaluator rolls back its own export-table entries before rethrowing.
            // JS:     }
        }
        // JS:     this.send(["push", value]);
        Self::send(state, tagged("push", [CapnwebValue::Array(value)]))?;
        // JS:
        // JS:     let entry = new ImportTableEntry(this, this.imports.length, false);
        let entry = {
            let mut session = state.lock().expect("RPC session mutex poisoned");
            let import_id = session.next_import_id;
            session.next_import_id += 1;
            let entry = Arc::new(Mutex::new(ImportTableEntry::new(
                state, session.id, import_id, false,
            )));
            // JS:     this.imports.push(entry);
            session.imports.insert(import_id, Arc::clone(&entry));
            entry
        };
        // JS:     return new RpcImportHook(/*isPromise=*/true, entry);
        Ok(Arc::new(RpcImportHook::new(true, entry)))
        // JS:   }
    }
    // JS:
    // JS:   sendStream(id: ImportId, path: PropertyPath, args: RpcPayload)
    // JS:       : {promise: Promise<void>, size: number} {
    fn send_stream(
        state: &Arc<Mutex<RpcSessionState>>,
        id: ImportId,
        path: PropertyPath,
        args: RpcPayload,
    ) -> Result<StreamCall> {
        // JS:     if (this.abortReason) {
        if let Some(error) = state
            .lock()
            .expect("RPC session mutex poisoned")
            .abort_reason
            .clone()
        {
            // JS:       args.dispose();
            drop(args);
            // JS:       throw this.abortReason;
            return Err(error);
            // JS:     }
        }
        // JS:
        // JS:     let value: Array<any> = ["pipeline", id, path];
        let mut value = vec![
            CapnwebValue::String("pipeline".to_owned()),
            CapnwebValue::Number(id as f64),
            path_to_value(path),
        ];
        // JS:     let devalue: unknown;
        // JS:     try {
        // JS:       devalue = Devaluator.devaluate(args.value, undefined, this, args, this.encodingLevel);
        let encoding_level = state
            .lock()
            .expect("RPC session mutex poisoned")
            .encoding_level;
        let mut session = RpcSessionImpl {
            state: Arc::clone(state),
        };
        let devalued =
            Devaluator::devaluate(&args.value, &mut session, Some(&args), encoding_level)?;
        // JS:     } catch (err) {
        // JS:       // Disposing the partially-serialized payload is safe for the same reasons as in sendCall().
        // JS:       args.dispose();
        // JS:       throw err;
        // JS:     }
        // JS:
        // JS:     // HACK: Since the args is an array, devaluator will wrap in a second array. Need to unwrap.
        // JS:     // TODO: Clean this up somehow.
        // JS:     value.push((<Array<unknown>>devalue)[0]);
        let CapnwebValue::Array(mut wrapper) = devalued else {
            return Err(Error::Type(
                "RPC stream arguments were not an array".to_owned(),
            ));
        };
        value.push(wrapper.remove(0));
        // JS:
        // JS:     let msg = ["stream", value];
        let message = tagged("stream", [CapnwebValue::Array(value)]);
        // JS:     let size = this.send(msg);
        let size = Self::send(state, message.clone())?;
        // JS:     if (size === undefined) {
        // JS:       size = estimateEncodedSize(msg);
        let size = size.unwrap_or_else(|| estimate_encoded_size(&message, 0));
        // JS:     }
        // JS:
        // JS:     // Create the import entry in "already pulling" state (pulling=true), since stream messages
        // JS:     // are automatically pulled. Set remoteRefcount to 0 so that resolve() won't send a release
        // JS:     // message — the server implicitly releases the export after sending the resolve. Set
        // JS:     // localRefcount to 1 so that resolve() doesn't treat this as already-disposed.
        // JS:     let importId = this.imports.length;
        let (import_id, entry) = {
            let mut session = state.lock().expect("RPC session mutex poisoned");
            let import_id = session.next_import_id;
            session.next_import_id += 1;
            // JS:     let entry = new ImportTableEntry(this, importId, /*pulling=*/true);
            let entry = Arc::new(Mutex::new(ImportTableEntry::new(
                state, session.id, import_id, true,
            )));
            // JS:     entry.remoteRefcount = 0;
            entry
                .lock()
                .expect("import entry mutex poisoned")
                .remote_refcount = 0;
            // JS:     entry.localRefcount = 1;
            entry
                .lock()
                .expect("import entry mutex poisoned")
                .local_refcount = 1;
            // JS:     this.imports.push(entry);
            session.imports.insert(import_id, Arc::clone(&entry));
            (import_id, entry)
        };
        // JS:
        // JS:     // Await the resolution, then dispose the result payload and clean up the import table entry.
        // JS:     // (Normally, sendRelease() cleans up the import table, but since remoteRefcount is 0, we
        // JS:     // need to do it manually.)
        // JS:     let promise = entry.awaitResolution().then(
        let weak_session = Arc::downgrade(state);
        let promise = Box::pin(async move {
            let result = ImportTableEntry::await_resolution(entry).await;
            // JS:       p => { p.dispose(); delete this.imports[importId]; },
            if let Ok(payload) = &result {
                let _ = payload;
            }
            // JS:       err => { delete this.imports[importId]; throw err; }
            if let Some(session) = weak_session.upgrade() {
                session
                    .lock()
                    .expect("RPC session mutex poisoned")
                    .imports
                    .remove(&import_id);
            }
            // JS:     );
            result.map(drop)
        });
        // JS:
        // JS:     return { promise, size };
        Ok(StreamCall {
            promise,
            size: Some(size),
        })
        // JS:   }
    }
    // JS:
    // JS:   sendMap(id: ImportId, path: PropertyPath, captures: StubHook[], instructions: unknown[])
    // JS:       : RpcImportHook {
    fn send_map(
        state: &Arc<Mutex<RpcSessionState>>,
        id: ImportId,
        path: PropertyPath,
        captures: Vec<StubHook>,
        instructions: MapInstructions,
    ) -> Result<StubHook> {
        // JS:     if (this.abortReason) {
        if let Some(error) = state
            .lock()
            .expect("RPC session mutex poisoned")
            .abort_reason
            .clone()
        {
            // JS:       for (let cap of captures) {
            // JS:         cap.dispose();
            // JS:       }
            // JS:       throw this.abortReason;
            return Err(error);
            // JS:     }
        }
        // JS:
        // JS:     // Note: This loop cannot throw synchronously after the abortReason check above. getImport()
        // JS:     // never throws, and exportStub()'s only throw is its own abortReason check — but aborts are
        // JS:     // always deferred to a microtask, so abortReason cannot become set partway through this loop.
        // JS:     // (Don't add a dispose() rollback here: exportStub() stores the hook itself, not a dup, in
        // JS:     // the export table, so disposing exported captures would corrupt the table.)
        // JS:     let devaluedCaptures = captures.map(hook => {
        let mut session = RpcSessionImpl {
            state: Arc::clone(state),
        };
        let mut devalued_captures = Vec::with_capacity(captures.len());
        for hook in captures {
            // JS:       let importId = this.getImport(hook);
            let import_id = session.get_import_impl(&hook);
            // JS:       if (importId !== undefined) {
            if let Some(import_id) = import_id {
                // JS:         return ["import", importId];
                devalued_captures.push(tagged("import", [CapnwebValue::Number(import_id as f64)]));
            // JS:       } else {
            } else {
                // JS:         return ["export", this.exportStub(hook)];
                let export_id = session.export_stub_impl(hook)?;
                devalued_captures.push(tagged("export", [CapnwebValue::Number(export_id as f64)]));
                // JS:       }
            }
            // JS:     });
        }
        // JS:
        // JS:     let value = ["remap", id, path, devaluedCaptures, instructions];
        let value = CapnwebValue::Array(vec![
            CapnwebValue::String("remap".to_owned()),
            CapnwebValue::Number(id as f64),
            path_to_value(path),
            CapnwebValue::Array(devalued_captures),
            CapnwebValue::Array(instructions),
        ]);
        // JS:
        // JS:     this.send(["push", value]);
        Self::send(state, tagged("push", [value]))?;
        // JS:
        // JS:     let entry = new ImportTableEntry(this, this.imports.length, false);
        let entry = {
            let mut session = state.lock().expect("RPC session mutex poisoned");
            let import_id = session.next_import_id;
            session.next_import_id += 1;
            let entry = Arc::new(Mutex::new(ImportTableEntry::new(
                state, session.id, import_id, false,
            )));
            // JS:     this.imports.push(entry);
            session.imports.insert(import_id, Arc::clone(&entry));
            entry
        };
        // JS:     return new RpcImportHook(/*isPromise=*/true, entry);
        Ok(Arc::new(RpcImportHook::new(true, entry)))
        // JS:   }
    }
    // JS:
    // JS:   sendPull(id: ImportId) {
    fn send_pull(state: &Arc<Mutex<RpcSessionState>>, id: ImportId) -> Result<()> {
        // JS:     if (this.abortReason) throw this.abortReason;
        if let Some(error) = state
            .lock()
            .expect("RPC session mutex poisoned")
            .abort_reason
            .clone()
        {
            return Err(error);
        }
        // JS:
        // JS:     this.send(["pull", id]);
        Self::send(state, tagged("pull", [CapnwebValue::Number(id as f64)]))?;
        // JS:   }
        Ok(())
    }
    // JS:
    // JS:   sendRelease(id: ImportId, remoteRefcount: number) {
    fn send_release(
        state: &Arc<Mutex<RpcSessionState>>,
        id: ImportId,
        remote_refcount: usize,
    ) -> Result<()> {
        // JS:     if (this.abortReason) return;
        if state
            .lock()
            .expect("RPC session mutex poisoned")
            .abort_reason
            .is_some()
        {
            return Ok(());
        }
        // JS:
        // JS:     this.send(["release", id, remoteRefcount]);
        Self::send(
            state,
            tagged(
                "release",
                [
                    CapnwebValue::Number(id as f64),
                    CapnwebValue::Number(remote_refcount as f64),
                ],
            ),
        )?;
        // JS:     delete this.imports[id];
        state
            .lock()
            .expect("RPC session mutex poisoned")
            .imports
            .remove(&id);
        // JS:   }
        Ok(())
    }
    // JS:
    // JS:   abort(error: any, trySendAbortMessage: boolean = true) {
    fn abort(state: &Arc<Mutex<RpcSessionState>>, error: Error, try_send_abort_message: bool) {
        // JS:     // Don't double-abort.
        // JS:     if (this.abortReason !== undefined) return;
        if state
            .lock()
            .expect("RPC session mutex poisoned")
            .abort_reason
            .is_some()
        {
            return;
        }
        // JS:
        // JS:     this.cancelReadLoop?.(error);
        // JS:     this.cancelReadLoop = undefined;
        // JS:
        // JS:     if (trySendAbortMessage) {
        if try_send_abort_message {
            // JS:       try {
            // JS:         let abortMsg = ["abort", Devaluator.devaluate(error, undefined, this, undefined, this.encodingLevel)];
            let value = CapnwebValue::Error(error.clone().into());
            let encoding_level = state
                .lock()
                .expect("RPC session mutex poisoned")
                .encoding_level;
            let mut session = RpcSessionImpl {
                state: Arc::clone(state),
            };
            if let Ok(value) = Devaluator::devaluate(&value, &mut session, None, encoding_level) {
                let _ = Self::send(state, tagged("abort", [value]));
            }
            // JS:         if (this.encodingLevel === "string") {
            // JS:           let sent = (this.transport as RpcTransport)
            // JS:               .send(JSON.stringify(abortMsg)) as Promise<void> | undefined;
            // JS:           if (sent !== undefined && typeof sent.catch === "function") {
            // JS:             sent.catch(err => {});
            // JS:           }
            // JS:         } else {
            // JS:           let result = (this.transport as RpcTransportWithCustomEncoding).send(abortMsg) as unknown;
            // JS:           if (result && typeof (result as PromiseLike<unknown>).then === "function") {
            // JS:             Promise.resolve(result).catch(err => {});
            // JS:           }
            // JS:         }
            // JS:       } catch (err) {
            // JS:         // ignore, probably the whole reason we're aborting is because the transport is broken
            // JS:       }
            // JS:     }
        }
        // JS:
        // JS:     if (error === undefined) {
        // JS:       // Shouldn't happen, but if it does, avoid setting `abortReason` to `undefined`.
        // JS:       error = "undefined";
        // JS:     }
        // JS:
        // JS:     this.abortReason = error;
        let (read_loop_abort, callbacks, imports, exports, transport, on_batch_done) = {
            let mut session = state.lock().expect("RPC session mutex poisoned");
            if session.abort_reason.is_some() {
                return;
            }
            session.abort_reason = Some(error.clone());
            (
                session.read_loop_abort.take(),
                std::mem::take(&mut session.on_broken_callbacks),
                session.imports.values().cloned().collect::<Vec<_>>(),
                std::mem::take(&mut session.exports),
                Arc::clone(&session.transport),
                session.on_batch_done.take(),
            )
        };
        if let Some(read_loop_abort) = read_loop_abort {
            read_loop_abort.abort();
        }
        // JS:     if (this.onBatchDone) {
        // JS:       this.onBatchDone.reject(error);
        if let Some(on_batch_done) = on_batch_done {
            on_batch_done.reject(error.clone());
        }
        // JS:     }
        // JS:
        // JS:     if (this.transport.abort) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            transport.abort(error.clone());
        }));
        // JS:       // Call transport's abort handler, but guard against buggy app code.
        // JS:       try {
        // JS:         this.transport.abort(error);
        // JS:       } catch (err) {
        // JS:         // Treat as unhandled rejection.
        // JS:         Promise.resolve(err);
        // JS:       }
        // JS:     }
        // JS:
        // JS:     // WATCH OUT: these are sparse arrays. `for/let/of` will iterate only positive indexes
        // JS:     // including deleted indexes -- bad. We need to use `for/let/in` instead.
        // JS:     for (let i in this.onBrokenCallbacks) {
        for callback in callbacks.into_iter().flatten() {
            // JS:       try {
            // JS:         this.onBrokenCallbacks[i](error);
            invoke_broken_callback(callback, error.clone());
            // JS:       } catch (err) {
            // JS:         // Treat as unhandled rejection.
            // JS:         Promise.resolve(err);
            // JS:       }
            // JS:     }
        }
        // JS:     for (let i in this.imports) {
        for entry in imports {
            // JS:       this.imports[i].abort(error);
            ImportTableEntry::abort(&entry, error.clone());
            // JS:     }
        }
        // JS:     for (let i in this.exports) {
        for (_, export) in exports {
            // JS:       this.exports[i].hook.dispose();
            drop(export);
            // JS:     }
        }
        // JS:   }
    }

    fn shutdown_state(state: &Arc<Mutex<RpcSessionState>>) {
        Self::abort(
            state,
            Error::Message("RPC session was shut down by disposing the main stub".to_owned()),
            false,
        );
    }
    // JS:
    // JS:   private async readLoop() {
    fn start_read_loop(state: &Arc<Mutex<RpcSessionState>>) {
        let weak_state = Arc::downgrade(state);
        let task = tokio::spawn(async move {
            // JS:     while (!this.abortReason) {
            loop {
                let Some(state) = weak_state.upgrade() else {
                    return;
                };
                if state
                    .lock()
                    .expect("RPC session mutex poisoned")
                    .abort_reason
                    .is_some()
                {
                    return;
                }
                // JS:       // Each receive needs its own abort promise so Promise.race() doesn't keep old reads.
                // JS:       let readCanceled = Promise.withResolvers<never>();
                // JS:       this.cancelReadLoop = readCanceled.reject;
                // JS:
                // JS:       let raw: unknown;
                // JS:
                // JS:       try {
                // JS:         raw = await Promise.race([this.transport.receive(), readCanceled.promise]);
                let transport =
                    Arc::clone(&state.lock().expect("RPC session mutex poisoned").transport);
                let raw = match transport.receive().await {
                    Ok(raw) => raw,
                    Err(error) => {
                        Self::abort(&state, error, true);
                        return;
                    }
                };
                // JS:       } finally {
                // JS:         if (this.cancelReadLoop === readCanceled.reject) {
                // JS:           this.cancelReadLoop = undefined;
                // JS:         }
                // JS:       }
                // JS:
                // JS:       // Bound a single string message before parsing it. At this point the transport has already
                // JS:       // buffered the complete message; true byte-level / pre-read enforcement belongs in the
                // JS:       // transport/socket. This backstop still prevents oversized messages from reaching JSON.parse
                // JS:       // and downstream deserialization work, and a throw here propagates out of readLoop and aborts
                // JS:       // the session. Only "string"-level transports hand us a measurable wire string; richer
                // JS:       // encoding levels deliver an already-decoded value, so the size cap does not apply.
                // JS:       if (this.encodingLevel === "string" &&
                // JS:           (raw as string).length > this.limits.maxMessageSize) {
                let (encoding, max_message_size) = {
                    let session = state.lock().expect("RPC session mutex poisoned");
                    (session.encoding_level, session.limits.max_message_size)
                };
                if let TransportMessage::Text(text) = &raw {
                    if text.encode_utf16().count() > max_message_size {
                        Self::abort(
                            &state,
                            Error::Type(format!(
                                "Incoming message exceeds maximum size of {max_message_size} UTF-16 code units."
                            )),
                            true,
                        );
                        return;
                    }
                }
                // JS:         throw new TypeError(
                // JS:             `Incoming message exceeds maximum size of ${this.limits.maxMessageSize} UTF-16 code ` +
                // JS:             `units.`);
                // JS:       }
                // JS:
                // JS:       if (this.abortReason) break;  // check again before processing
                if state
                    .lock()
                    .expect("RPC session mutex poisoned")
                    .abort_reason
                    .is_some()
                {
                    return;
                }
                // JS:
                // JS:       // Only parse JSON at "string" level; otherwise message is already an object
                // JS:       let msg = this.encodingLevel === "string" ? JSON.parse(raw as string) : raw;
                let message = match (encoding, raw) {
                    (EncodingLevel::String, TransportMessage::Text(text)) => {
                        serde_json::from_str(&text)
                            .map_err(|error| Error::Type(format!("invalid RPC JSON: {error}")))
                            .and_then(json_to_capnweb)
                    }
                    (EncodingLevel::String, _) => Err(Error::Type(
                        "string transport returned a non-string message".to_owned(),
                    )),
                    (_, TransportMessage::Value(value)) => Ok(value),
                    (_, _) => Err(Error::Type(
                        "custom transport returned a string message".to_owned(),
                    )),
                };
                let result = message.and_then(|message| Self::process_message(&state, message));
                if let Err(error) = result {
                    Self::abort(&state, error, true);
                    return;
                }
                // JS:
                // JS:       if (msg instanceof Array) {
                // JS:         switch (msg[0]) {
                // JS:           case "push":  // ["push", Expression]
                // JS:             if (msg.length > 1) {
                // JS:               let payload = new Evaluator(this, this.encodingLevel).evaluate(msg[1]);
                // JS:               let hook = new PayloadStubHook(payload);
                // JS:
                // JS:               // It's possible for a rejection to occur before the client gets a chance to send
                // JS:               // a "pull" message or to use the promise in a pipeline. We don't want that to be
                // JS:               // treated as an unhandled rejection on our end.
                // JS:               hook.ignoreUnhandledRejections();
                // JS:
                // JS:               this.exports.push({ hook, refcount: 1 });
                // JS:               continue;
                // JS:             }
                // JS:             break;
                // JS:
                // JS:           case "stream": {  // ["stream", Expression]
                // JS:             // Like "push", but:
                // JS:             // - Promise pipelining on the result is not supported.
                // JS:             // - The export is automatically considered "pulled".
                // JS:             // - Once the "resolve" is sent, the export is implicitly released.
                // JS:             if (msg.length > 1) {
                // JS:               let payload = new Evaluator(this, this.encodingLevel).evaluate(msg[1]);
                // JS:               let hook = new PayloadStubHook(payload);
                // JS:               hook.ignoreUnhandledRejections();
                // JS:
                // JS:               let exportId = this.exports.length;
                // JS:               this.exports.push({ hook, refcount: 1, autoRelease: true });
                // JS:
                // JS:               // Automatically pull since stream messages are always pulled.
                // JS:               this.ensureResolvingExport(exportId);
                // JS:               continue;
                // JS:             }
                // JS:             break;
                // JS:           }
                // JS:
                // JS:           case "pipe": {  // ["pipe"]
                // JS:             // Create a TransformStream. The writable end becomes the export (so the sender can
                // JS:             // write/close/abort it). The readable end is stashed for later retrieval via
                // JS:             // ["readable", importId].
                // JS:             let { readable, writable } = new TransformStream();
                // JS:             let hook = streamImpl.createWritableStreamHook(writable);
                // JS:             this.exports.push({ hook, refcount: 1, pipeReadable: readable });
                // JS:             continue;
                // JS:           }
                // JS:
                // JS:           case "pull": {  // ["pull", ImportId]
                // JS:             let exportId = msg[1];
                // JS:             if (typeof exportId == "number") {
                // JS:               this.ensureResolvingExport(exportId);
                // JS:               continue;
                // JS:             }
                // JS:             break;
                // JS:           }
                // JS:
                // JS:           case "resolve":   // ["resolve", ExportId, Expression]
                // JS:           case "reject": {  // ["reject", ExportId, Expression]
                // JS:             let importId = msg[1];
                // JS:             if (typeof importId == "number" && msg.length > 2) {
                // JS:               let imp = this.imports[importId];
                // JS:               if (imp) {
                // JS:                 if (msg[0] == "resolve") {
                // JS:                   imp.resolve(new PayloadStubHook(new Evaluator(this, this.encodingLevel).evaluate(msg[2])));
                // JS:                 } else {
                // JS:                   // HACK: We expect errors are always simple values (no stubs) so we can just
                // JS:                   //   pull the value out of the payload.
                // JS:                   let payload = new Evaluator(this, this.encodingLevel).evaluate(msg[2]);
                // JS:                   payload.dispose();  // just in case -- should be no-op
                // JS:                   imp.resolve(new ErrorStubHook(payload.value));
                // JS:                 }
                // JS:               } else {
                // JS:                 // Import ID is not found on the table. Probably we released it already, in which
                // JS:                 // case we do not care about the resolution, so whatever.
                // JS:
                // JS:                 if (msg[0] == "resolve") {
                // JS:                   // We need to evaluate the resolution and immediately dispose it so that we
                // JS:                   // release any stubs it contains.
                // JS:                   new Evaluator(this, this.encodingLevel).evaluate(msg[2]).dispose();
                // JS:                 }
                // JS:               }
                // JS:               continue;
                // JS:             }
                // JS:             break;
                // JS:           }
                // JS:
                // JS:           case "release": {
                // JS:             let exportId = msg[1];
                // JS:             let refcount = msg[2];
                // JS:             if (typeof exportId == "number" && typeof refcount == "number") {
                // JS:               this.releaseExport(exportId, refcount);
                // JS:               continue;
                // JS:             }
                // JS:             break;
                // JS:           }
                // JS:
                // JS:           case "abort": {
                // JS:             let payload = new Evaluator(this, this.encodingLevel).evaluate(msg[1]);
                // JS:             payload.dispose();  // just in case -- should be no-op
                // JS:             this.abort(payload.value, false);
                // JS:             break;
                // JS:           }
                // JS:         }
                // JS:       }
                // JS:
                // JS:       throw new Error(`bad RPC message: ${JSON.stringify(msg)}`);
                // JS:     }
            }
            // JS:   }
        });
        state
            .lock()
            .expect("RPC session mutex poisoned")
            .read_loop_abort = Some(task.abort_handle());
    }

    fn process_message(state: &Arc<Mutex<RpcSessionState>>, message: CapnwebValue) -> Result<()> {
        let CapnwebValue::Array(message) = message else {
            return Err(Error::Type("bad RPC message: expected array".to_owned()));
        };
        let Some(CapnwebValue::String(tag)) = message.first() else {
            return Err(Error::Type("bad RPC message: missing tag".to_owned()));
        };
        let encoding = state
            .lock()
            .expect("RPC session mutex poisoned")
            .encoding_level;
        match tag.as_str() {
            "push" | "stream" if message.len() > 1 => {
                let mut session = RpcSessionImpl {
                    state: Arc::clone(state),
                };
                let payload =
                    Evaluator::new(&mut session, encoding).evaluate(message[1].clone())?;
                let hook: StubHook = Arc::new(PayloadStubHook::new(payload));
                hook.ignore_unhandled_rejections();
                let (export_id, auto_release) = {
                    let mut session = state.lock().expect("RPC session mutex poisoned");
                    let export_id = session.next_incoming_export_id;
                    session.next_incoming_export_id += 1;
                    let auto_release = tag == "stream";
                    session.exports.insert(
                        export_id,
                        ExportTableEntry {
                            hook,
                            refcount: 1,
                            pulling: false,
                            auto_release,
                        },
                    );
                    (export_id, auto_release)
                };
                if auto_release {
                    Self::ensure_resolving_export(state, export_id)?;
                }
                Ok(())
            }
            "pipe" => Err(Error::UnsupportedBuiltin("TransformStream")),
            "pull" if message.len() > 1 => {
                let export_id = wire_integer(&message[1])
                    .ok_or_else(|| Error::Type("bad pull export ID".to_owned()))?;
                Self::ensure_resolving_export(state, export_id)
            }
            "resolve" | "reject" if message.len() > 2 => {
                let import_id = wire_integer(&message[1])
                    .ok_or_else(|| Error::Type("bad resolution import ID".to_owned()))?;
                let entry = state
                    .lock()
                    .expect("RPC session mutex poisoned")
                    .imports
                    .get(&import_id)
                    .cloned();
                if let Some(entry) = entry {
                    let mut session = RpcSessionImpl {
                        state: Arc::clone(state),
                    };
                    let payload =
                        Evaluator::new(&mut session, encoding).evaluate(message[2].clone())?;
                    let resolution: StubHook = if tag == "resolve" {
                        Arc::new(PayloadStubHook::new(payload))
                    } else {
                        Arc::new(ErrorStubHook::new(value_to_error(payload.value)))
                    };
                    ImportTableEntry::resolve(&entry, resolution)?;
                } else if tag == "resolve" {
                    let mut session = RpcSessionImpl {
                        state: Arc::clone(state),
                    };
                    drop(Evaluator::new(&mut session, encoding).evaluate(message[2].clone())?);
                }
                Ok(())
            }
            "release" if message.len() > 2 => {
                let export_id = wire_integer(&message[1])
                    .ok_or_else(|| Error::Type("bad release export ID".to_owned()))?;
                let refcount = wire_integer(&message[2])
                    .filter(|value| *value >= 0)
                    .ok_or_else(|| Error::Type("bad release refcount".to_owned()))?;
                Self::release_export(state, export_id, refcount as usize)
            }
            "abort" if message.len() > 1 => {
                let mut session = RpcSessionImpl {
                    state: Arc::clone(state),
                };
                let payload =
                    Evaluator::new(&mut session, encoding).evaluate(message[1].clone())?;
                Self::abort(state, value_to_error(payload.value), false);
                Ok(())
            }
            _ => Err(Error::Type(format!("bad RPC message tag: {tag}"))),
        }
    }
    // JS:
    // JS:   async drain(): Promise<void> {
    pub async fn drain(&self) -> Result<()> {
        // JS:     if (this.abortReason) {
        let promise = {
            let mut session = self.state.lock().expect("RPC session mutex poisoned");
            // JS:       throw this.abortReason;
            if let Some(error) = &session.abort_reason {
                return Err(error.clone());
            }
            // JS:     }
            // JS:
            // JS:     if (this.pullCount > 0) {
            // JS:       let {promise, resolve, reject} = Promise.withResolvers<void>();
            // JS:       this.onBatchDone = {resolve, reject};
            // JS:       await promise;
            if session.pull_count > 0 {
                let on_batch_done = PromiseWithResolvers::new();
                let promise = on_batch_done.promise();
                session.on_batch_done = Some(on_batch_done);
                Some(promise)
            } else {
                None
            }
            // JS:     }
        };
        if let Some(promise) = promise {
            promise.into_future().await?;
        }
        // JS:   }
        Ok(())
    }
    // JS:
    // JS:   getStats(): {imports: number, exports: number} {
    pub fn get_stats(&self) -> RpcSessionStats {
        // JS:     let result = {imports: 0, exports: 0};
        let session = self.state.lock().expect("RPC session mutex poisoned");
        // JS:     // We can't just use `.length` because the arrays can be sparse and can have negative indexes.
        // JS:     for (let i in this.imports) {
        // JS:       ++result.imports;
        // JS:     }
        // JS:     for (let i in this.exports) {
        // JS:       ++result.exports;
        // JS:     }
        // JS:     return result;
        RpcSessionStats {
            imports: session.imports.len(),
            exports: session.exports.len(),
        }
        // JS:   }
    }
    // JS: }
}

impl Exporter for RpcSessionImpl {
    fn export_stub(&mut self, hook: StubHook) -> Result<ExportId> {
        self.export_stub_impl(hook)
    }

    fn export_promise(&mut self, hook: StubHook) -> Result<ExportId> {
        self.export_promise_impl(hook)
    }

    fn get_import(&mut self, hook: &StubHook) -> Option<ImportId> {
        self.get_import_impl(hook)
    }

    fn unexport(&mut self, ids: Vec<ExportId>) -> Result<()> {
        self.unexport_impl(ids)
    }

    fn on_send_error(&mut self, error: CapnwebError) -> Option<CapnwebError> {
        self.on_send_error_impl(error)
    }
}

impl Importer for RpcSessionImpl {
    fn import_stub(&mut self, index: ImportId) -> Result<StubHook> {
        self.import_stub_impl(index)
    }

    fn import_promise(&mut self, index: ImportId) -> Result<StubHook> {
        self.import_promise_impl(index)
    }

    fn get_export(&mut self, index: ExportId) -> Option<StubHook> {
        self.get_export_impl(index)
    }

    fn get_limits(&self) -> RpcLimits {
        self.get_limits_impl()
    }
}
// JS:
// JS: // Public interface that wraps RpcSession and hides private implementation details (even from
// JS: // JavaScript with no type enforcement).
// JS: export class RpcSession {
/// Public owner for a running RPC session and its remote bootstrap stub.
pub struct RpcSession {
    // JS:   #session: RpcSessionImpl;
    session: RpcSessionImpl,
    // JS:   #mainStub: RpcStub;
    main_stub: RpcStub,
}

/// Current sparse import/export table occupancy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RpcSessionStats {
    /// Live imports.
    pub imports: usize,
    /// Live exports.
    pub exports: usize,
}
// JS:
// JS:   constructor(transport: AnyRpcTransport, localMain?: any, options: RpcSessionOptions = {}) {
impl RpcSession {
    /// Starts a public RPC session.
    pub fn new(
        transport: Arc<dyn AnyRpcTransport>,
        local_main: Option<CapnwebValue>,
        options: RpcSessionOptions,
    ) -> Result<Self> {
        // JS:     let mainHook: StubHook;
        // JS:     if (localMain) {
        // JS:       mainHook = new PayloadStubHook(RpcPayload.fromAppReturn(localMain));
        let main_hook: StubHook = if let Some(local_main) = local_main {
            Arc::new(PayloadStubHook::new(RpcPayload::from_app_return(
                local_main,
            )))
        // JS:     } else {
        } else {
            // JS:       mainHook = new ErrorStubHook(new Error("This connection has no main object."));
            Arc::new(ErrorStubHook::new(Error::Message(
                "This connection has no main object.".to_owned(),
            )))
            // JS:     }
        };
        // JS:     this.#session = new RpcSessionImpl(transport, mainHook, options);
        let session = RpcSessionImpl::new(transport, main_hook, options)?;
        // JS:     this.#mainStub = new RpcStub(this.#session.getMainImport());
        let main_stub = session.get_main_import()?;
        Ok(Self { session, main_stub })
        // JS:   }
    }
    // JS:
    // JS:   getRemoteMain(): RpcStub {
    pub fn get_remote_main(&self) -> RpcStub {
        // JS:     return this.#mainStub;
        self.main_stub.clone()
        // JS:   }
    }
    // JS:
    // JS:   getStats(): {imports: number, exports: number} {
    pub fn get_stats(&self) -> RpcSessionStats {
        // JS:     return this.#session.getStats();
        self.session.get_stats()
        // JS:   }
    }
    // JS:
    // JS:   drain(): Promise<void> {
    pub async fn drain(&self) -> Result<()> {
        // JS:     return this.#session.drain();
        self.session.drain().await
        // JS:   }
    }
    // JS: }
}
