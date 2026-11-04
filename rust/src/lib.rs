// JS: // Copyright (c) 2025 Cloudflare, Inc.
// JS: // Licensed under the MIT license found in the LICENSE.txt file or at:
// JS: //     https://opensource.org/license/mit
// JS:
// JS: import { RpcTarget as RpcTargetImpl, RpcStub as RpcStubImpl, RpcPromise as RpcPromiseImpl } from "./core.js";
mod core;
mod eager_promise;
mod typed;
// JS: import { serialize, deserialize, EncodingLevel } from "./serialize.js";
mod serialize;
// JS: import { RpcTransport, RpcTransportWithCustomEncoding, AnyRpcTransport, RpcSession as RpcSessionImpl, RpcSessionOptions } from "./rpc.js";
mod rpc;
// JS: import { RpcLimits, DEFAULT_LIMITS, DEFAULT_MAX_DEPTH } from "./serialize.js";
pub use capnweb_macros::{FromCapnwebValue, IntoCapnwebValue, rpc_interface, rpc_target};
pub use core::{
    CapnwebError, CapnwebValue, CustomMessageCodec, CustomMessageContext, CustomTypeHandler,
    CustomTypeRegistry, CustomValue, Error, IntoPropertyPath, MapInstructions, MaybePromise,
    PayloadStubHook, PromiseStubHook, PropertyKey, PropertyPath, Result, RpcPayload, RpcPromise,
    RpcStub, RpcTarget, RpcTargetInner, StreamCall, StubHook, StubHookInner, function_target,
};
pub use num_bigint::BigInt;
pub use rpc::{
    AnyRpcTransport, RpcSession, RpcSessionImpl, RpcSessionOptions, RpcSessionStats,
    TransportMessage, TransportSend,
};
pub use serialize::{
    DEFAULT_LIMITS, DEFAULT_MAX_DEPTH, EncodingLevel, ExportId, Exporter, ImportId, Importer,
    RpcLimits, deserialize, serialize,
};
pub use typed::{
    Bytes, FromCapnwebValue, IntoCapnwebValue, RpcInterfaceDescriptor, TypedRpcPromise,
};

// JS: import { RpcTargetBranded, RpcCompatible, Stub, ElideStub, PayloadOrStub,
// JS:          type RpcPromise as RpcPromiseType, __RPC_TARGET_BRAND } from "./types.js";
// JS: import { newWebSocketRpcSession as newWebSocketRpcSessionImpl,
// JS:          newWorkersWebSocketRpcResponse, WebSocketTransport } from "./websocket.js";
mod websocket;
pub use websocket::{MAX_CLOSE_REASON_BYTES, WebSocketTransport, new_websocket_rpc_session};
// JS: import { newHttpBatchRpcSession as newHttpBatchRpcSessionImpl,
// JS:          newHttpBatchRpcResponse, nodeHttpBatchRpcResponse } from "./batch.js";
mod batch;
pub use batch::{http_request, new_http_batch_rpc_response, new_http_batch_rpc_session};
// JS: import { newMessagePortRpcSession as newMessagePortRpcSessionImpl } from "./messageport.js";
// JS: import { forceInitMap } from "./map.js";
mod map;
pub use map::force_init_map;
// JS: import { forceInitStreams } from "./streams.js";
mod streams;
pub use streams::{FlowController, SendToken};
pub mod channel;
pub use channel::{BYTE_CHANNEL_VERSION, ByteChannel, ByteChannelSession};
mod framed_stream;
pub use framed_stream::{
    DEFAULT_MAX_FRAME_BYTES, FramedStreamTransport, FramedStreamTransportBuilder,
    TransportAbortHandler,
};
#[cfg(unix)]
mod uds;
#[cfg(unix)]
pub use uds::UdsTransport;
// JS:
// JS: forceInitMap();
// JS: forceInitStreams();
// JS:
// JS: // Re-export public API types.
// JS: export { serialize, deserialize, newWorkersWebSocketRpcResponse, newHttpBatchRpcResponse,
// JS:          nodeHttpBatchRpcResponse, WebSocketTransport, DEFAULT_LIMITS, DEFAULT_MAX_DEPTH };
// JS: export type { RpcTransport, RpcTransportWithCustomEncoding, AnyRpcTransport,
// JS:          RpcSessionOptions, RpcCompatible, EncodingLevel, RpcLimits };
// JS:
// JS: // Hack the type system to make RpcStub's types work nicely!
// JS: /**
// JS:  * Represents a reference to a remote object, on which methods may be remotely invoked via RPC.
// JS:  *
// JS:  * `RpcStub` can represent any interface (when using TypeScript, you pass the specific interface
// JS:  * type as `T`, but this isn't known at runtime). The way this works is, `RpcStub` is actually a
// JS:  * `Proxy`. It makes itself appear as if every possible method / property name is defined. You can
// JS:  * invoke any method name, and the invocation will be sent to the server. If it turns out that no
// JS:  * such method exists on the remote object, an exception is thrown back. But the client does not
// JS:  * actually know, until that point, what methods exist.
// JS:  */
// JS: export type RpcStub<T extends RpcCompatible<T>> = Stub<T>;
// JS: export const RpcStub: {
// JS:   new <T extends RpcCompatible<T>>(value: T): RpcStub<T>;
// JS: } = <any>RpcStubImpl;
// JS:
// JS: /**
// JS:  * Represents the result of an RPC call.
// JS:  *
// JS:  * Also used to represent properties. That is, `stub.foo` evaluates to an `RpcPromise` for the
// JS:  * value of `foo`.
// JS:  *
// JS:  * This isn't actually a JavaScript `Promise`. It does, however, have `then()`, `catch()`, and
// JS:  * `finally()` methods, like `Promise` does, and because it has a `then()` method, JavaScript will
// JS:  * allow you to treat it like a promise, e.g. you can `await` it.
// JS:  *
// JS:  * An `RpcPromise` is also a proxy, just like `RpcStub`, where calling methods or awaiting
// JS:  * properties will make a pipelined network request.
// JS:  *
// JS:  * Note that and `RpcPromise` is "lazy": the actual final result is not requested from the server
// JS:  * until you actually `await` the promise (or call `then()`, etc. on it). This is an optimization:
// JS:  * if you only intend to use the promise for pipelining and you never await it, then there's no
// JS:  * need to transmit the resolution!
// JS:  *
// JS:  * You may also construct an `RpcPromise` yourself from a regular `Promise`, using
// JS:  * `new RpcPromise(promise)`, allowing you to perform promise pipelining on a local promise. This
// JS:  * is semantically identical to creating a local-loopback RPC that returns the promise, and then
// JS:  * invoking it: pipelined calls wait until the promise resolves, then are delivered, in order, to
// JS:  * the resolution. This is useful when you plan to obtain some stub in the future, but want to
// JS:  * allow code to start queuing calls on it immediately. Note that the `RpcPromise` takes
// JS:  * ownership of the resolution: disposing it disposes the resolution, so resolve the promise
// JS:  * with a `dup()` if you also intend to keep the stub.
// JS:  */
// JS: export type RpcPromise<T extends RpcCompatible<T>> = RpcPromiseType<T>;
// JS: export const RpcPromise: {
// JS:   // The return type applies `ElideStub` — the same transformation `Result` applies to a
// JS:   // declared stub return — so constructing from a promised stub produces exactly the type a
// JS:   // method returning that stub would. See `PayloadOrStub` for what the promise may resolve to.
// JS:   //
// JS:   // Two overloads, for inference reasons. A context-sensitive argument — e.g.
// JS:   // `Promise.resolve({f() { ... }})`, where the method's return type must be inferred — is
// JS:   // contextually typed against the first overload only, and a contextual type containing a
// JS:   // `Stub` arm collapses such an argument's inference. The first overload therefore keeps its
// JS:   // parameter a plain `Promise<T>`. Since `PayloadOrStub`'s stub arm is `NoInfer` anyway, both
// JS:   // overloads infer identically; the second one matters only when `T` is explicitly annotated
// JS:   // and the payload is a stub, e.g. `new RpcPromise<Counter>(promiseOfStub)`.
// JS:   new <T extends RpcCompatible<T>>(value: Promise<T>): RpcPromiseType<ElideStub<T>>;
// JS:   new <T extends RpcCompatible<T>>(value: Promise<PayloadOrStub<T>>): RpcPromiseType<ElideStub<T>>;
// JS: } = <any>RpcPromiseImpl;
// JS:
// JS: /**
// JS:  * Use to construct an `RpcSession` on top of a custom `RpcTransport`.
// JS:  *
// JS:  * Most people won't use this. You only need it if you've implemented your own `RpcTransport`.
// JS:  */
// JS: export interface RpcSession<T extends RpcCompatible<T> = undefined> {
// JS:   getRemoteMain(): RpcStub<T>;
// JS:   getStats(): {imports: number, exports: number};
// JS:
// JS:   // Waits until the peer is not waiting on any more promise resolutions from us. This is useful
// JS:   // in particular to decide when a batch is complete.
// JS:   drain(): Promise<void>;
// JS: }
// JS: export const RpcSession: {
// JS:   new <T extends RpcCompatible<T> = undefined>(
// JS:       transport: AnyRpcTransport, localMain?: any, options?: RpcSessionOptions): RpcSession<T>;
// JS: } = <any>RpcSessionImpl;
// JS:
// JS: // RpcTarget needs some hackage too to brand it properly and account for the implementation
// JS: // conditionally being imported from "cloudflare:workers".
// JS: /**
// JS:  * Classes which are intended to be passed by reference and called over RPC must extend
// JS:  * `RpcTarget`. A class which does not extend `RpcTarget` (and which doesn't have built-in support
// JS:  * from the RPC system) cannot be passed in an RPC message at all; an exception will be thrown.
// JS:  *
// JS:  * Note that on Cloudflare Workers, this `RpcTarget` is an alias for the one exported from the
// JS:  * "cloudflare:workers" module, so they can be used interchangably.
// JS:  */
// JS: export interface RpcTarget extends RpcTargetBranded {};
// JS: export const RpcTarget: {
// JS:   new(): RpcTarget;
// JS: } = RpcTargetImpl;
// JS:
// JS: /**
// JS:  * Empty interface used as default type parameter for sessions where the other side doesn't
// JS:  * necessarily export a main interface.
// JS:  */
// JS: interface Empty {}
// JS:
// JS: /**
// JS:  * Start a WebSocket session given either an already-open WebSocket or a URL.
// JS:  *
// JS:  * @param webSocket Either the `wss://` URL to connect to, or an already-open WebSocket object to
// JS:  * use.
// JS:  * @param localMain The main RPC interface to expose to the peer. Returns a stub for the main
// JS:  * interface exposed from the peer.
// JS:  */
// JS: export let newWebSocketRpcSession:<T extends RpcCompatible<T> = Empty>
// JS:     (webSocket: WebSocket | string, localMain?: any, options?: RpcSessionOptions) => RpcStub<T> =
// JS:     <any>newWebSocketRpcSessionImpl;
// JS:
// JS: /**
// JS:  * Initiate an HTTP batch session from the client side.
// JS:  *
// JS:  * The parameters to this method have exactly the same signature as `fetch()`, but the return
// JS:  * value is an RpcStub. You can customize anything about the request except for the method
// JS:  * (it will always be set to POST) and the body (which the RPC system will fill in).
// JS:  */
// JS: export let newHttpBatchRpcSession:<T extends RpcCompatible<T>>
// JS:     (urlOrRequest: string | Request, options?: RpcSessionOptions) => RpcStub<T> =
// JS:     <any>newHttpBatchRpcSessionImpl;
// JS:
// JS: /**
// JS:  * Initiate an RPC session over a MessagePort, which is particularly useful for communicating
// JS:  * between an iframe and its parent frame in a browser context. Each side should call this function
// JS:  * on its own end of the MessageChannel.
// JS:  */
// JS: export let newMessagePortRpcSession:<T extends RpcCompatible<T> = Empty>
// JS:     (port: MessagePort, localMain?: any, options?: RpcSessionOptions) => RpcStub<T> =
// JS:     <any>newMessagePortRpcSessionImpl;
// JS:
// JS: /**
// JS:  * Implements unified handling of HTTP-batch and WebSocket responses for the Cloudflare Workers
// JS:  * Runtime.
// JS:  *
// JS:  * SECURITY WARNING: This function accepts cross-origin requests. If you do not want this, you
// JS:  * should validate the `Origin` header before calling this, or use `newHttpBatchRpcSession()` and
// JS:  * `newWebSocketRpcSession()` directly with appropriate security measures for each type of request.
// JS:  * But if your API uses in-band authorization (i.e. it has an RPC method that takes the user's
// JS:  * credentials as parameters and returns the authorized API), then cross-origin requests should
// JS:  * be safe.
// JS:  */
// JS: export async function newWorkersRpcResponse(
// JS:     request: Request, localMain: any, options?: RpcSessionOptions) {
// JS:   if (request.method === "POST") {
// JS:     let response = await newHttpBatchRpcResponse(request, localMain, options);
// JS:     // Since we're exposing the same API over WebSocket, too, and WebSocket always allows
// JS:     // cross-origin requests, the API necessarily must be safe for cross-origin use (e.g. because
// JS:     // it uses in-band authorization, as recommended in the readme). So, we might as well allow
// JS:     // batch requests to be made cross-origin as well.
// JS:     response.headers.set("Access-Control-Allow-Origin", "*");
// JS:     return response;
// JS:   } else if (request.headers.get("Upgrade")?.toLowerCase() === "websocket") {
// JS:     return newWorkersWebSocketRpcResponse(request, localMain, options);
// JS:   } else {
// JS:     return new Response("This endpoint only accepts POST or WebSocket requests.", { status: 400 });
// JS:   }
// JS: }
