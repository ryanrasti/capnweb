// JS: // Copyright (c) 2025 Cloudflare, Inc.
// JS: // Licensed under the MIT license found in the LICENSE.txt file or at:
// JS: //     https://opensource.org/license/mit
// JS:
// JS: /// <reference types="@cloudflare/workers-types" />
// JS:
// JS: import { RpcStub } from "./core.js";
// JS: import { RpcSession, RpcSessionOptions } from "./rpc.js";
// JS:
// JS: /** Close-frame reason max UTF-8 bytes: 125 payload - 2-byte status (RFC 6455 section 5.5). */
// JS: export const MAX_CLOSE_REASON_BYTES = 125 - 2;
// JS:
// JS: export function newWebSocketRpcSession(
// JS:     webSocket: WebSocket | string, localMain?: any, options?: RpcSessionOptions): RpcStub {
// JS:   if (typeof webSocket === "string") {
// JS:     webSocket = new WebSocket(webSocket);
// JS:   }
// JS:
// JS:   let transport = new WebSocketTransport(webSocket);
// JS:   let rpc = new RpcSession(transport, localMain, options);
// JS:   return rpc.getRemoteMain();
// JS: }
// JS:
// JS: /**
// JS:  * For use in Cloudflare Workers: Construct an HTTP response that starts a WebSocket RPC session
// JS:  * with the given `localMain`.
// JS:  */
// JS: export function newWorkersWebSocketRpcResponse(
// JS:     request: Request, localMain?: any, options?: RpcSessionOptions): Response {
// JS:   if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
// JS:     return new Response("This endpoint only accepts WebSocket requests.", { status: 400 });
// JS:   }
// JS:
// JS:   let pair = new WebSocketPair();
// JS:   let server = pair[0];
// JS:   server.accept()
// JS:   newWebSocketRpcSession(server, localMain, options);
// JS:   return new Response(null, {
// JS:     status: 101,
// JS:     webSocket: pair[1],
// JS:   });
// JS: }
// JS:
// JS: /**
// JS:  * Generic WebSocket transport. Default `T = string` is backward-compatible and satisfies
// JS:  * `RpcTransport`. Use `T = ArrayBuffer` as a building block for binary transports.
// JS:  */
// JS: export class WebSocketTransport<T extends string | ArrayBuffer = string> {
// JS:   constructor (webSocket: WebSocket) {
// JS:     this.#webSocket = webSocket;
// JS:
// JS:     // Always set binaryType — harmless for string mode, required for ArrayBuffer mode.
// JS:     webSocket.binaryType = "arraybuffer";
// JS:
// JS:     if (webSocket.readyState === WebSocket.CONNECTING) {
// JS:       this.#sendQueue = [];
// JS:       webSocket.addEventListener("open", event => {
// JS:         try {
// JS:           for (let message of this.#sendQueue!) {
// JS:             webSocket.send(message);
// JS:           }
// JS:         } catch (err) {
// JS:           this.#receivedError(err);
// JS:         }
// JS:         this.#sendQueue = undefined;
// JS:       });
// JS:     }
// JS:
// JS:     webSocket.addEventListener("message", (event: MessageEvent<any>) => {
// JS:       if (this.#error) {
// JS:         // Ignore further messages.
// JS:       } else if (typeof event.data === "string" || event.data instanceof ArrayBuffer) {
// JS:         if (this.#receiveResolver) {
// JS:           this.#receiveResolver(event.data as T);
// JS:           this.#receiveResolver = undefined;
// JS:           this.#receiveRejecter = undefined;
// JS:         } else {
// JS:           this.#receiveQueue.push(event.data as T);
// JS:         }
// JS:       } else {
// JS:         this.#receivedError(new TypeError("Received unexpected message type from WebSocket."));
// JS:       }
// JS:     });
// JS:
// JS:     webSocket.addEventListener("close", (event: CloseEvent) => {
// JS:       this.#receivedError(new Error(`Peer closed WebSocket: ${event.code} ${event.reason}`));
// JS:     });
// JS:
// JS:     webSocket.addEventListener("error", (event: Event) => {
// JS:       this.#receivedError(new Error(`WebSocket connection failed.`));
// JS:     });
// JS:   }
// JS:
// JS:   #webSocket: WebSocket;
// JS:   #sendQueue?: T[];  // only if not opened yet
// JS:   #receiveResolver?: (message: T) => void;
// JS:   #receiveRejecter?: (err: any) => void;
// JS:   #receiveQueue: T[] = [];
// JS:   #error?: any;
// JS:
// JS:   send(message: T): void {
// JS:     if (this.#sendQueue === undefined) {
// JS:       this.#webSocket.send(message);
// JS:     } else {
// JS:       // Not open yet, queue for later.
// JS:       this.#sendQueue.push(message);
// JS:     }
// JS:   }
// JS:
// JS:   receive(): Promise<T> {
// JS:     if (this.#receiveQueue.length > 0) {
// JS:       return Promise.resolve(this.#receiveQueue.shift()!);
// JS:     } else if (this.#error) {
// JS:       return Promise.reject(this.#error);
// JS:     } else {
// JS:       return new Promise<T>((resolve, reject) => {
// JS:         this.#receiveResolver = resolve;
// JS:         this.#receiveRejecter = reject;
// JS:       });
// JS:     }
// JS:   }
// JS:
// JS:   abort(reason: any): void {
// JS:     let message: string;
// JS:     if (reason instanceof Error) {
// JS:       message = reason.message;
// JS:     } else {
// JS:       message = `${reason}`;
// JS:     }
// JS:     // `stream: true` drops a trailing partial code point rather than emitting a replacement char.
// JS:     let reasonBytes = new TextEncoder().encode(message);
// JS:     if (reasonBytes.length > MAX_CLOSE_REASON_BYTES) {
// JS:       message = new TextDecoder().decode(reasonBytes.subarray(0, MAX_CLOSE_REASON_BYTES), { stream: true });
// JS:     }
// JS:     this.#webSocket.close(3000, message);
// JS:
// JS:     if (!this.#error) {
// JS:       this.#error = reason;
// JS:       // No need to call receiveRejecter(); RPC implementation will stop listening anyway.
// JS:     }
// JS:   }
// JS:
// JS:   #receivedError(reason: any) {
// JS:     if (!this.#error) {
// JS:       this.#error = reason;
// JS:       if (this.#receiveRejecter) {
// JS:         this.#receiveRejecter(reason);
// JS:         this.#receiveResolver = undefined;
// JS:         this.#receiveRejecter = undefined;
// JS:       }
// JS:     }
// JS:   }
// JS: }
// JS:
// JS: // This class is generic, so it can't `implements RpcTransport` (that would require every `T` to
// JS: // conform, but the ArrayBuffer instantiation intentionally doesn't). The default string
// JS: // instantiation's conformance is asserted in __type-tests__/rpc-types.test.ts.
