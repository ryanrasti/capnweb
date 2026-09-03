// JS: // Copyright (c) 2025 Cloudflare, Inc.
// JS: // Licensed under the MIT license found in the LICENSE.txt file or at:
// JS: //     https://opensource.org/license/mit
// JS:
// JS: import { RpcStub } from "./core.js";
// JS: import { RpcTransportWithCustomEncoding, RpcSession, RpcSessionOptions } from "./rpc.js";
// JS:
// JS: // Start a MessagePort session given a MessagePort or a pair of MessagePorts.
// JS: //
// JS: // `localMain` is the main RPC interface to expose to the peer. Returns a stub for the main
// JS: // interface exposed from the peer.
// JS: export function newMessagePortRpcSession(
// JS:     port: MessagePort, localMain?: any, options?: RpcSessionOptions): RpcStub {
// JS:   let transport = new MessagePortTransport(port);
// JS:   let rpc = new RpcSession(transport, localMain, options);
// JS:   return rpc.getRemoteMain();
// JS: }
// JS:
// JS: class MessagePortTransport implements RpcTransportWithCustomEncoding {
// JS:   readonly encodingLevel = "structuredClonable" as const;
// JS:
// JS:   constructor (port: MessagePort) {
// JS:     this.#port = port;
// JS:
// JS:     // Start listening for messages
// JS:     port.start();
// JS:
// JS:     port.addEventListener("message", (event: MessageEvent<any>) => {
// JS:       if (this.#error) {
// JS:         // Ignore further messages.
// JS:       } else if (event.data === null) {
// JS:         // Peer is signaling that they're closing the connection
// JS:         this.#receivedError(new Error("Peer closed MessagePort connection."));
// JS:       } else {
// JS:         // Accept any structured-clonable data
// JS:         if (this.#receiveResolver) {
// JS:           this.#receiveResolver(event.data);
// JS:           this.#receiveResolver = undefined;
// JS:           this.#receiveRejecter = undefined;
// JS:         } else {
// JS:           this.#receiveQueue.push(event.data);
// JS:         }
// JS:       }
// JS:     });
// JS:
// JS:     port.addEventListener("messageerror", (event: MessageEvent) => {
// JS:       this.#receivedError(new Error("MessagePort message error."));
// JS:     });
// JS:   }
// JS:
// JS:   #port: MessagePort;
// JS:   #receiveResolver?: (message: unknown) => void;
// JS:   #receiveRejecter?: (err: any) => void;
// JS:   #receiveQueue: unknown[] = [];
// JS:   #error?: any;
// JS:
// JS:   send(message: unknown): void {
// JS:     if (this.#error) {
// JS:       throw this.#error;
// JS:     }
// JS:     this.#port.postMessage(message);
// JS:   }
// JS:
// JS:   async receive(): Promise<unknown> {
// JS:     if (this.#receiveQueue.length > 0) {
// JS:       return this.#receiveQueue.shift()!;
// JS:     } else if (this.#error) {
// JS:       throw this.#error;
// JS:     } else {
// JS:       return new Promise<unknown>((resolve, reject) => {
// JS:         this.#receiveResolver = resolve;
// JS:         this.#receiveRejecter = reject;
// JS:       });
// JS:     }
// JS:   }
// JS:
// JS:   abort(reason: any): void {
// JS:     // Send close signal to peer before closing
// JS:     try {
// JS:       this.#port.postMessage(null);
// JS:     } catch (err) {
// JS:       // Ignore errors when sending close signal - port might already be closed
// JS:     }
// JS:
// JS:     this.#port.close();
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
