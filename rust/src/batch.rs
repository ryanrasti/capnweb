// JS: // Copyright (c) 2025 Cloudflare, Inc.
// JS: // Licensed under the MIT license found in the LICENSE.txt file or at:
// JS: //     https://opensource.org/license/mit
// JS:
// JS: import { RpcStub } from "./core.js";
// JS: import { RpcTransport, RpcSession, RpcSessionOptions } from "./rpc.js";
// JS: import type { IncomingMessage, ServerResponse, OutgoingHttpHeader, OutgoingHttpHeaders } from "node:http";
// JS:
// JS: type SendBatchFunc = (batch: string[]) => Promise<string[]>;
// JS:
// JS: // Yield a full macrotask; prefer setImmediate because Node/Bun clamp setTimeout(0) to 1ms.
// JS: const yieldToMacrotask: () => Promise<void> =
// JS:   typeof setImmediate === "function"
// JS:     ? () => new Promise(resolve => setImmediate(resolve))
// JS:     : () => new Promise(resolve => setTimeout(resolve, 0));
// JS:
// JS: class BatchClientTransport implements RpcTransport {
// JS:   constructor(sendBatch: SendBatchFunc) {
// JS:     this.#promise = this.#scheduleBatch(sendBatch);
// JS:   }
// JS:
// JS:   #promise: Promise<void>;
// JS:   #aborted: any;
// JS:
// JS:   #batchToSend: string[] | null = [];
// JS:   #batchToReceive: string[] | null = null;
// JS:
// JS:   send(message: string): void {
// JS:     // If the batch was already sent, we just ignore the message, because throwing may cause the
// JS:     // RPC system to abort prematurely. Once the last receive() is done then we'll throw an error
// JS:     // that aborts the RPC system at the right time and will propagate to all other requests.
// JS:     if (this.#batchToSend !== null) {
// JS:       this.#batchToSend.push(message);
// JS:     }
// JS:   }
// JS:
// JS:   async receive(): Promise<string> {
// JS:     if (!this.#batchToReceive) {
// JS:       await this.#promise;
// JS:     }
// JS:
// JS:     let msg = this.#batchToReceive!.shift();
// JS:     if (msg !== undefined) {
// JS:       return msg;
// JS:     } else {
// JS:       // No more messages. An error thrown here will propagate out of any calls that are still
// JS:       // open.
// JS:       throw new Error("Batch RPC request ended.");
// JS:     }
// JS:   }
// JS:
// JS:   abort?(reason: any): void {
// JS:     this.#aborted = reason;
// JS:   }
// JS:
// JS:   async #scheduleBatch(sendBatch: SendBatchFunc) {
// JS:     // Wait for microtask queue to clear before sending a batch.
// JS:     //
// JS:     // Note that simply waiting for one turn of the microtask queue (await Promise.resolve()) is
// JS:     // not good enough here as the application needs a chance to call `.then()` on every RPC
// JS:     // promise in order to explicitly indicate they want the results. Unfortunately, `await`ing
// JS:     // a thenable does not call `.then()` immediately -- for some reason it waits for a turn of
// JS:     // the microtask queue first, *then* calls `.then()`.
// JS:     await yieldToMacrotask();
// JS:
// JS:     if (this.#aborted !== undefined) {
// JS:       throw this.#aborted;
// JS:     }
// JS:
// JS:     let batch = this.#batchToSend!;
// JS:     this.#batchToSend = null;
// JS:     this.#batchToReceive = await sendBatch(batch);
// JS:   }
// JS: }
// JS:
// JS: export function newHttpBatchRpcSession(
// JS:     urlOrRequest: string | Request, options?: RpcSessionOptions): RpcStub {
// JS:   let sendBatch: SendBatchFunc = async (batch: string[]) => {
// JS:     let response = await fetch(urlOrRequest, {
// JS:       method: "POST",
// JS:       body: batch.join("\n"),
// JS:     });
// JS:
// JS:     if (!response.ok) {
// JS:       response.body?.cancel();
// JS:       throw new Error(`RPC request failed: ${response.status} ${response.statusText}`);
// JS:     }
// JS:
// JS:     let body = await response.text();
// JS:     return body == "" ? [] : body.split("\n");
// JS:   };
// JS:
// JS:   let transport = new BatchClientTransport(sendBatch);
// JS:   let rpc = new RpcSession(transport, undefined, options);
// JS:   return rpc.getRemoteMain();
// JS: }
// JS:
// JS: class BatchServerTransport implements RpcTransport {
// JS:   constructor(batch: string[]) {
// JS:     this.#batchToReceive = batch;
// JS:   }
// JS:
// JS:   #batchToSend: string[] = [];
// JS:   #batchToReceive: string[];
// JS:   #allReceived: PromiseWithResolvers<void> = Promise.withResolvers<void>();
// JS:
// JS:   send(message: string): void {
// JS:     this.#batchToSend.push(message);
// JS:   }
// JS:
// JS:   async receive(): Promise<string> {
// JS:     let msg = this.#batchToReceive!.shift();
// JS:     if (msg !== undefined) {
// JS:       return msg;
// JS:     } else {
// JS:       // No more messages.
// JS:       this.#allReceived.resolve();
// JS:       return new Promise(r => {});
// JS:     }
// JS:   }
// JS:
// JS:   abort?(reason: any): void {
// JS:     this.#allReceived.reject(reason);
// JS:   }
// JS:
// JS:   whenAllReceived() {
// JS:     return this.#allReceived.promise;
// JS:   }
// JS:
// JS:   getResponseBody(): string {
// JS:     return this.#batchToSend.join("\n");
// JS:   }
// JS: }
// JS:
// JS: /**
// JS:  * Implements the server end of an HTTP batch session, using standard Fetch API types to represent
// JS:  * HTTP requests and responses.
// JS:  *
// JS:  * @param request The request received from the client initiating the session.
// JS:  * @param localMain The main stub or RpcTarget which the server wishes to expose to the client.
// JS:  * @param options Optional RPC session options.
// JS:  * @returns The HTTP response to return to the client. Note that the returned object has mutable
// JS:  *     headers, so you can modify them using e.g. `response.headers.set("Foo", "bar")`.
// JS:  */
// JS: export async function newHttpBatchRpcResponse(
// JS:     request: Request, localMain: any, options?: RpcSessionOptions): Promise<Response> {
// JS:   if (request.method !== "POST") {
// JS:     return new Response("This endpoint only accepts POST requests.", { status: 405 });
// JS:   }
// JS:
// JS:   let body = await request.text();
// JS:   let batch = body === "" ? [] : body.split("\n");
// JS:
// JS:   let transport = new BatchServerTransport(batch);
// JS:   let rpc = new RpcSession(transport, localMain, options);
// JS:
// JS:   // TODO: Arguably we should arrange so any attempts to pull promise resolutions from the client
// JS:   //   will reject rather than just hang. But it IS valid to make server->client calls in order to
// JS:   //   then pipeline the result into something returned to the client. We don't want the errors to
// JS:   //   prematurely cancel anything that would eventually complete. So for now we just say, it's the
// JS:   //   app's responsibility to not wait on any server -> client calls since they will never
// JS:   //   complete.
// JS:
// JS:   await transport.whenAllReceived();
// JS:   await rpc.drain();
// JS:
// JS:   // TODO: Ask RpcSession to dispose everything it is still holding on to?
// JS:
// JS:   return new Response(transport.getResponseBody());
// JS: }
// JS:
// JS: /**
// JS:  * Implements the server end of an HTTP batch session using traditional Node.js HTTP APIs.
// JS:  *
// JS:  * @param request The request received from the client initiating the session.
// JS:  * @param response The response object, to which the response should be written.
// JS:  * @param localMain The main stub or RpcTarget which the server wishes to expose to the client.
// JS:  * @param options Optional RPC session options. You can also pass headers to set on the response.
// JS:  */
// JS: export async function nodeHttpBatchRpcResponse(
// JS:     request: IncomingMessage, response: ServerResponse,
// JS:     localMain: any,
// JS:     options?: RpcSessionOptions & {
// JS:       headers?: OutgoingHttpHeaders | OutgoingHttpHeader[],
// JS:     }): Promise<void> {
// JS:   if (request.method !== "POST") {
// JS:     response.writeHead(405, "This endpoint only accepts POST requests.", options?.headers);
// JS:     response.end();
// JS:     return;
// JS:   }
// JS:
// JS:   let body = await new Promise<string>((resolve, reject) => {
// JS:     let chunks: Buffer[] = [];
// JS:     request.on("data", chunk => {
// JS:       chunks.push(chunk);
// JS:     });
// JS:     request.on("end", () => {
// JS:       resolve(Buffer.concat(chunks).toString());
// JS:     });
// JS:     request.on("error", reject);
// JS:   });
// JS:   let batch = body === "" ? [] : body.split("\n");
// JS:
// JS:   let transport = new BatchServerTransport(batch);
// JS:   let rpc = new RpcSession(transport, localMain, options);
// JS:
// JS:   await transport.whenAllReceived();
// JS:   await rpc.drain();
// JS:
// JS:   response.writeHead(200, options?.headers);
// JS:   response.end(transport.getResponseBody());
// JS: }
