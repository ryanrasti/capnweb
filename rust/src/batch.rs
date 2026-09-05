// JS: // Copyright (c) 2025 Cloudflare, Inc.
// JS: // Licensed under the MIT license found in the LICENSE.txt file or at:
// JS: //     https://opensource.org/license/mit
// JS:
// JS: import { RpcStub } from "./core.js";
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use crate::core::{CapnwebValue, Error, Result, RpcStub};
// JS: import { RpcTransport, RpcSession, RpcSessionOptions } from "./rpc.js";
use crate::eager_promise::PromiseWithResolvers;
use crate::rpc::{AnyRpcTransport, RpcSession, RpcSessionOptions, TransportMessage, TransportSend};
// JS: import type { IncomingMessage, ServerResponse, OutgoingHttpHeader, OutgoingHttpHeaders } from "node:http";
// RUST-DEFERRED(JS built-in: node:http): Node request/response types are not represented.
// JS:
// JS: type SendBatchFunc = (batch: string[]) => Promise<string[]>;
type SendBatchFunc = Arc<
    dyn Fn(Vec<String>) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send>> + Send + Sync,
>;
// JS:
// JS: // Yield a full macrotask; prefer setImmediate because Node/Bun clamp setTimeout(0) to 1ms.
// JS: const yieldToMacrotask: () => Promise<void> =
async fn yield_to_macrotask() {
    // JS:   typeof setImmediate === "function"
    // JS:     ? () => new Promise(resolve => setImmediate(resolve))
    // JS:     : () => new Promise(resolve => setTimeout(resolve, 0));
    tokio::task::yield_now().await;
}
// JS:
// JS: class BatchClientTransport implements RpcTransport {
struct BatchClientTransport {
    state: Mutex<BatchClientState>,
    scheduled: PromiseWithResolvers<()>,
}

struct BatchClientState {
    aborted: Option<Error>,
    batch_to_send: Option<Vec<String>>,
    batch_to_receive: Option<Vec<String>>,
}
// JS:   constructor(sendBatch: SendBatchFunc) {
impl BatchClientTransport {
    fn new(send_batch: SendBatchFunc) -> Arc<Self> {
        // JS:     this.#promise = this.#scheduleBatch(sendBatch);
        let transport = Arc::new(Self {
            state: Mutex::new(BatchClientState {
                aborted: None,
                batch_to_send: Some(Vec::new()),
                batch_to_receive: None,
            }),
            scheduled: PromiseWithResolvers::new(),
        });
        let scheduled_transport = Arc::clone(&transport);
        tokio::spawn(async move {
            scheduled_transport.schedule_batch(send_batch).await;
        });
        transport
        // JS:   }
    }
}
// JS:
// JS:   #promise: Promise<void>;
// JS:   #aborted: any;
// JS:
// JS:   #batchToSend: string[] | null = [];
// JS:   #batchToReceive: string[] | null = null;
// JS:
impl AnyRpcTransport for BatchClientTransport {
    // JS:   send(message: string): void {
    fn send(&self, message: TransportMessage) -> Result<TransportSend> {
        let TransportMessage::Text(message) = message else {
            return Err(Error::Type("batch transport expected text".to_owned()));
        };
        // JS:     // If the batch was already sent, we just ignore the message, because throwing may cause the
        // JS:     // RPC system to abort prematurely. Once the last receive() is done then we'll throw an error
        // JS:     // that aborts the RPC system at the right time and will propagate to all other requests.
        // JS:     if (this.#batchToSend !== null) {
        if let Some(batch) = &mut self
            .state
            .lock()
            .expect("batch client mutex poisoned")
            .batch_to_send
        {
            // JS:       this.#batchToSend.push(message);
            batch.push(message);
            // JS:     }
        }
        Ok(TransportSend::Done(None))
        // JS:   }
    }
    // JS:
    // JS:   async receive(): Promise<string> {
    fn receive(
        self: Arc<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<TransportMessage>> + Send + 'static>> {
        Box::pin(async move {
            // JS:     if (!this.#batchToReceive) {
            if self
                .state
                .lock()
                .expect("batch client mutex poisoned")
                .batch_to_receive
                .is_none()
            {
                // JS:       await this.#promise;
                self.scheduled.promise().into_future().await?;
                // JS:     }
            }
            // JS:
            // JS:     let msg = this.#batchToReceive!.shift();
            let message = {
                let mut state = self.state.lock().expect("batch client mutex poisoned");
                state.batch_to_receive.as_mut().and_then(|batch| {
                    if batch.is_empty() {
                        None
                    } else {
                        Some(batch.remove(0))
                    }
                })
            };
            // JS:     if (msg !== undefined) {
            if let Some(message) = message {
                // JS:       return msg;
                Ok(TransportMessage::Text(message))
            // JS:     } else {
            } else {
                // JS:       // No more messages. An error thrown here will propagate out of any calls that are still
                // JS:       // open.
                // JS:       throw new Error("Batch RPC request ended.");
                Err(Error::Message("Batch RPC request ended.".to_owned()))
                // JS:     }
            }
            // JS:   }
        })
    }
    // JS:
    // JS:   abort?(reason: any): void {
    fn abort(&self, reason: Error) {
        // JS:     this.#aborted = reason;
        self.state
            .lock()
            .expect("batch client mutex poisoned")
            .aborted = Some(reason);
        // JS:   }
    }
}

impl BatchClientTransport {
    // JS:
    // JS:   async #scheduleBatch(sendBatch: SendBatchFunc) {
    async fn schedule_batch(&self, send_batch: SendBatchFunc) {
        // JS:     // Wait for microtask queue to clear before sending a batch.
        // JS:     //
        // JS:     // Note that simply waiting for one turn of the microtask queue (await Promise.resolve()) is
        // JS:     // not good enough here as the application needs a chance to call `.then()` on every RPC
        // JS:     // promise in order to explicitly indicate they want the results. Unfortunately, `await`ing
        // JS:     // a thenable does not call `.then()` immediately -- for some reason it waits for a turn of
        // JS:     // the microtask queue first, *then* calls `.then()`.
        // JS:     await yieldToMacrotask();
        yield_to_macrotask().await;
        // JS:
        // JS:     if (this.#aborted !== undefined) {
        let batch = {
            let mut state = self.state.lock().expect("batch client mutex poisoned");
            if let Some(error) = &state.aborted {
                // JS:       throw this.#aborted;
                self.scheduled.reject(error.clone());
                return;
                // JS:     }
            }
            // JS:
            // JS:     let batch = this.#batchToSend!;
            let batch = state.batch_to_send.take().unwrap_or_default();
            // JS:     this.#batchToSend = null;
            batch
        };
        // JS:     this.#batchToReceive = await sendBatch(batch);
        match send_batch(batch).await {
            Ok(received) => {
                self.state
                    .lock()
                    .expect("batch client mutex poisoned")
                    .batch_to_receive = Some(received);
                self.scheduled.resolve(());
            }
            Err(error) => self.scheduled.reject(error),
        }
        // JS:   }
    }
}
// JS: }
// JS:
// JS: export function newHttpBatchRpcSession(
// JS:     urlOrRequest: string | Request, options?: RpcSessionOptions): RpcStub {
/// Starts a client HTTP batch session against `url`.
pub fn new_http_batch_rpc_session(url: &str, options: RpcSessionOptions) -> Result<RpcStub> {
    // JS:   let sendBatch: SendBatchFunc = async (batch: string[]) => {
    let url = url.to_owned();
    let send_batch: SendBatchFunc = Arc::new(move |batch| {
        let url = url.clone();
        Box::pin(async move {
            // JS:     let response = await fetch(urlOrRequest, {
            // JS:       method: "POST",
            // JS:       body: batch.join("\n"),
            // JS:     });
            let (status, status_text, body) = http_post(&url, &batch.join("\n")).await?;
            // JS:
            // JS:     if (!response.ok) {
            if !(200..300).contains(&status) {
                // JS:       response.body?.cancel();
                // JS:       throw new Error(`RPC request failed: ${response.status} ${response.statusText}`);
                return Err(Error::Message(format!(
                    "RPC request failed: {status} {status_text}"
                )));
                // JS:     }
            }
            // JS:
            // JS:     let body = await response.text();
            // JS:     return body == "" ? [] : body.split("\n");
            Ok(if body.is_empty() {
                Vec::new()
            } else {
                body.split('\n').map(str::to_owned).collect()
            })
        })
    });
    // JS:   };
    // JS:
    // JS:   let transport = new BatchClientTransport(sendBatch);
    let transport = BatchClientTransport::new(send_batch);
    // JS:   let rpc = new RpcSession(transport, undefined, options);
    let rpc = RpcSession::new(transport, None, options)?;
    // JS:   return rpc.getRemoteMain();
    Ok(rpc.get_remote_main())
    // JS: }
}
// JS:
// JS: class BatchServerTransport implements RpcTransport {
struct BatchServerTransport {
    batch_to_send: Mutex<Vec<String>>,
    batch_to_receive: Mutex<Vec<String>>,
    all_received: PromiseWithResolvers<()>,
}
// JS:   constructor(batch: string[]) {
impl BatchServerTransport {
    fn new(batch: Vec<String>) -> Arc<Self> {
        // JS:     this.#batchToReceive = batch;
        Arc::new(Self {
            batch_to_send: Mutex::new(Vec::new()),
            batch_to_receive: Mutex::new(batch),
            all_received: PromiseWithResolvers::new(),
        })
        // JS:   }
    }
}
// JS:
// JS:   #batchToSend: string[] = [];
// JS:   #batchToReceive: string[];
// JS:   #allReceived: PromiseWithResolvers<void> = Promise.withResolvers<void>();
// JS:
impl AnyRpcTransport for BatchServerTransport {
    // JS:   send(message: string): void {
    fn send(&self, message: TransportMessage) -> Result<TransportSend> {
        let TransportMessage::Text(message) = message else {
            return Err(Error::Type("batch transport expected text".to_owned()));
        };
        // JS:     this.#batchToSend.push(message);
        self.batch_to_send
            .lock()
            .expect("batch server mutex poisoned")
            .push(message);
        Ok(TransportSend::Done(None))
        // JS:   }
    }
    // JS:
    // JS:   async receive(): Promise<string> {
    fn receive(
        self: Arc<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<TransportMessage>> + Send + 'static>> {
        Box::pin(async move {
            // JS:     let msg = this.#batchToReceive!.shift();
            let message = {
                let mut batch = self
                    .batch_to_receive
                    .lock()
                    .expect("batch server mutex poisoned");
                if batch.is_empty() {
                    None
                } else {
                    Some(batch.remove(0))
                }
            };
            // JS:     if (msg !== undefined) {
            if let Some(message) = message {
                // JS:       return msg;
                Ok(TransportMessage::Text(message))
            // JS:     } else {
            } else {
                // JS:       // No more messages.
                // JS:       this.#allReceived.resolve();
                self.all_received.resolve(());
                // JS:       return new Promise(r => {});
                std::future::pending().await
                // JS:     }
            }
            // JS:   }
        })
    }
    // JS:
    // JS:   abort?(reason: any): void {
    fn abort(&self, reason: Error) {
        // JS:     this.#allReceived.reject(reason);
        self.all_received.reject(reason);
        // JS:   }
    }
}

impl BatchServerTransport {
    // JS:
    // JS:   whenAllReceived() {
    fn when_all_received(&self) -> impl Future<Output = Result<()>> {
        // JS:     return this.#allReceived.promise;
        self.all_received.promise().into_future()
        // JS:   }
    }
    // JS:
    // JS:   getResponseBody(): string {
    fn get_response_body(&self) -> String {
        // JS:     return this.#batchToSend.join("\n");
        self.batch_to_send
            .lock()
            .expect("batch server mutex poisoned")
            .join("\n")
        // JS:   }
    }
}
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
/// Serves one HTTP batch request without depending on the Fetch `Request`/`Response` types.
pub async fn new_http_batch_rpc_response(
    method: &str,
    body: &str,
    local_main: CapnwebValue,
    options: RpcSessionOptions,
) -> Result<(u16, String)> {
    // JS:   if (request.method !== "POST") {
    if method != "POST" {
        // JS:     return new Response("This endpoint only accepts POST requests.", { status: 405 });
        return Ok((405, "This endpoint only accepts POST requests.".to_owned()));
        // JS:   }
    }
    // JS:
    // JS:   let body = await request.text();
    // JS:   let batch = body === "" ? [] : body.split("\n");
    let batch = if body.is_empty() {
        Vec::new()
    } else {
        body.split('\n').map(str::to_owned).collect()
    };
    // JS:
    // JS:   let transport = new BatchServerTransport(batch);
    let transport = BatchServerTransport::new(batch);
    // JS:   let rpc = new RpcSession(transport, localMain, options);
    let rpc = RpcSession::new(
        Arc::clone(&transport) as Arc<dyn AnyRpcTransport>,
        Some(local_main),
        options,
    )?;
    // JS:
    // JS:   // TODO: Arguably we should arrange so any attempts to pull promise resolutions from the client
    // JS:   //   will reject rather than just hang. But it IS valid to make server->client calls in order to
    // JS:   //   then pipeline the result into something returned to the client. We don't want the errors to
    // JS:   //   prematurely cancel anything that would eventually complete. So for now we just say, it's the
    // JS:   //   app's responsibility to not wait on any server -> client calls since they will never
    // JS:   //   complete.
    // JS:
    // JS:   await transport.whenAllReceived();
    transport.when_all_received().await?;
    // JS:   await rpc.drain();
    rpc.drain().await?;
    // JS:
    // JS:   // TODO: Ask RpcSession to dispose everything it is still holding on to?
    // JS:
    // JS:   return new Response(transport.getResponseBody());
    Ok((200, transport.get_response_body()))
    // JS: }
}
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
// RUST-DEFERRED(JS built-in: node:http): Node IncomingMessage/ServerResponse are not represented.
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

fn parse_http_url(url: &str) -> Result<(String, u16, String)> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| Error::Type("HTTP batch sessions require an http:// URL".to_owned()))?;
    let (host_port, path) = rest.split_once('/').unwrap_or((rest, ""));
    let path = if path.is_empty() {
        "/".to_owned()
    } else {
        format!("/{path}")
    };
    let (host, port) = if let Some((host, port)) = host_port.split_once(':') {
        (
            host.to_owned(),
            port.parse::<u16>()
                .map_err(|_| Error::Type("invalid HTTP port".to_owned()))?,
        )
    } else {
        (host_port.to_owned(), 80)
    };
    Ok((host, port, path))
}

async fn http_post(url: &str, body: &str) -> Result<(u16, String, String)> {
    http_request("POST", url, body).await
}

/// Sends one HTTP/1.1 request and returns status, reason, and body.
pub async fn http_request(method: &str, url: &str, body: &str) -> Result<(u16, String, String)> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (host, port, path) = parse_http_url(url)?;
    let mut stream = tokio::net::TcpStream::connect((host.as_str(), port))
        .await
        .map_err(|error| Error::Message(format!("HTTP connect failed: {error}")))?;
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|error| Error::Message(format!("HTTP write failed: {error}")))?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .map_err(|error| Error::Message(format!("HTTP read failed: {error}")))?;
    let (header, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| Error::Type("HTTP response missing header terminator".to_owned()))?;
    let status_line = header
        .lines()
        .next()
        .ok_or_else(|| Error::Type("HTTP response missing status line".to_owned()))?;
    let mut parts = status_line.splitn(3, ' ');
    let _version = parts.next();
    let status = parts
        .next()
        .and_then(|status| status.parse::<u16>().ok())
        .ok_or_else(|| Error::Type("HTTP response missing status code".to_owned()))?;
    let reason = parts.next().unwrap_or("").to_owned();
    Ok((status, reason, body.trim_end_matches('\n').to_owned()))
}
