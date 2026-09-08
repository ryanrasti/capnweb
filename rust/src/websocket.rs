// JS: // Copyright (c) 2025 Cloudflare, Inc.
// JS: // Licensed under the MIT license found in the LICENSE.txt file or at:
// JS: //     https://opensource.org/license/mit
// JS:
// JS: /// <reference types="@cloudflare/workers-types" />
// JS:
// JS: import { RpcStub } from "./core.js";
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

use crate::core::{CapnwebValue, Error, Result, RpcStub};
// JS: import { RpcSession, RpcSessionOptions } from "./rpc.js";
use crate::rpc::{AnyRpcTransport, RpcSession, RpcSessionOptions, TransportMessage, TransportSend};
// JS:
// JS: /** Close-frame reason max UTF-8 bytes: 125 payload - 2-byte status (RFC 6455 section 5.5). */
// JS: export const MAX_CLOSE_REASON_BYTES = 125 - 2;
/// Close-frame reason max UTF-8 bytes: 125 payload - 2-byte status (RFC 6455 section 5.5).
pub const MAX_CLOSE_REASON_BYTES: usize = 125 - 2;
// JS:
// JS: export function newWebSocketRpcSession(
// JS:     webSocket: WebSocket | string, localMain?: any, options?: RpcSessionOptions): RpcStub {
/// Starts a WebSocket RPC session from a `ws://` URL.
pub async fn new_websocket_rpc_session(
    web_socket: &str,
    local_main: Option<CapnwebValue>,
    options: RpcSessionOptions,
) -> Result<RpcStub> {
    // JS:   if (typeof webSocket === "string") {
    // JS:     webSocket = new WebSocket(webSocket);
    let transport = WebSocketTransport::connect(web_socket).await?;
    // JS:   }
    // JS:
    // JS:   let transport = new WebSocketTransport(webSocket);
    // JS:   let rpc = new RpcSession(transport, localMain, options);
    let rpc = RpcSession::new(transport, local_main, options)?;
    // JS:   return rpc.getRemoteMain();
    Ok(rpc.get_remote_main())
    // JS: }
}
// JS:
// JS: /**
// JS:  * For use in Cloudflare Workers: Construct an HTTP response that starts a WebSocket RPC session
// JS:  * with the given `localMain`.
// JS:  */
// JS: export function newWorkersWebSocketRpcResponse(
// JS:     request: Request, localMain?: any, options?: RpcSessionOptions): Response {
// RUST-DEFERRED(JS built-in: WebSocketPair): Workers WebSocket upgrade responses are not represented.
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
/// String WebSocket transport backing an RPC session.
pub struct WebSocketTransport {
    outgoing: tokio::sync::mpsc::UnboundedSender<Message>,
    incoming: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<Result<String>>>,
    error: Mutex<Option<Error>>,
}

impl WebSocketTransport {
    fn attach<S>(stream: WebSocketStream<S>) -> Arc<Self>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (mut sink, mut stream) = stream.split();
        let (outgoing, mut outgoing_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (incoming_sender, incoming) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    outgoing = outgoing_receiver.recv() => {
                        let Some(message) = outgoing else { break; };
                        if sink.send(message).await.is_err() {
                            break;
                        }
                    }
                    incoming = stream.next() => {
                        match incoming {
                            Some(Ok(Message::Text(text))) => {
                                if incoming_sender.send(Ok(text.to_string())).is_err() {
                                    break;
                                }
                            }
                            Some(Ok(Message::Ping(payload))) => {
                                let _ = sink.send(Message::Pong(payload)).await;
                            }
                            Some(Ok(Message::Pong(_))) | Some(Ok(Message::Frame(_))) => {}
                            Some(Ok(Message::Binary(_))) => {
                                let _ = incoming_sender.send(Err(Error::Type(
                                    "Received unexpected message type from WebSocket.".to_owned(),
                                )));
                                break;
                            }
                            Some(Ok(Message::Close(frame))) => {
                                let (code, reason) = frame
                                    .map(|frame| (u16::from(frame.code), frame.reason.to_string()))
                                    .unwrap_or((1005, String::new()));
                                let _ = incoming_sender.send(Err(Error::Message(format!(
                                    "Peer closed WebSocket: {code} {reason}"
                                ))));
                                break;
                            }
                            Some(Err(error)) => {
                                let _ = incoming_sender.send(Err(Error::Message(format!(
                                    "WebSocket connection failed: {error}"
                                ))));
                                break;
                            }
                            None => {
                                let _ = incoming_sender.send(Err(Error::Message(
                                    "Peer closed WebSocket: 1005 ".to_owned(),
                                )));
                                break;
                            }
                        }
                    }
                }
            }
        });
        Arc::new(Self {
            outgoing,
            incoming: tokio::sync::Mutex::new(incoming),
            error: Mutex::new(None),
        })
    }

    /// Connects a client-side WebSocket transport to `url`.
    pub async fn connect(url: &str) -> Result<Arc<Self>> {
        // Cap'n Web RPC consists of small request/response messages. Leaving
        // Nagle enabled can pair badly with delayed ACKs and add ~40 ms to
        // every round trip, so request TCP_NODELAY from tokio-tungstenite.
        let (stream, _) = tokio_tungstenite::connect_async_with_config(url, None, true)
            .await
            .map_err(|error| Error::Message(format!("WebSocket connection failed: {error}")))?;
        Ok(Self::attach(stream))
    }

    /// Accepts a server-side WebSocket on an already-connected TCP stream.
    pub async fn accept(stream: tokio::net::TcpStream) -> Result<Arc<Self>> {
        stream
            .set_nodelay(true)
            .map_err(|error| Error::Message(format!("Failed to set TCP_NODELAY: {error}")))?;
        let stream = tokio_tungstenite::accept_async(stream)
            .await
            .map_err(|error| Error::Message(format!("WebSocket accept failed: {error}")))?;
        Ok(Self::attach(stream))
    }
}
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
impl AnyRpcTransport for WebSocketTransport {
    // JS:   send(message: T): void {
    fn send(&self, message: TransportMessage) -> Result<TransportSend> {
        let TransportMessage::Text(message) = message else {
            return Err(Error::Type("WebSocket transport expected text".to_owned()));
        };
        // JS:     if (this.#sendQueue === undefined) {
        // JS:       this.#webSocket.send(message);
        self.outgoing
            .send(Message::Text(message.into()))
            .map_err(|_| Error::Message("WebSocket send failed".to_owned()))?;
        Ok(TransportSend::Done(None))
        // JS:     } else {
        // JS:       // Not open yet, queue for later.
        // JS:       this.#sendQueue.push(message);
        // JS:     }
    }
    // JS:   }
    // JS:
    // JS:   receive(): Promise<T> {
    fn receive(
        self: Arc<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<TransportMessage>> + Send + 'static>> {
        Box::pin(async move {
            // JS:     if (this.#receiveQueue.length > 0) {
            if let Some(error) = self.error.lock().expect("websocket mutex poisoned").clone() {
                return Err(error);
            }
            match self.incoming.lock().await.recv().await {
                Some(Ok(message)) => Ok(TransportMessage::Text(message)),
                // JS:       return Promise.resolve(this.#receiveQueue.shift()!);
                // JS:     } else if (this.#error) {
                Some(Err(error)) => {
                    *self.error.lock().expect("websocket mutex poisoned") = Some(error.clone());
                    Err(error)
                }
                // JS:       return Promise.reject(this.#error);
                // JS:     } else {
                None => {
                    let error = Error::Message("Peer closed WebSocket: 1005 ".to_owned());
                    *self.error.lock().expect("websocket mutex poisoned") = Some(error.clone());
                    Err(error)
                }
            }
            // JS:       return new Promise<T>((resolve, reject) => {
            // JS:         this.#receiveResolver = resolve;
            // JS:         this.#receiveRejecter = reject;
            // JS:       });
        })
        // JS:     }
    }
    // JS:   }
    // JS:
    // JS:   abort(reason: any): void {
    fn abort(&self, reason: Error) {
        // JS:     let message: string;
        let mut message = reason.to_string();
        // JS:     if (reason instanceof Error) {
        // JS:       message = reason.message;
        // JS:     } else {
        // JS:       message = `${reason}`;
        // JS:     }
        // JS:     // `stream: true` drops a trailing partial code point rather than emitting a replacement char.
        // JS:     let reasonBytes = new TextEncoder().encode(message);
        // JS:     if (reasonBytes.length > MAX_CLOSE_REASON_BYTES) {
        if message.len() > MAX_CLOSE_REASON_BYTES {
            // JS:       message = new TextDecoder().decode(reasonBytes.subarray(0, MAX_CLOSE_REASON_BYTES), { stream: true });
            let mut end = MAX_CLOSE_REASON_BYTES;
            while !message.is_char_boundary(end) {
                end -= 1;
            }
            message.truncate(end);
            // JS:     }
        }
        // JS:     this.#webSocket.close(3000, message);
        let _ = self.outgoing.send(Message::Close(Some(
            tokio_tungstenite::tungstenite::protocol::CloseFrame {
                code: 3000.into(),
                reason: message.into(),
            },
        )));
        // JS:
        // JS:     if (!this.#error) {
        let mut error = self.error.lock().expect("websocket mutex poisoned");
        if error.is_none() {
            // JS:       this.#error = reason;
            *error = Some(reason);
            // JS:       // No need to call receiveRejecter(); RPC implementation will stop listening anyway.
            // JS:     }
        }
    }
    // JS:   }
}
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
