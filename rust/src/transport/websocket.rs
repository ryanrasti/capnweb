//! String-frame portion of `src/websocket.ts`.
//!
//! Parity status: partial; session helpers and close lifecycle remain.

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

use super::{Transport, TransportReceiver, TransportSender};
use crate::{Error, Result};

/// String-level Cap'n Web transport over an established WebSocket.
pub struct WebSocketTransport<S> {
    socket: WebSocketStream<S>,
}

impl<S> WebSocketTransport<S> {
    /// Wraps an established socket.
    pub fn new(socket: WebSocketStream<S>) -> Self {
        Self { socket }
    }
}

struct Sender<S>(futures::stream::SplitSink<WebSocketStream<S>, Message>);
struct Receiver<S>(futures::stream::SplitStream<WebSocketStream<S>>);

#[async_trait]
impl<S: AsyncRead + AsyncWrite + Unpin + Send> TransportSender for Sender<S> {
    async fn send(&mut self, message: String) -> Result<()> {
        self.0
            .send(Message::Text(message.into()))
            .await
            .map_err(|error| Error::Io(error.to_string()))
    }
}

#[async_trait]
impl<S: AsyncRead + AsyncWrite + Unpin + Send> TransportReceiver for Receiver<S> {
    async fn receive(&mut self) -> Result<Option<String>> {
        loop {
            let Some(message) = self.0.next().await else {
                return Ok(None);
            };
            match message.map_err(|error| Error::Io(error.to_string()))? {
                Message::Text(message) => return Ok(Some(message.to_string())),
                Message::Close(_) => return Ok(None),
                Message::Ping(_) | Message::Pong(_) => {}
                Message::Binary(_) | Message::Frame(_) => {
                    return Err(Error::Protocol(
                        "Cap'n Web expects WebSocket text frames".into(),
                    ));
                }
            }
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> Transport for WebSocketTransport<S> {
    fn split(self) -> (Box<dyn TransportSender>, Box<dyn TransportReceiver>) {
        let (sender, receiver) = self.socket.split();
        (Box::new(Sender(sender)), Box::new(Receiver(receiver)))
    }
}
