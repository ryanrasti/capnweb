//! Rust framing boundary corresponding to the transport interfaces in `rpc.ts`.
//!
//! Parity status: partial; custom encoding levels and abort hooks remain.

mod io;
mod memory;
mod websocket;

use async_trait::async_trait;

pub use io::IoTransport;
pub use memory::{MemoryTransport, memory_transport_pair};
pub use websocket::WebSocketTransport;

use crate::Result;

/// Sending half of JS `RpcTransport`.
///
/// Difference from JS: splitting permits concurrent ownership by Tokio tasks.
#[async_trait]
pub trait TransportSender: Send {
    /// Sends one complete string-level message.
    async fn send(&mut self, message: String) -> Result<()>;
}

/// Receiving half of JS `RpcTransport`.
#[async_trait]
pub trait TransportReceiver: Send {
    /// Receives one complete message, or EOF.
    async fn receive(&mut self) -> Result<Option<String>>;
}

/// Splittable Rust equivalent of JS `RpcTransport`.
pub trait Transport: Send + 'static {
    /// Splits the transport into independently-owned halves.
    fn split(self) -> (Box<dyn TransportSender>, Box<dyn TransportReceiver>);
}
