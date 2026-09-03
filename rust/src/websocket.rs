//! Counterpart of `src/websocket.ts`.
//!
//! Parity status: partial. [`crate::transport::WebSocketTransport`] provides
//! framed string transport, but the JS constructor helpers, close-code/reason
//! handling, and brokenness behavior are not yet at parity.

pub use crate::transport::WebSocketTransport;
