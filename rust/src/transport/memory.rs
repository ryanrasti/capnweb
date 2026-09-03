use async_trait::async_trait;
use tokio::sync::mpsc;

use super::{Transport, TransportReceiver, TransportSender};
use crate::{Error, Result};

/// One endpoint of an in-memory string transport.
pub struct MemoryTransport {
    outgoing: mpsc::UnboundedSender<String>,
    incoming: mpsc::UnboundedReceiver<String>,
}

struct Sender(mpsc::UnboundedSender<String>);
struct Receiver(mpsc::UnboundedReceiver<String>);

/// Creates two connected in-memory endpoints.
pub fn memory_transport_pair() -> (MemoryTransport, MemoryTransport) {
    let (a_tx, a_rx) = mpsc::unbounded_channel();
    let (b_tx, b_rx) = mpsc::unbounded_channel();
    (
        MemoryTransport {
            outgoing: a_tx,
            incoming: b_rx,
        },
        MemoryTransport {
            outgoing: b_tx,
            incoming: a_rx,
        },
    )
}

#[async_trait]
impl TransportSender for Sender {
    async fn send(&mut self, message: String) -> Result<()> {
        self.0.send(message).map_err(|_| Error::Disconnected)
    }
}

#[async_trait]
impl TransportReceiver for Receiver {
    async fn receive(&mut self) -> Result<Option<String>> {
        Ok(self.0.recv().await)
    }
}

impl Transport for MemoryTransport {
    fn split(self) -> (Box<dyn TransportSender>, Box<dyn TransportReceiver>) {
        (
            Box::new(Sender(self.outgoing)),
            Box::new(Receiver(self.incoming)),
        )
    }
}
