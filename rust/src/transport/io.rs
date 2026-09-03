//! Rust-specific newline-delimited transport with no direct JS counterpart.

use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

use super::{Transport, TransportReceiver, TransportSender};
use crate::Result;

/// Newline-delimited string transport used by interoperability tests.
pub struct IoTransport<R, W> {
    reader: R,
    writer: W,
}

impl<R, W> IoTransport<R, W> {
    /// Creates a transport from separate I/O halves.
    pub fn new(reader: R, writer: W) -> Self {
        Self { reader, writer }
    }
}

struct Sender<W>(W);
struct Receiver<R>(BufReader<R>);

#[async_trait]
impl<W: AsyncWrite + Unpin + Send> TransportSender for Sender<W> {
    async fn send(&mut self, message: String) -> Result<()> {
        self.0.write_all(message.as_bytes()).await?;
        self.0.write_all(b"\n").await?;
        self.0.flush().await?;
        Ok(())
    }
}

#[async_trait]
impl<R: AsyncRead + Unpin + Send> TransportReceiver for Receiver<R> {
    async fn receive(&mut self) -> Result<Option<String>> {
        let mut line = String::new();
        match self.0.read_line(&mut line).await? {
            0 => Ok(None),
            _ => {
                while line.ends_with(['\n', '\r']) {
                    line.pop();
                }
                Ok(Some(line))
            }
        }
    }
}

impl<R, W> Transport for IoTransport<R, W>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    fn split(self) -> (Box<dyn TransportSender>, Box<dyn TransportReceiver>) {
        (
            Box::new(Sender(self.writer)),
            Box::new(Receiver(BufReader::new(self.reader))),
        )
    }
}
