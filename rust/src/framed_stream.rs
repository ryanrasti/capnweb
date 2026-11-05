//! Bounded length-prefixed Cap'n Web transport over a Tokio duplex stream.

use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Mutex as AsyncMutex, watch};

use crate::channel::ByteChannelSession;
use crate::core::{
    CustomMessageCodec, CustomMessageContext, CustomTypeHandler, CustomTypeRegistry, Error, Result,
};
use crate::rpc::{AnyRpcTransport, TransportMessage, TransportSend};
use crate::serialize::{EncodingLevel, capnweb_to_json, json_to_capnweb};

/// Default maximum encoded frame size: one MiB.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 1024 * 1024;

trait FramedRead: AsyncRead + Send + Unpin {}
impl<T> FramedRead for T where T: AsyncRead + Send + Unpin {}

trait FramedWrite: AsyncWrite + Send + Unpin {}
impl<T> FramedWrite for T where T: AsyncWrite + Send + Unpin {}

/// Receives notification when a control transport becomes unusable.
pub trait TransportAbortHandler: Send + Sync + 'static {
    /// Aborts resources whose lifetime is bounded by the control transport.
    fn transport_aborted(&self, reason: Error);
}

impl TransportAbortHandler for ByteChannelSession {
    fn transport_aborted(&self, reason: Error) {
        ByteChannelSession::abort(self, reason);
    }
}

/// Builder for a [`FramedStreamTransport`].
pub struct FramedStreamTransportBuilder {
    max_frame_bytes: usize,
    registry: CustomTypeRegistry,
    abort_handlers: Vec<Arc<dyn TransportAbortHandler>>,
}

impl Default for FramedStreamTransportBuilder {
    fn default() -> Self {
        Self {
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            registry: CustomTypeRegistry::new(),
            abort_handlers: Vec::new(),
        }
    }
}

impl FramedStreamTransportBuilder {
    /// Creates a builder with the default one-MiB frame limit.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum JSON payload size, excluding the four-byte prefix.
    pub fn max_frame_bytes(mut self, max_frame_bytes: usize) -> Self {
        self.max_frame_bytes = max_frame_bytes;
        self
    }

    /// Registers a transport-defined custom value handler.
    pub fn custom_type(mut self, handler: Arc<dyn CustomTypeHandler>) -> Result<Self> {
        self.registry.register(handler)?;
        Ok(self)
    }

    /// Registers a resource owner that must be aborted with this transport.
    pub fn abort_handler(mut self, handler: Arc<dyn TransportAbortHandler>) -> Self {
        self.abort_handlers.push(handler);
        self
    }

    /// Registers byte-channel encoding and binds all channels to this control transport.
    pub fn byte_channels(mut self, channels: Arc<ByteChannelSession>) -> Result<Self> {
        self.registry.register(channels.custom_type_handler())?;
        self.abort_handlers.push(channels);
        Ok(self)
    }

    /// Wraps and splits a Tokio duplex stream.
    pub fn build<S>(self, stream: S) -> Result<FramedStreamTransport>
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        if self.max_frame_bytes == 0 || self.max_frame_bytes > u32::MAX as usize {
            return Err(Error::Type(
                "framed stream maximum must be in 1..=u32::MAX".to_owned(),
            ));
        }
        let (reader, writer) = tokio::io::split(stream);
        let (abort_sender, _) = watch::channel(None);
        Ok(FramedStreamTransport {
            state: Arc::new(FramedState {
                reader: Arc::new(AsyncMutex::new(Some(Box::new(reader)))),
                writer: Arc::new(AsyncMutex::new(Some(Box::new(writer)))),
                abort_sender,
                abort_handlers: Mutex::new(Some(self.abort_handlers)),
            }),
            registry: Arc::new(self.registry),
            max_frame_bytes: self.max_frame_bytes,
        })
    }
}

struct EmptyMessageContext;

impl CustomMessageContext for EmptyMessageContext {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

struct FramedState {
    reader: Arc<AsyncMutex<Option<Box<dyn FramedRead>>>>,
    writer: Arc<AsyncMutex<Option<Box<dyn FramedWrite>>>>,
    abort_sender: watch::Sender<Option<Error>>,
    abort_handlers: Mutex<Option<Vec<Arc<dyn TransportAbortHandler>>>>,
}

impl FramedState {
    fn abort(self: &Arc<Self>, reason: Error) {
        let handlers = self
            .abort_handlers
            .lock()
            .expect("framed stream abort mutex poisoned")
            .take();
        let Some(handlers) = handlers else {
            return;
        };
        self.abort_sender.send_replace(Some(reason.clone()));
        for handler in handlers {
            handler.transport_aborted(reason.clone());
        }

        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let reader = Arc::clone(&self.reader);
            let writer = Arc::clone(&self.writer);
            runtime.spawn(async move {
                reader.lock().await.take();
                if let Some(mut writer) = writer.lock().await.take() {
                    let _ = writer.shutdown().await;
                }
            });
        }
    }

    fn reason(&self) -> Option<Error> {
        self.abort_sender.borrow().clone()
    }
}

async fn wait_for_abort(receiver: &mut watch::Receiver<Option<Error>>) -> Error {
    loop {
        if let Some(reason) = receiver.borrow().clone() {
            return reason;
        }
        if receiver.changed().await.is_err() {
            return Error::Message("framed stream transport was dropped".to_owned());
        }
    }
}

/// A bounded `u32`-length-prefixed JSON transport over any Tokio duplex stream.
pub struct FramedStreamTransport {
    state: Arc<FramedState>,
    registry: Arc<CustomTypeRegistry>,
    max_frame_bytes: usize,
}

impl FramedStreamTransport {
    /// Starts configuring a framed stream transport.
    pub fn builder() -> FramedStreamTransportBuilder {
        FramedStreamTransportBuilder::new()
    }

    /// Wraps a stream with default limits and no custom values.
    pub fn new<S>(stream: S) -> Result<Self>
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        Self::builder().build(stream)
    }
}

impl Drop for FramedStreamTransport {
    fn drop(&mut self) {
        self.state.abort(Error::Message(
            "framed stream control transport was dropped".to_owned(),
        ));
    }
}

impl AnyRpcTransport for FramedStreamTransport {
    fn encoding_level(&self) -> EncodingLevel {
        EncodingLevel::JsonCompatible
    }

    fn new_custom_codec(&self) -> Option<Box<CustomMessageCodec>> {
        Some(Box::new(CustomMessageCodec::new(
            Arc::clone(&self.registry),
            Box::new(EmptyMessageContext),
        )))
    }

    fn send(&self, message: TransportMessage) -> Result<TransportSend> {
        if let Some(reason) = self.state.reason() {
            return Err(reason);
        }
        let TransportMessage::Value { value, codec } = message else {
            return Err(Error::Type(
                "framed stream transport expected a structured value tree".to_owned(),
            ));
        };
        let json = capnweb_to_json(&value)?;
        let payload = serde_json::to_vec(&json)
            .map_err(|error| Error::Message(format!("failed to encode RPC frame: {error}")))?;
        if payload.len() > self.max_frame_bytes {
            return Err(Error::Message(format!(
                "RPC frame exceeds maximum size of {} bytes",
                self.max_frame_bytes
            )));
        }
        let length = u32::try_from(payload.len())
            .expect("frame limit guarantees that its length fits in u32")
            .to_be_bytes();
        let state = Arc::clone(&self.state);
        let mut abort = state.abort_sender.subscribe();
        let size = Some(length.len() + payload.len());
        let sent = Box::pin(async move {
            let _codec = codec;
            let result = tokio::select! {
                biased;
                reason = wait_for_abort(&mut abort) => Err(reason),
                result = async {
                    let mut writer = state.writer.lock().await;
                    let writer = writer.as_mut().ok_or_else(|| {
                        Error::Message("framed stream transport is closed".to_owned())
                    })?;
                    writer.write_all(&length).await
                        .map_err(|error| Error::Message(format!("failed to write RPC frame length: {error}")))?;
                    writer.write_all(&payload).await
                        .map_err(|error| Error::Message(format!("failed to write RPC frame: {error}")))?;
                    writer.flush().await
                        .map_err(|error| Error::Message(format!("failed to flush RPC frame: {error}")))?;
                    Ok(())
                } => result,
            };
            if let Err(error) = &result {
                state.abort(error.clone());
            }
            result
        });
        Ok(TransportSend::Pending { size, sent })
    }

    fn receive(
        self: Arc<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<TransportMessage>> + Send + 'static>> {
        Box::pin(async move {
            if let Some(reason) = self.state.reason() {
                return Err(reason);
            }
            let state = Arc::clone(&self.state);
            let registry = Arc::clone(&self.registry);
            let max_frame_bytes = self.max_frame_bytes;
            let mut abort = state.abort_sender.subscribe();
            let result = tokio::select! {
                biased;
                reason = wait_for_abort(&mut abort) => Err(reason),
                result = async {
                    let mut reader = state.reader.lock().await;
                    let reader = reader.as_mut().ok_or_else(|| {
                        Error::Message("framed stream transport is closed".to_owned())
                    })?;
                    let length = reader.read_u32().await
                        .map_err(|error| Error::Message(format!("failed to read RPC frame length: {error}")))?
                        as usize;
                    if length == 0 {
                        return Err(Error::Message("invalid RPC frame size 0".to_owned()));
                    }
                    if length > max_frame_bytes {
                        // (a push or stream message, by its first bytes, read past unbuffered: its call fails
                        // alone; anything else ends the session)
                        let mut head = [0u8; 10];
                        let seen = head.len().min(length);
                        reader.read_exact(&mut head[..seen]).await
                            .map_err(|error| Error::Message(format!("failed to read RPC frame: {error}")))?;
                        let stream = if head[..seen].starts_with(br#"["push","#) {
                            false
                        } else if head[..seen].starts_with(br#"["stream","#) {
                            true
                        } else {
                            return Err(Error::Message(format!(
                                "invalid RPC frame size {length}; maximum is {max_frame_bytes}"
                            )));
                        };
                        let rest = (length - seen) as u64;
                        let skipped = tokio::io::copy(&mut (&mut **reader).take(rest), &mut tokio::io::sink()).await
                            .map_err(|error| Error::Message(format!("failed to read RPC frame: {error}")))?;
                        if skipped != rest {
                            return Err(Error::Message("the RPC frame ended early".to_owned()));
                        }
                        return Ok(TransportMessage::Oversized {
                            stream,
                            error: Error::Type(format!(
                                "an RPC message of {length} bytes is over the frame limit of {max_frame_bytes} bytes"
                            )),
                        });
                    }
                    let mut payload = vec![0; length];
                    reader.read_exact(&mut payload).await
                        .map_err(|error| Error::Message(format!("failed to read RPC frame: {error}")))?;
                    let json: serde_json::Value = serde_json::from_slice(&payload)
                        .map_err(|error| Error::Type(format!("invalid RPC frame JSON: {error}")))?;
                    Ok(TransportMessage::Value {
                        value: json_to_capnweb(json)?,
                        codec: Some(Box::new(CustomMessageCodec::new(
                            registry,
                            Box::new(EmptyMessageContext),
                        ))),
                    })
                } => result,
            };
            if let Err(error) = &result {
                state.abort(error.clone());
            }
            result
        })
    }

    fn abort(&self, reason: Error) {
        self.state.abort(reason);
    }
}
