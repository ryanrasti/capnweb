//! Single-use byte-channel attachments and transport-independent rendezvous support.
//!
//! Cap'n Web carries a small channel reference while the transport carries the
//! ordered duplex byte stream. A [`ChannelRendezvous`] safely joins those two
//! pieces regardless of arrival order.

use std::any::TypeId;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::io;
#[cfg(unix)]
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Notify, mpsc, oneshot};

use crate::core::{
    CapnwebValue, CustomMessageContext, CustomTypeHandler, CustomValue, Error, Result,
};

/// Current version of the generic byte-channel reference format.
pub const BYTE_CHANNEL_VERSION: u16 = 1;

/// Size of the canonical byte-channel stream header.
pub const BYTE_CHANNEL_HEADER_BYTES: usize = 38;

const BYTE_CHANNEL_MAGIC: [u8; 4] = *b"CNWB";

/// An object-safe Tokio duplex byte stream.
pub trait AsyncByteStream: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

impl<T> AsyncByteStream for T where T: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

/// A boxed raw ordered duplex stream used inside transport adapters.
pub type BoxByteStream = Box<dyn AsyncByteStream>;

pub(crate) trait Revocable: Send + Sync {
    fn revoke(&self, reason: Error);
}

#[derive(Default)]
struct RevocationGroupState {
    reason: Option<Error>,
    active: Vec<Weak<dyn Revocable>>,
}

#[derive(Default)]
pub(crate) struct RevocationGroup {
    state: Mutex<RevocationGroupState>,
}

impl RevocationGroup {
    pub(crate) fn register<T>(&self, endpoint: &Arc<T>)
    where
        T: Revocable + 'static,
    {
        let endpoint: Arc<dyn Revocable> = endpoint.clone();
        let reason = {
            let mut state = self.state.lock().expect("revocation group mutex poisoned");
            if let Some(reason) = &state.reason {
                Some(reason.clone())
            } else {
                state.active.retain(|endpoint| endpoint.strong_count() > 0);
                state.active.push(Arc::downgrade(&endpoint));
                None
            }
        };
        if let Some(reason) = reason {
            endpoint.revoke(reason);
        }
    }

    pub(crate) fn revoke_all(&self, reason: Error) {
        let active = {
            let mut state = self.state.lock().expect("revocation group mutex poisoned");
            if state.reason.is_some() {
                return;
            }
            state.reason = Some(reason.clone());
            std::mem::take(&mut state.active)
        };
        for endpoint in active {
            if let Some(endpoint) = endpoint.upgrade() {
                endpoint.revoke(reason.clone());
            }
        }
    }
}

struct StreamStateInner {
    stream: Option<BoxByteStream>,
    reason: Option<Error>,
    read_waker: Option<Waker>,
    write_waker: Option<Waker>,
}

struct StreamState {
    inner: Mutex<StreamStateInner>,
}

impl Revocable for StreamState {
    fn revoke(&self, reason: Error) {
        let (read_waker, write_waker) = {
            let mut state = self
                .inner
                .lock()
                .expect("byte channel stream mutex poisoned");
            if state.reason.is_some() {
                return;
            }
            state.reason = Some(reason);
            state.stream.take();
            (state.read_waker.take(), state.write_waker.take())
        };
        if let Some(waker) = read_waker {
            waker.wake();
        }
        if let Some(waker) = write_waker {
            waker.wake();
        }
    }
}

/// A duplex stream whose lifetime remains bounded by its control session.
pub struct ByteChannelStream {
    state: Arc<StreamState>,
}

impl ByteChannelStream {
    fn new(stream: impl AsyncByteStream) -> Self {
        Self {
            state: Arc::new(StreamState {
                inner: Mutex::new(StreamStateInner {
                    stream: Some(Box::new(stream)),
                    reason: None,
                    read_waker: None,
                    write_waker: None,
                }),
            }),
        }
    }

    fn bind(&self, group: &RevocationGroup) {
        group.register(&self.state);
    }

    /// Resets this channel without affecting sibling channels.
    pub fn reset(&self, reason: impl Into<String>) {
        self.state.revoke(Error::Message(reason.into()));
    }

    fn revoked_error(reason: Option<&Error>) -> io::Error {
        io::Error::new(
            io::ErrorKind::ConnectionAborted,
            reason
                .map(ToString::to_string)
                .unwrap_or_else(|| "byte channel was reset".to_owned()),
        )
    }
}

impl fmt::Debug for ByteChannelStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ByteChannelStream")
            .finish_non_exhaustive()
    }
}

impl AsyncRead for ByteChannelStream {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut state = self
            .state
            .inner
            .lock()
            .expect("byte channel stream mutex poisoned");
        state.read_waker = Some(context.waker().clone());
        match state.stream.as_mut() {
            Some(stream) => Pin::new(stream.as_mut()).poll_read(context, buffer),
            None => Poll::Ready(Err(Self::revoked_error(state.reason.as_ref()))),
        }
    }
}

impl AsyncWrite for ByteChannelStream {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut state = self
            .state
            .inner
            .lock()
            .expect("byte channel stream mutex poisoned");
        state.write_waker = Some(context.waker().clone());
        match state.stream.as_mut() {
            Some(stream) => Pin::new(stream.as_mut()).poll_write(context, buffer),
            None => Poll::Ready(Err(Self::revoked_error(state.reason.as_ref()))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut state = self
            .state
            .inner
            .lock()
            .expect("byte channel stream mutex poisoned");
        state.write_waker = Some(context.waker().clone());
        match state.stream.as_mut() {
            Some(stream) => Pin::new(stream.as_mut()).poll_flush(context),
            None => Poll::Ready(Err(Self::revoked_error(state.reason.as_ref()))),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut state = self
            .state
            .inner
            .lock()
            .expect("byte channel stream mutex poisoned");
        state.write_waker = Some(context.waker().clone());
        match state.stream.as_mut() {
            Some(stream) => Pin::new(stream.as_mut()).poll_shutdown(context),
            None => Poll::Ready(Err(Self::revoked_error(state.reason.as_ref()))),
        }
    }
}

#[cfg(unix)]
struct FdState {
    descriptor: OwnedFd,
}

#[cfg(unix)]
impl Revocable for FdState {
    fn revoke(&self, _reason: Error) {
        let _ = rustix::net::shutdown(&self.descriptor, rustix::net::Shutdown::Both);
    }
}

/// A connected stream descriptor whose lifetime remains bounded by its control session.
#[cfg(unix)]
pub struct ByteChannelFd {
    state: Arc<FdState>,
}

#[cfg(unix)]
impl ByteChannelFd {
    pub(crate) fn new(descriptor: OwnedFd) -> Self {
        Self {
            state: Arc::new(FdState { descriptor }),
        }
    }

    pub(crate) fn bind(&self, group: &RevocationGroup) {
        group.register(&self.state);
    }

    /// Resets this channel without affecting sibling channels.
    pub fn reset(&self, reason: impl Into<String>) {
        self.state.revoke(Error::Message(reason.into()));
    }

    /// Duplicates the descriptor for installation while this revocation handle is retained.
    pub fn try_clone_to_owned(&self) -> io::Result<OwnedFd> {
        self.state.descriptor.try_clone()
    }
}

#[cfg(unix)]
impl AsFd for ByteChannelFd {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.state.descriptor.as_fd()
    }
}

#[cfg(unix)]
impl fmt::Debug for ByteChannelFd {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ByteChannelFd")
            .finish_non_exhaustive()
    }
}

/// The transport-specific endpoint delivered by a [`ByteChannel`].
pub enum ByteChannelEndpoint {
    /// A Tokio-compatible duplex stream, used by network and multiplexed transports.
    Stream(ByteChannelStream),
    /// A connected stream descriptor delivered with SCM_RIGHTS.
    #[cfg(unix)]
    FileDescriptor(ByteChannelFd),
}

impl ByteChannelEndpoint {
    /// Wraps a Tokio-compatible duplex stream.
    pub fn stream(stream: impl AsyncByteStream) -> Self {
        Self::Stream(ByteChannelStream::new(stream))
    }

    /// Extracts a session-bound Tokio-compatible stream.
    pub fn into_stream(self) -> Result<ByteChannelStream> {
        match self {
            Self::Stream(stream) => Ok(stream),
            #[cfg(unix)]
            Self::FileDescriptor(_) => Err(Error::Type(
                "byte channel contains a file descriptor, not an async stream".to_owned(),
            )),
        }
    }

    /// Wraps an owning file descriptor.
    #[cfg(unix)]
    pub fn file_descriptor(descriptor: OwnedFd) -> Self {
        Self::FileDescriptor(ByteChannelFd::new(descriptor))
    }

    /// Extracts a session-bound owning file descriptor.
    #[cfg(unix)]
    pub fn into_file_descriptor(self) -> Result<ByteChannelFd> {
        match self {
            Self::FileDescriptor(descriptor) => Ok(descriptor),
            Self::Stream(_) => Err(Error::Type(
                "byte channel contains an async stream, not a file descriptor".to_owned(),
            )),
        }
    }

    pub(crate) fn bind(&self, group: &RevocationGroup) {
        match self {
            Self::Stream(stream) => stream.bind(group),
            #[cfg(unix)]
            Self::FileDescriptor(descriptor) => descriptor.bind(group),
        }
    }
}

impl fmt::Debug for ByteChannelEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stream(_) => formatter.write_str("ByteChannelEndpoint::Stream(..)"),
            #[cfg(unix)]
            Self::FileDescriptor(_) => {
                formatter.write_str("ByteChannelEndpoint::FileDescriptor(..)")
            }
        }
    }
}

/// A connection-local reference sent in a Cap'n Web custom value and stream header.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChannelReference {
    /// Channel protocol version.
    pub version: u16,
    /// Identifies the authenticated Cap'n Web transport session.
    pub session_id: u128,
    /// Identifies one channel within the session and must never be reused.
    pub channel_id: u128,
}

impl ChannelReference {
    /// Constructs a channel reference.
    pub fn new(version: u16, session_id: u128, channel_id: u128) -> Self {
        Self {
            version,
            session_id,
            channel_id,
        }
    }

    /// Encodes fields suitable for a custom Cap'n Web value.
    pub fn to_fields(&self) -> Vec<CapnwebValue> {
        vec![
            CapnwebValue::Number(f64::from(self.version)),
            CapnwebValue::String(format!("{:032x}", self.session_id)),
            CapnwebValue::String(format!("{:032x}", self.channel_id)),
        ]
    }

    /// Decodes and validates fields from a custom Cap'n Web value.
    pub fn from_fields(fields: &[CapnwebValue]) -> Result<Self> {
        let [version, session_id, channel_id] = fields else {
            return Err(Error::Type("invalid byte channel reference".to_owned()));
        };
        let version = version
            .as_i64()
            .and_then(|value| u16::try_from(value).ok())
            .ok_or_else(|| Error::Type("invalid byte channel version".to_owned()))?;
        let CapnwebValue::String(session_id) = session_id else {
            return Err(Error::Type("invalid byte channel session ID".to_owned()));
        };
        let CapnwebValue::String(channel_id) = channel_id else {
            return Err(Error::Type("invalid byte channel ID".to_owned()));
        };
        let canonical_id = |value: &str| {
            value.len() == 32
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        };
        if !canonical_id(session_id) || !canonical_id(channel_id) {
            return Err(Error::Type(
                "byte channel IDs must be 128-bit lowercase hexadecimal strings".to_owned(),
            ));
        }
        let session_id = u128::from_str_radix(session_id, 16)
            .map_err(|_| Error::Type("invalid byte channel session ID".to_owned()))?;
        let channel_id = u128::from_str_radix(channel_id, 16)
            .map_err(|_| Error::Type("invalid byte channel ID".to_owned()))?;
        Ok(Self::new(version, session_id, channel_id))
    }

    /// Encodes the fixed-size header written at the start of an out-of-band stream.
    pub fn encode_header(&self) -> [u8; BYTE_CHANNEL_HEADER_BYTES] {
        let mut header = [0; BYTE_CHANNEL_HEADER_BYTES];
        header[..4].copy_from_slice(&BYTE_CHANNEL_MAGIC);
        header[4..6].copy_from_slice(&self.version.to_be_bytes());
        header[6..22].copy_from_slice(&self.session_id.to_be_bytes());
        header[22..38].copy_from_slice(&self.channel_id.to_be_bytes());
        header
    }

    /// Decodes the canonical fixed-size out-of-band stream header.
    pub fn decode_header(header: &[u8; BYTE_CHANNEL_HEADER_BYTES]) -> Result<Self> {
        if header[..4] != BYTE_CHANNEL_MAGIC {
            return Err(Error::Type("invalid byte channel stream header".to_owned()));
        }
        let version = u16::from_be_bytes([header[4], header[5]]);
        let session_id = u128::from_be_bytes(
            header[6..22]
                .try_into()
                .expect("byte channel session ID has a fixed size"),
        );
        let channel_id = u128::from_be_bytes(
            header[22..38]
                .try_into()
                .expect("byte channel ID has a fixed size"),
        );
        Ok(Self::new(version, session_id, channel_id))
    }

    /// Writes the canonical header to an out-of-band stream.
    pub async fn write_header(&self, stream: &mut (impl AsyncWrite + Unpin)) -> Result<()> {
        stream
            .write_all(&self.encode_header())
            .await
            .map_err(|error| {
                Error::Message(format!("failed to write byte channel header: {error}"))
            })
    }

    /// Reads the canonical header from an out-of-band stream.
    pub async fn read_header(stream: &mut (impl AsyncRead + Unpin)) -> Result<Self> {
        let mut header = [0; BYTE_CHANNEL_HEADER_BYTES];
        stream.read_exact(&mut header).await.map_err(|error| {
            Error::Message(format!("failed to read byte channel header: {error}"))
        })?;
        Self::decode_header(&header)
    }
}

type EndpointFuture = Pin<Box<dyn Future<Output = Result<ByteChannelEndpoint>> + Send + 'static>>;

enum ByteChannelState {
    Ready(ByteChannelEndpoint),
    Pending(EndpointFuture),
    Claimed,
}

struct ByteChannelInner {
    state: Mutex<ByteChannelState>,
}

/// A cloneable handle to a single-use duplex byte-channel attachment.
///
/// Cloning this handle does not duplicate the endpoint. Exactly one caller may
/// successfully [`claim`](Self::claim) it.
#[derive(Clone)]
pub struct ByteChannel {
    inner: Arc<ByteChannelInner>,
}

impl ByteChannel {
    /// Creates a channel from a Tokio-compatible duplex stream.
    pub fn from_stream(stream: impl AsyncByteStream) -> Self {
        Self::from_endpoint(ByteChannelEndpoint::stream(stream))
    }

    /// Creates a channel from an owning connected stream descriptor.
    #[cfg(unix)]
    pub fn from_fd(descriptor: OwnedFd) -> Self {
        Self::from_endpoint(ByteChannelEndpoint::file_descriptor(descriptor))
    }

    /// Creates a channel from a transport endpoint.
    pub fn from_endpoint(endpoint: ByteChannelEndpoint) -> Self {
        Self::with_state(ByteChannelState::Ready(endpoint))
    }

    fn pending(future: EndpointFuture) -> Self {
        Self::with_state(ByteChannelState::Pending(future))
    }

    fn with_state(state: ByteChannelState) -> Self {
        Self {
            inner: Arc::new(ByteChannelInner {
                state: Mutex::new(state),
            }),
        }
    }

    /// Claims and resolves the endpoint exactly once.
    pub async fn claim(&self) -> Result<ByteChannelEndpoint> {
        match self.take_state()? {
            ByteChannelState::Ready(endpoint) => Ok(endpoint),
            ByteChannelState::Pending(future) => future.await,
            ByteChannelState::Claimed => unreachable!("take_state rejects claimed channels"),
        }
    }

    /// Claims a Tokio-compatible stream exactly once.
    pub async fn claim_stream(&self) -> Result<ByteChannelStream> {
        self.claim().await?.into_stream()
    }

    /// Claims an owning connected stream descriptor exactly once.
    #[cfg(unix)]
    pub async fn claim_fd(&self) -> Result<ByteChannelFd> {
        self.claim().await?.into_file_descriptor()
    }

    /// Claims an endpoint synchronously for a transport encoder.
    ///
    /// This is useful for transports such as UDS where the endpoint is attached
    /// atomically to the control frame. Pending channels cannot be re-exported.
    pub fn take_ready_for_transport(&self) -> Result<ByteChannelEndpoint> {
        match self.take_state()? {
            ByteChannelState::Ready(endpoint) => Ok(endpoint),
            ByteChannelState::Pending(_) => Err(Error::Type(
                "a pending byte channel cannot be sent to another transport".to_owned(),
            )),
            ByteChannelState::Claimed => unreachable!("take_state rejects claimed channels"),
        }
    }

    fn take_state(&self) -> Result<ByteChannelState> {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("byte channel mutex poisoned");
        if matches!(*state, ByteChannelState::Claimed) {
            return Err(Error::Type("byte channel was already claimed".to_owned()));
        }
        Ok(std::mem::replace(&mut *state, ByteChannelState::Claimed))
    }
}

impl fmt::Debug for ByteChannel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ByteChannel")
            .finish_non_exhaustive()
    }
}

/// Transport-specific custom-value encoder/decoder for [`ByteChannel`].
///
/// UDS implementations can attach an FD through `context`; QUIC implementations
/// can encode a [`ChannelReference`] and use [`ChannelRendezvous`] to join the
/// independently arriving stream.
pub trait ByteChannelTransportAdapter: Send + Sync + 'static {
    /// Encodes one outgoing channel into fields following the `byteChannel` tag.
    fn encode(
        &self,
        channel: &ByteChannel,
        context: &mut dyn CustomMessageContext,
    ) -> Result<Vec<CapnwebValue>>;

    /// Decodes one incoming channel from fields following the `byteChannel` tag.
    fn decode(
        &self,
        fields: &[CapnwebValue],
        context: &mut dyn CustomMessageContext,
    ) -> Result<ByteChannel>;
}

/// Adapts a [`ByteChannelTransportAdapter`] to the generic custom-value registry.
pub struct ByteChannelTypeHandler {
    adapter: Arc<dyn ByteChannelTransportAdapter>,
}

impl ByteChannelTypeHandler {
    /// Creates a custom-value handler for a transport adapter.
    pub fn new(adapter: Arc<dyn ByteChannelTransportAdapter>) -> Self {
        Self { adapter }
    }
}

impl CustomTypeHandler for ByteChannelTypeHandler {
    fn value_type_id(&self) -> TypeId {
        TypeId::of::<ByteChannel>()
    }

    fn wire_tag(&self) -> &'static str {
        "byteChannel"
    }

    fn encode_fields(
        &self,
        value: &CustomValue,
        context: &mut dyn CustomMessageContext,
    ) -> Result<Vec<CapnwebValue>> {
        let channel = value.downcast::<ByteChannel>()?;
        self.adapter.encode(&channel, context)
    }

    fn decode_fields(
        &self,
        fields: &[CapnwebValue],
        context: &mut dyn CustomMessageContext,
    ) -> Result<CustomValue> {
        self.adapter.decode(fields, context).map(CustomValue::new)
    }
}

/// Bounds retained unmatched channels and total channel IDs in one session.
#[derive(Clone, Copy, Debug)]
pub struct ChannelRendezvousLimits {
    /// Maximum references and streams waiting for their counterpart.
    pub max_pending: usize,
    /// Maximum distinct channel IDs accepted during this session.
    pub max_channels: usize,
    /// Maximum time an unmatched reference or stream is retained.
    pub pending_timeout: Duration,
}

impl Default for ChannelRendezvousLimits {
    fn default() -> Self {
        Self {
            max_pending: 64,
            max_channels: 65_536,
            pending_timeout: Duration::from_secs(10),
        }
    }
}

enum RendezvousEntry {
    Reference {
        reference: ChannelReference,
        sender: oneshot::Sender<Result<ByteChannelEndpoint>>,
        created: Instant,
    },
    Stream {
        reference: ChannelReference,
        endpoint: ByteChannelEndpoint,
        created: Instant,
    },
}

struct RendezvousState {
    entries: HashMap<u128, RendezvousEntry>,
    known_ids: HashSet<u128>,
    abort_reason: Option<Error>,
}

struct RendezvousInner {
    version: u16,
    session_id: u128,
    limits: ChannelRendezvousLimits,
    state: Mutex<RendezvousState>,
}

/// Pairs Cap'n Web channel references and out-of-band streams in either order.
#[derive(Clone)]
pub struct ChannelRendezvous {
    inner: Arc<RendezvousInner>,
}

impl ChannelRendezvous {
    /// Creates a rendezvous registry scoped to one authenticated session.
    pub fn new(version: u16, session_id: u128, limits: ChannelRendezvousLimits) -> Self {
        Self {
            inner: Arc::new(RendezvousInner {
                version,
                session_id,
                limits,
                state: Mutex::new(RendezvousState {
                    entries: HashMap::new(),
                    known_ids: HashSet::new(),
                    abort_reason: None,
                }),
            }),
        }
    }

    /// Registers an RPC reference and returns an awaitable single-use channel.
    pub fn expect(&self, reference: ChannelReference) -> Result<ByteChannel> {
        self.validate(&reference)?;
        let mut state = self.inner.state.lock().expect("rendezvous mutex poisoned");
        if let Some(error) = &state.abort_reason {
            return Err(error.clone());
        }

        match state.entries.remove(&reference.channel_id) {
            Some(RendezvousEntry::Stream {
                reference: arrived,
                endpoint,
                ..
            }) => {
                if arrived != reference {
                    return Err(Error::Type(
                        "byte channel reference does not match the pending stream".to_owned(),
                    ));
                }
                Ok(ByteChannel::from_endpoint(endpoint))
            }
            Some(entry @ RendezvousEntry::Reference { .. }) => {
                state.entries.insert(reference.channel_id, entry);
                Err(Error::Type("duplicate byte channel reference".to_owned()))
            }
            None => {
                self.reserve(&mut state, reference.channel_id)?;
                let (sender, receiver) = oneshot::channel();
                state.entries.insert(
                    reference.channel_id,
                    RendezvousEntry::Reference {
                        reference: reference.clone(),
                        sender,
                        created: Instant::now(),
                    },
                );
                let future = Box::pin(async move {
                    receiver.await.unwrap_or_else(|_| {
                        Err(Error::Message(
                            "byte channel rendezvous was canceled".to_owned(),
                        ))
                    })
                });
                Ok(ByteChannel::pending(future))
            }
        }
    }

    /// Registers an independently arriving stream endpoint.
    pub fn arrive(&self, reference: ChannelReference, endpoint: ByteChannelEndpoint) -> Result<()> {
        self.validate(&reference)?;
        let mut state = self.inner.state.lock().expect("rendezvous mutex poisoned");
        if let Some(error) = &state.abort_reason {
            return Err(error.clone());
        }

        match state.entries.remove(&reference.channel_id) {
            Some(RendezvousEntry::Reference {
                reference: expected,
                sender,
                created,
            }) => {
                if expected != reference {
                    state.entries.insert(
                        expected.channel_id,
                        RendezvousEntry::Reference {
                            reference: expected,
                            sender,
                            created,
                        },
                    );
                    return Err(Error::Type(
                        "byte channel stream header does not match its RPC reference".to_owned(),
                    ));
                }
                sender
                    .send(Ok(endpoint))
                    .map_err(|_| Error::Message("byte channel receiver was canceled".to_owned()))
            }
            Some(entry @ RendezvousEntry::Stream { .. }) => {
                state.entries.insert(reference.channel_id, entry);
                Err(Error::Type("duplicate byte channel stream".to_owned()))
            }
            None => {
                self.reserve(&mut state, reference.channel_id)?;
                state.entries.insert(
                    reference.channel_id,
                    RendezvousEntry::Stream {
                        reference,
                        endpoint,
                        created: Instant::now(),
                    },
                );
                Ok(())
            }
        }
    }

    /// Expires unmatched references and streams and returns the number removed.
    pub fn expire_pending(&self) -> usize {
        let mut state = self.inner.state.lock().expect("rendezvous mutex poisoned");
        let now = Instant::now();
        let expired: Vec<u128> = state
            .entries
            .iter()
            .filter_map(|(id, entry)| {
                let created = match entry {
                    RendezvousEntry::Reference { created, .. }
                    | RendezvousEntry::Stream { created, .. } => *created,
                };
                (now.duration_since(created) >= self.inner.limits.pending_timeout).then_some(*id)
            })
            .collect();
        for id in &expired {
            if let Some(RendezvousEntry::Reference { sender, .. }) = state.entries.remove(id) {
                let _ = sender.send(Err(Error::Message(
                    "byte channel rendezvous expired".to_owned(),
                )));
            }
        }
        expired.len()
    }

    /// Aborts all waiters and prevents future channels from being registered.
    pub fn abort(&self, reason: Error) {
        let mut state = self.inner.state.lock().expect("rendezvous mutex poisoned");
        if state.abort_reason.is_some() {
            return;
        }
        state.abort_reason = Some(reason.clone());
        for (_, entry) in state.entries.drain() {
            if let RendezvousEntry::Reference { sender, .. } = entry {
                let _ = sender.send(Err(reason.clone()));
            }
        }
    }

    /// Returns the current number of unmatched references and streams.
    pub fn pending_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("rendezvous mutex poisoned")
            .entries
            .len()
    }

    fn validate(&self, reference: &ChannelReference) -> Result<()> {
        if reference.version != self.inner.version {
            return Err(Error::Type("unsupported byte channel version".to_owned()));
        }
        if reference.session_id != self.inner.session_id {
            return Err(Error::Type(
                "byte channel belongs to a different session".to_owned(),
            ));
        }
        Ok(())
    }

    fn reserve(&self, state: &mut RendezvousState, channel_id: u128) -> Result<()> {
        if state.known_ids.contains(&channel_id) {
            return Err(Error::Type("byte channel ID was already used".to_owned()));
        }
        if state.entries.len() >= self.inner.limits.max_pending {
            return Err(Error::Message(
                "too many unmatched byte channels".to_owned(),
            ));
        }
        if state.known_ids.len() >= self.inner.limits.max_channels {
            return Err(Error::Message(
                "byte channel session limit exceeded".to_owned(),
            ));
        }
        state.known_ids.insert(channel_id);
        Ok(())
    }
}

/// One outgoing attachment accepted by a network transport task.
pub struct OutgoingByteChannel {
    /// Header/reference to write at the start of the out-of-band stream.
    pub reference: ChannelReference,
    /// Endpoint whose bytes the transport carries on that stream.
    pub endpoint: ByteChannelEndpoint,
}

impl fmt::Debug for OutgoingByteChannel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OutgoingByteChannel")
            .field("reference", &self.reference)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

struct ByteChannelSessionState {
    next_channel_id: u128,
    sender: Option<mpsc::Sender<OutgoingByteChannel>>,
    abort_reason: Option<Error>,
}

/// A reusable custom-value adapter for transports with independent byte streams.
///
/// The RPC encoder queues [`OutgoingByteChannel`] values for the transport task.
/// The transport writes the reference as a bounded stream header and carries the
/// endpoint bytes on that stream. Incoming stream headers are submitted through
/// [`arrive`](Self::arrive), in either order relative to RPC decoding.
pub struct ByteChannelSession {
    version: u16,
    session_id: u128,
    rendezvous: ChannelRendezvous,
    revocations: RevocationGroup,
    state: Mutex<ByteChannelSessionState>,
    receiver: tokio::sync::Mutex<mpsc::Receiver<OutgoingByteChannel>>,
    abort_notify: Notify,
}

impl ByteChannelSession {
    /// Creates a session adapter with a bounded outgoing attachment queue.
    pub fn new(
        version: u16,
        session_id: u128,
        limits: ChannelRendezvousLimits,
        outgoing_capacity: usize,
    ) -> Result<Arc<Self>> {
        if outgoing_capacity == 0 {
            return Err(Error::Type(
                "byte channel outgoing capacity must be nonzero".to_owned(),
            ));
        }
        if limits.pending_timeout.is_zero() {
            return Err(Error::Type(
                "byte channel pending timeout must be nonzero".to_owned(),
            ));
        }
        let (sender, receiver) = mpsc::channel(outgoing_capacity);
        Ok(Arc::new(Self {
            version,
            session_id,
            rendezvous: ChannelRendezvous::new(version, session_id, limits),
            revocations: RevocationGroup::default(),
            state: Mutex::new(ByteChannelSessionState {
                next_channel_id: 1,
                sender: Some(sender),
                abort_reason: None,
            }),
            receiver: tokio::sync::Mutex::new(receiver),
            abort_notify: Notify::new(),
        }))
    }

    /// Returns the custom-value handler to register on the control transport.
    pub fn custom_type_handler(self: &Arc<Self>) -> Arc<dyn CustomTypeHandler> {
        let adapter: Arc<dyn ByteChannelTransportAdapter> = self.clone();
        Arc::new(ByteChannelTypeHandler::new(adapter))
    }

    /// Wraps an endpoint to be externalized when its RPC value is encoded.
    pub fn attach(&self, endpoint: ByteChannelEndpoint) -> ByteChannel {
        ByteChannel::from_endpoint(endpoint)
    }

    /// Wraps a Tokio-compatible stream for externalization.
    pub fn attach_stream(&self, stream: impl AsyncByteStream) -> ByteChannel {
        ByteChannel::from_stream(stream)
    }

    /// Creates a buffered local stream and an attachment for its other endpoint.
    pub fn open(&self, buffer: usize) -> Result<(ByteChannel, tokio::io::DuplexStream)> {
        if buffer == 0 {
            return Err(Error::Type(
                "byte channel stream buffer must be nonzero".to_owned(),
            ));
        }
        let (local, transport) = tokio::io::duplex(buffer);
        Ok((self.attach_stream(transport), local))
    }

    /// Waits for the next attachment that the network transport must carry.
    pub async fn accept_outgoing(&self) -> Result<OutgoingByteChannel> {
        let next = self.receiver.lock().await.recv().await;
        let aborted = self
            .state
            .lock()
            .expect("byte channel session mutex poisoned")
            .abort_reason
            .clone();
        if let Some(error) = aborted {
            return Err(error);
        }
        next.ok_or_else(|| Error::Message("byte channel session was shut down".to_owned()))
    }

    /// Submits an out-of-band endpoint and its decoded header.
    pub fn arrive(&self, reference: ChannelReference, endpoint: ByteChannelEndpoint) -> Result<()> {
        endpoint.bind(&self.revocations);
        self.rendezvous.arrive(reference, endpoint)
    }

    /// Submits a Tokio-compatible out-of-band stream and its decoded header.
    pub fn arrive_stream(
        &self,
        reference: ChannelReference,
        stream: impl AsyncByteStream,
    ) -> Result<()> {
        self.arrive(reference, ByteChannelEndpoint::stream(stream))
    }

    /// Runs periodic unmatched-channel expiry until the session is aborted.
    ///
    /// A network transport should spawn this once alongside its control and
    /// independent-stream receive loops.
    pub async fn run_maintenance(&self) -> Result<()> {
        loop {
            if let Some(reason) = self
                .state
                .lock()
                .expect("byte channel session mutex poisoned")
                .abort_reason
                .clone()
            {
                return Err(reason);
            }
            tokio::select! {
                () = tokio::time::sleep(self.rendezvous.inner.limits.pending_timeout) => {
                    self.expire_pending();
                }
                () = self.abort_notify.notified() => {}
            }
        }
    }

    /// Expires unmatched incoming stream headers and RPC references.
    pub fn expire_pending(&self) -> usize {
        self.rendezvous.expire_pending()
    }

    /// Aborts pending claims and prevents additional outgoing attachments.
    pub fn abort(&self, reason: Error) {
        let mut state = self
            .state
            .lock()
            .expect("byte channel session mutex poisoned");
        if state.abort_reason.is_some() {
            return;
        }
        state.abort_reason = Some(reason.clone());
        state.sender.take();
        drop(state);
        self.rendezvous.abort(reason.clone());
        self.revocations.revoke_all(reason);
        self.abort_notify.notify_one();
    }

    fn queue_outgoing(&self, endpoint: ByteChannelEndpoint) -> Result<ChannelReference> {
        endpoint.bind(&self.revocations);
        let mut state = self
            .state
            .lock()
            .expect("byte channel session mutex poisoned");
        if let Some(error) = &state.abort_reason {
            return Err(error.clone());
        }
        let channel_id = state.next_channel_id;
        let next_channel_id = channel_id
            .checked_add(1)
            .ok_or_else(|| Error::Message("byte channel ID space was exhausted".to_owned()))?;
        let reference = ChannelReference::new(self.version, self.session_id, channel_id);
        state
            .sender
            .as_ref()
            .ok_or_else(|| Error::Message("byte channel session was shut down".to_owned()))?
            .try_send(OutgoingByteChannel {
                reference: reference.clone(),
                endpoint,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    Error::Message("byte channel outgoing queue is full".to_owned())
                }
                mpsc::error::TrySendError::Closed(_) => {
                    Error::Message("byte channel session was shut down".to_owned())
                }
            })?;
        state.next_channel_id = next_channel_id;
        Ok(reference)
    }
}

impl Drop for ByteChannelSession {
    fn drop(&mut self) {
        let reason = Error::Message("byte channel control session was dropped".to_owned());
        self.rendezvous.abort(reason.clone());
        self.revocations.revoke_all(reason);
        self.abort_notify.notify_one();
    }
}

impl ByteChannelTransportAdapter for ByteChannelSession {
    fn encode(
        &self,
        channel: &ByteChannel,
        _context: &mut dyn CustomMessageContext,
    ) -> Result<Vec<CapnwebValue>> {
        let endpoint = channel.take_ready_for_transport()?;
        self.queue_outgoing(endpoint)
            .map(|reference| reference.to_fields())
    }

    fn decode(
        &self,
        fields: &[CapnwebValue],
        _context: &mut dyn CustomMessageContext,
    ) -> Result<ByteChannel> {
        self.rendezvous
            .expect(ChannelReference::from_fields(fields)?)
    }
}
