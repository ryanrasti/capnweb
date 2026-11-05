//! Unix-domain-socket transport with SCM_RIGHTS file-descriptor passing.

use std::any::{Any, TypeId};
use std::future::Future;
use std::io::{IoSlice, IoSliceMut};
use std::mem::MaybeUninit;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::pin::Pin;
use std::sync::Arc;

use rustix::net::{
    AddressFamily, RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags, ReturnFlags,
    SendAncillaryBuffer, SendAncillaryMessage, SendFlags, Shutdown, SocketFlags, SocketType,
    getpeername, recvmsg, sendmsg, shutdown, socketpair,
};
use tokio::io::unix::AsyncFd;

use crate::channel::{
    ByteChannel, ByteChannelEndpoint, ByteChannelFd, ByteChannelRelayLimits, ByteChannelTransfer,
    ByteChannelTransportAdapter, ByteChannelTypeHandler, EndpointFuture, RelayManager,
    RelayReservation, RevocationGroup,
};
use crate::core::{
    CapnwebValue, CustomMessageCodec, CustomMessageContext, CustomTypeHandler, CustomTypeRegistry,
    CustomValue, Error, Result,
};
use crate::rpc::{AnyRpcTransport, TransportMessage, TransportSend};
use crate::serialize::{EncodingLevel, capnweb_to_json, json_to_capnweb};

const MAX_MESSAGE_BYTES: usize = 256 * 1024;
const MAX_MESSAGE_FDS: usize = 64;

/// A message-oriented UDS transport which encodes custom [`OwnedFd`] values as `["fd", index]`
/// and transfers them with SCM_RIGHTS.
///
/// Wrap descriptors with [`CapnwebValue::custom`] and recover them with
/// [`CapnwebValue::downcast_custom`].
pub struct UdsTransport {
    socket: Arc<AsyncFd<OwnedFd>>,
    registry: Arc<CustomTypeRegistry>,
    send_lock: Arc<tokio::sync::Mutex<()>>,
    receive_lock: tokio::sync::Mutex<()>,
    revocations: Arc<RevocationGroup>,
}

impl UdsTransport {
    /// Wraps a connected, non-blocking Unix `SOCK_SEQPACKET` socket.
    pub fn new(socket: OwnedFd) -> std::io::Result<Self> {
        let revocations = Arc::new(RevocationGroup::default());
        let relays = Arc::new(
            RelayManager::new(ByteChannelRelayLimits::default())
                .map_err(|error| std::io::Error::other(error.to_string()))?,
        );
        let mut registry = CustomTypeRegistry::new();
        registry
            .register(Arc::new(UdsFdHandler))
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        registry
            .register(Arc::new(ByteChannelTypeHandler::new(Arc::new(
                UdsByteChannelAdapter {
                    revocations: Arc::clone(&revocations),
                    relays,
                },
            ))))
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        Ok(Self {
            socket: Arc::new(AsyncFd::new(socket)?),
            registry: Arc::new(registry),
            send_lock: Arc::new(tokio::sync::Mutex::new(())),
            receive_lock: tokio::sync::Mutex::new(()),
            revocations,
        })
    }

    /// Creates a connected non-blocking `SOCK_SEQPACKET` pair.
    pub fn pair() -> std::io::Result<(Arc<Self>, Arc<Self>)> {
        let (left, right) = socketpair(
            AddressFamily::UNIX,
            SocketType::SEQPACKET,
            SocketFlags::NONBLOCK | SocketFlags::CLOEXEC,
            None,
        )?;
        Ok((Arc::new(Self::new(left)?), Arc::new(Self::new(right)?)))
    }

    /// Creates a stream attachment and a local FD tied to this control transport.
    pub fn open_byte_channel(&self) -> std::io::Result<(ByteChannel, ByteChannelFd)> {
        let (local, attachment) = socketpair(
            AddressFamily::UNIX,
            SocketType::STREAM,
            SocketFlags::CLOEXEC,
            None,
        )?;
        Ok(self.attach_byte_channel(attachment, local))
    }

    /// Adopts both ends of an existing stream pair under this control transport.
    ///
    /// `attachment` is transferred in RPC while `local` remains with the caller.
    /// Retain the returned [`ByteChannelFd`] while any duplicate is installed in
    /// a VMM or bridge so control-session abort can shut the channel down.
    pub fn attach_byte_channel(
        &self,
        attachment: OwnedFd,
        local: OwnedFd,
    ) -> (ByteChannel, ByteChannelFd) {
        let local = ByteChannelFd::new(local);
        local.bind(&self.revocations);
        let attachment = ByteChannelEndpoint::file_descriptor(attachment);
        attachment.bind(&self.revocations);
        (ByteChannel::from_endpoint(attachment), local)
    }

    fn encode_message(message: &CapnwebValue) -> Result<Vec<u8>> {
        let json = capnweb_to_json(message)?;
        let bytes = serde_json::to_vec(&json)
            .map_err(|error| Error::Message(format!("failed to encode UDS message: {error}")))?;
        if bytes.len() > MAX_MESSAGE_BYTES {
            return Err(Error::Message(format!(
                "UDS message exceeds maximum size of {MAX_MESSAGE_BYTES} bytes"
            )));
        }
        Ok(bytes)
    }

    fn send_packet(
        socket: &OwnedFd,
        bytes: &[u8],
        descriptors: &[Arc<OwnedFd>],
    ) -> std::io::Result<()> {
        let borrowed: Vec<BorrowedFd<'_>> = descriptors.iter().map(|fd| fd.as_fd()).collect();
        let mut control_space =
            vec![MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(MAX_MESSAGE_FDS))];
        let mut control = SendAncillaryBuffer::new(&mut control_space);
        if !borrowed.is_empty() && !control.push(SendAncillaryMessage::ScmRights(&borrowed)) {
            return Err(std::io::Error::other(
                "failed to construct SCM_RIGHTS message",
            ));
        }
        let written = sendmsg(
            socket,
            &[IoSlice::new(bytes)],
            &mut control,
            SendFlags::empty(),
        )
        .map_err(std::io::Error::from)?;
        if written != bytes.len() {
            return Err(std::io::Error::other("UDS send was truncated"));
        }
        Ok(())
    }

    fn receive_packet(socket: &OwnedFd) -> std::io::Result<(Vec<u8>, Vec<OwnedFd>)> {
        let mut bytes = vec![0u8; MAX_MESSAGE_BYTES];
        let mut control_space =
            vec![MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(MAX_MESSAGE_FDS))];
        let mut control = RecvAncillaryBuffer::new(&mut control_space);
        let received = recvmsg(
            socket,
            &mut [IoSliceMut::new(&mut bytes)],
            &mut control,
            RecvFlags::CMSG_CLOEXEC,
        )
        .map_err(std::io::Error::from)?;
        if received
            .flags
            .intersects(ReturnFlags::TRUNC | ReturnFlags::CTRUNC)
        {
            return Err(std::io::Error::other(
                "UDS message or descriptor list was truncated",
            ));
        }
        bytes.truncate(received.bytes);

        let mut descriptors = Vec::new();
        for message in control.drain() {
            if let RecvAncillaryMessage::ScmRights(rights) = message {
                descriptors.extend(rights);
            }
        }
        Ok((bytes, descriptors))
    }

    fn decode_message(
        bytes: &[u8],
        descriptors: Vec<OwnedFd>,
        registry: Arc<CustomTypeRegistry>,
    ) -> Result<TransportMessage> {
        let json: serde_json::Value = serde_json::from_slice(bytes)
            .map_err(|error| Error::Type(format!("invalid UDS message: {error}")))?;
        let value = json_to_capnweb(json)?;
        let codec =
            CustomMessageCodec::new(registry, Box::new(UdsMessageContext::incoming(descriptors)));
        Ok(TransportMessage::Value {
            value,
            codec: Some(Box::new(codec)),
        })
    }
}

impl Drop for UdsTransport {
    fn drop(&mut self) {
        self.revocations.revoke_all(Error::Message(
            "UDS control transport was dropped".to_owned(),
        ));
        let _ = shutdown(self.socket.get_ref(), Shutdown::Both);
    }
}

impl AnyRpcTransport for UdsTransport {
    fn encoding_level(&self) -> EncodingLevel {
        EncodingLevel::JsonCompatible
    }

    fn new_custom_codec(&self) -> Option<Box<CustomMessageCodec>> {
        Some(Box::new(CustomMessageCodec::new(
            Arc::clone(&self.registry),
            Box::new(UdsMessageContext::outgoing()),
        )))
    }

    fn send(&self, message: TransportMessage) -> Result<TransportSend> {
        let TransportMessage::Value {
            value: message,
            mut codec,
        } = message
        else {
            return Err(Error::Type(
                "UDS transport expected a structured value tree".to_owned(),
            ));
        };
        let bytes = Self::encode_message(&message)?;
        let descriptors = match codec.as_deref_mut() {
            Some(codec) => codec
                .context_mut::<UdsMessageContext>()
                .ok_or_else(|| Error::Type("UDS message has the wrong custom codec".to_owned()))?
                .take_outgoing()?,
            None => Vec::new(),
        };
        let socket = Arc::clone(&self.socket);
        let send_lock = Arc::clone(&self.send_lock);
        let size = Some(bytes.len());
        let sent = Box::pin(async move {
            let _send_guard = send_lock.lock().await;
            loop {
                let mut ready = socket.writable().await.map_err(|error| {
                    Error::Message(format!("UDS send readiness failed: {error}"))
                })?;
                match ready
                    .try_io(|socket| Self::send_packet(socket.get_ref(), &bytes, &descriptors))
                {
                    Ok(Ok(())) => return Ok(()),
                    Ok(Err(error)) => {
                        return Err(Error::Message(format!("UDS send failed: {error}")));
                    }
                    Err(_would_block) => continue,
                }
            }
        });
        Ok(TransportSend::Pending { size, sent })
    }

    fn receive(
        self: Arc<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<TransportMessage>> + Send + 'static>> {
        Box::pin(async move {
            let _receive_guard = self.receive_lock.lock().await;
            loop {
                let mut ready = self.socket.readable().await.map_err(|error| {
                    Error::Message(format!("UDS receive readiness failed: {error}"))
                })?;
                match ready.try_io(|socket| Self::receive_packet(socket.get_ref())) {
                    Ok(Ok((bytes, descriptors))) => {
                        return Self::decode_message(
                            &bytes,
                            descriptors,
                            Arc::clone(&self.registry),
                        );
                    }
                    Ok(Err(error)) => {
                        return Err(Error::Message(format!("UDS receive failed: {error}")));
                    }
                    Err(_would_block) => continue,
                }
            }
        })
    }

    fn abort(&self, reason: Error) {
        self.revocations.revoke_all(reason);
        let _ = shutdown(self.socket.get_ref(), Shutdown::Both);
    }
}

enum UdsDescriptors {
    Outgoing(Vec<Arc<OwnedFd>>),
    Incoming(Vec<Option<OwnedFd>>),
}

struct UdsMessageContext {
    descriptors: UdsDescriptors,
}

impl UdsMessageContext {
    fn outgoing() -> Self {
        Self {
            descriptors: UdsDescriptors::Outgoing(Vec::new()),
        }
    }

    fn incoming(descriptors: Vec<OwnedFd>) -> Self {
        Self {
            descriptors: UdsDescriptors::Incoming(descriptors.into_iter().map(Some).collect()),
        }
    }

    fn push_outgoing(&mut self, descriptor: Arc<OwnedFd>) -> Result<usize> {
        let UdsDescriptors::Outgoing(descriptors) = &mut self.descriptors else {
            return Err(Error::Type(
                "UDS codec has incoming state during send".to_owned(),
            ));
        };
        if descriptors.len() >= MAX_MESSAGE_FDS {
            return Err(Error::Message(format!(
                "UDS message exceeds maximum descriptor count of {MAX_MESSAGE_FDS}"
            )));
        }
        let index = descriptors.len();
        descriptors.push(descriptor);
        Ok(index)
    }

    fn take_incoming(&mut self, index: usize) -> Result<OwnedFd> {
        let UdsDescriptors::Incoming(descriptors) = &mut self.descriptors else {
            return Err(Error::Type(
                "UDS codec has outgoing state during receive".to_owned(),
            ));
        };
        descriptors
            .get_mut(index)
            .ok_or_else(|| Error::Type("UDS descriptor index is out of range".to_owned()))?
            .take()
            .ok_or_else(|| Error::Type("UDS descriptor index was used twice".to_owned()))
    }

    fn take_outgoing(&mut self) -> Result<Vec<Arc<OwnedFd>>> {
        let UdsDescriptors::Outgoing(descriptors) = &mut self.descriptors else {
            return Err(Error::Type(
                "UDS codec has incoming state during send".to_owned(),
            ));
        };
        Ok(std::mem::take(descriptors))
    }
}

impl CustomMessageContext for UdsMessageContext {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn finish(&mut self) -> Result<()> {
        if let UdsDescriptors::Incoming(descriptors) = &self.descriptors {
            if descriptors.iter().any(Option::is_some) {
                return Err(Error::Type(
                    "UDS message contained an unreferenced SCM_RIGHTS descriptor".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

struct UdsFdHandler;

impl CustomTypeHandler for UdsFdHandler {
    fn value_type_id(&self) -> TypeId {
        TypeId::of::<OwnedFd>()
    }

    fn wire_tag(&self) -> &'static str {
        "fd"
    }

    fn encode_fields(
        &self,
        value: &CustomValue,
        context: &mut dyn CustomMessageContext,
    ) -> Result<Vec<CapnwebValue>> {
        let descriptor = value.downcast::<OwnedFd>()?;
        let context = context
            .as_any_mut()
            .downcast_mut::<UdsMessageContext>()
            .ok_or_else(|| Error::Type("fd handler received the wrong context".to_owned()))?;
        let index = context.push_outgoing(descriptor)?;
        Ok(vec![CapnwebValue::Number(index as f64)])
    }

    fn decode_fields(
        &self,
        fields: &[CapnwebValue],
        context: &mut dyn CustomMessageContext,
    ) -> Result<CustomValue> {
        let [index] = fields else {
            return Err(Error::Type("invalid UDS fd marker".to_owned()));
        };
        let index = index
            .as_i64()
            .filter(|index| *index >= 0)
            .map(|index| index as usize)
            .ok_or_else(|| Error::Type("invalid UDS fd marker index".to_owned()))?;
        let context = context
            .as_any_mut()
            .downcast_mut::<UdsMessageContext>()
            .ok_or_else(|| Error::Type("fd handler received the wrong context".to_owned()))?;
        let descriptor = context.take_incoming(index)?;
        Ok(CustomValue::new(descriptor))
    }
}

struct UdsByteChannelAdapter {
    revocations: Arc<RevocationGroup>,
    relays: Arc<RelayManager>,
}

struct ReservedUdsRelay {
    attachment: ByteChannelFd,
    destination: crate::channel::ByteChannelStream,
    reservation: RelayReservation,
}

impl UdsByteChannelAdapter {
    fn reserve_relay_descriptor(&self) -> Result<ReservedUdsRelay> {
        let reservation = self.relays.reserve()?;
        let (local, attachment) = socketpair(
            AddressFamily::UNIX,
            SocketType::STREAM,
            SocketFlags::CLOEXEC,
            None,
        )
        .map_err(|error| Error::Message(format!("failed to create relay socketpair: {error}")))?;
        let local = ByteChannelFd::new(local);
        local.bind(&self.revocations);
        let attachment = ByteChannelFd::new(attachment);
        attachment.bind(&self.revocations);
        Ok(ReservedUdsRelay {
            attachment,
            destination: local.into_relay_stream()?,
            reservation,
        })
    }

    fn start_relay_descriptor(
        &self,
        source: EndpointFuture,
        relay: ReservedUdsRelay,
    ) -> ByteChannelFd {
        RelayManager::spawn_to_reserved(source, relay.destination, relay.reservation);
        relay.attachment
    }
}

impl ByteChannelTransportAdapter for UdsByteChannelAdapter {
    fn encode(
        &self,
        channel: &ByteChannel,
        context: &mut dyn CustomMessageContext,
    ) -> Result<Vec<CapnwebValue>> {
        let relay = channel
            .needs_fd_relay()?
            .then(|| self.reserve_relay_descriptor())
            .transpose()?;
        let descriptor = match channel.take_for_transport()? {
            ByteChannelTransfer::Local(ByteChannelEndpoint::FileDescriptor(descriptor)) => {
                descriptor
            }
            ByteChannelTransfer::Local(endpoint @ ByteChannelEndpoint::Stream(_)) => self
                .start_relay_descriptor(
                    Box::pin(async move { Ok(endpoint) }),
                    relay.expect("stream requires a UDS relay reservation"),
                ),
            ByteChannelTransfer::Incoming(source) => self.start_relay_descriptor(
                source,
                relay.expect("incoming channel requires a UDS relay reservation"),
            ),
        };
        validate_connected_stream(&descriptor)?;
        descriptor.bind(&self.revocations);
        let outgoing = descriptor.try_clone_to_owned().map_err(|error| {
            Error::Message(format!("failed to duplicate byte channel: {error}"))
        })?;
        let context = context
            .as_any_mut()
            .downcast_mut::<UdsMessageContext>()
            .ok_or_else(|| Error::Type("byte channel received the wrong UDS context".to_owned()))?;
        let index = context.push_outgoing(Arc::new(outgoing))?;
        Ok(vec![CapnwebValue::Number(index as f64)])
    }

    fn decode(
        &self,
        fields: &[CapnwebValue],
        context: &mut dyn CustomMessageContext,
    ) -> Result<ByteChannel> {
        let [index] = fields else {
            return Err(Error::Type("invalid UDS byte channel marker".to_owned()));
        };
        let index = index
            .as_i64()
            .filter(|index| *index >= 0)
            .map(|index| index as usize)
            .ok_or_else(|| Error::Type("invalid UDS byte channel descriptor index".to_owned()))?;
        let context = context
            .as_any_mut()
            .downcast_mut::<UdsMessageContext>()
            .ok_or_else(|| Error::Type("byte channel received the wrong UDS context".to_owned()))?;
        let descriptor = context.take_incoming(index)?;
        validate_connected_stream(&descriptor)?;
        let descriptor = ByteChannelFd::new(descriptor);
        descriptor.bind(&self.revocations);
        Ok(ByteChannel::incoming_endpoint(
            ByteChannelEndpoint::FileDescriptor(descriptor),
        ))
    }
}

fn validate_connected_stream(descriptor: &impl AsFd) -> Result<()> {
    let socket_type = rustix::net::sockopt::socket_type(descriptor)
        .map_err(|error| Error::Type(format!("byte channel is not a socket: {error}")))?;
    if socket_type != SocketType::STREAM {
        return Err(Error::Type(
            "UDS byte channel descriptor is not SOCK_STREAM".to_owned(),
        ));
    }
    if getpeername(descriptor)
        .map_err(|error| Error::Type(format!("failed to inspect byte channel peer: {error}")))?
        .is_none()
    {
        return Err(Error::Type(
            "UDS byte channel descriptor is not connected".to_owned(),
        ));
    }
    Ok(())
}
