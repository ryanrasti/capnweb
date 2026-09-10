//! Unix-domain-socket transport with SCM_RIGHTS file-descriptor passing.

use std::collections::BTreeMap;
use std::future::Future;
use std::io::{IoSlice, IoSliceMut};
use std::mem::MaybeUninit;
use std::os::fd::{BorrowedFd, OwnedFd};
use std::pin::Pin;
use std::sync::Arc;

use base64::Engine as _;
use rustix::net::{
    AddressFamily, RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags, ReturnFlags,
    SendAncillaryBuffer, SendAncillaryMessage, SendFlags, Shutdown, SocketFlags, SocketType,
    recvmsg, sendmsg, shutdown, socketpair,
};
use tokio::io::unix::AsyncFd;

use crate::core::{CapnwebValue, Error, Fd, Result};
use crate::rpc::{AnyRpcTransport, TransportMessage, TransportSend};
use crate::serialize::{EncodingLevel, capnweb_to_json, json_to_capnweb};

const MAX_MESSAGE_BYTES: usize = 256 * 1024;
const MAX_MESSAGE_FDS: usize = 64;

/// A message-oriented UDS transport which encodes descriptors as `["fd", index]` and transfers
/// their ownership with SCM_RIGHTS.
pub struct UdsTransport {
    socket: Arc<AsyncFd<OwnedFd>>,
    send_lock: Arc<tokio::sync::Mutex<()>>,
    receive_lock: tokio::sync::Mutex<()>,
}

impl UdsTransport {
    /// Wraps a connected, non-blocking Unix `SOCK_SEQPACKET` socket.
    pub fn new(socket: OwnedFd) -> std::io::Result<Self> {
        Ok(Self {
            socket: Arc::new(AsyncFd::new(socket)?),
            send_lock: Arc::new(tokio::sync::Mutex::new(())),
            receive_lock: tokio::sync::Mutex::new(()),
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

    fn encode_message(message: CapnwebValue) -> Result<(Vec<u8>, Vec<Fd>)> {
        let mut descriptors = Vec::new();
        let message = prepare_outgoing(message, &mut descriptors)?;
        let json = capnweb_to_json(&message)?;
        let bytes = serde_json::to_vec(&json)
            .map_err(|error| Error::Message(format!("failed to encode UDS message: {error}")))?;
        if bytes.len() > MAX_MESSAGE_BYTES {
            return Err(Error::Message(format!(
                "UDS message exceeds maximum size of {MAX_MESSAGE_BYTES} bytes"
            )));
        }
        Ok((bytes, descriptors))
    }

    fn send_packet(socket: &OwnedFd, bytes: &[u8], descriptors: &[Fd]) -> std::io::Result<()> {
        let borrowed: Vec<BorrowedFd<'_>> = descriptors.iter().map(Fd::as_fd).collect();
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

    fn receive_packet(socket: &OwnedFd) -> std::io::Result<(Vec<u8>, Vec<Arc<OwnedFd>>)> {
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
                descriptors.extend(rights.map(Arc::new));
            }
        }
        Ok((bytes, descriptors))
    }

    fn decode_message(bytes: &[u8], descriptors: Vec<Arc<OwnedFd>>) -> Result<TransportMessage> {
        let json: serde_json::Value = serde_json::from_slice(bytes)
            .map_err(|error| Error::Type(format!("invalid UDS message: {error}")))?;
        let message = json_to_capnweb(json)?;
        let mut claimed = vec![false; descriptors.len()];
        let message = restore_message(message, &descriptors, &mut claimed)?;
        if claimed.iter().any(|claimed| !claimed) {
            return Err(Error::Type(
                "UDS message contained an unreferenced SCM_RIGHTS descriptor".to_owned(),
            ));
        }
        Ok(TransportMessage::Value(message))
    }
}

impl AnyRpcTransport for UdsTransport {
    fn encoding_level(&self) -> EncodingLevel {
        EncodingLevel::StructuredClonable
    }

    fn send(&self, message: TransportMessage) -> Result<TransportSend> {
        let TransportMessage::Value(message) = message else {
            return Err(Error::Type(
                "UDS transport expected a structured value tree".to_owned(),
            ));
        };
        let (bytes, descriptors) = Self::encode_message(message)?;
        let socket = Arc::clone(&self.socket);
        let send_lock = Arc::clone(&self.send_lock);
        Ok(TransportSend::Pending(Box::pin(async move {
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
        })))
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
                        return Self::decode_message(&bytes, descriptors);
                    }
                    Ok(Err(error)) => {
                        return Err(Error::Message(format!("UDS receive failed: {error}")));
                    }
                    Err(_would_block) => continue,
                }
            }
        })
    }

    fn abort(&self, _reason: Error) {
        let _ = shutdown(self.socket.get_ref(), Shutdown::Both);
    }
}

fn prepare_outgoing(value: CapnwebValue, descriptors: &mut Vec<Fd>) -> Result<CapnwebValue> {
    match value {
        CapnwebValue::Fd(fd) => {
            let index = descriptors.len();
            if index >= MAX_MESSAGE_FDS {
                return Err(Error::Message(format!(
                    "UDS message exceeds maximum descriptor count of {MAX_MESSAGE_FDS}"
                )));
            }
            descriptors.push(fd);
            Ok(fd_marker(index))
        }
        CapnwebValue::Bytes(bytes) => Ok(CapnwebValue::String(
            base64::engine::general_purpose::STANDARD_NO_PAD.encode(bytes),
        )),
        CapnwebValue::BigInt(value) => Ok(CapnwebValue::Array(vec![
            CapnwebValue::String("bigint".to_owned()),
            CapnwebValue::String(value.to_string()),
        ])),
        CapnwebValue::Undefined => Ok(CapnwebValue::Array(vec![CapnwebValue::String(
            "undefined".to_owned(),
        )])),
        CapnwebValue::Number(value) if value.is_nan() => {
            Ok(CapnwebValue::Array(vec![CapnwebValue::String(
                "nan".to_owned(),
            )]))
        }
        CapnwebValue::Number(value) if value == f64::INFINITY => {
            Ok(CapnwebValue::Array(vec![CapnwebValue::String(
                "inf".to_owned(),
            )]))
        }
        CapnwebValue::Number(value) if value == f64::NEG_INFINITY => {
            Ok(CapnwebValue::Array(vec![CapnwebValue::String(
                "-inf".to_owned(),
            )]))
        }
        CapnwebValue::Array(values) => values
            .into_iter()
            .map(|value| prepare_outgoing(value, descriptors))
            .collect::<Result<Vec<_>>>()
            .map(CapnwebValue::Array),
        CapnwebValue::Object(values) => values
            .into_iter()
            .map(|(key, value)| Ok((key, prepare_outgoing(value, descriptors)?)))
            .collect::<Result<BTreeMap<_, _>>>()
            .map(CapnwebValue::Object),
        value => Ok(value),
    }
}

fn fd_marker(index: usize) -> CapnwebValue {
    CapnwebValue::Array(vec![
        CapnwebValue::String("fd".to_owned()),
        CapnwebValue::Number(index as f64),
    ])
}

fn restore_message(
    value: CapnwebValue,
    descriptors: &[Arc<OwnedFd>],
    claimed: &mut [bool],
) -> Result<CapnwebValue> {
    let CapnwebValue::Array(mut message) = value else {
        return Ok(value);
    };
    let tag = message.first().and_then(CapnwebValue::as_str);
    let value_index = match tag {
        Some("push" | "stream" | "abort") if message.len() > 1 => Some(1),
        Some("resolve" | "reject") if message.len() > 2 => Some(2),
        _ => None,
    };
    if let Some(index) = value_index {
        let value = std::mem::replace(&mut message[index], CapnwebValue::Null);
        message[index] = restore_expression(value, descriptors, claimed)?;
    }
    Ok(CapnwebValue::Array(message))
}

fn restore_expression(
    value: CapnwebValue,
    descriptors: &[Arc<OwnedFd>],
    claimed: &mut [bool],
) -> Result<CapnwebValue> {
    match value {
        CapnwebValue::Array(values) if is_fd_marker(&values) => {
            let index = values[1]
                .as_i64()
                .filter(|index| *index >= 0)
                .map(|index| index as usize)
                .ok_or_else(|| Error::Type("invalid UDS fd marker index".to_owned()))?;
            let descriptor = descriptors
                .get(index)
                .ok_or_else(|| Error::Type("UDS fd marker index is out of range".to_owned()))?;
            claimed[index] = true;
            Ok(CapnwebValue::Fd(Fd::from_shared(Arc::clone(descriptor))))
        }
        CapnwebValue::Array(mut values)
            if values.len() == 1 && matches!(values.first(), Some(CapnwebValue::Array(_))) =>
        {
            let CapnwebValue::Array(literal) = values.remove(0) else {
                unreachable!();
            };
            let literal = literal
                .into_iter()
                .map(|value| restore_expression(value, descriptors, claimed))
                .collect::<Result<Vec<_>>>()?;
            Ok(CapnwebValue::Array(vec![CapnwebValue::Array(literal)]))
        }
        CapnwebValue::Array(mut values)
            if matches!(
                values.first().and_then(CapnwebValue::as_str),
                Some("pipeline" | "import")
            ) && values.len() == 4 =>
        {
            let args = std::mem::replace(&mut values[3], CapnwebValue::Null);
            let CapnwebValue::Array(args) = args else {
                return Err(Error::Type(
                    "RPC call arguments are not an array".to_owned(),
                ));
            };
            values[3] = CapnwebValue::Array(
                args.into_iter()
                    .map(|value| restore_expression(value, descriptors, claimed))
                    .collect::<Result<Vec<_>>>()?,
            );
            Ok(CapnwebValue::Array(values))
        }
        CapnwebValue::Array(values) => values
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                if index == 0 {
                    Ok(value)
                } else {
                    restore_expression(value, descriptors, claimed)
                }
            })
            .collect::<Result<Vec<_>>>()
            .map(CapnwebValue::Array),
        CapnwebValue::Object(values) => values
            .into_iter()
            .map(|(key, value)| Ok((key, restore_expression(value, descriptors, claimed)?)))
            .collect::<Result<BTreeMap<_, _>>>()
            .map(CapnwebValue::Object),
        value => Ok(value),
    }
}

fn is_fd_marker(values: &[CapnwebValue]) -> bool {
    matches!(
        values,
        [CapnwebValue::String(tag), CapnwebValue::Number(_)] if tag == "fd"
    )
}
