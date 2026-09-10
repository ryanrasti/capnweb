//! Unix-domain-socket transport with SCM_RIGHTS file-descriptor passing.

use std::collections::BTreeMap;
use std::future::Future;
use std::io::{IoSlice, IoSliceMut};
use std::mem::MaybeUninit;
use std::os::fd::{BorrowedFd, OwnedFd};
use std::os::unix::net::UnixDatagram;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use rustix::net::{
    RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags, ReturnFlags, SendAncillaryBuffer,
    SendAncillaryMessage, SendFlags, recvmsg, sendmsg,
};

use crate::core::{CapnwebValue, Error, Fd, Result};
use crate::rpc::{AnyRpcTransport, TransportMessage, TransportSend};
use crate::serialize::{EncodingLevel, capnweb_to_json, json_to_capnweb};

const MAX_MESSAGE_BYTES: usize = 256 * 1024;
const MAX_MESSAGE_FDS: usize = 64;

/// A message-oriented UDS transport which encodes descriptors as `["fd", index]` and transfers
/// their ownership with SCM_RIGHTS.
pub struct UdsTransport {
    socket: Arc<UnixDatagram>,
    send_lock: Mutex<()>,
}

impl UdsTransport {
    /// Wraps a connected Unix datagram socket.
    pub fn new(socket: UnixDatagram) -> Self {
        Self {
            socket: Arc::new(socket),
            send_lock: Mutex::new(()),
        }
    }

    /// Creates a connected pair, primarily for local peers and tests.
    pub fn pair() -> std::io::Result<(Arc<Self>, Arc<Self>)> {
        let (left, right) = UnixDatagram::pair()?;
        Ok((Arc::new(Self::new(left)), Arc::new(Self::new(right))))
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

    fn send_encoded(&self, bytes: &[u8], descriptors: &[Fd]) -> Result<()> {
        if descriptors.len() > MAX_MESSAGE_FDS {
            return Err(Error::Message(format!(
                "UDS message exceeds maximum descriptor count of {MAX_MESSAGE_FDS}"
            )));
        }
        let borrowed: Vec<BorrowedFd<'_>> = descriptors.iter().map(Fd::as_fd).collect();
        let mut control_space =
            vec![MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(MAX_MESSAGE_FDS))];
        let mut control = SendAncillaryBuffer::new(&mut control_space);
        if !borrowed.is_empty() && !control.push(SendAncillaryMessage::ScmRights(&borrowed)) {
            return Err(Error::Message(
                "failed to construct SCM_RIGHTS message".to_owned(),
            ));
        }
        let written = sendmsg(
            self.socket.as_ref(),
            &[IoSlice::new(bytes)],
            &mut control,
            SendFlags::empty(),
        )
        .map_err(|error| Error::Message(format!("UDS send failed: {error}")))?;
        if written != bytes.len() {
            return Err(Error::Message("UDS send was truncated".to_owned()));
        }
        Ok(())
    }

    fn receive_blocking(socket: Arc<UnixDatagram>) -> Result<TransportMessage> {
        let mut bytes = vec![0u8; MAX_MESSAGE_BYTES];
        let mut control_space =
            vec![MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(MAX_MESSAGE_FDS))];
        let mut control = RecvAncillaryBuffer::new(&mut control_space);
        let received = recvmsg(
            socket.as_ref(),
            &mut [IoSliceMut::new(&mut bytes)],
            &mut control,
            RecvFlags::CMSG_CLOEXEC,
        )
        .map_err(|error| Error::Message(format!("UDS receive failed: {error}")))?;
        if received
            .flags
            .intersects(ReturnFlags::TRUNC | ReturnFlags::CTRUNC)
        {
            return Err(Error::Message(
                "UDS message or descriptor list was truncated".to_owned(),
            ));
        }
        bytes.truncate(received.bytes);

        let mut descriptors: Vec<Arc<OwnedFd>> = Vec::new();
        for message in control.drain() {
            if let RecvAncillaryMessage::ScmRights(rights) = message {
                descriptors.extend(rights.map(Arc::new));
            }
        }
        if descriptors.len() > MAX_MESSAGE_FDS {
            return Err(Error::Message(format!(
                "UDS message exceeds maximum descriptor count of {MAX_MESSAGE_FDS}"
            )));
        }

        let json: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|error| Error::Type(format!("invalid UDS message: {error}")))?;
        let message = json_to_capnweb(json)?;
        let mut claimed = vec![false; descriptors.len()];
        let message = restore_incoming(message, &descriptors, &mut claimed)?;
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
        let _guard = self.send_lock.lock().expect("UDS send mutex poisoned");
        self.send_encoded(&bytes, &descriptors)?;
        Ok(TransportSend::Done(Some(bytes.len())))
    }

    fn receive(
        self: Arc<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<TransportMessage>> + Send + 'static>> {
        Box::pin(async move {
            let socket = Arc::clone(&self.socket);
            tokio::task::spawn_blocking(move || Self::receive_blocking(socket))
                .await
                .map_err(|error| Error::Message(format!("UDS receive task failed: {error}")))?
        })
    }

    fn abort(&self, _reason: Error) {
        let _ = self.socket.shutdown(std::net::Shutdown::Both);
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

fn restore_incoming(
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
        CapnwebValue::Array(values) => values
            .into_iter()
            .map(|value| restore_incoming(value, descriptors, claimed))
            .collect::<Result<Vec<_>>>()
            .map(CapnwebValue::Array),
        CapnwebValue::Object(values) => values
            .into_iter()
            .map(|(key, value)| Ok((key, restore_incoming(value, descriptors, claimed)?)))
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
