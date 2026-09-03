//! A direct Rust translation of the Cap'n Web runtime.
//!
//! Internal names and state transitions intentionally follow the JavaScript
//! implementation. Generated macros replace only JavaScript proxy method
//! interception and `RpcTarget` prototype reflection.
//!
//! Parity status is tracked explicitly in `rust/PARITY-AUDIT.md`; partial
//! counterparts are not treated as complete.

extern crate self as capnweb;

mod core;
mod rpc;
mod serialize;

/// HTTP batch RPC counterpart; deferred in the current parity pass.
pub mod batch;
/// Map instruction runtime. The wire protocol remains incomplete.
pub mod map;
/// JavaScript MessagePort counterpart; not yet implemented.
pub mod messageport;
/// Stream runtime counterpart; deferred in the current parity pass.
pub mod streams;
/// WebSocket session helpers and transport exports.
pub mod websocket;

/// Framed transport implementations and traits.
pub mod transport;

use std::sync::Arc;

pub use capnweb_macros::{rpc_interface, rpc_target};
pub use core::{
    CapnwebValue, ErrorStubHook, FromCapnwebValue, IntoCapnwebValue, MaybePromise, PayloadStubHook,
    PromiseStubHook, PropertyPath, RpcPayload, RpcPromise, RpcStub, RpcTarget, RpcTargetInner,
    StubHook, StubHookInner, TargetStubHook, TypedRpcPromise,
};
pub use num_bigint::BigInt;
pub use rpc::RpcSession;
pub use serialize::{
    DEFAULT_MAX_DEPTH, RpcLimits, deserialize, deserialize_with_limits, serialize,
};

/// Runtime result type used behind the generated application boundary.
pub type Result<T> = std::result::Result<T, Error>;

/// An RPC or transport error.
#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    /// A malformed or unsupported protocol message.
    #[error("protocol error: {0}")]
    Protocol(String),
    /// A dynamic value did not match the expected Rust type.
    #[error("type error: {0}")]
    Type(String),
    /// A target has no exposed method with this name.
    #[error("no such RPC method: {0}")]
    NoSuchMethod(String),
    /// The peer or local application rejected an operation.
    #[error("remote error: {0}")]
    Remote(String),
    /// The session transport disconnected.
    #[error("RPC transport disconnected")]
    Disconnected,
    /// An I/O operation failed.
    #[error("I/O error: {0}")]
    Io(String),
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

/// A generated-call argument.
pub struct Argument(CapnwebValue);

impl From<Argument> for CapnwebValue {
    fn from(argument: Argument) -> Self {
        argument.0
    }
}

impl From<f64> for Argument {
    fn from(value: f64) -> Self {
        Self(CapnwebValue::Number(value))
    }
}

impl From<bool> for Argument {
    fn from(value: bool) -> Self {
        Self(CapnwebValue::Bool(value))
    }
}

impl From<String> for Argument {
    fn from(value: String) -> Self {
        Self(CapnwebValue::String(value))
    }
}

impl From<&str> for Argument {
    fn from(value: &str) -> Self {
        Self(CapnwebValue::String(value.to_owned()))
    }
}

impl From<CapnwebValue> for Argument {
    fn from(value: CapnwebValue) -> Self {
        Self(value)
    }
}

impl From<BigInt> for Argument {
    fn from(value: BigInt) -> Self {
        Self(CapnwebValue::BigInt(value))
    }
}

impl From<Vec<u8>> for Argument {
    fn from(value: Vec<u8>) -> Self {
        Self(CapnwebValue::Bytes(value))
    }
}

impl From<RpcStub> for Argument {
    fn from(value: RpcStub) -> Self {
        Self(CapnwebValue::Stub(value))
    }
}

impl From<RpcPromise> for Argument {
    fn from(value: RpcPromise) -> Self {
        Self(CapnwebValue::Promise(value))
    }
}

impl<T> From<TypedRpcPromise<T>> for Argument
where
    T: FromCapnwebValue + Send + 'static,
{
    fn from(value: TypedRpcPromise<T>) -> Self {
        Self(CapnwebValue::Promise(value.into_untyped()))
    }
}

impl<T: RpcTargetInner> From<Arc<T>> for Argument {
    fn from(value: Arc<T>) -> Self {
        Self(CapnwebValue::Target(value))
    }
}

/// Starts a session and returns the peer's main stub.
pub fn start_session<T, M>(transport: T, local_main: Arc<M>) -> RpcStub
where
    T: transport::Transport,
    M: RpcTargetInner,
{
    start_session_with_limits(transport, local_main, RpcLimits::default())
}

/// Starts a session with explicit receive-side resource limits.
pub fn start_session_with_limits<T, M>(
    transport: T,
    local_main: Arc<M>,
    limits: RpcLimits,
) -> RpcStub
where
    T: transport::Transport,
    M: RpcTargetInner,
{
    let main: RpcTarget = local_main;
    RpcSession::new_with_limits(transport, main, limits).get_remote_main()
}

/// The crate version supplied by Cargo.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
