//! Stable typed facade used by generated Rust bindings.

use std::collections::BTreeMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use num_bigint::BigInt;

use crate::channel::ByteChannel;
use crate::core::{CapnwebValue, Error, Result, RpcPromise, RpcStub};

/// An opaque byte string, distinct from a Cap'n Web array of numeric values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Bytes(pub Vec<u8>);

impl From<Vec<u8>> for Bytes {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl From<Bytes> for Vec<u8> {
    fn from(value: Bytes) -> Self {
        value.0
    }
}

/// Describes the wire-visible members of a generated RPC interface.
pub trait RpcInterfaceDescriptor {
    /// Returns whether `wire_name` is declared by the interface.
    fn has_member(wire_name: &str) -> bool;

    /// Returns whether `wire_name` is declared as a callable method.
    fn has_method(wire_name: &str) -> bool {
        Self::has_member(wire_name)
    }

    /// Returns whether `wire_name` is declared as a property getter.
    fn has_getter(wire_name: &str) -> bool {
        Self::has_member(wire_name)
    }
}

/// Converts a typed application value into Cap'n Web's dynamic value representation.
pub trait IntoCapnwebValue {
    /// Performs the conversion.
    fn into_capnweb_value(self) -> Result<CapnwebValue>;
}

/// Converts Cap'n Web's dynamic value representation into a typed application value.
pub trait FromCapnwebValue: Sized {
    /// Performs the checked conversion.
    fn from_capnweb_value(value: CapnwebValue) -> Result<Self>;
}

impl IntoCapnwebValue for CapnwebValue {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        Ok(self)
    }
}

impl FromCapnwebValue for CapnwebValue {
    fn from_capnweb_value(value: CapnwebValue) -> Result<Self> {
        Ok(value)
    }
}

impl IntoCapnwebValue for () {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        Ok(CapnwebValue::Undefined)
    }
}

impl FromCapnwebValue for () {
    fn from_capnweb_value(value: CapnwebValue) -> Result<Self> {
        match value {
            CapnwebValue::Undefined => Ok(()),
            other => Err(type_mismatch("undefined", &other)),
        }
    }
}

impl IntoCapnwebValue for bool {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        Ok(CapnwebValue::Bool(self))
    }
}

impl FromCapnwebValue for bool {
    fn from_capnweb_value(value: CapnwebValue) -> Result<Self> {
        match value {
            CapnwebValue::Bool(value) => Ok(value),
            other => Err(type_mismatch("boolean", &other)),
        }
    }
}

impl IntoCapnwebValue for f64 {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        Ok(CapnwebValue::Number(self))
    }
}

impl FromCapnwebValue for f64 {
    fn from_capnweb_value(value: CapnwebValue) -> Result<Self> {
        match value {
            CapnwebValue::Number(value) => Ok(value),
            other => Err(type_mismatch("number", &other)),
        }
    }
}

impl IntoCapnwebValue for String {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        Ok(CapnwebValue::String(self))
    }
}

impl IntoCapnwebValue for &str {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        Ok(CapnwebValue::String(self.to_owned()))
    }
}

impl FromCapnwebValue for String {
    fn from_capnweb_value(value: CapnwebValue) -> Result<Self> {
        match value {
            CapnwebValue::String(value) => Ok(value),
            other => Err(type_mismatch("string", &other)),
        }
    }
}

impl IntoCapnwebValue for BigInt {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        Ok(CapnwebValue::BigInt(self))
    }
}

impl FromCapnwebValue for BigInt {
    fn from_capnweb_value(value: CapnwebValue) -> Result<Self> {
        match value {
            CapnwebValue::BigInt(value) => Ok(value),
            other => Err(type_mismatch("bigint", &other)),
        }
    }
}

impl IntoCapnwebValue for Bytes {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        Ok(CapnwebValue::Bytes(self.0))
    }
}

impl FromCapnwebValue for Bytes {
    fn from_capnweb_value(value: CapnwebValue) -> Result<Self> {
        match value {
            CapnwebValue::Bytes(value) => Ok(Self(value)),
            other => Err(type_mismatch("bytes", &other)),
        }
    }
}

impl IntoCapnwebValue for ByteChannel {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        Ok(CapnwebValue::custom(self))
    }
}

impl FromCapnwebValue for ByteChannel {
    fn from_capnweb_value(value: CapnwebValue) -> Result<Self> {
        match value {
            CapnwebValue::Custom(value) => value
                .downcast::<ByteChannel>()
                .map(|channel| channel.as_ref().clone()),
            other => Err(type_mismatch("byte channel", &other)),
        }
    }
}

impl IntoCapnwebValue for crate::streams::ReadableStream {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        Ok(CapnwebValue::ReadableStream(self))
    }
}

impl IntoCapnwebValue for crate::streams::WritableStream {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        Ok(CapnwebValue::WritableStream(self))
    }
}

impl<T: IntoCapnwebValue> IntoCapnwebValue for Vec<T> {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        self.into_iter()
            .map(IntoCapnwebValue::into_capnweb_value)
            .collect::<Result<Vec<_>>>()
            .map(CapnwebValue::Array)
    }
}

impl<T: FromCapnwebValue> FromCapnwebValue for Vec<T> {
    fn from_capnweb_value(value: CapnwebValue) -> Result<Self> {
        let CapnwebValue::Array(values) = value else {
            return Err(type_mismatch("array", &value));
        };
        values
            .into_iter()
            .map(FromCapnwebValue::from_capnweb_value)
            .collect()
    }
}

macro_rules! tuple_conversions {
    ($(($($type:ident $value:ident),+)),+ $(,)?) => {
        $(
            impl<$($type: IntoCapnwebValue),+> IntoCapnwebValue for ($($type,)+) {
                fn into_capnweb_value(self) -> Result<CapnwebValue> {
                    let ($($value,)+) = self;
                    Ok(CapnwebValue::Array(vec![
                        $($value.into_capnweb_value()?,)+
                    ]))
                }
            }

            impl<$($type: FromCapnwebValue),+> FromCapnwebValue for ($($type,)+) {
                fn from_capnweb_value(value: CapnwebValue) -> Result<Self> {
                    let CapnwebValue::Array(values) = value else {
                        return Err(type_mismatch("tuple", &value));
                    };
                    let mut values = values.into_iter();
                    let result = (
                        $($type::from_capnweb_value(
                            values.next().unwrap_or(CapnwebValue::Undefined),
                        )?,)+
                    );
                    if values.next().is_some() {
                        return Err(Error::Type("tuple has too many elements".to_owned()));
                    }
                    Ok(result)
                }
            }
        )+
    };
}

tuple_conversions!(
    (A a),
    (A a, B b),
    (A a, B b, C c),
    (A a, B b, C c, D d),
    (A a, B b, C c, D d, E e),
    (A a, B b, C c, D d, E e, F f),
    (A a, B b, C c, D d, E e, F f, G g),
    (A a, B b, C c, D d, E e, F f, G g, H h),
);

impl<T: IntoCapnwebValue> IntoCapnwebValue for BTreeMap<String, T> {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        self.into_iter()
            .map(|(key, value)| Ok((key, value.into_capnweb_value()?)))
            .collect::<Result<BTreeMap<_, _>>>()
            .map(CapnwebValue::Object)
    }
}

impl<T: FromCapnwebValue> FromCapnwebValue for BTreeMap<String, T> {
    fn from_capnweb_value(value: CapnwebValue) -> Result<Self> {
        let CapnwebValue::Object(values) = value else {
            return Err(type_mismatch("object", &value));
        };
        values
            .into_iter()
            .map(|(key, value)| Ok((key, T::from_capnweb_value(value)?)))
            .collect()
    }
}

impl<T: IntoCapnwebValue> IntoCapnwebValue for Option<T> {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        match self {
            Some(value) => value.into_capnweb_value(),
            None => Ok(CapnwebValue::Undefined),
        }
    }
}

impl<T: FromCapnwebValue> FromCapnwebValue for Option<T> {
    fn from_capnweb_value(value: CapnwebValue) -> Result<Self> {
        match value {
            CapnwebValue::Undefined | CapnwebValue::Null => Ok(None),
            value => T::from_capnweb_value(value).map(Some),
        }
    }
}

impl<T: crate::core::RpcTargetInner> IntoCapnwebValue for std::sync::Arc<T> {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        let target: std::sync::Arc<dyn crate::core::RpcTargetInner> = self;
        Ok(CapnwebValue::Target(target))
    }
}

impl IntoCapnwebValue for RpcStub {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        Ok(CapnwebValue::Stub(self))
    }
}

impl FromCapnwebValue for RpcStub {
    fn from_capnweb_value(value: CapnwebValue) -> Result<Self> {
        match value {
            CapnwebValue::Stub(stub) => Ok(stub),
            CapnwebValue::Target(target) => Ok(RpcStub::from_value(CapnwebValue::Target(target))),
            other => Err(type_mismatch("capability", &other)),
        }
    }
}

impl IntoCapnwebValue for RpcPromise {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        Ok(CapnwebValue::RpcPromise(self))
    }
}

impl FromCapnwebValue for RpcPromise {
    fn from_capnweb_value(value: CapnwebValue) -> Result<Self> {
        match value {
            CapnwebValue::RpcPromise(promise) => Ok(promise),
            other => Err(type_mismatch("RPC promise", &other)),
        }
    }
}

/// A generated promise facade which decodes the dynamic resolution as `T`.
pub struct TypedRpcPromise<T> {
    inner: RpcPromise,
    marker: PhantomData<fn() -> T>,
}

impl<T> TypedRpcPromise<T> {
    /// Wraps an untyped pipelined RPC promise.
    pub fn new(inner: RpcPromise) -> Self {
        Self {
            inner,
            marker: PhantomData,
        }
    }

    /// Returns the underlying untyped promise for additional generated pipelining.
    pub fn into_inner(self) -> RpcPromise {
        self.inner
    }
}

impl<T> Clone for TypedRpcPromise<T> {
    fn clone(&self) -> Self {
        Self::new(self.inner.clone())
    }
}

impl<T: FromCapnwebValue> Future for TypedRpcPromise<T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll(context) {
            Poll::Ready(Ok(value)) => Poll::Ready(T::from_capnweb_value(value)),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn type_mismatch(expected: &str, actual: &CapnwebValue) -> Error {
    Error::Type(format!("expected {expected}, got {actual:?}"))
}
