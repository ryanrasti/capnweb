//! Direct structural translation of `src/core.ts`.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use tokio::sync::{Notify, OnceCell};

use crate::rpc::RpcImportHook;
use crate::{Error, Result};

/// A property path in an RPC expression.
pub type PropertyPath = Vec<String>;

/// Rust equivalent of a JavaScript value accepted by the v0 implementation.
#[derive(Clone)]
pub enum CapnwebValue {
    /// JavaScript `null` or `undefined`.
    Null,
    /// A boolean.
    Bool(bool),
    /// A JavaScript number.
    Number(f64),
    /// A string.
    String(String),
    /// An array.
    Array(Vec<Self>),
    /// A plain object.
    Object(BTreeMap<String, Self>),
    /// A local target.
    Target(RpcTarget),
    /// A stub.
    Stub(RpcStub),
    /// An unresolved RPC value.
    Promise(RpcPromise),
}

impl std::fmt::Debug for CapnwebValue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => formatter.write_str("Null"),
            Self::Bool(value) => formatter.debug_tuple("Bool").field(value).finish(),
            Self::Number(value) => formatter.debug_tuple("Number").field(value).finish(),
            Self::String(value) => formatter.debug_tuple("String").field(value).finish(),
            Self::Array(value) => formatter.debug_tuple("Array").field(value).finish(),
            Self::Object(value) => formatter.debug_tuple("Object").field(value).finish(),
            Self::Target(_) => formatter.write_str("Target(..)"),
            Self::Stub(value) => formatter.debug_tuple("Stub").field(value).finish(),
            Self::Promise(value) => formatter.debug_tuple("Promise").field(value).finish(),
        }
    }
}

/// Converts a delivered dynamic value into a Rust value.
pub trait FromCapnwebValue: Sized {
    /// Performs the conversion.
    fn from_capnweb_value(value: CapnwebValue) -> Result<Self>;
}

/// Converts an application return value into a dynamic value.
pub trait IntoCapnwebValue {
    /// Performs the conversion.
    fn into_capnweb_value(self) -> Result<CapnwebValue>;
}

impl FromCapnwebValue for CapnwebValue {
    fn from_capnweb_value(value: CapnwebValue) -> Result<Self> {
        Ok(value)
    }
}

impl IntoCapnwebValue for CapnwebValue {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        Ok(self)
    }
}

impl FromCapnwebValue for f64 {
    fn from_capnweb_value(value: CapnwebValue) -> Result<Self> {
        match value {
            CapnwebValue::Number(value) => Ok(value),
            other => Err(Error::Type(format!("expected number, got {other:?}"))),
        }
    }
}

impl IntoCapnwebValue for f64 {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        Ok(CapnwebValue::Number(self))
    }
}

impl FromCapnwebValue for String {
    fn from_capnweb_value(value: CapnwebValue) -> Result<Self> {
        match value {
            CapnwebValue::String(value) => Ok(value),
            other => Err(Error::Type(format!("expected string, got {other:?}"))),
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

impl FromCapnwebValue for bool {
    fn from_capnweb_value(value: CapnwebValue) -> Result<Self> {
        match value {
            CapnwebValue::Bool(value) => Ok(value),
            other => Err(Error::Type(format!("expected boolean, got {other:?}"))),
        }
    }
}

impl IntoCapnwebValue for bool {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        Ok(CapnwebValue::Bool(self))
    }
}

impl IntoCapnwebValue for () {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        Ok(CapnwebValue::Null)
    }
}

impl<T: RpcTargetInner> IntoCapnwebValue for Arc<T> {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        Ok(CapnwebValue::Target(self))
    }
}

/// Rust equivalent of `RpcPayload`.
#[derive(Clone, Debug)]
pub struct RpcPayload {
    /// Root payload value.
    pub value: CapnwebValue,
}

impl RpcPayload {
    /// Equivalent of `RpcPayload.fromAppParams()`.
    ///
    /// Difference from JS: Rust values are moved into an owned payload, so no
    /// deferred defensive copy is needed.
    pub fn from_app_params(value: CapnwebValue) -> Self {
        Self { value }
    }

    /// Equivalent of `RpcPayload.fromAppReturn()`.
    ///
    /// Difference from JS: parameter and return payloads have identical local
    /// ownership because Rust expresses transfer with moves.
    pub fn from_app_return(value: CapnwebValue) -> Self {
        Self { value }
    }

    /// Recursively resolves promises before delivery to application code.
    ///
    /// Equivalent of `deliverTo()` followed by `Promise.all()` in
    /// `deliverCall()`. Difference from JS: substitution constructs a new owned
    /// tree rather than mutating a defensive copy in place.
    pub async fn deliver(self) -> Result<CapnwebValue> {
        deliver_value(self.value).await
    }

    /// Returns true when local delivery can happen without awaiting.
    pub fn is_immediate(&self) -> bool {
        value_is_immediate(&self.value)
    }
}

fn deliver_value(value: CapnwebValue) -> BoxFuture<'static, Result<CapnwebValue>> {
    Box::pin(async move {
        match value {
            CapnwebValue::Promise(promise) => {
                let payload = promise.pull().await?;
                deliver_value(payload.value).await
            }
            CapnwebValue::Array(values) => {
                let mut result = Vec::with_capacity(values.len());
                for value in values {
                    result.push(deliver_value(value).await?);
                }
                Ok(CapnwebValue::Array(result))
            }
            CapnwebValue::Object(values) => {
                let mut result = BTreeMap::new();
                for (key, value) in values {
                    result.insert(key, deliver_value(value).await?);
                }
                Ok(CapnwebValue::Object(result))
            }
            value => Ok(value),
        }
    })
}

fn value_is_immediate(value: &CapnwebValue) -> bool {
    match value {
        CapnwebValue::Promise(_) => false,
        CapnwebValue::Array(values) => values.iter().all(value_is_immediate),
        CapnwebValue::Object(values) => values.values().all(value_is_immediate),
        _ => true,
    }
}

/// Rust equivalent of `RpcPayload | Promise<RpcPayload>`.
pub enum MaybePromise<T> {
    /// The result is available synchronously.
    Immediate(T),
    /// The result will become available asynchronously.
    Pending(BoxFuture<'static, Result<T>>),
}

/// Alias intentionally matching the object-reference role of JS `StubHook`.
pub type StubHook = Arc<dyn StubHookInner>;

/// Direct Rust translation of the abstract JS `StubHook` class.
pub trait StubHookInner: Send + Sync + std::any::Any + 'static {
    /// Equivalent of `StubHook.call()`.
    fn call(&self, path: PropertyPath, args: RpcPayload) -> StubHook;

    /// Equivalent of `StubHook.get()`.
    fn get(&self, path: PropertyPath) -> StubHook;

    /// Equivalent of `StubHook.pull()`.
    fn pull(&self) -> MaybePromise<RpcPayload>;

    /// Equivalent of `StubHook.dup()`.
    ///
    /// This is intentionally distinct from `Arc::clone()`: `dup()` creates an
    /// independently owned RPC reference, while cloning an Arc only keeps the
    /// same logical hook alive temporarily.
    fn dup(&self) -> StubHook;
}

/// Direct translation of JS `ErrorStubHook`.
pub struct ErrorStubHook {
    error: Error,
}

impl ErrorStubHook {
    /// Constructs an error hook.
    pub fn create(error: Error) -> StubHook {
        Arc::new(Self { error })
    }
}

impl StubHookInner for ErrorStubHook {
    fn call(&self, _path: PropertyPath, _args: RpcPayload) -> StubHook {
        self.dup()
    }

    fn get(&self, _path: PropertyPath) -> StubHook {
        self.dup()
    }

    fn pull(&self) -> MaybePromise<RpcPayload> {
        let error = self.error.clone();
        MaybePromise::Pending(Box::pin(async move { Err(error) }))
    }

    fn dup(&self) -> StubHook {
        Self::create(self.error.clone())
    }
}

/// Object-safe target interface replacing JavaScript prototype reflection.
pub trait RpcTargetInner: Send + Sync + 'static {
    /// Dispatches a generated target method.
    ///
    /// Difference from JS: `TargetStubHook.call()` cannot use `followPath()` on
    /// a Rust prototype, so the target macro supplies this name-based dispatch.
    fn call(self: Arc<Self>, method: String, args: RpcPayload) -> StubHook;
}

/// A local target reference.
pub type RpcTarget = Arc<dyn RpcTargetInner>;

/// Direct translation of JS `TargetStubHook`.
pub struct TargetStubHook {
    target: RpcTarget,
}

impl TargetStubHook {
    /// Wraps a local target.
    pub fn create(target: RpcTarget) -> StubHook {
        Arc::new(Self { target })
    }
}

impl StubHookInner for TargetStubHook {
    fn call(&self, path: PropertyPath, args: RpcPayload) -> StubHook {
        match path.as_slice() {
            [method] => self.target.clone().call(method.clone(), args),
            _ => ErrorStubHook::create(Error::Type(
                "v0 target dispatch requires one method path component".into(),
            )),
        }
    }

    fn get(&self, path: PropertyPath) -> StubHook {
        // Difference from JS: generated getters are currently emitted as
        // zero-argument calls rather than reflected prototype property reads.
        self.call(
            path,
            RpcPayload::from_app_params(CapnwebValue::Array(Vec::new())),
        )
    }

    fn pull(&self) -> MaybePromise<RpcPayload> {
        MaybePromise::Pending(Box::pin(async {
            Err(Error::Type("cannot pull a non-promise target".into()))
        }))
    }

    fn dup(&self) -> StubHook {
        Self::create(self.target.clone())
    }
}

/// Direct translation of JS `PayloadStubHook`.
pub struct PayloadStubHook {
    payload: RpcPayload,
}

impl PayloadStubHook {
    /// Wraps an owned payload.
    pub fn create(payload: RpcPayload) -> StubHook {
        Arc::new(Self { payload })
    }
}

impl StubHookInner for PayloadStubHook {
    fn call(&self, path: PropertyPath, args: RpcPayload) -> StubHook {
        match &self.payload.value {
            CapnwebValue::Promise(promise) => promise.hook().call(path, args),
            CapnwebValue::Stub(stub) => stub.hook.call(path, args),
            CapnwebValue::Target(target) => TargetStubHook::create(target.clone()).call(path, args),
            _ => ErrorStubHook::create(Error::Type("RPC value is not callable".into())),
        }
    }

    fn get(&self, path: PropertyPath) -> StubHook {
        if path.is_empty() {
            return self.dup();
        }
        let mut value = self.payload.value.clone();
        for part in path {
            value = match value {
                CapnwebValue::Object(mut object) => match object.remove(&part) {
                    Some(value) => value,
                    None => {
                        return ErrorStubHook::create(Error::Type(format!("no property `{part}`")));
                    }
                },
                CapnwebValue::Promise(promise) => return promise.hook().get(vec![part]),
                _ => return ErrorStubHook::create(Error::Type("value has no properties".into())),
            };
        }
        Self::create(RpcPayload::from_app_return(value))
    }

    fn pull(&self) -> MaybePromise<RpcPayload> {
        MaybePromise::Immediate(self.payload.clone())
    }

    fn dup(&self) -> StubHook {
        // Difference from JS: CapnwebValue is immutable-by-ownership at the
        // boundary, so Clone replaces JS's explicit deep-copy traversal in v0.
        Self::create(self.payload.clone())
    }
}

#[derive(Clone)]
struct EagerPromise<T: Clone> {
    state: Arc<EagerPromiseState<T>>,
}

struct EagerPromiseState<T: Clone> {
    value: OnceCell<Result<T>>,
    ready: Notify,
}

impl<T: Clone + Send + Sync + 'static> EagerPromise<T> {
    fn new(future: impl Future<Output = Result<T>> + Send + 'static) -> Self {
        let state = Arc::new(EagerPromiseState {
            value: OnceCell::new(),
            ready: Notify::new(),
        });
        let task_state = state.clone();
        tokio::spawn(async move {
            if task_state.value.set(future.await).is_ok() {
                task_state.ready.notify_waiters();
            }
        });
        Self { state }
    }

    async fn get(&self) -> Result<T> {
        loop {
            let notified = self.state.ready.notified();
            if let Some(value) = self.state.value.get() {
                return value.clone();
            }
            notified.await;
        }
    }
}

/// Direct translation of JS `PromiseStubHook`.
pub struct PromiseStubHook {
    promise: EagerPromise<StubHook>,
}

impl PromiseStubHook {
    /// Wraps and eagerly drives a future resolving to another hook.
    ///
    /// Difference from JS: Rust futures are lazy, so construction explicitly
    /// spawns the future to reproduce eager JavaScript Promise execution.
    pub fn create(future: impl Future<Output = Result<StubHook>> + Send + 'static) -> StubHook {
        Arc::new(Self {
            promise: EagerPromise::new(future),
        })
    }
}

impl StubHookInner for PromiseStubHook {
    fn call(&self, path: PropertyPath, args: RpcPayload) -> StubHook {
        let promise = self.promise.clone();
        PromiseStubHook::create(async move {
            let hook = promise.get().await?;
            Ok(hook.call(path, args))
        })
    }

    fn get(&self, path: PropertyPath) -> StubHook {
        let promise = self.promise.clone();
        PromiseStubHook::create(async move {
            let hook = promise.get().await?;
            Ok(hook.get(path))
        })
    }

    fn pull(&self) -> MaybePromise<RpcPayload> {
        let promise = self.promise.clone();
        MaybePromise::Pending(Box::pin(async move {
            let hook = promise.get().await?;
            match hook.pull() {
                MaybePromise::Immediate(payload) => Ok(payload),
                MaybePromise::Pending(pending) => pending.await,
            }
        }))
    }

    fn dup(&self) -> StubHook {
        let promise = self.promise.clone();
        PromiseStubHook::create(async move {
            let hook = promise.get().await?;
            Ok(hook.dup())
        })
    }
}

/// Rust representation of JS `RpcStub` without proxy interception.
#[derive(Clone)]
pub struct RpcStub {
    pub(crate) hook: StubHook,
}

impl std::fmt::Debug for RpcStub {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RpcStub(..)")
    }
}

impl RpcStub {
    /// Constructs a stub around a hook.
    pub(crate) fn from_hook(hook: StubHook) -> Self {
        Self { hook }
    }

    /// Calls a generated method immediately.
    pub fn call(&self, path: PropertyPath, args: RpcPayload) -> RpcPromise {
        RpcPromise::new(do_call(self.hook.clone(), path, args), Vec::new())
    }

    /// Duplicates the RPC reference.
    pub fn dup(&self) -> Self {
        Self::from_hook(self.hook.dup())
    }
}

/// Rust representation of JS `RpcPromise`.
///
/// Difference from JS: this untyped provider exposes `pull()` directly;
/// generated `TypedRpcPromise<T>` implements Rust's `Future` trait and performs
/// final value conversion.
#[derive(Clone)]
pub struct RpcPromise {
    hook: StubHook,
    path: PropertyPath,
}

impl std::fmt::Debug for RpcPromise {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RpcPromise")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl RpcPromise {
    /// Constructs a promise from a hook and property path.
    pub(crate) fn new(hook: StubHook, path: PropertyPath) -> Self {
        Self { hook, path }
    }

    /// Returns the underlying hook after applying the property path.
    pub(crate) fn hook(&self) -> StubHook {
        if self.path.is_empty() {
            self.hook.clone()
        } else {
            self.hook.get(self.path.clone())
        }
    }

    /// Calls a generated method without pulling this promise.
    pub fn call(&self, mut path: PropertyPath, args: RpcPayload) -> RpcPromise {
        let mut full_path = self.path.clone();
        full_path.append(&mut path);
        RpcPromise::new(do_call(self.hook.clone(), full_path, args), Vec::new())
    }

    /// Pulls and recursively delivers this promise.
    pub async fn pull(self) -> Result<RpcPayload> {
        let hook = self.hook();
        match hook.pull() {
            MaybePromise::Immediate(payload) => Ok(payload),
            MaybePromise::Pending(pending) => pending.await,
        }
    }
}

/// Typed future wrapper used by generated methods.
pub struct TypedRpcPromise<T> {
    promise: Option<RpcPromise>,
    future: Option<Pin<Box<dyn Future<Output = Result<T>> + Send>>>,
}

impl<T> TypedRpcPromise<T>
where
    T: FromCapnwebValue + Send + 'static,
{
    /// Wraps an untyped RPC promise.
    pub fn new(promise: RpcPromise) -> Self {
        Self {
            promise: Some(promise),
            future: None,
        }
    }

    /// Converts back to the untyped promise for use as an argument.
    pub fn into_untyped(mut self) -> RpcPromise {
        self.promise
            .take()
            .expect("cannot use a typed RPC promise as an argument after polling it")
    }
}

impl<T> Future for TypedRpcPromise<T>
where
    T: FromCapnwebValue + Send + 'static,
{
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.future.is_none() {
            let promise = self
                .promise
                .take()
                .expect("promise polled after completion");
            self.future = Some(Box::pin(async move {
                let payload = promise.pull().await?;
                T::from_capnweb_value(payload.deliver().await?)
            }));
        }
        self.future
            .as_mut()
            .expect("future initialized")
            .as_mut()
            .poll(context)
    }
}

/// Default call interceptor, corresponding to JS `doCall`.
fn do_call(hook: StubHook, path: PropertyPath, args: RpcPayload) -> StubHook {
    // Difference from JS: map interception is not installed until `map.rs` is
    // translated. Keeping this function central preserves the same seam.
    hook.call(path, args)
}

/// Returns the import hook when this hook directly names one.
pub(crate) fn as_import_hook(hook: &StubHook) -> Option<&RpcImportHook> {
    hook.as_any().downcast_ref::<RpcImportHook>()
}

// Difference from JS: Rust trait objects need an explicit downcast seam for
// `RpcSession::getImport()`, which JS implements with `instanceof`.
impl dyn StubHookInner {
    pub(crate) fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
