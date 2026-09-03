//! Direct structural translation of `src/core.ts`.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use num_bigint::BigInt;
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
    /// A JavaScript bigint, represented without a size limit locally.
    BigInt(BigInt),
    /// A byte string (`Uint8Array` in JavaScript).
    Bytes(Vec<u8>),
    /// A JavaScript `Date` timestamp in milliseconds; `None` is Invalid Date.
    Date(Option<f64>),
    /// A serialized absolute URL.
    Url(String),
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
            Self::BigInt(value) => formatter.debug_tuple("BigInt").field(value).finish(),
            Self::Bytes(value) => formatter.debug_tuple("Bytes").field(value).finish(),
            Self::Date(value) => formatter.debug_tuple("Date").field(value).finish(),
            Self::Url(value) => formatter.debug_tuple("Url").field(value).finish(),
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

impl FromCapnwebValue for BigInt {
    fn from_capnweb_value(value: CapnwebValue) -> Result<Self> {
        match value {
            CapnwebValue::BigInt(value) => Ok(value),
            other => Err(Error::Type(format!("expected bigint, got {other:?}"))),
        }
    }
}

impl IntoCapnwebValue for BigInt {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        Ok(CapnwebValue::BigInt(self))
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

impl<T: FromCapnwebValue> FromCapnwebValue for Option<T> {
    fn from_capnweb_value(value: CapnwebValue) -> Result<Self> {
        match value {
            CapnwebValue::Null => Ok(None),
            value => T::from_capnweb_value(value).map(Some),
        }
    }
}

impl<T: IntoCapnwebValue> IntoCapnwebValue for Option<T> {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        match self {
            Some(value) => value.into_capnweb_value(),
            None => Ok(CapnwebValue::Null),
        }
    }
}

impl<T: FromCapnwebValue> FromCapnwebValue for Vec<T> {
    fn from_capnweb_value(value: CapnwebValue) -> Result<Self> {
        match value {
            CapnwebValue::Array(values) => values.into_iter().map(T::from_capnweb_value).collect(),
            other => Err(Error::Type(format!("expected array, got {other:?}"))),
        }
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

impl IntoCapnwebValue for RpcStub {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        Ok(CapnwebValue::Stub(self))
    }
}

impl IntoCapnwebValue for RpcPromise {
    fn into_capnweb_value(self) -> Result<CapnwebValue> {
        Ok(CapnwebValue::Promise(self))
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

/// Callback registered through `onRpcBroken()`.
pub type BrokenCallback = Arc<dyn Fn(Error) + Send + Sync>;

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

    /// Equivalent of `StubHook.ignoreUnhandledRejections()`.
    ///
    /// Difference from JS: Rust futures do not report unobserved errors, so
    /// most implementations need no bookkeeping here.
    fn ignore_unhandled_rejections(&self) {}

    /// Equivalent of `StubHook.dispose()`.
    ///
    /// Logical references are normally released by `Drop`; this hook is for
    /// promise cancellation and immediate forwarding where JS invokes it.
    fn dispose(&self) {}

    /// Equivalent of `StubHook.onBroken()`.
    fn on_broken(&self, _callback: BrokenCallback) {}
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

    fn on_broken(&self, callback: BrokenCallback) {
        callback(self.error.clone());
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
    fn call(&self, mut path: PropertyPath, args: RpcPayload) -> StubHook {
        match &self.payload.value {
            CapnwebValue::Promise(promise) => promise.hook().call(path, args),
            CapnwebValue::Stub(stub) => stub.current_hook().call(path, args),
            CapnwebValue::Target(target) => TargetStubHook::create(target.clone()).call(path, args),
            CapnwebValue::Object(object) if !path.is_empty() => {
                let first = path.remove(0);
                match object.get(&first) {
                    Some(value) => {
                        Self::create(RpcPayload::from_app_return(value.clone())).call(path, args)
                    }
                    None => ErrorStubHook::create(Error::Type(format!("no property `{first}`"))),
                }
            }
            CapnwebValue::Array(values) if !path.is_empty() => {
                let first = path.remove(0);
                match first
                    .parse::<usize>()
                    .ok()
                    .and_then(|index| values.get(index))
                {
                    Some(value) => {
                        Self::create(RpcPayload::from_app_return(value.clone())).call(path, args)
                    }
                    None => ErrorStubHook::create(Error::Type(format!("no property `{first}`"))),
                }
            }
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
                CapnwebValue::Object(mut object) => {
                    object.remove(&part).unwrap_or(CapnwebValue::Null)
                }
                CapnwebValue::Array(values) => part
                    .parse::<usize>()
                    .ok()
                    .and_then(|index| values.get(index).cloned())
                    .unwrap_or(CapnwebValue::Null),
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

    fn ignore_unhandled_rejections(&self) {
        match &self.payload.value {
            CapnwebValue::Stub(stub) => stub.current_hook().ignore_unhandled_rejections(),
            CapnwebValue::Promise(promise) => promise.hook().ignore_unhandled_rejections(),
            _ => {}
        }
    }

    fn on_broken(&self, callback: BrokenCallback) {
        match &self.payload.value {
            CapnwebValue::Stub(stub) => stub.current_hook().on_broken(callback),
            CapnwebValue::Promise(promise) => promise.hook().on_broken(callback),
            _ => {}
        }
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
    order: Arc<PromiseOperationOrder>,
}

struct PromiseOperationOrder {
    next_ticket: AtomicU64,
    next_to_run: AtomicU64,
    ready: Notify,
}

impl PromiseOperationOrder {
    fn new() -> Self {
        Self {
            next_ticket: AtomicU64::new(0),
            next_to_run: AtomicU64::new(0),
            ready: Notify::new(),
        }
    }

    fn ticket(self: &Arc<Self>) -> PromiseOperationTicket {
        PromiseOperationTicket {
            sequence: self.next_ticket.fetch_add(1, Ordering::Relaxed),
            order: self.clone(),
        }
    }
}

struct PromiseOperationTicket {
    sequence: u64,
    order: Arc<PromiseOperationOrder>,
}

impl PromiseOperationTicket {
    async fn wait(self) -> PromiseOperationGuard {
        loop {
            let notified = self.order.ready.notified();
            if self.order.next_to_run.load(Ordering::Acquire) == self.sequence {
                drop(notified);
                return PromiseOperationGuard { order: self.order };
            }
            notified.await;
        }
    }
}

struct PromiseOperationGuard {
    order: Arc<PromiseOperationOrder>,
}

impl Drop for PromiseOperationGuard {
    fn drop(&mut self) {
        self.order.next_to_run.fetch_add(1, Ordering::Release);
        self.order.ready.notify_waiters();
    }
}

impl PromiseStubHook {
    /// Wraps and eagerly drives a future resolving to another hook.
    ///
    /// Difference from JS: Rust futures are lazy, so construction explicitly
    /// spawns the future to reproduce eager JavaScript Promise execution.
    pub fn create(future: impl Future<Output = Result<StubHook>> + Send + 'static) -> StubHook {
        Arc::new(Self {
            promise: EagerPromise::new(future),
            order: Arc::new(PromiseOperationOrder::new()),
        })
    }
}

impl StubHookInner for PromiseStubHook {
    fn call(&self, path: PropertyPath, args: RpcPayload) -> StubHook {
        let promise = self.promise.clone();
        let ticket = self.order.ticket();
        PromiseStubHook::create(async move {
            let guard = ticket.wait().await;
            let hook = promise.get().await?;
            let result = hook.call(path, args);
            drop(guard);
            Ok(result)
        })
    }

    fn get(&self, path: PropertyPath) -> StubHook {
        let promise = self.promise.clone();
        let ticket = self.order.ticket();
        PromiseStubHook::create(async move {
            let guard = ticket.wait().await;
            let hook = promise.get().await?;
            let result = hook.get(path);
            drop(guard);
            Ok(result)
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

    fn ignore_unhandled_rejections(&self) {
        let promise = self.promise.clone();
        tokio::spawn(async move {
            if let Ok(hook) = promise.get().await {
                hook.ignore_unhandled_rejections();
            }
        });
    }

    fn dispose(&self) {
        let promise = self.promise.clone();
        let ticket = self.order.ticket();
        tokio::spawn(async move {
            let _guard = ticket.wait().await;
            if let Ok(hook) = promise.get().await {
                hook.dispose();
            }
        });
    }

    fn on_broken(&self, callback: BrokenCallback) {
        let promise = self.promise.clone();
        tokio::spawn(async move {
            match promise.get().await {
                Ok(hook) => hook.on_broken(callback),
                Err(error) => callback(error),
            }
        });
    }
}

/// Rust representation of JS `RpcStub` without proxy interception.
pub struct RpcStub {
    hook: Mutex<Option<StubHook>>,
}

impl std::fmt::Debug for RpcStub {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RpcStub(..)")
    }
}

impl Clone for RpcStub {
    fn clone(&self) -> Self {
        self.dup()
    }
}

impl RpcStub {
    /// Constructs a local stub around a target.
    pub fn from_target<T: RpcTargetInner>(target: Arc<T>) -> Self {
        Self::from_hook(TargetStubHook::create(target))
    }

    /// Constructs a local stub around a pass-by-value object or array.
    pub fn from_value(value: CapnwebValue) -> Self {
        Self::from_hook(PayloadStubHook::create(RpcPayload::from_app_return(value)))
    }

    /// Constructs a stub around a hook.
    pub(crate) fn from_hook(hook: StubHook) -> Self {
        Self {
            hook: Mutex::new(Some(hook)),
        }
    }

    pub(crate) fn current_hook(&self) -> StubHook {
        self.hook
            .lock()
            .expect("stub mutex poisoned")
            .clone()
            .unwrap_or_else(|| {
                ErrorStubHook::create(Error::Remote(
                    "attempted to use RPC stub after it was disposed".into(),
                ))
            })
    }

    /// Calls a generated method immediately.
    pub fn call(&self, path: PropertyPath, args: RpcPayload) -> RpcPromise {
        RpcPromise::new(do_call(self.current_hook(), path, args), Vec::new())
    }

    /// Accesses a property without pulling the containing stub.
    pub fn get(&self, path: PropertyPath) -> RpcPromise {
        RpcPromise::new(self.current_hook().get(path), Vec::new())
    }

    /// Duplicates the RPC reference.
    pub fn dup(&self) -> Self {
        Self::from_hook(self.current_hook().dup())
    }

    /// Registers a callback invoked if the capability breaks.
    pub fn on_rpc_broken(&self, callback: impl Fn(Error) + Send + Sync + 'static) {
        self.current_hook().on_broken(Arc::new(callback));
    }

    /// Idempotently disposes this logical RPC reference.
    pub fn dispose(&self) {
        if let Some(hook) = self.hook.lock().expect("stub mutex poisoned").take() {
            hook.dispose();
        }
    }
}

/// Rust representation of JS `RpcPromise`.
///
/// Difference from JS: this untyped provider exposes `pull()` directly;
/// generated `TypedRpcPromise<T>` implements Rust's `Future` trait and performs
/// final value conversion.
pub struct RpcPromise {
    hook: Mutex<Option<StubHook>>,
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

impl Clone for RpcPromise {
    fn clone(&self) -> Self {
        // JS `unwrapStubAndDup()` uses get(), including for an empty promise
        // path, so the duplicate remains independently pullable as a promise.
        Self::new(self.current_hook().get(self.path.clone()), Vec::new())
    }
}

impl RpcPromise {
    /// Constructs a promise from a hook and property path.
    pub(crate) fn new(hook: StubHook, path: PropertyPath) -> Self {
        Self {
            hook: Mutex::new(Some(hook)),
            path,
        }
    }

    fn current_hook(&self) -> StubHook {
        self.hook
            .lock()
            .expect("promise mutex poisoned")
            .clone()
            .unwrap_or_else(|| {
                ErrorStubHook::create(Error::Remote(
                    "attempted to use RPC promise after it was disposed".into(),
                ))
            })
    }

    /// Returns the underlying hook after applying the property path.
    pub(crate) fn hook(&self) -> StubHook {
        let hook = self.current_hook();
        if self.path.is_empty() {
            hook
        } else {
            hook.get(self.path.clone())
        }
    }

    /// Calls a generated method without pulling this promise.
    pub fn call(&self, mut path: PropertyPath, args: RpcPayload) -> RpcPromise {
        let mut full_path = self.path.clone();
        full_path.append(&mut path);
        RpcPromise::new(do_call(self.current_hook(), full_path, args), Vec::new())
    }

    /// Traverses a generated property without pulling this promise.
    pub fn get(&self, mut path: PropertyPath) -> RpcPromise {
        let mut full_path = self.path.clone();
        full_path.append(&mut path);
        RpcPromise::new(self.current_hook(), full_path)
    }

    /// Constructs an already-rejected provider for a local encoding error.
    pub fn rejected(error: Error) -> Self {
        Self::new(ErrorStubHook::create(error), Vec::new())
    }

    /// Applies a synchronous callback to a scalar or each array element.
    ///
    /// Difference from JS: this initial implementation applies the map after
    /// pulling the subject locally. The JavaScript `map.ts` instruction
    /// protocol will replace this fallback so calls made by the callback can
    /// be sent without first pulling the subject.
    pub fn map<F, R>(self, callback: F) -> RpcPromise
    where
        F: Fn(CapnwebValue) -> Result<R> + Send + Sync + 'static,
        R: IntoCapnwebValue,
    {
        crate::map::map_after_pull(self, callback)
    }

    /// Registers a callback invoked if this promise or capability breaks.
    pub fn on_rpc_broken(&self, callback: impl Fn(Error) + Send + Sync + 'static) {
        self.hook().on_broken(Arc::new(callback));
    }

    /// Idempotently disposes this logical promise reference.
    pub fn dispose(&self) {
        if let Some(hook) = self.hook.lock().expect("promise mutex poisoned").take() {
            hook.dispose();
        }
    }

    /// Pulls and recursively delivers this promise.
    pub async fn pull(&self) -> Result<RpcPayload> {
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
