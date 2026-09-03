//! Hand-written sketch of the bindings generated from `api.d.ts`.
//!
//! This file is a design fixture, not yet a compilable Cargo example.

use std::collections::BTreeMap;

use capnweb::{
    ArrayBuffer, DataView, Date, Headers, RemoteError, RpcArg, RpcCallback,
    RpcPromise, RpcReadStream, RpcStub, RpcTarget, RpcWriteSink,
    Undefined, Url,
};
use num_bigint::BigInt;

// -----------------------------------------------------------------------------
// Pass-by-value declarations

#[derive(Clone, Debug, capnweb::RpcValue)]
pub struct User {
    pub id: String,
    pub display_name: String,

    /// The field itself may be absent.
    pub nickname: Option<String>,

    /// Present with either a string or `null`.
    pub nullable_bio: Option<String>,

    /// Present with either a string or JavaScript `undefined`.
    pub explicit_undefined: Result<String, Undefined>,

    pub active: bool,

    /// JavaScript `number`, including NaN and infinities.
    pub score: f64,

    pub login_count: BigInt,
    pub tags: Vec<String>,
    pub aliases: Vec<String>,
    pub location: (f64, f64),
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, capnweb::RpcValue)]
#[capnweb(tag = "kind")]
pub enum UserEvent {
    #[capnweb(rename = "created")]
    Created { user: User },

    #[capnweb(rename = "renamed")]
    Renamed {
        user_id: String,
        display_name: String,
    },

    #[capnweb(rename = "deleted")]
    Deleted {
        user_id: String,
        reason: Option<String>,
    },
}

#[derive(Clone, Debug, capnweb::RpcValue)]
pub struct ByteValues {
    pub array_buffer: ArrayBuffer,
    pub data_view: DataView,
    pub int8: Vec<i8>,
    pub uint8: Vec<u8>,
    pub uint8_clamped: Vec<u8>,
    pub int16: Vec<i16>,
    pub uint16: Vec<u16>,
    pub int32: Vec<i32>,
    pub uint32: Vec<u32>,
    pub bigint64: Vec<i64>,
    pub biguint64: Vec<u64>,
    pub float32: Vec<f32>,
    pub float64: Vec<f64>,
}

// A capability-containing record has different send-side and receive-side
// representations. The generic parameter makes that transformation explicit.
#[derive(Clone, Debug, capnweb::RpcValue)]
pub struct CounterDirectory<C> {
    pub primary: C,
    pub all: Vec<C>,
    pub by_name: BTreeMap<String, C>,
}

pub type ReceivedCounterDirectory = CounterDirectory<CounterStub>;
pub type ReturnedCounterDirectory = CounterDirectory<CounterTarget>;

// -----------------------------------------------------------------------------
// Pass-by-reference interfaces

/// Generated interface markers carry schema metadata used by typed stubs and
/// `#[capnweb::target(...)]`. Application types do not implement these markers.
#[capnweb::interface]
pub enum Counter {}

pub type CounterStub = RpcStub<Counter>;
pub type CounterTarget = RpcTarget<Counter>;
pub type PendingCounter = RpcPromise<CounterStub>;

/// Generated argument conversion accepts a remote stub, an unresolved
/// same-session result, or a local target. These lower to import, pipeline, and
/// export expressions respectively.
pub trait IntoCounterArg: capnweb::sealed::Sealed {}
impl IntoCounterArg for CounterStub {}
impl IntoCounterArg for PendingCounter {}
impl IntoCounterArg for CounterTarget {}

/// Generated callable-interface marker for the `Formatter` function type.
#[capnweb::callback]
pub enum Formatter {}

#[derive(Clone, Debug, capnweb::RpcValue)]
pub struct FormatterOptions {
    pub uppercase: Option<bool>,
}

pub type FormatterStub = RpcStub<Formatter>;
pub type FormatterCallback = RpcCallback<Formatter>;

pub trait IntoFormatterArg: capnweb::sealed::Sealed {}
impl IntoFormatterArg for FormatterStub {}
impl IntoFormatterArg for FormatterCallback {}

#[capnweb::interface]
pub enum EventSink {}

pub type EventSinkStub = RpcStub<EventSink>;
pub type EventSinkTarget = RpcTarget<EventSink>;

pub trait IntoEventSinkArg: capnweb::sealed::Sealed {}
impl IntoEventSinkArg for EventSinkStub {}
impl IntoEventSinkArg for EventSinkTarget {}

#[capnweb::interface]
pub enum KitchenSinkApi {}

// -----------------------------------------------------------------------------
// Generated client surface

pub struct KitchenSinkApiStub {
    inner: RpcStub<KitchenSinkApi>,
}

impl KitchenSinkApiStub {
    // Every method emits a push immediately and returns a path-bearing promise.
    // `RpcArg<T>` accepts either a concrete T or a compatible RpcPromise<T>.
    pub fn nothing(&self) -> RpcPromise<()>;
    pub fn undefined_value(&self) -> RpcPromise<Undefined>;
    pub fn nullable_text(
        &self,
        value: impl RpcArg<Option<String>>,
    ) -> RpcPromise<Option<String>>;
    pub fn echo_boolean(&self, value: impl RpcArg<bool>) -> RpcPromise<bool>;
    pub fn echo_number(&self, value: impl RpcArg<f64>) -> RpcPromise<f64>;
    pub fn echo_big_int(&self, value: impl RpcArg<BigInt>) -> RpcPromise<BigInt>;
    pub fn echo_string(&self, value: impl RpcArg<String>) -> RpcPromise<String>;
    pub fn echo_user(&self, value: impl RpcArg<User>) -> RpcPromise<User>;
    pub fn echo_users(&self, value: impl RpcArg<Vec<User>>) -> RpcPromise<Vec<User>>;
    pub fn echo_tuple(
        &self,
        value: impl RpcArg<(String, f64, Option<bool>)>,
    ) -> RpcPromise<(String, f64, Option<bool>)>;
    pub fn echo_event(&self, value: impl RpcArg<UserEvent>) -> RpcPromise<UserEvent>;
    pub fn count_labels(&self, labels: Vec<impl RpcArg<String>>) -> RpcPromise<f64>;

    pub fn echo_bytes(&self, value: impl RpcArg<ByteValues>) -> RpcPromise<ByteValues>;
    pub fn echo_date(&self, value: impl RpcArg<Date>) -> RpcPromise<Date>;
    pub fn echo_url(&self, value: impl RpcArg<Url>) -> RpcPromise<Url>;
    pub fn echo_headers(&self, value: impl RpcArg<Headers>) -> RpcPromise<Headers>;
    pub fn echo_error(&self, value: impl RpcArg<RemoteError>) -> RpcPromise<RemoteError>;

    pub fn accept_deferred(&self, value: impl RpcArg<String>) -> RpcPromise<String>;

    pub fn make_counter(&self, initial: impl RpcArg<f64>) -> PendingCounter;
    pub fn use_counter(
        &self,
        counter: impl IntoCounterArg,
        by: impl RpcArg<f64>,
    ) -> RpcPromise<f64>;
    pub fn counter_directory(&self) -> RpcPromise<ReceivedCounterDirectory>;
    pub fn format(
        &self,
        formatter: impl IntoFormatterArg,
        value: impl RpcArg<String>,
    ) -> RpcPromise<String>;
    pub fn make_formatter(&self, prefix: impl RpcArg<String>) -> RpcPromise<FormatterStub>;
    pub fn watch(&self, sink: impl IntoEventSinkArg) -> RpcPromise<()>;

    pub fn download_users(&self) -> RpcPromise<RpcReadStream<User>>;
    pub fn upload_users(
        &self,
        source: impl RpcArg<RpcReadStream<User>>,
    ) -> RpcPromise<f64>;
    pub fn write_users(
        &self,
        destination: impl RpcArg<RpcWriteSink<User>>,
    ) -> RpcPromise<()>;
    pub fn copy_users(
        &self,
        source: impl RpcArg<RpcReadStream<User>>,
        destination: impl RpcArg<RpcWriteSink<User>>,
    ) -> RpcPromise<f64>;
}

// Generated provider methods are also implemented for unresolved capability
// results, enabling calls without awaiting the target first.
impl PendingCounter {
    pub fn value(&self) -> RpcPromise<f64>;
    pub fn increment(&self, by: impl RpcArg<Option<f64>>) -> RpcPromise<f64>;
    pub fn reset(&self) -> RpcPromise<()>;
}

// -----------------------------------------------------------------------------
// Application usage

struct CounterImpl;

/// The macro wraps these ordinary inherent methods in dynamic RPC dispatch.
#[capnweb::target(Counter)]
impl CounterImpl {
    fn value(&self) -> f64 {
        0.0
    }

    async fn increment(&self, by: Option<f64>) -> f64 {
        by.unwrap_or(1.0)
    }

    fn reset(&self) {}
}

fn client_examples(
    api: &KitchenSinkApiStub,
    counter: CounterStub,
    formatter: FormatterCallback,
) {
    // Ordinary generated calls emit operations immediately.
    let created = api.make_counter(10.0);

    // Calling on an unresolved target pipelines without awaiting it.
    let new_value = created.increment(Some(2.0));

    // An unresolved result can itself be forwarded as an argument.
    let text = api.echo_string("hello".to_owned());
    let formatted = api.format(formatter, text);

    // A local implementation can be passed where the schema expects a target;
    // argument lowering exports it by reference.
    let local_counter = CounterTarget::new(CounterImpl);
    let locally_backed_value = api.use_counter(local_counter, 3.0);

    // Clone is the Rust equivalent of JavaScript `dup()`. Both local values
    // share one Arc-backed import lease.
    let another_local_reference = counter.clone();

    let _ = (
        new_value,
        formatted,
        locally_backed_value,
        another_local_reference,
    );
}
