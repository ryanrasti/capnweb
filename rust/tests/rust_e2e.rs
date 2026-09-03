//! Rust-to-Rust end-to-end coverage.

use std::sync::Arc;

use capnweb::transport::memory_transport_pair;
use capnweb::{
    CapnwebValue, RpcPayload, TypedRpcPromise, rpc_interface, rpc_target, start_session,
};

#[rpc_interface]
trait CalculatorApi {
    fn add(left: f64, right: f64) -> f64;
    fn double(value: f64) -> f64;
    fn make_adder() -> CapnwebValue;
}

struct Adder;

#[rpc_target]
impl Adder {
    fn add_one(&self, value: f64) -> f64 {
        value + 1.0
    }
}

struct Calculator;

#[rpc_target(CalculatorApi)]
impl Calculator {
    async fn add(&self, left: f64, right: f64) -> f64 {
        tokio::task::yield_now().await;
        left + right
    }

    async fn double(&self, value: f64) -> f64 {
        value * 2.0
    }

    fn make_adder(&self) -> Arc<Adder> {
        Arc::new(Adder)
    }
}

#[tokio::test]
async fn rust_calls_rust_with_pipelined_argument() {
    let (left_transport, right_transport) = memory_transport_pair();
    let left = CalculatorApiStub::new(start_session(left_transport, Arc::new(Calculator)));
    let _right = start_session(right_transport, Arc::new(Calculator));

    let sum = left.add(20.0, 1.0);
    let doubled = left.double(sum);

    assert_eq!(doubled.await.expect("RPC succeeds"), 42.0);

    // A Rust target returned by reference is exported, imported as a stub, and
    // remains callable through the same hook path.
    let CapnwebValue::Stub(adder) = left.make_adder().await.expect("target result") else {
        panic!("expected a returned stub");
    };
    let incremented = TypedRpcPromise::<f64>::new(adder.call(
        vec!["addOne".into()],
        RpcPayload::from_app_params(CapnwebValue::Array(vec![CapnwebValue::Number(41.0)])),
    ));
    assert_eq!(incremented.await.expect("returned target call"), 42.0);
}
