#![allow(dead_code, missing_docs)]

use std::sync::Arc;

use capnweb::{RpcStub, rpc_target};

include!("generated/simple.rs");

struct ApiImpl;

struct CounterImpl {
    value: f64,
}

#[rpc_target(Counter)]
impl CounterImpl {
    fn increment(&self, by: f64) -> f64 {
        self.value + by
    }
}

#[rpc_target(Api)]
impl ApiImpl {
    fn current_user(&self) -> User {
        User {
            display_name: "current".to_owned(),
            nickname: None,
            active: true,
            status: Status::Active,
        }
    }

    fn echo_user(&self, user: User) -> User {
        user
    }

    fn make_counter(&self, initial: f64) -> Arc<CounterImpl> {
        Arc::new(CounterImpl { value: initial })
    }

    fn directory(&self) -> Directory {
        Directory {
            primary_counter: Counter::new(RpcStub::from_target(Arc::new(CounterImpl {
                value: 40.0,
            }))),
        }
    }

    fn implementation_helper(&self) -> String {
        "not remotely visible".to_owned()
    }
}

#[tokio::test]
async fn generated_client_round_trips_a_pojo() {
    let api = Api::new(RpcStub::from_target(Arc::new(ApiImpl)));
    let user = User {
        display_name: "Alice".to_owned(),
        nickname: Some("Al".to_owned()),
        active: true,
        status: Status::Disabled,
    };

    let echoed = api.echo_user(user).await.expect("echo user");
    assert_eq!(echoed.display_name, "Alice");
    assert_eq!(echoed.nickname.as_deref(), Some("Al"));
    assert!(echoed.active);
    assert_eq!(echoed.status, Status::Disabled);
}

#[tokio::test]
async fn generated_descriptor_hides_implementation_helpers() {
    let stub = RpcStub::from_target(Arc::new(ApiImpl));
    let result = stub
        .call(
            vec!["implementationHelper".to_owned()],
            capnweb::RpcPayload::from_app_params(capnweb::CapnwebValue::Array(vec![])),
        )
        .pull()
        .await;
    assert!(
        matches!(result, Err(capnweb::Error::NoSuchMethod(name)) if name == "implementationHelper")
    );
}

#[tokio::test]
async fn generated_capability_results_support_pipelined_calls() {
    let api = Api::new(RpcStub::from_target(Arc::new(ApiImpl)));
    let value = api
        .make_counter(40.0)
        .increment(2.0)
        .await
        .expect("increment");
    assert_eq!(value, 42.0);
}

#[tokio::test]
async fn generated_capability_fields_pipeline_methods() {
    let api = Api::new(RpcStub::from_target(Arc::new(ApiImpl)));
    let value = api
        .directory()
        .primary_counter
        .increment(2.0)
        .await
        .expect("directory counter increment");
    assert_eq!(value, 42.0);
}

#[tokio::test]
async fn generated_pending_fields_pipeline_property_gets() {
    let api = Api::new(RpcStub::from_target(Arc::new(ApiImpl)));
    let display_name = api.current_user().display_name.await.expect("display name");
    assert_eq!(display_name, "current");
}
