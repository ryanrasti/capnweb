#![allow(dead_code, missing_docs)]

use std::sync::Arc;

use capnweb::{RpcStub, rpc_target};

include!("generated/simple.rs");

struct ApiImpl;

fn generated_functions_have_call_syntax(formatter: &Formatter, pending: &PendingFormatter) {
    let _resolved_call = formatter("value".to_owned());
    let _pending_call = pending("value".to_owned());
}

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

    fn echo_event(&self, event: Event) -> Event {
        event
    }

    fn echo_identifier(&self, identifier: Identifier) -> Identifier {
        identifier
    }

    fn echo_big_int(&self, value: capnweb::BigInt) -> capnweb::BigInt {
        value
    }

    fn nothing(&self) {}

    fn echo_array(&self, value: Vec<String>) -> Vec<String> {
        value
    }

    fn echo_tuple(&self, value: (String, f64, Option<bool>)) -> (String, f64, Option<bool>) {
        value
    }

    fn echo_record(
        &self,
        value: std::collections::BTreeMap<String, String>,
    ) -> std::collections::BTreeMap<String, String> {
        value
    }

    fn echo_tree(&self, value: TreeNode) -> TreeNode {
        value
    }

    fn accept_deferred(&self, value: capnweb::CapnwebValue) -> String {
        match value {
            capnweb::CapnwebValue::String(value) => value,
            other => panic!("expected resolved promise value, got {other:?}"),
        }
    }

    fn echo_dynamic(&self, value: capnweb::CapnwebValue) -> capnweb::CapnwebValue {
        value
    }

    fn echo_bytes(&self, value: capnweb::Bytes) -> capnweb::Bytes {
        value
    }

    fn echo_date(&self, value: capnweb::CapnwebValue) -> capnweb::CapnwebValue {
        value
    }

    fn echo_stream(&self, value: capnweb::CapnwebValue) -> capnweb::CapnwebValue {
        value
    }

    async fn apply_formatter(
        &self,
        formatter: Formatter,
        value: String,
    ) -> capnweb::Result<String> {
        formatter(value).await
    }

    fn formatter(&self) -> Formatter {
        (|value: String| async move { format!("returned: {value}") }).into()
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
async fn generated_discriminated_unions_round_trip() {
    let api = Api::new(RpcStub::from_target(Arc::new(ApiImpl)));
    let event = Event::Created {
        user: User {
            display_name: "Alice".to_owned(),
            nickname: None,
            active: true,
            status: Status::Active,
        },
    };

    let result = api.echo_event(event).await.expect("echo event");
    let Event::Created { user } = result else {
        panic!("expected created event");
    };
    assert_eq!(user.display_name, "Alice");
}

#[tokio::test]
async fn generated_primitive_unions_round_trip() {
    let api = Api::new(RpcStub::from_target(Arc::new(ApiImpl)));
    let result = api
        .echo_identifier(Identifier::Number(42.0))
        .await
        .expect("echo identifier");
    let Identifier::Number(value) = result else {
        panic!("expected numeric identifier");
    };
    assert_eq!(value, 42.0);
}

#[tokio::test]
async fn generated_structural_and_dynamic_types_round_trip() {
    let api = Api::new(RpcStub::from_target(Arc::new(ApiImpl)));

    assert_eq!(
        api.echo_big_int(123.into()).await.expect("bigint"),
        123.into()
    );
    api.nothing().await.expect("void");
    assert_eq!(
        api.echo_array(vec!["a".to_owned(), "b".to_owned()])
            .await
            .expect("array"),
        vec!["a".to_owned(), "b".to_owned()]
    );
    assert_eq!(
        api.echo_tuple(("point".to_owned(), 4.0, None))
            .await
            .expect("tuple"),
        ("point".to_owned(), 4.0, None)
    );

    let record = std::collections::BTreeMap::from([("key".to_owned(), "value".to_owned())]);
    assert_eq!(
        api.echo_record(record.clone()).await.expect("record"),
        record
    );

    let tree = TreeNode {
        value: "root".to_owned(),
        children: vec![TreeNode {
            value: "leaf".to_owned(),
            children: vec![],
        }],
    };
    let tree = api.echo_tree(tree).await.expect("recursive object");
    assert_eq!(tree.children[0].value, "leaf");

    assert_eq!(
        api.accept_deferred(capnweb::CapnwebValue::String("resolved".to_owned()))
            .await
            .expect("generic promise parameter"),
        "resolved"
    );

    let dynamic = api
        .echo_dynamic(capnweb::CapnwebValue::Number(42.0))
        .await
        .expect("dynamic");
    assert!(matches!(dynamic, capnweb::CapnwebValue::Number(42.0)));

    let bytes = capnweb::Bytes(vec![0, 1, 2, 255]);
    assert_eq!(api.echo_bytes(bytes.clone()).await.expect("bytes"), bytes);

    let date = api
        .echo_date(capnweb::CapnwebValue::Number(1_700_000_000_000.0))
        .await
        .expect("generic date");
    assert!(matches!(date, capnweb::CapnwebValue::Number(_)));
}

#[tokio::test]
async fn generated_local_closures_are_callable_targets() {
    let formatter: Formatter =
        (|value: String| async move { format!("formatted: {value}") }).into();

    let result = formatter("hello".to_owned())
        .await
        .expect("call local formatter");

    assert_eq!(result, "formatted: hello");

    let api = Api::new(RpcStub::from_target(Arc::new(ApiImpl)));
    let callback: Formatter = (|value: String| async move { format!("callback: {value}") }).into();
    let result = api
        .apply_formatter(callback, "hello".to_owned())
        .await
        .expect("pass local callback through RPC");

    assert_eq!(result, "callback: hello");

    let result = api.formatter()("hello".to_owned())
        .await
        .expect("pipeline call to returned formatter");

    assert_eq!(result, "returned: hello");
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
    assert!(matches!(
        result,
        Err(capnweb::Error::Type(message))
            if message == "No such RPC method: implementationHelper"
    ));

    let getter_as_method = stub
        .call(
            vec!["currentUser".to_owned()],
            capnweb::RpcPayload::from_app_params(capnweb::CapnwebValue::Array(vec![])),
        )
        .pull()
        .await;
    assert!(matches!(
        getter_as_method,
        Err(capnweb::Error::Type(message)) if message == "No such RPC method: currentUser"
    ));

    let method_as_getter = stub.get(vec!["nothing".to_owned()]).pull().await;
    assert!(matches!(
        method_as_getter,
        Err(capnweb::Error::Type(message)) if message == "No such RPC property: nothing"
    ));
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
