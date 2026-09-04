use std::sync::Arc;

use capnweb::{
    CapnwebValue, Error, FromCapnwebValue, IntoCapnwebValue, RpcInterfaceDescriptor, RpcPayload,
    RpcPromise, RpcStub, TypedRpcPromise, function_target, rpc_target,
};

#[tokio::test(flavor = "current_thread")]
async fn function_target_accepts_empty_path_calls() {
    let function = function_target(|arguments| async move {
        let CapnwebValue::Array(mut arguments) = arguments.value else {
            return Err(Error::Type("arguments were not an array".to_owned()));
        };
        Ok(arguments.remove(0))
    });
    let stub = RpcStub::from_value(function);
    let result = stub
        .call(
            Vec::new(),
            RpcPayload::from_app_params(CapnwebValue::Array(vec![CapnwebValue::Number(42.0)])),
        )
        .pull()
        .await
        .unwrap();

    assert!(matches!(result, CapnwebValue::Number(42.0)));
}

#[tokio::test(flavor = "current_thread")]
async fn function_target_rejects_nonempty_paths() {
    let function = function_target(|_| async { Ok(CapnwebValue::Null) });
    let stub = RpcStub::from_value(function);
    let error = stub
        .call(
            vec!["notAFunctionMember".to_owned()],
            RpcPayload::from_app_params(CapnwebValue::Array(Vec::new())),
        )
        .pull()
        .await
        .unwrap_err();

    assert!(error.to_string().contains("empty path"));
}

struct FunctionFactoryInterface;

impl RpcInterfaceDescriptor for FunctionFactoryInterface {
    fn has_member(wire_name: &str) -> bool {
        wire_name == "formatter"
    }
}

struct FunctionFactory;

#[rpc_target(FunctionFactoryInterface)]
impl FunctionFactory {
    fn formatter(&self) -> RpcStub {
        RpcStub::from_value(function_target(|arguments| async move {
            let delivered = arguments.deliver_resolve().await?;
            let CapnwebValue::Array(mut values) = delivered else {
                return Err(Error::Type(
                    "function arguments were not an array".to_owned(),
                ));
            };
            let CapnwebValue::String(value) = values.remove(0) else {
                return Err(Error::Type("function argument was not a string".to_owned()));
            };
            Ok(CapnwebValue::String(format!("formatted: {value}")))
        }))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn functions_returned_through_promises_remain_callable() {
    let factory = RpcStub::from_target(Arc::new(FunctionFactory));
    let pipelined = factory.call(
        vec!["formatter".to_owned()],
        RpcPayload::from_app_params(CapnwebValue::Array(Vec::new())),
    );
    let pipelined_result = pipelined
        .call(
            Vec::new(),
            RpcPayload::from_app_params(CapnwebValue::Array(vec![CapnwebValue::String(
                "pipeline".to_owned(),
            )])),
        )
        .pull()
        .await
        .expect("pipeline call to returned function");
    assert!(
        matches!(pipelined_result, CapnwebValue::String(value) if value == "formatted: pipeline")
    );

    let returned = factory.call(
        vec!["formatter".to_owned()],
        RpcPayload::from_app_params(CapnwebValue::Array(Vec::new())),
    );
    let function = RpcStub::from_value(returned.pull().await.expect("return function"));
    let result = function
        .call(
            Vec::new(),
            RpcPayload::from_app_params(CapnwebValue::Array(vec![CapnwebValue::String(
                "value".to_owned(),
            )])),
        )
        .pull()
        .await
        .expect("call returned function");

    assert!(matches!(result, CapnwebValue::String(value) if value == "formatted: value"));
}

#[tokio::test(flavor = "current_thread")]
async fn typed_promise_decodes_its_resolution() {
    let promise = RpcPromise::from_value(CapnwebValue::String("typed".to_owned()));
    let typed = TypedRpcPromise::<String>::new(promise);
    assert_eq!(typed.await.unwrap(), "typed");
}

#[test]
fn typed_value_traits_preserve_undefined_unit_mapping() {
    assert!(matches!(
        ().into_capnweb_value().unwrap(),
        CapnwebValue::Undefined
    ));
    assert_eq!(
        <() as FromCapnwebValue>::from_capnweb_value(CapnwebValue::Undefined).unwrap(),
        ()
    );
}

#[test]
fn local_targets_can_be_constructed_without_hook_access() {
    struct Target;

    impl capnweb::RpcTargetInner for Target {
        fn call(
            self: Arc<Self>,
            _path: capnweb::PropertyPath,
            _args: RpcPayload,
        ) -> capnweb::Result<capnweb::StubHook> {
            Err(Error::Message("unused".to_owned()))
        }

        fn get(
            self: Arc<Self>,
            _path: capnweb::PropertyPath,
        ) -> capnweb::Result<capnweb::StubHook> {
            Err(Error::Message("unused".to_owned()))
        }
    }

    let _stub = RpcStub::from_target(Arc::new(Target));
}

struct TestInterface;

impl RpcInterfaceDescriptor for TestInterface {
    fn has_member(wire_name: &str) -> bool {
        matches!(wire_name, "profile" | "echo")
    }
}

struct GeneratedTarget;

#[rpc_target(TestInterface)]
impl GeneratedTarget {
    fn profile(&self) -> std::collections::BTreeMap<String, String> {
        std::collections::BTreeMap::from([("displayName".to_owned(), "Ada".to_owned())])
    }

    fn echo(&self, value: String) -> String {
        value
    }

    fn implementation_helper(&self) -> String {
        "hidden".to_owned()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn generated_target_dispatch_supports_calls_and_nested_gets() {
    let stub = RpcStub::from_target(Arc::new(GeneratedTarget));
    let echoed = stub
        .call(
            vec!["echo".to_owned()],
            RpcPayload::from_app_params(CapnwebValue::Array(vec![CapnwebValue::String(
                "hello".to_owned(),
            )])),
        )
        .pull()
        .await
        .unwrap();
    assert!(matches!(echoed, CapnwebValue::String(value) if value == "hello"));

    let display_name = stub
        .get(vec!["profile".to_owned(), "displayName".to_owned()])
        .pull()
        .await
        .unwrap();
    assert!(matches!(display_name, CapnwebValue::String(value) if value == "Ada"));

    let hidden = stub
        .call(
            vec!["implementationHelper".to_owned()],
            RpcPayload::from_app_params(CapnwebValue::Array(Vec::new())),
        )
        .pull()
        .await
        .unwrap_err();
    assert!(hidden.to_string().contains("implementationHelper"));
}
