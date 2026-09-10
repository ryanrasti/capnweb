use std::any::{Any, TypeId};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use capnweb::{
    AnyRpcTransport, CapnwebValue, CustomMessageCodec, CustomMessageContext, CustomTypeHandler,
    CustomTypeRegistry, CustomValue, EncodingLevel, Error, PayloadStubHook, PromiseStubHook,
    PropertyKey, PropertyPath, Result, RpcPayload, RpcSession, RpcSessionOptions, RpcTargetInner,
    StubHook, TransportMessage, TransportSend,
};

#[derive(Debug, Eq, PartialEq)]
struct Widget(String);

struct EmptyContext;

impl CustomMessageContext for EmptyContext {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

struct WidgetHandler;

impl CustomTypeHandler for WidgetHandler {
    fn value_type_id(&self) -> TypeId {
        TypeId::of::<Widget>()
    }

    fn wire_tag(&self) -> &'static str {
        "test-widget"
    }

    fn encode_fields(
        &self,
        value: &CustomValue,
        _context: &mut dyn CustomMessageContext,
    ) -> Result<Vec<CapnwebValue>> {
        let value = value.downcast::<Widget>()?;
        Ok(vec![CapnwebValue::String(value.0.clone())])
    }

    fn decode_fields(
        &self,
        fields: &[CapnwebValue],
        _context: &mut dyn CustomMessageContext,
    ) -> Result<CustomValue> {
        let [CapnwebValue::String(value)] = fields else {
            return Err(Error::Type("invalid widget encoding".to_owned()));
        };
        Ok(CustomValue::new(Widget(value.clone())))
    }
}

struct CustomTransport {
    sender: tokio::sync::mpsc::UnboundedSender<CapnwebValue>,
    receiver: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<CapnwebValue>>,
    registry: Arc<CustomTypeRegistry>,
}

impl AnyRpcTransport for CustomTransport {
    fn encoding_level(&self) -> EncodingLevel {
        EncodingLevel::JsonCompatible
    }

    fn new_custom_codec(&self) -> Option<Box<CustomMessageCodec>> {
        Some(Box::new(CustomMessageCodec::new(
            Arc::clone(&self.registry),
            Box::new(EmptyContext),
        )))
    }

    fn send(&self, message: TransportMessage) -> Result<TransportSend> {
        let TransportMessage::Value { value, .. } = message else {
            return Err(Error::Type("expected custom-encoded value".to_owned()));
        };
        self.sender
            .send(value)
            .map_err(|_| Error::Message("transport closed".to_owned()))?;
        Ok(TransportSend::Done(None))
    }

    fn receive(
        self: Arc<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<TransportMessage>> + Send + 'static>> {
        Box::pin(async move {
            let value = self
                .receiver
                .lock()
                .await
                .recv()
                .await
                .ok_or_else(|| Error::Message("transport closed".to_owned()))?;
            Ok(TransportMessage::Value {
                value,
                codec: self.new_custom_codec(),
            })
        })
    }
}

fn transport_pair() -> (Arc<CustomTransport>, Arc<CustomTransport>) {
    let mut registry = CustomTypeRegistry::new();
    registry.register(Arc::new(WidgetHandler)).unwrap();
    let registry = Arc::new(registry);
    let (to_left, from_right) = tokio::sync::mpsc::unbounded_channel();
    let (to_right, from_left) = tokio::sync::mpsc::unbounded_channel();
    (
        Arc::new(CustomTransport {
            sender: to_right,
            receiver: tokio::sync::Mutex::new(from_right),
            registry: Arc::clone(&registry),
        }),
        Arc::new(CustomTransport {
            sender: to_left,
            receiver: tokio::sync::Mutex::new(from_left),
            registry,
        }),
    )
}

struct EchoTarget;

impl RpcTargetInner for EchoTarget {
    fn call(self: Arc<Self>, path: PropertyPath, args: RpcPayload) -> Result<StubHook> {
        assert!(matches!(
            path.as_slice(),
            [PropertyKey::String(method)] if method == "echo"
        ));
        Ok(PromiseStubHook::from_future(async move {
            let CapnwebValue::Array(mut args) = args.deliver_resolve().await? else {
                return Err(Error::Type("arguments are not an array".to_owned()));
            };
            Ok(Arc::new(PayloadStubHook::new(RpcPayload::from_app_return(
                args.remove(0),
            ))) as StubHook)
        }))
    }

    fn get(self: Arc<Self>, path: PropertyPath) -> Result<StubHook> {
        Err(Error::Type(format!("No such RPC property: {path:?}")))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn registered_application_type_round_trips_over_an_arbitrary_transport() {
    let (client_transport, server_transport) = transport_pair();
    let client = RpcSession::new(client_transport, None, RpcSessionOptions::default()).unwrap();
    let _server = RpcSession::new(
        server_transport,
        Some(CapnwebValue::Target(Arc::new(EchoTarget))),
        RpcSessionOptions::default(),
    )
    .unwrap();

    let result = client
        .get_remote_main()
        .call(
            vec!["echo".to_owned()],
            RpcPayload::from_app_params(CapnwebValue::Array(vec![CapnwebValue::custom(Widget(
                "hello".to_owned(),
            ))])),
        )
        .pull()
        .await
        .unwrap();

    let widget = result.downcast_custom::<Widget>().unwrap();
    assert_eq!(*widget, Widget("hello".to_owned()));
}

#[test]
fn checked_downcasts_and_reserved_tags_are_enforced() {
    let value = CapnwebValue::custom(Widget("safe".to_owned()));
    assert!(value.downcast_custom::<Widget>().is_ok());
    assert!(value.downcast_custom::<String>().is_err());

    struct ReservedHandler;
    impl CustomTypeHandler for ReservedHandler {
        fn value_type_id(&self) -> TypeId {
            TypeId::of::<Widget>()
        }
        fn wire_tag(&self) -> &'static str {
            "error"
        }
        fn encode_fields(
            &self,
            _value: &CustomValue,
            _context: &mut dyn CustomMessageContext,
        ) -> Result<Vec<CapnwebValue>> {
            unreachable!()
        }
        fn decode_fields(
            &self,
            _fields: &[CapnwebValue],
            _context: &mut dyn CustomMessageContext,
        ) -> Result<CustomValue> {
            unreachable!()
        }
    }

    let mut registry = CustomTypeRegistry::new();
    assert!(registry.register(Arc::new(ReservedHandler)).is_err());
}
