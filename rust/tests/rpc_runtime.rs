#![allow(dead_code)]

#[path = "../src/core.rs"]
mod core;
#[path = "../src/eager_promise.rs"]
mod eager_promise;
#[path = "../src/rpc.rs"]
mod rpc;
#[path = "../src/serialize.rs"]
mod serialize;

use std::future::{Future, pending};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use core::{
    CapnwebValue, Error, PayloadStubHook, PropertyKey, PropertyPath, Result, RpcPayload,
    RpcTargetInner, StubHook,
};
use rpc::{
    AnyRpcTransport, RpcSession, RpcSessionImpl, RpcSessionOptions, TransportMessage, TransportSend,
};
use serialize::{DEFAULT_LIMITS, EncodingLevel, Exporter, Importer};

type ReceiveFuture = Pin<Box<dyn Future<Output = Result<TransportMessage>> + Send + 'static>>;

#[derive(Clone, Copy)]
enum SendBehavior {
    Done,
    FailSynchronously,
    FailAsynchronously,
}

struct RecordingTransport {
    behavior: SendBehavior,
    sent: Mutex<Vec<String>>,
}

impl RecordingTransport {
    fn new(behavior: SendBehavior) -> Self {
        Self {
            behavior,
            sent: Mutex::new(Vec::new()),
        }
    }
}

impl AnyRpcTransport for RecordingTransport {
    fn send(&self, message: TransportMessage) -> Result<TransportSend> {
        if let TransportMessage::Text(message) = message {
            self.sent.lock().expect("sent mutex poisoned").push(message);
        }
        match self.behavior {
            SendBehavior::Done => Ok(TransportSend::Done(None)),
            SendBehavior::FailSynchronously => {
                Err(Error::Message("synchronous send failure".to_owned()))
            }
            SendBehavior::FailAsynchronously => Ok(TransportSend::Pending(Box::pin(async {
                tokio::task::yield_now().await;
                Err(Error::Message("asynchronous send failure".to_owned()))
            }))),
        }
    }

    fn receive(self: Arc<Self>) -> ReceiveFuture {
        Box::pin(pending())
    }
}

fn value_hook(value: CapnwebValue) -> StubHook {
    Arc::new(PayloadStubHook::new(RpcPayload::from_app_return(value)))
}

fn new_session(transport: Arc<dyn AnyRpcTransport>) -> RpcSessionImpl {
    RpcSessionImpl::new(
        transport,
        value_hook(CapnwebValue::Null),
        RpcSessionOptions::default(),
    )
    .expect("session should start")
}

#[tokio::test(flavor = "current_thread")]
async fn repeated_exports_share_an_id_and_release_exact_counts() {
    let transport = Arc::new(RecordingTransport::new(SendBehavior::Done));
    let mut session = new_session(transport);
    let hook = value_hook(CapnwebValue::String("shared".to_owned()));

    let first = Exporter::export_stub(&mut session, Arc::clone(&hook)).unwrap();
    let second = Exporter::export_stub(&mut session, hook).unwrap();
    assert_eq!(first, second);

    Exporter::unexport(&mut session, vec![first]).unwrap();
    assert!(Importer::get_export(&mut session, first).is_some());
    Exporter::unexport(&mut session, vec![first]).unwrap();
    assert!(Importer::get_export(&mut session, first).is_none());
    assert!(Exporter::unexport(&mut session, vec![first]).is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn promise_exports_always_receive_fresh_ids() {
    let transport = Arc::new(RecordingTransport::new(SendBehavior::Done));
    let mut session = new_session(transport);
    let hook = value_hook(CapnwebValue::Number(1.0));

    let first = Exporter::export_promise(&mut session, Arc::clone(&hook)).unwrap();
    let second = Exporter::export_promise(&mut session, hook).unwrap();
    assert_ne!(first, second);
}

#[tokio::test(flavor = "current_thread")]
async fn final_import_drop_sends_one_release_for_the_remote_count() {
    let transport = Arc::new(RecordingTransport::new(SendBehavior::Done));
    let mut session = new_session(transport.clone());

    let first = Importer::import_stub(&mut session, 7).unwrap();
    let second = Importer::import_stub(&mut session, 7).unwrap();
    drop(first);
    assert!(transport.sent.lock().unwrap().is_empty());

    drop(second);
    assert_eq!(
        &transport.sent.lock().unwrap()[..],
        &["[\"release\",7.0,1.0]"]
    );
}

async fn assert_send_failure_is_deferred(behavior: SendBehavior, expected: &str) {
    let transport = Arc::new(RecordingTransport::new(behavior));
    let mut session = new_session(transport);
    let import = Importer::import_stub(&mut session, 3).unwrap();
    drop(import);

    // send() failure handling is promise/microtask deferred, so bookkeeping can finish first.
    assert!(
        Exporter::export_stub(
            &mut session,
            value_hook(CapnwebValue::String("before abort".to_owned())),
        )
        .is_ok()
    );

    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    let error = Exporter::export_stub(
        &mut session,
        value_hook(CapnwebValue::String("after abort".to_owned())),
    )
    .unwrap_err();
    assert!(error.to_string().contains(expected));
}

#[tokio::test(flavor = "current_thread")]
async fn synchronous_send_failure_aborts_on_a_later_turn() {
    assert_send_failure_is_deferred(SendBehavior::FailSynchronously, "synchronous").await;
}

#[tokio::test(flavor = "current_thread")]
async fn asynchronous_send_rejection_aborts_the_session() {
    assert_send_failure_is_deferred(SendBehavior::FailAsynchronously, "asynchronous").await;
}

struct ChannelTransport {
    sender: tokio::sync::mpsc::UnboundedSender<TransportMessage>,
    receiver: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<TransportMessage>>,
}

impl AnyRpcTransport for ChannelTransport {
    fn send(&self, message: TransportMessage) -> Result<TransportSend> {
        self.sender
            .send(message)
            .map_err(|_| Error::Message("channel closed".to_owned()))?;
        Ok(TransportSend::Done(None))
    }

    fn receive(self: Arc<Self>) -> ReceiveFuture {
        Box::pin(async move {
            self.receiver
                .lock()
                .await
                .recv()
                .await
                .ok_or_else(|| Error::Message("channel closed".to_owned()))
        })
    }
}

fn transport_pair() -> (Arc<ChannelTransport>, Arc<ChannelTransport>) {
    let (to_a, from_b) = tokio::sync::mpsc::unbounded_channel();
    let (to_b, from_a) = tokio::sync::mpsc::unbounded_channel();
    (
        Arc::new(ChannelTransport {
            sender: to_b,
            receiver: tokio::sync::Mutex::new(from_b),
        }),
        Arc::new(ChannelTransport {
            sender: to_a,
            receiver: tokio::sync::Mutex::new(from_a),
        }),
    )
}

struct AnswerTarget;

impl RpcTargetInner for AnswerTarget {
    fn call(self: Arc<Self>, path: PropertyPath, _args: RpcPayload) -> Result<StubHook> {
        assert!(matches!(
            &path[..],
            [PropertyKey::String(method)] if method == "answer"
        ));
        Ok(value_hook(CapnwebValue::Number(42.0)))
    }

    fn get(self: Arc<Self>, _path: PropertyPath) -> Result<StubHook> {
        unreachable!("the test only performs a method call")
    }
}

#[tokio::test(flavor = "current_thread")]
async fn in_memory_call_pull_resolve_and_release_round_trip() {
    let (client_transport, server_transport) = transport_pair();
    let client = RpcSession::new(client_transport, None, RpcSessionOptions::default()).unwrap();
    let _server = RpcSession::new(
        server_transport,
        Some(CapnwebValue::Target(Arc::new(AnswerTarget))),
        RpcSessionOptions::default(),
    )
    .unwrap();

    let method = client
        .get_remote_main()
        .property(PropertyKey::String("answer".to_owned()));
    let result = method
        .as_stub()
        .call_path(RpcPayload::from_app_params(CapnwebValue::Array(Vec::new())))
        .unwrap()
        .resolve()
        .await
        .unwrap();

    assert!(matches!(result, CapnwebValue::Number(42.0)));
    assert_eq!(client.get_stats().imports, 1);
}

struct CustomEncodingTransport {
    sent: Mutex<Vec<CapnwebValue>>,
}

impl AnyRpcTransport for CustomEncodingTransport {
    fn encoding_level(&self) -> EncodingLevel {
        EncodingLevel::JsonCompatibleWithBytes
    }

    fn send(&self, message: TransportMessage) -> Result<TransportSend> {
        let TransportMessage::Value(message) = message else {
            panic!("custom transport received string-level data")
        };
        self.sent.lock().unwrap().push(message);
        Ok(TransportSend::Done(Some(123)))
    }

    fn receive(self: Arc<Self>) -> ReceiveFuture {
        Box::pin(pending())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn custom_encoding_transport_receives_value_trees() {
    let transport = Arc::new(CustomEncodingTransport {
        sent: Mutex::new(Vec::new()),
    });
    let session = RpcSession::new(transport.clone(), None, RpcSessionOptions::default()).unwrap();
    let method = session
        .get_remote_main()
        .property(PropertyKey::String("unused".to_owned()));
    let pending = method
        .as_stub()
        .call_path(RpcPayload::from_app_params(CapnwebValue::Array(Vec::new())))
        .unwrap();
    drop(pending);

    let sent = transport.sent.lock().unwrap();
    assert!(matches!(sent.first(), Some(CapnwebValue::Array(message))
        if matches!(&message[..], [CapnwebValue::String(tag), ..] if tag == "push")));
}

struct OneMessageTransport {
    incoming: Mutex<Option<String>>,
}

impl AnyRpcTransport for OneMessageTransport {
    fn send(&self, _message: TransportMessage) -> Result<TransportSend> {
        Ok(TransportSend::Done(None))
    }

    fn receive(self: Arc<Self>) -> ReceiveFuture {
        Box::pin(async move {
            if let Some(message) = self.incoming.lock().unwrap().take() {
                Ok(TransportMessage::Text(message))
            } else {
                pending().await
            }
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn oversized_string_message_aborts_before_json_parsing() {
    let transport = Arc::new(OneMessageTransport {
        incoming: Mutex::new(Some("[\"message larger than limit\"]".to_owned())),
    });
    let mut limits = DEFAULT_LIMITS;
    limits.max_message_size = 4;
    let mut session = RpcSessionImpl::new(
        transport,
        value_hook(CapnwebValue::Null),
        RpcSessionOptions {
            on_send_error: None,
            limits,
        },
    )
    .unwrap();

    for _ in 0..3 {
        tokio::task::yield_now().await;
    }
    let error = Exporter::export_stub(
        &mut session,
        value_hook(CapnwebValue::String("after oversized message".to_owned())),
    )
    .unwrap_err();
    assert!(error.to_string().contains("maximum size"));
}
