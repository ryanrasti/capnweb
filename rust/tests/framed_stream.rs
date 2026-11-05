use std::sync::Arc;
use std::time::Duration;

use capnweb::channel::{ByteChannelTransportAdapter, ChannelRendezvousLimits};
use capnweb::{
    AnyRpcTransport, ByteChannel, ByteChannelSession, CapnwebValue, Error, FramedStreamTransport,
    IntoCapnwebValue, Result, RpcPayload, RpcSession, RpcSessionOptions, RpcStub, TransportMessage,
    TransportSend, rpc_interface, rpc_target,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn finish_send(send: TransportSend) -> Result<()> {
    match send {
        TransportSend::Done(_) => Ok(()),
        TransportSend::Pending { sent, .. } => sent.await,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn frames_round_trip_over_an_arbitrary_duplex_stream() {
    let (left, right) = tokio::io::duplex(64);
    let sender = Arc::new(FramedStreamTransport::new(left).unwrap());
    let receiver = Arc::new(FramedStreamTransport::new(right).unwrap());
    let value = CapnwebValue::Array(vec![
        CapnwebValue::String("test".to_owned()),
        CapnwebValue::Number(42.0),
    ]);

    finish_send(
        sender
            .send(TransportMessage::Value {
                value: value.clone(),
                codec: None,
            })
            .unwrap(),
    )
    .await
    .unwrap();
    let TransportMessage::Value {
        value: received, ..
    } = receiver.receive().await.unwrap()
    else {
        panic!("framed transport returned text");
    };
    assert!(matches!(
        received,
        CapnwebValue::Array(values)
            if matches!(values.as_slice(), [CapnwebValue::String(tag), CapnwebValue::Number(42.0)] if tag == "test")
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn send_and_receive_enforce_the_frame_limit_before_allocation() {
    let (transport_stream, mut peer) = tokio::io::duplex(64);
    let transport = Arc::new(
        FramedStreamTransport::builder()
            .max_frame_bytes(8)
            .build(transport_stream)
            .unwrap(),
    );
    let error = transport
        .send(TransportMessage::Value {
            value: CapnwebValue::String("too large".to_owned()),
            codec: None,
        })
        .err()
        .expect("oversized send should fail");
    assert!(error.to_string().contains("maximum size"));

    peer.write_all(&9u32.to_be_bytes()).await.unwrap();
    let error = transport
        .receive()
        .await
        .err()
        .expect("oversized receive should fail");
    assert!(error.to_string().contains("invalid RPC frame size 9"));
}

#[tokio::test(flavor = "current_thread")]
async fn abort_wakes_a_pending_receive() {
    let (stream, _peer) = tokio::io::duplex(64);
    let transport = Arc::new(FramedStreamTransport::new(stream).unwrap());
    let receive = {
        let transport = Arc::clone(&transport);
        tokio::spawn(async move { transport.receive().await })
    };
    tokio::task::yield_now().await;

    transport.abort(Error::Message("control lost".to_owned()));
    let error = tokio::time::timeout(Duration::from_secs(1), receive)
        .await
        .expect("abort did not wake receive")
        .unwrap()
        .err()
        .expect("aborted receive should fail");
    assert!(error.to_string().contains("control lost"));
}

#[tokio::test(flavor = "current_thread")]
async fn peer_eof_aborts_registered_byte_channels() {
    let channels = ByteChannelSession::new(1, 88, ChannelRendezvousLimits::default(), 1).unwrap();
    let reference = capnweb::channel::ChannelReference::new(1, 88, 1);
    let pending = channels
        .decode(&reference.to_fields(), &mut EmptyContext)
        .unwrap();
    let (stream, peer) = tokio::io::duplex(64);
    let transport = Arc::new(
        FramedStreamTransport::builder()
            .byte_channels(Arc::clone(&channels))
            .unwrap()
            .build(stream)
            .unwrap(),
    );
    drop(peer);

    assert!(transport.receive().await.is_err());
    let error = pending.claim().await.unwrap_err();
    assert!(error.to_string().contains("frame length"));
}

#[rpc_interface]
trait ChannelApi {
    fn consume(channel: ByteChannel) -> String;
}

struct ChannelTarget;

#[rpc_target(ChannelApi)]
impl ChannelTarget {
    async fn consume(&self, channel: ByteChannel) -> Result<String> {
        let mut stream = channel.claim_stream().await?;
        let mut bytes = Vec::new();
        stream
            .read_to_end(&mut bytes)
            .await
            .map_err(|error| Error::Message(error.to_string()))?;
        String::from_utf8(bytes).map_err(|error| Error::Type(error.to_string()))
    }
}

async fn connect_next_channel(source: &ByteChannelSession, destination: &ByteChannelSession) {
    let outgoing = source.accept_outgoing().await.unwrap();
    let (incoming, mut bridge) = tokio::io::duplex(1024);
    let mut outgoing_stream = outgoing.endpoint.into_stream().unwrap();
    tokio::spawn(async move {
        tokio::io::copy_bidirectional(&mut outgoing_stream, &mut bridge)
            .await
            .unwrap();
    });
    destination
        .arrive_stream(outgoing.reference, incoming)
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_byte_channel_crosses_framed_control_and_independent_streams() {
    let limits = ChannelRendezvousLimits::default();
    let client_channels = ByteChannelSession::new(1, 77, limits, 4).unwrap();
    let server_channels = ByteChannelSession::new(1, 77, limits, 4).unwrap();
    let (client_stream, server_stream) = tokio::io::duplex(4096);
    let client_transport = Arc::new(
        FramedStreamTransport::builder()
            .byte_channels(Arc::clone(&client_channels))
            .unwrap()
            .build(client_stream)
            .unwrap(),
    );
    let server_transport = Arc::new(
        FramedStreamTransport::builder()
            .byte_channels(Arc::clone(&server_channels))
            .unwrap()
            .build(server_stream)
            .unwrap(),
    );
    let client =
        RpcSession::new(client_transport.clone(), None, RpcSessionOptions::default()).unwrap();
    let server = RpcSession::new(
        server_transport.clone(),
        Some(CapnwebValue::Target(Arc::new(ChannelTarget))),
        RpcSessionOptions::default(),
    )
    .unwrap();
    let api = ChannelApiStub::new(client.get_remote_main());
    let (attachment, mut local) = client_channels.open(1024).unwrap();
    let result = api.consume(attachment);

    connect_next_channel(&client_channels, &server_channels).await;
    local.write_all(b"outside RPC").await.unwrap();
    local.shutdown().await.unwrap();

    assert_eq!(result.await.unwrap(), "outside RPC");

    client_transport.abort(Error::Message("test complete".to_owned()));
    server_transport.abort(Error::Message("test complete".to_owned()));
    drop(server);
}

#[rpc_interface]
trait BrokerApi {
    fn get_a() -> RpcStub;
}

struct BrokerTarget {
    a: RpcStub,
}

#[rpc_target(BrokerApi)]
impl BrokerTarget {
    fn get_a(&self) -> RpcStub {
        self.a.clone()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn channel_transparently_follows_a_three_party_proxied_stub() {
    let limits = ChannelRendezvousLimits::default();

    // C's client connection to A.
    let c_to_a_channels = ByteChannelSession::new(1, 301, limits, 4).unwrap();
    let a_channels = ByteChannelSession::new(1, 301, limits, 4).unwrap();
    let (c_to_a_stream, a_stream) = tokio::io::duplex(4096);
    let c_to_a_transport = Arc::new(
        FramedStreamTransport::builder()
            .byte_channels(Arc::clone(&c_to_a_channels))
            .unwrap()
            .build(c_to_a_stream)
            .unwrap(),
    );
    let a_transport = Arc::new(
        FramedStreamTransport::builder()
            .byte_channels(Arc::clone(&a_channels))
            .unwrap()
            .build(a_stream)
            .unwrap(),
    );
    let c_to_a =
        RpcSession::new(c_to_a_transport.clone(), None, RpcSessionOptions::default()).unwrap();
    let a_server = RpcSession::new(
        a_transport.clone(),
        Some(CapnwebValue::Target(Arc::new(ChannelTarget))),
        RpcSessionOptions::default(),
    )
    .unwrap();

    // B's client connection to C. C returns A's foreign stub without
    // implementing or observing A's calls.
    let b_channels = ByteChannelSession::new(1, 302, limits, 4).unwrap();
    let c_to_b_channels = ByteChannelSession::new(1, 302, limits, 4).unwrap();
    let (b_stream, c_to_b_stream) = tokio::io::duplex(4096);
    let b_transport = Arc::new(
        FramedStreamTransport::builder()
            .byte_channels(Arc::clone(&b_channels))
            .unwrap()
            .build(b_stream)
            .unwrap(),
    );
    let c_to_b_transport = Arc::new(
        FramedStreamTransport::builder()
            .byte_channels(Arc::clone(&c_to_b_channels))
            .unwrap()
            .build(c_to_b_stream)
            .unwrap(),
    );
    let b = RpcSession::new(b_transport.clone(), None, RpcSessionOptions::default()).unwrap();
    let c_server = RpcSession::new(
        c_to_b_transport.clone(),
        Some(CapnwebValue::Target(Arc::new(BrokerTarget {
            a: c_to_a.get_remote_main(),
        }))),
        RpcSessionOptions::default(),
    )
    .unwrap();

    let broker = BrokerApiStub::new(b.get_remote_main());
    let a_proxy = broker.get_a().await.unwrap();
    let (attachment, mut local) = b_channels.open(1024).unwrap();
    let result = a_proxy.call(
        vec!["consume".to_owned()],
        RpcPayload::from_app_params(CapnwebValue::Array(vec![
            attachment.into_capnweb_value().unwrap(),
        ])),
    );

    // B -> C delivers the source endpoint. Re-encoding C -> A creates the
    // transparent relay and a fresh destination attachment.
    connect_next_channel(&b_channels, &c_to_b_channels).await;
    connect_next_channel(&c_to_a_channels, &a_channels).await;
    local.write_all(b"three party").await.unwrap();
    local.shutdown().await.unwrap();

    assert!(matches!(
        result.pull().await.unwrap(),
        CapnwebValue::String(value) if value == "three party"
    ));

    for transport in [b_transport, c_to_b_transport, c_to_a_transport, a_transport] {
        transport.abort(Error::Message("test complete".to_owned()));
    }
    drop((a_server, c_server));
}

#[tokio::test(flavor = "current_thread")]
async fn control_abort_also_aborts_registered_byte_channels() {
    let channels = ByteChannelSession::new(1, 88, ChannelRendezvousLimits::default(), 1).unwrap();
    let (stream, _peer) = tokio::io::duplex(64);
    let transport = FramedStreamTransport::builder()
        .byte_channels(Arc::clone(&channels))
        .unwrap()
        .build(stream)
        .unwrap();

    transport.abort(Error::Message("control failed".to_owned()));
    let (channel, _local) = channels.open(64).unwrap();
    let error = channels.encode(&channel, &mut EmptyContext).unwrap_err();
    assert!(error.to_string().contains("control failed"));
}

struct EmptyContext;

impl capnweb::CustomMessageContext for EmptyContext {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}
