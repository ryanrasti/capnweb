use std::any::Any;
use std::time::Duration;

use capnweb::channel::{
    ByteChannelEndpoint, ByteChannelTransportAdapter, ChannelReference, ChannelRendezvous,
    ChannelRendezvousLimits,
};
use capnweb::{
    ByteChannel, ByteChannelSession, CustomMessageContext, Error, FromCapnwebValue,
    IntoCapnwebValue,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct EmptyContext;

impl CustomMessageContext for EmptyContext {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

fn reference(id: u128) -> ChannelReference {
    ChannelReference::new(1, 7, id)
}

fn stream_endpoint() -> (ByteChannelEndpoint, tokio::io::DuplexStream) {
    let (local, remote) = tokio::io::duplex(1024);
    (ByteChannelEndpoint::stream(remote), local)
}

#[tokio::test(flavor = "current_thread")]
async fn rpc_reference_can_arrive_before_the_stream() {
    let rendezvous = ChannelRendezvous::new(1, 7, ChannelRendezvousLimits::default());
    let channel = rendezvous.expect(reference(11)).unwrap();
    let (endpoint, mut peer) = stream_endpoint();

    rendezvous.arrive(reference(11), endpoint).unwrap();
    let mut stream = channel.claim_stream().await.unwrap();
    peer.write_all(b"hello").await.unwrap();
    let mut bytes = [0; 5];
    stream.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"hello");
}

#[tokio::test(flavor = "current_thread")]
async fn stream_can_arrive_before_the_rpc_reference() {
    let rendezvous = ChannelRendezvous::new(1, 7, ChannelRendezvousLimits::default());
    let (endpoint, mut peer) = stream_endpoint();

    rendezvous.arrive(reference(12), endpoint).unwrap();
    let channel = rendezvous.expect(reference(12)).unwrap();
    let mut stream = channel.claim_stream().await.unwrap();
    stream.write_all(b"world").await.unwrap();
    let mut bytes = [0; 5];
    peer.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"world");
}

#[tokio::test(flavor = "current_thread")]
async fn a_byte_channel_is_claimed_exactly_once_across_clones() {
    let (endpoint, _peer) = stream_endpoint();
    let channel = ByteChannel::from_endpoint(endpoint);
    let duplicate = channel.clone();

    channel.claim().await.unwrap();
    let error = duplicate.claim().await.unwrap_err();
    assert!(error.to_string().contains("already claimed"));
}

#[tokio::test(flavor = "current_thread")]
async fn duplicate_references_and_streams_are_rejected() {
    let rendezvous = ChannelRendezvous::new(1, 7, ChannelRendezvousLimits::default());
    let channel = rendezvous.expect(reference(13)).unwrap();
    assert!(rendezvous.expect(reference(13)).is_err());

    let (endpoint, _peer) = stream_endpoint();
    rendezvous.arrive(reference(13), endpoint).unwrap();
    channel.claim().await.unwrap();

    let (duplicate, _peer) = stream_endpoint();
    assert!(rendezvous.arrive(reference(13), duplicate).is_err());
}

#[test]
fn references_are_bound_to_the_protocol_version_and_session() {
    let rendezvous = ChannelRendezvous::new(1, 7, ChannelRendezvousLimits::default());
    assert!(rendezvous.expect(ChannelReference::new(2, 7, 1)).is_err());
    assert!(rendezvous.expect(ChannelReference::new(1, 8, 1)).is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_an_unclaimed_reference_rejects_a_late_stream() {
    let rendezvous = ChannelRendezvous::new(1, 7, ChannelRendezvousLimits::default());
    let channel = rendezvous.expect(reference(15)).unwrap();
    drop(channel);

    let (endpoint, _peer) = stream_endpoint();
    let error = rendezvous.arrive(reference(15), endpoint).unwrap_err();
    assert!(error.to_string().contains("canceled"));
}

#[tokio::test(flavor = "current_thread")]
async fn pending_limits_expiry_and_abort_are_bounded() {
    let limits = ChannelRendezvousLimits {
        max_pending: 1,
        max_channels: 4,
        pending_timeout: Duration::from_millis(1),
    };
    let rendezvous = ChannelRendezvous::new(1, 7, limits);
    let expired = rendezvous.expect(reference(20)).unwrap();
    assert!(rendezvous.expect(reference(21)).is_err());

    tokio::time::sleep(Duration::from_millis(2)).await;
    assert_eq!(rendezvous.expire_pending(), 1);
    assert!(
        expired
            .claim()
            .await
            .unwrap_err()
            .to_string()
            .contains("expired")
    );

    let aborted = rendezvous.expect(reference(22)).unwrap();
    rendezvous.abort(Error::Message("self-fenced".to_owned()));
    assert!(
        aborted
            .claim()
            .await
            .unwrap_err()
            .to_string()
            .contains("self-fenced")
    );
    assert!(rendezvous.expect(reference(23)).is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn session_adapter_exposes_outgoing_channels_to_a_transport() {
    let limits = ChannelRendezvousLimits::default();
    let sender = ByteChannelSession::new(1, 99, limits, 1).unwrap();
    let receiver = ByteChannelSession::new(1, 99, limits, 1).unwrap();
    let (attachment, mut local) = sender.open(1024).unwrap();

    let fields = sender.encode(&attachment, &mut EmptyContext).unwrap();
    let received = receiver.decode(&fields, &mut EmptyContext).unwrap();
    let outgoing = sender.accept_outgoing().await.unwrap();
    let (incoming_endpoint, bridge_peer) = stream_endpoint();
    let mut outgoing_stream = outgoing.endpoint.into_stream().unwrap();
    let mut bridge_peer = bridge_peer;
    tokio::spawn(async move {
        tokio::io::copy_bidirectional(&mut outgoing_stream, &mut bridge_peer)
            .await
            .unwrap();
    });
    receiver
        .arrive(outgoing.reference, incoming_endpoint)
        .unwrap();

    let mut remote = received.claim_stream().await.unwrap();
    local.write_all(b"through adapter").await.unwrap();
    let mut bytes = [0; 15];
    remote.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"through adapter");
}

#[tokio::test(flavor = "current_thread")]
async fn session_abort_revokes_an_already_claimed_stream() {
    let session = ByteChannelSession::new(1, 99, ChannelRendezvousLimits::default(), 1).unwrap();
    let reference = ChannelReference::new(1, 99, 1);
    let (endpoint, _peer) = stream_endpoint();
    session.arrive(reference.clone(), endpoint).unwrap();
    let channel = session
        .decode(&reference.to_fields(), &mut EmptyContext)
        .unwrap();
    let mut stream = channel.claim_stream().await.unwrap();
    let pending_read = tokio::spawn(async move {
        let mut byte = [0; 1];
        stream.read_exact(&mut byte).await
    });
    tokio::task::yield_now().await;

    session.abort(Error::Message("control lost".to_owned()));
    let error = tokio::time::timeout(Duration::from_secs(1), pending_read)
        .await
        .expect("revocation did not wake a pending read")
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("control lost"));
}

#[tokio::test(flavor = "current_thread")]
async fn one_stream_can_be_reset_without_affecting_a_sibling() {
    let (first_endpoint, _first_peer) = stream_endpoint();
    let (second_endpoint, mut second_peer) = stream_endpoint();
    let first = ByteChannel::from_endpoint(first_endpoint);
    let second = ByteChannel::from_endpoint(second_endpoint);
    let mut first = first.claim_stream().await.unwrap();
    let mut second = second.claim_stream().await.unwrap();

    first.reset("attachment detached");
    assert!(first.write_all(b"blocked").await.is_err());
    second.write_all(b"alive").await.unwrap();
    let mut bytes = [0; 5];
    second_peer.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"alive");
}

#[tokio::test(flavor = "current_thread")]
async fn session_adapter_bounds_queues_and_stops_maintenance_on_abort() {
    let session = ByteChannelSession::new(1, 99, ChannelRendezvousLimits::default(), 1).unwrap();
    let (first, _local) = session.open(64).unwrap();
    let (second, _local) = session.open(64).unwrap();

    session.encode(&first, &mut EmptyContext).unwrap();
    let error = session.encode(&second, &mut EmptyContext).unwrap_err();
    assert!(error.to_string().contains("queue is full"));

    let maintenance = {
        let session = session.clone();
        tokio::spawn(async move { session.run_maintenance().await })
    };
    session.abort(Error::Message("self-fenced".to_owned()));
    assert!(
        session
            .accept_outgoing()
            .await
            .unwrap_err()
            .to_string()
            .contains("self-fenced")
    );
    assert!(
        maintenance
            .await
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("self-fenced")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn canonical_stream_header_round_trips_and_is_bounded() {
    let reference = ChannelReference::new(1, 0x1234, 0x5678);
    let encoded = reference.encode_header();
    assert_eq!(encoded.len(), capnweb::channel::BYTE_CHANNEL_HEADER_BYTES);
    assert_eq!(
        ChannelReference::decode_header(&encoded).unwrap(),
        reference
    );

    let (mut writer, mut reader) = tokio::io::duplex(64);
    reference.write_header(&mut writer).await.unwrap();
    assert_eq!(
        ChannelReference::read_header(&mut reader).await.unwrap(),
        reference
    );

    let mut malformed = encoded;
    malformed[0] ^= 0xff;
    assert!(ChannelReference::decode_header(&malformed).is_err());
}

#[test]
fn byte_channel_has_typed_rpc_value_conversions_and_registration_helper() {
    let (endpoint, _peer) = stream_endpoint();
    let channel = ByteChannel::from_endpoint(endpoint);
    let value = channel.clone().into_capnweb_value().unwrap();
    let decoded = ByteChannel::from_capnweb_value(value).unwrap();

    futures::executor::block_on(decoded.claim()).unwrap();
    assert!(futures::executor::block_on(channel.claim()).is_err());

    let session = ByteChannelSession::new(1, 7, ChannelRendezvousLimits::default(), 1).unwrap();
    assert_eq!(session.custom_type_handler().wire_tag(), "byteChannel");
}
