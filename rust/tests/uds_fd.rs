#![cfg(unix)]

use std::io::{Read, Write};
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::sync::Arc;

use capnweb::{
    AnyRpcTransport, ByteChannel, CapnwebValue, Error, PayloadStubHook, PromiseStubHook,
    PropertyKey, PropertyPath, Result, RpcPayload, RpcSession, RpcSessionOptions, RpcStub,
    RpcTargetInner, StubHook, UdsTransport, rpc_interface, rpc_target, serialize,
};

#[rpc_interface]
trait ChannelApi {
    fn consume(channel: ByteChannel);
}

struct ChannelTarget;

#[rpc_target(ChannelApi)]
impl ChannelTarget {
    async fn consume(&self, channel: ByteChannel) -> Result<()> {
        let descriptor = channel.claim_fd().await?;
        let mut stream = std::fs::File::from(
            descriptor
                .try_clone_to_owned()
                .map_err(|error| Error::Message(error.to_string()))?,
        );
        stream
            .write_all(b"typed byte channel")
            .map_err(|error| Error::Message(error.to_string()))?;
        Ok(())
    }
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

struct HoldingChannelTarget {
    claimed: Arc<tokio::sync::Notify>,
}

#[rpc_target(ChannelApi)]
impl HoldingChannelTarget {
    async fn consume(&self, channel: ByteChannel) -> Result<()> {
        let _descriptor = channel.claim_fd().await?;
        self.claimed.notify_one();
        std::future::pending().await
    }
}

struct EchoFdTarget;

impl RpcTargetInner for EchoFdTarget {
    fn call(self: Arc<Self>, path: PropertyPath, args: RpcPayload) -> Result<StubHook> {
        assert!(matches!(
            path.as_slice(),
            [PropertyKey::String(method)] if method == "echoFd"
        ));
        Ok(PromiseStubHook::from_future(async move {
            let CapnwebValue::Array(mut args) = args.deliver_resolve().await? else {
                return Err(Error::Type("arguments are not an array".to_owned()));
            };
            let CapnwebValue::Custom(fd) = args.remove(0) else {
                return Err(Error::Type("argument is not a custom value".to_owned()));
            };
            fd.downcast::<OwnedFd>()?;
            let literal = args.remove(0);
            assert!(matches!(
                literal,
                CapnwebValue::Array(values)
                    if matches!(values.as_slice(), [CapnwebValue::String(tag), CapnwebValue::Number(0.0)] if tag == "fd")
            ));
            Ok(Arc::new(PayloadStubHook::new(RpcPayload::from_app_return(
                CapnwebValue::Custom(fd),
            ))) as StubHook)
        }))
    }

    fn get(self: Arc<Self>, path: PropertyPath) -> Result<StubHook> {
        Err(Error::Type(format!("No such RPC property: {path:?}")))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_byte_channel_passes_a_connected_stream_over_uds() {
    let (client_transport, server_transport) = UdsTransport::pair().unwrap();
    let client =
        RpcSession::new(client_transport.clone(), None, RpcSessionOptions::default()).unwrap();
    let server = RpcSession::new(
        server_transport.clone(),
        Some(CapnwebValue::Target(Arc::new(ChannelTarget))),
        RpcSessionOptions::default(),
    )
    .unwrap();
    let api = ChannelApiStub::new(client.get_remote_main());
    let (mut reader, writer) = UnixStream::pair().unwrap();
    let channel = ByteChannel::from_fd(OwnedFd::from(writer));

    api.consume(channel).await.unwrap();
    let mut bytes = vec![0; b"typed byte channel".len()];
    reader.read_exact(&mut bytes).unwrap();
    assert_eq!(bytes, b"typed byte channel");

    client_transport.abort(Error::Message("test complete".to_owned()));
    server_transport.abort(Error::Message("test complete".to_owned()));
    drop(server);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn byte_channel_rejects_non_stream_descriptors() {
    let (client_transport, server_transport) = UdsTransport::pair().unwrap();
    let client =
        RpcSession::new(client_transport.clone(), None, RpcSessionOptions::default()).unwrap();
    let server = RpcSession::new(
        server_transport.clone(),
        Some(CapnwebValue::Target(Arc::new(ChannelTarget))),
        RpcSessionOptions::default(),
    )
    .unwrap();
    let api = ChannelApiStub::new(client.get_remote_main());
    let (_peer, datagram) = std::os::unix::net::UnixDatagram::pair().unwrap();
    let channel = ByteChannel::from_fd(OwnedFd::from(datagram));

    let error = api.consume(channel).await.unwrap_err();
    assert!(error.to_string().contains("SOCK_STREAM"));

    client_transport.abort(Error::Message("test complete".to_owned()));
    server_transport.abort(Error::Message("test complete".to_owned()));
    drop(server);
}

#[tokio::test(flavor = "current_thread")]
async fn aborting_uds_control_revokes_locally_opened_data_channels() {
    let (transport, _peer) = UdsTransport::pair().unwrap();
    let (_attachment, local) = transport.open_byte_channel().unwrap();
    let mut stream = std::fs::File::from(local.try_clone_to_owned().unwrap());

    transport.abort(Error::Message("self-fenced".to_owned()));
    assert!(stream.write_all(b"must not escape").is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uds_channel_transparently_follows_a_three_party_proxied_stub() {
    let (c_to_a_transport, a_transport) = UdsTransport::pair().unwrap();
    let c_to_a =
        RpcSession::new(c_to_a_transport.clone(), None, RpcSessionOptions::default()).unwrap();
    let a_server = RpcSession::new(
        a_transport.clone(),
        Some(CapnwebValue::Target(Arc::new(ChannelTarget))),
        RpcSessionOptions::default(),
    )
    .unwrap();

    let (b_transport, c_to_b_transport) = UdsTransport::pair().unwrap();
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
    let (attachment, local) = b_transport.open_byte_channel().unwrap();
    let mut local = std::fs::File::from(local.try_clone_to_owned().unwrap());

    let call = a_proxy.call(
        vec!["consume".to_owned()],
        RpcPayload::from_app_params(CapnwebValue::Array(vec![CapnwebValue::custom(attachment)])),
    );
    call.pull().await.unwrap();
    let mut bytes = vec![0; b"typed byte channel".len()];
    local.read_exact(&mut bytes).unwrap();
    assert_eq!(bytes, b"typed byte channel");

    for transport in [b_transport, c_to_b_transport, c_to_a_transport, a_transport] {
        transport.abort(Error::Message("test complete".to_owned()));
    }
    drop((a_server, c_server));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborting_uds_control_revokes_a_remotely_claimed_data_channel() {
    let (client_transport, server_transport) = UdsTransport::pair().unwrap();
    let client =
        RpcSession::new(client_transport.clone(), None, RpcSessionOptions::default()).unwrap();
    let claimed = Arc::new(tokio::sync::Notify::new());
    let server = RpcSession::new(
        server_transport.clone(),
        Some(CapnwebValue::Target(Arc::new(HoldingChannelTarget {
            claimed: Arc::clone(&claimed),
        }))),
        RpcSessionOptions::default(),
    )
    .unwrap();
    let api = ChannelApiStub::new(client.get_remote_main());
    let (attachment, local) = client_transport.open_byte_channel().unwrap();
    let mut stream = std::fs::File::from(local.try_clone_to_owned().unwrap());
    let call = tokio::spawn(async move { api.consume(attachment).await });

    tokio::time::timeout(std::time::Duration::from_secs(1), claimed.notified())
        .await
        .expect("server did not claim the byte channel");
    server_transport.abort(Error::Message("self-fenced".to_owned()));
    assert!(stream.write_all(b"must not escape").is_err());

    call.abort();
    client_transport.abort(Error::Message("test complete".to_owned()));
    drop(server);
}

#[tokio::test(flavor = "current_thread")]
async fn peer_shutdown_wakes_a_pending_uds_receive() {
    let (waiting, peer) = UdsTransport::pair().unwrap();
    let receive = waiting.clone().receive();
    peer.abort(Error::Message("peer stopped".to_owned()));

    let result = tokio::time::timeout(std::time::Duration::from_secs(1), receive)
        .await
        .expect("pending UDS receive did not wake");
    assert!(result.is_err());
}

#[test]
fn ordinary_serialization_rejects_fds() {
    let (_peer, fd) = UnixStream::pair().unwrap();
    let error = serialize(&CapnwebValue::custom(OwnedFd::from(fd))).unwrap_err();
    assert!(error.to_string().contains("transport handler"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_a_real_fd_over_uds_with_scm_rights() {
    let (client_transport, server_transport) = UdsTransport::pair().unwrap();
    let client =
        RpcSession::new(client_transport.clone(), None, RpcSessionOptions::default()).unwrap();
    let server = RpcSession::new(
        server_transport.clone(),
        Some(CapnwebValue::Target(Arc::new(EchoFdTarget))),
        RpcSessionOptions::default(),
    )
    .unwrap();

    let (mut reader, writer) = UnixStream::pair().unwrap();
    let fd = CapnwebValue::custom(OwnedFd::from(writer));
    let returned = client
        .get_remote_main()
        .call(
            vec!["echoFd".to_owned()],
            RpcPayload::from_app_params(CapnwebValue::Array(vec![
                fd,
                CapnwebValue::Array(vec![
                    CapnwebValue::String("fd".to_owned()),
                    CapnwebValue::Number(0.0),
                ]),
            ])),
        )
        .pull()
        .await
        .unwrap();
    let CapnwebValue::Custom(returned) = returned else {
        panic!("RPC did not return a custom value");
    };
    let returned = returned.downcast::<OwnedFd>().unwrap();

    let mut returned_file = std::fs::File::from(returned.as_fd().try_clone_to_owned().unwrap());
    returned_file
        .write_all(b"fd passed over SCM_RIGHTS")
        .unwrap();
    drop(returned_file);
    drop(returned);

    let mut received = vec![0; b"fd passed over SCM_RIGHTS".len()];
    reader.read_exact(&mut received).unwrap();
    assert_eq!(received, b"fd passed over SCM_RIGHTS");

    client_transport.abort(Error::Message("test complete".to_owned()));
    server_transport.abort(Error::Message("test complete".to_owned()));
    drop(server);
}
