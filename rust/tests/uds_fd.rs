#![cfg(unix)]

use std::io::{Read, Write};
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::sync::Arc;

use capnweb::{
    AnyRpcTransport, CapnwebValue, Error, Fd, PayloadStubHook, PromiseStubHook, PropertyKey,
    PropertyPath, Result, RpcPayload, RpcSession, RpcSessionOptions, RpcTargetInner, StubHook,
    UdsTransport, serialize,
};

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
            let CapnwebValue::Fd(fd) = args.remove(0) else {
                return Err(Error::Type("argument is not an fd".to_owned()));
            };
            Ok(Arc::new(PayloadStubHook::new(RpcPayload::from_app_return(
                CapnwebValue::Fd(fd),
            ))) as StubHook)
        }))
    }

    fn get(self: Arc<Self>, path: PropertyPath) -> Result<StubHook> {
        Err(Error::Type(format!("No such RPC property: {path:?}")))
    }
}

#[test]
fn ordinary_serialization_rejects_fds() {
    let (_peer, fd) = UnixStream::pair().unwrap();
    let error = serialize(&CapnwebValue::Fd(Fd::new(OwnedFd::from(fd)))).unwrap_err();
    assert!(error.to_string().contains("structured-clonable transport"));
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
    let fd = Fd::new(OwnedFd::from(writer));
    let returned = client
        .get_remote_main()
        .call(
            vec!["echoFd".to_owned()],
            RpcPayload::from_app_params(CapnwebValue::Array(vec![CapnwebValue::Fd(fd)])),
        )
        .pull()
        .await
        .unwrap();
    let CapnwebValue::Fd(returned) = returned else {
        panic!("RPC did not return an fd");
    };

    let mut returned_file = std::fs::File::from(returned.try_clone_owned().unwrap());
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
