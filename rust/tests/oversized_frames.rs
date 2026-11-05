//! A frame over the framed transport's limit fails the one call (or stream write) it carried, its export rejected,
//! and the session goes on: the frame is read past without being buffered.

use std::sync::Arc;

use capnweb::{
    CapnwebValue, FramedStreamTransport, RpcSession, RpcSessionOptions, rpc_interface, rpc_target,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

#[rpc_interface]
trait EchoApi {
    fn echo(text: String) -> String;
}

struct EchoTarget;

#[rpc_target(EchoApi)]
impl EchoTarget {
    fn echo(&self, text: String) -> String {
        text
    }
}

async fn write_frame(peer: &mut DuplexStream, body: &[u8]) {
    peer.write_all(&u32::try_from(body.len()).unwrap().to_be_bytes())
        .await
        .unwrap();
    peer.write_all(body).await.unwrap();
}

async fn read_frame(peer: &mut DuplexStream) -> serde_json::Value {
    let length = peer.read_u32().await.unwrap() as usize;
    let mut body = vec![0; length];
    peer.read_exact(&mut body).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

/// a session serving EchoTarget over a framed transport of 256-byte frames (room for its rejections), and its peer's raw end
fn served() -> (RpcSession, DuplexStream) {
    let (server_stream, peer) = tokio::io::duplex(4096);
    let transport = Arc::new(
        FramedStreamTransport::builder()
            .max_frame_bytes(256)
            .build(server_stream)
            .unwrap(),
    );
    let session = RpcSession::new(
        transport,
        Some(CapnwebValue::Target(Arc::new(EchoTarget))),
        RpcSessionOptions::default(),
    )
    .unwrap();
    (session, peer)
}

#[tokio::test(flavor = "current_thread")]
async fn an_oversized_push_fails_that_call_and_the_session_goes_on() {
    let (_session, mut peer) = served();

    // (a call too big for a frame: its export, 1, rejected)
    let big = format!(
        r#"["push",["pipeline",0,["echo"],["{}"]]]"#,
        "x".repeat(1000)
    );
    write_frame(&mut peer, big.as_bytes()).await;
    write_frame(&mut peer, br#"["pull",1]"#).await;
    let answer = read_frame(&mut peer).await;
    assert_eq!(answer[0], "reject", "{answer}");
    assert_eq!(answer[1], 1, "{answer}");
    assert!(answer.to_string().contains("1037 bytes"), "{answer}");

    // (the next call, export 2, answered: the session went on)
    write_frame(&mut peer, br#"["push",["pipeline",0,["echo"],["hi"]]]"#).await;
    write_frame(&mut peer, br#"["pull",2]"#).await;
    assert_eq!(
        read_frame(&mut peer).await,
        serde_json::json!(["resolve", 2, "hi"])
    );
}

#[tokio::test(flavor = "current_thread")]
async fn an_oversized_stream_write_fails_that_write_and_the_session_goes_on() {
    let (_session, mut peer) = served();

    // (a stream message is pulled already: its rejection comes unasked)
    let big = format!(
        r#"["stream",["pipeline",0,["echo"],["{}"]]]"#,
        "x".repeat(1000)
    );
    write_frame(&mut peer, big.as_bytes()).await;
    let answer = read_frame(&mut peer).await;
    assert_eq!(answer[0], "reject", "{answer}");
    assert_eq!(answer[1], 1, "{answer}");

    write_frame(&mut peer, br#"["push",["pipeline",0,["echo"],["hi"]]]"#).await;
    write_frame(&mut peer, br#"["pull",2]"#).await;
    assert_eq!(
        read_frame(&mut peer).await,
        serde_json::json!(["resolve", 2, "hi"])
    );
}
