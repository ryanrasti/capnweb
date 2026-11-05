//! A ReadableStream inside a call's result, reached by promise pipelining as well as by the result's pull: sent once,
//! by whichever serializes it first; the other says so, and nothing else fails.

use std::sync::Arc;

use capnweb::{
    CapnwebValue, FramedStreamTransport, IntoCapnwebValue, ReadableStream, Result, RpcSession,
    RpcSessionOptions, rpc_interface, rpc_target,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

#[derive(IntoCapnwebValue)]
struct Started {
    pid: f64,
    out: ReadableStream,
}

#[rpc_interface]
trait StartApi {
    fn start() -> Started;
}

struct StartTarget;

#[rpc_target(StartApi)]
impl StartTarget {
    fn start(&self) -> Result<Started> {
        Ok(Started {
            pid: 7.0,
            out: ReadableStream::new(&b"hello"[..]),
        })
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

/// the frames the session sends until both exports' answers (resolve or reject) have come: each answer by its ID
async fn answers(peer: &mut DuplexStream, ids: &[i64]) -> Vec<(i64, serde_json::Value)> {
    let mut seen = Vec::new();
    while seen.len() < ids.len() {
        let frame = read_frame(peer).await;
        if matches!(frame[0].as_str(), Some("resolve" | "reject"))
            && ids.contains(&frame[1].as_i64().unwrap())
        {
            seen.push((frame[1].as_i64().unwrap(), frame));
        }
    }
    seen.sort_by_key(|(id, _)| *id);
    seen
}

fn served() -> (RpcSession, DuplexStream) {
    let (server_stream, peer) = tokio::io::duplex(1 << 16);
    let transport = Arc::new(FramedStreamTransport::new(server_stream).unwrap());
    let session = RpcSession::new(
        transport,
        Some(CapnwebValue::Target(Arc::new(StartTarget))),
        RpcSessionOptions::default(),
    )
    .unwrap();
    (session, peer)
}

#[tokio::test(flavor = "current_thread")]
async fn a_result_pulled_alone_sends_its_stream_once() {
    let (_session, mut peer) = served();
    write_frame(&mut peer, br#"["push",["pipeline",0,["start"],[]]]"#).await;
    write_frame(&mut peer, br#"["pull",1]"#).await;
    let seen = answers(&mut peer, &[1]).await;
    assert_eq!(seen[0].1[0], "resolve", "{:?}", seen);
    assert_eq!(seen[0].1[2]["out"][0], "readable", "{:?}", seen);
}

#[tokio::test(flavor = "current_thread")]
async fn a_stream_pipelined_and_pulled_whole_is_sent_by_one_and_refused_by_the_other() {
    let (_session, mut peer) = served();
    write_frame(&mut peer, br#"["push",["pipeline",0,["start"],[]]]"#).await;
    // (the result's stream by pipelining, export 2; then each pulled)
    write_frame(&mut peer, br#"["push",["pipeline",1,["out"]]]"#).await;
    write_frame(&mut peer, br#"["pull",2]"#).await;
    write_frame(&mut peer, br#"["pull",1]"#).await;
    let seen = answers(&mut peer, &[1, 2]).await;
    let sent: Vec<_> = seen
        .iter()
        .filter(|(_, frame)| frame.to_string().contains(r#"["readable","#))
        .collect();
    assert_eq!(sent.len(), 1, "the stream sent once: {seen:?}");
    let refused: Vec<_> = seen
        .iter()
        .filter(|(_, frame)| frame.to_string().contains("already sent"))
        .collect();
    assert_eq!(refused.len(), 1, "the other told so: {seen:?}");
}
