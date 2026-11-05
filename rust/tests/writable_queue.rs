//! An exported WritableStream queues at most MAX_QUEUED_WRITES writes: a sender that ignores flow control has the
//! writes past them rejected, not held.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use capnweb::{
    CapnwebValue, FramedStreamTransport, MAX_QUEUED_WRITES, Result, RpcSession, RpcSessionOptions,
    WritableStream, rpc_interface, rpc_target,
};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream};

/// a sink that never takes a byte: its first write waits for good
struct Stuck;

impl AsyncWrite for Stuck {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Pending
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }
}

#[rpc_interface]
trait SinkApi {
    fn sink() -> WritableStream;
}

struct SinkTarget;

#[rpc_target(SinkApi)]
impl SinkTarget {
    fn sink(&self) -> Result<WritableStream> {
        Ok(WritableStream::new(Stuck))
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

#[tokio::test(flavor = "current_thread")]
async fn writes_past_the_queue_limit_are_rejected() {
    let (server_stream, mut peer) = tokio::io::duplex(1 << 20);
    let transport = Arc::new(FramedStreamTransport::new(server_stream).unwrap());
    let _session = RpcSession::new(
        transport,
        Some(CapnwebValue::Target(Arc::new(SinkTarget))),
        RpcSessionOptions::default(),
    )
    .unwrap();

    write_frame(&mut peer, br#"["push",["pipeline",0,["sink"],[]]]"#).await;
    write_frame(&mut peer, br#"["pull",1]"#).await;
    let answer = read_frame(&mut peer).await;
    assert_eq!(answer[0], "resolve", "{answer}");
    let sink = answer[2][1].as_i64().expect("[\"writable\", id]");

    // (one write the sink holds, MAX_QUEUED_WRITES - 1 behind it, and one more, export 2 + MAX_QUEUED_WRITES)
    for _ in 0..=MAX_QUEUED_WRITES {
        let write = format!(r#"["stream",["pipeline",{sink},["write"],[["bytes","AAAA"]]]]"#);
        write_frame(&mut peer, write.as_bytes()).await;
    }
    let answer = tokio::time::timeout(std::time::Duration::from_secs(2), read_frame(&mut peer))
        .await
        .expect("no rejection: every write held");
    assert_eq!(answer[0], "reject", "{answer}");
    assert_eq!(answer[1], 2 + MAX_QUEUED_WRITES as i64, "{answer}");
    assert!(answer.to_string().contains("queued"), "{answer}");
}
