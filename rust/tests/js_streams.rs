//! ReadableStream and WritableStream against JavaScript's Cap'n Web: a Node peer
//! (tests/js-streams-peer.mjs) importing `$CAPNWEB_JS`, default this repository's build
//! (`../dist/index.js`), over newline-delimited stdio. Skipped, saying why, without `node` or that
//! module.

use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use capnweb::{
    AnyRpcTransport, CapnwebValue, Error, ReadableStream, Result, RpcSession, RpcSessionOptions,
    TransportMessage, TransportSend, WritableStream, rpc_target,
};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf};
use tokio::process::{ChildStdin, ChildStdout, Command};
use tokio::sync::mpsc;

/// Messages as lines: written in order by one task, read by another.
struct LineTransport {
    outgoing: mpsc::UnboundedSender<String>,
    incoming: tokio::sync::Mutex<mpsc::UnboundedReceiver<String>>,
}

impl LineTransport {
    fn new(mut stdin: ChildStdin, stdout: ChildStdout) -> Self {
        let (outgoing, mut to_write) = mpsc::unbounded_channel::<String>();
        tokio::spawn(async move {
            while let Some(line) = to_write.recv().await {
                if stdin
                    .write_all(format!("{line}\n").as_bytes())
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        let (received, incoming) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if received.send(line).is_err() {
                    break;
                }
            }
        });
        Self {
            outgoing,
            incoming: tokio::sync::Mutex::new(incoming),
        }
    }
}

impl AnyRpcTransport for LineTransport {
    fn send(&self, message: TransportMessage) -> Result<TransportSend> {
        let TransportMessage::Text(text) = message else {
            return Err(Error::Message("a string transport".to_owned()));
        };
        let size = text.len();
        self.outgoing
            .send(text)
            .map_err(|_| Error::Message("peer gone".to_owned()))?;
        Ok(TransportSend::Done(Some(size)))
    }

    fn receive(
        self: Arc<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<TransportMessage>> + Send + 'static>> {
        Box::pin(async move {
            match self.incoming.lock().await.recv().await {
                Some(line) => Ok(TransportMessage::Text(line)),
                None => Err(Error::Message("peer closed its stdout".to_owned())),
            }
        })
    }
}

/// count chunks of size bytes, byte i of the whole (i % 251)
struct Pattern {
    remaining: usize,
    size: usize,
    offset: usize,
}

impl AsyncRead for Pattern {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let n = self.remaining.min(self.size).min(buf.remaining());
        let start = self.offset;
        buf.put_slice(
            &(start..start + n)
                .map(|i| (i % 251) as u8)
                .collect::<Vec<_>>(),
        );
        self.offset += n;
        self.remaining -= n;
        Poll::Ready(Ok(()))
    }
}

/// zeros without end, counting what's read; its drop noted
struct Endless {
    produced: Arc<AtomicUsize>,
    dropped: Arc<AtomicBool>,
}

impl AsyncRead for Endless {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let n = buf.remaining();
        buf.put_slice(&vec![0; n]);
        self.produced.fetch_add(n, Ordering::Relaxed);
        Poll::Ready(Ok(()))
    }
}

impl Drop for Endless {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Relaxed);
    }
}

/// a few bytes, then an error
struct Failing(bool);

impl AsyncRead for Failing {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.0 {
            return Poll::Ready(Err(io::Error::other("boom")));
        }
        self.0 = true;
        buf.put_slice(b"some");
        Poll::Ready(Ok(()))
    }
}

/// what's written, kept
struct Collect(Arc<Mutex<Vec<u8>>>);

impl AsyncWrite for Collect {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.lock().unwrap().extend_from_slice(data);
        Poll::Ready(Ok(data.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// a sink taking 64 KiB every 10 ms, counting what it took
struct Slow {
    received: Arc<AtomicUsize>,
    sleep: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl AsyncWrite for Slow {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let sleep = self
            .sleep
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(Duration::from_millis(10))));
        if sleep.as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }
        self.sleep = None;
        let n = data.len().min(64 * 1024);
        self.received.fetch_add(n, Ordering::Relaxed);
        Poll::Ready(Ok(n))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[derive(Default)]
struct StreamApi {
    received: Arc<Mutex<Vec<u8>>>,
    produced: Arc<AtomicUsize>,
    dropped: Arc<AtomicBool>,
    slow_received: Arc<AtomicUsize>,
}

#[rpc_target]
impl StreamApi {
    fn pattern(&self, count: f64, size: f64) -> ReadableStream {
        let size = size as usize;
        ReadableStream::new(Pattern {
            remaining: count as usize * size,
            size,
            offset: 0,
        })
    }

    fn sink(&self) -> WritableStream {
        WritableStream::new(Collect(Arc::clone(&self.received)))
    }

    fn received(&self) -> String {
        let received = self.received.lock().unwrap();
        let in_order = received
            .chunks(10_000)
            .enumerate()
            .all(|(i, chunk)| chunk.iter().all(|b| *b == (i % 256) as u8));
        format!(
            "{}x10000{}",
            received.len() / 10_000,
            if in_order {
                " in order"
            } else {
                " out of order"
            }
        )
    }

    fn endless(&self) -> ReadableStream {
        ReadableStream::new(Endless {
            produced: Arc::clone(&self.produced),
            dropped: Arc::clone(&self.dropped),
        })
    }

    fn produced(&self) -> f64 {
        self.produced.load(Ordering::Relaxed) as f64
    }

    fn endless_dropped(&self) -> bool {
        self.dropped.load(Ordering::Relaxed)
    }

    fn slow_sink(&self) -> WritableStream {
        WritableStream::new(Slow {
            received: Arc::clone(&self.slow_received),
            sleep: None,
        })
    }

    fn slow_received(&self) -> f64 {
        self.slow_received.load(Ordering::Relaxed) as f64
    }

    fn failing(&self) -> ReadableStream {
        ReadableStream::new(Failing(false))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streams_interoperate_with_javascript() {
    let tests = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let module = std::env::var("CAPNWEB_JS").unwrap_or_else(|_| "../../dist/index.js".to_owned());
    let resolved = if module.starts_with('/') {
        Path::new(&module).to_path_buf()
    } else {
        tests.join(&module)
    };
    if !resolved.exists() {
        eprintln!(
            "skipped: no Cap'n Web JavaScript at {} (CAPNWEB_JS, or this repository built)",
            resolved.display()
        );
        return;
    }
    let child = Command::new("node")
        .arg(tests.join("js-streams-peer.mjs"))
        .env("CAPNWEB_JS", &resolved)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn();
    let Ok(mut child) = child else {
        eprintln!("skipped: no node");
        return;
    };
    let transport = Arc::new(LineTransport::new(
        child.stdin.take().unwrap(),
        child.stdout.take().unwrap(),
    ));
    let _session = RpcSession::new(
        transport,
        Some(CapnwebValue::Target(Arc::new(StreamApi::default()))),
        RpcSessionOptions::default(),
    )
    .unwrap();
    let mut stderr = BufReader::new(child.stderr.take().unwrap()).lines();
    let status = tokio::time::timeout(Duration::from_secs(60), async {
        let mut report = Vec::new();
        while let Ok(Some(line)) = stderr.next_line().await {
            eprintln!("peer: {line}");
            report.push(line);
        }
        (child.wait().await.unwrap(), report)
    })
    .await
    .expect("the JavaScript peer finished within 60 s");
    assert!(
        status.0.success(),
        "the JavaScript peer failed:\n{}",
        status.1.join("\n")
    );
}
