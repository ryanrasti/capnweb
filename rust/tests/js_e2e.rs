//! JavaScript-to-Rust end-to-end coverage.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;

use capnweb::transport::IoTransport;
use capnweb::{rpc_interface, rpc_target, start_session};
use tokio::process::Command;

#[rpc_interface]
trait JavaScriptCalculator {
    fn triple(value: f64) -> f64;
}

struct Calculator;

#[rpc_target]
impl Calculator {
    async fn add(&self, left: f64, right: f64) -> f64 {
        tokio::task::yield_now().await;
        left + right
    }

    async fn double(&self, value: f64) -> f64 {
        value * 2.0
    }
}

#[tokio::test]
async fn javascript_calls_rust_with_pipelined_argument() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let script = manifest.join("tests/js-peer.mjs");
    let javascript_library = manifest.join("../dist/index.js");
    assert!(
        javascript_library.exists(),
        "build the JavaScript package first with `npm run build`"
    );

    let mut child = Command::new("node")
        .arg(script)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("starts Node.js peer");

    let reader = child.stdout.take().expect("child stdout");
    let writer = child.stdin.take().expect("child stdin");
    let remote = JavaScriptCalculatorStub::new(start_session(
        IoTransport::new(reader, writer),
        Arc::new(Calculator),
    ));

    // Rust calls the JavaScript main target over the same bidirectional session.
    assert_eq!(
        remote.triple(14.0).await.expect("JavaScript RPC succeeds"),
        42.0
    );

    let status = child.wait().await.expect("waits for Node.js peer");
    assert!(status.success(), "Node.js peer exited with {status}");
}
