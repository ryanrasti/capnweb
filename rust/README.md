# Cap'n Web for Rust

An idiomatic Rust implementation of the Cap'n Web RPC protocol. The Cargo package is named
`capnweb-rs` (the library is imported as `capnweb`).

The current v0 spike implements dynamic values, immediate generated-style stub methods, target
dispatch macros, import/export tables, promise pipelining, HTTP batch, WebSocket, and Unix-domain
socket transports. It is deliberately incomplete and not ready for production use.

## External byte channels

Bulk duplex bytes can be externalized from RPC as a typed, single-use `ByteChannel`. On UDS, the
endpoint is transferred atomically with the control frame using `SCM_RIGHTS`. For QUIC-like
transports, `ByteChannelSession` pairs a connection-local `ChannelReference` with an independent
stream in either arrival order:

```rust
let channels = capnweb::ByteChannelSession::new(
    capnweb::BYTE_CHANNEL_VERSION,
    session_id,
    capnweb::ChannelRendezvousLimits::default(),
    16,
)?;
let (attachment, local_stream) = channels.open(64 * 1024)?;

// Pass `attachment` as an ordinary typed RPC argument. The transport task
// receives the other endpoint and writes the canonical fixed-size header.
let outgoing = channels.accept_outgoing().await?;
outgoing.reference.write_header(&mut quic_stream).await?;
```

A custom network transport registers `channels.custom_type_handler()`, runs
`channels.run_maintenance()`, and calls `arrive_stream()` after reading a header from an independently
accepted stream. Its control transport must call `channels.abort(reason)` when the Cap'n Web session
ends. This revokes queued, unmatched, and already-claimed streams and wakes pending I/O. Individual
claimed streams also support `reset(reason)` without affecting siblings.

UDS users should create outbound pairs with `UdsTransport::open_byte_channel()`, or adopt both ends
of an existing pair with `attach_byte_channel()`. Both return the RPC attachment and a session-bound
local FD; aborting the UDS control transport applies `SHUT_RDWR` to locally opened and remotely
claimed channels. A claimed `ByteChannelFd` implements `AsFd` and can duplicate an installation FD
while the revocation handle is retained.

Session IDs bind references to the authenticated transport; channel IDs are never reusable, pending
entries expire, and queues are bounded. Application meaning such as terminal, logs, or archive
remains in the typed RPC method rather than the generic channel protocol. See
[request-as-fd.md](request-as-fd.md).

## Framed stream control

`FramedStreamTransport` carries Cap'n Web over any Tokio `AsyncRead + AsyncWrite` stream using a
bounded four-byte big-endian length prefix and JSON payload. It is suitable for vsock, Unix streams,
and a dedicated QUIC control stream:

```rust
let transport = capnweb::FramedStreamTransport::builder()
    .max_frame_bytes(256 * 1024)
    .byte_channels(channels.clone())?
    .build(stream)?;
```

The builder registration both enables the `ByteChannel` custom value and ties the channel session to
the control transport. Explicit abort, malformed/truncated frames, EOF, and send/receive failures
abort registered channel resources and wake pending operations.

See [JS-PARITY.md](JS-PARITY.md) for the exact translated surface and documented Rust differences,
[rust-port.md](rust-port.md) for the original design proposal,
[rust-types.md](rust-types.md) for the TypeScript-to-Rust mapping proposal, and
[implementation-plan.md](implementation-plan.md) for the staged implementation plan. The
[kitchen-sink design fixture](examples/kitchen-sink/) pairs a broad TypeScript contract with the
intended hand-written Rust equivalent.

## Development

The crate uses the stable Rust toolchain and Rust 2024 edition.

Build the repository's JavaScript package before running the interoperability test:

```sh
# From the repository root:
npm install
npm run build

cd rust
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```
