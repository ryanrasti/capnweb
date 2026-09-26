# Byte channels

## Purpose

`ByteChannel` externalizes ordered duplex bytes from Cap'n Web RPC. RPC carries a typed,
single-use attachment value; a transport carries the bytes independently.

```text
Cap'n Web control   method calls, options, status, signals, exit
ByteChannel         ordered duplex bytes, backpressure, half-close, reset
```

Terminal, log, archive, and forwarded connection bytes therefore do not become JSON chunks and do
not consume RPC promise or capability accounting.

Application meaning is intentionally absent from the channel protocol. A channel passed to
`attach()`, `logs()`, or `copy()` gets its meaning from that typed method. The generic reference
contains only protocol version, control-session ID, and a never-reused channel ID.

## Application API

Create and send a network-capable channel:

```rust
let (attachment, mut local) = channels.open(64 * 1024)?;
remote.attach(options, attachment).await?;
local.write_all(b"input").await?;
```

Receive and claim it exactly once:

```rust
async fn attach(&self, options: AttachOptions, channel: ByteChannel) -> Result<Attachment> {
    let stream = channel.claim_stream().await?;
    self.terminal.attach(stream).await
}
```

For UDS, create a socketpair owned by the control transport:

```rust
let (attachment, local_fd) = uds.open_byte_channel()?;
remote.attach(options, attachment).await?;
```

A claimed `ByteChannelFd` implements `AsFd`. If a VMM needs ownership, duplicate the descriptor and
retain the `ByteChannelFd` for as long as the VMM uses that duplicate. The retained wrapper is the
revocation handle.

`ByteChannel` handles are cloneable for Rust ergonomics, but all clones share one state. The first
claim or transport encoding consumes the attachment; later attempts fail.

## Connection setup

A connection with independent network streams creates one `ByteChannelSession`:

```rust
let channels = ByteChannelSession::builder(BYTE_CHANNEL_VERSION, session_id)
    .rendezvous_limits(ChannelRendezvousLimits::default())
    .relay_limits(ByteChannelRelayLimits {
        max_active: 64,
        buffer_bytes: 64 * 1024,
        setup_timeout: Duration::from_secs(10),
        idle_timeout: Duration::from_secs(300),
    })
    .outgoing_capacity(64)
    .build()?;

let control = FramedStreamTransport::builder()
    .max_frame_bytes(256 * 1024)
    .byte_channels(channels.clone())?
    .build(control_stream)?;
```

`FramedStreamTransport::byte_channels()` both registers the custom value and installs the channel
session as a control-abort handler. EOF, malformed control frames, explicit abort, and transport I/O
failure therefore revoke the channel session.

The independent-stream adapter drains outgoing attachments:

```rust
while let Ok(outgoing) = channels.accept_outgoing().await {
    let mut stream = connection.open_bi().await?;
    outgoing.reference.write_header(&mut stream).await?;
    bridge(outgoing.endpoint, stream).await?;
}
```

It submits accepted streams in either arrival order relative to the RPC reference:

```rust
let reference = ChannelReference::read_header(&mut stream).await?;
channels.arrive_stream(reference, stream)?;
```

`run_maintenance()` expires unmatched streams and references. A transport should run it alongside
its control and independent-stream loops.

## Wire representation

The RPC custom value is conceptually:

```text
["byteChannel", version, session-id, channel-id]
```

The independent stream starts with a fixed 38-byte header:

```text
4 bytes   magic "CNWB"
2 bytes   big-endian protocol version
16 bytes  big-endian session ID
16 bytes  big-endian channel ID
```

The session ID binds the reference to the authenticated control connection. It is not an ownership
credential by itself. Channel IDs begin at one, are scoped to the session, and are never reused.

`ChannelRendezvous` handles both arrival orders:

```text
RPC reference first ─┐
                     ├── paired ByteChannel
stream first ────────┘
```

Unmatched entries, total channel IDs, and retention time are bounded. Duplicate references,
duplicate streams, cross-session references, and unsupported versions are protocol errors.

## Transparent forwarding

Cap'n Web capabilities can be proxied through another peer. For example, C can give B a stub for A,
after which B invokes A directly through the proxy:

```text
B ── Cap'n Web call + channel reference ──> C ── forwarded call ──> A
```

C's application does not observe the call. Explicit application-level `forward(channel)` is
therefore insufficient.

A decoded channel records that it is an incoming attachment. When C's A-side custom encoder sees
that incoming channel, it performs attachment transcoding:

1. Consume the incoming channel exactly once.
2. Acquire a bounded relay permit synchronously.
3. Create a bounded local duplex pair.
4. Queue one endpoint as a fresh channel on the destination session.
5. Encode the destination session/channel reference immediately.
6. Start a relay task which awaits the source endpoint and pumps to the retained local endpoint.

```text
B data stream
      ↕
 C bounded relay
      ↕
A data stream
```

No byte payload enters a Cap'n Web frame. Only relay setup is triggered by custom-value encoding.
The same application API is used for direct and proxied calls.

A locally-created stream can be queued directly on a network session. A locally-created FD sent by
a stream transport is converted into a relay because an arbitrary network transport cannot carry an
FD. A received channel is always treated as foreign and relayed when re-encoded, even if its source
endpoint happened to arrive before its RPC reference.

## Relay behavior

A transparent relay provides:

- bounded concurrent relay permits;
- a fixed copy buffer per direction;
- bounded source-attachment setup time;
- a whole-channel idle timeout refreshed by progress in either direction;
- ordered bytes and natural backpressure;
- write-half shutdown propagation on EOF;
- cancellation of both directions on an I/O error or timeout;
- source and destination control sessions as upper lifetime bounds.

One-way traffic keeps the relay alive. For example, continuous log output is activity even if the
reverse direction carries no input.

Outgoing queue capacity is reserved before the single-use attachment is consumed or a relay is
started. Queue backpressure therefore fails serialization synchronously while leaving the attachment
available for retry. Relay capacity and a Tokio runtime handle are likewise reserved before the
attachment is consumed. Relay-limit backpressure is therefore retryable too. Source setup happens
asynchronously, allowing the destination RPC call and source data stream to arrive in either order.

## Lifetime and reset

The lifetime rule is:

> A byte channel may end before its control session, but never after it.

Control-session abort revokes queued, unmatched, and claimed endpoints and wakes pending stream I/O.
The relay then tears down the opposite side. This joins source and destination control lifetimes
without relying on a final RPC message.

Individual endpoints can also be reset without affecting siblings:

```rust
stream.reset("attachment detached");
fd.reset("owner fenced");
```

Normal `AsyncWrite::shutdown()` is a write-half close, not a reset. Dropping the final endpoint closes
that local endpoint normally.

## UDS and SCM_RIGHTS

For a channel created specifically for one UDS call, `UdsTransport::open_byte_channel()` is the fast
path:

```text
socketpair
├── local ByteChannelFd retained by caller
└── attachment FD sent directly with SCM_RIGHTS
```

There is no userspace byte pump. The UDS codec validates that the descriptor is a connected
`SOCK_STREAM`, applies close-on-exec on receive, and consumes its frame-local descriptor index once.
`attach_byte_channel()` provides the same control binding for an existing socketpair.

When a channel received from one peer is re-encoded to another UDS peer, the current implementation
uses the safe universal relay path:

```text
source FD/stream ↔ bounded pump ↔ new socketpair attachment FD
```

This preserves joined control-session lifetime and normal close behavior.

### Possible UDS forwarding optimizations

A direct received-FD-to-outgoing-SCM_RIGHTS handoff avoids the pump, but its lifetime semantics must
be explicit:

1. **Transfer lifetime to the destination session.** This is cheap, but source-control failure no
   longer guarantees local revocation after handoff. It does not satisfy the default joined-lifetime
   rule.
2. **Retain a duplicate as a source revocation handle.** `shutdown(SHUT_RDWR)` can revoke every
   duplicate, but retaining a duplicate also keeps the socket endpoint open and can suppress normal
   EOF unless there is an explicit completion/release protocol.
3. **Track an explicit forwarding lease.** A destination close acknowledgement could release the
   retained duplicate. This adds a control protocol and must tolerate lost acknowledgements and
   control-session failure.
4. **Optimize the pump rather than ownership.** Linux `splice`, io_uring, or equivalent can reduce
   copying while retaining the same relay and lifetime semantics. This is the safest first
   optimization.

Accordingly, direct SCM_RIGHTS is used for newly-created local channels, while transparent
three-party forwarding pumps by default. A zero-copy forwarding optimization must prove equivalent
half-close, reset, normal EOF, and dual-session revocation behavior before replacing the pump.

## Failure behavior

- **Source control abort:** source endpoint resets; relay closes destination.
- **Destination control abort:** destination endpoint resets; relay closes source.
- **Source never arrives:** setup timeout closes destination.
- **No traffic in either direction:** idle timeout closes both sides.
- **Activity in one direction only:** relay remains alive.
- **Read/write failure:** relay task drops both endpoints.
- **Outgoing queue exhausted:** encoding fails without consuming the attachment.
- **Relay limit exhausted:** encoding fails without consuming the attachment.
- **Duplicate reference or stream:** protocol error.
- **Session or version mismatch:** protocol error.

## Test coverage

The Rust tests cover:

- either arrival order;
- duplicate and single-use enforcement;
- setup, pending, total-channel, outgoing-queue, and relay limits;
- setup and whole-channel idle timeout;
- one-way traffic keeping a relay alive;
- session-wide and per-channel reset;
- canonical stream headers;
- typed channels over framed control streams;
- a three-party proxied stub forwarding a channel without application involvement;
- typed UDS descriptor transfer;
- three-party UDS proxying through the safe pump path;
- connected-stream validation and peer shutdown.
