# Request as an FD

## Goal

Let a local Cap'n Web session transfer file descriptors through celld's framed
AF_UNIX transport. Celld can then map an HTTP exchange to a socketpair and pass
one connected endpoint to the colocated libkrun runtime.

```text
Cap'n Web RPC values/control
  └─ framed AF_UNIX transport + SCM_RIGHTS

socketpair FD
  └─ HTTP request, HTTP response, or upgraded WebSocket bytes
```

Possession of the descriptor is the capability. There is no callback URL,
rendezvous token, named Unix socket, or application-data multiplexing inside
Cap'n Web.

The FD is the local implementation of broader channel attachments, not the
public request-transport abstraction. The chosen local design has two branded
host types over the same generic FD transfer:

```text
CapabilityChannel -> SOCK_SEQPACKET endpoint -> nested framed session
RequestChannel    -> SOCK_STREAM endpoint    -> HTTP exchange
```

Capability-channel endpoints are recursively transferable. A bootstrap
session can create a pair, inherit one end into a child, return the other end
to JavaScript, and then close; the new peer-to-peer session carries subsequent
control messages without protocol proxying through the bootstrap service.
This transfers a session endpoint, not existing Cap'n Web import/export IDs,
which remain scoped to their original session unless explicitly re-exported.

A future cross-machine session can provide equivalent reliable bidirectional
semantics using QUIC streams, independent authenticated TCP connections, or a
multiplexed network transport. The HTTP codec should depend only on the duplex
stream contract; only the local transport depends on `SCM_RIGHTS` and `Fd`.

## Higher-level session abstraction

The capability-provider layer should expose framed control messages plus
independent duplex streams:

```text
CapabilitySession
├─ send/receive control frames
└─ open/accept reliable duplex streams
```

The required duplex semantics are ordered bytes, bounded backpressure,
write-half-close, reset/cancellation, and session-scoped lifetime. How a stream
reference crosses a frame is transport-specific:

```text
local AF_UNIX session   -> socketpair endpoint transferred as Fd
cross-machine session   -> QUIC stream ID, authenticated TCP stream, etc.
```

Two configured codecs sit above generic descriptor transfer. The capability-
channel codec creates or adopts a seqpacket endpoint and exposes it as another
framed session. The request-channel codec creates a stream, runs the native
HTTP client or server on its local endpoint, and places the transport's remote
stream attachment in the control frame. This keeps request forwarding and
application dispatch independent of whether the peers are processes on one
node or machines on a network.

## Current Rust status

The Rust port now has the basic local descriptor transport:

- `CapnwebValue::Fd` and an owning, clonable `Fd` wrapper;
- structured-clonable preservation of FD values;
- an asynchronous `SOCK_SEQPACKET` `UdsTransport`;
- `SCM_RIGHTS` send/receive with close-on-exec;
- `["fd", index]` transport encoding;
- message and descriptor bounds;
- a real RPC round-trip test returning a socket FD.

Remaining Rust work for the celld integration:

1. Expose typed `IntoCapnwebValue` and `FromCapnwebValue` conversions if the
   generated local-session API names `Fd` directly.
2. Add construction from an inherited or connected control descriptor; the
   production session will not normally call `UdsTransport::pair()` itself.
3. Decide and document whether cloning an `Fd` means duplicate-capability
   semantics or whether the first successful send consumes shared transfer
   state. The current `Arc<OwnedFd>` implements duplicate-capability semantics.
4. Make peer shutdown deterministic. A capability session must wake pending
   receive calls when its peer exits rather than depending only on an explicit
   RPC abort.
5. Test multiple and nested FDs, malformed or forged indexes, ancillary
   truncation, unreferenced descriptors, peer exit, cancellation, and FD leak
   counts.
6. Confirm the control-frame size contract. HTTP data will be externalized,
   but other Cap'n Web values can still exceed a practical seqpacket payload.
7. Add typed local control methods only after celld's generic session seam is
   fixed; no HTTP or consumer-specific types belong in capnweb-rs.

The JavaScript custom transport remains responsible for preserving its opaque
host FD value and delegating frames to celld. Rust continues to see only the
generic `Fd` variant.

## Keep the Cap'n Web boundary simple

Cap'n Web should understand only a new generic `Fd` value, analogous to its
special handling for bytes. It should not know what a `Request`, `Response`,
HTTP exchange, socketpair, or celld handler is.

The RPC system already gives a custom transport a partially encoded value tree
through `TransportMessage::Value(CapnwebValue)`. Keep that boundary. Do not add
an attachment list to `TransportMessage` and do not make the core RPC session
manage ancillary data.

Add an FD variant to the value tree:

```rust
pub enum CapnwebValue {
    // existing variants
    Bytes(Vec<u8>),
    Fd(Fd),
}
```

At the custom local encoding level, RPC serialization leaves both bytes and
FDs unencoded. Only an FD-capable UDS transport accepts a value tree containing
`Fd`.

## UDS transport encoding

When the transport sends a value, it walks the already-jsonified tree,
extracts each FD, and replaces it with a reserved marker:

```json
["fd", 0]
```

The resulting celld frame contains:

```text
payload:      ... ["fd", 0] ...
SCM_RIGHTS:   [descriptor 0]
```

The index addresses the SCM_RIGHTS descriptor array attached to that exact
frame. It is not a process-global descriptor number or a session-global ID.

On receive, the transport:

1. receives one framed payload and its ancillary descriptors;
2. parses the JSON-compatible value tree;
3. replaces valid `["fd", index]` markers with `CapnwebValue::Fd`;
4. returns `TransportMessage::Value` to the unchanged RPC machinery.

This is transport encoding in the same spirit as keeping bytes raw instead of
base64 encoding them. HTTP batch and WebSocket transports never see an
unencoded FD and reject one if asked to send it.

A dedicated encoding level such as `jsonCompatibleWithBytesAndFds`, or an
equivalent explicit transport capability, should make that constraint visible
to the serializer. It must not be inferred from a concrete transport type.

## Rust FD value

Rust must expose an owning wrapper around `OwnedFd`, never a bare numeric FD:

```rust
pub struct Fd {
    // private owning/transfer state
}

impl Fd {
    pub fn into_owned_fd(self) -> OwnedFd;
}
```

`CapnwebValue` is currently clonable, while an FD transfer is linear. The
wrapper therefore needs explicit shared transfer state or another rule that
preserves `CapnwebValue` cloning without duplicating ownership accidentally. A
safe initial semantic is:

- one FD value may be transferred once;
- clones refer to the same transferable resource;
- a second extraction or send fails;
- dropping the final untransferred value closes the descriptor;
- a failed send has deterministic ownership and cannot leak the descriptor.

`SCM_RIGHTS` creates a descriptor in the receiving process. Receive with
`MSG_CMSG_CLOEXEC` and immediately place it in `OwnedFd`. Celld or the runtime
may subsequently validate `SO_TYPE` and expected socket state; semantic FD
validation does not belong in Cap'n Web.

## JavaScript FD value

JavaScript gets an opaque, branded FD object supplied by celld. User code does
not need the numeric descriptor and must not be able to forge an FD by
constructing `["fd", 0]`.

At the local custom encoding level, Cap'n Web serialization recognizes this
host value and leaves it as an FD value in the partially encoded tree. The UDS
transport asks celld's host integration to detach the descriptor and append it
to the outgoing frame's SCM_RIGHTS array.

The reverse path asks celld to wrap a received ancillary descriptor as the
opaque JavaScript FD value before the RPC evaluator sees it.

This type can remain an internal celld extension initially. A public Cap'n Web
API can wait until transfer and disposal semantics are settled.

## Celld maps HTTP to the FD

Celld, not Cap'n Web, maps between HTTP and connected descriptors.

### Guest to JavaScript

1. The guest starts a TSI connection.
2. The VMM asks the colocated JavaScript router to connect the destination.
3. Celld creates `socketpair(AF_UNIX, SOCK_STREAM)`.
4. Celld runs its normal HTTP ingress handling on one endpoint, attached to the
   selected JS handler.
5. The other endpoint is returned through Cap'n Web as `Fd`.
6. The Rust VMM extracts its `OwnedFd` and installs it as the TSI connection.
7. HTTP request, response, and upgraded WebSocket bytes stay on the socketpair.

```text
guest <-> TSI <-> remote FD <-> retained FD <-> celld HTTP ingress <-> JS
```

### JavaScript to guest

1. `getTcpPort(port).fetch(request)` creates a socketpair.
2. Celld runs an HTTP client for the Request on one endpoint.
3. Celld sends the other endpoint to the VMM in an
   `injectAccepted(port, fd)` Cap'n Web call.
4. Libkrun queues the received FD on the matching guest TSI listener.
5. The guest accepts it and serves HTTP.
6. Celld resolves the JS Response from its retained endpoint.

```text
JS <-> celld HTTP client <-> retained FD <-> remote FD <-> TSI <-> guest
```

The Cap'n Web call establishes the exchange and reports setup failure. It does
not carry the HTTP response body; the socketpair does.

Ordinary celld request forwarding remains unchanged. Once celld has parsed an
FD-backed connection into a `Request`, service binding and Container DO
forwarding continue to pass that Request through the existing request path.

## Framed celld transport

Celld's local frame format needs to carry:

```text
one Cap'n Web value payload
zero or more SCM_RIGHTS descriptors
```

The framing implementation, rather than Cap'n Web RPC core, is responsible for
associating ancillary data with the correct frame. This matters on
`SOCK_STREAM`, where one receive can split or combine writes and SCM_RIGHTS is
associated with a byte position rather than an abstract application message.

The custom Cap'n Web transport is a thin adapter over this framed API:

```text
send(value):
  extract Fd values
  encode ["fd", index]
  frame.send(encodedValue, fds)

receive():
  frame.receive() -> (encodedValue, fds)
  revive ["fd", index]
  return value
```

## Node locality

FD values are accepted only by the local AF_UNIX transport. Node locality is
therefore structural:

- an FD cannot be sent through HTTP batch or WebSocket transports;
- no rendezvous state enters celld fleet metadata;
- no token or path can be forwarded to another node;
- losing the local session closes pending and active socketpairs;
- relocation creates a new VMM session and new connections.

The local transport should itself be bound to the intended runtime process,
preferably using an inherited socket or authenticated peer credentials.

## Limits and errors

Bound at least:

- FDs per frame;
- FDs in flight per session;
- active socketpairs per container;
- frame and nesting size before walking attachment markers;
- setup calls waiting for guest listen/connect completion.

Treat missing, duplicate, out-of-range, wrong-kind, or unreferenced ancillary
descriptors as protocol errors, closing every descriptor that was received.
Cancellation must close the retained endpoint and any FD not yet installed by
the VMM.

## Tests

Cover:

- JS-to-Rust and Rust-to-JS FD transfer;
- bidirectional bytes through a transferred socketpair;
- a transferred seqpacket endpoint used as a nested RPC session;
- third-party channel handoff and bootstrap-session shutdown;
- multiple FDs in one value and consecutive frames;
- nested FD values and repeated references;
- split/coalesced `SOCK_STREAM` frame reads;
- `CLOEXEC` on received descriptors;
- rejection by HTTP batch and WebSocket transports;
- forged, missing, duplicate, and out-of-range markers;
- unreferenced ancillary descriptors;
- failure before and during transport send;
- cancellation and session shutdown without FD leaks;
- concurrent HTTP requests on independent socketpairs;
- streamed request and response bodies;
- WebSocket upgrade over a socketpair;
- guest connect and guest accept through injected FDs.

## Implementation order

1. Add the owning `Fd` value to Rust and the branded host value to JS.
2. Add a local FD-capable custom encoding level.
3. Extend celld's framed AF_UNIX transport with SCM_RIGHTS arrays.
4. Implement the thin Cap'n Web UDS transport encoder/decoder.
5. Add malformed-frame, transfer-ownership, and leak tests.
6. Feed received FDs into celld's existing HTTP server/client paths.
7. Add VMM connect and `injectAccepted` control calls.
8. Replace the current loopback TSI broker with direct FD-backed streams.

The intended core change is deliberately small: `Fd` survives Cap'n Web's
partial encoding just as bytes can, and the one transport capable of carrying
it performs the final marker encoding and SCM_RIGHTS transfer.
