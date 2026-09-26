# Request as an FD

> The current generic channel API, transparent forwarding model, lifecycle rules, and UDS
> optimization analysis are specified in [byte-channels.md](byte-channels.md). This document retains
> the original HTTP/celld motivation and raw descriptor design notes.

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

The Rust port now has both the local descriptor transport and a generic,
single-use byte-channel seam:

- `CapnwebValue::Custom` with checked dynamic values;
- transport-defined `CustomTypeRegistry` and `CustomTypeHandler` hooks;
- typed `ByteChannel` arguments and results in generated interfaces;
- exactly-once channel claiming across cloned Rust handles;
- session-bound claimed streams and FDs with per-channel reset;
- control-session abort revoking queued, unmatched, and already-claimed channels;
- `ChannelReference` with protocol version, session ID, and never-reused channel ID;
- bounded `ChannelRendezvous` pairing references and streams in either order;
- `ByteChannelSession` for bounded outgoing queues, periodic expiry, and abort;
- transparent bounded channel relays when proxied capabilities re-encode foreign attachments;
- setup and whole-channel idle timeouts with one-way activity support;
- one canonical fixed-size binary header for independent network streams;
- transport-facing `ByteChannelTransportAdapter` and `ByteChannelTypeHandler`;
- an asynchronous `SOCK_SEQPACKET` `UdsTransport`;
- direct `ByteChannel` transfer as connected `SOCK_STREAM` descriptors;
- `SCM_RIGHTS` send/receive with close-on-exec and descriptor-type validation;
- deterministic wakeup of pending UDS receives and byte-channel I/O on shutdown;
- `UdsTransport::open_byte_channel()` for control-bound local socketpairs;
- message and descriptor bounds;
- custom-value isolation and real typed FD RPC round-trip tests.

The older raw `OwnedFd` custom value remains available for generic descriptor
transfer and retains copying semantics: clones share an `Arc<OwnedFd>`, while
each successful SCM_RIGHTS send creates a descriptor in the receiver. In
contrast, `ByteChannel` is consumed by its first claim or transport encoding;
a second use is an error.

Remaining Rust work for the celld integration:

1. Confirm construction and startup conventions for inherited control
   descriptors; production will not normally call `UdsTransport::pair()`.
2. Extend malformed-marker, multiple/nested FD, ancillary truncation,
   unreferenced-descriptor, cancellation, and FD-leak tests.
3. Confirm or configure the control-frame size contract. HTTP and terminal data
   are externalized, but other Cap'n Web values can still exceed a practical
   seqpacket payload.
4. Integrate a real QUIC/iroh adapter with `ByteChannelSession` and publish
   interoperability fixtures for the canonical stream-header format.

The JavaScript custom transport still needs equivalent custom-value hooks and
an opaque host descriptor supplied by celld.

## Keep the Cap'n Web boundary simple

Cap'n Web understands generic transport-defined custom values, not FDs,
`Request`, `Response`, HTTP exchanges, socketpairs, or celld handlers. The UDS
transport registers `OwnedFd` as one custom value and owns its message-local
SCM_RIGHTS sidecar state.

The RPC system gives a custom transport a partially encoded value tree and the
message-local codec created by that transport. HTTP batch and WebSocket
transports do not register the FD handler, so attempting to send that custom
value through them fails without introducing FD logic into those transports.

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
3. uses the registered UDS handler to replace valid `["fd", index]` markers
   with a checked custom value containing `OwnedFd`;
4. returns the value and message codec to the RPC machinery.

This is transport encoding in the same spirit as keeping bytes raw instead of
base64 encoding them. HTTP batch and WebSocket transports never see an
unencoded FD and reject one if asked to send it.

The transport opts into custom values by creating a message codec with its
registered handlers. No FD-specific encoding level or transport type check is
needed in the RPC core.

## Rust FD value

The UDS handler stores `OwnedFd` inside a checked `CustomValue`; a bare numeric
descriptor never reaches application code. The message context holds outgoing
`Arc<OwnedFd>` values until `sendmsg()` completes and consumes each received
descriptor at most once while decoding its marker.

`CapnwebValue` remains clonable, so the current behavior is capability
copying: clones refer to the same local open file description and each
successful SCM_RIGHTS send creates a descriptor in the receiving process.
This behavior must be documented and covered by repeated-send and failed-send
leak tests.

Receive uses `MSG_CMSG_CLOEXEC` and immediately places descriptors in
`OwnedFd`. Celld or the remote runtime may subsequently validate `SO_TYPE` and
expected socket state; semantic FD validation does not belong in Cap'n Web.

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
