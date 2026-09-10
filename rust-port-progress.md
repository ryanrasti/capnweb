# Rust Port Progress

## Status

- Branch: `rust-port`
- Package: `capnweb-rs` (library name: `capnweb`)
- Strict JS line parity: 7,407 runtime lines preserved
- Tests: 157 translated JS cases, 8 eager-promise, 10 RPC-runtime, 7 public-facade, and 9 generated-binding tests passing

## Complete

- Core values, serialization, RPC, promises, pipelining, maps, errors, disposal, limits, and flow control
- Stable generated-binding facade
- HTTP batch, WebSocket, and UDS/SCM_RIGHTS transports
- Transport abort and deserialization hardening tests
- Native `Fd` values with real bidirectional SCM_RIGHTS coverage
- Focused eager-promise polling, ordering, cancellation, panic, and settlement tests
- Portable core JavaScript test coverage

## Remaining

- Deferred special types: Fetch, Blob, streams, Date, and typed arrays
- Callback-order and accounting audits
- Decide executor-shutdown settlement behavior for pending eager promises
- Stronger mechanical adjacency checking
- Insertion-ordered object representation for exact key-order parity

MessagePort is intentionally omitted as JavaScript-specific.
