# Rust Port Progress

## Status

- Branch: `rust-port`
- Package: `capnweb-rs` (library name: `capnweb`)
- Strict JS line parity: 7,407 runtime lines preserved
- Tests: 157 JS-suite, 8 RPC-runtime, 7 public-facade, and 9 generated-binding tests passing

## Complete

- Core values, serialization, RPC, promises, pipelining, maps, errors, disposal, limits, and flow control
- Stable generated-binding facade
- HTTP batch and WebSocket transports
- Transport abort and deserialization hardening tests
- Focused eager-promise polling, ordering, cancellation, panic, and settlement tests
- Portable core JavaScript test coverage

## Remaining

- Deferred special types: Fetch, Blob, streams, Date, and typed arrays
- Malformed-message, late-resolution, callback-order, and accounting audits
- Decide executor-shutdown settlement behavior for pending eager promises
- Stronger mechanical adjacency checking
- Insertion-ordered object representation for exact key-order parity

MessagePort is intentionally omitted as JavaScript-specific.
