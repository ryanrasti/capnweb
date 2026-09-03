# Cap'n Web for Rust

An idiomatic Rust implementation of the Cap'n Web RPC protocol.

The current v0 spike implements dynamic values, immediate generated-style stub methods, target
dispatch macros, import/export tables, promise pipelining, and in-memory, newline-delimited I/O, and
WebSocket transports. It is deliberately incomplete and not ready for production use.

See [JS-PARITY.md](JS-PARITY.md) for the exact translated surface and documented Rust differences,
[rust-port.md](rust-port.md) for the original design proposal, and
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
