# Cap'n Web for Rust

An idiomatic Rust implementation of the Cap'n Web RPC protocol.

The implementation is currently being designed. See [rust-port.md](rust-port.md) for the design
proposal and [implementation-plan.md](implementation-plan.md) for the staged implementation plan.
The [kitchen-sink design fixture](examples/kitchen-sink/) pairs a broad TypeScript contract with the
intended hand-written Rust equivalent.

## Development

The crate uses the stable Rust toolchain and Rust 2024 edition.

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```
