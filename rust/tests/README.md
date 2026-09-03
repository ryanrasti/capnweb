# End-to-end tests

- `rust_e2e.rs` connects two Rust sessions with the in-memory transport and verifies a pipelined
  argument plus a Rust target returned and called by reference.
- `js_e2e.rs` starts the existing JavaScript Cap'n Web implementation over a newline-delimited
  stdio transport. JavaScript calls Rust with a pipelined argument and Rust calls the JavaScript
  main target over the same session.

Build the JavaScript package before running the full Rust suite:

```sh
npm install
npm run build
cd rust
cargo test
```
