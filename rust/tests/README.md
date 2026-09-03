# Tests

- `rust_e2e.rs` connects two Rust sessions with the in-memory transport and verifies a pipelined
  argument plus a Rust target returned and called by reference.
- `js_e2e.rs` starts the existing JavaScript Cap'n Web implementation over a newline-delimited
  stdio transport. JavaScript calls Rust with a pipelined argument and Rust calls the JavaScript
  main target over the same session.
- `javascript_suite.rs` loads the handwritten tree under `tests/javascript/`. Every JavaScript or
  TypeScript test file in `__tests__`, `__type-tests__`, and
  `packages/capnweb-validate/__tests__` has its own one-for-one `.test.rs` counterpart. Together,
  they inventory 427 static runtime test declarations. The 15 type-only suites additionally have
  one visible ignored compile-test placeholder each. Cases supported by Rust contain equivalent
  test bodies. Every other case uses `#[ignore = "reason"]` naming the unavailable runtime,
  platform, or type-system feature. Parameterized JavaScript tests have one Rust entry for the
  source declaration rather than one entry per generated parameter.

Check the handwritten parity tree after changing either test suite:

```sh
node rust/scripts/check-js-test-parity.mjs
```

The checker only compares file and test coverage; it never generates Rust source. Ignored parity
bodies deliberately panic if explicitly forced with `cargo test -- --ignored`; they must be
replaced with real assertions when enabled.

Build the JavaScript package before running the full Rust suite:

```sh
npm install
npm run build
cd rust
cargo test
```
