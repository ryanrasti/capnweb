# Kitchen-sink binding sketch

[`api.d.ts`](api.d.ts) is a hand-written TypeScript contract covering the initial Rust port's
intended value, capability, callback, promise, and stream surface.

[`equivalent.rs`](equivalent.rs) is a hand-written sketch of what generated Rust bindings should
look like. It intentionally does not compile yet: the runtime types and procedural macros it names
have not been implemented. Keeping it below `examples/kitchen-sink/` prevents Cargo from treating it
as a runnable top-level example.

The pair is a design fixture. Once code generation exists, `equivalent.rs` can become a golden-file
test and a separate runnable client/server example can implement the generated traits.
