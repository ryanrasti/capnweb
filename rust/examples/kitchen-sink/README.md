# Kitchen-sink generated bindings

[`api.d.ts`](api.d.ts) is a broad TypeScript contract covering values, capabilities, callbacks,
promises, streams, and browser host types.

[`generated.rs`](generated.rs) is ordinary Rust emitted by `capnweb-rust-codegen` and compiled by
`rust/tests/generated_kitchen_sink.rs`. [`equivalent.rs`](equivalent.rs) remains the earlier
hand-written design sketch.

The v0 generator uses typed Rust representations for its supported structural surface, including
POJOs, arrays, finite tuples, records, capabilities, functions, and `Uint8Array` bytes. Omitted host
and stream wrappers remain usable as `capnweb::CapnwebValue` rather than preventing the complete API
from being generated.

Regenerate with:

```sh
node packages/capnweb-rust-codegen/dist/cli.mjs \
  --project rust/examples/kitchen-sink/tsconfig.json \
  --input rust/examples/kitchen-sink/api.d.ts \
  --root KitchenSinkApi \
  --out rust/examples/kitchen-sink/generated.rs
```
