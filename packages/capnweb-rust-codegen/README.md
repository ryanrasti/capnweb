# capnweb-rust-codegen

Generate ordinary Rust bindings from a TypeScript Cap'n Web API declaration.

The package is a standalone Node.js CLI. It reads types through the TypeScript compiler API and
does not run Node.js from Cargo or require the JavaScript Cap'n Web runtime at generation time.

## Install

```sh
npm install --save-dev capnweb-rust-codegen typescript
```

## Usage

```sh
npx capnweb-rust-codegen \
  --project tsconfig.json \
  --input src/volume-api.ts \
  --root VolumeApi \
  --out src/generated/volume.rs
```

Select multiple exported roots by repeating `--root`. Use `--schema-out` to inspect the lowered
schema and `--rustfmt` to format output when `rustfmt` is installed.

Checked-in bindings can be verified without rewriting them:

```sh
npx capnweb-rust-codegen \
  --project tsconfig.json \
  --input src/volume-api.ts \
  --root VolumeApi \
  --out src/generated/volume.rs \
  --check
```

The command exits unsuccessfully when the generated file is missing or stale.

## Type policy

The typed v0 surface includes primitives, POJOs, arrays, finite tuples, string-keyed records,
nullable values, selected unions, targets, callbacks, and `Uint8Array` bytes. Omitted TypeScript
constructs generate as `capnweb::CapnwebValue` rather than failing the whole schema.

`Uint8Array` and `Uint8ClampedArray` generate as `capnweb::Bytes`; this keeps wire byte strings
distinct from ordinary `Vec<T>` RPC arrays.

## Development

The project can be built and tested independently of the Cap'n Web monorepo:

```sh
npm install
npm run typecheck
npm test
npm run build
npm pack
```

The monorepo additionally compiles generated Rust fixtures and checks the kitchen-sink schema.
