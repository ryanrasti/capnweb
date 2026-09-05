# Rust code generator plan

## Goal

Generate deterministic Rust bindings from exported TypeScript Cap'n Web contracts. The TypeScript
compiler is the source of semantic truth; generated bindings follow the mappings in
[`../../rust/rust-types.md`](../../rust/rust-types.md).

This v0 package deliberately follows the shape of `capnweb-validate` without sharing its internal
normal form. Validation and Rust generation need different information and have different fallback
rules. Stable semantic helpers can be extracted after both implementations have real coverage.

## Architecture

The generator is an explicit semantic compilation pipeline:

```text
.ts / .d.ts + tsconfig
        |
        v
TypeScript Program and TypeChecker
        |
        v
discover configured exported roots
        |
        v
versioned RustSchema normal form
        |
        +--> JSON diagnostics/debug output
        |
        v
Rust source emitter
        |
        v
rustfmt / --check
```

The CLI is explicit build tooling. Generated Rust is checked into source control; Cargo `build.rs`
does not invoke Node in v0.

## Package layout

- `src/context.ts`: tsconfig loading and `Program`/`TypeChecker` ownership.
- `src/discover.ts`: input module and exported root resolution.
- `src/schema.ts`: serializable, Rust-oriented normal form.
- `src/type-introspector.ts`: semantic TypeScript-to-schema lowering.
- `src/names.ts`: deterministic Rust identifier generation.
- `src/emit-rust.ts`: schema-to-Rust rendering.
- `src/cli.ts`: argument parsing, file output, and `--check`.
- `__tests__/`: schema, rendering, diagnostic, and golden tests.

## Milestones

### 1. Semantic skeleton

- [x] Create the package and versioned schema types.
- [x] Load a real TypeScript project and resolve an exported root.
- [x] Lower primitive references, promises, arrays, tuples, records, objects, targets, functions,
  unions, and selected built-ins into the schema.
- [x] Add focused schema tests using an in-memory TypeScript program.
- [x] Run the kitchen-sink fixture through the schema extractor.

### 2. Rust naming and rendering

- [x] Convert TypeScript identifiers to deterministic Rust identifiers.
- [x] Preserve every exact wire name separately.
- [x] Emit POJO structs and pending POJO views.
- [x] Emit string literal, discriminated object, and wire-distinguishable primitive union enums.
- [x] Emit target descriptors, resolved/pending capabilities, methods, and getters.
- [x] Emit callable resolved/pending function wrappers and local async closure conversion.
- [x] Support explicit `--rustfmt` CLI formatting when `rustfmt` is available.

The emitter writes ordinary Rust structs and impls containing objects, resolved and pending
providers, target methods/getters, conversion implementations, unions, functions, exact wire names,
and mapped Rust types. There is intentionally no intermediate Rust schema macro: the checked-in
output remains visible to rust-analyzer, rustdoc, and normal compiler diagnostics.

### 3. Diagnostics and CLI

- [x] Lower omitted nested types to the dynamic fallback instead of rejecting generation.
- [x] Add `--project`, `--input`, repeatable `--root`, `--out`, and `--check`.
- [x] Add a generated-file banner and schema/source hash.
- [x] Ensure output contains no absolute paths and is deterministic.
- [x] Reject ambiguous overloads and target instance fields.

### 4. End-to-end coverage

- [x] Generate and compile bindings for `rust/examples/kitchen-sink/api.d.ts`.
- [x] Compile generated bindings in Rust tests.
- [ ] Restore Rust-to-Rust and JavaScript-to-Rust interoperability tests using generated APIs.
- [x] Cover typed v0 getters, callbacks, capability fields, and promise pipelining; omitted streams and error values use the tested dynamic fallback.
- [x] Add a CI command that fails when checked-in generated files are stale.

## Semantic test matrix

Every supported semantic claim must have both a schema/emitter test and, when it affects runtime
behavior, a generated Rust integration test. A row marked pending is remaining implementation work,
not silently untested behavior.

Fully covered semantics:

- Project loading and exported roots: `generated-rust.test.ts` and the compiled fixture.
- Root `RpcTarget` enforcement: `semantic-matrix.test.ts`.
- Null, undefined, and optional values: `semantic-matrix.test.ts` and the POJO round trip.
- Known POJOs and exact wire field names: introspector/emitter tests and the POJO round trip.
- Arrays, finite tuples, records, bigint, void, bytes, recursive objects, and dynamic values: generated Rust round trips.
- Omitted TypeScript constructs, streams, and host built-ins: explicit `CapnwebValue` fallback tests.
- Recursive layouts: container indirection is preserved and by-value cycles produce source errors.
- Methods and explicit getters: introspector tests plus generated tests rejecting method/getter kind confusion.
- Targets and `RpcStub<T>` recovery: kitchen-sink assertions and the capability pipeline test.
- Target implementation privacy: introspector rejection and the descriptor helper-hiding test.
- Capability fields in pending POJOs: emitter assertion and the direct pipeline integration test.
- String literal unions: emitter tests and the generated `Status` round trip.
- Discriminated object unions: emitter tests and a generated Rust round trip.
- Wire-distinguishable primitive unions: emitter tests and a generated Rust round trip.
- Functions: resolved and pending wrappers have direct call syntax; local async closures work as callback arguments and as returned, pipeline-called capabilities.
- Rust naming, keywords, and collisions: `names.test.ts` and compiled output.
- Deterministic output, schema hashing, and stale checks: emitter, generated-output, and output tests.

Schema-covered semantics still needing generated Rust runtime tests:

- JavaScript/Rust transport behavior for generated function signatures.

Implementation and tests still pending together:

- target descriptor signatures for compile-time parameter/result/getter validation;
- generated JavaScript/Rust interoperability.

## V0 policy

- Roots are named exports selected explicitly by the caller.
- Root declarations must be Cap'n Web target types.
- `undefined` and `null` normalize to Rust nullability.
- Unsupported or structurally ambiguous unions lower to a dynamic value when safe.
- Omitted generics, conditional/mapped/intersection types, symbol-keyed values, variadic tuples,
  promise-valued parameters, streams, and most host built-ins lower to `CapnwebValue`; overloads
  and remotely traversable target fields remain diagnostics.
- TypeScript source names remain wire names. Rust names are backend metadata, never wire metadata.

## Possible sharing with `capnweb-validate`

Do not share the normal form yet. After the kitchen-sink fixture works, evaluate extracting only
stable checker helpers such as:

- alias and declaration lookup;
- TypeScript library-origin checks;
- tuple and array recognition;
- capability-brand and `RpcTarget` detection;
- recursive type identity.

Any extraction must preserve validator behavior and must not force validator-only concepts such as
`skipValidation` or Workers passthrough methods into the Rust schema.
