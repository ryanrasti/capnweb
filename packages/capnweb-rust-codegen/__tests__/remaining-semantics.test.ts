// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import { describe, expect, test } from "vitest";

import { emitRustSchema } from "../src/emit-rust.js";
import { formatGeneratedRust } from "../src/output.js";
import type { RustSchema } from "../src/schema.js";

/** Executable specifications for semantics that have not landed yet. */
describe("remaining code-generation semantics", () => {
  test("emits a discriminated object union as a Rust enum", () => {
    let schema: RustSchema = {
      version: 1,
      roots: [],
      declarations: [
        {
          kind: "union",
          id: "Event",
          sourceName: "Event",
          nullable: false,
          branches: [
            { kind: "named", id: "Created" },
            { kind: "named", id: "Deleted" },
          ],
        },
        {
          kind: "object",
          id: "Created",
          sourceName: "Created",
          fields: [
            {
              wireName: "kind",
              optional: false,
              readonly: false,
              type: { kind: "literal", value: "created" },
            },
            {
              wireName: "id",
              optional: false,
              readonly: false,
              type: { kind: "string" },
            },
          ],
        },
        {
          kind: "object",
          id: "Deleted",
          sourceName: "Deleted",
          fields: [
            {
              wireName: "kind",
              optional: false,
              readonly: false,
              type: { kind: "literal", value: "deleted" },
            },
            {
              wireName: "id",
              optional: false,
              readonly: false,
              type: { kind: "string" },
            },
          ],
        },
      ],
    };

    let output = emitRustSchema(schema);
    expect(output).toContain("pub enum Event {");
    expect(output).toMatch(/Created \{\s*id: String\s*\}/);
    expect(output).toMatch(/Deleted \{\s*id: String\s*\}/);
    expect(output).toContain('fields.get("kind")');
  });

  test("emits a wire-distinguishable primitive union as a Rust enum", () => {
    let schema: RustSchema = {
      version: 1,
      roots: [],
      declarations: [
        {
          kind: "union",
          id: "Identifier",
          sourceName: "Identifier",
          nullable: false,
          branches: [{ kind: "string" }, { kind: "number" }],
        },
      ],
    };

    let output = emitRustSchema(schema);
    expect(output).toContain("pub enum Identifier {");
    expect(output).toContain("String(String)");
    expect(output).toContain("Number(f64)");
  });

  test("wraps a multi-branch nullable union in Option", () => {
    let schema: RustSchema = {
      version: 1,
      roots: [],
      declarations: [
        {
          kind: "union",
          id: "Identifier",
          sourceName: "Identifier",
          nullable: true,
          branches: [{ kind: "string" }, { kind: "number" }],
        },
      ],
    };

    let output = emitRustSchema(schema);
    expect(output).toContain("pub type Identifier = Option<IdentifierValue>;");
  });

  test("emits callable Deref syntax for resolved and pending functions", () => {
    let schema = functionSchema();
    let output = emitRustSchema(schema);
    expect(output).toContain("impl ::std::ops::Deref for Formatter");
    expect(output).toContain("impl ::std::ops::Deref for PendingFormatter");
    expect(output).toContain("dyn Fn(String) -> ::capnweb::TypedRpcPromise<String>");
  });

  test("emits conversion from compatible local Rust closures", () => {
    let output = emitRustSchema(functionSchema());
    expect(output).toContain("impl<F, Fut> From<F> for Formatter");
    expect(output).toContain("F: Fn(String) -> Fut + Send + Sync + 'static");
  });

  test("emits pending capability fields as directly callable providers", () => {
    let schema: RustSchema = {
      version: 1,
      roots: [],
      declarations: [
        {
          kind: "object",
          id: "Directory",
          sourceName: "Directory",
          fields: [
            {
              wireName: "primaryCounter",
              optional: false,
              readonly: false,
              type: { kind: "named", id: "Counter" },
            },
          ],
        },
        {
          kind: "target",
          id: "Counter",
          sourceName: "Counter",
          members: [
            {
              kind: "method",
              wireName: "increment",
              parameters: [],
              returns: { kind: "number" },
            },
          ],
        },
      ],
    };

    let output = emitRustSchema(schema);
    expect(output).toContain("pub primary_counter: PendingCounter");
    expect(output).toContain('PendingCounter::new(promise.get(vec!["primaryCounter"');
  });

  test("maps variadic tuples to CapnwebValue", () => {
    let schema: RustSchema = {
      version: 1,
      roots: [],
      declarations: [
        {
          kind: "object",
          id: "Values",
          sourceName: "Values",
          fields: [
            {
              wireName: "tuple",
              optional: false,
              readonly: false,
              type: {
                kind: "tuple",
                elements: [{ optional: false, type: { kind: "string" } }],
                rest: { kind: "number" },
              },
            },
          ],
        },
      ],
    };
    expect(emitRustSchema(schema)).toContain("pub tuple: ::capnweb::CapnwebValue");
  });

  test("maps omitted streams and special values to CapnwebValue", () => {
    let schema: RustSchema = {
      version: 1,
      roots: ["Api"],
      declarations: [
        {
          kind: "target",
          id: "Api",
          sourceName: "Api",
          members: [
            {
              kind: "method",
              wireName: "omitted",
              parameters: [
                { sourceName: "date", optional: false, type: { kind: "builtin", name: "date" } },
                { sourceName: "error", optional: false, type: { kind: "builtin", name: "error" } },
                {
                  sourceName: "stream",
                  optional: false,
                  type: { kind: "readableStream", chunk: { kind: "string" } },
                },
              ],
              returns: { kind: "writableStream", chunk: { kind: "string" } },
            },
          ],
        },
      ],
    };
    let output = emitRustSchema(schema);
    expect(output).toContain(
      "date: ::capnweb::CapnwebValue,\n        error: ::capnweb::CapnwebValue,\n        stream: ::capnweb::CapnwebValue,"
    );
    expect(output).toContain("-> ::capnweb::TypedRpcPromise<::capnweb::CapnwebValue>");
    expect(output).not.toContain("RpcReadStream");
    expect(output).not.toContain("RemoteError");
  });

  test("emits target signatures for compile-time implementation validation", () => {
    let schema: RustSchema = {
      version: 1,
      roots: ["Api"],
      declarations: [
        {
          kind: "target",
          id: "Api",
          sourceName: "Api",
          members: [
            {
              kind: "getter",
              wireName: "version",
              returns: { kind: "string" },
            },
            {
              kind: "method",
              wireName: "add",
              parameters: [
                {
                  sourceName: "value",
                  optional: false,
                  type: { kind: "number" },
                },
              ],
              returns: { kind: "number" },
            },
          ],
        },
      ],
    };
    let output = emitRustSchema(schema);
    expect(output).toContain("type VersionResult = String");
    expect(output).toContain("type AddArguments = (f64,)");
    expect(output).toContain("type AddResult = f64");
  });

  test("formats generated output with rustfmt when available", async () => {
    let unformatted = "pub fn example( )->i32{1}\n";
    let formatted = await formatGeneratedRust(unformatted);
    // Environments without rustfmt deliberately retain deterministic emitter output.
    if (formatted === unformatted) return;
    expect(formatted).toBe("pub fn example() -> i32 {\n    1\n}\n");
  });
});

function functionSchema(): RustSchema {
  return {
    version: 1,
    roots: [],
    declarations: [
      {
        kind: "function",
        id: "Formatter",
        sourceName: "Formatter",
        parameters: [
          {
            sourceName: "value",
            optional: false,
            type: { kind: "string" },
          },
        ],
        returns: { kind: "string" },
      },
    ],
  };
}
