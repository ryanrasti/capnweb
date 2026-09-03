// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import { describe, expect, test } from "vitest";

import { emitRustSchema } from "../src/emit-rust.js";
import type { RustSchema } from "../src/schema.js";

/** Executable specifications for semantics that have not landed yet. */
describe("remaining code-generation semantics", () => {
  test.skip("emits a discriminated object union as a Rust enum", () => {
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
    expect(output).toContain("Created { id: String }");
    expect(output).toContain("Deleted { id: String }");
    expect(output).toContain('fields.get("kind")');
  });

  test.skip("emits a wire-distinguishable primitive union as a Rust enum", () => {
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

  test.skip("wraps a multi-branch nullable union in Option", () => {
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

  test.skip("emits callable Deref syntax for resolved and pending functions", () => {
    let schema = functionSchema();
    let output = emitRustSchema(schema);
    expect(output).toContain("impl ::std::ops::Deref for Formatter");
    expect(output).toContain("impl ::std::ops::Deref for PendingFormatter");
    expect(output).toContain("dyn Fn(String) -> ::capnweb::TypedRpcPromise<String>");
  });

  test.skip("emits conversion from compatible local Rust closures", () => {
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

  test.skip("emits source-located nested traversal diagnostics", () => {
    // The eventual diagnostic assertion deliberately includes both location
    // and traversal path; a generic unsupported-type message is insufficient.
    let diagnostic = "api.ts:8:17: Api.call argument 2.options.callback: unsupported mapped type";
    expect(diagnostic).toMatch(/^api\.ts:\d+:\d+: Api\.call argument 2\.options\.callback:/);
  });

  test.skip.each([
    "unconstrained generic",
    "conditional type",
    "mapped type",
    "arbitrary intersection",
    "symbol-keyed property",
  ])("diagnoses unsupported %s instead of guessing", (construct) => {
    let diagnostic = `api.ts:1:1: unsupported ${construct}`;
    expect(diagnostic).toContain(`unsupported ${construct}`);
    expect(diagnostic).toContain("api.ts:1:1");
  });

  test.skip("emits target signatures for compile-time implementation validation", () => {
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

  test.skip("formats generated output with rustfmt when available", () => {
    let output = emitRustSchema(functionSchema());
    // This assertion becomes a rustfmt subprocess/check once formatting is
    // part of the CLI rather than merely compatible for the checked fixture.
    expect(output).toContain("// formatted by rustfmt");
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
