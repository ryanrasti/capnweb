// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import { describe, expect, test } from "vitest";

import { emitRustSchema } from "../src/emit-rust.js";
import type { RustSchema } from "../src/schema.js";

describe("emitRustSchema", () => {
  test("emits objects, targets, functions, and exact wire names", () => {
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
              wireName: "currentUser",
              returns: { kind: "named", id: "User" },
            },
            {
              kind: "method",
              wireName: "formatValue",
              parameters: [
                {
                  sourceName: "type",
                  optional: false,
                  type: { kind: "string" },
                },
                {
                  sourceName: "formatter",
                  optional: true,
                  type: { kind: "named", id: "Formatter" },
                },
              ],
              returns: { kind: "nullable", inner: { kind: "string" } },
            },
          ],
        },
        {
          kind: "object",
          id: "User",
          sourceName: "User",
          fields: [
            {
              wireName: "displayName",
              optional: false,
              readonly: false,
              type: { kind: "string" },
            },
            {
              wireName: "nickname",
              optional: true,
              readonly: false,
              type: { kind: "nullable", inner: { kind: "string" } },
            },
          ],
        },
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

    let output = emitRustSchema(schema);
    expect(output).toMatch(/^\/\/ Schema SHA-256: [0-9a-f]{64}$/m);
    expect(output).not.toContain("rpc_schema!");
    expect(output).toContain("pub struct Api {");
    expect(output).toContain("impl ::capnweb::RpcInterfaceDescriptor for Api {");
    expect(output).toContain('"currentUser",');
    expect(output).toContain("].contains(&wire_name)");
    expect(output).toContain("pub fn current_user(&self) -> PendingUser {");
    expect(output).toMatch(
      /pub fn format_value\(\s*&self,\s*r#type: String,\s*formatter: Option<Formatter>,/
    );
    expect(output).toContain('vec!["formatValue".to_owned()]');
    expect(output).toContain("pub struct User {");
    expect(output).toContain("pub display_name: String,");
    expect(output).toContain("pub nickname: Option<String>,");
    expect(output).toMatch(/fields\s+\.remove\("displayName"\)/);
    expect(output).toContain("pub struct PendingUser {");
    expect(output).toContain("pub struct Formatter {");
    expect(output).toMatch(/impl Formatter \{[\s\S]*?pub fn call[\s\S]*?self\.inner\.call\(\s*vec!\[\]/);
  });

  test("emits string literal unions as ordinary Rust enums", () => {
    let schema: RustSchema = {
      version: 1,
      roots: [],
      declarations: [
        {
          kind: "union",
          id: "Status",
          sourceName: "Status",
          nullable: false,
          branches: [
            { kind: "literal", value: "in-progress" },
            { kind: "literal", value: "done" },
          ],
        },
      ],
    };
    let output = emitRustSchema(schema);
    expect(output).toContain("pub enum Status {");
    expect(output).toContain("InProgress,");
    expect(output).toContain('"in-progress" => Ok(Self::InProgress)');
    expect(output).toContain('Self::Done => "done"');
    expect(output).not.toContain("#[capnweb");
  });

  test("changes the schema hash when the schema changes", () => {
    let first: RustSchema = { version: 1, roots: [], declarations: [] };
    let second: RustSchema = { version: 1, roots: ["Missing"], declarations: [] };
    let hash = (output: string) => output.match(/Schema SHA-256: ([0-9a-f]+)/)?.[1];
    expect(hash(emitRustSchema(first))).not.toBe(hash(emitRustSchema(second)));
  });

  test("rejects dangling named references", () => {
    let schema: RustSchema = {
      version: 1,
      roots: ["Api"],
      declarations: [
        {
          kind: "object",
          id: "Api",
          sourceName: "Api",
          fields: [
            {
              wireName: "missing",
              optional: false,
              readonly: false,
              type: { kind: "named", id: "Missing" },
            },
          ],
        },
      ],
    };
    expect(() => emitRustSchema(schema)).toThrow(
      "Schema references missing declaration Missing"
    );
  });
});
