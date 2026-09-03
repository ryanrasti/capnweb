// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import { fileURLToPath } from "node:url";
import ts from "typescript";
import { describe, expect, test } from "vitest";

import { resolveExportedTypes } from "../src/discover.js";
import { emitRustSchema } from "../src/emit-rust.js";
import { introspectRustSchema } from "../src/type-introspector.js";

describe("kitchen-sink schema", () => {
  test("discovers the complete reachable API surface", () => {
    let fileName = fileURLToPath(
      new URL("../../../rust/examples/kitchen-sink/api.d.ts", import.meta.url)
    );
    let options: ts.CompilerOptions = {
      lib: ["lib.es2023.d.ts", "lib.dom.d.ts"],
      module: ts.ModuleKind.NodeNext,
      moduleResolution: ts.ModuleResolutionKind.NodeNext,
      skipLibCheck: true,
      strict: true,
      target: ts.ScriptTarget.ES2023,
    };
    let program = ts.createProgram({ rootNames: [fileName], options });
    let checker = program.getTypeChecker();
    let roots = resolveExportedTypes(
      program,
      checker,
      fileName,
      ["KitchenSinkApi"]
    );
    let schema = introspectRustSchema(checker, roots);

    expect(schema.roots).toEqual(["KitchenSinkApi"]);
    let root = schema.declarations.find(
      (declaration) => declaration.id === "KitchenSinkApi"
    );
    expect(root?.kind).toBe("target");
    if (root?.kind !== "target") throw new Error("KitchenSinkApi is not a target");
    expect(root.members).toHaveLength(28);
    let member = (wireName: string) =>
      root.members.find((candidate) => candidate.wireName === wireName);
    expect(member("echoUser")).toEqual({
      kind: "method",
      wireName: "echoUser",
      parameters: [
        {
          sourceName: "value",
          optional: false,
          type: { kind: "named", id: "User" },
        },
      ],
      returns: { kind: "named", id: "User" },
    });
    expect(member("acceptDeferred")).toMatchObject({
      parameters: [
        {
          type: { kind: "promise", value: { kind: "string" } },
        },
      ],
      returns: { kind: "string" },
    });
    expect(member("useCounter")).toMatchObject({
      parameters: [
        { type: { kind: "named", id: "Counter" } },
        { type: { kind: "number" } },
      ],
    });
    expect(member("format")).toMatchObject({
      parameters: [
        { type: { kind: "named", id: "Formatter" } },
        { type: { kind: "string" } },
      ],
    });
    expect(member("watch")).toMatchObject({
      parameters: [{ type: { kind: "named", id: "EventSink" } }],
    });
    expect(member("downloadUsers")).toMatchObject({
      returns: { kind: "readableStream", chunk: { kind: "named", id: "User" } },
    });
    expect(member("writeUsers")).toMatchObject({
      parameters: [
        { type: { kind: "writableStream", chunk: { kind: "named", id: "User" } } },
      ],
    });
    expect(member("echoError")).toMatchObject({
      parameters: [{ type: { kind: "builtin", name: "error" } }],
      returns: { kind: "builtin", name: "error" },
    });

    expect(
      schema.declarations.map(({ kind, sourceName }) => `${kind}:${sourceName}`)
    ).toEqual(
      expect.arrayContaining([
        "object:User",
        "union:UserEvent",
        "object:ByteValues",
        "target:Counter",
        "function:Formatter",
        "target:EventSink",
      ])
    );

    let event = schema.declarations.find(
      (declaration) => declaration.id === "UserEvent"
    );
    let counter = schema.declarations.find(
      (declaration) => declaration.id === "Counter"
    );
    expect(counter).toMatchObject({
      kind: "target",
      members: expect.arrayContaining([
        { kind: "getter", wireName: "value", returns: { kind: "number" } },
      ]),
    });
    let formatter = schema.declarations.find(
      (declaration) => declaration.id === "Formatter"
    );
    expect(formatter).toMatchObject({
      kind: "function",
      parameters: [
        { sourceName: "value", optional: false, type: { kind: "string" } },
        {
          sourceName: "options",
          optional: true,
          type: {
            kind: "nullable",
            inner: { kind: "named", id: expect.stringMatching(/^Anonymous/) },
          },
        },
      ],
      returns: { kind: "string" },
    });

    expect(event).toMatchObject({
      kind: "union",
      sourceName: "UserEvent",
      nullable: false,
      branches: expect.arrayContaining([
        { kind: "named", id: expect.stringMatching(/^Anonymous/) },
      ]),
    });

    let rust = emitRustSchema(schema);
    expect(rust).not.toContain("rpc_schema!");
    expect(rust).toContain("pub struct KitchenSinkApi {");
    expect(rust).toContain(
      "pub fn echo_user(&self, value: User) -> PendingUser"
    );
    expect(rust).toContain('vec!["echoUser".to_owned()]');
    expect(rust).toContain("pub struct Formatter {");
    expect(rust).toContain("pub type UserEvent = ::capnweb::CapnwebValue;");
  });
});
