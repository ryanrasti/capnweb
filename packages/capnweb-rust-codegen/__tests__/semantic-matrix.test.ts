// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import ts from "typescript";
import { describe, expect, test } from "vitest";

import { resolveExportedTypes } from "../src/discover.js";
import type { RustSchema, TargetMember } from "../src/schema.js";
import { introspectRustSchema } from "../src/type-introspector.js";

function schemaFor(source: string, root = "Api"): RustSchema {
  let fileName = "/api.ts";
  let options: ts.CompilerOptions = {
    module: ts.ModuleKind.ESNext,
    strict: true,
    target: ts.ScriptTarget.ES2023,
  };
  let host = ts.createCompilerHost(options, true);
  let getSourceFile = host.getSourceFile.bind(host);
  host.getSourceFile = (name, languageVersion, onError, createNew) =>
    name === fileName
      ? ts.createSourceFile(name, source, languageVersion, true)
      : getSourceFile(name, languageVersion, onError, createNew);
  host.fileExists = (name) => name === fileName || ts.sys.fileExists(name);
  host.readFile = (name) => (name === fileName ? source : ts.sys.readFile(name));
  let program = ts.createProgram({ rootNames: [fileName], options, host });
  let checker = program.getTypeChecker();
  let roots = resolveExportedTypes(program, checker, fileName, [root], "/");
  return introspectRustSchema(checker, roots);
}

function method(schema: RustSchema, wireName: string): TargetMember {
  let root = schema.declarations.find(
    (declaration) => declaration.id === schema.roots[0]
  );
  if (root?.kind !== "target") throw new Error("root is not a target");
  let result = root.members.find((member) => member.wireName === wireName);
  if (!result) throw new Error(`missing method ${wireName}`);
  return result;
}

const RPC_TARGET = "declare class RpcTarget {}";

describe("semantic type matrix", () => {
  test("lowers every primitive and nullish value", () => {
    let schema = schemaFor(`
      ${RPC_TARGET}
      export declare class Api extends RpcTarget {
        values(
          text: string,
          flag: boolean,
          count: number,
          large: bigint,
          missing: null,
          absent: undefined,
        ): void;
      }
    `);
    expect(method(schema, "values")).toEqual({
      kind: "method",
      wireName: "values",
      parameters: [
        { sourceName: "text", optional: false, type: { kind: "string" } },
        { sourceName: "flag", optional: false, type: { kind: "boolean" } },
        { sourceName: "count", optional: false, type: { kind: "number" } },
        { sourceName: "large", optional: false, type: { kind: "bigint" } },
        { sourceName: "missing", optional: false, type: { kind: "null" } },
        { sourceName: "absent", optional: false, type: { kind: "null" } },
      ],
      returns: { kind: "void" },
    });
  });

  test("lowers mutable/readonly arrays, tuples, records, rest, and promises", () => {
    let schema = schemaFor(`
      ${RPC_TARGET}
      export declare class Api extends RpcTarget {
        containers(
          mutable: string[],
          readonlyValues: readonly number[],
          tuple: readonly [name: string, score?: number],
          labels: Record<string, boolean>,
          promised: Promise<string>,
          ...rest: bigint[]
        ): Promise<string[]>;
      }
    `);
    expect(method(schema, "containers")).toEqual({
      kind: "method",
      wireName: "containers",
      parameters: [
        {
          sourceName: "mutable",
          optional: false,
          type: { kind: "array", element: { kind: "string" } },
        },
        {
          sourceName: "readonlyValues",
          optional: false,
          type: { kind: "array", element: { kind: "number" } },
        },
        {
          sourceName: "tuple",
          optional: false,
          type: {
            kind: "tuple",
            elements: [
              { label: "name", optional: false, type: { kind: "string" } },
              {
                label: "score",
                optional: true,
                type: { kind: "nullable", inner: { kind: "number" } },
              },
            ],
          },
        },
        {
          sourceName: "labels",
          optional: false,
          type: { kind: "record", value: { kind: "boolean" } },
        },
        {
          sourceName: "promised",
          optional: false,
          type: { kind: "promise", value: { kind: "string" } },
        },
      ],
      rest: { kind: "bigint" },
      returns: { kind: "array", element: { kind: "string" } },
    });
  });

  test("normalizes optional/null/undefined and preserves recursive object identity", () => {
    let schema = schemaFor(`
      ${RPC_TARGET}
      interface Node {
        name: string;
        parent?: Node | null;
      }
      export declare class Api extends RpcTarget {
        node(): Node;
      }
    `);
    let node = schema.declarations.find(
      (declaration) => declaration.sourceName === "Node"
    );
    expect(node).toMatchObject({
      kind: "object",
      fields: expect.arrayContaining([
        {
          wireName: "parent",
          optional: true,
          readonly: false,
          type: { kind: "nullable", inner: { kind: "named", id: "Node" } },
        },
      ]),
    });
  });

  test("classifies literal, primitive, discriminated, and ambiguous unions", () => {
    let schema = schemaFor(`
      ${RPC_TARGET}
      type Status = "ready" | "failed";
      type Identifier = string | number;
      type Event = { kind: "created"; id: string } | { kind: "deleted"; id: string };
      type Ambiguous = { name: string } | { id: string };
      export declare class Api extends RpcTarget {
        status(value: Status): Status;
        identifier(value: Identifier): Identifier;
        event(value: Event): Event;
        ambiguous(value: Ambiguous): Ambiguous;
      }
    `);
    for (let name of ["Status", "Identifier", "Event", "Ambiguous"]) {
      expect(schema.declarations.find((declaration) => declaration.id === name)).toMatchObject({
        kind: "union",
        sourceName: name,
      });
    }
  });

  test("lowers unknown values dynamically and rejects non-target roots", () => {
    let dynamic = schemaFor(`
      ${RPC_TARGET}
      export declare class Api extends RpcTarget {
        dynamic(value: unknown): unknown;
      }
    `);
    expect(method(dynamic, "dynamic")).toMatchObject({
      parameters: [
        {
          sourceName: "value",
          optional: false,
          type: { kind: "dynamic", reason: "unknown" },
        },
      ],
      returns: { kind: "dynamic", reason: "unknown" },
    });

    expect(() =>
      schemaFor("export interface Api { call(): void; }")
    ).toThrow("Root Api is not an RpcTarget");
  });
});
