// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import ts from "typescript";
import { describe, expect, test } from "vitest";

import { resolveExportedTypes } from "../src/discover.js";
import { introspectRustSchema } from "../src/type-introspector.js";

function programFor(source: string): ts.Program {
  let fileName = "/api.ts";
  let options: ts.CompilerOptions = {
    module: ts.ModuleKind.ESNext,
    strict: true,
    target: ts.ScriptTarget.ES2023,
  };
  let host = ts.createCompilerHost(options, true);
  let getSourceFile = host.getSourceFile.bind(host);
  host.getSourceFile = (name, languageVersion, onError, shouldCreateNewSourceFile) =>
    name === fileName
      ? ts.createSourceFile(name, source, languageVersion, true)
      : getSourceFile(name, languageVersion, onError, shouldCreateNewSourceFile);
  host.fileExists = (name) => name === fileName || ts.sys.fileExists(name);
  host.readFile = (name) => (name === fileName ? source : ts.sys.readFile(name));
  return ts.createProgram({ rootNames: [fileName], options, host });
}

describe("introspectRustSchema", () => {
  test("preserves wire names and distinguishes methods from getters", () => {
    let program = programFor(`
      declare class RpcTarget {}

      export interface User {
        displayName: string;
        nickname?: string;
      }

      export declare class Api extends RpcTarget {
        get currentUser(): User;
        findUser(id: string, limit?: number): Promise<User | null>;
      }
    `);
    let checker = program.getTypeChecker();
    let roots = resolveExportedTypes(program, checker, "/api.ts", ["Api"], "/");
    let schema = introspectRustSchema(checker, roots);

    expect(schema).toEqual({
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
              wireName: "findUser",
              parameters: [
                {
                  sourceName: "id",
                  optional: false,
                  type: { kind: "string" },
                },
                {
                  sourceName: "limit",
                  optional: true,
                  type: { kind: "nullable", inner: { kind: "number" } },
                },
              ],
              returns: {
                kind: "nullable",
                inner: { kind: "named", id: "User" },
              },
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
      ],
    });
  });

  test("rejects target instance fields rather than exposing them", () => {
    let program = programFor(`
      declare class RpcTarget {}

      export declare class Api extends RpcTarget {
        public state: string;
        call(): void;
      }
    `);
    let checker = program.getTypeChecker();
    let roots = resolveExportedTypes(program, checker, "/api.ts", ["Api"], "/");

    expect(() => introspectRustSchema(checker, roots)).toThrow(
      "Target Api field state is not remotely traversable"
    );
  });

  test("preserves callback signatures and stream chunk types", () => {
    let program = programFor(`
      declare class RpcTarget {}
      declare class ReadableStream<T> { readonly chunk?: T; }

      export type Formatter = (value: string, count?: number) => Promise<string>;

      export declare class Api extends RpcTarget {
        format(callback: Formatter): Formatter;
        read(): ReadableStream<string>;
      }
    `);
    let checker = program.getTypeChecker();
    let roots = resolveExportedTypes(program, checker, "/api.ts", ["Api"], "/");
    let schema = introspectRustSchema(checker, roots);
    let formatter = schema.declarations.find(
      (declaration) => declaration.sourceName === "Formatter"
    );

    expect(formatter).toEqual({
      kind: "function",
      id: "Formatter",
      sourceName: "Formatter",
      parameters: [
        {
          sourceName: "value",
          optional: false,
          type: { kind: "string" },
        },
        {
          sourceName: "count",
          optional: true,
          type: { kind: "nullable", inner: { kind: "number" } },
        },
      ],
      returns: { kind: "string" },
    });

    let api = schema.declarations[0];
    expect(api).toMatchObject({
      kind: "target",
      members: expect.arrayContaining([
        {
          kind: "method",
          wireName: "read",
          parameters: [],
          returns: { kind: "readableStream", chunk: { kind: "string" } },
        },
      ]),
    });
  });
});
