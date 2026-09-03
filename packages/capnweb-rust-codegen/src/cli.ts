#!/usr/bin/env node
// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import { writeFile } from "node:fs/promises";

import { loadProject } from "./context.js";
import { resolveExportedTypes } from "./discover.js";
import { emitRustSchema } from "./emit-rust.js";
import { writeGeneratedOutput } from "./output.js";
import { introspectRustSchema } from "./type-introspector.js";

interface Options {
  project: string;
  input: string;
  roots: string[];
  out?: string;
  schemaOut?: string;
  check: boolean;
}

async function main(argv: readonly string[]): Promise<void> {
  let options = parseOptions(argv);
  let { program, checker } = loadProject(options.project);
  let roots = resolveExportedTypes(program, checker, options.input, options.roots);
  let schema = introspectRustSchema(checker, roots);
  if (options.schemaOut) {
    let json = `${JSON.stringify(schema, null, 2)}\n`;
    await writeFile(options.schemaOut, json, "utf8");
  }

  let rust = emitRustSchema(schema);
  if (options.out) {
    await writeGeneratedOutput(options.out, rust, options.check);
  } else {
    process.stdout.write(rust);
  }
}

function parseOptions(argv: readonly string[]): Options {
  let project = "tsconfig.json";
  let input: string | undefined;
  let roots: string[] = [];
  let out: string | undefined;
  let schemaOut: string | undefined;
  let check = false;

  for (let index = 0; index < argv.length; index++) {
    let argument = argv[index]!;
    let value = argv[index + 1];
    switch (argument) {
      case "--project":
        project = requireValue(argument, value);
        index++;
        break;
      case "--input":
        input = requireValue(argument, value);
        index++;
        break;
      case "--root":
        roots.push(requireValue(argument, value));
        index++;
        break;
      case "--out":
        out = requireValue(argument, value);
        index++;
        break;
      case "--schema-out":
        schemaOut = requireValue(argument, value);
        index++;
        break;
      case "--check":
        check = true;
        break;
      case "--help":
        process.stdout.write(USAGE);
        process.exit(0);
      default:
        throw new Error(`Unknown argument: ${argument}\n\n${USAGE}`);
    }
  }

  if (!input || roots.length === 0) throw new Error(USAGE);
  if (check && !out) throw new Error("--check requires --out");
  return {
    project,
    input,
    roots,
    check,
    ...(out ? { out } : {}),
    ...(schemaOut ? { schemaOut } : {}),
  };
}

function requireValue(option: string, value: string | undefined): string {
  if (!value || value.startsWith("--")) throw new Error(`${option} requires a value`);
  return value;
}

const USAGE = `Usage: capnweb-rust-codegen [options]

Generate a Rust schema declaration from a TypeScript Cap'n Web contract.

Options:
  --project <tsconfig>  TypeScript project (default: tsconfig.json)
  --input <file>        Module containing the exported RPC root
  --root <name>         Exported target type; repeat for multiple roots
  --out <file>          Write generated Rust instead of stdout
  --schema-out <file>   Also write the intermediate schema as JSON
  --check               Fail if --out is absent or stale; do not write it
  --help                Show this help
`;

main(process.argv.slice(2)).catch((error: unknown) => {
  let message = error instanceof Error ? error.message : String(error);
  process.stderr.write(`capnweb-rust-codegen: ${message}\n`);
  process.exitCode = 1;
});
