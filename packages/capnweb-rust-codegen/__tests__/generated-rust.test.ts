// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { describe, expect, test } from "vitest";

import { loadProject } from "../src/context.js";
import { resolveExportedTypes } from "../src/discover.js";
import { emitRustSchema } from "../src/emit-rust.js";
import { introspectRustSchema } from "../src/type-introspector.js";

const fixtureDirectory = fileURLToPath(new URL("fixtures/simple/", import.meta.url));
const checkedInRust = fileURLToPath(
  new URL("../../../rust/tests/generated/simple.rs", import.meta.url)
);

describe("checked-in generated Rust", () => {
  test("is current", async () => {
    let { program, checker } = loadProject("tsconfig.json", fixtureDirectory);
    let roots = resolveExportedTypes(
      program,
      checker,
      "api.ts",
      ["Api"],
      fixtureDirectory
    );
    let generated = emitRustSchema(introspectRustSchema(checker, roots));
    await expect(readFile(checkedInRust, "utf8")).resolves.toBe(generated);
  });
});
