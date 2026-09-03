// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, test } from "vitest";

import { writeGeneratedOutput } from "../src/output.js";

let directories: string[] = [];
afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((directory) =>
      rm(directory, { recursive: true, force: true })
    )
  );
});

async function temporaryPath(): Promise<string> {
  let directory = await mkdtemp(join(tmpdir(), "capnweb-rust-codegen-"));
  directories.push(directory);
  return join(directory, "generated.rs");
}

describe("writeGeneratedOutput", () => {
  test("writes generated output", async () => {
    let path = await temporaryPath();
    await writeGeneratedOutput(path, "generated\n", false);
    await expect(readFile(path, "utf8")).resolves.toBe("generated\n");
  });

  test("accepts current output in check mode", async () => {
    let path = await temporaryPath();
    await writeFile(path, "generated\n");
    await expect(writeGeneratedOutput(path, "generated\n", true)).resolves.toBe(
      undefined
    );
  });

  test("reports missing and stale output in check mode", async () => {
    let path = await temporaryPath();
    await expect(writeGeneratedOutput(path, "new\n", true)).rejects.toThrow(
      `Generated Rust is missing: ${path}`
    );
    await writeFile(path, "old\n");
    await expect(writeGeneratedOutput(path, "new\n", true)).rejects.toThrow(
      `Generated Rust is stale: ${path}`
    );
  });
});
