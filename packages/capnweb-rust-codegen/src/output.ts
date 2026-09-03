// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import { readFile, writeFile } from "node:fs/promises";

export async function writeGeneratedOutput(
  path: string,
  contents: string,
  check: boolean
): Promise<void> {
  if (!check) {
    await writeFile(path, contents, "utf8");
    return;
  }

  let existing: string;
  try {
    existing = await readFile(path, "utf8");
  } catch (error) {
    if (isMissingFile(error)) {
      throw new Error(`Generated Rust is missing: ${path}`);
    }
    throw error;
  }
  if (existing !== contents) {
    throw new Error(
      `Generated Rust is stale: ${path}\n` +
        "Run capnweb-rust-codegen without --check to update it."
    );
  }
}

function isMissingFile(error: unknown): boolean {
  return (
    error instanceof Error &&
    "code" in error &&
    (error as NodeJS.ErrnoException).code === "ENOENT"
  );
}
