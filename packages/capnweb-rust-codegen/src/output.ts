// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import { spawn } from "node:child_process";
import { readFile, writeFile } from "node:fs/promises";

/** Formats generated Rust when rustfmt is installed, otherwise returns it unchanged. */
export async function formatGeneratedRust(contents: string): Promise<string> {
  return new Promise((resolve, reject) => {
    let child;
    try {
      child = spawn("rustfmt", ["--edition", "2024", "--emit", "stdout"], {
        stdio: ["pipe", "pipe", "pipe"],
      });
    } catch (error) {
      reject(error);
      return;
    }
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8").on("data", (chunk: string) => {
      stdout += chunk;
    });
    child.stderr.setEncoding("utf8").on("data", (chunk: string) => {
      stderr += chunk;
    });
    child.on("error", (error: NodeJS.ErrnoException) => {
      if (error.code === "ENOENT") resolve(contents);
      else reject(error);
    });
    child.on("close", (code) => {
      if (code === 0) resolve(stdout);
      else if (code !== null) reject(new Error(`rustfmt failed: ${stderr.trim()}`));
    });
    child.stdin.end(contents);
  });
}

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
