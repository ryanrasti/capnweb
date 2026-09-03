// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import { dirname, resolve } from "node:path";
import ts from "typescript";

export interface ProjectContext {
  projectPath: string;
  program: ts.Program;
  checker: ts.TypeChecker;
}

/** Load the same TypeScript project the contract's author sees. */
export function loadProject(
  project = "tsconfig.json",
  cwd = process.cwd()
): ProjectContext {
  let projectPath = resolve(cwd, project);
  let config = ts.readConfigFile(projectPath, ts.sys.readFile);
  if (config.error) throw new Error(formatDiagnostic(config.error));

  let parsed = ts.parseJsonConfigFileContent(
    config.config,
    ts.sys,
    dirname(projectPath),
    undefined,
    projectPath
  );
  if (parsed.errors.length > 0) {
    throw new Error(parsed.errors.map(formatDiagnostic).join("\n"));
  }

  let program = ts.createProgram({
    rootNames: parsed.fileNames,
    options: parsed.options,
  });
  let diagnostics = ts.getPreEmitDiagnostics(program);
  if (diagnostics.length > 0) {
    throw new Error(diagnostics.map(formatDiagnostic).join("\n"));
  }

  return {
    projectPath,
    program,
    checker: program.getTypeChecker(),
  };
}

function formatDiagnostic(diagnostic: ts.Diagnostic): string {
  let message = ts.flattenDiagnosticMessageText(diagnostic.messageText, "\n");
  if (!diagnostic.file || diagnostic.start === undefined) return message;

  let position = diagnostic.file.getLineAndCharacterOfPosition(diagnostic.start);
  return `${diagnostic.file.fileName}:${position.line + 1}:${position.character + 1}: ${message}`;
}
