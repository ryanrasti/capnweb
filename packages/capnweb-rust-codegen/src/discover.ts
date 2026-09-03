// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import { resolve } from "node:path";
import ts from "typescript";

export function resolveExportedTypes(
  program: ts.Program,
  checker: ts.TypeChecker,
  input: string,
  rootNames: readonly string[],
  cwd = process.cwd()
): ts.Type[] {
  let fileName = resolve(cwd, input);
  let sourceFile = program.getSourceFile(fileName);
  if (!sourceFile) {
    throw new Error(`TypeScript input is not part of the project: ${fileName}`);
  }

  let moduleSymbol = checker.getSymbolAtLocation(sourceFile);
  if (!moduleSymbol) throw new Error(`Input is not a TypeScript module: ${fileName}`);

  let exports = new Map(
    checker.getExportsOfModule(moduleSymbol).map((symbol) => [symbol.getName(), symbol])
  );

  return rootNames.map((name) => {
    let symbol = exports.get(name);
    if (!symbol) throw new Error(`Module ${input} has no exported type named ${name}`);
    symbol = resolveAlias(checker, symbol);
    let declaration = symbol.valueDeclaration ?? symbol.declarations?.[0];
    if (!declaration) throw new Error(`Export ${name} has no declaration`);
    return checker.getTypeAtLocation(declaration);
  });
}

function resolveAlias(checker: ts.TypeChecker, symbol: ts.Symbol): ts.Symbol {
  return symbol.flags & ts.SymbolFlags.Alias
    ? checker.getAliasedSymbol(symbol)
    : symbol;
}
