// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

/** Convert a TypeScript identifier into an idiomatic Rust snake-case identifier. */
export function toRustFieldName(sourceName: string): string {
  return escapeRustIdentifier(toSnakeCase(sourceName));
}

/** Convert a TypeScript type name into an idiomatic Rust UpperCamelCase identifier. */
export function toRustTypeName(sourceName: string): string {
  let words = splitWords(sourceName);
  let candidate = words.map(capitalize).join("") || "Generated";
  if (/^[0-9]/.test(candidate)) candidate = `Type${candidate}`;
  return escapeRustIdentifier(candidate);
}

/** Allocate deterministic, collision-free identifiers within one Rust namespace. */
export class NameAllocator {
  readonly #used = new Set<string>();

  allocate(preferred: string): string {
    if (!this.#used.has(preferred)) {
      this.#used.add(preferred);
      return preferred;
    }

    let raw = preferred.startsWith("r#") ? preferred.slice(2) : preferred;
    for (let suffix = 2; ; suffix++) {
      let candidate = `${raw}_${suffix}`;
      if (!this.#used.has(candidate)) {
        this.#used.add(candidate);
        return candidate;
      }
    }
  }
}

function toSnakeCase(value: string): string {
  let words = splitWords(value);
  let candidate = words.join("_") || "generated";
  if (/^[0-9]/.test(candidate)) candidate = `field_${candidate}`;
  return candidate;
}

function splitWords(value: string): string[] {
  return value
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1 $2")
    .split(/[^A-Za-z0-9]+/)
    .filter(Boolean)
    .map((word) => word.toLowerCase());
}

function capitalize(word: string): string {
  return word.length === 0 ? word : word[0]!.toUpperCase() + word.slice(1);
}

function escapeRustIdentifier(identifier: string): string {
  return RUST_KEYWORDS.has(identifier) ? `r#${identifier}` : identifier;
}

const RUST_KEYWORDS = new Set([
  "Self",
  "abstract",
  "as",
  "async",
  "await",
  "become",
  "box",
  "break",
  "const",
  "continue",
  "crate",
  "do",
  "dyn",
  "else",
  "enum",
  "extern",
  "false",
  "final",
  "fn",
  "for",
  "gen",
  "if",
  "impl",
  "in",
  "let",
  "loop",
  "macro",
  "match",
  "mod",
  "move",
  "mut",
  "override",
  "priv",
  "pub",
  "ref",
  "return",
  "static",
  "struct",
  "super",
  "trait",
  "true",
  "try",
  "type",
  "typeof",
  "union",
  "unsafe",
  "unsized",
  "use",
  "virtual",
  "where",
  "while",
  "yield",
]);
