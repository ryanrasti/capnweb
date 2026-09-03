// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

/** Serializable normal form consumed by the Rust source emitter. */
export interface RustSchema {
  version: 1;
  roots: TypeId[];
  declarations: RustDeclaration[];
}

/** Stable within one schema. It is not a protocol-level interface identifier. */
export type TypeId = string;

export type RustDeclaration =
  | ObjectDeclaration
  | TargetDeclaration
  | FunctionDeclaration
  | UnionDeclaration;

export interface ObjectDeclaration {
  kind: "object";
  id: TypeId;
  sourceName: string;
  fields: Field[];
}

export interface TargetDeclaration {
  kind: "target";
  id: TypeId;
  sourceName: string;
  members: TargetMember[];
}

export interface FunctionDeclaration {
  kind: "function";
  id: TypeId;
  sourceName: string;
  parameters: Parameter[];
  rest?: TypeRef;
  returns: TypeRef;
}

export interface UnionDeclaration {
  kind: "union";
  id: TypeId;
  sourceName: string;
  nullable: boolean;
  branches: TypeRef[];
}

export interface Field {
  /** Exact TypeScript property name used on the wire. */
  wireName: string;
  optional: boolean;
  readonly: boolean;
  type: TypeRef;
}

export interface Parameter {
  /** Source spelling, retained for diagnostics and generated Rust naming. */
  sourceName: string;
  optional: boolean;
  type: TypeRef;
}

export type TargetMember =
  | {
      kind: "method";
      wireName: string;
      parameters: Parameter[];
      rest?: TypeRef;
      returns: TypeRef;
    }
  | {
      kind: "getter";
      wireName: string;
      returns: TypeRef;
    };

export type TypeRef =
  | { kind: "string" }
  | { kind: "boolean" }
  | { kind: "number" }
  | { kind: "bigint" }
  | { kind: "void" }
  | { kind: "null" }
  | { kind: "literal"; value: string | number | boolean }
  | { kind: "nullable"; inner: TypeRef }
  | { kind: "array"; element: TypeRef }
  | {
      kind: "tuple";
      elements: TupleElement[];
      rest?: TypeRef;
    }
  | { kind: "record"; value: TypeRef }
  | { kind: "named"; id: TypeId }
  | { kind: "promise"; value: TypeRef }
  | { kind: "readableStream"; chunk: TypeRef }
  | { kind: "writableStream"; chunk: TypeRef }
  | { kind: "union"; branches: TypeRef[] }
  | { kind: "builtin"; name: BuiltinName }
  | { kind: "dynamic"; reason: string };

export interface TupleElement {
  label?: string;
  optional: boolean;
  type: TypeRef;
}

export type BuiltinName =
  | "arrayBuffer"
  | "dataView"
  | "date"
  | "error"
  | "headers"
  | "url"
  | "int8Array"
  | "uint8Array"
  | "uint8ClampedArray"
  | "int16Array"
  | "uint16Array"
  | "int32Array"
  | "uint32Array"
  | "bigInt64Array"
  | "bigUint64Array"
  | "float32Array"
  | "float64Array";
