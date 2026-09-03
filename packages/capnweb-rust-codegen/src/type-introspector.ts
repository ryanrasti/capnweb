// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import ts from "typescript";

import type {
  BuiltinName,
  Field,
  FunctionDeclaration,
  ObjectDeclaration,
  Parameter,
  RustDeclaration,
  RustSchema,
  TargetDeclaration,
  TargetMember,
  TupleElement,
  TypeId,
  TypeRef,
  UnionDeclaration,
} from "./schema.js";

export function introspectRustSchema(
  checker: ts.TypeChecker,
  roots: readonly ts.Type[]
): RustSchema {
  let resolver = new SchemaResolver(checker);
  let rootIds = roots.map((type) => resolver.resolveTarget(type));
  return {
    version: 1,
    roots: rootIds,
    declarations: resolver.declarations,
  };
}

class SchemaResolver {
  readonly declarations: RustDeclaration[] = [];

  readonly #checker: ts.TypeChecker;
  readonly #ids = new Map<ts.Type, TypeId>();
  readonly #declarations = new Map<TypeId, RustDeclaration>();
  #nextAnonymousId = 1;

  constructor(checker: ts.TypeChecker) {
    this.#checker = checker;
  }

  resolveTarget(type: ts.Type): TypeId {
    if (!this.#isTarget(type)) {
      let declaration = type.getSymbol()?.declarations?.[0];
      let message = `Root ${this.#sourceName(type)} is not an RpcTarget`;
      throw declaration ? schemaError(declaration, message) : new Error(message);
    }
    return this.#declare(type, "target");
  }

  #resolveType(type: ts.Type): TypeRef {
    let flags = type.flags;
    if (flags & ts.TypeFlags.StringLiteral) {
      return { kind: "literal", value: (type as ts.StringLiteralType).value };
    }
    if (flags & ts.TypeFlags.NumberLiteral) {
      return { kind: "literal", value: (type as ts.NumberLiteralType).value };
    }
    if (flags & ts.TypeFlags.BooleanLiteral) {
      return { kind: "literal", value: this.#checker.typeToString(type) === "true" };
    }
    if (flags & ts.TypeFlags.StringLike) return { kind: "string" };
    if (flags & ts.TypeFlags.NumberLike) return { kind: "number" };
    if (flags & ts.TypeFlags.BooleanLike) return { kind: "boolean" };
    if (flags & ts.TypeFlags.BigIntLike) return { kind: "bigint" };
    if (flags & (ts.TypeFlags.Null | ts.TypeFlags.Undefined)) return { kind: "null" };
    if (flags & (ts.TypeFlags.Void | ts.TypeFlags.Never)) return { kind: "void" };
    if (flags & (ts.TypeFlags.Any | ts.TypeFlags.Unknown)) {
      return { kind: "dynamic", reason: this.#checker.typeToString(type) };
    }

    // RpcStub<T> is a structural conditional/intersection type after TypeScript
    // instantiates it. Its private brand is the reliable route back to T.
    let stubBrand = this.#checker.getPropertyOfType(type, "__RPC_STUB_BRAND");
    let stubBrandDeclaration =
      stubBrand?.valueDeclaration ?? stubBrand?.declarations?.[0];
    if (stubBrand && stubBrandDeclaration) {
      let service = this.#checker.getTypeOfSymbolAtLocation(
        stubBrand,
        stubBrandDeclaration
      );
      if (service !== type) return this.#resolveType(service);
    }

    if (type.isUnion()) {
      let nonNullBranches = type.types.filter(
        (branch) =>
          !(branch.flags & (ts.TypeFlags.Null | ts.TypeFlags.Undefined))
      );
      if (type.aliasSymbol && nonNullBranches.length > 1) {
        return { kind: "named", id: this.#declare(type, "union") };
      }
      return this.#resolveUnion(type.types);
    }

    if (!(flags & ts.TypeFlags.Object)) {
      return {
        kind: "dynamic",
        reason: `unsupported TypeScript type ${this.#checker.typeToString(type)}`,
      };
    }

    let object = type as ts.ObjectType;
    if (this.#checker.isTupleType(object)) return this.#resolveTuple(object);
    if (this.#checker.isArrayType(object)) {
      let element = this.#typeArguments(object)[0];
      return {
        kind: "array",
        element: element
          ? this.#resolveType(element)
          : { kind: "dynamic", reason: "array element" },
      };
    }

    let symbolName = type.aliasSymbol?.getName() ?? type.getSymbol()?.getName();
    let arguments_ = this.#typeArguments(object);
    if (symbolName === "ReadonlyArray") {
      return {
        kind: "array",
        element: arguments_[0]
          ? this.#resolveType(arguments_[0])
          : { kind: "dynamic", reason: "readonly array element" },
      };
    }
    if (symbolName === "Promise") {
      return {
        kind: "promise",
        value: arguments_[0]
          ? this.#resolveType(arguments_[0])
          : { kind: "dynamic", reason: "promise result" },
      };
    }
    if (symbolName === "ReadableStream" || symbolName === "WritableStream") {
      let chunk = arguments_[0]
        ? this.#resolveType(arguments_[0])
        : ({ kind: "dynamic", reason: "stream chunk" } as const);
      return symbolName === "ReadableStream"
        ? { kind: "readableStream", chunk }
        : { kind: "writableStream", chunk };
    }
    if (symbolName === "Record" && arguments_[1]) {
      return { kind: "record", value: this.#resolveType(arguments_[1]) };
    }
    if ((symbolName === "RpcStub" || symbolName === "RpcPromise") && arguments_[0]) {
      return this.#resolveType(arguments_[0]);
    }

    let builtin = BUILTINS[symbolName ?? ""];
    if (builtin) return { kind: "builtin", name: builtin };

    let stringIndex = this.#checker.getIndexTypeOfType(type, ts.IndexKind.String);
    if (stringIndex && this.#checker.getPropertiesOfType(type).length === 0) {
      return { kind: "record", value: this.#resolveType(stringIndex) };
    }

    let callSignatures = type.getCallSignatures();
    if (callSignatures.length === 1) {
      return { kind: "named", id: this.#declare(type, "function") };
    }
    if (callSignatures.length > 1) {
      return { kind: "dynamic", reason: "overloaded function" };
    }
    if (this.#isTarget(type)) {
      return { kind: "named", id: this.#declare(type, "target") };
    }
    return { kind: "named", id: this.#declare(type, "object") };
  }

  #resolveUnion(types: readonly ts.Type[]): TypeRef {
    let hasNull = types.some(
      (type) => type.flags & (ts.TypeFlags.Null | ts.TypeFlags.Undefined)
    );
    let branches = types
      .filter((type) => !(type.flags & (ts.TypeFlags.Null | ts.TypeFlags.Undefined)))
      .map((type) => this.#resolveType(type));
    if (hasNull && branches.length === 1) {
      return { kind: "nullable", inner: branches[0]! };
    }
    if (branches.length === 0) return { kind: "null" };
    let union: TypeRef = { kind: "union", branches };
    return hasNull ? { kind: "nullable", inner: union } : union;
  }

  #resolveTuple(type: ts.ObjectType): TypeRef {
    let reference = type as ts.TypeReference;
    let elements = this.#checker.getTypeArguments(reference);
    let target = reference.target as ts.TupleType;
    let elementFlags = target.elementFlags ?? [];
    let labels = target.labeledElementDeclarations ?? [];
    let tupleElements: TupleElement[] = [];
    let rest: TypeRef | undefined;

    for (let index = 0; index < elements.length; index++) {
      let flag = elementFlags[index] ?? ts.ElementFlags.Required;
      let resolved = this.#resolveType(elements[index]!);
      if (flag & ts.ElementFlags.Rest) {
        rest = resolved.kind === "array" ? resolved.element : resolved;
        continue;
      }
      let declaration = labels[index];
      tupleElements.push({
        ...(declaration?.name && ts.isIdentifier(declaration.name)
          ? { label: declaration.name.text }
          : {}),
        optional: Boolean(flag & ts.ElementFlags.Optional),
        type: resolved,
      });
    }
    return { kind: "tuple", elements: tupleElements, ...(rest ? { rest } : {}) };
  }

  #declare(type: ts.Type, kind: RustDeclaration["kind"]): TypeId {
    let existing = this.#ids.get(type);
    if (existing) {
      let declaration = this.#declarations.get(existing);
      if (declaration && declaration.kind !== kind) {
        throw new Error(
          `Type ${existing} was classified as both ${declaration.kind} and ${kind}`
        );
      }
      return existing;
    }

    let sourceName = this.#sourceName(type);
    let id = this.#uniqueId(sourceName);
    this.#ids.set(type, id);

    // Install a placeholder before descending so recursive references terminate.
    let placeholder = { kind, id, sourceName } as RustDeclaration;
    this.#declarations.set(id, placeholder);
    this.declarations.push(placeholder);

    let declaration =
      kind === "target"
        ? this.#resolveTargetDeclaration(type, id, sourceName)
        : kind === "function"
          ? this.#resolveFunctionDeclaration(type, id, sourceName)
          : kind === "union"
            ? this.#resolveUnionDeclaration(type, id, sourceName)
            : this.#resolveObjectDeclaration(type, id, sourceName);
    this.#declarations.set(id, declaration);
    this.declarations[this.declarations.indexOf(placeholder)] = declaration;
    return id;
  }

  #resolveObjectDeclaration(
    type: ts.Type,
    id: TypeId,
    sourceName: string
  ): ObjectDeclaration {
    let fields: Field[] = this.#checker.getPropertiesOfType(type).map((property) => {
      let declaration = property.valueDeclaration ?? property.declarations?.[0];
      if (!declaration) throw new Error(`Property ${property.getName()} has no declaration`);
      let propertyType = this.#checker.getTypeOfSymbolAtLocation(property, declaration);
      return {
        wireName: property.getName(),
        optional: Boolean(property.flags & ts.SymbolFlags.Optional),
        readonly: hasReadonlyModifier(declaration),
        type: this.#resolveType(propertyType),
      };
    });
    return { kind: "object", id, sourceName, fields };
  }

  #resolveTargetDeclaration(
    type: ts.Type,
    id: TypeId,
    sourceName: string
  ): TargetDeclaration {
    let members: TargetMember[] = [];
    for (let property of this.#checker.getPropertiesOfType(type)) {
      if (RPC_CAPABILITY_BRAND_NAMES.has(property.getName())) continue;
      let declaration = property.valueDeclaration ?? property.declarations?.[0];
      if (!declaration || this.#isRpcBaseMember(declaration)) continue;
      if (isPrivateOrProtected(declaration)) continue;

      let propertyType = this.#checker.getTypeOfSymbolAtLocation(property, declaration);
      if (ts.isGetAccessorDeclaration(declaration)) {
        members.push({
          kind: "getter",
          wireName: property.getName(),
          returns: this.#resolveReturnType(propertyType),
        });
        continue;
      }

      let signatures = propertyType.getCallSignatures();
      if (signatures.length !== 1) {
        throw schemaError(
          declaration,
          signatures.length === 0
            ? `Target ${sourceName} field ${property.getName()} is not remotely traversable`
            : `Target ${sourceName} method ${property.getName()} is overloaded`
        );
      }
      let signature = signatures[0]!;
      let { parameters, rest } = this.#resolveParameters(signature);
      members.push({
        kind: "method",
        wireName: property.getName(),
        parameters,
        ...(rest ? { rest } : {}),
        returns: this.#resolveReturnType(signature.getReturnType()),
      });
    }
    return { kind: "target", id, sourceName, members };
  }

  #resolveUnionDeclaration(
    type: ts.Type,
    id: TypeId,
    sourceName: string
  ): UnionDeclaration {
    if (!type.isUnion()) throw new Error(`Type ${sourceName} is not a union`);
    let resolved = this.#resolveUnion(type.types);
    let nullable = resolved.kind === "nullable";
    let inner: TypeRef = resolved.kind === "nullable" ? resolved.inner : resolved;
    let branches = inner.kind === "union" ? inner.branches : [inner];
    return { kind: "union", id, sourceName, nullable, branches };
  }

  #resolveFunctionDeclaration(
    type: ts.Type,
    id: TypeId,
    sourceName: string
  ): FunctionDeclaration {
    let signature = type.getCallSignatures()[0];
    if (!signature) throw new Error(`Function ${sourceName} has no call signature`);
    let { parameters, rest } = this.#resolveParameters(signature);
    return {
      kind: "function",
      id,
      sourceName,
      parameters,
      ...(rest ? { rest } : {}),
      returns: this.#resolveReturnType(signature.getReturnType()),
    };
  }

  #resolveParameters(signature: ts.Signature): {
    parameters: Parameter[];
    rest?: TypeRef;
  } {
    let parameters: Parameter[] = [];
    let rest: TypeRef | undefined;
    for (let parameter of signature.getParameters()) {
      let declaration = parameter.valueDeclaration ?? parameter.declarations?.[0];
      if (!declaration) throw new Error(`Parameter ${parameter.getName()} has no declaration`);
      let type = this.#checker.getTypeOfSymbolAtLocation(parameter, declaration);
      if (ts.isParameter(declaration) && declaration.dotDotDotToken) {
        let resolved = this.#resolveType(type);
        rest = resolved.kind === "array" ? resolved.element : resolved;
      } else {
        parameters.push({
          sourceName: parameter.getName(),
          optional: Boolean(parameter.flags & ts.SymbolFlags.Optional) ||
            (ts.isParameter(declaration) && Boolean(declaration.questionToken)),
          type: this.#resolveType(type),
        });
      }
    }
    return { parameters, ...(rest ? { rest } : {}) };
  }

  #resolveReturnType(type: ts.Type): TypeRef {
    return this.#resolveType(this.#checker.getAwaitedType(type) ?? type);
  }

  #isTarget(type: ts.Type): boolean {
    if (type.getSymbol()?.getName() === "RpcTarget") return true;
    return (type.getBaseTypes() ?? []).some((base) => this.#isTarget(base));
  }

  #isRpcBaseMember(declaration: ts.Declaration): boolean {
    let parent = declaration.parent;
    return Boolean(
      parent &&
        (ts.isClassDeclaration(parent) || ts.isInterfaceDeclaration(parent)) &&
        parent.name?.text === "RpcTarget"
    );
  }

  #typeArguments(type: ts.ObjectType): readonly ts.Type[] {
    return type.objectFlags & ts.ObjectFlags.Reference
      ? this.#checker.getTypeArguments(type as ts.TypeReference)
      : [];
  }

  #sourceName(type: ts.Type): string {
    let name = type.aliasSymbol?.getName() ?? type.getSymbol()?.getName();
    return name && name !== "__type" ? name : `Anonymous${this.#nextAnonymousId++}`;
  }

  #uniqueId(sourceName: string): TypeId {
    if (!this.#declarations.has(sourceName)) return sourceName;
    for (let suffix = 2; ; suffix++) {
      let id = `${sourceName}_${suffix}`;
      if (!this.#declarations.has(id)) return id;
    }
  }
}

function schemaError(declaration: ts.Declaration, message: string): Error {
  let source = declaration.getSourceFile();
  let position = source.getLineAndCharacterOfPosition(declaration.getStart(source));
  return new Error(
    `${source.fileName}:${position.line + 1}:${position.character + 1}: ${message}`
  );
}

function hasReadonlyModifier(declaration: ts.Declaration): boolean {
  return Boolean(
    ts.canHaveModifiers(declaration) &&
      ts.getModifiers(declaration)?.some(
        (modifier) => modifier.kind === ts.SyntaxKind.ReadonlyKeyword
      )
  );
}

function isPrivateOrProtected(declaration: ts.Declaration): boolean {
  if (!ts.canHaveModifiers(declaration)) return false;
  return Boolean(
    ts.getModifiers(declaration)?.some(
      (modifier) =>
        modifier.kind === ts.SyntaxKind.PrivateKeyword ||
        modifier.kind === ts.SyntaxKind.ProtectedKeyword
    )
  );
}

const RPC_CAPABILITY_BRAND_NAMES = new Set([
  "__RPC_TARGET_BRAND",
  "__WORKER_ENTRYPOINT_BRAND",
  "__DURABLE_OBJECT_BRAND",
]);

const BUILTINS: Readonly<Record<string, BuiltinName | undefined>> = {
  ArrayBuffer: "arrayBuffer",
  DataView: "dataView",
  Date: "date",
  Error: "error",
  Headers: "headers",
  URL: "url",
  Int8Array: "int8Array",
  Uint8Array: "uint8Array",
  Uint8ClampedArray: "uint8ClampedArray",
  Int16Array: "int16Array",
  Uint16Array: "uint16Array",
  Int32Array: "int32Array",
  Uint32Array: "uint32Array",
  BigInt64Array: "bigInt64Array",
  BigUint64Array: "bigUint64Array",
  Float32Array: "float32Array",
  Float64Array: "float64Array",
};
