function followPath(value: unknown, parent: object | undefined,
                    path: PropertyPath, owner: RpcPayload | null): FollowPathResult {
  for (let i = 0; i < path.length; i++) {
    parent = <object>value;

    let part = path[i];
    if (part in Object.prototype) {
      // Don't allow messing with Object.prototype properties over RPC. We block these even if
      // the specific object has overridden them for consistency with the deserialization code,
      // which will refuse to deserialize an object containing such properties. Anyway, it's
      // impossible for a normal client to even request these because accessing Object prototype
      // properties on a stub will resolve to the local prototype property, not making an RPC at
      // all.
      value = undefined;
      continue;
    }

    let kind = typeForRpc(value);
    switch (kind) {
      case "object":
      case "function":
        // Must be own property, NOT inherited from a prototype.
        if (Object.hasOwn(<object>value, part)) {
          value = (<any>value)[part];
        } else {
          value = undefined;
        }
        break;

      case "array":
        // For arrays, restrict specifically to numeric indexes, to be consistent with
        // serialization, which only sends a flat list.
        if (Number.isInteger(part) && <number>part >= 0) {
          value = (<any>value)[part];
        } else {
          value = undefined;
        }
        break;

      case "rpc-target":
      case "rpc-thenable": {
        // Must be prototype property, and must NOT be inherited from `Object`.
        if (Object.hasOwn(<object>value, part)) {
          // We throw an error in this case, rather than return undefined, because otherwise
          // people tend to get confused about this. If you don't want it to be possible to
          // probe the existence of your instance properties, make them properly private (prefix
          // with #).
          throw new TypeError(
              `Attempted to access property '${part}', which is an instance property of the ` +
              `RpcTarget. To avoid leaking private internals, instance properties cannot be ` +
              `accessed over RPC. If you want to make this property available over RPC, define ` +
              `it as a method or getter on the class, instead of an instance property.`);
        } else {
          value = (<any>value)[part];
        }

        // Since we're descending into the RpcTarget, the rest of the path is not "owned" by any
        // RpcPayload.
        owner = null;
        break;
      }

      case "stub":
      case "rpc-promise": {
        let {hook: hook, pathIfPromise} = unwrapStubAndPath(<RpcStub>value);
        return { hook, remainingPath:
            pathIfPromise ? pathIfPromise.concat(path.slice(i)) : path.slice(i) };
      }

      case "primitive":
      case "bigint":
      case "bytes":
      case "date":
      case "error":
        // These have no properties that can be accessed remotely.
        value = undefined;
        break;

      case "undefined":
        // Intentionally produce TypeError.
        value = (value as any)[part];
        break;

      case "unsupported": {
        if (i === 0) {
          throw new TypeError(`RPC stub points at a non-serializable type.`);
        } else {
          let prefix = path.slice(0, i).join(".");
          let remainder = path.slice(0, i).join(".");
          throw new TypeError(
              `'${prefix}' is not a serializable type, so property ${remainder} cannot ` +
              `be accessed.`);
        }
      }

      default:
        kind satisfies never;
        throw new TypeError("unreachable");
    }
  }

  // If we reached a promise, we actually want the caller to forward to the promise, not return
  // the promise itself.
  if (value instanceof RpcPromise) {
    let {hook: hook, pathIfPromise} = unwrapStubAndPath(<RpcStub>value);
    return { hook, remainingPath: pathIfPromise || [] };
  }

  // We don't validate the final value itself because we don't know the intended use yet. If it's
  // for a call, any callable is valid. If it's for get(), then any serializable value is valid.
  return {
    value,
    parent,
    owner,
  };
}

export function typeForRpc(value: unknown): TypeForRpc {
  switch (typeof value) {
    case "boolean":
    case "number":
    case "string":
      return "primitive";

    case "undefined":
      return "undefined";

    case "object":
    case "function":
      // Test by prototype, below.
      break;

    case "bigint":
      return "bigint";

    default:
      return "unsupported";
  }

  // Ugh JavaScript, why is `typeof null` equal to "object" but null isn't otherwise anything like
  // an object?
  if (value === null) {
    return "primitive";
  }

  // Aside from RpcTarget, we generally don't support serializing *subclasses* of serializable
  // types, so we switch on the exact prototype rather than use `instanceof` here.
  let prototype = Object.getPrototypeOf(value);
  switch (prototype) {
    case Object.prototype:
      return "object";

    case Function.prototype:
    case AsyncFunction.prototype:
      return "function";

    case Array.prototype:
      return "array";

    case Date.prototype:
      return "date";

    case Uint8Array.prototype:
      return "bytes";

    // TODO: All other structured clone types.

    case RpcStub.prototype:
      return "stub";

    case RpcPromise.prototype:
      return "rpc-promise";

    // TODO: Promise<T> or thenable

    default:
      if (workersModule) {
        // TODO: We also need to match `RpcPromise` and `RpcProperty`, but they currently aren't
        //   exported by cloudflare:workers.
        if (prototype == workersModule.RpcStub.prototype ||
            value instanceof workersModule.ServiceStub) {
          return "rpc-target";
        } else if (prototype == workersModule.RpcPromise.prototype ||
                   prototype == workersModule.RpcProperty.prototype) {
          // Like rpc-target, but should be wrapped in RpcPromise, so that it can be pull()ed,
          // which will await the thenable.
          return "rpc-thenable";
        }
      }

      if (value instanceof RpcTarget) {
        return "rpc-target";
      }

      if (value instanceof Error) {
        return "error";
      }

      return "unsupported";
  }
}
