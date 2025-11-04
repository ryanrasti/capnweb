// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import { StubHook, PropertyPath, RpcPayload, RpcStub, RpcPromise, withCallInterceptor, mapImpl, PayloadStubHook, unwrapStubNoProperties, RpcTarget } from "./core.js";
import { Devaluator, Exporter, Importer, ExportId, ImportId, Evaluator } from "./serialize.js";

let currentMapBuilder: MapBuilder | undefined;

// We use this type signature when building the instructions for type checking purposes. It
// describes a subset of the overall RPC protocol.
export type MapInstruction =
    | ["pipeline", number, PropertyPath]
    | ["pipeline", number, PropertyPath, unknown]
    | ["remap", number, PropertyPath, ["import", number][], MapInstruction[]]

class MapBuilder implements Exporter {
  private context:
    | {parent: undefined, captures: StubHook[], subject: StubHook, path: PropertyPath}
    | {parent: MapBuilder, captures: number[], subject: number, path: PropertyPath};
  private captureMap: Map<StubHook, number> = new Map();

  private instructions: MapInstruction[] = [];

  constructor(subject: StubHook, path: PropertyPath) {
    if (currentMapBuilder) {
      this.context = {
        parent: currentMapBuilder,
        captures: [],
        subject: currentMapBuilder.capture(subject),
        path
      };
    } else {
      this.context = {
        parent: undefined,
        captures: [],
        subject,
        path
      };
    }

    currentMapBuilder = this;
  }

  unregister() {
    currentMapBuilder = this.context.parent;
  }

  makeInput(): MapVariableHook {
    return new MapVariableHook(this, 0);
  }

  makeOutput(result: RpcPayload): StubHook {
    let devalued: unknown;
    try {
      devalued = Devaluator.devaluate(result.value, undefined, this, result);
    } finally {
      result.dispose();
    }

    // The result is the final instruction. This doesn't actually fit our MapInstruction type
    // signature, so we cheat a bit.
    this.instructions.push(<any>devalued);

    if (this.context.parent) {
      this.context.parent.instructions.push(
        ["remap", this.context.subject, this.context.path,
                  this.context.captures.map(cap => ["import", cap]),
                  this.instructions]
      );
      return new MapVariableHook(this.context.parent, this.context.parent.instructions.length);
    } else {
      return this.context.subject.map(this.context.path, this.context.captures, this.instructions);
    }
  }

  pushCall(hook: StubHook, path: PropertyPath, params: RpcPayload): StubHook {
    let devalued = Devaluator.devaluate(params.value, undefined, this, params);
    // HACK: Since the args is an array, devaluator will wrap in a second array. Need to unwrap.
    // TODO: Clean this up somehow.
    devalued = (<Array<unknown>>devalued)[0];

    let subject = this.capture(hook.dup());
    this.instructions.push(["pipeline", subject, path, devalued]);
    return new MapVariableHook(this, this.instructions.length);
  }

  pushGet(hook: StubHook, path: PropertyPath): StubHook {
    let subject = this.capture(hook.dup());
    this.instructions.push(["pipeline", subject, path]);
    return new MapVariableHook(this, this.instructions.length);
  }

  capture(hook: StubHook): number {
    if (hook instanceof MapVariableHook && hook.mapper === this) {
      // Oh, this is already our own hook.
      return hook.idx;
    }

    // TODO: Well, the hooks passed in are always unique, so they'll never exist in captureMap.
    //   I suppose this is a problem with RPC as well. We need a way to identify hooks that are
    //   dupes of the same target.
    let result = this.captureMap.get(hook);
    if (result === undefined) {
      if (this.context.parent) {
        let parentIdx = this.context.parent.capture(hook);
        this.context.captures.push(parentIdx);
      } else {
        this.context.captures.push(hook);
      }
      result = -this.context.captures.length;
      this.captureMap.set(hook, result);
    }
    return result;
  }

  // ---------------------------------------------------------------------------
  // implements Exporter

  exportStub(hook: StubHook): ExportId {
    // It appears someone did something like:
    //
    //     stub.map(x => { return x.doSomething(new MyRpcTarget()); })
    //
    // That... won't work. They need to do this instead:
    //
    //     using myTargetStub = new RpcStub(new MyRpcTarget());
    //     stub.map(x => { return x.doSomething(myTargetStub.dup()); })
    //
    // TODO(someday): Consider carefully if the inline syntax is maybe OK. If so, perhaps the
    //   serializer could try calling `getImport()` even for known-local hooks.
    // TODO(someday): Do we need to support rpc-thenable somehow?
    throw new Error(
        "Can't construct an RpcTarget or RPC callback inside a mapper function. Try creating a " +
        "new RpcStub outside the callback first, then using it inside the callback.");
  }
  exportPromise(hook: StubHook): ExportId {
    return this.exportStub(hook);
  }
  getImport(hook: StubHook): ImportId | undefined {
    return this.capture(hook);
  }

  unexport(ids: Array<ExportId>): void {
    // Presumably this MapBuilder is cooked anyway, so we don't really have to release anything.
  }

  onSendError(error: Error): Error | void {
    // TODO(someday): Can we use the error-sender hook from the RPC system somehow?
  }
};

mapImpl.sendMap = (hook: StubHook, path: PropertyPath, func: (promise: RpcPromise) => unknown) => {
  let builder = new MapBuilder(hook, path);
  let result: RpcPayload;
  try {
    result = RpcPayload.fromAppReturn(withCallInterceptor(builder.pushCall.bind(builder), () => {
      return func(new RpcPromise(builder.makeInput(), []));
    }));
  } finally {
    builder.unregister();
  }

  // Detect misuse: Map callbacks cannot be async.
  if (result instanceof Promise) {
    // Squelch unhandled rejections from the map function itself -- it'll probably just throw
    // something about pulling a MapVariableHook.
    result.catch(err => {});

    // Throw an understandable error.
    throw new Error("RPC map() callbacks cannot be async.");
  }

  return new RpcPromise(builder.makeOutput(result), []);
}

function throwMapperBuilderUseError(): never {
  throw new Error(
      "Attempted to use an abstract placeholder from a mapper function. Please make sure your " +
      "map function has no side effects.");
}

// StubHook which represents a variable in a map function.
class MapVariableHook extends StubHook {
  constructor(public mapper: MapBuilder, public idx: number) {
    super();
  }

  // We don't have anything we actually need to dispose, so dup() can just return the same hook.
  dup(): StubHook { return this; }
  dispose(): void {}

  get(path: PropertyPath): StubHook {
    // This can actually be invoked as part of serialization, so we'll need to support it.
    if (path.length == 0) {
      // Since this hook cannot be pulled anyway, and dispose() is a no-op, we can actually just
      // return the same hook again to represent getting the empty path.
      return this;
    } else if (currentMapBuilder) {
      return currentMapBuilder.pushGet(this, path);
    } else {
      throwMapperBuilderUseError();
    }
  }

  // Other methods should never be called.
  call(path: PropertyPath, args: RpcPayload): StubHook {
    // Can't be called; all calls are intercepted.
    throwMapperBuilderUseError();
  }

  map(path: PropertyPath, captures: StubHook[], instructions: unknown[]): StubHook {
    // Can't be called; all map()s are intercepted.
    throwMapperBuilderUseError();
  }

  pull(): RpcPayload | Promise<RpcPayload> {
    // Map functions cannot await.
    throwMapperBuilderUseError();
  }

  ignoreUnhandledRejections(): void {
    // Probably never called but whatever.
  }

  onBroken(callback: (error: any) => void): void {
    throwMapperBuilderUseError();
  }
}

// =======================================================================================

class MapApplicator implements Importer {
  private variables: StubHook[];

  constructor(private captures: StubHook[], input: StubHook) {
    this.variables = [input];
  }

  dispose() {
    for (let variable of this.variables) {
      variable.dispose();
    }
  }

  apply(instructions: unknown[], returnRaw: boolean = false): RpcPayload | unknown {
    try {
      if (instructions.length < 1) {
        throw new Error("Invalid empty mapper function.");
      }

      for (let instruction of instructions.slice(0, -1)) {
        let payload = new Evaluator(this, true).evaluateCopy(instruction);

        // The payload almost always contains a single stub. As an optimization, unwrap it.
        if (payload.value instanceof RpcStub) {
          let hook = unwrapStubNoProperties(payload.value);
          if (hook) {
            this.variables.push(hook);
            continue;
          }
        }

        this.variables.push(new PayloadStubHook(payload));
      }

      const finalResult = new Evaluator(this, true).evaluateCopy(instructions[instructions.length - 1]);
      
      if (returnRaw) {
        // Return the raw value instead of the RpcPayload
        const value = finalResult.value;
        
        // Check if the result is a wrapper object from directInputHook
        if (value && typeof value === 'object' && 'value' in (value as any) && 'parent' in (value as any) && 'owner' in (value as any)) {
          // Unwrap the value from the wrapper object
          return (value as any).value;
        }
        
        // Check if the result is a Typegres object with toExpression method
        if (value && typeof value === 'object' && typeof (value as any).toExpression === 'function') {
          // This is likely a Typegres Bool or Any object, return it directly
          return value;
        }
        
        // Unwrap PayloadStubHook if present
        if (value && typeof value === 'object' && 'payload' in (value as any)) {
          const payload = (value as any).payload;
          if (payload && payload.value !== undefined) {
            return payload.value;
          }
        }
        
        return value;
      }
      
      return finalResult;
    } finally {
      for (let variable of this.variables) {
        variable.dispose();
      }
    }
  }

  importStub(idx: ImportId): StubHook {
    // This implies we saw an "export" appear inside the body of a mapper function. This should be
    // impossible because exportStub()/exportPromise() throw exceptions in MapBuilder.
    throw new Error("A mapper function cannot refer to exports.");
  }
  importPromise(idx: ImportId): StubHook {
    return this.importStub(idx);
  }

  getExport(idx: ExportId): StubHook | undefined {
    if (idx < 0) {
      return this.captures[-idx - 1];
    } else {
      return this.variables[idx];
    }
  }
}

function applyMapToElement(input: unknown, parent: object | undefined, owner: RpcPayload | null,
                           captures: StubHook[], instructions: unknown[]): RpcPayload {
  // TODO(perf): I wonder if we could use .fromAppParams() instead of .deepCopyFrom()? It
  //   maybe wouldn't correctly handle the case of RpcTargets in the input, so we need a variant
  //   which takes an `owner`, which does add some complexity.
  let inputHook = new PayloadStubHook(RpcPayload.deepCopyFrom(input, parent, owner));
  let mapper = new MapApplicator(captures, inputHook);
  try {
    return mapper.apply(instructions) as RpcPayload;
  } finally {
    mapper.dispose();
  }
}

mapImpl.applyMap = (input: unknown, parent: object | undefined, owner: RpcPayload | null,
                    captures: StubHook[], instructions: unknown[]) => {
  try {
    let result: RpcPayload;
    if (input instanceof RpcPromise) {
      // The caller is responsible for making sure the input is not a promise, since we can't
      // then know if it would resolve to an array later.
      throw new Error("applyMap() can't be called on RpcPromise");
    } else if (input instanceof Array) {
      let payloads: RpcPayload[] = [];
      try {
        for (let elem of input) {
          payloads.push(applyMapToElement(elem, input, owner, captures, instructions));
        }
      } catch (err) {
        for (let payload of payloads) {
          payload.dispose();
        }
        throw err;
      }

      result = RpcPayload.fromArray(payloads);
    } else if (input === null || input === undefined) {
      result = RpcPayload.fromAppReturn(input);
    } else {
      result = applyMapToElement(input, parent, owner, captures, instructions);
    }

    // TODO(perf): We should probably return a hook that allows pipelining but whose pull() doesn't
    //   resolve until all promises in the payload have been substituted.
    return new PayloadStubHook(result);
  } finally {
    for (let cap of captures) {
      cap.dispose();
    }
  }
}

mapImpl.applyMapToMethod = async (method: Function, parent: object | undefined, owner: RpcPayload | null,
                            captures: StubHook[], instructions: unknown[]) => {
  try {
    // Create a function that can be called by the method
    // This function will apply the recorded instructions to its input
    const callbackFunc = (input: unknown) => {
      
      // We need to duplicate captures for each invocation since they might be disposed
      let dupedCaptures = captures.map(cap => cap.dup());
      
      // Helper to resolve an RpcStub to its actual server-side instance
      const resolveStub = (stub: any): any => {
        if (!stub || typeof stub !== 'object') return stub;
        
        // If it's an RpcStub, try to get the actual object from its hook
        if ('raw' in stub) {
          const raw = stub.raw;
          if (raw?.hook) {
            const hook = raw.hook as any;
            
            // First try to get the target directly from the hook
            if (typeof hook.getTarget === 'function') {
              try {
                return hook.getTarget();
              } catch (e) {
                // Hook might be disposed, continue to check captures
              }
            } else if (hook.target !== undefined) {
              return hook.target;
            }
            
            // Check if this hook corresponds to one of our captures
            // The hook might be the same instance or a duplicate
            for (let i = 0; i < dupedCaptures.length; i++) {
              const captureHook = dupedCaptures[i] as any;
              // Check if hooks are the same or if they wrap the same target
              if (captureHook === hook || 
                  (captureHook.target && hook.target && captureHook.target === hook.target)) {
                // This is a capture, get the actual value from it
                if (typeof captureHook.getTarget === 'function') {
                  try {
                    return captureHook.getTarget();
                  } catch (e) {
                    // Fall through
                  }
                } else if (captureHook.target !== undefined) {
                  return captureHook.target;
                } else if (captureHook.getValue) {
                  try {
                    const val = captureHook.getValue();
                    return val?.value;
                  } catch (e) {
                    // Fall through
                  }
                }
              }
            }
          }
        }
        
        return stub;
      };
      
      // The input already contains the actual Typegres objects, so we can use them directly
      // Create a hook that can handle property access on Typegres objects
      const directInputHook = {
        get: (path: PropertyPath) => {
          if (path.length === 0) {
            return { value: input, parent: undefined, owner: null };
          }
          // Handle property access on the input object
          let current = input;
          for (const part of path) {
            if (current && typeof current === 'object' && part in current) {
              current = (current as any)[part];
            } else {
              return { value: undefined, parent: undefined, owner: null };
            }
          }
          return { value: current, parent: undefined, owner: null };
        },
        call: (path: PropertyPath, argsPayload: unknown) => {
          // Extract the actual arguments from the RpcPayload
          let args = (argsPayload as any)?.value || [];
          
          // Resolve any RpcStub arguments to their actual server-side instances
          // This is needed because when RPC stubs are passed as arguments (like tg),
          // we need to get the actual object on the server side
          // Note: tg is a regular RPC argument, not a callback, so it shouldn't go through map()
          args = args.map((arg: any) => resolveStub(arg));
          
          // Handle method calls on Typegres objects
          let current: any = input;
          let parent: any = input;
          
          // Navigate to the property, keeping track of the parent
          for (let i = 0; i < path.length; i++) {
            const part = path[i];
            if (current && typeof current === 'object' && part in current) {
              // For the last part, we want to keep the parent to use as 'this'
              if (i < path.length - 1) {
                parent = current;
              } else {
                parent = current; // The object that owns the method
              }
              current = current[part];
            } else {
              throw new Error(`Property ${part} not found`);
            }
          }
          
          // Call the method with the correct 'this' context
          if (typeof current === 'function') {
            // Before calling, resolve any RpcStub arguments from captures
            // Note: regular arguments like tg shouldn't go through map(), they're resolved here
            const finalArgs = args.map((arg: any) => resolveStub(arg));
            const result = current.apply(parent, finalArgs);
            return { value: result, parent: undefined, owner: null };
          } else {
            throw new Error(`Cannot call ${path.join('.')} - not a function`);
          }
        },
        map: () => { throw new Error("Not implemented"); },
        dup: () => directInputHook,
        dispose: () => {},
        pull: () => { throw new Error("Not implemented"); },
        ignoreUnhandledRejections: () => {},
        onBroken: () => { throw new Error("Not implemented"); }
      };
      
      let mapper = new MapApplicator(dupedCaptures, directInputHook as any);
      try {
        // Use returnRaw=true to get the raw value instead of wrapped in RpcPayload
        let value = mapper.apply(instructions, true);
        console.log("DBG callback result before unwrap:", typeof value, (value as any)?.constructor?.name, value);

        // Unwrap top-level wrapper returned by directInputHook.call
        if (value && typeof value === 'object' && 'value' in (value as any) && 'parent' in (value as any) && 'owner' in (value as any)) {
          console.log("DBG unwrap top-level directInput wrapper to:", (value as any).value?.constructor?.name);
          return (value as any).value;
        }

        // Unwrap top-level PayloadStubHook to its payload value if present
        if (value && typeof value === 'object' && 'payload' in (value as any)) {
          const payload = (value as any).payload;
          if (payload && payload.value !== undefined) {
            console.log("DBG unwrap top-level PayloadStubHook to:", payload.value?.constructor?.name);
            return payload.value;
          }
        }

        // Unwrap top-level RpcStub/RpcPromise carrying a PayloadStubHook
        if (value && typeof value === 'object' && 'raw' in (value as any)) {
          const raw = (value as any).raw;
          if (raw?.hook instanceof PayloadStubHook) {
            const payload = (raw.hook as any).payload;
            if (payload && payload.value !== undefined) {
              console.log("DBG unwrap top-level RpcPromise->PayloadStubHook to:", payload.value?.constructor?.name);
              return payload.value;
            }
          }
        }

        // The MapApplicator.apply method now handles Typegres object unwrapping

        // If value looks like a Typegres value/expression, return it directly
        if (value && typeof value === 'object' && (typeof (value as any).toExpression === 'function' || typeof (value as any).getClass === 'function')) {
          return value;
        }

        // Check if we need to unwrap anything inside plain objects
        if (value && typeof value === 'object') {
          const rawValue: any = {};
          for (const [k, v] of Object.entries(value)) {
            // Preserve Typegres instances as-is
            if (v && typeof v === 'object' && (typeof (v as any).toExpression === 'function' || typeof (v as any).getClass === 'function')) {
              rawValue[k] = v;
              continue;
            }
            
            // Check if this is a wrapper object from directInputHook
            if (v && typeof v === 'object' && 'value' in v && 'parent' in v && 'owner' in v) {
              rawValue[k] = (v as any).value;
              continue;
            }
            
            // Check if this is a PayloadStubHook directly
            if (v && typeof v === 'object' && 'payload' in v) {
              const payload = (v as any).payload;
              if (payload && payload.value !== undefined) {
                rawValue[k] = payload.value;
                continue;
              }
            }
            
            // Check if this is an RpcStub or RpcPromise
            if (v && typeof v === 'object' && 'raw' in v) {
              const raw = (v as any).raw;
              
              // Try to extract the actual value from the PayloadStubHook
              if (raw?.hook instanceof PayloadStubHook) {
                const payload = (raw.hook as any).payload;
                if (payload) {
                  rawValue[k] = payload.value;
                  continue;
                }
              }
              
              // If it's an error stub, we can't unwrap it
              if (raw?.hook?.error) {
                // Return null or undefined for error stubs
                rawValue[k] = undefined;
              } else {
                // Pass through if we can't unwrap
                rawValue[k] = v;
              }
            } else {
              // Not wrapped, use as-is
              rawValue[k] = v;
            }
          }
          console.log("DBG rawValue:", rawValue, rawValue.constructor?.name);
          return rawValue;
        }
        
        // The result contains promises that need to be preserved
        return value;
      } finally {
        mapper.dispose();
      }
    };

    // Call the method with the callback function
    let result = method.call(parent, callbackFunc);
    
    // If the result is a Promise, await it before creating the payload
    // This prevents serialization errors when trying to send a Promise over RPC
    if (result instanceof Promise) {
      result = await result;
    }
    
    const payload = RpcPayload.fromAppReturn(result);
    return new PayloadStubHook(payload);
  } finally {
    for (let cap of captures) {
      cap.dispose();
    }
  }
}

export function forceInitMap() {}

// Overwrite the isInMapContext function from core.ts
mapImpl.isInMapContext = (): boolean => {
  return currentMapBuilder !== undefined;
};
