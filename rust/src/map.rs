// JS: // Copyright (c) 2025 Cloudflare, Inc.
// JS: // Licensed under the MIT license found in the LICENSE.txt file or at:
// JS: //     https://opensource.org/license/mit
// JS:
// JS: import { StubHook, PropertyPath, RpcPayload, RpcStub, RpcPromise, withCallInterceptor, ErrorStubHook, mapImpl, PayloadStubHook, unwrapStubAndPath, unwrapStubNoProperties } from "./core.js";
// JS: import { Devaluator, Exporter, Importer, ExportId, ImportId, Evaluator, RpcLimits, DEFAULT_LIMITS } from "./serialize.js";
// JS:
// JS: let currentMapBuilder: MapBuilder | undefined;
// JS:
// JS: // We use this type signature when building the instructions for type checking purposes. It
// JS: // describes a subset of the overall RPC protocol.
// JS: export type MapInstruction =
// JS:     | ["pipeline", number, PropertyPath]
// JS:     | ["pipeline", number, PropertyPath, unknown]
// JS:     | ["remap", number, PropertyPath, ["import", number][], MapInstruction[]]
// JS:
// JS: class MapBuilder implements Exporter {
// JS:   private context:
// JS:     | {parent: undefined, captures: StubHook[], subject: StubHook, path: PropertyPath}
// JS:     | {parent: MapBuilder, captures: number[], subject: number, path: PropertyPath};
// JS:   private captureMap: Map<StubHook, number> = new Map();
// JS:
// JS:   private instructions: MapInstruction[] = [];
// JS:
// JS:   constructor(subject: StubHook, path: PropertyPath) {
// JS:     if (currentMapBuilder) {
// JS:       this.context = {
// JS:         parent: currentMapBuilder,
// JS:         captures: [],
// JS:         subject: currentMapBuilder.capture(subject),
// JS:         path
// JS:       };
// JS:     } else {
// JS:       this.context = {
// JS:         parent: undefined,
// JS:         captures: [],
// JS:         subject,
// JS:         path
// JS:       };
// JS:     }
// JS:
// JS:     currentMapBuilder = this;
// JS:   }
// JS:
// JS:   unregister() {
// JS:     currentMapBuilder = this.context.parent;
// JS:   }
// JS:
// JS:   makeInput(): MapVariableHook {
// JS:     return new MapVariableHook(this, 0);
// JS:   }
// JS:
// JS:   makeOutput(result: RpcPayload): StubHook {
// JS:     let devalued: unknown;
// JS:     try {
// JS:       devalued = Devaluator.devaluate(result.value, undefined, this, result);
// JS:     } finally {
// JS:       result.dispose();
// JS:     }
// JS:
// JS:     // The result is the final instruction. This doesn't actually fit our MapInstruction type
// JS:     // signature, so we cheat a bit.
// JS:     this.instructions.push(<any>devalued);
// JS:
// JS:     if (this.context.parent) {
// JS:       this.context.parent.instructions.push(
// JS:         ["remap", this.context.subject, this.context.path,
// JS:                   this.context.captures.map(cap => ["import", cap]),
// JS:                   this.instructions]
// JS:       );
// JS:       return new MapVariableHook(this.context.parent, this.context.parent.instructions.length);
// JS:     } else {
// JS:       return this.context.subject.map(this.context.path, this.context.captures, this.instructions);
// JS:     }
// JS:   }
// JS:
// JS:   pushCall(hook: StubHook, path: PropertyPath, params: RpcPayload): StubHook {
// JS:     let devalued = Devaluator.devaluate(params.value, undefined, this, params);
// JS:     // HACK: Since the args is an array, devaluator will wrap in a second array. Need to unwrap.
// JS:     // TODO: Clean this up somehow.
// JS:     devalued = (<Array<unknown>>devalued)[0];
// JS:
// JS:     let subject = this.capture(hook.dup());
// JS:     this.instructions.push(["pipeline", subject, path, devalued]);
// JS:     return new MapVariableHook(this, this.instructions.length);
// JS:   }
// JS:
// JS:   pushGet(hook: StubHook, path: PropertyPath): StubHook {
// JS:     let subject = this.capture(hook.dup());
// JS:     this.instructions.push(["pipeline", subject, path]);
// JS:     return new MapVariableHook(this, this.instructions.length);
// JS:   }
// JS:
// JS:   capture(hook: StubHook): number {
// JS:     if (hook instanceof MapVariableHook && hook.mapper === this) {
// JS:       // Oh, this is already our own hook.
// JS:       return hook.idx;
// JS:     }
// JS:
// JS:     // TODO: Well, the hooks passed in are always unique, so they'll never exist in captureMap.
// JS:     //   I suppose this is a problem with RPC as well. We need a way to identify hooks that are
// JS:     //   dupes of the same target.
// JS:     let result = this.captureMap.get(hook);
// JS:     if (result === undefined) {
// JS:       if (this.context.parent) {
// JS:         let parentIdx = this.context.parent.capture(hook);
// JS:         this.context.captures.push(parentIdx);
// JS:       } else {
// JS:         this.context.captures.push(hook);
// JS:       }
// JS:       result = -this.context.captures.length;
// JS:       this.captureMap.set(hook, result);
// JS:     }
// JS:     return result;
// JS:   }
// JS:
// JS:   // ---------------------------------------------------------------------------
// JS:   // implements Exporter
// JS:
// JS:   exportStub(hook: StubHook): ExportId {
// JS:     // It appears someone did something like:
// JS:     //
// JS:     //     stub.map(x => { return x.doSomething(new MyRpcTarget()); })
// JS:     //
// JS:     // That... won't work. They need to do this instead:
// JS:     //
// JS:     //     using myTargetStub = new RpcStub(new MyRpcTarget());
// JS:     //     stub.map(x => { return x.doSomething(myTargetStub.dup()); })
// JS:     //
// JS:     // TODO(someday): Consider carefully if the inline syntax is maybe OK. If so, perhaps the
// JS:     //   serializer could try calling `getImport()` even for known-local hooks.
// JS:     // TODO(someday): Do we need to support rpc-thenable somehow?
// JS:     throw new Error(
// JS:         "Can't construct an RpcTarget or RPC callback inside a mapper function. Try creating a " +
// JS:         "new RpcStub outside the callback first, then using it inside the callback.");
// JS:   }
// JS:   exportPromise(hook: StubHook): ExportId {
// JS:     return this.exportStub(hook);
// JS:   }
// JS:   getImport(hook: StubHook): ImportId | undefined {
// JS:     return this.capture(hook);
// JS:   }
// JS:
// JS:   unexport(ids: Array<ExportId>): void {
// JS:     // Presumably this MapBuilder is cooked anyway, so we don't really have to release anything.
// JS:   }
// JS:
// JS:   createPipe(readable: ReadableStream): never {
// JS:     throw new Error("Cannot send ReadableStream inside a mapper function.");
// JS:   }
// JS:
// JS:   onSendError(error: Error): Error | void {
// JS:     // TODO(someday): Can we use the error-sender hook from the RPC system somehow?
// JS:   }
// JS: };
// JS:
// JS: mapImpl.sendMap = (hook: StubHook, path: PropertyPath, func: (promise: RpcPromise) => unknown) => {
// JS:   let builder = new MapBuilder(hook, path);
// JS:   let result: RpcPayload;
// JS:   try {
// JS:     result = RpcPayload.fromAppReturn(withCallInterceptor(builder.pushCall.bind(builder), () => {
// JS:       return func(new RpcPromise(builder.makeInput(), []));
// JS:     }));
// JS:   } finally {
// JS:     builder.unregister();
// JS:   }
// JS:
// JS:   // Detect misuse: Map callbacks cannot be async.
// JS:   if (result instanceof Promise) {
// JS:     // Squelch unhandled rejections from the map function itself -- it'll probably just throw
// JS:     // something about pulling a MapVariableHook.
// JS:     result.catch(err => {});
// JS:
// JS:     // Throw an understandable error.
// JS:     throw new Error("RPC map() callbacks cannot be async.");
// JS:   }
// JS:
// JS:   return new RpcPromise(builder.makeOutput(result), []);
// JS: }
// JS:
// JS: function throwMapperBuilderUseError(): never {
// JS:   throw new Error(
// JS:       "Attempted to use an abstract placeholder from a mapper function. Please make sure your " +
// JS:       "map function has no side effects.");
// JS: }
// JS:
// JS: // StubHook which represents a variable in a map function.
// JS: class MapVariableHook extends StubHook {
// JS:   constructor(public mapper: MapBuilder, public idx: number) {
// JS:     super();
// JS:   }
// JS:
// JS:   // We don't have anything we actually need to dispose, so dup() can just return the same hook.
// JS:   dup(): StubHook { return this; }
// JS:   dispose(): void {}
// JS:
// JS:   get(path: PropertyPath): StubHook {
// JS:     // This can actually be invoked as part of serialization, so we'll need to support it.
// JS:     if (path.length == 0) {
// JS:       // Since this hook cannot be pulled anyway, and dispose() is a no-op, we can actually just
// JS:       // return the same hook again to represent getting the empty path.
// JS:       return this;
// JS:     } else if (currentMapBuilder) {
// JS:       return currentMapBuilder.pushGet(this, path);
// JS:     } else {
// JS:       throwMapperBuilderUseError();
// JS:     }
// JS:   }
// JS:
// JS:   // Other methods should never be called.
// JS:   call(path: PropertyPath, args: RpcPayload): StubHook {
// JS:     // Can't be called; all calls are intercepted. We still own `args` per the StubHook contract.
// JS:     args.dispose();
// JS:     throwMapperBuilderUseError();
// JS:   }
// JS:
// JS:   map(path: PropertyPath, captures: StubHook[], instructions: unknown[]): StubHook {
// JS:     // Can't be called; all map()s are intercepted. We still own the captures per the StubHook
// JS:     // contract.
// JS:     for (let cap of captures) {
// JS:       cap.dispose();
// JS:     }
// JS:     throwMapperBuilderUseError();
// JS:   }
// JS:
// JS:   pull(): RpcPayload | Promise<RpcPayload> {
// JS:     // Map functions cannot await.
// JS:     throwMapperBuilderUseError();
// JS:   }
// JS:
// JS:   ignoreUnhandledRejections(): void {
// JS:     // Probably never called but whatever.
// JS:   }
// JS:
// JS:   onBroken(callback: (error: any) => void): void {
// JS:     throwMapperBuilderUseError();
// JS:   }
// JS: }
// JS:
// JS: // =======================================================================================
// JS:
// JS: class MapApplicator implements Importer {
// JS:   private variables: StubHook[];
// JS:
// JS:   constructor(private captures: StubHook[], input: StubHook) {
// JS:     this.variables = [input];
// JS:   }
// JS:
// JS:   dispose() {
// JS:     for (let variable of this.variables) {
// JS:       variable.dispose();
// JS:     }
// JS:   }
// JS:
// JS:   apply(instructions: unknown[]): RpcPayload {
// JS:     try {
// JS:       if (instructions.length < 1) {
// JS:         throw new Error("Invalid empty mapper function.");
// JS:       }
// JS:
// JS:       for (let instruction of instructions.slice(0, -1)) {
// JS:         let payload = new Evaluator(this).evaluateCopy(instruction);
// JS:
// JS:         // The payload almost always contains a single stub. As an optimization, unwrap it.
// JS:         if (payload.value instanceof RpcStub) {
// JS:           let hook = unwrapStubNoProperties(payload.value);
// JS:           if (hook) {
// JS:             this.variables.push(hook);
// JS:             continue;
// JS:           }
// JS:         }
// JS:
// JS:         this.variables.push(new PayloadStubHook(payload));
// JS:       }
// JS:
// JS:       return new Evaluator(this).evaluateCopy(instructions[instructions.length - 1]);
// JS:     } finally {
// JS:       for (let variable of this.variables) {
// JS:         variable.dispose();
// JS:       }
// JS:     }
// JS:   }
// JS:
// JS:   importStub(idx: ImportId): StubHook {
// JS:     // This implies we saw an "export" appear inside the body of a mapper function. This should be
// JS:     // impossible because exportStub()/exportPromise() throw exceptions in MapBuilder.
// JS:     throw new Error("A mapper function cannot refer to exports.");
// JS:   }
// JS:   importPromise(idx: ImportId): StubHook {
// JS:     return this.importStub(idx);
// JS:   }
// JS:
// JS:   getExport(idx: ExportId): StubHook | undefined {
// JS:     if (idx < 0) {
// JS:       return this.captures[-idx - 1];
// JS:     } else {
// JS:       return this.variables[idx];
// JS:     }
// JS:   }
// JS:
// JS:   getPipeReadable(exportId: ExportId): never {
// JS:     throw new Error("A mapper function cannot use pipe readables.");
// JS:   }
// JS:
// JS:   getLimits(): RpcLimits {
// JS:     // A mapper's instructions arrived inside a ["remap"] message that was itself deserialized
// JS:     // under the session's limits, so the default floor is sufficient protection here without
// JS:     // threading per-session overrides through the several layers between the session and the
// JS:     // mapper.
// JS:     return DEFAULT_LIMITS;
// JS:   }
// JS: }
// JS:
// JS: function applyMapToElement(input: unknown, parent: object | undefined, owner: RpcPayload | null,
// JS:                            captures: StubHook[], instructions: unknown[]): RpcPayload {
// JS:   // TODO(perf): I wonder if we could use .fromAppParams() instead of .deepCopyFrom()? It
// JS:   //   maybe wouldn't correctly handle the case of RpcTargets in the input, so we need a variant
// JS:   //   which takes an `owner`, which does add some complexity.
// JS:   let inputHook = new PayloadStubHook(RpcPayload.deepCopyFrom(input, parent, owner));
// JS:   let mapper = new MapApplicator(captures, inputHook);
// JS:   try {
// JS:     return mapper.apply(instructions);
// JS:   } finally {
// JS:     mapper.dispose();
// JS:   }
// JS: }
// JS:
// JS: mapImpl.applyMap = (input: unknown, parent: object | undefined, owner: RpcPayload | null,
// JS:                     captures: StubHook[], instructions: unknown[]) => {
// JS:   try {
// JS:     let result: RpcPayload;
// JS:     if (input instanceof RpcPromise) {
// JS:       // The caller is responsible for making sure the input is not a promise, since we can't
// JS:       // then know if it would resolve to an array later.
// JS:       throw new Error("applyMap() can't be called on RpcPromise");
// JS:     } else if (input instanceof Array) {
// JS:       let payloads: RpcPayload[] = [];
// JS:       try {
// JS:         for (let elem of input) {
// JS:           payloads.push(applyMapToElement(elem, input, owner, captures, instructions));
// JS:         }
// JS:       } catch (err) {
// JS:         for (let payload of payloads) {
// JS:           payload.dispose();
// JS:         }
// JS:         throw err;
// JS:       }
// JS:
// JS:       result = RpcPayload.fromArray(payloads);
// JS:     } else if (input === null || input === undefined) {
// JS:       result = RpcPayload.fromAppReturn(input);
// JS:     } else {
// JS:       result = applyMapToElement(input, parent, owner, captures, instructions);
// JS:     }
// JS:
// JS:     // TODO(perf): We should probably return a hook that allows pipelining but whose pull() doesn't
// JS:     //   resolve until all promises in the payload have been substituted.
// JS:     return new PayloadStubHook(result);
// JS:   } finally {
// JS:     for (let cap of captures) {
// JS:       cap.dispose();
// JS:     }
// JS:   }
// JS: }
// JS:
// JS: export function forceInitMap() {}
