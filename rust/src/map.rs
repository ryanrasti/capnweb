// JS: // Copyright (c) 2025 Cloudflare, Inc.
// JS: // Licensed under the MIT license found in the LICENSE.txt file or at:
// JS: //     https://opensource.org/license/mit
// JS:
// JS: import { StubHook, PropertyPath, RpcPayload, RpcStub, RpcPromise, withCallInterceptor, ErrorStubHook, mapImpl, PayloadStubHook, unwrapStubAndPath, unwrapStubNoProperties } from "./core.js";
use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::core::{
    CapnwebError, CapnwebValue, Error, MapImpl, MapInstructions, MaybePromise, PayloadStubHook,
    PropertyKey, PropertyPath, Result, RpcPayload, RpcPromise, StubHook, StubHookInner,
    install_map_impl, unwrap_stub_no_properties, with_call_interceptor,
};
// JS: import { Devaluator, Exporter, Importer, ExportId, ImportId, Evaluator, RpcLimits, DEFAULT_LIMITS } from "./serialize.js";
use crate::serialize::{
    DEFAULT_LIMITS, Devaluator, EncodingLevel, Evaluator, ExportId, Exporter, ImportId, Importer,
    RpcLimits,
};
// JS:
// JS: let currentMapBuilder: MapBuilder | undefined;
thread_local! {
    static CURRENT_MAP_BUILDER: RefCell<Option<Arc<Mutex<MapBuilder>>>> = const { RefCell::new(None) };
}

static NEXT_MAP_BUILDER_ID: AtomicU64 = AtomicU64::new(1);
// JS:
// JS: // We use this type signature when building the instructions for type checking purposes. It
// JS: // describes a subset of the overall RPC protocol.
// JS: export type MapInstruction =
/// One recorded mapper instruction encoded as a dynamic wire expression.
pub type MapInstruction = CapnwebValue;

fn path_to_value(path: PropertyPath) -> CapnwebValue {
    CapnwebValue::Array(
        path.into_iter()
            .map(|part| match part {
                PropertyKey::String(value) => CapnwebValue::String(value),
                PropertyKey::Number(value) => CapnwebValue::Number(value),
            })
            .collect(),
    )
}
// JS:     | ["pipeline", number, PropertyPath]
// JS:     | ["pipeline", number, PropertyPath, unknown]
// JS:     | ["remap", number, PropertyPath, ["import", number][], MapInstruction[]]
// JS:
// JS: class MapBuilder implements Exporter {
enum MapContext {
    Root {
        captures: Vec<StubHook>,
        subject: StubHook,
        path: PropertyPath,
    },
    Nested {
        parent: Arc<Mutex<MapBuilder>>,
        captures: Vec<i64>,
        subject: i64,
        path: PropertyPath,
    },
}

struct MapBuilder {
    id: u64,
    context: MapContext,
    capture_map: Vec<(StubHook, i64)>,
    instructions: Vec<MapInstruction>,
}
// JS:   private context:
// JS:     | {parent: undefined, captures: StubHook[], subject: StubHook, path: PropertyPath}
// JS:     | {parent: MapBuilder, captures: number[], subject: number, path: PropertyPath};
// JS:   private captureMap: Map<StubHook, number> = new Map();
// `capture_map` uses `Arc::ptr_eq()` because trait-object pointers are not hash keys.
// JS:
// JS:   private instructions: MapInstruction[] = [];
// Initialized by the Rust constructor below.
// JS:
// JS:   constructor(subject: StubHook, path: PropertyPath) {
impl MapBuilder {
    fn new(subject: StubHook, path: PropertyPath) -> Arc<Mutex<Self>> {
        let parent = CURRENT_MAP_BUILDER.with(|current| current.borrow().clone());
        // JS:     if (currentMapBuilder) {
        let context = if let Some(parent) = parent {
            // JS:       this.context = {
            // JS:         parent: currentMapBuilder,
            // JS:         captures: [],
            // JS:         subject: currentMapBuilder.capture(subject),
            let subject = parent
                .lock()
                .expect("map builder mutex poisoned")
                .capture(subject);
            // JS:         path
            // JS:       };
            MapContext::Nested {
                parent,
                captures: Vec::new(),
                subject,
                path,
            }
        // JS:     } else {
        } else {
            // JS:       this.context = {
            // JS:         parent: undefined,
            // JS:         captures: [],
            // JS:         subject,
            // JS:         path
            // JS:       };
            MapContext::Root {
                captures: Vec::new(),
                subject,
                path,
            }
            // JS:     }
        };
        // JS:
        // JS:     currentMapBuilder = this;
        let builder = Arc::new(Mutex::new(Self {
            id: NEXT_MAP_BUILDER_ID.fetch_add(1, Ordering::Relaxed),
            context,
            capture_map: Vec::new(),
            instructions: Vec::new(),
        }));
        CURRENT_MAP_BUILDER.with(|current| current.replace(Some(Arc::clone(&builder))));
        builder
        // JS:   }
    }
    // JS:
    // JS:   unregister() {
    fn unregister(&self) {
        // JS:     currentMapBuilder = this.context.parent;
        let parent = match &self.context {
            MapContext::Root { .. } => None,
            MapContext::Nested { parent, .. } => Some(Arc::clone(parent)),
        };
        CURRENT_MAP_BUILDER.with(|current| current.replace(parent));
        // JS:   }
    }
    // JS:
    // JS:   makeInput(): MapVariableHook {
    fn make_input(builder: &Arc<Mutex<Self>>) -> StubHook {
        // JS:     return new MapVariableHook(this, 0);
        let id = builder.lock().expect("map builder mutex poisoned").id;
        Arc::new(MapVariableHook {
            mapper: Arc::clone(builder),
            mapper_id: id,
            index: 0,
        })
        // JS:   }
    }
    // JS:
    // JS:   makeOutput(result: RpcPayload): StubHook {
    fn make_output(builder: &Arc<Mutex<Self>>, result: RpcPayload) -> Result<StubHook> {
        // JS:     let devalued: unknown;
        // JS:     try {
        // JS:       devalued = Devaluator.devaluate(result.value, undefined, this, result);
        let mut builder = builder.lock().expect("map builder mutex poisoned");
        let devalued = Devaluator::devaluate(
            &result.value,
            &mut *builder,
            Some(&result),
            EncodingLevel::String,
        )?;
        // JS:     } finally {
        // JS:       result.dispose();
        // JS:     }
        // JS:
        // JS:     // The result is the final instruction. This doesn't actually fit our MapInstruction type
        // JS:     // signature, so we cheat a bit.
        // JS:     this.instructions.push(<any>devalued);
        builder.instructions.push(devalued);
        // JS:
        // JS:     if (this.context.parent) {
        let context = match &builder.context {
            MapContext::Nested {
                parent,
                captures,
                subject,
                path,
            } => Some((Arc::clone(parent), captures.clone(), *subject, path.clone())),
            MapContext::Root { .. } => None,
        };
        if let Some((parent, captures, subject, path)) = context {
            // JS:       this.context.parent.instructions.push(
            let captures = captures
                .into_iter()
                .map(|capture| {
                    CapnwebValue::Array(vec![
                        CapnwebValue::String("import".to_owned()),
                        CapnwebValue::Number(capture as f64),
                    ])
                })
                .collect();
            let instruction = CapnwebValue::Array(vec![
                CapnwebValue::String("remap".to_owned()),
                CapnwebValue::Number(subject as f64),
                path_to_value(path),
                CapnwebValue::Array(captures),
                CapnwebValue::Array(builder.instructions.clone()),
            ]);
            let parent_handle = Arc::clone(&parent);
            let mut parent = parent.lock().expect("map builder mutex poisoned");
            parent.instructions.push(instruction);
            // JS:         ["remap", this.context.subject, this.context.path,
            // JS:                   this.context.captures.map(cap => ["import", cap]),
            // JS:                   this.instructions]
            // JS:       );
            // JS:       return new MapVariableHook(this.context.parent, this.context.parent.instructions.length);
            let index = parent.instructions.len() as i64;
            let mapper_id = parent.id;
            return Ok(Arc::new(MapVariableHook {
                mapper: parent_handle,
                mapper_id,
                index,
            }));
            // JS:     } else {
        }
        // JS:       return this.context.subject.map(this.context.path, this.context.captures, this.instructions);
        let MapContext::Root {
            captures,
            subject,
            path,
        } = &mut builder.context
        else {
            unreachable!();
        };
        let captures = std::mem::take(captures);
        let subject = Arc::clone(subject);
        let path = path.clone();
        let instructions = std::mem::take(&mut builder.instructions);
        subject.map(path, captures, instructions)
        // JS:     }
        // JS:   }
    }
    // JS:
    // JS:   pushCall(hook: StubHook, path: PropertyPath, params: RpcPayload): StubHook {
    fn push_call(
        builder: &Arc<Mutex<Self>>,
        hook: StubHook,
        path: PropertyPath,
        params: RpcPayload,
    ) -> Result<StubHook> {
        let mut this = builder.lock().expect("map builder mutex poisoned");
        // JS:     let devalued = Devaluator.devaluate(params.value, undefined, this, params);
        let devalued = Devaluator::devaluate(
            &params.value,
            &mut *this,
            Some(&params),
            EncodingLevel::String,
        )?;
        // JS:     // HACK: Since the args is an array, devaluator will wrap in a second array. Need to unwrap.
        // JS:     // TODO: Clean this up somehow.
        // JS:     devalued = (<Array<unknown>>devalued)[0];
        let CapnwebValue::Array(mut wrapped) = devalued else {
            return Err(Error::Type(
                "map call arguments were not an array".to_owned(),
            ));
        };
        let devalued = wrapped.remove(0);
        // JS:
        // JS:     let subject = this.capture(hook.dup());
        let subject = this.capture(hook);
        // JS:     this.instructions.push(["pipeline", subject, path, devalued]);
        this.instructions.push(CapnwebValue::Array(vec![
            CapnwebValue::String("pipeline".to_owned()),
            CapnwebValue::Number(subject as f64),
            path_to_value(path),
            devalued,
        ]));
        // JS:     return new MapVariableHook(this, this.instructions.length);
        let index = this.instructions.len() as i64;
        Ok(Arc::new(MapVariableHook {
            mapper: Arc::clone(builder),
            mapper_id: this.id,
            index,
        }))
        // JS:   }
    }
    // JS:
    // JS:   pushGet(hook: StubHook, path: PropertyPath): StubHook {
    fn push_get(
        builder: &Arc<Mutex<Self>>,
        hook: StubHook,
        path: PropertyPath,
    ) -> Result<StubHook> {
        let mut this = builder.lock().expect("map builder mutex poisoned");
        // JS:     let subject = this.capture(hook.dup());
        let subject = this.capture(hook);
        // JS:     this.instructions.push(["pipeline", subject, path]);
        this.instructions.push(CapnwebValue::Array(vec![
            CapnwebValue::String("pipeline".to_owned()),
            CapnwebValue::Number(subject as f64),
            path_to_value(path),
        ]));
        // JS:     return new MapVariableHook(this, this.instructions.length);
        let index = this.instructions.len() as i64;
        Ok(Arc::new(MapVariableHook {
            mapper: Arc::clone(builder),
            mapper_id: this.id,
            index,
        }))
        // JS:   }
    }
    // JS:
    // JS:   capture(hook: StubHook): number {
    fn capture(&mut self, hook: StubHook) -> i64 {
        // JS:     if (hook instanceof MapVariableHook && hook.mapper === this) {
        // JS:       // Oh, this is already our own hook.
        // JS:       return hook.idx;
        if let Some((mapper_id, index)) = hook.map_variable() {
            if mapper_id == self.id {
                return index;
            }
        }
        // JS:     }
        // JS:
        // JS:     // TODO: Well, the hooks passed in are always unique, so they'll never exist in captureMap.
        // JS:     //   I suppose this is a problem with RPC as well. We need a way to identify hooks that are
        // JS:     //   dupes of the same target.
        // JS:     let result = this.captureMap.get(hook);
        if let Some((_, result)) = self
            .capture_map
            .iter()
            .find(|(known, _)| Arc::ptr_eq(known, &hook))
        {
            return *result;
        }
        let result;
        // JS:     if (result === undefined) {
        // JS:       if (this.context.parent) {
        // JS:         let parentIdx = this.context.parent.capture(hook);
        if let MapContext::Nested {
            parent, captures, ..
        } = &mut self.context
        {
            let parent_idx = parent
                .lock()
                .expect("map builder mutex poisoned")
                .capture(Arc::clone(&hook));
            // JS:         this.context.captures.push(parentIdx);
            captures.push(parent_idx);
        // JS:       } else {
        } else if let MapContext::Root { captures, .. } = &mut self.context {
            // JS:         this.context.captures.push(hook);
            captures.push(Arc::clone(&hook));
            // JS:       }
        }
        // JS:       result = -this.context.captures.length;
        result = match &self.context {
            MapContext::Root { captures, .. } => -(captures.len() as i64),
            MapContext::Nested { captures, .. } => -(captures.len() as i64),
        };
        // JS:       this.captureMap.set(hook, result);
        self.capture_map.push((hook, result));
        // JS:     }
        // JS:     return result;
        result
        // JS:   }
    }
}
// JS:
// JS:   // ---------------------------------------------------------------------------
// JS:   // implements Exporter
// JS:
impl Exporter for MapBuilder {
    // JS:   exportStub(hook: StubHook): ExportId {
    fn export_stub(&mut self, _hook: StubHook) -> Result<ExportId> {
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
        Err(Error::Message(
            "Can't construct an RpcTarget or RPC callback inside a mapper function. Try creating a new RpcStub outside the callback first, then using it inside the callback.".to_owned(),
        ))
        // JS:   }
    }
    // JS:   exportPromise(hook: StubHook): ExportId {
    fn export_promise(&mut self, hook: StubHook) -> Result<ExportId> {
        // JS:     return this.exportStub(hook);
        self.export_stub(hook)
        // JS:   }
    }
    // JS:   getImport(hook: StubHook): ImportId | undefined {
    fn get_import(&mut self, hook: &StubHook) -> Option<ImportId> {
        // JS:     return this.capture(hook);
        Some(self.capture(Arc::clone(hook)))
        // JS:   }
    }
    // JS:
    // JS:   unexport(ids: Array<ExportId>): void {
    fn unexport(&mut self, _ids: Vec<ExportId>) -> Result<()> {
        // JS:     // Presumably this MapBuilder is cooked anyway, so we don't really have to release anything.
        Ok(())
        // JS:   }
    }
    // JS:
    // JS:   createPipe(readable: ReadableStream): never {
    // JS:     throw new Error("Cannot send ReadableStream inside a mapper function.");
    // JS:   }
    // JS:
    // JS:   onSendError(error: Error): Error | void {
    fn on_send_error(&mut self, _error: CapnwebError) -> Option<CapnwebError> {
        // JS:     // TODO(someday): Can we use the error-sender hook from the RPC system somehow?
        None
        // JS:   }
    }
    // JS: };
}
// JS:
struct MapRuntime;

impl MapRuntime {
    // JS: mapImpl.sendMap = (hook: StubHook, path: PropertyPath, func: (promise: RpcPromise) => unknown) => {
    fn record_map(
        &self,
        hook: StubHook,
        path: PropertyPath,
        function: Box<dyn FnOnce(RpcPromise) -> Result<CapnwebValue> + Send>,
    ) -> Result<RpcPromise> {
        // JS:   let builder = new MapBuilder(hook, path);
        let builder = MapBuilder::new(hook, path);
        struct RegistrationGuard(Option<Arc<Mutex<MapBuilder>>>);
        impl RegistrationGuard {
            fn unregister(&mut self) {
                if let Some(builder) = self.0.take() {
                    builder
                        .lock()
                        .expect("map builder mutex poisoned")
                        .unregister();
                }
            }
        }
        impl Drop for RegistrationGuard {
            fn drop(&mut self) {
                self.unregister();
            }
        }
        let mut registration = RegistrationGuard(Some(Arc::clone(&builder)));
        // JS:   let result: RpcPayload;
        // JS:   try {
        // JS:     result = RpcPayload.fromAppReturn(withCallInterceptor(builder.pushCall.bind(builder), () => {
        let interceptor_builder = Arc::clone(&builder);
        let interceptor = Arc::new(move |hook, path, params| {
            MapBuilder::push_call(&interceptor_builder, hook, path, params)
        });
        let value = with_call_interceptor(interceptor, || {
            // JS:       return func(new RpcPromise(builder.makeInput(), []));
            function(RpcPromise::from_hook(
                MapBuilder::make_input(&builder),
                Vec::new(),
            ))
            // JS:     }));
        });
        // JS:   } finally {
        // JS:     builder.unregister();
        registration.unregister();
        // JS:   }
        let result = RpcPayload::from_app_return(value?);
        // JS:
        // JS:   // Detect misuse: Map callbacks cannot be async.
        // Rust's callback signature rejects async mapper callbacks at compile time.
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
        Ok(RpcPromise::from_hook(
            MapBuilder::make_output(&builder, result)?,
            Vec::new(),
        ))
        // JS: }
    }
}
// JS:
// JS: function throwMapperBuilderUseError(): never {
fn mapper_builder_use_error() -> Error {
    // JS:   throw new Error(
    // JS:       "Attempted to use an abstract placeholder from a mapper function. Please make sure your " +
    // JS:       "map function has no side effects.");
    Error::Message(
        "Attempted to use an abstract placeholder from a mapper function. Please make sure your map function has no side effects.".to_owned(),
    )
    // JS: }
}
// JS:
// JS: // StubHook which represents a variable in a map function.
// JS: class MapVariableHook extends StubHook {
struct MapVariableHook {
    mapper: Arc<Mutex<MapBuilder>>,
    mapper_id: u64,
    index: i64,
}

impl StubHookInner for MapVariableHook {
    fn map_variable(&self) -> Option<(u64, i64)> {
        Some((self.mapper_id, self.index))
    }
    // JS:   constructor(public mapper: MapBuilder, public idx: number) {
    // JS:     super();
    // JS:   }
    // JS:
    // JS:   // We don't have anything we actually need to dispose, so dup() can just return the same hook.
    // JS:   dup(): StubHook { return this; }
    // JS:   dispose(): void {}
    // JS:
    // JS:   get(path: PropertyPath): StubHook {
    fn get(&self, path: PropertyPath) -> Result<StubHook> {
        // JS:     // This can actually be invoked as part of serialization, so we'll need to support it.
        // JS:     if (path.length == 0) {
        if path.is_empty() {
            // JS:       // Since this hook cannot be pulled anyway, and dispose() is a no-op, we can actually just
            // JS:       // return the same hook again to represent getting the empty path.
            // JS:       return this;
            Ok(Arc::new(Self {
                mapper: Arc::clone(&self.mapper),
                mapper_id: self.mapper_id,
                index: self.index,
            }))
        // JS:     } else if (currentMapBuilder) {
        } else {
            // JS:       return currentMapBuilder.pushGet(this, path);
            let current = CURRENT_MAP_BUILDER.with(|current| current.borrow().clone());
            if let Some(current) = current {
                MapBuilder::push_get(
                    &current,
                    Arc::new(Self {
                        mapper: Arc::clone(&self.mapper),
                        mapper_id: self.mapper_id,
                        index: self.index,
                    }),
                    path,
                )
            // JS:     } else {
            } else {
                // JS:       throwMapperBuilderUseError();
                Err(mapper_builder_use_error())
                // JS:     }
            }
            // JS:   }
        }
    }
    // JS:
    // JS:   // Other methods should never be called.
    // JS:   call(path: PropertyPath, args: RpcPayload): StubHook {
    fn call(&self, _path: PropertyPath, args: RpcPayload) -> Result<StubHook> {
        // JS:     // Can't be called; all calls are intercepted. We still own `args` per the StubHook contract.
        // JS:     args.dispose();
        drop(args);
        // JS:     throwMapperBuilderUseError();
        Err(mapper_builder_use_error())
        // JS:   }
    }
    // JS:
    // JS:   map(path: PropertyPath, captures: StubHook[], instructions: unknown[]): StubHook {
    fn map(
        &self,
        _path: PropertyPath,
        captures: Vec<StubHook>,
        _instructions: MapInstructions,
    ) -> Result<StubHook> {
        // JS:     // Can't be called; all map()s are intercepted. We still own the captures per the StubHook
        // JS:     // contract.
        // JS:     for (let cap of captures) {
        // JS:       cap.dispose();
        // JS:     }
        drop(captures);
        // JS:     throwMapperBuilderUseError();
        Err(mapper_builder_use_error())
        // JS:   }
    }
    // JS:
    // JS:   pull(): RpcPayload | Promise<RpcPayload> {
    fn pull(&self) -> Result<MaybePromise<RpcPayload>> {
        // JS:     // Map functions cannot await.
        // JS:     throwMapperBuilderUseError();
        Err(mapper_builder_use_error())
        // JS:   }
    }
    // JS:
    // JS:   ignoreUnhandledRejections(): void {
    fn ignore_unhandled_rejections(&self) {
        // JS:     // Probably never called but whatever.
        // JS:   }
    }
    // JS:
    // JS:   onBroken(callback: (error: any) => void): void {
    fn on_broken(&self, callback: Box<dyn FnOnce(Error) + Send + 'static>) {
        // JS:     throwMapperBuilderUseError();
        callback(mapper_builder_use_error());
        // JS:   }
    }
    // JS: }
}
// JS:
// JS: // =======================================================================================
// JS:
// JS: class MapApplicator implements Importer {
struct MapApplicator {
    captures: Vec<StubHook>,
    variables: Vec<StubHook>,
}
// JS:   private variables: StubHook[];
// JS:
// JS:   constructor(private captures: StubHook[], input: StubHook) {
impl MapApplicator {
    fn new(captures: Vec<StubHook>, input: StubHook) -> Self {
        // JS:     this.variables = [input];
        Self {
            captures,
            variables: vec![input],
        }
        // JS:   }
    }
    // JS:
    // JS:   dispose() {
    fn dispose(&mut self) {
        // JS:     for (let variable of this.variables) {
        // JS:       variable.dispose();
        // JS:     }
        self.variables.clear();
        // JS:   }
    }
    // JS:
    // JS:   apply(instructions: unknown[]): RpcPayload {
    fn apply(&mut self, instructions: &[CapnwebValue]) -> Result<RpcPayload> {
        // JS:     try {
        // JS:       if (instructions.length < 1) {
        if instructions.is_empty() {
            // JS:         throw new Error("Invalid empty mapper function.");
            return Err(Error::Message("Invalid empty mapper function.".to_owned()));
            // JS:       }
        }
        let result = (|| -> Result<RpcPayload> {
            // JS:
            // JS:       for (let instruction of instructions.slice(0, -1)) {
            for instruction in &instructions[..instructions.len() - 1] {
                // JS:         let payload = new Evaluator(this).evaluateCopy(instruction);
                let payload =
                    Evaluator::new(self, EncodingLevel::String).evaluate_copy(instruction)?;
                // JS:
                // JS:         // The payload almost always contains a single stub. As an optimization, unwrap it.
                // JS:         if (payload.value instanceof RpcStub) {
                // JS:           let hook = unwrapStubNoProperties(payload.value);
                let hook = match &payload.value {
                    CapnwebValue::Stub(stub) => unwrap_stub_no_properties(stub),
                    _ => None,
                };
                // JS:           if (hook) {
                if let Some(hook) = hook {
                    // JS:             this.variables.push(hook);
                    self.variables.push(hook);
                    // JS:             continue;
                    continue;
                    // JS:           }
                }
                // JS:         }
                // JS:
                // JS:         this.variables.push(new PayloadStubHook(payload));
                self.variables.push(Arc::new(PayloadStubHook::new(payload)));
                // JS:       }
            }
            // JS:
            // JS:       return new Evaluator(this).evaluateCopy(instructions[instructions.length - 1]);
            Evaluator::new(self, EncodingLevel::String)
                .evaluate_copy(&instructions[instructions.len() - 1])
        })();
        // JS:     } finally {
        // JS:       for (let variable of this.variables) {
        // JS:         variable.dispose();
        // JS:       }
        self.dispose();
        // JS:     }
        // JS:   }
        result
    }
}
// JS:
impl Importer for MapApplicator {
    // JS:   importStub(idx: ImportId): StubHook {
    fn import_stub(&mut self, _index: ImportId) -> Result<StubHook> {
        // JS:     // This implies we saw an "export" appear inside the body of a mapper function. This should be
        // JS:     // impossible because exportStub()/exportPromise() throw exceptions in MapBuilder.
        // JS:     throw new Error("A mapper function cannot refer to exports.");
        Err(Error::Message(
            "A mapper function cannot refer to exports.".to_owned(),
        ))
        // JS:   }
    }
    // JS:   importPromise(idx: ImportId): StubHook {
    fn import_promise(&mut self, index: ImportId) -> Result<StubHook> {
        // JS:     return this.importStub(idx);
        self.import_stub(index)
        // JS:   }
    }
    // JS:
    // JS:   getExport(idx: ExportId): StubHook | undefined {
    fn get_export(&mut self, index: ExportId) -> Option<StubHook> {
        // JS:     if (idx < 0) {
        if index < 0 {
            // JS:       return this.captures[-idx - 1];
            self.captures.get((-index - 1) as usize).map(Arc::clone)
        // JS:     } else {
        } else {
            // JS:       return this.variables[idx];
            self.variables.get(index as usize).map(Arc::clone)
            // JS:     }
        }
        // JS:   }
    }
    // JS:
    // JS:   getPipeReadable(exportId: ExportId): never {
    // JS:     throw new Error("A mapper function cannot use pipe readables.");
    // RUST-DEFERRED(JS built-in: ReadableStream): `Importer` has no pipe-readable method.
    // JS:   }
    // JS:
    // JS:   getLimits(): RpcLimits {
    fn get_limits(&self) -> RpcLimits {
        // JS:     // A mapper's instructions arrived inside a ["remap"] message that was itself deserialized
        // JS:     // under the session's limits, so the default floor is sufficient protection here without
        // JS:     // threading per-session overrides through the several layers between the session and the
        // JS:     // mapper.
        // JS:     return DEFAULT_LIMITS;
        DEFAULT_LIMITS
        // JS:   }
    }
    // JS: }
}
// JS:
// JS: function applyMapToElement(input: unknown, parent: object | undefined, owner: RpcPayload | null,
// JS:                            captures: StubHook[], instructions: unknown[]): RpcPayload {
fn apply_map_to_element(
    input: &CapnwebValue,
    parent: Option<&CapnwebValue>,
    owner: Option<&RpcPayload>,
    captures: Vec<StubHook>,
    instructions: &[CapnwebValue],
) -> Result<RpcPayload> {
    // JS:   // TODO(perf): I wonder if we could use .fromAppParams() instead of .deepCopyFrom()? It
    // JS:   //   maybe wouldn't correctly handle the case of RpcTargets in the input, so we need a variant
    // JS:   //   which takes an `owner`, which does add some complexity.
    // JS:   let inputHook = new PayloadStubHook(RpcPayload.deepCopyFrom(input, parent, owner));
    let input_hook = Arc::new(PayloadStubHook::new(RpcPayload::deep_copy_from(
        input, parent, owner,
    )));
    // JS:   let mapper = new MapApplicator(captures, inputHook);
    let mut mapper = MapApplicator::new(captures, input_hook);
    // JS:   try {
    // JS:     return mapper.apply(instructions);
    let result = mapper.apply(instructions);
    // JS:   } finally {
    // JS:     mapper.dispose();
    mapper.dispose();
    // JS:   }
    result
    // JS: }
}
// JS:
// JS: mapImpl.applyMap = (input: unknown, parent: object | undefined, owner: RpcPayload | null,
impl MapImpl for MapRuntime {
    fn send_map(
        &self,
        hook: StubHook,
        path: PropertyPath,
        function: Box<dyn FnOnce(RpcPromise) -> Result<CapnwebValue> + Send>,
    ) -> Result<RpcPromise> {
        self.record_map(hook, path, function)
    }
    // JS:                     captures: StubHook[], instructions: unknown[]) => {
    fn apply_map(
        &self,
        input: CapnwebValue,
        parent: Option<&CapnwebValue>,
        owner: Option<&RpcPayload>,
        captures: Vec<StubHook>,
        instructions: MapInstructions,
    ) -> Result<StubHook> {
        // JS:   try {
        // JS:     let result: RpcPayload;
        // JS:     if (input instanceof RpcPromise) {
        // JS:       // The caller is responsible for making sure the input is not a promise, since we can't
        // JS:       // then know if it would resolve to an array later.
        // JS:       throw new Error("applyMap() can't be called on RpcPromise");
        if matches!(input, CapnwebValue::RpcPromise(_)) {
            return Err(Error::Message(
                "applyMap() can't be called on RpcPromise".to_owned(),
            ));
            // JS:     } else if (input instanceof Array) {
        }
        let result = if let CapnwebValue::Array(elements) = &input {
            // JS:       let payloads: RpcPayload[] = [];
            let mut payloads = Vec::with_capacity(elements.len());
            // JS:       try {
            // JS:         for (let elem of input) {
            for element in elements {
                // JS:           payloads.push(applyMapToElement(elem, input, owner, captures, instructions));
                payloads.push(apply_map_to_element(
                    element,
                    Some(&input),
                    owner,
                    captures.clone(),
                    &instructions,
                )?);
                // JS:         }
            }
            // JS:       } catch (err) {
            // JS:         for (let payload of payloads) {
            // JS:           payload.dispose();
            // JS:         }
            // JS:         throw err;
            // JS:       }
            // JS:
            // JS:       result = RpcPayload.fromArray(payloads);
            RpcPayload::from_array(payloads)
        // JS:     } else if (input === null || input === undefined) {
        } else if matches!(input, CapnwebValue::Null | CapnwebValue::Undefined) {
            // JS:       result = RpcPayload.fromAppReturn(input);
            RpcPayload::from_app_return(input)
        // JS:     } else {
        } else {
            // JS:       result = applyMapToElement(input, parent, owner, captures, instructions);
            apply_map_to_element(&input, parent, owner, captures.clone(), &instructions)?
            // JS:     }
        };
        // JS:
        // JS:     // TODO(perf): We should probably return a hook that allows pipelining but whose pull() doesn't
        // JS:     //   resolve until all promises in the payload have been substituted.
        // JS:     return new PayloadStubHook(result);
        Ok(Arc::new(PayloadStubHook::new(result)))
        // JS:   } finally {
        // JS:     for (let cap of captures) {
        // JS:       cap.dispose();
        // JS:     }
        // JS:   }
    }
    // JS: }
}
// JS:
// JS: export function forceInitMap() {}
/// Installs map recording and application support into the core runtime.
pub fn force_init_map() -> Result<()> {
    install_map_impl(Arc::new(MapRuntime))
}
