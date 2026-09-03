// JS: // Copyright (c) 2025 Cloudflare, Inc.
// JS: // Licensed under the MIT license found in the LICENSE.txt file or at:
// JS: //     https://opensource.org/license/mit
// JS:
// JS: import { StubHook, RpcPayload, RpcStub, PropertyPath, PayloadStubHook, ErrorStubHook, RpcTarget, unwrapStubAndPath, streamImpl } from "./core.js";
// JS: import { Devaluator, Evaluator, ExportId, ImportId, Exporter, Importer, serialize, EncodingLevel, RpcLimits, DEFAULT_LIMITS } from "./serialize.js";
// JS:
// JS: /**
// JS:  * Interface for a string-based RPC transport. This is the default transport type — no
// JS:  * `encodingLevel` field is needed. Messages are JSON strings. Implement this interface if the
// JS:  * built-in transports (e.g. for HTTP batch and WebSocket) don't meet your needs.
// JS:  */
// JS: export interface RpcTransport {
// JS:   /**
// JS:    * The encoding level this transport works with. For this interface it is always "string";
// JS:    * it may be omitted. (See `RpcTransportWithCustomEncoding` for the other levels.)
// JS:    */
// JS:   readonly encodingLevel?: "string";
// JS:
// JS:   /**
// JS:    * Sends a message to the other end. May optionally return a promise; if the promise rejects,
// JS:    * the session is aborted.
// JS:    */
// JS:   send(message: string): void | Promise<void>;
// JS:
// JS:   /**
// JS:    * Receives a message sent by the other end.
// JS:    *
// JS:    * If and when the transport becomes disconnected, this will reject. The thrown error will be
// JS:    * propagated to all outstanding calls and future calls on any stubs associated with the session.
// JS:    * If there are no outstanding calls (and none are made in the future), then the error does not
// JS:    * propagate anywhere -- this is considered a "clean" shutdown.
// JS:    */
// JS:   receive(): Promise<string>;
// JS:
// JS:   /**
// JS:    * Indicates that the RPC system has suffered an error that prevents the session from continuing.
// JS:    * The transport should ideally try to send any queued messages if it can, and then close the
// JS:    * connection. (It's not strictly necessary to deliver queued messages, but the last message sent
// JS:    * before abort() is called is often an "abort" message, which communicates the error to the
// JS:    * peer, so if that is dropped, the peer may have less information about what happened.)
// JS:    */
// JS:   abort?(reason: any): void;
// JS: }
// JS:
// JS: /**
// JS:  * Interface for a transport that receives partially encoded JS values instead of JSON strings.
// JS:  * The selected `encodingLevel` describes what the transport can assume about message values.
// JS:  */
// JS: export interface RpcTransportWithCustomEncoding {
// JS:   /**
// JS:    * The encoding level this transport works with.
// JS:    *
// JS:    * - "jsonCompatible": JSON-compatible JS value tree; transport handles final serialization.
// JS:    * - "jsonCompatibleWithBytes": Like "jsonCompatible" but Uint8Array values are left raw.
// JS:    * - "structuredClonable": Structured-clonable native values pass through where possible.
// JS:    */
// JS:   readonly encodingLevel: "jsonCompatible" | "jsonCompatibleWithBytes" | "structuredClonable";
// JS:
// JS:   /**
// JS:    * Encodes and sends a message to the other end. Returns the encoded byte size if known.
// JS:    * If the size is unavailable, return void; Cap'n Web will estimate stream message sizes for
// JS:    * flow control. Send errors should be propagated via `receive()` rejecting.
// JS:    */
// JS:   send(message: unknown): number | void;
// JS:
// JS:   /**
// JS:    * Receives and decodes a message sent by the other end.
// JS:    *
// JS:    * If and when the transport becomes disconnected, this will reject. The thrown error will be
// JS:    * propagated to all outstanding calls and future calls on any stubs associated with the session.
// JS:    * If there are no outstanding calls (and none are made in the future), then the error does not
// JS:    * propagate anywhere -- this is considered a "clean" shutdown.
// JS:    */
// JS:   receive(): Promise<unknown>;
// JS:
// JS:   /**
// JS:    * Indicates that the RPC system has suffered an error that prevents the session from continuing.
// JS:    * The transport should ideally try to send any queued messages if it can, and then close the
// JS:    * connection. (It's not strictly necessary to deliver queued messages, but the last message sent
// JS:    * before abort() is called is often an "abort" message, which communicates the error to the
// JS:    * peer, so if that is dropped, the peer may have less information about what happened.)
// JS:    */
// JS:   abort?(reason: any): void;
// JS: }
// JS:
// JS: /** Any supported transport type. */
// JS: export type AnyRpcTransport = RpcTransport | RpcTransportWithCustomEncoding;
// JS:
// JS: const ESTIMATED_OBJECT_OVERHEAD = 16;
// JS: const ESTIMATED_ENTRY_OVERHEAD = 8;
// JS: const ESTIMATED_BINARY_OVERHEAD = 16;
// JS: const MAX_ESTIMATE_DEPTH = 64;
// JS:
// JS: function estimateStringSize(value: string): number {
// JS:   // Bias high. UTF-8 uses up to 3 bytes for BMP code points, and surrogate pairs are 4 bytes for
// JS:   // 2 UTF-16 code units.
// JS:   return 2 + value.length * 3;
// JS: }
// JS:
// JS: function estimateEncodedSize(value: unknown, seen?: WeakSet<object>, depth: number = 0): number {
// JS:   if (depth >= MAX_ESTIMATE_DEPTH) return ESTIMATED_ENTRY_OVERHEAD;
// JS:
// JS:   switch (typeof value) {
// JS:     case "string":
// JS:       return estimateStringSize(value);
// JS:     case "number":
// JS:       return 16;
// JS:     case "bigint":
// JS:       return 16;
// JS:     case "boolean":
// JS:       return 8;
// JS:     case "undefined":
// JS:       return 16;
// JS:     case "object": {
// JS:       if (value === null) return 8;
// JS:       if (ArrayBuffer.isView(value)) return ESTIMATED_BINARY_OVERHEAD + value.byteLength;
// JS:       if (value instanceof ArrayBuffer) return ESTIMATED_BINARY_OVERHEAD + value.byteLength;
// JS:       if (typeof Blob !== "undefined" && value instanceof Blob) {
// JS:         return ESTIMATED_BINARY_OVERHEAD + value.size;
// JS:       }
// JS:       if (value instanceof Date) return 16;
// JS:
// JS:       // `seen` is only ever added to, never removed, so it dedupes by object identity across the
// JS:       // entire traversal rather than just along the current path. This is intentional: it keeps the
// JS:       // estimate safe against cyclic graphs (which would otherwise recurse forever). The trade-off
// JS:       // is that a value reachable via two different paths (shared but acyclic) is counted in full
// JS:       // the first time and only as ESTIMATED_ENTRY_OVERHEAD afterward, so shared substructure
// JS:       // under-counts slightly. That's acceptable here — this is a flow-control estimate, not an
// JS:       // exact serialized size, and it otherwise biases high.
// JS:       seen ??= new WeakSet();
// JS:       if (seen.has(value)) return ESTIMATED_ENTRY_OVERHEAD;
// JS:       seen.add(value);
// JS:
// JS:       if (value instanceof Array) {
// JS:         let size = ESTIMATED_OBJECT_OVERHEAD;
// JS:         for (let item of value) {
// JS:           size += ESTIMATED_ENTRY_OVERHEAD + estimateEncodedSize(item, seen, depth + 1);
// JS:         }
// JS:         return size;
// JS:       }
// JS:
// JS:       if (value instanceof Error) {
// JS:         let size = ESTIMATED_OBJECT_OVERHEAD + estimateStringSize(value.name) +
// JS:             estimateStringSize(value.message) + estimateStringSize(value.stack ?? "");
// JS:         for (let key of Object.keys(value)) {
// JS:           size += ESTIMATED_ENTRY_OVERHEAD + estimateStringSize(key) +
// JS:               estimateEncodedSize((value as any)[key], seen, depth + 1);
// JS:         }
// JS:         return size;
// JS:       }
// JS:
// JS:       let size = ESTIMATED_OBJECT_OVERHEAD;
// JS:       for (let key of Object.keys(value)) {
// JS:         size += ESTIMATED_ENTRY_OVERHEAD + estimateStringSize(key) +
// JS:             estimateEncodedSize((value as Record<string, unknown>)[key], seen, depth + 1);
// JS:       }
// JS:       return size;
// JS:     }
// JS:     default:
// JS:       return 16;
// JS:   }
// JS: }
// JS:
// JS: // Entry on the exports table.
// JS: type ExportTableEntry = {
// JS:   hook: StubHook,
// JS:   refcount: number,
// JS:   pull?: Promise<void>,
// JS:
// JS:   // If true, the export should be automatically released (with refcount 1) after its "resolve"
// JS:   // or "reject" message is sent. This is set for exports created by ["stream"] messages.
// JS:   autoRelease?: boolean,
// JS:
// JS:   // If this export was created by a ["pipe"] message, this holds the ReadableStream end of the
// JS:   // pipe. It is consumed (and set to undefined) when a ["readable", importId] expression
// JS:   // references this export.
// JS:   pipeReadable?: ReadableStream
// JS: };
// JS:
// JS: // Entry on the imports table.
// JS: class ImportTableEntry {
// JS:   constructor(public session: RpcSessionImpl, public importId: number, pulling: boolean) {
// JS:     if (pulling) {
// JS:       this.activePull = Promise.withResolvers<void>();
// JS:     }
// JS:   }
// JS:
// JS:   public localRefcount: number = 0;
// JS:   public remoteRefcount: number = 1;
// JS:
// JS:   private activePull?: PromiseWithResolvers<void>;
// JS:   public resolution?: StubHook;
// JS:
// JS:   // List of integer indexes into session.onBrokenCallbacks which are callbacks registered on
// JS:   // this import. Initialized on first use (so `undefined` is the same as an empty list).
// JS:   private onBrokenRegistrations?: number[];
// JS:
// JS:   resolve(resolution: StubHook) {
// JS:     // TODO: Need embargo handling here? PayloadStubHook needs to be wrapped in a
// JS:     // PromiseStubHook awaiting the embargo I suppose. Previous notes on embargoes:
// JS:     // - Resolve message specifies last call that was received before the resolve. The introducer is
// JS:     //   responsible for any embargoes up to that point.
// JS:     // - Any further calls forwarded by the introducer after that point MUST immediately resolve to
// JS:     //   a forwarded call. The caller is responsible for ensuring the last of these is handed off
// JS:     //   before direct calls can be delivered.
// JS:
// JS:     if (this.localRefcount == 0) {
// JS:       // Already disposed (canceled), so ignore the resolution and don't send a redundant release.
// JS:       resolution.dispose();
// JS:       return;
// JS:     }
// JS:
// JS:     this.resolution = resolution;
// JS:     this.sendRelease();
// JS:
// JS:     if (this.onBrokenRegistrations) {
// JS:       // Delete all our callback registrations from this session and re-register them on the
// JS:       // target stub.
// JS:       for (let i of this.onBrokenRegistrations) {
// JS:         let callback = this.session.onBrokenCallbacks[i];
// JS:         let endIndex = this.session.onBrokenCallbacks.length;
// JS:         resolution.onBroken(callback);
// JS:         if (this.session.onBrokenCallbacks[endIndex] === callback) {
// JS:           // Oh, calling onBroken() just registered the callback back on this connection again.
// JS:           // But when the connection dies, we want all the callbacks to be called in the order in
// JS:           // which they were registered. So we don't want this one pushed to the back of the line
// JS:           // here. So, let's remove the newly-added registration and keep the original.
// JS:           // TODO: This is quite hacky, think about whether this is really the right answer.
// JS:           delete this.session.onBrokenCallbacks[endIndex];
// JS:         } else {
// JS:           // The callback is now registered elsewhere, so delete it from our session.
// JS:           delete this.session.onBrokenCallbacks[i];
// JS:         }
// JS:       }
// JS:       this.onBrokenRegistrations = undefined;
// JS:     }
// JS:
// JS:     if (this.activePull) {
// JS:       this.activePull.resolve();
// JS:       this.activePull = undefined;
// JS:     }
// JS:   }
// JS:
// JS:   async awaitResolution(): Promise<RpcPayload> {
// JS:     if (!this.activePull) {
// JS:       this.session.sendPull(this.importId);
// JS:       this.activePull = Promise.withResolvers<void>();
// JS:     }
// JS:     await this.activePull.promise;
// JS:     return this.resolution!.pull();
// JS:   }
// JS:
// JS:   dispose() {
// JS:     if (this.resolution) {
// JS:       this.resolution.dispose();
// JS:     } else {
// JS:       this.abort(new Error("RPC was canceled because the RpcPromise was disposed."));
// JS:       this.sendRelease();
// JS:     }
// JS:   }
// JS:
// JS:   abort(error: any) {
// JS:     if (!this.resolution) {
// JS:       this.resolution = new ErrorStubHook(error);
// JS:
// JS:       if (this.activePull) {
// JS:         this.activePull.reject(error);
// JS:         this.activePull = undefined;
// JS:       }
// JS:
// JS:       // The RpcSession itself will have called all our callbacks so we don't need to track the
// JS:       // registrations anymore.
// JS:       this.onBrokenRegistrations = undefined;
// JS:     }
// JS:   }
// JS:
// JS:   onBroken(callback: (error: any) => void): void {
// JS:     if (this.resolution) {
// JS:       this.resolution.onBroken(callback);
// JS:     } else {
// JS:       let index = this.session.onBrokenCallbacks.length;
// JS:       this.session.onBrokenCallbacks.push(callback);
// JS:
// JS:       if (!this.onBrokenRegistrations) this.onBrokenRegistrations = [];
// JS:       this.onBrokenRegistrations.push(index);
// JS:     }
// JS:   }
// JS:
// JS:   private sendRelease() {
// JS:     if (this.remoteRefcount > 0) {
// JS:       this.session.sendRelease(this.importId, this.remoteRefcount);
// JS:       this.remoteRefcount = 0;
// JS:     }
// JS:   }
// JS: };
// JS:
// JS: class RpcImportHook extends StubHook {
// JS:   public entry?: ImportTableEntry;  // undefined when we're disposed
// JS:
// JS:   // `pulling` is true if we already expect that this import is going to be resolved later, and
// JS:   // null if this import is not allowed to be pulled (i.e. it's a stub not a promise).
// JS:   constructor(public isPromise: boolean, entry: ImportTableEntry) {
// JS:     super();
// JS:     ++entry.localRefcount;
// JS:     this.entry = entry;
// JS:   }
// JS:
// JS:   collectPath(path: PropertyPath): RpcImportHook {
// JS:     return this;
// JS:   }
// JS:
// JS:   getEntry(): ImportTableEntry {
// JS:     if (this.entry) {
// JS:       return this.entry;
// JS:     } else {
// JS:       // Shouldn't get here in practice since the holding stub should have replaced the hook when
// JS:       // disposed.
// JS:       throw new Error("This RpcImportHook was already disposed.");
// JS:     }
// JS:   }
// JS:
// JS:   // Like getEntry(), but for use in methods that take ownership of args or captures: if the
// JS:   // entry is gone, there is no callee left to consume them, so dispose them before throwing.
// JS:   private getEntryTakingOwnership(disposeOwned: () => void): ImportTableEntry {
// JS:     try {
// JS:       return this.getEntry();
// JS:     } catch (err) {
// JS:       disposeOwned();
// JS:       throw err;
// JS:     }
// JS:   }
// JS:
// JS:   // -------------------------------------------------------------------------------------
// JS:   // implements StubHook
// JS:
// JS:   call(path: PropertyPath, args: RpcPayload): StubHook {
// JS:     let entry = this.getEntryTakingOwnership(() => args.dispose());
// JS:     if (entry.resolution) {
// JS:       return entry.resolution.call(path, args);
// JS:     } else {
// JS:       return entry.session.sendCall(entry.importId, path, args);
// JS:     }
// JS:   }
// JS:
// JS:   stream(path: PropertyPath, args: RpcPayload): {promise: Promise<void>, size?: number} {
// JS:     let entry = this.getEntryTakingOwnership(() => args.dispose());
// JS:     if (entry.resolution) {
// JS:       return entry.resolution.stream(path, args);
// JS:     } else {
// JS:       return entry.session.sendStream(entry.importId, path, args);
// JS:     }
// JS:   }
// JS:
// JS:   map(path: PropertyPath, captures: StubHook[], instructions: unknown[]): StubHook {
// JS:     let entry = this.getEntryTakingOwnership(() => {
// JS:       for (let cap of captures) {
// JS:         cap.dispose();
// JS:       }
// JS:     });
// JS:     if (entry.resolution) {
// JS:       return entry.resolution.map(path, captures, instructions);
// JS:     } else {
// JS:       return entry.session.sendMap(entry.importId, path, captures, instructions);
// JS:     }
// JS:   }
// JS:
// JS:   get(path: PropertyPath): StubHook {
// JS:     let entry = this.getEntry();
// JS:     if (entry.resolution) {
// JS:       return entry.resolution.get(path);
// JS:     } else {
// JS:       return entry.session.sendCall(entry.importId, path);
// JS:     }
// JS:   }
// JS:
// JS:   dup(): RpcImportHook {
// JS:     return new RpcImportHook(false, this.getEntry());
// JS:   }
// JS:
// JS:   pull(): RpcPayload | Promise<RpcPayload> {
// JS:     let entry = this.getEntry();
// JS:
// JS:     if (!this.isPromise) {
// JS:       throw new Error("Can't pull this hook because it's not a promise hook.");
// JS:     }
// JS:
// JS:     if (entry.resolution) {
// JS:       return entry.resolution.pull();
// JS:     }
// JS:
// JS:     return entry.awaitResolution();
// JS:   }
// JS:
// JS:   ignoreUnhandledRejections(): void {
// JS:     // We don't actually have to do anything here because this method only has to ignore rejections
// JS:     // if pull() is *not* called, and if pull() is not called then we won't generate any rejections
// JS:     // anyway.
// JS:   }
// JS:
// JS:   dispose(): void {
// JS:     let entry = this.entry;
// JS:     this.entry = undefined;
// JS:     if (entry) {
// JS:       if (--entry.localRefcount === 0) {
// JS:         entry.dispose();
// JS:       }
// JS:     }
// JS:   }
// JS:
// JS:   onBroken(callback: (error: any) => void): void {
// JS:     if (this.entry) {
// JS:       this.entry.onBroken(callback);
// JS:     }
// JS:   }
// JS: }
// JS:
// JS: class RpcMainHook extends RpcImportHook {
// JS:   private session?: RpcSessionImpl;
// JS:
// JS:   constructor(entry: ImportTableEntry) {
// JS:     super(false, entry);
// JS:     this.session = entry.session;
// JS:   }
// JS:
// JS:   dispose(): void {
// JS:     if (this.session) {
// JS:       let session = this.session;
// JS:       this.session = undefined;
// JS:       session.shutdown();
// JS:     }
// JS:   }
// JS: }
// JS:
// JS: /**
// JS:  * Options to customize behavior of an RPC session. All functions which start a session should
// JS:  * optionally accept this.
// JS:  */
// JS: export type RpcSessionOptions = {
// JS:   /**
// JS:    * If provided, this function will be called whenever an `Error` object is serialized (for any
// JS:    * reason, not just because it was thrown). This can be used to log errors, and also to redact
// JS:    * them.
// JS:    *
// JS:    * If `onSendError` returns an Error object, than object will be substituted in place of the
// JS:    * original. If it has a stack property, the stack will be sent to the client.
// JS:    *
// JS:    * If `onSendError` doesn't return anything (or is not provided at all), the default behavior is
// JS:    * to serialize the error with the stack omitted.
// JS:    */
// JS:   onSendError?: (error: Error) => Error | void;
// JS:
// JS:   /**
// JS:    * Overrides for the resource limits enforced while deserializing messages from the peer. Any
// JS:    * field left unset falls back to `DEFAULT_LIMITS`. These guard against resource-exhaustion
// JS:    * attacks from untrusted peers; see `RpcLimits` for the meaning and defaults of each field.
// JS:    *
// JS:    * Limits are a purely local, receiver-side decision -- the protocol has no negotiation step, so
// JS:    * the peer never learns these values. A message that exceeds a limit is rejected, aborting the
// JS:    * session.
// JS:    */
// JS:   limits?: Partial<RpcLimits>;
// JS: };
// JS:
// JS: class RpcSessionImpl implements Importer, Exporter {
// JS:   private exports: Array<ExportTableEntry> = [];
// JS:   private reverseExports: Map<StubHook, ExportId> = new Map();
// JS:   private imports: Array<ImportTableEntry> = [];
// JS:   private abortReason?: any;
// JS:   private cancelReadLoop?: (error: any) => void;
// JS:
// JS:   // We assign positive numbers to imports we initiate, and negative numbers to exports we
// JS:   // initiate. So the next import ID is just `imports.length`, but the next export ID needs
// JS:   // to be tracked explicitly.
// JS:   private nextExportId = -1;
// JS:
// JS:   // If set, call this when all incoming calls are complete.
// JS:   private onBatchDone?: Omit<PromiseWithResolvers<void>, "promise">;
// JS:
// JS:   // How many promises is our peer expecting us to resolve?
// JS:   private pullCount = 0;
// JS:
// JS:   // Sparse array of onBrokenCallback registrations. Items are strictly appended to the end but
// JS:   // may be deleted from the middle (hence leaving the array sparse).
// JS:   onBrokenCallbacks: ((error: any) => void)[] = [];
// JS:
// JS:   // Encoding level from the transport (defaults to "string")
// JS:   private encodingLevel: EncodingLevel;
// JS:
// JS:   // Resource limits enforced on incoming messages, resolved once from the defaults plus any
// JS:   // per-session overrides.
// JS:   private limits: RpcLimits;
// JS:
// JS:   constructor(private transport: AnyRpcTransport, mainHook: StubHook,
// JS:       private options: RpcSessionOptions) {
// JS:     // `RpcTransport` has no `encodingLevel` field, so its presence is what marks a custom-encoding
// JS:     // transport. Read it defensively: treat a present-but-`undefined` value (e.g. an uninitialized
// JS:     // class field) as the default string level rather than mis-routing it down the custom-encoding
// JS:     // path, and reject any other unrecognized value (e.g. a stale pre-rename level name) loudly
// JS:     // instead of silently corrupting the wire.
// JS:     let level: EncodingLevel = "string";
// JS:     if ('encodingLevel' in transport) {
// JS:       let raw = transport.encodingLevel as unknown;
// JS:       if (raw !== undefined) {
// JS:         if (raw !== "string" && raw !== "jsonCompatible" &&
// JS:             raw !== "jsonCompatibleWithBytes" && raw !== "structuredClonable") {
// JS:           throw new TypeError(`Unknown transport encodingLevel: ${String(raw)}`);
// JS:         }
// JS:         level = raw;
// JS:       }
// JS:     }
// JS:     this.encodingLevel = level;
// JS:
// JS:     this.limits = { ...DEFAULT_LIMITS, ...options.limits };
// JS:
// JS:     // Export zero is automatically the bootstrap object.
// JS:     this.exports.push({hook: mainHook, refcount: 1});
// JS:
// JS:     // Import zero is the other side's bootstrap object.
// JS:     this.imports.push(new ImportTableEntry(this, 0, false));
// JS:
// JS:     this.readLoop().catch(err => this.abort(err));
// JS:   }
// JS:
// JS:   // Should only be called once immediately after construction.
// JS:   getMainImport(): RpcImportHook {
// JS:     return new RpcMainHook(this.imports[0]);
// JS:   }
// JS:
// JS:   shutdown(): void {
// JS:     // TODO(someday): Should we add some sort of "clean shutdown" mechanism? This gets the job
// JS:     //   done just fine for the moment.
// JS:     this.abort(new Error("RPC session was shut down by disposing the main stub"), false);
// JS:   }
// JS:
// JS:   exportStub(hook: StubHook): ExportId {
// JS:     if (this.abortReason) throw this.abortReason;
// JS:
// JS:     let existingExportId = this.reverseExports.get(hook);
// JS:     if (existingExportId !== undefined) {
// JS:       ++this.exports[existingExportId].refcount;
// JS:       return existingExportId;
// JS:     } else {
// JS:       let exportId = this.nextExportId--;
// JS:       this.exports[exportId] = { hook, refcount: 1 };
// JS:       this.reverseExports.set(hook, exportId);
// JS:       // TODO: Use onBroken().
// JS:       return exportId;
// JS:     }
// JS:   }
// JS:
// JS:   exportPromise(hook: StubHook): ExportId {
// JS:     if (this.abortReason) throw this.abortReason;
// JS:
// JS:     // Promises always use a new ID because otherwise the recipient could miss the resolution.
// JS:     let exportId = this.nextExportId--;
// JS:     this.exports[exportId] = { hook, refcount: 1 };
// JS:     this.reverseExports.set(hook, exportId);
// JS:
// JS:     // Automatically start resolving any promises we send.
// JS:     this.ensureResolvingExport(exportId);
// JS:     return exportId;
// JS:   }
// JS:
// JS:   unexport(ids: Array<ExportId>): void {
// JS:     for (let id of ids) {
// JS:       this.releaseExport(id, 1);
// JS:     }
// JS:   }
// JS:
// JS:   private releaseExport(exportId: ExportId, refcount: number) {
// JS:     let entry = this.exports[exportId];
// JS:     if (!entry) {
// JS:       throw new Error(`no such export ID: ${exportId}`);
// JS:     }
// JS:     if (entry.refcount < refcount) {
// JS:       throw new Error(`refcount would go negative: ${entry.refcount} < ${refcount}`);
// JS:     }
// JS:     entry.refcount -= refcount;
// JS:     if (entry.refcount === 0) {
// JS:       delete this.exports[exportId];
// JS:       this.reverseExports.delete(entry.hook);
// JS:       entry.hook.dispose();
// JS:     }
// JS:   }
// JS:
// JS:   onSendError(error: Error): Error | void {
// JS:     if (this.options.onSendError) {
// JS:       return this.options.onSendError(error);
// JS:     }
// JS:   }
// JS:
// JS:   private ensureResolvingExport(exportId: ExportId) {
// JS:     let exp = this.exports[exportId];
// JS:     if (!exp) {
// JS:       throw new Error(`no such export ID: ${exportId}`);
// JS:     }
// JS:     if (!exp.pull) {
// JS:       let resolve = async () => {
// JS:         let hook = exp.hook;
// JS:         for (;;) {
// JS:           let payload = await hook.pull();
// JS:           if (payload.value instanceof RpcStub) {
// JS:             let {hook: inner, pathIfPromise} = unwrapStubAndPath(payload.value);
// JS:             if (pathIfPromise && pathIfPromise.length == 0) {
// JS:               if (this.getImport(hook) === undefined) {
// JS:                 // Optimization: The resolution is just another promise, and it is not a promise
// JS:                 // pointing back to the peer. So if we send a resolve message, it's just going to
// JS:                 // resolve to another new promise export, which is just going to have to wait for
// JS:                 // another resolve message later. This intermediate resolve message gives the peer
// JS:                 // no useful information, so let's skip it and just wait for the chained
// JS:                 // resolution.
// JS:                 hook = inner;
// JS:                 continue;
// JS:               }
// JS:             }
// JS:           }
// JS:
// JS:           return payload;
// JS:         }
// JS:       };
// JS:
// JS:       let autoRelease = exp.autoRelease;
// JS:
// JS:       ++this.pullCount;
// JS:       exp.pull = resolve().then(
// JS:         payload => {
// JS:           // We don't transfer ownership of stubs in the payload since the payload
// JS:           // belongs to the hook which sticks around to handle pipelined requests.
// JS:           let value = Devaluator.devaluate(payload.value, undefined, this, payload, this.encodingLevel);
// JS:           this.send(["resolve", exportId, value]);
// JS:           if (autoRelease) this.releaseExport(exportId, 1);
// JS:         },
// JS:         error => {
// JS:           this.send(["reject", exportId, Devaluator.devaluate(error, undefined, this, undefined, this.encodingLevel)]);
// JS:           if (autoRelease) this.releaseExport(exportId, 1);
// JS:         }
// JS:       ).catch(
// JS:         error => {
// JS:           // If serialization failed, report the serialization error, which should
// JS:           // itself always be serializable.
// JS:           try {
// JS:             this.send(["reject", exportId, Devaluator.devaluate(error, undefined, this, undefined, this.encodingLevel)]);
// JS:             if (autoRelease) this.releaseExport(exportId, 1);
// JS:           } catch (error2) {
// JS:             // TODO: Shouldn't happen, now what?
// JS:             this.abort(error2);
// JS:           }
// JS:         }
// JS:       ).finally(() => {
// JS:         if (--this.pullCount === 0) {
// JS:           if (this.onBatchDone) {
// JS:             this.onBatchDone.resolve();
// JS:           }
// JS:         }
// JS:       });
// JS:     }
// JS:   }
// JS:
// JS:   getImport(hook: StubHook): ImportId | undefined {
// JS:     if (hook instanceof RpcImportHook && hook.entry && hook.entry.session === this) {
// JS:       return hook.entry.importId;
// JS:     } else {
// JS:       return undefined;
// JS:     }
// JS:   }
// JS:
// JS:   importStub(idx: ImportId): RpcImportHook {
// JS:     if (this.abortReason) throw this.abortReason;
// JS:
// JS:     let entry = this.imports[idx];
// JS:     if (!entry) {
// JS:       entry = new ImportTableEntry(this, idx, false);
// JS:       this.imports[idx] = entry;
// JS:     }
// JS:     return new RpcImportHook(/*isPromise=*/false, entry);
// JS:   }
// JS:
// JS:   importPromise(idx: ImportId): StubHook {
// JS:     if (this.abortReason) throw this.abortReason;
// JS:
// JS:     if (this.imports[idx]) {
// JS:       // Can't reuse an existing ID for a promise!
// JS:       return new ErrorStubHook(new Error(
// JS:           "Bug in RPC system: The peer sent a promise reusing an existing export ID."));
// JS:     }
// JS:
// JS:     // Create an already-pulling hook.
// JS:     let entry = new ImportTableEntry(this, idx, true);
// JS:     this.imports[idx] = entry;
// JS:     return new RpcImportHook(/*isPromise=*/true, entry);
// JS:   }
// JS:
// JS:   getExport(idx: ExportId): StubHook | undefined {
// JS:     return this.exports[idx]?.hook;
// JS:   }
// JS:
// JS:   getPipeReadable(exportId: ExportId): ReadableStream {
// JS:     let entry = this.exports[exportId];
// JS:     if (!entry || !entry.pipeReadable) {
// JS:       throw new Error(`Export ${exportId} is not a pipe or its readable end was already consumed.`);
// JS:     }
// JS:     let readable = entry.pipeReadable;
// JS:     entry.pipeReadable = undefined;
// JS:     return readable;
// JS:   }
// JS:
// JS:   getLimits(): RpcLimits {
// JS:     return this.limits;
// JS:   }
// JS:
// JS:   createPipe(readable: ReadableStream, readableHook: StubHook): ImportId {
// JS:     if (this.abortReason) throw this.abortReason;
// JS:
// JS:     this.send(["pipe"]);
// JS:
// JS:     let importId = this.imports.length;
// JS:     // The pipe import is not a promise -- it's immediately usable as a writable stream.
// JS:     let entry = new ImportTableEntry(this, importId, false);
// JS:     this.imports.push(entry);
// JS:
// JS:     // Create a proxy WritableStream from the import hook and pump the ReadableStream into it.
// JS:     let hook = new RpcImportHook(/*isPromise=*/false, entry);
// JS:     let writable = streamImpl.createWritableStreamFromHook(hook);
// JS:     readable.pipeTo(writable).catch(() => {
// JS:       // Errors are handled by the writable stream's error handling -- either the write fails
// JS:       // and the writable side reports it, or the readable side errors and pipeTo aborts the
// JS:       // writable side. Either way, the hook's disposal will handle cleanup.
// JS:     }).finally(() => readableHook.dispose());
// JS:
// JS:     return importId;
// JS:   }
// JS:
// JS:   // Serializes and sends a message. Returns the byte length reported by the transport, or
// JS:   // undefined if the transport doesn't report size.
// JS:   private send(msg: any): number | undefined {
// JS:     if (this.abortReason !== undefined) {
// JS:       // Ignore sends after we've aborted.
// JS:       return 0;
// JS:     }
// JS:
// JS:     if (this.encodingLevel === "string") {
// JS:       let msgText: string;
// JS:       try {
// JS:         msgText = JSON.stringify(msg);
// JS:       } catch (err) {
// JS:         // If JSON stringification failed, there's something wrong with the devaluator, as it
// JS:         // should not allow non-JSONable values to be injected in the first place.
// JS:         try { this.abort(err); } catch (err2) {}
// JS:         throw err;
// JS:       }
// JS:
// JS:       try {
// JS:         let sent = (this.transport as RpcTransport).send(msgText) as Promise<void> | undefined;
// JS:         if (sent !== undefined && typeof sent.catch === "function") {
// JS:           // If send fails, abort the connection, but don't try to send an abort message since
// JS:           // that'll probably also fail.
// JS:           sent.catch(err => this.abort(err, false));
// JS:         }
// JS:       } catch (err) {
// JS:         // The transport threw synchronously. Treat it like an async send failure: abort the
// JS:         // session (without trying to send an abort message over the broken transport), but
// JS:         // defer to a microtask so the caller finishes its own bookkeeping first, matching the
// JS:         // timing of a rejected promise from an async transport.
// JS:         queueMicrotask(() => this.abort(err, false));
// JS:       }
// JS:       return msgText.length;
// JS:     } else {
// JS:       // Custom encoding transport encodes and returns the actual encoded size, or void if size
// JS:       // is unavailable (e.g. structured clone).
// JS:       try {
// JS:         let size = (this.transport as RpcTransportWithCustomEncoding).send(msg);
// JS:         if (typeof size === "number") {
// JS:           return size;
// JS:         }
// JS:         // Defend against transports that return something other than a number, e.g. an
// JS:         // accidentally-async `send()` returning a promise: treat the size as unknown, and
// JS:         // observe any returned thenable so a rejection aborts the session rather than going
// JS:         // unhandled. (The documented contract is to report errors via `receive()`.)
// JS:         let thenable = size as unknown;
// JS:         if (thenable && typeof (thenable as PromiseLike<unknown>).then === "function") {
// JS:           Promise.resolve(thenable).catch(err => this.abort(err, false));
// JS:         }
// JS:         return undefined;
// JS:       } catch (err) {
// JS:         // Same as the synchronous failure case above.
// JS:         queueMicrotask(() => this.abort(err, false));
// JS:         return undefined;
// JS:       }
// JS:     }
// JS:   }
// JS:
// JS:   sendCall(id: ImportId, path: PropertyPath, args?: RpcPayload): RpcImportHook {
// JS:     if (this.abortReason) {
// JS:       args?.dispose();
// JS:       throw this.abortReason;
// JS:     }
// JS:
// JS:     let value: Array<any> = ["pipeline", id, path];
// JS:     if (args) {
// JS:       let devalue: unknown;
// JS:       try {
// JS:         devalue = Devaluator.devaluate(args.value, undefined, this, args, this.encodingLevel);
// JS:       } catch (err) {
// JS:         args.dispose();
// JS:         throw err;
// JS:       }
// JS:
// JS:       // HACK: Since the args is an array, devaluator will wrap in a second array. Need to unwrap.
// JS:       // TODO: Clean this up somehow.
// JS:       value.push((<Array<unknown>>devalue)[0]);
// JS:
// JS:       // Serializing the payload takes ownership of all stubs within, so the payload itself does
// JS:       // not need to be disposed on success. If serialization threw partway through, disposing
// JS:       // the payload above is safe: any hooks already exported were dups of the payload's hooks,
// JS:       // and the Devaluator rolls back its own export-table entries before rethrowing.
// JS:     }
// JS:     this.send(["push", value]);
// JS:
// JS:     let entry = new ImportTableEntry(this, this.imports.length, false);
// JS:     this.imports.push(entry);
// JS:     return new RpcImportHook(/*isPromise=*/true, entry);
// JS:   }
// JS:
// JS:   sendStream(id: ImportId, path: PropertyPath, args: RpcPayload)
// JS:       : {promise: Promise<void>, size: number} {
// JS:     if (this.abortReason) {
// JS:       args.dispose();
// JS:       throw this.abortReason;
// JS:     }
// JS:
// JS:     let value: Array<any> = ["pipeline", id, path];
// JS:     let devalue: unknown;
// JS:     try {
// JS:       devalue = Devaluator.devaluate(args.value, undefined, this, args, this.encodingLevel);
// JS:     } catch (err) {
// JS:       // Disposing the partially-serialized payload is safe for the same reasons as in sendCall().
// JS:       args.dispose();
// JS:       throw err;
// JS:     }
// JS:
// JS:     // HACK: Since the args is an array, devaluator will wrap in a second array. Need to unwrap.
// JS:     // TODO: Clean this up somehow.
// JS:     value.push((<Array<unknown>>devalue)[0]);
// JS:
// JS:     let msg = ["stream", value];
// JS:     let size = this.send(msg);
// JS:     if (size === undefined) {
// JS:       size = estimateEncodedSize(msg);
// JS:     }
// JS:
// JS:     // Create the import entry in "already pulling" state (pulling=true), since stream messages
// JS:     // are automatically pulled. Set remoteRefcount to 0 so that resolve() won't send a release
// JS:     // message — the server implicitly releases the export after sending the resolve. Set
// JS:     // localRefcount to 1 so that resolve() doesn't treat this as already-disposed.
// JS:     let importId = this.imports.length;
// JS:     let entry = new ImportTableEntry(this, importId, /*pulling=*/true);
// JS:     entry.remoteRefcount = 0;
// JS:     entry.localRefcount = 1;
// JS:     this.imports.push(entry);
// JS:
// JS:     // Await the resolution, then dispose the result payload and clean up the import table entry.
// JS:     // (Normally, sendRelease() cleans up the import table, but since remoteRefcount is 0, we
// JS:     // need to do it manually.)
// JS:     let promise = entry.awaitResolution().then(
// JS:       p => { p.dispose(); delete this.imports[importId]; },
// JS:       err => { delete this.imports[importId]; throw err; }
// JS:     );
// JS:
// JS:     return { promise, size };
// JS:   }
// JS:
// JS:   sendMap(id: ImportId, path: PropertyPath, captures: StubHook[], instructions: unknown[])
// JS:       : RpcImportHook {
// JS:     if (this.abortReason) {
// JS:       for (let cap of captures) {
// JS:         cap.dispose();
// JS:       }
// JS:       throw this.abortReason;
// JS:     }
// JS:
// JS:     // Note: This loop cannot throw synchronously after the abortReason check above. getImport()
// JS:     // never throws, and exportStub()'s only throw is its own abortReason check — but aborts are
// JS:     // always deferred to a microtask, so abortReason cannot become set partway through this loop.
// JS:     // (Don't add a dispose() rollback here: exportStub() stores the hook itself, not a dup, in
// JS:     // the export table, so disposing exported captures would corrupt the table.)
// JS:     let devaluedCaptures = captures.map(hook => {
// JS:       let importId = this.getImport(hook);
// JS:       if (importId !== undefined) {
// JS:         return ["import", importId];
// JS:       } else {
// JS:         return ["export", this.exportStub(hook)];
// JS:       }
// JS:     });
// JS:
// JS:     let value = ["remap", id, path, devaluedCaptures, instructions];
// JS:
// JS:     this.send(["push", value]);
// JS:
// JS:     let entry = new ImportTableEntry(this, this.imports.length, false);
// JS:     this.imports.push(entry);
// JS:     return new RpcImportHook(/*isPromise=*/true, entry);
// JS:   }
// JS:
// JS:   sendPull(id: ImportId) {
// JS:     if (this.abortReason) throw this.abortReason;
// JS:
// JS:     this.send(["pull", id]);
// JS:   }
// JS:
// JS:   sendRelease(id: ImportId, remoteRefcount: number) {
// JS:     if (this.abortReason) return;
// JS:
// JS:     this.send(["release", id, remoteRefcount]);
// JS:     delete this.imports[id];
// JS:   }
// JS:
// JS:   abort(error: any, trySendAbortMessage: boolean = true) {
// JS:     // Don't double-abort.
// JS:     if (this.abortReason !== undefined) return;
// JS:
// JS:     this.cancelReadLoop?.(error);
// JS:     this.cancelReadLoop = undefined;
// JS:
// JS:     if (trySendAbortMessage) {
// JS:       try {
// JS:         let abortMsg = ["abort", Devaluator.devaluate(error, undefined, this, undefined, this.encodingLevel)];
// JS:         if (this.encodingLevel === "string") {
// JS:           let sent = (this.transport as RpcTransport)
// JS:               .send(JSON.stringify(abortMsg)) as Promise<void> | undefined;
// JS:           if (sent !== undefined && typeof sent.catch === "function") {
// JS:             sent.catch(err => {});
// JS:           }
// JS:         } else {
// JS:           let result = (this.transport as RpcTransportWithCustomEncoding).send(abortMsg) as unknown;
// JS:           if (result && typeof (result as PromiseLike<unknown>).then === "function") {
// JS:             Promise.resolve(result).catch(err => {});
// JS:           }
// JS:         }
// JS:       } catch (err) {
// JS:         // ignore, probably the whole reason we're aborting is because the transport is broken
// JS:       }
// JS:     }
// JS:
// JS:     if (error === undefined) {
// JS:       // Shouldn't happen, but if it does, avoid setting `abortReason` to `undefined`.
// JS:       error = "undefined";
// JS:     }
// JS:
// JS:     this.abortReason = error;
// JS:     if (this.onBatchDone) {
// JS:       this.onBatchDone.reject(error);
// JS:     }
// JS:
// JS:     if (this.transport.abort) {
// JS:       // Call transport's abort handler, but guard against buggy app code.
// JS:       try {
// JS:         this.transport.abort(error);
// JS:       } catch (err) {
// JS:         // Treat as unhandled rejection.
// JS:         Promise.resolve(err);
// JS:       }
// JS:     }
// JS:
// JS:     // WATCH OUT: these are sparse arrays. `for/let/of` will iterate only positive indexes
// JS:     // including deleted indexes -- bad. We need to use `for/let/in` instead.
// JS:     for (let i in this.onBrokenCallbacks) {
// JS:       try {
// JS:         this.onBrokenCallbacks[i](error);
// JS:       } catch (err) {
// JS:         // Treat as unhandled rejection.
// JS:         Promise.resolve(err);
// JS:       }
// JS:     }
// JS:     for (let i in this.imports) {
// JS:       this.imports[i].abort(error);
// JS:     }
// JS:     for (let i in this.exports) {
// JS:       this.exports[i].hook.dispose();
// JS:     }
// JS:   }
// JS:
// JS:   private async readLoop() {
// JS:     while (!this.abortReason) {
// JS:       // Each receive needs its own abort promise so Promise.race() doesn't keep old reads.
// JS:       let readCanceled = Promise.withResolvers<never>();
// JS:       this.cancelReadLoop = readCanceled.reject;
// JS:
// JS:       let raw: unknown;
// JS:
// JS:       try {
// JS:         raw = await Promise.race([this.transport.receive(), readCanceled.promise]);
// JS:       } finally {
// JS:         if (this.cancelReadLoop === readCanceled.reject) {
// JS:           this.cancelReadLoop = undefined;
// JS:         }
// JS:       }
// JS:
// JS:       // Bound a single string message before parsing it. At this point the transport has already
// JS:       // buffered the complete message; true byte-level / pre-read enforcement belongs in the
// JS:       // transport/socket. This backstop still prevents oversized messages from reaching JSON.parse
// JS:       // and downstream deserialization work, and a throw here propagates out of readLoop and aborts
// JS:       // the session. Only "string"-level transports hand us a measurable wire string; richer
// JS:       // encoding levels deliver an already-decoded value, so the size cap does not apply.
// JS:       if (this.encodingLevel === "string" &&
// JS:           (raw as string).length > this.limits.maxMessageSize) {
// JS:         throw new TypeError(
// JS:             `Incoming message exceeds maximum size of ${this.limits.maxMessageSize} UTF-16 code ` +
// JS:             `units.`);
// JS:       }
// JS:
// JS:       if (this.abortReason) break;  // check again before processing
// JS:
// JS:       // Only parse JSON at "string" level; otherwise message is already an object
// JS:       let msg = this.encodingLevel === "string" ? JSON.parse(raw as string) : raw;
// JS:
// JS:       if (msg instanceof Array) {
// JS:         switch (msg[0]) {
// JS:           case "push":  // ["push", Expression]
// JS:             if (msg.length > 1) {
// JS:               let payload = new Evaluator(this, this.encodingLevel).evaluate(msg[1]);
// JS:               let hook = new PayloadStubHook(payload);
// JS:
// JS:               // It's possible for a rejection to occur before the client gets a chance to send
// JS:               // a "pull" message or to use the promise in a pipeline. We don't want that to be
// JS:               // treated as an unhandled rejection on our end.
// JS:               hook.ignoreUnhandledRejections();
// JS:
// JS:               this.exports.push({ hook, refcount: 1 });
// JS:               continue;
// JS:             }
// JS:             break;
// JS:
// JS:           case "stream": {  // ["stream", Expression]
// JS:             // Like "push", but:
// JS:             // - Promise pipelining on the result is not supported.
// JS:             // - The export is automatically considered "pulled".
// JS:             // - Once the "resolve" is sent, the export is implicitly released.
// JS:             if (msg.length > 1) {
// JS:               let payload = new Evaluator(this, this.encodingLevel).evaluate(msg[1]);
// JS:               let hook = new PayloadStubHook(payload);
// JS:               hook.ignoreUnhandledRejections();
// JS:
// JS:               let exportId = this.exports.length;
// JS:               this.exports.push({ hook, refcount: 1, autoRelease: true });
// JS:
// JS:               // Automatically pull since stream messages are always pulled.
// JS:               this.ensureResolvingExport(exportId);
// JS:               continue;
// JS:             }
// JS:             break;
// JS:           }
// JS:
// JS:           case "pipe": {  // ["pipe"]
// JS:             // Create a TransformStream. The writable end becomes the export (so the sender can
// JS:             // write/close/abort it). The readable end is stashed for later retrieval via
// JS:             // ["readable", importId].
// JS:             let { readable, writable } = new TransformStream();
// JS:             let hook = streamImpl.createWritableStreamHook(writable);
// JS:             this.exports.push({ hook, refcount: 1, pipeReadable: readable });
// JS:             continue;
// JS:           }
// JS:
// JS:           case "pull": {  // ["pull", ImportId]
// JS:             let exportId = msg[1];
// JS:             if (typeof exportId == "number") {
// JS:               this.ensureResolvingExport(exportId);
// JS:               continue;
// JS:             }
// JS:             break;
// JS:           }
// JS:
// JS:           case "resolve":   // ["resolve", ExportId, Expression]
// JS:           case "reject": {  // ["reject", ExportId, Expression]
// JS:             let importId = msg[1];
// JS:             if (typeof importId == "number" && msg.length > 2) {
// JS:               let imp = this.imports[importId];
// JS:               if (imp) {
// JS:                 if (msg[0] == "resolve") {
// JS:                   imp.resolve(new PayloadStubHook(new Evaluator(this, this.encodingLevel).evaluate(msg[2])));
// JS:                 } else {
// JS:                   // HACK: We expect errors are always simple values (no stubs) so we can just
// JS:                   //   pull the value out of the payload.
// JS:                   let payload = new Evaluator(this, this.encodingLevel).evaluate(msg[2]);
// JS:                   payload.dispose();  // just in case -- should be no-op
// JS:                   imp.resolve(new ErrorStubHook(payload.value));
// JS:                 }
// JS:               } else {
// JS:                 // Import ID is not found on the table. Probably we released it already, in which
// JS:                 // case we do not care about the resolution, so whatever.
// JS:
// JS:                 if (msg[0] == "resolve") {
// JS:                   // We need to evaluate the resolution and immediately dispose it so that we
// JS:                   // release any stubs it contains.
// JS:                   new Evaluator(this, this.encodingLevel).evaluate(msg[2]).dispose();
// JS:                 }
// JS:               }
// JS:               continue;
// JS:             }
// JS:             break;
// JS:           }
// JS:
// JS:           case "release": {
// JS:             let exportId = msg[1];
// JS:             let refcount = msg[2];
// JS:             if (typeof exportId == "number" && typeof refcount == "number") {
// JS:               this.releaseExport(exportId, refcount);
// JS:               continue;
// JS:             }
// JS:             break;
// JS:           }
// JS:
// JS:           case "abort": {
// JS:             let payload = new Evaluator(this, this.encodingLevel).evaluate(msg[1]);
// JS:             payload.dispose();  // just in case -- should be no-op
// JS:             this.abort(payload.value, false);
// JS:             break;
// JS:           }
// JS:         }
// JS:       }
// JS:
// JS:       throw new Error(`bad RPC message: ${JSON.stringify(msg)}`);
// JS:     }
// JS:   }
// JS:
// JS:   async drain(): Promise<void> {
// JS:     if (this.abortReason) {
// JS:       throw this.abortReason;
// JS:     }
// JS:
// JS:     if (this.pullCount > 0) {
// JS:       let {promise, resolve, reject} = Promise.withResolvers<void>();
// JS:       this.onBatchDone = {resolve, reject};
// JS:       await promise;
// JS:     }
// JS:   }
// JS:
// JS:   getStats(): {imports: number, exports: number} {
// JS:     let result = {imports: 0, exports: 0};
// JS:     // We can't just use `.length` because the arrays can be sparse and can have negative indexes.
// JS:     for (let i in this.imports) {
// JS:       ++result.imports;
// JS:     }
// JS:     for (let i in this.exports) {
// JS:       ++result.exports;
// JS:     }
// JS:     return result;
// JS:   }
// JS: }
// JS:
// JS: // Public interface that wraps RpcSession and hides private implementation details (even from
// JS: // JavaScript with no type enforcement).
// JS: export class RpcSession {
// JS:   #session: RpcSessionImpl;
// JS:   #mainStub: RpcStub;
// JS:
// JS:   constructor(transport: AnyRpcTransport, localMain?: any, options: RpcSessionOptions = {}) {
// JS:     let mainHook: StubHook;
// JS:     if (localMain) {
// JS:       mainHook = new PayloadStubHook(RpcPayload.fromAppReturn(localMain));
// JS:     } else {
// JS:       mainHook = new ErrorStubHook(new Error("This connection has no main object."));
// JS:     }
// JS:     this.#session = new RpcSessionImpl(transport, mainHook, options);
// JS:     this.#mainStub = new RpcStub(this.#session.getMainImport());
// JS:   }
// JS:
// JS:   getRemoteMain(): RpcStub {
// JS:     return this.#mainStub;
// JS:   }
// JS:
// JS:   getStats(): {imports: number, exports: number} {
// JS:     return this.#session.getStats();
// JS:   }
// JS:
// JS:   drain(): Promise<void> {
// JS:     return this.#session.drain();
// JS:   }
// JS: }
