import type { RpcTransport, RpcSessionOptions } from "../rpc.js";

export class RpcTarget {}

class RpcStub {
    #importId: number;
    #session: RpcSessionImpl;
    #path: string[];

    constructor(importId: number, session: RpcSessionImpl, path: string[]) {
        this.#importId = importId;
        this.#session = session;
        this.#path = path;

        return new Proxy(() => {}, {
            apply: (_target, _thisArg, argumentsList) => {
                const resultImportId = this.#session.push(
                    this.#importId, this.#path, argumentsList,
                ) 
                return new RpcPromise(resultImportId, this.#session, []);
            },
            get: (_target, prop, _receiver) => {
                if (prop in this) {
                    return this[prop as keyof this];
                }
                if (typeof prop === "string") {
                    return new RpcPromise(this.#importId, this.#session, this.#path.concat([prop]));
                }
                return undefined;
            },
            has: (_target, prop) => {
                return prop in this || typeof prop === "string";
            },
        }) as unknown as RpcStub;
    }
}

class RpcPromise extends RpcStub implements Promise<unknown> {
    #importId: number;
    #session: RpcSessionImpl;
    #path: string[];
    #promise?: Promise<unknown>;

    constructor(importId: number, session: RpcSessionImpl, path: string[]) {
        super(importId, session, path);
        this.#importId = importId;
        this.#session = session;
        this.#path = path;
    }

    async #pull(): Promise<any> {
        if (!this.#promise) {
            if (this.#path.length > 0) {
                const resultImportId = this.#session.push(this.#importId, this.#path) 
                this.#promise = new RpcPromise(resultImportId, this.#session, []);
            } else {
                this.#promise = this.#session.pull(this.#importId);
            }
         }
         return this.#promise;
    }

    [Symbol.toStringTag]: string = "RpcPromise";
    then<TResult1 = unknown, TResult2 = never>(onfulfilled?: (value: unknown) => TResult1 | PromiseLike<TResult1>, onrejected?: (reason: unknown) => TResult2 | PromiseLike<TResult2>): Promise<TResult1 | TResult2> {
        return this.#pull().then(onfulfilled, onrejected);
    }

    catch<TResult = never>(onrejected?: (reason: unknown) => TResult | PromiseLike<TResult>): Promise<TResult> {
        return this.#pull().catch(onrejected);
    }

    finally(onfinally?: () => void | PromiseLike<void>): Promise<void> {
        return this.#pull().finally(onfinally);
    }

}

export interface ImportExporter {
    createExport(target: RpcTarget | Function): number;
    getExport(id: number): RpcTarget | Function | undefined;
    createImport(id: number): RpcPromise | RpcStub;
    getImport(id: number): Promise<RpcStub>;
}

class RpcSessionImpl implements ImportExporter {
    public readonly transport: RpcTransport;
    public readonly options: RpcSessionOptions;
    public readonly imports: Map<number, PromiseWithResolvers<RpcStub>>;
    public readonly exports: Map<number, {
        target: RpcTarget | Function,
        refcount: number,
    }>;
    public nextExportId: number = -1;
    public readonly exportIdsByTarget: Map<RpcTarget | Function, number>;
    public readonly callResults: Map<number, Promise<unknown>>;
    public readonly nextImportId: number = 0;

    constructor(transport: RpcTransport, options: RpcSessionOptions = {}) {
        this.transport = transport;
        this.options = options;
        this.imports = new Map();
        this.exports = new Map();
        this.exportIdsByTarget = new Map();
        this.callResults = new Map();
    }

    assert(condition: boolean, message: string): asserts condition {
        if (!condition) {
            throw new Error(message);
        }
    }

    async send(message: unknown[]) {
        this.transport.send(JSON.stringify(message));
    }

    async run() {
        while (true) {
            const message = await this.transport.receive();
            const parsed = JSON.parse(message) as unknown[];
            this.assert(Array.isArray(parsed), "Invalid message: not an array");
            const [type, ...args] = parsed;

            if (type === "push") {
                await this.handlePush(args);
            } else if (type === "pull") {
                await this.handlePull(args);
            }
            else if (type === "resolve") {
                await this.handleResolve(args);
            }
            else if (type === "reject") {
                await this.handleReject(args);
            }
            else if (type === "release") {
                await this.handleRelease(args);
            }
            else if (type === "abort") {
                break;
            } else {
                this.assert(false, `Invalid message type: ${JSON.stringify(type)}`);
            }
        }
    }

    async handlePush([id, value, ...rest]: unknown[]) {
        this.assert(rest.length === 0 && typeof id === "number" && id > 0, "Invalid push message");
        this.assert(!this.callResults.has(id), "Import ID already used");
        this.callResults.set(id, this.evaluate(value));        
    }

    async handlePull([id, ...rest]: unknown[]) {
        this.assert(rest.length === 0 && typeof id === "number" && id > 0, "Invalid pull message");
        this.assert(id > 0, "Invalid import ID");
        const resultPromise = this.callResults.get(id);
        this.assert(resultPromise != null, "Import ID not found");
        const result = await resultPromise;
        return await this.sendResolve(id, result);
    }

    async sendResolve(id: number, result: unknown) {
        const devalulated = this.devaluate(result);
        return await this.send(["resolve", id, devalulated]);
    }

    async handleResolve([id, value, ...rest]: unknown[]) {
        this.assert(rest.length === 0 && typeof id === "number" && id > 0, "Invalid resolve message");
        const importPromise = this.imports.get(id);
        this.assert(importPromise != null, "Import ID not found");
        const evaluated = this.evaluate(value);
        importPromise.resolve(evaluated);
        // Immediately release the call result:
        await this.sendRelease(id, 1);

        // TODO: Release intermediates that were used by this call.
        //
        // Example: `stub.makeCounter(4).increment(3)`
        //   - push(0, ["makeCounter"], [4]) → importId 1 (the Counter)
        //   - push(1, ["increment"], [3])  → importId 2 (the result)
        //   - pull(2) → triggers resolve for importId 2
        //
        // When we receive the resolve for importId 2, we also need to release
        // importId 1 (the intermediate Counter) since it was only used to get
        // to importId 2 and is no longer needed.
        //
        // Options:
        //   1. Track dependencies: when push(1, ...) happens, record that importId 2
        //      depends on importId 1. On resolve of 2, release 1.
        //   2. Server-side tracking: server knows which intermediates were used
        //      to produce a result and releases them when the result is pulled.
        //   3. Batch model: buffer all calls until await, send as batch with
        //      internal refs, intermediates never cross the wire.
    }

    async handleReject([id, value, ...rest]: unknown[]) {
        this.assert(rest.length === 0 && typeof id === "number" && id > 0, "Invalid reject message");
        const importPromise = this.imports.get(id);
        this.assert(importPromise != null, "Import ID not found");
        const evaluated = this.evaluate(value);
        importPromise.reject(evaluated);
    }

    async handleRelease([id, refcount, ...rest]: unknown[]) {
        this.assert(rest.length === 0 && typeof id === "number" && typeof refcount === "number" && refcount > 0, "Invalid release message");
        this.assert(id !== 0, "Attempting to release bootstrap object");
        if (id > 0) {
            this.callResults.delete(id);    
        } else {
            const exportEntry = this.exports.get(id);
            if (exportEntry != null) {
                this.assert(exportEntry.refcount >= refcount, "Refcount would go negative");
                exportEntry.refcount -= refcount;
                if (exportEntry.refcount === 0) {
                    this.exports.delete(id);
                    this.exportIdsByTarget.delete(exportEntry.target);
                }
            }
        }
    }

    export(target: RpcTarget | Function): number {
        let currentId = this.exportIdsByTarget.get(target);
        if (currentId == null) {
            currentId = this.nextExportId;
            this.exportIdsByTarget.set(target, currentId);
            this.nextExportId--;
        }
        let currentEntry = this.exports.get(currentId);
        if (currentEntry == null) {
            currentEntry = {
                target: target,
                refcount: 0,
            };
            this.exports.set(currentId, currentEntry);
        } 
        currentEntry.refcount++;
        return currentId;
    }

    import(id: number): Promise<RpcStub> {
        const importPromise = this.imports.get(id);
        if (importPromise == null) {
            throw new Error(`Import ID not found: ${id}`);
        }
        return importPromise.promise;
    }
}

export class RpcSession {
    #session: RpcSessionImpl;
    #mainStub: RpcStub;
  
    constructor(transport: RpcTransport, options: RpcSessionOptions = {}) {
      this.#session = new RpcSessionImpl(transport, options);
      this.#mainStub = new RpcStub(0, this.#session, []);
    }
  
    getRemoteMain(): RpcStub {
      return this.#mainStub;
    }
  
    getStats(): {imports: number, exports: number} {
      return todo();
    }
  
    drain(): Promise<void> {
      return todo();
    }
  }