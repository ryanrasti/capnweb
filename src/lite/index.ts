import type { RpcTransport, RpcSessionOptions } from "../rpc.js";

const todo = () => {
    throw new Error("Not implemented");
}

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

interface ImportExporter {
    export(target: RpcTarget | Function): number;
    import(id: number): Promise<RpcStub>;
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

class Serializer {
    constructor(private exporter: ImportExporter) {}


    assert(condition: boolean, message: string): asserts condition {
        if (!condition) {
            throw new Error(message);
        }
    }

    // Serialize a value into an instruction array:
    devaluate(value: unknown): unknown[] {
        return todo();
    }

    // Deserialize an instruction array into a promise:
    evaluate(value: unknown): unknown | Promise<unknown> {
        if (Array.isArray(value)) {
            if (value.length == 1 && Array.isArray(value[0])) {
                // Escaped array. Evaluate the contents.
                return this.evaluateArray(value[0]);
            } else {
                const [type, ...args] = value;
                this.assert(typeof type === "string", "Invalid instruction type");
                switch (type) {
                    case "bigint":
                        return this.evaluateBigInt(args);
                    case "date":
                        return this.evaluateDate(args);
                    case "bytes":
                        return this.evaluateBytes(args);
                    case "error":
                        return this.evaluateError(args);
                    case "undefined":
                        return this.evaluateUndefined(args);
                    case "inf":
                        return Infinity;
                    case "-inf":
                        return -Infinity;
                    case "nan":
                        return NaN;
                    case "callback":
                        return this.evaluateCallback(args);
                    case "import":
                        return this.evaluateImport(args);
                    case "pipeline":
                        return this.evaluatePipeline(args);
                    case "export":
                        return this.evaluateExport(args);
                    case "remap":
                        return this.evaluateRemap(args);

                    default:
                        throw new Error(`Invalid instruction type: ${JSON.stringify(type)}`);
                }
            }
        } else if (value instanceof Object) {
            return this.evaluateObject(value);
        } else {
            // Other JSON types just pass through.
            return value;
        }
    }


    evaluateArray(value: unknown[]): unknown[] | Promise<unknown[]> {
        let hasPromises = false;
        const result = value.map((item) => {
            const res = this.evaluate(item)
            if (res instanceof Promise) {
                hasPromises = true;
            }
            return res;
        });
        if (hasPromises) {
            return Promise.all(result);
        } else {
            return result;
        }
    }

    evaluateBigInt([val, ...rest]: unknown[]) {
        this.assert(rest.length === 0 && typeof val === "string", "Invalid bigint instruction");
        return BigInt(val);
    }

    evaluateDate([val, ...rest]: unknown[]) {
        this.assert(rest.length === 0 && typeof val === "number", "Invalid date instruction");
        return new Date(val);
    }

    evaluateBytes([val, ...rest]: unknown[]) {
        this.assert(rest.length === 0 && typeof val === "string", "Invalid bytes instruction");
        let b64 = Uint8Array as FromBase64;
        if (b64.fromBase64) {
            return b64.fromBase64(val);
          } else {
            let bs = atob(val);
            let len = bs.length;
            let bytes = new Uint8Array(len);
            for (let i = 0; i < len; i++) {
                bytes[i] = bs.charCodeAt(i);
            }
            return bytes;
        }
    }

    evaluateError([type, message, stack,...rest]: unknown[]) {
        this.assert(rest.length === 0 && typeof type === "string" && typeof message === "string", "Invalid error instruction");
        const ERROR_TYPES: Record<string, any> = {
            Error, EvalError, RangeError, ReferenceError, SyntaxError, TypeError, URIError, AggregateError,
            // TODO: DOMError? Others?
          };
          let cls = ERROR_TYPES[type] || Error;
          let result = new cls(message);
          if (typeof stack === "string") {
            result.stack = stack;
          }
          return result;
    }

    evaluateUndefined([...rest]: unknown[]) {
        this.assert(rest.length === 0, "Invalid undefined instruction");
        return undefined;
    }

    evaluateInf([...rest]: unknown[]) {
        this.assert(rest.length === 0, "Invalid inf instruction");
        return Infinity;
    }

    evaluateNegInf([...rest]: unknown[]) {
        this.assert(rest.length === 0, "Invalid -inf instruction");
        return -Infinity;
    }

    evaluateNan([...rest]: unknown[]) {
        this.assert(rest.length === 0, "Invalid nan instruction");
        return NaN;
    }
    
    evaluateCallback([callback, ...rest]: unknown[]) {
        this.assert(rest.length === 0 && typeof callback === "function", "Invalid callback instruction");
        return callback;
    }

    evaluateImport(args: unknown[]) {
        return this.evaluatePipeline(args);
    }

    evaluatePipeline([id, path, ...rest]: unknown[]) {
        this.assert(rest.length === 0 && typeof id === "number", "Invalid pipeline instruction");
        this.assert(Array.isArray(path) && path.every((item) => typeof item === "string" || typeof item === "number"), "Invalid path");
        return this.exporter.import(id);
    }

    evaluateExport([target, ...rest]: unknown[]) {
        this.assert(rest.length === 0 && (target instanceof Function || target instanceof RpcTarget), "Invalid export instruction");
        return this.exporter.export(target);
    }

    evaluateRemap([id, path, captures, instructions, ...rest]: unknown[]) {
        this.assert(rest.length === 0 && typeof id === "number" && Array.isArray(path) && Array.isArray(captures) && Array.isArray(instructions), "Invalid remap instruction");
        return this.exporter.import(id);
    }

    async evaluateObject(value: Object): Promise<unknown> {
        let result = <Record<string, unknown>>value;
        for (let key in result) {
          if (key in Object.prototype || key === "toJSON") {
            // Out of an abundance of caution, we will ignore properties that override properties
            // of Object.prototype. It's especially important that we don't allow `__proto__` as it
            // may lead to prototype pollution. We also would rather not allow, e.g., `toString()`,
            // as overriding this could lead to various mischief.
            //
            // We also block `toJSON()` for similar reasons -- even though Object.prototype doesn't
            // actually define it, `JSON.stringify()` treats it specially and we don't want someone
            // snooping on JSON calls.
            //
            // We do still evaluate the inner value so that we can properly release any stubs.
            this.evaluate(result[key]);
            delete result[key];
          } else {
            result[key] = await this.evaluate(result[key]);
          }
        }
        return result;    
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