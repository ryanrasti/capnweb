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

class RpcSessionImpl {
    public readonly transport: RpcTransport;
    public readonly options: RpcSessionOptions;
    public readonly imports: Map<number, RpcStub>;
    public readonly exports: Map<number, RpcStub>;

    constructor(transport: RpcTransport, options: RpcSessionOptions = {}) {
        this.transport = transport;
        this.options = options;
        this.imports = new Map();
        this.exports = new Map();
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