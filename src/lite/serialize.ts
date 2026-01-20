import { ImportExporter, RpcTarget } from "./index.js";

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

    
    async evaluatePipeline([id, path, args, ...rest]: unknown[]) {
        this.assert(rest.length === 0 && typeof id === "number", "Invalid pipeline instruction");
        this.assert(path === undefined || Array.isArray(path) && path.every((item) => typeof item === "string" || typeof item === "number"), "Invalid path");
        this.assert(args === undefined || Array.isArray(args), "Invalid args");
         

        // It's an "import" from the perspective of the sender, so it's an export from our
        // side. In other words, the sender is passing our own object back to us.
        const rawTarget = this.exporter.getExport(id);
        this.assert(rawTarget != null, "Import ID not found");
        const {target, parent} = folowPath(rawTarget, path ?? []);
        const evaledArgs = args && await this.evaluateArray(args);
        if (evaledArgs != null) {
            this.assert(typeof target === 'function', "Target is not callable");
            return target.call(parent, ...evaledArgs);
        }
        return target;
    }

    evaluateExport([idx, ...rest]: unknown[]) {
        // It's an "export" from the perspective of the sender, i.e. they sent us a new object
        // which we want to import.
        //
        // "promise" is same as "export" but should not be delivered to the application. If any
        // promises appear in a value, they must be resolved and substituted with their results
        // before delivery. Note that if the value being evaluated appeared in call params, or
        // appeared in a resolve message for a promise that is being pulled, then the new promise
        // is automatically also being pulled, otherwise it is not.
        this.assert(rest.length === 0 && (typeof idx === 'number'), "Invalid export instruction");
        return this.exporter.getImport(idx);
    }

    evaluateRemap([id, path, captures, instructions, ...rest]: unknown[]) {
        this.assert(rest.length === 0 && typeof id === "number" && Array.isArray(path) && Array.isArray(captures) && Array.isArray(instructions), "Invalid remap instruction");
        const subject = this.exporter.getExport(id);
        this.assert(subject != null, `No such entry on exports table: ${id}`);
        this.assert(isPath(path), "Invalid path in remap instruction");
        const evaluatedCaptures = captures.map((cap) => {
            this.assert(Array.isArray(cap), "Invalid capture in remap instruction");
            const [type, ...args] = cap;
            switch (type) {
                case "export":
                    return this.evaluateExport(args);
                case "import":
                    return this.evaluateImport(args);
                default:
                    throw new Error(`Invalid capture type in remap instruction: ${type}`);
            }
        });

        // To evaluate remap -- we create a new reference table with:
        // - the subject at index 0 (replace the root object)
        // - captures at negative indices (replaces exports)
        // the just evaluate the instructions with that table.
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

const isPath = (value: unknown): value is (string | number)[] => {
    if (!Array.isArray(value)) {
        return false;
    }
    return value.every((item) => typeof item === "string" || typeof item === "number");
}