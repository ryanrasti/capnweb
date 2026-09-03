// JS: // Copyright (c) 2025 Cloudflare, Inc.
// Copyright (c) 2025 Cloudflare, Inc.
// JS: // Licensed under the MIT license found in the LICENSE.txt file or at:
// Licensed under the MIT license found in the LICENSE.txt file or at:
// JS: //     https://opensource.org/license/mit
//     https://opensource.org/license/mit
// JS:

// JS: import { StubHook, RpcPayload, typeForRpc, RpcStub, RpcPromise, LocatedPromise, RpcTarget, unwrapStubAndPath, streamImpl, PromiseStubHook, PayloadStubHook } from "./core.js";
use std::sync::Arc;

use base64::Engine as _;
use serde_json::{Number, Value as WireValue};

use crate::core::{
    CapnwebValue, Error, PropertyKey, Result, RpcPayload, StubHook, unwrap_stub_and_path,
};
// JS:
// JS: export type ImportId = number;
/// A peer-assigned import-table identifier.
pub type ImportId = i64;
// JS: export type ExportId = number;
/// A local export-table identifier.
pub type ExportId = i64;
// JS:
// JS: /**
// JS:  * Encoding levels determine what representation the RPC system hands to the transport.
// JS:  * Each level names what the transport can assume about message values.
// JS:  *
// JS:  * - `"string"`: JSON string. Default, used by HTTP batch and WebSocket transports.
// JS:  * - `"jsonCompatible"`: JSON-compatible JS value tree. For custom encoders.
// JS:  * - `"jsonCompatibleWithBytes"`: Like `"jsonCompatible"` but Uint8Array stays raw.
// JS:  * - `"structuredClonable"`: Structured-clonable native values pass through where possible.
// JS:  *
// JS:  * @example
// JS:  * ```ts
// JS:  * // What happens to Uint8Array([1, 2, 3]) at each level:
// JS:  * "string"          → '["bytes","AQID"]'           // JSON string with base64
// JS:  * "jsonCompatible"          → ["bytes", "AQID"]      // JS array with base64
// JS:  * "jsonCompatibleWithBytes" → ["bytes", Uint8Array]  // JS array with raw bytes
// JS:  * "structuredClonable"      → ["bytes", Uint8Array]  // + Date, BigInt stay native
// JS:  * ```
// JS:  */
// JS: export type EncodingLevel = "string" | "jsonCompatible" | "jsonCompatibleWithBytes" |
// JS:     "structuredClonable";
/// The representation handed from serialization to a transport.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum EncodingLevel {
    /// A complete JSON string.
    #[default]
    String,
    /// A JSON-compatible value tree.
    JsonCompatible,
    /// A JSON-compatible tree which may contain raw byte strings.
    JsonCompatibleWithBytes,
    /// A structured-clone-compatible native tree.
    StructuredClonable,
}
// JS:
// JS: // =======================================================================================
// JS: // Resource limits applied while deserializing messages from a peer.
// JS: //
// JS: // These guard against resource-exhaustion attacks from untrusted peers (see issue #184). They are
// JS: // purely *local, receiver-side* decisions: the protocol has no negotiation step, so a peer cannot
// JS: // learn or agree on these values. Consequently the defaults are chosen to be far larger than any
// JS: // legitimate message, so that no honest sender is ever rejected. A peer that exceeds a limit has
// JS: // its message rejected (a TypeError is thrown), which tears down the session via abort().
// JS:
// JS: export interface RpcLimits {
/// Receiver-side limits applied to untrusted messages.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RpcLimits {
    // JS:   // Maximum number of magnitude digits permitted in a ["bigint", "..."] wire value, excluding a
    // JS:   // leading "-" sign.
    // JS:   //
    // JS:   // Bigints are serialized as decimal strings. Decimal parsing via BigInt() is synchronous and
    // JS:   // superlinear in the number of digits, so this cap bounds the worst-case parse cost.
    // JS:   //
    // JS:   // The default is far larger than practical big-integer or cryptographic values, but far below
    // JS:   // the millions of decimal digits needed to cause meaningful blocking.
    // JS:   maxBigIntDigits: number;
    /// Maximum decimal magnitude digits in a bigint wire value.
    pub max_big_int_digits: usize,
    // JS:
    // JS:   // Maximum nesting depth of a single deserialized message. Guards against stack overflow from a
    // JS:   // deeply-nested payload (e.g. [[[[...]]]]). This is enforced uniformly across the whole message,
    // JS:   // including nested call arguments that are delivered as separate RpcPayloads.
    // JS:   maxDepth: number;
    /// Maximum nested message depth.
    pub max_depth: usize,
    // JS:
    // JS:   // Maximum size of a single incoming message string, measured in UTF-16 code units (JavaScript
    // JS:   // String.length), before it is JSON-parsed. This is not a byte count: a string of N code units
    // JS:   // may occupy more than N bytes when encoded as UTF-8. Enforced in the session read loop, after
    // JS:   // the transport has already returned a complete string, to bound JSON.parse and downstream
    // JS:   // deserialization work. True byte-level enforcement belongs in the transport/socket, so apps
    // JS:   // exposed to untrusted peers should also configure transport/socket-native payload limits where
    // JS:   // available.
    // JS:   maxMessageSize: number;
    /// Maximum incoming encoded message size.
    pub max_message_size: usize,
    // JS: }
}
// JS:
// JS: export const DEFAULT_MAX_DEPTH = 256;
/// Default maximum message depth.
pub const DEFAULT_MAX_DEPTH: usize = 256;
// JS:
// JS: export const DEFAULT_LIMITS: RpcLimits = {
/// Default receiver-side resource limits.
pub const DEFAULT_LIMITS: RpcLimits = RpcLimits {
    // JS:   maxBigIntDigits: 16384,
    max_big_int_digits: 16_384,
    // JS:   maxDepth: DEFAULT_MAX_DEPTH,
    max_depth: DEFAULT_MAX_DEPTH,
    // JS:   maxMessageSize: 32 * 1024 * 1024,
    max_message_size: 32 * 1024 * 1024,
    // JS: };
};
// JS:
// JS: // =======================================================================================
// JS:
// JS: const NATIVE_LITTLE_ENDIAN = new Uint8Array(new Uint16Array([1]).buffer)[0] === 1;
// RUST-DEFERRED(JS built-in: typed-array identity): byte strings have no container subtype.
// JS:
// JS: const BYTE_CONTAINER_TYPE_NAMES = [
// JS:   "ArrayBuffer",
// JS:   "DataView",
// JS:   "Int8Array",
// JS:   "Uint8Array",
// JS:   "Uint8ClampedArray",
// JS:   "Int16Array",
// JS:   "Uint16Array",
// JS:   "Int32Array",
// JS:   "Uint32Array",
// JS:   "BigInt64Array",
// JS:   "BigUint64Array",
// JS:   "Float32Array",
// JS:   "Float64Array",
// JS: ] as const;
// JS:
// JS: type ByteContainerTypeName = typeof BYTE_CONTAINER_TYPE_NAMES[number];
// JS: type MarkedByteContainerTypeName = Exclude<ByteContainerTypeName, "Uint8Array">;
// JS:
// JS: function isValidByteContainerName(value: string): value is ByteContainerTypeName {
// JS:   return (BYTE_CONTAINER_TYPE_NAMES as readonly string[]).includes(value);
// JS: }
// JS:
// JS: // Listing every type, including those without multi-byte elements, makes adding
// JS: // a new ByteContainerTypeName a compile error until its element size is considered.
// JS: const TYPED_ARRAY_ELEMENT_SIZE: Record<ByteContainerTypeName, number | undefined> = {
// JS:   ArrayBuffer: undefined,
// JS:   DataView: undefined,
// JS:   Int8Array: undefined,
// JS:   Uint8Array: undefined,
// JS:   Uint8ClampedArray: undefined,
// JS:   Int16Array: 2,
// JS:   Uint16Array: 2,
// JS:   Int32Array: 4,
// JS:   Uint32Array: 4,
// JS:   BigInt64Array: 8,
// JS:   BigUint64Array: 8,
// JS:   Float32Array: 4,
// JS:   Float64Array: 8,
// JS: };
// JS:
// JS: // Uint8Array intentionally isn't included because it uses the markerless legacy form.
// JS: const BYTE_CONTAINER_PROTOTYPES: Record<MarkedByteContainerTypeName, object> = {
// JS:   ArrayBuffer: ArrayBuffer.prototype,
// JS:   DataView: DataView.prototype,
// JS:   Int8Array: Int8Array.prototype,
// JS:   Uint8ClampedArray: Uint8ClampedArray.prototype,
// JS:   Int16Array: Int16Array.prototype,
// JS:   Uint16Array: Uint16Array.prototype,
// JS:   Int32Array: Int32Array.prototype,
// JS:   Uint32Array: Uint32Array.prototype,
// JS:   BigInt64Array: BigInt64Array.prototype,
// JS:   BigUint64Array: BigUint64Array.prototype,
// JS:   Float32Array: Float32Array.prototype,
// JS:   Float64Array: Float64Array.prototype,
// JS: };
// JS:
// JS: const BYTE_CONTAINER_TYPE_BY_PROTOTYPE = new Map<object, MarkedByteContainerTypeName>();
// JS: for (let type of Object.keys(BYTE_CONTAINER_PROTOTYPES) as MarkedByteContainerTypeName[]) {
// JS:   BYTE_CONTAINER_TYPE_BY_PROTOTYPE.set(BYTE_CONTAINER_PROTOTYPES[type], type);
// JS: }
// RUST-DEFERRED(JS built-in: typed-array prototypes): Rust serializes one canonical byte-string type.
// JS:
// JS: // Reverse each element's bytes in place. Production callers only need this on
// JS: // big-endian hosts; it is exported solely so the behavior can be tested on
// JS: // little-endian hosts.
// JS: export function swapByteOrder(bytes: Uint8Array, elementSize: number): void {
/// Reverses each fixed-width element's bytes in place.
pub fn swap_byte_order(bytes: &mut [u8], element_size: usize) -> Result<()> {
    // JS:   if (elementSize !== 2 && elementSize !== 4 && elementSize !== 8) {
    if !matches!(element_size, 2 | 4 | 8) {
        // JS:     throw new RangeError(`Unsupported element size: ${elementSize}`);
        return Err(Error::Message(format!(
            "Unsupported element size: {element_size}"
        )));
        // JS:   }
    }
    // JS:
    // JS:   let view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    // JS:   for (let offset = 0; offset < bytes.byteLength; offset += elementSize) {
    // JS:     switch (elementSize) {
    // JS:       case 2:
    // JS:         view.setUint16(offset, view.getUint16(offset, false), true);
    // JS:         break;
    // JS:       case 4:
    // JS:         view.setUint32(offset, view.getUint32(offset, false), true);
    // JS:         break;
    // JS:       case 8:
    // JS:         view.setBigUint64(offset, view.getBigUint64(offset, false), true);
    // JS:         break;
    // JS:     }
    // JS:   }
    for element in bytes.chunks_exact_mut(element_size) {
        element.reverse();
    }
    Ok(())
    // JS: }
}
// JS:
// JS: export interface Exporter {
/// Session callbacks used while serializing capabilities.
pub trait Exporter {
    // JS:   exportStub(hook: StubHook): ExportId;
    /// Exports a non-promise capability.
    fn export_stub(&mut self, hook: StubHook) -> Result<ExportId>;
    // JS:   exportPromise(hook: StubHook): ExportId;
    /// Exports a promise capability.
    fn export_promise(&mut self, hook: StubHook) -> Result<ExportId>;
    // JS:   getImport(hook: StubHook): ImportId | undefined;
    /// Returns the import ID when this hook came from the same peer.
    fn get_import(&self, hook: &StubHook) -> Option<ImportId>;
    // JS:
    // JS:   // If a serialization error occurs after having exported some capabilities, this will be called
    // JS:   // to roll back the exports.
    // JS:   unexport(ids: Array<ExportId>): void;
    /// Rolls back exports created by a failed serialization.
    fn unexport(&mut self, ids: Vec<ExportId>) -> Result<()>;
    // JS:
    // JS:   // Creates a pipe by sending a ["pipe"] message, then starts pumping the given ReadableStream
    // JS:   // into the pipe's writable end. Returns the import ID assigned to the pipe. `hook` should be
    // JS:   // disposed when the pipe finishes.
    // JS:   createPipe(readable: ReadableStream, hook: StubHook): ImportId;
    // RUST-DEFERRED(JS built-in: ReadableStream): pipe creation is not part of this trait yet.
    // JS:
    // JS:   onSendError(error: Error): Error | void;
    /// Optionally replaces an error raised while sending.
    fn on_send_error(&mut self, error: Error) -> Option<Error>;
    // JS: }
}
// JS:
// JS: class NullExporter implements Exporter {
#[derive(Clone, Copy, Debug, Default)]
struct NullExporter;

impl Exporter for NullExporter {
    // JS:   exportStub(stub: StubHook): never {
    fn export_stub(&mut self, _stub: StubHook) -> Result<ExportId> {
        // JS:     throw new Error("Cannot serialize RPC stubs without an RPC session.");
        Err(Error::Message(
            "Cannot serialize RPC stubs without an RPC session.".to_owned(),
        ))
        // JS:   }
    }
    // JS:   exportPromise(stub: StubHook): never {
    fn export_promise(&mut self, _stub: StubHook) -> Result<ExportId> {
        // JS:     throw new Error("Cannot serialize RPC stubs without an RPC session.");
        Err(Error::Message(
            "Cannot serialize RPC stubs without an RPC session.".to_owned(),
        ))
        // JS:   }
    }
    // JS:   getImport(hook: StubHook): ImportId | undefined {
    fn get_import(&self, _hook: &StubHook) -> Option<ImportId> {
        // JS:     return undefined;
        None
        // JS:   }
    }
    // JS:   unexport(ids: Array<ExportId>): void {}
    fn unexport(&mut self, _ids: Vec<ExportId>) -> Result<()> {
        Ok(())
    }
    // JS:   createPipe(readable: ReadableStream): never {
    // JS:     throw new Error("Cannot create pipes without an RPC session.");
    // JS:   }
    // RUST-DEFERRED(JS built-in: ReadableStream): NullExporter has no pipe method.
    // JS:
    // JS:   onSendError(error: Error): Error | void {}
    fn on_send_error(&mut self, _error: Error) -> Option<Error> {
        None
    }
    // JS: }
}
// JS:
// JS: const NULL_EXPORTER = new NullExporter();
const NULL_EXPORTER: NullExporter = NullExporter;
// JS:
// JS: // Collect all bytes from a ReadableStream into a Blob with the given MIME type. Used on the
// JS: // receive side to assemble a Blob from a pipe stream before delivering to user code.
// JS: //
// JS: // `Response` is a standard global in every runtime we support (Node >=18, browsers, workerd), so
// JS: // we can rely on `Response.blob()` for the heavy lifting. `Response.blob()` may discard the
// JS: // caller-specified MIME type, so we `slice()` to reattach it if needed.
// JS: async function streamToBlob(stream: ReadableStream, type: string): Promise<Blob> {
// JS:   let b = await new Response(stream).blob();
// JS:   return b.type === type ? b : b.slice(0, b.size, type);
// JS: }
// JS:
// JS: // Maps error name to error class for deserialization. Null-prototype so a wire-supplied (untrusted)
// JS: // name can't resolve to an inherited member like `constructor` (which would build a `String`
// JS: // wrapper, not an `Error`) or `toString`; unknown names fall back to `Error`.
// JS: const ERROR_TYPES: Record<string, any> = {
// JS:   __proto__: null,
// JS:   Error, EvalError, RangeError, ReferenceError, SyntaxError, TypeError, URIError, AggregateError,
// JS:   // TODO: DOMError? Others?
// JS: };
// JS:
// JS: // Converts fully-hydrated messages into object trees that are JSON-serializable for sending over
// JS: // the wire. This is used to implement serialization -- but it doesn't take the last step of
// JS: // actually converting to a string. (The name is meant to be the opposite of "Evaluator", which
// JS: // implements the opposite direction.)
// JS: export class Devaluator {
/// Converts dynamic RPC values to their wire expression tree.
pub struct Devaluator<'a> {
    exporter: &'a mut dyn Exporter,
    source: Option<&'a RpcPayload>,
    encoding_level: EncodingLevel,
    exports: Vec<ExportId>,
}

impl<'a> Devaluator<'a> {
    // JS:   private constructor(
    // JS:     private exporter: Exporter,
    // JS:     private source: RpcPayload | undefined,
    // JS:     private encodingLevel: EncodingLevel
    // JS:   ) {}
    // JS:
    // JS:   // Devaluate the given value.
    // JS:   // * value: The value to devaluate.
    // JS:   // * parent: The value's parent object, which would be used as `this` if the value were called
    // JS:   //     as a function.
    // JS:   // * exporter: Callbacks to the RPC session for exporting capabilities found in this message.
    // JS:   // * source: The RpcPayload which contains the value, and therefore owns stubs within.
    // JS:   // * encodingLevel: How much encoding to apply (default "string").
    // JS:   //
    // JS:   // Returns: The devaluated value, ready to be JSON-serialized (or passed to transport directly
    // JS:   // for non-string levels).
    // JS:   public static devaluate(
    // JS:       value: unknown, parent?: object, exporter: Exporter = NULL_EXPORTER, source?: RpcPayload,
    // JS:       encodingLevel: EncodingLevel = "string")
    // JS:       : unknown {
    // JS:     let devaluator = new Devaluator(exporter, source, encodingLevel);
    // JS:     try {
    // JS:       return devaluator.devaluateImpl(value, parent, 0);
    // JS:     } catch (err) {
    // JS:       if (devaluator.exports) {
    // JS:         try {
    // JS:           exporter.unexport(devaluator.exports);
    // JS:         } catch (err) {
    // JS:           // probably a side effect of the original error, ignore it
    // JS:         }
    // JS:       }
    // JS:       // TODO: This rollback only releases exports. Pipes created via `createPipe` (for
    // JS:       // ReadableStreams, Blobs, and the Firefox request-body fallback) have already sent a
    // JS:       // ["pipe"] frame and started pumping.
    // JS:       throw err;
    // JS:     }
    // JS:   }
    /// Devaluates one value and rolls back capabilities if any later branch fails.
    pub fn devaluate(
        value: &CapnwebValue,
        exporter: &'a mut dyn Exporter,
        source: Option<&'a RpcPayload>,
        encoding_level: EncodingLevel,
    ) -> Result<WireValue> {
        let mut devaluator = Self {
            exporter,
            source,
            encoding_level,
            exports: Vec::new(),
        };
        match devaluator.devaluate_impl(value, 0) {
            Ok(value) => Ok(value),
            Err(error) => {
                let exports = std::mem::take(&mut devaluator.exports);
                let _ = devaluator.exporter.unexport(exports);
                Err(error)
            }
        }
    }
    // JS:
    // JS:   private exports?: Array<ExportId>;
    // `exports` is the rollback list stored on the Rust struct above.
    // JS:
    // JS:   private devaluateImpl(value: unknown, parent: object | undefined, depth: number): unknown {
    // JS:     if (depth >= DEFAULT_MAX_DEPTH) {
    // JS:       throw new Error(
    // JS:           "Serialization exceeded maximum allowed depth. (Does the message contain cycles?)");
    // JS:     }
    // JS:
    // JS:     let kind = typeForRpc(value);
    // JS:     switch (kind) {
    // JS:       case "unsupported": {
    // JS:         let msg;
    // JS:         try {
    // JS:           msg = `Cannot serialize value: ${value}`;
    // JS:         } catch (err) {
    // JS:           msg = "Cannot serialize value: (couldn't stringify value)";
    // JS:         }
    // JS:         throw new TypeError(msg);
    // JS:       }
    // JS:
    // JS:       case "primitive":
    // JS:         if (typeof value === "number" && !isFinite(value)) {
    // JS:           // At structuredClonable level, keep Infinity/NaN as native values
    // JS:           if (this.encodingLevel === "structuredClonable") {
    // JS:             return value;
    // JS:           }
    // JS:           if (value === Infinity) {
    // JS:             return ["inf"];
    // JS:           } else if (value === -Infinity) {
    // JS:             return ["-inf"];
    // JS:           } else {
    // JS:             return ["nan"];
    // JS:           }
    // JS:         } else {
    // JS:           // Supported directly by JSON.
    // JS:           return value;
    // JS:         }
    // JS:
    // JS:       case "object": {
    // JS:         let object = <Record<string, unknown>>value;
    // JS:         let result: Record<string, unknown> = {};
    // JS:         for (let key in object) {
    // JS:           result[key] = this.devaluateImpl(object[key], object, depth + 1);
    // JS:         }
    // JS:         return result;
    // JS:       }
    // JS:
    // JS:       case "array": {
    // JS:         let array = <Array<unknown>>value;
    // JS:         let len = array.length;
    // JS:         let result = new Array(len);
    // JS:         for (let i = 0; i < len; i++) {
    // JS:           result[i] = this.devaluateImpl(array[i], array, depth + 1);
    // JS:         }
    // JS:         // Wrap literal arrays in an outer one-element array, to "escape" them.
    // JS:         return [result];
    // JS:       }
    // JS:
    // JS:       case "bigint":
    // JS:         // At structuredClonable level, keep BigInt as native value
    // JS:         if (this.encodingLevel === "structuredClonable") {
    // JS:           return value;
    // JS:         }
    // JS:         return ["bigint", (<bigint>value).toString()];
    // JS:
    // JS:       case "date": {
    // JS:         // At structuredClonable level, keep Date as native value
    // JS:         if (this.encodingLevel === "structuredClonable") {
    // JS:           return value;
    // JS:         }
    // JS:         const time = (<Date>value).getTime();
    // JS:         return ["date", Number.isNaN(time) ? null : time];
    // JS:       }
    // JS:
    // JS:       case "bytes": {
    // JS:         let alternateTypeName = BYTE_CONTAINER_TYPE_BY_PROTOTYPE.get(Object.getPrototypeOf(value));
    // JS:         let bytes: Uint8Array;
    // JS:         if (alternateTypeName === "ArrayBuffer") {
    // JS:           bytes = new Uint8Array(value as ArrayBuffer);
    // JS:         } else if (alternateTypeName === undefined) {
    // JS:           bytes = value as Uint8Array;
    // JS:         } else {
    // JS:           let view = value as ArrayBufferView;
    // JS:           bytes = new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
    // JS:           let elementSize = TYPED_ARRAY_ELEMENT_SIZE[alternateTypeName];
    // JS:           if (!NATIVE_LITTLE_ENDIAN && elementSize) {
    // JS:             bytes = bytes.slice();
    // JS:             swapByteOrder(bytes, elementSize);
    // JS:           }
    // JS:         }
    // JS:
    // JS:         // At structuredClonable or jsonCompatibleWithBytes level, keep the bytes raw.
    // JS:         if (this.encodingLevel === "structuredClonable" ||
    // JS:             this.encodingLevel === "jsonCompatibleWithBytes") {
    // JS:           return alternateTypeName === undefined
    // JS:               ? ["bytes", bytes] : ["bytes", bytes, alternateTypeName];
    // JS:         }
    // JS:
    // JS:         let b64: string;
    // JS:         if (bytes.toBase64) {
    // JS:           b64 = bytes.toBase64({omitPadding: true});
    // JS:         } else if (typeof Buffer !== "undefined") {
    // JS:           let buf = bytes instanceof Buffer ? bytes
    // JS:               : Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    // JS:           b64 = buf.toString("base64");
    // JS:         } else {
    // JS:           let binary = "";
    // JS:           for (let i = 0; i < bytes.length; i++) {
    // JS:             binary += String.fromCharCode(bytes[i]);
    // JS:           }
    // JS:           b64 = btoa(binary);
    // JS:         }
    // JS:         b64 = b64.replace(/=+$/, "");
    // JS:         return alternateTypeName === undefined ? ["bytes", b64] : ["bytes", b64, alternateTypeName];
    // JS:       }
    // JS:
    // JS:       case "url":
    // JS:         // Always tuple-encode URLs; structured-clone support for URL isn't universal.
    // JS:         return ["url", (value as URL).href];
    // JS:
    // JS:       case "headers":
    // JS:         // The `Headers` TS type apparently doesn't declare itself as being
    // JS:         // Iterable<[string, string]>, but it is.
    // JS:         return ["headers", [...<Iterable<[string, string]>>value]];
    // JS:
    // JS:       case "request": {
    // JS:         let req = <Request>value;
    // JS:         let init: Record<string, unknown> = {};
    // JS:
    // JS:         // For many properties below, the official Fetch spec says they must always be present,
    // JS:         // but some platforms don't support them. So, we check both whether the property exists,
    // JS:         // and whether it is equal to the default, before bothering to add it to `init`.
    // JS:
    // JS:         if (req.method !== "GET") init.method = req.method;
    // JS:
    // JS:         let headers = [...<Iterable<[string, string]>><any>req.headers];
    // JS:         if (headers.length > 0) {
    // JS:           // Note that we don't need to serialize this as ["headers", headers] because we are only
    // JS:           // trying to create a valid RequestInit object.
    // JS:           init.headers = headers;
    // JS:         }
    // JS:
    // JS:         if (req.body) {
    // JS:           init.body = this.devaluateImpl(req.body, req, depth + 1);
    // JS:
    // JS:           // Apparently the fetch spec technically requires that `duplex` be specified when a
    // JS:           // body is specified, and Chrome in fact requires this, and requires the value is "half".
    // JS:           // Workers hasn't implemented this (and actually supports full duplex by default, lol).
    // JS:           // The TS types for Request currently don't define this property, but it is there (on
    // JS:           // Chrome at least).
    // JS:           init.duplex = (<any>req).duplex || "half";
    // JS:         } else if (req.body === undefined &&
    // JS:             !["GET", "HEAD", "OPTIONS", "TRACE", "DELETE"].includes(req.method)) {
    // JS:           // If the body is undefined rather than null, most likely we're on a platform that
    // JS:           // doesn't support request body streams (*cough*Firefox*cough*). We'll need to hack
    // JS:           // around this by using `req.arrayBuffer()` to get the body. Unfortunately this is async,
    // JS:           // so we can't just embed the resulting body into the message we are constructing. We
    // JS:           // will actually have to construct a ReadableStream. Ugh!
    // JS:
    // JS:           let bodyPromise = req.arrayBuffer();
    // JS:
    // JS:           let readable = new ReadableStream<Uint8Array>({
    // JS:             async start(controller) {
    // JS:               try {
    // JS:                 // `as Uint8Array` is needed here to work around some sort of weird bug in the TS
    // JS:                 // types where `new Uint8Array` somehow doesn't return a `Uint8Array`. Instead it
    // JS:                 // somehow returns `Uint8Array<ArrayBuffer>` -- but `Uint8Array` is not a generic
    // JS:                 // type! WTF?
    // JS:                 // TODO(cleanup): This is apparently fixed in TS 6.
    // JS:                 controller.enqueue(new Uint8Array(await bodyPromise) as Uint8Array);
    // JS:                 controller.close();
    // JS:               } catch (err) {
    // JS:                 controller.error(err);
    // JS:               }
    // JS:             }
    // JS:           });
    // JS:
    // JS:           // We can't recurse to devaluateImpl() to serialize the body because it'll call
    // JS:           // source.getHookForReadableStream(), adding a hook on the payload which isn't actually
    // JS:           // reachable by walking the payload, which will cause trouble later. So we have to
    // JS:           // inline it a bit here...
    // JS:           let hook = streamImpl.createReadableStreamHook(readable);
    // JS:           let importId = this.exporter.createPipe(readable, hook);
    // JS:           init.body = ["readable", importId];
    // JS:           init.duplex = (<any>req).duplex || "half";
    // JS:         }
    // JS:
    // JS:         if (req.cache && req.cache !== "default") init.cache = req.cache;
    // JS:         if (req.redirect !== "follow") init.redirect = req.redirect;
    // JS:         if (req.integrity) init.integrity = req.integrity;
    // JS:
    // JS:         // These properties are only meaningful in browsers and not supported by most WinterCG
    // JS:         // (server-side) platforms.
    // JS:         if (req.mode && req.mode !== "cors") init.mode = req.mode;
    // JS:         if (req.credentials && req.credentials !== "same-origin") {
    // JS:           init.credentials = req.credentials;
    // JS:         }
    // JS:         if (req.referrer && req.referrer !== "about:client") init.referrer = req.referrer;
    // JS:         if (req.referrerPolicy) init.referrerPolicy = req.referrerPolicy;
    // JS:         if (req.keepalive) init.keepalive = req.keepalive;
    // JS:
    // JS:         // These properties are specific to Cloudflare Workers. Cast the request to `any` to
    // JS:         // silence type errors on other platforms.
    // JS:         let cfReq = req as any;
    // JS:         if (cfReq.cf) init.cf = cfReq.cf;
    // JS:         if (cfReq.encodeResponseBody && cfReq.encodeResponseBody !== "automatic") {
    // JS:           init.encodeResponseBody = cfReq.encodeResponseBody;
    // JS:         }
    // JS:
    // JS:         // TODO: Support request.signal. Annoyingly, all `Request`s have a `signal` property even
    // JS:         //   if none was passed to the constructor, and there's no way to tell if it's a real
    // JS:         //   signal. So for now, since we don't support AbortSignal yet, all we can do is ignore
    // JS:         //   it; we can't throw an error if it's present.
    // JS:
    // JS:         return ["request", req.url, init];
    // JS:       }
    // JS:
    // JS:       case "response": {
    // JS:         let resp = <Response>value;
    // JS:         let body = this.devaluateImpl(resp.body, resp, depth + 1);
    // JS:         let init: Record<string, unknown> = {};
    // JS:
    // JS:         if (resp.status !== 200) init.status = resp.status;
    // JS:         if (resp.statusText) init.statusText = resp.statusText;
    // JS:
    // JS:         let headers = [...<Iterable<[string, string]>><any>resp.headers];
    // JS:         if (headers.length > 0) {
    // JS:           // Note that we don't need to serialize this as ["headers", headers] because we are only
    // JS:           // trying to create a valid ResponseInit object.
    // JS:           init.headers = headers;
    // JS:         }
    // JS:
    // JS:         // These properties are specific to Cloudflare Workers. Cast the request to `any` to
    // JS:         // silence type errors on other platforms.
    // JS:         let cfResp = resp as any;
    // JS:         if (cfResp.cf) init.cf = cfResp.cf;
    // JS:         if (cfResp.encodeBody && cfResp.encodeBody !== "automatic") {
    // JS:           init.encodeBody = cfResp.encodeBody;
    // JS:         }
    // JS:         if (cfResp.webSocket) {
    // JS:           // As of this writing, we don't support WebSocket, but we might someday.
    // JS:           throw new TypeError("Can't serialize a Response containing a webSocket.");
    // JS:         }
    // JS:
    // JS:         return ["response", body, init];
    // JS:       }
    // JS:
    // JS:       case "blob": {
    // JS:         // Blobs are streamed through a pipe. This allows very large blobs to be sent without
    // JS:         // causing excessively large individual messages nor blocking other messages in the
    // JS:         // meantime.
    // JS:         //
    // JS:         // Ideally, small Blobs would be inlined. But, there is no way to read a blob
    // JS:         // synchronously, and we MUST serialize the message synchronously. Hence, we have no choice
    // JS:         // but to use streaming even for small blobs.
    // JS:         let blob = value as Blob;
    // JS:         let readable = blob.stream();
    // JS:         let hook = streamImpl.createReadableStreamHook(readable);
    // JS:         let importId = this.exporter.createPipe(readable, hook);
    // JS:         return ["blob", blob.type, ["readable", importId]];
    // JS:       }
    // JS:
    // JS:       case "error": {
    // JS:         let e = <Error>value;
    // JS:
    // JS:         // TODO:
    // JS:         // - Determine type by checking prototype rather than `name`, which can be overridden?
    // JS:
    // JS:         let rewritten = this.exporter.onSendError(e);
    // JS:         if (rewritten) {
    // JS:           e = rewritten;
    // JS:         }
    // JS:
    // JS:         // Capture own enumerable properties plus the standard non-enumerable slots `cause`
    // JS:         // and (for AggregateError) `errors`. Each value is run through devaluateImpl so any
    // JS:         // supported type round-trips. If a property's value can't be serialized, drop the
    // JS:         // property: the error itself must always make it through. Use `onSendError` to scrub
    // JS:         // heavy or sensitive fields explicitly.
    // JS:         //
    // JS:         // On per-property failure we roll back any exports the partial walk produced by
    // JS:         // splicing them off `this.exports` and unexporting them.
    // JS:         //
    // JS:         // TODO: this can't roll back pipes created by `createPipe` (ReadableStream, Blob,
    // JS:         // Firefox request-body); the `["pipe"]` frame and pump have already started, with
    // JS:         // no inverse on the `Exporter` interface, so they leak until session shutdown.
    // JS:         // Same caveat as the rollback in the static `devaluate` method above.
    // JS:         let anyE = <any>e;
    // JS:         let props: Record<string, unknown> | undefined;
    // JS:         let captureProp = (key: string, val: unknown) => {
    // JS:           let exportsBefore = this.exports?.length ?? 0;
    // JS:           try {
    // JS:             let encoded = this.devaluateImpl(val, e, depth + 1);
    // JS:             if (!props) props = {};
    // JS:             props[key] = encoded;
    // JS:           } catch (err) {
    // JS:             // Drop this property; the error itself still propagates. Roll back any exports
    // JS:             // the partial walk produced.
    // JS:             if (this.exports && this.exports.length > exportsBefore) {
    // JS:               let tail = this.exports.splice(exportsBefore);
    // JS:               try {
    // JS:                 this.exporter.unexport(tail);
    // JS:               } catch (err2) {
    // JS:                 // probably a side effect of the original error, ignore it
    // JS:               }
    // JS:             }
    // JS:           }
    // JS:         };
    // JS:         for (let key of Object.keys(e)) {
    // JS:           if (key === "name" || key === "message" || key === "stack") continue;
    // JS:           captureProp(key, anyE[key]);
    // JS:         }
    // JS:         // `cause` is normally non-enumerable, so Object.keys() misses it.
    // JS:         if ("cause" in e) {
    // JS:           captureProp("cause", anyE.cause);
    // JS:         }
    // JS:         if (e instanceof AggregateError) {
    // JS:           captureProp("errors", e.errors);
    // JS:         }
    // JS:
    // JS:         // Backwards-compat: only emit the new tail elements when there's something to add.
    // JS:         // Errors with no extras serialize to the legacy 3- or 4-element form, byte-identical
    // JS:         // to what previous versions produced.
    // JS:         let result: unknown[] = ["error", e.name, e.message];
    // JS:         if (props) {
    // JS:           // Normalize the stack slot to null so `props` is always at index 4.
    // JS:           result.push(rewritten && rewritten.stack ? rewritten.stack : null);
    // JS:           result.push(props);
    // JS:         } else if (rewritten && rewritten.stack) {
    // JS:           result.push(rewritten.stack);
    // JS:         }
    // JS:         return result;
    // JS:       }
    // JS:
    // JS:       case "undefined":
    // JS:         // At structuredClonable level, keep undefined as native value
    // JS:         if (this.encodingLevel === "structuredClonable") {
    // JS:           return undefined;
    // JS:         }
    // JS:         return ["undefined"];
    // JS:
    // JS:       case "stub":
    // JS:       case "rpc-promise": {
    // JS:         if (!this.source) {
    // JS:           throw new Error("Can't serialize RPC stubs in this context.");
    // JS:         }
    // JS:
    // JS:         let {hook, pathIfPromise} = unwrapStubAndPath(<RpcStub>value);
    // JS:         let importId = this.exporter.getImport(hook);
    // JS:         if (importId !== undefined) {
    // JS:           if (pathIfPromise) {
    // JS:             // It's a promise pointing back to the peer, so we are doing pipelining here.
    // JS:             if (pathIfPromise.length > 0) {
    // JS:               return ["pipeline", importId, pathIfPromise];
    // JS:             } else {
    // JS:               return ["pipeline", importId];
    // JS:             }
    // JS:           } else {
    // JS:             return ["import", importId];
    // JS:           }
    // JS:         }
    // JS:
    // JS:         if (pathIfPromise) {
    // JS:           hook = hook.get(pathIfPromise);
    // JS:         } else {
    // JS:           hook = hook.dup();
    // JS:         }
    // JS:
    // JS:         return this.devaluateHook(pathIfPromise ? "promise" : "export", hook);
    // JS:       }
    // JS:
    // JS:       case "function":
    // JS:       case "rpc-target": {
    // JS:         if (!this.source) {
    // JS:           throw new Error("Can't serialize RPC stubs in this context.");
    // JS:         }
    // JS:
    // JS:         let hook = this.source.getHookForRpcTarget(<RpcTarget|Function>value, parent);
    // JS:         return this.devaluateHook("export", hook);
    // JS:       }
    // JS:
    // JS:       case "rpc-thenable": {
    // JS:         if (!this.source) {
    // JS:           throw new Error("Can't serialize RPC stubs in this context.");
    // JS:         }
    // JS:
    // JS:         let hook = this.source.getHookForRpcTarget(<RpcTarget>value, parent);
    // JS:         return this.devaluateHook("promise", hook);
    // JS:       }
    // JS:
    // JS:       case "writable": {
    // JS:         if (!this.source) {
    // JS:           throw new Error("Can't serialize WritableStream in this context.");
    // JS:         }
    // JS:
    // JS:         let hook = this.source.getHookForWritableStream(<WritableStream>value, parent);
    // JS:         return this.devaluateHook("writable", hook);
    // JS:       }
    // JS:
    // JS:       case "readable": {
    // JS:         if (!this.source) {
    // JS:           throw new Error("Can't serialize ReadableStream in this context.");
    // JS:         }
    // JS:
    // JS:         let ws = <ReadableStream>value;
    // JS:         let hook = this.source.getHookForReadableStream(ws, parent);
    // JS:
    // JS:         // Create a pipe and start pumping the ReadableStream into it.
    // JS:         let importId = this.exporter.createPipe(ws, hook);
    // JS:
    // JS:         return ["readable", importId];
    // JS:       }
    // JS:
    // JS:       default:
    // JS:         kind satisfies never;
    // JS:         throw new Error("unreachable");
    // JS:     }
    // JS:   }
    fn devaluate_impl(&mut self, value: &CapnwebValue, depth: usize) -> Result<WireValue> {
        let _encoding_level = self.encoding_level;
        if depth >= DEFAULT_MAX_DEPTH {
            return Err(Error::Message(
                "Serialization exceeded maximum allowed depth. (Does the message contain cycles?)"
                    .to_owned(),
            ));
        }

        match value {
            CapnwebValue::Null => Ok(WireValue::Null),
            CapnwebValue::Bool(value) => Ok(WireValue::Bool(*value)),
            CapnwebValue::Number(value) if value.is_finite() => Number::from_f64(*value)
                .map(WireValue::Number)
                .ok_or_else(|| Error::Type("invalid finite number".to_owned())),
            CapnwebValue::Number(value) if value.is_nan() => Ok(wire_tag("nan", Vec::new())),
            CapnwebValue::Number(value) if value.is_sign_positive() => {
                Ok(wire_tag("inf", Vec::new()))
            }
            CapnwebValue::Number(_) => Ok(wire_tag("-inf", Vec::new())),
            CapnwebValue::String(value) => Ok(WireValue::String(value.clone())),
            CapnwebValue::BigInt(value) => Ok(wire_tag(
                "bigint",
                vec![WireValue::String(value.to_string())],
            )),
            CapnwebValue::Bytes(value) => Ok(wire_tag(
                "bytes",
                vec![WireValue::String(
                    base64::engine::general_purpose::STANDARD_NO_PAD.encode(value),
                )],
            )),
            CapnwebValue::Array(values) => {
                let values = values
                    .iter()
                    .map(|value| self.devaluate_impl(value, depth + 1))
                    .collect::<Result<Vec<_>>>()?;
                Ok(WireValue::Array(vec![WireValue::Array(values)]))
            }
            CapnwebValue::Object(values) => {
                let values = values
                    .iter()
                    .map(|(key, value)| Ok((key.clone(), self.devaluate_impl(value, depth + 1)?)))
                    .collect::<Result<serde_json::Map<_, _>>>()?;
                Ok(WireValue::Object(values))
            }
            CapnwebValue::Error(error) => {
                let error = self
                    .exporter
                    .on_send_error(error.clone())
                    .unwrap_or_else(|| error.clone());
                Ok(wire_tag(
                    "error",
                    vec![
                        WireValue::String("Error".to_owned()),
                        WireValue::String(error.to_string()),
                    ],
                ))
            }
            CapnwebValue::Stub(stub) => self.devaluate_stub(stub, false),
            CapnwebValue::RpcPromise(promise) => self.devaluate_stub(promise.as_stub(), true),
            CapnwebValue::Target(target) => {
                let source = self.source.ok_or_else(|| {
                    Error::Message("Can't serialize RPC targets in this context.".to_owned())
                })?;
                self.devaluate_hook(
                    "export",
                    source.get_hook_for_rpc_target(Arc::clone(target), false),
                )
            }
            CapnwebValue::Function(target) => {
                let source = self.source.ok_or_else(|| {
                    Error::Message("Can't serialize RPC functions in this context.".to_owned())
                })?;
                self.devaluate_hook(
                    "export",
                    source.get_hook_for_rpc_target(Arc::clone(target), true),
                )
            }
        }
    }

    fn devaluate_stub(&mut self, stub: &crate::core::RpcStub, promise: bool) -> Result<WireValue> {
        if self.source.is_none() {
            return Err(Error::Message(
                "Can't serialize RPC stubs in this context.".to_owned(),
            ));
        }

        let (hook, path) = unwrap_stub_and_path(stub);
        if let Some(import_id) = self.exporter.get_import(&hook) {
            if let Some(path) = path {
                let mut expression = vec![
                    WireValue::String("pipeline".to_owned()),
                    WireValue::Number(import_id.into()),
                ];
                if !path.is_empty() {
                    expression.push(property_path_to_wire(path)?);
                }
                return Ok(WireValue::Array(expression));
            }
            return Ok(wire_tag(
                "import",
                vec![WireValue::Number(import_id.into())],
            ));
        }

        let hook = match path {
            Some(path) => hook.get(path)?,
            None => hook,
        };
        self.devaluate_hook(if promise { "promise" } else { "export" }, hook)
    }
    // JS:
    // JS:   private devaluateHook(type: "export" | "promise" | "writable", hook: StubHook): unknown {
    // JS:     if (!this.exports) this.exports = [];
    // JS:     let exportId = type === "promise" ? this.exporter.exportPromise(hook)
    // JS:                                       : this.exporter.exportStub(hook);
    // JS:     this.exports.push(exportId);
    // JS:     return [type, exportId];
    // JS:   }
    fn devaluate_hook(&mut self, kind: &'static str, hook: StubHook) -> Result<WireValue> {
        let export_id = if kind == "promise" {
            self.exporter.export_promise(hook)?
        } else {
            self.exporter.export_stub(hook)?
        };
        self.exports.push(export_id);
        Ok(wire_tag(kind, vec![WireValue::Number(export_id.into())]))
    }
    // JS: }
}

fn wire_tag(tag: &str, tail: Vec<WireValue>) -> WireValue {
    let mut expression = Vec::with_capacity(tail.len() + 1);
    expression.push(WireValue::String(tag.to_owned()));
    expression.extend(tail);
    WireValue::Array(expression)
}

fn property_path_to_wire(path: Vec<PropertyKey>) -> Result<WireValue> {
    path.into_iter()
        .map(|part| match part {
            PropertyKey::String(value) => Ok(WireValue::String(value)),
            PropertyKey::Number(value) => Number::from_f64(value)
                .map(WireValue::Number)
                .ok_or_else(|| {
                    Error::Type("property path contains a non-finite number".to_owned())
                }),
        })
        .collect::<Result<Vec<_>>>()
        .map(WireValue::Array)
}
// JS:
// JS: /**
// JS:  * Serialize a value, using Cap'n Web's underlying serialization. This won't be able to serialize
// JS:  * RPC stubs, but it will support basic data types.
// JS:  */
// JS: export function serialize(value: unknown): string {
// JS:   return JSON.stringify(Devaluator.devaluate(value));
// JS: }
/// Serializes a standalone pass-by-value value as Cap'n Web JSON.
pub fn serialize(value: &CapnwebValue) -> Result<String> {
    let mut exporter = NULL_EXPORTER;
    let wire = Devaluator::devaluate(value, &mut exporter, None, EncodingLevel::String)?;
    serde_json::to_string(&wire)
        .map_err(|error| Error::Message(format!("failed to encode JSON: {error}")))
}
// JS:
// JS: // =======================================================================================
// JS:
// JS: export interface Importer {
// JS:   importStub(idx: ImportId): StubHook;
// JS:   importPromise(idx: ImportId): StubHook;
// JS:   getExport(idx: ExportId): StubHook | undefined;
// JS:
// JS:   // Retrieves the ReadableStream end of a pipe created by a ["pipe"] message.
// JS:   // The exportId must refer to an export that was created as a pipe.
// JS:   // This can only be called once per pipe.
// JS:   getPipeReadable(exportId: ExportId): ReadableStream;
// JS:
// JS:   // The resource limits the Evaluator should enforce while deserializing. Surfaced through the
// JS:   // Importer (rather than the Evaluator constructor) so that the per-session options reach the
// JS:   // Evaluator without changing how Evaluators are constructed throughout the codebase.
// JS:   getLimits(): RpcLimits;
// JS: }
// JS:
// JS: class NullImporter implements Importer {
// JS:   importStub(idx: ImportId): never {
// JS:     throw new Error("Cannot deserialize RPC stubs without an RPC session.");
// JS:   }
// JS:   importPromise(idx: ImportId): never {
// JS:     throw new Error("Cannot deserialize RPC stubs without an RPC session.");
// JS:   }
// JS:   getExport(idx: ExportId): StubHook | undefined {
// JS:     return undefined;
// JS:   }
// JS:   getPipeReadable(exportId: ExportId): never {
// JS:     throw new Error("Cannot retrieve pipe readable without an RPC session.");
// JS:   }
// JS:   getLimits(): RpcLimits {
// JS:     // The standalone deserialize() entry point has no session and therefore no per-session
// JS:     // overrides, but it must still be protected -- so it enforces the bare defaults.
// JS:     return DEFAULT_LIMITS;
// JS:   }
// JS: }
// JS:
// JS: const NULL_IMPORTER = new NullImporter();
// JS:
// JS: // Some runtimes (Firefox) don't support `request.body` as a stream, but we receive request bodies
// JS: // as streams. We'll need to read the body into an ArrayBuffer and recreate the request. This is
// JS: // asynchronous, so we'll have to swap in a promise here. This potentially breaks e-order but
// JS: // that's something people will just have to live with when sending a Request to a Firefox
// JS: // endpoint (probably rare).
// JS: function fixBrokenRequestBody(request: Request, body: ReadableStream): RpcPromise {
// JS:   // Reuse built-in code to read the stream into an array.
// JS:   let promise = new Response(body).arrayBuffer().then(arrayBuffer => {
// JS:     let bytes = new Uint8Array(arrayBuffer);
// JS:     let result = new Request(request, {body: bytes});
// JS:     return new PayloadStubHook(RpcPayload.fromAppReturn(result));
// JS:   });
// JS:   return new RpcPromise(new PromiseStubHook(promise), []);
// JS: }
// JS:
// JS: // Unfortuntaely, even though Blobs can only be read asynchronously, there is no way to create
// JS: // a blob backed by an asynchronous source; the bytes MUST all be provided upfront. This
// JS: // effectively makes it impossible to manitain e-order when sending Blobs.
// JS: //
// JS: // As a compromise, we deliver a message as if it contained an RpcPromise that resolves to the
// JS: // Blob. This has the effect that the RPC system will wait for the whole Blob to stream in before
// JS: // delivering the message -- reusing the existing machinery for handling promises.
// JS: function streamToBlobPromise(stream: ReadableStream, type: string): RpcPromise {
// JS:   let promise = streamToBlob(stream, type).then(blob => {
// JS:     return new PayloadStubHook(RpcPayload.fromAppReturn(blob));
// JS:   });
// JS:   return new RpcPromise(new PromiseStubHook(promise), []);
// JS: }
// JS:
// JS: // Takes object trees parse from JSON and converts them into fully-hydrated JavaScript objects for
// JS: // delivery to the app. This is used to implement deserialization, except that it doesn't actually
// JS: // start from a raw string.
// JS: export class Evaluator {
// JS:   private limits: RpcLimits;
// JS:
// JS:   constructor(private importer: Importer, private encodingLevel: EncodingLevel = "string") {
// JS:     this.limits = importer.getLimits();
// JS:   }
// JS:
// JS:   private hooks: StubHook[] = [];
// JS:   private promises: LocatedPromise[] = [];
// JS:
// JS:   public evaluate(value: unknown): RpcPayload {
// JS:     return this.evaluateWithDepth(value, 0);
// JS:   }
// JS:
// JS:   private evaluateWithDepth(value: unknown, depth: number): RpcPayload {
// JS:     let payload = RpcPayload.forEvaluate(this.hooks, this.promises);
// JS:     try {
// JS:       payload.value = this.evaluateImpl(value, payload, "value", depth);
// JS:       return payload;
// JS:     } catch (err) {
// JS:       payload.dispose();
// JS:       throw err;
// JS:     }
// JS:   }
// JS:
// JS:   // Evaluate the value without destroying it.
// JS:   public evaluateCopy(value: unknown): RpcPayload {
// JS:     return this.evaluate(structuredClone(value));
// JS:   }
// JS:
// JS:   private evaluateImpl(
// JS:       value: unknown, parent: object, property: string | number, depth: number): unknown {
// JS:     let maxDepth = this.limits.maxDepth;
// JS:     if (depth >= maxDepth) {
// JS:       throw new TypeError(
// JS:           `Deserialization exceeded maximum allowed message depth of ${maxDepth}.`);
// JS:     }
// JS:
// JS:     // At structuredClonable level, some native types pass through devaluation unencoded: Date and
// JS:     // BigInt (as well as undefined and non-finite numbers, which the generic paths below already
// JS:     // handle). Note that bytes and errors are tuple-encoded at every level, so raw `Uint8Array`
// JS:     // and `Error` values are intentionally *not* accepted here.
// JS:     if (this.encodingLevel === "structuredClonable") {
// JS:       if (value instanceof Date || typeof value === "bigint") {
// JS:         return value;
// JS:       }
// JS:     }
// JS:
// JS:     if (value instanceof Array) {
// JS:       if (value.length == 1 && value[0] instanceof Array) {
// JS:         // Escaped array. Evaluate the contents.
// JS:         let result = value[0];
// JS:         for (let i = 0; i < result.length; i++) {
// JS:           result[i] = this.evaluateImpl(result[i], result, i, depth + 1);
// JS:         }
// JS:         return result;
// JS:       } else switch (value[0]) {
// JS:         case "bigint":
// JS:           if (typeof value[1] == "string") {
// JS:             let digits = value[1];
// JS:             let maxBigIntDigits = this.limits.maxBigIntDigits;
// JS:             // Cap the length before the superlinear BigInt() parse (DoS guard).
// JS:             if (digits.length > maxBigIntDigits) {
// JS:               throw new TypeError(
// JS:                   `Deserialized bigint exceeds maximum length of ${maxBigIntDigits} digits.`);
// JS:             }
// JS:             return BigInt(digits);
// JS:           }
// JS:           break;
// JS:         case "date":
// JS:           if (value[1] === null) {
// JS:             return new Date(NaN);
// JS:           }
// JS:           if (typeof value[1] == "number") {
// JS:             return new Date(value[1]);
// JS:           }
// JS:           break;
// JS:         case "bytes": {
// JS:           let bytes: Uint8Array;
// JS:           // At jsonCompatibleWithBytes/structuredClonable level, bytes may already be raw.
// JS:           if (value[1] instanceof Uint8Array) {
// JS:             bytes = value[1];
// JS:           } else if (typeof value[1] == "string") {
// JS:             if (typeof Buffer !== "undefined") {
// JS:               bytes = Buffer.from(value[1], "base64");
// JS:             } else if (Uint8Array.fromBase64) {
// JS:               bytes = Uint8Array.fromBase64(value[1]);
// JS:             } else {
// JS:               let bs = atob(value[1]);
// JS:               let len = bs.length;
// JS:               bytes = new Uint8Array(len);
// JS:               for (let i = 0; i < len; i++) {
// JS:                 bytes[i] = bs.charCodeAt(i);
// JS:               }
// JS:             }
// JS:           } else {
// JS:             break;
// JS:           }
// JS:
// JS:           if (value.length === 2) {
// JS:             return bytes;
// JS:           }
// JS:           if (typeof value[2] !== "string") {
// JS:             throw new TypeError(`Unknown bytes type marker type: ${typeof value[2]}`);
// JS:           }
// JS:
// JS:           if (!isValidByteContainerName(value[2])) {
// JS:             let marker = value[2].slice(0, 64);
// JS:             throw new TypeError(`Unknown bytes type marker: ${marker}`);
// JS:           }
// JS:
// JS:           let marker = value[2];
// JS:           let elementSize = TYPED_ARRAY_ELEMENT_SIZE[marker];
// JS:           if (elementSize !== undefined && bytes.byteLength % elementSize !== 0) {
// JS:             throw new TypeError(
// JS:                 `Invalid byte length ${bytes.byteLength} for ${marker}; ` +
// JS:                 `expected a multiple of ${elementSize}`);
// JS:           }
// JS:
// JS:           // Copy exactly the decoded range rather than exposing or aliasing a pooled Buffer.
// JS:           let buffer = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
// JS:           if (!NATIVE_LITTLE_ENDIAN && elementSize !== undefined) {
// JS:             swapByteOrder(new Uint8Array(buffer), elementSize);
// JS:           }
// JS:           switch (marker) {
// JS:             case "ArrayBuffer": return buffer;
// JS:             case "DataView": return new DataView(buffer);
// JS:             case "Int8Array": return new Int8Array(buffer);
// JS:             case "Uint8Array": return new Uint8Array(buffer);
// JS:             case "Uint8ClampedArray": return new Uint8ClampedArray(buffer);
// JS:             case "Int16Array": return new Int16Array(buffer);
// JS:             case "Uint16Array": return new Uint16Array(buffer);
// JS:             case "Int32Array": return new Int32Array(buffer);
// JS:             case "Uint32Array": return new Uint32Array(buffer);
// JS:             case "BigInt64Array": return new BigInt64Array(buffer);
// JS:             case "BigUint64Array": return new BigUint64Array(buffer);
// JS:             case "Float32Array": return new Float32Array(buffer);
// JS:             case "Float64Array": return new Float64Array(buffer);
// JS:             default: marker satisfies never;
// JS:           }
// JS:         }
// JS:         case "error":
// JS:           if (value.length >= 3 && typeof value[1] === "string" && typeof value[2] === "string") {
// JS:             let cls = ERROR_TYPES[value[1]] || Error;
// JS:             // AggregateError's constructor takes (errors, message); we pass an empty array
// JS:             // and patch `errors` from the props bag below.
// JS:             let result = cls === AggregateError ? new cls([], value[2]) : new cls(value[2]);
// JS:             if (typeof value[3] === "string") {
// JS:               result.stack = value[3];
// JS:             }
// JS:             // Optional 5th element: own properties bag. Unknown keys are assigned as own
// JS:             // enumerable properties so the receiver sees what the sender attached.
// JS:             if (value.length >= 5) {
// JS:               let props = value[4];
// JS:               if (!props || typeof props !== "object" || Array.isArray(props)) {
// JS:                 break;  // malformed; fall through to the "unknown special value" throw
// JS:               }
// JS:               let anyResult = <any>result;
// JS:               let propsObj = <Record<string, unknown>>props;
// JS:               for (let key of Object.keys(propsObj)) {
// JS:                 if (key === "name" || key === "message" || key === "stack") continue;
// JS:                 if (key in Object.prototype || key === "toJSON") {
// JS:                   // Consistent with the plain-object deserializer below: don't allow error
// JS:                   // properties to override Object.prototype members (e.g. __proto__, toString,
// JS:                   // valueOf) or toJSON. Still evaluate the inner value so any stubs are released.
// JS:                   this.evaluateImpl(propsObj[key], result, key, depth + 1);
// JS:                   continue;
// JS:                 }
// JS:                 anyResult[key] = this.evaluateImpl(propsObj[key], result, key, depth + 1);
// JS:               }
// JS:             }
// JS:             return result;
// JS:           }
// JS:           break;
// JS:         case "undefined":
// JS:           if (value.length === 1) {
// JS:             return undefined;
// JS:           }
// JS:           break;
// JS:         case "inf":
// JS:           return Infinity;
// JS:         case "-inf":
// JS:           return -Infinity;
// JS:         case "nan":
// JS:           return NaN;
// JS:
// JS:         case "url":
// JS:           if (value.length === 2 && typeof value[1] === "string") {
// JS:             return new URL(value[1]);
// JS:           }
// JS:           break;
// JS:
// JS:         case "headers":
// JS:           // We only need to validate that the parameter is an array, so as not to invoke an
// JS:           // unexpected variant of the Headers constructor. So long as it is an array then we can
// JS:           // rely on the constructor to perform type checking.
// JS:           if (value.length === 2 && value[1] instanceof Array) {
// JS:             return new Headers(value[1] as [string, string][]);
// JS:           }
// JS:           break;
// JS:
// JS:         case "request": {
// JS:           if (value.length !== 3 || typeof value[1] !== "string") break;
// JS:           let url = value[1] as string;
// JS:           let init = value[2];
// JS:           if (typeof init !== "object" || init === null) break;
// JS:
// JS:           // Evaluate specific properties which are expected to contain non-trivial types.
// JS:           if (init.body) {
// JS:             init.body = this.evaluateImpl(init.body, init, "body", depth + 1);
// JS:             if (init.body === null ||
// JS:                 typeof init.body === "string" ||
// JS:                 init.body instanceof Uint8Array ||
// JS:                 init.body instanceof ReadableStream) {
// JS:               // Acceptable types.
// JS:             } else {
// JS:               throw new TypeError("Request body must be of type ReadableStream.");
// JS:             }
// JS:           }
// JS:           if (init.signal) {
// JS:             init.signal = this.evaluateImpl(init.signal, init, "signal", depth + 1);
// JS:             if (!(init.signal instanceof AbortSignal)) {
// JS:               throw new TypeError("Request siganl must be of type AbortSignal.");
// JS:             }
// JS:           }
// JS:
// JS:           // Type-check `headers` is an array because the constructor allows multiple
// JS:           // representations and we don't want to allow the others.
// JS:           if (init.headers && !(init.headers instanceof Array)) {
// JS:             throw new TypeError("Request headers must be serialized as an array of pairs.");
// JS:           }
// JS:
// JS:           // We assume the `Request` constructor can type-check the remaining properties.
// JS:           let result = new Request(url, init as RequestInit);
// JS:
// JS:           if (init.body instanceof ReadableStream && result.body === undefined) {
// JS:             // Oh no! We must be on Firefox where request bodies are not supported, but we had a
// JS:             // body.
// JS:             let promise = fixBrokenRequestBody(result, init.body);
// JS:             this.promises.push({promise, parent, property});
// JS:             return promise;
// JS:           } else {
// JS:             return result;
// JS:           }
// JS:         }
// JS:
// JS:         case "response": {
// JS:           if (value.length !== 3) break;
// JS:
// JS:           let body = this.evaluateImpl(value[1], parent, property, depth + 1);
// JS:           if (body === null ||
// JS:               typeof body === "string" ||
// JS:               body instanceof Uint8Array ||
// JS:               body instanceof ReadableStream) {
// JS:             // Acceptable types.
// JS:           } else {
// JS:             throw new TypeError("Response body must be of type ReadableStream.");
// JS:           }
// JS:
// JS:           let init = value[2];
// JS:           if (typeof init !== "object" || init === null) break;
// JS:
// JS:           // Evaluate specific properties which are expected to contain non-trivial types.
// JS:           if (init.webSocket) {
// JS:             // `response.webSocket` is a Cloudflare Workers extension. Not (yet?) supported for
// JS:             // serialization.
// JS:             throw new TypeError("Can't deserialize a Response containing a webSocket.");
// JS:           }
// JS:
// JS:           // Type-check `headers` is an array because the constructor allows multiple
// JS:           // representations and we don't want to allow the others.
// JS:           if (init.headers && !(init.headers instanceof Array)) {
// JS:             throw new TypeError("Request headers must be serialized as an array of pairs.");
// JS:           }
// JS:
// JS:           return new Response(body as BodyInit | null, init as ResponseInit);
// JS:         }
// JS:
// JS:         case "blob": {
// JS:           // Wire format is strictly ["blob", type, ["readable", id]] — the encoder always streams
// JS:           // bytes through a pipe, so the content expression must evaluate to a ReadableStream.
// JS:           if (value.length !== 3 || typeof value[1] !== "string") break;
// JS:           let contentType = value[1] as string;
// JS:           let content = this.evaluateImpl(value[2], parent, property, depth + 1);
// JS:           if (!(content instanceof ReadableStream)) {
// JS:             throw new TypeError("Blob content must be serialized as a ReadableStream.");
// JS:           }
// JS:           // Reuse the RpcPromise infrastructure (same pattern as fixBrokenRequestBody): the
// JS:           // payload-delivery machinery resolves the promise and substitutes the real Blob before
// JS:           // user code sees the value.
// JS:           let promise = streamToBlobPromise(content, contentType);
// JS:           this.promises.push({promise, parent, property});
// JS:           return promise;
// JS:         }
// JS:
// JS:         case "import":
// JS:         case "pipeline": {
// JS:           // It's an "import" from the perspective of the sender, so it's an export from our
// JS:           // side. In other words, the sender is passing our own object back to us.
// JS:
// JS:           if (value.length < 2 || value.length > 4) {
// JS:             break;   // report error below
// JS:           }
// JS:
// JS:           // First parameter is import ID (from the sender's perspective, so export ID from
// JS:           // ours).
// JS:           if (typeof value[1] != "number") {
// JS:             break;   // report error below
// JS:           }
// JS:
// JS:           let hook = this.importer.getExport(value[1]);
// JS:           if (!hook) {
// JS:             throw new Error(`no such entry on exports table: ${value[1]}`);
// JS:           }
// JS:
// JS:           let isPromise = value[0] == "pipeline";
// JS:
// JS:           let addStub = (hook: StubHook) => {
// JS:             if (isPromise) {
// JS:               let promise = new RpcPromise(hook, []);
// JS:               this.promises.push({promise, parent, property});
// JS:               return promise;
// JS:             } else {
// JS:               this.hooks.push(hook);
// JS:               return new RpcPromise(hook, []);
// JS:             }
// JS:           };
// JS:
// JS:           if (value.length == 2) {
// JS:             // Just referencing the export itself.
// JS:             if (isPromise) {
// JS:               // We need to use hook.get([]) to make sure we get a promise hook.
// JS:               return addStub(hook.get([]));
// JS:             } else {
// JS:               // dup() returns a stub hook.
// JS:               return addStub(hook.dup());
// JS:             }
// JS:           }
// JS:
// JS:           // Second parameter, if given, is a property path.
// JS:           let path = value[2];
// JS:           if (!(path instanceof Array)) {
// JS:             break;  // report error below
// JS:           }
// JS:           if (!path.every(
// JS:               part => { return typeof part == "string" || typeof part == "number"; })) {
// JS:             break;  // report error below
// JS:           }
// JS:
// JS:           if (value.length == 3) {
// JS:             // Just referencing the path, not a call.
// JS:             return addStub(hook.get(path));
// JS:           }
// JS:
// JS:           // Third parameter, if given, is call arguments. The sender has identified a function
// JS:           // and wants us to call it.
// JS:           //
// JS:           // Usually this is used with "pipeline", in which case we evaluate to an
// JS:           // RpcPromise. However, this can be used with "import", in which case the caller is
// JS:           // asking that the result be coerced to RpcStub. This distinction matters if the
// JS:           // result of this evaluation is to be passed as arguments to another call -- promises
// JS:           // must be resolved in advance, but stubs can be passed immediately.
// JS:           let args = value[3];
// JS:           if (!(args instanceof Array)) {
// JS:             break;  // report error below
// JS:           }
// JS:
// JS:           // We need a new evaluator for the args, to build a separate payload.
// JS:           let subEval = new Evaluator(this.importer);
// JS:           args = subEval.evaluateWithDepth([args], depth);
// JS:
// JS:           return addStub(hook.call(path, args));
// JS:         }
// JS:
// JS:         case "remap": {
// JS:           if (value.length !== 5 ||
// JS:               typeof value[1] !== "number" ||
// JS:               !(value[2] instanceof Array) ||
// JS:               !(value[3] instanceof Array) ||
// JS:               !(value[4] instanceof Array)) {
// JS:             break;   // report error below
// JS:           }
// JS:
// JS:           let hook = this.importer.getExport(value[1]);
// JS:           if (!hook) {
// JS:             throw new Error(`no such entry on exports table: ${value[1]}`);
// JS:           }
// JS:
// JS:           let path = value[2];
// JS:           if (!path.every(
// JS:               part => { return typeof part == "string" || typeof part == "number"; })) {
// JS:             break;  // report error below
// JS:           }
// JS:
// JS:           let captures: StubHook[] = value[3].map(cap => {
// JS:             if (!(cap instanceof Array) ||
// JS:                 cap.length !== 2 ||
// JS:                 (cap[0] !== "import" && cap[0] !== "export") ||
// JS:                 typeof cap[1] !== "number") {
// JS:               throw new TypeError(`unknown map capture: ${JSON.stringify(cap)}`);
// JS:             }
// JS:
// JS:             if (cap[0] === "export") {
// JS:               return this.importer.importStub(cap[1]);
// JS:             } else {
// JS:               let exp = this.importer.getExport(cap[1]);
// JS:               if (!exp) {
// JS:                 throw new Error(`no such entry on exports table: ${cap[1]}`);
// JS:               }
// JS:               return exp.dup();
// JS:             }
// JS:           });
// JS:
// JS:           let instructions = value[4];
// JS:
// JS:           let resultHook = hook.map(path, captures, instructions);
// JS:
// JS:           let promise = new RpcPromise(resultHook, []);
// JS:           this.promises.push({promise, parent, property});
// JS:           return promise;
// JS:         }
// JS:
// JS:         case "export":
// JS:         case "promise":
// JS:           // It's an "export" from the perspective of the sender, i.e. they sent us a new object
// JS:           // which we want to import.
// JS:           //
// JS:           // "promise" is same as "export" but should not be delivered to the application. If any
// JS:           // promises appear in a value, they must be resolved and substituted with their results
// JS:           // before delivery. Note that if the value being evaluated appeared in call params, or
// JS:           // appeared in a resolve message for a promise that is being pulled, then the new promise
// JS:           // is automatically also being pulled, otherwise it is not.
// JS:           if (typeof value[1] == "number") {
// JS:             if (value[0] == "promise") {
// JS:               let hook = this.importer.importPromise(value[1]);
// JS:               let promise = new RpcPromise(hook, []);
// JS:               this.promises.push({parent, property, promise});
// JS:               return promise;
// JS:             } else {
// JS:               let hook = this.importer.importStub(value[1]);
// JS:               this.hooks.push(hook);
// JS:               return new RpcStub(hook);
// JS:             }
// JS:           }
// JS:           break;
// JS:
// JS:         case "writable":
// JS:           // It's a WritableStream export from the sender. We import it and create a proxy
// JS:           // WritableStream that forwards writes to the remote end.
// JS:           if (typeof value[1] == "number") {
// JS:             let hook = this.importer.importStub(value[1]);
// JS:             let stream = streamImpl.createWritableStreamFromHook(hook);
// JS:             // Track the stream for disposal.
// JS:             this.hooks.push(hook);
// JS:             return stream;
// JS:           }
// JS:           break;
// JS:
// JS:         case "readable":
// JS:           // References the readable end of a pipe. The import ID (from the sender's perspective)
// JS:           // is our export ID.
// JS:           if (typeof value[1] == "number") {
// JS:             let stream = this.importer.getPipeReadable(value[1]);
// JS:             // Track the stream for disposal so that if the payload is disposed before the
// JS:             // app reads the stream, the ReadableStream is properly canceled.
// JS:             let hook = streamImpl.createReadableStreamHook(stream);
// JS:             this.hooks.push(hook);
// JS:             return stream;
// JS:           }
// JS:           break;
// JS:       }
// JS:       throw new TypeError(`unknown special value: ${JSON.stringify(value)}`);
// JS:     } else if (value instanceof Object) {
// JS:       let result = <Record<string, unknown>>value;
// JS:       for (let key in result) {
// JS:         if (key in Object.prototype || key === "toJSON") {
// JS:           // Out of an abundance of caution, we will ignore properties that override properties
// JS:           // of Object.prototype. It's especially important that we don't allow `__proto__` as it
// JS:           // may lead to prototype pollution. We also would rather not allow, e.g., `toString()`,
// JS:           // as overriding this could lead to various mischief.
// JS:           //
// JS:           // We also block `toJSON()` for similar reasons -- even though Object.prototype doesn't
// JS:           // actually define it, `JSON.stringify()` treats it specially and we don't want someone
// JS:           // snooping on JSON calls.
// JS:           //
// JS:           // We do still evaluate the inner value so that we can properly release any stubs.
// JS:           this.evaluateImpl(result[key], result, key, depth + 1);
// JS:           delete result[key];
// JS:         } else {
// JS:           result[key] = this.evaluateImpl(result[key], result, key, depth + 1);
// JS:         }
// JS:       }
// JS:       return result;
// JS:     } else {
// JS:       // Other JSON types just pass through.
// JS:       return value;
// JS:     }
// JS:   }
// JS: }
// JS:
// JS: /**
// JS:  * Deserialize a value serialized using serialize().
// JS:  */
// JS: export function deserialize(value: string): unknown {
// JS:   let payload = new Evaluator(NULL_IMPORTER).evaluate(JSON.parse(value));
// JS:   payload.dispose();  // should be no-op but just in case
// JS:   return payload.value;
// JS: }
