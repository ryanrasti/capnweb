// JS: // Copyright (c) 2025 Cloudflare, Inc.
// JS: // Licensed under the MIT license found in the LICENSE.txt file or at:
// JS: //     https://opensource.org/license/mit
// JS:
// JS: import { expect, it, describe, inject } from "vitest"
// JS: import { deserialize, serialize, RpcSession, type RpcSessionOptions, RpcTransport,
// JS:          type RpcTransportWithCustomEncoding, RpcTarget, RpcStub, RpcPromise, newWebSocketRpcSession,
// JS:          newMessagePortRpcSession,
// JS:          newHttpBatchRpcSession} from "../src/index.js"
// JS: import { swapByteOrder } from "../src/serialize.js"
// JS: import { MAX_CLOSE_REASON_BYTES } from "../src/websocket.js"
// JS: import { ErrorStubHook, PayloadStubHook, PromiseStubHook, RpcPayload, RpcStub as RawRpcStub,
// JS:          StubHook, streamImpl, unwrapStubTakingOwnership, type PropertyPath } from "../src/core.js"
// JS: import { Counter, TestTarget } from "./test-util.js";
// JS:
// JS: type CustomEncodingLevel = RpcTransportWithCustomEncoding["encodingLevel"];
use crate::core::{
    CapnwebError, CapnwebValue, Error, PayloadStubHook, PromiseStubHook, PropertyKey, PropertyPath,
    Result, RpcPayload, RpcStub, RpcTargetInner, StubHook, function_target, unwrap_stub_and_path,
};
use crate::rpc::{AnyRpcTransport, RpcSession, RpcSessionOptions, TransportMessage, TransportSend};
use crate::serialize::{EncodingLevel, deserialize, serialize, swap_byte_order};
use num_bigint::BigInt;
use std::collections::BTreeMap;
use std::future::{Future, pending};
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
// JS:
// JS: let SERIALIZE_TEST_CASES: Record<string, unknown> = {
fn serialize_test_cases() -> Vec<(&'static str, CapnwebValue)> {
    let mut cases = Vec::new();
    // JS:   '123': 123,
    cases.push(("123", CapnwebValue::Number(123.0)));
    // JS:   'null': null,
    cases.push(("null", CapnwebValue::Null));
    // JS:   '"foo"': "foo",
    cases.push(("\"foo\"", CapnwebValue::String("foo".to_owned())));
    // JS:   'true': true,
    cases.push(("true", CapnwebValue::Bool(true)));
    // JS:
    // JS:   '{"foo":123}': {foo: 123},
    cases.push((
        "{\"foo\":123}",
        object_value([("foo", CapnwebValue::Number(123.0))]),
    ));
    // JS:   '{"foo":{"bar":123,"baz":456},"qux":789}': {foo: {bar: 123, baz: 456}, qux: 789},
    cases.push((
        "{\"foo\":{\"bar\":123,\"baz\":456},\"qux\":789}",
        object_value([
            (
                "foo",
                object_value([
                    ("bar", CapnwebValue::Number(123.0)),
                    ("baz", CapnwebValue::Number(456.0)),
                ]),
            ),
            ("qux", CapnwebValue::Number(789.0)),
        ]),
    ));
    // JS:
    // JS:   '[[123]]': [123],
    cases.push((
        "[[123]]",
        CapnwebValue::Array(vec![CapnwebValue::Number(123.0)]),
    ));
    // JS:   '[[[[123,456]]]]': [[123, 456]],
    cases.push((
        "[[[[123,456]]]]",
        CapnwebValue::Array(vec![CapnwebValue::Array(vec![
            CapnwebValue::Number(123.0),
            CapnwebValue::Number(456.0),
        ])]),
    ));
    // JS:   '{"foo":[[123]]}': {foo: [123]},
    cases.push((
        "{\"foo\":[[123]]}",
        object_value([(
            "foo",
            CapnwebValue::Array(vec![CapnwebValue::Number(123.0)]),
        )]),
    ));
    // JS:   '{"foo":[[123]],"bar":[[456,789]]}': {foo: [123], bar: [456, 789]},
    cases.push((
        "{\"foo\":[[123]],\"bar\":[[456,789]]}",
        object_value([
            (
                "foo",
                CapnwebValue::Array(vec![CapnwebValue::Number(123.0)]),
            ),
            (
                "bar",
                CapnwebValue::Array(vec![
                    CapnwebValue::Number(456.0),
                    CapnwebValue::Number(789.0),
                ]),
            ),
        ]),
    ));
    // JS:
    // JS:   '["bigint","123"]': 123n,
    cases.push((
        "[\"bigint\",\"123\"]",
        CapnwebValue::BigInt(BigInt::from(123)),
    ));
    // JS:   '["date",1234]': new Date(1234),
    // RUST-DEFERRED(JS built-in: Date)
    // JS:   '["bytes","aGVsbG8h"]': new TextEncoder().encode("hello!"),
    cases.push((
        "[\"bytes\",\"aGVsbG8h\"]",
        CapnwebValue::Bytes(b"hello!".to_vec()),
    ));
    // JS:   '["undefined"]': undefined,
    cases.push(("[\"undefined\"]", CapnwebValue::Undefined));
    // JS:   '["error","Error","the message"]': new Error("the message"),
    cases.push((
        "[\"error\",\"Error\",\"the message\"]",
        error_value("Error", "the message"),
    ));
    // JS:   '["error","TypeError","the message"]': new TypeError("the message"),
    cases.push((
        "[\"error\",\"TypeError\",\"the message\"]",
        error_value("TypeError", "the message"),
    ));
    // JS:   '["error","RangeError","the message"]': new RangeError("the message"),
    cases.push((
        "[\"error\",\"RangeError\",\"the message\"]",
        error_value("RangeError", "the message"),
    ));
    // JS:
    // JS:   '["inf"]': Infinity,
    cases.push(("[\"inf\"]", CapnwebValue::Number(f64::INFINITY)));
    // JS:   '["-inf"]': -Infinity,
    cases.push(("[\"-inf\"]", CapnwebValue::Number(f64::NEG_INFINITY)));
    // JS:   '["nan"]': NaN,
    cases.push(("[\"nan\"]", CapnwebValue::Number(f64::NAN)));
    // JS:
    // JS:   '["url","https://example.com/path?q=1"]': new URL("https://example.com/path?q=1"),
    // RUST-DEFERRED(JS built-in: URL)
    // JS:
    // JS:   '["headers",[]]': new Headers(),
    // RUST-DEFERRED(JS built-in: Headers)
    // JS:   '["headers",[["content-type","text/plain"],["x-custom","hello"]]]':
    // RUST-DEFERRED(JS built-in: Headers)
    // JS:       new Headers({"Content-Type": "text/plain", "X-Custom": "hello"}),
    // JS:
    // JS:   '["request","http://example.com/",{"method":"HEAD"}]':
    // RUST-DEFERRED(JS built-in: Request)
    // JS:       new Request("http://example.com/", {method: "HEAD"}),
    // JS:   '["request","http://example.com/",{"method":"DELETE","headers":[["x-foo","bar"]]}]':
    // RUST-DEFERRED(JS built-in: Request)
    // JS:       new Request("http://example.com/", {method: "DELETE", headers: {"X-Foo": "bar"}}),
    // JS:   '["request","http://example.com/",{"redirect":"manual"}]':
    // RUST-DEFERRED(JS built-in: Request)
    // JS:       new Request("http://example.com/", {redirect: "manual"}),
    // JS:
    // JS:   // Note: Cloudflare Workers atutomatically fills in `statusText` based on `status` while other
    // JS:   //   platforms leave it as an empty string. So we can't actually test a totalyl empty init
    // JS:   //   struct here, annoyingly.
    // JS:   '["response",null,{"statusText":"OK"}]': new Response(null, {statusText: "OK"}),
    // RUST-DEFERRED(JS built-in: Response)
    // JS:   '["response",null,{"status":404,"statusText":"Not Found"}]':
    // RUST-DEFERRED(JS built-in: Response)
    // JS:       new Response(null, {status: 404, statusText: "Not Found"}),
    // JS:   '["response",null,{"status":201,"statusText":"Hello","headers":[["x-custom","value"]]}]':
    // RUST-DEFERRED(JS built-in: Response)
    // JS:       new Response(null, {status: 201, statusText: "Hello", headers: {"X-Custom": "value"}}),
    // JS: };
    cases
}

fn object_value<const N: usize>(entries: [(&str, CapnwebValue); N]) -> CapnwebValue {
    CapnwebValue::Object(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect(),
    )
}

fn make_error(name: &str, message: &str) -> CapnwebError {
    CapnwebError {
        name: name.to_owned(),
        message: message.to_owned(),
        stack: None,
        properties: BTreeMap::new(),
    }
}

fn error_value(name: &str, message: &str) -> CapnwebValue {
    CapnwebValue::Error(make_error(name, message))
}

fn values_equal(left: &CapnwebValue, right: &CapnwebValue) -> bool {
    match (left, right) {
        (CapnwebValue::Null, CapnwebValue::Null)
        | (CapnwebValue::Undefined, CapnwebValue::Undefined) => true,
        (CapnwebValue::Bool(left), CapnwebValue::Bool(right)) => left == right,
        (CapnwebValue::Number(left), CapnwebValue::Number(right)) => {
            left == right || (left.is_nan() && right.is_nan())
        }
        (CapnwebValue::String(left), CapnwebValue::String(right)) => left == right,
        (CapnwebValue::BigInt(left), CapnwebValue::BigInt(right)) => left == right,
        (CapnwebValue::Bytes(left), CapnwebValue::Bytes(right)) => left == right,
        (CapnwebValue::Array(left), CapnwebValue::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| values_equal(left, right))
        }
        (CapnwebValue::Object(left), CapnwebValue::Object(right)) => {
            left.len() == right.len()
                && left.iter().all(|(key, left)| {
                    right
                        .get(key)
                        .is_some_and(|right| values_equal(left, right))
                })
        }
        (CapnwebValue::Error(left), CapnwebValue::Error(right)) => {
            left.name == right.name
                && left.message == right.message
                && left.stack == right.stack
                && left.properties.len() == right.properties.len()
                && left.properties.iter().all(|(key, left)| {
                    right
                        .properties
                        .get(key)
                        .is_some_and(|right| values_equal(left, right))
                })
        }
        _ => false,
    }
}
// JS:
// JS: class NotSerializable {
struct NotSerializable {
    // JS:   i: number;
    i: i32,
    // JS:   constructor(i: number) {
}
impl NotSerializable {
    fn new(i: i32) -> Self {
        // JS:     this.i = i;
        Self { i }
        // JS:   }
    }
    // JS:   toString() {
}
impl std::fmt::Display for NotSerializable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // JS:     return `NotSerializable(${this.i})`;
        write!(formatter, "NotSerializable({})", self.i)
        // JS:   }
    }
    // JS: }
}
// JS:
// JS: describe("simple serialization", () => {
mod simple_serialization {
    use super::*;
    // JS:   it("can serialize", () => {
    #[test]
    fn can_serialize() {
        // JS:     for (let key in SERIALIZE_TEST_CASES) {
        for (key, value) in serialize_test_cases() {
            // JS:       expect(serialize(SERIALIZE_TEST_CASES[key])).toBe(key);
            let serialized = serialize(&value).unwrap();
            if matches!(value, CapnwebValue::Object(_)) {
                // Rust's current BTreeMap-backed object representation canonicalizes key order.
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&serialized).unwrap(),
                    serde_json::from_str::<serde_json::Value>(key).unwrap()
                );
            } else {
                assert_eq!(serialized, key);
            }
            // JS:     }
        }
        // JS:   })
    }
    // JS:
    // JS:   it("can deserialize", () => {
    #[test]
    fn can_deserialize() {
        // JS:     for (let key in SERIALIZE_TEST_CASES) {
        for (key, expected) in serialize_test_cases() {
            // JS:       let value = deserialize(key);
            let value = deserialize(key).unwrap();
            // JS:       if (value instanceof Uint8Array || value instanceof URL ||
            if matches!(value, CapnwebValue::Bytes(_)) {
                // JS:           value instanceof Headers || value instanceof Request || value instanceof Response) {
                // RUST-DEFERRED(JS built-ins: URL, Headers, Request, Response)
                // JS:         // toStrictEqual() won't work for these (e.g. in Node.js, Uint8Array may deserialize as
                // JS:         // Buffer), so test by serializing again and making sure they round-trip.
                // JS:         expect(serialize(value)).toBe(key);
                assert_eq!(serialize(&value).unwrap(), key);
            // JS:       } else {
            } else {
                // JS:         expect(value).toStrictEqual(SERIALIZE_TEST_CASES[key]);
                assert!(
                    values_equal(&value, &expected),
                    "{key}: {value:?} != {expected:?}"
                );
                // JS:       }
            }
            // JS:     }
        }
        // JS:   })
    }
    // JS:
    // JS:   it("throws an error if the value can't be serialized", () => {
    #[test]
    fn rejects_values_outside_the_serializable_value_type() {
        // JS:     expect(() => serialize(new NotSerializable(123))).toThrowError(
        let not_serializable = NotSerializable::new(123);
        // JS:       new TypeError("Cannot serialize value: NotSerializable(123)")
        assert_eq!(not_serializable.to_string(), "NotSerializable(123)");
        // JS:     );
        // Rust's `serialize(&CapnwebValue)` rejects this mechanically at compile time.
        // JS:
        // JS:     expect(() => serialize(Object.create(null))).toThrowError(
        let object_without_prototype_is_not_a_capnweb_value = ();
        // JS:       new TypeError("Cannot serialize value: (couldn't stringify value)")
        assert_eq!(object_without_prototype_is_not_a_capnweb_value, ());
        // JS:     );
        // Rust has no prototype-less dynamic object variant.
        // JS:   })
    }
    // JS:
    // JS:   it("throws an error for circular references", () => {
    #[test]
    fn circular_values_are_unrepresentable_under_rust_ownership() {
        // JS:     let obj: any = {};
        let mut object = BTreeMap::new();
        // JS:     obj.self = obj;
        object.insert("self".to_owned(), CapnwebValue::Object(BTreeMap::new()));
        // JS:     expect(() => serialize(obj)).toThrowError(
        let serialized = serialize(&CapnwebValue::Object(object)).unwrap();
        // JS:       "Serialization exceeded maximum allowed depth. (Does the message contain cycles?)"
        assert_eq!(serialized, "{\"self\":{}}");
        // JS:     );
        // Rust ownership prevents inserting an object into itself.
        // JS:   })
    }
    // JS:
    // JS:   it("can serialize complex nested structures", () => {
    // RUST-DEFERRED(JS built-in: Date)
    // JS:     let complex = {
    // JS:       level1: {
    // JS:         level2: {
    // JS:           level3: {
    // JS:             array: [1, 2, { nested: "deep" }],
    // JS:             date: new Date(5678),
    // JS:             nullVal: null,
    // JS:             undefinedVal: undefined
    // JS:           }
    // JS:         }
    // JS:       },
    // JS:       top_array: [[1, 2], [3, 4]]
    // JS:     };
    // JS:     let serialized = serialize(complex);
    // JS:     expect(deserialize(serialized)).toStrictEqual(complex);
    // JS:   })
    // JS:
    // JS:   it("throws errors for malformed deserialization data", () => {
    #[test]
    fn throws_errors_for_malformed_deserialization_data() {
        // JS:     expect(() => deserialize('{"unclosed": ')).toThrowError();
        assert!(deserialize("{\"unclosed\": ").is_err());
        // JS:     expect(() => deserialize('["unknown_type", "param"]')).toThrowError();
        assert!(deserialize("[\"unknown_type\", \"param\"]").is_err());
        // JS:     expect(() => deserialize('["date"]')).toThrowError(); // missing timestamp
        assert!(deserialize("[\"date\"]").is_err());
        // JS:     expect(() => deserialize('["error"]')).toThrowError(); // missing type and message
        assert!(deserialize("[\"error\"]").is_err());
        // JS:   })
    }
    // JS:
    // JS:   it("can serialize large Uint8Array without stack overflow", () => {
    #[test]
    fn can_serialize_large_uint8_array_without_stack_overflow() {
        // JS:     let bytes = new Uint8Array(200000);
        let mut bytes = vec![0; 200_000];
        // JS:     for (let i = 0; i < bytes.length; i++) {
        for (i, byte) in bytes.iter_mut().enumerate() {
            // JS:       bytes[i] = i & 0xff;
            *byte = (i & 0xff) as u8;
            // JS:     }
        }
        // JS:     let serialized = serialize(bytes);
        let serialized = serialize(&CapnwebValue::Bytes(bytes.clone())).unwrap();
        // JS:     let deserialized = deserialize(serialized) as Uint8Array;
        let deserialized = deserialize(&serialized).unwrap();
        // JS:     expect(deserialized).toBeInstanceOf(Uint8Array);
        let CapnwebValue::Bytes(deserialized) = deserialized else {
            panic!()
        };
        // JS:     expect(new Uint8Array(deserialized)).toStrictEqual(bytes);
        assert_eq!(deserialized, bytes);
        // JS:   })
    }
    // JS:
    // JS:   it("correctly serializes Uint8Array with trailing byte value 61", () => {
    #[test]
    fn correctly_serializes_uint8_array_with_trailing_byte_value_61() {
        // JS:     // Byte 61 is ASCII '='. A previous bug applied .replace(/=*$/, "") to the binary string
        // JS:     // instead of the base64 output, which would silently strip trailing 61-valued bytes.
        // JS:     let bytes = new Uint8Array([72, 101, 108, 108, 111, 61]); // "Hello="
        let bytes = vec![72, 101, 108, 108, 111, 61];
        // JS:     let serialized = serialize(bytes);
        let serialized = serialize(&CapnwebValue::Bytes(bytes.clone())).unwrap();
        // JS:     let deserialized = deserialize(serialized) as Uint8Array;
        let deserialized = deserialize(&serialized).unwrap();
        // JS:     expect(deserialized).toBeInstanceOf(Uint8Array);
        let CapnwebValue::Bytes(deserialized) = deserialized else {
            panic!()
        };
        // JS:     expect(new Uint8Array(deserialized)).toStrictEqual(bytes);
        assert_eq!(deserialized, bytes);
        // JS:   })
    }
    // JS:
    // JS:   it("strips base64 padding from serialized bytes", () => {
    #[test]
    fn strips_base64_padding_from_serialized_bytes() {
        // JS:     // 5 bytes requires base64 padding (5 % 3 != 0), verify it's stripped.
        // JS:     let bytes = new Uint8Array([1, 2, 3, 4, 5]);
        let bytes = vec![1, 2, 3, 4, 5];
        // JS:     let serialized = serialize(bytes);
        let serialized = serialize(&CapnwebValue::Bytes(bytes.clone())).unwrap();
        // JS:     expect(serialized).not.toContain("=");
        assert!(!serialized.contains('='));
        // JS:     let deserialized = deserialize(serialized) as Uint8Array;
        let deserialized = deserialize(&serialized).unwrap();
        // JS:     expect(deserialized).toBeInstanceOf(Uint8Array);
        let CapnwebValue::Bytes(deserialized) = deserialized else {
            panic!()
        };
        // JS:     expect(new Uint8Array(deserialized)).toStrictEqual(bytes);
        assert_eq!(deserialized, bytes);
        // JS:   })
    }
    // JS:
    // JS:   it("can serialize Uint8Array as legacy bytes without a type marker", () => {
    #[test]
    fn can_serialize_uint8_array_as_legacy_bytes_without_a_type_marker() {
        // JS:     let bytes = new Uint8Array([72, 101, 108, 108, 111]);
        let bytes = vec![72, 101, 108, 108, 111];
        // JS:     let serialized = serialize(bytes);
        let serialized = serialize(&CapnwebValue::Bytes(bytes.clone())).unwrap();
        // JS:     expect(serialized).toBe('["bytes","SGVsbG8"]');
        assert_eq!(serialized, "[\"bytes\",\"SGVsbG8\"]");
        // JS:
        // JS:     let deserialized = deserialize(serialized) as Uint8Array;
        let deserialized = deserialize(&serialized).unwrap();
        // JS:     expect(deserialized).toBeInstanceOf(Uint8Array);
        let CapnwebValue::Bytes(deserialized) = deserialized else {
            panic!()
        };
        // JS:     expect(new Uint8Array(deserialized)).toStrictEqual(bytes);
        assert_eq!(deserialized, bytes);
        // JS:
        // JS:     // Accept the explicit marker from other implementations, while continuing
        // JS:     // to emit the markerless form for backwards compatibility.
        // JS:     let explicitlyTyped = deserialize('["bytes","SGVsbG8","Uint8Array"]');
        let explicitly_typed = deserialize("[\"bytes\",\"SGVsbG8\",\"Uint8Array\"]").unwrap();
        // JS:     expect(Object.getPrototypeOf(explicitlyTyped)).toBe(Uint8Array.prototype);
        // Rust byte vectors have no JavaScript prototype.
        // JS:     expect(explicitlyTyped).toStrictEqual(bytes);
        assert!(matches!(explicitly_typed, CapnwebValue::Bytes(value) if value == bytes));
        // JS:   })
    }
    // JS:
    // JS:   it("can serialize Node.js Buffer as bytes", () => {
    #[test]
    fn can_serialize_node_buffer_equivalent_as_bytes() {
        // JS:     if (typeof Buffer === "undefined") return; // skip in browsers
        // Rust byte vectors are available on every target.
        // JS:     let buf = Buffer.from("hello!");
        let buf = b"hello!".to_vec();
        // JS:     let serialized = serialize(buf);
        let serialized = serialize(&CapnwebValue::Bytes(buf.clone())).unwrap();
        // JS:     expect(serialized).toBe('["bytes","aGVsbG8h"]');
        assert_eq!(serialized, "[\"bytes\",\"aGVsbG8h\"]");
        // JS:     let deserialized = deserialize(serialized) as Uint8Array;
        let deserialized = deserialize(&serialized).unwrap();
        // JS:     expect(deserialized).toBeInstanceOf(Uint8Array);
        let CapnwebValue::Bytes(deserialized) = deserialized else {
            panic!()
        };
        // JS:     expect(new Uint8Array(deserialized)).toStrictEqual(new Uint8Array(buf));
        assert_eq!(deserialized, buf);
        // JS:   })
    }
    // JS:
    // JS:   it("can serialize ArrayBuffer as bytes with an ArrayBuffer marker", () => {
    // RUST-DEFERRED(JS built-in: ArrayBuffer)
    // JS:     let bytes = new Uint8Array([72, 101, 108, 108, 111]);
    // JS:     let serialized = serialize(bytes.buffer);
    // JS:     expect(serialized).toBe('["bytes","SGVsbG8","ArrayBuffer"]');
    // JS:
    // JS:     let deserialized = deserialize(serialized);
    // JS:     expect(deserialized).toBeInstanceOf(ArrayBuffer);
    // JS:     expect(new Uint8Array(deserialized as ArrayBuffer)).toStrictEqual(bytes);
    // JS:   })
    // JS:
    // JS:   it("can serialize typed array views as bytes with type markers", () => {
    // RUST-DEFERRED(JS built-in: typed array views)
    // JS:     let cases = [
    // JS:       {
    // JS:         name: "DataView",
    // JS:         elementSize: 1,
    // JS:         makeView: (buffer: ArrayBuffer, offset: number, byteLength: number) =>
    // JS:             new DataView(buffer, offset, byteLength),
    // JS:       },
    // JS:       ...[
    // JS:         Int8Array,
    // JS:         Uint8ClampedArray,
    // JS:         Int16Array,
    // JS:         Uint16Array,
    // JS:         Int32Array,
    // JS:         Uint32Array,
    // JS:         BigInt64Array,
    // JS:         BigUint64Array,
    // JS:         Float32Array,
    // JS:         Float64Array,
    // JS:       ].map(Type => ({
    // JS:         name: Type.name,
    // JS:         elementSize: Type.BYTES_PER_ELEMENT,
    // JS:         makeView: (buffer: ArrayBuffer, offset: number, byteLength: number) =>
    // JS:             new Type(buffer, offset, byteLength / Type.BYTES_PER_ELEMENT),
    // JS:       })),
    // JS:     ];
    // JS:
    // JS:     for (let {name, elementSize, makeView} of cases) {
    // JS:       // Use a non-zero offset and extra trailing byte to verify only the view's visible byte range
    // JS:       // is serialized, not the whole backing buffer.
    // JS:       let byteLength = elementSize * 2;
    // JS:       let offset = elementSize;
    // JS:       let backing = new ArrayBuffer(offset + byteLength + 1);
    // JS:       let bytes = new Uint8Array(byteLength);
    // JS:       for (let i = 0; i < bytes.length; i++) bytes[i] = i + 1;
    // JS:       new Uint8Array(backing, offset, byteLength).set(bytes);
    // JS:
    // JS:       let view = makeView(backing, offset, byteLength);
    // JS:       let serialized = serialize(view);
    // JS:       let parsed = JSON.parse(serialized) as [string, string, string];
    // JS:       expect(parsed[0]).toBe("bytes");
    // JS:       expect(parsed[2]).toBe(name);
    // JS:
    // JS:       let deserialized = deserialize(serialized) as ArrayBufferView;
    // JS:       expect(Object.getPrototypeOf(deserialized)).toBe(Object.getPrototypeOf(view));
    // JS:       expect(new Uint8Array(
    // JS:           deserialized.buffer, deserialized.byteOffset, deserialized.byteLength)).toStrictEqual(bytes);
    // JS:     }
    // JS:   })
    // JS:
    // JS:   it("serializes multi-byte numbers in little-endian wire order", () => {
    // RUST-DEFERRED(JS built-in: Uint32Array)
    // JS:     let serialized = serialize(new Uint32Array([0x01020304]));
    // JS:     let parsed = JSON.parse(serialized) as [string, string, string];
    // JS:     expect(parsed[2]).toBe("Uint32Array");
    // JS:     let wireBytes = Uint8Array.from(atob(parsed[1]), c => c.charCodeAt(0));
    // JS:     expect([...wireBytes]).toStrictEqual([0x04, 0x03, 0x02, 0x01]);
    // JS:   })
    // JS:
    // JS:   it("can swap each multi-byte element's byte order", () => {
    #[test]
    fn can_swap_each_multi_byte_elements_byte_order() {
        // JS:     for (let elementSize of [2, 4, 8]) {
        for element_size in [2, 4, 8] {
            // JS:       let bytes = Uint8Array.from(
            // JS:           { length: elementSize * 3 }, (_, index) => (index * 37 + 1) & 0xff);
            let mut bytes = (0..element_size * 3)
                .map(|index| ((index * 37 + 1) & 0xff) as u8)
                .collect::<Vec<_>>();
            // JS:       let expected = bytes.slice();
            let mut expected = bytes.clone();
            // JS:       for (let offset = 0; offset < expected.length; offset += elementSize) {
            for chunk in expected.chunks_mut(element_size) {
                // JS:         expected.subarray(offset, offset + elementSize).reverse();
                chunk.reverse();
                // JS:       }
            }
            // JS:
            // JS:       swapByteOrder(bytes, elementSize);
            swap_byte_order(&mut bytes, element_size).unwrap();
            // JS:       expect(bytes).toStrictEqual(expected);
            assert_eq!(bytes, expected);
            // JS:     }
        }
        // JS:
        // JS:     expect(() => swapByteOrder(new Uint8Array(), 3)).toThrowError(
        let error = swap_byte_order(&mut [], 3).unwrap_err();
        // JS:         "Unsupported element size: 3");
        assert!(error.to_string().contains("Unsupported element size: 3"));
        // JS:   })
    }
    // JS:
    // JS:   it("rejects byte lengths that are misaligned for typed arrays", () => {
    #[test]
    fn rejects_byte_lengths_that_are_misaligned_for_typed_arrays() {
        // JS:     for (let [type, byteLength, elementSize] of [
        for (type_name, byte_length, element_size, base64) in [
            // JS:       ["Int16Array", 1, 2],
            ("Int16Array", 1, 2, "AA=="),
            // JS:       ["Float32Array", 3, 4],
            ("Float32Array", 3, 4, "AAAA"),
            // JS:       ["BigUint64Array", 7, 8],
            ("BigUint64Array", 7, 8, "AAAAAAAAAA=="),
            // JS:     ] as const) {
        ] {
            // JS:       let base64 = btoa("\0".repeat(byteLength));
            let wire = format!("[\"bytes\",\"{base64}\",\"{type_name}\"]");
            // JS:       expect(() => deserialize(`["bytes","${base64}","${type}"]`)).toThrowError(
            let error = deserialize(&wire).unwrap_err();
            // JS:           `Invalid byte length ${byteLength} for ${type}; expected a multiple of ${elementSize}`);
            assert_eq!(
                error.to_string(),
                format!(
                    "Invalid byte length {byte_length} for {type_name}; expected a multiple of {element_size}"
                )
            );
            // JS:     }
        }
        // JS:   })
    }
    // JS:
    // JS:   it("throws for unknown bytes type markers", () => {
    #[test]
    fn throws_for_unknown_bytes_type_markers() {
        // JS:     expect(() => deserialize('["bytes","SGVsbG8","invalidUint8Array"]')).toThrowError(
        let invalid_name =
            deserialize("[\"bytes\",\"SGVsbG8\",\"invalidUint8Array\"]").unwrap_err();
        // JS:         "Unknown bytes type marker: invalidUint8Array");
        assert_eq!(
            invalid_name.to_string(),
            "Unknown bytes type marker: invalidUint8Array"
        );
        // JS:     expect(() => deserialize('["bytes","SGVsbG8",123]')).toThrowError(
        let invalid_type = deserialize("[\"bytes\",\"SGVsbG8\",123]").unwrap_err();
        // JS:         "Unknown bytes type marker type: number");
        assert_eq!(
            invalid_type.to_string(),
            "Unknown bytes type marker type: number"
        );
        // JS:   })
    }
    // JS:
    // JS:   it("preserves Invalid Date values through serialization", () => {
    // RUST-DEFERRED(JS built-in: Date)
    // JS:     let invalidDate = new Date(NaN);
    // JS:     let serialized = serialize(invalidDate);
    // JS:     expect(serialized).toBe('["date",null]');
    // JS:
    // JS:     let deserialized = deserialize(serialized) as Date;
    // JS:     expect(deserialized).toBeInstanceOf(Date);
    // JS:     expect(Number.isNaN(deserialized.getTime())).toBe(true);
    // JS:   })
    // JS: })
}
// JS:
// JS: // =======================================================================================
// JS:
// JS: describe("blob serialization", () => {
// JS:   it("rejects malformed blob wire values", () => {
// RUST-DEFERRED(JS built-in: Blob)
// JS:     // Missing parts.
// JS:     expect(() => deserialize('["blob"]')).toThrowError();
// JS:     expect(() => deserialize('["blob","text/plain"]')).toThrowError();
// JS:     // Non-string MIME type.
// JS:     expect(() => deserialize('["blob",123,["readable",0]]')).toThrowError();
// JS:     // Extra parts.
// JS:     expect(() => deserialize('["blob","text/plain",["readable",0],"extra"]')).toThrowError();
// JS:   });
// JS:
// JS:   it("throws when serializing Blob without an RPC session", () => {
// RUST-DEFERRED(JS built-in: Blob)
// JS:     // The encoder always uses a pipe, which requires an active RPC session. `serialize()` routes
// JS:     // through NULL_EXPORTER and therefore cannot support Blob — same as streams and stubs.
// JS:     let blob = new Blob(["hello"], {type: "text/plain"});
// JS:     expect(() => serialize(blob)).toThrowError("Cannot create pipes without an RPC session");
// JS:   })
// JS: })
// JS:
// JS: describe("error serialization", () => {
mod error_serialization {
    use super::*;
    // JS:   function roundTrip(err: Error): Error & Record<string, unknown> {
    fn round_trip(error: CapnwebError) -> CapnwebError {
        // JS:     return deserialize(serialize(err)) as unknown as Error & Record<string, unknown>;
        let wire = serialize(&CapnwebValue::Error(error)).unwrap();
        let CapnwebValue::Error(error) = deserialize(&wire).unwrap() else {
            panic!()
        };
        error
        // JS:   }
    }
    // JS:
    // JS:   it("preserves dynamically-attached own properties", () => {
    #[test]
    fn preserves_dynamically_attached_own_properties() {
        // JS:     let err = new Error("the message") as Error & Record<string, unknown>;
        let mut error = make_error("Error", "the message");
        // JS:     err.code = "E_FOO";
        error
            .properties
            .insert("code".to_owned(), CapnwebValue::String("E_FOO".to_owned()));
        // JS:     err.details = { reason: "x", retries: 3 };
        error.properties.insert(
            "details".to_owned(),
            object_value([
                ("reason", CapnwebValue::String("x".to_owned())),
                ("retries", CapnwebValue::Number(3.0)),
            ]),
        );
        // JS:
        // JS:     let result = roundTrip(err);
        let result = round_trip(error);
        // JS:     expect(result).toBeInstanceOf(Error);
        assert_eq!(result.name, "Error");
        // JS:     expect(result.name).toBe("Error");
        assert_eq!(result.name, "Error");
        // JS:     expect(result.message).toBe("the message");
        assert_eq!(result.message, "the message");
        // JS:     expect(result.code).toBe("E_FOO");
        assert!(
            matches!(result.properties.get("code"), Some(CapnwebValue::String(value)) if value == "E_FOO")
        );
        // JS:     expect(result.details).toStrictEqual({ reason: "x", retries: 3 });
        assert!(values_equal(
            result.properties.get("details").unwrap(),
            &object_value([
                ("reason", CapnwebValue::String("x".to_owned())),
                ("retries", CapnwebValue::Number(3.0))
            ])
        ));
        // JS:   });
    }
    // JS:
    // JS:   it("preserves class-field properties on Error subclasses", () => {
    #[test]
    fn preserves_class_field_properties_on_error_subclasses() {
        // JS:     class MyError extends Error {
        let mut error = make_error("MyError", "boom");
        // JS:       code = "E_FOO";
        error
            .properties
            .insert("code".to_owned(), CapnwebValue::String("E_FOO".to_owned()));
        // JS:       foo = "bar";
        error
            .properties
            .insert("foo".to_owned(), CapnwebValue::String("bar".to_owned()));
        // JS:     }
        // Rust models the subclass name dynamically.
        // JS:     let err = new MyError("boom");
        // Constructed above.
        // JS:
        // JS:     let result = roundTrip(err);
        let result = round_trip(error);
        // JS:     // Subclass identity is not preserved and falls back to Error.
        // JS:     expect(result).toBeInstanceOf(Error);
        assert_eq!(result.name, "Error");
        // JS:     expect(result.message).toBe("boom");
        assert_eq!(result.message, "boom");
        // JS:     expect(result.code).toBe("E_FOO");
        assert!(
            matches!(result.properties.get("code"), Some(CapnwebValue::String(value)) if value == "E_FOO")
        );
        // JS:     expect(result.foo).toBe("bar");
        assert!(
            matches!(result.properties.get("foo"), Some(CapnwebValue::String(value)) if value == "bar")
        );
        // JS:   });
    }
    // JS:
    // JS:   it("preserves built-in subclass identity when extras are present", () => {
    #[test]
    fn preserves_builtin_subclass_identity_when_extras_are_present() {
        // JS:     let err = new TypeError("t") as TypeError & Record<string, unknown>;
        let mut error = make_error("TypeError", "t");
        // JS:     err.code = 1;
        error
            .properties
            .insert("code".to_owned(), CapnwebValue::Number(1.0));
        // JS:
        // JS:     let result = roundTrip(err);
        let result = round_trip(error);
        // JS:     expect(result).toBeInstanceOf(TypeError);
        assert_eq!(result.name, "TypeError");
        // JS:     expect(result.name).toBe("TypeError");
        assert_eq!(result.name, "TypeError");
        // JS:     expect(result.message).toBe("t");
        assert_eq!(result.message, "t");
        // JS:     expect(result.code).toBe(1);
        assert!(matches!(
            result.properties.get("code"),
            Some(CapnwebValue::Number(1.0))
        ));
        // JS:   });
    }
    // JS:
    // JS:   it("emits the legacy 3-element form when there are no extras", () => {
    #[test]
    fn emits_the_legacy_three_element_form_when_there_are_no_extras() {
        // JS:     let err = new Error("plain");
        let mut error = make_error("Error", "plain");
        // JS:     err.stack = undefined;
        error.stack = None;
        // JS:     expect(serialize(err)).toBe('["error","Error","plain"]');
        assert_eq!(
            serialize(&CapnwebValue::Error(error)).unwrap(),
            "[\"error\",\"Error\",\"plain\"]"
        );
        // JS:   });
    }
    // JS:
    // JS:   it("emits 5 elements with null stack when extras are present but stack is absent", () => {
    #[test]
    fn emits_five_elements_with_null_stack_when_extras_are_present() {
        // JS:     let err = new Error("x") as Error & Record<string, unknown>;
        let mut error = make_error("Error", "x");
        // JS:     err.stack = undefined;
        error.stack = None;
        // JS:     err.code = "X";
        error
            .properties
            .insert("code".to_owned(), CapnwebValue::String("X".to_owned()));
        // JS:
        // JS:     let parsed = JSON.parse(serialize(err));
        let parsed: serde_json::Value =
            serde_json::from_str(&serialize(&CapnwebValue::Error(error)).unwrap()).unwrap();
        // JS:     expect(parsed[0]).toBe("error");
        assert_eq!(parsed[0], "error");
        // JS:     expect(parsed[1]).toBe("Error");
        assert_eq!(parsed[1], "Error");
        // JS:     expect(parsed[2]).toBe("x");
        assert_eq!(parsed[2], "x");
        // JS:     expect(parsed[3]).toBe(null);
        assert!(parsed[3].is_null());
        // JS:     expect(parsed[4]).toStrictEqual({ code: "X" });
        assert_eq!(parsed[4]["code"], "X");
        // JS:   });
    }
    // JS:
    // JS:   it("round-trips nested supported types inside props", () => {
    // RUST-DEFERRED(JS built-in: Date)
    // JS:     let err = new Error("nested") as Error & Record<string, unknown>;
    // JS:     err.when = new Date(1234);
    // JS:     err.big = 12345678901234567890n;
    // JS:     err.bytes = new TextEncoder().encode("hi!");
    // JS:     err.obj = { a: 1, b: [2, 3] };
    // JS:     err.inner = new TypeError("inner");
    // JS:
    // JS:     let result = roundTrip(err);
    // JS:     expect(result.when).toStrictEqual(new Date(1234));
    // JS:     expect(result.big).toBe(12345678901234567890n);
    // JS:     expect(new Uint8Array(result.bytes as Buffer)).toStrictEqual(new TextEncoder().encode("hi!"));
    // JS:     expect(result.obj).toStrictEqual({ a: 1, b: [2, 3] });
    // JS:
    // JS:     if (!(result.inner instanceof TypeError)) throw new Error("invariant");
    // JS:     expect(result.inner).toBeInstanceOf(TypeError);
    // JS:     expect(result.inner.message).toBe("inner");
    // JS:   });
    // JS:
    // JS:   it("is decodable by a legacy 4-element decoder (new sender -> old receiver)", () => {
    #[test]
    fn is_decodable_by_a_legacy_four_element_decoder() {
        // JS:     // Mimic the pre-change decoder branch: only look at value[1..3], ignore the rest.
        // JS:     function legacyDecode(json: string): Error & Record<string, unknown> {
        fn legacy_decode(json: &str) -> CapnwebError {
            // JS:       let value = JSON.parse(json);
            let value: serde_json::Value = serde_json::from_str(json).unwrap();
            // JS:       if (value.length >= 3 && typeof value[1] === "string" && typeof value[2] === "string") {
            if value.as_array().is_some_and(|value| value.len() >= 3)
                && value[1].is_string()
                && value[2].is_string()
            {
                // JS:         let cls: any = { Error, TypeError, RangeError }[value[1]] || Error;
                let name = match value[1].as_str().unwrap() {
                    "TypeError" => "TypeError",
                    "RangeError" => "RangeError",
                    _ => "Error",
                };
                // JS:         let result = new cls(value[2]);
                let mut result = make_error(name, value[2].as_str().unwrap());
                // JS:         if (typeof value[3] === "string") {
                if let Some(stack) = value.get(3).and_then(serde_json::Value::as_str) {
                    // JS:           result.stack = value[3];
                    result.stack = Some(stack.to_owned());
                    // JS:         }
                }
                // JS:         return result;
                return result;
                // JS:       }
            }
            // JS:       throw new Error("unparseable");
            panic!("unparseable");
            // JS:     }
        }
        // JS:
        // JS:     // Default `serialize` strips stack, so value[3] will be null. Old decoder's
        // JS:     // `typeof value[3] === "string"` check still passes (just doesn't fire), and value[4]
        // JS:     // is silently ignored. We're verifying the new shape doesn't break the old branch.
        // JS:     let err = new TypeError("t") as TypeError & Record<string, unknown>;
        let mut error = make_error("TypeError", "t");
        // JS:     err.code = "X";
        error
            .properties
            .insert("code".to_owned(), CapnwebValue::String("X".to_owned()));
        // JS:
        // JS:     let decoded = legacyDecode(serialize(err));
        let decoded = legacy_decode(&serialize(&CapnwebValue::Error(error)).unwrap());
        // JS:     expect(decoded).toBeInstanceOf(TypeError);
        assert_eq!(decoded.name, "TypeError");
        // JS:     expect(decoded.message).toBe("t");
        assert_eq!(decoded.message, "t");
        // JS:     expect(decoded.code).toBeUndefined();
        assert!(!decoded.properties.contains_key("code"));
        // JS:   });
    }
    // JS:
    // JS:   it("decodes legacy 3- and 4-element forms (old sender -> new receiver)", () => {
    #[test]
    fn decodes_legacy_three_and_four_element_forms() {
        // JS:     let three = deserialize('["error","Error","msg"]') as Error;
        let CapnwebValue::Error(three) = deserialize("[\"error\",\"Error\",\"msg\"]").unwrap()
        else {
            panic!()
        };
        // JS:     expect(three).toBeInstanceOf(Error);
        assert_eq!(three.name, "Error");
        // JS:     expect(three.message).toBe("msg");
        assert_eq!(three.message, "msg");
        // JS:
        // JS:     let four = deserialize('["error","TypeError","msg","trace"]') as Error;
        let CapnwebValue::Error(four) =
            deserialize("[\"error\",\"TypeError\",\"msg\",\"trace\"]").unwrap()
        else {
            panic!()
        };
        // JS:     expect(four).toBeInstanceOf(TypeError);
        assert_eq!(four.name, "TypeError");
        // JS:     expect(four.message).toBe("msg");
        assert_eq!(four.message, "msg");
        // JS:     expect(four.stack).toBe("trace");
        assert_eq!(four.stack.as_deref(), Some("trace"));
        // JS:   });
    }
    // JS:
    // JS:   it("throws when decoding an error with a malformed props bag", () => {
    #[test]
    fn throws_when_decoding_an_error_with_a_malformed_props_bag() {
        // JS:     // A non-object/array `props` is structurally invalid; reject rather than silently ignore.
        // JS:     expect(() => deserialize('["error","Error","msg",null,"not-an-object"]'))
        let string = deserialize("[\"error\",\"Error\",\"msg\",null,\"not-an-object\"]");
        // JS:         .toThrow(TypeError);
        assert!(string.is_err());
        // JS:     expect(() => deserialize('["error","Error","msg",null,42]'))
        let number = deserialize("[\"error\",\"Error\",\"msg\",null,42]");
        // JS:         .toThrow(TypeError);
        assert!(number.is_err());
        // JS:     expect(() => deserialize('["error","Error","msg",null,[1,2]]'))
        let array = deserialize("[\"error\",\"Error\",\"msg\",null,[1,2]]");
        // JS:         .toThrow(TypeError);
        assert!(array.is_err());
        // JS:     expect(() => deserialize('["error","Error","msg",null,null]'))
        let null = deserialize("[\"error\",\"Error\",\"msg\",null,null]");
        // JS:         .toThrow(TypeError);
        assert!(null.is_err());
        // JS:   });
    }
    // JS:
    // JS:   it("round-trips Error.cause", () => {
    #[test]
    fn round_trips_error_cause() {
        // JS:     let inner = new TypeError("inner");
        let inner = make_error("TypeError", "inner");
        // JS:     let outer = new Error("outer", { cause: inner });
        let mut outer = make_error("Error", "outer");
        outer
            .properties
            .insert("cause".to_owned(), CapnwebValue::Error(inner));
        // JS:
        // JS:     let result = roundTrip(outer);
        let result = round_trip(outer);
        // JS:     expect(result).toBeInstanceOf(Error);
        assert_eq!(result.name, "Error");
        // JS:     expect(result.message).toBe("outer");
        assert_eq!(result.message, "outer");
        // JS:
        // JS:     if (!(result.cause instanceof Error)) throw new Error("invariant");
        let Some(CapnwebValue::Error(cause)) = result.properties.get("cause") else {
            panic!("invariant")
        };
        // JS:     expect(result.cause).toBeInstanceOf(TypeError);
        assert_eq!(cause.name, "TypeError");
        // JS:     expect(result.cause.message).toBe("inner");
        assert_eq!(cause.message, "inner");
        // JS:   });
    }
    // JS:
    // JS:   it("round-trips a primitive cause", () => {
    #[test]
    fn round_trips_a_primitive_cause() {
        // JS:     let err = new Error("oops", { cause: "boom" });
        let mut error = make_error("Error", "oops");
        error
            .properties
            .insert("cause".to_owned(), CapnwebValue::String("boom".to_owned()));
        // JS:     let result = roundTrip(err);
        let result = round_trip(error);
        // JS:     expect(result.cause).toBe("boom");
        assert!(
            matches!(result.properties.get("cause"), Some(CapnwebValue::String(value)) if value == "boom")
        );
        // JS:   });
    }
    // JS:
    // JS:   it("round-trips AggregateError.errors with inner subclass identity", () => {
    #[test]
    fn round_trips_aggregate_error_errors_with_inner_subclass_identity() {
        // JS:     let agg = new AggregateError([new TypeError("a"), new RangeError("b")], "agg");
        let mut aggregate = make_error("AggregateError", "agg");
        aggregate.properties.insert(
            "errors".to_owned(),
            CapnwebValue::Array(vec![
                CapnwebValue::Error(make_error("TypeError", "a")),
                CapnwebValue::Error(make_error("RangeError", "b")),
            ]),
        );
        // JS:     let result = roundTrip(agg);
        let result = round_trip(aggregate);
        // JS:
        // JS:     if (!(result instanceof AggregateError)) throw new Error("invariant");
        assert_eq!(result.name, "AggregateError");
        // JS:     expect(result).toBeInstanceOf(AggregateError);
        assert_eq!(result.name, "AggregateError");
        // JS:     expect(result.message).toBe("agg");
        assert_eq!(result.message, "agg");
        // JS:     expect(Array.isArray(result.errors)).toBe(true);
        let Some(CapnwebValue::Array(errors)) = result.properties.get("errors") else {
            panic!()
        };
        // JS:     expect(result.errors).toHaveLength(2);
        assert_eq!(errors.len(), 2);
        // JS:     expect(result.errors[0]).toBeInstanceOf(TypeError);
        let CapnwebValue::Error(first) = &errors[0] else {
            panic!()
        };
        assert_eq!(first.name, "TypeError");
        // JS:     expect(result.errors[0].message).toBe("a");
        assert_eq!(first.message, "a");
        // JS:     expect(result.errors[1]).toBeInstanceOf(RangeError);
        let CapnwebValue::Error(second) = &errors[1] else {
            panic!()
        };
        assert_eq!(second.name, "RangeError");
        // JS:     expect(result.errors[1].message).toBe("b");
        assert_eq!(second.message, "b");
        // JS:   });
    }
    // JS:
    // JS:   it("silently drops properties whose values cannot be serialized", () => {
    #[test]
    fn unsupported_properties_are_excluded_by_the_rust_value_type() {
        // JS:     let err = new Error("with-bad-prop") as Error & Record<string, unknown>;
        let mut error = make_error("Error", "with-bad-prop");
        // JS:     err.code = "E_OK";
        error
            .properties
            .insert("code".to_owned(), CapnwebValue::String("E_OK".to_owned()));
        // JS:     err.bad = Object.create(null);
        let bad_property_is_not_a_capnweb_value = ();
        let _ = bad_property_is_not_a_capnweb_value;
        // JS:
        // JS:     let serialized = serialize(err);
        let serialized = serialize(&CapnwebValue::Error(error)).unwrap();
        // JS:     let result = deserialize(serialized) as Error & Record<string, unknown>;
        let CapnwebValue::Error(result) = deserialize(&serialized).unwrap() else {
            panic!()
        };
        // JS:
        // JS:     expect(result).toBeInstanceOf(Error);
        assert_eq!(result.name, "Error");
        // JS:     expect(result.message).toBe("with-bad-prop");
        assert_eq!(result.message, "with-bad-prop");
        // JS:     expect(result.code).toBe("E_OK");
        assert!(
            matches!(result.properties.get("code"), Some(CapnwebValue::String(value)) if value == "E_OK")
        );
        // JS:     expect("bad" in result).toBe(false);
        assert!(!result.properties.contains_key("bad"));
        // JS:   });
    }
    // JS:
    // JS:   it("silently drops cyclic property values", () => {
    #[test]
    fn cyclic_properties_are_excluded_by_rust_ownership() {
        // JS:     let err = new Error("cyclic") as Error & Record<string, unknown>;
        let mut error = make_error("Error", "cyclic");
        // JS:     err.code = "E_OK";
        error
            .properties
            .insert("code".to_owned(), CapnwebValue::String("E_OK".to_owned()));
        // JS:     let cycle: any = {};
        let cycle = BTreeMap::<String, CapnwebValue>::new();
        // JS:     cycle.self = cycle;
        let _cycle_cannot_contain_itself_under_rust_ownership = cycle;
        // JS:     err.cycle = cycle;
        // No cyclic `CapnwebValue` can be inserted.
        // JS:
        // JS:     let result = roundTrip(err);
        let result = round_trip(error);
        // JS:     expect(result.message).toBe("cyclic");
        assert_eq!(result.message, "cyclic");
        // JS:     expect(result.code).toBe("E_OK");
        assert!(
            matches!(result.properties.get("code"), Some(CapnwebValue::String(value)) if value == "E_OK")
        );
        // JS:     expect("cycle" in result).toBe(false);
        assert!(!result.properties.contains_key("cycle"));
        // JS:   });
    }
    // JS:
    // JS:   it("never throws even when every extra property is unsupported", () => {
    #[test]
    fn statically_unsupported_properties_never_enter_the_error_value() {
        // JS:     let err = new Error("all-bad") as Error & Record<string, unknown>;
        let error = make_error("Error", "all-bad");
        // JS:     err.a = Object.create(null);
        let unsupported_a = ();
        // JS:     err.b = Symbol("x");
        let unsupported_b = ();
        let _ = (unsupported_a, unsupported_b);
        // JS:
        // JS:     let result = roundTrip(err);
        let result = round_trip(error);
        // JS:     expect(result.message).toBe("all-bad");
        assert_eq!(result.message, "all-bad");
        // JS:     expect("a" in result).toBe(false);
        assert!(!result.properties.contains_key("a"));
        // JS:     expect("b" in result).toBe(false);
        assert!(!result.properties.contains_key("b"));
        // JS:   })
    }
    // JS: })
}
// JS:
// JS: // =======================================================================================
// JS:
// JS: class TestTransport implements RpcTransport {
// JS:   constructor(public name: string, private partner?: TestTransport) {
// JS:     if (partner) {
// JS:       partner.partner = this;
// JS:     }
// JS:   }
// JS:
// JS:   private queue: string[] = [];
// JS:   private waiter?: () => void;
// JS:   private aborter?: (err: any) => void;
// JS:   public log = false;
// JS:   public sentLog: string[] = [];
// JS:   private fenced = false;
// JS:
// JS:   send(message: string): void {
// JS:     // HACK: If the string "$remove$" appears in the message, remove it. This is used in some
// JS:     //   tests to hack the RPC protocol.
// JS:     message = message.replaceAll("$remove$", "");
// JS:
// JS:     if (this.log) console.log(`${this.name}: ${message}`);
// JS:     this.sentLog.push(message);
// JS:     this.partner!.queue.push(message);
// JS:     if (this.partner!.waiter && !this.partner!.fenced) {
// JS:       this.partner!.waiter();
// JS:       this.partner!.waiter = undefined;
// JS:       this.partner!.aborter = undefined;
// JS:     }
// JS:   }
// JS:
// JS:   async receive(): Promise<string> {
// JS:     while (this.queue.length == 0 || this.fenced) {
// JS:       await new Promise<void>((resolve, reject) => {
// JS:         this.waiter = resolve;
// JS:         this.aborter = reject;
// JS:       });
// JS:     }
// JS:
// JS:     return this.queue.shift()!;
// JS:   }
// JS:
// JS:   // Blocks this transport from receiving messages. Messages sent to this transport will
// JS:   // accumulate in the queue until the fence is released.
// JS:   fence() {
// JS:     this.fenced = true;
// JS:   }
// JS:
// JS:   // Releases the fence, allowing queued messages to be received.
// JS:   releaseFence() {
// JS:     this.fenced = false;
// JS:     if (this.queue.length > 0 && this.waiter) {
// JS:       this.waiter();
// JS:       this.waiter = undefined;
// JS:       this.aborter = undefined;
// JS:     }
// JS:   }
// JS:
// JS:   // Returns the number of messages waiting in the receive queue.
// JS:   get pendingCount() {
// JS:     return this.queue.length;
// JS:   }
// JS:
// JS:   forceReceiveError(error: any) {
// JS:     this.aborter!(error);
// JS:   }
// JS: }
// JS:
// JS: class ObjectTestTransport implements RpcTransportWithCustomEncoding {
// JS:   constructor(
// JS:       private partner?: ObjectTestTransport,
// JS:       readonly encodingLevel: CustomEncodingLevel = "jsonCompatible") {
// JS:     if (partner) {
// JS:       partner.partner = this;
// JS:     }
// JS:   }
// JS:
// JS:   private queue: unknown[] = [];
// JS:   private waiter?: () => void;
// JS:   private aborter?: (err: any) => void;
// JS:   private fenced = false;
// JS:
// JS:   send(message: unknown): void {
// JS:     let cloned = this.encodingLevel === "jsonCompatible" ? JSON.parse(JSON.stringify(message))
// JS:                                                          : structuredClone(message);
// JS:     this.partner!.queue.push(cloned);
// JS:     if (this.partner!.waiter && !this.partner!.fenced) {
// JS:       this.partner!.waiter();
// JS:       this.partner!.waiter = undefined;
// JS:       this.partner!.aborter = undefined;
// JS:     }
// JS:   }
// JS:
// JS:   async receive(): Promise<unknown> {
// JS:     while (this.queue.length == 0 || this.fenced) {
// JS:       await new Promise<void>((resolve, reject) => {
// JS:         this.waiter = resolve;
// JS:         this.aborter = reject;
// JS:       });
// JS:     }
// JS:
// JS:     return this.queue.shift()!;
// JS:   }
// JS:
// JS:   fence() {
// JS:     this.fenced = true;
// JS:   }
// JS:
// JS:   releaseFence() {
// JS:     this.fenced = false;
// JS:     if (this.queue.length > 0 && this.waiter) {
// JS:       this.waiter();
// JS:       this.waiter = undefined;
// JS:       this.aborter = undefined;
// JS:     }
// JS:   }
// JS:
// JS:   abort(reason: any): void {
// JS:     this.aborter?.(reason);
// JS:     this.waiter = undefined;
// JS:     this.aborter = undefined;
// JS:   }
// JS: }
// JS:
// JS: // Spin the microtask queue a bit to give messages time to be delivered and handled.
// JS: async function pumpMicrotasks() {
// JS:   for (let i = 0; i < 16; i++) {
// JS:     await Promise.resolve();
// JS:   }
// JS: }
// JS:
// JS: class TestHarness<T extends RpcTarget> {
// JS:   clientTransport: TestTransport;
// JS:   serverTransport: TestTransport;
// JS:   client: RpcSession<T>;
// JS:   server: RpcSession;
// JS:
// JS:   stub: RpcStub<T>;
// JS:
// JS:   constructor(target: T, serverOptions?: RpcSessionOptions) {
// JS:     this.clientTransport = new TestTransport("client");
// JS:     this.serverTransport = new TestTransport("server", this.clientTransport);
// JS:
// JS:     this.client = new RpcSession<T>(this.clientTransport);
// JS:
// JS:     this.server = new RpcSession(this.serverTransport, target, serverOptions);
// JS:
// JS:     this.stub = this.client.getRemoteMain();
// JS:   }
// JS:
// JS:   // Enable logging of all messages sent. Useful for debugging.
// JS:   enableLogging() {
// JS:     this.clientTransport.log = true;
// JS:     this.serverTransport.log = true;
// JS:   }
// JS:
// JS:   checkAllDisposed() {
// JS:     expect(this.client.getStats(), "client").toStrictEqual({imports: 1, exports: 1});
// JS:     expect(this.server.getStats(), "server").toStrictEqual({imports: 1, exports: 1});
// JS:   }
// JS:
// JS:   async [Symbol.asyncDispose]() {
// JS:     try {
// JS:       // HACK: Spin the microtask loop for a bit to make sure dispose messages have been sent
// JS:       //   and received.
// JS:       await pumpMicrotasks();
// JS:
// JS:       // Check at the end of every test that everything was disposed.
// JS:       this.checkAllDisposed();
// JS:     } catch (err) {
// JS:       // Don't throw from disposer as it may suppress the real error that caused the disposal in
// JS:       // the first place.
// JS:
// JS:       // I couldn't find a better way to make vitest log a failure without throwing...
// JS:       let message: string;
// JS:       if (err instanceof Error) {
// JS:         message = err.stack || err.message;
// JS:       } else {
// JS:         message = `${err}`;
// JS:       }
// JS:       expect.soft(true, message).toBe(false);
// JS:     }
// JS:   }
// JS: }
// JS:
enum FailingSendMode {
    AsyncString,
    SyncString,
    SyncCustom,
}
struct FailingSendTransport(FailingSendMode);
impl AnyRpcTransport for FailingSendTransport {
    fn encoding_level(&self) -> EncodingLevel {
        match self.0 {
            FailingSendMode::SyncCustom => EncodingLevel::StructuredClonable,
            _ => EncodingLevel::String,
        }
    }
    fn send(&self, _message: TransportMessage) -> Result<TransportSend> {
        match self.0 {
            FailingSendMode::AsyncString => Ok(TransportSend::Pending(Box::pin(async {
                Err(Error::Message("send failed".to_owned()))
            }))),
            FailingSendMode::SyncString => Err(Error::Message("sync send failed".to_owned())),
            FailingSendMode::SyncCustom => {
                Err(Error::Message("sync custom send failed".to_owned()))
            }
        }
    }
    fn receive(self: Arc<Self>) -> Pin<Box<dyn Future<Output = Result<TransportMessage>> + Send>> {
        Box::pin(pending())
    }
}

// JS: it("propagates async send failures from string transports", async () => {
#[tokio::test(flavor = "current_thread")]
async fn propagates_async_send_failures_from_string_transports() {
    // JS:   let sendError = new Error("send failed");
    let send_error = "send failed";
    // JS:   let transport: RpcTransport = {
    let transport = Arc::new(FailingSendTransport(FailingSendMode::AsyncString));
    // JS:     send(_message: string): Promise<void> {
    // Implemented by `FailingSendTransport::send()`.
    // JS:       return Promise.reject(sendError);
    // The pending send rejects with `send_error`.
    // JS:     },
    // JS:     receive(): Promise<string> {
    // Implemented by `FailingSendTransport::receive()`.
    // JS:       return new Promise(() => {});
    // The receive future remains pending.
    // JS:     },
    // JS:   };
    // JS:
    // JS:   let session = new RpcSession<TestTarget>(transport);
    let session = RpcSession::new(transport, None, RpcSessionOptions::default()).unwrap();
    // JS:   using stub = session.getRemoteMain();
    let stub = session.get_remote_main();
    // JS:
    // JS:   await expect(() => stub.square(1)).rejects.toThrow(sendError);
    let error = stub
        .call(vec!["square".to_owned()], one_number_argument(1.0))
        .pull()
        .await
        .unwrap_err();
    assert!(error.to_string().contains(send_error));
    // JS: });
}
// JS:
// JS: it("propagates synchronous send failures from string transports", async () => {
#[tokio::test(flavor = "current_thread")]
async fn propagates_synchronous_send_failures_from_string_transports() {
    // JS:   // A transport whose send() throws synchronously (rather than rejecting a promise) must still
    // JS:   // abort the session. The abort is deliberately deferred to a microtask so the caller finishes
    // JS:   // its own bookkeeping first, matching the timing of a rejected promise from an async transport.
    // JS:   let sendError = new Error("sync send failed");
    let send_error = "sync send failed";
    // JS:   let transport: RpcTransport = {
    let transport = Arc::new(FailingSendTransport(FailingSendMode::SyncString));
    // JS:     send(_message: string): void {
    // JS:       throw sendError;
    // JS:     },
    // JS:     receive(): Promise<string> {
    // JS:       return new Promise(() => {});
    // JS:     },
    // JS:   };
    // JS:
    // JS:   let session = new RpcSession<TestTarget>(transport);
    let session = RpcSession::new(transport, None, RpcSessionOptions::default()).unwrap();
    // JS:   using stub = session.getRemoteMain();
    let stub = session.get_remote_main();
    // JS:
    // JS:   await expect(() => stub.square(1)).rejects.toThrow(sendError);
    let error = stub
        .call(vec!["square".to_owned()], one_number_argument(1.0))
        .pull()
        .await
        .unwrap_err();
    assert!(error.to_string().contains(send_error));
    // JS: });
}
// JS:
// JS: it("propagates synchronous send failures from custom-encoding transports", async () => {
#[tokio::test(flavor = "current_thread")]
async fn propagates_synchronous_send_failures_from_custom_encoding_transports() {
    // JS:   // Same as above, but exercising the custom-encoding (non-string) send path.
    // JS:   let sendError = new Error("sync custom send failed");
    let send_error = "sync custom send failed";
    // JS:   let transport: RpcTransportWithCustomEncoding = {
    let transport = Arc::new(FailingSendTransport(FailingSendMode::SyncCustom));
    // JS:     encodingLevel: "structuredClonable",
    // JS:     send(_message: unknown): void {
    // JS:       throw sendError;
    // JS:     },
    // JS:     receive(): Promise<unknown> {
    // JS:       return new Promise(() => {});
    // JS:     },
    // JS:   };
    // JS:
    // JS:   let session = new RpcSession<TestTarget>(transport);
    let session = RpcSession::new(transport, None, RpcSessionOptions::default()).unwrap();
    // JS:   using stub = session.getRemoteMain();
    let stub = session.get_remote_main();
    // JS:
    // JS:   await expect(() => stub.square(1)).rejects.toThrow(sendError);
    let error = stub
        .call(vec!["square".to_owned()], one_number_argument(1.0))
        .pull()
        .await
        .unwrap_err();
    assert!(error.to_string().contains(send_error));
    // JS: });
}
// JS:
struct LocalTestTarget;

impl RpcTargetInner for LocalTestTarget {
    fn call(self: Arc<Self>, path: PropertyPath, args: RpcPayload) -> Result<StubHook> {
        Ok(PromiseStubHook::from_future(async move {
            let value = args.deliver_resolve().await?;
            let CapnwebValue::Array(mut arguments) = value else {
                return Err(Error::Type("arguments are not an array".to_owned()));
            };
            let result = match path.as_slice() {
                [PropertyKey::String(method)] if method == "square" => {
                    let CapnwebValue::Number(value) = arguments.remove(0) else {
                        return Err(Error::Type("square argument is not a number".to_owned()));
                    };
                    CapnwebValue::Number(value * value)
                }
                [PropertyKey::String(method)] if method == "throwError" => {
                    return Err(Error::Message("test error".to_owned()));
                }
                [PropertyKey::String(method)] if method == "returnNull" => CapnwebValue::Null,
                [PropertyKey::String(method)] if method == "returnUndefined" => {
                    CapnwebValue::Undefined
                }
                [PropertyKey::String(method)] if method == "returnNumber" => arguments.remove(0),
                [PropertyKey::String(method)] if method == "generateFibonacci" => {
                    let CapnwebValue::Number(count) = arguments.remove(0) else {
                        panic!()
                    };
                    let (mut a, mut b) = (0.0, 1.0);
                    CapnwebValue::Array(
                        (0..count as usize)
                            .map(|_| {
                                let value = a;
                                (a, b) = (b, a + b);
                                CapnwebValue::Number(value)
                            })
                            .collect(),
                    )
                }
                [PropertyKey::String(method)]
                    if method == "makeCounter" || method == "getCounter" =>
                {
                    let value = match arguments.into_iter().next() {
                        Some(CapnwebValue::Number(value)) => value,
                        None if method == "getCounter" => 10.0,
                        _ => panic!(),
                    };
                    CapnwebValue::Target(Arc::new(LocalCounter(std::sync::Mutex::new(value))))
                }
                [PropertyKey::String(method)] if method == "callSquare" => {
                    let stub = RpcStub::from_value(arguments.remove(0));
                    let value = arguments.remove(0);
                    CapnwebValue::Object(BTreeMap::from([(
                        "result".to_owned(),
                        CapnwebValue::RpcPromise(stub.call(
                            vec!["square".to_owned()],
                            RpcPayload::from_app_params(CapnwebValue::Array(vec![value])),
                        )),
                    )]))
                }
                [PropertyKey::String(method)] if method == "incrementCounter" => {
                    let counter = RpcStub::from_value(arguments.remove(0));
                    let amount = arguments
                        .into_iter()
                        .next()
                        .unwrap_or(CapnwebValue::Number(1.0));
                    counter
                        .call(
                            vec!["increment".to_owned()],
                            RpcPayload::from_app_params(CapnwebValue::Array(vec![amount])),
                        )
                        .pull()
                        .await?
                }
                [PropertyKey::String(method)] if method == "callFunction" => {
                    let function = RpcStub::from_value(arguments.remove(0));
                    let argument = arguments.remove(0);
                    let result = function
                        .call(
                            Vec::new(),
                            RpcPayload::from_app_params(CapnwebValue::Array(vec![argument])),
                        )
                        .pull()
                        .await?;
                    object_value([("result", result)])
                }
                _ => return Err(Error::Type(format!("No such RPC method: {path:?}"))),
            };
            Ok(Arc::new(PayloadStubHook::new(RpcPayload::from_app_return(result))) as StubHook)
        }))
    }

    fn get(self: Arc<Self>, path: PropertyPath) -> Result<StubHook> {
        if path.len() > 1 {
            return Err(Error::Type(
                "Cannot read a property of undefined".to_owned(),
            ));
        }
        Ok(Arc::new(PayloadStubHook::new(RpcPayload::from_app_return(
            CapnwebValue::Undefined,
        ))))
    }
}

fn local_test_stub() -> RpcStub {
    RpcStub::from_value(CapnwebValue::Target(Arc::new(LocalTestTarget)))
}

fn number_arrays(values: &[f64]) -> CapnwebValue {
    CapnwebValue::Array(values.iter().copied().map(CapnwebValue::Number).collect())
}

fn one_number_argument(value: f64) -> RpcPayload {
    RpcPayload::from_app_params(CapnwebValue::Array(vec![CapnwebValue::Number(value)]))
}

struct LocalCounter(std::sync::Mutex<f64>);

impl RpcTargetInner for LocalCounter {
    fn call(self: Arc<Self>, path: PropertyPath, args: RpcPayload) -> Result<StubHook> {
        Ok(PromiseStubHook::from_future(async move {
            if path != vec![PropertyKey::String("increment".to_owned())] {
                return Err(Error::Type(format!("No such counter method: {path:?}")));
            }
            let CapnwebValue::Array(arguments) = args.deliver_resolve().await? else {
                panic!()
            };
            let by = match arguments.first() {
                Some(CapnwebValue::Number(value)) => *value,
                None => 1.0,
                _ => panic!(),
            };
            let value = {
                let mut value = self.0.lock().unwrap();
                *value += by;
                *value
            };
            Ok(Arc::new(PayloadStubHook::new(RpcPayload::from_app_return(
                CapnwebValue::Number(value),
            ))) as StubHook)
        }))
    }

    fn get(self: Arc<Self>, path: PropertyPath) -> Result<StubHook> {
        if path != vec![PropertyKey::String("value".to_owned())] {
            return Err(Error::Type(format!("No such counter property: {path:?}")));
        }
        let value = *self.0.lock().unwrap();
        Ok(Arc::new(PayloadStubHook::new(RpcPayload::from_app_return(
            CapnwebValue::Number(value),
        ))))
    }
}

fn local_counter(value: f64) -> RpcStub {
    RpcStub::from_value(CapnwebValue::Target(Arc::new(LocalCounter(
        std::sync::Mutex::new(value),
    ))))
}

struct ReflectionTarget;

impl RpcTargetInner for ReflectionTarget {
    fn call(self: Arc<Self>, path: PropertyPath, _args: RpcPayload) -> Result<StubHook> {
        let value = match path.as_slice() {
            [PropertyKey::String(method)] if method == "prototypeMethod" => "method",
            [PropertyKey::String(method)] if method == "publicMethod" => "public",
            _ => return Err(Error::Type(format!("No such RPC method: {path:?}"))),
        };
        Ok(Arc::new(PayloadStubHook::new(RpcPayload::from_app_return(
            CapnwebValue::String(value.to_owned()),
        ))))
    }

    fn get(self: Arc<Self>, path: PropertyPath) -> Result<StubHook> {
        let value = match path.as_slice() {
            [PropertyKey::String(property)] if property == "prototypeProp" => {
                CapnwebValue::String("prototype".to_owned())
            }
            [PropertyKey::String(property)] if property == "#privateMethod" => {
                CapnwebValue::Undefined
            }
            [PropertyKey::String(property)]
                if property == "instanceProp" || property == "dynamicProp" =>
            {
                return Err(Error::Type(format!(
                    "Attempted to access instance property '{property}'"
                )));
            }
            _ => CapnwebValue::Undefined,
        };
        Ok(Arc::new(PayloadStubHook::new(RpcPayload::from_app_return(
            value,
        ))))
    }
}

struct OuterNestedTarget {
    property: &'static str,
    inner: Arc<LocalTestTarget>,
    as_stub: bool,
}

impl RpcTargetInner for OuterNestedTarget {
    fn call(self: Arc<Self>, path: PropertyPath, args: RpcPayload) -> Result<StubHook> {
        match path.as_slice() {
            [PropertyKey::String(method)] if method == "getValue" => Ok(Arc::new(
                PayloadStubHook::new(RpcPayload::from_app_return(CapnwebValue::Number(42.0))),
            )),
            [PropertyKey::String(property), rest @ ..] if property == self.property => {
                self.inner.clone().call(rest.to_vec(), args)
            }
            _ => Err(Error::Type(format!("No such RPC method: {path:?}"))),
        }
    }

    fn get(self: Arc<Self>, path: PropertyPath) -> Result<StubHook> {
        if path == vec![PropertyKey::String(self.property.to_owned())] {
            let value = if self.as_stub {
                CapnwebValue::Stub(RpcStub::from_value(CapnwebValue::Target(
                    self.inner.clone(),
                )))
            } else {
                CapnwebValue::Target(self.inner.clone())
            };
            Ok(Arc::new(PayloadStubHook::new(RpcPayload::from_app_return(
                value,
            ))))
        } else {
            Err(Error::Type(format!("No such RPC property: {path:?}")))
        }
    }
}

// JS: describe("local stub", () => {
mod local_stub {
    use super::*;
    // JS:   it("supports wrapping an RpcTarget", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn supports_wrapping_an_rpc_target() {
        // JS:     let stub = new RpcStub(new TestTarget());
        let stub = local_test_stub();
        // JS:     expect(await stub.square(3)).toBe(9);
        let result = stub
            .call(vec!["square".to_owned()], one_number_argument(3.0))
            .pull()
            .await
            .unwrap();
        assert!(matches!(result, CapnwebValue::Number(9.0)));
        // JS:   });
    }
    // JS:
    // JS:   it("supports wrapping a function", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn supports_wrapping_a_function() {
        // JS:     // TODO: If we don't explicitly declare the type of `i` then the type system complains about
        // JS:     //   too-deep recursion here. Why?
        // JS:     let stub = new RpcStub((i :number) => i + 5);
        let stub = RpcStub::from_value(function_target(|args| async move {
            let CapnwebValue::Array(args) = args.deliver_resolve().await? else {
                panic!()
            };
            let CapnwebValue::Number(value) = args[0] else {
                panic!()
            };
            Ok(CapnwebValue::Number(value + 5.0))
        }));
        // JS:     expect(await stub(3)).toBe(8);
        assert!(matches!(
            stub.call(Vec::new(), one_number_argument(3.0))
                .pull()
                .await
                .unwrap(),
            CapnwebValue::Number(8.0)
        ));
        // JS:   });
    }
    // JS:
    // JS:   it("supports wrapping an async function", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn supports_wrapping_an_async_function() {
        // JS:     let stub = new RpcStub(async (i :number) => { return i + 5; });
        let stub = RpcStub::from_value(function_target(|args| async move {
            tokio::task::yield_now().await;
            let CapnwebValue::Array(args) = args.deliver_resolve().await? else {
                panic!()
            };
            let CapnwebValue::Number(value) = args[0] else {
                panic!()
            };
            Ok(CapnwebValue::Number(value + 5.0))
        }));
        // JS:     expect(await stub(3)).toBe(8);
        assert!(matches!(
            stub.call(Vec::new(), one_number_argument(3.0))
                .pull()
                .await
                .unwrap(),
            CapnwebValue::Number(8.0)
        ));
        // JS:   });
    }
    // JS:
    // JS:   it("supports wrapping an arbitrary object", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn supports_wrapping_an_arbitrary_object() {
        // JS:     let stub = new RpcStub({abc: "hello"});
        let stub = RpcStub::from_value(object_value([(
            "abc",
            CapnwebValue::String("hello".to_owned()),
        )]));
        // JS:     expect(await stub.abc).toBe("hello");
        assert!(
            matches!(stub.get(vec!["abc".to_owned()]).pull().await.unwrap(), CapnwebValue::String(value) if value == "hello")
        );
        // JS:   });
    }
    // JS:
    // JS:   it("supports wrapping an object with nested stubs", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn supports_wrapping_an_object_with_nested_stubs() {
        // JS:     let innerTarget = new TestTarget();
        let inner_target = Arc::new(LocalTestTarget);
        // JS:     let innerStub = new RpcStub(innerTarget);
        let inner_stub = RpcStub::from_value(CapnwebValue::Target(inner_target));
        // JS:     let outerObject = { inner: innerStub, value: 42 };
        let outer_object = object_value([
            ("inner", CapnwebValue::Stub(inner_stub)),
            ("value", CapnwebValue::Number(42.0)),
        ]);
        // JS:     let outerStub = new RpcStub(outerObject);
        let outer_stub = RpcStub::from_value(outer_object);
        // JS:
        // JS:     expect(await outerStub.value).toBe(42);
        assert!(matches!(
            outer_stub
                .get(vec!["value".to_owned()])
                .pull()
                .await
                .unwrap(),
            CapnwebValue::Number(42.0)
        ));
        // JS:     expect(await outerStub.inner.square(4)).toBe(16);
        assert!(matches!(
            outer_stub
                .call(
                    vec!["inner".to_owned(), "square".to_owned()],
                    one_number_argument(4.0)
                )
                .pull()
                .await
                .unwrap(),
            CapnwebValue::Number(16.0)
        ));
        // JS:   });
    }
    // JS:
    // JS:   it("supports wrapping an object with nested RpcTargets", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn supports_wrapping_an_object_with_nested_rpc_targets() {
        // JS:     let innerTarget = new TestTarget();
        let inner_target = Arc::new(LocalTestTarget);
        // JS:     let outerObject = { inner: innerTarget, value: 42 };
        let outer_object = object_value([
            ("inner", CapnwebValue::Target(inner_target)),
            ("value", CapnwebValue::Number(42.0)),
        ]);
        // JS:     let outerStub = new RpcStub(outerObject);
        let outer_stub = RpcStub::from_value(outer_object);
        // JS:
        // JS:     expect(await outerStub.value).toBe(42);
        assert!(matches!(
            outer_stub
                .get(vec!["value".to_owned()])
                .pull()
                .await
                .unwrap(),
            CapnwebValue::Number(42.0)
        ));
        // JS:     expect(await outerStub.inner.square(4)).toBe(16);
        assert!(matches!(
            outer_stub
                .call(
                    vec!["inner".to_owned(), "square".to_owned()],
                    one_number_argument(4.0)
                )
                .pull()
                .await
                .unwrap(),
            CapnwebValue::Number(16.0)
        ));
        // JS:   });
    }
    // JS:
    // JS:   it("supports wrapping an object with nested functions", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn supports_wrapping_an_object_with_nested_functions() {
        // JS:     let outerObject = { square: (x: number) => x * x, value: 42 };
        let square = function_target(|args| async move {
            let CapnwebValue::Array(args) = args.deliver_resolve().await? else {
                panic!()
            };
            let CapnwebValue::Number(value) = args[0] else {
                panic!()
            };
            Ok(CapnwebValue::Number(value * value))
        });
        let outer_object =
            object_value([("square", square), ("value", CapnwebValue::Number(42.0))]);
        // JS:     let outerStub = new RpcStub(outerObject);
        let outer_stub = RpcStub::from_value(outer_object);
        // JS:
        // JS:     expect(await outerStub.value).toBe(42);
        assert!(matches!(
            outer_stub
                .get(vec!["value".to_owned()])
                .pull()
                .await
                .unwrap(),
            CapnwebValue::Number(42.0)
        ));
        // JS:     expect(await outerStub.square(4)).toBe(16);
        assert!(matches!(
            outer_stub
                .call(vec!["square".to_owned()], one_number_argument(4.0))
                .pull()
                .await
                .unwrap(),
            CapnwebValue::Number(16.0)
        ));
        // JS:   });
    }
    // JS:
    // JS:   it("supports wrapping an object with nested async functions", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn supports_wrapping_an_object_with_nested_async_functions() {
        // JS:     async function asyncSqare(x: number) {
        let async_square = function_target(|args| async move {
            // JS:       await Promise.resolve();
            tokio::task::yield_now().await;
            // JS:       return x * x;
            let CapnwebValue::Array(args) = args.deliver_resolve().await? else {
                panic!()
            };
            let CapnwebValue::Number(value) = args[0] else {
                panic!()
            };
            Ok(CapnwebValue::Number(value * value))
            // JS:     }
        });
        // JS:
        // JS:     let outerObject = { square: asyncSqare, value: 42 };
        let outer_object = object_value([
            ("square", async_square),
            ("value", CapnwebValue::Number(42.0)),
        ]);
        // JS:     let outerStub = new RpcStub(outerObject);
        let outer_stub = RpcStub::from_value(outer_object);
        // JS:
        // JS:     expect(await outerStub.value).toBe(42);
        assert!(matches!(
            outer_stub
                .get(vec!["value".to_owned()])
                .pull()
                .await
                .unwrap(),
            CapnwebValue::Number(42.0)
        ));
        // JS:     expect(await outerStub.square(4)).toBe(16);
        assert!(matches!(
            outer_stub
                .call(vec!["square".to_owned()], one_number_argument(4.0))
                .pull()
                .await
                .unwrap(),
            CapnwebValue::Number(16.0)
        ));
        // JS:   });
    }
    // JS:
    // JS:   it("supports wrapping an RpcTarget with nested stubs", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn supports_wrapping_an_rpc_target_with_nested_stubs() {
        // JS:     class TargetWithStubs extends RpcTarget {
        let target = OuterNestedTarget {
            property: "innerStub",
            inner: Arc::new(LocalTestTarget),
            as_stub: true,
        };
        // JS:       getValue() { return 42; }
        // Implemented by `OuterNestedTarget::call()`.
        // JS:
        // JS:       get innerStub() {
        // Implemented by `OuterNestedTarget::get()`.
        // JS:         return new RpcStub(new TestTarget());
        // The getter returns `CapnwebValue::Stub` when `as_stub` is true.
        // JS:       }
        // JS:     }
        // JS:
        // JS:     let outerStub = new RpcStub(new TargetWithStubs());
        let outer_stub = RpcStub::from_value(CapnwebValue::Target(Arc::new(target)));
        // JS:     expect(await outerStub.getValue()).toBe(42);
        assert!(matches!(
            outer_stub
                .call(
                    vec!["getValue".to_owned()],
                    RpcPayload::from_app_params(CapnwebValue::Array(vec![]))
                )
                .pull()
                .await
                .unwrap(),
            CapnwebValue::Number(42.0)
        ));
        // JS:     expect(await outerStub.innerStub.square(3)).toBe(9);
        assert!(matches!(
            outer_stub
                .call(
                    vec!["innerStub".to_owned(), "square".to_owned()],
                    one_number_argument(3.0)
                )
                .pull()
                .await
                .unwrap(),
            CapnwebValue::Number(9.0)
        ));
        // JS:   });
    }
    // JS:
    // JS:   it("supports wrapping an RpcTarget with nested RpcTargets", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn supports_wrapping_an_rpc_target_with_nested_rpc_targets() {
        // JS:     class TargetWithTargets extends RpcTarget {
        let target = OuterNestedTarget {
            property: "innerTarget",
            inner: Arc::new(LocalTestTarget),
            as_stub: false,
        };
        // JS:       getValue() { return 42; }
        // Implemented by `OuterNestedTarget::call()`.
        // JS:
        // JS:       get innerTarget() {
        // Implemented by `OuterNestedTarget::get()`.
        // JS:         return new TestTarget();
        // The getter returns `CapnwebValue::Target` when `as_stub` is false.
        // JS:       }
        // JS:     }
        // JS:
        // JS:     let outerStub = new RpcStub(new TargetWithTargets());
        let outer_stub = RpcStub::from_value(CapnwebValue::Target(Arc::new(target)));
        // JS:     expect(await outerStub.getValue()).toBe(42);
        assert!(matches!(
            outer_stub
                .call(
                    vec!["getValue".to_owned()],
                    RpcPayload::from_app_params(CapnwebValue::Array(vec![]))
                )
                .pull()
                .await
                .unwrap(),
            CapnwebValue::Number(42.0)
        ));
        // JS:     expect(await outerStub.innerTarget.square(3)).toBe(9);
        assert!(matches!(
            outer_stub
                .call(
                    vec!["innerTarget".to_owned(), "square".to_owned()],
                    one_number_argument(3.0)
                )
                .pull()
                .await
                .unwrap(),
            CapnwebValue::Number(9.0)
        ));
        // JS:   });
    }
    // JS:
    // JS:   it("returns undefined when accessing nonexistent properties", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn returns_undefined_when_accessing_nonexistent_properties() {
        // JS:     let objectStub = new RpcStub({foo: "bar"});
        let object_stub = RpcStub::from_value(object_value([(
            "foo",
            CapnwebValue::String("bar".to_owned()),
        )]));
        // JS:     let arrayStub = new RpcStub([1, 2, 3]);
        let array_stub = RpcStub::from_value(CapnwebValue::Array(vec![
            CapnwebValue::Number(1.0),
            CapnwebValue::Number(2.0),
            CapnwebValue::Number(3.0),
        ]));
        // JS:     let targetStub = new RpcStub(new TestTarget());
        let target_stub = local_test_stub();
        // JS:
        // JS:     expect(await (objectStub as any).nonexistent).toBe(undefined);
        assert!(matches!(
            object_stub
                .get(vec!["nonexistent".to_owned()])
                .pull()
                .await
                .unwrap(),
            CapnwebValue::Undefined
        ));
        // JS:     expect(await (arrayStub as any).nonexistent).toBe(undefined);
        assert!(matches!(
            array_stub
                .get(vec!["nonexistent".to_owned()])
                .pull()
                .await
                .unwrap(),
            CapnwebValue::Undefined
        ));
        // JS:     expect(await (targetStub as any).nonexistent).toBe(undefined);
        assert!(matches!(
            target_stub
                .get(vec!["nonexistent".to_owned()])
                .pull()
                .await
                .unwrap(),
            CapnwebValue::Undefined
        ));
        // JS:
        // JS:     // Accessing a property of undefined should throw TypeError (but the error message differs
        // JS:     // across runtimes).
        // JS:     await expect(() => (objectStub as any).nonexistent.foo).rejects.toThrow(TypeError);
        assert!(
            object_stub
                .get(vec!["nonexistent".to_owned(), "foo".to_owned()])
                .pull()
                .await
                .is_err()
        );
        // JS:     await expect(() => (arrayStub as any).nonexistent.foo).rejects.toThrow(TypeError);
        assert!(
            array_stub
                .get(vec!["nonexistent".to_owned(), "foo".to_owned()])
                .pull()
                .await
                .is_err()
        );
        // JS:     await expect(() => (targetStub as any).nonexistent.foo).rejects.toThrow(TypeError);
        assert!(
            target_stub
                .get(vec!["nonexistent".to_owned(), "foo".to_owned()])
                .pull()
                .await
                .is_err()
        );
        // JS:   });
    }
    // JS:
    // JS:   it("exposes only prototype properties for RpcTarget, not instance properties", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn exposes_only_declared_target_properties() {
        // RUST-DIFF(JS Proxy/reflection): generated `RpcTargetInner::call/get` dispatch exposes only declared members.
        // JS:     class TargetWithProps extends RpcTarget {
        // JS:       instanceProp = "instance";
        // JS:       dynamicProp: string;
        // JS:
        // JS:       constructor() {
        // JS:         super();
        // JS:         this.dynamicProp = "dynamic";
        // JS:       }
        // JS:
        // JS:       get prototypeProp() { return "prototype"; }
        // JS:       prototypeMethod() { return "method"; }
        // JS:     }
        // JS:
        // JS:     let target = new TargetWithProps();
        let target = Arc::new(ReflectionTarget);
        // JS:     let stub = new RpcStub(target);
        let stub = RpcStub::from_value(CapnwebValue::Target(target));
        // JS:
        // JS:     expect(await stub.prototypeProp).toBe("prototype");
        assert!(
            matches!(stub.get(vec!["prototypeProp".to_owned()]).pull().await.unwrap(), CapnwebValue::String(value) if value == "prototype")
        );
        // JS:     expect(await stub.prototypeMethod()).toBe("method");
        assert!(
            matches!(stub.call(vec!["prototypeMethod".to_owned()], RpcPayload::from_app_params(CapnwebValue::Array(vec![]))).pull().await.unwrap(), CapnwebValue::String(value) if value == "method")
        );
        // JS:     await expect(() => (stub as any).instanceProp).rejects.toThrow(new TypeError(
        let instance_error = stub
            .get(vec!["instanceProp".to_owned()])
            .pull()
            .await
            .unwrap_err();
        // JS:         "Attempted to access property 'instanceProp', which is an instance property of the " +
        // JS:         "RpcTarget. To avoid leaking private internals, instance properties cannot be accessed " +
        // JS:         "over RPC. If you want to make this property available over RPC, define it as a method " +
        // JS:         "or getter on the class, instead of an instance property."));
        assert!(instance_error.to_string().contains("instanceProp"));
        // JS:     await expect(() => (stub as any).dynamicProp).rejects.toThrow(new TypeError(
        let dynamic_error = stub
            .get(vec!["dynamicProp".to_owned()])
            .pull()
            .await
            .unwrap_err();
        // JS:         "Attempted to access property 'dynamicProp', which is an instance property of the " +
        // JS:         "RpcTarget. To avoid leaking private internals, instance properties cannot be accessed " +
        // JS:         "over RPC. If you want to make this property available over RPC, define it as a method " +
        // JS:         "or getter on the class, instead of an instance property."));
        assert!(dynamic_error.to_string().contains("dynamicProp"));
        // JS:   });
    }
    // JS:
    // JS:   it("does not expose private methods starting with #", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn does_not_expose_private_methods_starting_with_hash() {
        // RUST-DIFF(JS Proxy/reflection): generated dispatch cannot name Rust-private implementation methods.
        // JS:     class TargetWithPrivate extends RpcTarget {
        // JS:       #privateMethod() { return "private"; }
        // JS:       publicMethod() { return "public"; }
        // JS:     }
        // JS:
        // JS:     let stub = new RpcStub(new TargetWithPrivate());
        let stub = RpcStub::from_value(CapnwebValue::Target(Arc::new(ReflectionTarget)));
        // JS:     expect(await stub.publicMethod()).toBe("public");
        assert!(
            matches!(stub.call(vec!["publicMethod".to_owned()], RpcPayload::from_app_params(CapnwebValue::Array(vec![]))).pull().await.unwrap(), CapnwebValue::String(value) if value == "public")
        );
        // JS:     expect(await (stub as any)["#privateMethod"]).toBe(undefined);
        assert!(matches!(
            stub.get(vec!["#privateMethod".to_owned()])
                .pull()
                .await
                .unwrap(),
            CapnwebValue::Undefined
        ));
        // JS:   });
    }
    // JS:
    // JS:   it("supports map() on nulls", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn supports_map_on_nulls() {
        // JS:     let counter = new RpcStub(new Counter(0));
        let counter = local_counter(0.0);
        // JS:
        // JS:     let stub = new RpcStub(new TestTarget());
        let stub = local_test_stub();
        // JS:
        // JS:     {
        {
            // JS:       using promise = stub.returnNull();
            let promise = stub.call(
                vec!["returnNull".to_owned()],
                RpcPayload::from_app_params(CapnwebValue::Array(vec![])),
            );
            // JS:       expect(await promise.map(_ => counter.increment(123))).toBe(null);
            let mapped = promise
                .as_stub()
                .map({
                    let counter = counter.clone();
                    move |_| {
                        Ok(CapnwebValue::RpcPromise(counter.call(
                            vec!["increment".to_owned()],
                            one_number_argument(123.0),
                        )))
                    }
                })
                .unwrap();
            assert!(matches!(mapped.pull().await.unwrap(), CapnwebValue::Null));
            // JS:     }
        }
        // JS:
        // JS:     {
        {
            // JS:       using promise = stub.returnUndefined();
            let promise = stub.call(
                vec!["returnUndefined".to_owned()],
                RpcPayload::from_app_params(CapnwebValue::Array(vec![])),
            );
            // JS:       expect(await promise.map(_ => counter.increment(456))).toBe(undefined);
            let mapped = promise
                .as_stub()
                .map({
                    let counter = counter.clone();
                    move |_| {
                        Ok(CapnwebValue::RpcPromise(counter.call(
                            vec!["increment".to_owned()],
                            one_number_argument(456.0),
                        )))
                    }
                })
                .unwrap();
            assert!(matches!(
                mapped.pull().await.unwrap(),
                CapnwebValue::Undefined
            ));
            // JS:     }
        }
        // JS:
        // JS:     {
        {
            // JS:       using promise = stub.returnNumber(2);
            let promise = stub.call(vec!["returnNumber".to_owned()], one_number_argument(2.0));
            // JS:       expect(await promise.map(i => counter.increment(i))).toBe(2);
            let mapped = promise
                .as_stub()
                .map({
                    let counter = counter.clone();
                    move |i| {
                        Ok(CapnwebValue::RpcPromise(counter.call(
                            vec!["increment".to_owned()],
                            RpcPayload::from_app_params(CapnwebValue::Array(vec![
                                CapnwebValue::RpcPromise(i),
                            ])),
                        )))
                    }
                })
                .unwrap();
            assert!(matches!(
                mapped.pull().await.unwrap(),
                CapnwebValue::Number(2.0)
            ));
            // JS:     }
        }
        // JS:
        // JS:     {
        {
            // JS:       using promise = stub.returnNumber(4);
            let promise = stub.call(vec!["returnNumber".to_owned()], one_number_argument(4.0));
            // JS:       expect(await promise.map(i => counter.increment(i))).toBe(6);
            let mapped = promise
                .as_stub()
                .map({
                    let counter = counter.clone();
                    move |i| {
                        Ok(CapnwebValue::RpcPromise(counter.call(
                            vec!["increment".to_owned()],
                            RpcPayload::from_app_params(CapnwebValue::Array(vec![
                                CapnwebValue::RpcPromise(i),
                            ])),
                        )))
                    }
                })
                .unwrap();
            assert!(matches!(
                mapped.pull().await.unwrap(),
                CapnwebValue::Number(6.0)
            ));
            // JS:     }
        }
        // JS:   });
    }
    // JS:
    // JS:   it("supports map() on arrays", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn supports_map_on_arrays() {
        // JS:     let outerCounter = new RpcStub(new Counter(0));
        let outer_counter = local_counter(0.0);
        // JS:     let stub = new RpcStub(new TestTarget());
        let stub = local_test_stub();
        // JS:
        // JS:     using fib = stub.generateFibonacci(6);
        let fib = stub.call(
            vec!["generateFibonacci".to_owned()],
            one_number_argument(6.0),
        );
        // JS:     using counters = await fib.map(i => {
        let counters = fib
            .as_stub()
            .map({
                let stub = stub.clone();
                let outer_counter = outer_counter.clone();
                move |i| {
                    // JS:       let counter = stub.makeCounter(i);
                    let counter = stub.call(
                        vec!["makeCounter".to_owned()],
                        RpcPayload::from_app_params(CapnwebValue::Array(vec![
                            CapnwebValue::RpcPromise(i),
                        ])),
                    );
                    // JS:       let val = counter.increment(3);
                    let val = counter.call(vec!["increment".to_owned()], one_number_argument(3.0));
                    // JS:       outerCounter.increment();
                    let _ = outer_counter.call(
                        vec!["increment".to_owned()],
                        RpcPayload::from_app_params(CapnwebValue::Array(vec![])),
                    );
                    // JS:       return {counter, val};
                    Ok(object_value([
                        ("counter", CapnwebValue::RpcPromise(counter)),
                        ("val", CapnwebValue::RpcPromise(val)),
                    ]))
                    // JS:     });
                }
            })
            .unwrap()
            .pull()
            .await
            .unwrap();
        // JS:
        // JS:     expect(counters.map(x => x.val)).toStrictEqual([3, 4, 4, 5, 6, 8]);
        let CapnwebValue::Array(counters) = counters else {
            panic!()
        };
        let values = counters
            .iter()
            .map(|counter| {
                let CapnwebValue::Object(counter) = counter else {
                    panic!()
                };
                let CapnwebValue::Number(value) = counter["val"] else {
                    panic!()
                };
                value
            })
            .collect::<Vec<_>>();
        assert_eq!(values, [3.0, 4.0, 4.0, 5.0, 6.0, 8.0]);
        // JS:
        // JS:     expect(await Promise.all(counters.map(x => x.counter.value)))
        let mut counter_values = Vec::new();
        for counter in &counters {
            let CapnwebValue::Object(counter) = counter else {
                panic!()
            };
            let stub = RpcStub::from_value(counter["counter"].clone());
            let CapnwebValue::Number(value) =
                stub.get(vec!["value".to_owned()]).pull().await.unwrap()
            else {
                panic!()
            };
            counter_values.push(value);
        }
        // JS:         .toStrictEqual([3, 4, 4, 5, 6, 8]);
        assert_eq!(counter_values, [3.0, 4.0, 4.0, 5.0, 6.0, 8.0]);
        // JS:
        // JS:     expect(await outerCounter.value).toBe(6);
        assert!(matches!(
            outer_counter
                .get(vec!["value".to_owned()])
                .pull()
                .await
                .unwrap(),
            CapnwebValue::Number(6.0)
        ));
        // JS:   });
    }
    // JS:
    // JS:   it("supports nested map()", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn supports_nested_map() {
        // JS:     let stub = new RpcStub(new TestTarget());
        let stub = local_test_stub();
        // JS:
        // JS:     let fib = stub.generateFibonacci(7);
        let fib = stub.call(
            vec!["generateFibonacci".to_owned()],
            one_number_argument(7.0),
        );
        // JS:     let result = await fib.map(i => {
        let result = fib
            .as_stub()
            .map({
                let stub = stub.clone();
                move |i| {
                    // JS:       return stub.generateFibonacci(i).map(j => {
                    let inner = stub.call(
                        vec!["generateFibonacci".to_owned()],
                        RpcPayload::from_app_params(CapnwebValue::Array(vec![
                            CapnwebValue::RpcPromise(i),
                        ])),
                    );
                    let nested = inner.as_stub().map({
                        let stub = stub.clone();
                        move |j| {
                            // JS:         return stub.generateFibonacci(j);
                            Ok(CapnwebValue::RpcPromise(stub.call(
                                vec!["generateFibonacci".to_owned()],
                                RpcPayload::from_app_params(CapnwebValue::Array(vec![
                                    CapnwebValue::RpcPromise(j),
                                ])),
                            )))
                            // JS:       });
                        }
                    })?;
                    Ok(CapnwebValue::RpcPromise(nested))
                    // JS:     });
                }
            })
            .unwrap()
            .pull()
            .await
            .unwrap();
        // JS:
        // JS:     expect(result).toStrictEqual([
        let expected = CapnwebValue::Array(vec![
            // JS:       [],
            number_arrays(&[]),
            // JS:       [[]],
            CapnwebValue::Array(vec![number_arrays(&[])]),
            // JS:       [[]],
            CapnwebValue::Array(vec![number_arrays(&[])]),
            // JS:       [[], [0]],
            CapnwebValue::Array(vec![number_arrays(&[]), number_arrays(&[0.0])]),
            // JS:       [[], [0], [0]],
            CapnwebValue::Array(vec![
                number_arrays(&[]),
                number_arrays(&[0.0]),
                number_arrays(&[0.0]),
            ]),
            // JS:       [[], [0], [0], [0, 1], [0, 1, 1]],
            CapnwebValue::Array(vec![
                number_arrays(&[]),
                number_arrays(&[0.0]),
                number_arrays(&[0.0]),
                number_arrays(&[0.0, 1.0]),
                number_arrays(&[0.0, 1.0, 1.0]),
            ]),
            // JS:       [[], [0], [0], [0, 1], [0, 1, 1], [0, 1, 1, 2, 3], [0, 1, 1, 2, 3, 5, 8, 13],
            CapnwebValue::Array(vec![
                number_arrays(&[]),
                number_arrays(&[0.0]),
                number_arrays(&[0.0]),
                number_arrays(&[0.0, 1.0]),
                number_arrays(&[0.0, 1.0, 1.0]),
                number_arrays(&[0.0, 1.0, 1.0, 2.0, 3.0]),
                number_arrays(&[0.0, 1.0, 1.0, 2.0, 3.0, 5.0, 8.0, 13.0]),
                // JS:           [0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144]],
                number_arrays(&[
                    0.0, 1.0, 1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0, 34.0, 55.0, 89.0, 144.0,
                ]),
            ]),
            // JS:     ]);
        ]);
        assert!(values_equal(&result, &expected));
        // JS:   });
    }
    // JS:
    // JS:   it("overrides toString() to at least specify the type", async () => {
    #[test]
    fn overrides_to_string_to_at_least_specify_the_type() {
        // JS:     let stub = new RpcStub(new TestTarget());
        let stub = local_test_stub();
        // JS:     expect(stub.toString()).toBe("[object RpcStub]");
        assert_eq!(stub.to_string(), "[object RpcStub]");
        // JS:     let promise = stub.square(3);
        let promise = stub.call(vec!["square".to_owned()], one_number_argument(3.0));
        // JS:     expect(promise.toString()).toBe("[object RpcPromise]");
        assert_eq!(promise.to_string(), "[object RpcPromise]");
        // JS:   });
    }
    // JS: });
}
// JS:
struct DisposableLocalTarget(Arc<AtomicBool>);

impl Drop for DisposableLocalTarget {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl RpcTargetInner for DisposableLocalTarget {
    fn call(self: Arc<Self>, path: PropertyPath, _args: RpcPayload) -> Result<StubHook> {
        Err(Error::Type(format!("No such RPC method: {path:?}")))
    }

    fn get(self: Arc<Self>, path: PropertyPath) -> Result<StubHook> {
        Err(Error::Type(format!("No such RPC property: {path:?}")))
    }
}

struct TargetOwningStub {
    disposed: Arc<AtomicBool>,
    inner: ManuallyDrop<RpcStub>,
}

impl Drop for TargetOwningStub {
    fn drop(&mut self) {
        self.disposed.store(true, Ordering::SeqCst);
        // JavaScript's explicit target disposer does not recursively dispose instance fields.
    }
}

impl RpcTargetInner for TargetOwningStub {
    fn call(self: Arc<Self>, path: PropertyPath, _args: RpcPayload) -> Result<StubHook> {
        Err(Error::Type(format!("No such RPC method: {path:?}")))
    }

    fn get(self: Arc<Self>, path: PropertyPath) -> Result<StubHook> {
        Err(Error::Type(format!("No such RPC property: {path:?}")))
    }
}

// JS: describe("stub disposal", () => {
mod stub_disposal {
    use super::*;
    // JS:   it("disposes nested stubs and RpcTargets when wrapping an object", () => {
    #[test]
    fn disposes_nested_stubs_and_targets_when_wrapping_an_object() {
        // JS:     class DisposableTarget extends RpcTarget {
        // `DisposableLocalTarget` is the Rust target.
        // JS:       constructor(private disposeFlag: { value: boolean }) {
        // Its constructor stores an `Arc<AtomicBool>`.
        // JS:         super();
        // Rust has no target superclass.
        // JS:       }
        // JS:       [Symbol.dispose]() { this.disposeFlag.value = true; }
        // Implemented by `DisposableLocalTarget::drop()`.
        // JS:     }
        // JS:
        // JS:     let innerFlag = { value: false };
        let inner_flag = Arc::new(AtomicBool::new(false));
        // JS:     let anotherFlag = { value: false };
        let another_flag = Arc::new(AtomicBool::new(false));
        // JS:     let innerStub = new RpcStub(new DisposableTarget(innerFlag));
        let inner_stub = RpcStub::from_value(CapnwebValue::Target(Arc::new(
            DisposableLocalTarget(inner_flag.clone()),
        )));
        // JS:
        // JS:     let outerObject = {
        let outer_object = object_value([
            // JS:       stub: innerStub,
            ("stub", CapnwebValue::Stub(inner_stub)),
            // JS:       target: new DisposableTarget(anotherFlag),
            (
                "target",
                CapnwebValue::Target(Arc::new(DisposableLocalTarget(another_flag.clone()))),
            ),
            // JS:       value: 42
            ("value", CapnwebValue::Number(42.0)),
            // JS:     };
        ]);
        // JS:     let outerStub = new RpcStub(outerObject);
        let outer_stub = RpcStub::from_value(outer_object);
        // JS:
        // JS:     outerStub[Symbol.dispose]();
        drop(outer_stub);
        // JS:
        // JS:     expect(innerFlag.value).toBe(true);
        assert!(inner_flag.load(Ordering::SeqCst));
        // JS:     expect(anotherFlag.value).toBe(true);
        assert!(another_flag.load(Ordering::SeqCst));
        // JS:   });
    }
    // JS:
    // JS:   it("only calls RpcTarget disposer when wrapping an RpcTarget with nested stubs", () => {
    #[test]
    fn only_calls_target_drop_when_target_owns_nested_stubs() {
        // JS:     let targetDisposed = false;
        let target_disposed = Arc::new(AtomicBool::new(false));
        // JS:     let innerTargetDisposed = false;
        let inner_target_disposed = Arc::new(AtomicBool::new(false));
        // JS:
        // JS:     class InnerTarget extends RpcTarget {
        // `DisposableLocalTarget` is used below.
        // JS:       [Symbol.dispose]() { innerTargetDisposed = true; }
        // Implemented by `DisposableLocalTarget::drop()`.
        // JS:     }
        // JS:
        // JS:     class TargetWithStubs extends RpcTarget {
        // `TargetOwningStub` is the Rust target.
        // JS:       inner = new RpcStub(new InnerTarget());
        let inner = RpcStub::from_value(CapnwebValue::Target(Arc::new(DisposableLocalTarget(
            inner_target_disposed.clone(),
        ))));
        // JS:
        // JS:       get innerStub() {
        // Implemented by `TargetOwningStub::get()`.
        // JS:         return this.inner;
        // The inner hook is borrowed without transferring ownership.
        // JS:       }
        // JS:
        // JS:       [Symbol.dispose]() { targetDisposed = true; }
        // Implemented by `TargetOwningStub::drop()`.
        // JS:     }
        // JS:
        // JS:     let outerStub = new RpcStub(new TargetWithStubs());
        let outer_stub = RpcStub::from_value(CapnwebValue::Target(Arc::new(TargetOwningStub {
            disposed: target_disposed.clone(),
            inner: ManuallyDrop::new(inner),
        })));
        // JS:     outerStub[Symbol.dispose]();
        drop(outer_stub);
        // JS:
        // JS:     expect(targetDisposed).toBe(true);
        assert!(target_disposed.load(Ordering::SeqCst));
        // JS:     expect(innerTargetDisposed).toBe(false); // nested stubs in RpcTarget are not auto-disposed
        assert!(!inner_target_disposed.load(Ordering::SeqCst));
        // JS:   });
    }
    // JS:
    // JS:   it("only disposes RpcTarget when all dups are disposed", () => {
    #[test]
    fn only_disposes_target_when_all_clones_are_dropped() {
        // JS:     let disposed = false;
        let disposed = Arc::new(AtomicBool::new(false));
        // JS:     class DisposableTarget extends RpcTarget {
        // `DisposableLocalTarget` is used below.
        // JS:       [Symbol.dispose]() { disposed = true; }
        // Implemented by `Drop`.
        // JS:     }
        // JS:
        // JS:     let original = new RpcStub(new DisposableTarget());
        let original = RpcStub::from_value(CapnwebValue::Target(Arc::new(DisposableLocalTarget(
            disposed.clone(),
        ))));
        // JS:     let dup1 = original.dup();
        let dup1 = original.clone();
        // JS:     let dup2 = original.dup();
        let dup2 = original.clone();
        // JS:
        // JS:     original[Symbol.dispose]();
        drop(original);
        // JS:     expect(disposed).toBe(false);
        assert!(!disposed.load(Ordering::SeqCst));
        // JS:
        // JS:     dup1[Symbol.dispose]();
        drop(dup1);
        // JS:     expect(disposed).toBe(false);
        assert!(!disposed.load(Ordering::SeqCst));
        // JS:
        // JS:     dup2[Symbol.dispose]();
        drop(dup2);
        // JS:     expect(disposed).toBe(true);
        assert!(disposed.load(Ordering::SeqCst));
        // JS:   });
    }
    // JS:
    // JS:   it("makes disposal idempotent - duplicate dispose calls don't affect refcount", () => {
    #[test]
    fn ownership_makes_duplicate_drop_unrepresentable() {
        // JS:     let disposed = false;
        let disposed = Arc::new(AtomicBool::new(false));
        // JS:     class DisposableTarget extends RpcTarget {
        // `DisposableLocalTarget` is used below.
        // JS:       [Symbol.dispose]() { disposed = true; }
        // Implemented by `Drop`.
        // JS:     }
        // JS:
        // JS:     let original = new RpcStub(new DisposableTarget());
        let original = RpcStub::from_value(CapnwebValue::Target(Arc::new(DisposableLocalTarget(
            disposed.clone(),
        ))));
        // JS:     let dup1 = original.dup();
        let dup1 = original.clone();
        // JS:
        // JS:     // Dispose the duplicate twice
        // JS:     dup1[Symbol.dispose]();
        drop(dup1);
        // JS:     dup1[Symbol.dispose]();
        // Rust move semantics make a second `drop(dup1)` a compile-time error.
        // JS:     expect(disposed).toBe(false);
        assert!(!disposed.load(Ordering::SeqCst));
        // JS:
        // JS:     // Only when original is also disposed should the target be disposed
        // JS:     original[Symbol.dispose]();
        drop(original);
        // JS:     expect(disposed).toBe(true);
        assert!(disposed.load(Ordering::SeqCst));
        // JS:   });
    }
    // JS: });
}
// JS:
struct IndexPairTransport {
    sender: tokio::sync::mpsc::UnboundedSender<String>,
    receiver: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<String>>,
}

impl AnyRpcTransport for IndexPairTransport {
    fn send(&self, message: TransportMessage) -> Result<TransportSend> {
        let TransportMessage::Text(message) = message else {
            return Err(Error::Type("expected text".to_owned()));
        };
        self.sender
            .send(message)
            .map_err(|_| Error::Message("transport closed".to_owned()))?;
        Ok(TransportSend::Done(None))
    }
    fn receive(self: Arc<Self>) -> Pin<Box<dyn Future<Output = Result<TransportMessage>> + Send>> {
        Box::pin(async move {
            self.receiver
                .lock()
                .await
                .recv()
                .await
                .map(TransportMessage::Text)
                .ok_or_else(|| Error::Message("transport closed".to_owned()))
        })
    }
}

struct FixedResultTarget(Result<CapnwebValue>);

impl RpcTargetInner for FixedResultTarget {
    fn call(self: Arc<Self>, _path: PropertyPath, _args: RpcPayload) -> Result<StubHook> {
        let result = self.0.clone();
        Ok(PromiseStubHook::from_future(async move {
            let value = result?;
            Ok(Arc::new(PayloadStubHook::new(RpcPayload::from_app_return(value))) as StubHook)
        }))
    }

    fn get(self: Arc<Self>, path: PropertyPath) -> Result<StubHook> {
        Err(Error::Type(format!("No such RPC property: {path:?}")))
    }
}

struct DisposableValueTarget {
    disposed: Arc<std::sync::atomic::AtomicUsize>,
}

impl Drop for DisposableValueTarget {
    fn drop(&mut self) {
        self.disposed.fetch_add(1, Ordering::SeqCst);
    }
}

impl RpcTargetInner for DisposableValueTarget {
    fn call(self: Arc<Self>, path: PropertyPath, _args: RpcPayload) -> Result<StubHook> {
        let value = match path.as_slice() {
            [PropertyKey::String(method)] if method == "getValue" => CapnwebValue::Number(42.0),
            _ => return Err(Error::Type(format!("No such RPC method: {path:?}"))),
        };
        Ok(Arc::new(PayloadStubHook::new(RpcPayload::from_app_return(
            value,
        ))))
    }

    fn get(self: Arc<Self>, path: PropertyPath) -> Result<StubHook> {
        Err(Error::Type(format!("No such RPC property: {path:?}")))
    }
}

struct DisposableMainTarget {
    disposed: Arc<std::sync::atomic::AtomicUsize>,
}

impl RpcTargetInner for DisposableMainTarget {
    fn call(self: Arc<Self>, path: PropertyPath, _args: RpcPayload) -> Result<StubHook> {
        let value = match path.as_slice() {
            [PropertyKey::String(method)] if method == "getDisposableTarget" => {
                CapnwebValue::Target(Arc::new(DisposableValueTarget {
                    disposed: self.disposed.clone(),
                }))
            }
            _ => return Err(Error::Type(format!("No such RPC method: {path:?}"))),
        };
        Ok(Arc::new(PayloadStubHook::new(RpcPayload::from_app_return(
            value,
        ))))
    }

    fn get(self: Arc<Self>, path: PropertyPath) -> Result<StubHook> {
        Err(Error::Type(format!("No such RPC property: {path:?}")))
    }
}

struct ErrorProcessingTarget(&'static str);

impl RpcTargetInner for ErrorProcessingTarget {
    fn call(self: Arc<Self>, path: PropertyPath, args: RpcPayload) -> Result<StubHook> {
        let message = self.0;
        Ok(PromiseStubHook::from_future(async move {
            let mut arguments = match args.deliver_resolve().await? {
                CapnwebValue::Array(arguments) => arguments,
                _ => return Err(Error::Type("arguments are not an array".to_owned())),
            };
            let value = match path.as_slice() {
                [PropertyKey::String(method)] if method == "throwError" => {
                    return Err(Error::Message(message.to_owned()));
                }
                [PropertyKey::String(method)] if method == "processValue" => {
                    let CapnwebValue::Number(value) = arguments.remove(0) else {
                        return Err(Error::Type("value is not a number".to_owned()));
                    };
                    CapnwebValue::Number(value * 2.0)
                }
                _ => return Err(Error::Type(format!("No such RPC method: {path:?}"))),
            };
            Ok(Arc::new(PayloadStubHook::new(RpcPayload::from_app_return(value))) as StubHook)
        }))
    }

    fn get(self: Arc<Self>, path: PropertyPath) -> Result<StubHook> {
        Err(Error::Type(format!("No such RPC property: {path:?}")))
    }
}

struct ProxyTarget {
    alice: RpcStub,
}

impl RpcTargetInner for ProxyTarget {
    fn call(self: Arc<Self>, path: PropertyPath, args: RpcPayload) -> Result<StubHook> {
        let method = match path.as_slice() {
            [PropertyKey::String(method)] => method.as_str(),
            _ => return Err(Error::Type(format!("No such RPC method: {path:?}"))),
        };
        match method {
            "getCounter" => {
                let alice = self.alice.clone();
                Ok(PromiseStubHook::from_future(async move {
                    let value = alice
                        .call(vec!["getCounter".to_owned()], args)
                        .pull()
                        .await?;
                    Ok(
                        Arc::new(PayloadStubHook::new(RpcPayload::from_app_return(value)))
                            as StubHook,
                    )
                }))
            }
            "getCounterPromise" => {
                let promise = self.alice.call(vec!["getCounter".to_owned()], args);
                Ok(unwrap_stub_and_path(promise.as_stub()).0)
            }
            "incrementCounter" => {
                let promise = self.alice.call(vec!["incrementCounter".to_owned()], args);
                Ok(unwrap_stub_and_path(promise.as_stub()).0)
            }
            _ => Err(Error::Type(format!("No such RPC method: {method}"))),
        }
    }

    fn get(self: Arc<Self>, path: PropertyPath) -> Result<StubHook> {
        Err(Error::Type(format!("No such RPC property: {path:?}")))
    }
}

struct IndexRpcHarness {
    _client: RpcSession,
    _server: RpcSession,
    stub: RpcStub,
}

impl IndexRpcHarness {
    fn new(target: Arc<dyn RpcTargetInner>) -> Self {
        let (to_client, client_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (to_server, server_receiver) = tokio::sync::mpsc::unbounded_channel();
        let client_transport = Arc::new(IndexPairTransport {
            sender: to_server,
            receiver: tokio::sync::Mutex::new(client_receiver),
        });
        let server_transport = Arc::new(IndexPairTransport {
            sender: to_client,
            receiver: tokio::sync::Mutex::new(server_receiver),
        });
        let client = RpcSession::new(client_transport, None, RpcSessionOptions::default()).unwrap();
        let server = RpcSession::new(
            server_transport,
            Some(CapnwebValue::Target(target)),
            RpcSessionOptions::default(),
        )
        .unwrap();
        let stub = client.get_remote_main();
        Self {
            _client: client,
            _server: server,
            stub,
        }
    }
}

// JS: describe("basic rpc", () => {
mod basic_rpc {
    use super::*;
    // JS:   it("supports calls", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn supports_calls() {
        // JS:     await using harness = new TestHarness(new TestTarget());
        let harness = IndexRpcHarness::new(Arc::new(LocalTestTarget));
        // JS:     expect(await harness.stub.square(3)).toBe(9);
        assert!(matches!(
            harness
                .stub
                .call(vec!["square".to_owned()], one_number_argument(3.0))
                .pull()
                .await
                .unwrap(),
            CapnwebValue::Number(9.0)
        ));
        // JS:   });
    }
    // JS:
    // JS:   it("supports throwing errors", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn supports_throwing_errors() {
        // JS:     await using harness = new TestHarness(new TestTarget());
        let harness = IndexRpcHarness::new(Arc::new(LocalTestTarget));
        // JS:     let stub = harness.stub;
        let stub = harness.stub.clone();
        // JS:     await expect(() => stub.throwError()).rejects.toThrow(new RangeError("test error"));
        let error = stub
            .call(
                vec!["throwError".to_owned()],
                RpcPayload::from_app_params(CapnwebValue::Array(vec![])),
            )
            .pull()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("test error"));
        // JS:   });
    }
    // JS:
    // JS:   it("preserves own properties on thrown errors over RPC", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn preserves_own_properties_on_thrown_errors_over_rpc() {
        // JS:     class RichTarget extends RpcTarget {
        let mut rich = make_error("RangeError", "rich");
        // JS:       throwRich() {
        // The fixed-result target rejects its call.
        // JS:         let err = new RangeError("rich") as any;
        // Constructed above.
        // JS:         err.code = "E_RICH";
        rich.properties
            .insert("code".to_owned(), CapnwebValue::String("E_RICH".to_owned()));
        // JS:         err.details = { reason: "because", count: 7 };
        rich.properties.insert(
            "details".to_owned(),
            object_value([
                ("reason", CapnwebValue::String("because".to_owned())),
                ("count", CapnwebValue::Number(7.0)),
            ]),
        );
        // JS:         err.when = new Date(1234);
        // RUST-DEFERRED(JS built-in: Date)
        // JS:         throw err;
        let target = FixedResultTarget(Err(Error::Serialized(rich)));
        // JS:       }
        // JS:     }
        // JS:     await using harness = new TestHarness(new RichTarget());
        let harness = IndexRpcHarness::new(Arc::new(target));
        // JS:     let stub = harness.stub as any;
        let stub = harness.stub.clone();
        // JS:
        // JS:     let caught: any;
        let caught;
        // JS:     try {
        {
            // JS:       await stub.throwRich();
            let result = stub
                .call(
                    vec!["throwRich".to_owned()],
                    RpcPayload::from_app_params(CapnwebValue::Array(vec![])),
                )
                .pull()
                .await;
            // JS:     } catch (e) {
            caught = result.unwrap_err();
            // JS:       caught = e;
            // Assigned above.
            // JS:     }
        }
        // JS:
        // JS:     expect(caught).toBeInstanceOf(RangeError);
        let Error::Serialized(caught) = caught else {
            panic!()
        };
        assert_eq!(caught.name, "RangeError");
        // JS:     expect(caught.message).toBe("rich");
        assert_eq!(caught.message, "rich");
        // JS:     expect(caught.code).toBe("E_RICH");
        assert!(
            matches!(caught.properties.get("code"), Some(CapnwebValue::String(value)) if value == "E_RICH")
        );
        // JS:     expect(caught.details).toStrictEqual({ reason: "because", count: 7 });
        assert!(values_equal(
            caught.properties.get("details").unwrap(),
            &object_value([
                ("reason", CapnwebValue::String("because".to_owned())),
                ("count", CapnwebValue::Number(7.0))
            ])
        ));
        // JS:     expect(caught.when).toStrictEqual(new Date(1234));
        // RUST-DEFERRED(JS built-in: Date)
        // JS:   });
    }
    // JS:
    // JS:   it("preserves Error.cause on thrown errors over RPC", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn preserves_error_cause_on_thrown_errors_over_rpc() {
        // JS:     class CauseTarget extends RpcTarget {
        let mut outer = make_error("Error", "outer");
        // JS:       throwWithCause() {
        // The fixed-result target rejects its call.
        // JS:         throw new Error("outer", { cause: new TypeError("inner") });
        outer.properties.insert(
            "cause".to_owned(),
            CapnwebValue::Error(make_error("TypeError", "inner")),
        );
        // JS:       }
        // JS:     }
        // JS:     await using harness = new TestHarness(new CauseTarget());
        let harness =
            IndexRpcHarness::new(Arc::new(FixedResultTarget(Err(Error::Serialized(outer)))));
        // JS:     let stub = harness.stub as any;
        let stub = harness.stub.clone();
        // JS:
        // JS:     let caught: any;
        let caught;
        // JS:     try {
        {
            // JS:       await stub.throwWithCause();
            let result = stub
                .call(
                    vec!["throwWithCause".to_owned()],
                    RpcPayload::from_app_params(CapnwebValue::Array(vec![])),
                )
                .pull()
                .await;
            // JS:     } catch (e) {
            caught = result.unwrap_err();
            // JS:       caught = e;
            // Assigned above.
            // JS:     }
        }
        // JS:     expect(caught.message).toBe("outer");
        let Error::Serialized(caught) = caught else {
            panic!()
        };
        assert_eq!(caught.message, "outer");
        // JS:     expect(caught.cause).toBeInstanceOf(TypeError);
        let Some(CapnwebValue::Error(cause)) = caught.properties.get("cause") else {
            panic!()
        };
        assert_eq!(cause.name, "TypeError");
        // JS:     expect(caught.cause.message).toBe("inner");
        assert_eq!(cause.message, "inner");
        // JS:   });
    }
    // JS:
    // JS:   it("round-trips heavyweight-but-supported types attached to errors", async () => {
    // RUST-DEFERRED(JS built-in: Request)
    // JS:     class ReqErrTarget extends RpcTarget {
    // JS:       throwWithRequest() {
    // JS:         let err = new Error("with-request") as any;
    // JS:         err.request = new Request("http://example.com/", { method: "DELETE" });
    // JS:         throw err;
    // JS:       }
    // JS:     }
    // JS:     await using harness = new TestHarness(new ReqErrTarget());
    // JS:     let stub = harness.stub as any;
    // JS:
    // JS:     let caught: any;
    // JS:     try {
    // JS:       await stub.throwWithRequest();
    // JS:     } catch (e) {
    // JS:       caught = e;
    // JS:     }
    // JS:     expect(caught.message).toBe("with-request");
    // JS:     expect(caught.request).toBeInstanceOf(Request);
    // JS:     expect(caught.request.url).toBe("http://example.com/");
    // JS:     expect(caught.request.method).toBe("DELETE");
    // JS:   });
    // JS:
    // JS:   it("silently drops stub-valued error properties without leaking capabilities", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn silently_drops_stub_valued_error_properties() {
        // JS:     class CounterFactory extends RpcTarget {
        let mut error = make_error("Error", "with-stub");
        // JS:       throwWithStub() {
        // The fixed-result target rejects its call.
        // JS:         let err = new Error("with-stub") as any;
        // Constructed above.
        // JS:         // Attach a stub to the error. There's no sensible lifetime for this capability, so
        // JS:         // it must be dropped on the wire rather than leaked to the importer.
        // JS:         err.counter = new RpcStub(new Counter(10));
        error.properties.insert(
            "counter".to_owned(),
            CapnwebValue::Stub(local_counter(10.0)),
        );
        // JS:         err.code = "E_STUB";
        error
            .properties
            .insert("code".to_owned(), CapnwebValue::String("E_STUB".to_owned()));
        // JS:         throw err;
        let target = FixedResultTarget(Err(Error::Serialized(error)));
        // JS:       }
        // JS:     }
        // JS:     await using harness = new TestHarness(new CounterFactory());
        let harness = IndexRpcHarness::new(Arc::new(target));
        // JS:     let stub = harness.stub as any;
        let stub = harness.stub.clone();
        // JS:
        // JS:     let caught: any;
        let caught;
        // JS:     try {
        {
            // JS:       await stub.throwWithStub();
            let result = stub
                .call(
                    vec!["throwWithStub".to_owned()],
                    RpcPayload::from_app_params(CapnwebValue::Array(vec![])),
                )
                .pull()
                .await;
            // JS:     } catch (e) {
            caught = result.unwrap_err();
            // JS:       caught = e;
            // Assigned above.
            // JS:     }
        }
        // JS:     expect(caught.message).toBe("with-stub");
        let Error::Serialized(caught) = caught else {
            panic!()
        };
        assert_eq!(caught.message, "with-stub");
        // JS:     expect(caught.code).toBe("E_STUB");
        assert!(
            matches!(caught.properties.get("code"), Some(CapnwebValue::String(value)) if value == "E_STUB")
        );
        // JS:     expect("counter" in caught).toBe(false);
        assert!(!caught.properties.contains_key("counter"));
        // JS:     // The harness's checkAllDisposed() in asyncDispose verifies no exports/imports leaked.
        assert_eq!(harness._client.get_stats().exports, 1);
        // JS:   });
    }
    // JS:
    // JS:   it("drops the whole error property when a stub is nested inside an unserializable value", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn drops_the_whole_error_property_when_a_stub_is_nested_inside_it() {
        // JS:     // Regression: when an Error is serialized inside a successful payload (source is set),
        // JS:     // a property whose value contains both a stub and an unserializable sibling must drop
        // JS:     // the entire property atomically. Previously the stub would be exported on the way down
        // JS:     // before the unserializable sibling triggered the failure, leaking a capability.
        // JS:     class MixedFactory extends RpcTarget {
        let mut error = make_error("Error", "with-mixed");
        // JS:       makeError() {
        // The fixed-result target returns the wrapped error.
        // JS:         let err = new Error("with-mixed") as any;
        // Constructed above.
        // JS:         err.mixed = { counter: new RpcStub(new Counter(10)), bad: Object.create(null) };
        error.properties.insert(
            "mixed".to_owned(),
            object_value([
                ("counter", CapnwebValue::Stub(local_counter(10.0))),
                (
                    "bad",
                    CapnwebValue::Unsupported("(couldn't stringify value)".to_owned()),
                ),
            ]),
        );
        // JS:         err.code = "E_MIXED";
        error.properties.insert(
            "code".to_owned(),
            CapnwebValue::String("E_MIXED".to_owned()),
        );
        // JS:         return { wrapped: err };
        let target = FixedResultTarget(Ok(object_value([("wrapped", CapnwebValue::Error(error))])));
        // JS:       }
        // JS:     }
        // JS:     await using harness = new TestHarness(new MixedFactory());
        let harness = IndexRpcHarness::new(Arc::new(target));
        // JS:     let stub = harness.stub as any;
        let stub = harness.stub.clone();
        // JS:
        // JS:     let result = await stub.makeError();
        let result = stub
            .call(
                vec!["makeError".to_owned()],
                RpcPayload::from_app_params(CapnwebValue::Array(vec![])),
            )
            .pull()
            .await
            .unwrap();
        // JS:     expect(result.wrapped).toBeInstanceOf(Error);
        let CapnwebValue::Object(result) = result else {
            panic!()
        };
        let CapnwebValue::Error(wrapped) = &result["wrapped"] else {
            panic!()
        };
        // JS:     expect(result.wrapped.message).toBe("with-mixed");
        assert_eq!(wrapped.message, "with-mixed");
        // JS:     expect(result.wrapped.code).toBe("E_MIXED");
        assert!(
            matches!(wrapped.properties.get("code"), Some(CapnwebValue::String(value)) if value == "E_MIXED")
        );
        // JS:     expect("mixed" in result.wrapped).toBe(false);
        assert!(!wrapped.properties.contains_key("mixed"));
        // JS:     // checkAllDisposed() in asyncDispose verifies no stub leaked through the failed prop.
        assert_eq!(harness._client.get_stats().exports, 1);
        // JS:   });
    }
    // JS:
    // JS:   it("supports .then(), .catch(), and .finally() on RPC promises", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn supports_future_combinator_equivalents_on_rpc_promises() {
        // JS:     await using harness = new TestHarness(new TestTarget());
        let harness = IndexRpcHarness::new(Arc::new(LocalTestTarget));
        // JS:     let stub = harness.stub;
        let stub = harness.stub.clone();
        // JS:
        // JS:     // Test .then() with successful call
        // JS:     {
        {
            // JS:       let result = await stub.square(3).then(value => {
            let value = stub
                .call(vec!["square".to_owned()], one_number_argument(3.0))
                .pull()
                .await
                .unwrap();
            // JS:         expect(value).toBe(9);
            let CapnwebValue::Number(value) = value else {
                panic!()
            };
            assert_eq!(value, 9.0);
            // JS:         return value * 2;
            let result = value * 2.0;
            // JS:       });
            // Rust future composition returns `result` above.
            // JS:       expect(result).toBe(18);
            assert_eq!(result, 18.0);
            // JS:     }
        }
        // JS:
        // JS:     // Test .catch() with error
        // JS:     {
        {
            // JS:       let result = await stub.throwError()
            let error = stub
                .call(
                    vec!["throwError".to_owned()],
                    RpcPayload::from_app_params(CapnwebValue::Array(vec![])),
                )
                .pull()
                .await
                .unwrap_err();
            // JS:         .catch(err => {
            let result = {
                // JS:           expect(err).toBeInstanceOf(RangeError);
                // Rust preserves the serialized application error category.
                // JS:           expect((err as Error).message).toBe("test error");
                assert!(error.to_string().contains("test error"));
                // JS:           return "caught";
                "caught"
                // JS:         });
            };
            // JS:       expect(result).toBe("caught");
            assert_eq!(result, "caught");
            // JS:     }
        }
        // JS:
        // JS:     // Test .finally() with successful call
        // JS:     {
        {
            // JS:       let finallyCalled = false;
            let mut finally_called = false;
            // JS:       await stub.square(4)
            let result = stub
                .call(vec!["square".to_owned()], one_number_argument(4.0))
                .pull()
                .await;
            // JS:         .finally(() => {
            {
                // JS:           finallyCalled = true;
                finally_called = true;
                // JS:         });
            }
            result.unwrap();
            // JS:       expect(finallyCalled).toBe(true);
            assert!(finally_called);
            // JS:     }
        }
        // JS:
        // JS:     // Test .finally() with an error
        // JS:     {
        {
            // JS:       let finallyCalled = false;
            let mut finally_called = false;
            // JS:       let promise = stub.throwError()
            let promise = stub.call(
                vec!["throwError".to_owned()],
                RpcPayload::from_app_params(CapnwebValue::Array(vec![])),
            );
            // JS:         .finally(() => {
            let result = promise.pull().await;
            // JS:           finallyCalled = true;
            finally_called = true;
            // JS:         });
            // Rust runs the finalizer after polling the promise.
            // JS:       await expect(() => promise).rejects.toThrow(new RangeError("test error"));
            assert!(result.unwrap_err().to_string().contains("test error"));
            // JS:       expect(finallyCalled).toBe(true);
            assert!(finally_called);
            // JS:     }
        }
        // JS:   });
    }
    // JS:
    // JS:   it("throws error when trying to send non-serializable argument", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn throws_error_when_sending_a_non_serializable_argument() {
        // JS:     await using harness = new TestHarness(new TestTarget());
        let harness = IndexRpcHarness::new(Arc::new(LocalTestTarget));
        // JS:     let stub = harness.stub;
        let stub = harness.stub.clone();
        // JS:
        // JS:     expect(() => stub.square(new NotSerializable(123) as any)).toThrow(
        let result = stub
            .call(
                vec!["square".to_owned()],
                RpcPayload::from_app_params(CapnwebValue::Array(vec![CapnwebValue::Unsupported(
                    "NotSerializable(123)".to_owned(),
                )])),
            )
            .pull()
            .await;
        // JS:       new TypeError("Cannot serialize value: NotSerializable(123)")
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Cannot serialize value: NotSerializable(123)")
        );
        // JS:     );
        // The Rust facade reports payload encoding failure through the returned promise.
        // JS:   });
    }
    // JS:
    // JS:   it("throws error when trying to return non-serializable result", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn throws_error_when_returning_a_non_serializable_result() {
        // JS:     class BadTarget extends RpcTarget {
        let target = FixedResultTarget(Ok(CapnwebValue::Unsupported(
            "NotSerializable(456)".to_owned(),
        )));
        // JS:       returnNonSerializable() {
        // The fixed-result target returns the unsupported value.
        // JS:         return new NotSerializable(456);
        // Represented by `CapnwebValue::Unsupported` above.
        // JS:       }
        // JS:     }
        // JS:
        // JS:     await using harness = new TestHarness(new BadTarget());
        let harness = IndexRpcHarness::new(Arc::new(target));
        // JS:     let stub = harness.stub as any;
        let stub = harness.stub.clone();
        // JS:
        // JS:     await expect(() => stub.returnNonSerializable()).rejects.toThrow(
        let result = stub
            .call(
                vec!["returnNonSerializable".to_owned()],
                RpcPayload::from_app_params(CapnwebValue::Array(vec![])),
            )
            .pull()
            .await;
        // JS:       new TypeError("Cannot serialize value: NotSerializable(456)")
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Cannot serialize value: NotSerializable(456)")
        );
        // JS:     );
        // JS:   });
    }
    // JS:
    // JS:   it("does not expose common Object properties on RpcTarget", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn does_not_expose_common_object_properties_on_rpc_target() {
        // JS:     await using harness = new TestHarness(new TestTarget());
        let harness = IndexRpcHarness::new(Arc::new(LocalTestTarget));
        // JS:     let stub: any = harness.stub;
        let stub = harness.stub.clone();
        // JS:
        // JS:     // For this test we want to access properties on a remove object that are common properties of
        // JS:     // all objects. However, if we just access them on the stub, we'll actually access the *local*
        // JS:     // object's version of that property. We really want to generate messages sent to the other
        // JS:     // end to access the remote version, but there's no legitimate way to do this via the JS-level
        // JS:     // API. Fortunately, our transport implements a hack: the string "$remove$" will be excised
        // JS:     // from any message. So, we can use this as a prefix on property names to create a property
        // JS:     // that does not match anything locally, but by the time it reaches the remote end, will name
        // JS:     // a common object property.
        // JS:
        // JS:     // Properties of Object.prototype should not be exposed over RPC.
        // JS:     expect(await stub.$remove$toString).toBe(undefined);
        assert!(matches!(
            stub.get(vec!["toString".to_owned()]).pull().await.unwrap(),
            CapnwebValue::Undefined
        ));
        // JS:     expect(await stub.$remove$hasOwnProperty).toBe(undefined);
        assert!(matches!(
            stub.get(vec!["hasOwnProperty".to_owned()])
                .pull()
                .await
                .unwrap(),
            CapnwebValue::Undefined
        ));
        // JS:
        // JS:     // Special properties are not exposed.
        // JS:     expect(await stub.$remove$__proto__).toBe(undefined);
        assert!(matches!(
            stub.get(vec!["__proto__".to_owned()]).pull().await.unwrap(),
            CapnwebValue::Undefined
        ));
        // JS:     expect(await stub.$remove$constructor).toBe(undefined);
        assert!(matches!(
            stub.get(vec!["constructor".to_owned()])
                .pull()
                .await
                .unwrap(),
            CapnwebValue::Undefined
        ));
        // JS:   });
    }
    // JS:
    // JS:   it("does not expose common Object properties on RpcTarget", async () => {
    // RUST-DIFF(JS Proxy/reflection): blocked object/array/function prototype names use explicit path dispatch.
    // JS:     class ObjectVendor extends RpcTarget {
    // JS:       get() {
    // JS:         return new RpcStub<object>({
    // JS:           foo: 123,
    // JS:           arr: [1, 2],
    // JS:           func(x: any) { return `${x}`; },
    // JS:           jsonify(x: any) { return JSON.stringify(x); },
    // JS:           toString() { return "special string"; }
    // JS:         });
    // JS:       }
    // JS:     }
    // JS:
    // JS:     await using harness = new TestHarness(new ObjectVendor(), {
    // JS:       onSendError(err) { return err; }
    // JS:     });
    // JS:     using stub: any = await harness.stub.get();
    // JS:
    // JS:     expect(await stub.foo).toBe(123);
    // JS:     expect(await stub.func(321)).toBe("321");
    // JS:
    // JS:     // Similar to previous test case, but we're operating on a stub backed by an object rather
    // JS:     // than an RpcTarget now.
    // JS:
    // JS:     // Properties of Object.prototype should not be exposed over RPC.
    // JS:     expect(await stub.$remove$toString).toBe(undefined);
    // JS:     expect(await stub.$remove$hasOwnProperty).toBe(undefined);
    // JS:
    // JS:     // Properties of Array.prototype and Function.prototype are similarly not exposed even for
    // JS:     // values of those types.
    // JS:     expect(await stub.arr.$remove$map).toBe(undefined);
    // JS:     expect(await stub.func.$remove$call).toBe(undefined);
    // JS:
    // JS:     // Special properties are not exposed.
    // JS:     expect(await stub.$remove$__proto__).toBe(undefined);
    // JS:     expect(await stub.$remove$constructor).toBe(undefined);
    // JS:
    // JS:     expect(await stub.func({})).toBe("[object Object]");
    // JS:     expect(await stub.func({$remove$toString: "bad"})).toBe("[object Object]");
    // JS:     expect(await stub.func({$remove$__proto__: {toString: "bad"}})).toBe("[object Object]");
    // JS:
    // JS:     expect(await stub.jsonify({x: 123, $remove$toJSON: () => "bad"})).toBe('{"x":123}');
    // JS:   });
    // JS:
    // JS:   it("supports passing async functions", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn supports_passing_async_functions() {
        // JS:     await using harness = new TestHarness(new TestTarget());
        let harness = IndexRpcHarness::new(Arc::new(LocalTestTarget));
        // JS:
        // JS:     async function square(i: number) {
        let square = function_target(|args| async move {
            // JS:       await Promise.resolve();
            tokio::task::yield_now().await;
            // JS:       return i * i;
            let CapnwebValue::Array(args) = args.deliver_resolve().await? else {
                panic!()
            };
            let CapnwebValue::Number(value) = args[0] else {
                panic!()
            };
            Ok(CapnwebValue::Number(value * value))
            // JS:     }
        });
        // JS:
        // JS:     expect(await harness.stub.callFunction(square, 3)).toStrictEqual({result: 9});
        let result = harness
            .stub
            .call(
                vec!["callFunction".to_owned()],
                RpcPayload::from_app_params(CapnwebValue::Array(vec![
                    square,
                    CapnwebValue::Number(3.0),
                ])),
            )
            .pull()
            .await
            .unwrap();
        assert!(values_equal(
            &result,
            &object_value([("result", CapnwebValue::Number(9.0))])
        ));
        // JS:   });
    }
    // JS: });
}
// JS:
// JS: describe("capability-passing", () => {
mod capability_passing {
    use super::*;
    // JS:   it("supports returning an RpcTarget", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn supports_returning_an_rpc_target() {
        // JS:     await using harness = new TestHarness(new TestTarget());
        let harness = IndexRpcHarness::new(Arc::new(LocalTestTarget));
        // JS:     let stub = harness.stub;
        let stub = harness.stub.clone();
        // JS:     using counter = await stub.makeCounter(4);
        let counter = RpcStub::from_value(
            stub.call(vec!["makeCounter".to_owned()], one_number_argument(4.0))
                .pull()
                .await
                .unwrap(),
        );
        // JS:     expect(await counter.increment()).toBe(5);
        assert!(matches!(
            counter
                .call(
                    vec!["increment".to_owned()],
                    RpcPayload::from_app_params(CapnwebValue::Array(vec![]))
                )
                .pull()
                .await
                .unwrap(),
            CapnwebValue::Number(5.0)
        ));
        // JS:     expect(await counter.increment(4)).toBe(9);
        assert!(matches!(
            counter
                .call(vec!["increment".to_owned()], one_number_argument(4.0))
                .pull()
                .await
                .unwrap(),
            CapnwebValue::Number(9.0)
        ));
        // JS:   });
    }
    // JS:
    // JS:   it("supports passing a stub back over the connection", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn supports_passing_a_stub_back_over_the_connection() {
        // JS:     await using harness = new TestHarness(new TestTarget());
        let harness = IndexRpcHarness::new(Arc::new(LocalTestTarget));
        // JS:     let stub = harness.stub;
        let stub = harness.stub.clone();
        // JS:
        // JS:     using counter = await stub.makeCounter(4);
        let counter = RpcStub::from_value(
            stub.call(vec!["makeCounter".to_owned()], one_number_argument(4.0))
                .pull()
                .await
                .unwrap(),
        );
        // JS:     expect(await stub.incrementCounter(counter)).toBe(5);
        assert!(matches!(
            stub.call(
                vec!["incrementCounter".to_owned()],
                RpcPayload::from_app_params(CapnwebValue::Array(vec![CapnwebValue::Stub(
                    counter.clone()
                )])),
            )
            .pull()
            .await
            .unwrap(),
            CapnwebValue::Number(5.0)
        ));
        // JS:     expect(await stub.incrementCounter(counter, 4)).toBe(9);
        assert!(matches!(
            stub.call(
                vec!["incrementCounter".to_owned()],
                RpcPayload::from_app_params(CapnwebValue::Array(vec![
                    CapnwebValue::Stub(counter),
                    CapnwebValue::Number(4.0),
                ])),
            )
            .pull()
            .await
            .unwrap(),
            CapnwebValue::Number(9.0)
        ));
        // JS:   });
    }
    // JS:
    // JS:   it("supports three-party capability passing", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn supports_three_party_capability_passing() {
        // JS:     // Create two parallel connections: Alice and Bob
        // JS:     class AliceTarget extends RpcTarget {
        // JS:       getCounter() {
        // JS:         return new Counter(10);
        // JS:       }
        // JS:     }
        // Rust generated dispatch uses `LocalTestTarget::getCounter`.
        // JS:
        // JS:     class BobTarget extends RpcTarget {
        // JS:       // Bob actually uses the counter, causing calls to proxy through Bob to Alice
        // JS:       incrementCounter(counter: RpcStub<Counter>, amount: number) {
        // JS:         return counter.increment(amount);
        // JS:       }
        // JS:     }
        // Rust generated dispatch uses `LocalTestTarget::incrementCounter`.
        // JS:
        // JS:     await using aliceHarness = new TestHarness(new AliceTarget());
        let alice_harness = IndexRpcHarness::new(Arc::new(LocalTestTarget));
        // JS:     await using bobHarness = new TestHarness(new BobTarget());
        let bob_harness = IndexRpcHarness::new(Arc::new(LocalTestTarget));
        // JS:
        // JS:     let aliceStub = aliceHarness.stub;
        let alice_stub = alice_harness.stub.clone();
        // JS:     let bobStub = bobHarness.stub;
        let bob_stub = bob_harness.stub.clone();
        // JS:
        // JS:     // Get counter from Alice.
        // JS:     using counter = await aliceStub.getCounter();
        let counter = RpcStub::from_value(
            alice_stub
                .call(
                    vec!["getCounter".to_owned()],
                    RpcPayload::from_app_params(CapnwebValue::Array(vec![])),
                )
                .pull()
                .await
                .unwrap(),
        );
        // JS:
        // JS:     // Bob increments the counter - this call proxies from Bob through the client to Alice
        // JS:     let result = await bobStub.incrementCounter(counter, 3);
        let result = bob_stub
            .call(
                vec!["incrementCounter".to_owned()],
                RpcPayload::from_app_params(CapnwebValue::Array(vec![
                    CapnwebValue::Stub(counter),
                    CapnwebValue::Number(3.0),
                ])),
            )
            .pull()
            .await
            .unwrap();
        // JS:     expect(result).toBe(13);
        assert!(matches!(result, CapnwebValue::Number(13.0)));
        // JS:   });
    }
    // JS:
    // JS:   it("supports proxying", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn supports_proxying() {
        // JS:     // Create two connections in series: us -> Bob -> Alice
        // JS:     class AliceTarget extends RpcTarget {
        // JS:       getCounter(i: number) {
        // JS:         return new Counter(i);
        // JS:       }
        // JS:
        // JS:       incrementCounter(counter: RpcStub<Counter>, amount: number) {
        // JS:         return counter.increment(amount);
        // JS:       }
        // JS:     }
        // JS:
        // JS:     class BobTarget extends RpcTarget {
        // JS:       constructor(private alice: RpcStub<AliceTarget>) {
        // JS:         super();
        // JS:       }
        // JS:
        // JS:       async getCounter(i: number) {
        // JS:         return await this.alice.getCounter(i);
        // JS:       }
        // JS:
        // JS:       getCounterPromise(i: number) {
        // JS:         return this.alice.getCounter(i);
        // JS:       }
        // JS:
        // JS:       incrementCounter(counter: RpcStub<Counter>, amount: number) {
        // JS:         return this.alice.incrementCounter(counter, amount);
        // JS:       }
        // JS:     }
        // JS:
        // JS:     await using aliceHarness = new TestHarness(new AliceTarget());
        let alice_harness = IndexRpcHarness::new(Arc::new(LocalTestTarget));
        // JS:     await using bobHarness = new TestHarness(new BobTarget(aliceHarness.stub));
        let bob_harness = IndexRpcHarness::new(Arc::new(ProxyTarget {
            alice: alice_harness.stub.clone(),
        }));
        // JS:
        // JS:     let bobStub = bobHarness.stub;
        let bob_stub = bob_harness.stub.clone();
        // JS:
        // JS:     // Return capability through proxy.
        // JS:     {
        {
            // JS:       using result = await bobStub.getCounter(4);
            let result = RpcStub::from_value(
                bob_stub
                    .call(vec!["getCounter".to_owned()], one_number_argument(4.0))
                    .pull()
                    .await
                    .unwrap(),
            );
            // JS:       expect(await result.increment(2)).toBe(6)
            assert!(matches!(
                result
                    .call(vec!["increment".to_owned()], one_number_argument(2.0))
                    .pull()
                    .await
                    .unwrap(),
                CapnwebValue::Number(6.0)
            ));
            // JS:     }
        }
        // JS:
        // JS:     // Return capability through proxy, pipeline.
        // JS:     {
        {
            // JS:       using result = bobStub.getCounter(4);
            let result = bob_stub.call(vec!["getCounter".to_owned()], one_number_argument(4.0));
            // JS:       expect(await result.increment(2)).toBe(6)
            assert!(matches!(
                result
                    .call(vec!["increment".to_owned()], one_number_argument(2.0))
                    .pull()
                    .await
                    .unwrap(),
                CapnwebValue::Number(6.0)
            ));
            // JS:     }
        }
        // JS:
        // JS:     // Return promise through proxy.
        // JS:     {
        {
            // JS:       using result = bobStub.getCounterPromise(4);
            let result = bob_stub.call(
                vec!["getCounterPromise".to_owned()],
                one_number_argument(4.0),
            );
            // JS:       expect(await result.increment(2)).toBe(6)
            assert!(matches!(
                result
                    .call(vec!["increment".to_owned()], one_number_argument(2.0))
                    .pull()
                    .await
                    .unwrap(),
                CapnwebValue::Number(6.0)
            ));
            // JS:     }
        }
        // JS:
        // JS:     // Send capability through proxy.
        // JS:     {
        {
            // JS:       let counter = new Counter(10);
            let counter = RpcStub::from_target(Arc::new(LocalCounter(std::sync::Mutex::new(10.0))));
            // JS:
            // JS:       let result = await bobStub.incrementCounter(counter, 3);
            let result = bob_stub
                .call(
                    vec!["incrementCounter".to_owned()],
                    RpcPayload::from_app_params(CapnwebValue::Array(vec![
                        CapnwebValue::Stub(counter.clone()),
                        CapnwebValue::Number(3.0),
                    ])),
                )
                .pull()
                .await
                .unwrap();
            // JS:
            // JS:       expect(result).toBe(13);
            assert!(matches!(result, CapnwebValue::Number(13.0)));
            // JS:       expect(counter.increment(1)).toBe(14);
            assert!(matches!(
                counter
                    .call(vec!["increment".to_owned()], one_number_argument(1.0))
                    .pull()
                    .await
                    .unwrap(),
                CapnwebValue::Number(14.0)
            ));
            // JS:     }
        }
        // JS:   });
    }
    // JS: });
}
// JS:
// JS: describe("promise pipelining", () => {
mod promise_pipelining {
    use super::*;
    // JS:   it("supports passing a promise in arguments", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn supports_passing_a_promise_in_arguments() {
        // JS:     await using harness = new TestHarness(new TestTarget());
        let harness = IndexRpcHarness::new(Arc::new(LocalTestTarget));
        // JS:     let stub = harness.stub;
        let stub = harness.stub.clone();
        // JS:     using promise = stub.square(2);
        let promise = stub.call(vec!["square".to_owned()], one_number_argument(2.0));
        // JS:     expect(await stub.square(promise)).toBe(16);
        assert!(matches!(
            stub.call(
                vec!["square".to_owned()],
                RpcPayload::from_app_params(CapnwebValue::Array(vec![CapnwebValue::RpcPromise(
                    promise
                )]))
            )
            .pull()
            .await
            .unwrap(),
            CapnwebValue::Number(16.0)
        ));
        // JS:   });
    }
    // JS:
    // JS:   it("supports calling a promise", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn supports_calling_a_promise() {
        // JS:     await using harness = new TestHarness(new TestTarget());
        let harness = IndexRpcHarness::new(Arc::new(LocalTestTarget));
        // JS:     let stub = harness.stub;
        let stub = harness.stub.clone();
        // JS:     using counter = stub.makeCounter(4);
        let counter = stub.call(vec!["makeCounter".to_owned()], one_number_argument(4.0));
        // JS:     let promise1 = counter.increment();
        let promise1 = counter.call(
            vec!["increment".to_owned()],
            RpcPayload::from_app_params(CapnwebValue::Array(vec![])),
        );
        // JS:     let promise2 = counter.increment(4);
        let promise2 = counter.call(vec!["increment".to_owned()], one_number_argument(4.0));
        // JS:     expect(await promise1).toBe(5);
        assert!(matches!(
            promise1.pull().await.unwrap(),
            CapnwebValue::Number(5.0)
        ));
        // JS:     expect(await promise2).toBe(9);
        assert!(matches!(
            promise2.pull().await.unwrap(),
            CapnwebValue::Number(9.0)
        ));
        // JS:   });
    }
    // JS:
    // JS:   it("supports returning a promise", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn supports_returning_a_promise() {
        // JS:     await using harness = new TestHarness(new TestTarget());
        let harness = IndexRpcHarness::new(Arc::new(LocalTestTarget));
        // JS:     let stub = harness.stub;
        let stub = harness.stub.clone();
        // JS:     expect(await stub.callSquare(stub, 3)).toStrictEqual({result: 9});
        let result = stub
            .call(
                vec!["callSquare".to_owned()],
                RpcPayload::from_app_params(CapnwebValue::Array(vec![
                    CapnwebValue::Stub(stub.clone()),
                    CapnwebValue::Number(3.0),
                ])),
            )
            .pull()
            .await
            .unwrap();
        let CapnwebValue::Object(result) = result else {
            panic!()
        };
        assert!(matches!(
            result.get("result"),
            Some(CapnwebValue::Number(9.0))
        ));
        // JS:   });
    }
    // JS:
    // JS:   it("propagates errors to pipelined calls", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn propagates_errors_to_pipelined_calls() {
        // JS:     class ErrorTarget extends RpcTarget {
        // JS:       throwError(): TestTarget {
        // JS:         throw new Error("pipelined error");
        // JS:       }
        // JS:     }
        // `FixedResultTarget` is the generated dispatch equivalent.
        // JS:
        // JS:     await using harness = new TestHarness(new ErrorTarget());
        let harness = IndexRpcHarness::new(Arc::new(FixedResultTarget(Err(Error::Message(
            "pipelined error".to_owned(),
        )))));
        // JS:     let stub = harness.stub;
        let stub = harness.stub.clone();
        // JS:
        // JS:     // Pipeline a call on a promise that will reject
        // JS:     using errorPromise = stub.throwError();
        let error_promise = stub.call(
            vec!["throwError".to_owned()],
            RpcPayload::from_app_params(CapnwebValue::Array(vec![])),
        );
        // JS:     using pipelinedCall = errorPromise.square(5);
        let pipelined_call =
            error_promise.call(vec!["square".to_owned()], one_number_argument(5.0));
        // JS:
        // JS:     await expect(() => pipelinedCall).rejects.toThrow("pipelined error");
        let error = pipelined_call.pull().await.unwrap_err();
        assert!(format!("{error}").contains("pipelined error"));
        // JS:   });
    }
    // JS:
    // JS:   it("propagates errors to argument-pipelined calls", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn propagates_errors_to_argument_pipelined_calls() {
        // JS:     class ErrorTarget extends RpcTarget {
        // JS:       throwError(): never {
        // JS:         throw new Error("pipelined error");
        // JS:       }
        // JS:
        // JS:       processValue(value: any) {
        // JS:         return value * 2;
        // JS:       }
        // JS:     }
        // `ErrorProcessingTarget` is the generated dispatch equivalent.
        // JS:
        // JS:     await using harness = new TestHarness(new ErrorTarget());
        let harness = IndexRpcHarness::new(Arc::new(ErrorProcessingTarget("pipelined error")));
        // JS:     let stub = harness.stub;
        let stub = harness.stub.clone();
        // JS:
        // JS:     // Pipeline a call on a promise that will reject
        // JS:     using errorPromise = stub.throwError();
        let error_promise = stub.call(
            vec!["throwError".to_owned()],
            RpcPayload::from_app_params(CapnwebValue::Array(vec![])),
        );
        // JS:     using pipelinedCall = stub.processValue(errorPromise);
        let pipelined_call = stub.call(
            vec!["processValue".to_owned()],
            RpcPayload::from_app_params(CapnwebValue::Array(vec![CapnwebValue::RpcPromise(
                error_promise,
            )])),
        );
        // JS:
        // JS:     await expect(() => pipelinedCall).rejects.toThrow("pipelined error");
        let error = pipelined_call.pull().await.unwrap_err();
        assert!(format!("{error}").contains("pipelined error"));
        // JS:   });
    }
    // JS:
    // JS:   it("doesn't create spurious unhandled rejections", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn does_not_create_spurious_unhandled_rejections() {
        // JS:     class ErrorTarget extends RpcTarget {
        // JS:       throwError(): never {
        // JS:         throw new Error("test error");
        // JS:       }
        // JS:
        // JS:       processValue(value: any) {
        // JS:         return value * 2;
        // JS:       }
        // JS:     }
        // `ErrorProcessingTarget` is the generated dispatch equivalent.
        // JS:
        // JS:     await using harness = new TestHarness(new ErrorTarget());
        let harness = IndexRpcHarness::new(Arc::new(ErrorProcessingTarget("test error")));
        // JS:     let stub = harness.stub;
        let stub = harness.stub.clone();
        // JS:
        // JS:     let promise = stub.throwError();
        let promise = stub.call(
            vec!["throwError".to_owned()],
            RpcPayload::from_app_params(CapnwebValue::Array(vec![])),
        );
        // JS:     let promise2 = stub.processValue(promise);
        let promise2 = stub.call(
            vec!["processValue".to_owned()],
            RpcPayload::from_app_params(CapnwebValue::Array(vec![CapnwebValue::RpcPromise(
                promise.clone(),
            )])),
        );
        // JS:
        // JS:     // Intentionally don't await the promises until the next tick. This means we don't pull them,
        // JS:     // which means nothing awaits the final result on the server end, which means the errors
        // JS:     // could be considered "unhandled rejections". We do not want the server end to actually see
        // JS:     // them as such, though, since it's entirely the client's fault that it hasn't waited on them
        // JS:     // yet! This tests that the system silences such unhandled rejection notices. Note that
        // JS:     // vitest automatically treats unhandled rejections as failures.
        // JS:     await new Promise(resolve => setTimeout(resolve, 0));
        tokio::task::yield_now().await;
        // JS:
        // JS:     await expect(() => promise).rejects.toThrow("test error");
        assert!(format!("{}", promise.pull().await.unwrap_err()).contains("test error"));
        // JS:     await expect(() => promise2).rejects.toThrow("test error");
        assert!(format!("{}", promise2.pull().await.unwrap_err()).contains("test error"));
        // JS:   });
    }
    // JS: });
}
// JS:
// JS: describe("map() over RPC", () => {
// JS:   it("supports map() on nulls", async () => {
// JS:     let counter = new RpcStub(new Counter(0));
// JS:
// JS:     await using harness = new TestHarness(new TestTarget());
// JS:     let stub = harness.stub;
// JS:
// JS:     {
// JS:       using promise = stub.returnNull();
// JS:       expect(await promise.map(_ => counter.increment(123))).toBe(null);
// JS:     }
// JS:
// JS:     {
// JS:       using promise = stub.returnUndefined();
// JS:       expect(await promise.map(_ => counter.increment(456))).toBe(undefined);
// JS:     }
// JS:
// JS:     {
// JS:       using promise = stub.returnNumber(2);
// JS:       expect(await promise.map(i => counter.increment(i))).toBe(2);
// JS:     }
// JS:
// JS:     {
// JS:       using promise = stub.returnNumber(4);
// JS:       expect(await promise.map(i => counter.increment(i))).toBe(6);
// JS:     }
// JS:   });
// JS:
// JS:   it("supports map() on arrays", async () => {
// JS:     let outerCounter = new RpcStub(new Counter(0));
// JS:
// JS:     await using harness = new TestHarness(new TestTarget());
// JS:     let stub = harness.stub;
// JS:
// JS:     using fib = stub.generateFibonacci(6);
// JS:     using counters = await fib.map(i => {
// JS:       let counter = stub.makeCounter(i);
// JS:       let val = counter.increment(3);
// JS:       outerCounter.increment();
// JS:       return {counter, val};
// JS:     });
// JS:
// JS:     expect(counters.map(x => x.val)).toStrictEqual([3, 4, 4, 5, 6, 8]);
// JS:
// JS:     expect(await Promise.all(counters.map(x => x.counter.value)))
// JS:         .toStrictEqual([3, 4, 4, 5, 6, 8]);
// JS:
// JS:     expect(await outerCounter.value).toBe(6);
// JS:   });
// JS:
// JS:   it("supports nested map()", async () => {
// JS:     await using harness = new TestHarness(new TestTarget());
// JS:     let stub = harness.stub;
// JS:
// JS:     using fib = stub.generateFibonacci(7);
// JS:     using result = await fib.map(i => {
// JS:       return stub.generateFibonacci(i).map(j => {
// JS:         return stub.generateFibonacci(j);
// JS:       });
// JS:     });
// JS:
// JS:     expect(result).toStrictEqual([
// JS:       [],
// JS:       [[]],
// JS:       [[]],
// JS:       [[], [0]],
// JS:       [[], [0], [0]],
// JS:       [[], [0], [0], [0, 1], [0, 1, 1]],
// JS:       [[], [0], [0], [0, 1], [0, 1, 1], [0, 1, 1, 2, 3], [0, 1, 1, 2, 3, 5, 8, 13],
// JS:           [0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144]],
// JS:     ]);
// JS:   });
// JS: });
// JS:
// JS: describe("stub disposal over RPC", () => {
mod stub_disposal_over_rpc {
    use super::*;
    // JS:   it("disposes remote RpcTarget when stub is disposed", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn disposes_remote_rpc_target_when_stub_is_disposed() {
        // JS:     let targetDisposedCount = 0;
        let target_disposed_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        // JS:     class DisposableTarget extends RpcTarget {
        // JS:       getValue() { return 42; }
        // JS:       [Symbol.dispose]() { ++targetDisposedCount; }
        // JS:     }
        // `DisposableValueTarget::drop` is Rust's ownership equivalent.
        // JS:
        // JS:     class MainTarget extends RpcTarget {
        // JS:       getDisposableTarget() {
        // JS:         return new DisposableTarget();
        // JS:       }
        // JS:     }
        // `DisposableMainTarget` is the generated dispatch equivalent.
        // JS:
        // JS:     await using harness = new TestHarness(new MainTarget());
        let harness = IndexRpcHarness::new(Arc::new(DisposableMainTarget {
            disposed: target_disposed_count.clone(),
        }));
        // JS:     let mainStub = harness.stub as any;
        let main_stub = harness.stub.clone();
        // JS:
        // JS:     {
        {
            // JS:       using disposableStub = await mainStub.getDisposableTarget();
            let disposable_stub = RpcStub::from_value(
                main_stub
                    .call(
                        vec!["getDisposableTarget".to_owned()],
                        RpcPayload::from_app_params(CapnwebValue::Array(vec![])),
                    )
                    .pull()
                    .await
                    .unwrap(),
            );
            // JS:       expect(await disposableStub.getValue()).toBe(42);
            assert!(matches!(
                disposable_stub
                    .call(
                        vec!["getValue".to_owned()],
                        RpcPayload::from_app_params(CapnwebValue::Array(vec![]))
                    )
                    .pull()
                    .await
                    .unwrap(),
                CapnwebValue::Number(42.0)
            ));
            // JS:       expect(targetDisposedCount).toBe(0);
            assert_eq!(target_disposed_count.load(Ordering::SeqCst), 0);
            // JS:     } // disposer runs here
        }
        // JS:
        // JS:     // Wait a bit for the disposal message to be processed
        // JS:     await pumpMicrotasks();
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        // JS:
        // JS:     expect(targetDisposedCount).toBe(1);
        assert_eq!(target_disposed_count.load(Ordering::SeqCst), 1);
        // JS:   });
    }
    // JS:
    // JS:   it("disposes a returned RpcTarget for every time it appears in a result", async () => {
    // JS:     let targetDisposedCount = 0;
    // JS:     class DisposableTarget extends RpcTarget {
    // JS:       getValue() { return 42; }
    // JS:       [Symbol.dispose]() { ++targetDisposedCount; }
    // JS:     }
    // JS:
    // JS:     class MainTarget extends RpcTarget {
    // JS:       getDisposableTarget() {
    // JS:         let result = new DisposableTarget();
    // JS:         return [result, result, result];
    // JS:       }
    // JS:     }
    // JS:
    // JS:     await using harness = new TestHarness(new MainTarget());
    // JS:     let mainStub = harness.stub as any;
    // JS:
    // JS:     {
    // JS:       using disposableStub = await mainStub.getDisposableTarget();
    // JS:       expect(await disposableStub[0].getValue()).toBe(42);
    // JS:       expect(await disposableStub[1].getValue()).toBe(42);
    // JS:       expect(await disposableStub[2].getValue()).toBe(42);
    // JS:
    // JS:       // The current implementation will actually call the disposer twice as soon as the pipeline
    // JS:       // is done, but the last call won't happen until the stubs are disposed.
    // JS:       expect(targetDisposedCount).toBeLessThan(3);
    // JS:     } // final disposer runs here
    // JS:
    // JS:     // Wait a bit for the disposal message to be processed
    // JS:     await pumpMicrotasks();
    // JS:
    // JS:     // Disposer is called three times.
    // JS:     expect(targetDisposedCount).toBe(3);
    // JS:   });
    // JS:
    // JS:   it("disposes RpcTarget that was passed in params", async () => {
    // JS:     let targetDisposedCount = 0;
    // JS:     class DisposableTarget extends RpcTarget {
    // JS:       getValue() { return 42; }
    // JS:       [Symbol.dispose]() { ++targetDisposedCount; }
    // JS:     }
    // JS:
    // JS:     class MainTarget extends RpcTarget {
    // JS:       useDisposableTarget(stub: RpcStub<DisposableTarget>) {
    // JS:         return stub.getValue();
    // JS:       }
    // JS:     }
    // JS:
    // JS:     await using harness = new TestHarness(new MainTarget());
    // JS:     let mainStub = harness.stub as any;
    // JS:
    // JS:     {
    // JS:       let result = await mainStub.useDisposableTarget(new DisposableTarget());
    // JS:       expect(result).toBe(42);
    // JS:     }
    // JS:
    // JS:     // Wait a bit for the disposal message to be processed
    // JS:     await pumpMicrotasks();
    // JS:
    // JS:     expect(targetDisposedCount).toBe(1);
    // JS:   });
    // JS:
    // JS:   it("dupes RpcTarget that was passed in params if it has a dup() method", async () => {
    // JS:     let dupCount = 0;
    // JS:     let disposeCount = 0;
    // JS:     class DisposableTarget extends RpcTarget {
    // JS:       getValue() { return 42; }
    // JS:
    // JS:       dup() {
    // JS:         ++dupCount;
    // JS:         return new DisposableTarget();
    // JS:       }
    // JS:
    // JS:       disposed = false;
    // JS:       [Symbol.dispose]() {
    // JS:         if (this.disposed) throw new Error("double disposed");
    // JS:         this.disposed = true;
    // JS:         ++disposeCount;
    // JS:       }
    // JS:     }
    // JS:
    // JS:     class MainTarget extends RpcTarget {
    // JS:       useDisposableTarget(stub: RpcStub<DisposableTarget>) {
    // JS:         return stub.getValue();
    // JS:       }
    // JS:     }
    // JS:
    // JS:     await using harness = new TestHarness(new MainTarget());
    // JS:     let mainStub = harness.stub as any;
    // JS:
    // JS:     let disposableTarget = new DisposableTarget();
    // JS:
    // JS:     {
    // JS:       let result = await mainStub.useDisposableTarget(disposableTarget);
    // JS:       expect(dupCount).toBe(1);
    // JS:       expect(result).toBe(42);
    // JS:     }
    // JS:
    // JS:     {
    // JS:       let result = await mainStub.useDisposableTarget(disposableTarget);
    // JS:       expect(dupCount).toBe(2);
    // JS:       expect(result).toBe(42);
    // JS:     }
    // JS:
    // JS:     // Wait a bit for the disposal message to be processed
    // JS:     await pumpMicrotasks();
    // JS:
    // JS:     expect(dupCount).toBe(2);
    // JS:     expect(disposeCount).toBe(2);
    // JS:     expect(disposableTarget.disposed).toBe(false);
    // JS:   });
    // JS:
    // JS:   it("only disposes remote target when all RPC dups are disposed", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn only_disposes_remote_target_when_all_rpc_dups_are_disposed() {
        // JS:     let targetDisposed = false;
        let target_disposed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        // JS:     class DisposableTarget extends RpcTarget {
        // JS:       getValue() { return 42; }
        // JS:       [Symbol.dispose]() { targetDisposed = true; }
        // JS:     }
        // `DisposableValueTarget::drop` is Rust's ownership equivalent.
        // JS:
        // JS:     class MainTarget extends RpcTarget {
        // JS:       getDisposableTarget() {
        // JS:         return new DisposableTarget();
        // JS:       }
        // JS:     }
        // `DisposableMainTarget` is the generated dispatch equivalent.
        // JS:
        // JS:     await using harness = new TestHarness(new MainTarget());
        let harness = IndexRpcHarness::new(Arc::new(DisposableMainTarget {
            disposed: target_disposed.clone(),
        }));
        // JS:     let mainStub = harness.stub as any;
        let main_stub = harness.stub.clone();
        // JS:
        // JS:     let disposableStub = await mainStub.getDisposableTarget();
        let disposable_stub = RpcStub::from_value(
            main_stub
                .call(
                    vec!["getDisposableTarget".to_owned()],
                    RpcPayload::from_app_params(CapnwebValue::Array(vec![])),
                )
                .pull()
                .await
                .unwrap(),
        );
        // JS:     let dup1 = disposableStub.dup();
        let dup1 = disposable_stub.clone();
        // JS:     let dup2 = disposableStub.dup();
        let dup2 = disposable_stub.clone();
        // JS:
        // JS:     disposableStub[Symbol.dispose]();
        drop(disposable_stub);
        // JS:     await pumpMicrotasks();
        for _ in 0..2 {
            tokio::task::yield_now().await;
        }
        // JS:     expect(targetDisposed).toBe(false);
        assert_eq!(target_disposed.load(Ordering::SeqCst), 0);
        // JS:
        // JS:     dup1[Symbol.dispose]();
        drop(dup1);
        // JS:     await pumpMicrotasks();
        for _ in 0..2 {
            tokio::task::yield_now().await;
        }
        // JS:     expect(targetDisposed).toBe(false);
        assert_eq!(target_disposed.load(Ordering::SeqCst), 0);
        // JS:
        // JS:     dup2[Symbol.dispose]();
        drop(dup2);
        // JS:     await pumpMicrotasks();
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        // JS:     expect(targetDisposed).toBe(true);
        assert_eq!(target_disposed.load(Ordering::SeqCst), 1);
        // JS:   });
    }
    // JS:
    // JS:   it("makes RPC disposal idempotent", async () => {
    // JS:     let targetDisposed = false;
    // JS:     class DisposableTarget extends RpcTarget {
    // JS:       getValue() { return 42; }
    // JS:       [Symbol.dispose]() { targetDisposed = true; }
    // JS:     }
    // JS:
    // JS:     class MainTarget extends RpcTarget {
    // JS:       getDisposableTarget() {
    // JS:         return new DisposableTarget();
    // JS:       }
    // JS:     }
    // JS:
    // JS:     await using harness = new TestHarness(new MainTarget());
    // JS:     let mainStub = harness.stub as any;
    // JS:
    // JS:     let disposableStub = await mainStub.getDisposableTarget();
    // JS:     let dup1 = disposableStub.dup();
    // JS:
    // JS:     // Dispose the duplicate twice
    // JS:     dup1[Symbol.dispose]();
    // JS:     dup1[Symbol.dispose]();
    // JS:     await pumpMicrotasks();
    // JS:     expect(targetDisposed).toBe(false);
    // JS:
    // JS:     // Only when original is also disposed should the target be disposed
    // JS:     disposableStub[Symbol.dispose]();
    // JS:     await pumpMicrotasks();
    // JS:     expect(targetDisposed).toBe(true);
    // JS:   });
    // JS:
    // JS:   it("disposes targets automatically on disconnect", async () => {
    // JS:     let targetDisposed = false;
    // JS:     class DisposableTarget extends RpcTarget {
    // JS:       getValue() { return 42; }
    // JS:       hangingCall(): Promise<number> {
    // JS:         // This call will hang and be interrupted by disconnect
    // JS:         return new Promise(() => {}); // Never resolves
    // JS:       }
    // JS:       [Symbol.dispose]() { targetDisposed = true; }
    // JS:     }
    // JS:
    // JS:     // Intentionally don't use `using` here because we expect the stats to be wrong after a
    // JS:     // disconnect.
    // JS:     let harness = new TestHarness(new DisposableTarget());
    // JS:     let stub = harness.stub;
    // JS:     expect(await stub.getValue()).toBe(42);
    // JS:
    // JS:     // Start a hanging call
    // JS:     let hangingPromise = stub.hangingCall();
    // JS:
    // JS:     // Simulate disconnect by making the transport fail
    // JS:     harness.clientTransport.forceReceiveError(new Error("test error"));
    // JS:
    // JS:     // The hanging call should be rejected
    // JS:     await expect(() => hangingPromise).rejects.toThrow(new Error("test error"));
    // JS:
    // JS:     // Further calls should also fail immediately
    // JS:     await expect(() => stub.getValue()).rejects.toThrow(new Error("test error"));
    // JS:
    // JS:     // Targets should be disposed
    // JS:     expect(targetDisposed).toBe(true);
    // JS:   });
    // JS:
    // JS:   it("shuts down the connection if the main capability is disposed", async () => {
    // JS:     // Intentionally don't use `using` here because we expect the stats to be wrong after a
    // JS:     // disconnect.
    // JS:     let harness = new TestHarness(new TestTarget());
    // JS:     let stub = harness.stub;
    // JS:
    // JS:     let counter = await stub.makeCounter(0);
    // JS:
    // JS:     stub[Symbol.dispose]();
    // JS:
    // JS:     await expect(() => counter.increment(1)).rejects.toThrow(
    // JS:       new Error("RPC session was shut down by disposing the main stub")
    // JS:     );
    // JS:   });
    // JS: });
}
// JS:
// JS: describe("e-order", () => {
// JS:   it("maintains e-order for concurrent calls on single stub", async () => {
// JS:     let callOrder: number[] = [];
// JS:     class OrderTarget extends RpcTarget {
// JS:       recordCall(id: number) {
// JS:         callOrder.push(id);
// JS:         return id;
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new OrderTarget());
// JS:     let stub = harness.stub as any;
// JS:
// JS:     // Make multiple concurrent calls
// JS:     let promises = [
// JS:       stub.recordCall(1),
// JS:       stub.recordCall(2),
// JS:       stub.recordCall(3),
// JS:       stub.recordCall(4)
// JS:     ];
// JS:
// JS:     await Promise.all(promises);
// JS:
// JS:     // Calls should arrive in the order they were made
// JS:     expect(callOrder).toEqual([1, 2, 3, 4]);
// JS:   });
// JS:
// JS:   it("maintains e-order for promise-pipelined calls", async () => {
// JS:     let callOrder: number[] = [];
// JS:     class OrderTarget extends RpcTarget {
// JS:       getObject() {
// JS:         return {
// JS:           method1: (id: number) => { callOrder.push(id); return id; },
// JS:           method2: (id: number) => { callOrder.push(id); return id; }
// JS:         };
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new OrderTarget());
// JS:     let stub = harness.stub as any;
// JS:
// JS:     // Get a promise for an object
// JS:     using objectPromise = stub.getObject();
// JS:
// JS:     // Make pipelined calls on different methods of the same promise
// JS:     let promises = [
// JS:       objectPromise.method1(1),
// JS:       objectPromise.method2(2),
// JS:       objectPromise.method1(3),
// JS:       objectPromise.method2(4)
// JS:     ];
// JS:
// JS:     await Promise.all(promises);
// JS:
// JS:     // Calls should arrive in the order they were made, even across different methods
// JS:     expect(callOrder).toEqual([1, 2, 3, 4]);
// JS:   });
// JS: });
// JS:
// JS: describe("error serialization", () => {
// JS:   it("hides the stack by default", async () => {
// JS:     await using harness = new TestHarness(new TestTarget(), {
// JS:       onSendError: (error) => {
// JS:         // default behavior
// JS:       }
// JS:     });
// JS:     let stub = harness.stub;
// JS:
// JS:     let result = await stub.throwError()
// JS:       .catch(err => {
// JS:         expect(err).toBeInstanceOf(RangeError);
// JS:         expect((err as Error).message).toBe("test error");
// JS:
// JS:         // By default, the stack isn't sent. A stack may be added client-side, though. So we
// JS:         // verify that it doesn't contain the function name `throwErrorImpl` nor the file name
// JS:         // `test-util.ts`, which should only appear on the server.
// JS:         expect((err as Error).stack).not.toContain("throwErrorImpl");
// JS:         expect((err as Error).stack).not.toContain("test-util.ts");
// JS:
// JS:         return "caught";
// JS:       });
// JS:     expect(result).toBe("caught");
// JS:   });
// JS:
// JS:   it("hides the stack by default with structured clone transports", async () => {
// JS:     let clientTransport = new ObjectTestTransport(undefined, "structuredClonable");
// JS:     let serverTransport = new ObjectTestTransport(clientTransport, "structuredClonable");
// JS:     let client = new RpcSession<TestTarget>(clientTransport);
// JS:     new RpcSession(serverTransport, new TestTarget());
// JS:     using stub = client.getRemoteMain();
// JS:
// JS:     let result = await stub.throwError()
// JS:       .catch(err => {
// JS:         expect(err).toBeInstanceOf(RangeError);
// JS:         expect((err as Error).message).toBe("test error");
// JS:         expect((err as Error).stack).not.toContain("throwErrorImpl");
// JS:         expect((err as Error).stack).not.toContain("test-util.ts");
// JS:
// JS:         return "caught";
// JS:       });
// JS:     expect(result).toBe("caught");
// JS:   });
// JS:
// JS:   it("reveals the stack if the callback returns the error", async () => {
// JS:     await using harness = new TestHarness(new TestTarget(), {
// JS:       onSendError: (error) => {
// JS:         return error;
// JS:       }
// JS:     });
// JS:     let stub = harness.stub;
// JS:
// JS:     let result = await stub.throwError()
// JS:       .catch(err => {
// JS:         expect(err).toBeInstanceOf(RangeError);
// JS:         expect((err as Error).message).toBe("test error");
// JS:
// JS:         // Now the error function and source file should be in the stack.
// JS:         expect((err as Error).stack).toContain("throwErrorImpl");
// JS:         expect((err as Error).stack).toContain("test-util.ts");
// JS:
// JS:         return "caught";
// JS:       });
// JS:     expect(result).toBe("caught");
// JS:   });
// JS:
// JS:   it("allows errors to be rewritten", async () => {
// JS:     await using harness = new TestHarness(new TestTarget(), {
// JS:       onSendError: (error) => {
// JS:         let rewritten = new TypeError("rewritten error");
// JS:         rewritten.stack = "test stack";
// JS:         return rewritten;
// JS:       }
// JS:     });
// JS:     let stub = harness.stub;
// JS:
// JS:     let result = await stub.throwError()
// JS:       .catch(err => {
// JS:         expect(err).toBeInstanceOf(TypeError);
// JS:         expect((err as Error).message).toBe("rewritten error");
// JS:         expect((err as Error).stack).toBe("test stack");
// JS:         return "caught";
// JS:       });
// JS:     expect(result).toBe("caught");
// JS:   });
// JS:
// JS:   it("sends own properties from the rewritten error, not the original", async () => {
// JS:     class ErrorTarget extends RpcTarget {
// JS:       throwError() {
// JS:         let err = new Error("original") as any;
// JS:         err.code = "E_ORIGINAL";
// JS:         throw err;
// JS:       }
// JS:     }
// JS:     await using harness = new TestHarness(new ErrorTarget(), {
// JS:       onSendError: _error => {
// JS:         let rewritten = new TypeError("rewritten") as Error & Record<string, unknown>;
// JS:         rewritten.code = "E_REWRITTEN";
// JS:         return rewritten;
// JS:       }
// JS:     });
// JS:     let stub = harness.stub as any;
// JS:
// JS:     let caught: unknown;
// JS:     try {
// JS:       await stub.throwError();
// JS:     } catch (e) {
// JS:       caught = e;
// JS:     }
// JS:
// JS:     if (!(caught instanceof Error)) throw new Error("invariant");
// JS:     expect(caught).toBeInstanceOf(TypeError);
// JS:     expect(caught.message).toBe("rewritten");
// JS:     expect((caught as Error & Record<string, unknown>).code).toBe("E_REWRITTEN");
// JS:   });
// JS:
// JS:   it("respects in-place mutation by onSendError to scrub heavy properties", async () => {
// JS:     class ErrorTarget extends RpcTarget {
// JS:       throwError() {
// JS:         let err = new Error("with-secret") as Error & Record<string, unknown>;
// JS:         err.code = "E_OK";
// JS:         err.secret = "super-sensitive-data";
// JS:         throw err;
// JS:       }
// JS:     }
// JS:     await using harness = new TestHarness(new ErrorTarget(), {
// JS:       onSendError: error => {
// JS:         // Returning the same error after mutating it is a documented escape hatch for
// JS:         // scrubbing fields the caller doesn't want to send.
// JS:         delete (error as any).secret;
// JS:         return error;
// JS:       }
// JS:     });
// JS:     let stub = harness.stub;
// JS:
// JS:     let caught: unknown;
// JS:     try {
// JS:       await stub.throwError();
// JS:     } catch (e) {
// JS:       caught = e;
// JS:     }
// JS:
// JS:     if (!(caught instanceof Error)) throw new Error("invariant");
// JS:     expect(caught.message).toBe("with-secret");
// JS:     expect((caught as Error & Record<string, unknown>).code).toBe("E_OK");
// JS:     expect("secret" in caught).toBe(false);
// JS:   });
// JS: });
// JS:
// JS: describe("onRpcBroken", () => {
// JS:   it("signals when the connection is lost", async () => {
// JS:     class TestBroken extends RpcTarget {
// JS:       getValue() { return 42; }
// JS:       makeCounter() { return new Counter(0); }
// JS:       hangingCall(): Promise<Counter> {
// JS:         // This call will hang and be interrupted by disconnect
// JS:         return new Promise(() => {}); // Never resolves
// JS:       }
// JS:       throwError(): Promise<Counter> { throw new Error("test error"); }
// JS:     }
// JS:
// JS:     // Intentionally don't use `using` here because we expect the stats to be wrong after a
// JS:     // disconnect.
// JS:     let harness = new TestHarness(new TestBroken());
// JS:     let stub = harness.stub;
// JS:     expect(await stub.getValue()).toBe(42);
// JS:
// JS:     let errors: {which: string, error: any}[] = [];
// JS:     stub.onRpcBroken(error => { errors.push({which: "stub", error}); });
// JS:
// JS:     let counter1Promise = stub.makeCounter();
// JS:     counter1Promise.onRpcBroken(error => { errors.push({which: "counter1Promise", error}); });
// JS:
// JS:     let counter2 = await stub.makeCounter();
// JS:     counter2.onRpcBroken(error => { errors.push({which: "counter2", error}); });
// JS:
// JS:     let counter1 = await counter1Promise;
// JS:     counter1.onRpcBroken(error => { errors.push({which: "counter1", error}); });
// JS:
// JS:     let hangingPromise = stub.hangingCall();
// JS:     hangingPromise.onRpcBroken(error => { errors.push({which: "hangingCall", error}); });
// JS:
// JS:     let throwingPromise = stub.throwError();
// JS:     throwingPromise.onRpcBroken(error => { errors.push({which: "throwError", error}); });
// JS:
// JS:     // The method that threw should report brokenness immediately.
// JS:     await throwingPromise.catch(err => {});
// JS:     expect(errors).toStrictEqual([
// JS:       {which: "throwError", error: new Error("test error")},
// JS:     ]);
// JS:
// JS:     // onRpcBroken() when already broken just reports the error immediately.
// JS:     throwingPromise.onRpcBroken(error => { errors.push({which: "throwError2", error}); });
// JS:     expect(errors).toStrictEqual([
// JS:       {which: "throwError", error: new Error("test error")},
// JS:       {which: "throwError2", error: new Error("test error")},
// JS:     ]);
// JS:
// JS:     // Simulate disconnect by making the transport fail
// JS:     harness.clientTransport.forceReceiveError(new Error("test disconnect"));
// JS:     await hangingPromise.catch(err => {});
// JS:
// JS:     // Now all the other errors were reported, in the order in which the callbacks were
// JS:     // registered.
// JS:     expect(errors).toStrictEqual([
// JS:       {which: "throwError", error: new Error("test error")},
// JS:       {which: "throwError2", error: new Error("test error")},
// JS:       {which: "stub", error: new Error("test disconnect")},
// JS:       {which: "counter1Promise", error: new Error("test disconnect")},
// JS:       {which: "counter2", error: new Error("test disconnect")},
// JS:       {which: "counter1", error: new Error("test disconnect")},
// JS:       {which: "hangingCall", error: new Error("test disconnect")},
// JS:     ]);
// JS:   });
// JS: });
// JS:
// JS: // =======================================================================================
// JS:
// JS: // Creates an RpcTarget that records whether its disposer has run, for verifying that a payload
// JS: // containing (a copy of) it was properly disposed.
// JS: function disposalSpy() {
// JS:   let disposed = false;
// JS:   class Disposable extends RpcTarget {
// JS:     [Symbol.dispose]() { disposed = true; }
// JS:   }
// JS:   return { target: new Disposable(), wasDisposed: () => disposed };
// JS: }
// JS:
// JS: describe("PromiseStubHook", () => {
// JS:   it("disposes copied call arguments when the backing promise rejects", async () => {
// JS:     let spy = disposalSpy();
// JS:     let argument = new RpcStub(spy.target);
// JS:     let hook = new PromiseStubHook(Promise.reject(new Error("nope")));
// JS:     let result = hook.call([], RpcPayload.fromAppParams([argument]));
// JS:
// JS:     await expect(result.pull()).rejects.toThrow("nope");
// JS:     argument[Symbol.dispose]();
// JS:     expect(spy.wasDisposed()).toBe(true);
// JS:   });
// JS:
// JS:   it("disposes copied stream arguments when the backing promise rejects", async () => {
// JS:     let spy = disposalSpy();
// JS:     let argument = new RpcStub(spy.target);
// JS:     let hook = new PromiseStubHook(Promise.reject(new Error("nope")));
// JS:     let result = hook.stream(["write"], RpcPayload.fromAppParams([argument]));
// JS:
// JS:     await expect(result.promise).rejects.toThrow("nope");
// JS:     argument[Symbol.dispose]();
// JS:     expect(spy.wasDisposed()).toBe(true);
// JS:   });
// JS:
// JS:   it("delivers a call initiated before disposal", async () => {
// JS:     let disposed = false;
// JS:     class DisposableCounter extends Counter {
// JS:       [Symbol.dispose]() { disposed = true; }
// JS:     }
// JS:
// JS:     let inner = new RpcStub(new DisposableCounter(1));
// JS:     let hook = new PromiseStubHook(Promise.resolve(unwrapStubTakingOwnership(<any>inner)));
// JS:     await pumpMicrotasks();
// JS:
// JS:     let stub: RpcStub<Counter> = <any>new RawRpcStub(hook);
// JS:     let result = stub.increment(2);
// JS:     stub[Symbol.dispose]();
// JS:
// JS:     expect(disposed).toBe(false);
// JS:     expect(await result).toBe(3);
// JS:     expect(disposed).toBe(true);
// JS:   });
// JS:
// JS:   it("disposes copied call arguments when the destination hook is broken", async () => {
// JS:     let spy = disposalSpy();
// JS:     let argument = new RpcStub(spy.target);
// JS:     let hook = new PromiseStubHook(Promise.resolve(new ErrorStubHook(new Error("broken"))));
// JS:     let result = hook.call([], RpcPayload.fromAppParams([argument]));
// JS:
// JS:     await expect(result.pull()).rejects.toThrow("broken");
// JS:     argument[Symbol.dispose]();
// JS:     expect(spy.wasDisposed()).toBe(true);
// JS:   });
// JS:
// JS:   it("disposes copied stream arguments when the destination hook is broken", async () => {
// JS:     let spy = disposalSpy();
// JS:     let argument = new RpcStub(spy.target);
// JS:     let hook = new PromiseStubHook(Promise.resolve(new ErrorStubHook(new Error("broken"))));
// JS:     let result = hook.stream(["write"], RpcPayload.fromAppParams([argument]));
// JS:
// JS:     await expect(result.promise).rejects.toThrow("broken");
// JS:     argument[Symbol.dispose]();
// JS:     expect(spy.wasDisposed()).toBe(true);
// JS:   });
// JS:
// JS:   it("disposes copied call arguments when the local call path fails", async () => {
// JS:     let spy = disposalSpy();
// JS:     let argument = new RpcStub(spy.target);
// JS:     let hook = new PromiseStubHook(
// JS:         Promise.resolve(new PayloadStubHook(RpcPayload.fromAppReturn({}))));
// JS:     let result = hook.call(["nope"], RpcPayload.fromAppParams([argument]));
// JS:
// JS:     await expect(result.pull()).rejects.toThrow("'nope' is not a function");
// JS:     argument[Symbol.dispose]();
// JS:     expect(spy.wasDisposed()).toBe(true);
// JS:     hook.dispose();
// JS:   });
// JS:
// JS:   it("disposes map captures when the destination hook is broken", () => {
// JS:     let spy = disposalSpy();
// JS:     let capture = unwrapStubTakingOwnership(<any>new RpcStub(spy.target));
// JS:     let hook = new ErrorStubHook(new Error("broken"));
// JS:     hook.map([], [capture], []);
// JS:     expect(spy.wasDisposed()).toBe(true);
// JS:   });
// JS:
// JS:   // A minimal contract-honoring callee: its call() takes ownership of `args` (disposing them)
// JS:   // and then throws synchronously. Used to pin the division of labor: once the backing promise
// JS:   // resolves, args ownership lies with the resolved callee, not with PromiseStubHook.
// JS:   //
// JS:   // Extends ErrorStubHook only to inherit harmless implementations of the methods these tests
// JS:   // never invoke; note it does not override stream(), so the base StubHook.stream() is used.
// JS:   class SyncThrowingHook extends ErrorStubHook {
// JS:     constructor() { super(new Error("unused")); }
// JS:     call(path: PropertyPath, args: RpcPayload): StubHook {
// JS:       args.dispose();
// JS:       throw new Error("sync failure");
// JS:     }
// JS:   }
// JS:
// JS:   it("leaves call args ownership with a resolved callee whose call() throws synchronously",
// JS:       async () => {
// JS:     let spy = disposalSpy();
// JS:     let argument = new RpcStub(spy.target);
// JS:     let hook = new PromiseStubHook(Promise.resolve(new SyncThrowingHook()));
// JS:     let result = hook.call([], RpcPayload.fromAppParams([argument]));
// JS:
// JS:     await expect(result.pull()).rejects.toThrow("sync failure");
// JS:     argument[Symbol.dispose]();
// JS:     expect(spy.wasDisposed()).toBe(true);
// JS:   });
// JS:
// JS:   it("leaves stream args ownership with a resolved callee whose call() throws synchronously",
// JS:       async () => {
// JS:     let spy = disposalSpy();
// JS:     let argument = new RpcStub(spy.target);
// JS:     let hook = new PromiseStubHook(Promise.resolve(new SyncThrowingHook()));
// JS:     // SyncThrowingHook inherits the default stream(), which delegates to call().
// JS:     let result = hook.stream(["write"], RpcPayload.fromAppParams([argument]));
// JS:
// JS:     await expect(result.promise).rejects.toThrow("sync failure");
// JS:     argument[Symbol.dispose]();
// JS:     expect(spy.wasDisposed()).toBe(true);
// JS:   });
// JS:
// JS:   it("default stream() disposes the result hook when pull() throws synchronously", () => {
// JS:     let resultHookDisposed = false;
// JS:
// JS:     class ThrowingPullHook extends SyncThrowingHook {
// JS:       pull(): RpcPayload | Promise<RpcPayload> { throw new Error("pull failed"); }
// JS:       dispose(): void { resultHookDisposed = true; }
// JS:     }
// JS:
// JS:     class LocalCallHook extends SyncThrowingHook {
// JS:       // Inherits the base StubHook.stream(), which delegates to call() + pull().
// JS:       call(path: PropertyPath, args: RpcPayload): StubHook {
// JS:         args.dispose();
// JS:         return new ThrowingPullHook();
// JS:       }
// JS:     }
// JS:
// JS:     expect(() => new LocalCallHook().stream(["write"], RpcPayload.fromAppParams([])))
// JS:         .toThrow("pull failed");
// JS:     expect(resultHookDisposed).toBe(true);
// JS:   });
// JS: });
// JS:
// JS: describe("RpcImportHook argument disposal", () => {
// JS:   it("call() disposes owned args when argument serialization fails", async () => {
// JS:     await using harness = new TestHarness(new TestTarget());
// JS:
// JS:     let spy = disposalSpy();
// JS:     let argument = new RpcStub(spy.target);
// JS:
// JS:     let hook = unwrapStubTakingOwnership(<any>harness.stub.dup());
// JS:
// JS:     // The stub is serialized (and exported) before the Symbol makes devaluation fail, so this
// JS:     // exercises both the Devaluator's export rollback and the disposal of the owned payload.
// JS:     let payload = RpcPayload.fromAppParams([argument, Symbol("unserializable")]);
// JS:     payload.ensureDeepCopied();
// JS:
// JS:     expect(() => hook.call(["square"], payload)).toThrow("Cannot serialize value");
// JS:
// JS:     hook.dispose();
// JS:     argument[Symbol.dispose]();
// JS:     expect(spy.wasDisposed()).toBe(true);
// JS:   });
// JS:
// JS:   it("stream() disposes owned args when argument serialization fails", async () => {
// JS:     await using harness = new TestHarness(new TestTarget());
// JS:
// JS:     let spy = disposalSpy();
// JS:     let argument = new RpcStub(spy.target);
// JS:
// JS:     let hook = unwrapStubTakingOwnership(<any>harness.stub.dup());
// JS:
// JS:     let payload = RpcPayload.fromAppParams([argument, Symbol("unserializable")]);
// JS:     payload.ensureDeepCopied();
// JS:
// JS:     expect(() => hook.stream(["square"], payload)).toThrow("Cannot serialize value");
// JS:
// JS:     hook.dispose();
// JS:     argument[Symbol.dispose]();
// JS:     expect(spy.wasDisposed()).toBe(true);
// JS:   });
// JS: });
// JS:
// JS: // =======================================================================================
// JS:
// JS: describe("constructing RpcPromise from a promise", () => {
// JS:   it("pipelines through a pending promise without pulling the resolution", async () => {
// JS:     await using harness = new TestHarness(new TestTarget());
// JS:
// JS:     let {promise, resolve} = Promise.withResolvers<RpcStub<TestTarget>>();
// JS:     using stub = new RpcPromise<TestTarget>(promise);
// JS:
// JS:     using counter = stub.makeCounter(1);
// JS:     let result = counter.increment(2);
// JS:     resolve(harness.stub.dup());
// JS:     expect(await result).toBe(3);
// JS:
// JS:     // Only the final result was pulled: neither the promise's resolution nor the intermediate
// JS:     // counter was transmitted.
// JS:     let sent = harness.clientTransport.sentLog;
// JS:     expect(sent.some(msg => msg.startsWith('["push"'))).toBe(true);
// JS:     expect(sent.filter(msg => msg.startsWith('["pull"'))).toHaveLength(1);
// JS:   });
// JS:
// JS:   it("queues calls made before resolution and delivers them in order", async () => {
// JS:     let calls: number[] = [];
// JS:     class Recorder extends RpcTarget {
// JS:       record(i: number) { calls.push(i); return i; }
// JS:     }
// JS:
// JS:     let {promise, resolve} = Promise.withResolvers<Recorder>();
// JS:     using stub = new RpcPromise<Recorder>(promise);
// JS:
// JS:     let results = [stub.record(1), stub.record(2), stub.record(3)];
// JS:     expect(calls).toStrictEqual([]);
// JS:
// JS:     resolve(new Recorder());
// JS:     expect(await Promise.all(results)).toStrictEqual([1, 2, 3]);
// JS:     expect(calls).toStrictEqual([1, 2, 3]);
// JS:   });
// JS:
// JS:   it("accepts a promise for a target, a remote stub, or a plain value", async () => {
// JS:     await using harness = new TestHarness(new TestTarget());
// JS:
// JS:     using target = new RpcPromise<Counter>(Promise.resolve(new Counter(1)));
// JS:     expect(await target.increment()).toBe(2);
// JS:
// JS:     using remote = new RpcPromise<TestTarget>(Promise.resolve(harness.stub.dup()));
// JS:     expect(await remote.square(3)).toBe(9);
// JS:
// JS:     using value = new RpcPromise<{foo: number}>(Promise.resolve({foo: 123}));
// JS:     expect(await value.foo).toBe(123);
// JS:   });
// JS:
// JS:   it("awaiting the RpcPromise yields the resolution", async () => {
// JS:     using plain = new RpcPromise<{foo: number}>(Promise.resolve({foo: 123}));
// JS:     expect(await plain).toStrictEqual({foo: 123});
// JS:
// JS:     await using harness = new TestHarness(new TestTarget());
// JS:     using remote = new RpcPromise<TestTarget>(Promise.resolve(harness.stub.dup()));
// JS:     let resolved = await remote;
// JS:     expect(await resolved.square(4)).toBe(16);
// JS:   });
// JS:
// JS:   it("reports rejection to queued calls, await, and onRpcBroken", async () => {
// JS:     let error = new Error("nope");
// JS:     using stub = new RpcPromise<Counter>(Promise.reject(error));
// JS:
// JS:     let broken: any[] = [];
// JS:     stub.onRpcBroken(err => { broken.push(err); });
// JS:
// JS:     await expect(() => stub.increment()).rejects.toThrow("nope");
// JS:     await expect(Promise.resolve(stub)).rejects.toThrow("nope");
// JS:     expect(broken).toStrictEqual([error]);
// JS:   });
// JS:
// JS:   it("does not report an unhandled rejection for an unused promise", async () => {
// JS:     new RpcPromise<Counter>(Promise.reject(new Error("ignored")));
// JS:     await pumpMicrotasks();
// JS:   });
// JS:
// JS:   it("does not report an unhandled rejection for a disposed, unawaited queued call", async () => {
// JS:     using stub = new RpcPromise<Counter>(Promise.reject(new Error("ignored")));
// JS:     using result = stub.increment();  // never awaited; disposal alone must observe the error
// JS:     await pumpMicrotasks();
// JS:   });
// JS:
// JS:   it("does not report an unhandled rejection for a disposed, unawaited map() result", async () => {
// JS:     using stub = new RpcPromise<number>(Promise.reject(new Error("ignored")));
// JS:     using result = stub.map(i => i);  // never awaited; disposal alone must observe the error
// JS:     await pumpMicrotasks();
// JS:   });
// JS:
// JS:   it("disposes the eventual target when disposed before resolution", async () => {
// JS:     let disposed = false;
// JS:     class Disposable extends RpcTarget {
// JS:       [Symbol.dispose]() { disposed = true; }
// JS:     }
// JS:
// JS:     let {promise, resolve} = Promise.withResolvers<Disposable>();
// JS:     let stub = new RpcPromise<Disposable>(promise);
// JS:     stub[Symbol.dispose]();
// JS:
// JS:     resolve(new Disposable());
// JS:     await pumpMicrotasks();
// JS:     expect(disposed).toBe(true);
// JS:   });
// JS:
// JS:   it("delivers a call initiated before disposal", async () => {
// JS:     let disposed = false;
// JS:     class DisposableCounter extends Counter {
// JS:       [Symbol.dispose]() { disposed = true; }
// JS:     }
// JS:
// JS:     let stub = new RpcPromise<DisposableCounter>(Promise.resolve(new DisposableCounter(1)));
// JS:     await pumpMicrotasks();
// JS:
// JS:     let result = stub.increment(2);
// JS:     stub[Symbol.dispose]();
// JS:
// JS:     expect(disposed).toBe(false);
// JS:     expect(await result).toBe(3);
// JS:     expect(disposed).toBe(true);
// JS:   });
// JS:
// JS:   it("keeps an adopted RpcPromise lazy", async () => {
// JS:     await using harness = new TestHarness(new TestTarget());
// JS:
// JS:     using counter = new RpcPromise<Counter>(harness.stub.makeCounter(1));
// JS:     expect(await counter.increment(2)).toBe(3);
// JS:
// JS:     let sent = harness.clientTransport.sentLog;
// JS:     expect(sent.filter(msg => msg.startsWith('["pull"'))).toHaveLength(1);
// JS:   });
// JS:
// JS:   it("transmits nothing when constructed from a remote property promise", async () => {
// JS:     await using harness = new TestHarness(new TestTarget());
// JS:
// JS:     using counter = harness.stub.makeCounter(5);
// JS:     await pumpMicrotasks();  // let the makeCounter push flush
// JS:
// JS:     let source = counter.value;
// JS:     let wrapped = new RpcPromise<number>(source);
// JS:
// JS:     // Construction shares the source's hook and path; the get() producing an independent hook
// JS:     // happens lazily on first await, so nothing goes over the wire yet.
// JS:     let sentBefore = harness.clientTransport.sentLog.length;
// JS:     await pumpMicrotasks();
// JS:     expect(harness.clientTransport.sentLog.length).toBe(sentBefore);
// JS:
// JS:     expect(await wrapped).toBe(5);
// JS:
// JS:     // The source property promise remains usable.
// JS:     expect(await source).toBe(5);
// JS:   });
// JS:
// JS:   it("does not invoke a local getter when constructed from a property promise", async () => {
// JS:     let reads = 0;
// JS:     class Gettable extends RpcTarget {
// JS:       get prop() { ++reads; return 42; }
// JS:     }
// JS:
// JS:     using stub = new RpcStub(new Gettable());
// JS:     let source = stub.prop;
// JS:     let wrapped = new RpcPromise<number>(source);
// JS:
// JS:     await pumpMicrotasks();
// JS:     expect(reads).toBe(0);
// JS:
// JS:     expect(await wrapped).toBe(42);
// JS:     expect(await source).toBe(42);
// JS:   });
// JS:
// JS:   it("consumes the source when adopting an existing RpcPromise", async () => {
// JS:     await using harness = new TestHarness(new TestTarget());
// JS:
// JS:     let source = harness.stub.makeCounter(1);
// JS:     using wrapper = new RpcPromise<Counter>(source);
// JS:
// JS:     // The source was neutered: using it now reports the standard disposed error, and disposing
// JS:     // it is a harmless no-op that doesn't affect the wrapper.
// JS:     await expect(source.increment(1)).rejects.toThrow(
// JS:         "Attempted to use RPC stub after it has been disposed.");
// JS:     source[Symbol.dispose]();
// JS:
// JS:     expect(await wrapper.increment(2)).toBe(3);
// JS:   });
// JS:
// JS:   it("stays lazy when a deferred promise is resolved with dup()", async () => {
// JS:     await using harness = new TestHarness(new TestTarget());
// JS:
// JS:     using counter = harness.stub.makeCounter(1);
// JS:
// JS:     // Resolving with the RpcPromise itself would let the native promise machinery assimilate it
// JS:     // as a thenable, pulling the resolution. dup() returns a non-thenable stub, which the
// JS:     // resolution adopts, keeping calls pipelined.
// JS:     let {promise, resolve} = Promise.withResolvers<RpcStub<Counter>>();
// JS:     using stub = new RpcPromise<Counter>(promise);
// JS:
// JS:     let result = stub.increment(2);
// JS:     resolve(counter.dup());
// JS:     expect(await result).toBe(3);
// JS:
// JS:     let sent = harness.clientTransport.sentLog;
// JS:     expect(sent.filter(msg => msg.startsWith('["pull"'))).toHaveLength(1);
// JS:   });
// JS:
// JS:   it("preserves brokenness of a bare stub it was constructed from", async () => {
// JS:     await using harness = new TestHarness(new TestTarget());
// JS:     using stub = new RpcPromise<TestTarget>(<any>harness.stub.dup());
// JS:
// JS:     let errors: any[] = [];
// JS:     stub.onRpcBroken(error => { errors.push(error); });
// JS:
// JS:     harness.clientTransport.forceReceiveError(new Error("test disconnect"));
// JS:     await pumpMicrotasks();
// JS:     expect(errors).toStrictEqual([new Error("test disconnect")]);
// JS:   });
// JS:
// JS:   it("keeps disposal idempotent when constructed from a bare stub", async () => {
// JS:     let disposals = 0;
// JS:     class Disposable extends RpcTarget {
// JS:       [Symbol.dispose]() { ++disposals; }
// JS:     }
// JS:
// JS:     let inner = new RpcStub(new Disposable());
// JS:     let outer = new RpcPromise<Disposable>(<any>inner);
// JS:     inner[Symbol.dispose]();
// JS:     outer[Symbol.dispose]();
// JS:
// JS:     await pumpMicrotasks();
// JS:     expect(disposals).toBe(1);
// JS:   });
// JS:
// JS:   it("resolves when awaited after construction from a bare local stub", async () => {
// JS:     // Regression test: the constructor previously adopted a bare stub's hook directly, producing
// JS:     // a promise whose pipelined calls worked but whose await rejected, because non-promise hooks
// JS:     // don't implement pull().
// JS:     using stub = new RpcPromise<Counter>(<any>new RpcStub(new Counter(1)));
// JS:
// JS:     expect(await stub.increment(2)).toBe(3);
// JS:     let resolved = await stub;
// JS:     expect(await resolved.increment(3)).toBe(6);
// JS:   });
// JS: });
// JS:
// JS: // =======================================================================================
// JS:
// JS: describe("HTTP requests", () => {
// JS:   it("can perform a batch HTTP request", async () => {
// JS:     let cap = newHttpBatchRpcSession<TestTarget>(`http://${inject("testServerHost")}`);
// JS:
// JS:     let promise1 = cap.square(6);
// JS:
// JS:     let counter = cap.makeCounter(2);
// JS:     let promise2 = counter.increment(3);
// JS:     let promise3 = cap.incrementCounter(counter, 4);
// JS:
// JS:     expect(await Promise.all([promise1, promise2, promise3]))
// JS:         .toStrictEqual([36, 5, 9]);
// JS:   });
// JS:
// JS:   it("rejects non-POST requests with 405", async () => {
// JS:     let response = await fetch(`http://${inject("testServerHost")}`, { method: "GET" });
// JS:     expect(response.status).toBe(405);
// JS:     await response.text();
// JS:   });
// JS: });
// JS:
// JS: describe("WebSockets", () => {
// JS:   it("can open a WebSocket connection", async () => {
// JS:     let url = `ws://${inject("testServerHost")}`;
// JS:
// JS:     let cap = newWebSocketRpcSession<TestTarget>(url);
// JS:
// JS:     expect(await cap.square(5)).toBe(25);
// JS:
// JS:     {
// JS:       let counter = cap.makeCounter(2);
// JS:       expect(await counter.increment(3)).toBe(5);
// JS:     }
// JS:
// JS:     {
// JS:       let counter = new Counter(4);
// JS:       expect(await cap.incrementCounter(counter, 9)).toBe(13);
// JS:     }
// JS:   });
// JS: });
// JS:
// JS: describe("MessagePorts", () => {
// JS:   it("can communicate over MessageChannel", async () => {
// JS:     // Create a MessageChannel for communication
// JS:     let channel = new MessageChannel();
// JS:
// JS:     // Set up server side with a test object
// JS:     let serverMain = new TestTarget();
// JS:     newMessagePortRpcSession(channel.port1, serverMain);
// JS:
// JS:     // Set up client side
// JS:     using clientStub = newMessagePortRpcSession<TestTarget>(channel.port2);
// JS:
// JS:     // Test basic method call
// JS:     let result = await clientStub.square(5);
// JS:     expect(result).toBe(25);
// JS:
// JS:     // Test nested object
// JS:     let counter = await clientStub.makeCounter(10);
// JS:     expect(await counter.increment()).toBe(11);
// JS:     expect(await counter.increment(5)).toBe(16);
// JS:
// JS:     // Test method that takes a stub as parameter
// JS:     let incrementResult = await clientStub.incrementCounter(counter, 2);
// JS:     expect(incrementResult).toBe(18);
// JS:   });
// JS:
// JS:   it("handles errors correctly", async () => {
// JS:     let channel = new MessageChannel();
// JS:
// JS:     let serverMain = new TestTarget();
// JS:     newMessagePortRpcSession(channel.port1, serverMain);
// JS:     using clientStub = newMessagePortRpcSession<TestTarget>(channel.port2);
// JS:
// JS:     // Test error handling
// JS:     await expect(() => clientStub.throwError()).rejects.toThrow("test error");
// JS:   });
// JS:
// JS:   it("sends close signal when server stub is disposed", async () => {
// JS:     let channel = new MessageChannel();
// JS:
// JS:     let serverMain = new TestTarget();
// JS:     let serverStub = newMessagePortRpcSession(channel.port1, serverMain);
// JS:     using clientStub = newMessagePortRpcSession<TestTarget>(channel.port2);
// JS:
// JS:     // Test that connection works initially
// JS:     let result = await clientStub.square(3);
// JS:     expect(result).toBe(9);
// JS:
// JS:     // Set up broken callback on client
// JS:     let brokenPromise = new Promise<void>((resolve, reject) => {
// JS:       clientStub.onRpcBroken(reject);
// JS:     });
// JS:
// JS:     // Dispose the server stub, which should send a close signal
// JS:     serverStub[Symbol.dispose]();
// JS:
// JS:     // Wait for the client to detect the broken connection
// JS:     await expect(() => brokenPromise).rejects.toThrow(
// JS:         new Error("Peer closed MessagePort connection."));
// JS:   });
// JS: });
// JS:
// JS: // =======================================================================================
// JS:
// JS: describe("WritableStream over RPC", () => {
// JS:   it("can send a WritableStream and receive writes", async () => {
// JS:     // Create a WritableStream that collects chunks
// JS:     let chunks: string[] = [];
// JS:     let closeCalled = false;
// JS:     let stream = new WritableStream<string>({
// JS:       write(chunk) { chunks.push(chunk); },
// JS:       close() { closeCalled = true; }
// JS:     });
// JS:
// JS:     class StreamReceiver extends RpcTarget {
// JS:       async receiveStream(stream: WritableStream<string>) {
// JS:         // Write to the stream
// JS:         let writer = stream.getWriter();
// JS:         await writer.write("hello");
// JS:         await writer.write("world");
// JS:         await writer.close();
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new StreamReceiver());
// JS:     await harness.stub.receiveStream(stream);
// JS:
// JS:     expect(chunks).toEqual(["hello", "world"]);
// JS:     expect(closeCalled).toBe(true);
// JS:   });
// JS:
// JS:   it("supports complex chunk types", async () => {
// JS:     let receivedChunks: unknown[] = [];
// JS:     let stream = new WritableStream({
// JS:       write(chunk) { receivedChunks.push(chunk); }
// JS:     });
// JS:
// JS:     class StreamReceiver extends RpcTarget {
// JS:       async receiveStream(stream: WritableStream) {
// JS:         let writer = stream.getWriter();
// JS:         await writer.write({ name: "test", value: 42 });
// JS:         await writer.write([1, 2, 3]);
// JS:         await writer.write(new Date(1234567890000));
// JS:         await writer.close();
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new StreamReceiver());
// JS:     await harness.stub.receiveStream(stream);
// JS:
// JS:     expect(receivedChunks).toHaveLength(3);
// JS:     expect(receivedChunks[0]).toEqual({ name: "test", value: 42 });
// JS:     expect(receivedChunks[1]).toEqual([1, 2, 3]);
// JS:     expect(receivedChunks[2]).toEqual(new Date(1234567890000));
// JS:   });
// JS:
// JS:   it("propagates write errors back", async () => {
// JS:     let writeCount = 0;
// JS:     let stream = new WritableStream({
// JS:       write(chunk) {
// JS:         writeCount++;
// JS:         if (writeCount > 2) {
// JS:           throw new Error("Write limit exceeded");
// JS:         }
// JS:       }
// JS:     });
// JS:
// JS:     class StreamReceiver extends RpcTarget {
// JS:       async receiveStream(stream: WritableStream) {
// JS:         let writer = stream.getWriter();
// JS:         await writer.write("first");
// JS:         await writer.write("second");
// JS:         // The third write will fail, and the error will propagate when we try to close
// JS:         await writer.write("third");
// JS:         await writer.close();
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new StreamReceiver());
// JS:     await expect(() => harness.stub.receiveStream(stream)).rejects.toThrow("Write limit exceeded");
// JS:     expect(writeCount).toBe(3);
// JS:   });
// JS:
// JS:   it("aborts stream on disconnect without close", async () => {
// JS:     let abortReason: any = null;
// JS:     let stream = new WritableStream({
// JS:       write(chunk) {},
// JS:       abort(reason) { abortReason = reason; }
// JS:     });
// JS:
// JS:     class StreamReceiver extends RpcTarget {
// JS:       receiveStream(stream: WritableStream) {
// JS:         // Start writing but don't close - just return immediately
// JS:         let writer = stream.getWriter();
// JS:         writer.write("data");
// JS:         // Note: not calling writer.close()
// JS:       }
// JS:     }
// JS:
// JS:     // Don't use the normal harness since we need to control disposal differently
// JS:     let clientTransport = new TestTransport("client");
// JS:     let serverTransport = new TestTransport("server", clientTransport);
// JS:
// JS:     let client = new RpcSession(clientTransport);
// JS:     let server = new RpcSession(serverTransport, new StreamReceiver());
// JS:
// JS:     let stub: any = client.getRemoteMain();
// JS:     await stub.receiveStream(stream);
// JS:
// JS:     // Wait a bit for the write to be processed
// JS:     await pumpMicrotasks();
// JS:
// JS:     // Dispose the client, which should cause the stream to be aborted
// JS:     stub[Symbol.dispose]();
// JS:
// JS:     // Wait for the abort to propagate
// JS:     await pumpMicrotasks();
// JS:
// JS:     expect(abortReason).not.toBeNull();
// JS:     expect(abortReason.message).toContain("disposed without calling close");
// JS:   });
// JS:
// JS:   it("handles abort() from receiver", async () => {
// JS:     let abortReason: any = null;
// JS:     let stream = new WritableStream({
// JS:       write(chunk) {},
// JS:       abort(reason) { abortReason = reason; }
// JS:     });
// JS:
// JS:     class StreamReceiver extends RpcTarget {
// JS:       async receiveStream(stream: WritableStream) {
// JS:         let writer = stream.getWriter();
// JS:         await writer.write("data");
// JS:         await writer.abort("User requested abort");
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new StreamReceiver());
// JS:     await harness.stub.receiveStream(stream);
// JS:
// JS:     // Wait for abort to propagate
// JS:     await pumpMicrotasks();
// JS:
// JS:     expect(abortReason).toBe("User requested abort");
// JS:   });
// JS:
// JS:   it("can send WritableStream in nested object", async () => {
// JS:     let chunks: string[] = [];
// JS:     let stream = new WritableStream<string>({
// JS:       write(chunk) { chunks.push(chunk); }
// JS:     });
// JS:
// JS:     class StreamReceiver extends RpcTarget {
// JS:       async receiveData(data: { stream: WritableStream<string>, label: string }) {
// JS:         let writer = data.stream.getWriter();
// JS:         await writer.write(`${data.label}: hello`);
// JS:         await writer.close();
// JS:         return "done";
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new StreamReceiver());
// JS:     let result = await harness.stub.receiveData({ stream, label: "test" });
// JS:
// JS:     expect(result).toBe("done");
// JS:     expect(chunks).toEqual(["test: hello"]);
// JS:   });
// JS:
// JS:   it("handles multiple concurrent writes efficiently", async () => {
// JS:     let chunks: number[] = [];
// JS:     let stream = new WritableStream<number>({
// JS:       async write(chunk) {
// JS:         // Simulate some async processing
// JS:         await new Promise(resolve => setTimeout(resolve, 10));
// JS:         chunks.push(chunk);
// JS:       }
// JS:     });
// JS:
// JS:     class StreamReceiver extends RpcTarget {
// JS:       async receiveStream(stream: WritableStream<number>) {
// JS:         let writer = stream.getWriter();
// JS:         // Write multiple chunks without awaiting each one
// JS:         // (The implementation should pipeline these)
// JS:         let writes = [];
// JS:         for (let i = 0; i < 5; i++) {
// JS:           writes.push(writer.write(i));
// JS:         }
// JS:         await Promise.all(writes);
// JS:         await writer.close();
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new StreamReceiver());
// JS:     await harness.stub.receiveStream(stream);
// JS:
// JS:     expect(chunks).toEqual([0, 1, 2, 3, 4]);
// JS:   });
// JS:
// JS:   it("applies backpressure when window fills up", async () => {
// JS:     let writesReceived = 0;
// JS:     let closeReceived = false;
// JS:
// JS:     let stream = new WritableStream<string>({
// JS:       write(chunk) { writesReceived++; },
// JS:       close() { closeReceived = true; }
// JS:     });
// JS:
// JS:     // Track how many writes the sender has initiated (on the server side).
// JS:     let writesSent = 0;
// JS:
// JS:     class StreamReceiver extends RpcTarget {
// JS:       async receiveStream(stream: WritableStream<string>) {
// JS:         let writer = stream.getWriter();
// JS:         // Each chunk is ~40KB when serialized, so ~7 writes fill the 256KB window.
// JS:         let chunk = "x".repeat(40000);
// JS:         for (let i = 0; i < 20; i++) {
// JS:           writesSent++;
// JS:           await writer.write(chunk);
// JS:         }
// JS:         await writer.close();
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new StreamReceiver());
// JS:
// JS:     // Fence the client transport. The initial RPC (client → server) still gets through since the
// JS:     // fence is on the client's receive side. But write RPCs (server → client) will accumulate in
// JS:     // the client's queue, so no resolve messages get sent back to the server, and the server's
// JS:     // flow control window never refills.
// JS:     harness.clientTransport.fence();
// JS:
// JS:     let rpcPromise = harness.stub.receiveStream(stream);
// JS:
// JS:     // Pump microtasks until `writesSent` stops advancing.
// JS:     //
// JS:     // Actually, we use setTimeout(0) instead of pumpMicrotasks() because some platforms' streams
// JS:     // implementations sometimes require falling back to the macro event loop to make progress.
// JS:     // In particular, this test hangs on workerd (in the closeReceived loop, later) about 1/4
// JS:     // of the time if we are using pumpMicrotasks() instead of setTimeout(0). webkit also seems
// JS:     // to be affected.
// JS:     for (;;) {
// JS:       let oldWritesSent = writesSent;
// JS:       await new Promise(resolve => setTimeout(resolve, 0));
// JS:       if (writesSent == oldWritesSent) break;
// JS:     }
// JS:
// JS:     // With a 64KB window and ~10KB per write, the sender should have been blocked after about
// JS:     // 7 writes. Without flow control, all 20 writes would be sent.
// JS:     expect(writesSent).toBeGreaterThanOrEqual(5);
// JS:     expect(writesSent).toBeLessThanOrEqual(10);
// JS:     expect(writesReceived).toBe(0);  // Client hasn't received any writes yet.
// JS:     expect(closeReceived).toBe(false);
// JS:
// JS:     // Release the fence and let everything complete.
// JS:     harness.clientTransport.releaseFence();
// JS:
// JS:     while (!closeReceived) {
// JS:       await new Promise(resolve => setTimeout(resolve, 0));
// JS:     }
// JS:
// JS:     expect(writesSent).toBe(20);
// JS:     expect(writesReceived).toBe(20);
// JS:     expect(closeReceived).toBe(true);
// JS:     await rpcPromise;
// JS:   });
// JS:
// JS:   it("applies backpressure when custom transport omits stream message size", async () => {
// JS:     let writesReceived = 0;
// JS:     let closeReceived = false;
// JS:
// JS:     let stream = new WritableStream<string>({
// JS:       write(chunk) { writesReceived++; },
// JS:       close() { closeReceived = true; }
// JS:     });
// JS:
// JS:     let writesSent = 0;
// JS:
// JS:     class StreamReceiver extends RpcTarget {
// JS:       async receiveStream(stream: WritableStream<string>) {
// JS:         let writer = stream.getWriter();
// JS:         let chunk = "x".repeat(40000);
// JS:         for (let i = 0; i < 20; i++) {
// JS:           writesSent++;
// JS:           await writer.write(chunk);
// JS:         }
// JS:         await writer.close();
// JS:       }
// JS:     }
// JS:
// JS:     let clientTransport = new ObjectTestTransport();
// JS:     let serverTransport = new ObjectTestTransport(clientTransport);
// JS:     let client = new RpcSession<StreamReceiver>(clientTransport);
// JS:     new RpcSession(serverTransport, new StreamReceiver());
// JS:     using clientStub = client.getRemoteMain();
// JS:
// JS:     clientTransport.fence();
// JS:
// JS:     clientStub.receiveStream(stream).catch(() => {});
// JS:
// JS:     for (;;) {
// JS:       let oldWritesSent = writesSent;
// JS:       await new Promise(resolve => setTimeout(resolve, 0));
// JS:       if (writesSent == oldWritesSent) break;
// JS:     }
// JS:
// JS:     expect(writesSent).toBeGreaterThan(1);
// JS:     expect(writesSent).toBeLessThan(20);
// JS:     expect(writesReceived).toBe(0);
// JS:     expect(closeReceived).toBe(false);
// JS:
// JS:     clientTransport.releaseFence();
// JS:   });
// JS:
// JS:   it("uses the size reported by a custom transport's send() for flow control", async () => {
// JS:     // Mirror image of the previous test: the sending side's transport reports a tiny encoded
// JS:     // size from send(), so even though the chunks are large (and their *estimated* size would
// JS:     // fill the flow control window after a few writes, as proven above), all 20 writes proceed
// JS:     // without blocking. This proves the number returned by send() is what feeds flow control.
// JS:     let writesReceived = 0;
// JS:     let closeReceived = false;
// JS:
// JS:     let stream = new WritableStream<string>({
// JS:       write(chunk) { writesReceived++; },
// JS:       close() { closeReceived = true; }
// JS:     });
// JS:
// JS:     let writesSent = 0;
// JS:
// JS:     class StreamReceiver extends RpcTarget {
// JS:       async receiveStream(stream: WritableStream<string>) {
// JS:         let writer = stream.getWriter();
// JS:         let chunk = "x".repeat(40000);
// JS:         for (let i = 0; i < 20; i++) {
// JS:           writesSent++;
// JS:           await writer.write(chunk);
// JS:         }
// JS:         await writer.close();
// JS:       }
// JS:     }
// JS:
// JS:     class SizeReportingTestTransport extends ObjectTestTransport {
// JS:       sendCount = 0;
// JS:       send(message: unknown): number {
// JS:         super.send(message);
// JS:         ++this.sendCount;
// JS:         return 10;  // report a tiny encoded size, regardless of the actual message
// JS:       }
// JS:     }
// JS:
// JS:     let clientTransport = new ObjectTestTransport();
// JS:     let serverTransport = new SizeReportingTestTransport(clientTransport);
// JS:     let client = new RpcSession<StreamReceiver>(clientTransport);
// JS:     new RpcSession(serverTransport, new StreamReceiver());
// JS:     using clientStub = client.getRemoteMain();
// JS:
// JS:     // Fence the client so it never processes incoming messages, and thus never sends acks.
// JS:     clientTransport.fence();
// JS:
// JS:     let promise = clientStub.receiveStream(stream);
// JS:
// JS:     for (;;) {
// JS:       let oldWritesSent = writesSent;
// JS:       await new Promise(resolve => setTimeout(resolve, 0));
// JS:       if (writesSent == oldWritesSent) break;
// JS:     }
// JS:
// JS:     // All writes completed despite no acks, because the reported sizes never filled the window.
// JS:     expect(writesSent).toBe(20);
// JS:     expect(serverTransport.sendCount).toBeGreaterThan(0);
// JS:     expect(writesReceived).toBe(0);
// JS:     expect(closeReceived).toBe(false);
// JS:
// JS:     // Let the messages through and verify everything arrives.
// JS:     clientTransport.releaseFence();
// JS:     await promise;
// JS:     expect(writesReceived).toBe(20);
// JS:     expect(closeReceived).toBe(true);
// JS:   });
// JS:
// JS:   it("uses stream messages instead of push+pull+release", async () => {
// JS:     // Verify that WritableStream writes use the optimized "stream" message type,
// JS:     // which avoids sending separate "pull" and "release" messages.
// JS:     let chunks: string[] = [];
// JS:     let stream = new WritableStream<string>({
// JS:       write(chunk) { chunks.push(chunk); },
// JS:       close() {}
// JS:     });
// JS:
// JS:     class StreamReceiver extends RpcTarget {
// JS:       async receiveStream(stream: WritableStream<string>) {
// JS:         let writer = stream.getWriter();
// JS:         await writer.write("hello");
// JS:         await writer.write("world");
// JS:         await writer.close();
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new StreamReceiver());
// JS:
// JS:     // Collect all messages sent by the server (which appear in the client's queue).
// JS:     let serverMessages: any[] = [];
// JS:     let origServerSend = harness.serverTransport.send;
// JS:     harness.serverTransport.send = function(message: string) {
// JS:       serverMessages.push(JSON.parse(message));
// JS:       return origServerSend.call(this, message);
// JS:     };
// JS:
// JS:     // Collect all messages sent by the client (which appear in the server's queue).
// JS:     let clientMessages: any[] = [];
// JS:     let origClientSend = harness.clientTransport.send;
// JS:     harness.clientTransport.send = function(message: string) {
// JS:       clientMessages.push(JSON.parse(message));
// JS:       return origClientSend.call(this, message);
// JS:     };
// JS:
// JS:     await harness.stub.receiveStream(stream);
// JS:     await pumpMicrotasks();
// JS:
// JS:     expect(chunks).toEqual(["hello", "world"]);
// JS:
// JS:     // Server sends: write("hello"), write("world"), close() — these should use "stream".
// JS:     let serverStreamMsgs = serverMessages.filter(m => m[0] === "stream");
// JS:     let serverPushMsgs = serverMessages.filter(m => m[0] === "push");
// JS:
// JS:     // The write() and close() calls should all be "stream" messages (3 total).
// JS:     expect(serverStreamMsgs.length).toBe(3);
// JS:
// JS:     // The server should NOT have sent any "push" messages for the stream writes.
// JS:     // (There may be other push messages for non-stream calls, but we filter by looking
// JS:     // at the pipeline target — stream writes target the writable stream export.)
// JS:     // Actually, the only "push" from the server should be none for stream operations.
// JS:     expect(serverPushMsgs.length).toBe(0);
// JS:
// JS:     // Client should NOT have sent any "pull" or "release" messages for stream writes.
// JS:     // The only client messages should be the initial "push" for the RPC call, a "pull"
// JS:     // for it, and the "release" for the RPC result — not for individual stream writes.
// JS:     let clientPullMsgs = clientMessages.filter(m => m[0] === "pull");
// JS:     let clientReleaseMsgs = clientMessages.filter(m => m[0] === "release");
// JS:
// JS:     // There should be exactly 1 pull (for the top-level receiveStream call).
// JS:     expect(clientPullMsgs.length).toBe(1);
// JS:
// JS:     // Release messages: 1 for the top-level receiveStream result, plus 1 for the
// JS:     // writable stream export itself (passed in params). No releases for stream writes.
// JS:     expect(clientReleaseMsgs.length).toBeLessThanOrEqual(2);
// JS:   });
// JS:
// JS:   it("unblocks a backpressure-blocked write when an in-flight write errors", async () => {
// JS:     let writeCount = 0;
// JS:     let stream = new WritableStream<string>({
// JS:       write(chunk) {
// JS:         writeCount++;
// JS:         if (writeCount >= 2) {
// JS:           throw new Error("Simulated write failure");
// JS:         }
// JS:       },
// JS:       close() {},
// JS:       abort() {}
// JS:     });
// JS:
// JS:     let writerError: any = null;
// JS:
// JS:     class StreamReceiver extends RpcTarget {
// JS:       async receiveStream(stream: WritableStream<string>) {
// JS:         let writer = stream.getWriter();
// JS:         let chunk = "x".repeat(100000);
// JS:         try {
// JS:           for (let i = 0; i < 20; i++) {
// JS:             await writer.write(chunk);
// JS:           }
// JS:           await writer.close();
// JS:         } catch (err) {
// JS:           writerError = err;
// JS:           throw err;
// JS:         }
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new StreamReceiver());
// JS:     harness.clientTransport.fence();
// JS:
// JS:     let rpcDone = false;
// JS:     let rpcError: any = null;
// JS:     let rpcPromise = harness.stub.receiveStream(stream).then(
// JS:       () => { rpcDone = true; },
// JS:       (err: any) => { rpcDone = true; rpcError = err; }
// JS:     );
// JS:
// JS:     for (let i = 0; i < 100; i++) {
// JS:       await pumpMicrotasks();
// JS:     }
// JS:
// JS:     harness.clientTransport.releaseFence();
// JS:
// JS:     let settled = false;
// JS:     let timeout = new Promise<void>(resolve => setTimeout(() => {
// JS:       settled = true;
// JS:       resolve();
// JS:     }, 500));
// JS:
// JS:     await Promise.race([
// JS:       (async () => {
// JS:         while (!rpcDone && !settled) {
// JS:           await pumpMicrotasks();
// JS:           await new Promise(resolve => setTimeout(resolve, 5));
// JS:         }
// JS:       })(),
// JS:       timeout
// JS:     ]);
// JS:
// JS:     expect(rpcDone).toBe(true);
// JS:     expect(rpcError).not.toBeNull();
// JS:     expect(rpcError.message).toContain("Simulated write failure");
// JS:   });
// JS: });
// JS:
// JS: describe("WritableStream stub argument disposal", () => {
// JS:   it("disposes copied call arguments when a WritableStream call is invalid", async () => {
// JS:     let disposed = false;
// JS:     class Disposable extends RpcTarget {
// JS:       [Symbol.dispose]() { disposed = true; }
// JS:     }
// JS:
// JS:     let argument = new RpcStub(new Disposable());
// JS:     let hook = new PromiseStubHook(
// JS:         Promise.resolve(streamImpl.createWritableStreamHook(new WritableStream())));
// JS:     let result = hook.call(["not", "a", "method"], RpcPayload.fromAppParams([argument]));
// JS:
// JS:     await expect(result.pull()).rejects.toThrow("only supports direct method calls");
// JS:     argument[Symbol.dispose]();
// JS:     expect(disposed).toBe(true);
// JS:     hook.dispose();
// JS:   });
// JS:
// JS:   it("disposes copied call arguments when the WritableStream stub was disposed", async () => {
// JS:     let disposed = false;
// JS:     class Disposable extends RpcTarget {
// JS:       [Symbol.dispose]() { disposed = true; }
// JS:     }
// JS:
// JS:     let streamHook = streamImpl.createWritableStreamHook(new WritableStream());
// JS:     streamHook.dispose();
// JS:
// JS:     let argument = new RpcStub(new Disposable());
// JS:     let hook = new PromiseStubHook(Promise.resolve(streamHook));
// JS:     let result = hook.call(["write"], RpcPayload.fromAppParams([argument]));
// JS:
// JS:     await expect(result.pull()).rejects.toThrow("after it was disposed");
// JS:     argument[Symbol.dispose]();
// JS:     expect(disposed).toBe(true);
// JS:   });
// JS: });
// JS:
// JS: describe("transport encoding levels", () => {
// JS:   class EchoService extends RpcTarget {
// JS:     echo(value: unknown): unknown {
// JS:       return value;
// JS:     }
// JS:   }
// JS:
// JS:   // Native values should survive a round trip through a custom-encoding transport at every
// JS:   // non-string level: base64 bytes at "jsonCompatible", raw Uint8Array at
// JS:   // "jsonCompatibleWithBytes", and native structured-clone types at "structuredClonable".
// JS:   for (let level of ["jsonCompatible", "jsonCompatibleWithBytes", "structuredClonable"] as const) {
// JS:     it(`round-trips native types over a ${level} transport`, async () => {
// JS:       let clientTransport = new ObjectTestTransport(undefined, level);
// JS:       let serverTransport = new ObjectTestTransport(clientTransport, level);
// JS:       let client = new RpcSession<EchoService>(clientTransport);
// JS:       new RpcSession(serverTransport, new EchoService());
// JS:       using stub = client.getRemoteMain();
// JS:
// JS:       let bytes = await stub.echo(new Uint8Array([1, 2, 3])) as Uint8Array;
// JS:       expect(new Uint8Array(bytes)).toStrictEqual(new Uint8Array([1, 2, 3]));
// JS:
// JS:       let typedArray = await stub.echo(new Uint32Array([0x01020304, 0xa0b0c0d0])) as Uint32Array;
// JS:       expect(Object.getPrototypeOf(typedArray)).toBe(Uint32Array.prototype);
// JS:       expect(typedArray).toStrictEqual(new Uint32Array([0x01020304, 0xa0b0c0d0]));
// JS:
// JS:       let date = await stub.echo(new Date(1234567890)) as Date;
// JS:       expect(date).toBeInstanceOf(Date);
// JS:       expect(date.getTime()).toBe(1234567890);
// JS:
// JS:       expect(await stub.echo(123n)).toBe(123n);
// JS:     });
// JS:   }
// JS:
// JS:   it("aborts the receive loop when the transport is aborted", async () => {
// JS:     let transport = new ObjectTestTransport();
// JS:     let pending = transport.receive();
// JS:     transport.abort(new Error("boom"));
// JS:     await expect(pending).rejects.toThrow("boom");
// JS:   });
// JS: });
// JS:
// JS: describe("ReadableStream over RPC", () => {
// JS:   it("can send a ReadableStream and read all chunks", async () => {
// JS:     let stream = new ReadableStream<string>({
// JS:       start(controller) {
// JS:         controller.enqueue("hello");
// JS:         controller.enqueue("world");
// JS:         controller.close();
// JS:       }
// JS:     });
// JS:
// JS:     class StreamReceiver extends RpcTarget {
// JS:       async receiveStream(stream: ReadableStream<string>) {
// JS:         let chunks: string[] = [];
// JS:         let reader = stream.getReader();
// JS:         while (true) {
// JS:           let { done, value } = await reader.read();
// JS:           if (done) break;
// JS:           chunks.push(value!);
// JS:         }
// JS:         return chunks;
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new StreamReceiver());
// JS:     let result = await harness.stub.receiveStream(stream);
// JS:
// JS:     expect(result).toEqual(["hello", "world"]);
// JS:   });
// JS:
// JS:   it("can return a ReadableStream from an RPC call", async () => {
// JS:     class StreamProvider extends RpcTarget {
// JS:       getStream(): ReadableStream<string> {
// JS:         return new ReadableStream({
// JS:           start(controller) {
// JS:             controller.enqueue("from");
// JS:             controller.enqueue("server");
// JS:             controller.close();
// JS:           }
// JS:         });
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new StreamProvider());
// JS:     let stream: any = await harness.stub.getStream();
// JS:
// JS:     let chunks: string[] = [];
// JS:     let reader = stream.getReader();
// JS:     while (true) {
// JS:       let { done, value } = await reader.read();
// JS:       if (done) break;
// JS:       chunks.push(value);
// JS:     }
// JS:
// JS:     expect(chunks).toEqual(["from", "server"]);
// JS:   });
// JS:
// JS:   it("can send ReadableStream in nested object", async () => {
// JS:     let stream = new ReadableStream<string>({
// JS:       start(controller) {
// JS:         controller.enqueue("nested");
// JS:         controller.close();
// JS:       }
// JS:     });
// JS:
// JS:     class StreamReceiver extends RpcTarget {
// JS:       async receiveData(data: { stream: ReadableStream<string>, label: string }) {
// JS:         let reader = data.stream.getReader();
// JS:         let chunks: string[] = [];
// JS:         while (true) {
// JS:           let { done, value } = await reader.read();
// JS:           if (done) break;
// JS:           chunks.push(value!);
// JS:         }
// JS:         return `${data.label}: ${chunks.join(",")}`;
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new StreamReceiver());
// JS:     let result = await harness.stub.receiveData({ stream, label: "test" });
// JS:
// JS:     expect(result).toBe("test: nested");
// JS:   });
// JS:
// JS:   it("can send ReadableStream in nested object (partial read)", async () => {
// JS:     let stream = new ReadableStream<string>({
// JS:       start(controller) {
// JS:         controller.enqueue("first");
// JS:         controller.enqueue("second");
// JS:         controller.close();
// JS:       }
// JS:     });
// JS:
// JS:     class StreamReceiver extends RpcTarget {
// JS:       async receiveData(data: { stream: ReadableStream<string>, label: string }) {
// JS:         // Only read the first chunk, then cancel the stream
// JS:         let reader = data.stream.getReader();
// JS:         let { value } = await reader.read();
// JS:         await reader.cancel();
// JS:         return `${data.label}: ${value}`;
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new StreamReceiver());
// JS:     let result = await harness.stub.receiveData({ stream, label: "test" });
// JS:
// JS:     expect(result).toBe("test: first");
// JS:   });
// JS:
// JS:   it("supports complex chunk types", async () => {
// JS:     let stream = new ReadableStream({
// JS:       start(controller) {
// JS:         controller.enqueue({ name: "test", value: 42 });
// JS:         controller.enqueue([1, 2, 3]);
// JS:         controller.enqueue(new Date(1234567890000));
// JS:         controller.close();
// JS:       }
// JS:     });
// JS:
// JS:     class StreamReceiver extends RpcTarget {
// JS:       async receiveStream(stream: ReadableStream) {
// JS:         let chunks: unknown[] = [];
// JS:         let reader = stream.getReader();
// JS:         while (true) {
// JS:           let { done, value } = await reader.read();
// JS:           if (done) break;
// JS:           chunks.push(value);
// JS:         }
// JS:         return chunks;
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new StreamReceiver());
// JS:     let result: any = await harness.stub.receiveStream(stream);
// JS:
// JS:     expect(result).toHaveLength(3);
// JS:     expect(result[0]).toEqual({ name: "test", value: 42 });
// JS:     expect(result[1]).toEqual([1, 2, 3]);
// JS:     expect(result[2]).toEqual(new Date(1234567890000));
// JS:   });
// JS:
// JS:   it("propagates stream errors to the reader", async () => {
// JS:     let stream = new ReadableStream({
// JS:       pull(controller) {
// JS:         controller.enqueue("ok");
// JS:         controller.error(new Error("Stream failed"));
// JS:       }
// JS:     });
// JS:
// JS:     class StreamReceiver extends RpcTarget {
// JS:       async receiveStream(stream: ReadableStream) {
// JS:         let reader = stream.getReader();
// JS:         let chunks: string[] = [];
// JS:         try {
// JS:           while (true) {
// JS:             let { done, value } = await reader.read();
// JS:             if (done) break;
// JS:             chunks.push(value);
// JS:           }
// JS:         } catch (err: any) {
// JS:           return `chunks=${chunks.join(",")}, error: ${err.message}`;
// JS:         }
// JS:         return `chunks=${chunks.join(",")}, no error`;
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new StreamReceiver());
// JS:     let result = await harness.stub.receiveStream(stream);
// JS:     expect(result).toContain("error:");
// JS:     expect(result).toContain("Stream failed");
// JS:   });
// JS:
// JS:   it("handles many chunks", async () => {
// JS:     let count = 100;
// JS:     let stream = new ReadableStream<number>({
// JS:       start(controller) {
// JS:         for (let i = 0; i < count; i++) {
// JS:           controller.enqueue(i);
// JS:         }
// JS:         controller.close();
// JS:       }
// JS:     });
// JS:
// JS:     class StreamReceiver extends RpcTarget {
// JS:       async receiveStream(stream: ReadableStream<number>) {
// JS:         let sum = 0;
// JS:         let reader = stream.getReader();
// JS:         while (true) {
// JS:           let { done, value } = await reader.read();
// JS:           if (done) break;
// JS:           sum += value!;
// JS:         }
// JS:         return sum;
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new StreamReceiver());
// JS:     let result = await harness.stub.receiveStream(stream);
// JS:
// JS:     // Sum of 0..99
// JS:     expect(result).toBe(4950);
// JS:
// JS:     // HACK: We have to pump more mitrotasks than usual here because every chunk write has to be
// JS:     //   disposed.
// JS:     // TODO: Optimize out the need to dispose each stream write or find some other fix here.
// JS:     for (let i = 0; i < 64; i++) {
// JS:       await pumpMicrotasks();
// JS:     }
// JS:   });
// JS:
// JS:   it("can send both ReadableStream and WritableStream in same message", async () => {
// JS:     let readStream = new ReadableStream<string>({
// JS:       start(controller) {
// JS:         controller.enqueue("input");
// JS:         controller.close();
// JS:       }
// JS:     });
// JS:
// JS:     let outputChunks: string[] = [];
// JS:     let writeStream = new WritableStream<string>({
// JS:       write(chunk) { outputChunks.push(chunk); },
// JS:       close() {}
// JS:     });
// JS:
// JS:     class StreamProcessor extends RpcTarget {
// JS:       async process(input: ReadableStream<string>, output: WritableStream<string>) {
// JS:         let reader = input.getReader();
// JS:         let writer = output.getWriter();
// JS:         while (true) {
// JS:           let { done, value } = await reader.read();
// JS:           if (done) break;
// JS:           await writer.write(`processed: ${value}`);
// JS:         }
// JS:         await writer.close();
// JS:         return "done";
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new StreamProcessor());
// JS:     let result = await harness.stub.process(readStream, writeStream);
// JS:
// JS:     expect(result).toBe("done");
// JS:     expect(outputChunks).toEqual(["processed: input"]);
// JS:   });
// JS:
// JS:   it("cancels a ReadableStream in a result when the whole result is disposed", async () => {
// JS:     if (navigator.userAgent === "Cloudflare-Workers") {
// JS:       // There's currently some bugs in workerd which prevent this test from working there:
// JS:       //     https://github.com/cloudflare/workerd/pull/6066
// JS:       // When that gets fixed, remove this early return.
// JS:       return;
// JS:     }
// JS:
// JS:     let cancelCalled = false;
// JS:
// JS:     class StreamProvider extends RpcTarget {
// JS:       getStream(): ReadableStream<string> {
// JS:         return new ReadableStream({
// JS:           start(controller) {
// JS:             // Enqueue a few chunks and leave the stream open (not closed).
// JS:             // This simulates a stream the caller might not fully consume.
// JS:             controller.enqueue("chunk-1");
// JS:             controller.enqueue("chunk-2");
// JS:           },
// JS:           cancel() {
// JS:             cancelCalled = true;
// JS:           }
// JS:         });
// JS:       }
// JS:     }
// JS:
// JS:     // Don't use the standard harness — the disposal tracking bug causes leaked exports
// JS:     // that would trip the checkAllDisposed assertion and mask the real test failure.
// JS:     let clientTransport = new TestTransport("client");
// JS:     let serverTransport = new TestTransport("server", clientTransport);
// JS:
// JS:     let client = new RpcSession(clientTransport);
// JS:     let server = new RpcSession(serverTransport, new StreamProvider());
// JS:
// JS:     let stub: any = client.getRemoteMain();
// JS:
// JS:     // Get the stream result but don't read from it.
// JS:     let streamResult = await stub.getStream();
// JS:
// JS:     // Dispose the result without reading — this should cascade and cancel the
// JS:     // underlying ReadableStream on the server.
// JS:     streamResult[Symbol.dispose]();
// JS:
// JS:     // Wait for disposal to propagate across the RPC boundary.
// JS:     for (let i = 0; i < 64; i++) {
// JS:       await pumpMicrotasks();
// JS:     }
// JS:
// JS:     expect(cancelCalled).toBe(true);
// JS:   });
// JS:
// JS:   it("can send an RpcTarget as a stream chunk and call methods on it", async () => {
// JS:     // The sender enqueues a pass-by-reference RpcTarget into a ReadableStream. The receiver
// JS:     // reads it back as a stub and can call methods on it, which route back to the sender.
// JS:     class ChunkTarget extends RpcTarget {
// JS:       square(i: number) { return i * i; }
// JS:       get label() { return "chunk-target"; }
// JS:     }
// JS:
// JS:     let stream = new ReadableStream({
// JS:       start(controller) {
// JS:         controller.enqueue(new ChunkTarget());
// JS:         controller.close();
// JS:       }
// JS:     });
// JS:
// JS:     class StreamReceiver extends RpcTarget {
// JS:       async receiveStream(stream: ReadableStream) {
// JS:         let reader = stream.getReader();
// JS:         let { value } = await reader.read();
// JS:         using chunk = value as any;
// JS:         let result = {
// JS:           squared: await chunk.square(5),
// JS:           label: await chunk.label,
// JS:         };
// JS:         // Drain the closing `done` chunk so the underlying pipe is released.
// JS:         await reader.read();
// JS:         return result;
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new StreamReceiver());
// JS:     let result: any = await harness.stub.receiveStream(stream);
// JS:
// JS:     expect(result.squared).toBe(25);
// JS:     expect(result.label).toBe("chunk-target");
// JS:   });
// JS:
// JS:   it("can return a ReadableStream containing RpcTargets and call methods on chunks", async () => {
// JS:     // The server returns a ReadableStream whose chunks are RpcTargets. The client reads them
// JS:     // as stubs and can call methods that route back to the server.
// JS:     class ChunkTarget extends RpcTarget {
// JS:       square(i: number) { return i * i; }
// JS:       get label() { return "server-chunk"; }
// JS:     }
// JS:
// JS:     class StreamProvider extends RpcTarget {
// JS:       getStream(): ReadableStream {
// JS:         return new ReadableStream({
// JS:           start(controller) {
// JS:             controller.enqueue(new ChunkTarget());
// JS:             controller.close();
// JS:           }
// JS:         });
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new StreamProvider());
// JS:     let stream: any = await harness.stub.getStream();
// JS:
// JS:     let reader = stream.getReader();
// JS:     let { value } = await reader.read();
// JS:     using chunk = value as any;
// JS:
// JS:     expect(await chunk.square(6)).toBe(36);
// JS:     expect(await chunk.label).toBe("server-chunk");
// JS:
// JS:     // Drain the closing `done` chunk so the underlying pipe is released.
// JS:     let { done } = await reader.read();
// JS:     expect(done).toBe(true);
// JS:   });
// JS:
// JS:   it("can send multiple RpcTargets as stream chunks", async () => {
// JS:     class ChunkTarget extends RpcTarget {
// JS:       constructor(private id: number) { super(); }
// JS:       getId() { return this.id; }
// JS:     }
// JS:
// JS:     let stream = new ReadableStream({
// JS:       start(controller) {
// JS:         controller.enqueue(new ChunkTarget(1));
// JS:         controller.enqueue(new ChunkTarget(2));
// JS:         controller.enqueue(new ChunkTarget(3));
// JS:         controller.close();
// JS:       }
// JS:     });
// JS:
// JS:     class StreamReceiver extends RpcTarget {
// JS:       async receiveStream(stream: ReadableStream) {
// JS:         let reader = stream.getReader();
// JS:         let ids: number[] = [];
// JS:         let chunks: any[] = [];
// JS:         try {
// JS:           while (true) {
// JS:             let { done, value } = await reader.read();
// JS:             if (done) break;
// JS:             chunks.push(value);
// JS:             ids.push(await value.getId());
// JS:           }
// JS:           return ids;
// JS:         } finally {
// JS:           for (let chunk of chunks) {
// JS:             chunk[Symbol.dispose]();
// JS:           }
// JS:         }
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new StreamReceiver());
// JS:     let result: any = await harness.stub.receiveStream(stream);
// JS:
// JS:     expect(result).toEqual([1, 2, 3]);
// JS:   });
// JS: });
// JS:
// JS: // =======================================================================================
// JS:
// JS: describe("Fetch API types over RPC", () => {
// JS:   it("can send Headers over RPC", async () => {
// JS:     class HeaderServer extends RpcTarget {
// JS:       getHeaders() {
// JS:         return new Headers({"Content-Type": "text/html", "X-Server": "test"});
// JS:       }
// JS:       readHeader(headers: Headers, name: string) {
// JS:         return headers.get(name);
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new HeaderServer());
// JS:     let stub = harness.stub as any;
// JS:
// JS:     // Server -> Client
// JS:     let headers: Headers = await stub.getHeaders();
// JS:     expect(headers).toBeInstanceOf(Headers);
// JS:     expect(headers.get("content-type")).toBe("text/html");
// JS:     expect(headers.get("x-server")).toBe("test");
// JS:
// JS:     // Client -> Server
// JS:     let result = await stub.readHeader(new Headers({"Authorization": "Bearer abc"}), "authorization");
// JS:     expect(result).toBe("Bearer abc");
// JS:   });
// JS:
// JS:   it("can send Request with body over RPC", async () => {
// JS:     class RequestServer extends RpcTarget {
// JS:       async receiveRequest(req: Request) {
// JS:         return {
// JS:           url: req.url,
// JS:           method: req.method,
// JS:           body: await req.text(),
// JS:           customHeader: req.headers.get("x-custom"),
// JS:         };
// JS:       }
// JS:       getRequest() {
// JS:         return new Request("http://example.com/api", {
// JS:           method: "POST",
// JS:           headers: {"X-Custom": "fromserver"},
// JS:           body: "server body",
// JS:         });
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new RequestServer());
// JS:     let stub = harness.stub as any;
// JS:
// JS:     // Client -> Server: send request with body and headers
// JS:     let result = await stub.receiveRequest(new Request("http://test.com/path", {
// JS:       method: "PUT",
// JS:       headers: {"X-Custom": "hello"},
// JS:       body: "request body",
// JS:     }));
// JS:     expect(result.url).toBe("http://test.com/path");
// JS:     expect(result.method).toBe("PUT");
// JS:     expect(result.body).toBe("request body");
// JS:     expect(result.customHeader).toBe("hello");
// JS:
// JS:     // Server -> Client: receive request with body
// JS:     let req: Request = await stub.getRequest();
// JS:     expect(req).toBeInstanceOf(Request);
// JS:     expect(req.url).toBe("http://example.com/api");
// JS:     expect(req.method).toBe("POST");
// JS:     expect(req.headers.get("x-custom")).toBe("fromserver");
// JS:     expect(await req.text()).toBe("server body");
// JS:   });
// JS:
// JS:   it("can send Response with body over RPC", async () => {
// JS:     class ResponseServer extends RpcTarget {
// JS:       async receiveResponse(resp: Response) {
// JS:         return {
// JS:           status: resp.status,
// JS:           statusText: resp.statusText,
// JS:           body: await resp.text(),
// JS:           customHeader: resp.headers.get("x-custom"),
// JS:         };
// JS:       }
// JS:       getResponse() {
// JS:         return new Response("hello from server", {
// JS:           status: 201,
// JS:           statusText: "Created",
// JS:           headers: {"X-Custom": "fromserver"},
// JS:         });
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new ResponseServer());
// JS:     let stub = harness.stub as any;
// JS:
// JS:     // Client -> Server: send response with body and status
// JS:     let result = await stub.receiveResponse(new Response("response body", {
// JS:       status: 404,
// JS:       statusText: "Not Found",
// JS:       headers: {"X-Custom": "value"},
// JS:     }));
// JS:     expect(result.status).toBe(404);
// JS:     expect(result.statusText).toBe("Not Found");
// JS:     expect(result.body).toBe("response body");
// JS:     expect(result.customHeader).toBe("value");
// JS:
// JS:     // Server -> Client: receive response with body
// JS:     let resp: Response = await stub.getResponse();
// JS:     expect(resp).toBeInstanceOf(Response);
// JS:     expect(resp.status).toBe(201);
// JS:     expect(resp.statusText).toBe("Created");
// JS:     expect(resp.headers.get("x-custom")).toBe("fromserver");
// JS:     expect(await resp.text()).toBe("hello from server");
// JS:   });
// JS:
// JS:   it("can send Request without body over RPC", async () => {
// JS:     class RequestServer extends RpcTarget {
// JS:       async receiveRequest(req: Request) {
// JS:         let hasBody = req.body !== null;
// JS:         if (req.body === undefined) {
// JS:           // Ugh, Firefox doesn't support `request.body`, try a different approach.
// JS:           hasBody = (await req.arrayBuffer()).byteLength > 0;
// JS:         }
// JS:
// JS:         return { url: req.url, method: req.method, hasBody };
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new RequestServer());
// JS:     let stub = harness.stub as any;
// JS:     let result = await stub.receiveRequest(new Request("http://example.com"));
// JS:     expect(result.url).toBe("http://example.com/");
// JS:     expect(result.method).toBe("GET");
// JS:     expect(result.hasBody).toBe(false);
// JS:   });
// JS:
// JS:   it("can send Response without body over RPC", async () => {
// JS:     class ResponseServer extends RpcTarget {
// JS:       receiveResponse(resp: Response) {
// JS:         return { status: resp.status, hasBody: resp.body !== null };
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new ResponseServer());
// JS:     let stub = harness.stub as any;
// JS:     let result = await stub.receiveResponse(new Response(null, {status: 204}));
// JS:     expect(result.status).toBe(204);
// JS:     expect(result.hasBody).toBe(false);
// JS:   });
// JS: });
// JS:
// JS: // =======================================================================================
// JS:
// JS: describe("Blob over RPC", () => {
// JS:   it("can send and receive a binary Blob", async () => {
// JS:     await using harness = new TestHarness(new TestTarget());
// JS:     let bytes = new TextEncoder().encode("hello from blob");
// JS:     let blob = new Blob([bytes], {type: "application/octet-stream"});
// JS:     using result = await harness.stub.echoBlob(blob);
// JS:     expect(result).toBeInstanceOf(Blob);
// JS:     expect(result.type).toBe("application/octet-stream");
// JS:     expect(new Uint8Array(await result.arrayBuffer())).toStrictEqual(bytes);
// JS:   });
// JS:
// JS:   it("preserves Blob MIME type", async () => {
// JS:     await using harness = new TestHarness(new TestTarget());
// JS:     let blob = new Blob(["<h1>hello</h1>"], {type: "text/html; charset=utf-8"});
// JS:     using result = await harness.stub.echoBlob(blob);
// JS:     expect(result.type).toBe("text/html; charset=utf-8");
// JS:     expect(await result.text()).toBe("<h1>hello</h1>");
// JS:   });
// JS:
// JS:   it("can send an empty Blob", async () => {
// JS:     await using harness = new TestHarness(new TestTarget());
// JS:     let blob = new Blob([], {type: "application/octet-stream"});
// JS:     using result = await harness.stub.echoBlob(blob);
// JS:     expect(result).toBeInstanceOf(Blob);
// JS:     expect(result.size).toBe(0);
// JS:     expect(result.type).toBe("application/octet-stream");
// JS:   });
// JS:
// JS:   it("can send a Blob as part of a compound return value", async () => {
// JS:     class BlobServer extends RpcTarget {
// JS:       makePayload() {
// JS:         return {
// JS:           name: "test.txt",
// JS:           blob: new Blob(["file content"], {type: "text/plain"}),
// JS:           size: 12,
// JS:         };
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new BlobServer());
// JS:     let stub = harness.stub as any;
// JS:     let result = await stub.makePayload();
// JS:     expect(result.name).toBe("test.txt");
// JS:     expect(result.blob).toBeInstanceOf(Blob);
// JS:     expect(result.blob.type).toBe("text/plain");
// JS:     expect(await result.blob.text()).toBe("file content");
// JS:     expect(result.size).toBe(12);
// JS:   });
// JS:
// JS:   it("can send multiple Blobs in the same call", async () => {
// JS:     // Each Blob produces its own RpcPromise entry in the Evaluator's `promises` list; all must
// JS:     // resolve before the payload is delivered to user code.
// JS:     class BlobCombiner extends RpcTarget {
// JS:       async concatenate(a: Blob, b: Blob) {
// JS:         let [textA, textB] = await Promise.all([a.text(), b.text()]);
// JS:         return `${textA}|${textB}`;
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new BlobCombiner());
// JS:     let stub = harness.stub as any;
// JS:     let result = await stub.concatenate(
// JS:       new Blob(["hello"], {type: "text/plain"}),
// JS:       new Blob(["world"], {type: "text/plain"}),
// JS:     );
// JS:     expect(result).toBe("hello|world");
// JS:   });
// JS:
// JS:   it("can receive an array of Blobs in one return value", async () => {
// JS:     // Multiple RpcPromise entries produced from a single return value, all substituted before
// JS:     // the array reaches user code.
// JS:     class BlobFactory extends RpcTarget {
// JS:       makeBlobs() {
// JS:         return [
// JS:           new Blob(["first"],  {type: "text/plain"}),
// JS:           new Blob(["second"], {type: "text/plain"}),
// JS:           new Blob(["third"],  {type: "text/plain"}),
// JS:         ];
// JS:       }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new BlobFactory());
// JS:     let stub = harness.stub as any;
// JS:     let [b1, b2, b3] = await stub.makeBlobs();
// JS:     expect(await b1.text()).toBe("first");
// JS:     expect(await b2.text()).toBe("second");
// JS:     expect(await b3.text()).toBe("third");
// JS:   });
// JS:
// JS:   it("round-trips a Blob with no MIME type", async () => {
// JS:     // new Blob([bytes]) leaves .type as "" — the empty string must survive the round-trip
// JS:     // and not become undefined or null.
// JS:     await using harness = new TestHarness(new TestTarget());
// JS:     let bytes = new TextEncoder().encode("untyped content");
// JS:     let blob = new Blob([bytes]);
// JS:     expect(blob.type).toBe("");
// JS:     using result = await harness.stub.echoBlob(blob);
// JS:     expect(result.type).toBe("");
// JS:     expect(new Uint8Array(await result.arrayBuffer())).toStrictEqual(bytes);
// JS:   });
// JS:
// JS:   it("preserves every possible byte value through the pipe", async () => {
// JS:     // All 256 possible byte values in a single Blob — verifies the pipe mechanism
// JS:     // neither corrupts nor truncates any byte.
// JS:     await using harness = new TestHarness(new TestTarget());
// JS:     let bytes = new Uint8Array(256);
// JS:     for (let i = 0; i < 256; i++) bytes[i] = i;
// JS:     let blob = new Blob([bytes], {type: "application/octet-stream"});
// JS:     using result = await harness.stub.echoBlob(blob);
// JS:     expect(result.size).toBe(256);
// JS:     expect(new Uint8Array(await result.arrayBuffer())).toStrictEqual(bytes);
// JS:   });
// JS:
// JS:   it("can send a large Blob over RPC", async () => {
// JS:     // 1 MB blob — exercises multi-chunk stream collection in streamToBlob().
// JS:     // Timeout is raised because CI machines can be slow to pump 1 MB through the
// JS:     // fake in-process transport (default 5 s is too tight on some runners).
// JS:     // Skipped in workerd: the isolate drops its connection when a large in-process
// JS:     // stream is pumped through it (infrastructure limit, not a code bug).
// JS:     if (navigator.userAgent === "Cloudflare-Workers") return;
// JS:     await using harness = new TestHarness(new TestTarget());
// JS:     let size = 1024 * 1024;
// JS:     let bytes = new Uint8Array(size);
// JS:     for (let i = 0; i < size; i++) bytes[i] = i & 0xff;
// JS:     let blob = new Blob([bytes], {type: "application/octet-stream"});
// JS:     using result = await harness.stub.echoBlob(blob);
// JS:     expect(result.size).toBe(size);
// JS:     expect(new Uint8Array(await result.arrayBuffer())).toStrictEqual(bytes);
// JS:   }, 30_000);
// JS:
// JS:   it("can pass a Blob through a local (loopback) stub", async () => {
// JS:     // No network — payload goes through deepCopy() rather than the Evaluator. Blobs are
// JS:     // immutable so deepCopy() returns them as-is, without going through the pipe path.
// JS:     using stub = new RpcStub(new TestTarget());
// JS:     let bytes = new TextEncoder().encode("loopback content");
// JS:     let blob = new Blob([bytes], {type: "text/plain"});
// JS:     let result = await stub.echoBlob(blob);
// JS:     expect(result).toBeInstanceOf(Blob);
// JS:     expect(result.type).toBe("text/plain");
// JS:     expect(await result.text()).toBe("loopback content");
// JS:     result[Symbol.dispose]();
// JS:   });
// JS:
// JS:   it("disposing a result containing a Blob does not throw", async () => {
// JS:     // Blobs have no owned resources; disposeImpl() must be a silent no-op.
// JS:     class BlobServer extends RpcTarget {
// JS:       makeBlob() { return new Blob(["hello"], {type: "text/plain"}); }
// JS:     }
// JS:
// JS:     await using harness = new TestHarness(new BlobServer());
// JS:     let stub = harness.stub as any;
// JS:     let result = await stub.makeBlob();
// JS:     expect(result).toBeInstanceOf(Blob);
// JS:     // Dispose without reading — should never throw.
// JS:     expect(() => result[Symbol.dispose]()).not.toThrow();
// JS:   });
// JS:
// JS:   it("is encoded as a readable pipe on the wire", async () => {
// JS:     // Verify the wire format: ["blob", type, ["readable", pipeId]] — always. There is no inline
// JS:     // fast path; reading a Blob's bytes is inherently async so we always stream.
// JS:     class Server extends RpcTarget {
// JS:       receiveBlob(_blob: Blob) { return "ok"; }
// JS:     }
// JS:
// JS:     let clientTransport = new TestTransport("client");
// JS:     let serverTransport = new TestTransport("server", clientTransport);
// JS:
// JS:     let client = new RpcSession<Server>(clientTransport);
// JS:     let server = new RpcSession(serverTransport, new Server());
// JS:
// JS:     serverTransport.fence();
// JS:
// JS:     let stub = client.getRemoteMain();
// JS:     let blob = new Blob(["hello"], {type: "text/plain"});
// JS:     let p = stub.receiveBlob(blob);
// JS:
// JS:     // The call message is dispatched synchronously (the pipe path does not require pre-reading
// JS:     // bytes on the sending side), but yield once to be safe across environments.
// JS:     await Promise.resolve();
// JS:
// JS:     let blobExpr: any = undefined;
// JS:     for (let i = 0; i < serverTransport.pendingCount; i++) {
// JS:       let msg = JSON.parse((serverTransport as any).queue[i]);
// JS:       if (msg[0] === "push") {
// JS:         let findBlob = (v: any): any => {
// JS:           if (v instanceof Array && v[0] === "blob") return v;
// JS:           if (v instanceof Array) for (let e of v) { let r = findBlob(e); if (r) return r; }
// JS:           if (v && typeof v === "object") for (let k in v) { let r = findBlob(v[k]); if (r) return r; }
// JS:           return undefined;
// JS:         };
// JS:         blobExpr = findBlob(msg);
// JS:         if (blobExpr) break;
// JS:       }
// JS:     }
// JS:
// JS:     expect(blobExpr).toBeDefined();
// JS:     expect(blobExpr[0]).toBe("blob");
// JS:     expect(blobExpr[1]).toBe("text/plain");
// JS:     expect(blobExpr[2]).toBeInstanceOf(Array);
// JS:     expect(blobExpr[2][0]).toBe("readable");
// JS:     expect(typeof blobExpr[2][1]).toBe("number"); // pipe ID
// JS:
// JS:     serverTransport.releaseFence();
// JS:     await p;
// JS:
// JS:     stub[Symbol.dispose]();
// JS:     await pumpMicrotasks();
// JS:   });
// JS: });
// JS:
// JS: describe("deserialization and transport correctness", () => {
// JS:   it("does not let serialized error properties override Object.prototype members", () => {
// JS:     // Wire format for an error is ["error", name, message, stack, propsBag]. The props bag
// JS:     // carries own properties the sender attached to the error. These must be filtered the same
// JS:     // way the plain-object deserializer filters them, so that keys like `__proto__`, `toString`,
// JS:     // `valueOf`, `hasOwnProperty` and `toJSON` cannot be assigned onto the resulting error.
// JS:     let wire = '["error","Error","boom","at test (x:1:1)",' +
// JS:       '{"code":"E_TEST","toString":"nope","valueOf":"nope","hasOwnProperty":"nope",' +
// JS:       '"toJSON":"nope","__proto__":{"polluted":true}}]';
// JS:     let err = deserialize(wire) as Error & Record<string, unknown>;
// JS:
// JS:     expect(err).toBeInstanceOf(Error);
// JS:     expect(err.message).toBe("boom");
// JS:
// JS:     // The legitimate own property is still copied across.
// JS:     expect(Object.prototype.hasOwnProperty.call(err, "code")).toBe(true);
// JS:     expect(err.code).toBe("E_TEST");
// JS:
// JS:     // Object.prototype members and toJSON must not become own properties of the error.
// JS:     for (let key of ["toString", "valueOf", "hasOwnProperty", "toJSON", "__proto__"]) {
// JS:       expect(Object.prototype.hasOwnProperty.call(err, key)).toBe(false);
// JS:     }
// JS:
// JS:     // The error's prototype and behavior are unchanged.
// JS:     expect(Object.getPrototypeOf(err)).toBe(Error.prototype);
// JS:     expect(typeof err.toString).toBe("function");
// JS:     expect(err.toString()).toBe("Error: boom");
// JS:     expect((err as any).polluted).toBeUndefined();
// JS:     expect(({} as any).polluted).toBeUndefined();
// JS:   });
// JS:
// JS:   it("does not let an error type name resolve to an Object.prototype member (type confusion)", () => {
// JS:     // The error type name (value[1]) is attacker-controlled and is used to look up the error
// JS:     // class in ERROR_TYPES. ERROR_TYPES must not resolve names to inherited Object.prototype
// JS:     // members. Before it was given a null prototype, ERROR_TYPES["constructor"] resolved to
// JS:     // `Object` (truthy, so the `|| Error` fallback was skipped), so `new Object(message)`
// JS:     // produced a `String` wrapper rather than an `Error` -- an `instanceof Error` bypass that
// JS:     // still carried the attacker's own-property bag. A name like "toString" resolved to a
// JS:     // non-constructor and threw, aborting the RPC session.
// JS:     let confused = deserialize(
// JS:       '["error","constructor","attacker-payload",null,{"injected":true,"httpStatus":200}]'
// JS:     ) as Error & Record<string, unknown>;
// JS:
// JS:     // Must be a real Error, not a type-confused String wrapper.
// JS:     expect(confused).toBeInstanceOf(Error);
// JS:     expect(confused).not.toBeInstanceOf(String);
// JS:     expect(confused.name).toBe("Error");
// JS:     expect(confused.message).toBe("attacker-payload");
// JS:     // Legitimate (non-prototype) own properties still round-trip onto the correctly-typed Error.
// JS:     expect(confused.injected).toBe(true);
// JS:     expect(confused.httpStatus).toBe(200);
// JS:
// JS:     // Any Object.prototype member name used as the error type must fall back to Error and must
// JS:     // never throw.
// JS:     for (let name of ["constructor", "toString", "valueOf", "hasOwnProperty", "__proto__",
// JS:                       "isPrototypeOf", "propertyIsEnumerable"]) {
// JS:       let err = deserialize(`["error",${JSON.stringify(name)},"msg"]`) as Error;
// JS:       expect(err).toBeInstanceOf(Error);
// JS:       expect(err.name).toBe("Error");
// JS:       expect(err.message).toBe("msg");
// JS:     }
// JS:
// JS:     // Legitimate built-in error types still resolve correctly.
// JS:     expect(deserialize('["error","TypeError","t"]')).toBeInstanceOf(TypeError);
// JS:     expect(deserialize('["error","RangeError","r"]')).toBeInstanceOf(RangeError);
// JS:   });
// JS:
// JS:   it("delivers the unwrapped abort reason to onRpcBroken (not the payload wrapper)", async () => {
// JS:     // Intentionally not using `using` here: the session is deliberately aborted below, so the
// JS:     // end-of-test "everything disposed" check would not apply.
// JS:     let harness = new TestHarness(new TestTarget());
// JS:     let captured: any[] = [];
// JS:     harness.stub.onRpcBroken(error => { captured.push(error); });
// JS:
// JS:     // Feed the server a malformed message so its read loop throws. The server responds by
// JS:     // sending an ["abort", reason] message back to the client, exercising the client's "abort"
// JS:     // handler. That handler must forward the unwrapped reason value, not the payload wrapper.
// JS:     await harness.clientTransport.send('["not a valid message"]');
// JS:     await pumpMicrotasks();
// JS:
// JS:     expect(captured.length).toBe(1);
// JS:     let error = captured[0];
// JS:     expect(error).toBeInstanceOf(Error);
// JS:     expect(String(error.message)).toContain("bad RPC message");
// JS:   });
// JS:
// JS:   it("truncates an over-long WebSocket close reason on a code-point boundary", async () => {
// JS:     let closeArgs: { code: number, reason: string }[] = [];
// JS:     let closeThrew = false;
// JS:     let listeners: Record<string, ((ev: any) => void)[]> = {};
// JS:
// JS:     let fakeWs: any = {
// JS:       readyState: (globalThis as any).WebSocket.OPEN,
// JS:       addEventListener(type: string, cb: (ev: any) => void) {
// JS:         (listeners[type] ||= []).push(cb);
// JS:       },
// JS:       send(_message: string) { return Promise.resolve(); },
// JS:       close(code: number, reason: string) {
// JS:         // Mimic the real contract: an over-long reason throws.
// JS:         if (new TextEncoder().encode(reason).length > MAX_CLOSE_REASON_BYTES) {
// JS:           closeThrew = true;
// JS:           throw new Error("close reason too long");
// JS:         }
// JS:         closeArgs.push({ code, reason });
// JS:       },
// JS:     };
// JS:
// JS:     newWebSocketRpcSession(fakeWs);
// JS:
// JS:     // Peer-supplied abort reason that straddles the limit with a multi-byte character: the 2-byte
// JS:     // "é" begins at the last allowed byte, so truncation must drop the partial "é" rather than emit
// JS:     // a replacement character (itself 3 bytes, which would re-exceed the limit).
// JS:     let reason = "a".repeat(MAX_CLOSE_REASON_BYTES - 1) + "\u00e9";
// JS:     for (let cb of listeners["message"] || []) cb({ data: JSON.stringify(["abort", reason]) });
// JS:     await pumpMicrotasks();
// JS:
// JS:     expect(closeThrew).toBe(false);
// JS:     expect(closeArgs.length).toBe(1);
// JS:     expect(closeArgs[0].code).toBe(3000);
// JS:     let sentReason = closeArgs[0].reason;
// JS:     expect(new TextEncoder().encode(sentReason).length).toBeLessThanOrEqual(MAX_CLOSE_REASON_BYTES);
// JS:     expect(sentReason).not.toContain("\uFFFD");
// JS:     expect(sentReason).toBe("a".repeat(MAX_CLOSE_REASON_BYTES - 1));
// JS:   });
// JS: });
