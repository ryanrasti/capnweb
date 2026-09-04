// JS: // Copyright (c) 2026 Cloudflare, Inc.
// JS: // Licensed under the MIT license found in the LICENSE.txt file or at:
// JS: //     https://opensource.org/license/mit
// JS:
// JS: // Tests for the receive-side resource limits that guard against untrusted-peer resource
// JS: // exhaustion (issue #184): bigint length cap, message nesting depth, and max message size. See
// JS: // `RpcLimits` / `DEFAULT_LIMITS` in src/serialize.ts.
// JS:
// JS: import { expect, it, describe } from "vitest"
// JS: import { deserialize, serialize, RpcSession, RpcTransport, RpcTarget, DEFAULT_LIMITS,
use num_bigint::BigInt;
use std::str::FromStr;

use crate::core::CapnwebValue;
use crate::serialize::{DEFAULT_LIMITS, DEFAULT_MAX_DEPTH, deserialize, serialize};
// JS:          DEFAULT_MAX_DEPTH, type RpcLimits } from "../src/index.js"
// JS:
// JS: // Builds the wire string for a value nested inside `depth` escaped-array layers. Each escaped
// JS: // array `[[ inner ]]` adds one level of recursion to the deserializer (evaluateImpl recurses into
// JS: // its single element). The innermost value is the number 1.
// JS: function nestedEscapedArrays(depth: number): string {
fn nested_escaped_arrays(depth: usize) -> String {
    // JS:   return `${"[[".repeat(depth)}1${"]]".repeat(depth)}`;
    format!("{}1{}", "[[".repeat(depth), "]]".repeat(depth))
    // JS: }
}
// JS:
// JS: // Builds a pipeline call whose sole argument is another pipeline call, repeated `depth` times.
// JS: // This crosses the call-argument RpcPayload boundary at every level.
// JS: function nestedCallArgs(depth: number): string {
fn nested_call_args(depth: usize) -> String {
    // JS:   return `["pipeline",0,["echo"],[`.repeat(depth) + "1" + "]]".repeat(depth);
    format!(
        "{}1{}",
        "[\"pipeline\",0,[\"echo\"],[".repeat(depth),
        "]]".repeat(depth)
    )
    // JS: }
}
// JS:
// JS: // A one-shot transport that feeds a single pre-baked frame to the session and then blocks, while
// JS: // capturing everything the session sends back. Lets us assert exactly how the session reacts to a
// JS: // hostile incoming message.
// JS: class ScriptedTransport implements RpcTransport {
// JS:   sent: string[] = [];
// JS:   aborted = false;
// JS:   private toReceive: string[];
// JS:   private blockForever = new Promise<string>(() => {});
// JS:
// JS:   constructor(frames: string[]) {
// JS:     this.toReceive = [...frames];
// JS:   }
// JS:
// JS:   async send(message: string): Promise<void> {
// JS:     this.sent.push(message);
// JS:   }
// JS:
// JS:   async receive(): Promise<string> {
// JS:     let next = this.toReceive.shift();
// JS:     return next === undefined ? this.blockForever : next;
// JS:   }
// JS:
// JS:   abort(reason: any): void {
// JS:     this.aborted = true;
// JS:   }
// JS:
// JS:   // Resolves once the session has sent an ["abort", ...] frame, or rejects after a spin budget.
// JS:   async waitForAbort(): Promise<string> {
// JS:     for (let i = 0; i < 200; i++) {
// JS:       let abortFrame = this.sent.find(m => m.startsWith('["abort"'));
// JS:       if (abortFrame !== undefined) return abortFrame;
// JS:       await Promise.resolve();
// JS:     }
// JS:     throw new Error(`session did not abort; sent: ${JSON.stringify(this.sent)}`);
// JS:   }
// JS:
// JS:   async expectNoAbort(): Promise<void> {
// JS:     for (let i = 0; i < 50; i++) await Promise.resolve();
// JS:     let abortFrame = this.sent.find(m => m.startsWith('["abort"'));
// JS:     expect(abortFrame, "session unexpectedly aborted").toBeUndefined();
// JS:     expect(this.aborted, "transport unexpectedly aborted").toBe(false);
// JS:   }
// JS: }
// JS:
// JS: // Drives a server session that processes a single ["push", expr] frame, returning the transport so
// JS: // the caller can inspect the reaction.
// JS: function pushFrame(expr: string, limits?: Partial<RpcLimits>): ScriptedTransport {
// JS:   let transport = new ScriptedTransport([`["push",${expr}]`]);
// JS:   new RpcSession(transport, new EchoTarget(), limits ? { limits } : undefined);
// JS:   return transport;
// JS: }
// JS:
// JS: class EchoTarget extends RpcTarget {
// JS:   echo(value: unknown) { return value; }
// JS: }
// JS:
// JS: describe("bigint deserialization limits", () => {
mod bigint_deserialization_limits {
    use super::*;
    // JS:   it("accepts a decimal bigint at the default digit limit", () => {
    #[test]
    fn accepts_a_decimal_bigint_at_the_default_digit_limit() {
        // JS:     let digits = "9".repeat(DEFAULT_LIMITS.maxBigIntDigits);
        let digits = "9".repeat(DEFAULT_LIMITS.max_big_int_digits);
        // JS:     let value = deserialize(`["bigint","${digits}"]`);
        let value = deserialize(&format!("[\"bigint\",\"{digits}\"]")).unwrap();
        // JS:     expect(value).toBe(BigInt(digits));
        assert!(matches!(value, CapnwebValue::BigInt(value)
            if value == BigInt::from_str(&digits).unwrap()));
        // JS:   });
    }
    // JS:
    // JS:   it("rejects a bigint over the default digit limit", () => {
    #[test]
    fn rejects_a_bigint_over_the_default_digit_limit() {
        // JS:     let digits = "9".repeat(DEFAULT_LIMITS.maxBigIntDigits + 1);
        let digits = "9".repeat(DEFAULT_LIMITS.max_big_int_digits + 1);
        // JS:     expect(() => deserialize(`["bigint","${digits}"]`))
        let error = deserialize(&format!("[\"bigint\",\"{digits}\"]")).unwrap_err();
        // JS:         .toThrowError(/bigint exceeds maximum length/);
        assert!(error.to_string().contains("bigint exceeds maximum length"));
        // JS:   });
    }
    // JS:
    // JS:   it("round-trips bigints through the decimal wire format", () => {
    #[test]
    fn round_trips_bigints_through_the_decimal_wire_format() {
        // JS:     let values = [
        let large: BigInt = (BigInt::from(1_u8) << 4096_usize) + BigInt::from(123_456_789_u64);
        // JS:       0n,
        // JS:       255n,
        // JS:       -255n,
        // JS:       (1n << 4096n) + 123456789n,
        // JS:       -((1n << 4096n) + 123456789n),
        // JS:     ];
        let values = [
            BigInt::from(0),
            BigInt::from(255),
            BigInt::from(-255),
            large.clone(),
            -large,
        ];
        // JS:
        // JS:     for (let value of values) {
        for value in values {
            // JS:       let wire = serialize(value);
            let wire = serialize(&CapnwebValue::BigInt(value.clone())).unwrap();
            // JS:       expect(wire).toMatch(/^\["bigint","-?[0-9]+"\]$/);
            assert!(wire.starts_with("[\"bigint\",\"") && wire.ends_with("\"]"));
            // JS:       expect(deserialize(wire)).toBe(value);
            assert!(
                matches!(deserialize(&wire).unwrap(), CapnwebValue::BigInt(decoded)
                if decoded == value)
            );
            // JS:     }
        }
        // JS:   });
    }
    // JS:
    // JS:   it("accepts decimal bigint strings", () => {
    #[test]
    fn accepts_decimal_bigint_strings() {
        // JS:     expect(deserialize('["bigint","123"]')).toBe(123n);
        assert!(matches!(deserialize("[\"bigint\",\"123\"]").unwrap(),
            CapnwebValue::BigInt(value) if value == BigInt::from(123)));
        // JS:     expect(deserialize('["bigint","-123"]')).toBe(-123n);
        assert!(matches!(deserialize("[\"bigint\",\"-123\"]").unwrap(),
            CapnwebValue::BigInt(value) if value == BigInt::from(-123)));
        // JS:   });
    }
    // JS:
    // JS:   it("honors a per-session override of maxBigIntDigits", async () => {
    // JS:     // The cap counts the full wire string length. "1234" is 4 characters.
    // JS:     await pushFrame('["bigint","1234"]', { maxBigIntDigits: 4 }).expectNoAbort();
    // JS:
    // JS:     let aborted = await pushFrame('["bigint","12345"]', { maxBigIntDigits: 4 }).waitForAbort();
    // JS:     expect(aborted).toMatch(/bigint exceeds maximum length/);
    // JS:
    // JS:     // The leading "-" counts toward the length, so "-123" is 4 characters (within a limit of 4)
    // JS:     // while "-1234" is 5 and aborts.
    // JS:     await pushFrame('["bigint","-123"]', { maxBigIntDigits: 4 }).expectNoAbort();
    // JS:
    // JS:     let abortedNeg = await pushFrame('["bigint","-1234"]', { maxBigIntDigits: 4 }).waitForAbort();
    // JS:     expect(abortedNeg).toMatch(/bigint exceeds maximum length/);
    // JS:
    // JS:     // The 5-digit value is well under the default limit, so it does NOT abort by default.
    // JS:     await pushFrame('["bigint","12345"]').expectNoAbort();
    // JS:   });
    // JS: });
}
// JS:
// JS: describe("message depth limits", () => {
mod message_depth_limits {
    use super::*;
    // JS:   it("accepts nesting just under the default depth limit", () => {
    #[test]
    fn accepts_nesting_just_under_the_default_depth_limit() {
        // JS:     // The top value sits at depth 0, so maxDepth-1 escaped-array layers stay within the limit.
        // JS:     let value = deserialize(nestedEscapedArrays(DEFAULT_LIMITS.maxDepth - 1));
        let value = deserialize(&nested_escaped_arrays(DEFAULT_LIMITS.max_depth - 1)).unwrap();
        // JS:     expect(value).toBeInstanceOf(Array);
        assert!(matches!(value, CapnwebValue::Array(_)));
        // JS:   });
    }
    // JS:
    // JS:   it("rejects nesting over the default depth limit", () => {
    #[test]
    fn rejects_nesting_over_the_default_depth_limit() {
        // JS:     expect(() => deserialize(nestedEscapedArrays(DEFAULT_LIMITS.maxDepth + 5)))
        let error = deserialize(&nested_escaped_arrays(DEFAULT_LIMITS.max_depth + 5)).unwrap_err();
        // JS:         .toThrowError(/maximum allowed message depth/);
        assert!(error.to_string().contains("maximum allowed message depth"));
        // JS:   });
    }
    // JS:
    // JS:   it("honors a per-session override of maxDepth", async () => {
    // JS:     // Override depth to a small value; nesting beyond it aborts the session.
    // JS:     let deep = nestedEscapedArrays(10);
    // JS:     let aborted = await pushFrame(deep, { maxDepth: 4 }).waitForAbort();
    // JS:     expect(aborted).toMatch(/maximum allowed message depth/);
    // JS:
    // JS:     // The same depth is well within the default, so by default it does not abort.
    // JS:     await pushFrame(deep).expectNoAbort();
    // JS:   });
    // JS:
    // JS:   it("does not reset depth across nested call arguments", async () => {
    // JS:     let aborted = await pushFrame(nestedCallArgs(8), { maxDepth: 8 }).waitForAbort();
    // JS:     expect(aborted).toMatch(/maximum allowed message depth/);
    // JS:   });
    // JS:
    // JS:   it("accepts nested call arguments within the default depth limit", async () => {
    // JS:     await pushFrame(nestedCallArgs(64)).expectNoAbort();
    // JS:   });
    // JS:
    // JS:   it("accepts near-limit call arguments that the default sender can serialize", async () => {
    // JS:     let appArg = deserialize(nestedEscapedArrays(DEFAULT_MAX_DEPTH - 2));
    // JS:     let wireArgs = JSON.parse(serialize([appArg]))[0];
    // JS:     await pushFrame(`["pipeline",0,["echo"],${JSON.stringify(wireArgs)}]`).expectNoAbort();
    // JS:   });
    // JS: });
}
// JS:
// JS: describe("message size limits", () => {
// JS:   it("aborts the session on a message over the size limit", async () => {
// JS:     // The pushed expression is a long-but-valid JSON string literal, so the resulting frame is
// JS:     // valid JSON and well over the 64-char override. The size check runs (and aborts) before
// JS:     // JSON.parse, so this exercises the size guard specifically, not a parse error.
// JS:     let aborted = await pushFrame(`"${"a".repeat(200)}"`, { maxMessageSize: 64 }).waitForAbort();
// JS:     expect(aborted).toMatch(/exceeds maximum size/);
// JS:   });
// JS:
// JS:   it("does not abort on a message under the size limit", async () => {
// JS:     await pushFrame('"small"', { maxMessageSize: 1024 }).expectNoAbort();
// JS:   });
// JS: });
// JS:
// JS: describe("limits backwards compatibility", () => {
mod limits_backwards_compatibility {
    use super::*;
    // JS:   it("DEFAULT_LIMITS has the documented values", () => {
    #[test]
    fn default_limits_have_the_documented_values() {
        // JS:     expect(DEFAULT_MAX_DEPTH).toBe(256);
        assert_eq!(DEFAULT_MAX_DEPTH, 256);
        // JS:     expect(DEFAULT_LIMITS).toStrictEqual({
        // JS:       maxBigIntDigits: 16384,
        // JS:       maxDepth: DEFAULT_MAX_DEPTH,
        // JS:       maxMessageSize: 32 * 1024 * 1024,
        // JS:     });
        assert_eq!(DEFAULT_LIMITS.max_big_int_digits, 16_384);
        assert_eq!(DEFAULT_LIMITS.max_depth, DEFAULT_MAX_DEPTH);
        assert_eq!(DEFAULT_LIMITS.max_message_size, 32 * 1024 * 1024);
        // JS:   });
    }
    // JS:
    // JS:   it("standalone deserialize round-trips normal values unchanged with no config", () => {
    #[test]
    fn standalone_deserialize_round_trips_normal_values_without_config() {
        // JS:     expect(deserialize('["bigint","123"]')).toBe(123n);
        assert!(matches!(deserialize("[\"bigint\",\"123\"]").unwrap(),
            CapnwebValue::BigInt(value) if value == BigInt::from(123)));
        // JS:     expect(deserialize(serialize(12345678901234567890n))).toBe(12345678901234567890n);
        let bigint = BigInt::from_str("12345678901234567890").unwrap();
        let wire = serialize(&CapnwebValue::BigInt(bigint.clone())).unwrap();
        assert!(
            matches!(deserialize(&wire).unwrap(), CapnwebValue::BigInt(value) if value == bigint)
        );
        // JS:     expect(deserialize('[[[[123,456]]]]')).toStrictEqual([[123, 456]]);
        assert!(
            matches!(deserialize("[[[[123,456]]]]").unwrap(), CapnwebValue::Array(outer)
            if matches!(&outer[..], [CapnwebValue::Array(inner)]
                if matches!(&inner[..], [CapnwebValue::Number(123.0), CapnwebValue::Number(456.0)])))
        );
        // JS:     expect(deserialize('{"foo":{"bar":123}}')).toStrictEqual({foo: {bar: 123}});
        let CapnwebValue::Object(object) = deserialize("{\"foo\":{\"bar\":123}}").unwrap() else {
            panic!()
        };
        assert!(matches!(&object["foo"], CapnwebValue::Object(foo)
            if matches!(foo.get("bar"), Some(CapnwebValue::Number(123.0)))));
        // JS:   });
    }
    // JS:
    // JS:   it("a session constructed with no options behaves exactly as before", async () => {
    // JS:     // A legitimate bigint, normal nesting, and a normal call all flow through a default session
    // JS:     // without aborting.
    // JS:     using harness = new SessionPair(new EchoTarget());
    // JS:
    // JS:     let stub = harness.stub as any;
    // JS:     expect(await stub.echo(123n)).toBe(123n);
    // JS:     expect(await stub.echo([[1, 2, 3]])).toStrictEqual([[1, 2, 3]]);
    // JS:     expect(await stub.echo({a: {b: {c: 1}}})).toStrictEqual({a: {b: {c: 1}}});
    // JS:
    // JS:     await harness.dispose();
    // JS:   });
    // JS:
    // JS:   it("a session constructed with an empty options object behaves identically", async () => {
    // JS:     using harness = new SessionPair(new EchoTarget(), {});
    // JS:     let stub = harness.stub as any;
    // JS:     expect(await stub.echo(999n)).toBe(999n);
    // JS:     await harness.dispose();
    // JS:   });
    // JS: });
}
// JS:
// JS: // A minimal in-memory bidirectional session pair, sufficient for the round-trip checks above.
// JS: class PairTransport implements RpcTransport {
// JS:   partner!: PairTransport;
// JS:   private queue: string[] = [];
// JS:   private waiter?: () => void;
// JS:
// JS:   async send(message: string): Promise<void> {
// JS:     this.partner.queue.push(message);
// JS:     this.partner.waiter?.();
// JS:     this.partner.waiter = undefined;
// JS:   }
// JS:
// JS:   async receive(): Promise<string> {
// JS:     while (this.queue.length === 0) {
// JS:       await new Promise<void>(resolve => { this.waiter = resolve; });
// JS:     }
// JS:     return this.queue.shift()!;
// JS:   }
// JS: }
// JS:
// JS: class SessionPair {
// JS:   private clientTransport = new PairTransport();
// JS:   private serverTransport = new PairTransport();
// JS:   private client: RpcSession;
// JS:   private server: RpcSession;
// JS:   stub: ReturnType<RpcSession["getRemoteMain"]>;
// JS:
// JS:   constructor(target: RpcTarget, options?: { limits?: Partial<RpcLimits> }) {
// JS:     this.clientTransport.partner = this.serverTransport;
// JS:     this.serverTransport.partner = this.clientTransport;
// JS:     this.client = new RpcSession(this.clientTransport);
// JS:     this.server = new RpcSession(this.serverTransport, target, options);
// JS:     this.stub = this.client.getRemoteMain();
// JS:   }
// JS:
// JS:   async dispose() {
// JS:     for (let i = 0; i < 16; i++) await Promise.resolve();
// JS:   }
// JS:
// JS:   [Symbol.dispose]() {}
// JS: }
