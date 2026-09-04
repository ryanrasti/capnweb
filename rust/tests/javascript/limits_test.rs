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
use std::collections::VecDeque;
use std::future::{Future, pending};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::core::{
    CapnwebValue, Error, PayloadStubHook, PromiseStubHook, PropertyKey, PropertyPath, Result,
    RpcPayload, RpcTargetInner, StubHook,
};
use crate::rpc::{AnyRpcTransport, RpcSession, RpcSessionOptions, TransportMessage, TransportSend};
use crate::serialize::{
    DEFAULT_LIMITS, DEFAULT_MAX_DEPTH, EncodingLevel, RpcLimits, deserialize, serialize,
};
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
struct ScriptedTransport {
    // JS:   sent: string[] = [];
    sent: Mutex<Vec<String>>,
    // JS:   aborted = false;
    aborted: AtomicBool,
    // JS:   private toReceive: string[];
    to_receive: Mutex<VecDeque<String>>,
    // JS:   private blockForever = new Promise<string>(() => {});
    block_forever: (),
    session: Mutex<Option<RpcSession>>,
    // JS:
    // JS:   constructor(frames: string[]) {
}
impl ScriptedTransport {
    fn new(frames: Vec<String>) -> Self {
        // JS:     this.toReceive = [...frames];
        Self {
            sent: Mutex::new(Vec::new()),
            aborted: AtomicBool::new(false),
            to_receive: Mutex::new(frames.into()),
            block_forever: (),
            session: Mutex::new(None),
        }
        // JS:   }
    }
}
// JS:
// JS:   async send(message: string): Promise<void> {
impl AnyRpcTransport for ScriptedTransport {
    fn encoding_level(&self) -> EncodingLevel {
        EncodingLevel::String
    }

    fn send(&self, message: TransportMessage) -> Result<TransportSend> {
        let TransportMessage::Text(message) = message else {
            return Err(Error::Type("expected string transport message".to_owned()));
        };
        // JS:     this.sent.push(message);
        self.sent.lock().unwrap().push(message);
        Ok(TransportSend::Done(None))
        // JS:   }
    }
    // JS:
    // JS:   async receive(): Promise<string> {
    fn receive(self: Arc<Self>) -> Pin<Box<dyn Future<Output = Result<TransportMessage>> + Send>> {
        Box::pin(async move {
            // JS:     let next = this.toReceive.shift();
            let next = self.to_receive.lock().unwrap().pop_front();
            // JS:     return next === undefined ? this.blockForever : next;
            match next {
                Some(next) => Ok(TransportMessage::Text(next)),
                None => pending().await,
            }
            // JS:   }
        })
    }
    // JS:
    // JS:   abort(reason: any): void {
    fn abort(&self, _reason: Error) {
        // JS:     this.aborted = true;
        self.aborted.store(true, Ordering::SeqCst);
        // JS:   }
    }
}
// JS:
// JS:   // Resolves once the session has sent an ["abort", ...] frame, or rejects after a spin budget.
// JS:   async waitForAbort(): Promise<string> {
impl ScriptedTransport {
    async fn wait_for_abort(&self) -> String {
        // JS:     for (let i = 0; i < 200; i++) {
        for _ in 0..200 {
            // JS:       let abortFrame = this.sent.find(m => m.startsWith('["abort"'));
            let abort_frame = self
                .sent
                .lock()
                .unwrap()
                .iter()
                .find(|message| message.starts_with("[\"abort\""))
                .cloned();
            // JS:       if (abortFrame !== undefined) return abortFrame;
            if let Some(abort_frame) = abort_frame {
                return abort_frame;
            }
            // JS:       await Promise.resolve();
            tokio::task::yield_now().await;
            // JS:     }
        }
        // JS:     throw new Error(`session did not abort; sent: ${JSON.stringify(this.sent)}`);
        panic!(
            "session did not abort; sent: {:?}",
            self.sent.lock().unwrap()
        );
        // JS:   }
    }
    // JS:
    // JS:   async expectNoAbort(): Promise<void> {
    async fn expect_no_abort(&self) {
        // JS:     for (let i = 0; i < 50; i++) await Promise.resolve();
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        // JS:     let abortFrame = this.sent.find(m => m.startsWith('["abort"'));
        let abort_frame = self
            .sent
            .lock()
            .unwrap()
            .iter()
            .find(|message| message.starts_with("[\"abort\""))
            .cloned();
        // JS:     expect(abortFrame, "session unexpectedly aborted").toBeUndefined();
        assert!(
            abort_frame.is_none(),
            "session unexpectedly aborted: {abort_frame:?}"
        );
        // JS:     expect(this.aborted, "transport unexpectedly aborted").toBe(false);
        assert!(
            !self.aborted.load(Ordering::SeqCst),
            "transport unexpectedly aborted"
        );
        // JS:   }
    }
    // JS: }
}
// JS:
// JS: // Drives a server session that processes a single ["push", expr] frame, returning the transport so
// JS: // the caller can inspect the reaction.
// JS: function pushFrame(expr: string, limits?: Partial<RpcLimits>): ScriptedTransport {
fn push_frame(expr: String, limits: Option<RpcLimits>) -> Arc<ScriptedTransport> {
    // JS:   let transport = new ScriptedTransport([`["push",${expr}]`]);
    let transport = Arc::new(ScriptedTransport::new(vec![format!("[\"push\",{expr}]")]));
    // JS:   new RpcSession(transport, new EchoTarget(), limits ? { limits } : undefined);
    let session = RpcSession::new(
        transport.clone(),
        Some(CapnwebValue::Target(Arc::new(EchoTarget))),
        RpcSessionOptions {
            limits: limits.unwrap_or(DEFAULT_LIMITS),
            ..Default::default()
        },
    )
    .unwrap();
    *transport.session.lock().unwrap() = Some(session);
    // JS:   return transport;
    transport
    // JS: }
}
// JS:
// JS: class EchoTarget extends RpcTarget {
struct EchoTarget;
// JS:   echo(value: unknown) { return value; }
impl RpcTargetInner for EchoTarget {
    fn call(self: Arc<Self>, path: PropertyPath, args: RpcPayload) -> Result<StubHook> {
        if path != vec![PropertyKey::String("echo".to_owned())] {
            return Err(Error::Type(format!("No such RPC method: {path:?}")));
        }
        Ok(PromiseStubHook::from_future(async move {
            let delivered = args.deliver_resolve().await?;
            let CapnwebValue::Array(mut values) = delivered else {
                return Err(Error::Type("arguments are not an array".to_owned()));
            };
            if values.len() != 1 {
                return Err(Error::Type("echo expects one argument".to_owned()));
            }
            Ok(Arc::new(PayloadStubHook::new(RpcPayload::from_app_return(
                values.remove(0),
            ))) as StubHook)
        }))
    }

    fn get(self: Arc<Self>, path: PropertyPath) -> Result<StubHook> {
        Err(Error::Type(format!("No such RPC property: {path:?}")))
    }
}
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
    #[tokio::test(flavor = "current_thread")]
    async fn honors_a_per_session_override_of_max_big_int_digits() {
        let limits = RpcLimits {
            max_big_int_digits: 4,
            ..DEFAULT_LIMITS
        };
        // JS:     // The cap counts the full wire string length. "1234" is 4 characters.
        // JS:     await pushFrame('["bigint","1234"]', { maxBigIntDigits: 4 }).expectNoAbort();
        push_frame("[\"bigint\",\"1234\"]".to_owned(), Some(limits))
            .expect_no_abort()
            .await;
        // JS:
        // JS:     let aborted = await pushFrame('["bigint","12345"]', { maxBigIntDigits: 4 }).waitForAbort();
        let aborted = push_frame("[\"bigint\",\"12345\"]".to_owned(), Some(limits))
            .wait_for_abort()
            .await;
        // JS:     expect(aborted).toMatch(/bigint exceeds maximum length/);
        assert!(aborted.contains("bigint exceeds maximum length"));
        // JS:
        // JS:     // The leading "-" counts toward the length, so "-123" is 4 characters (within a limit of 4)
        // JS:     // while "-1234" is 5 and aborts.
        // JS:     await pushFrame('["bigint","-123"]', { maxBigIntDigits: 4 }).expectNoAbort();
        push_frame("[\"bigint\",\"-123\"]".to_owned(), Some(limits))
            .expect_no_abort()
            .await;
        // JS:
        // JS:     let abortedNeg = await pushFrame('["bigint","-1234"]', { maxBigIntDigits: 4 }).waitForAbort();
        let aborted_neg = push_frame("[\"bigint\",\"-1234\"]".to_owned(), Some(limits))
            .wait_for_abort()
            .await;
        // JS:     expect(abortedNeg).toMatch(/bigint exceeds maximum length/);
        assert!(aborted_neg.contains("bigint exceeds maximum length"));
        // JS:
        // JS:     // The 5-digit value is well under the default limit, so it does NOT abort by default.
        // JS:     await pushFrame('["bigint","12345"]').expectNoAbort();
        push_frame("[\"bigint\",\"12345\"]".to_owned(), None)
            .expect_no_abort()
            .await;
        // JS:   });
    }
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
    #[tokio::test(flavor = "current_thread")]
    async fn honors_a_per_session_override_of_max_depth() {
        // JS:     // Override depth to a small value; nesting beyond it aborts the session.
        // JS:     let deep = nestedEscapedArrays(10);
        let deep = nested_escaped_arrays(10);
        // JS:     let aborted = await pushFrame(deep, { maxDepth: 4 }).waitForAbort();
        let aborted = push_frame(
            deep.clone(),
            Some(RpcLimits {
                max_depth: 4,
                ..DEFAULT_LIMITS
            }),
        )
        .wait_for_abort()
        .await;
        // JS:     expect(aborted).toMatch(/maximum allowed message depth/);
        assert!(aborted.contains("maximum allowed message depth"));
        // JS:
        // JS:     // The same depth is well within the default, so by default it does not abort.
        // JS:     await pushFrame(deep).expectNoAbort();
        push_frame(deep, None).expect_no_abort().await;
        // JS:   });
    }
    // JS:
    // JS:   it("does not reset depth across nested call arguments", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn does_not_reset_depth_across_nested_call_arguments() {
        // JS:     let aborted = await pushFrame(nestedCallArgs(8), { maxDepth: 8 }).waitForAbort();
        let aborted = push_frame(
            nested_call_args(8),
            Some(RpcLimits {
                max_depth: 8,
                ..DEFAULT_LIMITS
            }),
        )
        .wait_for_abort()
        .await;
        // JS:     expect(aborted).toMatch(/maximum allowed message depth/);
        assert!(aborted.contains("maximum allowed message depth"));
        // JS:   });
    }
    // JS:
    // JS:   it("accepts nested call arguments within the default depth limit", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn accepts_nested_call_arguments_within_the_default_depth_limit() {
        // JS:     await pushFrame(nestedCallArgs(64)).expectNoAbort();
        push_frame(nested_call_args(64), None)
            .expect_no_abort()
            .await;
        // JS:   });
    }
    // JS:
    // JS:   it("accepts near-limit call arguments that the default sender can serialize", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn accepts_near_limit_call_arguments_that_the_default_sender_can_serialize() {
        // JS:     let appArg = deserialize(nestedEscapedArrays(DEFAULT_MAX_DEPTH - 2));
        let app_arg = stacker::grow(8 * 1024 * 1024, || {
            deserialize(&nested_escaped_arrays(DEFAULT_MAX_DEPTH - 2))
        })
        .unwrap();
        // JS:     let wireArgs = JSON.parse(serialize([appArg]))[0];
        let wire_args = stacker::grow(8 * 1024 * 1024, || {
            serialize(&CapnwebValue::Array(vec![app_arg]))
        })
        .unwrap();
        let wire_args = &wire_args[1..wire_args.len() - 1];
        // JS:     await pushFrame(`["pipeline",0,["echo"],${JSON.stringify(wireArgs)}]`).expectNoAbort();
        push_frame(format!("[\"pipeline\",0,[\"echo\"],{wire_args}]"), None)
            .expect_no_abort()
            .await;
        // JS:   });
    }
    // JS: });
}
// JS:
// JS: describe("message size limits", () => {
mod message_size_limits {
    use super::*;
    // JS:   it("aborts the session on a message over the size limit", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn aborts_the_session_on_a_message_over_the_size_limit() {
        // JS:     // The pushed expression is a long-but-valid JSON string literal, so the resulting frame is
        // JS:     // valid JSON and well over the 64-char override. The size check runs (and aborts) before
        // JS:     // JSON.parse, so this exercises the size guard specifically, not a parse error.
        // JS:     let aborted = await pushFrame(`"${"a".repeat(200)}"`, { maxMessageSize: 64 }).waitForAbort();
        let aborted = push_frame(
            format!("\"{}\"", "a".repeat(200)),
            Some(RpcLimits {
                max_message_size: 64,
                ..DEFAULT_LIMITS
            }),
        )
        .wait_for_abort()
        .await;
        // JS:     expect(aborted).toMatch(/exceeds maximum size/);
        assert!(aborted.contains("exceeds maximum size"));
        // JS:   });
    }
    // JS:
    // JS:   it("does not abort on a message under the size limit", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn does_not_abort_on_a_message_under_the_size_limit() {
        // JS:     await pushFrame('"small"', { maxMessageSize: 1024 }).expectNoAbort();
        push_frame(
            "\"small\"".to_owned(),
            Some(RpcLimits {
                max_message_size: 1024,
                ..DEFAULT_LIMITS
            }),
        )
        .expect_no_abort()
        .await;
        // JS:   });
    }
    // JS: });
}
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
    #[tokio::test(flavor = "current_thread")]
    async fn a_session_constructed_with_no_options_behaves_exactly_as_before() {
        // JS:     // A legitimate bigint, normal nesting, and a normal call all flow through a default session
        // JS:     // without aborting.
        // JS:     using harness = new SessionPair(new EchoTarget());
        let harness = SessionPair::new(None);
        // JS:
        // JS:     let stub = harness.stub as any;
        let stub = harness.stub.clone();
        // JS:     expect(await stub.echo(123n)).toBe(123n);
        let bigint = rpc_echo(&stub, CapnwebValue::BigInt(BigInt::from(123))).await;
        assert!(matches!(bigint, CapnwebValue::BigInt(value) if value == BigInt::from(123)));
        // JS:     expect(await stub.echo([[1, 2, 3]])).toStrictEqual([[1, 2, 3]]);
        let array = CapnwebValue::Array(vec![CapnwebValue::Array(vec![
            CapnwebValue::Number(1.0),
            CapnwebValue::Number(2.0),
            CapnwebValue::Number(3.0),
        ])]);
        assert!(capnweb_values_equal(
            &rpc_echo(&stub, array.clone()).await,
            &array
        ));
        // JS:     expect(await stub.echo({a: {b: {c: 1}}})).toStrictEqual({a: {b: {c: 1}}});
        let object = CapnwebValue::Object(std::collections::BTreeMap::from([(
            "a".to_owned(),
            CapnwebValue::Object(std::collections::BTreeMap::from([(
                "b".to_owned(),
                CapnwebValue::Object(std::collections::BTreeMap::from([(
                    "c".to_owned(),
                    CapnwebValue::Number(1.0),
                )])),
            )])),
        )]));
        assert!(capnweb_values_equal(
            &rpc_echo(&stub, object.clone()).await,
            &object
        ));
        // JS:
        // JS:     await harness.dispose();
        harness.dispose().await;
        // JS:   });
    }
    // JS:
    // JS:   it("a session constructed with an empty options object behaves identically", async () => {
    #[tokio::test(flavor = "current_thread")]
    async fn a_session_constructed_with_an_empty_options_object_behaves_identically() {
        // JS:     using harness = new SessionPair(new EchoTarget(), {});
        let harness = SessionPair::new(Some(RpcSessionOptions::default()));
        // JS:     let stub = harness.stub as any;
        let stub = harness.stub.clone();
        // JS:     expect(await stub.echo(999n)).toBe(999n);
        let value = rpc_echo(&stub, CapnwebValue::BigInt(BigInt::from(999))).await;
        assert!(matches!(value, CapnwebValue::BigInt(value) if value == BigInt::from(999)));
        // JS:     await harness.dispose();
        harness.dispose().await;
        // JS:   });
    }
    // JS: });
}
// JS:
// JS: // A minimal in-memory bidirectional session pair, sufficient for the round-trip checks above.
// JS: class PairTransport implements RpcTransport {
struct PairTransport {
    // JS:   partner!: PairTransport;
    partner: tokio::sync::mpsc::UnboundedSender<String>,
    // JS:   private queue: string[] = [];
    queue: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<String>>,
    // JS:   private waiter?: () => void;
    waiter: (),
    // JS:
    // JS:   async send(message: string): Promise<void> {
}
impl AnyRpcTransport for PairTransport {
    fn send(&self, message: TransportMessage) -> Result<TransportSend> {
        let TransportMessage::Text(message) = message else {
            return Err(Error::Type("expected string transport message".to_owned()));
        };
        // JS:     this.partner.queue.push(message);
        self.partner
            .send(message)
            .map_err(|_| Error::Message("pair transport closed".to_owned()))?;
        // JS:     this.partner.waiter?.();
        let _ = self.waiter;
        // JS:     this.partner.waiter = undefined;
        let _ = self.waiter;
        Ok(TransportSend::Done(None))
        // JS:   }
    }
    // JS:
    // JS:   async receive(): Promise<string> {
    fn receive(self: Arc<Self>) -> Pin<Box<dyn Future<Output = Result<TransportMessage>> + Send>> {
        Box::pin(async move {
            // JS:     while (this.queue.length === 0) {
            loop {
                // JS:       await new Promise<void>(resolve => { this.waiter = resolve; });
                if let Some(message) = self.queue.lock().await.recv().await {
                    // JS:     }
                    // JS:     return this.queue.shift()!;
                    return Ok(TransportMessage::Text(message));
                }
                return Err(Error::Message("pair transport closed".to_owned()));
            }
            // JS:   }
        })
    }
    // JS: }
}
// JS:
// JS: class SessionPair {
struct SessionPair {
    // JS:   private clientTransport = new PairTransport();
    client_transport: Arc<PairTransport>,
    // JS:   private serverTransport = new PairTransport();
    server_transport: Arc<PairTransport>,
    // JS:   private client: RpcSession;
    client: RpcSession,
    // JS:   private server: RpcSession;
    server: RpcSession,
    // JS:   stub: ReturnType<RpcSession["getRemoteMain"]>;
    stub: crate::core::RpcStub,
    // JS:
    // JS:   constructor(target: RpcTarget, options?: { limits?: Partial<RpcLimits> }) {
}
impl SessionPair {
    fn new(options: Option<RpcSessionOptions>) -> Self {
        let (to_client, client_queue) = tokio::sync::mpsc::unbounded_channel();
        let (to_server, server_queue) = tokio::sync::mpsc::unbounded_channel();
        // JS:     this.clientTransport.partner = this.serverTransport;
        let client_transport = Arc::new(PairTransport {
            partner: to_server,
            queue: tokio::sync::Mutex::new(client_queue),
            waiter: (),
        });
        // JS:     this.serverTransport.partner = this.clientTransport;
        let server_transport = Arc::new(PairTransport {
            partner: to_client,
            queue: tokio::sync::Mutex::new(server_queue),
            waiter: (),
        });
        // JS:     this.client = new RpcSession(this.clientTransport);
        let client =
            RpcSession::new(client_transport.clone(), None, RpcSessionOptions::default()).unwrap();
        // JS:     this.server = new RpcSession(this.serverTransport, target, options);
        let server = RpcSession::new(
            server_transport.clone(),
            Some(CapnwebValue::Target(Arc::new(EchoTarget))),
            options.unwrap_or_default(),
        )
        .unwrap();
        // JS:     this.stub = this.client.getRemoteMain();
        let stub = client.get_remote_main();
        Self {
            client_transport,
            server_transport,
            client,
            server,
            stub,
        }
        // JS:   }
    }
    // JS:
    // JS:   async dispose() {
    async fn dispose(self) {
        // JS:     for (let i = 0; i < 16; i++) await Promise.resolve();
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        // JS:   }
    }
    // JS:
    // JS:   [Symbol.dispose]() {}
    // Rust ownership disposes both sessions with the harness.
    // JS: }
}

async fn rpc_echo(stub: &crate::core::RpcStub, value: CapnwebValue) -> CapnwebValue {
    stub.call(
        vec!["echo".to_owned()],
        RpcPayload::from_app_params(CapnwebValue::Array(vec![value])),
    )
    .pull()
    .await
    .unwrap()
}

fn capnweb_values_equal(left: &CapnwebValue, right: &CapnwebValue) -> bool {
    match (left, right) {
        (CapnwebValue::Null, CapnwebValue::Null)
        | (CapnwebValue::Undefined, CapnwebValue::Undefined) => true,
        (CapnwebValue::Bool(left), CapnwebValue::Bool(right)) => left == right,
        (CapnwebValue::Number(left), CapnwebValue::Number(right)) => left == right,
        (CapnwebValue::String(left), CapnwebValue::String(right)) => left == right,
        (CapnwebValue::BigInt(left), CapnwebValue::BigInt(right)) => left == right,
        (CapnwebValue::Bytes(left), CapnwebValue::Bytes(right)) => left == right,
        (CapnwebValue::Array(left), CapnwebValue::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| capnweb_values_equal(left, right))
        }
        (CapnwebValue::Object(left), CapnwebValue::Object(right)) => {
            left.len() == right.len()
                && left.iter().all(|(key, left)| {
                    right
                        .get(key)
                        .is_some_and(|right| capnweb_values_equal(left, right))
                })
        }
        _ => false,
    }
}
