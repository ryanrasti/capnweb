// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file.
//! One-for-one Rust counterpart of `__tests__/limits.test.ts`.

use super::*;

#[test]
fn accepts_a_decimal_bigint_at_the_default_digit_limit() {
    let digits = "9".repeat(RpcLimits::default().max_bigint_digits);
    assert!(matches!(
        deserialize(&format!(r#"["bigint","{digits}"]"#)),
        Ok(CapnwebValue::BigInt(_))
    ));
}

#[test]
fn rejects_a_bigint_over_the_default_digit_limit() {
    let digits = "9".repeat(RpcLimits::default().max_bigint_digits + 1);
    let error = deserialize(&format!(r#"["bigint","{digits}"]"#)).expect_err("oversized bigint");
    assert!(error.to_string().contains("bigint exceeds maximum length"));
}

#[test]
fn round_trips_bigints_through_the_decimal_wire_format() {
    let large: BigInt = (BigInt::from(1_u8) << 4096_usize) + BigInt::from(123_456_789_u64);
    for value in [
        BigInt::from(0),
        BigInt::from(255),
        BigInt::from(-255),
        large.clone(),
        -large,
    ] {
        let wire = serialize(&CapnwebValue::BigInt(value.clone())).expect("serialize bigint");
        let CapnwebValue::BigInt(result) = deserialize(&wire).expect("deserialize bigint") else {
            panic!("expected bigint");
        };
        assert_eq!(result, value);
    }
}

#[test]
fn accepts_decimal_bigint_strings() {
    assert!(
        matches!(deserialize(r#"["bigint","123"]"#), Ok(CapnwebValue::BigInt(value)) if value == BigInt::from(123))
    );
    assert!(
        matches!(deserialize(r#"["bigint","-123"]"#), Ok(CapnwebValue::BigInt(value)) if value == BigInt::from(-123))
    );
}

#[tokio::test]
async fn honors_a_per_session_override_of_max_bigint_digits() {
    let limits = RpcLimits {
        max_bigint_digits: 4,
        ..RpcLimits::default()
    };
    assert!(
        !session_reaction(r#"["bigint","1234"]"#, limits.clone())
            .await
            .iter()
            .any(|m| m.contains("abort"))
    );
    assert!(
        session_reaction(r#"["bigint","12345"]"#, limits)
            .await
            .iter()
            .any(|m| m.contains("bigint exceeds maximum length"))
    );
}

#[test]
fn accepts_nesting_just_under_the_default_depth_limit() {
    assert!(deserialize(&nested_escaped_arrays(DEFAULT_MAX_DEPTH - 1)).is_ok());
}

#[test]
fn rejects_nesting_over_the_default_depth_limit() {
    let error = deserialize(&nested_escaped_arrays(DEFAULT_MAX_DEPTH + 5)).expect_err("deep value");
    assert!(error.to_string().contains("maximum allowed message depth"));
}

#[tokio::test]
async fn honors_a_per_session_override_of_max_depth() {
    let limits = RpcLimits {
        max_depth: 4,
        ..RpcLimits::default()
    };
    let messages = session_reaction(&nested_escaped_arrays(10), limits).await;
    assert!(
        messages
            .iter()
            .any(|message| message.contains("maximum allowed message depth"))
    );
}

#[tokio::test]
async fn does_not_reset_depth_across_nested_call_arguments() {
    let limits = RpcLimits {
        max_depth: 8,
        ..RpcLimits::default()
    };
    let messages = session_reaction(&nested_call_args(8), limits).await;
    assert!(
        messages
            .iter()
            .any(|message| message.contains("maximum allowed message depth"))
    );
}

#[tokio::test]
async fn accepts_nested_call_arguments_within_the_default_depth_limit() {
    let messages = session_reaction(&nested_call_args(64), RpcLimits::default()).await;
    assert!(!messages.iter().any(|message| message.contains("abort")));
}

skipped_test!(
    js_011_limits_accepts_near_limit_call_arguments_that_the_default_sender_can_serialize_line_166,
    "__tests__/limits.test.ts:166",
    "accepts near-limit call arguments that the default sender can serialize",
    "configurable receive-side limits are not implemented in Rust"
);

#[tokio::test]
async fn aborts_the_session_on_a_message_over_the_size_limit() {
    let limits = RpcLimits {
        max_message_size: 64,
        ..RpcLimits::default()
    };
    let expression = format!("\"{}\"", "a".repeat(200));
    let messages = session_reaction(&expression, limits).await;
    assert!(
        messages
            .iter()
            .any(|message| message.contains("exceeds maximum size"))
    );
}

#[tokio::test]
async fn does_not_abort_on_a_message_under_the_size_limit() {
    let limits = RpcLimits {
        max_message_size: 1024,
        ..RpcLimits::default()
    };
    let messages = session_reaction(r#""small""#, limits).await;
    assert!(!messages.iter().any(|message| message.contains("abort")));
}

#[test]
fn default_limits_has_the_documented_values() {
    let limits = RpcLimits::default();
    assert_eq!(DEFAULT_MAX_DEPTH, 256);
    assert_eq!(limits.max_bigint_digits, 16_384);
    assert_eq!(limits.max_depth, DEFAULT_MAX_DEPTH);
    assert_eq!(limits.max_message_size, 32 * 1024 * 1024);
}

skipped_test!(
    js_015_limits_standalone_deserialize_round_trips_normal_values_unchanged_with_no_config_line_197,
    "__tests__/limits.test.ts:197",
    "standalone deserialize round-trips normal values unchanged with no config",
    "configurable receive-side limits are not implemented in Rust"
);

skipped_test!(
    js_016_limits_a_session_constructed_with_no_options_behaves_exactly_as_before_line_204,
    "__tests__/limits.test.ts:204",
    "a session constructed with no options behaves exactly as before",
    "configurable receive-side limits are not implemented in Rust"
);

skipped_test!(
    js_017_limits_a_session_constructed_with_an_empty_options_object_behaves_identically_line_217,
    "__tests__/limits.test.ts:217",
    "a session constructed with an empty options object behaves identically",
    "configurable receive-side limits are not implemented in Rust"
);
