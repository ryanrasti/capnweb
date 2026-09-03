// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file.
//! One-for-one Rust counterpart of `__tests__/index.test.ts`.

use super::*;

skipped_test!(
    js_001_index_can_serialize_line_78,
    "__tests__/index.test.ts:78",
    "can serialize",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_002_index_can_deserialize_line_84,
    "__tests__/index.test.ts:84",
    "can deserialize",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_003_index_throws_an_error_if_the_value_can_t_be_serialized_line_98,
    "__tests__/index.test.ts:98",
    "throws an error if the value can't be serialized",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_004_index_throws_an_error_for_circular_references_line_108,
    "__tests__/index.test.ts:108",
    "throws an error for circular references",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_005_index_can_serialize_complex_nested_structures_line_116,
    "__tests__/index.test.ts:116",
    "can serialize complex nested structures",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

#[test]
fn throws_errors_for_malformed_deserialization_data() {
    assert!(deserialize(r#"{"unclosed": "#).is_err());
    assert!(deserialize(r#"["unknown_type","param"]"#).is_err());
    assert!(deserialize(r#"["date"]"#).is_err());
    assert!(deserialize(r#"["error"]"#).is_err());
}

#[test]
fn can_serialize_large_uint8array_without_stack_overflow() {
    let bytes = (0..200_000)
        .map(|index| (index & 0xff) as u8)
        .collect::<Vec<_>>();
    let encoded = serialize(&CapnwebValue::Bytes(bytes.clone())).expect("serialize bytes");
    let CapnwebValue::Bytes(decoded) = deserialize(&encoded).expect("deserialize bytes") else {
        panic!("expected bytes");
    };
    assert_eq!(decoded, bytes);
}

#[test]
fn correctly_serializes_uint8array_with_trailing_byte_value_61() {
    let bytes = b"Hello=".to_vec();
    let encoded = serialize(&CapnwebValue::Bytes(bytes.clone())).expect("serialize bytes");
    let CapnwebValue::Bytes(decoded) = deserialize(&encoded).expect("deserialize bytes") else {
        panic!("expected bytes");
    };
    assert_eq!(decoded, bytes);
}

#[test]
fn strips_base64_padding_from_serialized_bytes() {
    let encoded = serialize(&CapnwebValue::Bytes(vec![1, 2, 3, 4, 5])).expect("serialize bytes");
    assert!(!encoded.contains('='));
}

#[test]
fn can_serialize_uint8array_as_legacy_bytes_without_a_type_marker() {
    let encoded = serialize(&CapnwebValue::Bytes(b"Hello".to_vec())).expect("serialize bytes");
    assert_eq!(encoded, r#"["bytes","SGVsbG8"]"#);
    let CapnwebValue::Bytes(decoded) = deserialize(&encoded).expect("deserialize bytes") else {
        panic!("expected bytes");
    };
    assert_eq!(decoded, b"Hello");
}

skipped_test!(
    js_011_index_can_serialize_node_js_buffer_as_bytes_line_188,
    "__tests__/index.test.ts:188",
    "can serialize Node.js Buffer as bytes",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_012_index_can_serialize_arraybuffer_as_bytes_with_an_arraybuffer_marker_line_198,
    "__tests__/index.test.ts:198",
    "can serialize ArrayBuffer as bytes with an ArrayBuffer marker",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_013_index_can_serialize_typed_array_views_as_bytes_with_type_markers_line_208,
    "__tests__/index.test.ts:208",
    "can serialize typed array views as bytes with type markers",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_014_index_serializes_multi_byte_numbers_in_little_endian_wire_order_line_258,
    "__tests__/index.test.ts:258",
    "serializes multi-byte numbers in little-endian wire order",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_015_index_can_swap_each_multi_byte_element_s_byte_order_line_266,
    "__tests__/index.test.ts:266",
    "can swap each multi-byte element's byte order",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_016_index_rejects_byte_lengths_that_are_misaligned_for_typed_arrays_line_283,
    "__tests__/index.test.ts:283",
    "rejects byte lengths that are misaligned for typed arrays",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

#[test]
fn throws_for_unknown_bytes_type_markers() {
    let error =
        deserialize(r#"["bytes","SGVsbG8","invalidUint8Array"]"#).expect_err("unknown marker");
    assert!(
        error
            .to_string()
            .contains("unknown bytes type marker: invalidUint8Array")
    );
    let error = deserialize(r#"["bytes","SGVsbG8",123]"#).expect_err("non-string marker");
    assert!(error.to_string().contains("unknown bytes type marker type"));
}

#[test]
fn preserves_invalid_date_values_through_serialization() {
    let encoded = serialize(&CapnwebValue::Date(None)).expect("serialize invalid date");
    assert_eq!(encoded, r#"["date",null]"#);
    assert!(matches!(
        deserialize(&encoded),
        Ok(CapnwebValue::Date(None))
    ));
}

skipped_test!(
    js_019_index_rejects_malformed_blob_wire_values_line_316,
    "__tests__/index.test.ts:316",
    "rejects malformed blob wire values",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_020_index_throws_when_serializing_blob_without_an_rpc_session_line_326,
    "__tests__/index.test.ts:326",
    "throws when serializing Blob without an RPC session",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_021_index_preserves_dynamically_attached_own_properties_line_339,
    "__tests__/index.test.ts:339",
    "preserves dynamically-attached own properties",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_022_index_preserves_class_field_properties_on_error_subclasses_line_352,
    "__tests__/index.test.ts:352",
    "preserves class-field properties on Error subclasses",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_023_index_preserves_built_in_subclass_identity_when_extras_are_present_line_367,
    "__tests__/index.test.ts:367",
    "preserves built-in subclass identity when extras are present",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_024_index_emits_the_legacy_3_element_form_when_there_are_no_extras_line_378,
    "__tests__/index.test.ts:378",
    "emits the legacy 3-element form when there are no extras",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_025_index_emits_5_elements_with_null_stack_when_extras_are_present_but_stack_is_abse_line_384,
    "__tests__/index.test.ts:384",
    "emits 5 elements with null stack when extras are present but stack is absent",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_026_index_round_trips_nested_supported_types_inside_props_line_397,
    "__tests__/index.test.ts:397",
    "round-trips nested supported types inside props",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_027_index_is_decodable_by_a_legacy_4_element_decoder_new_sender_old_receiver_line_416,
    "__tests__/index.test.ts:416",
    "is decodable by a legacy 4-element decoder (new sender -> old receiver)",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_028_index_decodes_legacy_3_and_4_element_forms_old_sender_new_receiver_line_443,
    "__tests__/index.test.ts:443",
    "decodes legacy 3- and 4-element forms (old sender -> new receiver)",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_029_index_throws_when_decoding_an_error_with_a_malformed_props_bag_line_454,
    "__tests__/index.test.ts:454",
    "throws when decoding an error with a malformed props bag",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_030_index_round_trips_error_cause_line_466,
    "__tests__/index.test.ts:466",
    "round-trips Error.cause",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_031_index_round_trips_a_primitive_cause_line_479,
    "__tests__/index.test.ts:479",
    "round-trips a primitive cause",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_032_index_round_trips_aggregateerror_errors_with_inner_subclass_identity_line_485,
    "__tests__/index.test.ts:485",
    "round-trips AggregateError.errors with inner subclass identity",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_033_index_silently_drops_properties_whose_values_cannot_be_serialized_line_500,
    "__tests__/index.test.ts:500",
    "silently drops properties whose values cannot be serialized",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_034_index_silently_drops_cyclic_property_values_line_514,
    "__tests__/index.test.ts:514",
    "silently drops cyclic property values",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_035_index_never_throws_even_when_every_extra_property_is_unsupported_line_527,
    "__tests__/index.test.ts:527",
    "never throws even when every extra property is unsupported",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_036_index_propagates_async_send_failures_from_string_transports_line_724,
    "__tests__/index.test.ts:724",
    "propagates async send failures from string transports",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_037_index_propagates_synchronous_send_failures_from_string_transports_line_741,
    "__tests__/index.test.ts:741",
    "propagates synchronous send failures from string transports",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_038_index_propagates_synchronous_send_failures_from_custom_encoding_transports_line_761,
    "__tests__/index.test.ts:761",
    "propagates synchronous send failures from custom-encoding transports",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

#[tokio::test]
async fn supports_wrapping_an_rpc_target() {
    let stub = RpcStub::from_target(Arc::new(ParityTarget));
    let result = TypedRpcPromise::<f64>::new(stub.call(
        vec!["square".into()],
        RpcPayload::from_app_params(CapnwebValue::Array(vec![CapnwebValue::Number(3.0)])),
    ));
    assert_eq!(result.await.expect("local square"), 9.0);
}

skipped_test!(
    js_040_index_supports_wrapping_a_function_line_786,
    "__tests__/index.test.ts:786",
    "supports wrapping a function",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_041_index_supports_wrapping_an_async_function_line_793,
    "__tests__/index.test.ts:793",
    "supports wrapping an async function",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

#[tokio::test]
async fn supports_wrapping_an_arbitrary_object() {
    let stub = RpcStub::from_value(CapnwebValue::Object(BTreeMap::from([(
        "abc".into(),
        CapnwebValue::String("hello".into()),
    )])));
    let value = TypedRpcPromise::<String>::new(stub.get(vec!["abc".into()]));
    assert_eq!(value.await.expect("object property"), "hello");
}

#[tokio::test]
async fn supports_wrapping_an_object_with_nested_stubs() {
    let inner = RpcStub::from_target(Arc::new(ParityTarget));
    let outer = RpcStub::from_value(CapnwebValue::Object(BTreeMap::from([
        ("inner".into(), CapnwebValue::Stub(inner)),
        ("value".into(), CapnwebValue::Number(42.0)),
    ])));
    assert_eq!(
        TypedRpcPromise::<f64>::new(outer.get(vec!["value".into()]))
            .await
            .expect("value"),
        42.0
    );
    let result = TypedRpcPromise::<f64>::new(outer.call(
        vec!["inner".into(), "square".into()],
        RpcPayload::from_app_params(CapnwebValue::Array(vec![CapnwebValue::Number(4.0)])),
    ));
    assert_eq!(result.await.expect("nested call"), 16.0);
}

#[tokio::test]
async fn supports_wrapping_an_object_with_nested_rpc_targets() {
    let outer = RpcStub::from_value(CapnwebValue::Object(BTreeMap::from([
        ("inner".into(), CapnwebValue::Target(Arc::new(ParityTarget))),
        ("value".into(), CapnwebValue::Number(42.0)),
    ])));
    assert_eq!(
        TypedRpcPromise::<f64>::new(outer.get(vec!["value".into()]))
            .await
            .expect("value"),
        42.0
    );
    let result = TypedRpcPromise::<f64>::new(outer.call(
        vec!["inner".into(), "square".into()],
        RpcPayload::from_app_params(CapnwebValue::Array(vec![CapnwebValue::Number(4.0)])),
    ));
    assert_eq!(result.await.expect("nested target call"), 16.0);
}

skipped_test!(
    js_045_index_supports_wrapping_an_object_with_nested_functions_line_822,
    "__tests__/index.test.ts:822",
    "supports wrapping an object with nested functions",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_046_index_supports_wrapping_an_object_with_nested_async_functions_line_830,
    "__tests__/index.test.ts:830",
    "supports wrapping an object with nested async functions",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_047_index_supports_wrapping_an_rpctarget_with_nested_stubs_line_843,
    "__tests__/index.test.ts:843",
    "supports wrapping an RpcTarget with nested stubs",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_048_index_supports_wrapping_an_rpctarget_with_nested_rpctargets_line_857,
    "__tests__/index.test.ts:857",
    "supports wrapping an RpcTarget with nested RpcTargets",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_049_index_returns_undefined_when_accessing_nonexistent_properties_line_871,
    "__tests__/index.test.ts:871",
    "returns undefined when accessing nonexistent properties",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_050_index_exposes_only_prototype_properties_for_rpctarget_not_instance_properties_line_887,
    "__tests__/index.test.ts:887",
    "exposes only prototype properties for RpcTarget, not instance properties",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_051_index_does_not_expose_private_methods_starting_with_line_918,
    "__tests__/index.test.ts:918",
    "does not expose private methods starting with #",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_052_index_supports_map_on_nulls_line_929,
    "__tests__/index.test.ts:929",
    "supports map() on nulls",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_053_index_supports_map_on_arrays_line_955,
    "__tests__/index.test.ts:955",
    "supports map() on arrays",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_054_index_supports_nested_map_line_975,
    "__tests__/index.test.ts:975",
    "supports nested map()",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_055_index_overrides_tostring_to_at_least_specify_the_type_line_997,
    "__tests__/index.test.ts:997",
    "overrides toString() to at least specify the type",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_056_index_disposes_nested_stubs_and_rpctargets_when_wrapping_an_object_line_1006,
    "__tests__/index.test.ts:1006",
    "disposes nested stubs and RpcTargets when wrapping an object",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_057_index_only_calls_rpctarget_disposer_when_wrapping_an_rpctarget_with_nested_stubs_line_1031,
    "__tests__/index.test.ts:1031",
    "only calls RpcTarget disposer when wrapping an RpcTarget with nested stubs",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

#[test]
fn only_disposes_rpc_target_when_all_dups_are_disposed() {
    let disposed = Arc::new(AtomicBool::new(false));
    let original = RpcStub::from_target(Arc::new(DisposableTarget(disposed.clone())));
    let duplicate_one = original.dup();
    let duplicate_two = original.dup();
    drop(original);
    assert!(!disposed.load(Ordering::Acquire));
    drop(duplicate_one);
    assert!(!disposed.load(Ordering::Acquire));
    drop(duplicate_two);
    assert!(disposed.load(Ordering::Acquire));
}

skipped_test!(
    js_059_index_makes_disposal_idempotent_duplicate_dispose_calls_don_t_affect_refcount_line_1076,
    "__tests__/index.test.ts:1076",
    "makes disposal idempotent - duplicate dispose calls don't affect refcount",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

#[tokio::test]
async fn supports_calls() {
    assert_eq!(parity_stub().square(3.0).await.expect("square call"), 9.0);
}

skipped_test!(
    js_061_index_supports_throwing_errors_line_1102,
    "__tests__/index.test.ts:1102",
    "supports throwing errors",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_062_index_preserves_own_properties_on_thrown_errors_over_rpc_line_1108,
    "__tests__/index.test.ts:1108",
    "preserves own properties on thrown errors over RPC",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_063_index_preserves_error_cause_on_thrown_errors_over_rpc_line_1135,
    "__tests__/index.test.ts:1135",
    "preserves Error.cause on thrown errors over RPC",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_064_index_round_trips_heavyweight_but_supported_types_attached_to_errors_line_1155,
    "__tests__/index.test.ts:1155",
    "round-trips heavyweight-but-supported types attached to errors",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_065_index_silently_drops_stub_valued_error_properties_without_leaking_capabilities_line_1178,
    "__tests__/index.test.ts:1178",
    "silently drops stub-valued error properties without leaking capabilities",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_066_index_drops_the_whole_error_property_when_a_stub_is_nested_inside_an_unserializa_line_1204,
    "__tests__/index.test.ts:1204",
    "drops the whole error property when a stub is nested inside an unserializable value",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_067_index_supports_then_catch_and_finally_on_rpc_promises_line_1228,
    "__tests__/index.test.ts:1228",
    "supports .then(), .catch(), and .finally() on RPC promises",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_068_index_throws_error_when_trying_to_send_non_serializable_argument_line_1274,
    "__tests__/index.test.ts:1274",
    "throws error when trying to send non-serializable argument",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_069_index_throws_error_when_trying_to_return_non_serializable_result_line_1283,
    "__tests__/index.test.ts:1283",
    "throws error when trying to return non-serializable result",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_070_index_does_not_expose_common_object_properties_on_rpctarget_line_1298,
    "__tests__/index.test.ts:1298",
    "does not expose common Object properties on RpcTarget",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_071_index_does_not_expose_common_object_properties_on_rpctarget_line_1320,
    "__tests__/index.test.ts:1320",
    "does not expose common Object properties on RpcTarget",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_072_index_supports_passing_async_functions_line_1364,
    "__tests__/index.test.ts:1364",
    "supports passing async functions",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

#[tokio::test]
async fn supports_returning_an_rpc_target() {
    let CapnwebValue::Stub(counter) = parity_stub().make_counter(4.0).await.expect("counter")
    else {
        panic!("expected counter stub");
    };
    let result = TypedRpcPromise::<f64>::new(counter.call(
        vec!["increment".into()],
        RpcPayload::from_app_params(CapnwebValue::Array(vec![CapnwebValue::Number(1.0)])),
    ));
    assert_eq!(result.await.expect("increment call"), 5.0);
}

skipped_test!(
    js_074_index_supports_passing_a_stub_back_over_the_connection_line_1385,
    "__tests__/index.test.ts:1385",
    "supports passing a stub back over the connection",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_075_index_supports_three_party_capability_passing_line_1394,
    "__tests__/index.test.ts:1394",
    "supports three-party capability passing",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_076_index_supports_proxying_line_1423,
    "__tests__/index.test.ts:1423",
    "supports proxying",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

#[tokio::test]
async fn supports_passing_a_promise_in_arguments() {
    let stub = parity_stub();
    let promise = stub.square(2.0);
    assert_eq!(stub.square(promise).await.expect("pipelined square"), 16.0);
}

skipped_test!(
    js_078_index_supports_calling_a_promise_line_1496,
    "__tests__/index.test.ts:1496",
    "supports calling a promise",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_079_index_supports_returning_a_promise_line_1506,
    "__tests__/index.test.ts:1506",
    "supports returning a promise",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_080_index_propagates_errors_to_pipelined_calls_line_1512,
    "__tests__/index.test.ts:1512",
    "propagates errors to pipelined calls",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_081_index_propagates_errors_to_argument_pipelined_calls_line_1529,
    "__tests__/index.test.ts:1529",
    "propagates errors to argument-pipelined calls",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_082_index_doesn_t_create_spurious_unhandled_rejections_line_1550,
    "__tests__/index.test.ts:1550",
    "doesn't create spurious unhandled rejections",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

#[tokio::test]
async fn supports_map_on_nulls_over_rpc() {
    let stub = parity_stub();
    let counter = RpcStub::from_target(Arc::new(Counter(std::sync::Mutex::new(0.0))));

    let untouched = stub.return_null().into_untyped().map({
        let counter = counter.dup();
        move |value| {
            Ok(increment(
                &counter,
                FromCapnwebValue::from_capnweb_value(value)?,
            ))
        }
    });
    assert!(matches!(
        untouched.pull().await.expect("mapped null").deliver().await,
        Ok(CapnwebValue::Null)
    ));

    let mapped = stub.return_number(4.0).into_untyped().map({
        let counter = counter.dup();
        move |value| {
            Ok(increment(
                &counter,
                FromCapnwebValue::from_capnweb_value(value)?,
            ))
        }
    });
    let value = mapped
        .pull()
        .await
        .expect("mapped scalar")
        .deliver()
        .await
        .expect("deliver scalar");
    assert!(matches!(value, CapnwebValue::Number(4.0)));
}

#[tokio::test]
async fn supports_map_on_arrays_over_rpc() {
    let stub = parity_stub();
    let mapped = stub.generate_fibonacci(6.0).into_untyped().map(|value| {
        let value = f64::from_capnweb_value(value)?;
        Ok(value + 3.0)
    });
    let value = mapped
        .pull()
        .await
        .expect("mapped array")
        .deliver()
        .await
        .expect("deliver array");
    let result = Vec::<f64>::from_capnweb_value(value).expect("number array");
    assert_eq!(result, vec![3.0, 4.0, 4.0, 5.0, 6.0, 8.0]);
}

skipped_test!(
    js_085_index_supports_nested_map_line_1630,
    "__tests__/index.test.ts:1630",
    "supports nested map()",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_086_index_disposes_remote_rpctarget_when_stub_is_disposed_line_1655,
    "__tests__/index.test.ts:1655",
    "disposes remote RpcTarget when stub is disposed",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_087_index_disposes_a_returned_rpctarget_for_every_time_it_appears_in_a_result_line_1683,
    "__tests__/index.test.ts:1683",
    "disposes a returned RpcTarget for every time it appears in a result",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_088_index_disposes_rpctarget_that_was_passed_in_params_line_1718,
    "__tests__/index.test.ts:1718",
    "disposes RpcTarget that was passed in params",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_089_index_dupes_rpctarget_that_was_passed_in_params_if_it_has_a_dup_method_line_1745,
    "__tests__/index.test.ts:1745",
    "dupes RpcTarget that was passed in params if it has a dup() method",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_090_index_only_disposes_remote_target_when_all_rpc_dups_are_disposed_line_1795,
    "__tests__/index.test.ts:1795",
    "only disposes remote target when all RPC dups are disposed",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_091_index_makes_rpc_disposal_idempotent_line_1828,
    "__tests__/index.test.ts:1828",
    "makes RPC disposal idempotent",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_092_index_disposes_targets_automatically_on_disconnect_line_1859,
    "__tests__/index.test.ts:1859",
    "disposes targets automatically on disconnect",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

#[tokio::test]
async fn shuts_down_the_connection_if_the_main_capability_is_disposed() {
    let (client_transport, server_transport) = capnweb::transport::memory_transport_pair();
    let client = RpcSession::new(client_transport, Arc::new(ParityTarget));
    let _server = RpcSession::new(server_transport, Arc::new(ParityTarget));
    let main = client.get_remote_main();
    let counter_value = TypedRpcPromise::<CapnwebValue>::new(main.call(
        vec!["makeCounter".into()],
        RpcPayload::from_app_params(CapnwebValue::Array(vec![CapnwebValue::Number(0.0)])),
    ))
    .await
    .expect("counter");
    let CapnwebValue::Stub(counter) = counter_value else {
        panic!("expected counter stub");
    };

    main.dispose();
    let result = TypedRpcPromise::<f64>::new(increment(&counter, 1.0)).await;
    assert!(
        result
            .expect_err("session shutdown")
            .to_string()
            .contains("RPC session was shut down by disposing the main stub")
    );
}

#[tokio::test]
async fn maintains_e_order_for_concurrent_calls_on_single_stub() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let stub = RpcStub::from_target(Arc::new(OrderTarget(order.clone())));
    let calls = [1.0, 2.0, 3.0, 4.0].map(|id| {
        TypedRpcPromise::<f64>::new(stub.call(
            vec!["recordCall".into()],
            RpcPayload::from_app_params(CapnwebValue::Array(vec![CapnwebValue::Number(id)])),
        ))
    });
    futures::future::try_join_all(calls)
        .await
        .expect("ordered calls");
    assert_eq!(
        *order.lock().expect("order mutex poisoned"),
        [1.0, 2.0, 3.0, 4.0]
    );
}

skipped_test!(
    js_095_index_maintains_e_order_for_promise_pipelined_calls_line_1935,
    "__tests__/index.test.ts:1935",
    "maintains e-order for promise-pipelined calls",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_096_index_hides_the_stack_by_default_line_1968,
    "__tests__/index.test.ts:1968",
    "hides the stack by default",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_097_index_hides_the_stack_by_default_with_structured_clone_transports_line_1992,
    "__tests__/index.test.ts:1992",
    "hides the stack by default with structured clone transports",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_098_index_reveals_the_stack_if_the_callback_returns_the_error_line_2011,
    "__tests__/index.test.ts:2011",
    "reveals the stack if the callback returns the error",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_099_index_allows_errors_to_be_rewritten_line_2033,
    "__tests__/index.test.ts:2033",
    "allows errors to be rewritten",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_100_index_sends_own_properties_from_the_rewritten_error_not_the_original_line_2053,
    "__tests__/index.test.ts:2053",
    "sends own properties from the rewritten error, not the original",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_101_index_respects_in_place_mutation_by_onsenderror_to_scrub_heavy_properties_line_2083,
    "__tests__/index.test.ts:2083",
    "respects in-place mutation by onSendError to scrub heavy properties",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

#[tokio::test]
async fn signals_when_the_connection_is_lost() {
    let (client_transport, client_control, server_transport, _server_control) =
        controlled_transport_pair();
    let client = RpcSession::new(client_transport, Arc::new(ParityTarget));
    let _server = RpcSession::new(server_transport, Arc::new(BrokenTarget));
    let stub = client.get_remote_main();
    let empty_args = || RpcPayload::from_app_params(CapnwebValue::Array(Vec::new()));

    let value = TypedRpcPromise::<f64>::new(stub.call(vec!["getValue".into()], empty_args()))
        .await
        .expect("initial call");
    assert_eq!(value, 42.0);

    let errors = Arc::new(Mutex::new(Vec::<String>::new()));
    stub.on_rpc_broken({
        let errors = errors.clone();
        move |_| {
            errors
                .lock()
                .expect("errors mutex poisoned")
                .push("stub".into())
        }
    });

    let counter_one_promise = stub.call(vec!["makeCounter".into()], empty_args());
    counter_one_promise.on_rpc_broken({
        let errors = errors.clone();
        move |_| {
            errors
                .lock()
                .expect("errors mutex poisoned")
                .push("counter1Promise".into())
        }
    });

    let counter_two_value =
        TypedRpcPromise::<CapnwebValue>::new(stub.call(vec!["makeCounter".into()], empty_args()))
            .await
            .expect("second counter");
    let CapnwebValue::Stub(counter_two) = counter_two_value else {
        panic!("expected second counter");
    };
    counter_two.on_rpc_broken({
        let errors = errors.clone();
        move |_| {
            errors
                .lock()
                .expect("errors mutex poisoned")
                .push("counter2".into())
        }
    });

    let counter_one_value = counter_one_promise
        .pull()
        .await
        .expect("first counter")
        .deliver()
        .await
        .expect("deliver first counter");
    let CapnwebValue::Stub(counter_one) = counter_one_value else {
        panic!("expected first counter");
    };
    counter_one.on_rpc_broken({
        let errors = errors.clone();
        move |_| {
            errors
                .lock()
                .expect("errors mutex poisoned")
                .push("counter1".into())
        }
    });

    let hanging = stub.call(vec!["hangingCall".into()], empty_args());
    hanging.on_rpc_broken({
        let errors = errors.clone();
        move |_| {
            errors
                .lock()
                .expect("errors mutex poisoned")
                .push("hangingCall".into())
        }
    });

    let throwing = stub.call(vec!["throwError".into()], empty_args());
    throwing.on_rpc_broken({
        let errors = errors.clone();
        move |_| {
            errors
                .lock()
                .expect("errors mutex poisoned")
                .push("throwError".into())
        }
    });
    assert!(throwing.pull().await.is_err());
    assert_eq!(
        *errors.lock().expect("errors mutex poisoned"),
        ["throwError"]
    );

    throwing.on_rpc_broken({
        let errors = errors.clone();
        move |_| {
            errors
                .lock()
                .expect("errors mutex poisoned")
                .push("throwError2".into())
        }
    });
    assert_eq!(
        *errors.lock().expect("errors mutex poisoned"),
        ["throwError", "throwError2"]
    );

    client_control
        .0
        .send(Err(capnweb::Error::Io("test disconnect".into())))
        .expect("inject disconnect");
    assert!(hanging.pull().await.is_err());
    assert_eq!(
        *errors.lock().expect("errors mutex poisoned"),
        [
            "throwError",
            "throwError2",
            "stub",
            "counter1Promise",
            "counter2",
            "counter1",
            "hangingCall",
        ]
    );
}

skipped_test!(
    js_103_index_disposes_copied_call_arguments_when_the_backing_promise_rejects_line_2196,
    "__tests__/index.test.ts:2196",
    "disposes copied call arguments when the backing promise rejects",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_104_index_disposes_copied_stream_arguments_when_the_backing_promise_rejects_line_2207,
    "__tests__/index.test.ts:2207",
    "disposes copied stream arguments when the backing promise rejects",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_105_index_delivers_a_call_initiated_before_disposal_line_2218,
    "__tests__/index.test.ts:2218",
    "delivers a call initiated before disposal",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_106_index_disposes_copied_call_arguments_when_the_destination_hook_is_broken_line_2237,
    "__tests__/index.test.ts:2237",
    "disposes copied call arguments when the destination hook is broken",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_107_index_disposes_copied_stream_arguments_when_the_destination_hook_is_broken_line_2248,
    "__tests__/index.test.ts:2248",
    "disposes copied stream arguments when the destination hook is broken",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_108_index_disposes_copied_call_arguments_when_the_local_call_path_fails_line_2259,
    "__tests__/index.test.ts:2259",
    "disposes copied call arguments when the local call path fails",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_109_index_disposes_map_captures_when_the_destination_hook_is_broken_line_2272,
    "__tests__/index.test.ts:2272",
    "disposes map captures when the destination hook is broken",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_110_index_leaves_call_args_ownership_with_a_resolved_callee_whose_call_throws_synchr_line_2294,
    "__tests__/index.test.ts:2294",
    "leaves call args ownership with a resolved callee whose call() throws synchronously",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_111_index_leaves_stream_args_ownership_with_a_resolved_callee_whose_call_throws_sync_line_2306,
    "__tests__/index.test.ts:2306",
    "leaves stream args ownership with a resolved callee whose call() throws synchronously",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_112_index_default_stream_disposes_the_result_hook_when_pull_throws_synchronously_line_2319,
    "__tests__/index.test.ts:2319",
    "default stream() disposes the result hook when pull() throws synchronously",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_113_index_call_disposes_owned_args_when_argument_serialization_fails_line_2342,
    "__tests__/index.test.ts:2342",
    "call() disposes owned args when argument serialization fails",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_114_index_stream_disposes_owned_args_when_argument_serialization_fails_line_2362,
    "__tests__/index.test.ts:2362",
    "stream() disposes owned args when argument serialization fails",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_115_index_pipelines_through_a_pending_promise_without_pulling_the_resolution_line_2384,
    "__tests__/index.test.ts:2384",
    "pipelines through a pending promise without pulling the resolution",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_116_index_queues_calls_made_before_resolution_and_delivers_them_in_order_line_2402,
    "__tests__/index.test.ts:2402",
    "queues calls made before resolution and delivers them in order",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_117_index_accepts_a_promise_for_a_target_a_remote_stub_or_a_plain_value_line_2419,
    "__tests__/index.test.ts:2419",
    "accepts a promise for a target, a remote stub, or a plain value",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_118_index_awaiting_the_rpcpromise_yields_the_resolution_line_2432,
    "__tests__/index.test.ts:2432",
    "awaiting the RpcPromise yields the resolution",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_119_index_reports_rejection_to_queued_calls_await_and_onrpcbroken_line_2442,
    "__tests__/index.test.ts:2442",
    "reports rejection to queued calls, await, and onRpcBroken",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_120_index_does_not_report_an_unhandled_rejection_for_an_unused_promise_line_2454,
    "__tests__/index.test.ts:2454",
    "does not report an unhandled rejection for an unused promise",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_121_index_does_not_report_an_unhandled_rejection_for_a_disposed_unawaited_queued_cal_line_2459,
    "__tests__/index.test.ts:2459",
    "does not report an unhandled rejection for a disposed, unawaited queued call",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_122_index_does_not_report_an_unhandled_rejection_for_a_disposed_unawaited_map_result_line_2465,
    "__tests__/index.test.ts:2465",
    "does not report an unhandled rejection for a disposed, unawaited map() result",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_123_index_disposes_the_eventual_target_when_disposed_before_resolution_line_2471,
    "__tests__/index.test.ts:2471",
    "disposes the eventual target when disposed before resolution",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_124_index_delivers_a_call_initiated_before_disposal_line_2486,
    "__tests__/index.test.ts:2486",
    "delivers a call initiated before disposal",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_125_index_keeps_an_adopted_rpcpromise_lazy_line_2503,
    "__tests__/index.test.ts:2503",
    "keeps an adopted RpcPromise lazy",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_126_index_transmits_nothing_when_constructed_from_a_remote_property_promise_line_2513,
    "__tests__/index.test.ts:2513",
    "transmits nothing when constructed from a remote property promise",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_127_index_does_not_invoke_a_local_getter_when_constructed_from_a_property_promise_line_2534,
    "__tests__/index.test.ts:2534",
    "does not invoke a local getter when constructed from a property promise",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_128_index_consumes_the_source_when_adopting_an_existing_rpcpromise_line_2551,
    "__tests__/index.test.ts:2551",
    "consumes the source when adopting an existing RpcPromise",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_129_index_stays_lazy_when_a_deferred_promise_is_resolved_with_dup_line_2566,
    "__tests__/index.test.ts:2566",
    "stays lazy when a deferred promise is resolved with dup()",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_130_index_preserves_brokenness_of_a_bare_stub_it_was_constructed_from_line_2585,
    "__tests__/index.test.ts:2585",
    "preserves brokenness of a bare stub it was constructed from",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_131_index_keeps_disposal_idempotent_when_constructed_from_a_bare_stub_line_2597,
    "__tests__/index.test.ts:2597",
    "keeps disposal idempotent when constructed from a bare stub",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_132_index_resolves_when_awaited_after_construction_from_a_bare_local_stub_line_2612,
    "__tests__/index.test.ts:2612",
    "resolves when awaited after construction from a bare local stub",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_133_index_can_perform_a_batch_http_request_line_2627,
    "__tests__/index.test.ts:2627",
    "can perform a batch HTTP request",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_134_index_rejects_non_post_requests_with_405_line_2640,
    "__tests__/index.test.ts:2640",
    "rejects non-POST requests with 405",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_135_index_can_open_a_websocket_connection_line_2648,
    "__tests__/index.test.ts:2648",
    "can open a WebSocket connection",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_136_index_can_communicate_over_messagechannel_line_2668,
    "__tests__/index.test.ts:2668",
    "can communicate over MessageChannel",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_137_index_handles_errors_correctly_line_2693,
    "__tests__/index.test.ts:2693",
    "handles errors correctly",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_138_index_sends_close_signal_when_server_stub_is_disposed_line_2704,
    "__tests__/index.test.ts:2704",
    "sends close signal when server stub is disposed",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_139_index_can_send_a_writablestream_and_receive_writes_line_2732,
    "__tests__/index.test.ts:2732",
    "can send a WritableStream and receive writes",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_140_index_supports_complex_chunk_types_line_2758,
    "__tests__/index.test.ts:2758",
    "supports complex chunk types",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_141_index_propagates_write_errors_back_line_2783,
    "__tests__/index.test.ts:2783",
    "propagates write errors back",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_142_index_aborts_stream_on_disconnect_without_close_line_2810,
    "__tests__/index.test.ts:2810",
    "aborts stream on disconnect without close",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_143_index_handles_abort_from_receiver_line_2849,
    "__tests__/index.test.ts:2849",
    "handles abort() from receiver",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_144_index_can_send_writablestream_in_nested_object_line_2873,
    "__tests__/index.test.ts:2873",
    "can send WritableStream in nested object",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_145_index_handles_multiple_concurrent_writes_efficiently_line_2895,
    "__tests__/index.test.ts:2895",
    "handles multiple concurrent writes efficiently",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_146_index_applies_backpressure_when_window_fills_up_line_2925,
    "__tests__/index.test.ts:2925",
    "applies backpressure when window fills up",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_147_index_applies_backpressure_when_custom_transport_omits_stream_message_size_line_2993,
    "__tests__/index.test.ts:2993",
    "applies backpressure when custom transport omits stream message size",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_148_index_uses_the_size_reported_by_a_custom_transport_s_send_for_flow_control_line_3040,
    "__tests__/index.test.ts:3040",
    "uses the size reported by a custom transport's send() for flow control",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_149_index_uses_stream_messages_instead_of_push_pull_release_line_3106,
    "__tests__/index.test.ts:3106",
    "uses stream messages instead of push+pull+release",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_150_index_unblocks_a_backpressure_blocked_write_when_an_in_flight_write_errors_line_3174,
    "__tests__/index.test.ts:3174",
    "unblocks a backpressure-blocked write when an in-flight write errors",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_151_index_disposes_copied_call_arguments_when_a_writablestream_call_is_invalid_line_3244,
    "__tests__/index.test.ts:3244",
    "disposes copied call arguments when a WritableStream call is invalid",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_152_index_disposes_copied_call_arguments_when_the_writablestream_stub_was_disposed_line_3261,
    "__tests__/index.test.ts:3261",
    "disposes copied call arguments when the WritableStream stub was disposed",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_153_index_dynamic_round_trips_native_types_over_a_level_transport_line_3291,
    "__tests__/index.test.ts:3291",
    "<dynamic: `round-trips native types over a ${level} transport`>",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_154_index_aborts_the_receive_loop_when_the_transport_is_aborted_line_3313,
    "__tests__/index.test.ts:3313",
    "aborts the receive loop when the transport is aborted",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_155_index_can_send_a_readablestream_and_read_all_chunks_line_3322,
    "__tests__/index.test.ts:3322",
    "can send a ReadableStream and read all chunks",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_156_index_can_return_a_readablestream_from_an_rpc_call_line_3350,
    "__tests__/index.test.ts:3350",
    "can return a ReadableStream from an RPC call",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_157_index_can_send_readablestream_in_nested_object_line_3377,
    "__tests__/index.test.ts:3377",
    "can send ReadableStream in nested object",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_158_index_can_send_readablestream_in_nested_object_partial_read_line_3404,
    "__tests__/index.test.ts:3404",
    "can send ReadableStream in nested object (partial read)",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_159_index_supports_complex_chunk_types_line_3429,
    "__tests__/index.test.ts:3429",
    "supports complex chunk types",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_160_index_propagates_stream_errors_to_the_reader_line_3461,
    "__tests__/index.test.ts:3461",
    "propagates stream errors to the reader",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_161_index_handles_many_chunks_line_3492,
    "__tests__/index.test.ts:3492",
    "handles many chunks",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_162_index_can_send_both_readablestream_and_writablestream_in_same_message_line_3530,
    "__tests__/index.test.ts:3530",
    "can send both ReadableStream and WritableStream in same message",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_163_index_cancels_a_readablestream_in_a_result_when_the_whole_result_is_disposed_line_3565,
    "__tests__/index.test.ts:3565",
    "cancels a ReadableStream in a result when the whole result is disposed",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_164_index_can_send_an_rpctarget_as_a_stream_chunk_and_call_methods_on_it_line_3616,
    "__tests__/index.test.ts:3616",
    "can send an RpcTarget as a stream chunk and call methods on it",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_165_index_can_return_a_readablestream_containing_rpctargets_and_call_methods_on_chun_line_3653,
    "__tests__/index.test.ts:3653",
    "can return a ReadableStream containing RpcTargets and call methods on chunks",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_166_index_can_send_multiple_rpctargets_as_stream_chunks_line_3687,
    "__tests__/index.test.ts:3687",
    "can send multiple RpcTargets as stream chunks",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_167_index_can_send_headers_over_rpc_line_3733,
    "__tests__/index.test.ts:3733",
    "can send Headers over RPC",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_168_index_can_send_request_with_body_over_rpc_line_3757,
    "__tests__/index.test.ts:3757",
    "can send Request with body over RPC",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_169_index_can_send_response_with_body_over_rpc_line_3799,
    "__tests__/index.test.ts:3799",
    "can send Response with body over RPC",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_170_index_can_send_request_without_body_over_rpc_line_3841,
    "__tests__/index.test.ts:3841",
    "can send Request without body over RPC",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_171_index_can_send_response_without_body_over_rpc_line_3862,
    "__tests__/index.test.ts:3862",
    "can send Response without body over RPC",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_172_index_can_send_and_receive_a_binary_blob_line_3880,
    "__tests__/index.test.ts:3880",
    "can send and receive a binary Blob",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_173_index_preserves_blob_mime_type_line_3890,
    "__tests__/index.test.ts:3890",
    "preserves Blob MIME type",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_174_index_can_send_an_empty_blob_line_3898,
    "__tests__/index.test.ts:3898",
    "can send an empty Blob",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_175_index_can_send_a_blob_as_part_of_a_compound_return_value_line_3907,
    "__tests__/index.test.ts:3907",
    "can send a Blob as part of a compound return value",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_176_index_can_send_multiple_blobs_in_the_same_call_line_3928,
    "__tests__/index.test.ts:3928",
    "can send multiple Blobs in the same call",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_177_index_can_receive_an_array_of_blobs_in_one_return_value_line_3947,
    "__tests__/index.test.ts:3947",
    "can receive an array of Blobs in one return value",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_178_index_round_trips_a_blob_with_no_mime_type_line_3968,
    "__tests__/index.test.ts:3968",
    "round-trips a Blob with no MIME type",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_179_index_preserves_every_possible_byte_value_through_the_pipe_line_3980,
    "__tests__/index.test.ts:3980",
    "preserves every possible byte value through the pipe",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_180_index_can_send_a_large_blob_over_rpc_line_3992,
    "__tests__/index.test.ts:3992",
    "can send a large Blob over RPC",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_181_index_can_pass_a_blob_through_a_local_loopback_stub_line_4009,
    "__tests__/index.test.ts:4009",
    "can pass a Blob through a local (loopback) stub",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_182_index_disposing_a_result_containing_a_blob_does_not_throw_line_4022,
    "__tests__/index.test.ts:4022",
    "disposing a result containing a Blob does not throw",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_183_index_is_encoded_as_a_readable_pipe_on_the_wire_line_4036,
    "__tests__/index.test.ts:4036",
    "is encoded as a readable pipe on the wire",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_184_index_does_not_let_serialized_error_properties_override_object_prototype_members_line_4090,
    "__tests__/index.test.ts:4090",
    "does not let serialized error properties override Object.prototype members",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_185_index_does_not_let_an_error_type_name_resolve_to_an_object_prototype_member_type_line_4120,
    "__tests__/index.test.ts:4120",
    "does not let an error type name resolve to an Object.prototype member (type confusion)",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_186_index_delivers_the_unwrapped_abort_reason_to_onrpcbroken_not_the_payload_wrapper_line_4156,
    "__tests__/index.test.ts:4156",
    "delivers the unwrapped abort reason to onRpcBroken (not the payload wrapper)",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);

skipped_test!(
    js_187_index_truncates_an_over_long_websocket_close_reason_on_a_code_point_boundary_line_4175,
    "__tests__/index.test.ts:4175",
    "truncates an over-long WebSocket close reason on a code-point boundary",
    "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice"
);
