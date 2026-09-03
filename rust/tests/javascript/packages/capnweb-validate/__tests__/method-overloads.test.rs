// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file.
//! One-for-one Rust counterpart of `packages/capnweb-validate/__tests__/method-overloads.test.ts`.

skipped_test!(
    js_001_method_overloads_passes_an_overloaded_method_through_unvalidated_with_a_single_w_line_26,
    "packages/capnweb-validate/__tests__/method-overloads.test.ts:26",
    "passes an overloaded method through unvalidated with a single warning",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_002_method_overloads_handles_overloads_that_differ_in_arity_line_43,
    "packages/capnweb-validate/__tests__/method-overloads.test.ts:43",
    "handles overloads that differ in arity",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_003_method_overloads_still_validates_a_single_signature_method_no_false_positive_line_56,
    "packages/capnweb-validate/__tests__/method-overloads.test.ts:56",
    "still validates a single-signature method (no false positive)",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_004_method_overloads_does_not_warn_when_the_overloaded_method_is_explicitly_skiprpcv_line_70,
    "packages/capnweb-validate/__tests__/method-overloads.test.ts:70",
    "does not warn when the overloaded method is explicitly @skipRpcValidation",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_005_method_overloads_does_not_warn_when_skiprpcvalidation_is_on_the_overload_impleme_line_84,
    "packages/capnweb-validate/__tests__/method-overloads.test.ts:84",
    "does not warn when @skipRpcValidation is on the overload implementation",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);
