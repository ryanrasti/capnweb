// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file.
//! One-for-one Rust counterpart of `packages/capnweb-validate/__tests__/error-subclass.test.ts`.

skipped_test!(
    js_001_error_subclass_lowers_a_user_defined_error_subclass_to_v_error_not_a_structural_line_16,
    "packages/capnweb-validate/__tests__/error-subclass.test.ts:16",
    "lowers a user-defined Error subclass to v.error, not a structural object",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_002_error_subclass_lowers_a_deep_subclass_chain_to_v_error_line_33,
    "packages/capnweb-validate/__tests__/error-subclass.test.ts:33",
    "lowers a deep subclass chain to v.error",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_003_error_subclass_still_lowers_the_global_error_and_standard_subclasses_to_v_error_line_47,
    "packages/capnweb-validate/__tests__/error-subclass.test.ts:47",
    "still lowers the global Error and standard subclasses to v.error",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_004_error_subclass_does_not_hijack_a_user_type_merely_named_error_that_is_not_the_gl_line_62,
    "packages/capnweb-validate/__tests__/error-subclass.test.ts:62",
    "does not hijack a user type merely named Error that is not the global",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);
