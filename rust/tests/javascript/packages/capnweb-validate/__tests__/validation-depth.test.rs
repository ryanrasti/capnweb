// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file.
//! One-for-one Rust counterpart of `packages/capnweb-validate/__tests__/validation-depth.test.ts`.

skipped_test!(
    js_001_validation_depth_rejects_values_nested_beyond_the_max_depth_fail_closed_line_13,
    "packages/capnweb-validate/__tests__/validation-depth.test.ts:13",
    "rejects values nested beyond the max depth (fail closed)",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_002_validation_depth_accepts_well_typed_values_within_the_max_depth_line_19,
    "packages/capnweb-validate/__tests__/validation-depth.test.ts:19",
    "accepts well-typed values within the max depth",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_003_validation_depth_still_rejects_a_wrong_leaf_within_the_max_depth_line_25,
    "packages/capnweb-validate/__tests__/validation-depth.test.ts:25",
    "still rejects a wrong leaf within the max depth",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);
