// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file.
//! One-for-one Rust counterpart of `packages/capnweb-validate/__tests__/marker-coverage.test.ts`.

skipped_test!(
    js_001_marker_coverage_sees_capnweb_s_full_multi_condition_export_surface_line_61,
    "packages/capnweb-validate/__tests__/marker-coverage.test.ts:61",
    "sees capnweb's full multi-condition export surface",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_002_marker_coverage_every_capnweb_rpc_entry_point_is_a_marker_or_explicitly_unsuppor_line_68,
    "packages/capnweb-validate/__tests__/marker-coverage.test.ts:68",
    "every capnweb RPC entry point is a marker or explicitly unsupported",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_003_marker_coverage_the_unsupported_allowlist_does_not_list_names_we_actually_cover_line_80,
    "packages/capnweb-validate/__tests__/marker-coverage.test.ts:80",
    "the unsupported allowlist does not list names we actually cover",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_004_marker_coverage_every_allowlisted_name_still_exists_in_capnweb_no_stale_allowlis_line_85,
    "packages/capnweb-validate/__tests__/marker-coverage.test.ts:85",
    "every allowlisted name still exists in capnweb (no stale allowlist)",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);
