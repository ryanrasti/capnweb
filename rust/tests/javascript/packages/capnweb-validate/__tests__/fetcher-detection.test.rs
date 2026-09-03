// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file.
//! One-for-one Rust counterpart of `packages/capnweb-validate/__tests__/fetcher-detection.test.ts`.

skipped_test!(
    js_001_fetcher_detection_validates_an_ordinary_fetch_connect_user_type_as_an_object_not_line_48,
    "packages/capnweb-validate/__tests__/fetcher-detection.test.ts:48",
    "validates an ordinary {fetch,connect} user type as an object, not a stub",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_002_fetcher_detection_keeps_a_real_fetcher_user_as_a_pass_by_reference_stub_line_68,
    "packages/capnweb-validate/__tests__/fetcher-detection.test.ts:68",
    "keeps a real Fetcher<User> as a pass-by-reference stub",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_003_fetcher_detection_keeps_a_bare_fetcher_alias_erased_as_a_stub_via_the_fetch_sign_line_82,
    "packages/capnweb-validate/__tests__/fetcher-detection.test.ts:82",
    "keeps a bare Fetcher (alias erased) as a stub via the fetch signature",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);
