// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file.
//! One-for-one Rust counterpart of `packages/capnweb-validate/__tests__/method-edge-cases.test.ts`.

skipped_test!(
    js_001_method_edge_cases_validates_an_optional_method_instead_of_dropping_it_line_20,
    "packages/capnweb-validate/__tests__/method-edge-cases.test.ts:20",
    "validates an optional method instead of dropping it",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_002_method_edge_cases_validates_a_required_method_alongside_an_optional_one_line_36,
    "packages/capnweb-validate/__tests__/method-edge-cases.test.ts:36",
    "validates a required method alongside an optional one",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_003_method_edge_cases_allows_undefined_for_optional_parameters_when_strictnullchecks_line_54,
    "packages/capnweb-validate/__tests__/method-edge-cases.test.ts:54",
    "allows undefined for optional parameters when strictNullChecks is off",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_004_method_edge_cases_allows_undefined_for_default_initialized_parameters_line_72,
    "packages/capnweb-validate/__tests__/method-edge-cases.test.ts:72",
    "allows undefined for default-initialized parameters",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_005_method_edge_cases_reports_an_unresolved_generic_type_parameter_in_human_terms_line_87,
    "packages/capnweb-validate/__tests__/method-edge-cases.test.ts:87",
    "reports an unresolved generic type parameter in human terms",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);
