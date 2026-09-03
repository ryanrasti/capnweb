// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file.
//! One-for-one Rust counterpart of `packages/capnweb-validate/__tests__/builtin-global-sources.test.ts`.

skipped_test!(
    js_001_builtin_global_sources_cloudflare_workers_types_no_dom_lib_line_67,
    "packages/capnweb-validate/__tests__/builtin-global-sources.test.ts:67",
    "@cloudflare/workers-types (no DOM lib)",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_002_builtin_global_sources_wrangler_generated_worker_configuration_d_ts_no_package_line_74,
    "packages/capnweb-validate/__tests__/builtin-global-sources.test.ts:74",
    "wrangler-generated worker-configuration.d.ts (no package)",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_003_builtin_global_sources_types_node_line_81,
    "packages/capnweb-validate/__tests__/builtin-global-sources.test.ts:81",
    "@types/node",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_004_builtin_global_sources_node_fetch_response_validates_structurally_not_as_v_respo_line_90,
    "packages/capnweb-validate/__tests__/builtin-global-sources.test.ts:90",
    "node-fetch Response validates structurally, not as v.response",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);
