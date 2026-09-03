// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file.
//! One-for-one Rust counterpart of `packages/capnweb-validate/__tests__/rpc-compatible-types.test.ts`.

skipped_test!(
    js_001_rpc_compatible_types_accepts_arraybuffer_dataview_regexp_map_set_and_typed_array_line_12,
    "packages/capnweb-validate/__tests__/rpc-compatible-types.test.ts:12",
    "accepts ArrayBuffer, DataView, RegExp, Map/Set, and typed arrays at build time",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_002_rpc_compatible_types_lowers_readonlymap_and_readonlyset_to_map_set_runtime_valid_line_55,
    "packages/capnweb-validate/__tests__/rpc-compatible-types.test.ts:55",
    "lowers ReadonlyMap and ReadonlySet to Map/Set runtime validators",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_003_rpc_compatible_types_rejects_non_cloneable_collection_types_at_build_time_line_76,
    "packages/capnweb-validate/__tests__/rpc-compatible-types.test.ts:76",
    "rejects non-cloneable collection types at build time",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_004_rpc_compatible_types_rejects_explicit_sharedarraybuffer_backed_views_at_build_ti_line_90,
    "packages/capnweb-validate/__tests__/rpc-compatible-types.test.ts:90",
    "rejects explicit SharedArrayBuffer-backed views at build time",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_005_rpc_compatible_types_rejects_explicit_union_view_backing_types_that_include_shar_line_101,
    "packages/capnweb-validate/__tests__/rpc-compatible-types.test.ts:101",
    "rejects explicit union view backing types that include SharedArrayBuffer",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_006_rpc_compatible_types_rejects_generic_view_backing_constraints_that_include_share_line_113,
    "packages/capnweb-validate/__tests__/rpc-compatible-types.test.ts:113",
    "rejects generic view backing constraints that include SharedArrayBuffer",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_007_rpc_compatible_types_keeps_arraybuffer_and_uint8array_runtime_validators_distinc_line_124,
    "packages/capnweb-validate/__tests__/rpc-compatible-types.test.ts:124",
    "keeps ArrayBuffer and Uint8Array runtime validators distinct",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_008_rpc_compatible_types_validates_rpc_compatible_built_in_brands_at_runtime_line_131,
    "packages/capnweb-validate/__tests__/rpc-compatible-types.test.ts:131",
    "validates RPC-compatible built-in brands at runtime",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_009_rpc_compatible_types_rejects_views_backed_by_sharedarraybuffer_at_runtime_line_145,
    "packages/capnweb-validate/__tests__/rpc-compatible-types.test.ts:145",
    "rejects views backed by SharedArrayBuffer at runtime",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_010_rpc_compatible_types_validates_map_keys_and_values_at_runtime_line_153,
    "packages/capnweb-validate/__tests__/rpc-compatible-types.test.ts:153",
    "validates Map keys and values at runtime",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);
