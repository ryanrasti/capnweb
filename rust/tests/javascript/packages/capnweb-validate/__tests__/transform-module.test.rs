// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file.
//! One-for-one Rust counterpart of `packages/capnweb-validate/__tests__/transform-module.test.ts`.

skipped_test!(
    js_001_transform_module_server_rewrites_newworkersrpcresponse_and_emits_a_validator_line_109,
    "packages/capnweb-validate/__tests__/transform-module.test.ts:109",
    "server: rewrites newWorkersRpcResponse and emits a validator",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_002_transform_module_client_does_not_rewrite_capnweb_client_constructors_line_129,
    "packages/capnweb-validate/__tests__/transform-module.test.ts:129",
    "client: does not rewrite capnweb client constructors",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_003_transform_module_client_rewrites_explicit_validatestub_with_the_explicit_type_ar_line_150,
    "packages/capnweb-validate/__tests__/transform-module.test.ts:150",
    "client: rewrites explicit validateStub with the explicit type argument",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_004_transform_module_client_validatestub_without_a_type_argument_unwraps_the_stub_st_line_169,
    "packages/capnweb-validate/__tests__/transform-module.test.ts:169",
    "client: validateStub without a type argument unwraps the stub structurally",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_005_transform_module_does_not_rewrite_a_local_binding_that_shadows_the_marker_name_line_188,
    "packages/capnweb-validate/__tests__/transform-module.test.ts:188",
    "does not rewrite a local binding that shadows the marker name",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_006_transform_module_does_not_rewrite_a_decorator_binding_that_shadows_the_marker_na_line_212,
    "packages/capnweb-validate/__tests__/transform-module.test.ts:212",
    "does not rewrite a decorator binding that shadows the marker name",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_007_transform_module_client_validatestub_works_for_workers_rpc_style_service_stubs_line_237,
    "packages/capnweb-validate/__tests__/transform-module.test.ts:237",
    "client: validateStub works for Workers RPC style service stubs",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_008_transform_module_client_does_not_dedup_anonymous_surfaces_with_different_collect_line_256,
    "packages/capnweb-validate/__tests__/transform-module.test.ts:256",
    "client: does not dedup anonymous surfaces with different collection validators",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_009_transform_module_client_validatestub_plus_capnweb_server_marker_imports_both_run_line_289,
    "packages/capnweb-validate/__tests__/transform-module.test.ts:289",
    "client: validateStub plus capnweb server marker imports both runtimes",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_010_transform_module_decorator_rewrites_validaterpc_to_wrap_the_class_line_308,
    "packages/capnweb-validate/__tests__/transform-module.test.ts:308",
    "decorator: rewrites @validateRpc to wrap the class",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_011_transform_module_decorator_filters_inherited_workerentrypoint_platform_methods_line_326,
    "packages/capnweb-validate/__tests__/transform-module.test.ts:326",
    "decorator: filters inherited WorkerEntrypoint platform methods",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_012_transform_module_decorator_filters_overridden_workerentrypoint_platform_methods_line_347,
    "packages/capnweb-validate/__tests__/transform-module.test.ts:347",
    "decorator: filters overridden WorkerEntrypoint platform methods",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_013_transform_module_server_marker_filters_s_subclass_platform_connect_methods_line_369,
    "packages/capnweb-validate/__tests__/transform-module.test.ts:369",
    "server marker: filters %s subclass platform + connect methods",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_014_transform_module_server_marker_filters_connect_from_package_shaped_workers_types_line_399,
    "packages/capnweb-validate/__tests__/transform-module.test.ts:399",
    "server marker: filters connect from package-shaped workers types",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_015_transform_module_server_marker_keeps_non_workers_rpctarget_connect_methods_valid_line_431,
    "packages/capnweb-validate/__tests__/transform-module.test.ts:431",
    "server marker: keeps non-Workers RpcTarget connect methods validated",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_016_transform_module_decorator_filters_inherited_durableobject_platform_methods_line_450,
    "packages/capnweb-validate/__tests__/transform-module.test.ts:450",
    "decorator: filters inherited DurableObject platform methods",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_017_transform_module_rejects_a_non_cloneable_built_in_at_build_time_line_471,
    "packages/capnweb-validate/__tests__/transform-module.test.ts:471",
    "rejects a non-cloneable built-in at build time",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_018_transform_module_fails_loud_when_a_server_marker_call_has_no_resolvable_service_line_484,
    "packages/capnweb-validate/__tests__/transform-module.test.ts:484",
    "fails loud when a server marker call has no resolvable service type",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_019_transform_module_runtime_the_emitted_validator_accepts_good_args_and_rejects_bad_line_493,
    "packages/capnweb-validate/__tests__/transform-module.test.ts:493",
    "runtime: the emitted validator accepts good args and rejects bad ones",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_020_transform_module_recognizes_typescript_lib_files_across_separators_line_513,
    "packages/capnweb-validate/__tests__/transform-module.test.ts:513",
    "recognizes TypeScript lib files across separators",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);
