// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file.
//! One-for-one Rust counterpart of `packages/capnweb-validate/__tests__/decorated-class-runtime.test.ts`.

skipped_test!(
    js_001_decorated_class_runtime_constructs_real_instances_identity_prototype_chain_and_f_line_31,
    "packages/capnweb-validate/__tests__/decorated-class-runtime.test.ts:31",
    "constructs real instances: identity, prototype chain, and # fields intact",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_002_decorated_class_runtime_decorated_subclass_of_a_decorated_base_resolves_subclass_line_61,
    "packages/capnweb-validate/__tests__/decorated-class-runtime.test.ts:61",
    "decorated subclass of a decorated base resolves subclass-only methods",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_003_decorated_class_runtime_validates_subclass_only_method_args_against_the_subclass_line_98,
    "packages/capnweb-validate/__tests__/decorated-class-runtime.test.ts:98",
    "validates subclass-only method args against the subclass validator",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_004_decorated_class_runtime_does_not_double_validate_inherited_methods_or_double_dec_line_130,
    "packages/capnweb-validate/__tests__/decorated-class-runtime.test.ts:130",
    "does not double-validate inherited methods or double decoration",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_005_decorated_class_runtime_wraps_declared_getters_on_the_prototype_with_this_intact_line_172,
    "packages/capnweb-validate/__tests__/decorated-class-runtime.test.ts:172",
    "wraps declared getters on the prototype with `this` intact",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_006_decorated_class_runtime_leaves_undeclared_prototype_methods_callable_exposure_is_line_190,
    "packages/capnweb-validate/__tests__/decorated-class-runtime.test.ts:190",
    "leaves undeclared prototype methods callable (exposure is the RPC layer's model)",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_007_decorated_class_runtime_wrapservertarget_passes_through_already_wrapped_methods_line_216,
    "packages/capnweb-validate/__tests__/decorated-class-runtime.test.ts:216",
    "wrapServerTarget passes through already-wrapped methods without re-validating",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);
