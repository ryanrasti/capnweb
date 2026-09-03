// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file.
//! One-for-one Rust counterpart of `packages/capnweb-validate/__tests__/namespace-imports.test.ts`.

skipped_test!(
    js_001_namespace_imports_server_import_as_cv_validate_pkg_cv_newworkersrpcresponse_line_23,
    "packages/capnweb-validate/__tests__/namespace-imports.test.ts:23",
    "server: import * as cv (validate pkg) -> cv.newWorkersRpcResponse()",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);

skipped_test!(
    js_002_namespace_imports_decorator_import_as_cv_cv_validaterpc_is_transformed_line_47,
    "packages/capnweb-validate/__tests__/namespace-imports.test.ts:47",
    "decorator: import * as cv -> @cv.validateRpc() is transformed",
    "capnweb-validate transformer and runtime validation are not implemented in Rust"
);
