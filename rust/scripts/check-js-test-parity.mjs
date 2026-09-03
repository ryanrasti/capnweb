#!/usr/bin/env node
// Checks that every JS/TS test file and runtime test declaration has a
// separately-maintained Rust counterpart. This script never generates Rust.

import fs from "node:fs";
import path from "node:path";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "../..");
const rustRoot = path.join(root, "rust/tests/javascript");
const sourceRoots = ["__tests__", "__type-tests__", "packages/capnweb-validate/__tests__"];

const activeTests = new Map([
  ["__tests__/index.test.ts:134", "throws_errors_for_malformed_deserialization_data"],
  ["__tests__/index.test.ts:141", "can_serialize_large_uint8array_without_stack_overflow"],
  ["__tests__/index.test.ts:152", "correctly_serializes_uint8array_with_trailing_byte_value_61"],
  ["__tests__/index.test.ts:162", "strips_base64_padding_from_serialized_bytes"],
  ["__tests__/index.test.ts:172", "can_serialize_uint8array_as_legacy_bytes_without_a_type_marker"],
  ["__tests__/index.test.ts:295", "throws_for_unknown_bytes_type_markers"],
  ["__tests__/index.test.ts:302", "preserves_invalid_date_values_through_serialization"],
  ["__tests__/index.test.ts:781", "supports_wrapping_an_rpc_target"],
  ["__tests__/index.test.ts:798", "supports_wrapping_an_arbitrary_object"],
  ["__tests__/index.test.ts:803", "supports_wrapping_an_object_with_nested_stubs"],
  ["__tests__/index.test.ts:813", "supports_wrapping_an_object_with_nested_rpc_targets"],
  ["__tests__/index.test.ts:1056", "only_disposes_rpc_target_when_all_dups_are_disposed"],
  ["__tests__/index.test.ts:1097", "supports_calls"],
  ["__tests__/index.test.ts:1377", "supports_returning_an_rpc_target"],
  ["__tests__/index.test.ts:1489", "supports_passing_a_promise_in_arguments"],
  ["__tests__/index.test.ts:1581", "supports_map_on_nulls_over_rpc"],
  ["__tests__/index.test.ts:1608", "supports_map_on_arrays_over_rpc"],
  ["__tests__/limits.test.ts:83", "accepts_a_decimal_bigint_at_the_default_digit_limit"],
  ["__tests__/limits.test.ts:89", "rejects_a_bigint_over_the_default_digit_limit"],
  ["__tests__/limits.test.ts:95", "round_trips_bigints_through_the_decimal_wire_format"],
  ["__tests__/limits.test.ts:111", "accepts_decimal_bigint_strings"],
  ["__tests__/limits.test.ts:116", "honors_a_per_session_override_of_max_bigint_digits"],
  ["__tests__/limits.test.ts:136", "accepts_nesting_just_under_the_default_depth_limit"],
  ["__tests__/limits.test.ts:142", "rejects_nesting_over_the_default_depth_limit"],
  ["__tests__/limits.test.ts:147", "honors_a_per_session_override_of_max_depth"],
  ["__tests__/limits.test.ts:157", "does_not_reset_depth_across_nested_call_arguments"],
  ["__tests__/limits.test.ts:162", "accepts_nested_call_arguments_within_the_default_depth_limit"],
  ["__tests__/limits.test.ts:174", "aborts_the_session_on_a_message_over_the_size_limit"],
  ["__tests__/limits.test.ts:182", "does_not_abort_on_a_message_under_the_size_limit"],
  ["__tests__/limits.test.ts:188", "default_limits_has_the_documented_values"],
]);

function walk(relative) {
  const directory = path.join(root, relative);
  return fs.readdirSync(directory, { recursive: true })
    .filter(name => name.endsWith(".test.ts"))
    .map(name => path.join(directory, name));
}

function rootIdentifier(expression) {
  let current = expression;
  while (ts.isCallExpression(current) || ts.isPropertyAccessExpression(current)) {
    current = current.expression;
  }
  return ts.isIdentifier(current) ? current.text : undefined;
}

function testDeclarations(filename) {
  const source = ts.createSourceFile(
    filename,
    fs.readFileSync(filename, "utf8"),
    ts.ScriptTarget.Latest,
    true,
  );
  const declarations = [];
  function visit(node) {
    if (ts.isCallExpression(node) && ["it", "test"].includes(rootIdentifier(node.expression))) {
      if (node.arguments.length > 0) {
        const { line } = source.getLineAndCharacterOfPosition(node.getStart(source));
        declarations.push(line + 1);
        return;
      }
    }
    ts.forEachChild(node, visit);
  }
  visit(source);
  return declarations;
}

const errors = [];
let fileCount = 0;
let testCount = 0;
for (const filename of sourceRoots.flatMap(walk).sort()) {
  fileCount++;
  const source = path.relative(root, filename);
  const counterpart = path.join(rustRoot, source.replace(/\.ts$/, ".rs"));
  if (!fs.existsSync(counterpart)) {
    errors.push(`missing Rust test file: ${path.relative(root, counterpart)}`);
    continue;
  }

  const rust = fs.readFileSync(counterpart, "utf8");
  const declarations = testDeclarations(filename);
  if (declarations.length === 0) {
    if (!rust.includes("typescript_compile_time_assertions")) {
      errors.push(`missing compile-test placeholder: ${source}`);
    }
    continue;
  }

  for (const line of declarations) {
    testCount++;
    const key = `${source}:${line}`;
    const activeFunction = activeTests.get(key);
    if (activeFunction !== undefined) {
      if (!rust.includes(`fn ${activeFunction}(`)) {
        errors.push(`active test body missing for ${key}: ${activeFunction}`);
      }
    } else if (!rust.includes(JSON.stringify(key))) {
      errors.push(`test is neither ported nor explicitly ignored: ${key}`);
    }
  }
}

for (const [key, functionName] of activeTests) {
  const [source] = key.split(":");
  const counterpart = path.join(rustRoot, source.replace(/\.ts$/, ".rs"));
  if (!fs.existsSync(counterpart) || !fs.readFileSync(counterpart, "utf8").includes(`fn ${functionName}(`)) {
    errors.push(`stale active-test entry: ${key}`);
  }
}

if (errors.length > 0) {
  console.error(errors.join("\n"));
  process.exitCode = 1;
} else {
  console.log(`Rust parity tree covers ${fileCount} JS/TS files and ${testCount} runtime test declarations.`);
}
