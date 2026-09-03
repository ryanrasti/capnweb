#!/usr/bin/env node
// Generates one Rust file for every JavaScript/TypeScript test file.
// Keep skipped tests visible: when a runtime feature lands, replace its active
// body below and remove the generated `skipped_test!` entry.

import fs from "node:fs";
import path from "node:path";
import ts from "typescript";

const root = path.resolve(import.meta.dirname, "../..");
const generatedRoot = path.join(root, "rust/tests/javascript");
const harnessOutput = path.join(root, "rust/tests/javascript_suite.rs");
const roots = ["__tests__", "__type-tests__", "packages/capnweb-validate/__tests__"];

function filesUnder(relative) {
  const absolute = path.join(root, relative);
  return fs.readdirSync(absolute, { recursive: true })
    .filter(name => name.endsWith(".test.ts"))
    .map(name => path.join(absolute, name));
}

function rootIdentifier(expression) {
  let current = expression;
  while (ts.isCallExpression(current) || ts.isPropertyAccessExpression(current)) {
    current = current.expression;
  }
  return ts.isIdentifier(current) ? current.text : undefined;
}

function testTitle(argument, source) {
  if (ts.isStringLiteral(argument) || ts.isNoSubstitutionTemplateLiteral(argument)) {
    return argument.text;
  }
  // Parameterized tests expand this declaration at runtime. Preserve its source
  // spelling as one parity entry rather than silently losing the declaration.
  return `<dynamic: ${argument.getText(source)}>`;
}

const sourceFiles = roots.flatMap(filesUnder).sort();
const suites = [];
for (const filename of sourceFiles) {
  const text = fs.readFileSync(filename, "utf8");
  const source = ts.createSourceFile(filename, text, ts.ScriptTarget.Latest, true);
  const cases = [];
  function visit(node) {
    if (ts.isCallExpression(node) && ["it", "test"].includes(rootIdentifier(node.expression))) {
      const title = node.arguments.length > 0 ? testTitle(node.arguments[0], source) : undefined;
      if (title !== undefined) {
        const { line } = source.getLineAndCharacterOfPosition(node.getStart(source));
        cases.push({ line: line + 1, title });
        return; // Do not count an `it.each()` builder inside its outer declaration.
      }
    }
    ts.forEachChild(node, visit);
  }
  visit(source);
  suites.push({ source: path.relative(root, filename), cases });
}

function rustString(value) {
  return JSON.stringify(value).replaceAll("\\u2028", "\\u{2028}").replaceAll("\\u2029", "\\u{2029}");
}

function sanitize(value, limit = 80) {
  return value.toLowerCase()
    .replaceAll(/[^a-z0-9]+/g, "_")
    .replaceAll(/^_+|_+$/g, "")
    .slice(0, limit)
    .replaceAll(/_+$/g, "");
}

function identifier(suite, testCase, index) {
  const stem = sanitize(`${path.basename(suite.source, ".test.ts")}_${testCase.title}`);
  return `js_${String(index + 1).padStart(3, "0")}_${stem}_line_${testCase.line}`;
}

function moduleIdentifier(source, index) {
  return `suite_${String(index + 1).padStart(2, "0")}_${sanitize(source, 100)}`;
}

function generatedRelativePath(source) {
  return source.replace(/\.ts$/, ".rs");
}

function reason(suite) {
  if (suite.source.startsWith("__type-tests__/")) {
    return "TypeScript compile-time assertion; generated Rust typing tests are not implemented";
  }
  if (suite.source.startsWith("packages/capnweb-validate/")) {
    return "capnweb-validate transformer and runtime validation are not implemented in Rust";
  }
  if (suite.source.endsWith("bun.test.ts")) {
    return "Bun-specific server and WebSocket APIs are unavailable in the Rust port";
  }
  if (suite.source.endsWith("workerd.test.ts")) {
    return "workerd native RPC and Durable Object integration are unavailable in Rust";
  }
  if (suite.source.endsWith("flow-control.test.ts")) {
    return "stream flow control is not implemented in Rust";
  }
  if (suite.source.endsWith("limits.test.ts")) {
    return "configurable receive-side limits are not implemented in Rust";
  }
  return "the JavaScript behavior exercised by this case is not implemented in the Rust parity slice";
}

const active = new Map([
  ["__tests__/index.test.ts:1097", `
#[tokio::test]
async fn supports_calls() {
    assert_eq!(parity_stub().square(3.0).await.expect("square call"), 9.0);
}
`],
  ["__tests__/index.test.ts:1377", `
#[tokio::test]
async fn supports_returning_an_rpc_target() {
    let CapnwebValue::Stub(counter) = parity_stub().make_counter(4.0).await.expect("counter")
    else {
        panic!("expected counter stub");
    };
    let result = TypedRpcPromise::<f64>::new(counter.call(
        vec!["increment".into()],
        RpcPayload::from_app_params(CapnwebValue::Array(vec![CapnwebValue::Number(1.0)])),
    ));
    assert_eq!(result.await.expect("increment call"), 5.0);
}
`],
  ["__tests__/index.test.ts:1489", `
#[tokio::test]
async fn supports_passing_a_promise_in_arguments() {
    let stub = parity_stub();
    let promise = stub.square(2.0);
    assert_eq!(stub.square(promise).await.expect("pipelined square"), 16.0);
}
`],
]);

fs.rmSync(generatedRoot, { recursive: true, force: true });
let harness = `// @generated by scripts/generate-js-test-parity.mjs; do not edit by hand.\n`;
harness += `//! One-for-one file and test inventory of the JavaScript suites.\n\n`;
harness += `use std::sync::Arc;\n\n`;
harness += `use capnweb::{\n`;
harness += `    CapnwebValue, RpcPayload, TypedRpcPromise, rpc_interface, rpc_target, start_session,\n`;
harness += `};\n\n`;
harness += `macro_rules! skipped_test {\n`;
harness += `    ($name:ident, $source:literal, $title:literal, $reason:literal) => {\n`;
harness += `        #[doc = concat!("JavaScript source: ", $source, " — ", $title)]\n`;
harness += `        #[test]\n`;
harness += `        #[ignore = $reason]\n`;
harness += `        fn $name() {\n`;
harness += `            panic!("skipped pending parity: {}", $title);\n`;
harness += `        }\n`;
harness += `    };\n`;
harness += `}\n\n`;
harness += `#[rpc_interface]\n`;
harness += `trait ParityApi {\n`;
harness += `    fn square(value: f64) -> f64;\n`;
harness += `    fn make_counter(value: f64) -> CapnwebValue;\n`;
harness += `}\n\n`;
harness += `struct Counter(std::sync::Mutex<f64>);\n\n`;
harness += `#[rpc_target]\n`;
harness += `impl Counter {\n`;
harness += `    fn increment(&self, amount: f64) -> f64 {\n`;
harness += `        let mut value = self.0.lock().expect("counter mutex poisoned");\n`;
harness += `        *value += amount;\n`;
harness += `        *value\n`;
harness += `    }\n`;
harness += `}\n\n`;
harness += `struct ParityTarget;\n\n`;
harness += `#[rpc_target]\n`;
harness += `impl ParityTarget {\n`;
harness += `    fn square(&self, value: f64) -> f64 {\n`;
harness += `        value * value\n`;
harness += `    }\n`;
harness += `    fn make_counter(&self, value: f64) -> Arc<Counter> {\n`;
harness += `        Arc::new(Counter(std::sync::Mutex::new(value)))\n`;
harness += `    }\n`;
harness += `}\n\n`;
harness += `fn parity_stub() -> ParityApiStub {\n`;
harness += `    let (client_transport, server_transport) = capnweb::transport::memory_transport_pair();\n`;
harness += `    let client_main = start_session(client_transport, Arc::new(ParityTarget));\n`;
harness += `    let _server_main = start_session(server_transport, Arc::new(ParityTarget));\n`;
harness += `    ParityApiStub::new(client_main)\n`;
harness += `}\n\n`;

let total = 0;
let activeCount = 0;
for (const [suiteIndex, suite] of suites.entries()) {
  const relativeOutput = generatedRelativePath(suite.source);
  const absoluteOutput = path.join(generatedRoot, relativeOutput);
  fs.mkdirSync(path.dirname(absoluteOutput), { recursive: true });

  let file = `// @generated from ${suite.source}; do not edit by hand.\n`;
  file += `//! One-for-one Rust counterpart of \`${suite.source}\`.\n`;
  if (suite.cases.some(testCase => active.has(`${suite.source}:${testCase.line}`))) {
    file += `\nuse super::*;\n`;
  }
  if (suite.cases.length === 0) {
    file += `\n// This suite consists of TypeScript compile-time assertions. Keep a visible skipped\n`;
    file += `// Rust counterpart until generated Rust compile tests exist.\n`;
    file += `skipped_test!(\n`;
    file += `    typescript_compile_time_assertions,\n`;
    file += `    ${rustString(suite.source)},\n`;
    file += `    "TypeScript compile-time assertions",\n`;
    file += `    "generated Rust compile-time typing tests are not implemented"\n`;
    file += `);\n`;
  }
  for (const [caseIndex, testCase] of suite.cases.entries()) {
    total++;
    const key = `${suite.source}:${testCase.line}`;
    const body = active.get(key);
    if (body !== undefined) {
      activeCount++;
      file += body;
      continue;
    }
    file += `\nskipped_test!(\n`;
    file += `    ${identifier(suite, testCase, caseIndex)},\n`;
    file += `    ${rustString(`${suite.source}:${testCase.line}`)},\n`;
    file += `    ${rustString(testCase.title)},\n`;
    file += `    ${rustString(reason(suite))}\n`;
    file += `);\n`;
  }
  fs.writeFileSync(absoluteOutput, file);

  const pathFromHarness = path.relative(path.dirname(harnessOutput), absoluteOutput).replaceAll(path.sep, "/");
  harness += `#[path = ${rustString(pathFromHarness)}]\n`;
  harness += `mod ${moduleIdentifier(suite.source, suiteIndex)};\n`;
}

if (activeCount !== active.size) {
  const discovered = new Set(suites.flatMap(suite =>
    suite.cases.map(testCase => `${suite.source}:${testCase.line}`)));
  const missing = [...active.keys()].filter(key => !discovered.has(key));
  throw new Error(`active Rust ports no longer match JavaScript tests: ${missing.join(", ")}`);
}

harness += `\n// Inventory: ${suites.length} files, ${total} test declarations; `;
harness += `${activeCount} active, ${total - activeCount} ignored.\n`;
fs.writeFileSync(harnessOutput, harness);
console.log(`generated ${suites.length} Rust files: ${total} tests (${activeCount} active, ${total - activeCount} ignored)`);
