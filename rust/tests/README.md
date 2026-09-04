# Rust tests

The previous condensed-runtime tests and generated ignored-test inventory were removed before the
line-preserving rewrite.

Portable JavaScript suites are copied line-for-line into `tests/javascript/`, with each original
line preserved as an exact `// JS:` comment and the handwritten Rust equivalent placed immediately
below it. `copy-js-tests.sh --reset` seeds these files destructively; `check-js-test-lines.sh` pins
and checks the source baseline. These files are translations, not generated ignored-test inventories.

`capnweb-validate`, Bun, workerd, and JavaScript-built-in identity tests are not copied. Only tests
that directly require the explicitly deferred Web built-ins may be ignored.

`js-peer.mjs` remains as the JavaScript interoperability fixture for the eventual JavaScript↔Rust
session tests.
