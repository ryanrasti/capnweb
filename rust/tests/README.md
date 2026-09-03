# Rust tests

The previous condensed-runtime tests and generated ignored-test inventory were removed before the
line-preserving rewrite.

Tests will be restored as handwritten Rust counterparts after each runtime source file is
translated. `capnweb-validate`, Bun, workerd, and JavaScript-built-in identity tests are not copied.
Only tests that directly require the explicitly deferred Web built-ins may be ignored.

`js-peer.mjs` remains as the JavaScript interoperability fixture for the eventual JavaScript↔Rust
session tests.
