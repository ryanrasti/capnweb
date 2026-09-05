// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    include: ["__tests__/**/*.test.ts"],
    // These are monorepo integration tests. The standalone package has no
    // dependency on the sibling Rust source tree.
    exclude: [
      "__tests__/generated-rust.test.ts",
      "__tests__/kitchen-sink.test.ts",
    ],
  },
});
