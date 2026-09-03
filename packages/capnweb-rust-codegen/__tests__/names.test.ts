// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import { describe, expect, test } from "vitest";

import {
  NameAllocator,
  toRustFieldName,
  toRustTypeName,
} from "../src/names.js";

describe("Rust names", () => {
  test.each([
    ["displayName", "display_name"],
    ["userID", "user_id"],
    ["URLValue", "url_value"],
    ["already_snake", "already_snake"],
    ["type", "r#type"],
    ["123value", "field_123value"],
  ])("maps field name %s to %s", (source, expected) => {
    expect(toRustFieldName(source)).toBe(expected);
  });

  test.each([
    ["user", "User"],
    ["user_event", "UserEvent"],
    ["URLValue", "UrlValue"],
    ["123value", "Type123value"],
  ])("maps type name %s to %s", (source, expected) => {
    expect(toRustTypeName(source)).toBe(expected);
  });

  test("resolves post-conversion collisions deterministically", () => {
    let names = new NameAllocator();
    expect(names.allocate("user_id")).toBe("user_id");
    expect(names.allocate("user_id")).toBe("user_id_2");
    expect(names.allocate("r#type")).toBe("r#type");
    expect(names.allocate("r#type")).toBe("type_2");
  });
});
