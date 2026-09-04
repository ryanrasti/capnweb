#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" != "--reset" ]]; then
  printf 'refusing to replace translated tests; rerun with --reset\n' >&2
  exit 1
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source_dir="$repo_root/__tests__"
destination_dir="$repo_root/rust/tests/javascript"

comment_source() {
  local source="$1"
  local destination="$2"

  mkdir -p "$(dirname "$destination")"
  : > "$destination"
  while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ -z "$line" ]]; then
      printf '%s\n' '// JS:' >> "$destination"
    else
      printf '// JS: %s\n' "$line" >> "$destination"
    fi
  done < "$source"
}

rm -rf "$destination_dir"
mkdir -p "$destination_dir"

# Bun- and workerd-specific suites remain outside the portable Rust test scope.
comment_source "$source_dir/index.test.ts" "$destination_dir/index_test.rs"
comment_source "$source_dir/flow-control.test.ts" "$destination_dir/flow_control_test.rs"
comment_source "$source_dir/limits.test.ts" "$destination_dir/limits_test.rs"
