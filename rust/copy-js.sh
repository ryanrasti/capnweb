#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
src_dir="$repo_root/src"
rust_src_dir="$repo_root/rust/src"

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

rm -rf "$rust_src_dir"
mkdir -p "$rust_src_dir"

# Runtime files covered by rust/JS-PARITY.md. Keep this list explicit so adding
# a JavaScript runtime file requires an intentional parity decision.
comment_source "$src_dir/index.ts"       "$rust_src_dir/lib.rs"
comment_source "$src_dir/symbols.ts"     "$rust_src_dir/symbols.rs"
comment_source "$src_dir/core.ts"        "$rust_src_dir/core.rs"
comment_source "$src_dir/serialize.ts"   "$rust_src_dir/serialize.rs"
comment_source "$src_dir/rpc.ts"         "$rust_src_dir/rpc.rs"
comment_source "$src_dir/map.ts"         "$rust_src_dir/map.rs"
comment_source "$src_dir/streams.ts"     "$rust_src_dir/streams.rs"
comment_source "$src_dir/websocket.ts"   "$rust_src_dir/websocket.rs"
comment_source "$src_dir/messageport.ts" "$rust_src_dir/messageport.rs"
comment_source "$src_dir/batch.ts"       "$rust_src_dir/batch.rs"
