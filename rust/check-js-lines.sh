#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

baseline_commit=2b2dc89049c08ed6bf01f5955b8cdfc606e45896
baseline_tree=185594ec5a9b8ba0d02cb0b6dfe6483038417fc1
actual_tree="$(git rev-parse HEAD:src)"

if [[ "$actual_tree" != "$baseline_tree" ]]; then
  printf 'src/ tree differs from parity baseline %s (found %s)\n' \
    "$baseline_tree" "$actual_tree" >&2
  exit 1
fi

if ! git diff --quiet "$baseline_commit" -- src; then
  printf 'src/ has uncommitted changes relative to parity baseline %s\n' \
    "$baseline_commit" >&2
  exit 1
fi

python3 - <<'PY'
from pathlib import Path

pairs = {
    "src/index.ts": "rust/src/lib.rs",
    "src/symbols.ts": "rust/src/symbols.rs",
    "src/core.ts": "rust/src/core.rs",
    "src/serialize.ts": "rust/src/serialize.rs",
    "src/rpc.ts": "rust/src/rpc.rs",
    "src/map.ts": "rust/src/map.rs",
    "src/streams.ts": "rust/src/streams.rs",
    "src/websocket.ts": "rust/src/websocket.rs",
    "src/messageport.ts": "rust/src/messageport.rs",
    "src/batch.ts": "rust/src/batch.rs",
}

failed = False
for source_name, rust_name in pairs.items():
    source = Path(source_name).read_text().splitlines()
    rust_lines = Path(rust_name).read_text().splitlines()
    copied = []
    for line_number, line in enumerate(rust_lines, 1):
        marker = line.lstrip()
        if marker == "// JS:":
            copied.append("")
        elif marker.startswith("// JS: "):
            copied.append(marker[len("// JS: "):])
        elif marker.startswith("// JS:"):
            print(f"{rust_name}:{line_number}: malformed // JS: marker")
            failed = True

    if copied != source:
        failed = True
        common = min(len(source), len(copied))
        mismatch = next((i for i in range(common) if source[i] != copied[i]), common)
        if mismatch < common:
            print(
                f"{rust_name}: source line {mismatch + 1} differs:\n"
                f"  expected {source[mismatch]!r}\n"
                f"  found    {copied[mismatch]!r}"
            )
        else:
            print(
                f"{rust_name}: expected {len(source)} source markers, "
                f"found {len(copied)}"
            )
    else:
        print(f"{rust_name}: {len(source)} source lines preserved")

if failed:
    raise SystemExit(1)
PY
