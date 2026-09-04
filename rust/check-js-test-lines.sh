#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

baseline_commit=2b2dc89049c08ed6bf01f5955b8cdfc606e45896
baseline_tree=c1b927f1ba1dda6f4af89c2aa5d925a236fbdff8
actual_tree="$(git rev-parse HEAD:__tests__)"

if [[ "$actual_tree" != "$baseline_tree" ]]; then
  printf '__tests__/ tree differs from parity baseline %s (found %s)\n' \
    "$baseline_tree" "$actual_tree" >&2
  exit 1
fi

if ! git diff --quiet "$baseline_commit" -- __tests__; then
  printf '__tests__/ has uncommitted changes relative to parity baseline %s\n' \
    "$baseline_commit" >&2
  exit 1
fi

python3 - <<'PY'
from pathlib import Path

pairs = {
    "__tests__/index.test.ts": "rust/tests/javascript/index_test.rs",
    "__tests__/flow-control.test.ts": "rust/tests/javascript/flow_control_test.rs",
    "__tests__/limits.test.ts": "rust/tests/javascript/limits_test.rs",
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
