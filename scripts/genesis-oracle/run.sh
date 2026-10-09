#!/usr/bin/env bash
# Build the independent test oracle without modifying the supplied Agave checkout.
set -euo pipefail
revision=7e51da963aee49622a395f562386a6bd8ba0e717
if [[ $# -lt 1 || $# -gt 2 || (${2:-} != "" && ${2:-} != --with-shreds) ]]; then
  echo "usage: $0 /path/to/agave [--with-shreds]" >&2
  exit 2
fi
agave=$(cd "$1" && pwd)
actual=$(git -C "$agave" rev-parse "$revision^{commit}")
[[ "$actual" == "$revision" ]]
source_dir=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d "${TMPDIR:-/tmp}/mithril-genesis-oracle.XXXXXX")
# Leave the isolated source/build available for subsequent differential tests.
git -C "$agave" archive "$revision" | tar -x -C "$work"
mkdir -p "$work/runtime/examples"
cp "$source_dir/main.rs" "$work/runtime/examples/mithril_genesis_oracle.rs"
# Only expose an existing read-only accessor to the example. Bank behavior is unchanged.
python3 - "$work/runtime/src/bank.rs" <<'PY'
from pathlib import Path
import sys
p=Path(sys.argv[1])
s=p.read_text()
old='pub(crate) fn get_fields_to_serialize'
assert s.count(old)==1
p.write_text(s.replace(old,'pub fn get_fields_to_serialize'))
PY
(cd "$work" && cargo build --locked -p solana-runtime --example mithril_genesis_oracle --features dev-context-only-utils,agave-unstable-api)
printf '\nOracle executable: %s\n' "$work/target/debug/examples/mithril_genesis_oracle"
printf 'Run Go parity tests with MITHRIL_AGAVE_ORACLE set to that absolute path.\n'
if [[ ${2:-} == --with-shreds ]]; then
  mkdir -p "$work/ledger/examples"
  cp "$source_dir/shreds.rs" "$work/ledger/examples/mithril_shred_oracle.rs"
  # RocksDB's bindgen dependency needs libclang at build and dynamic-load time.
  if [[ $(uname -s) == Darwin && -f "$(xcode-select -p)/usr/lib/libclang.dylib" ]]; then
    export LIBCLANG_PATH="${LIBCLANG_PATH:-$(xcode-select -p)/usr/lib}"
    export DYLD_FALLBACK_LIBRARY_PATH="$LIBCLANG_PATH${DYLD_FALLBACK_LIBRARY_PATH:+:$DYLD_FALLBACK_LIBRARY_PATH}"
  fi
  (cd "$work" && cargo build --locked -p solana-ledger --example mithril_shred_oracle --features dev-context-only-utils,agave-unstable-api)
  printf '\nShred oracle executable: %s\n' "$work/target/debug/examples/mithril_shred_oracle"
  printf 'Run signed-ingress tests with MITHRIL_AGAVE_SHRED_ORACLE set to that absolute path.\n'
fi
