#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat >&2 <<'USAGE'
Usage: scripts/install_bellerophon_rs.sh PREFIX

Install bellerophon-rs into PREFIX/bin using cargo install --path.

Examples:
  scripts/install_bellerophon_rs.sh "$HOME/.local"
  scripts/install_bellerophon_rs.sh /nfs7/path/to/prefix
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

if [[ $# -ne 1 ]]; then
    usage
    exit 2
fi

PREFIX="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CARGO_BIN="${CARGO:-cargo}"

if [[ -z "${LIBCLANG_PATH:-}" ]]; then
    for candidate in "${CONDA_PREFIX:-}/lib" "$REPO_ROOT/.pixi/envs/default/lib"; do
        if [[ -f "$candidate/libclang.so" ]]; then
            export LIBCLANG_PATH="$candidate"
            break
        fi
    done
fi

mkdir -p "$PREFIX/bin"
"$CARGO_BIN" install --path "$REPO_ROOT/rust/bellerophon-rs" --root "$PREFIX" --locked --force

cat <<EOF
Installed bellerophon-rs to:
  $PREFIX/bin/bellerophon-rs

Add this to PATH if needed:
  export PATH="$PREFIX/bin:\$PATH"
EOF
