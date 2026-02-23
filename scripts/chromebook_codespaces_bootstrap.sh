#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Install Rust/Cargo when missing, then load cargo env for this shell.
if ! command -v cargo >/dev/null 2>&1; then
  curl https://sh.rustup.rs -sSf | sh -s -- -y
fi

# Ensure cargo/bin is available in this shell even if rustup was just installed.
if [ -f "$HOME/.cargo/env" ]; then
  # shellcheck disable=SC1090
  . "$HOME/.cargo/env"
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo is not available on PATH after rustup install" >&2
  exit 1
fi

# Install wasm-pack only if not already present.
if ! command -v wasm-pack >/dev/null 2>&1; then
  cargo install wasm-pack
fi

# Build WASM package when the crate is configured for wasm-bindgen.
if grep -Eq '^wasm-bindgen\s*=' crates/dsl_wasm/Cargo.toml; then
  wasm-pack build crates/dsl_wasm --target web --out-dir crates/dsl_wasm/pkg
else
  echo "warning: crates/dsl_wasm/Cargo.toml does not declare wasm-bindgen; skipping wasm-pack build." >&2
  echo "warning: playground UI will run, but execution shows 'WASM package not built yet' until wasm bindings are configured." >&2
fi

cd web
npm install --no-audit --no-fund
npm run dev
