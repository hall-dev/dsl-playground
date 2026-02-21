# DSL Playground (v0 MVP)

This repo is a Rust workspace + React app for a tiny pipeline DSL.

## Architecture

- `crates/dsl_syntax`: parser + AST for v0 syntax.
- `crates/dsl_runtime`: deterministic in-memory execution (fixtures in, tables/logs out).
- `crates/dsl_wasm`: stable minimal WASM API wrappers around compile/run.
- `web/`: Vite + React playground UI.

The runtime intentionally keeps dynamic values in v0 and does runtime checks for reversible stages.

## Quickstart

### Native (library tests)

```bash
cargo test
```

### Web

```bash
cd web
npm install
npm run dev
```

Optional: build wasm package for real execution in UI (otherwise the UI shows a placeholder message):

```bash
cargo install wasm-pack
wasm-pack build crates/dsl_wasm --target web --out-dir pkg
```

Then restart `npm run dev`.
