# DSL Playground (v0 MVP)

This repo is a Rust workspace + React app for a tiny pipeline DSL.

## Architecture

- `crates/dsl_syntax`: parser + AST for v0 syntax.
- `crates/dsl_runtime`: deterministic in-memory execution (fixtures in, tables/logs out).
- `crates/dsl_wasm`: stable minimal WASM API wrappers around compile/run.
- `web/`: Vite + React playground UI.

The runtime intentionally keeps dynamic values in v0 and does runtime checks for reversible stages.

## v0 MVP framing

The baseline language/runtime target is still **v0 MVP** (`SPEC.md`).

## Implemented v1 preview stages

The runtime also includes a small preview subset from `SPEC_FULL.md`:

- `kv.load`
- `lookup.kv`
- `lookup.batch_kv`
- `group.collect_all`
- `array.map`
- `array.filter`
- `array.flat_map`
- `array.any`
- `array.contains`
- `default`

See `LANGUAGE.md` for syntax, output shapes, and runnable examples.

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

Optional: build wasm package for execution in UI (otherwise the UI shows a placeholder message):

```bash
cargo install wasm-pack
wasm-pack build crates/dsl_wasm --target web --out-dir crates/dsl_wasm/pkg
```

Then restart `npm run dev`.

## Try examples

In the web app, use the **Example program** dropdown to load runnable demos (including RBAC and v1 preview examples), then click **Run**.

## Browser-only (Chromebook) checklist

If you only have a browser (for example, on a Chromebook), use GitHub Codespaces:

1. Fork this repository in GitHub.
2. Click **Code** → **Codespaces** → **Create codespace on main**.
3. In the Codespaces terminal, run the bootstrap script:

   ```bash
   bash scripts/chromebook_codespaces_bootstrap.sh
   ```

   The script is robust to missing or preinstalled toolchains: it installs Rust/Cargo only when needed, sources `~/.cargo/env` in the current shell, installs `wasm-pack` only when needed, and starts the web app.

4. Open the forwarded Vite port (usually `5173`) in your browser.
5. In the playground, select an example program and click **Run**.

Notes:
- If `cargo install wasm-pack` takes too long, you can still open the UI; the script will continue and the playground will show a placeholder when WASM bindings are unavailable.
- If `crates/dsl_wasm/Cargo.toml` does not include `wasm-bindgen`, the script skips `wasm-pack build` to avoid the common Codespaces error and prints a warning.
- Re-run the script whenever you change Rust/WASM code.
