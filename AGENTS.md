# Instructions for Codex agents (dsl-playground)

This repo is a Rust workspace + React (Vite) app for a tiny pipeline DSL.

## Source of truth
- `SPEC.md` defines **v0 contract**. Do not break it.
- `LANGUAGE.md` describes **what is supported right now** (v0 + v1 preview). Update this whenever functionality changes.
- `SPEC_FULL.md` is **north-star**; do not treat it as implemented unless explicitly stated in `LANGUAGE.md`.

## Development principles
1) **Backwards compatibility:** Existing v0 acceptance programs and current web examples must keep running.
2) **Small PRs:** Each PR should be focused (parser OR runtime OR web UI OR docs), with tests.
3) **Determinism:** Browser/WASM runtime must remain deterministic by default. No network/filesystem/RNG without explicit gating.
4) **Clear outputs:** Any stage that changes output shape must be documented in `LANGUAGE.md`.

## Runtime conventions
- Values are dynamic (`Null/Bool/I64/String/Bytes/Array/Record/Unit`).
- Stages are applied with `|>`; stage composition is `>>`; inverse is `~`.
- Reversible stage direction is chosen by runtime type tags unless `~` forces inverse.
- Stages that produce structured outputs should prefer stable record shapes:
  - `lookup.*` outputs `{ left, right }`
  - grouping outputs `{ key, items }`

## Adding a new stage
When adding a stage:
1) Update `dsl_runtime`:
   - Parse stage call + args in `eval_expr` (stage constructor)
   - Implement `apply_stage` behavior
2) Add tests in `crates/dsl_runtime/tests/` covering:
   - happy path
   - type errors / missing args
3) Add or update a web example in `web/src/App.tsx` (or future examples loader)
4) Update `LANGUAGE.md`:
   - stage signature
   - output shapes
   - minimal example

## Parser conventions
- Keep parsing errors span-aware and human-friendly.
- Avoid adding syntax that conflicts with existing operators (`:=`, `|>`, `>>`, `~`).

## Web UI conventions
- The playground should render:
  - `Explain` output
  - tables as HTML tables when possible
  - logs
- Prefer minimal dependencies; if adding an editor, keep it lightweight and stable.
- Ensure `npm run build` passes.

## Checks before merging
- `cargo test`
- `cd web && npm run build`
- Ensure example programs in UI still run
- If behavior changes: update `LANGUAGE.md` and note it in `README.md` “Implemented features” section.
