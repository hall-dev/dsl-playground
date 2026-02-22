# DSL Playground v0 Spec

## Syntax (v0)
- Binding: `name := expr;`
- Pipeline statement: `expr |> stageExpr |> stageExpr ... ;`
- Stage composition: `a >> b` produces a stage value.
- Stage inversion: `~s` forces inverse direction (only for reversible stages).
- Identifiers: `[A-Za-z_][A-Za-z0-9_]*`
- Strings: double-quoted JSON-style escapes.
- Numbers: i64 only (v0).
- Arrays: `[e1, e2, ...]`
- Records: `{ field: expr, ... }`
- Field access: `x.field`
- Placeholder var: `_` inside `map(...)`, `filter(...)`, `flat_map(...)`.

## Semantics
- Everything is a stream of values.
- `|>` applies a stage to a stream.
- Stages are values; some are reversible.
- Direction inference for `stream |> stage`:
  - If input type matches stage forward input: use forward.
  - Else if stage has inverse and input matches inverse input: use inverse.
  - Else error.
- `~stage` forces inverse. If stage is not reversible -> error.
- Composition `a >> b` composes forward directions: output of `a` must match input of `b`.
  - `~(a >> b)` behaves as `(~b) >> (~a)` (only if both reversible).

## Types (v0 - dynamic)
v0 runtime uses dynamic values:
- Null, Bool, I64, String, Bytes, Array, Record
- No static typechecking in v0. Direction inference uses runtime tag checks:
  - Example: utf8 forward expects String, inverse expects Bytes.

## Built-in stages (v0)
### Sources
- `input.json(name: string)` -> Stream[Bytes]
  - Reads fixture named `name` which is JSON array in the fixtures map.
  - Each element is encoded as bytes representing the JSON element (not the whole array).
  - If fixture is missing -> error.

### Pure stages
- `map(expr)` : T -> U, evaluates expr with `_` bound to current value.
- `filter(expr)` : T -> T, keeps element if expr evaluates to true.
- `flat_map(expr)` : T -> U, expr must evaluate to Array[U], emits each element.
- `rank.topk(k, by, order)` : selects top K by `by` key (`I64`/`String`), `order` in {`"asc"`, `"desc"`}.
- `rank.kmerge_arrays(by, order, limit)` : input item must be `Array[Array[Value]]` of pre-sorted lists; merges lists by key and emits up to `limit`.

### Reversible atoms
- `json` : Bytes <-> Record/Array/Scalar JSON (dynamic)
  - forward: Value -> Bytes (serialize JSON)
  - inverse: Bytes -> Value (parse JSON)
- `utf8` : String <-> Bytes
- `base64` : Bytes <-> String

### Sinks
- `ui.table(name: string)` : Value -> Unit
  - Appends/upserts rows to a named table.
  - v0: append-only (no keying).
- `ui.log(name: string)` : Value -> Unit
  - Appends stringified value to named log.

## WASM API
Expose from wasm module:
- `compile(program: string) -> { ok: bool, diagnostics: string }`
- `run(program: string, fixtures_json: string) -> { tables_json: string, logs_json: string, explain: string }`
  - fixtures_json is a JSON object mapping fixture-name -> JSON array.
  - tables_json: JSON map tableName -> array of JSON values
  - logs_json: JSON map logName -> array of strings
  - explain: textual plan (list stages)

## Explain (v0)
Return a simple list:
- each statement, each stage in order
- tag stage as [source]/[pure]/[reversible]/[sink]

## Acceptance programs
A. Map/filter:

```
xs := input.json("xs") |> json;
xs |> map(_ + 1) |> filter(_ > 2) |> ui.table("out");
```

B. Roundtrip:

```
chain := base64 >> ~base64;
input.json("bs") |> chain |> ui.table("t");
```

C. UTF8:

```
input.json("ss") |> json |> utf8 |> ~utf8 |> ui.table("rt");
```
