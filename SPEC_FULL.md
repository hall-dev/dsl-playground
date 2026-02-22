# DSL Playground Full Spec (North Star)

This document is a future-facing spec. v0 behavior is defined by SPEC.md and must remain stable.
Implement v1+ incrementally via new stages and optional typed layer.

## 1. Goals
- Keep v0 syntax: `:=`, `|>`, `>>`, `~`
- Everything is a stream; stages transform streams
- Small, safe, deterministic playground runtime (WASM)
- Optional: compile-to-Rust for performance later (server-side or native)

## 2. Core language
### 2.1 Statements
- Binding: `name := expr;`
- Pipeline statement: `expr |> stageExpr |> ... ;`

### 2.2 Expressions
- Literals: null/bool/i64/string/bytes
- Arrays: `[e1, e2, ...]`
- Records: `{ field: expr, ... }`
- Field access: `x.field`
- Calls: `ident(args...)` and `ident(key=value, ...)`
- Operators (v1): `== != < <= > >= + - * / && ||`
- Control (v2): `if ... then ... else ...`, `match`, `fn`, `for`

### 2.3 Stages
- A stage is a value applied in pipelines using `|>`.
- Reversible stage: has an inverse; `~stage` forces inverse.
- Composition: `a >> b` composes forward directions. If both reversible, inverse is `~b >> ~a`.

### 2.4 Dynamic types (v0/v1)
Runtime values: Null, Bool, I64, String, Bytes, Array, Record, Unit.
Direction inference uses runtime tags.

### 2.5 Optional static types (v2+)
- `type` and `enum` declarations
- `Stream[T]` and typed stages
- Type-directed direction inference for reversible stages
- This is optional; v0/v1 remain dynamic.

## 3. Runtime model
- Iterator-first pull execution internally; UI and topics push into internal queues.
- Deterministic by default. Nondeterministic sources gated.
- Bounded budgets: max events, max state size, max output rows.

## 4. Built-in stdlib (planned catalog)

### 4.1 Core stream algebra
- `map(expr)`, `filter(expr)`, `flat_map(expr)`
- `mux(a,b,...)`
- `key_by(expr)` (v1)

### 4.2 Data + collections helpers (v1)
- `array.map/filter/flat_map/any/all/fold/contains/uniq`
- `map.get/put/contains`, `set.from/contains`

### 4.3 Grouping, collecting, ranking (v1)
- `group.collect_all(by_key=expr, within_ms=i64, limit=i64)`
- `group.topn_items(by_key=expr, n=i64, order_by=expr, order=asc|desc)`
- `rank.topk(k=i64, by=expr, order=asc|desc, tie=expr)`
- `collect.array(within_ms=i64, limit=i64)`
- `collect.handles(within_ms=i64, limit=i64)` (stream-of-streams)

### 4.4 Lookup / KV / DB (v1)
- `kv.load(store, key_field="key", value_field="value")` for fixtures
- `lookup.kv(store, key=expr)`
- `lookup.batch_kv(store, key=expr, batch_size=i64, within_ms=i64)`
- `db.query/db.exec` (future; in playground use mock stores)

### 4.5 Time and state (v1/v2)
- `ticker(every=duration)` (source)
- `stateful { ... }` or functional stateful stage
- `window.tumbling/sliding` + `aggregate(...)` (v2)
- `time.debounce/throttle` (v2)

### 4.6 Reversible atoms (v0)
- `json`, `utf8`, `base64` (+ `gzip` later)
- Reversible atoms are ordinary stages.

### 4.7 UI integration (v2)
UI widgets are sources and sinks:
- sources: `ui.button`, `ui.textbox`, `ui.select`, `ui.table_edit`, `websocket.sessions`
- sinks: `ui.table`, `ui.log`, `ui.set_text`, `ui.toast`, `ui.chart`, `ui.canvas`
Reactive helpers:
- `react.combine_latest`, `react.sample_on`

### 4.8 Batteries (optional, built from primitives)
- `rbac.evaluate` (can be DSL or Rust stage)
- `timeline.pull`
- `dispatch.match`
- `stories.tray.build/live`

## 5. Explain
- Explain plan lists stages with tags: source/pure/reversible/effect/sink
- Planner notes: inserted buffers/shards (v2+)

## 6. Security / sandbox
- No network/filesystem in browser mode
- Effect stages must be explicitly enabled in hosted mode
- Quotas and budgets enforced

## 7. Incremental roadmap
- v1: KV stores + lookup + batch lookup + group.collect_all + array helpers
- v2: UI event sources + reactive helpers + view.materialize
- v3: typed layer + compile-to-rust option
