# LANGUAGE.md

This document describes the **currently supported DSL** in this repo:

- **v0 MVP** (stable baseline)
- **implemented v1 preview** stages that already run in the current runtime

`SPEC.md` remains the source of truth for v0 behavior.

## Syntax

- Binding: `name := expr;`
- Pipeline: `expr |> stage |> stage ... ;`
- Stage composition: `a >> b`
- Stage inversion: `~stage` (for reversible stages)

Example:

```dsl
chain := base64 >> ~base64;
input.json("bs") |> chain |> ui.table("out");
```

## Expressions

Supported expression forms today:

- Scalars: `null`, booleans, i64 numbers, strings
- Records: `{ id: _.id, name: _.name }`
- Arrays: `[1, 2, 3]`
- Field access: `_.user_id`, `x.field`
- Placeholder `_` inside expression-evaluating stages/functions
- Operators:
  - `+` for number addition
  - `+` for string concatenation
  - `>` comparisons

Examples:

```dsl
map(_ + 1)
map("user/" + _.id)
filter(_.score > 10)
map({ id: _.id, tags: ["a", "b"] })
```

## Built-in stages

## v0 stages

- Source: `input.json(name="...")` / `input.json("...")`
- Pure: `map(expr)`, `filter(expr)`, `flat_map(expr)`
- Reversible: `json`, `utf8`, `base64`
- Sinks: `ui.table("name")`, `ui.log("name")`
- Domain demo stage: `rbac.evaluate(...)`

## Implemented v1 preview stages

- `kv.load(store="name", key_field="key", value_field="value")`
- `lookup.kv(store="name", key=expr)`
- `lookup.batch_kv(store="name", key=expr, batch_size=..., within_ms=...)`
- `group.collect_all(by_key=expr, within_ms=..., limit=...)`
- `array.map(arr, expr)`
- `array.filter(arr, expr)`
- `array.flat_map(arr, expr)`
- `array.any(arr, expr)`
- `array.contains(arr, value)`
- `default(value, fallback)`

## Output shapes to know

- `lookup.kv` and `lookup.batch_kv` emit records shaped like:
  - `{ left: <input_row>, right: <matched_value_or_null> }`
- `group.collect_all` emits records shaped like:
  - `{ key: <group_key>, items: [<original_rows...>] }`

## Known limitations

- No user-defined lambdas/functions yet.
- No windowing/time operators yet.
- No top-k/ranking stages yet.
- No general stateful stage API yet.

---

## Runnable samples

### 1) v0 map/filter

```dsl
xs := input.json("xs") |> json;
xs |> map(_ + 1) |> filter(_ > 2) |> ui.table("out");
```

Fixtures:

```json
{"xs":[1,2,3]}
```

### 2) reversible composition (`>>` and `~`)

```dsl
chain := base64 >> ~base64;
input.json("bs") |> chain |> ui.table("t");
```

Fixtures:

```json
{"bs":["AQID","SGVsbG8="]}
```

### 3) KV + single lookup

```dsl
input.json("users") |> json |> kv.load(store="users");

input.json("events")
  |> json
  |> lookup.kv(store="users", key=_.user_id)
  |> ui.table("joined");
```

Fixtures:

```json
{
  "users": [
    {"key":"u1","value":{"name":"Ada"}},
    {"key":"u2","value":{"name":"Lin"}}
  ],
  "events": [
    {"user_id":"u1","action":"login"},
    {"user_id":"u9","action":"logout"}
  ]
}
```

### 4) batch KV lookup

```dsl
input.json("users") |> json |> kv.load(store="users");

input.json("events")
  |> json
  |> lookup.batch_kv(store="users", key=_.user_id, batch_size=100, within_ms=10)
  |> ui.table("batch_joined");
```

Fixtures: same as sample 3.

### 5) `group.collect_all` shape demo

```dsl
input.json("rows")
  |> json
  |> group.collect_all(by_key=_.team, within_ms=1000, limit=10)
  |> ui.table("groups");
```

Fixtures:

```json
{
  "rows": [
    {"team":"a","id":1},
    {"team":"b","id":2},
    {"team":"a","id":3}
  ]
}
```

### 6) Stories tray snapshot (simple)

```dsl
inbox := input.json("inbox") |> json;

inbox
  |> filter(_.expires_at > "2026-02-21T12:00:00Z")
  |> map({ author_id: _.author_id, story_id: _.story_id, created_at: _.created_at })
  |> ui.table("tray_items");
```

Fixtures:

```json
{
  "inbox": [
    {"author_id":"user/a1","story_id":"s1","created_at":"2026-02-21T10:00:00Z","expires_at":"2026-02-22T10:00:00Z"},
    {"author_id":"user/a1","story_id":"s2","created_at":"2026-02-21T11:00:00Z","expires_at":"2026-02-22T11:00:00Z"},
    {"author_id":"user/a2","story_id":"s3","created_at":"2026-02-20T23:00:00Z","expires_at":"2026-02-21T11:30:00Z"}
  ]
}
```

### 7) RBAC evaluation

```dsl
requests := input.json("requests") |> json;

requests
  |> rbac.evaluate(
    principal_bindings="principal_bindings",
    role_perms="role_perms",
    resource_ancestors="resource_ancestors"
  )
  |> ui.table("decisions");
```

Fixtures: see `examples/demos/07_rbac_full/fixtures.json`.

### 8) LeetCode-ish: "plus one"

```dsl
input.json("rows")
  |> json
  |> map({
    input: _.nums,
    output: array.map(_.nums, _ + 1)
  })
  |> ui.table("plus_one");
```

Fixtures:

```json
{"rows":[{"nums":[1,2,3]},{"nums":[9]}]}
```

### 9) LeetCode-ish: "group by key and collect"

```dsl
input.json("pairs")
  |> json
  |> group.collect_all(by_key=_.k, within_ms=100, limit=50)
  |> map({ key: _.key, values: array.map(_.items, _.v) })
  |> ui.table("grouped");
```

Fixtures:

```json
{"pairs":[{"k":"a","v":1},{"k":"b","v":2},{"k":"a","v":3}]}
```
