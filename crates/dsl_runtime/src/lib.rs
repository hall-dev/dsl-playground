use dsl_syntax::{parse_program, CallArg, Expr, Program, Stmt};
use serde_json::{Map, Value as JsonValue};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    I64(i64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<Value>),
    Record(BTreeMap<String, Value>),
    Unit,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Stream {
    values: Vec<Value>,
}

impl Stream {
    fn new(values: Vec<Value>) -> Self {
        Self { values }
    }
}

impl IntoIterator for Stream {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Outputs {
    pub tables: BTreeMap<String, Vec<JsonValue>>,
    pub logs: BTreeMap<String, Vec<String>>,
    pub explain: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct RuntimeState {
    kv_stores: HashMap<String, HashMap<String, Value>>,
}

#[derive(Debug, Clone)]
enum Binding {
    Stream(Stream),
    Stage(Stage),
}

#[derive(Debug, Clone)]
enum Stage {
    Map(Expr),
    Filter(Expr),
    FlatMap(Expr),
    GroupCollectAll {
        by_key: Expr,
        within_ms: i64,
        limit: i64,
    },
    RankTopK {
        k: i64,
        by: Expr,
        order: SortOrder,
    },
    GroupTopNItems {
        by_key: Expr,
        n: i64,
        order_by: Expr,
        order: SortOrder,
    },
    KvLoad {
        store: String,
    },
    LookupKv {
        store: String,
        key: Expr,
    },
    LookupBatchKv {
        store: String,
        key: Expr,
        batch_size: i64,
        within_ms: i64,
    },
    RbacEvaluate {
        principal_bindings: String,
        role_perms: String,
        resource_ancestors: String,
    },
    Json(Direction),
    Utf8(Direction),
    Base64(Direction),
    UiTable(String),
    UiLog(String),
    Compose(Vec<Stage>),
}

#[derive(Debug, Clone, Copy)]
enum Direction {
    Auto,
    Inverse,
}

#[derive(Debug, Clone, Copy)]
enum SortOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
enum SortKey {
    I64(i64),
    String(String),
}

pub fn compile(program: &str) -> Result<Program, String> {
    parse_program(program).map_err(|e| e.to_string())
}

pub fn run(program: &str, fixtures: JsonValue) -> Result<Outputs, String> {
    let program = compile(program)?;
    let fixture_map = parse_fixtures(fixtures)?;
    let mut env: BTreeMap<String, Binding> = BTreeMap::new();
    let mut state = RuntimeState::default();
    let mut outputs = Outputs::default();

    for stmt in &program.statements {
        match stmt {
            Stmt::Binding { name, expr, .. } => {
                outputs.explain.push(format!("binding {name}"));
                let val = eval_expr(expr, &env, &fixture_map, &mut state, &mut outputs)?;
                env.insert(name.clone(), val);
            }
            Stmt::Pipeline { expr, .. } => {
                outputs.explain.push("pipeline".to_string());
                let _ = expect_stream(eval_expr(
                    expr,
                    &env,
                    &fixture_map,
                    &mut state,
                    &mut outputs,
                )?)?;
            }
        }
    }

    Ok(outputs)
}

fn eval_expr(
    expr: &Expr,
    env: &BTreeMap<String, Binding>,
    fixtures: &BTreeMap<String, Vec<JsonValue>>,
    state: &mut RuntimeState,
    outputs: &mut Outputs,
) -> Result<Binding, String> {
    match expr {
        Expr::Pipeline { input, stages, .. } => {
            let mut stream = expect_stream(eval_expr(input, env, fixtures, state, outputs)?)?;
            for stage_expr in stages {
                let stage = expect_stage(eval_expr(stage_expr, env, fixtures, state, outputs)?)?;
                stream = apply_stage(&stage, stream, fixtures, state, outputs)?;
            }
            Ok(Binding::Stream(stream))
        }
        Expr::Call { callee, args, .. } => {
            let name = callee_name(callee).ok_or_else(|| "unsupported callee".to_string())?;
            match name.as_str() {
                "input.json" => {
                    let fixture_name = expect_string(positional_arg(args, 0)?)?;
                    outputs
                        .explain
                        .push(format!("  [source] input.json({fixture_name})"));
                    let items = fixtures
                        .get(&fixture_name)
                        .ok_or_else(|| format!("missing fixture: {fixture_name}"))?;
                    let values = items
                        .iter()
                        .map(|item| {
                            serde_json::to_vec(item)
                                .map(Value::Bytes)
                                .map_err(|e| e.to_string())
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(Binding::Stream(Stream::new(values)))
                }
                "map" => Ok(Binding::Stage(Stage::Map(positional_arg(args, 0)?.clone()))),
                "filter" => Ok(Binding::Stage(Stage::Filter(
                    positional_arg(args, 0)?.clone(),
                ))),
                "flat_map" => Ok(Binding::Stage(Stage::FlatMap(
                    positional_arg(args, 0)?.clone(),
                ))),
                "group.collect_all" => Ok(Binding::Stage(Stage::GroupCollectAll {
                    by_key: named_arg(args, "by_key")?.clone(),
                    within_ms: expect_i64_literal(named_arg(args, "within_ms")?)?,
                    limit: expect_i64_literal(named_arg(args, "limit")?)?,
                })),
                "rank.topk" => Ok(Binding::Stage(Stage::RankTopK {
                    k: expect_i64_literal(named_arg(args, "k")?)?,
                    by: named_arg(args, "by")?.clone(),
                    order: parse_sort_order(named_arg(args, "order")?)?,
                })),
                "group.topn_items" => Ok(Binding::Stage(Stage::GroupTopNItems {
                    by_key: named_arg(args, "by_key")?.clone(),
                    n: expect_i64_literal(named_arg(args, "n")?)?,
                    order_by: named_arg(args, "order_by")?.clone(),
                    order: parse_sort_order(named_arg(args, "order")?)?,
                })),
                "kv.load" => Ok(Binding::Stage(Stage::KvLoad {
                    store: expect_string(named_arg(args, "store")?)?,
                })),
                "lookup.kv" => Ok(Binding::Stage(Stage::LookupKv {
                    store: expect_string(named_arg(args, "store")?)?,
                    key: named_arg(args, "key")?.clone(),
                })),
                "lookup.batch_kv" => Ok(Binding::Stage(Stage::LookupBatchKv {
                    store: expect_string(named_arg(args, "store")?)?,
                    key: named_arg(args, "key")?.clone(),
                    batch_size: expect_i64_literal(named_arg(args, "batch_size")?)?,
                    within_ms: expect_i64_literal(named_arg(args, "within_ms")?)?,
                })),
                "rbac.evaluate" => Ok(Binding::Stage(Stage::RbacEvaluate {
                    principal_bindings: expect_string(named_arg(args, "principal_bindings")?)?,
                    role_perms: expect_string(named_arg(args, "role_perms")?)?,
                    resource_ancestors: expect_string(named_arg(args, "resource_ancestors")?)?,
                })),
                "ui.table" => Ok(Binding::Stage(Stage::UiTable(expect_string(
                    positional_arg(args, 0)?,
                )?))),
                "ui.log" => Ok(Binding::Stage(Stage::UiLog(expect_string(
                    positional_arg(args, 0)?,
                )?))),
                _ => Err(format!("unsupported call: {name}")),
            }
        }
        Expr::Ident { name, .. } if name == "json" => {
            Ok(Binding::Stage(Stage::Json(Direction::Auto)))
        }
        Expr::Ident { name, .. } if name == "utf8" => {
            Ok(Binding::Stage(Stage::Utf8(Direction::Auto)))
        }
        Expr::Ident { name, .. } if name == "base64" => {
            Ok(Binding::Stage(Stage::Base64(Direction::Auto)))
        }
        Expr::Ident { name, .. } => env
            .get(name)
            .cloned()
            .ok_or_else(|| format!("unknown ident {name}")),
        Expr::Compose { left, right, .. } => Ok(Binding::Stage(Stage::Compose(vec![
            expect_stage(eval_expr(left, env, fixtures, state, outputs)?)?,
            expect_stage(eval_expr(right, env, fixtures, state, outputs)?)?,
        ]))),
        Expr::Inverse { expr, .. } => Ok(Binding::Stage(invert_stage(expect_stage(eval_expr(
            expr, env, fixtures, state, outputs,
        )?)?)?)),
        _ => Err("unsupported expression for stream/stage evaluation".to_string()),
    }
}

fn apply_stage(
    stage: &Stage,
    stream: Stream,
    fixtures: &BTreeMap<String, Vec<JsonValue>>,
    state: &mut RuntimeState,
    outputs: &mut Outputs,
) -> Result<Stream, String> {
    match stage {
        Stage::Map(expr) => {
            outputs.explain.push("  [pure] map".to_string());
            let out = stream
                .into_iter()
                .map(|item| eval_value_expr(expr, Some(&item)))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Stream::new(out))
        }
        Stage::Filter(expr) => {
            outputs.explain.push("  [pure] filter".to_string());
            let mut out = Vec::new();
            for item in stream {
                if truthy(&eval_value_expr(expr, Some(&item))?)? {
                    out.push(item);
                }
            }
            Ok(Stream::new(out))
        }
        Stage::FlatMap(expr) => {
            outputs.explain.push("  [pure] flat_map".to_string());
            let mut out = Vec::new();
            for item in stream {
                match eval_value_expr(expr, Some(&item))? {
                    Value::Array(values) => out.extend(values),
                    _ => return Err("flat_map expression must return Array".to_string()),
                }
            }
            Ok(Stream::new(out))
        }
        Stage::GroupCollectAll {
            by_key,
            within_ms,
            limit,
        } => {
            if *within_ms < 0 {
                return Err("group.collect_all within_ms must be >= 0".to_string());
            }
            if *limit < 0 {
                return Err("group.collect_all limit must be >= 0".to_string());
            }
            outputs
                .explain
                .push("  [pure] group.collect_all".to_string());

            let mut groups: Vec<(Value, Vec<Value>)> = Vec::new();
            for item in stream {
                let key = eval_value_expr(by_key, Some(&item))?;
                if let Some((_, items)) = groups.iter_mut().find(|(k, _)| *k == key) {
                    items.push(item);
                } else {
                    groups.push((key, vec![item]));
                }
            }

            let max_items = *limit as usize;
            let out = groups
                .into_iter()
                .map(|(key, mut items)| {
                    if items.len() > max_items {
                        items.truncate(max_items);
                    }
                    Value::Record(BTreeMap::from([
                        ("key".to_string(), key),
                        ("items".to_string(), Value::Array(items)),
                    ]))
                })
                .collect();
            Ok(Stream::new(out))
        }
        Stage::RankTopK { k, by, order } => {
            if *k < 0 {
                return Err("rank.topk k must be >= 0".to_string());
            }
            outputs.explain.push("  [pure] rank.topk".to_string());

            let mut rows: Vec<(usize, SortKey, Value)> = Vec::new();
            for (idx, item) in stream.into_iter().enumerate() {
                let key = expect_sort_key(
                    eval_value_expr(by, Some(&item))?,
                    "rank.topk by expression must evaluate to I64 or String",
                )?;
                rows.push((idx, key, item));
            }

            rows.sort_by(|(idx_a, key_a, _), (idx_b, key_b, _)| {
                compare_keys(key_a, key_b, *order).then_with(|| idx_a.cmp(idx_b))
            });

            let top_k = *k as usize;
            let out = rows
                .into_iter()
                .take(top_k)
                .map(|(_, _, item)| item)
                .collect();
            Ok(Stream::new(out))
        }
        Stage::GroupTopNItems {
            by_key,
            n,
            order_by,
            order,
        } => {
            if *n < 0 {
                return Err("group.topn_items n must be >= 0".to_string());
            }
            outputs
                .explain
                .push("  [pure] group.topn_items".to_string());

            let mut groups: Vec<(Value, Vec<(usize, SortKey, Value)>)> = Vec::new();
            for (idx, item) in stream.into_iter().enumerate() {
                let key = eval_value_expr(by_key, Some(&item))?;
                expect_group_key(
                    &key,
                    "group.topn_items by_key must evaluate to I64 or String",
                )?;
                let order_key = expect_sort_key(
                    eval_value_expr(order_by, Some(&item))?,
                    "group.topn_items order_by must evaluate to I64 or String",
                )?;

                if let Some((_, items)) = groups
                    .iter_mut()
                    .find(|(existing_key, _)| *existing_key == key)
                {
                    items.push((idx, order_key, item));
                } else {
                    groups.push((key, vec![(idx, order_key, item)]));
                }
            }

            let max_items = *n as usize;
            let out = groups
                .into_iter()
                .map(|(key, mut items)| {
                    items.sort_by(|(idx_a, key_a, _), (idx_b, key_b, _)| {
                        compare_keys(key_a, key_b, *order).then_with(|| idx_a.cmp(idx_b))
                    });
                    if items.len() > max_items {
                        items.truncate(max_items);
                    }
                    Value::Record(BTreeMap::from([
                        ("key".to_string(), key),
                        (
                            "items".to_string(),
                            Value::Array(items.into_iter().map(|(_, _, item)| item).collect()),
                        ),
                    ]))
                })
                .collect();
            Ok(Stream::new(out))
        }
        Stage::KvLoad { store } => {
            outputs.explain.push(format!("  [sink] kv.load({store})"));
            let kv = state.kv_stores.entry(store.clone()).or_default();
            for item in stream {
                let record = expect_record(item, "kv.load input must be Record")?;
                let key = expect_string_value(
                    record.get("key").cloned().unwrap_or(Value::Null),
                    "kv.load input.key must be String",
                )?;
                let value = record
                    .get("value")
                    .cloned()
                    .ok_or_else(|| "kv.load input must contain field 'value'".to_string())?;
                kv.insert(key, value);
            }
            Ok(Stream::new(vec![Value::Unit]))
        }
        Stage::LookupKv { store, key } => {
            outputs.explain.push(format!("  [pure] lookup.kv({store})"));
            let kv = state.kv_stores.get(store);
            let mut out = Vec::new();
            for item in stream {
                let lookup_key = expect_string_value(
                    eval_value_expr(key, Some(&item))?,
                    "lookup.kv key must evaluate to String",
                )?;
                let right = kv
                    .and_then(|s| s.get(&lookup_key).cloned())
                    .unwrap_or(Value::Null);
                out.push(Value::Record(BTreeMap::from([
                    ("left".to_string(), item),
                    ("right".to_string(), right),
                ])));
            }
            Ok(Stream::new(out))
        }
        Stage::LookupBatchKv {
            store,
            key,
            batch_size,
            within_ms,
        } => {
            if *batch_size < 0 || *within_ms < 0 {
                return Err("lookup.batch_kv batch_size/within_ms must be >= 0".to_string());
            }
            outputs
                .explain
                .push(format!("  [pure] lookup.batch_kv({store})"));
            let kv = state.kv_stores.get(store);
            let items: Vec<Value> = stream.into_iter().collect();
            let mut out = Vec::new();
            for item in items {
                let lookup_key = expect_string_value(
                    eval_value_expr(key, Some(&item))?,
                    "lookup.batch_kv key must evaluate to String",
                )?;
                let right = kv
                    .and_then(|s| s.get(&lookup_key).cloned())
                    .unwrap_or(Value::Null);
                out.push(Value::Record(BTreeMap::from([
                    ("left".to_string(), item),
                    ("right".to_string(), right),
                ])));
            }
            Ok(Stream::new(out))
        }
        Stage::RbacEvaluate {
            principal_bindings,
            role_perms,
            resource_ancestors,
        } => {
            outputs.explain.push("  [pure] rbac.evaluate".to_string());
            let bindings = fixtures
                .get(principal_bindings)
                .ok_or_else(|| format!("missing fixture: {principal_bindings}"))?;
            let perms = fixtures
                .get(role_perms)
                .ok_or_else(|| format!("missing fixture: {role_perms}"))?;
            let ancestors = fixtures
                .get(resource_ancestors)
                .ok_or_else(|| format!("missing fixture: {resource_ancestors}"))?;
            eval_rbac(stream, bindings, perms, ancestors)
        }
        Stage::Json(direction) => {
            outputs.explain.push("  [reversible] json".to_string());
            apply_reversible(
                stream,
                *direction,
                json_forward,
                json_inverse,
                accepts_json_forward,
                accepts_json_inverse,
            )
        }
        Stage::Utf8(direction) => {
            outputs.explain.push("  [reversible] utf8".to_string());
            apply_reversible(
                stream,
                *direction,
                utf8_forward,
                utf8_inverse,
                accepts_utf8_forward,
                accepts_utf8_inverse,
            )
        }
        Stage::Base64(direction) => {
            outputs.explain.push("  [reversible] base64".to_string());
            apply_reversible(
                stream,
                *direction,
                base64_forward,
                base64_inverse,
                accepts_base64_forward,
                accepts_base64_inverse,
            )
        }
        Stage::UiTable(name) => {
            outputs.explain.push(format!("  [sink] ui.table({name})"));
            let table = outputs.tables.entry(name.clone()).or_default();
            for item in stream {
                table.push(value_to_json(item));
            }
            Ok(Stream::new(vec![Value::Unit]))
        }
        Stage::UiLog(name) => {
            outputs.explain.push(format!("  [sink] ui.log({name})"));
            let log = outputs.logs.entry(name.clone()).or_default();
            for item in stream {
                let json = value_to_json(item);
                log.push(serde_json::to_string(&json).map_err(|e| e.to_string())?);
            }
            Ok(Stream::new(vec![Value::Unit]))
        }
        Stage::Compose(stages) => {
            let mut current = stream;
            for part in stages {
                current = apply_stage(part, current, fixtures, state, outputs)?;
            }
            Ok(current)
        }
    }
}

fn eval_rbac(
    stream: Stream,
    principal_bindings: &[JsonValue],
    role_perms: &[JsonValue],
    resource_ancestors: &[JsonValue],
) -> Result<Stream, String> {
    let mut roles_by_principal: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in principal_bindings {
        let principal = expect_json_string_field(row, "principal")?;
        let role = expect_json_string_field(row, "role")?;
        roles_by_principal.entry(principal).or_default().push(role);
    }

    let mut perms_by_role_action: BTreeMap<(String, String), Vec<JsonValue>> = BTreeMap::new();
    for row in role_perms {
        let role = expect_json_string_field(row, "role")?;
        let action = expect_json_string_field(row, "action")?;
        perms_by_role_action
            .entry((role, action))
            .or_default()
            .push(row.clone());
    }

    let mut ancestor_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in resource_ancestors {
        let resource = expect_json_string_field(row, "resource")?;
        let ancestor = expect_json_string_field(row, "ancestor")?;
        ancestor_map.entry(resource).or_default().push(ancestor);
    }

    let mut out = Vec::new();
    for request in stream {
        let request_json = value_to_json(request.clone());
        let principal = expect_json_string_field(&request_json, "principal")?;
        let action = expect_json_string_field(&request_json, "action")?;
        let resource = expect_json_string_field(&request_json, "resource")?;

        let roles = roles_by_principal
            .get(&principal)
            .cloned()
            .unwrap_or_default();
        let reachable_resources = collect_resource_ancestors(&resource, &ancestor_map);

        let mut matches = Vec::new();
        for role in &roles {
            if let Some(candidates) = perms_by_role_action.get(&(role.clone(), action.clone())) {
                for perm in candidates {
                    let perm_resource = expect_json_string_field(perm, "resource")?;
                    if reachable_resources.iter().any(|r| r == &perm_resource) {
                        matches.push(perm.clone());
                    }
                }
            }
        }

        out.push(json_to_value(JsonValue::Object(Map::from_iter([
            ("request".to_string(), request_json),
            (
                "decision".to_string(),
                JsonValue::String(if matches.is_empty() {
                    "deny".to_string()
                } else {
                    "allow".to_string()
                }),
            ),
            ("matches".to_string(), JsonValue::Array(matches)),
        ]))));
    }

    Ok(Stream::new(out))
}

fn collect_resource_ancestors(
    resource: &str,
    ancestor_map: &BTreeMap<String, Vec<String>>,
) -> Vec<String> {
    let mut out = vec![resource.to_string()];
    let mut idx = 0usize;
    while idx < out.len() {
        if let Some(ancestors) = ancestor_map.get(&out[idx]) {
            for ancestor in ancestors {
                if !out.iter().any(|existing| existing == ancestor) {
                    out.push(ancestor.clone());
                }
            }
        }
        idx += 1;
    }
    out
}

fn expect_json_string_field(value: &JsonValue, name: &str) -> Result<String, String> {
    match value {
        JsonValue::Object(map) => match map.get(name) {
            Some(JsonValue::String(value)) => Ok(value.clone()),
            _ => Err(format!("expected string field '{name}'")),
        },
        _ => Err("expected object".to_string()),
    }
}

fn apply_reversible(
    stream: Stream,
    direction: Direction,
    forward: fn(Value) -> Result<Value, String>,
    inverse: fn(Value) -> Result<Value, String>,
    forward_accepts: fn(&Value) -> bool,
    inverse_accepts: fn(&Value) -> bool,
) -> Result<Stream, String> {
    let mut out = Vec::new();
    for value in stream {
        let next = match direction {
            Direction::Inverse => inverse(value)?,
            Direction::Auto => {
                if forward_accepts(&value) {
                    forward(value)?
                } else if inverse_accepts(&value) {
                    inverse(value)?
                } else {
                    return Err("no matching direction for stage".to_string());
                }
            }
        };
        out.push(next);
    }
    Ok(Stream::new(out))
}

fn invert_stage(stage: Stage) -> Result<Stage, String> {
    Ok(match stage {
        Stage::Json(_) => Stage::Json(Direction::Inverse),
        Stage::Utf8(_) => Stage::Utf8(Direction::Inverse),
        Stage::Base64(_) => Stage::Base64(Direction::Inverse),
        Stage::Compose(stages) => Stage::Compose(
            stages
                .into_iter()
                .rev()
                .map(invert_stage)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        _ => return Err("stage is not reversible".to_string()),
    })
}

fn eval_value_expr(expr: &Expr, current: Option<&Value>) -> Result<Value, String> {
    let mut env = BTreeMap::new();
    if let Some(v) = current {
        env.insert("_".to_string(), v.clone());
    }
    eval_value_expr_with_env(expr, &env)
}

fn eval_value_expr_with_env(expr: &Expr, env: &BTreeMap<String, Value>) -> Result<Value, String> {
    match expr {
        Expr::Placeholder { .. } => env
            .get("_")
            .cloned()
            .ok_or_else(|| "placeholder _ is not bound".to_string()),
        Expr::Ident { name, .. } => env
            .get(name)
            .cloned()
            .ok_or_else(|| format!("unknown identifier {name}")),
        Expr::Number { value, .. } => Ok(Value::I64(*value)),
        Expr::String { value, .. } => Ok(Value::String(value.clone())),
        Expr::Array { items, .. } => {
            let mut out = Vec::new();
            for item in items {
                out.push(eval_value_expr_with_env(item, env)?);
            }
            Ok(Value::Array(out))
        }
        Expr::Record { fields, .. } => {
            let mut out = BTreeMap::new();
            for field in fields {
                out.insert(
                    field.name.clone(),
                    eval_value_expr_with_env(&field.value, env)?,
                );
            }
            Ok(Value::Record(out))
        }
        Expr::FieldAccess { expr, field, .. } => match eval_value_expr_with_env(expr, env)? {
            Value::Record(mut rec) => rec
                .remove(field)
                .ok_or_else(|| format!("field not found: {field}")),
            _ => Err("field access requires a record".to_string()),
        },
        Expr::Raw { text, .. } => eval_raw(text, env),
        Expr::Call { callee, args, .. } => {
            let name = callee_name(callee).ok_or_else(|| "unsupported callee".to_string())?;
            match name.as_str() {
                "array.map" => {
                    let arr = eval_value_expr_with_env(positional_arg(args, 0)?, env)?;
                    let func = positional_arg(args, 1)?;
                    let items = expect_array(arr)?;
                    let mut out = Vec::new();
                    for item in items {
                        out.push(eval_with_current(func, env, item)?);
                    }
                    Ok(Value::Array(out))
                }
                "array.filter" => {
                    let arr = eval_value_expr_with_env(positional_arg(args, 0)?, env)?;
                    let func = positional_arg(args, 1)?;
                    let items = expect_array(arr)?;
                    let mut out = Vec::new();
                    for item in items {
                        if truthy(&eval_with_current(func, env, item.clone())?)? {
                            out.push(item);
                        }
                    }
                    Ok(Value::Array(out))
                }
                "array.any" => {
                    let arr = eval_value_expr_with_env(positional_arg(args, 0)?, env)?;
                    let func = positional_arg(args, 1)?;
                    let items = expect_array(arr)?;
                    for item in items {
                        if truthy(&eval_with_current(func, env, item)?)? {
                            return Ok(Value::Bool(true));
                        }
                    }
                    Ok(Value::Bool(false))
                }
                "array.flat_map" => {
                    let arr = eval_value_expr_with_env(positional_arg(args, 0)?, env)?;
                    let func = positional_arg(args, 1)?;
                    let items = expect_array(arr)?;
                    let mut out = Vec::new();
                    for item in items {
                        let mapped = eval_with_current(func, env, item)?;
                        out.extend(expect_array(mapped)?);
                    }
                    Ok(Value::Array(out))
                }
                "array.contains" => {
                    let arr = eval_value_expr_with_env(positional_arg(args, 0)?, env)?;
                    let needle = eval_value_expr_with_env(positional_arg(args, 1)?, env)?;
                    let items = expect_array(arr)?;
                    Ok(Value::Bool(items.into_iter().any(|item| item == needle)))
                }
                "default" => {
                    let value = eval_value_expr_with_env(positional_arg(args, 0)?, env)?;
                    if matches!(value, Value::Null) {
                        eval_value_expr_with_env(positional_arg(args, 1)?, env)
                    } else {
                        Ok(value)
                    }
                }
                _ => Err(format!("unsupported expression call: {name}")),
            }
        }
        _ => Err("unsupported expression form".to_string()),
    }
}

fn eval_with_current(
    expr: &Expr,
    env: &BTreeMap<String, Value>,
    current: Value,
) -> Result<Value, String> {
    let mut scoped = env.clone();
    scoped.insert("_".to_string(), current);
    eval_value_expr_with_env(expr, &scoped)
}

fn expect_array(value: Value) -> Result<Vec<Value>, String> {
    match value {
        Value::Array(items) => Ok(items),
        _ => Err("expected array".to_string()),
    }
}

fn expect_record(value: Value, err: &str) -> Result<BTreeMap<String, Value>, String> {
    match value {
        Value::Record(record) => Ok(record),
        _ => Err(err.to_string()),
    }
}

fn expect_string_value(value: Value, err: &str) -> Result<String, String> {
    match value {
        Value::String(s) => Ok(s),
        _ => Err(err.to_string()),
    }
}

fn eval_raw(text: &str, env: &BTreeMap<String, Value>) -> Result<Value, String> {
    let raw = text.trim();
    if let Some((l, r)) = split_top_level(raw, '>') {
        let lhs = eval_raw(l, env)?;
        let rhs = eval_raw(r, env)?;
        let (x, y) = match (lhs, rhs) {
            (Value::I64(x), Value::I64(y)) => (x, y),
            _ => return Err("operator > expects i64 operands".to_string()),
        };
        return Ok(Value::Bool(x > y));
    }
    if let Some((l, r)) = split_top_level(raw, '+') {
        let lhs = eval_raw(l, env)?;
        let rhs = eval_raw(r, env)?;
        return match (lhs, rhs) {
            (Value::I64(x), Value::I64(y)) => Ok(Value::I64(x + y)),
            (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{a}{b}"))),
            _ => Err("operator + expects i64 or string operands".to_string()),
        };
    }

    if raw == "_" {
        return env
            .get("_")
            .cloned()
            .ok_or_else(|| "placeholder _ is not bound".to_string());
    }

    if let Ok(n) = raw.parse::<i64>() {
        return Ok(Value::I64(n));
    }

    if raw.starts_with('"') {
        return match serde_json::from_str(raw).map_err(|e| e.to_string())? {
            JsonValue::String(s) => Ok(Value::String(s)),
            _ => Err("invalid string literal".to_string()),
        };
    }

    if raw == "true" {
        return Ok(Value::Bool(true));
    }
    if raw == "false" {
        return Ok(Value::Bool(false));
    }
    if raw == "null" {
        return Ok(Value::Null);
    }

    if let Some((root, field)) = raw.rsplit_once('.') {
        let root_val = eval_raw(root, env)?;
        return match root_val {
            Value::Record(mut rec) => rec
                .remove(field)
                .ok_or_else(|| format!("field not found: {field}")),
            _ => Err("field access requires a record".to_string()),
        };
    }

    env.get(raw)
        .cloned()
        .ok_or_else(|| format!("unknown expression: {raw}"))
}

fn split_top_level(input: &str, needle: char) -> Option<(&str, &str)> {
    let mut depth_paren = 0usize;
    let mut depth_brack = 0usize;
    let mut depth_brace = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for (idx, c) in input.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_string = false;
            }
            continue;
        }

        match c {
            '"' => in_string = true,
            '(' => depth_paren += 1,
            ')' => depth_paren = depth_paren.saturating_sub(1),
            '[' => depth_brack += 1,
            ']' => depth_brack = depth_brack.saturating_sub(1),
            '{' => depth_brace += 1,
            '}' => depth_brace = depth_brace.saturating_sub(1),
            _ if c == needle && depth_paren == 0 && depth_brack == 0 && depth_brace == 0 => {
                let left = input[..idx].trim();
                let right = input[idx + c.len_utf8()..].trim();
                if !left.is_empty() && !right.is_empty() {
                    return Some((left, right));
                }
            }
            _ => {}
        }
    }
    None
}

fn truthy(value: &Value) -> Result<bool, String> {
    match value {
        Value::Bool(v) => Ok(*v),
        _ => Err("filter expression must evaluate to bool".to_string()),
    }
}

fn json_forward(value: Value) -> Result<Value, String> {
    let json = value_to_json(value);
    serde_json::to_vec(&json)
        .map(Value::Bytes)
        .map_err(|e| e.to_string())
}

fn json_inverse(value: Value) -> Result<Value, String> {
    match value {
        Value::Bytes(bytes) => serde_json::from_slice(&bytes)
            .map(json_to_value)
            .map_err(|e| e.to_string()),
        _ => Err("json inverse expects Bytes".to_string()),
    }
}

fn utf8_forward(value: Value) -> Result<Value, String> {
    match value {
        Value::String(s) => Ok(Value::Bytes(s.into_bytes())),
        _ => Err("utf8 forward expects String".to_string()),
    }
}

fn utf8_inverse(value: Value) -> Result<Value, String> {
    match value {
        Value::Bytes(bytes) => String::from_utf8(bytes)
            .map(Value::String)
            .map_err(|e| e.to_string()),
        _ => Err("utf8 inverse expects Bytes".to_string()),
    }
}

fn base64_forward(value: Value) -> Result<Value, String> {
    match value {
        Value::Bytes(bytes) => Ok(Value::String(base64_encode(&bytes))),
        _ => Err("base64 forward expects Bytes".to_string()),
    }
}

fn base64_inverse(value: Value) -> Result<Value, String> {
    match value {
        Value::String(s) => Ok(Value::Bytes(base64_decode(&s)?)),
        _ => Err("base64 inverse expects String".to_string()),
    }
}

fn accepts_json_forward(value: &Value) -> bool {
    !matches!(value, Value::Bytes(_) | Value::Unit)
}

fn accepts_json_inverse(value: &Value) -> bool {
    matches!(value, Value::Bytes(_))
}

fn accepts_utf8_forward(value: &Value) -> bool {
    matches!(value, Value::String(_))
}

fn accepts_utf8_inverse(value: &Value) -> bool {
    matches!(value, Value::Bytes(_))
}

fn accepts_base64_forward(value: &Value) -> bool {
    matches!(value, Value::Bytes(_))
}

fn accepts_base64_inverse(value: &Value) -> bool {
    matches!(value, Value::String(_))
}

fn parse_fixtures(fixtures: JsonValue) -> Result<BTreeMap<String, Vec<JsonValue>>, String> {
    match fixtures {
        JsonValue::Object(map) => {
            let mut out = BTreeMap::new();
            for (name, value) in map {
                match value {
                    JsonValue::Array(items) => {
                        out.insert(name, items);
                    }
                    _ => return Err("fixture values must be arrays".to_string()),
                }
            }
            Ok(out)
        }
        _ => Err("fixtures must be an object".to_string()),
    }
}

fn callee_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Ident { name, .. } => Some(name.clone()),
        Expr::FieldAccess { expr, field, .. } => {
            callee_name(expr).map(|base| format!("{base}.{field}"))
        }
        _ => None,
    }
}

fn positional_arg(args: &[CallArg], index: usize) -> Result<&Expr, String> {
    args.get(index)
        .ok_or_else(|| "missing positional arg".to_string())
        .and_then(|arg| match arg {
            CallArg::Positional(expr) => Ok(expr),
            CallArg::Named { .. } => Err("named arguments are not supported in v0".to_string()),
        })
}

fn named_arg<'a>(args: &'a [CallArg], name: &str) -> Result<&'a Expr, String> {
    args.iter()
        .find_map(|arg| match arg {
            CallArg::Named {
                name: arg_name,
                value,
                ..
            } if arg_name == name => Some(value),
            _ => None,
        })
        .ok_or_else(|| format!("missing named arg: {name}"))
}

fn expect_string(expr: &Expr) -> Result<String, String> {
    match expr {
        Expr::String { value, .. } => Ok(value.clone()),
        _ => Err("expected string literal".to_string()),
    }
}

fn expect_i64_literal(expr: &Expr) -> Result<i64, String> {
    match expr {
        Expr::Number { value, .. } => Ok(*value),
        _ => Err("expected i64 literal".to_string()),
    }
}

fn parse_sort_order(expr: &Expr) -> Result<SortOrder, String> {
    match expect_string(expr)?.as_str() {
        "asc" => Ok(SortOrder::Asc),
        "desc" => Ok(SortOrder::Desc),
        _ => Err("order must be \"asc\" or \"desc\"".to_string()),
    }
}

fn expect_sort_key(value: Value, err: &str) -> Result<SortKey, String> {
    match value {
        Value::I64(v) => Ok(SortKey::I64(v)),
        Value::String(v) => Ok(SortKey::String(v)),
        _ => Err(err.to_string()),
    }
}

fn expect_group_key(value: &Value, err: &str) -> Result<(), String> {
    match value {
        Value::I64(_) | Value::String(_) => Ok(()),
        _ => Err(err.to_string()),
    }
}

fn compare_keys(a: &SortKey, b: &SortKey, order: SortOrder) -> std::cmp::Ordering {
    let cmp = match (a, b) {
        (SortKey::I64(x), SortKey::I64(y)) => x.cmp(y),
        (SortKey::String(x), SortKey::String(y)) => x.cmp(y),
        (SortKey::I64(_), SortKey::String(_)) => std::cmp::Ordering::Less,
        (SortKey::String(_), SortKey::I64(_)) => std::cmp::Ordering::Greater,
    };

    match order {
        SortOrder::Asc => cmp,
        SortOrder::Desc => cmp.reverse(),
    }
}

fn expect_stage(binding: Binding) -> Result<Stage, String> {
    match binding {
        Binding::Stage(stage) => Ok(stage),
        _ => Err("expected stage".to_string()),
    }
}

fn expect_stream(binding: Binding) -> Result<Stream, String> {
    match binding {
        Binding::Stream(stream) => Ok(stream),
        _ => Err("expected stream".to_string()),
    }
}

fn value_to_json(value: Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Bool(v) => JsonValue::Bool(v),
        Value::I64(v) => JsonValue::Number(v.into()),
        Value::String(v) => JsonValue::String(v),
        Value::Bytes(v) => JsonValue::Array(
            v.into_iter()
                .map(|b| JsonValue::Number((b as i64).into()))
                .collect(),
        ),
        Value::Array(items) => JsonValue::Array(items.into_iter().map(value_to_json).collect()),
        Value::Record(record) => {
            let mut out = Map::new();
            for (k, v) in record {
                out.insert(k, value_to_json(v));
            }
            JsonValue::Object(out)
        }
        Value::Unit => JsonValue::Null,
    }
}

fn json_to_value(value: JsonValue) -> Value {
    match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(v) => Value::Bool(v),
        JsonValue::Number(v) => Value::I64(v.as_i64().unwrap_or_default()),
        JsonValue::String(v) => Value::String(v),
        JsonValue::Array(items) => Value::Array(items.into_iter().map(json_to_value).collect()),
        JsonValue::Object(map) => Value::Record(
            map.into_iter()
                .map(|(k, v)| (k, json_to_value(v)))
                .collect(),
        ),
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut o = String::new();
    let mut i = 0;
    while i < bytes.len() {
        let b0 = bytes[i] as u32;
        let b1 = if i + 1 < bytes.len() {
            bytes[i + 1] as u32
        } else {
            0
        };
        let b2 = if i + 2 < bytes.len() {
            bytes[i + 2] as u32
        } else {
            0
        };
        let n = (b0 << 16) | (b1 << 8) | b2;
        o.push(T[((n >> 18) & 63) as usize] as char);
        o.push(T[((n >> 12) & 63) as usize] as char);
        o.push(if i + 1 < bytes.len() {
            T[((n >> 6) & 63) as usize] as char
        } else {
            '='
        });
        o.push(if i + 2 < bytes.len() {
            T[(n & 63) as usize] as char
        } else {
            '='
        });
        i += 3;
    }
    o
}

fn base64_decode(s: &str) -> Result<Vec<u8>, String> {
    fn v(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }

    let bytes = s.as_bytes();
    if bytes.len() % 4 != 0 {
        return Err("invalid base64 length".to_string());
    }

    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c0 = v(bytes[i]).ok_or_else(|| "invalid base64".to_string())? as u32;
        let c1 = v(bytes[i + 1]).ok_or_else(|| "invalid base64".to_string())? as u32;
        let c2 = if bytes[i + 2] == b'=' {
            64
        } else {
            v(bytes[i + 2]).ok_or_else(|| "invalid base64".to_string())? as u32
        };
        let c3 = if bytes[i + 3] == b'=' {
            64
        } else {
            v(bytes[i + 3]).ok_or_else(|| "invalid base64".to_string())? as u32
        };

        let n = (c0 << 18) | (c1 << 12) | ((c2 & 63) << 6) | (c3 & 63);
        out.push(((n >> 16) & 255) as u8);
        if c2 != 64 {
            out.push(((n >> 8) & 255) as u8);
        }
        if c3 != 64 {
            out.push((n & 255) as u8);
        }
        i += 4;
    }
    Ok(out)
}
