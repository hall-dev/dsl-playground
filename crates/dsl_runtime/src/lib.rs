use dsl_syntax::{parse_program, CallArg, Expr, Program, Stmt};
use serde_json::{Map, Value as JsonValue};
use std::collections::BTreeMap;

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

pub fn compile(program: &str) -> Result<Program, String> {
    parse_program(program).map_err(|e| e.to_string())
}

pub fn run(program: &str, fixtures: JsonValue) -> Result<Outputs, String> {
    let program = compile(program)?;
    let fixture_map = parse_fixtures(fixtures)?;
    let mut env: BTreeMap<String, Binding> = BTreeMap::new();
    let mut outputs = Outputs::default();

    for stmt in &program.statements {
        match stmt {
            Stmt::Binding { name, expr, .. } => {
                outputs.explain.push(format!("binding {name}"));
                let val = eval_expr(expr, &env, &fixture_map, &mut outputs)?;
                env.insert(name.clone(), val);
            }
            Stmt::Pipeline { expr, .. } => {
                outputs.explain.push("pipeline".to_string());
                let _ = expect_stream(eval_expr(expr, &env, &fixture_map, &mut outputs)?)?;
            }
        }
    }

    Ok(outputs)
}

fn eval_expr(
    expr: &Expr,
    env: &BTreeMap<String, Binding>,
    fixtures: &BTreeMap<String, Vec<JsonValue>>,
    outputs: &mut Outputs,
) -> Result<Binding, String> {
    match expr {
        Expr::Pipeline { input, stages, .. } => {
            let mut stream = expect_stream(eval_expr(input, env, fixtures, outputs)?)?;
            for stage_expr in stages {
                let stage = expect_stage(eval_expr(stage_expr, env, fixtures, outputs)?)?;
                stream = apply_stage(&stage, stream, outputs)?;
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
                "filter" => Ok(Binding::Stage(Stage::Filter(positional_arg(args, 0)?.clone()))),
                "flat_map" => Ok(Binding::Stage(Stage::FlatMap(positional_arg(args, 0)?.clone()))),
                "ui.table" => Ok(Binding::Stage(Stage::UiTable(expect_string(positional_arg(
                    args, 0,
                )?)?))),
                "ui.log" => Ok(Binding::Stage(Stage::UiLog(expect_string(positional_arg(
                    args, 0,
                )?)?))),
                _ => Err(format!("unsupported call: {name}")),
            }
        }
        Expr::Ident { name, .. } if name == "json" => Ok(Binding::Stage(Stage::Json(Direction::Auto))),
        Expr::Ident { name, .. } if name == "utf8" => Ok(Binding::Stage(Stage::Utf8(Direction::Auto))),
        Expr::Ident { name, .. } if name == "base64" => {
            Ok(Binding::Stage(Stage::Base64(Direction::Auto)))
        }
        Expr::Ident { name, .. } => env
            .get(name)
            .cloned()
            .ok_or_else(|| format!("unknown ident {name}")),
        Expr::Compose { left, right, .. } => Ok(Binding::Stage(Stage::Compose(vec![
            expect_stage(eval_expr(left, env, fixtures, outputs)?)?,
            expect_stage(eval_expr(right, env, fixtures, outputs)?)?,
        ]))),
        Expr::Inverse { expr, .. } => Ok(Binding::Stage(invert_stage(expect_stage(eval_expr(
            expr, env, fixtures, outputs,
        )?)?)?)),
        _ => Err("unsupported expression for stream/stage evaluation".to_string()),
    }
}

fn apply_stage(stage: &Stage, stream: Stream, outputs: &mut Outputs) -> Result<Stream, String> {
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
        Stage::Json(direction) => {
            outputs.explain.push("  [reversible] json".to_string());
            apply_reversible(stream, *direction, json_forward, json_inverse, accepts_json_forward, accepts_json_inverse)
        }
        Stage::Utf8(direction) => {
            outputs.explain.push("  [reversible] utf8".to_string());
            apply_reversible(stream, *direction, utf8_forward, utf8_inverse, accepts_utf8_forward, accepts_utf8_inverse)
        }
        Stage::Base64(direction) => {
            outputs.explain.push("  [reversible] base64".to_string());
            apply_reversible(stream, *direction, base64_forward, base64_inverse, accepts_base64_forward, accepts_base64_inverse)
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
                current = apply_stage(part, current, outputs)?;
            }
            Ok(current)
        }
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
                out.insert(field.name.clone(), eval_value_expr_with_env(&field.value, env)?);
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
        Expr::Call { .. } => Err("function calls are not supported inside expressions".to_string()),
        _ => Err("unsupported expression form".to_string()),
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
        Expr::FieldAccess { expr, field, .. } => callee_name(expr).map(|base| format!("{base}.{field}")),
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

fn expect_string(expr: &Expr) -> Result<String, String> {
    match expr {
        Expr::String { value, .. } => Ok(value.clone()),
        _ => Err("expected string literal".to_string()),
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
