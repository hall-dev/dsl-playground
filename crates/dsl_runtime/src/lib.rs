use dsl_syntax::{parse_program, Expr, Program, Stmt};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct RunOutput {
    pub tables_json: String,
    pub logs_json: String,
    pub explain: String,
}

#[derive(Clone, Debug, PartialEq)]
enum J {
    Null,
    Bool(bool),
    Num(i64),
    Str(String),
    Arr(Vec<J>),
    Obj(BTreeMap<String, J>),
}

#[derive(Clone, Debug)]
enum RuntimeVal {
    Json(J),
    Bytes(Vec<u8>),
    Unit,
}
#[derive(Clone, Debug)]
enum BindingVal {
    Stream(Vec<RuntimeVal>),
    Stage(Stage),
}
#[derive(Clone, Debug)]
enum Stage {
    Map(String),
    Filter(String),
    FlatMap(String),
    Json { inv: bool },
    Utf8 { inv: bool },
    Base64 { inv: bool },
    UiTable(String),
    UiLog(String),
    Composed(Vec<Stage>),
}

pub fn compile(program: &str) -> Result<Program, String> {
    parse_program(program).map_err(|e| e.to_string())
}

pub fn run(program: &str, fixtures_json: &str) -> Result<RunOutput, String> {
    let program = compile(program)?;
    let fixtures = parse_fixtures(fixtures_json)?;
    let mut env: BTreeMap<String, BindingVal> = BTreeMap::new();
    let mut tables: BTreeMap<String, Vec<J>> = BTreeMap::new();
    let mut logs: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut explain = vec![];

    for stmt in &program.statements {
        match stmt {
            Stmt::Binding { name, expr } => {
                explain.push(format!("binding {name}"));
                let v = eval_expr(expr, &env, &fixtures, &mut tables, &mut logs, &mut explain)?;
                env.insert(name.clone(), v);
            }
            Stmt::Pipeline { expr } => {
                explain.push("pipeline".to_string());
                let _ = eval_expr(expr, &env, &fixtures, &mut tables, &mut logs, &mut explain)?;
            }
        }
    }

    Ok(RunOutput {
        tables_json: stringify_tables(&tables),
        logs_json: stringify_logs(&logs),
        explain: explain.join("\n"),
    })
}

fn eval_expr(
    expr: &Expr,
    env: &BTreeMap<String, BindingVal>,
    fixtures: &BTreeMap<String, Vec<J>>,
    tables: &mut BTreeMap<String, Vec<J>>,
    logs: &mut BTreeMap<String, Vec<String>>,
    explain: &mut Vec<String>,
) -> Result<BindingVal, String> {
    match expr {
        Expr::Pipeline { input, stages } => {
            let mut stream =
                expect_stream(eval_expr(input, env, fixtures, tables, logs, explain)?)?;
            for s in stages {
                stream = apply_stage(
                    &expect_stage(eval_expr(s, env, fixtures, tables, logs, explain)?)?,
                    stream,
                    tables,
                    logs,
                    explain,
                )?;
            }
            Ok(BindingVal::Stream(stream))
        }
        Expr::Call { name, args } if name == "input.json" => {
            let fixture_name = if let Expr::String(s) = &args[0] {
                s.clone()
            } else {
                return Err("input.json expects string".to_string());
            };
            explain.push(format!("  [source] input.json({fixture_name})"));
            let fixture = fixtures
                .get(&fixture_name)
                .ok_or_else(|| format!("missing fixture: {fixture_name}"))?;
            Ok(BindingVal::Stream(
                fixture
                    .iter()
                    .cloned()
                    .map(|j| RuntimeVal::Bytes(stringify_json(&j).into_bytes()))
                    .collect(),
            ))
        }
        Expr::Call { name, args } if name == "map" => {
            Ok(BindingVal::Stage(Stage::Map(render_arg(args)?)))
        }
        Expr::Call { name, args } if name == "filter" => {
            Ok(BindingVal::Stage(Stage::Filter(render_arg(args)?)))
        }
        Expr::Call { name, args } if name == "flat_map" => {
            Ok(BindingVal::Stage(Stage::FlatMap(render_arg(args)?)))
        }
        Expr::Call { name, args } if name == "ui.table" => {
            Ok(BindingVal::Stage(Stage::UiTable(expect_str(&args[0])?)))
        }
        Expr::Call { name, args } if name == "ui.log" => {
            Ok(BindingVal::Stage(Stage::UiLog(expect_str(&args[0])?)))
        }
        Expr::Ident(name) if name == "json" => Ok(BindingVal::Stage(Stage::Json { inv: false })),
        Expr::Ident(name) if name == "utf8" => Ok(BindingVal::Stage(Stage::Utf8 { inv: false })),
        Expr::Ident(name) if name == "base64" => {
            Ok(BindingVal::Stage(Stage::Base64 { inv: false }))
        }
        Expr::Ident(name) => env
            .get(name)
            .cloned()
            .ok_or_else(|| format!("unknown ident {name}")),
        Expr::Compose { left, right } => Ok(BindingVal::Stage(Stage::Composed(vec![
            expect_stage(eval_expr(left, env, fixtures, tables, logs, explain)?)?,
            expect_stage(eval_expr(right, env, fixtures, tables, logs, explain)?)?,
        ]))),
        Expr::Inverse(inner) => Ok(BindingVal::Stage(invert(expect_stage(eval_expr(
            inner, env, fixtures, tables, logs, explain,
        )?)?)?)),
        _ => Err("unsupported expression".to_string()),
    }
}

fn apply_stage(
    stage: &Stage,
    stream: Vec<RuntimeVal>,
    tables: &mut BTreeMap<String, Vec<J>>,
    logs: &mut BTreeMap<String, Vec<String>>,
    explain: &mut Vec<String>,
) -> Result<Vec<RuntimeVal>, String> {
    match stage {
        Stage::Map(expr) => {
            explain.push(format!("  [pure] map({expr})"));
            stream.into_iter().map(|v| eval_map(expr, v)).collect()
        }
        Stage::Filter(expr) => {
            explain.push(format!("  [pure] filter({expr})"));
            let mut o = vec![];
            for v in stream {
                if eval_pred(expr, &v)? {
                    o.push(v);
                }
            }
            Ok(o)
        }
        Stage::FlatMap(expr) => {
            explain.push(format!("  [pure] flat_map({expr})"));
            let mut out = vec![];
            for v in stream {
                if let RuntimeVal::Json(J::Arr(xs)) = eval_map(expr, v)? {
                    out.extend(xs.into_iter().map(RuntimeVal::Json));
                } else {
                    return Err("flat_map must return array".to_string());
                }
            }
            Ok(out)
        }
        Stage::Json { inv } => {
            explain.push("  [reversible] json".to_string());
            if *inv {
                stream
                    .into_iter()
                    .map(|v| match v {
                        RuntimeVal::Bytes(b) => Ok(RuntimeVal::Json(parse_json(
                            &String::from_utf8(b).map_err(|_| "invalid utf8".to_string())?,
                        )?)),
                        _ => Err("json inverse expects bytes".to_string()),
                    })
                    .collect()
            } else {
                stream
                    .into_iter()
                    .map(|v| match v {
                        RuntimeVal::Json(j) => {
                            Ok(RuntimeVal::Bytes(stringify_json(&j).into_bytes()))
                        }
                        _ => Err("json forward expects json".to_string()),
                    })
                    .collect()
            }
        }
        Stage::Utf8 { inv } => {
            explain.push("  [reversible] utf8".to_string());
            if *inv {
                stream
                    .into_iter()
                    .map(|v| match v {
                        RuntimeVal::Bytes(b) => Ok(RuntimeVal::Json(J::Str(
                            String::from_utf8(b).map_err(|_| "invalid utf8".to_string())?,
                        ))),
                        _ => Err("utf8 inverse expects bytes".to_string()),
                    })
                    .collect()
            } else {
                stream
                    .into_iter()
                    .map(|v| match v {
                        RuntimeVal::Json(J::Str(s)) => Ok(RuntimeVal::Bytes(s.into_bytes())),
                        _ => Err("utf8 forward expects string".to_string()),
                    })
                    .collect()
            }
        }
        Stage::Base64 { inv } => {
            explain.push("  [reversible] base64".to_string());
            if *inv {
                stream
                    .into_iter()
                    .map(|v| match v {
                        RuntimeVal::Json(J::Str(s)) => Ok(RuntimeVal::Bytes(base64_decode(&s)?)),
                        _ => Err("base64 inverse expects string".to_string()),
                    })
                    .collect()
            } else {
                stream
                    .into_iter()
                    .map(|v| match v {
                        RuntimeVal::Bytes(b) => Ok(RuntimeVal::Json(J::Str(base64_encode(&b)))),
                        _ => Err("base64 forward expects bytes".to_string()),
                    })
                    .collect()
            }
        }
        Stage::UiTable(name) => {
            explain.push(format!("  [sink] ui.table({name})"));
            let t = tables.entry(name.clone()).or_default();
            for v in stream {
                t.push(as_json(v)?);
            }
            Ok(vec![RuntimeVal::Unit])
        }
        Stage::UiLog(name) => {
            explain.push(format!("  [sink] ui.log({name})"));
            let l = logs.entry(name.clone()).or_default();
            for v in stream {
                l.push(stringify_json(&as_json(v)?));
            }
            Ok(vec![RuntimeVal::Unit])
        }
        Stage::Composed(stages) => {
            let mut cur = stream;
            for s in stages {
                cur = apply_stage(s, cur, tables, logs, explain)?;
            }
            Ok(cur)
        }
    }
}

fn invert(stage: Stage) -> Result<Stage, String> {
    Ok(match stage {
        Stage::Json { inv } => Stage::Json { inv: !inv },
        Stage::Utf8 { inv } => Stage::Utf8 { inv: !inv },
        Stage::Base64 { inv } => Stage::Base64 { inv: !inv },
        Stage::Composed(xs) => Stage::Composed(
            xs.into_iter()
                .rev()
                .map(invert)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        _ => return Err("stage is not reversible".to_string()),
    })
}
fn expect_stream(v: BindingVal) -> Result<Vec<RuntimeVal>, String> {
    if let BindingVal::Stream(s) = v {
        Ok(s)
    } else {
        Err("expected stream".to_string())
    }
}
fn expect_stage(v: BindingVal) -> Result<Stage, String> {
    if let BindingVal::Stage(s) = v {
        Ok(s)
    } else {
        Err("expected stage".to_string())
    }
}
fn expect_str(e: &Expr) -> Result<String, String> {
    if let Expr::String(s) = e {
        Ok(s.clone())
    } else {
        Err("expected string".to_string())
    }
}
fn render_arg(args: &[Expr]) -> Result<String, String> {
    if args.len() != 1 {
        return Err("expected one arg".to_string());
    };
    Ok(match &args[0] {
        Expr::Raw(s) | Expr::Ident(s) => s.clone(),
        Expr::Number(n) => n.to_string(),
        Expr::String(s) => format!("\"{s}\""),
        _ => "_".to_string(),
    })
}
fn as_json(v: RuntimeVal) -> Result<J, String> {
    match v {
        RuntimeVal::Json(j) => Ok(j),
        RuntimeVal::Bytes(b) => Ok(J::Arr(b.into_iter().map(|x| J::Num(x as i64)).collect())),
        RuntimeVal::Unit => Ok(J::Null),
    }
}
fn eval_map(expr: &str, v: RuntimeVal) -> Result<RuntimeVal, String> {
    let j = as_json(v)?;
    if expr == "_" {
        return Ok(RuntimeVal::Json(j));
    }
    if let Some(r) = expr.strip_prefix("_ +") {
        let n = r
            .trim()
            .parse::<i64>()
            .map_err(|_| "bad number".to_string())?;
        if let J::Num(x) = j {
            return Ok(RuntimeVal::Json(J::Num(x + n)));
        }
    }
    Err("unsupported map expression in v0".to_string())
}
fn eval_pred(expr: &str, v: &RuntimeVal) -> Result<bool, String> {
    let j = as_json(v.clone())?;
    if let Some(r) = expr.strip_prefix("_ >") {
        let n = r
            .trim()
            .parse::<i64>()
            .map_err(|_| "bad number".to_string())?;
        if let J::Num(x) = j {
            return Ok(x > n);
        }
    }
    Err("unsupported filter expression in v0".to_string())
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
    let b = s.as_bytes();
    if b.len() % 4 != 0 {
        return Err("invalid base64 length".to_string());
    }
    let mut o = vec![];
    let mut i = 0;
    while i < b.len() {
        let c0 = v(b[i]).ok_or_else(|| "invalid base64".to_string())? as u32;
        let c1 = v(b[i + 1]).ok_or_else(|| "invalid base64".to_string())? as u32;
        let c2 = if b[i + 2] == b'=' {
            64
        } else {
            v(b[i + 2]).ok_or_else(|| "invalid base64".to_string())? as u32
        };
        let c3 = if b[i + 3] == b'=' {
            64
        } else {
            v(b[i + 3]).ok_or_else(|| "invalid base64".to_string())? as u32
        };
        let n = (c0 << 18) | (c1 << 12) | ((c2 & 63) << 6) | (c3 & 63);
        o.push(((n >> 16) & 255) as u8);
        if c2 != 64 {
            o.push(((n >> 8) & 255) as u8);
        }
        if c3 != 64 {
            o.push((n & 255) as u8);
        }
        i += 4;
    }
    Ok(o)
}

fn parse_fixtures(input: &str) -> Result<BTreeMap<String, Vec<J>>, String> {
    let j = parse_json(input)?;
    let mut out = BTreeMap::new();
    if let J::Obj(m) = j {
        for (k, v) in m {
            if let J::Arr(xs) = v {
                out.insert(k, xs);
            } else {
                return Err("fixture values must be arrays".to_string());
            }
        }
        Ok(out)
    } else {
        Err("fixtures_json must be object".to_string())
    }
}
fn stringify_tables(t: &BTreeMap<String, Vec<J>>) -> String {
    let mut m = BTreeMap::new();
    for (k, v) in t {
        m.insert(k.clone(), J::Arr(v.clone()));
    }
    stringify_json(&J::Obj(m))
}
fn stringify_logs(t: &BTreeMap<String, Vec<String>>) -> String {
    let mut m = BTreeMap::new();
    for (k, v) in t {
        m.insert(
            k.clone(),
            J::Arr(v.iter().map(|s| J::Str(s.clone())).collect()),
        );
    }
    stringify_json(&J::Obj(m))
}

fn stringify_json(j: &J) -> String {
    match j {
        J::Null => "null".to_string(),
        J::Bool(b) => b.to_string(),
        J::Num(n) => n.to_string(),
        J::Str(s) => format!(
            "\"{}\"",
            s.replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n")
        ),
        J::Arr(a) => format!(
            "[{}]",
            a.iter().map(stringify_json).collect::<Vec<_>>().join(",")
        ),
        J::Obj(o) => format!(
            "{{{}}}",
            o.iter()
                .map(|(k, v)| format!("\"{}\":{}", k.replace('"', "\\\""), stringify_json(v)))
                .collect::<Vec<_>>()
                .join(",")
        ),
    }
}

fn parse_json(input: &str) -> Result<J, String> {
    let mut p = JsonP {
        b: input.as_bytes(),
        i: 0,
    };
    let v = p.value()?;
    p.ws();
    if p.i != p.b.len() {
        return Err("trailing json".to_string());
    }
    Ok(v)
}
struct JsonP<'a> {
    b: &'a [u8],
    i: usize,
}
impl<'a> JsonP<'a> {
    fn ws(&mut self) {
        while self.i < self.b.len() && self.b[self.i].is_ascii_whitespace() {
            self.i += 1;
        }
    }
    fn value(&mut self) -> Result<J, String> {
        self.ws();
        if self.i >= self.b.len() {
            return Err("eof".to_string());
        }
        match self.b[self.i] {
            b'n' => {
                self.expect(b"null")?;
                Ok(J::Null)
            }
            b't' => {
                self.expect(b"true")?;
                Ok(J::Bool(true))
            }
            b'f' => {
                self.expect(b"false")?;
                Ok(J::Bool(false))
            }
            b'"' => Ok(J::Str(self.string()?)),
            b'[' => self.array(),
            b'{' => self.object(),
            b'-' | b'0'..=b'9' => self.number(),
            _ => Err("bad json value".to_string()),
        }
    }
    fn expect(&mut self, s: &[u8]) -> Result<(), String> {
        if self.b.get(self.i..self.i + s.len()) == Some(s) {
            self.i += s.len();
            Ok(())
        } else {
            Err("bad token".to_string())
        }
    }
    fn string(&mut self) -> Result<String, String> {
        self.i += 1;
        let mut o = String::new();
        while self.i < self.b.len() {
            let c = self.b[self.i];
            self.i += 1;
            if c == b'"' {
                return Ok(o);
            }
            if c == b'\\' {
                if self.i >= self.b.len() {
                    return Err("bad escape".to_string());
                }
                let e = self.b[self.i];
                self.i += 1;
                o.push(match e {
                    b'"' => '"',
                    b'\\' => '\\',
                    b'n' => '\n',
                    b't' => '\t',
                    _ => return Err("bad escape".to_string()),
                });
            } else {
                o.push(c as char)
            }
        }
        Err("unterminated string".to_string())
    }
    fn number(&mut self) -> Result<J, String> {
        let s = self.i;
        if self.b[self.i] == b'-' {
            self.i += 1;
        }
        while self.i < self.b.len() && self.b[self.i].is_ascii_digit() {
            self.i += 1;
        }
        let n = std::str::from_utf8(&self.b[s..self.i])
            .map_err(|_| "utf8".to_string())?
            .parse::<i64>()
            .map_err(|_| "num".to_string())?;
        Ok(J::Num(n))
    }
    fn array(&mut self) -> Result<J, String> {
        self.i += 1;
        let mut o = vec![];
        loop {
            self.ws();
            if self.i < self.b.len() && self.b[self.i] == b']' {
                self.i += 1;
                return Ok(J::Arr(o));
            }
            o.push(self.value()?);
            self.ws();
            if self.i < self.b.len() && self.b[self.i] == b',' {
                self.i += 1;
                continue;
            }
            if self.i < self.b.len() && self.b[self.i] == b']' {
                self.i += 1;
                return Ok(J::Arr(o));
            }
            return Err("bad array".to_string());
        }
    }
    fn object(&mut self) -> Result<J, String> {
        self.i += 1;
        let mut o = BTreeMap::new();
        loop {
            self.ws();
            if self.i < self.b.len() && self.b[self.i] == b'}' {
                self.i += 1;
                return Ok(J::Obj(o));
            }
            let k = self.string()?;
            self.ws();
            if self.i >= self.b.len() || self.b[self.i] != b':' {
                return Err("bad object".to_string());
            }
            self.i += 1;
            let v = self.value()?;
            o.insert(k, v);
            self.ws();
            if self.i < self.b.len() && self.b[self.i] == b',' {
                self.i += 1;
                continue;
            }
            if self.i < self.b.len() && self.b[self.i] == b'}' {
                self.i += 1;
                return Ok(J::Obj(o));
            }
            return Err("bad object".to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn map_filter_program() {
        let p = "xs := input.json(\"xs\") |> ~json; xs |> map(_ + 1) |> filter(_ > 2) |> ui.table(\"out\");";
        let out = run(p, "{\"xs\":[1,2,3]}").unwrap();
        assert_eq!(out.tables_json, "{\"out\":[3,4]}");
    }
    #[test]
    fn utf8_roundtrip() {
        let p = "input.json(\"ss\") |> ~json |> utf8 |> ~utf8 |> ui.table(\"rt\");";
        let out = run(p, "{\"ss\":[\"hi\"]}").unwrap();
        assert_eq!(out.tables_json, "{\"rt\":[\"hi\"]}");
    }
}
