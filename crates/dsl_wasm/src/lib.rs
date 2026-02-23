//! Minimal stable API surface for wasm-facing bindings.

use serde_json::{Map, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsValue(String);

impl JsValue {
    pub fn from_json_string(value: String) -> Self {
        Self(value)
    }

    pub fn as_string(&self) -> Option<String> {
        Some(self.0.clone())
    }
}

fn json_string(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string())
}

fn object(entries: Vec<(&str, Value)>) -> Value {
    let mut map = Map::new();
    for (k, v) in entries {
        map.insert(k.to_string(), v);
    }
    Value::Object(map)
}

pub fn compile(program: String) -> JsValue {
    let (ok, diagnostics) = match dsl_runtime::compile(&program) {
        Ok(_) => (true, String::new()),
        Err(e) => (false, e),
    };

    JsValue::from_json_string(json_string(&object(vec![
        ("ok", Value::Bool(ok)),
        ("diagnostics", Value::String(diagnostics)),
    ])))
}

pub fn run(program: String, fixtures_json: String) -> JsValue {
    let fixtures = match serde_json::from_str(&fixtures_json) {
        Ok(value) => value,
        Err(e) => {
            return JsValue::from_json_string(json_string(&object(vec![
                ("tables_json", Value::String("{}".to_string())),
                ("logs_json", Value::String("{}".to_string())),
                (
                    "explain",
                    Value::String(format!("error: invalid fixtures_json: {e}")),
                ),
            ])));
        }
    };

    match dsl_runtime::run(&program, fixtures) {
        Ok(out) => {
            let mut table_obj: Map = Map::new();
            for (name, rows) in out.tables {
                table_obj.insert(name, Value::Array(rows));
            }
            let tables_json = json_string(&Value::Object(table_obj));

            let mut log_obj: Map = Map::new();
            for (name, rows) in out.logs {
                log_obj.insert(
                    name,
                    Value::Array(rows.into_iter().map(Value::String).collect()),
                );
            }
            let logs_json = json_string(&Value::Object(log_obj));

            JsValue::from_json_string(json_string(&object(vec![
                ("tables_json", Value::String(tables_json)),
                ("logs_json", Value::String(logs_json)),
                ("explain", Value::String(out.explain.join("\n"))),
            ])))
        }
        Err(e) => JsValue::from_json_string(json_string(&object(vec![
            ("tables_json", Value::String("{}".to_string())),
            ("logs_json", Value::String("{}".to_string())),
            ("explain", Value::String(format!("error: {e}"))),
        ]))),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    fn get_field<'a>(value: &'a Value, key: &str) -> &'a Value {
        match value {
            Value::Object(map) => map.get(key).expect("field should exist"),
            _ => panic!("expected object"),
        }
    }

    #[test]
    fn compile_returns_diagnostics_on_parse_error() {
        let out = super::compile("x :=".to_string());
        let text = out
            .as_string()
            .expect("compile should return string JsValue");
        let body: Value = serde_json::from_str(&text).expect("valid json object");

        assert_eq!(get_field(&body, "ok"), &Value::Bool(false));
        let diagnostics = match get_field(&body, "diagnostics") {
            Value::String(v) => v,
            _ => panic!("diagnostics should be string"),
        };
        assert!(!diagnostics.is_empty());
    }

    #[test]
    fn run_returns_output_json_strings() {
        let program = r#"
xs := input.json("xs") |> json;
xs |> map(_ + 1) |> ui.table("out");
"#;

        let out = super::run(program.to_string(), "{\"xs\": [1, 2]}".to_string());
        let text = out.as_string().expect("run should return string JsValue");
        let body: Value = serde_json::from_str(&text).expect("valid json object");

        let tables_text = match get_field(&body, "tables_json") {
            Value::String(v) => v,
            _ => panic!("tables_json should be string"),
        };
        let tables: Value =
            serde_json::from_str(tables_text).expect("tables_json should be valid json");
        assert_eq!(get_field(&tables, "out"), &serde_json::json!([2, 3]));
        match get_field(&body, "logs_json") {
            Value::String(_) => {}
            _ => panic!("logs_json should be string"),
        }
        match get_field(&body, "explain") {
            Value::String(_) => {}
            _ => panic!("explain should be string"),
        }
    }
}
