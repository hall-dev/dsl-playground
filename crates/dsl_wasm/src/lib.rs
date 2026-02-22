//! Minimal stable API surface for wasm bindings.
//! In a wasm32 build, this crate is intended to be wrapped by wasm-bindgen tooling.

use serde_json::{Map, Value};

fn esc(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

pub fn compile(program: &str) -> String {
    match dsl_runtime::compile(program) {
        Ok(_) => "{\"ok\":true,\"diagnostics\":\"\"}".to_string(),
        Err(e) => format!("{{\"ok\":false,\"diagnostics\":\"{}\"}}", esc(&e)),
    }
}

pub fn run(program: &str, fixtures_json: &str) -> String {
    let fixtures = match serde_json::from_str(fixtures_json) {
        Ok(value) => value,
        Err(e) => {
            return format!(
                "{{\"tables_json\":\"{{}}\",\"logs_json\":\"{{}}\",\"explain\":\"error: {}\"}}",
                esc(&e.to_string())
            )
        }
    };

    match dsl_runtime::run(program, fixtures) {
        Ok(out) => {
            let mut table_obj: Map = Map::new();
            for (name, rows) in out.tables {
                table_obj.insert(name, Value::Array(rows));
            }
            let tables_json =
                serde_json::to_string(&Value::Object(table_obj)).unwrap_or_else(|_| "{}".to_string());

            let mut log_obj: Map = Map::new();
            for (name, rows) in out.logs {
                log_obj.insert(
                    name,
                    Value::Array(rows.into_iter().map(Value::String).collect()),
                );
            }
            let logs_json =
                serde_json::to_string(&Value::Object(log_obj)).unwrap_or_else(|_| "{}".to_string());

            let explain = out.explain.join("\n");
            format!(
                "{{\"tables_json\":\"{}\",\"logs_json\":\"{}\",\"explain\":\"{}\"}}",
                esc(&tables_json),
                esc(&logs_json),
                esc(&explain)
            )
        }
        Err(e) => format!(
            "{{\"tables_json\":\"{{}}\",\"logs_json\":\"{{}}\",\"explain\":\"error: {}\"}}",
            esc(&e)
        ),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn compile_shape() {
        assert!(
            super::compile("x := 1;").contains("\"ok\":false")
                || super::compile("x := input.json(\"x\") |> json;").contains("\"ok\":true")
        );
    }
}
