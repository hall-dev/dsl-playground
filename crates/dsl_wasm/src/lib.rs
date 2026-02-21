//! Minimal stable API surface for wasm bindings.
//! In a wasm32 build, this crate is intended to be wrapped by wasm-bindgen tooling.

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
    match dsl_runtime::run(program, fixtures_json) {
        Ok(out) => format!(
            "{{\"tables_json\":\"{}\",\"logs_json\":\"{}\",\"explain\":\"{}\"}}",
            esc(&out.tables_json),
            esc(&out.logs_json),
            esc(&out.explain)
        ),
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
                || super::compile("x := input.json(\"x\") |> ~json;").contains("\"ok\":true")
        );
    }
}
