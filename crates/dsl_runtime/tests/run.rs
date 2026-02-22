use dsl_runtime::run;
use serde_json::json;

#[test]
fn acceptance_program_a_map_filter() {
    let program = r#"
xs := input.json("xs") |> json;
xs |> map(_ + 1) |> filter(_ > 2) |> ui.table("out");
"#;

    let out = run(program, json!({"xs": [1, 2, 3]})).expect("program A should run");
    assert_eq!(out.tables.get("out"), Some(&vec![json!(3), json!(4)]));
}

#[test]
fn acceptance_program_b_roundtrip_base64() {
    let program = r#"
chain := base64 >> ~base64;
input.json("bs") |> chain |> ui.table("t");
"#;

    let out = run(program, json!({"bs": ["aGk=", "eA=="]})).expect("program B should run");
    assert_eq!(
        out.tables.get("t"),
        Some(&vec![
            json!([34, 97, 71, 107, 61, 34]),
            json!([34, 101, 65, 61, 61, 34]),
        ])
    );
}

#[test]
fn acceptance_program_c_utf8_roundtrip() {
    let program = r#"
input.json("ss") |> json |> utf8 |> ~utf8 |> ui.table("rt");
"#;

    let out = run(program, json!({"ss": ["hi", "ok"]})).expect("program C should run");
    assert_eq!(out.tables.get("rt"), Some(&vec![json!("hi"), json!("ok")]));
}

#[test]
fn ui_table_accumulates_rows_across_pipelines() {
    let program = r#"
input.json("a") |> json |> ui.table("out");
input.json("b") |> json |> ui.table("out");
"#;

    let out = run(program, json!({"a": [{"x": 1}], "b": [2, 3]})).expect("program should run");
    assert_eq!(
        out.tables.get("out"),
        Some(&vec![json!({"x": 1}), json!(2), json!(3)])
    );
}
