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

#[test]
fn group_collect_all_with_array_helpers() {
    let program = r#"
input.json("rows")
  |> json
  |> group.collect_all(by_key=_.team, within_ms=250, limit=10)
  |> map({
    key: _.key,
    ids: array.map(_.items, _.id),
    adults: array.filter(_.items, _.age > 17),
    has_adult: array.any(_.items, _.age > 17),
    flat: array.flat_map(_.items, [_.id, _.age]),
    has_two: array.contains(array.map(_.items, _.id), 2)
  })
  |> ui.table("out");
"#;

    let fixtures = json!({
        "rows": [
            {"team": "a", "id": 1, "age": 17},
            {"team": "b", "id": 2, "age": 20},
            {"team": "a", "id": 3, "age": 21}
        ]
    });

    let out = run(program, fixtures).expect("program should run");
    assert_eq!(
        out.tables.get("out"),
        Some(&vec![
            json!({
                "key": "a",
                "ids": [1, 3],
                "adults": [{"team": "a", "id": 3, "age": 21}],
                "has_adult": true,
                "flat": [1, 17, 3, 21],
                "has_two": false
            }),
            json!({
                "key": "b",
                "ids": [2],
                "adults": [{"team": "b", "id": 2, "age": 20}],
                "has_adult": true,
                "flat": [2, 20],
                "has_two": true
            })
        ])
    );
}

#[test]
fn group_collect_all_applies_limit_per_group() {
    let program = r#"
input.json("rows")
  |> json
  |> group.collect_all(by_key=_.k, within_ms=1, limit=2)
  |> ui.table("out");
"#;

    let out = run(
        program,
        json!({"rows": [
            {"k": "x", "v": 1},
            {"k": "x", "v": 2},
            {"k": "x", "v": 3}
        ]}),
    )
    .expect("program should run");

    assert_eq!(
        out.tables.get("out"),
        Some(&vec![json!({
            "key": "x",
            "items": [
                {"k": "x", "v": 1},
                {"k": "x", "v": 2}
            ]
        })])
    );
}
