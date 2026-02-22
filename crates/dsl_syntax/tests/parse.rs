use dsl_syntax::parse_program;

fn parse_debug(src: &str) -> String {
    format!("{:#?}", parse_program(src).expect("program should parse"))
}

#[test]
fn parses_acceptance_program_a() {
    let src = r#"
xs := input.json("xs") |> json;
xs |> map(_ + 1) |> filter(_ > 2) |> ui.table("out");
"#;
    let got = parse_debug(src);
    assert!(got.contains("Binding"));
    assert!(got.contains("Pipeline"));
    assert!(got.contains("Raw"));
    assert!(got.contains("_ + 1"));
    assert!(got.contains("_ > 2"));
}

#[test]
fn parses_acceptance_program_b() {
    let src = r#"
chain := base64 >> ~base64;
input.json("bs") |> chain |> ui.table("t");
"#;
    let got = parse_debug(src);
    assert!(got.contains("Compose"));
    assert!(got.contains("Inverse"));
    assert!(got.contains("chain"));
}

#[test]
fn parses_acceptance_program_c() {
    let src = r#"
input.json("ss") |> json |> utf8 |> ~utf8 |> ui.table("rt");
"#;
    let got = parse_debug(src);
    assert!(got.contains("Pipeline"));
    assert!(got.contains("utf8"));
    assert!(got.contains("Inverse"));
}

#[test]
fn parses_literals_and_field_access() {
    let src = r#"
v := {a: [1, "x"], b: rec.field};
v;
"#;
    let got = parse_debug(src);
    assert!(got.contains("Record"));
    assert!(got.contains("Array"));
    assert!(got.contains("FieldAccess"));
}

#[test]
fn parses_group_collect_all_and_array_helpers() {
    let src = r#"
input.json("xs")
  |> json
  |> group.collect_all(by_key=_.kind, within_ms=100, limit=10)
  |> map({
    mapped: array.map(_.items, _.id),
    filtered: array.filter(_.items, _.ok),
    any_ok: array.any(_.items, _.ok),
    flat: array.flat_map(_.items, [_.id]),
    has_one: array.contains(array.map(_.items, _.id), 1)
  })
  |> ui.table("out");
"#;
    let got = parse_debug(src);
    assert!(got.contains("group"));
    assert!(got.contains("collect_all"));
    assert!(got.contains("Named"));
    assert!(got.contains("array"));
    assert!(got.contains("contains"));
}
