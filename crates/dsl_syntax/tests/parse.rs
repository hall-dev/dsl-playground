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
