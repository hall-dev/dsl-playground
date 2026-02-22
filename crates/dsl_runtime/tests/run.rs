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

#[test]
fn rbac_evaluate_outputs_decisions_and_matches() {
    let program = r#"
requests := input.json("requests") |> json;

requests
  |> rbac.evaluate(
    principal_bindings="principal_bindings",
    role_perms="role_perms",
    resource_ancestors="resource_ancestors"
  )
  |> ui.table("decisions");
"#;

    let fixtures = json!({
        "principal_bindings": [
            {"principal": "alice", "role": "reader"},
            {"principal": "bob", "role": "writer"},
            {"principal": "carol", "role": "admin"}
        ],
        "role_perms": [
            {"role": "reader", "action": "read", "resource": "folder:engineering"},
            {"role": "writer", "action": "write", "resource": "doc:eng-plan"},
            {"role": "admin", "action": "delete", "resource": "folder:root"}
        ],
        "resource_ancestors": [
            {"resource": "doc:eng-plan", "ancestor": "folder:engineering"},
            {"resource": "folder:engineering", "ancestor": "folder:root"}
        ],
        "requests": [
            {"principal": "alice", "action": "read", "resource": "doc:eng-plan"},
            {"principal": "alice", "action": "write", "resource": "doc:eng-plan"},
            {"principal": "bob", "action": "write", "resource": "doc:eng-plan"},
            {"principal": "carol", "action": "delete", "resource": "doc:eng-plan"},
            {"principal": "dave", "action": "read", "resource": "doc:eng-plan"}
        ]
    });

    let out = run(program, fixtures).expect("rbac example should run");
    assert_eq!(
        out.tables.get("decisions"),
        Some(&vec![
            json!({
                "request": {"principal": "alice", "action": "read", "resource": "doc:eng-plan"},
                "decision": "allow",
                "matches": [{"role": "reader", "action": "read", "resource": "folder:engineering"}]
            }),
            json!({
                "request": {"principal": "alice", "action": "write", "resource": "doc:eng-plan"},
                "decision": "deny",
                "matches": []
            }),
            json!({
                "request": {"principal": "bob", "action": "write", "resource": "doc:eng-plan"},
                "decision": "allow",
                "matches": [{"role": "writer", "action": "write", "resource": "doc:eng-plan"}]
            }),
            json!({
                "request": {"principal": "carol", "action": "delete", "resource": "doc:eng-plan"},
                "decision": "allow",
                "matches": [{"role": "admin", "action": "delete", "resource": "folder:root"}]
            }),
            json!({
                "request": {"principal": "dave", "action": "read", "resource": "doc:eng-plan"},
                "decision": "deny",
                "matches": []
            })
        ])
    );
}

#[test]
fn kv_load_and_lookup_supports_single_and_batch_lookup() {
    let program = r#"
input.json("users")
  |> json
  |> kv.load(store="users");

input.json("events")
  |> json
  |> lookup.kv(store="users", key=_.user_id)
  |> ui.table("single");

input.json("events")
  |> json
  |> lookup.batch_kv(store="users", key=_.user_id, batch_size=100, within_ms=10)
  |> ui.table("batch");
"#;

    let fixtures = json!({
        "users": [
            {"key": "u1", "value": {"name": "Ada"}},
            {"key": "u2", "value": {"name": "Lin"}}
        ],
        "events": [
            {"user_id": "u1", "action": "login"},
            {"user_id": "u9", "action": "logout"}
        ]
    });

    let out = run(program, fixtures).expect("program should run");
    let expected = vec![
        json!({
            "left": {"user_id": "u1", "action": "login"},
            "right": {"name": "Ada"}
        }),
        json!({
            "left": {"user_id": "u9", "action": "logout"},
            "right": null
        }),
    ];

    assert_eq!(out.tables.get("single"), Some(&expected));
    assert_eq!(out.tables.get("batch"), Some(&expected));
}

#[test]
fn array_helpers_and_default_builtin_work_in_map_stage() {
    let program = r#"
input.json("rows")
  |> json
  |> map({
    mapped: array.map(_.nums, _ + 1),
    filtered: array.filter(_.nums, _ > 1),
    any_big: array.any(_.nums, _ > 2),
    flattened: array.flat_map(_.nums, [_, _]),
    contains_two: array.contains(_.nums, 2),
    fallback_name: default(_.name, "n/a")
  })
  |> ui.table("out");
"#;

    let fixtures = json!({
        "rows": [
            {"nums": [1, 2], "name": null},
            {"nums": [3], "name": "ok"}
        ]
    });

    let out = run(program, fixtures).expect("program should run");
    assert_eq!(
        out.tables.get("out"),
        Some(&vec![
            json!({
                "mapped": [2, 3],
                "filtered": [2],
                "any_big": false,
                "flattened": [1, 1, 2, 2],
                "contains_two": true,
                "fallback_name": "n/a"
            }),
            json!({
                "mapped": [4],
                "filtered": [3],
                "any_big": true,
                "flattened": [3, 3],
                "contains_two": false,
                "fallback_name": "ok"
            })
        ])
    );
}

#[test]
fn group_collect_all_groups_entire_finite_stream() {
    let program = r#"
input.json("rows")
  |> json
  |> group.collect_all(by_key=_.team, within_ms=1000, limit=10)
  |> ui.table("out");
"#;

    let fixtures = json!({
        "rows": [
            {"team": "a", "id": 1},
            {"team": "b", "id": 2},
            {"team": "a", "id": 3}
        ]
    });

    let out = run(program, fixtures).expect("program should run");
    assert_eq!(
        out.tables.get("out"),
        Some(&vec![
            json!({
                "key": "a",
                "items": [
                    {"team": "a", "id": 1},
                    {"team": "a", "id": 3}
                ]
            }),
            json!({
                "key": "b",
                "items": [
                    {"team": "b", "id": 2}
                ]
            })
        ])
    );
}

#[test]
fn rank_topk_on_ints_desc_with_stable_ties() {
    let program = r#"
input.json("xs")
  |> json
  |> rank.topk(k=3, by=_, order="desc")
  |> ui.table("out");
"#;

    let out = run(program, json!({"xs": [3, 1, 4, 3, 2]})).expect("program should run");
    assert_eq!(
        out.tables.get("out"),
        Some(&vec![json!(4), json!(3), json!(3)])
    );
}

#[test]
fn rank_topk_on_records_by_field() {
    let program = r#"
input.json("rows")
  |> json
  |> rank.topk(k=2, by=_.score, order="asc")
  |> ui.table("out");
"#;

    let out = run(
        program,
        json!({"rows": [
            {"id": "a", "score": 8},
            {"id": "b", "score": 3},
            {"id": "c", "score": 5},
            {"id": "d", "score": 3}
        ]}),
    )
    .expect("program should run");

    assert_eq!(
        out.tables.get("out"),
        Some(&vec![
            json!({"id": "b", "score": 3}),
            json!({"id": "d", "score": 3})
        ])
    );
}

#[test]
fn group_count_counts_by_key_and_preserves_first_seen_group_order() {
    let program = r#"
input.json("rows")
  |> json
  |> group.count(by_key=_.tag)
  |> ui.table("out");
"#;

    let out = run(
        program,
        json!({"rows": [
            {"tag": "rust", "id": 1},
            {"tag": "sql", "id": 2},
            {"tag": "rust", "id": 3},
            {"tag": "sql", "id": 4},
            {"tag": "rust", "id": 5}
        ]}),
    )
    .expect("program should run");

    assert_eq!(
        out.tables.get("out"),
        Some(&vec![
            json!({"key": "rust", "count": 3}),
            json!({"key": "sql", "count": 2})
        ])
    );
}

#[test]
fn group_count_top_k_frequent() {
    let program = r#"
input.json("rows")
  |> json
  |> group.count(by_key=_.tag)
  |> rank.topk(k=2, by=_.count, order="desc")
  |> ui.table("top");
"#;

    let out = run(
        program,
        json!({"rows": [
            {"tag": "rust"},
            {"tag": "ui"},
            {"tag": "rust"},
            {"tag": "db"},
            {"tag": "ui"},
            {"tag": "rust"},
            {"tag": "ui"},
            {"tag": "api"}
        ]}),
    )
    .expect("program should run");

    assert_eq!(
        out.tables.get("top"),
        Some(&vec![
            json!({"key": "rust", "count": 3}),
            json!({"key": "ui", "count": 3})
        ])
    );
}

#[test]
fn group_count_requires_string_or_i64_keys() {
    let program = r#"
input.json("rows")
  |> json
  |> group.count(by_key=_.obj)
  |> ui.table("out");
"#;

    let err = run(
        program,
        json!({"rows": [
            {"obj": {"nested": true}}
        ]}),
    )
    .expect_err("program should fail");

    assert!(err.contains("group.count by_key must evaluate to I64 or String"));
}

#[test]
fn group_topn_items_per_key() {
    let program = r#"
input.json("stories")
  |> json
  |> group.topn_items(by_key=_.author_id, n=2, order_by=_.created_at, order="desc")
  |> ui.table("out");
"#;

    let out = run(
        program,
        json!({"stories": [
            {"author_id": "a1", "story_id": "s1", "created_at": "2026-02-21T10:00:00Z"},
            {"author_id": "a2", "story_id": "s2", "created_at": "2026-02-21T09:00:00Z"},
            {"author_id": "a1", "story_id": "s3", "created_at": "2026-02-21T12:00:00Z"},
            {"author_id": "a1", "story_id": "s4", "created_at": "2026-02-21T11:00:00Z"}
        ]}),
    )
    .expect("program should run");

    assert_eq!(
        out.tables.get("out"),
        Some(&vec![
            json!({
                "key": "a1",
                "items": [
                    {"author_id": "a1", "story_id": "s3", "created_at": "2026-02-21T12:00:00Z"},
                    {"author_id": "a1", "story_id": "s4", "created_at": "2026-02-21T11:00:00Z"}
                ]
            }),
            json!({
                "key": "a2",
                "items": [
                    {"author_id": "a2", "story_id": "s2", "created_at": "2026-02-21T09:00:00Z"}
                ]
            })
        ])
    );
}
