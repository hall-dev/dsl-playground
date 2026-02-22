reqs := input.json("requests") |> json;
reqs |> ui.table("requests");

// show that fixtures load and are well-formed (dump them)
input.json("bindings")  |> json |> ui.table("bindings");
input.json("role_perms") |> json |> ui.table("role_perms");
input.json("ancestors") |> json |> ui.table("ancestors");

// TODO(v1): lookup.kv + lookup.batch_kv + group.collect_all + perm.matches