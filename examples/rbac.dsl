requests := input.json("requests") |> json;

requests
  |> rbac.evaluate(
    principal_bindings="principal_bindings",
    role_perms="role_perms",
    resource_ancestors="resource_ancestors"
  )
  |> ui.table("decisions");
