// Load stores
input.json("ancestors")  |> json |> flat_map(_) |> kv.load(store="resource_ancestors");
input.json("role_perms") |> json |> flat_map(_) |> kv.load(store="role_perms");
input.json("bindings")   |> json |> flat_map(_) |> kv.load(store="principal_bindings");

// Requests
reqs := input.json("requests") |> json;

// Attach bindings
reqs
  |> lookup.kv(store="principal_bindings", key=_.principal_id)
  |> map({
       req: _.left,
       bindings: default(_.right, [])
     })

  // Attach ancestors
  |> lookup.kv(store="resource_ancestors", key=_.req.resource_id)
  |> map({
       req: _.left.req,
       bindings: _.left.bindings,
       ancestors: default(_.right, [_.left.req.resource_id])
     })

  // Expand bindings to matches
  |> map({
       req: _.req,
       matches:
         array.flat_map(_.bindings, b =>
           if array.contains(_.ancestors, b.scope_id)
           then
             perms := default(
               lookup("role_perms", b.role_id),    // implement helper if needed
               []
             );
             array.flat_map(perms, p =>
               if perm.matches(p, _.req.action)
               then [{ effect:b.effect, scope_id:b.scope_id, role_id:b.role_id, perm:p }]
               else []
             )
           else []
         )
     })

  |> map({
       req_id: _.req.req_id,
       principal_id: _.req.principal_id,
       action: _.req.action,
       resource_id: _.req.resource_id,
       allow:
         let deny := array.any(_.matches, m => m.effect == "deny");
         let allow := array.any(_.matches, m => m.effect == "allow");
         if deny then false else allow,
       matches: _.matches
     })

  |> ui.table("decisions");