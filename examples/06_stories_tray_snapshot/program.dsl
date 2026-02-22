inbox := input.json("inbox") |> json;

// Keep only "unexpired" by comparing ISO strings lexicographically for demo.
// TODO(v1): add time.now() and timestamp comparison
inbox
  |> filter(_.expires_at > "2026-02-21T12:00:00Z")
  |> ui.table("active_inbox");

// Shape to "tray items"
inbox
  |> map({ author_id: _.author_id, story_id: _.story_id, created_at: _.created_at })
  |> ui.table("tray_items");

// TODO(v1): lookup views + group.topn_items(by author_id) + rank.topk