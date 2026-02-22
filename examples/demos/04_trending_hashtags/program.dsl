events := input.json("events") |> json;

events
  |> map({ tag: _.tag, one: 1 })
  |> ui.table("tag_events");

// TODO(v1): group_by(tag) + count + topk