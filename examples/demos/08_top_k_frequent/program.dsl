input.json("rows")
  |> json
  |> group.count(by_key=_.tag)
  |> rank.topk(k=3, by=_.count, order="desc")
  |> ui.table("top_freq");
