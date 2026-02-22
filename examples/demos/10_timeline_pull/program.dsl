input.json("timeline_batches")
  |> json
  |> rank.kmerge_arrays(by=_.created_at, order="desc", limit=5)
  |> ui.table("timeline");
