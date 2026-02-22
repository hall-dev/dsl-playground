input.json("batches")
  |> json
  |> rank.kmerge_arrays(by=_, order="asc", limit=10)
  |> ui.table("merged");
