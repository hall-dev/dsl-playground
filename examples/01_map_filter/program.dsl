xs := input.json("xs") |> json;
xs |> map(_ + 1) |> filter(_ > 2) |> ui.table("out");
