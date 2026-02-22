chain := base64 >> ~base64;
input.json("bs") |> chain |> ui.table("t");
