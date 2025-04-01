[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"bronze youtube_videos" = let
  Source = Lakehouse.Contents([]),
  #"Navigation 1" = Source{[workspaceId = "41c1bbe3-1b20-4e9c-8afb-99229f989290"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "09659cea-c84d-4906-be1d-8146403c9777"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "bronze.youtube_videos", ItemKind = "Table"]}[Data],
  #"Choose columns" = Table.SelectColumns(#"Navigation 3", {"items.snippet.channelId"}),
  #"Removed duplicates" = Table.Distinct(#"Choose columns", {"items.snippet.channelId"})
in
  #"Removed duplicates";
