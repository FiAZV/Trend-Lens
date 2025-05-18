# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "09659cea-c84d-4906-be1d-8146403c9777",
# META       "default_lakehouse_name": "Lakehouse",
# META       "default_lakehouse_workspace_id": "41c1bbe3-1b20-4e9c-8afb-99229f989290",
# META       "known_lakehouses": [
# META         {
# META           "id": "09659cea-c84d-4906-be1d-8146403c9777"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df1 = spark.sql("SELECT * FROM Lakehouse.silver.youtube_categories LIMIT 10")
df2 = spark.sql("SELECT * FROM Lakehouse.silver.youtube_channels LIMIT 10")
df3 = spark.sql("SELECT * FROM Lakehouse.silver.youtube_channels_branding LIMIT 10")
df4 = spark.sql("SELECT * FROM Lakehouse.silver.youtube_channels_keywords LIMIT 10")
df5 = spark.sql("SELECT * FROM Lakehouse.silver.youtube_comments LIMIT 10")
df6 = spark.sql("SELECT * FROM Lakehouse.silver.youtube_languages LIMIT 10")
df7 = spark.sql("SELECT * FROM Lakehouse.silver.youtube_tags LIMIT 10")
df8 = spark.sql("SELECT * FROM Lakehouse.silver.youtube_thumbnails LIMIT 10")
df9 = spark.sql("SELECT * FROM Lakehouse.silver.youtube_videos LIMIT 10")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def print_all_schemas():
    dfs = {
        "youtube_categories": spark.sql("SELECT * FROM Lakehouse.silver.youtube_categories LIMIT 10"),
        "youtube_channels": spark.sql("SELECT * FROM Lakehouse.silver.youtube_channels LIMIT 10"),
        "youtube_channels_branding": spark.sql("SELECT * FROM Lakehouse.silver.youtube_channels_branding LIMIT 10"),
        "youtube_channels_keywords": spark.sql("SELECT * FROM Lakehouse.silver.youtube_channels_keywords LIMIT 10"),
        "youtube_comments": spark.sql("SELECT * FROM Lakehouse.silver.youtube_comments LIMIT 10"),
        "youtube_languages": spark.sql("SELECT * FROM Lakehouse.silver.youtube_languages LIMIT 10"),
        "youtube_tags": spark.sql("SELECT * FROM Lakehouse.silver.youtube_tags LIMIT 10"),
        "youtube_thumbnails": spark.sql("SELECT * FROM Lakehouse.silver.youtube_thumbnails LIMIT 10"),
        "youtube_videos": spark.sql("SELECT * FROM Lakehouse.silver.youtube_videos LIMIT 10")
    }

    for name, df in dfs.items():
        print(f"\nSchema de '{name}':")
        df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print_all_schemas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
