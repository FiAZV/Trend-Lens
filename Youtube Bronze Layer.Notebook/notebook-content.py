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

# Configuração Inicial
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
import requests
import json

# Inicializar sessão Spark
spark = SparkSession.builder.appName("YouTubeDataExtraction").getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_delta_table(raw_path, table_name):
    df = spark.read.json(raw_path)
    
    # Escrever com tratamento de schema
    df.write.format("delta") \
        .option("overwriteSchema", "true") \
        .mode("append") \
        .saveAsTable(f"Lakehouse.bronze.{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def main():
    create_delta_table("Files/raw/youtube/videos", "youtube_videos")
    create_delta_table("Files/raw/youtube/categories", "youtube_categories")
    create_delta_table("Files/raw/youtube/comments", "youtube_comments")
    create_delta_table("Files/raw/youtube/channels", "youtube_channels")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
