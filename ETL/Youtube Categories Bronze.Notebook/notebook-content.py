# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ae2924b7-5e60-462d-b9ab-7ea0f21771e9",
# META       "default_lakehouse_name": "Lakehouse",
# META       "default_lakehouse_workspace_id": "208bbd6c-42ba-400e-8343-f0093a1d2d9d",
# META       "known_lakehouses": [
# META         {
# META           "id": "ae2924b7-5e60-462d-b9ab-7ea0f21771e9"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Configuração Inicial
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, current_timestamp, lit
from pyspark.sql.types import (StructType, StructField, StringType, BooleanType,LongType, TimestampType, IntegerType, MapType, ArrayType)
import requests
import json
from datetime import date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inicializar sessão Spark
spark = SparkSession.builder.appName("YoutubeCategoriesBronze").getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

VIDEO_CATEGORIES_SCHEMA = StructType([
    StructField("kind", StringType(), True),
    StructField("etag", StringType(), True),
    StructField("id", StringType(), True),
    StructField("snippet", StructType([
        StructField("title", StringType(), True),
        StructField("assignable", StringType(), True),
        StructField("channel_id", StringType(), True)
    ]), True),
    StructField("extracted_at", StringType(), True),
    StructField("data_source", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_bronze_table(landing_path, table_name, schema):
    """
    Reads raw JSON data, processes it, and saves it as a partitioned Delta table.

    Args:
        raw_path (str): The file path to the raw JSON data.
        table_name (str): The name of the table to be created in the Bronze layer.
    """

    print(f"Reading raw data from: {landing_path}")
    df = spark.read.schema(schema).option("multiLine", "true").json(landing_path)

    # Adiciona as colunas de partição.
    df = df.withColumn("year", year("extracted_at")) \
           .withColumn("month", month("extracted_at")) \
           .withColumn("day", dayofmonth("extracted_at"))

    print(f"Writing partitioned data to the Bronze layer as {table_name}...")
    df.write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("append") \
        .partitionBy("year", "month", "day") \
        .saveAsTable(f"Lakehouse.bronze.{table_name}")
    
    print(f"Delta table '{table_name}' created successfully in the Bronze layer.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def main():

    today_date = date.today().strftime("%Y-%m-%d")
    landing_path = f"Files/landing/youtube/categories/{today_date}"
    bronze_table = "youtube_categories"
    create_bronze_table(landing_path, bronze_table, VIDEO_CATEGORIES_SCHEMA)

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
