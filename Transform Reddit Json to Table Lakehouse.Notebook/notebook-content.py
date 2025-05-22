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
from pyspark.sql.types import (StructType, StructField, StringType, BooleanType,LongType, TimestampType, IntegerType, MapType, ArrayType)
import requests
import json

# Inicializar sessão Spark
spark = SparkSession.builder.appName("RedditDataExtraction").getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# SCHEMAS

# CELL ********************

USUARIOS_SCHEMA = StructType([
    StructField("nome", StringType(), True),
    StructField("karma_total", LongType(), True),
    StructField("karma_post", LongType(), True),
    StructField("karma_comentario", LongType(), True),
    StructField("data_criacao", StringType(), True),
    StructField("premium", BooleanType(), True),
    StructField("data_coleta", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

POSTS_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("titulo", StringType(), True),
    StructField("autor", StringType(), True),
    StructField("upvotes", LongType(), True),
    StructField("comentarios", LongType(), True),
    StructField("link", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("data_postagem", StringType(), True),
    StructField("data_coleta", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

COMUNIDADES_SCHEMA = StructType([
    StructField("post_id", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("data_coleta", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# FUNCTIONS

# CELL ********************

def create_delta_table(raw_path, table_name, schema):
    df = spark.read.schema(schema).json(raw_path)
    
    # Escrever com tratamento de schema
    df.write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .saveAsTable(f"Lakehouse.bronze.{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def drop_delta_tables():
    spark.sql("DROP TABLE IF EXISTS Lakehouse.bronze.reddit_usuarios")
    spark.sql("DROP TABLE IF EXISTS Lakehouse.bronze.reddit_posts")
    spark.sql("DROP TABLE IF EXISTS Lakehouse.bronze.reddit_comunidades")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def main():
    drop_delta_tables()

    raw_path = "Files/raw/reddit"

    raw_usuarios = f"{raw_path}/usuarios"
    table_usuarios = "reddit_usuarios"
    create_delta_table(raw_usuarios, table_usuarios, USUARIOS_SCHEMA)
    
    raw_posts = f"{raw_path}/posts"
    table_posts = "reddit_posts"
    create_delta_table(raw_posts, table_posts, POSTS_SCHEMA)

    raw_comunidades = f"{raw_path}/comunidades"
    table_comunidades = "reddit_comunidades"
    create_delta_table(raw_comunidades, table_comunidades, COMUNIDADES_SCHEMA)

main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
