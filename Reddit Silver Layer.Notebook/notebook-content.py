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

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
    LongType, TimestampType, IntegerType, MapType, ArrayType
)
import re

spark = SparkSession.builder.appName("RedditSilverLayer").getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


lakehouse_name = "Lakehouse"
bronze_schema = f"{lakehouse_name}.bronze"
silver_schema = f"{lakehouse_name}.silver"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def save_table(df, schema: str, table_name: str) -> None:
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{schema}.{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def drop_delta_tables():
    spark.sql("DROP TABLE IF EXISTS Lakehouse.silver.reddit_usuarios")
    spark.sql("DROP TABLE IF EXISTS Lakehouse.silver.reddit_posts")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

def transform_usuarios(df): 
    # Transforma as colunas data_criacao e data_coleta para timestamp e trata valores nulos
    # Seleciona todas as colunas do DataFrame original
    transformed_df = df.select(
        F.col("nome"),
        F.col("karma_total"),
        F.col("karma_post"),
        F.col("karma_comentario"),
        F.col("data_criacao"),
        F.col("data_coleta"),
        F.col("premium")
    ).dropDuplicates(["nome"])
    
    return transformed_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

def transform_posts_com_comunidade(posts_df, comunidades_df):
    # Realiza um JOIN entre posts e comunidades para obter o subreddit correspondente a cada post
    combined_df = posts_df.join(
        comunidades_df.select("post_id", "subreddit"),
        posts_df.id == comunidades_df.post_id,
        "left"
    )
    
    # Seleciona as colunas relevantes e renomeia a coluna subreddit para comunidade
    transformed_df = combined_df.select(
        posts_df.id,
        posts_df.titulo,
        posts_df.autor,
        posts_df.upvotes,
        posts_df.comentarios,
        posts_df.link,
        comunidades_df.subreddit.alias("comunidade"),
        posts_df.data_postagem,
        posts_df.data_coleta
    )
    
    # Remove duplicatas com base na coluna id
    transformed_df = transformed_df.dropDuplicates(["id"])
    
    return transformed_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def main():
    # Bronze layer tables paths
    bronze_table_usuarios = f"{bronze_schema}.reddit_usuarios"
    bronze_table_posts = f"{bronze_schema}.reddit_posts"
    bronze_table_comunidades = f"{bronze_schema}.reddit_comunidades"

    # Reading bronze layer tables into dataframes
    bronze_usuarios_df = spark.table(bronze_table_usuarios)
    bronze_posts_df = spark.table(bronze_table_posts)
    bronze_comunidades_df = spark.table(bronze_table_comunidades)

    # Transformating bronze layer tables to silver layer tables and writing it on dataframes
    silver_usuarios_df = transform_usuarios(bronze_usuarios_df)
    silver_posts_df = transform_posts_com_comunidade(bronze_posts_df, bronze_comunidades_df)

    # Defining silver layer tables
    usuarios_table = "reddit_usuarios"
    posts_table = "reddit_posts"
    comunidades_table = "reddit_comunidades"

    # Saving tables into silver schema
    drop_delta_tables()
    save_table(silver_usuarios_df, silver_schema, usuarios_table)
    save_table(silver_posts_df, silver_schema, posts_table)

    print("✅ Camada Silver criada com sucesso!")

main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
