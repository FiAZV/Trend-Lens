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

spark = SparkSession.builder.appName("RedditGoldLayer").getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lakehouse_name = "Lakehouse"
silver_schema = f"{lakehouse_name}.silver"
gold_schema = f"{lakehouse_name}.gold"

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
    spark.sql("DROP TABLE IF EXISTS Lakehouse.gold.reddit_usuarios")
    spark.sql("DROP TABLE IF EXISTS Lakehouse.gold.reddit_posts")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Configuração para lidar com o formato de data com 'T'
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

def extrair_horario(data_col):
    # Função para extrair horário, tratando ambos os casos (com espaço ou 'T')
    return F.when(
        F.length(F.split(data_col, ' ')[1]) >= 5,
        F.substring(F.split(data_col, ' ')[1], 1, 5)
    ).when(
        F.length(F.split(data_col, 'T')[1]) >= 5,
        F.substring(F.split(data_col, 'T')[1], 1, 5)
    ).otherwise(F.lit("00:00"))

def transform_usuarios(df): 
    # Dividir data_criacao em data e horario
    transformed_df = df.select(
        F.col("nome"),
        F.col("karma_total").alias("engajamento_total"),
        # Criando coluna 'data' no formato dd/mm/aaaa a partir de data_criacao
        F.date_format(
            F.to_date(F.regexp_replace(F.col("data_criacao"), 'T', ' '), "yyyy-MM-dd"), 
            "dd/MM/yyyy"
        ).alias("data"),
        # Criando coluna 'horario' no formato HH:MM a partir de data_criacao
        extrair_horario("data_criacao").alias("horario"),
        F.col("data_coleta"),  # Mantendo a coluna data_coleta original
        F.col("premium").alias("conta_premium")
    )
    
    return transformed_df

def transform_posts(df): 
    # Dividir data_postagem em data e horario
    transformed_df = df.select(
        F.col("id"),
        F.col("titulo").alias("conteudo_post"),
        F.col("autor"),
        F.col("upvotes"),
        F.col("comentarios"),
        F.col("link"),
        F.col("comunidade"),
        F.col("data_coleta"),
        # Criando coluna 'data' no formato dd/mm/aaaa a partir de data_postagem
        F.date_format(
            F.to_date(F.regexp_replace(F.col("data_postagem"), 'T', ' '), "yyyy-MM-dd"), 
            "dd/MM/yyyy"
        ).alias("data"),
        # Criando coluna 'horario' no formato HH:MM a partir de data_postagem
        extrair_horario("data_postagem").alias("horario")
    )
    
    return transformed_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def main():
    # Silver layer tables paths
    silver_table_usuarios = f"{silver_schema}.reddit_usuarios"
    silver_table_posts = f"{silver_schema}.reddit_posts"

    # Reading silver layer tables into dataframes
    silver_usuarios_df = spark.table(silver_table_usuarios)
    silver_posts_df = spark.table(silver_table_posts)

    # Transformating bronze layer tables to silver layer tables and writing it on dataframes
    gold_usuarios_df = transform_usuarios(silver_usuarios_df)
    gold_posts_df = transform_posts(silver_posts_df)

    # Defining silver layer tables
    usuarios_table = "reddit_usuarios"
    posts_table = "reddit_posts"

    # Saving tables into silver schema
    drop_delta_tables()
    save_table(gold_usuarios_df, gold_schema, usuarios_table)
    save_table(gold_posts_df, gold_schema, posts_table)

    print("✅ Camada Gold criada com sucesso!")

main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
