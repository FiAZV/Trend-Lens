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
from pyspark.sql.functions import col, when, trim, coalesce, lit, to_timestamp
from pyspark.sql.types import IntegerType

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

# Célula 1 - Criação das Tabelas Silver Vazias
from delta import *

# Schema para Usuários
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.reddit_usuarios (
    nome STRING,
    karma_total INT,
    karma_post INT,
    karma_comentario INT,
    data_criacao TIMESTAMP,
    premium BOOLEAN,
    data_coleta TIMESTAMP,
    data_processamento_silver TIMESTAMP
) USING DELTA
""")

# Schema para Posts
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.reddit_posts (
    id STRING,
    titulo STRING,
    autor STRING,
    upvotes INT,
    comentarios INT,
    link STRING,
    subreddit STRING,
    data_postagem TIMESTAMP,
    data_coleta TIMESTAMP,
    data_processamento_silver TIMESTAMP
) USING DELTA
""")

# Schema para Comunidades
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.reddit_comunidades (
    post_id STRING,
    subreddit STRING,
    data_coleta TIMESTAMP,
    data_processamento_silver TIMESTAMP
) USING DELTA
""")

# Verificar
print("Tabelas criadas:")
spark.sql("SHOW TABLES IN silver").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
