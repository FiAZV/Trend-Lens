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
from pyspark.sql.types import (StructType, StructField, StringType, BooleanType,
                              LongType, TimestampType, IntegerType, MapType, ArrayType)
import json
import os
import glob

# Inicializar sessão Spark
spark = SparkSession.builder.appName("RedditDataExtraction").getOrCreate()

# Schema para a tabela usuarios_reddit
USUARIOS_SCHEMA = StructType([
    StructField("nome", StringType(), True),
    StructField("karma_total", LongType(), True),
    StructField("karma_post", LongType(), True),
    StructField("karma_comentario", LongType(), True),
    StructField("data_criacao", StringType(), True),
    StructField("premium", BooleanType(), True),
    StructField("data_coleta", StringType(), True)
])

# Schema para a tabela posts_reddit
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

# Schema para a tabela comunidades_reddit
COMUNIDADES_SCHEMA = StructType([
    StructField("post_id", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("data_coleta", StringType(), True)
])

# Função para criar tabela Delta (exatamente como no código do YouTube)
def create_delta_table(raw_path, table_name, schema):
    df = spark.read.schema(schema).json(raw_path)
    
    # Escrever com tratamento de schema
    df.write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .saveAsTable(f"Lakehouse.bronze.{table_name}")

# Função principal
def main():
    # Definir caminhos para os arquivos JSON
    raw_path       = "/lakehouse/default/Files/raw/reddit"
    raw_usuarios   = f"{raw_path}/usuarios"
    raw_posts      = f"{raw_path}/posts"
    raw_comunidades = f"{raw_path}/comunidades"

    # Definir os nomes das tabelas (com os novos nomes solicitados)
    table_usuarios = "usuarios_reddit"
    table_posts = "posts_reddit"
    table_comunidades = "comunidades_reddit"

    # Criar as tabelas Delta
    print("Criando tabela usuarios_reddit...")
    create_delta_table(raw_usuarios, table_usuarios, USUARIOS_SCHEMA)
    
    print("Criando tabela posts_reddit...")
    create_delta_table(raw_posts, table_posts, POSTS_SCHEMA)
    
    print("Criando tabela comunidades_reddit...")
    create_delta_table(raw_comunidades, table_comunidades, COMUNIDADES_SCHEMA)
    
    print("Todas as tabelas foram criadas com sucesso!")
    
    # Verificar as tabelas criadas
    print("\nVerificando tabela usuarios_reddit:")
    spark.sql("SELECT * FROM Lakehouse.bronze.usuarios_reddit LIMIT 5").show(truncate=False)
    
    print("\nVerificando tabela posts_reddit:")
    spark.sql("SELECT * FROM Lakehouse.bronze.posts_reddit LIMIT 5").show(truncate=False)
    
    print("\nVerificando tabela comunidades_reddit:")
    spark.sql("SELECT * FROM Lakehouse.bronze.comunidades_reddit LIMIT 5").show(truncate=False)

# Executar o processo principal
main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
