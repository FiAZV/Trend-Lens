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

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import *
from functools import reduce
from typing import List
import requests
import json
import os
import time
import re
from datetime import date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inicializar sessão Spark
spark = SparkSession.builder.appName("youtubeCategoriesIngestion").getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Configurações da API do YouTube
API_KEY = "AIzaSyC0O_tDb6CKobRAWv2VBKk_TsVNZ1ZnY_U"
BASE_URL = "https://www.googleapis.com/"

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
        StructField("channelId", StringType(), True)
    ]), True),
    StructField("extracted_at", TimestampType(), True),
    StructField("data_source", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_youtube_data(endpoint, params):
    
    """
    Faz requisições à API do YouTube com tratamento básico de erros
    Retorna dados em formato JSON
    """

    url = f"{BASE_URL}{endpoint}"
    params['key'] = API_KEY
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()

        return response.json()

    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição para {url}: {str(e)}")

        return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_video_categories():

    """
    Extrai as categorias de vídeos disponíveis
    """

    # Define endpoint and params of API response
    endpoint = 'youtube/v3/videoCategories'
    params = {
        'part': 'snippet',
        'regionCode': 'BR'
    }
    
    data = fetch_youtube_data(endpoint, params)

    if data and 'items' in data:
        df = spark.createDataFrame(data['items'], schema=VIDEO_CATEGORIES_SCHEMA)

        return df.withColumn("extracted_at", F.current_timestamp()) \
                .withColumn("data_source", F.lit(endpoint))

    return spark.createDataFrame([], schema=VIDEO_CATEGORIES_SCHEMA)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_delta_table(df, table_name, schema):

    # Escrever com tratamento de schema
    lakehouse_name = "Lakehouse"
    schema_name = "dbo"


    df.write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("append") \
        .saveAsTable(f"{lakehouse_name}.{schema_name}.{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def main():

    """
    Função principal de extração de dados do YouTube para Lakehouse.
    Extrai vídeos populares, categorias, comentários e canais.
    Salva os dados em arquivos JSON particionados por tipo.
    """

    table_categories = "bronze_youtube_categories"

    print("Extraindo categorias de vídeos...")
    categories_df = extract_video_categories()

    print("Criando tabela de categorias")
    create_delta_table(categories_df, table_categories, VIDEO_CATEGORIES_SCHEMA)

    print("Pipeline de extração finalizado com sucesso.")


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
