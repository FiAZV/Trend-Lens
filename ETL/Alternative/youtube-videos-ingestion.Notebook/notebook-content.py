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

VIDEOS_SCHEMA = StructType([
    StructField("kind", StringType(), True),
    StructField("etag", StringType(), True),
    StructField("id", StringType(), True),

    StructField("snippet", StructType([
        StructField("publishedAt", StringType(), True),
        StructField("channelId", StringType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),

        StructField("thumbnails", StructType([
            StructField("default", StructType([
                StructField("url", StringType(), True),
                StructField("width", IntegerType(), True),
                StructField("height", IntegerType(), True),
            ]), True),
            StructField("medium", StructType([
                StructField("url", StringType(), True),
                StructField("width", IntegerType(), True),
                StructField("height", IntegerType(), True),
            ]), True),
            StructField("high", StructType([
                StructField("url", StringType(), True),
                StructField("width", IntegerType(), True),
                StructField("height", IntegerType(), True),
            ]), True),
            StructField("standard", StructType([
                StructField("url", StringType(), True),
                StructField("width", IntegerType(), True),
                StructField("height", IntegerType(), True),
            ]), True),
            StructField("maxres", StructType([
                StructField("url", StringType(), True),
                StructField("width", IntegerType(), True),
                StructField("height", IntegerType(), True),
            ]), True)
        ]), True),

        StructField("channelTitle", StringType(), True),
        StructField("tags", ArrayType(StringType()), True),
        StructField("categoryId", StringType(), True),
        StructField("liveBroadcastContent", StringType(), True),
        StructField("localized", StructType([
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
        ]), True),
        StructField("defaultAudioLanguage", StringType(), True)
    ]), True),

    StructField("contentDetails", StructType([
        StructField("duration", StringType(), True),
        StructField("dimension", StringType(), True),
        StructField("definition", StringType(), True),
        StructField("caption", StringType(), True),
        StructField("licensedContent", BooleanType(), True),
        StructField("contentRating", MapType(StringType(), StringType()), True),
        StructField("projection", StringType(), True),
    ]), True),

    StructField("statistics", StructType([
        StructField("viewCount", StringType(), True),
        StructField("likeCount", StringType(), True),
        StructField("favoriteCount", StringType(), True),
        StructField("commentCount", StringType(), True)
    ]), True)
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

def extract_popular_videos():

    """
    Extrai os 50 vídeos mais populares do Brasil para cada categoria.
    """

    endpoint = 'youtube/v3/videos'
    base_params = {
        'part': 'snippet,contentDetails,statistics',
        'chart': 'mostPopular',
        'regionCode': "BR",
        'maxResults': 50
    }

    category_ids = [1, 2, 10, 15, 17, 20, 22, 23, 24, 25, 26, 27, 28] # add a adynamic function later

    all_dfs = []

    for category_id in category_ids:
        
        params = base_params.copy()
        params['videoCategoryId'] = category_id
        
        data = fetch_youtube_data(endpoint, params)

        if data and 'items' in data and len(data['items']) > 0:
            df = spark.createDataFrame(data['items'], schema=VIDEOS_SCHEMA)
            df = df.withColumn("extracted_at", F.current_timestamp()) \
                   .withColumn("data_source", F.lit(endpoint))
            all_dfs.append(df)

    if all_dfs:
        return reduce(lambda a, b: a.unionByName(b), all_dfs)

    return spark.createDataFrame([], schema=VIDEOS_SCHEMA)


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

    table_videos = "bronze_youtube_videos"

    print("Extraindo vídeos...")
    videos_df = extract_popular_videos()

    print("Criando tabela de vídeos")
    create_delta_table(videos_df, table_videos, VIDEOS_SCHEMA)

    print("Pipeline de extração de vídeos finalizado com sucesso.")


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
