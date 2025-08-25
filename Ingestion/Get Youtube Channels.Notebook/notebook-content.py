# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Configuração Inicial
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
    LongType, TimestampType, IntegerType, MapType, ArrayType
)
from functools import reduce
from typing import List
import requests
import json
import os
import time
import re

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inicializar sessão Spark
spark = SparkSession.builder.appName("YouTubeDataExtractionCategories").getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Configurações da API do YouTube
API_KEY = "AIzaSyDmAULHlDdg3HNIGeE-k45IMxLj1XoH5CA"
# API_KEY = "AIzaSyC0O_tDb6CKobRAWv2VBKk_TsVNZ1ZnY_U"
BASE_URL = "https://www.googleapis.com/"
REGION_CODE = "BR"

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

def extract_video_categories():

    """
    Extrai as categorias de vídeos disponíveis
    """

    # Define endpoint and params of API response
    endpoint = 'youtube/v3/videoCategories'
    params = {
        'part': 'snippet',
        'regionCode': REGION_CODE
    }
    
    data = fetch_youtube_data(endpoint, params)

    if data and 'items' in data:
        df = spark.createDataFrame(data['items'], schema=VIDEO_CATEGORIES_SCHEMA)

        return df.withColumn("extracted_at", current_timestamp()) \
                .withColumn("data_source", lit(endpoint))

    return spark.createDataFrame([], schema=VIDEO_CATEGORIES_SCHEMA)

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

    lakehouse_base_path = "Files/landing/youtube"

    # --- 1. Extrair categorias de vídeos ---
    print("Extraindo categorias de vídeos...")
    categories_df = extract_video_categories()
    categories_df.write.mode("overwrite").json(f"{lakehouse_base_path}/categories")

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
