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
spark = SparkSession.builder.appName("YouTubeDataExtraction").getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Configurações da API do YouTube
# API_KEY = "AIzaSyDmAULHlDdg3HNIGeE-k45IMxLj1XoH5CA"
API_KEY = "AIzaSyC0O_tDb6CKobRAWv2VBKk_TsVNZ1ZnY_U"
BASE_URL = "https://www.googleapis.com/"
REGION_CODE = "BR"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Schemas

# CELL ********************

VIDEOS_SCHEMA = StructType([
    StructField("kind", StringType(), True),
    StructField("etag", StringType(), True),
    StructField("id", StringType(), True),

    StructField("snippet", StructType([
        StructField("publishedAt", StringType(), True),  # Pode ser convertido para Timestamp depois
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
        StructField("viewCount", StringType(), True),    # Pode ser convertido para LongType depois
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

VIDEO_CATEGORIES_SCHEMA = StructType([
    StructField("kind", StringType(), True),
    StructField("etag", StringType(), True),
    StructField("id", StringType(), True),
    StructField("snippet", StructType([
        StructField("title", StringType(), True),
        StructField("assignable", StringType(), True),  # normalmente é "true" ou "false" como string
        StructField("channel_id", StringType(), True)  # normalmente é "true" ou "false" como string
    ]), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

COMMENTS_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("snippet", StructType([
        StructField("channelId", StringType(), True),
        StructField("videoId", StringType(), True),
        StructField("topLevelComment", StructType([
            StructField("id", StringType(), True),
            StructField("snippet", StructType([
                StructField("authorDisplayName", StringType(), True),
                StructField("authorProfileImageUrl", StringType(), True),
                StructField("authorChannelUrl", StringType(), True),
                StructField("authorChannelId", StructType([
                    StructField("value", StringType(), True)
                ])),
                StructField("channelId", StringType(), True),
                StructField("videoId", StringType(), True),
                StructField("textDisplay", StringType(), True),
                StructField("textOriginal", StringType(), True),
                StructField("canRate", BooleanType(), True),
                StructField("viewerRating", StringType(), True),
                StructField("likeCount", IntegerType(), True),
                StructField("publishedAt", StringType(), True),
                StructField("updatedAt", StringType(), True)
            ]))
        ])),
        StructField("canReply", BooleanType(), True),
        StructField("totalReplyCount", IntegerType(), True),
        StructField("isPublic", BooleanType(), True)
    ]))
])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CHANNELS_SCHEMA = StructType([
    StructField("kind", StringType(), True),
    StructField("etag", StringType(), True),
    StructField("id", StringType(), True),

    StructField("snippet", StructType([
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("customUrl", StringType(), True),
        StructField("publishedAt", StringType(), True),
        StructField("thumbnails", StructType([
            StructField("default", StructType([
                StructField("url", StringType(), True),
                StructField("width", IntegerType(), True),
                StructField("height", IntegerType(), True)
            ])),
            StructField("medium", StructType([
                StructField("url", StringType(), True),
                StructField("width", IntegerType(), True),
                StructField("height", IntegerType(), True)
            ])),
            StructField("high", StructType([
                StructField("url", StringType(), True),
                StructField("width", IntegerType(), True),
                StructField("height", IntegerType(), True)
            ]))
        ])),
        StructField("localized", StructType([
            StructField("title", StringType(), True),
            StructField("description", StringType(), True)
        ])),
        StructField("country", StringType(), True)
    ])),

    StructField("contentDetails", StructType([
        StructField("relatedPlaylists", StructType([
            StructField("likes", StringType(), True),
            StructField("uploads", StringType(), True)
        ]))
    ])),

    StructField("statistics", StructType([
        StructField("viewCount", StringType(), True),
        StructField("subscriberCount", StringType(), True),
        StructField("hiddenSubscriberCount", BooleanType(), True),
        StructField("videoCount", StringType(), True)
    ])),

    StructField("topicDetails", StructType([
        StructField("topicIds", ArrayType(StringType()), True),
        StructField("topicCategories", ArrayType(StringType()), True)
    ])),

    StructField("status", StructType([
        StructField("privacyStatus", StringType(), True),
        StructField("isLinked", BooleanType(), True),
        StructField("longUploadsStatus", StringType(), True),
        StructField("madeForKids", BooleanType(), True)
    ])),

    StructField("brandingSettings", StructType([
        StructField("channel", StructType([
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("keywords", StringType(), True),
            StructField("trackingAnalyticsAccountId", StringType(), True),
            StructField("unsubscribedTrailer", StringType(), True),
            StructField("country", StringType(), True)
        ])),
        StructField("image", StructType([
            StructField("bannerExternalUrl", StringType(), True)
        ]))
    ]))

    # ,StructField("contentOwnerDetails", StructType([]))  # vazio, mas incluído por completude
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Functions to Extract Data

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

def get_video_category_ids(path: str = "Files/raw/youtube/categories") -> list:
    
    """
    Lê o arquivo de categorias de vídeos no Lakehouse e retorna uma lista de IDs de categoria.

    Parâmetros:
        path (str): Caminho no Lakehouse onde as categorias estão armazenadas em formato Delta.

    Retorna:
        List[str]: Lista de IDs das categorias.
    """

    try:
        categories_df = spark.read.format("json").load(path)
        category_ids = [row["id"] for row in categories_df.select("id").distinct().collect()]

        return category_ids

    except Exception as e:
        print(f"Erro ao carregar categorias: {e}")
        
        return []


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
        'regionCode': REGION_CODE,
        'maxResults': 50
    }

    category_ids = get_video_category_ids()

    all_dfs = []

    for category_id in category_ids:
        
        params = base_params.copy()
        params['videoCategoryId'] = category_id
        
        data = fetch_youtube_data(endpoint, params)

        if data and 'items' in data and len(data['items']) > 0:
            df = spark.createDataFrame(data['items'], schema=VIDEOS_SCHEMA)
            df = df.withColumn("extracted_at", current_timestamp()) \
                   .withColumn("data_source", lit(endpoint))
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

def get_video_ids(videos_df: DataFrame) -> list:
    """
    Extrai uma lista de IDs únicos de vídeos de um DataFrame de vídeos.
    
    Parâmetros:
        videos_df (DataFrame): DataFrame contendo vídeos.
    
    Retorna:
        List[str]: Lista de IDs de vídeos.
    """

    videos_id_list = [row.id for row in videos_df.select(col("id")).distinct().collect()]

    return videos_id_list


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_channel_ids(videos_df: DataFrame, spark: SparkSession) -> list:
    
    """
    Extrai IDs únicos de canais dos vídeos e inclui também canais já armazenados anteriormente.
    
    Parâmetros:
        videos_df (DataFrame): DataFrame com vídeos.
        spark (SparkSession): Sessão Spark para leitura dos canais existentes.
    
    Retorna:
        List[str]: Lista de IDs únicos de canais a serem extraídos/atualizados.
    """
    
    # IDs de canais a partir dos vídeos
    video_channel_ids = videos_df \
        .select(col("snippet.channelId").alias("channelId")) \
        .distinct() \
        .dropna() \
        .rdd.map(lambda row: row.channelId) \
        .collect()
    
    # Tentar ler canais já salvos
    try:
        existing_channels_df = spark.read.json("Files/raw/youtube/channels")
        existing_channel_ids = existing_channels_df \
            .select(col("id").alias("channelId")) \
            .distinct() \
            .rdd.map(lambda row: row.channelId) \
            .collect()
    except Exception:
        existing_channel_ids = []

    # Combina e remove duplicatas
    all_channel_ids = list(set(video_channel_ids + existing_channel_ids))
    
    return all_channel_ids


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_video_comments(video_ids):

    """
    Extrai comentários para uma lista de vídeos.
    """

    endpoint = 'youtube/v3/commentThreads'
    base_params = {
        'part': 'snippet',
        'maxResults': 15,
        'order': 'relevance'
    }

    all_dfs = []

    for video_id in video_ids:
        
        params = base_params.copy()
        params['videoId'] = video_id

        data = fetch_youtube_data(endpoint, params)

        if data and 'items' in data and len(data['items']) > 0:
            df = spark.createDataFrame(data['items'], schema=COMMENTS_SCHEMA)
            df = df.withColumn("extracted_at", current_timestamp()) \
                   .withColumn("data_source", lit(endpoint))
            all_dfs.append(df)

    if all_dfs:
        return reduce(lambda a, b: a.unionByName(b), all_dfs)

    return spark.createDataFrame([], schema=COMMENTS_SCHEMA)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_channels(channel_ids: List[str], spark: SparkSession, schema: StructType) -> DataFrame:
    """
    Extrai informações dos canais com base nos IDs fornecidos, processando em lotes de até 50 IDs por requisição.

    Parâmetros:
        channel_ids (List[str]): Lista de IDs de canais do YouTube.
        spark (SparkSession): Sessão Spark ativa.
        schema (StructType): Esquema do DataFrame para os canais.

    Retorna:
        DataFrame: Dados dos canais extraídos.
    """
    endpoint = 'youtube/v3/channels'
    batch_size = 50
    all_dfs = []

    for i in range(0, len(channel_ids), batch_size):
        batch_ids = channel_ids[i:i + batch_size]
        params = {
            'part': 'snippet,contentDetails,statistics,brandingSettings,topicDetails,localizations,status', #contentOwnerDetails
            'id': ','.join(batch_ids)
        }

        data = fetch_youtube_data(endpoint, params)

        if data and 'items' in data and len(data['items']) > 0:
            df = spark.createDataFrame(data['items'], schema=schema)
            df = df.withColumn("extracted_at", current_timestamp()) \
                   .withColumn("data_source", lit(endpoint))
            all_dfs.append(df)

    if all_dfs:
        return reduce(lambda a, b: a.unionByName(b), all_dfs)

    return spark.createDataFrame([], schema=schema)


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

    lakehouse_base_path = "Files/raw/youtube"

    # --- 1. Extrair categorias de vídeos ---
    print("Extraindo categorias de vídeos...")
    categories_df = extract_video_categories()
    categories_df.write.mode("append").json(f"{lakehouse_base_path}/categories")

    # --- 2. Extrair vídeos populares ---
    print("Extraindo vídeos populares...")
    videos_df = extract_popular_videos()
    videos_df.write.mode("append").json(f"{lakehouse_base_path}/videos")

    # --- 3. Obter IDs de vídeos e canais válidos ---
    print("Coletando IDs de vídeos e canais...")
    video_ids = get_video_ids(videos_df)
    channel_ids = get_channel_ids(videos_df, spark)

    # --- 4. Extrair comentários dos vídeos ---
    print(f"Extraindo comentários dos vídeos...")
    comments_df = extract_video_comments(video_ids)
    comments_df.write.mode("append").json(f"{lakehouse_base_path}/comments")

    # --- 5. Extrair informações dos canais ---
    if channel_ids:
        print(f"Extraindo informações de {len(channel_ids)} canais...")
        channels_df = extract_channels(channel_ids, spark=spark, schema=CHANNELS_SCHEMA)
        channels_df.write.mode("append").json(f"{lakehouse_base_path}/channels")
    else:
        print("Nenhum ID de canal válido encontrado.")

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
