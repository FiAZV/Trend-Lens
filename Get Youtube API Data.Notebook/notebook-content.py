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
# META       "default_lakehouse_workspace_id": "41c1bbe3-1b20-4e9c-8afb-99229f989290"
# META     }
# META   }
# META }

# CELL ********************

# Configuração Inicial
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
import requests
import json

# Inicializar sessão Spark
spark = SparkSession.builder.appName("YouTubeDataExtraction").getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Configurações da API do YouTube
API_KEY = "AIzaSyDmAULHlDdg3HNIGeE-k45IMxLj1XoH5CA"
BASE_URL = "https://www.googleapis.com/"
REGION_CODE = "BR"

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
    Extrai os 50 vídeos mais populares do Brasil
    """
    
    params = {
        'part': 'snippet,contentDetails,statistics',
        'chart': 'mostPopular',
        'regionCode': REGION_CODE,
        'maxResults': 50
    }
    
    data = fetch_youtube_data('youtube/v3/videos', params)
    if data and 'items' in data:
        df = spark.createDataFrame(data['items'])
        
        # Adicionar metadados de auditoria
        return df.withColumn("extracted_at", current_timestamp()) \
                .withColumn("data_source", lit("youtube/v3/videos"))
    return spark.createDataFrame([], "id STRING, snippet STRUCT<...>")

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
    
    params = {
        'part': 'snippet',
        'regionCode': REGION_CODE
    }
    
    data = fetch_youtube_data('youtube/v3/videoCategories', params)
    if data and 'items' in data:
        df = spark.createDataFrame(data['items'])
        return df.withColumn("extracted_at", current_timestamp()) \
                .withColumn("data_source", lit("youtube/v3/videoCategories"))
    return spark.createDataFrame([], "id STRING, snippet STRUCT<...>")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_video_comments(video_ids):
    
    """
    Extrai comentários para uma lista de vídeos
    """
    
    all_comments = []
    
    for video_id in video_ids:
        params = {
            'part': 'snippet',
            'videoId': video_id,
            'maxResults': 20,
            'order': 'relevance'
        }
        
        data = fetch_youtube_data('youtube/v3/commentThreads', params)
        if data and 'items' in data:
            # Adicionar video_id para relacionamento
            for item in data['items']:
                item['snippet']['topLevelComment']['snippet']['videoId'] = video_id
            all_comments.extend(data['items'])
    
    if all_comments:
        df = spark.createDataFrame(all_comments)
        return df.withColumn("extracted_at", current_timestamp()) \
                .withColumn("data_source", lit("youtube/v3/commentThreads"))
    return spark.createDataFrame([], "id STRING, snippet STRUCT<...>")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_channels(channel_ids):
    
    """
    Extrai informações dos canais
    """

    params = {
        'part': 'snippet,contentDetails,statistics',
        'id': ','.join(channel_ids)
    }
    
    data = fetch_youtube_data('youtube/v3/channels', params)
    if data and 'items' in data:
        df = spark.createDataFrame(data['items'])
        return df.withColumn("extracted_at", current_timestamp()) \
                .withColumn("data_source", lit("youtube/v3/channels"))
    return spark.createDataFrame([], "id STRING, snippet STRUCT<...>")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_delta_table(raw_path, table_name):
        df = spark.read.json(raw_path)
        
        # Escrever com tratamento de schema
        df.write.format("delta") \
          .option("overwriteSchema", "true") \
          .mode("overwrite") \
          .saveAsTable(f"Lakehouse.bronze.{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def main():

    # Extrair vídeos populares
    videos_df = extract_popular_videos()
    videos_df.printSchema()
    videos_df.write.mode("overwrite").json("Files/raw/youtube2_teste/videos")
    
    #Extrair categorias
    categories_df = extract_video_categories()
    categories_df.write.mode("overwrite").json("Files/raw/youtube2_teste/categories")
    
    # Passo 3: Processar IDs para relacionamentos
    from pyspark.sql.functions import col
    
    # Coletar IDs de vídeos
    video_ids = [row.id for row in videos_df.select(col("id")).collect()]
    
    # Coletar IDs de canais únicos e válidos
    channel_ids = [row.channelId for row in videos_df.select(
        col("snippet.channelId").alias("channelId")
    ).distinct().collect() if row.channelId is not None]
    
    # Extrair comentários
    comments_df = extract_video_comments(video_ids)
    comments_df.write.mode("overwrite").json("Files/raw/youtube2_teste/comments")
    
    # Extrair canais
    if channel_ids:  # Só executa se houver IDs válidos
        channels_df = extract_channels(channel_ids)
        channels_df.write.mode("overwrite").json("Files/raw/youtube2_teste/channels")
    else:
        print("Nenhum ID de canal válido encontrado")
    
    # Criação das Tabelas Delta com Schema Handling
 
    create_delta_table("Files/raw/youtube2_teste/videos", "youtube2_videos")
    create_delta_table("Files/raw/youtube2_teste/categories", "youtube2_categories")
    create_delta_table("Files/raw/youtube2_teste/comments", "youtube2_comments")
    
    if channel_ids:
        create_delta_table("Files/raw/youtube2_teste/channels", "youtube2_channels")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Executar pipeline
if __name__ == "__main__":
    main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
