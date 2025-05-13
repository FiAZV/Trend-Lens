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

# MARKDOWN ********************

# ### Initial Configuration

# CELL ********************

# Configuração Inicial
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, broadcast
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType, LongType, TimestampType
from tenacity import retry, stop_after_attempt, wait_exponential
from functools import reduce
from datetime import datetime
import requests
import json
import logging

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

# Configurações da API do YouTube
API_KEY = "AIzaSyDmAULHlDdg3HNIGeE-k45IMxLj1XoH5CA"
BASE_URL = "https://www.googleapis.com/youtube/v3/"
LAKEHOUSE_PATH = "Files/raw/youtube"
REGION_CODE = "BR"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Support Functions

# CELL ********************

def fetch_youtube_data(endpoint: str, params: dict, timeout: int = 10) -> dict:
    """
    Faz requisições resilientes à API do YouTube com tratamento avançado de erros
    
    Args:
        endpoint: Path da API (ex: 'videos', 'commentThreads')
        params: Parâmetros da requisição
        timeout: Tempo máximo de espera em segundos
    
    Returns:
        dict: Dados no formato JSON ou None em caso de falha persistente
    
    Raises:
        HTTPError: Para erros 4xx/5xx após retentativas
    """
    
    url = f"{BASE_URL}{endpoint.strip('/')}"
    headers = {
        "Accept": "application/json",
        "User-Agent": "YouTubeDataExtraction/1.0"
    }
    params = {
        **params,
        "key": API_KEY,
        "alt": "json"
    }
    
    try:
        response = requests.get(
            url,
            params=params,
            headers=headers,
            timeout=timeout
        )
        response.raise_for_status()
        
        # Verifica estrutura básica da resposta
        if not response.json().get("items"):
            logger.warning(f"Resposta vazia para: {url}")
            return None
            
        return response.json()
    
    except requests.exceptions.HTTPError as e:
        logger.error(f"Erro HTTP {e.response.status_code}: {e.response.text}")
        if e.response.status_code == 403:
            logger.error("Verifique as permissões da API Key no Google Cloud Console")
        raise
    except requests.exceptions.Timeout:
        logger.error(f"Timeout excedido ({timeout}s) para: {url}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro de conexão: {str(e)}")
        raise
    except json.JSONDecodeError:
        logger.error("Resposta não é JSON válido")
        return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_supported_regions():

    """Obtém todas as regiões suportadas da API do YouTube"""

    path = 'i18nRegions'
    params = {'part': 'snippet'}
    
    data = fetch_youtube_data(path, params)

    # return [region['id'] for region in data.get('items', [])] if data else []
    return ['BR', 'US'] # Selecionando regiões para deixar o arquivo mais leve

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def save_to_lakehouse(df, output_path, partitions=[]):
    df.write.mode("overwrite") \
        .partitionBy(*partitions) \
        .format("json") \
        .save(output_path)

    # current_date = datetime.now().strftime("%Y%m%d")
    # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # # Nome do arquivo com data e timestamp
    # file_name = f"data_{timestamp}.json"
    # full_path = f"{output_path}/{file_name}"
    
    # # Controlar número de arquivos
    # df.coalesce(1).write.mode("append") \
    #     .partitionBy(*partitions) \
    #     .format("json") \
    #     .save(full_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Video Categories

# CELL ********************

def get_video_categories(region_code):

    path = 'videoCategories'
    params = {
        "part": "snippet",
        "regionCode": region_code
    }

    data = fetch_youtube_data(path, params)
    
    return [item["id"] for item in data.get("items", []) if item["snippet"].get("assignable")]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_video_categories(region_codes=None):
    """
    Extrai categorias de vídeos para múltiplas regiões com particionamento.

    Args:
        region_codes: Lista de códigos de região (se None, busca todas as regiões)

    Returns:
        DataFrame particionado por region_code com metadados
    """
    if region_codes is None:
        region_codes = get_supported_regions()

    path = 'videoCategories'
    all_rows = []

    for region in region_codes:
        try:
            params = {
                'part': 'snippet',
                'regionCode': region
            }

            data = fetch_youtube_data(path, params)

            if data and 'items' in data:
                for item in data['items']:
                    item['region_code'] = region
                    item['extracted_at'] = datetime.utcnow().isoformat()
                    item['data_source'] = f"{BASE_URL}{path}"
                    all_rows.append(item)

        except Exception as e:
            logger.warning(f"Erro ao extrair categorias para região {region}: {e}")
            continue

    if not all_rows:
        return spark.createDataFrame([], schema=StructType([
            StructField("id", StringType()),
            StructField("snippet", MapType(StringType(), StringType())),
            StructField("etag", StringType()),
            StructField("kind", StringType()),
            StructField("region_code", StringType()),
            StructField("extracted_at", StringType()),
            StructField("data_source", StringType())
        ]))

    # Criar DataFrame a partir da lista de linhas
    df = spark.createDataFrame(all_rows)

    # Salvar com particionamento
    save_to_lakehouse(
        df,
        output_path=f"{LAKEHOUSE_PATH}/video_categories",
        partitions=["region_code"]
    )

    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# def extract_video_categories():

#     """
#     Extrai as categorias de vídeos disponíveis
#     """
    
#     params = {
#         'part': 'snippet',
#         'regionCode': REGION_CODE
#     }
    
#     data = fetch_youtube_data('youtube/v3/videoCategories', params)
#     if data and 'items' in data:
#         df = spark.createDataFrame(data['items'])
#         return df.withColumn("extracted_at", current_timestamp()) \
#                 .withColumn("data_source", lit("youtube/v3/videoCategories"))
#     return spark.createDataFrame([], "id STRING, snippet STRUCT<...>")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# ### Videos

# CELL ********************

def get_popular_videos(region_code, category_id):

    path = 'videos'
    params = {
        "part": "snippet,contentDetails,statistics",
        "chart": "mostPopular",
        "regionCode": region_code,
        "videoCategoryId": category_id,
        "maxResults": 50
    }
    data = fetch_youtube_data(path, params)
    return data.get("items", [])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_popular_videos_by_region_and_category(spark):

    regions = get_supported_regions()
    now = datetime.utcnow()
    current_date = datetime.now().strftime("%Y%m%d")

    schema = StructType([
        StructField("region_code", StringType(), True),
        StructField("category_id", StringType(), True),
        StructField("video_id", StringType(), True),
        StructField("snippet", StructType([
            StructField("publishedAt", StringType(), True),
            StructField("channelId", StringType(), True),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("tags", ArrayType(StringType()), True),
            StructField("channelTitle", StringType(), True)
        ])),
        StructField("statistics", StructType([
            StructField("viewCount", LongType(), True),
            StructField("likeCount", LongType(), True),
            StructField("commentCount", LongType(), True)
        ])),
        StructField("contentDetails", StructType([
            StructField("duration", StringType(), True),
            StructField("dimension", StringType(), True),
            StructField("definition", StringType(), True),
            StructField("caption", StringType(), True)
        ])),
        StructField("extracted_at", TimestampType(), True),
        StructField("data_source", StringType(), True)
    ])

    rows = []

    for region_code in regions: # melhorar loop para procurar apenas em categorias existentes

        category_ids = get_video_categories(region_code)

        for category_id in category_ids:

            try:
                items = get_popular_videos(region_code, category_id)

                for item in items:

                    snippet = item.get("snippet", {})
                    statistics = item.get("statistics", {})
                    content = item.get("contentDetails", {})

                    row = (
                        region_code,
                        category_id,
                        item.get("id"),
                        {
                            "publishedAt": snippet.get("publishedAt"),
                            "channelId": snippet.get("channelId"),
                            "title": snippet.get("title"),
                            "description": snippet.get("description"),
                            "tags": snippet.get("tags"),
                            "channelTitle": snippet.get("channelTitle")
                        },
                        {
                            "viewCount": int(statistics.get("viewCount", 0)) if statistics.get("viewCount") else None,
                            "likeCount": int(statistics.get("likeCount", 0)) if statistics.get("likeCount") else None,
                            "commentCount": int(statistics.get("commentCount", 0)) if statistics.get("commentCount") else None
                        },
                        {
                            "duration": content.get("duration"),
                            "dimension": content.get("dimension"),
                            "definition": content.get("definition"),
                            "caption": content.get("caption")
                        },
                        now,
                        current_date,
                        f"{BASE_URL}videos"
                    )
                    rows.append(row)
            except Exception as e:
                logging.warning(f"Erro em região {region_code} e categoria {category_id}: {e}")

    if rows:
        df = spark.createDataFrame(rows, schema)
        save_to_lakehouse(
            df,
            output_path=f"{LAKEHOUSE_PATH}/videos",
            partitions=["region_code", "category_id", "current_date"]
        )
    else:
        logging.warning("Nenhum vídeo extraído.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# def extract_popular_videos():

#     """
#     Extrai os 50 vídeos mais populares do Brasil
#     """
#     path = 'videos'
    
#     params = {
#         'part': 'snippet,contentDetails,statistics',
#         'chart': 'mostPopular',
#         'regionCode': REGION_CODE,
#         'maxResults': 50
#     }
    
#     data = fetch_youtube_data(path, params)
#     if data and 'items' in data:
#         df = spark.createDataFrame(data['items'])
        
#         # Adicionar metadados de auditoria
#         return df.withColumn("extracted_at", current_timestamp()) \
#                 .withColumn("data_source", lit(f"{BASE_URL}{path}"))
#     return spark.createDataFrame([], "id STRING, snippet STRUCT<...>")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

def get_extracted_video_ids():

    # Carrega os vídeos já extraídos
    videos_df = spark.read.json(f"{LAKEHOUSE_PATH}/videos")
    
    if videos_df.isEmpty():
        logger.warning("Nenhum vídeo encontrado para extrair comentários")
        return spark.createDataFrame([], "id STRING, snippet STRUCT<...>")
        
    # Obtém os IDs dos vídeos
    return [row.video_id for row in videos_df.select("video_id").collect()]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_video_comments(spark):
    """
    Extrai comentários para todos os vídeos populares já extraídos
    
    Args:
        spark: SparkSession ativa
        
    Returns:
        DataFrame com os comentários ou DataFrame vazio se não houver vídeos
    """
    try:
        video_ids = get_extracted_video_ids()
        all_comments = []
        
        # Alterado para endpoint correto da API
        path = 'commentThreads' 
        
        for video_id in video_ids:
            params = {
                'part': 'snippet',
                'videoId': video_id, 
                'maxResults': 100,
                'order': 'relevance',
                'textFormat': 'plainText'  # Novo parâmetro útil
            }
            
            try:
                data = fetch_youtube_data(path, params)
                if data and 'items' in data:
                    # Estrutura de transformação mantida
                    for item in data['items']:
                        comment = item['snippet']['topLevelComment']['snippet']
                        comment['videoId'] = video_id  # Adiciona ID do vídeo
                        all_comments.append(comment)  # Coleta apenas dados relevantes
                        
            except Exception as e:
                logger.warning(f"Erro no vídeo {video_id[:5]}...: {str(e)[:50]}...")
                continue

        # Criação do DataFrame corrigida
        if all_comments:
            schema = StructType([
                StructField("videoId", StringType()),
                StructField("textDisplay", StringType()),
                StructField("authorDisplayName", StringType()),
                StructField("likeCount", IntegerType()),
                StructField("publishedAt", TimestampType())
            ])
            
            comments_df = spark.createDataFrame(
                [(c.get('videoId'), c.get('textDisplay'), 
                  c.get('authorDisplayName'), c.get('likeCount', 0),
                  c.get('publishedAt')) for c in all_comments],
                schema=schema
            )
            
            save_to_lakehouse(
                comments_df,
                output_path=f"{LAKEHOUSE_PATH}/comments"
            )
            return comments_df
            
        logger.warning("Nenhum comentário extraído")
        return
        
    except Exception as e:
        logger.error(f"Falha crítica: {str(e)}")
        return

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# def extract_video_comments(video_ids):
    
#     """
#     Extrai comentários para uma lista de vídeos
#     """
    
#     all_comments = []

#     path = 'commentThreads'
    
#     for video_id in video_ids:
#         params = {
#             'part': 'snippet',
#             'videoId': video_id,
#             'maxResults': 20,
#             'order': 'relevance'
#         }
        
#         data = fetch_youtube_data(path, params)
#         if data and 'items' in data:
#             # Adicionar video_id para relacionamento
#             for item in data['items']:
#                 item['snippet']['topLevelComment']['snippet']['videoId'] = video_id
#             all_comments.extend(data['items'])
    
#     if all_comments:
#         df = spark.createDataFrame(all_comments)
#         return df.withColumn("extracted_at", current_timestamp()) \
#                 .withColumn("data_source", lit(f"{BASE_URL}{path}"))
#     return spark.createDataFrame([], "id STRING, snippet STRUCT<...>")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

def get_channel_ids():
    """Obtém todos os IDs de canais já extraídos do lakehouse"""
    
    # Variáveis
    lakehouse_path_videos = f"{LAKEHOUSE_PATH}/videos"
    lakehouse_path_channels = f"{LAKEHOUSE_PATH}/channels"

    # Carrega os canais já extraídos
    try:
        channels_df = spark.read.json(f"{LAKEHOUSE_PATH}/channels")
    except AnalysisException:
        logger.warning("Pasta de canais não encontrada")
        channels_df = spark.createDataFrame([], channel_schema)
    
    if channels_df.isEmpty():
        logger.warning("Nenhum canal encontrado no lakehouse")
        channels_from_channels = []
    else:
        channels_from_channels = [row.id for row in channels_df.select("id").distinct().collect()]
        
    # Obtém os IDs únicos dos canais
    try:
        videos_df = spark.read.json(f"{LAKEHOUSE_PATH}/videos")
    except AnalysisException:
        logger.warning("Pasta de vídeos não encontrada")
        videos_df = spark.createDataFrame([], video_schema)
        
    if videos_df.isEmpty():
        logger.warning("Nenhum vídeo encontrado para extrair canais")
        channels_from_videos = []
    else:
        channels_from_videos = [row.snippet.channelId for row in videos_df.select(col("snippet.channelId")).distinct().collect()]

    channels_ids = list(set(channels_from_channels + channels_from_videos))

    return channels_ids

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

def extract_channels():
    """
    Extrai informações de canais da API do YouTube.
    
    Retorna:
        DataFrame Spark com dados dos canais
    """

    channel_ids = get_channel_ids()
    
    # Esquema completo para canais (movido para antes do primeiro uso)
    schema = StructType([
        StructField("id", StringType()),
        StructField("snippet", StructType([
            StructField("title", StringType()),
            StructField("description", StringType()),
            StructField("customUrl", StringType()),
            StructField("publishedAt", StringType()),
            StructField("thumbnails", MapType(StringType(), StructType([
                StructField("url", StringType()),
                StructField("width", LongType()),
                StructField("height", LongType())
            ]))),
            StructField("country", StringType())
        ])),
        StructField("statistics", StructType([
            StructField("viewCount", LongType()),
            StructField("subscriberCount", LongType()),
            StructField("hiddenSubscriberCount", BooleanType()),
            StructField("videoCount", LongType())
        ])),
        StructField("extracted_at", StringType()),
        StructField("data_source", StringType())
    ])

    # Dividir em lotes de 50 IDs (limite da API)
    batch_size = 50
    all_items = []
    
    for i in range(0, len(channel_ids), batch_size):
        batch_ids = channel_ids[i:i+batch_size]
        
        try:
            data = fetch_youtube_data(
                endpoint='channels',
                params={
                    'part': 'snippet,contentDetails,statistics,brandingSettings',
                    'id': ','.join(batch_ids),
                    'maxResults': batch_size
                }
            )

            if data and 'items' in data:
                # Adicionar metadados
                for item in data['items']:
                    item['extracted_at'] = datetime.utcnow().isoformat()
                    item['data_source'] = f"{BASE_URL}channels"
                
                all_items.extend(data['items'])

        except Exception as e:
            logger.error(f"Erro no batch {i//batch_size}: {str(e)}")
            continue

    # Se nenhum item foi coletado
    if not all_items:
        logger.warning("Nenhum dado de canal foi extraído")
        return spark.createDataFrame([], schema=schema)
        
    # Corrigido: usar all_items ao invés do último batch
    df = spark.createDataFrame(all_items, schema=schema)
    
    # Salva no lakehouse
    save_to_lakehouse(
        df,
        output_path=f"{LAKEHOUSE_PATH}/channels",
        partitions=[]
    )
    
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# def extract_channels(channel_ids):
    
#     """
#     Extrai informações dos canais
#     """

#     path = 'channels'
#     params = {
#         'part': 'snippet,contentDetails,statistics',
#         'id': ','.join(channel_ids)
#     }
    
#     data = fetch_youtube_data(path, params)
#     if data and 'items' in data:
#         df = spark.createDataFrame(data['items'])
#         return df.withColumn("extracted_at", current_timestamp()) \
#                 .withColumn("data_source", lit(f"{BASE_URL}{path}"))
#     return spark.createDataFrame([], "id STRING, snippet STRUCT<...>")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def main():
    
    """
    Função principal que executa as extrações da API do YouTube
    e salva os dados no Lakehouse particionado.
    """

    logger.info("Iniciando pipeline de extração da API do YouTube...")

    # Extrair categorias de vídeos
    extract_video_categories()

    # Extrair vídeos mais populares por região e categoria
    extract_popular_videos_by_region_and_category(spark)

    # Extrair comentários dos vídeos
    extract_video_comments(spark)

    # Extrair canais
    extract_channels()

    logger.info("Pipeline finalizado com sucesso.")

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
