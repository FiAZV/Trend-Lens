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
# META     },
# META     "warehouse": {
# META       "default_warehouse": "c7ab93f1-bec2-963e-4a84-bfc6db9babcf",
# META       "known_warehouses": [
# META         {
# META           "id": "c7ab93f1-bec2-963e-4a84-bfc6db9babcf",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, coalesce, lit, to_timestamp
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("YouTubeSilverLayer").getOrCreate()

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


def transform_videos():
    videos_df = spark.table(f"{bronze_schema}.youtube_videos")
    
    # Flatten nested fields + seleção de colunas
    videos_silver = videos_df.select(
        col("id").alias("id"),
        col("snippet.title").alias("title"),
        col("snippet.description").alias("description"),
        col("snippet.channelId").alias("channel_id"),
        col("snippet.categoryId").cast(IntegerType()).alias("category_id"),
        to_timestamp(col("snippet.publishedAt")).alias("published_at"),
        coalesce(col("statistics.viewCount").cast(IntegerType()), lit(0)).alias("views"),
        coalesce(col("statistics.likeCount").cast(IntegerType()), lit(0)).alias("likes"),
        coalesce(col("statistics.favoriteCount").cast(IntegerType()), lit(0)).alias("favorites"),
        coalesce(col("statistics.commentCount").cast(IntegerType()), lit(0)).alias("comments"),
        col("contentDetails.duration").alias("duration"),
        col("contentDetails.dimension").alias("dimension"),
        col("contentDetails.definition").alias("definition"),
        col("contentDetails.caption").alias("caption"),
        col("contentDetails.licensedContent").cast(IntegerType()).alias("licensedContent"),
        col("contentDetails.projection").alias("projection"),
        col("extracted_at")
    ).distinct()  # Remover duplicatas
    
    # Escrever tabela Silver
    videos_silver.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{silver_schema}.youtube_videos")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType

def transform_thumbnails():
    # Ler tabela de vídeos da Bronze
    videos_df = spark.table(f"{bronze_schema}.youtube_videos")
    
    # 1. Definir schema do JSON de thumbnails
    thumbnail_schema = StructType([
        StructField("default", StructType([
            StructField("url", StringType()),
            StructField("width", IntegerType()),
            StructField("height", IntegerType())
        ])),
        StructField("medium", StructType([
            StructField("url", StringType()),
            StructField("width", IntegerType()),
            StructField("height", IntegerType())
        ])),
        StructField("high", StructType([
            StructField("url", StringType()),
            StructField("width", IntegerType()),
            StructField("height", IntegerType())
        ])),
        StructField("standard", StructType([
            StructField("url", StringType()),
            StructField("width", IntegerType()),
            StructField("height", IntegerType())
        ])),
        StructField("maxres", StructType([
            StructField("url", StringType()),
            StructField("width", IntegerType()),
            StructField("height", IntegerType())
        ]))
    ])
    
    # 2. Parse do JSON para struct
    videos_parsed = videos_df.withColumn(
        "thumbnails_parsed",
        from_json(col("snippet.thumbnails"), thumbnail_schema)
    )
    
    # 3. Extrair campos da struct parseada
    thumbnails_silver = videos_parsed.select(
        col("id").alias("video_id"),
        col("thumbnails_parsed.default.url").alias("default_url"),
        col("thumbnails_parsed.default.width").alias("default_width"),
        col("thumbnails_parsed.default.height").alias("default_height"),
        col("thumbnails_parsed.medium.url").alias("medium_url"),
        col("thumbnails_parsed.medium.width").alias("medium_width"),
        col("thumbnails_parsed.medium.height").alias("medium_height"),
        col("thumbnails_parsed.high.url").alias("high_url"),
        col("thumbnails_parsed.high.width").alias("high_width"),
        col("thumbnails_parsed.high.height").alias("high_height"),
        col("thumbnails_parsed.standard.url").alias("standard_url"),
        col("thumbnails_parsed.standard.width").alias("standard_width"),
        col("thumbnails_parsed.standard.height").alias("standard_height"),
        col("thumbnails_parsed.maxres.url").alias("maxres_url"),
        col("thumbnails_parsed.maxres.width").alias("maxres_width"), 
        col("thumbnails_parsed.maxres.height").alias("maxres_height"), 
        col("extracted_at")
    ).dropDuplicates(["video_id"])
    
    # 4. Escrever tabela Silver
    thumbnails_silver.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{silver_schema}.youtube_thumbnails")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_tags():
    videos_df = spark.table(f"{bronze_schema}.youtube_videos")
    
    # Flatten nested fields + seleção de colunas
    videos_silver = videos_df.select(
        col("id").alias("video_id"),
        col("snippet.tags").alias("tag"),
    ).distinct()
    
    # Escrever tabela Silver
    videos_silver.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{silver_schema}.youtube_tags")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_languages():
    videos_df = spark.table(f"{bronze_schema}.youtube_videos")
    
    # Flatten nested fields + seleção de colunas
    videos_silver = videos_df.select(
        col("id").alias("video_id"),
        col("snippet.defaultLanguage").alias("defaultLanguage"),
        col("snippet.defaultAudioLanguage").alias("defaultAudioLanguage"),
    ).distinct()  # Remover duplicatas
    
    # Escrever tabela Silver
    videos_silver.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{silver_schema}.youtube_languages")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


def transform_categories():
    categories_df = spark.table(f"{bronze_schema}.youtube_categories")
    
    categories_silver = categories_df.select(
        col("id").cast(IntegerType()).alias("category_id"),
        trim(col("snippet.title")).alias("category_title")  # Normalizar texto
    ).distinct()  # Garantir unicidade
    
    categories_silver.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{silver_schema}.youtube_categories")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import from_json, col, trim, coalesce, lit, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

def transform_comments():
    comments_df = spark.table(f"{bronze_schema}.youtube_comments")
    
    # Definir schema do JSON contido em 'snippet.topLevelComment'
    top_level_comment_schema = StructType([
        StructField("snippet", StructType([
            StructField("videoId", StringType()),
            StructField("textDisplay", StringType()),
            StructField("authorDisplayName", StringType()),
            StructField("publishedAt", StringType())
        ]))
    ])
    
    # Parse do JSON string para struct
    comments_parsed = comments_df.withColumn(
        "topLevelComment_parsed",
        from_json(col("snippet.topLevelComment"), top_level_comment_schema)
    )
    
    # Extrair campos do struct parseado
    comments_silver = comments_parsed.select(
        col("id").alias("comment_id"),
        col("topLevelComment_parsed.snippet.videoId").alias("video_id"),
        trim(col("topLevelComment_parsed.snippet.textDisplay")).alias("comment_text"),
        coalesce(trim(col("topLevelComment_parsed.snippet.authorDisplayName")), lit("Anônimo")).alias("author"),
        to_timestamp(col("topLevelComment_parsed.snippet.publishedAt")).alias("published_at"),
        col("extracted_at")
    ).dropDuplicates(["comment_id"])  # Remover duplicatas
    
    comments_silver.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{silver_schema}.youtube_comments")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


def transform_channels():
    channels_df = spark.table(f"{bronze_schema}.youtube_channels")
    
    channels_silver = channels_df.select(
        col("id").alias("channel_id"),
        trim(col("snippet.title")).alias("channel_title"),
        coalesce(col("statistics.subscriberCount").cast(IntegerType()), lit(0)).alias("subscribers"),
        coalesce(col("statistics.videoCount").cast(IntegerType()), lit(0)).alias("total_videos"),
        col("extracted_at")
    ).dropDuplicates(["channel_id"])  # Canais únicos
    
    channels_silver.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{silver_schema}.youtube_channels")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def main():
    transform_videos()
    transform_thumbnails()
    transform_tags()
    transform_languages()
    transform_categories()
    transform_comments()
    transform_channels()

    print("Camada Silver criada com sucesso!")

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
