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

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, coalesce, lit, to_timestamp, year, month, dayofmonth, length, datediff
from pyspark.sql.types import IntegerType, FloatType

spark = SparkSession.builder.appName("YouTubeGoldLayer").getOrCreate()

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

def create_dim_video():
    videos_silver = spark.table(f"{silver_schema}.youtube_videos")
    
    dim_video = videos_silver.select(
        col("id").alias("video_id"),
        col("title"),
        col("duration"),
        col("category_id"),
        col("channel_id"),
        col("published_at"),
        col("definition"),
        col("caption"),
        col("licensedContent")
    ).distinct()
    
    dim_video.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{gold_schema}.dim_video")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_dim_channel():
    channels_silver = spark.table(f"{silver_schema}.youtube_channels")
    
    dim_channel = channels_silver.select(
        col("channel_id"),
        col("channel_title"),
        col("subscribers"),
        col("total_videos")
    ).distinct()
    
    dim_channel.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{gold_schema}.dim_channel")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_dim_category():
    categories_silver = spark.table(f"{silver_schema}.youtube_categories")
    
    dim_category = categories_silver.select(
        col("category_id"),
        col("category_title")
    ).distinct()
    
    dim_category.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{gold_schema}.dim_category")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_dim_date():
    # Extrair datas únicas de published_at e extracted_at
    videos = spark.table(f"{silver_schema}.youtube_videos")
    comments = spark.table(f"{silver_schema}.youtube_comments")
    
    dates_videos = videos.select(to_timestamp(col("published_at")).alias("date"))
    dates_comments = comments.select(to_timestamp(col("published_at")).alias("date"))
    
    all_dates = dates_videos.union(dates_comments).distinct()
    
    dim_date = all_dates.select(
        year("date").alias("year"),
        month("date").alias("month"),
        dayofmonth("date").alias("day"),
        col("date").alias("full_date")
    ).distinct()
    
    dim_date.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{gold_schema}.dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_fact_video_performance():
    videos = spark.table(f"{silver_schema}.youtube_videos")
    
    fact_video = videos.select(
        col("id").alias("video_id"),
        col("channel_id"),
        col("category_id"),
        year(to_timestamp(col("published_at"))).alias("year"),
        month(to_timestamp(col("published_at"))).alias("month"),
        col("views"),
        col("likes"),
        col("comments"),
        col("favorites"),
        col("duration"),
        ((col("likes") + col("comments") + col("favorites")).cast(FloatType()) / col("views")).alias("engagement_rate")
    ).filter(col("views") > 0)  # Evitar divisão por zero
    
    fact_video.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{gold_schema}.fact_video_performance")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_fact_channel_growth():
    channels = spark.table(f"{silver_schema}.youtube_channels")
    
    fact_channel = channels.select(
        col("channel_id"),
        col("subscribers"),
        col("total_videos")
    )
    
    fact_channel.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{gold_schema}.fact_channel_growth")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_fact_comment_analysis():
    comments = spark.table(f"{silver_schema}.youtube_comments")
    videos = spark.table(f"{silver_schema}.youtube_videos").select("id", "published_at")

    # Join para obter a data de publicação do vídeo
    comments_with_video_date = comments.join(
        videos,
        comments.video_id == videos.id,
        "left"
    ).select(
        comments["*"],
        videos["published_at"].alias("video_published_at")
    )

    # Calcular métricas por comentário
    fact_comment = comments_with_video_date.select(
        col("comment_id"),
        col("video_id"),
        col("author"),
        col("comment_text"),
        length(col("comment_text")).alias("comment_length"),  # Comprimento do texto
        datediff(to_timestamp(col("published_at")), to_timestamp(col("video_published_at"))).alias("days_after_video_publish"),  # Tempo após publicação
        to_timestamp(col("published_at")).alias("comment_date")
    ).distinct()

    # Escrever tabela fato
    fact_comment.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{gold_schema}.fact_comment_analysis")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def main():
    # Criar dimensões
    create_dim_video()
    create_dim_channel()
    create_dim_category()
    create_dim_date()
    
    # Criar fatos
    create_fact_video_performance()
    create_fact_channel_growth()
    create_fact_comment_analysis()
    
    print("Camada Gold criada com sucesso!")

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
