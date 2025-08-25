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

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, FloatType, DateType
)
import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark = SparkSession.builder.appName("YouTubeGoldLayer").getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Functions

# MARKDOWN ********************

# ### Create Dim Tables

# CELL ********************

def create_dim_date(spark, start_date="2020-01-01", days_ahead=365):
    """
    Cria uma tabela de dimensão de data de forma dinâmica.
    
    Parâmetros:
        spark (SparkSession): Sessão Spark ativa.
        start_date (str): Data inicial no formato "YYYY-MM-DD".
        days_ahead (int): Quantos dias no futuro incluir a partir de hoje.
    
    Retorna:
        DataFrame com colunas da dimensão de data.
    """
    start = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.date.today() + datetime.timedelta(days=days_ahead)
    num_days = (end - start).days + 1

    df = df = spark.range(0, num_days).toDF("offset") \
        .withColumn("date", F.expr(f"date_add('{start}', cast(offset as integer))")) \
        .withColumn("year", F.year("date")) \
        .withColumn("quarter", F.quarter("date")) \
        .withColumn("month", F.month("date")) \
        .withColumn("day", F.dayofmonth("date")) \
        .withColumn("week", F.weekofyear("date")) \
        .withColumn("day_of_week", F.date_format("date", "EEEE")) \
        .withColumn("day_name_short", F.date_format("date", "E")) \
        .withColumn("month_name", F.date_format("date", "MMMM")) \
        .withColumn("month_name_short", F.date_format("date", "MMM")) \
        .withColumn("is_weekend", F.when(F.col("day_of_week").isin("Saturday", "Sunday"), True).otherwise(False)) \
        .drop("offset")
    
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_dim_video(videos_df, thumbnails_df):

    """
    Cria a dimensão de vídeos com atributos descritivos do vídeo do YouTube,
    incluindo dados de thumbnails.

    Parâmetros:
        videos_df (DataFrame): Tabela silver de vídeos.
        thumbnails_df (DataFrame): Tabela de thumbnails dos vídeos.

    Retorna:
        DataFrame da tabela dimensão de vídeos enriquecida com thumbnails.
    """
    
    dim_df = videos_df.alias("v") \
        .join(thumbnails_df.alias("t"), F.col("v.id") == F.col("t.video_id"), "left") \
        .withColumn("video_url", F.concat(F.lit("https://www.youtube.com/watch?v="), F.col("v.id"))) \
        .select(
            F.col("v.id").alias("video_id"),
            "v.title",
            "v.description",
            "v.channel_id",
            "v.category_id",
            "v.definition",
            "v.dimension",
            "v.projection",
            "v.caption",
            "v.licensed_content",
            "v.duration_seconds",
            "v.published_at",
            "video_url",
            "t.default_url", 
            "t.default_width", 
            "t.default_height",
            "t.medium_url", 
            "t.medium_width", 
            "t.medium_height",
            "t.high_url", 
            "t.high_width", 
            "t.high_height",
            "t.standard_url", 
            "t.standard_width", 
            "t.standard_height",
            "t.maxres_url", 
            "t.maxres_width", 
            "t.maxres_height"
        )

    return dim_df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_dim_channel(channels_df, channels_branding_df):

    """
    Cria a dimensão de canais com atributos descritivos e dados de branding dos canais do YouTube.

    Parâmetros:
        channels_df (DataFrame): Tabela silver de canais.
        channels_branding_df (DataFrame): Tabela com dados de branding dos canais.

    Retorna:
        DataFrame da tabela dimensão de canais enriquecida.
    """
    
    dim_df = channels_df.alias("c") \
        .join(channels_branding_df.alias("b"), F.col("c.channel_id") == F.col("b.channel_id"), "left") \
        .withColumn("channel_url", F.concat(F.lit("https://www.youtube.com/channel/"), F.col("c.channel_id"))) \
        .select(
            F.col("c.channel_id"),
            "c.channel_title",
            "c.channel_description",
            "channel_url",
            "c.custom_url",
            "c.published_at",
            "c.channel_country",
            "c.views",
            "c.subscribers",
            "c.videos",
            "c.hidden_subscribers",
            "c.privacy_status",
            "c.is_linked",
            "c.long_uploads_status",
            "c.is_made_for_kids",
            "b.icon_default_url",
            "b.icon_medium_url",
            "b.icon_high_url",
            "b.banner_url",
            "b.channel_keywords"
        )

    return dim_df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_dim_comment(comments_df):
    
    """
    Cria a tabela de dimensão de comentários do YouTube.

    Parâmetros:
        comments_df (DataFrame): Tabela silver de comentários.

    Retorna:
        DataFrame da dimensão de comentários.
    """
    
    dim_df = comments_df.select(
        F.col("comment_id"),
        F.col("video_id"),
        F.col("author_display_name"),
        F.col("author_profile_image_url"),
        F.col("author_channel_url"),
        F.col("author_channel_id"),
        F.col("comment_text"),
        F.col("comment_text_original"),
        F.col("can_rate"),
        F.col("viewer_rating"),
        F.col("likes"),
        F.col("published_at").alias("comment_published_at"),
        F.col("updated_at").alias("comment_updated_at")
    ).dropDuplicates(["comment_id"]) 
    
    return dim_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_dim_tag(tags_df):
    dim_tag_df = tags_df \
        .select("tag") \
        .dropna() \
        .dropDuplicates() \
        .withColumn("tag_id", F.sha2(F.col("tag"), 256)) \
        .select("tag_id", "tag")
    
    return dim_tag_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_dim_channels_keywords(channels_keywords_df):

    """
    Cria a dimensão de palavras-chave associadas aos canais do YouTube.

    Parâmetros:
        channels_keywords_df (DataFrame): Tabela silver com keywords dos canais.

    Retorna:
        DataFrame da dimensão de palavras-chave por canal.
    """
    
    dim_df = channels_keywords_df.select(
        "channel_id",
        "keyword"
    ).dropDuplicates(["channel_id", "keyword"])

    return dim_df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_dim_categories(categories_df):

    """
    Cria a dimensão de categorias de vídeos do YouTube.

    Parâmetros:
        categories_df (DataFrame): Tabela silver com categorias de vídeos.

    Retorna:
        DataFrame da dimensão de categorias.
    """
    
    dim_df = categories_df.select(
        F.col("id").alias("category_id"),
        F.col("category").alias("category_name")
    ).dropDuplicates(["category_id"])

    return dim_df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Create Bridges Table

# CELL ********************

def create_bridge_video_tags(tags_df):
    bridge_df = tags_df \
        .dropna(subset=["video_id", "tag"]) \
        .withColumn("tag_id", F.sha2(F.col("tag"), 256)) \
        .select("video_id", "tag_id")
    
    return bridge_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Create Fact Tables

# CELL ********************

def create_fact_video_engagement(videos_df):

    """
    Cria a tabela fato com métricas de performance e engajamento de vídeos do YouTube.

    Parâmetros:
        videos_df (DataFrame): Tabela silver de vídeos.
        dim_date_df (DataFrame): Tabela dim_date com date_id.

    Retorna:
        DataFrame da tabela fato enriquecida com métricas.
    """

    fact_df = videos_df \
        .withColumn("published_date", F.to_date("published_at")) \
        .withColumn("total_engagement", 
                    F.coalesce(F.col("likes"), F.lit(0)) +
                    F.coalesce(F.col("comments"), F.lit(0)) +
                    F.coalesce(F.col("favorites"), F.lit(0))) \
        .withColumn("engagement_rate", 
                    F.when(F.col("views") > 0, F.col("total_engagement") / F.col("views"))
                    .otherwise(F.lit(0.0))) \
        .select(
            F.col("id").alias("video_id"),
            "channel_id",
            "category_id",
            "published_date",
            "views",
            "likes",
            "comments",
            "favorites",
            "duration_seconds",
            "total_engagement",
            "engagement_rate"
        )

    return fact_df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### main

# CELL ********************

def main():

    # Lakehouse's paths
    lakehouse_name = "Lakehouse"
    silver_schema = f"{lakehouse_name}.silver"
    gold_schema = f"{lakehouse_name}.gold"

    # Reading Silver Layer Tables
    categories_df        = spark.table(f"{silver_schema}.youtube_categories")
    channels_df          = spark.table(f"{silver_schema}.youtube_channels")
    channels_branding_df = spark.table(f"{silver_schema}.youtube_channels_branding")
    channels_keywords_df = spark.table(f"{silver_schema}.youtube_channels_keywords")
    comments_df          = spark.table(f"{silver_schema}.youtube_comments")
    tags_df              = spark.table(f"{silver_schema}.youtube_tags")
    thumbnails_df        = spark.table(f"{silver_schema}.youtube_thumbnails")
    videos_df            = spark.table(f"{silver_schema}.youtube_videos")
    
    # Dimension Tables
    dim_date_df             = create_dim_date(spark)
    dim_video_df            = create_dim_video(videos_df, thumbnails_df)
    dim_channel_df          = create_dim_channel(channels_df, channels_branding_df)
    dim_category_df         = create_dim_categories(categories_df)
    dim_tag_df              = create_dim_tag(tags_df)
    dim_comment_df          = create_dim_comment(comments_df)
    dim_channel_keywords_df = create_dim_channels_keywords(channels_keywords_df)
    
    # Bridge Tables
    bridge_video_tag_df = create_bridge_video_tags(tags_df)
    
    # Fact Tables
    fact_video_engagement_df = create_fact_video_engagement(videos_df)
    
    # Writing tables in Silver Layer
    dim_date_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{gold_schema}.dim_date")
    dim_video_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{gold_schema}.dim_video")
    dim_channel_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{gold_schema}.dim_channel")
    dim_category_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{gold_schema}.dim_category")
    dim_tag_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{gold_schema}.dim_tag")
    dim_comment_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{gold_schema}.dim_comment")
    dim_channel_keywords_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{gold_schema}.dim_channel_keywords")
    
    bridge_video_tag_df.write.format("delta").mode("overwrite").saveAsTable(f"{gold_schema}.bridge_video_tag")
    
    fact_video_engagement_df.write.format("delta").mode("overwrite").saveAsTable(f"{gold_schema}.fact_video_engagement")

    print("✅ Camada Gold criada com sucesso!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Execution

# CELL ********************

main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
