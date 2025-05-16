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
# META       "default_warehouse": "db9babcf-bfc6-4a84-963e-bec2c7ab93f1",
# META       "known_warehouses": [
# META         {
# META           "id": "db9babcf-bfc6-4a84-963e-bec2c7ab93f1",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
    LongType, TimestampType, IntegerType, MapType, ArrayType
)
import re

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark = SparkSession.builder.appName("YouTubeSilverLayer").getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Paths

# CELL ********************


lakehouse_name = "Lakehouse"
bronze_schema = f"{lakehouse_name}.bronze"
silver_schema = f"{lakehouse_name}.silver"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Functions

# MARKDOWN ********************

# ### Auxiliar

# CELL ********************

# Define Function
def parse_iso8601_duration(duration: str) -> int:
    
    if duration is None:
        return None

    pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = pattern.match(duration)

    if not match:
        return None

    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0

    total_seconds = hours * 3600 + minutes * 60 + seconds
    return total_seconds

# Register UDF with IntegerType return
parse_duration_udf = F.udf(parse_iso8601_duration, IntegerType())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define Function
def split_keywords(text):
    if text:
        return re.findall(r'"[^"]+"|\S+', text)
    return []

# Define Function as UDF
split_keywords_udf = F.udf(split_keywords, ArrayType(StringType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def save_table(df, schema: str, table_name: str) -> None:
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{schema}.{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Transform Data

# CELL ********************


def transform_videos(df): 
    
    transformed_df = df.select(
        F.col("id").cast(StringType())                                                                                                    .alias("id"),
        F.coalesce(F.to_timestamp(F.col("snippet.publishedAt"), "yyyy-MM-dd'T'HH:mm:ss'Z'"), F.to_timestamp(F.lit("1900-01-01 00:00:00"))).alias("published_at"),
        F.coalesce(F.trim(F.col("snippet.title")).cast(StringType()), F.lit("N/A"))                                                       .alias("title"),
        F.coalesce(F.col("snippet.description").cast(StringType()), F.lit("N/A"))                                                         .alias("description"),
        F.coalesce(F.col("snippet.channelId").cast(StringType()), F.lit(None))                                                            .alias("channel_id"),
        F.coalesce(F.col("snippet.categoryId").cast(IntegerType()), F.lit(None))                                                          .alias("category_id"),
        F.coalesce(F.col("statistics.viewCount").cast(IntegerType()), F.lit(0))                                                           .alias("views"),
        F.coalesce(F.col("statistics.likeCount").cast(IntegerType()), F.lit(0))                                                           .alias("likes"),
        F.coalesce(F.col("statistics.favoriteCount").cast(IntegerType()), F.lit(0))                                                       .alias("favorites"),
        F.coalesce(F.col("statistics.commentCount").cast(IntegerType()), F.lit(0))                                                        .alias("comments"),
        F.coalesce(parse_duration_udf(F.col("contentDetails.duration")), F.lit(0))                                                        .alias("duration_seconds"),
        F.coalesce(F.col("contentDetails.dimension").cast(StringType()), F.lit("N/A"))                                                    .alias("dimension"),
        F.coalesce(F.col("contentDetails.definition").cast(StringType()), F.lit("N/A"))                                                   .alias("definition"),
        F.coalesce(F.col("contentDetails.caption").cast(BooleanType()), F.lit(None))                                                      .alias("caption"),
        F.coalesce(F.col("contentDetails.licensedContent").cast(BooleanType()), F.lit(None))                                              .alias("licensedContent"),
        F.coalesce(F.col("contentDetails.projection").cast(StringType()), F.lit("N/A"))                                                   .alias("projection"),
        F.coalesce(F.col("data_source").cast(StringType()), F.lit("N/A"))                                                                 .alias("data_source"),
        F.coalesce(F.col("extracted_at").cast(TimestampType()), F.to_timestamp(F.lit("1900-01-01 00:00:00")))                             .alias("extracted_at"),
        F.current_timestamp().cast(TimestampType())                                                                                       .alias("ingestion_timestamp")
    ).dropDuplicates(["id"])
    
    return transformed_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_thumbnails(df):

    transformed_df = df.select(
        F.col("id").alias("video_id"),
        F.coalesce(F.col("snippet.thumbnails.default.url").cast(StringType()), F.lit("N/A"))                 .alias("default_url"),
        F.coalesce(F.col("snippet.thumbnails.default.width").cast(StringType()), F.lit("N/A"))               .alias("default_width"),
        F.coalesce(F.col("snippet.thumbnails.default.height").cast(StringType()), F.lit("N/A"))              .alias("default_height"),
        F.coalesce(F.col("snippet.thumbnails.medium.url").cast(StringType()), F.lit("N/A"))                  .alias("medium_url"),
        F.coalesce(F.col("snippet.thumbnails.medium.width").cast(StringType()), F.lit("N/A"))                .alias("medium_width"),
        F.coalesce(F.col("snippet.thumbnails.medium.height").cast(StringType()), F.lit("N/A"))               .alias("medium_height"),
        F.coalesce(F.col("snippet.thumbnails.high.url").cast(StringType()), F.lit("N/A"))                    .alias("high_url"),
        F.coalesce(F.col("snippet.thumbnails.high.width").cast(StringType()), F.lit("N/A"))                  .alias("high_width"),
        F.coalesce(F.col("snippet.thumbnails.high.height").cast(StringType()), F.lit("N/A"))                 .alias("high_height"),
        F.coalesce(F.col("snippet.thumbnails.standard.url").cast(StringType()), F.lit("N/A"))                .alias("standard_url"),
        F.coalesce(F.col("snippet.thumbnails.standard.width").cast(StringType()), F.lit("N/A"))              .alias("standard_width"),
        F.coalesce(F.col("snippet.thumbnails.standard.height").cast(StringType()), F.lit("N/A"))             .alias("standard_height"),
        F.coalesce(F.col("snippet.thumbnails.maxres.url").cast(StringType()), F.lit("N/A"))                  .alias("maxres_url"),
        F.coalesce(F.col("snippet.thumbnails.maxres.width").cast(StringType()), F.lit("N/A"))                .alias("maxres_width"), 
        F.coalesce(F.col("snippet.thumbnails.maxres.height").cast(StringType()), F.lit("N/A"))               .alias("maxres_height"),
        F.coalesce(F.col("data_source").cast(StringType()), F.lit("N/A"))                                    .alias("data_source"), 
        F.coalesce(F.col("extracted_at").cast(TimestampType()), F.to_timestamp(F.lit("1900-01-01 00:00:00"))).alias("extracted_at"),
        F.current_timestamp().cast(TimestampType())                                                          .alias("ingestion_timestamp")
    ).dropDuplicates(["video_id"])
    
    return transformed_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_tags(df):
    
    transformed_df = df.select(
        F.col("id")                                                                                          .alias("video_id"),
        F.col("snippet.tags")                                                                                .alias("tag"),
        F.coalesce(F.col("data_source").cast(StringType()), F.lit("N/A"))                                    .alias("data_source"),
        F.coalesce(F.col("extracted_at").cast(TimestampType()), F.to_timestamp(F.lit("1900-01-01 00:00:00"))).alias("extracted_at"),
        F.current_timestamp().cast(TimestampType())                                                          .alias("ingestion_timestamp")
    ).dropDuplicates()
    
    return transformed_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_languages(df):
    
    transformed_df = df.select(
        F.col("id").cast(StringType())                                                                       .alias("video_id"),
        F.coalesce(F.col("snippet.defaultAudioLanguage").cast(StringType()), F.lit("N/A"))                   .alias("default_audio_language"),
        F.coalesce(F.col("data_source").cast(StringType()), F.lit("N/A"))                                    .alias("data_source"),
        F.coalesce(F.col("extracted_at").cast(TimestampType()), F.to_timestamp(F.lit("1900-01-01 00:00:00"))).alias("extracted_at"),
        F.current_timestamp().cast(TimestampType())                                                          .alias("ingestion_timestamp")
    ).dropDuplicates()

    return transformed_df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_categories(df):
    
    transformed_df = df.select(
        F.col("id").cast(IntegerType())                                                                      .alias("id"),
        F.coalesce(F.trim(F.col("snippet.title")).cast(StringType()), F.lit("N/A"))                          .alias("category"),
        F.coalesce(F.col("data_source").cast(StringType()), F.lit("N/A"))                                    .alias("data_source"),
        F.coalesce(F.col("extracted_at").cast(TimestampType()), F.to_timestamp(F.lit("1900-01-01 00:00:00"))).alias("extracted_at"),
        F.current_timestamp().cast(TimestampType())                                                          .alias("ingestion_timestamp")
    ).dropDuplicates(["id"])

    return transformed_df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_comments(df):

    transformed_df = df.select(
        F.col("id").cast(StringType())                                                                                                .alias("comment_id"),
        F.col("snippet.videoId").cast(StringType())                                                                                   .alias("video_id"),
        F.coalesce(F.col("snippet.topLevelComment.snippet.authorDisplayName").cast(StringType()), F.lit("N/A"))                       .alias("author_display_name"),
        F.coalesce(F.col("snippet.topLevelComment.snippet.authorProfileImageUrl").cast(StringType()), F.lit("N/A"))                   .alias("author_profile_image_url"),
        F.coalesce(F.col("snippet.topLevelComment.snippet.authorChannelUrl").cast(StringType()), F.lit("N/A"))                        .alias("author_channel_url"),
        F.coalesce(F.col("snippet.topLevelComment.snippet.authorChannelId.value").cast(StringType()), F.lit("N/A"))                   .alias("author_channel_id"),
        F.coalesce(F.col("snippet.topLevelComment.snippet.channelId").cast(StringType()), F.lit("N/A"))                               .alias("video_channel_id"),
        F.coalesce(F.trim(F.col("snippet.topLevelComment.snippet.textDisplay")).cast(StringType()), F.lit("N/A"))                     .alias("comment_text"),
        F.coalesce(F.trim(F.col("snippet.topLevelComment.snippet.textOriginal")).cast(StringType()), F.lit("N/A"))                    .alias("comment_text_original"),
        F.coalesce(F.col("snippet.topLevelComment.snippet.canRate").cast(BooleanType()), F.lit(None))                                 .alias("can_rate"),
        F.coalesce(F.col("snippet.topLevelComment.snippet.viewerRating").cast(StringType()), F.lit("N/A"))                            .alias("viewer_rating"),
        F.coalesce(F.col("snippet.topLevelComment.snippet.likeCount").cast(IntegerType()), F.lit(0))                                  .alias("likes"),
        F.coalesce(F.to_timestamp(F.col("snippet.topLevelComment.snippet.publishedAt")), F.to_timestamp(F.lit("1900-01-01 00:00:00"))).alias("published_at"),
        F.coalesce(F.to_timestamp(F.col("snippet.topLevelComment.snippet.updatedAt")), F.to_timestamp(F.lit("1900-01-01 00:00:00")))  .alias("updated_at"),
        F.coalesce(F.col("data_source").cast(StringType()), F.lit("N/A"))                                                             .alias("data_source"),
        F.coalesce(F.col("extracted_at").cast(TimestampType()), F.to_timestamp(F.lit("1900-01-01 00:00:00")))                         .alias("extracted_at"),
        F.current_timestamp().cast(TimestampType())                                                                                   .alias("ingestion_timestamp")
    ).dropDuplicates(["comment_id"])
    
    return transformed_df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_channels(df):

    transformed_df = df.select(
        F.coalesce(F.col("id").cast(StringType()), F.lit("N/A")).alias("channel_id"),
        F.coalesce(F.trim(F.col("snippet.title")).cast(StringType()), F.lit("N/A")).alias("channel_title"),
        F.coalesce(F.trim(F.col("snippet.description")).cast(StringType()), F.lit("N/A")).alias("channel_description"),
        F.coalesce(F.col("snippet.customUrl").cast(StringType()), F.lit("N/A")).alias("custom_url"),
        F.coalesce(F.to_timestamp(F.col("snippet.publishedAt")), F.to_timestamp(F.lit("1900-01-01 00:00:00"))).alias("published_at"),
        F.coalesce(F.col("snippet.thumbnails.default.url").cast(StringType()), F.lit("N/A")).alias("thumbnail_default_url"),
        F.coalesce(F.col("snippet.thumbnails.medium.url").cast(StringType()), F.lit("N/A")).alias("thumbnail_medium_url"),
        F.coalesce(F.col("snippet.thumbnails.high.url").cast(StringType()), F.lit("N/A")).alias("thumbnail_high_url"),
        F.coalesce(F.col("snippet.localized.title").cast(StringType()), F.lit("N/A")).alias("localized_title"),
        F.coalesce(F.col("snippet.localized.description").cast(StringType()), F.lit("N/A")).alias("localized_description"),
        F.coalesce(F.col("snippet.country").cast(StringType()), F.lit("N/A")).alias("channel_country"),
        F.coalesce(F.col("statistics.viewCount").cast(IntegerType()), F.lit(0)).alias("views"),
        F.coalesce(F.col("statistics.subscriberCount").cast(IntegerType()), F.lit(0)).alias("subscribers"),
        F.coalesce(F.col("statistics.videoCount").cast(IntegerType()), F.lit(0)).alias("videos"),
        F.coalesce(F.col("statistics.hiddenSubscriberCount").cast(BooleanType()), F.lit(False)).alias("hidden_subscribers"),
        F.coalesce(F.col("status.privacyStatus").cast(StringType()), F.lit("N/A")).alias("privacy_status"),
        F.coalesce(F.col("status.isLinked").cast(BooleanType()), F.lit(None)).alias("is_linked"),
        F.coalesce(F.col("status.longUploadsStatus").cast(StringType()), F.lit("N/A")).alias("long_uploads_status"),
        F.coalesce(F.col("status.madeForKids").cast(BooleanType()), F.lit(None)).alias("is_made_for_kids"),
        F.coalesce(F.col("data_source").cast(StringType()), F.lit("N/A")).alias("data_source"),
        F.coalesce(F.col("extracted_at").cast(TimestampType()), F.to_timestamp(F.lit("1900-01-01 00:00:00"))).alias("extracted_at"),
        F.current_timestamp().cast(TimestampType()).alias("ingestion_timestamp")
    ).dropDuplicates(["channel_id"])

    return transformed_df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_channels_branding(df):

    transformed_df = df.select(
        F.col("id").cast(StringType())                                                                       .alias("channel_id"),
        F.coalesce(F.col("snippet.thumbnails.default.url").cast(StringType()), F.lit("N/A"))                 .alias("icon_default_url"),
        F.coalesce(F.col("snippet.thumbnails.default.width").cast(StringType()), F.lit("N/A"))               .alias("icon_default_width"),
        F.coalesce(F.col("snippet.thumbnails.default.height").cast(StringType()), F.lit("N/A"))              .alias("icon_default_height"),
        F.coalesce(F.col("snippet.thumbnails.medium.url").cast(StringType()), F.lit("N/A"))                  .alias("icon_medium_url"),
        F.coalesce(F.col("snippet.thumbnails.medium.width").cast(StringType()), F.lit("N/A"))                .alias("icon_medium_width"),
        F.coalesce(F.col("snippet.thumbnails.medium.height").cast(StringType()), F.lit("N/A"))               .alias("icon_medium_height"),
        F.coalesce(F.col("snippet.thumbnails.high.url").cast(StringType()), F.lit("N/A"))                    .alias("icon_high_url"),
        F.coalesce(F.col("snippet.thumbnails.high.width").cast(StringType()), F.lit("N/A"))                  .alias("icon_high_width"),
        F.coalesce(F.col("snippet.thumbnails.high.height").cast(StringType()), F.lit("N/A"))                 .alias("icon_high_height"),
        F.coalesce(F.col("brandingSettings.image.bannerExternalUrl").cast(StringType()), F.lit("N/A"))       .alias("banner_url"),
        F.coalesce(F.col("brandingSettings.channel.title").cast(StringType()), F.lit("N/A"))                 .alias("channel_title"),
        F.coalesce(F.col("brandingSettings.channel.description").cast(StringType()), F.lit("N/A"))           .alias("channel_description"),
        F.coalesce(F.col("brandingSettings.channel.keywords").cast(StringType()), F.lit("N/A"))              .alias("channel_keywords"),
        F.coalesce(F.col("brandingSettings.channel.country").cast(StringType()), F.lit("N/A"))               .alias("channel_country"),
        F.coalesce(F.col("data_source").cast(StringType()), F.lit("N/A"))                                    .alias("data_source"),
        F.coalesce(F.col("extracted_at").cast(TimestampType()), F.to_timestamp(F.lit("1900-01-01 00:00:00"))).alias("extracted_at"),
        F.current_timestamp().cast(TimestampType())                                                          .alias("ingestion_timestamp")
    ).dropDuplicates(["channel_id"])

    return transformed_df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_channels_keywords(df):

    transformed_df = df.select(
        F.col("id").cast(StringType()).alias("channel_id"),
        F.coalesce(F.col("brandingSettings.channel.keywords").cast(StringType()), F.lit(""))                 .alias("branding_keywords"),
        F.coalesce(F.col("data_source").cast(StringType()), F.lit("N/A"))                                    .alias("data_source"),
        F.coalesce(F.col("extracted_at").cast(TimestampType()), F.to_timestamp(F.lit("1900-01-01 00:00:00"))).alias("extracted_at"),
        F.current_timestamp().cast(TimestampType())                                                          .alias("ingestion_timestamp")
    ).filter(F.col("branding_keywords") != "")

    transformed_exploded_df = transformed_df.withColumn("keyword", F.explode(split_keywords_udf(F.col("branding_keywords")))) \
        .withColumn("keyword", F.trim(F.regexp_replace(F.col("keyword"), r'^"|"$', ''))) \
        .drop("branding_keywords")

    return transformed_exploded_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def main():
    # Bronze layer tables paths
    bronze_table_videos     = f"{bronze_schema}.youtube_videos"
    bronze_table_categories = f"{bronze_schema}.youtube_categories"
    bronze_table_comments   = f"{bronze_schema}.youtube_comments"
    bronze_table_channels   = f"{bronze_schema}.youtube_channels"

    # Reading bronze layer tables into dataframes
    bronze_videos_df     = spark.table(bronze_table_videos)
    bronze_categories_df = spark.table(bronze_table_categories)
    bronze_comments_df   = spark.table(bronze_table_comments)
    bronze_channels_df   = spark.table(bronze_table_channels)

    # Transformating bronze layer tables to silver layer tables and writing it on dataframes
    silver_videos_df            = transform_videos(bronze_videos_df)
    silver_thumbnails_df        = transform_thumbnails(bronze_videos_df)
    silver_tags_df              = transform_tags(bronze_videos_df)
    silver_languages_df         = transform_languages(bronze_videos_df)
    silver_categories_df        = transform_categories(bronze_categories_df)
    silver_comments_df          = transform_comments(bronze_comments_df)
    silver_channels_df          = transform_channels(bronze_channels_df)
    silver_channels_branding_df = transform_channels_branding(bronze_channels_df)
    silver_channels_keywords_df = transform_channels_keywords(bronze_channels_df)

    # Defining silver layer tables
    videos_table            = "youtube_videos"
    thumbnails_table        = "youtube_thumbnails"
    tags_table              = "youtube_tags"
    languages_table         = "youtube_languages"
    categories_table        = "youtube_categories"
    comments_table          = "youtube_comments"
    channels_table          = "youtube_channels"
    channels_branding_table = "youtube_channels_branding"
    channels_keywords_table = "youtube_channels_keywords"

    # Saving tables into silver schema
    save_table(silver_videos_df, silver_schema, videos_table)
    save_table(silver_thumbnails_df, silver_schema, thumbnails_table)
    save_table(silver_tags_df, silver_schema, tags_table)
    save_table(silver_languages_df, silver_schema, languages_table)
    save_table(silver_categories_df, silver_schema, categories_table)
    save_table(silver_comments_df, silver_schema, comments_table)
    save_table(silver_channels_df, silver_schema, channels_table)
    save_table(silver_channels_branding_df, silver_schema, channels_branding_table)
    save_table(silver_channels_keywords_df, silver_schema, channels_keywords_table)

    print("✅ Camada Silver criada com sucesso!")


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
