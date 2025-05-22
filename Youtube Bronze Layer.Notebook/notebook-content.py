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
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import (StructType, StructField, StringType, BooleanType,LongType, TimestampType, IntegerType, MapType, ArrayType)
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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Schemas

# CELL ********************

VIDEO_CATEGORIES_SCHEMA = StructType([
    StructField("kind", StringType(), True),
    StructField("etag", StringType(), True),
    StructField("id", StringType(), True),
    StructField("snippet", StructType([
        StructField("title", StringType(), True),
        StructField("assignable", StringType(), True),  # normalmente é "true" ou "false" como string
        StructField("channel_id", StringType(), True)  # normalmente é "true" ou "false" como string
    ]), True),
    StructField("extracted_at", StringType(), True),
    StructField("data_source", StringType(), True)
])

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
    ]), True),
    StructField("extracted_at", StringType(), True),
    StructField("data_source", StringType(), True)
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
    ])),
    StructField("extracted_at", StringType(), True),
    StructField("data_source", StringType(), True)
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
    ])),
    StructField("extracted_at", StringType(), True),
    StructField("data_source", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Functions

# CELL ********************

def create_delta_table(raw_path, table_name, schema):
    df = spark.read.schema(schema).json(raw_path)
    
    # Escrever com tratamento de schema
    df.write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .saveAsTable(f"Lakehouse.bronze.{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def main():

    raw_path       = "Files/raw/youtube"
    raw_categories = f"{raw_path}/categories"
    raw_videos     = f"{raw_path}/videos"
    raw_comments   = f"{raw_path}/comments"
    raw_channels   = f"{raw_path}/channels"

    table_categories = "youtube_categories"
    table_videos     = "youtube_videos"
    table_comments   = "youtube_comments"
    table_channels   = "youtube_channels"

    create_delta_table(raw_categories, table_categories, VIDEO_CATEGORIES_SCHEMA)
    create_delta_table(raw_videos    , table_videos    , VIDEOS_SCHEMA)
    create_delta_table(raw_comments  , table_comments  , COMMENTS_SCHEMA)
    create_delta_table(raw_channels  , table_channels  , CHANNELS_SCHEMA)

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
