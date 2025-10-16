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

from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, col
import pandas as pd
from transformers import pipeline
import mlflow

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_comments = spark.read.format("delta").load("Tables/silver/youtube_comments")

df_comments.select("comment_id", "comment_text").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sentiment_model = pipeline(
    "sentiment-analysis",
    model="nlptown/bert-base-multilingual-uncased-sentiment",  # ou "pysentimiento/bertweet-sentiment"
    tokenizer="nlptown/bert-base-multilingual-uncased-sentiment"
)

model1 = pipeline("sentiment-analysis", model="pysentimiento/bert-base-portuguese-sentiment")
model2 = pipeline("sentiment-analysis", model="pierreguillou/bert-base-cased-sentiment-br")
model3 = pipeline("sentiment-analysis", model="pysentimiento/bertweet-sentiment")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema = StructType([
    StructField("sentiment", StringType()),
    StructField("confidence", DoubleType()),
    StructField("stars", IntegerType()) # Nova coluna para a nota de 1 a 5
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

@pandas_udf(schema)
def sentiment_udf(comments: pd.Series) -> pd.DataFrame:

    results = sentiment_model(comments.tolist(), truncation=True, max_length=512)

    labels = []
    scores = []
    stars_list = [] # Lista para armazenar as notas

    for r in results:
        # A label vem como '1 star', '2 stars', etc.
        stars = int(r["label"][0])
        score = r["score"]

        # Mapeamento para positive / neutral / negative
        if stars <= 2:
            sentiment_class = "negative"
        elif stars == 3:
            sentiment_class = "neutral"
        else:
            sentiment_class = "positive"

        labels.append(sentiment_class)
        scores.append(score)
        stars_list.append(stars) # Adiciona a nota à lista

    # Retorna o DataFrame com a nova coluna
    return pd.DataFrame({"sentiment": labels, "confidence": scores, "stars": stars_list})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Análise de sentimento para cada comentário
df_sentiments = df_comments.withColumn("sentiment_struct", sentiment_udf(col("comment_text")))

# --- ATUALIZADO: Extraindo a nova coluna 'stars' ---
df_sentiments = df_sentiments.select(
    "*",
    col("sentiment_struct.sentiment").alias("sentiment"),
    col("sentiment_struct.confidence").alias("confidence"),
    col("sentiment_struct.stars").alias("sentiment_score") # Renomeando 'stars' para 'sentiment_score'
).drop("sentiment_struct")

# Salva a tabela de sentimentos dos comentários (dim_comments_sentiment)
output_path_comments = "Tables/gold/dim_comments_sentiment"
df_sentiments.write.format("delta").mode("overwrite").save(output_path_comments)
print("Análise de sentimentos dos comentários concluída e salva em:", output_path_comments)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- ATUALIZADO: Agregação pela MÉDIA DA NOTA (SCORE) ---
df_video_sentiments = df_sentiments.groupBy("video_id") \
    .agg(avg("sentiment_score").alias("avg_sentiment_score"))

# Salva a tabela de sentimentos dos vídeos
output_path_videos = "Tables/gold/dim_videos_sentiment"
df_video_sentiments.write.format("delta").mode("overwrite").save(output_path_videos)
print("Análise de sentimentos por vídeo concluída e salva em:", output_path_videos)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- ATUALIZADO: Agregação pela MÉDIA DA NOTA (SCORE) ---
df_channel_data = df_sentiments.join(df_videos.select("video_id", "channel_id"), "video_id")

df_channel_sentiments = df_channel_data.groupBy("channel_id") \
    .agg(avg("sentiment_score").alias("avg_sentiment_score"))

# Salva a tabela de sentimentos dos canais
output_path_channels = "Tables/gold/dim_channels_sentiment"
df_channel_sentiments.write.format("delta").mode("overwrite").save(output_path_channels)
print("Análise de sentimentos por canal concluída e salva em:", output_path_channels)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
