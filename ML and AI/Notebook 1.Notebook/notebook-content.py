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
    StructField("confidence", DoubleType())
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

@pandas_udf(schema)
def sentiment_udf(comments: pd.Series) -> pd.DataFrame:
    
    results = sentiment_model(comments.tolist(), truncation=True)
    
    labels = []
    scores = []

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

    return pd.DataFrame({"sentiment": labels, "confidence": scores})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sentiments = df_comments.withColumns({
    "sentiment": sentiment_udf(col("comment_text"))["sentiment"],
    "confidence": sentiment_udf(col("comment_text"))["confidence"]
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

output_path = "Tables/gold/dim_comments_sentiment"

df_sentiments.write.format("delta").mode("overwrite").save(output_path)

print("Análise de sentimentos concluída e salva em:", output_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
