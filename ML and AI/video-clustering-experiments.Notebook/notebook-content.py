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

%pip install sentence-transformers

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, DoubleType
import pandas as pd

from sentence_transformers import SentenceTransformer
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors, VectorUDT

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_videos_table():

    query = """
        SELECT DISTINCT
            v.id,
            c.category,
            v.title,
            v.views,
            (v.likes + v.favorites + v.comments) as engagements,
            v.duration_seconds
        FROM 
            Lakehouse.silver.youtube_videos AS v
            LEFT JOIN Lakehouse.silver.youtube_categories AS c ON v.category_id = c.id
        ORDER BY id

    """
    
    df_videos = spark.sql(query)

    return df_videos

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_embedding_udf():
    model_name="all-MiniLM-L6-v2"
    model = SentenceTransformer(model_name)

    @pandas_udf("array<float>", PandasUDFType.SCALAR)
    def embed_text(title_series: pd.Series) -> pd.Series:
        texts = title_series.fillna("")
        embeddings = model.encode(texts.tolist())
        return pd.Series(embeddings.tolist())

    return embed_text

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def encode_categories(df, input_col="category", output_col="category_vec"):
    indexer = StringIndexer(inputCol=input_col, outputCol=f"{input_col}_index", handleInvalid="keep")
    df_indexed = indexer.fit(df).transform(df)

    encoder = OneHotEncoder(inputCols=[f"{input_col}_index"], outputCols=[output_col])
    df_encoded = encoder.fit(df_indexed).transform(df_indexed)

    return df_encoded

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def normalize_numeric_features(df, input_cols, output_col="engagement_scaled"):
    
    # Montar vetor de entrada com métricas
    assembler = VectorAssembler(inputCols=input_cols, outputCol="engagement_features")
    df = assembler.transform(df)

    # Escalar
    scaler = StandardScaler(inputCol="engagement_features", outputCol=output_col, withStd=True, withMean=True)
    df = scaler.fit(df).transform(df)

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def convert_array_to_vector(df, input_col="embedding", output_col="embedding_vec"):
    def array_to_vector(array_col):
        return Vectors.dense(array_col)
    array_to_vector_udf = udf(array_to_vector, VectorUDT())
    df = df.withColumn(output_col, array_to_vector_udf(F.col(input_col)))
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def main():
    # Ler vídeos
    df = read_videos_table()

    # Criar embeddings
    embed_udf = create_embedding_udf()
    df = df.withColumn("embedding", embed_udf(F.col("title")))

    # One-hot encoding das categorias
    df = encode_categories(df, input_col="category", output_col="category_vec")

    # Converter embeddings para DenseVector
    df = convert_array_to_vector(df, input_col="embedding", output_col="embedding_vec")

    # Normalizar métricas de engajamento
    df = normalize_numeric_features(df,input_cols=["views", "engagements", "duration_seconds"], output_col="engagement_scaled")

    # Montar vetor final de features
    assembler_all = VectorAssembler(
        inputCols=["engagement_scaled","embedding_vec"],#, "embedding_vec"], #"category_vec"],
        outputCol="features"
    )
    df = assembler_all.transform(df)

    # Clusterização
    kmeans = KMeans(k=3, seed=42, featuresCol="features", predictionCol="cluster")
    df = kmeans.fit(df).transform(df)

    # Criar tabela Gold no Lakehouse
    df_gold = df.select(
        F.col("id").alias("video_id"),
        F.col("cluster")
    )

    df_gold.write.format("delta").mode("overwrite").saveAsTable("Lakehouse.gold.dim_videos_clusters")

    print("Tabela Gold 'dim_videos_clusters' criada/atualizada com sucesso!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if __name__ == "__main__":
    main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
