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

spark = SparkSession.builder.appName("YoutubeCategoriesSilverLayer").getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_schema = "Lakehouse.bronze"
silver_schema = "Lakehouse.silver"

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

def save_table(df, schema: str, table_name: str) -> None:
    
    target_table_path = f"{schema}.{table_name}"
    
    if spark._jvm.io.delta.tables.DeltaTable.isDeltaTable(spark._jsparkSession, target_table_path):
        print("A tabela já existe. Executando MERGE INTO para atualizar/inserir...")
        
        delta_table = DeltaTable.forName(spark, target_table_path)
        
        # --- Alteração: Usa 'whenMatchedUpdate' para atualizar colunas específicas ---
        delta_table.alias("target") \
            .merge(
                df.alias("source"),
                "target.id = source.id"
            ) \
            .whenMatchedUpdate(set = {
                "category": "source.category",
                "updated_at": "source.updated_at"
            }) \
            .whenNotMatchedInsert(values = {
                "id": "source.id",
                "category": "source.category",
                "data_source": "source.data_source",
                "extracted_at": "source.extracted_at",
                "ingestion_timestamp": "source.ingestion_timestamp",
                "updated_at": "source.updated_at"
            }) \
            .execute()
            
    else:
        print("A tabela não existe. Criando nova tabela Delta...")
        df.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable(target_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def main():
    # Bronze layer tables paths
    bronze_table_categories = f"{bronze_schema}.youtube_categories"

    # Reading bronze layer tables into dataframes
    bronze_categories_df = spark.table(bronze_table_categories)

    # Transformating bronze layer tables to silver layer tables and writing it on dataframes
    silver_categories_df = transform_categories(bronze_categories_df)

    # Defining silver layer tables
    categories_table = "youtube_categories"

    # Saving tables into silver schema
    save_table(silver_categories_df, silver_schema, categories_table)

    print("Camada Silver criada com sucesso!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
