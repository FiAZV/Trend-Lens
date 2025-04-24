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

import os
import json
import glob
import pandas as pd
from datetime import datetime

# Caminho base onde os arquivos JSON estão armazenados
SOURCE_PATH = "/lakehouse/default/Files/raw/reddit"

# Carregar dados de usuários
def carregar_arquivos_json(pasta):
    caminho_pasta = f"{SOURCE_PATH}/{pasta}"
    arquivos = glob.glob(f"{caminho_pasta}/*.json")
    
    todos_dados = []
    for arquivo in arquivos:
        try:
            with open(arquivo, 'r', encoding='utf-8') as f:
                dados = json.load(f)
                if isinstance(dados, list):
                    todos_dados.extend(dados)
                elif isinstance(dados, dict):
                    todos_dados.append(dados)
        except Exception as e:
            print(f"Erro ao carregar arquivo {arquivo}: {e}")
    
    return todos_dados

# Carregar dados
usuarios_data = carregar_arquivos_json("usuarios")
posts_data = carregar_arquivos_json("posts")
comunidades_data = carregar_arquivos_json("comunidades")

print(f"Carregados {len(usuarios_data)} registros de usuários")
print(f"Carregados {len(posts_data)} registros de posts")
print(f"Carregados {len(comunidades_data)} registros de comunidades")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Abordagem corrigida para inserir dados no warehouse

# 1. Primeiro, vamos verificar se podemos ler as tabelas existentes
print("Verificando as tabelas existentes...")

try:
    # Tentar ler as tabelas para verificar se existem e têm dados
    usuarios_count_antes = spark.sql("SELECT COUNT(*) FROM bronze.reddit_usuarios").collect()[0][0]
    posts_count_antes = spark.sql("SELECT COUNT(*) FROM bronze.reddit_posts").collect()[0][0]
    comunidades_count_antes = spark.sql("SELECT COUNT(*) FROM bronze.reddit_comunidades").collect()[0][0]
    
    print(f"Contagem atual: Usuários: {usuarios_count_antes}, Posts: {posts_count_antes}, Comunidades: {comunidades_count_antes}")
except Exception as e:
    print(f"Erro ao ler tabelas existentes: {str(e)}")
    print("Criando as tabelas do zero...")
    usuarios_count_antes = 0
    posts_count_antes = 0
    comunidades_count_antes = 0

# 2. Certificar-se que os dados estão no formato correto antes de criar o DataFrame Spark
def validar_e_converter_tipos(df, tabela):
    """Converte os tipos de dados para corresponder ao esquema da tabela."""
    if tabela == 'reddit_usuarios':
        # Converter tipos para usuários
        if 'karma_total' in df.columns:
            df['karma_total'] = pd.to_numeric(df['karma_total'], errors='coerce')
        if 'karma_post' in df.columns:
            df['karma_post'] = pd.to_numeric(df['karma_post'], errors='coerce')
        if 'karma_comentario' in df.columns:
            df['karma_comentario'] = pd.to_numeric(df['karma_comentario'], errors='coerce')
        if 'premium' in df.columns:
            df['premium'] = df['premium'].apply(lambda x: 1 if x else 0)
    
    elif tabela == 'reddit_posts':
        # Converter tipos para posts
        if 'upvotes' in df.columns:
            df['upvotes'] = pd.to_numeric(df['upvotes'], errors='coerce')
        if 'comentarios' in df.columns:
            df['comentarios'] = pd.to_numeric(df['comentarios'], errors='coerce')
    
    # Converter datas para o formato correto
    for col in df.columns:
        if 'data' in col.lower() and df[col].dtype == 'object':
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except:
                pass
    
    return df

# Processar os dados com validação de tipos
df_usuarios = processar_dados_usuarios(usuarios_data)
df_usuarios = validar_e_converter_tipos(df_usuarios, 'reddit_usuarios')

df_posts = processar_dados_posts(posts_data)
df_posts = validar_e_converter_tipos(df_posts, 'reddit_posts')

df_comunidades = processar_dados_comunidades(comunidades_data)
df_comunidades = validar_e_converter_tipos(df_comunidades, 'reddit_comunidades')

# 3. Método 1: Usar createOrReplaceTempView e INSERT INTO
print("\nMétodo 1: Inserção usando SQL...")
try:
    # Criar views temporárias
    spark_df_usuarios = spark.createDataFrame(df_usuarios)
    spark_df_usuarios.createOrReplaceTempView("temp_usuarios")
    
    spark_df_posts = spark.createDataFrame(df_posts)
    spark_df_posts.createOrReplaceTempView("temp_posts")
    
    spark_df_comunidades = spark.createDataFrame(df_comunidades)
    spark_df_comunidades.createOrReplaceTempView("temp_comunidades")
    
    # Inserir dados usando SQL
    spark.sql("INSERT INTO bronze.reddit_usuarios SELECT * FROM temp_usuarios")
    spark.sql("INSERT INTO bronze.reddit_posts SELECT * FROM temp_posts")
    spark.sql("INSERT INTO bronze.reddit_comunidades SELECT * FROM temp_comunidades")
    
    # Verificar se dados foram inseridos
    usuarios_count_depois = spark.sql("SELECT COUNT(*) FROM bronze.reddit_usuarios").collect()[0][0]
    posts_count_depois = spark.sql("SELECT COUNT(*) FROM bronze.reddit_posts").collect()[0][0]
    comunidades_count_depois = spark.sql("SELECT COUNT(*) FROM bronze.reddit_comunidades").collect()[0][0]
    
    print(f"Usuários: {usuarios_count_antes} -> {usuarios_count_depois} (+{usuarios_count_depois - usuarios_count_antes})")
    print(f"Posts: {posts_count_antes} -> {posts_count_depois} (+{posts_count_depois - posts_count_antes})")
    print(f"Comunidades: {comunidades_count_antes} -> {comunidades_count_depois} (+{comunidades_count_depois - comunidades_count_antes})")
    
    if (usuarios_count_depois > usuarios_count_antes and 
        posts_count_depois > posts_count_antes and 
        comunidades_count_depois > comunidades_count_antes):
        print("✅ Inserção via SQL bem-sucedida!")
    else:
        print("❌ Possível problema na inserção via SQL. Tentando método alternativo...")
        raise Exception("Dados não foram inseridos corretamente")
        
except Exception as e:
    print(f"Erro no Método 1: {str(e)}")
    print("\nMétodo 2: Inserção direta com writeStream...")
        
       

# 6. Verificação final
print("\nVerificação final:")
try:
    # Mostrar alguns dados para confirmar
    print("\nAmostra de dados de usuários:")
    spark.sql("SELECT * FROM bronze.reddit_usuarios LIMIT 3").show(truncate=False)
    
    print("\nAmostra de dados de posts:")
    spark.sql("SELECT * FROM bronze.reddit_posts LIMIT 3").show(truncate=False)
    
    print("\nAmostra de dados de comunidades:")
    spark.sql("SELECT * FROM bronze.reddit_comunidades LIMIT 3").show(truncate=False)
    
    # Contagem final
    usuarios_final = spark.sql("SELECT COUNT(*) FROM bronze.reddit_usuarios").collect()[0][0]
    posts_final = spark.sql("SELECT COUNT(*) FROM bronze.reddit_posts").collect()[0][0]
    comunidades_final = spark.sql("SELECT COUNT(*) FROM bronze.reddit_comunidades").collect()[0][0]
    
    print(f"\nContagem final: Usuários: {usuarios_final}, Posts: {posts_final}, Comunidades: {comunidades_final}")
    
except Exception as e:
    print(f"Erro na verificação final: {str(e)}")

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
