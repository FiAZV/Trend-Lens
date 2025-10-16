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

# MARKDOWN ********************

# Cria a **tabela de Clientes** a partir de um arquivo CSV em **Files** no Lakehouse

# CELL ********************

# --- CÓDIGO FINAL PARA CRIAR/RECRIAR A TABELA DE CLIENTES COM ID ---

from pyspark.sql.functions import monotonically_increasing_id

# Define os nomes
nome_arquivo_csv = "clientes_potenciais.csv"
nome_nova_tabela = "gold.dim_clientes_potenciais"
caminho_arquivo = f"Files/{nome_arquivo_csv}"

print(f"Lendo o arquivo CSV: {caminho_arquivo}...")
df_csv = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(caminho_arquivo)

print("Limpando os nomes das colunas...")
novos_nomes_colunas = [c.lower().replace(' ', '_').replace('(', '').replace(')', '') for c in df_csv.columns]
df_csv_limpo = df_csv.toDF(*novos_nomes_colunas)

print("Adicionando a coluna 'id_da_empresa'...")
df_com_id = df_csv_limpo.withColumn("id_da_empresa", monotonically_increasing_id())

print(f"\nRecriando a tabela Delta em: {nome_nova_tabela}...")
# --- CORREÇÃO AQUI ---
# Adicionamos a opção para permitir a sobrescrita do schema (estrutura) da tabela
df_com_id.write \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(nome_nova_tabela)

print("\n✅ Tabela 'dim_clientes_potenciais' recriada com sucesso, agora com 'id_da_empresa'!")
# Mostra as colunas principais para confirmar
df_com_id.select("id_da_empresa", "nome_da_empresa").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Cria tabela de **Registros**

# CELL ********************

# --- CÓDIGO PARA RECRIAR A TABELA DE RELATÓRIOS (SEM A COLUNA STATUS) ---

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

nome_tabela_relatorios = "gold.fact_relatorios_gerados"

# Apaga a tabela antiga para garantir uma recriação limpa.
print(f"Apagando a versão antiga da tabela {nome_tabela_relatorios}, se existir...")
spark.sql(f"DROP TABLE IF EXISTS {nome_tabela_relatorios}")

# Define a NOVA estrutura da tabela, agora sem o campo 'status'
schema_relatorio_final = StructType([
    StructField("id_da_empresa", LongType(), True),
    StructField("nome_da_empresa", StringType(), True),
    StructField("data_geracao", TimestampType(), True),
    StructField("texto_relatorio", StringType(), True),
    StructField("versao_prompt", StringType(), True)
])

# Cria um DataFrame Spark vazio com a nova estrutura
df_vazio = spark.createDataFrame([], schema_relatorio_final)

print(f"Recriando a tabela vazia: {nome_tabela_relatorios} com a estrutura final...")
df_vazio.write.mode("ignore").format("delta").saveAsTable(nome_tabela_relatorios)

print("\n✅ Tabela de relatórios recriada com sucesso, agora sem a coluna 'status'!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Célula de Manutenção - Limpar Tabela de Relatórios

# Define o nome completo da tabela que queremos limpar
nome_tabela_para_limpar = "gold.fact_relatorios_gerados"

print(f"Iniciando a limpeza da tabela: {nome_tabela_para_limpar}...")

try:
    # O comando TRUNCATE TABLE é o mais eficiente para apagar todas as linhas
    # de uma tabela, mantendo sua estrutura intacta.
    spark.sql(f"TRUNCATE TABLE {nome_tabela_para_limpar}")
    
    print("\n✅ Tabela limpa com sucesso!")
    
    # Vamos verificar se a tabela está realmente vazia
    print("\nVerificando o conteúdo da tabela após a limpeza:")
    df_verificacao = spark.read.table(nome_tabela_para_limpar)
    
    if df_verificacao.count() == 0:
        print("   - Confirmação: A tabela está vazia.")
    else:
        print(f"   - Atenção: A tabela ainda contém {df_verificacao.count()} linhas.")

except Exception as e:
    print(f"\n❌ Ocorreu um erro ao tentar limpar a tabela: {e}")

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
