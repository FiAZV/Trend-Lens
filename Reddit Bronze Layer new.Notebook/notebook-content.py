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
from pyspark.sql.functions import current_timestamp, lit, col, from_json
from pyspark.sql.types import (StructType, StructField, StringType, BooleanType,
                              LongType, TimestampType, IntegerType, MapType, ArrayType)
import json
import os
from glob import glob

# Inicializar sessão Spark com configurações adicionais para melhor tratamento de JSON
spark = SparkSession.builder \
    .appName("RedditDataExtraction") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Schema para a tabela reddit_usuarios
USUARIOS_SCHEMA = StructType([
    StructField("nome", StringType(), True),
    StructField("karma_total", LongType(), True),  # Usando LongType para garantir compatibilidade
    StructField("karma_post", LongType(), True),   # Usando LongType para garantir compatibilidade
    StructField("karma_comentario", LongType(), True),  # Usando LongType para garantir compatibilidade
    StructField("data_criacao", StringType(), True),
    StructField("premium", BooleanType(), True),
    StructField("data_coleta", StringType(), True)
])

# Schema para a tabela reddit_posts
POSTS_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("titulo", StringType(), True),
    StructField("autor", StringType(), True),
    StructField("upvotes", LongType(), True),  # Usando LongType para garantir compatibilidade
    StructField("comentarios", LongType(), True),  # Usando LongType para garantir compatibilidade
    StructField("link", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("data_postagem", StringType(), True),
    StructField("data_coleta", StringType(), True)
])

# Schema para a tabela reddit_comunidades
COMUNIDADES_SCHEMA = StructType([
    StructField("post_id", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("data_coleta", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def find_correct_path(base_paths, subdir):
    """
    Tenta encontrar o caminho correto para os arquivos, testando diferentes formatos de caminho.
    
    Parâmetros:
    - base_paths: Lista de possíveis caminhos base
    - subdir: Subdiretório a ser anexado ao caminho base
    
    Retorna:
    - O caminho completo correto, ou o primeiro caminho da lista se nenhum for encontrado
    """
    for base_path in base_paths:
        full_path = os.path.join(base_path, subdir) if subdir else base_path
        
        # Verificar se o caminho existe
        if os.path.exists(full_path):
            print(f"Caminho encontrado: {full_path}")
            return full_path
        else:
            print(f"Caminho não encontrado: {full_path}")
    
    # Se nenhum caminho existir, retornar o primeiro da lista
    default_path = os.path.join(base_paths[0], subdir) if subdir else base_paths[0]
    print(f"Nenhum caminho existente encontrado. Usando: {default_path}")
    return default_path

def check_json_format(file_path):
    """
    Verifica o formato dos arquivos JSON e retorna uma amostra do conteúdo.
    
    Parâmetros:
    - file_path: Caminho para o arquivo JSON
    
    Retorna:
    - Uma amostra do conteúdo do arquivo
    """
    try:
        with open(file_path, 'r') as f:
            content = f.read(1000)  # Lê os primeiros 1000 caracteres para análise
            return content
    except Exception as e:
        print(f"Erro ao ler o arquivo {file_path}: {str(e)}")
        return None

def convert_to_jsonl_if_needed(input_dir, schema_type):
    """
    Verifica e converte arquivos JSON para o formato JSONL (JSON Lines) se necessário.
    
    Parâmetros:
    - input_dir: Diretório contendo os arquivos JSON
    - schema_type: Tipo de schema (para logging)
    
    Retorna:
    - Lista de caminhos para os arquivos processados
    """
    processed_files = []
    
    # Verificar se o diretório existe
    if not os.path.exists(input_dir):
        print(f"Aviso: Diretório {input_dir} não encontrado. Criando diretório vazio.")
        os.makedirs(input_dir, exist_ok=True)
        return processed_files
    
    # Listar todos os arquivos JSON no diretório
    json_files = glob(os.path.join(input_dir, "*.json"))
    
    if not json_files:
        print(f"Aviso: Nenhum arquivo JSON encontrado em {input_dir}")
        return processed_files
    
    print(f"Encontrados {len(json_files)} arquivos JSON em {input_dir}")
    
    for json_file in json_files:
        try:
            # Verificar o formato do arquivo
            with open(json_file, 'r') as f:
                content = f.read()
                
            # Tentar carregar como JSON
            try:
                # Se for um array JSON
                if content.strip().startswith('[') and content.strip().endswith(']'):
                    data = json.loads(content)
                    jsonl_file = json_file.replace('.json', '.jsonl')
                    
                    with open(jsonl_file, 'w') as f:
                        for item in data:
                            f.write(json.dumps(item) + '\n')
                    
                    processed_files.append(jsonl_file)
                    print(f"Convertido arquivo {json_file} para formato JSONL: {jsonl_file}")
                    
                # Se for um objeto JSON único
                elif content.strip().startswith('{') and content.strip().endswith('}'):
                    data = json.loads(content)
                    jsonl_file = json_file.replace('.json', '.jsonl')
                    
                    with open(jsonl_file, 'w') as f:
                        f.write(json.dumps(data) + '\n')
                    
                    processed_files.append(jsonl_file)
                    print(f"Convertido arquivo {json_file} para formato JSONL: {jsonl_file}")
                    
                # Se já estiver no formato JSONL (múltiplos objetos JSON, um por linha)
                else:
                    # Verificar se cada linha é um JSON válido
                    with open(json_file, 'r') as f:
                        lines = f.readlines()
                    
                    all_valid = True
                    for line in lines:
                        if line.strip():
                            try:
                                json.loads(line.strip())
                            except:
                                all_valid = False
                                break
                    
                    if all_valid and lines:
                        processed_files.append(json_file)
                        print(f"Arquivo {json_file} já está no formato JSONL")
                    else:
                        # Tentar tratar como múltiplos objetos JSON separados por vírgula
                        try:
                            content = content.strip()
                            if content.startswith('[') and content.endswith(']'):
                                content = content[1:-1]
                            
                            # Dividir por vírgula e reconstruir objetos JSON
                            objects = []
                            current_obj = ""
                            brace_count = 0
                            
                            for char in content:
                                current_obj += char
                                if char == '{':
                                    brace_count += 1
                                elif char == '}':
                                    brace_count -= 1
                                    if brace_count == 0 and current_obj.strip():
                                        objects.append(current_obj.strip())
                                        current_obj = ""
                            
                            jsonl_file = json_file.replace('.json', '.jsonl')
                            with open(jsonl_file, 'w') as f:
                                for obj in objects:
                                    if obj.strip().startswith('{') and obj.strip().endswith('}'):
                                        try:
                                            # Validar que é um JSON válido
                                            parsed = json.loads(obj)
                                            f.write(json.dumps(parsed) + '\n')
                                        except:
                                            pass
                            
                            processed_files.append(jsonl_file)
                            print(f"Convertido arquivo {json_file} para formato JSONL: {jsonl_file}")
                        except Exception as e:
                            print(f"Erro ao processar arquivo {json_file}: {str(e)}")
            except Exception as e:
                print(f"Erro ao processar arquivo {json_file}: {str(e)}")
                
                # Tentar processar como múltiplos objetos JSON, um por linha
                try:
                    with open(json_file, 'r') as f:
                        lines = f.readlines()
                    
                    jsonl_file = json_file.replace('.json', '.jsonl')
                    with open(jsonl_file, 'w') as f:
                        for line in lines:
                            if line.strip():
                                try:
                                    # Tentar carregar e reescrever cada linha como JSON válido
                                    obj = json.loads(line.strip())
                                    f.write(json.dumps(obj) + '\n')
                                except:
                                    # Se falhar, pular esta linha
                                    pass
                    
                    processed_files.append(jsonl_file)
                    print(f"Convertido arquivo {json_file} para formato JSONL: {jsonl_file}")
                except Exception as e2:
                    print(f"Erro ao processar arquivo {json_file} como linhas: {str(e2)}")
        except Exception as e:
            print(f"Erro ao processar arquivo {json_file}: {str(e)}")
    
    return processed_files

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def drop_table_if_exists(table_name):
    """
    Exclui uma tabela se ela existir no Lakehouse.
    
    Parâmetros:
    - table_name: Nome completo da tabela (incluindo schema)
    """
    try:
        # Verificar se a tabela existe
        tables = spark.sql(f"SHOW TABLES IN Lakehouse.bronze LIKE '{table_name}'")
        if tables.count() > 0:
            print(f"Excluindo tabela existente: {table_name}")
            spark.sql(f"DROP TABLE IF EXISTS Lakehouse.bronze.{table_name}")
            print(f"Tabela {table_name} excluída com sucesso.")
        else:
            print(f"Tabela {table_name} não existe. Nenhuma exclusão necessária.")
    except Exception as e:
        print(f"Erro ao verificar/excluir tabela {table_name}: {str(e)}")
        # Tentar excluir de qualquer forma
        try:
            spark.sql(f"DROP TABLE IF EXISTS Lakehouse.bronze.{table_name}")
            print(f"Tabela {table_name} excluída com sucesso (método alternativo).")
        except Exception as e2:
            print(f"Não foi possível excluir a tabela {table_name}: {str(e2)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_delta_table(raw_path, table_name, schema):
    """
    Cria uma tabela Delta no schema bronze do Lakehouse a partir de arquivos JSON.
    
    Parâmetros:
    - raw_path: Caminho para os arquivos JSON de origem
    - table_name: Nome da tabela a ser criada no Lakehouse
    - schema: Schema da tabela a ser criada
    """
    print(f"Processando dados para a tabela {table_name} a partir de {raw_path}")
    
    # Excluir a tabela existente para evitar conflitos de schema
    drop_table_if_exists(table_name)
    
    # Verificar e converter arquivos para formato JSONL se necessário
    processed_files = convert_to_jsonl_if_needed(raw_path, table_name)
    
    if not processed_files:
        print(f"Aviso: Nenhum arquivo processado para {table_name}. Criando tabela vazia.")
        # Criar um DataFrame vazio com o schema correto
        empty_df = spark.createDataFrame([], schema)
        
        # Escrever tabela vazia
        empty_df.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable(f"Lakehouse.bronze.{table_name}")
        
        print(f"Tabela vazia {table_name} criada com sucesso no schema bronze do Lakehouse.")
        return
    
    # Ler os arquivos JSONL processados
    print(f"Lendo {len(processed_files)} arquivos para a tabela {table_name}")
    
    # Criar um DataFrame vazio com o schema correto
    df = None
    
    # Processar cada arquivo individualmente e unir os resultados
    for file_path in processed_files:
        try:
            # Ler o arquivo com o schema definido
            file_df = spark.read.schema(schema).json(file_path)
            
            # Se o DataFrame principal ainda não foi criado, inicializá-lo
            if df is None:
                df = file_df
            else:
                # Unir com o DataFrame principal
                df = df.union(file_df)
                
            print(f"Arquivo {file_path} processado com sucesso.")
        except Exception as e:
            print(f"Erro ao processar arquivo {file_path}: {str(e)}")
            
            # Tentar ler sem schema para diagnóstico
            try:
                sample_df = spark.read.json(file_path)
                print("Schema inferido pelo Spark:")
                sample_df.printSchema()
                print("Amostra dos dados:")
                sample_df.show(5, truncate=False)
            except Exception as e2:
                print(f"Não foi possível ler o arquivo nem mesmo sem schema: {str(e2)}")
                
                # Tentar ler o conteúdo bruto do arquivo para diagnóstico
                content = check_json_format(file_path)
                if content:
                    print(f"Amostra do conteúdo do arquivo {file_path}:")
                    print(content[:500] + "..." if len(content) > 500 else content)
    
    # Se nenhum DataFrame foi criado, criar um vazio com o schema correto
    if df is None:
        print(f"Aviso: Nenhum dado válido encontrado para {table_name}. Criando tabela vazia.")
        df = spark.createDataFrame([], schema)
    
    # Mostrar a contagem de registros e um exemplo dos dados
    count = df.count()
    print(f"Total de registros encontrados: {count}")
    
    if count > 0:
        print("Exemplo dos dados:")
        df.show(5, truncate=False)
        print("Schema dos dados:")
        df.printSchema()
    
    # Escrever com tratamento de schema
    df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"Lakehouse.bronze.{table_name}")
    
    print(f"Tabela {table_name} criada com sucesso no schema bronze do Lakehouse.")
    print("-" * 80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def main():
    """
    Função principal que executa o processo de extração e carregamento dos dados do Reddit.
    """
    print("Iniciando o processo de extração e carregamento dos dados do Reddit...")
    
    # Definir possíveis caminhos base para os arquivos JSON
    possible_base_paths = [
        "/Files/raw/reddit",  # Caminho absoluto com barra inicial
        "Files/raw/reddit",   # Caminho relativo sem barra inicial
        "/files/raw/reddit",  # Caminho absoluto com barra inicial (minúsculo)
        "files/raw/reddit"    # Caminho relativo sem barra inicial (minúsculo)
    ]
    
    # Encontrar o caminho base correto
    print("Procurando o caminho correto para os arquivos...")
    raw_path = find_correct_path(possible_base_paths, "")
    
    # Definir os caminhos para os arquivos JSON
    raw_usuarios = find_correct_path([raw_path], "usuarios")
    raw_posts = find_correct_path([raw_path], "posts")
    raw_comunidades = find_correct_path([raw_path], "comunidades")
    
    # Verificar se os diretórios existem
    for path in [raw_path, raw_usuarios, raw_posts, raw_comunidades]:
        if not os.path.exists(path):
            print(f"Aviso: Diretório {path} não encontrado. Criando diretório.")
            os.makedirs(path, exist_ok=True)
    
    # Definir os nomes das tabelas
    table_usuarios = "reddit_usuarios"
    table_posts = "reddit_posts"
    table_comunidades = "reddit_comunidades"
    
    # Criar as tabelas Delta
    print("\n1. Processando dados de usuários do Reddit...")
    create_delta_table(raw_usuarios, table_usuarios, USUARIOS_SCHEMA)
    
    print("\n2. Processando dados de posts do Reddit...")
    create_delta_table(raw_posts, table_posts, POSTS_SCHEMA)
    
    print("\n3. Processando dados de comunidades do Reddit...")
    create_delta_table(raw_comunidades, table_comunidades, COMUNIDADES_SCHEMA)
    
    print("\nProcesso de extração e carregamento dos dados do Reddit concluído com sucesso!")
    print("Todas as tabelas foram criadas no schema bronze do Lakehouse.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Executar o processo principal
main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verificar a tabela reddit_usuarios
print("Verificando a tabela reddit_usuarios:")
spark.sql("SELECT * FROM Lakehouse.bronze.reddit_usuarios LIMIT 5").show(truncate=False)

# Verificar a tabela reddit_posts
print("Verificando a tabela reddit_posts:")
spark.sql("SELECT * FROM Lakehouse.bronze.reddit_posts LIMIT 5").show(truncate=False)

# Verificar a tabela reddit_comunidades
print("Verificando a tabela reddit_comunidades:")
spark.sql("SELECT * FROM Lakehouse.bronze.reddit_comunidades LIMIT 5").show(truncate=False)

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
