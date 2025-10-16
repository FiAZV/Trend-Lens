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

# --- Instalação das bibliotecas necessárias ---
%pip install google-generativeai tabulate requests markdown

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Importa as **bibliotecas**

# CELL ********************

# Célula 1 (Versão Final Corrigida com todos os Imports)


# --- Imports para o Notebook ---
# Bibliotecas principais
import pandas as pd
import google.generativeai as genai
from IPython.display import display, Markdown
from datetime import datetime
import json
import requests
from IPython.display import HTML
import time

# Funções do PySpark
from pyspark.sql.functions import col, collect_list, concat_ws, desc, monotonically_increasing_id, current_timestamp, lit
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

# --- CORREÇÃO AQUI: Bibliotecas para envio de e-mail ---
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import markdown

print("✅ Todas as bibliotecas foram carregadas e estão prontas para uso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Configura **API do Gemini**

# CELL ********************

GOOGLE_API_KEY = 'AIzaSyAB-OCnCp6ISU_wL8hi6BzpcA8meWDSFuk'
genai.configure(api_key=GOOGLE_API_KEY)

print("✅ API Key configurada.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 1. Carrega os **dados coletados** do **Youtube**
# 2. Carrega os **dados** da tabela de **clientes**

# CELL ********************

# Célula 3 (v2.4 - Geração de Contexto Rico com Performance e Sentimento)

from pyspark.sql.functions import col, collect_list, concat_ws, desc, count, when

# --- 1. Carregando os Dados da Empresa Alvo (sem alterações) ---
NOME_TABELA_CLIENTES = "gold.dim_clientes_potenciais" 
print(f"Lendo a tabela de clientes: {NOME_TABELA_CLIENTES}...")
df_clientes = spark.read.table(NOME_TABELA_CLIENTES).toPandas()
print("✅ Tabela de clientes carregada.")


# --- 2. Construindo o CONTEXTO DE PERFORMANCE ---
print("\nConstruindo Contexto de Performance (Vídeos em Alta)...")

# Carrega as tabelas necessárias para a análise de performance
fact_engagement = spark.read.table("gold.fact_video_engagement")
dim_video = spark.read.table("gold.dim_video")
dim_category = spark.read.table("gold.dim_category")
bridge_video_tag = spark.read.table("gold.bridge_video_tag")
dim_tag = spark.read.table("gold.dim_tag")

# Agrega as tags (sem alterações)
video_tags_agg = bridge_video_tag.join(dim_tag, "tag_id") \
    .groupBy("video_id") \
    .agg(concat_ws(", ", collect_list("tag")).alias("Tags"))

# Junta as tabelas e seleciona mais colunas para um contexto mais rico
performance_df = fact_engagement \
    .join(dim_video, "video_id") \
    .join(dim_category, "category_id") \
    .join(video_tags_agg, "video_id", "left") \
    .select(
        col("title").alias("Titulo"),
        col("views").alias("Visualizacoes"),
        col("likes").alias("Likes"),
        col("comments").alias("Comentarios"),
        col("engagement_rate").alias("Taxa_Engajamento"), # <-- DADO NOVO E VALIOSO
        col("category_name").alias("Categoria"),
        col("Tags")
    ) \
    .orderBy(desc("Visualizacoes")) \
    .limit(20) # Reduzimos o limite para focar nos vídeos mais relevantes

df_videos_performance = performance_df.toPandas()
print("✅ Contexto de Performance criado.")


# --- 3. Construindo o CONTEXTO DE SENTIMENTO ---
print("\nConstruindo Contexto de Sentimento da Audiência...")

# Carrega as tabelas de sentimento e vídeo
dim_sentiment = spark.read.table("gold.dim_comments_sentiment")

# Agrupa por vídeo e "pivota" a coluna de sentimento para contar as ocorrências
sentiment_summary_df = dim_sentiment \
    .groupBy("video_id") \
    .pivot("sentiment", ["positive", "negative", "neutral"]) \
    .agg(count("*")) \
    .na.fill(0) # Preenche com 0 os sentimentos que não apareceram

# Junta com a tabela de vídeos para obter os títulos e o total de comentários
sentiment_final_df = sentiment_summary_df \
    .join(dim_video.select("video_id", "title"), "video_id") \
    .join(fact_engagement.select("video_id", "comments"), "video_id") \
    .select(
        col("title").alias("Titulo_Video"),
        col("comments").alias("Total_Comentarios"),
        col("positive").alias("Comentarios_Positivos"),
        col("negative").alias("Comentarios_Negativos"),
        col("neutral").alias("Comentarios_Neutros")
    ) \
    .orderBy(desc("Total_Comentarios")) \
    .limit(15) # Focamos nos 15 vídeos com mais comentários para a análise

df_videos_sentimento = sentiment_final_df.toPandas()
print("✅ Contexto de Sentimento criado.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 1. Define configurações de **geração**
# 2. Define o **Prompt**
# 3. **Itera** pela tabela de **clientes** e **gera um relatório** para cada
# 4. **Salva** os relatorios na tabela

# CELL ********************

# --- 1. PREPARAÇÃO DO AMBIENTE E MODELO ---
print("Configurando o ambiente para a execução em lote...")

SEU_EMAIL_GMAIL = "brunocarrarabpc@gmail.com"
SUA_SENHA_DE_APP = "uezt aazs anbi wkit"


# Configuração do modelo e da tabela de destino
model = genai.GenerativeModel('gemini-2.0-flash-lite') 
generation_config = genai.GenerationConfig(temperature=0.1)
nome_tabela_relatorios = "gold.fact_relatorios_gerados"
versao_atual_prompt = "4.0"

# Pega a lista de clientes do DataFrame pandas que já carregamos na célula anterior
lista_de_clientes = df_clientes.to_dict('records')
total_clientes = len(lista_de_clientes)
print(f"Encontrados {total_clientes} clientes para processar.")


# --- 2. LOOP DE GERAÇÃO E SALVAMENTO ---
print("\n--- INICIANDO PROCESSAMENTO EM LOTE ---")

# O loop principal que vai iterar por cada cliente da lista
for i, cliente in enumerate(lista_de_clientes):
    
    # Extrai as informações de cada cliente do loop
    id_empresa_alvo = cliente['id_da_empresa']
    nome_empresa_para_relatorio = cliente['nome_da_empresa']
    email_do_cliente = cliente.get('contato_opcional', None)
    
    print(f"\n[{i+1}/{total_clientes}] Processando empresa: {nome_empresa_para_relatorio} (ID: {id_empresa_alvo})")
    
    # --- A. Preparação dos dados para o prompt ---
    info_empresa_texto = ""
    for indice, valor in cliente.items():
        info_empresa_texto += f"- **{indice.replace('_', ' ').title()}:** {valor}\n"
    
    info_performance_texto = df_videos_performance.to_markdown(index=False)
    info_sentimento_texto = df_videos_sentimento.to_markdown(index=False)
    
    # --- B. Montagem do prompt ---
    data_hoje = datetime.now().strftime('%d/%m/%Y')
    prompt_final = f"""
    [INSTRUÇÕES]
    Sua tarefa é gerar um relatório de recomendações de vídeo DIRETAMENTE EM FORMATO HTML.
    Preencha o template HTML fornecido na [ESTRUTURA DE SAÍDA EM HTML].
    Analise os dados da empresa e os dados de mercado para criar insights valiosos.
    Preencha CADA seção marcada com ``.
    Mantenha a estrutura e o CSS inline fornecidos para garantir a compatibilidade.
    Use tags HTML padrão como <p>, <ul>, <li>, <strong> para formatar seu texto dentro de cada seção.

    [DADOS DA EMPRESA]
    {info_empresa_texto}

    [TENDÊNCIAS DE PERFORMANCE (VÍDEOS EM ALTA)]
    {info_performance_texto}

    [ANÁLISE DE SENTIMENTO DA AUDIÊNCIA (VÍDEOS MAIS COMENTADOS)]
    {info_sentimento_texto}

    [ESTRUTURA DE SAÍDA EM HTML - PREENCHA OBRIGATORIAMENTE]
    <!DOCTYPE html>
    <html>
    <head>
      <title>Relatório de Vídeo - {nome_empresa_para_relatorio}</title>
    </head>
    <body style="font-family: Arial, sans-serif; margin: 0; padding: 0; background-color: #f4f4f4;">
      <table width="100%" border="0" cellspacing="0" cellpadding="0">
        <tr>
          <td align="center" style="padding: 20px 0;">
            <table width="600" border="0" cellspacing="0" cellpadding="0" style="background-color: #ffffff; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.1);">
              <tr>
                <td align="center" style="padding: 40px 20px; background-color: #4A90E2; color: #ffffff; border-top-left-radius: 8px; border-top-right-radius: 8px;">
                  <h1 style="margin: 0; font-size: 24px;">Relatório Estratégico de Vídeo</h1>
                </td>
              </tr>
              <tr>
                <td style="padding: 30px;">
                  <p style="font-size: 16px; color: #333333;">Prezado(a) Cliente ({nome_empresa_para_relatorio}),</p>
                  <p style="font-size: 16px; color: #555555; line-height: 1.5;">Segue seu relatório com recomendações estratégicas de conteúdo para {data_hoje}, baseado em uma análise aprofundada de performance e sentimento do público.</p>
                  
                  <h2 style="font-size: 20px; color: #4A90E2; border-bottom: 2px solid #f4f4f4; padding-bottom: 10px; margin-top: 30px;">1. Reflexão Estratégica</h2>
                  <h2 style="font-size: 20px; color: #4A90E2; border-bottom: 2px solid #f4f4f4; padding-bottom: 10px; margin-top: 30px;">2. Sugestão de Formato de Vídeo</h2>
                  <h2 style="font-size: 20px; color: #4A90E2; border-bottom: 2px solid #f4f4f4; padding-bottom: 10px; margin-top: 30px;">3. Sugestões de Assuntos</h2>
                  <h2 style="font-size: 20px; color: #4A90E2; border-bottom: 2px solid #f4f4f4; padding-bottom: 10px; margin-top: 30px;">4. Sugestões de Títulos e Palavras-Chave</h2>
                  <h2 style="font-size: 20px; color: #4A90E2; border-bottom: 2px solid #f4f4f4; padding-bottom: 10px; margin-top: 30px;">5. Sugestão de Tags</h2>
                  <h2 style="font-size: 20px; color: #4A90E2; border-bottom: 2px solid #f4f4f4; padding-bottom: 10px; margin-top: 30px;">6. Sugestão de Tempo de Vídeo</h2>
                  </td>
              </tr>
              <tr>
                <td align="center" style="padding: 20px; background-color: #f4f4f4; color: #888888; font-size: 12px; border-bottom-left-radius: 8px; border-bottom-right-radius: 8px;">
                  <p style="margin: 0;">Relatório gerado por AI Agent &copy; 2025</p>
                </td>
              </tr>
            </table>
          </td>
        </tr>
      </table>
    </body>
    </html>
"""
    
    # --- C. Geração do relatório ---
    try:
        print("   - Gerando relatório com a IA...")
        response = model.generate_content(prompt_final, generation_config=generation_config)
        texto_gerado_html = response.text
        geracao_sucesso = True
        print("   - ✅ Relatório gerado.")

    except Exception as e:
        print(f"   - ❌ Erro ao gerar o relatório: {e}")
        texto_gerado_html = f"ERRO: A geração do relatório falhou. Detalhes: {str(e)}"
        geracao_sucesso = False
        
    # --- D. Salvamento do resultado ---
    try:
        if geracao_sucesso:
          print(f"   - Salvando resultado na tabela: {nome_tabela_relatorios}...")
          id_empresa_alvo_int = int(id_empresa_alvo)
          
          RelatorioRow = Row("id_da_empresa", "nome_da_empresa", "texto_relatorio", "versao_prompt")
          df_para_salvar = spark.createDataFrame([
              RelatorioRow(id_empresa_alvo_int, nome_empresa_para_relatorio, texto_gerado_html, versao_atual_prompt)
          ]).withColumn("data_geracao", current_timestamp())
          
          df_para_salvar.write.mode("append").format("delta").saveAsTable(nome_tabela_relatorios)
          print("   - ✅ Relatório salvo com sucesso!")
    except Exception as e:
        print(f"   - ❌ Erro ao salvar o relatório no Lakehouse: {e}")
    
    if geracao_sucesso and email_do_cliente:
        print(f"   - Enviando relatório por e-mail para: {email_do_cliente}...")
        try:
            # Converte o relatório para HTML
            relatorio_html = markdown.markdown(texto_gerado_html)
            payload = {
                "email_destinatario": email_do_cliente,
                "nome_empresa": nome_empresa_para_relatorio,
                "texto_relatorio": relatorio_html
            }
            
            # Monta o e-mail
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"Seu Relatório Estratégico de Vídeo para {nome_empresa_para_relatorio}"
            msg['From'] = SEU_EMAIL_GMAIL
            msg['To'] = email_do_cliente
            msg.attach(MIMEText(relatorio_html, 'html'))
            
            # Conecta ao servidor do Gmail e envia
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
                smtp_server.login(SEU_EMAIL_GMAIL, SUA_SENHA_DE_APP)
                smtp_server.sendmail(SEU_EMAIL_GMAIL, email_do_cliente, msg.as_string())
            print("   - ✅ E-mail enviado com sucesso!")
            
        except Exception as e:
            print(f"   - ❌ Ocorreu um erro ao enviar o e-mail: {e}")
            
    elif not email_do_cliente:
        print("   - ⚠️ E-mail não enviado: Cliente sem e-mail de contato na tabela.")
    
    # --- F. Exibição do Resultado no Notebook ---
    print("\n--- Visualização do Relatório HTML Gerado ---")
    # Usamos a função HTML para renderizar o resultado diretamente no notebook
    display(HTML(texto_gerado_html))

    print("\n   - Aguardando 15 segundos para evitar o limite de quota...")
    time.sleep(15)




print("\n--- PROCESSAMENTO EM LOTE CONCLUÍDO ---")

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
