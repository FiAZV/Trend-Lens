# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%pip install -q -U google-generativeai

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

GEMINI_API_KEY = "AIzaSyAgH0A_bQtMAObhHJKqq0DZbfqSTnOnZGQ"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import google.generativeai as genai

# Configure o SDK do Gemini com a chave obtida de forma segura
genai.configure(api_key=GEMINI_API_KEY)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time

# Seleciona o modelo a ser usado
model = genai.GenerativeModel('gemini-1.5-flash') # Modelo rápido e de baixo custo, ideal para tarefas em lote

def analisar_sentimento_com_gemini(texto_feedback):
    """
    Função que envia um texto para a API do Gemini e retorna o sentimento.
    """
    if not texto_feedback or not isinstance(texto_feedback, str):
        return "Texto inválido"

    prompt = f"""
    Analise o sentimento do seguinte feedback de cliente.
    Responda apenas com uma das seguintes palavras: Positivo, Negativo ou Neutro.

    Feedback: "{texto_feedback}"
    Sentimento:
    """

    try:
        response = model.generate_content(prompt)
        # Adicionar um pequeno delay para não sobrecarregar a API
        time.sleep(1) 
        return response.text.strip()
    except Exception as e:
        # É importante tratar exceções, pois chamadas de API podem falhar
        return f"Erro na API: {str(e)}"

# Teste rápido da função
print(analisar_sentimento_com_gemini("O produto é incrível e a entrega foi super rápida!"))
print(analisar_sentimento_com_gemini("Achei o material de baixa qualidade e demorou para chegar."))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
