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

# Criar estrutura de pastas no Lakehouse
import os

# Caminho base para o Lakehouse
BASE_PATH = "/lakehouse/default/Files/raw/reddit"

# Pastas a serem criadas
pastas = ["posts", "usuarios", "comunidades"]

# Criar cada pasta
for pasta in pastas:
    caminho_pasta = f"{BASE_PATH}/{pasta}"
    try:
        os.makedirs(caminho_pasta, exist_ok=True)
        print(f"✅ Pasta criada: {caminho_pasta}")
    except Exception as e:
        print(f"❌ Erro ao criar pasta {caminho_pasta}: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Instala a biblioteca praw**

# CELL ********************

%pip install praw


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Código que realiza a coleta dos dados**

# CELL ********************

import praw
import json
import datetime
import os

# Configuração da API do Reddit
reddit = praw.Reddit(
    client_id="R9RUY5bNncIX4WUrCk5Nlg",
    client_secret="u3y4MeGXr5NA999-OUek6IMDJPtkYg",
    user_agent="Trend-Lens:1.0 (by u/Dry-Peace-8127)",
)

# Caminho base para salvar os arquivos JSON Lines
BASE_PATH = "/lakehouse/default/Files/raw/reddit"

# Função para salvar dados em JSON Lines
def salvar_jsonl(pasta, nome_arquivo, dados):
    # Cria a pasta se não existir
    caminho_pasta = f"{BASE_PATH}/{pasta}"
    os.makedirs(caminho_pasta, exist_ok=True)
    
    # Caminho completo do arquivo
    caminho_arquivo = f"{caminho_pasta}/{nome_arquivo}"
    
    # Salva cada dicionário como uma linha JSON
    with open(caminho_arquivo, "w", encoding="utf-8") as f:
        for item in dados:
            json.dump(item, f, ensure_ascii=False)
            f.write('\n')  # Adiciona uma nova linha após cada item
    
    print(f"✅ Arquivo salvo em {caminho_arquivo}")

# Obtendo os posts mais populares do momento
def coletar_posts_hot(subreddit_nome="all"):
    subreddit = reddit.subreddit(subreddit_nome)
    posts_lista = []

    for post in subreddit.hot(limit=50):
        post_info = {
            "id": post.id,
            "titulo": post.title,
            "autor": post.author.name if post.author else "[Deletado]",
            "upvotes": post.score,
            "comentarios": post.num_comments,
            "link": post.url,
            "subreddit": post.subreddit.display_name,
            "data_postagem": datetime.datetime.fromtimestamp(post.created_utc).isoformat(),
            "data_coleta": datetime.datetime.now().isoformat()
        }
        posts_lista.append(post_info)
    
    data_atual = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    nome_arquivo = f"posts_hot-{data_atual}.jsonl"  # Alterado para .jsonl
    
    salvar_jsonl("posts", nome_arquivo, posts_lista)  # Usando a nova função
    
    return posts_lista

def coletar_info_usuarios(posts_lista):
    usuarios_data = []
    usuarios_coletados = set()  # Para evitar duplicatas

    for post in posts_lista:
        nome_usuario = post["autor"]
        if nome_usuario == "[Deletado]" or nome_usuario in usuarios_coletados:
            continue  # Ignorar usuários deletados e evitar repetir dados

        try:
            usuario = reddit.redditor(nome_usuario)
            user_info = {
                "nome": usuario.name,
                "karma_total": usuario.link_karma + usuario.comment_karma,
                "karma_post": usuario.link_karma,
                "karma_comentario": usuario.comment_karma,
                "data_criacao": datetime.datetime.fromtimestamp(usuario.created_utc).isoformat(),
                "premium": usuario.is_gold,
                "data_coleta": datetime.datetime.now().isoformat()
            }
            usuarios_data.append(user_info)
            usuarios_coletados.add(nome_usuario)  
        except Exception as e:
            print(f"❌ Erro ao coletar usuário {nome_usuario}: {e}")

    data_atual = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    nome_arquivo = f"usuarios_hot-{data_atual}.jsonl"  # Alterado para .jsonl

    salvar_jsonl("usuarios", nome_arquivo, usuarios_data)  # Usando a nova função
    
    return usuarios_data

def coletar_comunidades(posts_lista):
    comunidades_data = []

    for post in posts_lista:
        comunidade_info = {
            "post_id": post["id"],
            "subreddit": post["subreddit"],
            "data_coleta": datetime.datetime.now().isoformat()
        }
        comunidades_data.append(comunidade_info)

    data_atual = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    nome_arquivo = f"comunidades_hot-{data_atual}.jsonl"  # Alterado para .jsonl
    
    salvar_jsonl("comunidades", nome_arquivo, comunidades_data)  # Usando a nova função

    return comunidades_data

# Função principal para executar a coleta de dados
def executar_coleta():
    print("Iniciando coleta de dados do Reddit...")
    posts = coletar_posts_hot()
    usuarios = coletar_info_usuarios(posts)
    comunidades = coletar_comunidades(posts)
    print("Coleta de dados concluída com sucesso!")
    return {
        "posts": len(posts),
        "usuarios": len(usuarios),
        "comunidades": len(comunidades)
    }

# Executar a coleta quando o script for executado diretamente
if __name__ == "__main__":
    resultados = executar_coleta()
    print(f"Resumo da coleta: {resultados}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
