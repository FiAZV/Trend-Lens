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
# META       "default_lakehouse_workspace_id": "41c1bbe3-1b20-4e9c-8afb-99229f989290"
# META     }
# META   }
# META }

# CELL ********************

import requests

API_KEY = "AIzaSyDmAULHlDdg3HNIGeE-k45IMxLj1XoH5CA"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import requests
from datetime import datetime
from notebookutils import mssparkutils

# Configurações da API
API_KEY = "AIzaSyDmAULHlDdg3HNIGeE-k45IMxLj1XoH5CA"
url = "https://www.googleapis.com/youtube/v3/videos"
params = {
    "key": API_KEY,
    "part": "snippet,statistics",
    "chart": "mostPopular",
    "maxResults": 50,
    "regionCode": "BR"
}

# Obter dados da API
response = requests.get(url, params=params)
raw_data = response.json()  # Dados brutos (dict ou lista)

# --------------------------------------------------
# Salvar JSON bruto na camada bronze (Files)
# --------------------------------------------------

# Criar caminho com timestamp para evitar sobrescritas
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
ingestion_date = datetime.now().strftime("%Y-%m-%d")

# Caminho completo no Lakehouse
base_path = f"Files/bronze/youtube/videos/ingestion_date={ingestion_date}"
file_name = f"raw_data_{timestamp}.json"
full_path = f"{base_path}/{file_name}"

# Criar diretório se não existir (usando mssparkutils)
mssparkutils.fs.mkdirs(base_path)

# Salvar o JSON bruto usando mssparkutils (evita erros de permissão)
mssparkutils.fs.put(
    file_name=full_path,
    contents=json.dumps(raw_data),
    overwrite=False
)

print(f"Arquivo salvo em: {full_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
