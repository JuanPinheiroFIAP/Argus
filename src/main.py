import os 
import pandas as pd 
import requests
from dotenv import load_dotenv

load_dotenv()
api_token = os.getenv("TOKEN")
url = "https://argus.app.br/apiargus/report/tabulacoesdetalhadas?periodoInicial=2025-11-17&periodoFinal=2025-11-17&idCampanha=1"

header = {
    "Token-Signature" : api_token
}
response = requests.get(url, headers=header)
print(response.json())