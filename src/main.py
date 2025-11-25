import os
import ssl
import requests
from requests.adapters import HTTPAdapter
from dotenv import load_dotenv
import json
import logging
import pandas as pd


# --- Variaveis de Ambiente ---
load_dotenv()
api_token = os.getenv("TOKEN_API_ARGUS")

path_log_file = "logs/argus.log"
path_data_file = "data/argus.xlsx"

data_inicial = "2025-11-10"
data_final = ""


# --- LOGGING ---
logging.config(
    filename="LOG_FILE",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# --- Funções ---
class TLSAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)

        ctx.set_ciphers("HIGH:!aNULL:!eNULL:!MD5:!RC4")
        kwargs["ssl_context"] = ctx
        return super(TLSAdapter, self).init_poolmanager(*args, **kwargs)


def salvar_json(data):
    '''
    Creates a JSON file from the response data.
    '''
    with open("resultado.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def fazer_requisicao(data_inicial, data_final):
    '''
    Makes a request to the Argus API and returns the JSON response.

    Parameters:
        data_inicial (str): Start date for the request (YYYY-MM-DD)
        data_final (str): End date for the request (YYYY-MM-DD)
    
    Returns:
        dict or list: Parsed JSON response from the API
    '''

    url = (
        f"https://argus.app.br/apiargus/report/tabulacoesdetalhadas?"
        f"periodoInicial={data_inicial}&periodoFinal={data_final}&idCampanha=1"
    )

    headers = {"Token-Signature": api_token}


    session = requests.Session()
    session.mount("https://", TLSAdapter())

    response = session.get(url, headers=headers)
    response.raise_for_status()
    dados = response.json()
    return dados


def salvar_arquivo(data):
    '''
    Save the response data in the Excel file   
    '''
    if not os.path.exists(path_data_file):
        os.makedirs(os.path.dirname(path_data_file), exist_ok=True)
    else:
        df = pd.DataFrame(data)
        df.to_excel(path_data_file)
        pass

def main():
    
    pass



if __name__ == "__main__":
    data = fazer_requisicao()
