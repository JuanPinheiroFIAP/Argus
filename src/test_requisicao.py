import os
import ssl
import requests
from requests.adapters import HTTPAdapter
from dotenv import load_dotenv
import json
import logging
import pandas as pd
from datetime import datetime, timedelta
import time

# --- Variaveis de Ambiente ---
load_dotenv()
api_token = os.getenv("TOKEN_API_ARGUS")

path_log_file = "logs/argus.log"
path_data_file = "data/argus.xlsx"
path_data_dict = ""




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

def fazer_requisicao(data_inicial, data_final, idProxima):
    '''
    Makes a request to the Argus API and returns the JSON response.

    Parameters:
        data_inicial (str): Start date for the request (YYYY-MM-DD)
        data_final (str): End date for the request (YYYY-MM-DD)
    
    Returns:
        dict or list: Parsed JSON response from the API
    '''
    data_inicial = data_inicial.strftime("%Y-%m-%d")
    data_final = data_final.strftime("%Y-%m-%d")

    url = (
        f"https://argus.app.br/apiargus/report/ligacoesdetalhadas?"
        f"periodoInicial={data_inicial}&periodoFinal={data_final}&idCampanha=1&{idProxima}"
    )

    headers = {"Token-Signature": api_token}


    session = requests.Session()
    session.mount("https://", TLSAdapter())

    response = session.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    dados = response.json()
    return dados

resultado = []
def main():
    data_inicial = datetime.strptime("2025-11-14", "%Y-%m-%d")
    data_final = data_inicial
    id_proxima = 0

    while True:
        data = fazer_requisicao(data_inicial, data_final,id_proxima)
        id_proxima = data.get("idProxPagina")

        for i in data.get("ligacoesDetalhadas"):
            tab = i.get('tabulcao')
            if tab or tab != 'NÃO TABULADO':
                resultado.append(i)

        if resultado:
            df = pd.DataFrame(resultado)

            # Converter dataHoraLigacao corretamente
            df["dataHoraLigacao"] = pd.to_datetime(df["dataHoraLigacao"], errors="coerce")
            df["dataImportacao"] = pd.to_datetime(df["dataImportacao"], errors="coerce")

            pasta = "Data"
            os.makedirs(pasta, exist_ok=True)
            caminho = os.path.join(pasta, "teste.xlsx")

            df.to_excel(caminho, index=False)
            print(f"💾 Arquivo salvo com sucesso em: {caminho}")
        else:
            print("⚠️ Nenhum registro válido encontrado.")

        
main()