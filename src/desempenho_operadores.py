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

path_data_file = "data/argus_tabulacoes.xlsx"
path_data_dict = ""

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID_MS = os.getenv("CLIENT_ID_MS")
CLIENT_SECRET_MS = os.getenv("CLIENT_SECRET_MS")

DRIVE_ID = os.getenv("DRIVE_ID")
SHAREPOINT_PATH = os.getenv("SHAREPOINT_PATH", "/Relatórios BI/Base Argus")

PASTA_DESTINO = "Data"
os.makedirs(PASTA_DESTINO, exist_ok=True)
# =============================
# TOKEN MICROSOFT
# =============================
def get_access_token_ms():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    payload = {
        "client_id": CLIENT_ID_MS,
        "scope": "https://graph.microsoft.com/.default",
        "client_secret": CLIENT_SECRET_MS,
        "grant_type": "client_credentials"
    }
    response = requests.post(url, data=payload, timeout=15)
    response.raise_for_status()
    return response.json()['access_token']

# =============================
# UPLOAD SHAREPOINT
# =============================
def upload_files():
    token = get_access_token_ms()
    headers = {'Authorization': f'Bearer {token}'}

    for file in os.listdir(PASTA_DESTINO):
        path = os.path.join(PASTA_DESTINO, file)
        url = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/root:{SHAREPOINT_PATH}/{file}:/content"

        with open(path, 'rb') as f:
            r = requests.put(url, headers=headers, data=f)

        if r.status_code in [200, 201]:
            print(f"✅ Upload OK: {file}")
        else:
            print(f"❌ Erro {file}: {r.text}")


# --- LOGGING ---
os.makedirs("logs", exist_ok=True)
path_log_file = caminho = os.path.join('logs','argus.log')
logging.basicConfig(
    filename=path_log_file,
    level=logging.INFO,
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s'
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
    data_inicial = data_inicial.strftime("%Y-%m-%d")
    data_final = data_final.strftime("%Y-%m-%d")

    url = (
        f"https://argus.app.br/apiargus/report/desempenhoresumido?"
        f"periodoInicial={data_inicial}&periodoFinal={data_final}&idCampanha=1"
    )

    headers = {"Token-Signature": api_token}


    session = requests.Session()
    session.mount("https://", TLSAdapter())

    response = session.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    dados = response.json()
    return dados


# def salvar_arquivo(data):
#     caminho = os.path.join("Data", "Argus.xlsx")
#     os.makedirs(os.path.dirname(caminho), exist_ok=True)

#     # achatar lista de listas
#     registros = [item for sublist in data for item in sublist]

#     # criar DataFrame estruturado
#     df = pd.DataFrame.from_records(registros)

#     df.to_excel(caminho, index=False)
#     print("✅ Excel salvo com colunas corretas")
def tratar_datas_api(tabulacoes):
    for item in tabulacoes:
        if 'data' in item and item['data']:
            dt = datetime.fromisoformat(item['data'])

            # remover timezone
            dt = dt.replace(tzinfo=None)

            # criar novos campos
            item['data'] = dt.date()
            item['horaData'] = dt.time()

        if 'dataHoraLogin' in item and item['dataHoraLogin']:
            dt = datetime.fromisoformat(item['dataHoraLogin'])
            dt = dt.replace(tzinfo=None)

            item['dataHoraLogin'] = dt.date()
            item['horaLogin'] = dt.time()
        
        if 'dataHoraLogout' in item and item['dataHoraLogout']:
            dt = datetime.fromisoformat(item['dataHoraLogout'])
            dt = dt.replace(tzinfo=None)

            item['dataHoraLogout'] = dt.date()
            item['horaLogout'] = dt.time()


    return tabulacoes

resultado = []

def main():
    caminho = os.path.join("Data", "Argus_desempenho_operadores.xlsx")
    resultado = []
    df = pd.DataFrame()

    logging.info("🚀 Início da execução do script")

    if not os.path.exists(caminho):
        logging.info("Arquivo não encontrado, criando nova base de dados")

        data_inicial = datetime.strptime("2025-11-10", "%Y-%m-%d")
        data_final = data_inicial

        while True:
            try:
                logging.info(f"Consultando API para data: {data_inicial.date()}")

                data = fazer_requisicao(data_inicial, data_final)
                tabulacoes = tratar_datas_api(data.get("desempenhosResumidos", []))
                resultado.append(tabulacoes)
                

                data_inicial += timedelta(days=1)
                data_final = data_inicial

                if data_inicial >= datetime.now():
                    break

            except Exception as e:
                logging.exception(f"Erro ao processar data {data_inicial.date()}: {e}")

    else:
        logging.info(f"Arquivo encontrado, carregando dados existentes{caminho}")

        df = pd.read_excel(caminho)
        df['data'] = pd.to_datetime(df['data'], errors='coerce').dt.date

        ultima_data = max(df['data'])
        ultima_data  -= timedelta(days=1)
        print(ultima_data)
        logging.info(f"Última data salva no Excel: {ultima_data}")

        # Remover completamente a última data para reprocessar
        df = df[df['data'] != ultima_data]

        # Buscar novamente essa data
        data_inicial = datetime.combine(ultima_data, datetime.min.time())
        data_final = data_inicial

        while True:
            try:
                logging.info(f"Consultando API para data: {data_inicial.date()}")

                data = fazer_requisicao(data_inicial, data_final)
                tabulacoes = tratar_datas_api(data.get("desempenhosResumidos", []))
                resultado.append(tabulacoes)

                data_inicial += timedelta(days=1)
                data_final = data_inicial

                if data_inicial >= datetime.now():
                    break

            except Exception as e:
                logging.exception(f"Erro ao processar data {data_inicial.date()}: {e}")

    # Flatten da lista
    novos_registros = [item for sublist in resultado for item in sublist]

    if novos_registros:
        logging.info(f"{len(novos_registros)} novos registros encontrados")

        df_novo = pd.DataFrame.from_records(novos_registros)
        df = pd.concat([df, df_novo], ignore_index=True)

        df['data'] = pd.to_datetime(df['data'], errors='coerce').dt.date
        df.to_excel(caminho, index=False)

        logging.info("✅ Atualização concluída com sucesso")
    else:
        logging.info("Nenhum dado novo encontrado")

    logging.info("🏁 Execução finalizada")



if __name__ == "__main__":
    main()
    upload_files()
