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
path_data_file = "data/argus_tabulacoes.xlsx"
path_data_dict = ""




# --- LOGGING ---
logging.basicConfig(
    filename="logs/argus.log",
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
    data_inicial = data_inicial.strftime("%Y-%m-%d")
    data_final = data_final.strftime("%Y-%m-%d")

    url = (
        f"https://argus.app.br/apiargus/report/tabulacoesdetalhadas?"
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
        if 'dataEvento' in item and item['dataEvento']:
            dt = datetime.fromisoformat(item['dataEvento'])

            # remover timezone
            dt = dt.replace(tzinfo=None)

            # criar novos campos
            item['dataEvento'] = dt.date()
            item['horaEvento'] = dt.time()

        if 'dataImportacao' in item and item['dataImportacao']:
            dt = datetime.fromisoformat(item['dataImportacao'])
            dt = dt.replace(tzinfo=None)

            item['dataImportacao'] = dt.date()
            item['horaImportacao'] = dt.time()

    return tabulacoes

resultado = []

def main():
    caminho = os.path.join("Data", "Argus_tabulacoes.xlsx")
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
                tabulacoes = tratar_datas_api(data.get("ligacoesDetalhadas", []))
                resultado.append(tabulacoes)

                data_inicial += timedelta(days=1)
                data_final = data_inicial

                if data_inicial >= datetime.now():
                    break

            except Exception as e:
                logging.exception(f"Erro ao processar data {data_inicial.date()}: {e}")

    else:
        logging.info("Arquivo encontrado, carregando dados existentes")

        df = pd.read_excel(caminho)
        df['dataEvento'] = pd.to_datetime(df['dataEvento'], errors='coerce').dt.date

        ultima_data = max(df['dataEvento'])
        logging.info(f"Última data salva no Excel: {ultima_data}")

        # Remover completamente a última data para reprocessar
        df = df[df['dataEvento'] != ultima_data]

        # Buscar novamente essa data
        data_inicial = datetime.combine(ultima_data, datetime.min.time())
        data_final = data_inicial

        while True:
            try:
                logging.info(f"Consultando API para data: {data_inicial.date()}")

                data = fazer_requisicao(data_inicial, data_final)
                tabulacoes = tratar_datas_api(data.get("ligacoesDetalhadas", []))
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

        df['dataEvento'] = pd.to_datetime(df['dataEvento'], errors='coerce').dt.date
        df.to_excel(caminho, index=False)

        logging.info("✅ Atualização concluída com sucesso")
    else:
        logging.info("Nenhum dado novo encontrado")

    logging.info("🏁 Execução finalizada")



if __name__ == "__main__":
    main()
