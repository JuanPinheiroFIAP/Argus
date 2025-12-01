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
        f"https://argus.app.br/apiargus/report/tabulacoesdetalhadas?"
        f"idStatusLigacao=1&periodoInicial={data_inicial}&periodoFinal={data_final}&idCampanha=1&{idProxima}"
    )

    headers = {"Token-Signature": api_token}


    session = requests.Session()
    session.mount("https://", TLSAdapter())

    response = session.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    dados = response.json()
    return dados

def tratar_datas_api(tabulacoes):
    for item in tabulacoes:
        if 'dataHoraLigacao' in item and item['dataHoraLigacao']:
            dt = datetime.fromisoformat(item['dataHoraLigacao'])

            # remover timezone
            dt = dt.replace(tzinfo=None)

            # criar novos campos
            item['dataHoraLigacao'] = dt.date()
            item['horaLigacao'] = dt.time()

        if 'dataImportacao' in item and item['dataImportacao']:
            dt = datetime.fromisoformat(item['dataImportacao'])
            dt = dt.replace(tzinfo=None)

            item['dataImportacao'] = dt.date()
            item['horaImportacao'] = dt.time()

    return tabulacoes

resultado = []
def main():
    tentativa = 1
    pagina = 1
    id_proxima = 0

    caminho = os.path.join("Data", "Argus.xlsx")
    df = pd.DataFrame()

    logging.info("🚀 Início da execução do script")

    # ✅ DEFINIR data inicial corretamente
    if not os.path.exists(caminho):
        logging.info("Arquivo não encontrado, criando nova base de dados")
        data_inicial = datetime.strptime("2025-11-10", "%Y-%m-%d")
    else:
        logging.info("Arquivo encontrado, carregando dados existentes")
        df = pd.read_excel(caminho)

        df["dataHoraLigacao"] = pd.to_datetime(df["dataHoraLigacao"], errors="coerce").dt.date

        ultima_data = max(df["dataHoraLigacao"])
        logging.info(f"Última data salva: {ultima_data}")

        # Remove essa data para reprocessar completamente
        df = df[df["dataHoraLigacao"] != ultima_data]

        data_inicial = datetime.combine(ultima_data, datetime.min.time())

    while True:
        try:
            print(f"\n📅 Buscando data: {data_inicial.date()} | Página: {pagina} | Tentativa: {tentativa}")

            data = fazer_requisicao(data_inicial, data_inicial, id_proxima)

            if not data:
                raise Exception("Resposta vazia da API")

            registros = data.get("ligacoesDetalhadas", [])
            registros = tratar_datas_api(registros)
            id_proxima = data.get("idProxPagina")

            print(f"📦 Registros recebidos: {len(registros)}")

            for i in registros:
                tab = i.get('tabulacao', '').strip()
                if tab and tab.upper() != 'NÃO TABULADO':
                    
                    resultado.append(i)

            print(f"✅ Total acumulado: {len(resultado)}")

            # ✅ Se acabou paginação do dia, troca de data
            if not id_proxima:
                print("📆 Mudando para próxima data...\n")
                data_inicial += timedelta(days=1)
                id_proxima = 0
                pagina = 1

                # Parar se chegou na data atual
                if data_inicial.date() >= datetime.now().date() + timedelta(days=1):
                    print("🏁 Todas as datas processadas.")
                    break
            else:
                pagina += 1

            tentativa = 1

        except Exception as e:
            print("❌ Erro:", e)
            print("⏳ Aguardando 30 segundos...")
            time.sleep(30)
            tentativa += 1

    # ✅ SALVAR APENAS NO FINAL
    if resultado:
        logging.info(f"{len(resultado)} novos registros encontrados")

        df_novo = pd.DataFrame(resultado)
        df = pd.concat([df, df_novo], ignore_index=True)

        df["dataHoraLigacao"] = pd.to_datetime(df["dataHoraLigacao"]).dt.date
        df["dataImportacao"] = pd.to_datetime(df["dataImportacao"]).dt.date


        os.makedirs("Data", exist_ok=True)
        df.to_excel(caminho, index=False)

        logging.info("✅ Arquivo atualizado com sucesso")

    else:
        logging.info("Nenhum dado novo encontrado")

    logging.info("🏁 Processo finalizado")

if __name__ == "__main__":
    main()
