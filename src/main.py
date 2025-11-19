import os
import ssl
import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from dotenv import load_dotenv


class TLSAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        
        ctx.set_ciphers('HIGH:!aNULL:!eNULL:!MD5:!RC4')
        kwargs['ssl_context'] = ctx
        return super(TLSAdapter, self).init_poolmanager(*args, **kwargs)


load_dotenv()
api_token = os.getenv("TOKEN")

url = "https://argus.app.br/apiargus/report/tabulacoesdetalhadas?periodoInicial=2025-11-17&periodoFinal=2025-11-17&idCampanha=1"

headers = {
    "Token-Signature": api_token
}

session = requests.Session()
session.mount("https://", TLSAdapter())

response = session.get(url, headers=headers)

print(response.status_code)
print(response.text)
