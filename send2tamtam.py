import requests
from urllib.parse import urljoin
import config



def send_json(chat_id, jsn):
    """format is enum: markdown, html"""
    url_init = urljoin(config.TAM_TAM_URL_MESSAGE, f"?chat_id={chat_id}&access_token={config.TAM_TAM_TOKEN}")
    requests.post(url_init, json=jsn)
