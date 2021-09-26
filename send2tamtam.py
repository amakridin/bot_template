import requests
from urllib.parse import urljoin

URL_MESSAGE = "https://botapi.tamtam.chat/messages"

def send_json(chat_id, jsn, token):
    """format is enum: markdown, html"""
    url_init = urljoin(URL_MESSAGE, f"?chat_id={chat_id}&access_token={token}")
    requests.post(url_init, json=jsn)
