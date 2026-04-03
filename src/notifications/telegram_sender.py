import requests
from src.config.config import BOT_TOKEN

def send_telegram(chat_id, text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": text
    }

    response = requests.post(url, json=payload)
    return response.json()