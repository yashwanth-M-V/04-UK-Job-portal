import requests
import time
import supabase
from src.config.config import BOT_TOKEN, supabase

last_update_id = None


def send_message(chat_id, text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    requests.post(url, json={
        "chat_id": chat_id,
        "text": text
    })


def fetch_updates():
    global last_update_id

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    params = {"timeout": 10}

    if last_update_id:
        params["offset"] = last_update_id + 1

    response = requests.get(url, params=params).json()

    # 👇 Add this to see what Telegram is actually saying
    if not response.get("ok"):
        print("Telegram error:", response)
        return []

    return response["result"]


def handle_users():
    updates = fetch_updates()

    for update in updates:
        if "message" not in update:
            continue

        message = update["message"]
        text = message.get("text", "")
        chat_id = str(message["chat"]["id"])

        existing = supabase.table("subscribers")\
            .select("*")\
            .eq("telegram_chat_id", chat_id)\
            .execute()

        user = existing.data[0] if existing.data else None

        # START
        if text == "/start":
            if not user:
                supabase.table("subscribers").insert({
                    "telegram_chat_id": chat_id,
                    "preferred_channel": "telegram",
                    "channel_verified": True,
                    "is_active": True
                }).execute()

                send_message(chat_id, "🚀 Welcome aboard!\n\nYou’ll receive the top Data Engineering jobs every day at 9 PM.\n\nNo spam. Just the best opportunities.\n\nLet’s get you hired 💼")

            else:
                supabase.table("subscribers")\
                    .update({"is_active": True})\
                    .eq("telegram_chat_id", chat_id)\
                    .execute()

                send_message(chat_id, "🎉 Welcome back!\n\nYou’re subscribed again and will receive the top Data Engineering jobs every day at 9 PM.\n\nLet’s get you hired 💼")

        # STOP
        elif text == "/stop":
            if user:
                supabase.table("subscribers")\
                    .update({"is_active": False})\
                    .eq("telegram_chat_id", chat_id)\
                    .execute()

                send_message(chat_id, """🛑 You’ve been unsubscribed.

                                        No more job alerts will be sent.

                                        If you change your mind, just type /start 🙂""")


if __name__ == "__main__":
    print("🤖 Bot is running locally...")

    while True:
        try:
            handle_users()
            time.sleep(3)  # check every 3 sec

        except Exception as e:
            print("Error:", e)
            time.sleep(5)