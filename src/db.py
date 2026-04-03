from supabase import create_client
from src.config.config import SUPABASE_URL, SUPABASE_KEY

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_active_subscribers():
    response = supabase.table("subscribers")\
        .select("*")\
        .eq("is_active", True)\
        .execute()
    return response.data


def get_updates():
    response = supabase.table("updates")\
        .select("*")\
        .eq("is_active", True)\
        .execute()
    return response.data


def insert_log(subscriber_id, update_id, message, status, error=None):
    supabase.table("message_logs").insert({
        "subscriber_id": subscriber_id,
        "update_id": update_id,
        "channel": "telegram",
        "message": message,
        "status": status,
        "error_message": error
    }).execute()


def get_unsent_updates(subscriber):
    subscriber_id = subscriber["id"]
    joined_at = subscriber["created_at"]

    # Step 1: get already sent updates
    sent_updates = supabase.table("message_logs")\
        .select("update_id")\
        .eq("subscriber_id", subscriber_id)\
        .execute()

    sent_ids = [item["update_id"] for item in sent_updates.data]

    # Step 2: fetch only relevant updates
    query = supabase.table("updates")\
        .select("*")\
        .gte("created_at", joined_at)\
        .eq("is_active", True)\
        .limit(5)\
        .order("created_at", desc=True)

    if sent_ids:
        query = query.not_.in_("id", sent_ids)

    response = query.execute()

    return response.data

def update_last_sent(subscriber_id, today):
    supabase.table("subscribers")\
        .update({"last_sent_date": str(today)})\
        .eq("id", subscriber_id)\
        .execute()