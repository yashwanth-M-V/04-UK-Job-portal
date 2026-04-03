from datetime import datetime
import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from supabase import create_client, Client


load_dotenv()

SILVER_DIR = "data/silver"

def get_today_partition():
    today = datetime.utcnow().date().isoformat()
    return f"{SILVER_DIR}/date = {today}/jobs.parquet"

# Supabase configuration

SUPABASE_URL= os.getenv("API_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BUCKET_NAME = os.getenv("SUPABASE_BUCKET_NAME")
DATABASE_URL = os.getenv("CONNECTION_STRING")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
supabase_service = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY) 

# Telegram
BOT_TOKEN = os.getenv("BOT_TOKEN")

# SendGrid
SEND_GRID_API_KEY = os.getenv("SEND_GRID_API_KEY")
FROM_EMAIL        = os.getenv("FROM_EMAIL")


def connect_to_DB():
    conn = psycopg2.connect(DATABASE_URL)
    print("Connected to the database successfully!")
    return conn