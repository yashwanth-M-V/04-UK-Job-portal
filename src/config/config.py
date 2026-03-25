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


SUPABASE_URL= os.getenv("API_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BUCKET_NAME = os.getenv("SUPABASE_BUCKET_NAME")
DATABASE_URL = os.getenv("CONNECTION_STRING")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def connect_to_DB():
    conn = psycopg2.connect(DATABASE_URL)
    print("Connected to the database successfully!")
    return conn