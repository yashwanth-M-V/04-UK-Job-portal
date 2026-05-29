from datetime import datetime
import os
from dotenv import load_dotenv
import psycopg2
from supabase import create_client, Client

# Load environment variables
load_dotenv()

SILVER_DIR = "data/silver"


def get_today_partition():
    today = datetime.utcnow().date().isoformat()
    return f"{SILVER_DIR}/date={today}/jobs.parquet"


# ─────────────────────────────────────────────────────────────
# Environment Variables
# ─────────────────────────────────────────────────────────────

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

BUCKET_NAME = os.getenv("SUPABASE_BUCKET_NAME")
DATABASE_URL = os.getenv("CONNECTION_STRING")

BOT_TOKEN = os.getenv("BOT_TOKEN")

SEND_GRID_API_KEY = os.getenv("SEND_GRID_API_KEY")
FROM_EMAIL = os.getenv("FROM_EMAIL")


# ─────────────────────────────────────────────────────────────
# Debug Mode
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":

    print("\n===== ENVIRONMENT VARIABLES =====\n")

    print("SUPABASE_URL:", SUPABASE_URL)
    print("SUPABASE_KEY:", "Loaded ✅" if SUPABASE_KEY else "Missing ❌")
    print(
        "SUPABASE_SERVICE_KEY:",
        "Loaded ✅" if SUPABASE_SERVICE_KEY else "Missing ❌",
    )

    print("BUCKET_NAME:", BUCKET_NAME)
    print("DATABASE_URL:", "Loaded ✅" if DATABASE_URL else "Missing ❌")

    print("\n=================================\n")


# ─────────────────────────────────────────────────────────────
# Supabase Clients
# ─────────────────────────────────────────────────────────────

if not SUPABASE_URL:
    raise ValueError("SUPABASE_URL is missing")

if not SUPABASE_KEY:
    raise ValueError("SUPABASE_KEY is missing")

supabase: Client = create_client(
    SUPABASE_URL,
    SUPABASE_KEY
)

# Optional service client
supabase_service = None

if SUPABASE_SERVICE_KEY:
    supabase_service = create_client(
        SUPABASE_URL,
        SUPABASE_SERVICE_KEY
    )


# ─────────────────────────────────────────────────────────────
# Database Connection
# ─────────────────────────────────────────────────────────────

def connect_to_DB():

    if not DATABASE_URL:
        raise ValueError("DATABASE_URL is missing")

    conn = psycopg2.connect(DATABASE_URL)

    print("Connected to database successfully ✅")

    return conn