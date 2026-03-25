import os
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client
import psycopg2

from src.config.config import connect_to_DB

load_dotenv()

SUPABASE_URL = os.getenv("API_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BUCKET_NAME = os.getenv("SUPABASE_BUCKET_NAME")
DATABASE_URL = os.getenv("CONNECTION_STRING")


print("Loaded KEY exists:", SUPABASE_KEY is not None)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


DATA_DIR = "data"
SILVER_DIR = "data/silver"

def convert_csv_to_parquet():
    csv_path = os.path.join(DATA_DIR, "jobs_silver.csv")
    parquet_path = os.path.join(SILVER_DIR, "job_postings.parquet")
    
    if not os.path.exists(csv_path):
        print(f"❌ File not found: {csv_path}")
        return
    
    df = pd.read_csv(csv_path)
    df.to_parquet(parquet_path, index=False)
    
    print(f"✅ Converted {csv_path} to {parquet_path}")
    

def upload_parquet_files():
    paths = [
        "data/jobs_silver.parquet",
        "data/silver/2026-03-23/jobs.parquet"
    ]

    for file_path in paths:
        if not os.path.exists(file_path):
            print(f"Skipping missing file: {file_path}")
            continue

        with open(file_path, "rb") as f:
            supabase.storage.from_(BUCKET_NAME).upload(
                file_path,
                f,
                {"upsert": "true"}
            )

        print(f"Uploaded: {file_path}")


def migrate_gold_to_db():
    gold_csv = "data/jobs_gold.csv"

    if not os.path.exists(gold_csv):
        print("Gold CSV not found")
        return

    df = pd.read_csv(gold_csv)

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    inserted = 0

    for _, row in df.iterrows():
        cur.execute("""
        INSERT INTO job_postings (
            site, title, company, location,
            date_posted, job_type, job_url, description
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (job_url) DO NOTHING
        """, (
            row["site"],
            row["title"],
            row["company"],
            row["location"],
            row["date_posted"],
            row["job_type"],
            row["job_url"],
            row.get("description")
        ))

        inserted += 1

    conn.commit()
    conn.close()

    print(f"Migrated {inserted} rows into Supabase")
    
    
if __name__ == "__main__":
    print("Starting data migration...\n")

    convert_csv_to_parquet()
    upload_parquet_files()
    migrate_gold_to_db()

    print("\nMigration complete.")