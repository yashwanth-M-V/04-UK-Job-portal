import os
import re
import pandas as pd
from jobspy import scrape_jobs
from tqdm import tqdm
from datetime import datetime
from dotenv import load_dotenv
import tempfile
from io import BytesIO
from src.config.config import BUCKET_NAME, supabase

load_dotenv()

JUNIOR_KEYWORDS = [
    "junior data engineer",
    "graduate data engineer",
    "entry level data engineer",
    "associate data engineer",
    "trainee data engineer",
]


def is_data_role(title: str) -> bool:
    if not isinstance(title, str):
        return False
    title = title.lower()

    patterns = [
        r"\bdata engineer\b",
        r"\banalytics engineer\b",
        r"\bdata platform engineer\b",
        r"\bmachine learning engineer\b",
        r"\betl developer\b",
        r"\bbig data engineer\b",
        r"\bcloud data engineer\b",
        r"\bbi engineer\b",
    ]

    return any(re.search(p, title) for p in patterns)


def scrape_to_silver(hours_old: int = 72):
    print("\nStarting scraping process\n")

    all_jobs = []
    total = 0

    for keyword in tqdm(JUNIOR_KEYWORDS, desc="Scraping"):
        jobs = scrape_jobs(
            site_name=["indeed", "linkedin", "google"],
            search_term=keyword,
            location="United Kingdom",
            results_wanted=50,
            hours_old=hours_old,
            country_indeed="United Kingdom",
        )

        if jobs is not None and not jobs.empty:
            all_jobs.append(jobs)
            total += len(jobs)

    if not all_jobs:
        raise RuntimeError("No jobs scraped")

    df = pd.concat(all_jobs, ignore_index=True)

    print(f"Total jobs scraped: {total}")

    df = df.drop_duplicates(subset=["site", "job_url"])

    tqdm.pandas(desc="Filtering roles")
    df = df[df["title"].progress_apply(is_data_role)]

    if df.empty:
        raise RuntimeError("No valid data roles found")

    df["date_posted"] = pd.to_datetime(df["date_posted"], errors="coerce")
    df = df[df["date_posted"].notna()]
    df["scraped_at"] = pd.Timestamp.utcnow()

    print(f"Jobs after cleaning: {len(df)}")

    # ------------------------
    # TEMP SILVER FILE
    # ------------------------
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
    silver_path = tmp.name
    df.to_parquet(silver_path, index=False)

    print(f"Temporary silver file created: {silver_path}")

    # ------------------------
    # ARCHIVE STORAGE
    # ------------------------
    today = datetime.utcnow().date().isoformat()
    storage_path = f"silver/date-{today}/jobs.parquet"

    print("Uploading archive to Supabase storage")

    try:
        existing = supabase.storage.from_(BUCKET_NAME).download(storage_path)
        old_df = pd.read_parquet(BytesIO(existing))
        df_archive = pd.concat([old_df, df], ignore_index=True)
    except Exception:
        df_archive = df

    archive_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
    archive_path = archive_tmp.name
    archive_tmp.close()
    df_archive.to_parquet(archive_path, index=False)
    
    try:
        # Try to download existing file
        existing = supabase.storage.from_(BUCKET_NAME).download(storage_path)

        old_df = pd.read_parquet(BytesIO(existing))
        combined_df = pd.concat([old_df, df], ignore_index=True)

        # Optional but recommended
        combined_df = combined_df.drop_duplicates(subset=["site", "job_url"])

        combined_df.to_parquet(archive_path, index=False)

        # delete old file before upload
        supabase.storage.from_(BUCKET_NAME).remove([storage_path])

        print("Existing archive found — appending data")

    except Exception:
        # No file exists yet
        print("Creating new archive file")
        df.to_parquet(archive_path, index=False)

    # Upload new file
    supabase.storage.from_(BUCKET_NAME).upload(
        storage_path,
        archive_path
    )

    os.remove(archive_path)

    print("Archive uploaded successfully")

    return df, silver_path


if __name__ == "__main__":
    scrape_to_silver()