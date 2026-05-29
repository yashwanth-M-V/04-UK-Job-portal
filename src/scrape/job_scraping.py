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

# ─── Data Engineering keywords ───────────────────────────────────────────────
DE_KEYWORDS = [
    "junior data engineer",
    "graduate data engineer",
    "entry level data engineer",
    "associate data engineer",
    "trainee data engineer",
]

# ─── Healthcare keywords ──────────────────────────────────────────────────────
HC_KEYWORDS = [
    "health care assistant",
    "healthcare assistant",
    "HCA healthcare",
    "support worker",
    "care support worker",
    "community support worker",
]

# ─── Role validation patterns ─────────────────────────────────────────────────
DE_PATTERNS = [
    r"\bdata engineer\b",
    r"\banalytics engineer\b",
    r"\bdata platform engineer\b",
    r"\bmachine learning engineer\b",
    r"\betl developer\b",
    r"\bbig data engineer\b",
    r"\bcloud data engineer\b",
    r"\bbi engineer\b",
]

HC_PATTERNS = [
    r"\bhealthcare assistant\b",
    r"\bhealth care assistant\b",
    r"\bhca\b",
    r"\bsupport worker\b",
    r"\bcare assistant\b",
    r"\bcommunity support worker\b",
    r"\bresidential support worker\b",
    r"\bcare support worker\b",
]


def is_data_role(title: str) -> bool:
    if not isinstance(title, str):
        return False
    return any(re.search(p, title.lower()) for p in DE_PATTERNS)


def is_healthcare_role(title: str) -> bool:
    if not isinstance(title, str):
        return False
    return any(re.search(p, title.lower()) for p in HC_PATTERNS)


def _scrape_category(keywords: list[str], location: str, label: str) -> pd.DataFrame:
    """Scrape jobs for a list of keywords at a given location. Returns raw df."""
    all_jobs = []

    for keyword in tqdm(keywords, desc=f"Scraping {label}"):
        jobs = scrape_jobs(
            site_name=["indeed", "linkedin", "google"],
            search_term=keyword,
            location=location,
            results_wanted=50,
            hours_old=72,
            country_indeed="United Kingdom",
        )
        if jobs is not None and not jobs.empty:
            all_jobs.append(jobs)

    if not all_jobs:
        return pd.DataFrame()

    return pd.concat(all_jobs, ignore_index=True)


def scrape_to_silver(hours_old: int = 72):
    print("\nStarting scraping process\n")

    frames = []

    # ── 1. Data Engineering jobs — UK-wide ───────────────────────────────────
    print("── Data Engineering (UK) ──")
    de_df = _scrape_category(DE_KEYWORDS, "United Kingdom", "Data Engineering")
    if not de_df.empty:
        de_df = de_df.drop_duplicates(subset=["site", "job_url"])
        tqdm.pandas(desc="Filtering DE roles")
        de_df = de_df[de_df["title"].progress_apply(is_data_role)]
        de_df["category"] = "data_engineering"
        frames.append(de_df)
        print(f"  → {len(de_df)} valid Data Engineering roles")

    # ── 2. Healthcare jobs — Manchester ──────────────────────────────────────
    print("\n── Healthcare / Support Worker (Manchester + UK) ──")
    hc_manchester = _scrape_category(HC_KEYWORDS, "Manchester, UK", "Healthcare Manchester")
    hc_uk         = _scrape_category(HC_KEYWORDS, "United Kingdom",  "Healthcare UK")

    hc_df = pd.concat([hc_manchester, hc_uk], ignore_index=True) if any(
        not f.empty for f in [hc_manchester, hc_uk]
    ) else pd.DataFrame()

    if not hc_df.empty:
        hc_df = hc_df.drop_duplicates(subset=["site", "job_url"])
        tqdm.pandas(desc="Filtering HC roles")
        hc_df = hc_df[hc_df["title"].progress_apply(is_healthcare_role)]
        hc_df["category"] = "healthcare"
        frames.append(hc_df)
        print(f"  → {len(hc_df)} valid Healthcare roles")

    if not frames:
        raise RuntimeError("No jobs scraped across any category")

    # ── 3. Combine ────────────────────────────────────────────────────────────
    df = pd.concat(frames, ignore_index=True)
    df["date_posted"] = pd.to_datetime(df["date_posted"], errors="coerce")
    df = df[df["date_posted"].notna()]
    df["scraped_at"] = pd.Timestamp.utcnow()

    print(f"\nTotal jobs after cleaning: {len(df)}")

    # ── 4. Temp silver file ───────────────────────────────────────────────────
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
    silver_path = tmp.name
    tmp.close()
    df.to_parquet(silver_path, index=False)
    print(f"Temporary silver file: {silver_path}")

    # ── 5. Archive to Supabase storage ───────────────────────────────────────
    today        = datetime.utcnow().date().isoformat()
    storage_path = f"silver/date-{today}/jobs.parquet"
    print("Uploading archive to Supabase storage")

    archive_tmp  = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
    archive_path = archive_tmp.name
    archive_tmp.close()

    try:
        existing    = supabase.storage.from_(BUCKET_NAME).download(storage_path)
        old_df      = pd.read_parquet(BytesIO(existing))
        combined_df = pd.concat([old_df, df], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=["site", "job_url"])
        combined_df.to_parquet(archive_path, index=False)
        supabase.storage.from_(BUCKET_NAME).remove([storage_path])
        print("  Existing archive found — appending data")
    except Exception:
        print("  Creating new archive file")
        df.to_parquet(archive_path, index=False)

    supabase.storage.from_(BUCKET_NAME).upload(storage_path, archive_path)
    os.remove(archive_path)
    print("Archive uploaded successfully")

    return df, silver_path


if __name__ == "__main__":
    scrape_to_silver()