import os
import re
import pandas as pd
from jobspy import scrape_jobs
from tqdm import tqdm

# ---------------------------
# Search terms
# ---------------------------
JUNIOR_KEYWORDS = [
    "junior data engineer",
    "graduate data engineer",
    "entry level data engineer",
    "associate data engineer",
    "trainee data engineer",
]

# ---------------------------
# Silver path (source of truth)
# ---------------------------
SILVER_PATH = "data/jobs_silver.parquet"


# ---------------------------
# Title filter
# ---------------------------
def is_data_role(title: str) -> bool:
    if not isinstance(title, str):
        return False

    title = title.lower()

    patterns = [
        r"\bdata engineer\b",
        r"\banalytics engineer\b",
        r"\bplatform engineer\b.*\bdata\b",
        r"\bdata platform engineer\b",
        r"\bmachine learning engineer\b",
        r"\bml engineer\b",
        r"\bdatabase administrator\b",
        r"\bdatabase engineer\b",
        r"\betl developer\b",
        r"\bdata reliability engineer\b",
        r"\bbig data engineer\b",
        r"\bdata architect\b",
        r"\bcloud data engineer\b",
        r"\bbi engineer\b",
        r"\bbusiness intelligence engineer\b",
    ]

    return any(re.search(pattern, title) for pattern in patterns)


# ---------------------------
# Scrape → Silver
# ---------------------------
def scrape_to_silver(hours_old: int = 72) -> pd.DataFrame:
    print("\n🔎 Starting job scraping...\n")

    all_jobs = []
    total_scraped = 0

    # Progress bar for keywords
    for keyword in tqdm(JUNIOR_KEYWORDS, desc="Scraping job keywords"):
        jobs = scrape_jobs(
            site_name=["indeed", "linkedin", "google"],
            search_term=keyword,
            location="United Kingdom",
            results_wanted=50,
            hours_old=hours_old,
            country_indeed="United Kingdom",
        )

        if jobs is not None and not jobs.empty:
            count = len(jobs)
            total_scraped += count
            tqdm.write(f"Collected {count} jobs for '{keyword}'")
            all_jobs.append(jobs)

    if not all_jobs:
        raise RuntimeError("No jobs scraped from any source")

    print(f"\n📦 Total raw jobs collected: {total_scraped}")

    df = pd.concat(all_jobs, ignore_index=True)

    # ---------------------------
    # Cleaning stage
    # ---------------------------
    print("\n🧹 Cleaning and filtering jobs...")

    # Remove duplicates
    df = df.drop_duplicates(subset=["site", "job_url"])

    # Filter only relevant roles (with progress bar)
    tqdm.pandas(desc="Filtering job titles")
    df = df[df["title"].progress_apply(is_data_role)]

    if df.empty:
        raise RuntimeError("No data-engineering roles found after filtering")

    print(f"Remaining jobs after filtering: {len(df)}")

    # ---------------------------
    # Date handling
    # ---------------------------
    df["date_posted"] = pd.to_datetime(df["date_posted"], errors="coerce")
    df["date_posted"] = df["date_posted"].dt.tz_localize(None)

    # Remove rows without valid date
    df = df[df["date_posted"].notna()]

    # Tag when pipeline scraped this job
    df["scraped_at"] = pd.Timestamp.utcnow().replace(tzinfo=None)

    # ---------------------------
    # Save to Silver (Parquet history)
    # ---------------------------
    print("\n💾 Updating Silver dataset...")

    os.makedirs(os.path.dirname(SILVER_PATH), exist_ok=True)

    if os.path.exists(SILVER_PATH):
        existing = pd.read_parquet(SILVER_PATH)

        before = len(existing)

        df = pd.concat([existing, df], ignore_index=True)
        df = df.drop_duplicates(subset=["site", "job_url"], keep="first")

        after = len(df)
        added = after - before

        print(f"New unique jobs added: {added}")

    # Save updated dataset
    df = df.reset_index(drop=True)
    df.to_parquet(SILVER_PATH, index=False)

    print(f"\n✅ Silver saved → {SILVER_PATH}")
    print(f"📊 Total jobs stored in history: {len(df)}\n")

    return df