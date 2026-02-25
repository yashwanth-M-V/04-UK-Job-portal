import csv
import os
import re
import pandas as pd
from jobspy import scrape_jobs

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
SILVER_PATH = "data/jobs_silver.csv"


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
    all_jobs = []

    for keyword in JUNIOR_KEYWORDS:
        jobs = scrape_jobs(
            site_name=["indeed", "linkedin", "google"],
            search_term=keyword,
            location="United Kingdom",
            results_wanted=50,
            hours_old=hours_old,
            country_indeed="United Kingdom",
        )
        if not jobs.empty:
            all_jobs.append(jobs)

    if not all_jobs:
        raise RuntimeError("No jobs scraped")

    df = pd.concat(all_jobs, ignore_index=True)

    # --- Basic cleaning ---
    df = df.drop_duplicates(subset=["site", "job_url"])
    df = df[df["title"].apply(is_data_role)]

    if df.empty:
        raise RuntimeError("No data-engineering roles found after filtering")

    # Parse dates cleanly
    df["date_posted"] = pd.to_datetime(df["date_posted"], errors="coerce")
    df["date_posted"] = df["date_posted"].dt.tz_localize(None)

    # Tag when it was scraped
    df["scraped_at"] = pd.Timestamp.utcnow().replace(tzinfo=None)

    # --- Append to silver (dedupe on site + job_url) ---
    os.makedirs(os.path.dirname(SILVER_PATH), exist_ok=True)

    if os.path.exists(SILVER_PATH):
        existing = pd.read_csv(SILVER_PATH, parse_dates=["date_posted", "scraped_at"])
        df = pd.concat([existing, df], ignore_index=True)
        df = df.drop_duplicates(subset=["site", "job_url"], keep="first")

    df.to_csv(
        SILVER_PATH,
        index=False,
        quoting=csv.QUOTE_NONNUMERIC,
        escapechar="\\",
    )

    print(f"✅ Silver saved → {SILVER_PATH} ({len(df)} total records)")
    return df