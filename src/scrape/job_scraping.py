import csv
import os
import pandas as pd
from jobspy import scrape_jobs


# ---------------------------
# Public / README-facing columns
# ---------------------------
DISPLAY_COLUMNS = [
    "site",
    "title",
    "company",
    "location",
    "date_posted",
    "job_type",
    "job_url",
]

# ---------------------------
# Junior-focused search terms
# (used only for scraping, not filtering)
# ---------------------------
JUNIOR_KEYWORDS = [
    "junior data engineer",
    "graduate data engineer",
    "entry level data engineer",
    "associate data engineer",
    "trainee data engineer",
]

# ---------------------------
# STRICT data-role whitelist
# ---------------------------
DATA_ROLE_KEYWORDS = [
    "data engineer",
    "analytics engineer",
    "platform engineer (data)",
    "data platform engineer",
    "machine learning engineer",
    "ml engineer",
    "database administrator",
    "database engineer",
    "etl developer",
    "data reliability engineer",
    "big data engineer",
    "data architect",
    "cloud data engineer",
    "bi engineer",
    "business intelligence engineer",
]

# ---------------------------
# Internal history file
# ---------------------------
HISTORY_PATH = "data/old_jobs.csv"


# ---------------------------
# strict title filter
# ---------------------------
import re

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
# Main pipeline
# ---------------------------
def scrape_and_save(output_path: str, hours_old: int = 72) -> pd.DataFrame:
    all_jobs = []

    # ---------------------------
    # Scrape broadly
    # ---------------------------
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

    # ---------------------------
    # Combine & deduplicate
    # ---------------------------
    df = pd.concat(all_jobs, ignore_index=True)
    df = df.drop_duplicates(subset=["site", "job_url"])

    # ---------------------------
    # STRICT data-role filtering
    # ---------------------------
    df = df[df["title"].apply(is_data_role)]

    if df.empty:
        raise RuntimeError("No data-engineering roles found after filtering")

    # ---------------------------
    # ===== STREAM 1 =====
    # Snapshot (clean & small)
    # ---------------------------
    snapshot_df = df[DISPLAY_COLUMNS]

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    snapshot_df.to_csv(
        output_path,
        index=False,
        quoting=csv.QUOTE_NONNUMERIC,
        escapechar="\\",
    )

    # ---------------------------
    # ===== STREAM 2 =====
    # Append-only history
    # ---------------------------
    history_df = df.copy()
    history_df["scraped_at"] = pd.Timestamp.utcnow()

    os.makedirs(os.path.dirname(HISTORY_PATH), exist_ok=True)

    if os.path.exists(HISTORY_PATH):
        old_history = pd.read_csv(HISTORY_PATH)
        history_df = pd.concat([old_history, history_df], ignore_index=True)
        history_df = history_df.drop_duplicates(subset=["site", "job_url"])

    history_df.to_csv(
        HISTORY_PATH,
        index=False,
        quoting=csv.QUOTE_NONNUMERIC,
        escapechar="\\",
    )

    return snapshot_df
