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
# Keywords
# ---------------------------
JUNIOR_KEYWORDS = [
    "junior data engineer",
    "graduate data engineer",
    "entry level data engineer",
    "associate data engineer",
    "trainee data engineer",
]

# ---------------------------
# Internal history file
# ---------------------------
HISTORY_PATH = "data/old_jobs.csv"


def scrape_and_save(output_path: str, hours_old: int = 72) -> pd.DataFrame:
    all_jobs = []

    # ---------------------------
    # Scrape
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
    # ===== STREAM 1 =====
    # Overwrite snapshot (clean, small)
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
    # Append-only history (rich, analytical)
    # ---------------------------
    history_df = df.copy()

    # Add metadata for analysis
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

    # Return snapshot for pipeline use
    return snapshot_df
