import csv
import os
import pandas as pd

# ---------------------------
# Columns exposed in gold
# ---------------------------
GOLD_COLUMNS = [
    "site",
    "title",
    "company",
    "location",
    "date_posted",
    "job_type",
    "job_url",
    "description",   # kept for future skill scoring
]

SILVER_PATH = "data/jobs_silver.csv"
GOLD_PATH   = "data/jobs_gold.csv"


def silver_to_gold(days: int = 14) -> pd.DataFrame:
    if not os.path.exists(SILVER_PATH):
        raise FileNotFoundError(f"Silver file not found: {SILVER_PATH}")

    df = pd.read_csv(SILVER_PATH, parse_dates=["date_posted"])

    # --- Date filter ---
    cutoff = pd.Timestamp.utcnow().replace(tzinfo=None) - pd.Timedelta(days=days)
    df = df[df["date_posted"] >= cutoff]

    # --- Keep only gold columns that actually exist ---
    available = [c for c in GOLD_COLUMNS if c in df.columns]
    df = df[available]

    # --- Sort newest first ---
    df = df.sort_values("date_posted", ascending=False).reset_index(drop=True)

    # --- Overwrite gold daily ---
    os.makedirs(os.path.dirname(GOLD_PATH), exist_ok=True)
    df.to_csv(
        GOLD_PATH,
        index=False,
        quoting=csv.QUOTE_NONNUMERIC,
        escapechar="\\",
    )

    print(f"✅ Gold saved → {GOLD_PATH} ({len(df)} records, last {days} days)")
    return df