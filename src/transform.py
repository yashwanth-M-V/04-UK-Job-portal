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

SILVER_PATH = "data/jobs_silver.parquet"
GOLD_PATH   = "data/jobs_gold.csv"

def silver_to_gold(days: int = 14) -> pd.DataFrame:
  
    # --- Ensure silver exists ---
    if not os.path.exists(SILVER_PATH):
        raise FileNotFoundError(f"Silver file not found: {SILVER_PATH}")

    # --- Read Silver Parquet ---
    df = pd.read_parquet(SILVER_PATH)

    if df.empty:
        raise RuntimeError("Silver dataset is empty")

    # --- Clean date column ---
    df["date_posted"] = pd.to_datetime(df["date_posted"], errors="coerce")

    # Remove rows without valid date
    df = df[df["date_posted"].notna()]

    # --- Filter recent jobs ---
    cutoff = pd.Timestamp.utcnow().replace(tzinfo=None) - pd.Timedelta(days=days)
    df = df[df["date_posted"] >= cutoff]

    # --- Keep only columns that exist ---
    available_columns = [col for col in GOLD_COLUMNS if col in df.columns]
    df = df[available_columns]

    # --- Basic cleanup for missing values ---
    if "job_type" in df.columns:
        df["job_type"] = df["job_type"].fillna("N/A")

    if "location" in df.columns:
        df["location"] = df["location"].fillna("Unknown")

    # --- Sort newest first ---
    df = df.sort_values("date_posted", ascending=False).reset_index(drop=True)

    # --- Save Gold dataset ---
    os.makedirs(os.path.dirname(GOLD_PATH), exist_ok=True)

    df.to_csv(
        GOLD_PATH,
        index=False,
        quoting=csv.QUOTE_NONNUMERIC,
        escapechar="\\",
    )

    print(f"✅ Gold saved → {GOLD_PATH} ({len(df)} records, last {days} days)")

    return df
