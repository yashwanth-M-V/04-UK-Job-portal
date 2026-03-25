import os
import pandas as pd

GOLD_COLUMNS = [
    "site",
    "title",
    "company",
    "location",
    "date_posted",
    "job_type",
    "job_url",
    "description",
]


def silver_to_gold(temp_path: str, days: int = 14):

    print("Loading silver temp file")

    df = pd.read_parquet(temp_path)

    if df.empty:
        raise RuntimeError("Silver dataset empty")

    df["date_posted"] = pd.to_datetime(df["date_posted"], errors="coerce", utc=True)
    df = df[df["date_posted"].notna()]
    
    df["date_posted"] = df["date_posted"].dt.tz_convert(None)

    cutoff = (pd.Timestamp.utcnow() - pd.Timedelta(days=days)).tz_localize(None)
    df = df[df["date_posted"] >= cutoff]

    available_cols = [c for c in GOLD_COLUMNS if c in df.columns]
    df = df[available_cols]

    if "location" in df.columns:
        df["location"] = df["location"].fillna("Unknown")

    if "job_type" in df.columns:
        df["job_type"] = df["job_type"].fillna("N/A")

    df = df.sort_values("date_posted", ascending=False).reset_index(drop=True)

    print(f"Gold dataset ready: {len(df)} jobs")

    # delete temp file
    os.remove(temp_path)
    print("Temporary silver file deleted")

    return df

if __name__=="__main__":
    silver_to_gold()