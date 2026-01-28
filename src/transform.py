import pandas as pd


def filter_active_jobs(df: pd.DataFrame, days: int = 14) -> pd.DataFrame:
    """
    Keep jobs posted in the last `days` days.
    """

    df = df.copy()

    if "date_posted" not in df.columns:
        raise ValueError("Expected column 'date_posted' not found")

    # Convert to datetime (coerce errors)
    df["date_posted"] = pd.to_datetime(df["date_posted"], errors="coerce")

    # Make tz-naive (just in case)
    df["date_posted"] = df["date_posted"].dt.tz_localize(None)

    # Cutoff datetime (tz-naive)
    cutoff = pd.Timestamp.utcnow().replace(tzinfo=None) - pd.Timedelta(days=days)

    # Filter recent jobs
    active_df = df[df["date_posted"] >= cutoff]
    active_df = active_df.sort_values("date_posted", ascending=False)

    return active_df
