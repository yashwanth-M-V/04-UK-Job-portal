import pandas as pd

REQUIRED_COLUMNS = {
    "Role",
    "Company",
    "Location",
    "Experience",
    "Link",
    "deadline"
}

def load_jobs(filepath: str) -> pd.DataFrame:
    df = pd.read_excel(filepath)

    missing_cols = REQUIRED_COLUMNS - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    return df
