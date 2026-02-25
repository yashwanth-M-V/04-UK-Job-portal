import pandas as pd

GOLD_PATH = "data/jobs_gold.csv"

def load_jobs(path: str = GOLD_PATH) -> pd.DataFrame:
    return pd.read_csv(path, parse_dates=["date_posted"])