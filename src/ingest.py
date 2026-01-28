import pandas as pd

def load_jobs(path: str) -> pd.DataFrame:
    return pd.read_csv(path)