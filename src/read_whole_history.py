import glob
from turtle import pd
import pandas as pd

files = glob.glob("data/silver/date=*/jobs.parquet")

df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)