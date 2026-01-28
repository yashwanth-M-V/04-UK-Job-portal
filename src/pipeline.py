from src.scrape.job_scraping import scrape_and_save
from src.ingest import load_jobs
from src.transform import filter_active_jobs
from src.render import render_markdown
import pandas as pd

RAW_PATH = "data/jobs_raw.csv"

def run_pipeline():
    scrape_and_save(RAW_PATH)
    df = load_jobs(RAW_PATH)
    active_df = filter_active_jobs(df)
    markdown = render_markdown(active_df)
    return markdown

print("🚀 Running job pipeline...")