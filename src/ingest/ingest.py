import pandas as pd
from src.config.config import supabase

def insert_jobs(df: pd.DataFrame):

    if df is None or df.empty:
        print("No jobs to insert.")
        return

    rows = [
        {
            "site":        r.site,
            "title":       r.title,
            "company":     r.company,
            "location":    r.location,
            "date_posted": str(r.date_posted),
            "job_type":    r.job_type,
            "job_url":     r.job_url,
            "description": r.description,
        }
        for r in df.itertuples()
    ]

    result = supabase.table("job_postings").upsert(rows, on_conflict="job_url").execute()

    print(f"Inserted/updated {len(result.data)} jobs into database.")