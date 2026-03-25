import pandas as pd
from src.config.config import connect_to_DB

def insert_jobs(df: pd.DataFrame):

    if df is None or df.empty:
        print("No jobs to insert.")
        return

    conn = connect_to_DB()
    cur = conn.cursor()

    query = """
    INSERT INTO job_postings (
        site, title, company, location,
        date_posted, job_type, job_url, description
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (job_url) DO NOTHING
    """

    rows = [
        (
            r.site,
            r.title,
            r.company,
            r.location,
            r.date_posted,
            r.job_type,
            r.job_url,
            r.description,
        )
        for r in df.itertuples()
    ]

    cur.executemany(query, rows)

    conn.commit()

    inserted = cur.rowcount

    cur.close()
    conn.close()

    print(f"Inserted {inserted} new jobs into database.")