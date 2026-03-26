import re
import pandas as pd
from datetime import datetime
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from src.config.config import supabase


# ---------------------------
# Paths
# ---------------------------
BASE_DIR = Path(__file__).resolve().parent.parent.parent
TEMPLATE_DIR = BASE_DIR / "template"
OUTPUT_PATH = BASE_DIR / "index.html"

OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)


# ---------------------------
# Role grouping
# ---------------------------
ROLE_GROUPS = {
    r"\bdata engineer\b":               "Data Engineer",
    r"\banalytics engineer\b":          "Analytics Engineer",
    r"\bml engineer\b|machine learning engineer\b": "ML Engineer",
    r"\bdatabase administrator\b|database engineer\b": "Database Engineer",
    r"\betl developer\b":               "ETL Developer",
    r"\bdata architect\b":              "Data Architect",
    r"\bbi engineer\b|business intelligence engineer\b": "BI Engineer",
    r"\bdata platform engineer\b|platform engineer\b": "Platform Engineer",
    r"\bdata reliability engineer\b":   "Data Reliability Engineer",
    r"\bbig data engineer\b":           "Big Data Engineer",
    r"\bcloud data engineer\b":         "Cloud Data Engineer",
}

def get_role_group(title: str) -> str:
    if not isinstance(title, str):
        return "Other"
    title_lower = title.lower()
    for pattern, label in ROLE_GROUPS.items():
        if re.search(pattern, title_lower):
            return label
    return "Other"


# ---------------------------
# Load jobs from database      ← CHANGED: supabase client instead of psycopg2
# ---------------------------
def load_jobs_from_db(limit: int = 500) -> pd.DataFrame:
    response = supabase.table("job_postings") \
        .select("site, title, company, location, date_posted, job_type, job_url, description") \
        .order("date_posted", desc=True) \
        .limit(limit) \
        .execute()

    df = pd.DataFrame(response.data)
    print(f"Loaded {len(df)} jobs from database")
    return df


# ---------------------------
# Render HTML via Jinja2       ← unchanged
# ---------------------------
def render_html(df: pd.DataFrame, output_path: Path = OUTPUT_PATH) -> None:
    if df.empty:
        print("⚠️  No jobs to render into HTML")
        return

    jobs = []
    for _, row in df.iterrows():
        date_val = row.get("date_posted")
        try:
            date_obj = pd.to_datetime(date_val)
            date_posted     = date_obj.strftime("%d %b %Y")
            date_posted_iso = date_obj.strftime("%Y-%m-%d")
        except Exception:
            date_posted     = "Unknown"
            date_posted_iso = ""

        jobs.append({
            "title":          row.get("title", ""),
            "company":        row.get("company", ""),
            "location":       row.get("location", ""),
            "site":           row.get("site", ""),
            "job_type":       row.get("job_type", "N/A") or "N/A",
            "job_url":        row.get("job_url", "#"),
            "date_posted":    date_posted,
            "date_posted_iso": date_posted_iso,
            "role_group":     get_role_group(row.get("title", "")),
        })

    roles = sorted(set(j["role_group"] for j in jobs))

    env      = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)))
    template = env.get_template("job_template.html")

    html = template.render(
        jobs         = jobs,
        roles        = roles,
        total_jobs   = len(jobs),
        last_updated = datetime.utcnow().strftime("%d %b %Y, %H:%M UTC"),
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    print(f"✅ HTML rendered → {output_path} ({len(jobs)} jobs)")


# ---------------------------
# Run renderer standalone
# ---------------------------
if __name__ == "__main__":
    df = load_jobs_from_db()
    render_html(df)