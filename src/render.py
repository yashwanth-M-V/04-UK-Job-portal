import re
import pandas as pd
from datetime import datetime
from pathlib import Path
from jinja2 import Environment, FileSystemLoader


# ---------------------------
# Paths
# ---------------------------
TEMPLATE_DIR  = Path(__file__).parent.parent / "template"
OUTPUT_PATH   = Path("index.html")


# ---------------------------
# Role grouping
# Maps a job title → a clean filter label
# Add more as your role list grows
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
    """Return a clean role label for a job title."""
    if not isinstance(title, str):
        return "Other"
    title_lower = title.lower()
    for pattern, label in ROLE_GROUPS.items():
        if re.search(pattern, title_lower):
            return label
    return "Other"


# ---------------------------
# Render README markdown
# (kept for backward compat)
# ---------------------------
def render_markdown(df: pd.DataFrame) -> str:
    if df.empty:
        return "_No active job postings found._"

    blocks = []
    for _, row in df.iterrows():
        link = f"[Apply]({row['job_url']})"
        date_posted = (
            row["date_posted"].strftime("%d %b %Y")
            if pd.notnull(row["date_posted"])
            else "Unknown"
        )
        block = f"""### {row['title']} – {row['company']}
📍 Location: {row['location']}  
🧠 Job Type: {row.get('job_type', 'N/A')}  
🗓 Posted on: {date_posted}  
🔗 {link}
"""
        blocks.append(block)
    return "\n---\n\n".join(blocks)


# ---------------------------
# Render HTML via Jinja2
# ---------------------------
def render_html(df: pd.DataFrame, output_path: Path = OUTPUT_PATH) -> None:
    if df.empty:
        print("⚠️  No jobs to render into HTML")
        return

    # -- Prepare job dicts for the template --
    jobs = []
    for _, row in df.iterrows():
        date_posted = (
            row["date_posted"].strftime("%d %b %Y")
            if pd.notnull(row["date_posted"])
            else "Unknown"
        )
        jobs.append({
            "title":       row.get("title", ""),
            "company":     row.get("company", ""),
            "location":    row.get("location", ""),
            "site":        row.get("site", ""),
            "job_type":    row.get("job_type", "N/A") or "N/A",
            "job_url":     row.get("job_url", "#"),
            "date_posted": date_posted,
            "date_posted_iso": row["date_posted"].strftime("%Y-%m-%d") if pd.notnull(row["date_posted"]) else "",
            "role_group":  get_role_group(row.get("title", "")),
        })

    # -- Unique role labels for filter buttons --
    roles = sorted(set(j["role_group"] for j in jobs))

    # -- Load and render template --
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