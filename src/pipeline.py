from src.scrape.job_scraping import scrape_to_silver
from src.transform import silver_to_gold
from src.ingest import load_jobs
from src.render import render_markdown, render_html

def run_pipeline() -> str:
    print("\n🚀 Starting Job Data Pipeline\n")

    print("Step 1/4: Scraping jobs...")
    scrape_to_silver()

    print("Step 2/4: Building gold dataset...")
    silver_to_gold()

    print("Step 3/4: Loading dataset...")
    df = load_jobs()

    print("Step 4/4: Rendering website...")
    render_html(df)

    markdown = render_markdown(df)

    print("\n✅ Pipeline completed successfully\n")

    return markdown