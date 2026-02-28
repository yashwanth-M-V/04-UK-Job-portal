from src.scrape.job_scraping import scrape_to_silver
from src.transform import silver_to_gold
from src.ingest import load_jobs
from src.render import render_markdown, render_html


def run_pipeline() -> str:
    # Step 1: Scrape → append to silver (source of truth)
    scrape_to_silver()

    # Step 2: Silver → gold (filtered, display-ready)
    silver_to_gold()

    # Step 3: Load gold
    df = load_jobs()

    # Step 4a: Render to HTML (GitHub Pages)
    render_html(df)

    # Step 4b: Render to markdown (README — kept for now)
    markdown = render_markdown(df)

    return markdown


if __name__ == "__main__":
    print("🚀 Running job pipeline...")
    run_pipeline()