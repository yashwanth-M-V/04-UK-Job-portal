from src.pipeline.pipeline import run_pipeline
from src.render.render import render_html, load_jobs_from_db
from pathlib import Path

# ---------------------------
# Main entry point
# ---------------------------

def main():
    print("\n🚀 Starting Job Pipeline\n")

    # Run the full pipeline (scrape → transform → ingest)
    run_pipeline()

    # Load latest jobs from database
    df = load_jobs_from_db()

    # Render HTML to root folder
    output_path = Path("index.html")
    render_html(df, output_path)

    print("\n✅ Pipeline completed successfully")
    print(f"📄 HTML output → {output_path.resolve()}\n")


if __name__ == "__main__":
    main()