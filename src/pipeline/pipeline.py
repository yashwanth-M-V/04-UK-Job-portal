from src.scrape.job_scraping import scrape_to_silver
from src.transform.transform import silver_to_gold
from src.ingest.ingest import insert_jobs


def run_pipeline():

    print("\nJob Pipeline Started\n")

    # Step 1
    print("Step 1: Scraping jobs")
    df_silver, temp_path = scrape_to_silver()

    # Step 2
    print("Step 2: Transforming silver to gold")
    df_gold = silver_to_gold(temp_path)

    # Step 3
    print("Step 3: Inserting into database")
    insert_jobs(df_gold)

    print("\nPipeline completed successfully\n")


if __name__ == "__main__":
    run_pipeline()