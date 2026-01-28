from src.pipeline import run_pipeline
import pandas as pd

README_PATH = "README.md"
START = "<!-- START:JOB_POSTINGS -->"
END = "<!-- END:JOB_POSTINGS -->"

def update_readme(content: str):
    with open(README_PATH, "r", encoding="utf-8") as f:
        readme = f.read()

    before = readme.split(START)[0]
    after = readme.split(END)[1]

    new_readme = before + START + "\n\n" + content + "\n\n" + END + after

    with open(README_PATH, "w", encoding="utf-8") as f:
        f.write(new_readme)

def main():
    markdown = run_pipeline()
    update_readme(markdown)

if __name__ == "__main__":
    main()
