import pandas as pd

def render_markdown(df) -> str:
    blocks = []

    for _, row in df.iterrows():
        link = f"[Apply]({row['job_url']})"

        # Handle date safely
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

