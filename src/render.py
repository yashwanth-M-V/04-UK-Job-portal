def render_markdown(df) -> str:
    blocks = []
    
    for _, row in df.iterrows():
        link_html = (
            f'<a href="{row["link"]}" '
            f'target="_blank" rel="noopener noreferrer">Link</a>'
        )

        deadline = row["deadline"].strftime("%d %b %Y")

    for _, row in df.iterrows():
        block = f"""### {row['Role']} – {row['Company']}
📍 Location: {row['Location']}  
🧠 Experience: {row['Experience']}
⏳ Deadline: {row['deadline'].date()}   
🔗 Apply: {link_html}
"""
        blocks.append(block)

    return "\n---\n\n".join(blocks)
