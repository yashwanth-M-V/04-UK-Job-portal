"""
notify.py
Reads today's new jobs from job_postings and sends them to all active subscribers.
Run by GitHub Actions at 9pm UTC, after scrape + render.
"""

import os
from datetime import date, timedelta
from dotenv import load_dotenv
from supabase import create_client

from src.config.config import supabase_service, BOT_TOKEN, SEND_GRID_API_KEY, FROM_EMAIL
from src.notifications.email_sender import send_email
from src.notifications.telegram_sender import send_telegram
from src.db import insert_log, get_active_subscribers, get_unsent_updates, update_last_sent


load_dotenv()

supabase = create_client(os.getenv("SUPABASE_URL_URL"), os.getenv("SUPABASE_SERVICE_KEY"))

SITE_URL = "https://yashwanth-m-v.github.io/Data-hire-UK/" 


def get_todays_jobs(limit: int = 5) -> list[dict]:
    today = str(date.today())
    yesterday = str(date.today() - timedelta(days=1))

    response = (
        supabase.table("job_postings")
        .select("title, company, location, job_url, date_posted")
        .gte("date_posted", yesterday)
        .order("date_posted", desc=True)
        .limit(limit)
        .execute()
    )
    return response.data


def build_email_html(jobs: list[dict]) -> str:
    rows = ""
    for j in jobs:
        rows += f"""
        <tr>
          <td style="padding:10px 0; border-bottom:1px solid #eee;">
            <a href="{j['job_url']}" style="font-weight:600;color:#4f46e5;text-decoration:none;">
              {j['title']}
            </a><br>
            <span style="color:#666;font-size:13px;">{j['company']} · {j.get('location','UK')}</span>
          </td>
        </tr>"""

    return f"""
    <div style="font-family:sans-serif;max-width:600px;margin:auto;color:#1a1a2e;">
      <h2 style="color:#4f46e5;">🇬🇧 UK Data Engineering Jobs — Daily Update</h2>
      <p style="color:#666;">Here are today's freshest roles. Full list at
        <a href="{SITE_URL}">{SITE_URL}</a>
      </p>
      <table style="width:100%;border-collapse:collapse;">{rows}</table>
      <hr style="margin-top:24px;border:none;border-top:1px solid #eee;">
      <p style="font-size:12px;color:#aaa;">
        You're receiving this because you subscribed at {SITE_URL}.<br>
        To unsubscribe, reply STOP or message /stop to the Telegram bot.
      </p>
    </div>"""


def build_telegram_text(jobs: list[dict]) -> str:
    lines = ["🇬🇧 *UK Data Engineering Jobs — Daily Update*\n"]
    for j in jobs:
        company = j.get("company", "")
        location = j.get("location", "UK")
        lines.append(f"• [{j['title']}]({j['job_url']}) — {company}, {location}")
    lines.append(f"\n🔗 [See all jobs]({SITE_URL})")
    return "\n".join(lines)


def run():
    jobs = get_todays_jobs()
    if not jobs:
        print("No new jobs today — nothing to send.")
        return

    subscribers = (
        supabase.table("subscribers")
        .select("*")
        .eq("is_active", True)
        .execute()
        .data
    )

    print(f"Sending to {len(subscribers)} subscribers, {len(jobs)} jobs found.")

    email_html = build_email_html(jobs)
    telegram_text = build_telegram_text(jobs)

    for sub in subscribers:
        channel = sub.get("preferred_channel", "telegram")

        try:
            if channel == "email" and sub.get("email"):
                ok = send_email(
                    to_email=sub["email"],
                    subject="🇬🇧 UK Data Engineering Jobs — Daily Update",
                    content=email_html,
                )
                status = "sent" if ok else "failed"

            elif channel == "telegram" and sub.get("telegram_chat_id"):
                resp = send_telegram(sub["telegram_chat_id"], telegram_text)
                status = "sent" if resp.get("ok") else "failed"

            else:
                print(f"  Skipping subscriber {sub['id']} — no valid contact.")
                continue

            # Log it
            supabase.table("message_logs").insert({
                "subscriber_id": sub["id"],
                "channel": channel,
                "message": f"{len(jobs)} jobs sent",
                "status": status,
            }).execute()

            print(f"  [{status}] → {channel} / sub {sub['id']}")

        except Exception as e:
            print(f"  [error] sub {sub['id']}: {e}")
            supabase.table("message_logs").insert({
                "subscriber_id": sub["id"],
                "channel": channel,
                "message": "",
                "status": "failed",
                "error_message": str(e),
            }).execute()


if __name__ == "__main__":
    run()
