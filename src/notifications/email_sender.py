import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from dotenv import load_dotenv
load_dotenv()


SEND_GRID_API_KEY = os.getenv("SEND_GRID_API_KEY")
FROM_EMAIL = os.getenv("FROM_EMAIL")

def send_email(to_email, subject, content):
    message = Mail(
        from_email=FROM_EMAIL,
        to_emails=to_email,
        subject=subject,
        html_content=content
    )

    try:
        sg = SendGridAPIClient(SEND_GRID_API_KEY)
        response = sg.send(message)
        print(f"✅ Email sent to {to_email}")
        return True 

    except Exception as e:
        print(f"❌ Email failed: {e}")
        return False
