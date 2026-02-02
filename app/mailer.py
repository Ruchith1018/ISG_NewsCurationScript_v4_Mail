# --------------------------------------------------
# GMAIL – SEND LATEST FINAL OUTPUT
# --------------------------------------------------
import smtplib
from email.message import EmailMessage
from pathlib import Path
from datetime import datetime
import mimetypes
import os

def _get_latest_finaloutput_today(project_dir: Path) -> Path:
    today_folder = project_dir / "Outputs" / f"{datetime.now():%Y%m%d}_NewsAPI_extraction"

    if not today_folder.exists():
        raise FileNotFoundError(f"Today's output folder not found: {today_folder}")

    files = list(today_folder.glob("FinalOutput_*.xlsx"))
    if not files:
        raise FileNotFoundError("No FinalOutput_*.xlsx found for today")

    # pick latest by modified time
    return max(files, key=lambda f: f.stat().st_mtime)

def send_latest_finaloutput_gmail(project_dir: Path):
    host = os.environ["SMTP_HOST"]
    port = int(os.environ.get("SMTP_PORT", 587))
    user = os.environ["SMTP_USER"]
    password = os.environ["SMTP_PASS"]
    mail_from = os.environ.get("MAIL_FROM", user)
    mail_to = [x.strip() for x in os.environ["MAIL_TO"].split(",") if x.strip()]

    attachment = _get_latest_finaloutput_today(project_dir)

    msg = EmailMessage()
    msg["Subject"] = f"Daily FinalOutput Report - {datetime.now():%Y-%m-%d}"
    msg["From"] = mail_from
    msg["To"] = ", ".join(mail_to)

    msg.set_content(
        f"Hi Team,\n\nPlease find attached the latest Final Output report for {datetime.now():%Y-%m-%d}.\n\nRegards, \nISG DAILY NEWS"
    )

    ctype, _ = mimetypes.guess_type(attachment)
    maintype, subtype = (ctype or "application/octet-stream").split("/", 1)

    with open(attachment, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype=maintype,
            subtype=subtype,
            filename=attachment.name,
        )

    with smtplib.SMTP(host, port) as server:
        server.starttls()
        server.login(user, password)
        server.send_message(msg)

    print(f"✅ Gmail sent: {attachment}")
