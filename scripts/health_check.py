"""
Scraper health monitor.

Runs after every GitHub Actions scrape. For each retailer, compares the count of
listings written this run against the count written by the previous run and emails
an alert to gbmcoffice@gmail.com if a scraper appears broken.

Alerting rules:
  CRITICAL  retailer had >= MIN_BASELINE listings last run and 0 this run
  WARN      retailer had >= MIN_BASELINE listings last run and dropped more than
            DROP_THRESHOLD fraction this run

Retailers with no previous baseline (e.g. never-scraped, or intentionally disabled
like Bereli) self-mute -they produce no alerts because we have nothing to compare
against.

Required env:
  SUPABASE_URL, SUPABASE_KEY
  GMAIL_APP_PASSWORD   Gmail app password for the sender account. If unset, the
                       script prints the alert instead of sending (useful locally).

Optional env:
  GMAIL_USER           Sender address. Default: gbmcoffice@gmail.com
  ALERT_RECIPIENT      Recipient address. Default: gbmcoffice@gmail.com
  FORCE_ALERT          If "1", treat every retailer as alerting (preview formatting).
"""

import os
import smtplib
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")
GMAIL_USER = os.environ.get("GMAIL_USER", "gbmcoffice@gmail.com")
ALERT_RECIPIENT = os.environ.get("ALERT_RECIPIENT", "gbmcoffice@gmail.com")
FORCE_ALERT = os.environ.get("FORCE_ALERT") == "1"

CURRENT_WINDOW_HOURS = 3
PREVIOUS_WINDOW_HOURS = 3
DROP_THRESHOLD = 0.5
MIN_BASELINE = 5
PAGE_SIZE = 1000


def fetch_retailers(sb):
    rows = sb.table("retailers").select("id,name").eq("is_active", True).execute().data
    return {r["id"]: r["name"] for r in rows}


def count_current(sb, cutoff_iso):
    counts = defaultdict(int)
    start = 0
    while True:
        end = start + PAGE_SIZE - 1
        batch = (
            sb.table("listings")
            .select("retailer_id")
            .gte("last_updated", cutoff_iso)
            .range(start, end)
            .execute()
            .data
        )
        if not batch:
            break
        for row in batch:
            counts[row["retailer_id"]] += 1
        if len(batch) < PAGE_SIZE:
            break
        start += PAGE_SIZE
    return counts


def count_previous(sb, window_start_iso, window_end_iso):
    # price_history has no retailer_id column; join via listings(retailer_id).
    # Count distinct listing_ids per retailer so re-scrapes that write multiple
    # history rows in one run don't inflate the baseline.
    per_retailer = defaultdict(set)
    start = 0
    while True:
        end = start + PAGE_SIZE - 1
        batch = (
            sb.table("price_history")
            .select("listing_id,listings(retailer_id)")
            .gte("recorded_at", window_start_iso)
            .lt("recorded_at", window_end_iso)
            .range(start, end)
            .execute()
            .data
        )
        if not batch:
            break
        for row in batch:
            joined = row.get("listings")
            if not joined:
                continue
            rid = joined.get("retailer_id")
            lid = row.get("listing_id")
            if rid is None or lid is None:
                continue
            per_retailer[rid].add(lid)
        if len(batch) < PAGE_SIZE:
            break
        start += PAGE_SIZE
    return {rid: len(ids) for rid, ids in per_retailer.items()}


def evaluate(retailers, current_counts, previous_counts):
    alerts = []
    for rid, name in retailers.items():
        cur = current_counts.get(rid, 0)
        prev = previous_counts.get(rid, 0)

        if FORCE_ALERT:
            alerts.append(("TEST", name, f"force-alert: current={cur} previous={prev}"))
            continue

        if prev < MIN_BASELINE:
            # No meaningful baseline - retailer likely isn't scraping this cycle.
            continue

        if cur == 0:
            alerts.append(("CRITICAL", name, f"0 listings this run (previous: {prev})"))
            continue

        if cur < prev * (1 - DROP_THRESHOLD):
            pct = (1 - cur / prev) * 100
            alerts.append(("WARN", name, f"{cur} this run vs {prev} previous ({pct:.0f}% drop)"))

    return alerts


def format_body(alerts, retailers, current_counts, previous_counts, windows):
    cur_start, cur_end, prev_start, prev_end = windows
    lines = ["AmmoRaccoon scraper health alert", ""]
    for severity, name, detail in alerts:
        lines.append(f"[{severity}] {name} - {detail}")
    lines += [
        "",
        f"Current window:  {cur_start.isoformat()} .. {cur_end.isoformat()}",
        f"Previous window: {prev_start.isoformat()} .. {prev_end.isoformat()}",
        "",
        "Full breakdown (current / previous):",
    ]
    for rid, name in sorted(retailers.items(), key=lambda kv: kv[1].lower()):
        cur = current_counts.get(rid, 0)
        prev = previous_counts.get(rid, 0)
        if cur or prev:
            lines.append(f"  {name}: {cur} / {prev}")
    return "\n".join(lines)


def send_email(subject, body):
    if not GMAIL_APP_PASSWORD:
        print("GMAIL_APP_PASSWORD not set - printing alert instead of sending.")
        print("-" * 60)
        print(f"To: {ALERT_RECIPIENT}")
        print(f"From: {GMAIL_USER}")
        print(f"Subject: {subject}")
        print()
        print(body)
        print("-" * 60)
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = GMAIL_USER
    msg["To"] = ALERT_RECIPIENT
    msg.set_content(body)

    with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
        smtp.starttls()
        smtp.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        smtp.send_message(msg)
    print(f"Alert email sent to {ALERT_RECIPIENT}.")


def main():
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    now = datetime.now(timezone.utc)
    cur_start = now - timedelta(hours=CURRENT_WINDOW_HOURS)
    prev_end = cur_start
    prev_start = prev_end - timedelta(hours=PREVIOUS_WINDOW_HOURS)

    retailers = fetch_retailers(sb)
    current = count_current(sb, cur_start.isoformat())
    previous = count_previous(sb, prev_start.isoformat(), prev_end.isoformat())

    print(f"Current window:  {cur_start.isoformat()} -> {now.isoformat()}")
    print(f"Previous window: {prev_start.isoformat()} -> {prev_end.isoformat()}")
    print("Per-retailer (current / previous):")
    for rid, name in sorted(retailers.items()):
        cur = current.get(rid, 0)
        prev = previous.get(rid, 0)
        if cur or prev:
            print(f"  [{rid:>2}] {name}: {cur} / {prev}")

    alerts = evaluate(retailers, current, previous)
    if not alerts:
        print("\nAll scrapers healthy -no alert sent.")
        return 0

    print(f"\n{len(alerts)} alert(s):")
    for severity, name, detail in alerts:
        print(f"  [{severity}] {name} - {detail}")

    subject = f"[AmmoRaccoon] {len(alerts)} scraper alert(s)"
    body = format_body(alerts, retailers, current, previous, (cur_start, now, prev_start, prev_end))
    send_email(subject, body)
    return 0


if __name__ == "__main__":
    sys.exit(main())
