"""
Scraper health monitor.

Runs after every GitHub Actions scrape. For each retailer, compares the count of
listings written this run against the count written by the previous run and emails
an alert to gbmcoffice@gmail.com if a scraper appears broken.

Alerting rules:
  CRITICAL  retailer had >= MIN_BASELINE listings last run and 0 this run
  WARN      retailer had >= MIN_BASELINE listings last run and dropped more than
            DROP_THRESHOLD fraction this run
  CRITICAL  zero-coverage: a retailer that finished a run this window and saved
            listings for SOME calibers saved zero for other calibers its own
            CALIBER_PATHS config says it covers (one alert line per retailer)

Retailers with no previous baseline (e.g. never-scraped, or intentionally disabled
like Bereli) self-mute -they produce no alerts because we have nothing to compare
against.

The zero-coverage check (2026-06-12) closes the silent-failure class the
caliber-scoping audit caught: Target Sports USA's renumbered category IDs
left 9 of 10 calibers dark for weeks with NO alert, because 9mm kept saving
and the retailer-level total never hit zero. Coverage truth comes from the
scrapers themselves — CALIBER_PATHS keys are AST-parsed out of every scraper
wired into scrape_light.yml — so parking a caliber in a scraper (PSA's five
Cloudflare-walled ones) automatically removes it from the expectation; no
second source of truth to drift. Deliberate boundaries: only light-tier
scrapers (this check runs at the end of scrape_light; medium/heavy runs can
straddle the window mid-run and would false-alarm), only retailers whose
last_scraped_at landed inside the current window (i.e. the run FINISHED),
and only retailers that saved at least one listing — a fully-dark retailer
is the existing CRITICAL rule's job at onset, and persistently-dark
detection is the promoted scale-#5 monitoring item in ammoraccoon-web/TASKS.md.

Also checks the homepage badge cache (homepage_segment_aggregates_cache):
if its newest refreshed_at is older than CACHE_MAX_AGE_HOURS the refresh
job has been failing for ~3 consecutive ticks — alert BEFORE the web's
24h fallback silently kicks in (2026-06-10: the refresh failed green for
19h under continue-on-error before anyone noticed).

Required env:
  SUPABASE_URL, SUPABASE_KEY
  GMAIL_APP_PASSWORD   Gmail app password for the sender account. If unset, the
                       script prints the alert instead of sending (useful locally).

Optional env:
  GMAIL_USER           Sender address. Default: gbmcoffice@gmail.com
  ALERT_RECIPIENT      Recipient address. Default: gbmcoffice@gmail.com
  DISCORD_WEBHOOK_URL  Ops-channel webhook. If set, alerts ALSO post to
                       Discord; if unset, Discord is skipped with a printed
                       note (email path unaffected).
  FORCE_ALERT          If "1", treat every retailer as alerting (preview formatting).
"""

import ast
import json
import os
import re
import smtplib
import sys
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")
GMAIL_USER = os.environ.get("GMAIL_USER", "gbmcoffice@gmail.com")
ALERT_RECIPIENT = os.environ.get("ALERT_RECIPIENT", "gbmcoffice@gmail.com")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")
FORCE_ALERT = os.environ.get("FORCE_ALERT") == "1"

CURRENT_WINDOW_HOURS = 3
PREVIOUS_WINDOW_HOURS = 3
DROP_THRESHOLD = 0.5
MIN_BASELINE = 5
PAGE_SIZE = 1000
# Badge cache refreshes on every scrape_light tick (~2h as delivered);
# 6h = ~3 missed refreshes, well before the web's 24h slow-RPC fallback.
CACHE_MAX_AGE_HOURS = 6


REPO_ROOT = Path(__file__).resolve().parents[1]
LIGHT_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "scrape_light.yml"


def fetch_retailers(sb):
    rows = (
        sb.table("retailers")
        .select("id,name,slug,last_scraped_at")
        .eq("is_active", True)
        .execute()
        .data
    )
    return {r["id"]: r for r in rows}


def count_current(sb, cutoff_iso):
    """Listings written this window: totals per retailer AND per
    (retailer, caliber) — the latter feeds the zero-coverage check."""
    counts = defaultdict(int)
    by_caliber = defaultdict(int)
    start = 0
    while True:
        end = start + PAGE_SIZE - 1
        batch = (
            sb.table("listings")
            .select("retailer_id,caliber_normalized")
            .gte("last_updated", cutoff_iso)
            .range(start, end)
            .execute()
            .data
        )
        if not batch:
            break
        for row in batch:
            counts[row["retailer_id"]] += 1
            by_caliber[(row["retailer_id"], row["caliber_normalized"])] += 1
        if len(batch) < PAGE_SIZE:
            break
        start += PAGE_SIZE
    return counts, by_caliber


def declared_coverage():
    """Coverage expectations straight from the scraper configs.

    Reads scrape_light.yml for the scraper files actually wired into the
    light tier, then AST-parses each one (no imports — module level in a
    scraper builds a Supabase client) for its module-level RETAILER_SLUG /
    RETAILER_ID and the keys of CALIBER_PATHS. Values may be strings or
    lists (targetsports holds lists since the 2026-06 category split);
    only the keys matter here. Scrapers without both names self-exclude.

    Returns a list of (slug_or_None, retailer_id_or_None, frozenset(calibers),
    filename). Never raises — a parse failure just drops that scraper from
    the check (and prints, so the gap is visible in the Actions log).
    """
    entries = []
    try:
        wf_text = LIGHT_WORKFLOW.read_text(encoding="utf-8")
    except OSError as e:
        print(f"coverage: cannot read {LIGHT_WORKFLOW} ({e}) - skipping check.")
        return entries
    for fname in re.findall(r"run:\s*python\s+(scraper_\w+\.py)", wf_text):
        path = REPO_ROOT / fname
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (OSError, SyntaxError) as e:
            print(f"coverage: cannot parse {fname} ({e}) - excluded.")
            continue
        slug = rid = calibers = None
        for node in tree.body:
            if not isinstance(node, ast.Assign) or len(node.targets) != 1:
                continue
            target = node.targets[0]
            if not isinstance(target, ast.Name):
                continue
            if target.id == "RETAILER_SLUG" and isinstance(node.value, ast.Constant):
                slug = node.value.value
            elif target.id == "RETAILER_ID" and isinstance(node.value, ast.Constant):
                rid = node.value.value
            elif target.id == "CALIBER_PATHS" and isinstance(node.value, ast.Dict):
                calibers = frozenset(
                    k.value for k in node.value.keys
                    if isinstance(k, ast.Constant) and isinstance(k.value, str)
                )
        if calibers and (slug or rid):
            entries.append((slug, rid, calibers, fname))
    return entries


def evaluate_coverage(retailers, coverage_entries, current_counts,
                      current_by_caliber, cur_start):
    """Zero-coverage rule: ran-and-finished this window, saved listings,
    but a config-covered caliber got nothing. One line per retailer."""
    alerts = []
    by_slug = {r["slug"]: rid for rid, r in retailers.items() if r.get("slug")}
    for slug, hard_id, calibers, fname in coverage_entries:
        rid = by_slug.get(slug) if slug else hard_id
        if rid is None or rid not in retailers:
            continue
        r = retailers[rid]
        last = r.get("last_scraped_at")
        if not last:
            continue
        try:
            last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
        except ValueError:
            continue
        if last_dt < cur_start:
            # No FINISHED run inside this window (mark_retailer_scraped
            # fires at the very end of a scrape) - mid-run or not-run
            # retailers would false-alarm on calibers not reached yet.
            continue
        total = current_counts.get(rid, 0)
        if total == 0:
            # Fully dark is the run-over-run CRITICAL's territory.
            continue
        missing = sorted(
            c for c in calibers if current_by_caliber.get((rid, c), 0) == 0
        )
        if missing:
            alerts.append((
                "CRITICAL",
                r["name"],
                f"zero-coverage: saved {total} listings this run but ZERO for "
                f"{len(missing)} of {len(calibers)} config-covered calibers: "
                f"{', '.join(missing)} (config: {fname})",
            ))
    return alerts


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
    for rid, r in retailers.items():
        name = r["name"]
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


def check_cache_age(sb):
    """Alert if the homepage badge cache has stopped refreshing.

    Returns a list in the same (severity, name, detail) shape evaluate()
    uses. Never raises — a failed check degrades to a WARN alert rather
    than killing the retailer health check it rides along with.
    """
    try:
        rows = (
            sb.table("homepage_segment_aggregates_cache")
            .select("refreshed_at")
            .order("refreshed_at", desc=True)
            .limit(1)
            .execute()
            .data
        )
        if not rows:
            return [("CRITICAL", "Badge cache",
                     "homepage_segment_aggregates_cache is EMPTY - homepage "
                     "badges are running on the slow live RPC")]
        newest = datetime.fromisoformat(rows[0]["refreshed_at"])
        age_h = (datetime.now(timezone.utc) - newest).total_seconds() / 3600
        if age_h > CACHE_MAX_AGE_HOURS:
            return [("CRITICAL", "Badge cache",
                     f"newest refreshed_at is {age_h:.1f}h old (threshold "
                     f"{CACHE_MAX_AGE_HOURS}h) - the refresh step is failing; "
                     "at 24h the homepage silently falls back to the slow live RPC")]
        return []
    except Exception as e:  # noqa: BLE001 - degrade, never crash the health check
        return [("WARN", "Badge cache",
                 f"cache age check itself failed ({type(e).__name__}: {e})")]


def post_discord(message):
    if not DISCORD_WEBHOOK_URL:
        print("DISCORD_WEBHOOK_URL not set - skipping Discord post.")
        return
    req = urllib.request.Request(
        DISCORD_WEBHOOK_URL,
        data=json.dumps({"content": message[:2000]}).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            # Discord's edge 403s the default Python-urllib agent.
            "User-Agent": "AmmoRaccoon-health-check/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15):
            pass
        print("Alert posted to Discord.")
    except Exception as e:  # noqa: BLE001 - Discord down must not fail the run
        print(f"Discord post failed ({type(e).__name__}: {e}) - email path unaffected.")


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
    for rid, r in sorted(retailers.items(), key=lambda kv: kv[1]["name"].lower()):
        cur = current_counts.get(rid, 0)
        prev = previous_counts.get(rid, 0)
        if cur or prev:
            lines.append(f"  {r['name']}: {cur} / {prev}")
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
    current, current_by_caliber = count_current(sb, cur_start.isoformat())
    previous = count_previous(sb, prev_start.isoformat(), prev_end.isoformat())

    print(f"Current window:  {cur_start.isoformat()} -> {now.isoformat()}")
    print(f"Previous window: {prev_start.isoformat()} -> {prev_end.isoformat()}")
    print("Per-retailer (current / previous):")
    for rid, r in sorted(retailers.items()):
        cur = current.get(rid, 0)
        prev = previous.get(rid, 0)
        if cur or prev:
            print(f"  [{rid:>2}] {r['name']}: {cur} / {prev}")

    alerts = evaluate(retailers, current, previous)
    alerts += evaluate_coverage(retailers, declared_coverage(), current,
                                current_by_caliber, cur_start)
    alerts += check_cache_age(sb)
    if not alerts:
        print("\nAll scrapers healthy -no alert sent.")
        return 0

    print(f"\n{len(alerts)} alert(s):")
    for severity, name, detail in alerts:
        print(f"  [{severity}] {name} - {detail}")

    subject = f"[AmmoRaccoon] {len(alerts)} scraper alert(s)"
    body = format_body(alerts, retailers, current, previous, (cur_start, now, prev_start, prev_end))
    send_email(subject, body)
    post_discord(
        "\n".join([f"[{sev}] {name} - {detail}" for sev, name, detail in alerts])
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
