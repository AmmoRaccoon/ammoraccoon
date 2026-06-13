"""Caliber pricing audit.

Runs after the scrape cycle. For every caliber_normalized present in
the listings table, computes count / min / max / avg of price_per_round
and flags anything that looks broken before it lands in front of users.

Rules:
  CRITICAL  min price_per_round < $0.05                 (probably a unit-conversion bug)
  CRITICAL  max price_per_round > $5.00                 (per-round should never get that high)
  CRITICAL  avg > 3x the caliber's expected max         (most listings are mispriced)
  WARNING   count < 5                                   (not enough data to be meaningful)

HEALTHY calibers produce no noise. When any WARNING/CRITICAL fires,
the full report is emailed to ALERT_RECIPIENT via Gmail SMTP — same
credentials used by health_check.py.

Required env:
  SUPABASE_URL, SUPABASE_KEY
  GMAIL_APP_PASSWORD   Gmail app password. If unset, the alert prints
                       to stdout instead of sending (local dev).

Optional env:
  GMAIL_USER           Sender address. Default: gbmcoffice@gmail.com
  ALERT_RECIPIENT      Recipient address. Default: gbmcoffice@gmail.com
  FORCE_ALERT          If "1", emails the report even when everything
                       is healthy (useful to preview formatting).
"""

import os
import smtplib
import sys
from collections import defaultdict
from email.message import EmailMessage

from dotenv import load_dotenv
from supabase import create_client

# Caliber-level sanity ranges in $/round (min = rock-bottom realistic, max =
# upper end of a premium-defensive load) used for the 3x-avg sanity check in
# evaluate(). Phase B step 1 (2026-06-12): these re-export from the generated
# caliber registry (caliber_registry_gen.py at the repo root, emitted from
# calibers.json) instead of a hand-written literal, so a new caliber's audit
# range comes from the single source of truth. Values are byte-identical to
# the prior literal (proven by scripts/check_caliber_registry.py) — zero
# behavior change. caliber_audit.py runs as `python scripts/caliber_audit.py`
# (sys.path[0] = scripts/), so add the repo root where the gen module lives.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from caliber_registry_gen import (  # noqa: E402
    AUDIT_EXPECTED_RANGES as EXPECTED_RANGES,
    AUDIT_DEFAULT_RANGE as DEFAULT_RANGE,
)

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")
GMAIL_USER = os.environ.get("GMAIL_USER", "gbmcoffice@gmail.com")
ALERT_RECIPIENT = os.environ.get("ALERT_RECIPIENT", "gbmcoffice@gmail.com")
FORCE_ALERT = os.environ.get("FORCE_ALERT") == "1"

PAGE_SIZE = 1000
MIN_COUNT = 5
MIN_PPR_FLOOR = 0.05
MAX_PPR_CEILING = 5.00
AVG_MAX_MULTIPLIER = 3
# EXPECTED_RANGES + DEFAULT_RANGE now import from caliber_registry_gen above.


def fetch_listings(sb):
    """Stream every in-stock listing's caliber + price_per_round."""
    rows = []
    start = 0
    while True:
        end = start + PAGE_SIZE - 1
        batch = (
            sb.table('listings')
            .select('caliber_normalized,price_per_round')
            .eq('in_stock', True)
            .range(start, end)
            .execute()
            .data
        )
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        start += PAGE_SIZE
    return rows


def aggregate(rows):
    by_caliber = defaultdict(list)
    for r in rows:
        cal = r.get('caliber_normalized')
        ppr = r.get('price_per_round')
        if not cal or ppr is None:
            continue
        try:
            p = float(ppr)
        except (TypeError, ValueError):
            continue
        if p <= 0:
            continue
        by_caliber[cal].append(p)

    stats = {}
    for cal, values in by_caliber.items():
        stats[cal] = {
            'count': len(values),
            'min': min(values),
            'max': max(values),
            'avg': sum(values) / len(values),
        }
    return stats


def evaluate(stats):
    """Return a list of (caliber, stats, severity, [flag_strings])."""
    results = []
    for cal, s in stats.items():
        expected_min, expected_max = EXPECTED_RANGES.get(cal, DEFAULT_RANGE)
        flags = []
        severity = 'HEALTHY'

        # WARNING conditions first — CRITICAL ones can still override below.
        if s['count'] < MIN_COUNT:
            flags.append(f"only {s['count']} listing(s); need >= {MIN_COUNT}")
            severity = 'WARNING'

        if s['min'] < MIN_PPR_FLOOR:
            flags.append(f"min ${s['min']:.4f}/rd < floor ${MIN_PPR_FLOOR:.2f}")
            severity = 'CRITICAL'

        if s['max'] > MAX_PPR_CEILING:
            flags.append(f"max ${s['max']:.2f}/rd > ceiling ${MAX_PPR_CEILING:.2f}")
            severity = 'CRITICAL'

        if s['avg'] > expected_max * AVG_MAX_MULTIPLIER:
            flags.append(
                f"avg ${s['avg']:.3f}/rd > 3x expected max "
                f"(${expected_max:.2f}) for {cal}"
            )
            severity = 'CRITICAL'

        results.append((cal, s, severity, flags))
    return results


SEVERITY_ORDER = {'CRITICAL': 0, 'WARNING': 1, 'HEALTHY': 2}


def format_report(results):
    lines = []
    lines.append("AmmoRaccoon caliber pricing audit")
    lines.append("=" * 68)
    lines.append(f"{'caliber':<10} {'count':>6}  {'min':>8}  {'avg':>8}  {'max':>8}  status")
    lines.append("-" * 68)

    sorted_results = sorted(
        results,
        key=lambda r: (SEVERITY_ORDER[r[2]], r[0]),
    )

    for cal, s, severity, flags in sorted_results:
        tag = {'HEALTHY': '[ok]  ', 'WARNING': '[WARN]', 'CRITICAL': '[CRIT]'}[severity]
        lines.append(
            f"{cal:<10} {s['count']:>6}  "
            f"${s['min']:>6.4f}  ${s['avg']:>6.4f}  ${s['max']:>6.2f}  "
            f"{tag} {severity}"
        )
        for flag in flags:
            lines.append(f"           - {flag}")

    crit = sum(1 for r in results if r[2] == 'CRITICAL')
    warn = sum(1 for r in results if r[2] == 'WARNING')
    ok = sum(1 for r in results if r[2] == 'HEALTHY')
    lines.append("-" * 68)
    lines.append(f"Totals: {crit} critical, {warn} warning, {ok} healthy")
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
    msg['Subject'] = subject
    msg['From'] = GMAIL_USER
    msg['To'] = ALERT_RECIPIENT
    msg.set_content(body)
    with smtplib.SMTP('smtp.gmail.com', 587) as smtp:
        smtp.starttls()
        smtp.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        smtp.send_message(msg)
    print(f"Alert email sent to {ALERT_RECIPIENT}.")


def main():
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    rows = fetch_listings(sb)
    if not rows:
        print("No listings found — nothing to audit.")
        return 0

    stats = aggregate(rows)
    results = evaluate(stats)
    report = format_report(results)
    print(report)

    crit = sum(1 for r in results if r[2] == 'CRITICAL')
    warn = sum(1 for r in results if r[2] == 'WARNING')

    if crit == 0 and warn == 0 and not FORCE_ALERT:
        print("\nAll calibers healthy - no alert sent.")
        return 0

    subject = f"[AmmoRaccoon] Caliber audit: {crit} critical, {warn} warning"
    send_email(subject, report)
    return 0


if __name__ == '__main__':
    sys.exit(main())
