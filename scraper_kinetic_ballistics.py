"""scraper_kinetic_ballistics.py — manufacturer ballistics scraper for SFCC/Demandware-backed brands.

Originally written for The Kinetic Group brands (Federal, Remington, CCI,
Speer) which all publish their consumer catalog on the same Salesforce
Commerce Cloud (Demandware) backend. Subsequently extended to Fiocchi USA
on 2026-05-17 — separate corporate parent (Fiocchi is unrelated to Vista's
Kinetic Group), same SFCC backend, identical parser shape. **This file
groups by backend signature, not by corporate parent.**

Any future SFCC-backed manufacturer can be added with just a new SOURCES
entry; no parser changes needed. Compatibility checklist on a candidate
product page:
  - JSON-LD <script type="application/ld+json"> with @type:Product whose
    `name` field follows "<line>, <caliber>, <grain> Grain, <bullet>, ... fps".
  - A spec <table> with rows like <td>Muzzle Velocity</td><td>1200</td>.
  - <script id="chart-data-velocity"> and <script id="chart-data-energy">
    with points arrays like [[1200,1138,1086,1043,1007]].
If a candidate site has all three, this parser handles it as-is.

Each product page exposes ballistics in three structurally distinct places,
in order of preference:
  1. <script type="application/ld+json"> with @type:Product — gives sku, name
     (which embeds caliber + grain + bullet type + muzzle velocity), and brand.
  2. A spec <table> with rows like <td>Muzzle Velocity</td><td>1180</td> and
     <td>Test Barrel Length In</td><td>4</td>.
  3. <script id="chart-data-velocity"> and <script id="chart-data-energy">
     with downrange points like {"points":[[1180,1106,1048,1001,961]],"labels":[0,25,50,75,100]}.
     This is the only source for 50-yard and 100-yard velocities.

Writes to:
  manufacturer_ballistics  (one row per product page)

Required env:
  SUPABASE_URL, SUPABASE_KEY

Usage:
  python scraper_kinetic_ballistics.py --dry-run
  python scraper_kinetic_ballistics.py --source federal
"""

import argparse
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_KEY']

USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
)

# Caliber-name → caliber_normalized (matches listings.caliber_normalized).
# Mirrors the values in scraper_lib but kept local so this module doesn't
# pull in scraper_lib's playwright import path.
CALIBER_NORMALIZE = {
    '9mm luger': '9mm',
    '9mm': '9mm',
    '380 acp': '380acp', '.380 acp': '380acp', '.380 auto': '380acp',
    '38 special': '38spl', '.38 special': '38spl', '38 spl': '38spl',
    '357 magnum': '357mag', '.357 magnum': '357mag', '357 mag': '357mag',
    '40 s&w': '40sw', '.40 s&w': '40sw', '.40 smith & wesson': '40sw',
    '22 lr': '22lr', '.22 lr': '22lr', '.22 long rifle': '22lr',
    '223 remington': '223-556', '.223 remington': '223-556', '5.56 nato': '223-556',
    '5.56x45mm nato': '223-556', '5.56': '223-556',
    '308 winchester': '308win', '.308 winchester': '308win', '.308 win': '308win',
    '7.62x39mm': '762x39', '7.62x39': '762x39',
    '300 blackout': '300blk', '300 aac blackout': '300blk', '.300 blackout': '300blk',
}

SOURCES = {
    'federal': {
        'brand': 'Federal',
        'base_url': 'https://www.federalpremium.com',
        # Seed URLs for initial dev/dry-run. Discovery via category-page crawl
        # (e.g. /handgun/shop-by-caliber/9mm-luger/) is a separate follow-up.
        'seed_urls': [
            'https://www.federalpremium.com/handgun/american-eagle/american-eagle-handgun/11-AE9DP.html',
            'https://www.federalpremium.com/handgun/american-eagle/american-eagle-handgun/11-AE9AP.html',
            'https://www.federalpremium.com/handgun/american-eagle/american-eagle-handgun/11-AE9FP.html',
            'https://www.federalpremium.com/handgun/american-eagle/american-eagle-handgun/11-AE9DP100.html',
            'https://www.federalpremium.com/handgun/syntech-defense/syntech-defense/11-S9SJT1.html',  # 9mm 138gr Syntech Defense SJHP, 20-count (audit row #14 was wrong about phantom Punch 138gr — real product is Syntech Defense)
            'https://www.federalpremium.com/handgun/syntech-defense/syntech-defense/11-S9SJT2.html',  # 9mm 138gr Syntech Defense SJHP, 50-count
            'https://www.federalpremium.com/handgun/personal-defense-hst/11-P9HST2S.html',  # 9mm 147gr HST JHP (audit row #1 MEDIUM, audit URL was LE variant /personal-defense-hst/personal-defense-hst/11-P9HST2.html which 500'd; this is the Personal Defense variant suffix)
            'https://www.federalpremium.com/handgun/hydra-shok-deep/hydra-shok-deep/11-P9HSD1.html',  # 9mm 135gr Hydra-Shok Deep JHP (audit row #63 MEDIUM, audit URL verified correct)
            # .40 S&W — Tier 1 (2026-05-27): training FMJ + flagship HST. FMJ velocity is
            # consistent across Federal .40 FMJ lines, so the displayed number is honest
            # regardless of which FMJ line a listing actually is. The 165gr JHP group is
            # deliberately HELD (Hydra-Shok/Punch/Hydra-Shok Deep share 165gr JHP, ~150fps
            # spread, and the matcher is line-blind) — see TASKS matcher-line-awareness.
            'https://www.federalpremium.com/handgun/american-eagle/american-eagle-handgun/11-AE40R1.html',  # .40 S&W 180gr FMJ, 1000 fps (verified live)
            'https://www.federalpremium.com/handgun/american-eagle/american-eagle-handgun/11-AE40R3.html',  # .40 S&W 165gr FMJ, 1130 fps
            'https://www.federalpremium.com/handgun/american-eagle/american-eagle-handgun/11-AE40R2.html',  # .40 S&W 155gr FMJ, 1160 fps
            'https://www.federalpremium.com/handgun/premium-personal-defense/personal-defense-hst/11-P40HST1S.html',  # .40 S&W 180gr HST JHP, 1010 fps (verified live)
        ],
    },
    'remington': {
        'brand': 'Remington',
        'base_url': 'https://www.remington.com',
        'seed_urls': [
            'https://www.remington.com/handgun/umc-handgun/29-23753.html',
            'https://www.remington.com/handgun/umc-handgun/29-23732.html',
            'https://www.remington.com/handgun/umc-handgun/29-23718.html',
            'https://www.remington.com/handgun/remington-range/29-R27778.html',
            'https://www.remington.com/handgun/high-terminal-performance/29-28288.html',
            'https://www.remington.com/handgun/golden-saber-defense/29-27604.html',  # 9mm 147gr Golden Saber Defense JHP, internal SKU GS9MMC (audit row #22 MEDIUM, audit URL /handgun/golden-saber-defense/29-GS9MMC.html 404'd — Remington uses numeric IDs in URLs not SKU strings)
            # .40 S&W — Tier 1 + 165gr JHP (2026-05-27): UMC training FMJ + HTP 155gr JHP
            # (sole 155gr line) + Golden Saber Defense 165gr JHP. The 165gr JHP is safe to
            # stamp one row across all 18 listings because Remington spec'd ALL its .40
            # 165gr defense lines (Golden Saber Defense/Bonded + Ultimate Defense) to the
            # SAME 1150 fps — zero velocity spread (HTP makes no 165gr .40). The 180gr JHP
            # group is HELD: Golden Saber Compact's 785 fps vs standard 1015 fps is a
            # 230fps within-grain collision — see TASKS matcher-line-awareness.
            'https://www.remington.com/handgun/umc-handgun/29-23742.html',  # .40 S&W 180gr FMJ, 990 fps (verified live)
            'https://www.remington.com/handgun/umc-handgun/29-23746.html',  # .40 S&W 165gr FMJ, 1150 fps (verified live)
            'https://www.remington.com/handgun/high-terminal-performance/29-22306.html',  # .40 S&W 155gr JHP, 1205 fps (sole 155gr line)
            'https://www.remington.com/handgun/golden-saber-defense/29-27607.html',  # .40 S&W 165gr JHP, 1150 fps (all 165gr defense lines = 1150)
        ],
    },
    'cci': {
        'brand': 'CCI',
        'base_url': 'https://www.cci-ammunition.com',
        'seed_urls': [
            'https://www.cci-ammunition.com/handgun/blazer/blazer-brass/6-5200.html',
            'https://www.cci-ammunition.com/handgun/blazer/blazer-brass-hp/6-5239.html',
            'https://www.cci-ammunition.com/handgun/blazer/blazer-brass/6-5201.html',
            'https://www.cci-ammunition.com/handgun/blazer/blazer-aluminum/6-3509.html',
            'https://www.cci-ammunition.com/handgun/blazer/blazer-brass/6-5203.html',  # 9mm 147gr FMJ (audit row #16, corrected from audit's wrong SKU 5202 which is .380 95gr)
            # .40 S&W — Tier 1 brass (2026-05-28): single-line clean per (grain,bullet), zero
            # within-grain collision on the brass side. Matches Blazer-tagged listings via
            # BRAND_ALIASES CCI<-Blazer. ~61 in-stock .40 listings.
            'https://www.cci-ammunition.com/handgun/blazer/blazer-brass/6-5210.html',     # .40 165gr FMJ, 1050 fps
            'https://www.cci-ammunition.com/handgun/blazer/blazer-brass/6-5220.html',     # .40 180gr FMJ, 985 fps
            'https://www.cci-ammunition.com/handgun/blazer/blazer-brass-hp/6-5241.html',  # .40 180gr JHP, 1015 fps
            # Blazer Aluminum 3589/3591 evaluated 2026-05-28 and DROPPED: CCI labels aluminum
            # "Full Metal Jacket" so the parser emits FMJ (not TMJ), which cannot reach the
            # TMJ-tagged listings and would collide 50fps with brass 5210 at 165gr. Zero gain.
            # Clean-Fire 3477 — KEPT (2026-05-28): CCI labels it "Total Metal Jacket" so the
            # parser emits TMJ; sole TMJ-180 source (no collision), recovers 6 TMJ-tagged .40
            # listings. The 3 165gr-TMJ listings have no CCI .40 product (permanently unmatched).
            'https://www.cci-ammunition.com/handgun/blazer/blazer-clean-fire/6-3477.html',  # .40 180gr TMJ, 1000 fps
        ],
    },
    'speer': {
        'brand': 'Speer',
        'base_url': 'https://www.speer.com',
        'seed_urls': [
            'https://www.speer.com/ammunition/lawman/lawman-handgun-training/19-53620.html',
            'https://www.speer.com/ammunition/lawman/lawman-handgun-training/19-53651.html',
            'https://www.speer.com/ammunition/lawman/lawman-handgun-training/19-53661.html',
            'https://www.speer.com/ammunition/gold-dot/gold-dot-handgun-personal-protection/19-23614GD.html',
            'https://www.speer.com/ammunition/gold-dot/gold-dot-handgun-personal-protection/19-23618GD.html',
            'https://www.speer.com/ammunition/gold-dot/gold-dot-handgun-personal-protection/19-23619GD.html',  # 9mm 147gr Gold Dot JHP (audit row #17)
            'https://www.speer.com/ammunition/lawman/lawman-handgun-training/19-53650.html',  # 9mm 115gr Lawman TMJ (audit row #24, corrected from broken audit URL 19-53615 — recovered via 2026-05-17 spec-table fallback after Demandware template drift broke JSON-LD name regex)
            'https://www.speer.com/ammunition/gold-dot/gold-dot-carry-gun/19-24260.html',  # 9mm 135gr Gold Dot Carry Gun JHP (audit row #55 MEDIUM, audit SKU 23922GD was wrong — real SKU 24260)
            # .40 S&W — Tier 1 + 165gr JHP (2026-05-27): Lawman 180 TMJ + Gold Dot 165 JHP.
            # 180 TMJ: Lawman standard + Clean-Fire variant both 1000 fps (zero spread).
            # 165 JHP: Gold Dot Personal Protection + Carry Gun both 1150 fps (zero spread,
            # same convergent-velocity logic as the approved Remington 165 JHP). Held lines:
            # 180gr JHP (PP 1025 / G2 1015 / Short Barrel 950 = 75 fps spread, short-barrel
            # buyers need the right number) and 165gr TMJ (Lawman 1150 vs Clean-Fire 1050 =
            # 100 fps spread) — see TASKS matcher-line-awareness.
            'https://www.speer.com/ammunition/lawman/lawman-handgun-training/19-53652.html',  # .40 S&W 180gr TMJ, 1000 fps (verified live)
            'https://www.speer.com/ammunition/gold-dot/gold-dot-handgun-personal-protection/19-23970GD.html',  # .40 S&W 165gr JHP, 1150 fps (verified live)
        ],
    },
    # Fiocchi USA runs on the same Salesforce Commerce Cloud backend as the
    # Kinetic Group brands above (separate company, same SFCC signature).
    # Added 2026-05-17 for the 9mm coverage epic Sub-task 2. URL shape is
    # /centerfire-pistol/<line-slug>/33-<sku>.html — the "33-" prefix is
    # Fiocchi's Demandware master-style code.
    'fiocchi': {
        'brand': 'Fiocchi',
        'base_url': 'https://fiocchiusa.com',
        'seed_urls': [
            'https://fiocchiusa.com/centerfire-pistol/range-dynamics/33-9AP.html',       # 115gr FMJ
            'https://fiocchiusa.com/centerfire-pistol/defense-dynamics/33-9APDHP.html',  # 147gr JHP
        ],
    },
}


@dataclass
class ParsedBallistics:
    external_id: str           # SKU or stable URL slug
    source_url: str
    sku: Optional[str] = None
    product_line: Optional[str] = None
    caliber_normalized: Optional[str] = None
    grain: Optional[int] = None
    bullet_type: Optional[str] = None
    muzzle_velocity_fps: Optional[int] = None
    muzzle_energy_ftlb: Optional[int] = None
    bc_g1: Optional[float] = None
    velocity_50yd: Optional[int] = None
    velocity_100yd: Optional[int] = None
    raw_name: Optional[str] = None


def fetch(url: str) -> str:
    resp = requests.get(
        url,
        headers={'User-Agent': USER_AGENT, 'Accept-Encoding': 'gzip'},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.text


def _normalize_caliber(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    key = s.lower().strip().replace('  ', ' ')
    return CALIBER_NORMALIZE.get(key)


# Sorted by needle length descending so the most specific phrase wins.
# Note "hollow point" maps to JHP (not HP): retailers' listings.bullet_type
# overwhelmingly uses JHP for any hollow-point product, even when the
# manufacturer markets it as plain "Hollow Point" (Speer Gold Dot).
# Aligning the manufacturer parser with that convention keeps the
# 4-column equi-join in the matcher productive.
_BULLET_TYPE_LOOKUP = [
    ('jacketed hollow point', 'JHP'),
    ('total metal jacket', 'TMJ'),
    ('full metal jacket', 'FMJ'),
    ('open tip match', 'OTM'),
    ('hollow point', 'JHP'),
    ('soft point', 'SP'),
    ('round nose', 'LRN'),
    ('flat point', 'FP'),
]


def _normalize_bullet_type(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    low = text.lower()
    for needle, code in _BULLET_TYPE_LOOKUP:
        if needle in low:
            return code
    # Fall back to existing 2-3 letter codes if the input is already short.
    short = text.strip().upper()
    if 2 <= len(short) <= 4 and short.isalpha():
        return short
    return None


def _find_jsonld_product(soup) -> Optional[dict]:
    """Return the @type:Product JSON-LD payload, or None."""
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            payload = json.loads(script.string or '{}')
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        # @graph wrappers are allowed by spec; flatten if present.
        candidates = payload if isinstance(payload, list) else [payload]
        for item in candidates:
            if isinstance(item, dict) and item.get('@type') == 'Product':
                return item
    return None


def _spec_table_value(soup, label: str) -> Optional[str]:
    """Return the value cell from a 2-col spec <table> row matching label."""
    for tr in soup.find_all('tr'):
        cells = tr.find_all('td')
        if len(cells) != 2:
            continue
        if cells[0].get_text(strip=True).lower() == label.lower():
            return cells[1].get_text(strip=True)
    return None


def _chart_points(soup, chart_id: str) -> list:
    """Return points[0] from a <script id="chart-data-X"> JSON payload, or []."""
    script = soup.find('script', id=chart_id)
    if not script:
        return []
    try:
        data = json.loads(script.string or '{}')
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    points = data.get('points') or []
    return points[0] if points and isinstance(points[0], list) else []


def _product_line_from_url(url: str) -> Optional[str]:
    """Extract product line from the URL path segment immediately before
    the SKU filename. Used as a fallback when the JSON-LD name's
    first-comma-segment doesn't produce a clean line (canonical case:
    Speer Lawman 53650's refreshed Demandware template dropped the
    comma-tuple shape from the name field).

    Examples:
      /ammunition/lawman/lawman-handgun-training/19-53650.html
        -> "Lawman Handgun Training"
      /handgun/blazer/blazer-brass/6-5200.html -> "Blazer Brass"
      /centerfire-pistol/range-dynamics/33-9AP.html -> "Range Dynamics"

    Title-cases naively from the hyphenated slug. Brands with uppercase
    acronyms in their line names (e.g. "Personal Defense HST") would
    render here as "Personal Defense Hst" — cosmetic only since the
    matcher doesn't use product_line. Existing comma-tuple SKUs are
    unaffected because the JSON-LD name path runs first and produces
    canonical capitalization."""
    parts = url.rstrip('/').split('/')
    if len(parts) < 2:
        return None
    slug = parts[-2]
    if not slug or '/' in slug:
        return None
    return ' '.join(word.capitalize() for word in slug.split('-') if word)


def parse_product_page(html: str, source_url: str) -> ParsedBallistics:
    soup = BeautifulSoup(html, 'html.parser')

    product = _find_jsonld_product(soup) or {}

    sku = product.get('sku')
    name = product.get('name') or ''

    # Pull caliber + grain + bullet from the JSON-LD name when present.
    # Format: "<line>, <caliber>, <grain> Grain, <bullet>, <fps> fps ..."
    caliber_text = None
    grain = None
    bullet_text = None

    # Caliber: between first comma and "<N> Grain".
    cal_match = re.search(r',\s*([^,]+?),\s*\d+\s+Grain', name, re.IGNORECASE)
    if cal_match:
        caliber_text = cal_match.group(1).strip()

    grain_match = re.search(r'(\d+)\s+Grain', name, re.IGNORECASE)
    if grain_match:
        grain = int(grain_match.group(1))

    bullet_match = re.search(r'Grain,\s*([^,]+?),', name, re.IGNORECASE)
    if bullet_match:
        bullet_text = bullet_match.group(1).strip()

    # Extract product_line from the JSON-LD name's first comma-separated
    # segment, IF the name has the comma-tuple shape. Names without
    # commas (e.g. Speer 53650's refreshed Demandware template that
    # ships the name as "Lawman Handgun Training 9mm Luger Caliber Ammo
    # - 50 Rounds of 115 Grain Weight Ammunition (53650)") fall through
    # to the URL-slug fallback below.
    product_line = None
    if ',' in name:
        line_match = re.match(r'^([^,]+),', name)
        if line_match:
            product_line = line_match.group(1).strip()

    # Title-tag fallbacks for grain + velocity.
    title_tag = soup.find('title')
    title_text = title_tag.get_text() if title_tag else ''
    if grain is None:
        m = re.search(r'(\d+)\s*Grain', title_text, re.IGNORECASE)
        if m:
            grain = int(m.group(1))

    # Spec-table fallback for caliber / grain / bullet when the JSON-LD
    # name regex fails. Salesforce Commerce templates refreshed mid-2026
    # to ship product names without the comma-separated tuple shape that
    # parse-from-name relied on (canonical case: Speer Lawman 53650 — see
    # web repo TASKS "Speer Lawman 53650 Demandware template drift").
    # The <table class="table"> spec block on SFCC product pages exposes
    # the same fields under stable labels (Caliber / Grain Weight /
    # Bullet Style) and is a safer extraction surface across future
    # template revisions. Only fires when the primary regex returns
    # None, so existing comma-tuple SKUs produce byte-identical output.
    if caliber_text is None:
        caliber_text = _spec_table_value(soup, 'Caliber')
    if grain is None:
        weight_text = _spec_table_value(soup, 'Grain Weight')
        if weight_text:
            m = re.search(r'(\d+)', weight_text)
            if m:
                grain = int(m.group(1))
    if bullet_text is None:
        bullet_text = _spec_table_value(soup, 'Bullet Style')
    if product_line is None:
        product_line = _product_line_from_url(source_url)

    # Muzzle velocity: prefer the spec table (a bare integer with no fps suffix
    # makes for the cleanest extraction); fall back to title regex.
    mv_text = _spec_table_value(soup, 'Muzzle Velocity')
    muzzle_velocity = None
    if mv_text:
        m = re.search(r'(\d+)', mv_text)
        if m:
            muzzle_velocity = int(m.group(1))
    if muzzle_velocity is None:
        m = re.search(r'(\d+)\s*FPS', title_text, re.IGNORECASE)
        if m:
            muzzle_velocity = int(m.group(1))

    # Downrange velocity from the chart JSON. labels = [0,25,50,75,100].
    velocity_points = _chart_points(soup, 'chart-data-velocity')
    velocity_50yd = None
    velocity_100yd = None
    if len(velocity_points) >= 5:
        velocity_50yd = int(velocity_points[2])    # 50 Y
        velocity_100yd = int(velocity_points[4])   # 100 Y
        # Sanity-check muzzle velocity against the chart's first point.
        chart_muzzle = int(velocity_points[0])
        if muzzle_velocity is None:
            muzzle_velocity = chart_muzzle
        elif muzzle_velocity != chart_muzzle:
            # Trust the spec table over the chart if they disagree; print a hint.
            print(f'    [warn] muzzle velocity mismatch: spec_table={muzzle_velocity} '
                  f'chart={chart_muzzle} url={source_url}')

    # Muzzle energy from the energy chart.
    energy_points = _chart_points(soup, 'chart-data-energy')
    muzzle_energy = int(energy_points[0]) if energy_points else None

    # external_id: prefer sku; fall back to URL filename.
    external_id = sku or source_url.rstrip('/').rsplit('/', 1)[-1].replace('.html', '')

    return ParsedBallistics(
        external_id=external_id,
        source_url=source_url,
        sku=sku,
        product_line=product_line,
        caliber_normalized=_normalize_caliber(caliber_text),
        grain=grain,
        bullet_type=_normalize_bullet_type(bullet_text),
        muzzle_velocity_fps=muzzle_velocity,
        muzzle_energy_ftlb=muzzle_energy,
        velocity_50yd=velocity_50yd,
        velocity_100yd=velocity_100yd,
        raw_name=name or None,
    )


def upsert_ballistics(supabase, source: str, brand: str,
                      bal: ParsedBallistics, html_hash: str) -> int:
    if bal.muzzle_velocity_fps is None:
        raise ValueError(
            f'{bal.external_id}: muzzle_velocity_fps is null; refusing to insert.'
        )

    now = datetime.now(timezone.utc).isoformat()
    row = {
        'external_id': bal.external_id,
        'source': source,
        'brand': brand,
        'sku': bal.sku,
        'product_line': bal.product_line,
        'caliber_normalized': bal.caliber_normalized,
        'grain': bal.grain,
        'bullet_type': bal.bullet_type,
        'muzzle_velocity_fps': bal.muzzle_velocity_fps,
        'muzzle_energy_ftlb': bal.muzzle_energy_ftlb,
        'bc_g1': bal.bc_g1,
        'velocity_50yd': bal.velocity_50yd,
        'velocity_100yd': bal.velocity_100yd,
        'source_url': bal.source_url,
        'last_seen_at': now,
        'last_scraped_at': now,
        'raw_html_hash': html_hash,
    }
    res = (
        supabase.table('manufacturer_ballistics')
        .upsert(row, on_conflict='source,external_id')
        .execute()
    )
    return res.data[0]['id']


def scrape_source(source: str, dry_run: bool, supabase=None) -> int:
    cfg = SOURCES[source]
    print(f'\n=== {source} ({cfg["brand"]}) ===')

    saved = 0
    for url in cfg['seed_urls']:
        try:
            html = fetch(url)
        except Exception as e:
            print(f'\n  FETCH FAILED {url}: {e}')
            continue
        html_hash = hashlib.sha256(html.encode('utf-8')).hexdigest()
        bal = parse_product_page(html, url)

        print(f'\n  [{bal.external_id}] {bal.product_line or "?"}')
        print(f'    raw name: {bal.raw_name!r}')
        print(f'    caliber={bal.caliber_normalized!r}  grain={bal.grain}  '
              f'bullet={bal.bullet_type!r}  sku={bal.sku!r}')
        print(f'    muzzle_velocity={bal.muzzle_velocity_fps} fps  '
              f'muzzle_energy={bal.muzzle_energy_ftlb} ft-lb')
        print(f'    velocity_50yd={bal.velocity_50yd}  velocity_100yd={bal.velocity_100yd}')

        if dry_run:
            saved += 1
            continue
        try:
            upsert_ballistics(supabase, source, cfg['brand'], bal, html_hash)
            saved += 1
        except Exception as e:
            print(f'    UPSERT FAILED: {e}')

    return saved


def main() -> int:
    parser = argparse.ArgumentParser(description='Scrape Kinetic-brand ballistics.')
    parser.add_argument('--dry-run', action='store_true',
                        help='Parse and print only; no DB writes.')
    parser.add_argument('--source', choices=list(SOURCES.keys()) + ['all'], default='all',
                        help='Which source to scrape. Default: all known sources.')
    args = parser.parse_args()

    supabase = None if args.dry_run else create_client(SUPABASE_URL, SUPABASE_KEY)
    sources = list(SOURCES.keys()) if args.source == 'all' else [args.source]

    total = 0
    for s in sources:
        try:
            total += scrape_source(s, args.dry_run, supabase=supabase)
        except Exception as e:
            print(f'  source {s} FAILED: {e}')

    mode = 'DRY RUN' if args.dry_run else 'LIVE'
    print(f'\nDone ({mode}). {total} product(s) {"would be " if args.dry_run else ""}upserted.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
