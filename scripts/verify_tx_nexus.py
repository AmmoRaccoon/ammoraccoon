"""verify_tx_nexus.py — surface evidence for unverified TX tax_nexus rows.

For each retailer with tax_nexus.state_code='TX' AND verified=False, this
script probes likely policy pages on the retailer's website (static paths
like /sales-tax, plus footer links discovered on the homepage), looks for
explicit text claims about Texas sales-tax collection, and classifies the
finding as one of:

  confirmed    — page text says they collect sales tax in TX
  denies       — page text says they do NOT collect sales tax in TX
  silent       — pages reachable but no TX-specific tax claim found
  fetch_failed — couldn't reach any candidate page (blocked, 404, etc.)
  no_website   — retailer row has no website_url

This script is READ-ONLY against the database — it never flips
tax_nexus.verified. Output is meant to feed a manual review.

Usage:
  python scripts/verify_tx_nexus.py
  python scripts/verify_tx_nexus.py --retailer-id 5      # single retailer
  python scripts/verify_tx_nexus.py --max-candidates 20  # broaden the probe
"""

import argparse
import os
import re
import sys
import time
from typing import Optional, Tuple
from urllib.parse import urljoin, urlparse

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

# Common URL paths that retailers use for tax/shipping policy pages.
# Probed on every retailer regardless of platform.
STATIC_PATHS = [
    '/tax', '/taxes', '/tax-info', '/tax-policy',
    '/sales-tax', '/sales-tax-policy', '/sales-tax-info',
    '/shipping-tax', '/shipping-and-tax', '/shipping-and-taxes',
    '/pages/tax', '/pages/sales-tax', '/pages/shipping-policy',
    '/policies/tax', '/policies/refund-policy',
    '/help/sales-tax', '/customer-service/sales-tax',
    '/info/tax', '/info/sales-tax', '/about/tax',
    '/faq', '/help', '/customer-service',
    '/shipping', '/shipping-policy',
]

# Anchor text / href substrings that suggest a tax or policy page in a footer.
FOOTER_KEYWORDS = ('tax', 'shipping', 'sales-tax', 'sales tax', 'policy', 'faq')

# Tax-collection language. Case-insensitive, applied to the paragraph text.
COLLECT_PATTERNS = [
    r'\b(?:we |are )?required to (?:collect|charge)',
    r'\bwe (?:must |will |currently )?(?:collect|charge|apply|add)\b',
    r'\bsales tax (?:is|are|will|may|must|shall) (?:be )?(?:applied|charged|collected|added)\b',
    r'\b(?:are|will be|is|may be) (?:charged|collected|subject to)\b',
    r'\bcollects? (?:applicable )?(?:sales |state |local )?tax',
    r'\bcharge(?:s|d)? (?:applicable )?(?:sales |state )?tax',
    r'\b(?:applicable )?sales tax (?:will be |is |are )?added',
]

# Negation cues. If present in the same paragraph as a TX/Texas mention,
# we lean toward 'denies' — though a confirmed match elsewhere on the page
# still wins (some sites list both "we collect in" and "we don't collect in").
NEGATION_PATTERNS = [
    r'\bdo (?:not|n\'t)\s+(?:collect|charge|apply)',
    r'\bdoes (?:not|n\'t)\s+(?:collect|charge|apply)',
    r'\b(?:are|is) not required to (?:collect|charge)',
    r'\bno sales tax\b',
    r'\btax[- ]free\b',
    r'\bexempt from (?:sales )?tax',
]

# Words that suggest the page is a Cloudflare/Akamai challenge, not a real page.
CHALLENGE_MARKERS = (
    'just a moment', 'checking your browser', 'cf-challenge',
    'access denied', 'attention required', 'enable javascript and cookies',
)

PRIORITY = {'confirmed': 3, 'denies': 2, 'silent': 1, 'fetch_failed': 0, 'no_website': 0}


_session = requests.Session()
_session.headers.update({
    'User-Agent': USER_AGENT,
    'Accept-Encoding': 'gzip',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
})


def fetch(url: str, timeout: int = 15) -> Optional[requests.Response]:
    try:
        r = _session.get(url, timeout=timeout, allow_redirects=True)
    except Exception:
        return None
    if r.status_code >= 400:
        return None
    body = r.text or ''
    if len(body) < 2500 and any(m in body.lower() for m in CHALLENGE_MARKERS):
        return None
    return r


def discover_candidates(homepage_url: str) -> list:
    """Return URLs to probe in priority order. Footer-discovered links come
    first (most likely to be real on a given retailer's site), then the
    list of common static paths as fallback for sites that don't surface
    a tax/policy link from the homepage."""
    footer_links: list = []
    home = fetch(homepage_url)
    if home is not None:
        soup = BeautifulSoup(home.text, 'html.parser')
        home_host = urlparse(homepage_url).netloc
        for a in soup.find_all('a', href=True):
            href = a['href']
            text = (a.get_text() or '').strip().lower()
            href_low = href.lower()
            if any(kw in href_low or kw in text for kw in FOOTER_KEYWORDS):
                abs_url = urljoin(homepage_url, href).split('#', 1)[0]
                if urlparse(abs_url).netloc == home_host:
                    footer_links.append(abs_url)

    static_links = [homepage_url.rstrip('/') + p for p in STATIC_PATHS]
    return list(dict.fromkeys(footer_links + static_links))


def _extract_text(html: str) -> str:
    soup = BeautifulSoup(html, 'html.parser')
    for tag in soup(['script', 'style', 'noscript', 'svg']):
        tag.decompose()
    return soup.get_text(separator='\n')


def _split_paragraphs(text: str) -> list:
    norm = re.sub(r'[ \t]+', ' ', text)
    norm = re.sub(r'\n{2,}', '\n\n', norm)
    paras = [re.sub(r'\s+', ' ', p).strip() for p in norm.split('\n\n')]
    return [p for p in paras if p]


def classify_page(text: str, url: str) -> Tuple[str, Optional[Tuple[str, str]]]:
    """Return (status, (url, quote)) — status is confirmed/denies/silent."""
    best_status = 'silent'
    best_evidence: Optional[Tuple[str, str]] = None

    for para in _split_paragraphs(text):
        # Word-boundary 'TX' or 'Texas' — avoids matching "TXS", "Texas A&M" is fine.
        if not re.search(r'\b(?:Texas|TX)\b', para):
            continue
        if not re.search(r'\btax', para, re.IGNORECASE):
            continue

        para_low = para.lower()
        has_collect = any(re.search(p, para_low) for p in COLLECT_PATTERNS)
        has_negation = any(re.search(p, para_low) for p in NEGATION_PATTERNS)

        if has_negation and not has_collect:
            status = 'denies'
        elif has_collect and not has_negation:
            status = 'confirmed'
        elif has_collect and has_negation:
            # Both present — usually "we do not collect except in [list]". Lean confirmed
            # only if a state list with TX is in the same paragraph; otherwise denies.
            if re.search(r'(?:[A-Z]{2}[, ]+){3,}', para):
                status = 'confirmed'
            else:
                status = 'denies'
        else:
            status = 'silent'

        if PRIORITY[status] > PRIORITY[best_status]:
            best_status = status
            # Trim to a readable excerpt centered on the TX/Texas mention.
            m = re.search(r'\b(?:Texas|TX)\b', para)
            if m:
                start = max(0, m.start() - 120)
                end = min(len(para), m.end() + 200)
                quote = ('...' if start > 0 else '') + para[start:end] + ('...' if end < len(para) else '')
            else:
                quote = para[:300]
            best_evidence = (url, quote)
    return best_status, best_evidence


def check_retailer(retailer: dict, max_candidates: int) -> dict:
    rid = retailer['id']
    slug = retailer['slug']
    home = retailer.get('website_url')
    if not home:
        return {'rid': rid, 'slug': slug, 'status': 'no_website',
                'evidence': None, 'pages_checked': 0, 'candidates': []}

    candidates = discover_candidates(home)[:max_candidates]
    pages_checked = 0
    best_status = 'silent'
    best_evidence: Optional[Tuple[str, str]] = None

    for url in candidates:
        r = fetch(url)
        if r is None:
            continue
        pages_checked += 1
        text = _extract_text(r.text)
        status, ev = classify_page(text, r.url)
        if PRIORITY[status] > PRIORITY[best_status]:
            best_status = status
            best_evidence = ev
        # Light politeness delay between requests on the same domain.
        time.sleep(0.2)

    if pages_checked == 0:
        return {'rid': rid, 'slug': slug, 'status': 'fetch_failed',
                'evidence': None, 'pages_checked': 0, 'candidates': candidates}
    return {'rid': rid, 'slug': slug, 'status': best_status,
            'evidence': best_evidence, 'pages_checked': pages_checked,
            'candidates': candidates}


def main() -> int:
    parser = argparse.ArgumentParser(description='Probe retailer policy pages for TX-tax claims.')
    parser.add_argument('--retailer-id', type=int, help='Check just this retailer_id.')
    parser.add_argument('--max-candidates', type=int, default=18,
                        help='Cap on URLs probed per retailer (default 18).')
    args = parser.parse_args()

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    nx = (sb.table('tax_nexus')
            .select('retailer_id')
            .eq('state_code', 'TX')
            .eq('verified', False)
            .execute())
    ids = sorted({r['retailer_id'] for r in nx.data})
    if args.retailer_id is not None:
        ids = [i for i in ids if i == args.retailer_id]

    if not ids:
        print('No unverified TX nexus rows match.')
        return 0

    rs = sb.table('retailers').select('id,slug,name,website_url').in_('id', ids).execute()
    retailers = sorted(rs.data, key=lambda r: r['id'])

    print(f'Checking {len(retailers)} retailer(s) with unverified TX nexus '
          f'(up to {args.max_candidates} pages each)...\n')

    results = []
    for r in retailers:
        out = check_retailer(r, args.max_candidates)
        marker = {
            'confirmed': '+',
            'denies': '-',
            'silent': '?',
            'fetch_failed': 'x',
            'no_website': 'x',
        }[out['status']]
        print(f'  {marker} [{out["rid"]:>3}] {out["slug"]:<28} '
              f'{out["status"]:<13} ({out["pages_checked"]} page(s) reachable)')
        results.append(out)

    by_status: dict = {}
    for o in results:
        by_status.setdefault(o['status'], []).append(o)

    print()
    print('=' * 72)
    print(f'Summary: {len(results)} retailer(s) checked')
    print('=' * 72)
    for status in ('confirmed', 'denies', 'silent', 'fetch_failed', 'no_website'):
        items = by_status.get(status, [])
        print(f'\n--- {status} ({len(items)}) ---')
        for o in items:
            print(f'  [{o["rid"]:>3}] {o["slug"]}')
            if o['evidence']:
                src, quote = o['evidence']
                print(f'        source: {src}')
                print(f'        quote:  "{quote}"')

    print()
    print('NO DATABASE WRITES PERFORMED.')
    print('Review the confirmed/denies findings and bulk-update tax_nexus.verified manually.')
    print('"silent" and "fetch_failed" need manual verification — likely a JS-rendered '
          'policy page or an anti-bot wall.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
