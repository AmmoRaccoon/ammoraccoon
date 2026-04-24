"""Shared helpers for AmmoRaccoon scrapers.

CALIBERS is the canonical list of normalized caliber slugs we collect.
Each scraper defines a CALIBER_PATHS dict mapping these slugs to its
own per-caliber URL fragment.

normalize_caliber() detects the caliber from a product title so a
single retailer visit that crosses categories (e.g. rifle and pistol
shelves) can still tag listings correctly.
"""

from datetime import datetime, timezone
import re

CALIBERS = {
    '9mm': '9mm Luger',
    '223-556': '.223 / 5.56 NATO',
    '22lr': '.22 LR',
    '380acp': '.380 ACP',
    '40sw': '.40 S&W',
    '308win': '.308 Winchester',
    '762x39': '7.62x39',
    '300blk': '.300 AAC Blackout',
    '38spl': '.38 Special',
    '357mag': '.357 Magnum',
}


def normalize_caliber(text):
    """Detect a caliber from product text. Returns (display, normalized) or (None, None)."""
    if not text:
        return (None, None)
    t = text.lower()
    if '5.56' in t or '5.56x45' in t or '5.56nato' in t or '5.56 nato' in t \
            or re.search(r'\b\.?223\b', t) or '223 rem' in t or '.223 rem' in t:
        return ('.223 / 5.56 NATO', '223-556')
    if re.search(r'\b22\s*lr\b', t) or '22 long rifle' in t or '.22lr' in t \
            or re.search(r'\b\.22\s*lr\b', t):
        return ('.22 LR', '22lr')
    if '380 acp' in t or '.380 acp' in t or '380auto' in t or '380 auto' in t \
            or re.search(r'\b\.?380\b', t):
        return ('.380 ACP', '380acp')
    if '40 s&w' in t or '40s&w' in t or '.40 s&w' in t or '.40sw' in t \
            or '40 smith' in t or re.search(r'\b\.?40\s*sw\b', t):
        return ('.40 S&W', '40sw')
    if '308 win' in t or '.308 win' in t or '7.62x51' in t or '7.62 x 51' in t \
            or re.search(r'\b\.?308\b', t):
        return ('.308 Winchester', '308win')
    if '7.62x39' in t or '7.62 x 39' in t or '762x39' in t:
        return ('7.62x39', '762x39')
    if '300 blackout' in t or '.300 blackout' in t or '300 blk' in t \
            or '.300 blk' in t or '300 aac' in t or '.300 aac' in t:
        return ('.300 AAC Blackout', '300blk')
    if '38 special' in t or '.38 special' in t or '38 spl' in t or '.38 spl' in t \
            or '38special' in t:
        return ('.38 Special', '38spl')
    if '357 mag' in t or '.357 mag' in t or '357 magnum' in t or '.357 magnum' in t \
            or '357mag' in t:
        return ('.357 Magnum', '357mag')
    if '9mm' in t or '9 mm' in t or '9x19' in t or '9 x 19' in t or '9 luger' in t \
            or '9mm luger' in t:
        return ('9mm Luger', '9mm')
    return (None, None)


_LIMIT_PATTERNS = [
    re.compile(r'limit\s*(?:of\s*)?(\d+)', re.IGNORECASE),
    re.compile(r'max(?:imum)?\s*(?:qty|quantity)\s*[:=]?\s*(\d+)', re.IGNORECASE),
    re.compile(r'(\d+)\s*per\s*(?:customer|order|household)', re.IGNORECASE),
    re.compile(r'qty\s*limit\s*[:=]?\s*(\d+)', re.IGNORECASE),
]


def parse_purchase_limit(text):
    """Return an int purchase limit if found in the text, else None.

    Matches common retailer copy like "Limit 2", "Max qty: 5",
    "5 per customer", "Qty Limit: 3".
    """
    if not text:
        return None
    for pat in _LIMIT_PATTERNS:
        m = pat.search(text)
        if m:
            try:
                n = int(m.group(1))
                if 1 <= n <= 999:
                    return n
            except ValueError:
                pass
    return None


# Canonical brand names. Multi-word entries must come before any
# single-word prefix/suffix to let the longest match win ("American
# Eagle" before "Federal" so Federal American Eagle titles don't
# collapse to the base brand on the first pass).
_BRAND_ALIASES = [
    # (match_lowercase, canonical_display)
    ('federal american eagle', 'Federal'),
    ('american eagle', 'Federal'),
    ('federal champion', 'Federal'),
    ('federal personal defense', 'Federal'),
    ('federal premium', 'Federal'),
    ('federal hst', 'Federal'),
    ('federal', 'Federal'),
    ('winchester usa forged', 'Winchester'),
    ('winchester supreme elite', 'Winchester'),
    ('winchester white box', 'Winchester'),
    ('winchester', 'Winchester'),
    ('remington golden saber', 'Remington'),
    ('remington htp', 'Remington'),
    ('remington umc', 'Remington'),
    ('remington', 'Remington'),
    ('hornady critical duty', 'Hornady'),
    ('hornady critical defense', 'Hornady'),
    ('hornady', 'Hornady'),
    ('cci blazer', 'CCI'),
    ('cci', 'CCI'),
    ('speer gold dot', 'Speer'),
    ('speer lawman', 'Speer'),
    ('speer', 'Speer'),
    ('blazer brass', 'Blazer'),
    ('blazer', 'Blazer'),
    ('magtech', 'Magtech'),
    ('pmc bronze', 'PMC'),
    ('pmc', 'PMC'),
    ('fiocchi', 'Fiocchi'),
    ('sellier and bellot', 'Sellier & Bellot'),
    ('sellier & bellot', 'Sellier & Bellot'),
    ('seller & bellot', 'Sellier & Bellot'),
    ('s&b', 'Sellier & Bellot'),
    ('tulammo', 'Tula'),
    ('tula', 'Tula'),
    ('wolf', 'Wolf'),
    ('prvi partizan (ppu)', 'Prvi Partizan'),
    ('prvi partizan', 'Prvi Partizan'),
    ('ppu ', 'Prvi Partizan'),
    ('norma', 'Norma'),
    ('lapua', 'Lapua'),
    ('black hills', 'Black Hills'),
    ('underwood', 'Underwood'),
    ('sig sauer', 'Sig Sauer'),
    ('liberty ammunition', 'Liberty'),
    ('liberty', 'Liberty'),
    ('maxxtech', 'Maxxtech'),
    ('igman', 'Igman'),
    ('armscor', 'Armscor'),
    ('aguila', 'Aguila'),
    ('browning', 'Browning'),
    ('barnes', 'Barnes'),
    ('sierra', 'Sierra'),
    ('atomic', 'Atomic'),
    ('sterling steel', 'Sterling'),
    ('sterling', 'Sterling'),
    ('belom', 'Belom'),
    ('bvac', 'BVAC'),
    ('veteran', 'Veteran'),
    ('hyperion', 'Hyperion'),
    ('staccato', 'Staccato'),
    ('corbon', 'Corbon'),
    ('precision one', 'Precision One'),
    ('freedom munitions', 'Freedom Munitions'),
    ('turan', 'Turan'),
    ('stv', 'STV'),
    ('silver bear', 'Silver Bear'),
    ('brown bear', 'Brown Bear'),
    ('barnaul', 'Barnaul'),
    ('red army', 'Red Army'),
    ('new republic', 'New Republic'),
    ('paraklese', 'Paraklese'),
    ('excalibur', 'Excalibur'),
    ('colt', 'Colt'),
    ('cbc', 'Magtech'),  # CBC is Magtech's international brand.
    ('geco', 'Geco'),
    ('monarch', 'Monarch'),
    ('sgammo', 'SGAmmo'),
]


def parse_brand(text):
    """Return a canonical manufacturer name from product text, or None.

    Matches the longest alias first so "Federal American Eagle"
    resolves before the bare "Federal" prefix.
    """
    if not text:
        return None
    t = text.lower()
    # Sort by descending pattern length for every call so new aliases
    # inserted anywhere in the list still yield longest-match behavior.
    for needle, canonical in sorted(_BRAND_ALIASES, key=lambda kv: -len(kv[0])):
        if needle in t:
            return canonical
    return None


PPR_FLOOR = 0.01   # Below this, something is off by 100x the other way.
PPR_CEILING = 5.00 # $5/rd is premium-defensive territory; above it is almost
                   # certainly a unit-conversion bug. Belt-and-suspenders
                   # guard against the April 22 cents-to-dollars regression.


def sanity_check_ppr(ppr, price, rounds, context=''):
    """Return True if a computed price_per_round looks physically plausible.

    Falsifies when the scraper's arithmetic is obviously wrong — stops a
    misparsed row from leaking into the DB regardless of which scraper
    produced it. Scrapers should call this after computing ppr and
    `continue` on False.
    """
    if ppr is None:
        return False
    try:
        p = float(ppr)
    except (TypeError, ValueError):
        return False
    if p < PPR_FLOOR or p > PPR_CEILING:
        print(
            f"  [sanity] ppr ${p:.4f} outside [{PPR_FLOOR}, {PPR_CEILING}] "
            f"(price=${price}, rounds={rounds}) {context}"
        )
        return False
    return True


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def with_stock_fields(listing, in_stock, now=None):
    """Add in_stock / stock_level / last_seen_in_stock to a listing dict.

    last_seen_in_stock is only set when in_stock is True so that
    out-of-stock cycles don't overwrite the previous good timestamp.
    """
    listing['in_stock'] = bool(in_stock)
    listing['stock_level'] = 'In Stock' if in_stock else 'Out of Stock'
    if in_stock:
        listing['last_seen_in_stock'] = now or now_iso()
    return listing
