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
