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
from urllib.parse import urlparse

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
    # Word-boundary regex on `\b9mm\b` so a title containing
    # "5.45x39mm" (Russian 5.45) doesn't false-match the bare "9mm"
    # substring inside "39mm". Sportsman's Guide surfaced this bug
    # 2026-04-26 — Hornady Black 5.45x39mm got bucketed as 9mm.
    if re.search(r'\b9mm\b', t) or '9 mm' in t or '9x19' in t \
            or '9 x 19' in t or '9 luger' in t:
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
    # Note: an earlier ('cci blazer', 'CCI') entry was removed
    # 2026-05-10. It was load-bearing-by-misunderstanding — the longer
    # 'blazer brass' (12 chars) already wins longest-match against
    # 'cci blazer' (10) for every Blazer Brass title, so the entry
    # only fired for the rare bare "CCI Blazer" stub without "brass",
    # and that's what migration 017 reclassifies anyway. Customers
    # shop the Blazer line as "Blazer", not "CCI"; canonical now
    # produces 'Blazer' for any title carrying the 'blazer' token.
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
    # Long-tail brands surfaced by the null-manufacturer audit.
    ('nosler asp', 'Nosler'),
    ('nosler', 'Nosler'),
    ('g2 research', 'G2 Research'),
    ('g2 telos', 'G2 Research'),
    ('winusa', 'Winchester'),
    ('american quality ammunition', 'American Quality'),
    ('us cartridge', 'US Cartridge'),
    ('sinterfire', 'Sinterfire'),
    ('patriot sports', 'Patriot Sports'),
    ('rangemaster', 'Prvi Partizan'),
    ('privi', 'Prvi Partizan'),  # spelling variant in slugs
    ('seller bellot', 'Sellier & Bellot'),  # frequent typo
    ('sellier bellot', 'Sellier & Bellot'),  # hyphen-stripped form
    ('v crown', 'Sig Sauer'),  # V-Crown is the Sig Sauer JHP line
    ('sig sauer ep', 'Sig Sauer'),
    ('sig sauer match elite', 'Sig Sauer'),
    ('elite v crown', 'Sig Sauer'),
    ('xie9mm', 'Sig Sauer'),  # SKU prefix used in some URLs
    ('xi51', 'Sig Sauer'),  # SKU prefix
    # Tier 2 brand audit (2026-05-09). 21 high-confidence brands
    # surfaced from the manufacturer='Unknown' audit across all
    # retailers — each appeared 10+ times as a leading-token in
    # Unknown listings' product URLs. Trailing-space aliases on
    # short bare tokens (`hsm `, `fsm `, `dbltap `, `rws `, `wyoming `)
    # require a token boundary so substring matches inside unrelated
    # words can't false-positive. Frontier Cartridge maps to Hornady
    # because Frontier IS Hornady's economy sub-brand (mirrors the
    # American-Eagle→Federal pattern). `cor bon` is a normalization
    # variant of the existing 'corbon' alias — slug separators get
    # turned into spaces before matching, so hyphenated "cor-bon"
    # arrives here as "cor bon".
    ('ammo incorporated', 'Ammo Inc'),
    ('ammo inc', 'Ammo Inc'),
    ('hsm ammunition', 'HSM'),
    ('hsm ', 'HSM'),
    ('buffalo bore ammunition', 'Buffalo Bore'),
    ('buffalo bore ammo', 'Buffalo Bore'),
    ('buffalo bore', 'Buffalo Bore'),
    ('fort scott munitions', 'Fort Scott Munitions'),
    ('fort scott', 'Fort Scott Munitions'),
    ('fsm ', 'Fort Scott Munitions'),
    ('doubletap ammunition', 'DoubleTap'),
    ('doubletap', 'DoubleTap'),
    ('double tap', 'DoubleTap'),
    ('dbltap ', 'DoubleTap'),
    ('frontier cartridge', 'Hornady'),  # Hornady's economy sub-brand
    ('frontier ammo', 'Hornady'),
    ('grizzly cartridge', 'Grizzly Cartridge'),
    ('grizzly ammo', 'Grizzly Cartridge'),
    ('grizzly', 'Grizzly Cartridge'),
    ('eley', 'Eley'),
    ('cor bon', 'Corbon'),  # Slug-normalized form of 'cor-bon' / 'corbon'
    ('novx', 'NovX'),
    ('lehigh defense', 'Lehigh Defense'),
    ('lehigh', 'Lehigh Defense'),
    ('berger bullets', 'Berger'),
    ('berger', 'Berger'),
    ('wilson combat', 'Wilson Combat'),
    ('glaser silver', 'Glaser'),
    ('glaser blue', 'Glaser'),
    ('glaser', 'Glaser'),
    ('supernova', 'Supernova'),
    ('wyoming cartridge company', 'Wyoming Cartridge'),
    ('wyoming cartridge co', 'Wyoming Cartridge'),
    ('wyoming cartridge', 'Wyoming Cartridge'),
    ('wyoming ', 'Wyoming Cartridge'),
    ('atlanta arms', 'Atlanta Arms'),
    ('piney mountain', 'Piney Mountain'),
    ('maxim defense', 'Maxim Defense'),
    ('global ordnance', 'Global Ordnance'),
    ('rws ', 'RWS'),
    ('century arms', 'Century Arms'),
]


# Caliber names that contain a brand token in their formal designation
# (".223 Remington", ".308 Winchester", ".45 Colt", ".357 SIG Sauer", etc.).
# Stripped from the input before brand matching so the caliber's brand
# name doesn't outweigh the actual manufacturer prefix at the start of
# the title via the longest-match rule. Without this, "Hornady Frontier
# .223 Remington 55gr FMJ" parses as Remington (9 chars beats hornady's
# 7) instead of Hornady. Audit 2026-05-07 found 109 listings affected
# across .223 Rem, .308 Win, .357 SIG, .44 Rem Mag, .300 Win Mag, and
# .22 WMR cartridges.
_CAL_BRAND_NOISE_RE = re.compile(
    r'\b\d+(?:\.\d+)?[\s.-]+'
    r'(?:'
        r'sig\s+sauer'   # ".357 SIG Sauer" — must come before bare "sig"
        r'|long\s+colt'  # ".45 Long Colt"
        r'|remington|winchester|colt|sig|rem|win'
    r')'
    r'(?:\s+(?:magnum|mag|rimfire|rim\s*fire))*',
    re.IGNORECASE,
)


def parse_brand(text):
    """Return a canonical manufacturer name from product text, or None.

    Matches the longest alias first so "Federal American Eagle"
    resolves before the bare "Federal" prefix. URL-style and slug-style
    separators are normalized to spaces so aliases written with spaces
    (e.g. "sellier and bellot", "prvi partizan") match URL slugs
    (e.g. "sellier-bellot", "prvi-partizan") too.

    Caliber names that embed a brand token (".223 Remington",
    ".308 Winchester", ".45 Colt", ".357 SIG") are stripped before the
    alias scan — otherwise the bare brand inside the caliber name
    out-lengths the actual manufacturer prefix and wins the longest-match.
    """
    if not text:
        return None
    t = text.lower().replace('-', ' ').replace('_', ' ').replace('/', ' ')
    t = _CAL_BRAND_NOISE_RE.sub(' ', t)
    # Sort by descending pattern length for every call so new aliases
    # inserted anywhere in the list still yield longest-match behavior.
    for needle, canonical in sorted(_BRAND_ALIASES, key=lambda kv: -len(kv[0])):
        if needle in t:
            return canonical
    return None


def parse_brand_or_unknown(text):
    """Same as parse_brand but returns 'Unknown' instead of None.

    Use this when assigning the manufacturer column on a listing —
    null breaks frontend filtering, while 'Unknown' is a real first-
    class brand bucket users can choose to include or exclude.
    """
    return parse_brand(text) or 'Unknown'


# Slug-start-only brand abbreviations. Keyed by exact lowercase token —
# matched ONLY against the leading [a-z]+ run of a product URL's last
# path segment (i.e. the SKU-style slug). Cannot live in _BRAND_ALIASES
# above because that table uses unanchored substring matching: a 3-4
# letter abbreviation like 'win' or 'fed' would false-positive against
# slugs containing 'twin', 'winston', 'federalist', 'winnow', etc., or
# random page chrome that happened to embed the trigram. Anchoring at
# slug-start scopes the match to retailer SKU prefixes (Gunbuyer's
# 'WIN X193150…', Firearms Depot's 'cent-arms-…', Shadowsmith's
# 'rem-22lr-…') where the abbreviation is unambiguous.
#
# Sourced from the 2026-05-10 Unknown-rows audit. Resolves ~115 rows
# concentrated in three retailers (Gunbuyer for win/fed/fio/spr,
# Shadowsmith for rem, Firearms Depot for cent).
_BRAND_SLUG_PREFIX_ALIASES = {
    'win':  'Winchester',
    'fed':  'Federal',
    'fio':  'Fiocchi',
    'rem':  'Remington',
    'cent': 'Century Arms',
    'spr':  'Speer',
}

_LEADING_TOKEN_RE = re.compile(r'^([a-z]+)')


def _slug_prefix_brand(url):
    """Look up the leading [a-z]+ run of a URL's last path segment in
    _BRAND_SLUG_PREFIX_ALIASES. Returns None if no URL, no leading
    alpha token, or no matching prefix."""
    if not url:
        return None
    last = urlparse(url).path.rstrip('/').rsplit('/', 1)[-1]
    last = last.split('?')[0].split('#')[0].lower()
    m = _LEADING_TOKEN_RE.match(last)
    if not m:
        return None
    return _BRAND_SLUG_PREFIX_ALIASES.get(m.group(1))


def parse_brand_with_url(title, url):
    """parse_brand with a slug-start fallback for SKU-prefix abbreviations.

    Resolution order:
      1. parse_brand(title) — the title-text alias scan as it has always
         worked. Returns immediately if it resolves.
      2. _slug_prefix_brand(url) — slug-start-only lookup against
         _BRAND_SLUG_PREFIX_ALIASES. Catches retailer-specific 3-4
         letter SKU prefixes (Gunbuyer 'win-…', Shadowsmith 'rem-…',
         Firearms Depot 'cent-…') that can't safely live in the main
         alias table because their bare-substring form would collide.
         Runs BEFORE step 3 because slug-prefix is SKU-anchored and
         more specific than unanchored URL substring; e.g. Gunbuyer's
         `spr-lawman-…cci53651bx-b.html` is a Speer Lawman product
         that step 3 would mis-tag as CCI on the embedded SKU stem.
      3. parse_brand(url) — same alias scan against the URL. Catches
         cases where the title is empty or stripped to a SKU code while
         the URL slug carries a full brand name and no slug-prefix
         alias resolved.

    Use this in scrapers that have product_url in scope at brand-parse
    time. Scrapers without URL context should keep using parse_brand;
    they lose nothing — the slug-start aliases only kick in when the
    title-based pass returns None.
    """
    return (parse_brand(title)
            or _slug_prefix_brand(url)
            or parse_brand(url))


# Canonical bullet types accepted by the listings table. Any value
# parse_bullet_type emits MUST appear in this set so frontend filters
# and per-retailer audits stay in sync. Five values (LRN/JSP/Frangible/
# Blank/WC) were promoted into the canonical set 2026-05-02 after the
# bullet-type quality audit found them already in production data — see
# scripts/audit_bullet_type.py for the audit and scripts/backfill_bullet_type.py
# for the rollout.
BULLET_TYPES = frozenset({
    'FMJ',       # Full Metal Jacket
    'TMJ',       # Total Metal Jacket
    'JHP',       # Jacketed Hollow Point — incl. brand lines FTX/FLEXLOCK/HONEYBADGER/BJHP
    'HP',        # Hollow Point and the polymer-tip / BTHP / V-MAX family;
                 # includes spire-point/spitzer (formal SP family — see
                 # 2026-05-07 expansion); brand lines SST/MatchKing/TSX/VLD/HST
    'OTM',       # Open Tip Match
    'SP',        # Soft Point — incl. "jacketed soft point", Power Point,
                 # Spire Point, Spitzer (pointed soft-tip rifle bullets)
    'JSP',       # Jacketed Soft Point (legacy retained for in-DB rows)
    'FP',        # Flat Point — incl. RNFP
    'FN',        # Flat Nose — distinct from FP (FN is the bullet shape;
                 # FP is the older nomenclature commonly applied to lead
                 # cast bullets). Promoted 2026-05-07 — Sierra Pro-Hunter
                 # FN, Lehigh Dangerous Game FN dominate.
    'LRN',       # Lead Round Nose
    'RN',        # Round Nose — non-lead variant (jacketed round nose,
                 # plated round nose). Promoted 2026-05-07.
    'WC',        # Wadcutter — incl. Semi-Wadcutter (SWC/LSWC) and
                 # Hollow Base Wadcutter (HBWC) folded in 2026-05-07.
    'Solid',     # Monolithic copper / brass solid (non-expanding) —
                 # Barnes Banded Solid, Hornady DGS, Lehigh Solid Copper
                 # Fluted, etc. Distinct from solid-copper EXPANDING
                 # bullets (TSX/CX) which are still classed as HP because
                 # their hollow cavity does the work.
    'Frangible', # Frangible
    'Blank',     # Blank cartridge
    'Incendiary', # Incendiary specialty round
})


# (regex, canonical) pairs for parse_bullet_type. The order is
# significant — multi-word phrases come first so "jacketed soft point"
# resolves before bare JSP/SP tokens; family aliases (V-MAX/A-MAX/ELD)
# come next; bare 2-letter codes (HP/SP/FP/WC) come last so they don't
# pre-empt longer matches. Every pattern uses \b boundaries and runs
# against text normalized via _normalize_for_bullet_match() — which
# lowercases the input and turns slug separators (-_./()) into spaces
# so a single match list serves both human titles and URL slugs.
_BULLET_PATTERNS = [
    # Multi-word descriptive phrases (longest first within group)
    (re.compile(r'\bjacketed\s+hollow\s+point\b'), 'JHP'),
    (re.compile(r'\bsemi[\s-]?jacketed\s+hollow\s+point\b'), 'JHP'),
    (re.compile(r'\bjacketed\s+soft\s+point\b'), 'SP'),
    (re.compile(r'\bsemi[\s-]?jacketed\s+soft\s+point\b'), 'SP'),
    (re.compile(r'\bfull\s+metal\s+jacket\b'), 'FMJ'),
    (re.compile(r'\btotal\s+metal\s+jacket\b'), 'TMJ'),
    (re.compile(r'\bopen\s+tip\s+match\b'), 'OTM'),
    (re.compile(r'\blead\s+round\s+nose\b'), 'LRN'),
    (re.compile(r'\broundn?\s*nose\s+flat\s+point\b'), 'FP'),
    (re.compile(r'\bsoft\s+point\b'), 'SP'),
    (re.compile(r'\bhollow\s+point\b'), 'JHP'),
    (re.compile(r'\bflat\s+point\b'), 'FP'),
    (re.compile(r'\bpower\s*point\b'), 'SP'),  # Winchester Power Point line
    (re.compile(r'\bpolymer\s+tip(?:ped)?\b'), 'HP'),
    (re.compile(r'\bballistic\s+tip\b'), 'HP'),
    (re.compile(r'\bwadcutter\b'), 'WC'),
    (re.compile(r'\bfrangible\b'), 'Frangible'),
    (re.compile(r'\bblank\b'), 'Blank'),

    # Multi-word formal shapes added 2026-05-07 — these were the largest
    # gap in coverage (NULL bullet_type at 19.9% site-wide before the fix;
    # spire-point alone accounted for 152 NULLs across listings + components).
    # Solid-family patterns are intentionally narrow ("banded solid",
    # "solid dgs", "monolithic solid", "solid copper fluted") so the bare
    # word "solid" — which appears in titles like "Solid Hollow Point CX"
    # (Hornady CX, an EXPANDING monolithic) — doesn't out-priority the
    # existing "hollow point" → JHP match. CX-family stays JHP per audit.
    (re.compile(r'\bbanded\s+solid\b'), 'Solid'),
    (re.compile(r'\bmonolithic\s+solid\b'), 'Solid'),
    (re.compile(r'\bsolid\s+dgs\b'), 'Solid'),       # Hornady Dangerous Game Solid
    (re.compile(r'\bsolid\s+copper\s+fluted\b'), 'Solid'),  # Lehigh Xtreme line
    (re.compile(r'\bsemi[\s-]?wad\s*cutter\b'), 'WC'),
    (re.compile(r'\bhollow\s+base\s+wadcutter\b'), 'WC'),
    (re.compile(r'\bspire\s+point\b'), 'SP'),
    (re.compile(r'\bspitzer\b'), 'SP'),              # synonym for spire point
    (re.compile(r'\bround\s+nose\b'), 'RN'),
    (re.compile(r'\bflat\s+nose\b'), 'FN'),

    # Family aliases — Hornady polymer-tip line, Fenix house brand.
    # All map to HP because the open polymer cavity makes them
    # mechanically hollow points; keeps filter buckets coherent.
    (re.compile(r'\bv[\s-]?max\b'), 'HP'),
    (re.compile(r'\ba[\s-]?max\b'), 'HP'),
    (re.compile(r'\beld[\s-]?x\b'), 'HP'),
    (re.compile(r'\beld[\s-]?m(?:atch)?\b'), 'HP'),  # "ELD M" or "ELD Match"
    (re.compile(r'\beldx\b'), 'HP'),
    (re.compile(r'\bxtp\b'), 'JHP'),    # Hornady XTP is a JHP
    (re.compile(r'\bfxp\b'), 'HP'),     # Fenix FXP house brand HPs
    (re.compile(r'\bcphp\b'), 'HP'),    # Copper Plated HP (CCI Velocitor etc)
    (re.compile(r'\bcpfp\b'), 'FP'),    # Copper Plated Flat Point
    (re.compile(r'\bftx\b'), 'JHP'),    # Hornady FTX (Flex Tip eXpanding) — JHP base
    (re.compile(r'\bflexlock\b'), 'JHP'),    # Federal HST Flexlock — bonded JHP
    (re.compile(r'\bhoneybadger\b'), 'JHP'),  # Black Hills HoneyBadger — solid copper, classed as JHP per legacy convention
    (re.compile(r'\bbjhp\b'), 'JHP'),   # Bonded JHP
    (re.compile(r'\bincendiary\b'), 'Incendiary'),

    # Definitive brand-line abbreviations added 2026-05-07. Each maps to
    # exactly one bullet type across that line — context-dependent lines
    # (InterLock, GameKing, Pro-Hunter) are intentionally NOT included
    # because they span multiple shapes within the same product family.
    (re.compile(r'\bgold\s*dot\b'), 'JHP'),  # Speer Gold Dot — bonded JHP
    (re.compile(r'\bgdhp\b'), 'JHP'),        # Gold Dot HP abbreviation
    (re.compile(r'\baccu[\s-]?tip\b'), 'HP'),  # Remington AccuTip — polymer tip
    (re.compile(r'\bsst\b'), 'HP'),          # Hornady SST — polymer-tip Super Shock Tip
    (re.compile(r'\bmatchking\b'), 'HP'),    # Sierra MatchKing — match BTHP family
    (re.compile(r'\bvld\b'), 'HP'),          # Berger VLD — match HP
    (re.compile(r'\bttsx\b'), 'HP'),         # Barnes TTSX — must match BEFORE \btsx\b
    (re.compile(r'\btsx\b'), 'HP'),          # Barnes TSX — solid-copper expanding HP
    (re.compile(r'\blrx\b'), 'HP'),          # Barnes LRX — long-range expanding
    (re.compile(r'\bhst\b'), 'JHP'),         # Federal HST — bonded self-defense JHP

    # Compound abbreviations (longer/more-specific first within group)
    (re.compile(r'\bbthp\b|\bhpbt\b'), 'HP'),
    (re.compile(r'\bfmjbt\b|\bfmjfn\b|\bfmjfb\b'), 'FMJ'),
    (re.compile(r'\bsjsp\b'), 'SP'),
    (re.compile(r'\bsjhp\b'), 'JHP'),
    (re.compile(r'\brnfp\b'), 'FP'),
    # Wadcutter-family abbreviations folded to WC 2026-05-07.
    (re.compile(r'\blswc\b'), 'WC'),    # Lead Semi-Wadcutter
    (re.compile(r'\bswc\b'), 'WC'),     # Semi-Wadcutter
    (re.compile(r'\bhbwc\b'), 'WC'),    # Hollow Base Wadcutter

    # Bare 3-letter codes — safe enough that word-boundary catches
    # most common false positives (e.g. "Speer" doesn't trip \bjhp\b).
    (re.compile(r'\blrn\b'), 'LRN'),
    (re.compile(r'\bjhp\b'), 'JHP'),
    (re.compile(r'\bjsp\b'), 'SP'),  # treat JSP-the-token as SP per audit decision
    (re.compile(r'\bfmj\b'), 'FMJ'),
    (re.compile(r'\btmj\b'), 'TMJ'),
    (re.compile(r'\botm\b'), 'OTM'),

    # Bare 2-letter codes — word-bounded to avoid false positives like
    # "Speer"/"Sport"/"Spire" → SP, "FPS" → FP, "WCC" → WC. Last so
    # they can't shadow a longer match earlier in the list.
    (re.compile(r'\bhp\b'), 'HP'),
    (re.compile(r'\bsp\b'), 'SP'),
    (re.compile(r'\bfp\b'), 'FP'),
    (re.compile(r'\bwc\b'), 'WC'),
]


def _normalize_for_bullet_match(text):
    """Lowercase and turn slug separators into spaces so the bullet-type
    pattern list matches uniformly against both human titles
    ("Jacketed Hollow Point") and URL slugs ("jacketed-hollow-point").
    Collapses runs of whitespace so `\\s+` patterns work predictably."""
    if not text:
        return ''
    s = text.lower()
    for ch in '-_/.()':
        s = s.replace(ch, ' ')
    return re.sub(r'\s+', ' ', s).strip()


def parse_bullet_type(text):
    """Return a canonical bullet type from product text, or None.

    Accepts both human-readable titles and URL slugs — the input is
    normalized so a single pattern list serves both. Patterns are
    word-bounded to avoid false positives like "Speer" → SP or
    "FPS" → FP.

    Return value is one of `BULLET_TYPES` or None when no canonical
    type is detected.
    """
    s = _normalize_for_bullet_match(text)
    if not s:
        return None
    for pat, bt in _BULLET_PATTERNS:
        if pat.search(s):
            return bt
    return None


def parse_bullet_type_with_url_fallback(title, product_url):
    """parse_bullet_type with URL-slug fallback when title parsing fails.

    Some retailers (Gunbuyer, Firearms Depot) abbreviate titles to SKU
    codes that omit the bullet-type token even when the URL slug
    includes it. Audit 2026-05-02 found ~150 in-stock NULLs across
    these two retailers that the slug exposes but the title does not.
    Use this variant in scrapers where titles are unreliable; titles
    win when both have a hit so brand-specific aliases keep precedence.
    """
    bt = parse_bullet_type(title)
    if bt is not None:
        return bt
    return parse_bullet_type(product_url)


PPR_ABSOLUTE_FLOOR = 0.01  # Catches the cents-as-dollars / 100x-off-the-other-way regression.
PPR_CEILING = 5.00         # $5/rd is premium-defensive territory; above it is almost
                           # certainly a unit-conversion bug. Belt-and-suspenders
                           # guard against the April 22 cents-to-dollars regression.
                           # TODO: when .50 BMG (real-world ~$6-10/rd), .338 Lapua,
                           # or similar premium-rifle calibers are added, raise the
                           # ceiling OR scope it per caliber_normalized — a blanket
                           # $5 ceiling will silently drop legitimate listings.

# Per-caliber lower bounds. The blanket $0.01 floor was too loose — a
# misparsed 9mm at 3.2¢/rd was clearly impossible (street floor for
# brass-case range 9mm hasn't been below ~22¢ in years) but still slipped
# through. Each value is the lowest-plausible per-round price for *new*
# brass-case range ammo in that caliber as of 2026. Set conservatively
# below the cheapest seen on the market so an actual sale still passes.
# Keys here are the loose names the project tracks publicly; the
# CALIBERS-key normalization below maps the canonical `caliber_normalized`
# slugs onto these.
CALIBER_PRICE_FLOORS = {
    '9mm':    0.15,
    '22lr':   0.04,
    '223':    0.20,
    '556':    0.20,
    '308':    0.40,
    '380':    0.18,
    '40sw':   0.20,
    '45acp':  0.25,
    '357mag': 0.25,
    '38spl':  0.20,
    '300blk': 0.35,
    '762x39': 0.15,
}
DEFAULT_FLOOR = 0.15

# Map the canonical CALIBERS keys (what scrapers actually emit as
# caliber_normalized) onto the loose floor keys above. Combined slugs
# like '223-556' fall back to the .223 floor since the chamber pressure
# and street price for both rounds are roughly identical.
_CALIBER_TO_FLOOR_KEY = {
    '9mm':     '9mm',
    '22lr':    '22lr',
    '223-556': '223',
    '380acp':  '380',
    '40sw':    '40sw',
    '38spl':   '38spl',
    '357mag':  '357mag',
    '308win':  '308',
    '762x39':  '762x39',
    '300blk':  '300blk',
}


def floor_for_caliber(caliber):
    """Return the per-round floor for a caliber identifier.

    Accepts either a canonical CALIBERS key ('9mm', '223-556', '380acp',
    …) or a loose name ('9mm', '223', '380'). Falls back to DEFAULT_FLOOR
    for unrecognized inputs so a new caliber doesn't silently disable
    the gate.
    """
    if not caliber:
        return DEFAULT_FLOOR
    key = _CALIBER_TO_FLOOR_KEY.get(caliber, caliber)
    return CALIBER_PRICE_FLOORS.get(key, DEFAULT_FLOOR)


# Per-caliber upper bounds. The blanket $5/rd ceiling let too much
# premium-defensive / boutique pricing through and produced misleading
# "deal" badges in the UI when a $4/rd Liberty Civil Defense round
# briefly out-PPR'd a $0.50/rd bulk SKU after a parse error. Tightened
# 2026-04-26 — values reflect the per-caliber ceiling above which a
# listing is far more likely to be a misparse than a real boutique
# SKU. Mirrors CALIBER_PRICE_FLOORS / floor_for_caliber.
CALIBER_PRICE_CEILINGS = {
    '9mm':    1.50,
    '22lr':   0.75,
    '223':    2.50,
    '556':    2.50,
    '308':    4.00,
    '380':    2.00,
    '40sw':   2.00,
    '357mag': 3.00,
    '38spl':  2.50,
    '300blk': 4.00,
    '762x39': 2.00,
}
DEFAULT_CEILING = 3.00


def ceiling_for_caliber(caliber):
    """Return the per-round ceiling for a caliber identifier.

    Mirrors floor_for_caliber. Falls back to DEFAULT_CEILING for
    unrecognized inputs so a new caliber doesn't silently disable
    the upper gate.
    """
    if not caliber:
        return DEFAULT_CEILING
    key = _CALIBER_TO_FLOOR_KEY.get(caliber, caliber)
    return CALIBER_PRICE_CEILINGS.get(key, DEFAULT_CEILING)


def sanity_check_ppr(ppr, price, rounds, context='', caliber=None):
    """Return True if a computed price_per_round looks physically plausible.

    Falsifies when the scraper's arithmetic is obviously wrong — stops a
    misparsed row from leaking into the DB regardless of which scraper
    produced it. Scrapers should call this after computing ppr and
    `continue` on False, passing `caliber=caliber_norm` so the per-caliber
    floor (CALIBER_PRICE_FLOORS) applies. Without a caliber the function
    falls back to DEFAULT_FLOOR.
    """
    if ppr is None:
        return False
    try:
        p = float(ppr)
    except (TypeError, ValueError):
        return False
    if p < PPR_ABSOLUTE_FLOOR or p > PPR_CEILING:
        print(
            f"  [sanity] ppr ${p:.4f} outside [{PPR_ABSOLUTE_FLOOR}, {PPR_CEILING}] "
            f"(price=${price}, rounds={rounds}) {context}"
        )
        return False
    floor = floor_for_caliber(caliber)
    if p < floor:
        # Distinct prefix so the per-caliber rejections are easy to grep
        # out of the scrape log when tuning the floor map.
        print(
            f"  [floor] {caliber or 'default'}: ppr ${p:.4f} below floor ${floor:.2f} "
            f"(price=${price}, rounds={rounds}) {context}"
        )
        return False
    ceiling = ceiling_for_caliber(caliber)
    if p > ceiling:
        # Distinct prefix mirrors the floor case so per-caliber
        # ceiling rejections are easy to grep out of the scrape log.
        print(
            f"  [ceiling] {caliber or 'default'}: ppr ${p:.4f} above ceiling ${ceiling:.2f} "
            f"(price=${price}, rounds={rounds}) {context}"
        )
        return False
    return True


# Map common typographic glyphs to ASCII so the listings table stays
# clean across terminals/locales. Inlined in gorilla/velocity/shadowsmith;
# new scrapers should import this instead of redefining it.
TYPOGRAPHIC = str.maketrans({
    '–': '-', '—': '-',
    '‘': "'", '’': "'", '“': '"', '”': '"',
    '®': '', '™': '',
    '·': '*', '•': '*', '×': 'x',
})


def clean_title(text):
    """Translate typographic glyphs in a product title and strip whitespace."""
    if not text:
        return ''
    return text.translate(TYPOGRAPHIC).strip()


# Firearm-type classification for manufacturer_rebates rows. Mirrors
# the values accepted by the manufacturer_rebates_firearm_type_chk
# constraint added in migration 012: 'shotshell' | 'handgun' | 'rifle'
# | 'rimfire'. NULL is the explicit "ambiguous / unknown" state — the
# matcher (scripts/match_manufacturer_rebates_to_listings.py) gates
# conservatively when firearm_type IS NULL, so a NULL is always safer
# than a misclassification.
#
# Token lists per category — kept here (not in a regex) so future
# additions are diff-friendly. Tokens are matched as case-insensitive
# substrings against (title + ' ' + raw_terms) lowercased.
_FIREARM_TYPE_SHOTSHELL = (
    'shotshell', 'shot shell', 'shotgun',
    'turkey', 'waterfowl', 'upland', 'clays', 'sporting clays',
    'slug', 'birdshot', 'buckshot',
    '12 ga', '12ga', '12 gauge',
    '20 ga', '20ga', '20 gauge',
    '16 ga', '16ga', '16 gauge',
    '28 ga', '28ga', '28 gauge',
    '410 bore', '.410',
    'long beard',  # Winchester turkey sub-brand — strong shotshell signal
)
# Bare 'rifle' is intentionally NOT in this list — observed 2026-05-10
# in Winchester's rebate page boilerplate ("Winchester Ammunition
# Products: rifle, handgun, rimfire, shotshell …") which appears in
# raw_terms after the rebate-specific copy. The bare word lit up every
# Winchester rebate as cross-category and forced the classifier to
# NULL even on plainly-shotshell rebates. Specific rifle calibers /
# 'rifle ammunition' / 'centerfire rifle' / AR-15 / MSR all remain as
# strong unambiguous signals; a real rifle rebate will carry at least
# one of them.
_FIREARM_TYPE_RIFLE = (
    'rifle ammunition', 'centerfire rifle',
    '.223', '5.56', '.308', '7.62x', '7.62 x', '30-06', '30 06',
    '6.5 creedmoor', 'creedmoor', '.243', '.270', '.300 win mag',
    '.338', '.50 bmg',
    'ar-15', 'ar15', 'ar-10', 'precision rifle', 'msr',
)
_FIREARM_TYPE_HANDGUN = (
    'handgun', 'pistol',
    '9mm', '.45 acp', '45 acp', '45acp',
    '.40 s&w', '40 s&w', '.380', '380 acp',
    '.357 sig', '.357 mag', '38 special', '38spl', '.38 spl',
    '.44 mag', '.44 special', '.44 magnum',
    '10mm',
)
_FIREARM_TYPE_RIMFIRE = (
    'rimfire',
    '.22 lr', '22 lr', '22lr', '.22lr', '.22 long rifle', '22 long rifle',
    '.22 wmr', '22 wmr', '17 hmr', '17hmr', '.17 hmr', '.17hmr',
)
_FIREARM_TYPE_CATEGORIES = {
    'shotshell': _FIREARM_TYPE_SHOTSHELL,
    'rifle':     _FIREARM_TYPE_RIFLE,
    'handgun':   _FIREARM_TYPE_HANDGUN,
    'rimfire':   _FIREARM_TYPE_RIMFIRE,
}


def _firearm_type_hits(text):
    text = (text or '').lower()
    return {cat: sum(1 for t in toks if t in text)
            for cat, toks in _FIREARM_TYPE_CATEGORIES.items()}


def parse_firearm_type(title, raw_terms):
    """Classify a rebate as 'shotshell' | 'rifle' | 'handgun' | 'rimfire',
    or return None when the rebate covers multiple categories or has no
    discernible firearm-type signal.

    Tiered approach (chosen 2026-05-10 after a naive "any token in any
    field" classifier returned NULL on plainly-shotshell Winchester
    rebates whose raw_terms fields carried trailing site boilerplate
    mentioning every product category):

      1. Scan title alone. If exactly one category hits, return it.
         Most rebate titles are unambiguous ("Winchester 16 GA Ammunition
         Rebate", "Federal Pistol Rebate") and this short-circuits the
         common case.
      2. Otherwise scan title + raw_terms together and apply a dominance
         test: the top-scoring category must have at least 2x the hits
         of the runner-up AND at least 2 absolute hits. The 2x margin
         absorbs occasional cross-category boilerplate noise; the 2-hit
         floor prevents a single ambiguous word from carrying the call.
      3. If neither pass commits, return None — the matcher gates
         conservatively on NULL, so under-classifying is the safe
         default.

    Output domain is constrained by the manufacturer_rebates check
    constraint introduced in migration 012; values outside the four
    listed categories will fail insert.
    """
    title_hits = _firearm_type_hits(title)
    title_present = {c: n for c, n in title_hits.items() if n > 0}
    if len(title_present) == 1:
        return next(iter(title_present))

    combined = _firearm_type_hits((title or '') + ' ' + (raw_terms or ''))
    sorted_hits = sorted(combined.items(), key=lambda kv: -kv[1])
    top_cat, top_n = sorted_hits[0]
    second_n = sorted_hits[1][1] if len(sorted_hits) > 1 else 0
    if top_n >= 2 and top_n >= 2 * max(second_n, 1):
        return top_cat
    return None


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def mark_retailer_scraped(supabase, retailer_id):
    """Bump retailers.last_scraped_at to NOW() for the given retailer_id.

    Call from the END of a scraper's scrape()/main() — AFTER the upsert
    loop AND AFTER any storefront-drift guardrail (the
    EMPTY_FAIL_THRESHOLD pattern in 9 scrapers as of 2026-05-10) has
    cleared. A successful run that finds 0 in-stock listings (everything
    OOS, or a transient empty result the guardrail accepts) writes
    nothing to listings.last_updated; this column is the truth signal
    for "the scraper ran fine, just had nothing to upsert", which is
    the gap /status currently struggles with when MAX(listings.last_updated)
    is the only freshness signal.

    Drift-fail (sys.exit(1) inside the EMPTY_FAIL_THRESHOLD block) and
    unhandled-exception paths skip this naturally, because they exit
    or raise before control reaches the call. That's deliberate —
    drift detection is the explicit "do not silently succeed"
    guardrail and should NOT be hidden by a fresh-looking timestamp.

    Wired into 34 active-retailer listings scrapers as of 2026-05-10.
    Out of scope for non-listings scrapers (rebates, ballistics,
    components — no retailer concept) and inactive-retailer scrapers
    (academy, bereli, bulkmunitions).
    """
    supabase.table('retailers').update({
        'last_scraped_at': now_iso(),
    }).eq('id', retailer_id).execute()


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
