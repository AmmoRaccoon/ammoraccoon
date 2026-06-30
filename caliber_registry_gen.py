"""GENERATED FROM ammoraccoon/calibers.json - DO NOT EDIT.

Regenerate: node scripts/gen-calibers/index.mjs --write (run from ammoraccoon-web).
Registry sha256: 40d48b76ee3edb6db3f283d7b1710fa6f00325d376cad9da96e9ac0f911518fd

Phase A (2026-06-12): NOTHING imports this module yet. Every table below
is the registry-derived twin of a hand-maintained table in scraper_lib /
caliber_audit / the ballistics scrapers / the rebates matcher, kept
verbatim-identical (quirks included: loose floor keys, the 556 twin row).
Parity proof: scripts/check_caliber_registry.py.
"""
import re as _re

REGISTRY_SHA256 = '40d48b76ee3edb6db3f283d7b1710fa6f00325d376cad9da96e9ac0f911518fd'

# Twin of scraper_lib.CALIBERS
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
    '45acp': '.45 ACP',
    '10mm': '10mm Auto',
    '30-06': '.30-06 Springfield',
    '270win': '.270 Winchester',
}

# Twin of scraper_lib.normalize_caliber — branch order + specs verbatim.
NORMALIZE_PRIORITY = ['223-556', '22lr', '380acp', '40sw', '308win', '762x39', '300blk', '38spl', '357mag', '9mm', '45acp', '10mm', '30-06', '270win']
NORMALIZE_SPECS = {
    '223-556': [('sub', '5.56'), ('sub', '5.56x45'), ('sub', '5.56nato'), ('sub', '5.56 nato'), ('re', r'\b\.?223\b'), ('sub', '223 rem'), ('sub', '.223 rem'), ('re', r'223\s*rem'), ('re', r'\b556\b')],
    '22lr': [('re', r'\b22\s*lr\b'), ('sub', '22 long rifle'), ('sub', '.22lr'), ('re', r'\b\.22\s*lr\b')],
    '380acp': [('sub', '380 acp'), ('sub', '.380 acp'), ('sub', '380auto'), ('sub', '380 auto'), ('re', r'\b\.?380\s*acp\b'), ('re', r'\b\.?380\b')],
    '40sw': [('sub', '40 s&w'), ('sub', '40s&w'), ('sub', '.40 s&w'), ('sub', '.40sw'), ('sub', '40 smith'), ('re', r'\b\.?40\s*sw\b')],
    '308win': [('sub', '308 win'), ('sub', '.308 win'), ('sub', '7.62x51'), ('sub', '7.62 x 51'), ('re', r'\b\.?308\s*win\b'), ('re', r'\b7\.?62\s*nato\b'), ('re', r'\b\.?308\b')],
    '762x39': [('sub', '7.62x39'), ('sub', '7.62 x 39'), ('sub', '762x39')],
    '300blk': [('sub', '300 blackout'), ('sub', '.300 blackout'), ('sub', '300 blk'), ('sub', '.300 blk'), ('sub', '300 aac'), ('sub', '.300 aac'), ('re', r'300\s*(?:aac|blk|blackout)'), ('re', r'\.300\s*(?:aac|blk|blackout)')],
    '38spl': [('sub', '38 special'), ('sub', '.38 special'), ('sub', '38 spl'), ('sub', '.38 spl'), ('sub', '38special'), ('re', r'\b\.?38\s*spl\b')],
    '357mag': [('sub', '357 mag'), ('sub', '.357 mag'), ('sub', '357 magnum'), ('sub', '.357 magnum'), ('sub', '357mag')],
    '9mm': [('re', r'\b9mm\b'), ('sub', '9 mm'), ('sub', '9x19'), ('sub', '9 x 19'), ('sub', '9 luger')],
    '45acp': [('sub', '45 acp'), ('sub', '.45 acp'), ('re', r'\b\.?45\s*auto\b(?!\s*rim)'), ('re', r'\b\.?45\s*acp\b')],
    '10mm': [('sub', '10mm auto'), ('sub', '10 mm auto'), ('re', r'\b10\s*mm\b')],
    '30-06': [('sub', '30-06 springfield'), ('sub', '30-06 sprg'), ('re', r'\b\.?30[\s-]?06\b')],
    '270win': [('re', r'(?:\b270\s*win(?:chester)?\b(?!\s*short))|(?:\b\.270\b(?!\s*(?:wsm|win(?:chester)?\s+short)))')],
}
_NORMALIZE_COMPILED = {
    cal: [(kind, _re.compile(val) if kind == 're' else val) for kind, val in specs]
    for cal, specs in NORMALIZE_SPECS.items()
}


def normalize_caliber_gen(text):
    """Data-driven twin of scraper_lib.normalize_caliber.

    Returns (display, normalized) or (None, None). A branch in the
    original is `if A or B or C: return X` — equivalent to first-spec-
    match within the branch, so spec order inside a caliber is
    irrelevant to the verdict; branch (priority) order is load-bearing.
    """
    if not text:
        return (None, None)
    t = text.lower()
    for cal in NORMALIZE_PRIORITY:
        for kind, pat in _NORMALIZE_COMPILED[cal]:
            hit = (pat in t) if kind == 'sub' else bool(pat.search(t))
            if hit:
                return (CALIBERS[cal], cal)
    return (None, None)


# Twin of scraper_lib.CALIBER_PRICE_FLOORS (loose keys kept verbatim).
CALIBER_PRICE_FLOORS = {
    '9mm': 0.15,
    '223': 0.20,
    '556': 0.20,
    '22lr': 0.04,
    '380': 0.18,
    '40sw': 0.20,
    '308': 0.40,
    '762x39': 0.15,
    '300blk': 0.35,
    '38spl': 0.20,
    '357mag': 0.25,
    '45acp': 0.25,
    '10mm': 0.20,
    '30-06': 0.40,
    '270win': 0.40,
}
DEFAULT_FLOOR = 0.15

# Twin of scraper_lib._CALIBER_TO_FLOOR_KEY.
CALIBER_TO_FLOOR_KEY = {
    '9mm': '9mm',
    '223-556': '223',
    '22lr': '22lr',
    '380acp': '380',
    '40sw': '40sw',
    '308win': '308',
    '762x39': '762x39',
    '300blk': '300blk',
    '38spl': '38spl',
    '357mag': '357mag',
    '45acp': '45acp',
    '10mm': '10mm',
    '30-06': '30-06',
    '270win': '270win',
}

# Twin of scraper_lib.CALIBER_PRICE_CEILINGS.
CALIBER_PRICE_CEILINGS = {
    '9mm': 1.50,
    '223': 2.50,
    '556': 2.50,
    '22lr': 0.75,
    '380': 2.00,
    '40sw': 2.00,
    '308': 4.00,
    '762x39': 2.00,
    '300blk': 4.00,
    '38spl': 2.50,
    '357mag': 3.00,
    '45acp': 2.50,
    '10mm': 3.50,
    '30-06': 4.00,
    '270win': 4.00,
}
DEFAULT_CEILING = 3.00

# Twin of scripts/caliber_audit.py EXPECTED_RANGES.
AUDIT_EXPECTED_RANGES = {
    '9mm': (0.15, 0.80),
    '223-556': (0.25, 1.50),
    '22lr': (0.05, 0.30),
    '380acp': (0.20, 1.00),
    '40sw': (0.20, 0.90),
    '308win': (0.50, 3.00),
    '762x39': (0.20, 1.00),
    '300blk': (0.50, 2.50),
    '38spl': (0.25, 1.50),
    '357mag': (0.30, 1.50),
    '45acp': (0.25, 1.20),
    '10mm': (0.30, 2.50),
    '30-06': (0.50, 3.00),
    '270win': (0.50, 3.00),
}
AUDIT_DEFAULT_RANGE = (0.10, 5.00)

# UNION of the six ballistics scrapers' CALIBER_NORMALIZE maps (D2:
# single shared map, behavior proven by dry-run replay per source).
BALLISTICS_CALIBER_NORMALIZE = {
    '9mm luger': '9mm',
    '9mm': '9mm',
    '9mm luger +p': '9mm',
    '9mm nato': '9mm',
    '9x19mm nato': '9mm',
    '9x19': '9mm',
    '9x19mm': '9mm',
    '9 mm luger': '9mm',
    '223 rem': '223-556',
    '.223 rem': '223-556',
    '223 remington': '223-556',
    '.223 remington': '223-556',
    '5.56 nato': '223-556',
    '5.56x45 nato': '223-556',
    '5.56x45mm nato': '223-556',
    '5.56': '223-556',
    '5.56mm': '223-556',
    '22 lr': '22lr',
    '.22 lr': '22lr',
    '.22 long rifle': '22lr',
    '22 long rifle': '22lr',
    '380 auto': '380acp',
    '.380 auto': '380acp',
    '380 acp': '380acp',
    '.380 acp': '380acp',
    '380 automatic': '380acp',
    '40 s&w': '40sw',
    '.40 s&w': '40sw',
    '.40 smith & wesson': '40sw',
    '40 smith & wesson': '40sw',
    '308 win': '308win',
    '.308 win': '308win',
    '308 winchester': '308win',
    '.308 winchester': '308win',
    '7.62x39': '762x39',
    '7.62x39mm': '762x39',
    '7.62 x 39mm': '762x39',
    '300 blk': '300blk',
    '.300 blk': '300blk',
    '300 blackout': '300blk',
    '.300 blackout': '300blk',
    '300 aac blackout': '300blk',
    '38 special': '38spl',
    '.38 special': '38spl',
    '38 spl': '38spl',
    '38 special +p': '38spl',
    '357 magnum': '357mag',
    '.357 magnum': '357mag',
    '357 mag': '357mag',
    '.357 mag': '357mag',
    '45 auto': '45acp',
    '.45 auto': '45acp',
    '45 acp': '45acp',
    '44 magnum': '44mag',
    '.44 magnum': '44mag',
    '44 mag': '44mag',
    '10mm auto': '10mm',
    '10mm': '10mm',
    '10 mm auto': '10mm',
    '30-06': '30-06',
    '30-06 springfield': '30-06',
    '.30-06 springfield': '30-06',
    '30-06 sprg': '30-06',
    '270 win': '270win',
    '270 winchester': '270win',
    '.270 winchester': '270win',
    '.270 win': '270win',
}

# Discovery url-slug aliases (expansion #4). Used by the discovery
# adapters + validation harness only; NOT by normalize_caliber.
CALIBER_URL_ALIASES = {
    '9mm': ['9mm', '9mm-luger', '9-mm'],
    '223-556': ['223-rem', '223rem', '223-remington', '223', '223-556', '223-556mm', '223-5-56', '223-5.56', '223-rem-5-56-nato', '5-56', '556-nato', '556mm-nato', '556x45-nato', '5.56x45', '5-56x45mm', '5-56x45mm-nato'],
    '22lr': ['22-lr', '22lr', '22-long-rifle', '22lr-long-rifle'],
    '380acp': ['380-acp', '380-auto', '380'],
    '40sw': ['40-sw', '40-s-w', '40sw', '40-cal', '40-cal-sw'],
    '308win': ['308-win', '308-winchester', '308', '308-762x51', '308-win-762x51', '308-7-62-nato', '308-7-62x51', '308-7-62x51mm', '308-win-7-62x51', '762x51-nato', '762x51mm-nato', '7-62x51'],
    '762x39': ['7-62x39', '762x39', '7.62x39', '7-62x39mm', '762x39mm', '7.62x39mm', '7-62-x39', '7-62-x-39', '7-62-x-39mm'],
    '300blk': ['300-blackout', '300-aac-blackout', '300-aac', '300blk'],
    '38spl': ['38-special', '38-specials', '38-spl'],
    '357mag': ['357-magnum', '357-mag', '357'],
    '45acp': ['45-acp', '45-auto', '45acp', '45-acp-auto'],
    '10mm': ['10mm', '10mm-auto', '10mm-ammo', '10mm-auto-ammo', '10-mm', '10mm-ammunition'],
    '30-06': ['30-06', '30-06-springfield', '30-06-sprg', '3006', '30-06-ammo', '30-06-springfield-ammo', '30-06-sprg-ammo', '3006-springfield', '30-06-spring', '30-06-ammunition'],
    '270win': ['270-win', '270-winchester', '270-win-ammo', '270-winchester-ammo', '270', '270-ammo', '270win'],
}

# Twins of scripts/match_manufacturer_rebates_to_listings.py caliber sets.
REBATE_HANDGUN_CALIBERS = ('9mm', '380acp', '40sw', '38spl', '357mag', '45acp', '10mm')
REBATE_RIFLE_CALIBERS   = ('223-556', '308win', '762x39', '300blk', '30-06', '270win')
REBATE_RIMFIRE_CALIBERS = ('22lr',)
