"""Pure 5-gate validation evaluator for caliber_paths URLs (#4 Step-3).

    evaluate(page, caliber, entry_kind, title_filter=None) -> dict

NO I/O. The fetch adapter (step 3) loads a category page in the retailer's
REAL fetch environment and hands this evaluator a normalized `Page`; the
evaluator decides PASS / FAIL / NEEDS_REVIEW from the 5 gates. Being pure makes
it unit-testable against fixtures with no network — generalizing the hand-built
scripts/_probe_tsusa_dryrun.py.

THE 5 GATES
  1 HTTP 200
  2 no redirect (reuses scraper_lib.category_redirected: query + trailing-slash tolerant)
  3 page <title> names the caliber (title_mentions, built from the registry's
    CALIBER_URL_ALIASES — there is NO prebuilt urlMentionsCaliber)
  4 product-title gate-pass: normalize_caliber(card) == caliber for a high % of cards
  5 enough products (n_products)

VERDICTS (the approved #4 design — do not drift)
  FAIL          = non-200 (>=400) OR redirected OR gate4 <30% OR gate3 names a
                  DIFFERENT tracked caliber.
  NEEDS_REVIEW  = the honest gray zone: unreachable/transient, generic title
                  (per-caliber, products OK but title unconfirmed), gate4 30-70%,
                  1-4 products, OR n_products==0 on an otherwise-healthy page
                  (note 'empty-healthy' -> propose PARK, NEVER FAIL — the aeammo
                  308win case).
  PASS          = 200, not redirected, gate4 PASS (>=70%), gate5 PASS (>=5), and
                  gate3 confirmed (title names the caliber) or parent-exempt.

ENTRY-KIND AWARE (critical — a flat gate false-FAILs every parent_paths retailer)
  'per_caliber' : gate3 must name the caliber; gate4 = % cards == that caliber
                  (70/30 purity bands); gate5 on total cards.
  'parent'      : gate3 EXEMPT (parent titles say "Rifle Ammo", not a caliber);
                  gate4 = does the parent yield ANY tracked-caliber cards
                  (PRESENCE, not the 70% purity bar — parents are mixed by
                  design); gate5 on the tracked-card count.
  title_filter  : gritr-style mixed page — apply the regex FIRST, then run the
                  per-caliber gates on the post-filter subset.
"""
import re

from scraper_lib import normalize_caliber, category_redirected
from caliber_registry_gen import CALIBER_URL_ALIASES

# Verdicts
PASS = 'PASS'
FAIL = 'FAIL'
NEEDS_REVIEW = 'NEEDS_REVIEW'

# Entry kinds
PER_CALIBER = 'per_caliber'
PARENT = 'parent'

# Thresholds (approved #4 design — do not drift)
GATE4_PASS_PCT = 70.0
GATE4_FAIL_PCT = 30.0
GATE5_MIN_PRODUCTS = 5

# The tracked-caliber universe = the calibers that carry URL aliases.
TRACKED_CALIBERS = tuple(CALIBER_URL_ALIASES.keys())


class Page:
    """Normalized page object the fetch adapter (step 3) produces and the
    evaluator consumes. Dependency-free so fixtures are trivial to build.

      status        HTTP status int, or None when the fetch never resolved
                    (timeout / connection error after the adapter's retry).
      requested_url the URL the adapter asked for (relative or absolute).
      landed_url    where it actually landed (for the redirect gate).
      title         the page <title> text.
      card_titles   product-card title strings (adapter extracts via the
                    config `selectors` for DOM retailers, or straight from
                    /products.json for requests-Shopify).
    """
    __slots__ = ('status', 'requested_url', 'landed_url', 'title', 'card_titles')

    def __init__(self, status, requested_url, landed_url, title, card_titles):
        self.status = status
        self.requested_url = requested_url
        self.landed_url = landed_url
        self.title = title or ''
        self.card_titles = list(card_titles or [])


def _normspace(text):
    """Lowercase, collapse every non-alphanumeric run to one space, and pad
    with spaces so a slug alias ('9mm-luger') and a human title ('9mm Luger
    Ammo') match by whole-token containment ('556' won't match in '5560')."""
    return ' ' + re.sub(r'[^a-z0-9]+', ' ', (text or '').lower()).strip() + ' '


def title_mentions(title, caliber):
    """True when `title` names `caliber`, via the registry's CALIBER_URL_ALIASES
    (the discovery/validation slug list). Whole-token containment."""
    hay = _normspace(title)
    return any(_normspace(alias) in hay
               for alias in CALIBER_URL_ALIASES.get(caliber, ()))


def _gate3(title, caliber, is_parent):
    """Return (title_match, gate3_fail).

    title_match is None for parents (gate exempt). For per-caliber/gritr,
    gate3_fail is True ONLY when the title names a DIFFERENT tracked caliber and
    not this one (a positive wrong-page signal); a generic title that names no
    caliber is (False, False) — gray, never a fail."""
    if is_parent:
        return None, False
    if title_mentions(title, caliber):
        return True, False
    names_other = any(title_mentions(title, c)
                      for c in TRACKED_CALIBERS if c != caliber)
    return False, names_other


def _result(verdict, status, redirect=None, title_match=None,
            gate_pass_pct=None, n_products=None, note=''):
    """The validation record: the measured gate fields + verdict + note. The
    write-back (step 4) adds method/validated_at; the evaluator never does."""
    return {
        'verdict': verdict,
        'status': status,
        'redirect': redirect,
        'title_match': title_match,
        'gate_pass_pct': gate_pass_pct,
        'n_products': n_products,
        'note': note,
    }


def evaluate(page, caliber, entry_kind, title_filter=None):
    """Run the 5 gates and return a validation record. Pure."""
    is_parent = entry_kind in (PARENT, 'parent_paths')
    status = page.status

    # --- Gate 1: HTTP 200 ---
    if not isinstance(status, int):
        return _result(NEEDS_REVIEW, status,
                       note='unreachable: fetch did not resolve (transient?)')
    if status >= 400:
        return _result(FAIL, status, note=f'HTTP {status}')

    # --- Gate 2: no redirect ---
    if category_redirected(page.requested_url, page.landed_url):
        return _result(FAIL, status, redirect=True,
                       note=f'redirected to {page.landed_url}')

    # --- Gate 3: title names the caliber ---
    title_match, gate3_fail = _gate3(page.title, caliber, is_parent)
    if gate3_fail:
        return _result(FAIL, status, redirect=False, title_match=False,
                       note='title names a different tracked caliber')

    # --- Gates 4 & 5: product cards ---
    titles = [t.strip() for t in page.card_titles if t and t.strip()]
    if title_filter is not None:
        rx = (title_filter if hasattr(title_filter, 'search')
              else re.compile(title_filter, re.IGNORECASE))
        titles = [t for t in titles if rx.search(t)]
    n_products = len(titles)

    if is_parent:
        n_match = sum(1 for t in titles if normalize_caliber(t)[1] in TRACKED_CALIBERS)
    else:
        n_match = sum(1 for t in titles if normalize_caliber(t)[1] == caliber)
    gate_pass_pct = round(100.0 * n_match / n_products, 1) if n_products else 0.0

    # Gate 5 special case: empty but otherwise healthy. NEVER FAIL — propose PARK
    # (the aeammo 308win catalog-absence case: page 200, no redirect, zero products).
    if n_products == 0:
        return _result(NEEDS_REVIEW, status, redirect=False, title_match=title_match,
                       gate_pass_pct=0.0, n_products=0,
                       note='empty-healthy: page OK but zero products (propose PARK, not replace)')

    # --- Gate 4 verdict ---
    if is_parent:
        # Mixed by design: presence of tracked cards, NOT the 70% purity bar
        # (that bar would false-FAIL every parent). Zero tracked among real
        # cards = wrong/broken parent.
        gate4 = FAIL if n_match == 0 else PASS
        gate4_note = 'gate4: parent yields zero tracked-caliber cards'
    else:
        if gate_pass_pct >= GATE4_PASS_PCT:
            gate4 = PASS
        elif gate_pass_pct < GATE4_FAIL_PCT:
            gate4 = FAIL
        else:
            gate4 = NEEDS_REVIEW
        gate4_note = f'gate4: only {gate_pass_pct}% of {n_products} cards match {caliber}'

    if gate4 == FAIL:
        return _result(FAIL, status, redirect=False, title_match=title_match,
                       gate_pass_pct=gate_pass_pct, n_products=n_products,
                       note=gate4_note)

    # --- Gate 5 verdict (count). Parents count tracked cards; per-caliber counts
    # all cards. 1-4 is NEEDS_REVIEW, never a hard FAIL (small mfrs are legit). ---
    count_for_gate5 = n_match if is_parent else n_products
    gate5 = PASS if count_for_gate5 >= GATE5_MIN_PRODUCTS else NEEDS_REVIEW

    # --- Combine ---
    grays = []
    if gate4 == NEEDS_REVIEW:
        grays.append(gate4_note)
    if gate5 == NEEDS_REVIEW:
        grays.append(f'gate5: only {count_for_gate5} product(s)')
    if not is_parent and not title_match:
        grays.append('title does not name the caliber (generic)')

    if gate4 == PASS and gate5 == PASS and (is_parent or title_match):
        return _result(PASS, status, redirect=False, title_match=title_match,
                       gate_pass_pct=gate_pass_pct, n_products=n_products,
                       note='all gates pass')
    return _result(NEEDS_REVIEW, status, redirect=False, title_match=title_match,
                   gate_pass_pct=gate_pass_pct, n_products=n_products,
                   note='; '.join(grays))
