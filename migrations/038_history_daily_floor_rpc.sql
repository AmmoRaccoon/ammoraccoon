-- 038_history_daily_floor_rpc.sql
-- Server-side implementation of the /history chart's CURRENT daily-floor
-- algorithm (June-2026 rebuild), plus three companion functions that let the
-- page drop its full-catalog download entirely. Perf-audit 2026-07-05 #1 /
-- Sub-task 3b, Phase A.
--
-- ⚠️  DO NOT APPLY until Jon approves in chat. Phase A writes this file only.
--
-- WHAT THIS IS NOT: the old segment_daily_floor (migrations 023/025/027)
-- implements the RETIRED alive-at-T algorithm — 6h buckets, latest-row-per-
-- bucket, min-alive gate, LOCF carry-forward, pool gated on CURRENT
-- listings.in_stock. None of that exists here. This migration neither touches
-- nor reuses it.
--
-- THE ALGORITHM (must stay bit-for-bit equal to app/history/HistoryClient.js
-- buildPointsFromRows + its pool pipeline; the authoritative extraction lives
-- in ammoraccoon-web/reports/history-rpc-phase-a-2026-07-08.md §1):
--   • Fixed 24h UTC calendar bins: bin = floor(epoch_seconds / 86400).
--   • A listing contributes to a bin iff it had ≥1 in_stock=TRUE price_history
--     row recorded in that bin; its bin value is its CHEAPEST such price.
--   • Bin value = mean of the 3 cheapest distinct listings' bin values
--     (fewer than 3 → mean of what exists).
--   • HONEST GAPS: a bin with zero in-stock observations is simply absent.
--     No carry-forward, no interpolation, no minimum-alive gate — ever.
--   • NO FORMING POINT: the current (incomplete) UTC day is never returned.
--   • Current listings.in_stock is used NOWHERE — only the per-row flag.
--
-- FLOAT-PARITY CONTRACT (why the SQL looks pedantic):
--   The client computes in IEEE-754 float64. Every arithmetic step here is
--   forced onto double precision (float8) with the SAME operation order:
--   numeric→float8 casts mirror JS parseFloat (both take the nearest double
--   of the decimal), the per-row tax multiply happens BEFORE min/average
--   (as in addSeries), the cheapest-3 mean is an explicit left-associative
--   ((v1+v2)+v3)/3 — matching Array.reduce — never a SUM() aggregate whose
--   addition order is unspecified. Upper-median×5 outlier caps replicate
--   sorted[floor(n/2)] * 5 exactly.
--
-- REGISTRY RULE RESPECTED: this SQL contains NO caliber-specific constants.
-- Per-caliber floors/ceilings, grain buckets, the brand alias map, the brand
-- list, and free-shipping thresholds all arrive as PARAMETERS supplied by the
-- web client from lib/caliberRegistry.gen.js / lib/brandNormalizer.js /
-- lib/retailerConfig.js — one source of truth, nothing to drift.
-- (Required params p_price_floor / p_hard_ceiling have NO defaults on
-- purpose — the 025 experience showed a defaulted floor silently diverging.)
--
-- OBJECTS CREATED (all read-only; invoker rights; anon has SELECT on the
-- underlying tables per migration 022):
--   VIEW  history_stage1_listings        — component-excluded + outlier-capped
--                                          catalog ("listings" React state twin)
--   FN    history_brand_matches(...)     — brand predicate (shared, IMMUTABLE)
--   FN    history_grain_matches(...)     — grain-bucket predicate (shared)
--   FN    history_pool_listings(...)     — the matchingListings pool, slim
--   FN    history_daily_floor(...)       — THE chart series (parity-gated)
--   FN    history_filter_options(...)    — dropdown option lists
--   FN    history_ticker(...)            — per-caliber cheapest Brass/New row
--
-- Idempotency: CREATE OR REPLACE throughout; safe to re-run.
-- Requires PostgreSQL 15+ (security_invoker view option) — Supabase is 15+.

-- ---------------------------------------------------------------------------
-- Stage-1 catalog: listings after (a) component exclusion and (b) the
-- per-caliber listing-level outlier filter. Twin of the client's
--   filterListingOutliers(all.filter(l => !isLikelyComponent(l)
--                                         && l.is_component !== true))
-- Component signal: the DB flag only. The JS heuristic is write-time ported
-- to the scrapers (parity-guarded), so flag==heuristic in steady state; the
-- Phase-A preflight probe measures live disagreement and the parity harness
-- catches any pool effect. Cap rule (lib/outliers.js):
--   cap[cal] = 5 × upper-median of finite positive prices of that caliber
--   (upper median = 0-indexed element floor(n/2) of the ascending sort,
--    i.e. row_number = n/2 + 1 with integer division);
--   calibers with no finite positive price (or NULL/'' caliber) have no cap
--   and their listings pass through unfiltered — exactly the JS behavior for
--   caps[cal] === undefined.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.history_stage1_listings
WITH (security_invoker = true) AS
WITH priced AS (
  -- NaN guard: numeric can hold NaN, and Postgres sorts/compares NaN as
  -- GREATER than everything (NaN > 0 is TRUE here), while the client's
  -- isFinite() drops it. Explicit <> 'NaN' keeps the median input sets
  -- identical. (Preflight probe 2026-07-08: zero NaN prices live — this
  -- pins the pathological case anyway.)
  SELECT caliber_normalized AS cal,
         price_per_round::float8 AS price,
         row_number() OVER (PARTITION BY caliber_normalized
                            ORDER BY price_per_round::float8) AS rn,
         count(*) OVER (PARTITION BY caliber_normalized) AS n
    FROM public.listings
   WHERE is_component IS NOT TRUE
     AND caliber_normalized IS NOT NULL AND caliber_normalized <> ''
     AND price_per_round IS NOT NULL
     AND price_per_round::float8 <> 'NaN'::float8
     AND price_per_round::float8 > 0
),
caps AS (
  SELECT cal, price * 5.0::float8 AS cap
    FROM priced
   WHERE rn = (n / 2) + 1
)
SELECT l.*
  FROM public.listings l
  LEFT JOIN caps c ON c.cal = l.caliber_normalized
 WHERE l.is_component IS NOT TRUE
   AND (c.cap IS NULL
        OR (l.price_per_round IS NOT NULL
            AND l.price_per_round::float8 > 0
            AND l.price_per_round::float8 <= c.cap));

GRANT SELECT ON public.history_stage1_listings TO anon, authenticated;

-- ---------------------------------------------------------------------------
-- Brand predicate. Twin of HistoryClient getBrand(l) === filterBrand:
--   manufacturer truthy (non-NULL, non-'') →
--     brand = ALIAS_MAP[lower(trim(m))] ?? trim(m)   (case-sensitive compare)
--   manufacturer falsy → first BRAND_LIST entry whose lowercase form is a
--     substring of lower(product_url); no URL / no match → no brand → false.
-- The alias map arrives as two arrays: p_brand_alias_keys = the lowercased
-- keys that normalize to p_brand; p_alias_keys_all = every alias key (needed
-- to detect "this manufacturer aliases to some OTHER brand" — trim-equality
-- must not match those). p_brand_list is the ordered BRAND_LIST (first-match
-- wins, same as the JS loop).
-- btrim set mirrors JS String.trim for ASCII whitespace ( \t\n\r\f\v );
-- exotic Unicode whitespace in manufacturer values is assumed absent
-- (preflight probe checks trim-needed counts).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.history_brand_matches(
  p_manufacturer     text,
  p_product_url      text,
  p_brand            text,
  p_brand_alias_keys text[],
  p_alias_keys_all   text[],
  p_brand_list       text[]
) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE
    WHEN p_manufacturer IS NOT NULL AND p_manufacturer <> '' THEN
      CASE WHEN lower(btrim(p_manufacturer, E' \t\n\r\f\x0B')) = ANY (p_alias_keys_all)
           THEN lower(btrim(p_manufacturer, E' \t\n\r\f\x0B')) = ANY (p_brand_alias_keys)
           ELSE btrim(p_manufacturer, E' \t\n\r\f\x0B') = p_brand
      END
    WHEN p_product_url IS NOT NULL AND p_product_url <> '' THEN
      COALESCE(
        (SELECT bl.b
           FROM unnest(p_brand_list) WITH ORDINALITY AS bl(b, ord)
          WHERE strpos(lower(p_product_url), lower(bl.b)) > 0
          ORDER BY bl.ord
          LIMIT 1) = p_brand,
        FALSE)
    ELSE FALSE
  END
$$;

-- ---------------------------------------------------------------------------
-- Grain-bucket predicate. Twin of matchesGrainBucket (lib/grainBuckets.js).
-- The client translates the selected bucket into atoms:
--   named bucket  → its predicate atoms, p_negate = false
--                   (every registry bucket is one of g<v, g=v, g>v)
--   'other'       → ALL of the caliber's bucket atoms, p_negate = true
--   legacy number → single eq atom
--   unparseable   → the client short-circuits (never calls with NaN)
-- grain NULL → false whenever a grain filter is present (JS early return).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.history_grain_matches(
  p_grain        float8,
  p_grain_atoms  jsonb,
  p_grain_negate boolean
) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE
    WHEN p_grain_atoms IS NULL THEN TRUE
    WHEN p_grain IS NULL THEN FALSE
    -- NaN guard: JS comparisons with NaN are all false, so a NaN grain
    -- matches NO named bucket but DOES match 'other' (the negation of
    -- no-bucket-matched). Postgres instead treats NaN as greater than
    -- everything (NaN > v would be TRUE), which would flip the XOR — so
    -- the NaN case is resolved explicitly to the JS outcome: negate value
    -- itself ('other' → true, named bucket → false).
    WHEN p_grain = 'NaN'::float8 THEN COALESCE(p_grain_negate, FALSE)
    ELSE (COALESCE(p_grain_negate, FALSE) <> EXISTS (
            SELECT 1
              FROM jsonb_to_recordset(p_grain_atoms) AS a(op text, v float8)
             WHERE (a.op = 'lt' AND p_grain <  a.v)
                OR (a.op = 'eq' AND p_grain =  a.v)
                OR (a.op = 'gt' AND p_grain >  a.v)))
  END
$$;

-- ---------------------------------------------------------------------------
-- The pool: twin of the matchingListings effect (HistoryClient ~line 1300).
-- Returns full listing rows (callers vertical-filter via PostgREST select=).
-- NOT filtered by current in_stock — per the daily-floor rebuild, a listing
-- that is OOS today still plots on the bins where it was observed in stock.
--
-- p_freeship implements the zip-agnostic ("free-ship only") mode. The client
-- sends one {rid, thr} entry per retailer in its hardcoded RETAILERS map,
-- EXCLUDING freeOver<0 ("never free") retailers; thr=0 encodes always-free /
-- unknown-threshold (kept unconditionally). Rows replicate the JS cart math
--   boxes = targetRounds>0 ? ceil(targetRounds/total_rounds) : 1
--   cart  = (price × total_rounds) × boxes        (left-assoc, float64)
--   keep iff NOT (cart < thr)  — so NaN carts (null price; null/0 rounds
--   with targetRounds>0) are KEPT, and a 0 cart (null/0 rounds, no target)
--   fails any positive threshold. Negative total_rounds is assumed absent.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.history_pool_listings(
  p_caliber          text,
  p_case             text    DEFAULT NULL,
  p_condition        text    DEFAULT NULL,
  p_bullet_type      text    DEFAULT NULL,
  p_retailer_id      integer DEFAULT NULL,
  p_grain_atoms      jsonb   DEFAULT NULL,
  p_grain_negate     boolean DEFAULT FALSE,
  p_model_grain      float8  DEFAULT NULL,
  p_model_bullet     text    DEFAULT NULL,
  p_brand            text    DEFAULT NULL,
  p_brand_alias_keys text[]  DEFAULT NULL,
  p_alias_keys_all   text[]  DEFAULT NULL,
  p_brand_list       text[]  DEFAULT NULL,
  p_freeship         jsonb   DEFAULT NULL,
  p_target_rounds    integer DEFAULT 0
) RETURNS SETOF public.listings
LANGUAGE sql STABLE AS $$
  SELECT s.*
    FROM public.history_stage1_listings s
   WHERE s.caliber_normalized = p_caliber
     AND (p_case IS NULL OR s.case_material = p_case)
     AND (p_condition IS NULL OR s.condition_type = p_condition)
     AND (p_bullet_type IS NULL OR s.bullet_type = p_bullet_type)
     AND (p_retailer_id IS NULL OR s.retailer_id = p_retailer_id)
     AND (p_model_grain IS NULL
          OR (s.grain IS NOT NULL AND s.grain::float8 = p_model_grain))
     AND (p_model_bullet IS NULL OR s.bullet_type = p_model_bullet)
     AND public.history_grain_matches(s.grain::float8, p_grain_atoms, p_grain_negate)
     AND (p_brand IS NULL
          OR public.history_brand_matches(s.manufacturer, s.product_url,
               p_brand, p_brand_alias_keys, p_alias_keys_all, p_brand_list))
     AND (p_freeship IS NULL OR EXISTS (
           SELECT 1
             FROM jsonb_to_recordset(p_freeship) AS f(rid int, thr float8)
            WHERE f.rid = s.retailer_id
              AND (
                f.thr <= 0
                OR s.price_per_round IS NULL
                OR ((s.total_rounds IS NULL OR s.total_rounds <= 0)
                    AND p_target_rounds > 0)
                OR (CASE WHEN s.total_rounds IS NULL OR s.total_rounds <= 0
                         THEN 0.0::float8
                         ELSE (s.price_per_round::float8 * s.total_rounds::float8)
                              * (CASE WHEN p_target_rounds > 0
                                      THEN ceil(p_target_rounds::float8
                                                / s.total_rounds::float8)
                                      ELSE 1.0::float8 END)
                    END) >= f.thr)))
$$;

-- ---------------------------------------------------------------------------
-- THE CHART SERIES. One row per (series_key, completed UTC day).
--   p_split='none'      → series_key ''            (single line)
--   p_split='retailer'  → series_key = retailer_id::text; rows whose listing
--                         has NULL retailer_id are dropped (JS: rid==null →
--                         skipped)
--   p_split='condition' → series_key = condition_type, NULL/'' → 'Unknown'
--                         (callers pass p_condition=NULL in this mode, same
--                         as the client which skips the condition filter
--                         when splitting)
-- Row-level bounds twin addSeries: floor = p_price_floor (registry
-- PER_CALIBER_FLOOR ?? 0.10), ceiling = LEAST(stage-2 dynamic cap, registry
-- PER_CALIBER_CEILING ?? 2.00). The stage-2 cap is the upper-median×5 over
-- the STAGE-1 catalog of this caliber — i.e. buildOutlierCaps(listings)[cal],
-- caps-of-the-already-capped set, exactly as the client double-computes it.
-- Bounds test the RAW price; p_price_mult (tax overlay) multiplies AFTER,
-- per row, before binning — the client's exact order of operations.
-- pool_size = pool LISTING count for the series' group (matchingListings
-- .length / per-group counts in JS), independent of how many rows plotted.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.history_daily_floor(
  p_caliber          text,
  p_from             timestamptz,
  p_price_floor      float8,
  p_hard_ceiling     float8,
  p_to               timestamptz DEFAULT NULL,
  p_case             text    DEFAULT NULL,
  p_condition        text    DEFAULT NULL,
  p_bullet_type      text    DEFAULT NULL,
  p_retailer_id      integer DEFAULT NULL,
  p_grain_atoms      jsonb   DEFAULT NULL,
  p_grain_negate     boolean DEFAULT FALSE,
  p_model_grain      float8  DEFAULT NULL,
  p_model_bullet     text    DEFAULT NULL,
  p_brand            text    DEFAULT NULL,
  p_brand_alias_keys text[]  DEFAULT NULL,
  p_alias_keys_all   text[]  DEFAULT NULL,
  p_brand_list       text[]  DEFAULT NULL,
  p_price_mult       float8  DEFAULT 1.0,
  p_split            text    DEFAULT 'none',
  p_freeship         jsonb   DEFAULT NULL,
  p_target_rounds    integer DEFAULT 0
) RETURNS TABLE (series_key text, day date, value float8, pool_size integer)
LANGUAGE plpgsql STABLE AS $$
#variable_conflict use_column
DECLARE
  -- Today's (incomplete) UTC bin — never plotted. floor(epoch/86400) is the
  -- same day number as the client's floor(Date.now()/86400000).
  v_today_bin bigint := floor(extract(epoch FROM now()) / 86400)::bigint;
BEGIN
  IF p_split NOT IN ('none', 'retailer', 'condition') THEN
    RAISE EXCEPTION
      'history_daily_floor: p_split must be none|retailer|condition (got %)', p_split;
  END IF;
  IF p_price_mult IS NULL OR p_price_mult <= 0 THEN
    RAISE EXCEPTION
      'history_daily_floor: p_price_mult must be > 0 (got %)', p_price_mult;
  END IF;

  RETURN QUERY
  WITH stage1_cal AS (
    -- This caliber's stage-1 slice (used only for the stage-2 cap below;
    -- the pool itself comes from history_pool_listings).
    SELECT s.price_per_round
      FROM public.history_stage1_listings s
     WHERE s.caliber_normalized = p_caliber
  ),
  cap2 AS (
    -- Stage-2 dynamic cap: 5 × upper-median over stage-1 prices (finite>0)
    -- of this caliber. NULL (→ hard ceiling alone) when no such price exists.
    SELECT (
      SELECT sc.price_per_round::float8
        FROM stage1_cal sc
       WHERE sc.price_per_round IS NOT NULL
         AND sc.price_per_round::float8 <> 'NaN'::float8   -- see view NaN note
         AND sc.price_per_round::float8 > 0
       ORDER BY sc.price_per_round::float8
      OFFSET (SELECT count(*) / 2
                FROM stage1_cal sc2
               WHERE sc2.price_per_round IS NOT NULL
                 AND sc2.price_per_round::float8 <> 'NaN'::float8
                 AND sc2.price_per_round::float8 > 0)
       LIMIT 1
    ) * 5.0::float8 AS cap
  ),
  bounds AS (
    SELECT p_price_floor AS lo,
           CASE WHEN c.cap IS NULL THEN p_hard_ceiling
                ELSE LEAST(c.cap, p_hard_ceiling) END AS hi
      FROM cap2 c
  ),
  pool AS (
    SELECT pl.id, pl.retailer_id, pl.condition_type
      FROM public.history_pool_listings(
             p_caliber          := p_caliber,
             p_case             := p_case,
             p_condition        := p_condition,
             p_bullet_type      := p_bullet_type,
             p_retailer_id      := p_retailer_id,
             p_grain_atoms      := p_grain_atoms,
             p_grain_negate     := p_grain_negate,
             p_model_grain      := p_model_grain,
             p_model_bullet     := p_model_bullet,
             p_brand            := p_brand,
             p_brand_alias_keys := p_brand_alias_keys,
             p_alias_keys_all   := p_alias_keys_all,
             p_brand_list       := p_brand_list,
             p_freeship         := p_freeship,
             p_target_rounds    := p_target_rounds) AS pl
  ),
  kept AS (
    -- Per-row in_stock=TRUE observations in window, raw price inside
    -- [floor, ceiling], then the tax multiplier applied per row (client
    -- order: bounds on raw, multiply after). The (>0) guard mirrors
    -- buildPointsFromRows' p>0 check on the post-multiplier value.
    SELECT pl.id AS listing_id,
           pl.retailer_id,
           pl.condition_type,
           (ph.price_per_round::float8 * p_price_mult) AS price,
           floor(extract(epoch FROM ph.recorded_at) / 86400)::bigint AS day_num
      FROM public.price_history ph
      JOIN pool pl ON pl.id = ph.listing_id
      CROSS JOIN bounds b
     WHERE ph.in_stock = TRUE
       AND ph.recorded_at >= p_from
       AND (p_to IS NULL OR ph.recorded_at <= p_to)
       AND ph.price_per_round IS NOT NULL
       AND ph.price_per_round::float8 >= b.lo
       AND ph.price_per_round::float8 <= b.hi
       AND (ph.price_per_round::float8 * p_price_mult) > 0
  ),
  binned AS (
    -- Cheapest observation per (series, listing, completed day). Today's
    -- (and any future) bin is excluded HERE, matching the client which
    -- skips bins >= nowBin.
    SELECT CASE p_split
             WHEN 'retailer'  THEN k.retailer_id::text
             WHEN 'condition' THEN CASE WHEN k.condition_type IS NULL
                                          OR k.condition_type = ''
                                        THEN 'Unknown' ELSE k.condition_type END
             ELSE ''
           END AS skey,
           k.day_num,
           k.listing_id,
           min(k.price) AS lprice
      FROM kept k
     WHERE k.day_num < v_today_bin
       AND (p_split <> 'retailer' OR k.retailer_id IS NOT NULL)
     GROUP BY 1, 2, 3
  ),
  ranked AS (
    SELECT b2.skey, b2.day_num, b2.lprice,
           row_number() OVER (PARTITION BY b2.skey, b2.day_num
                              ORDER BY b2.lprice ASC) AS rnk
      FROM binned b2
  ),
  agg AS (
    -- The ≤3 cheapest per-listing values, ascending. Ties among equal
    -- prices rank arbitrarily but contribute identical values, so the
    -- mean is unaffected (same as the client's value-array sort).
    SELECT r.skey, r.day_num,
           array_agg(r.lprice ORDER BY r.lprice ASC) AS vals
      FROM ranked r
     WHERE r.rnk <= 3
     GROUP BY r.skey, r.day_num
  ),
  pool_counts AS (
    SELECT CASE p_split
             WHEN 'retailer'  THEN pl.retailer_id::text
             WHEN 'condition' THEN CASE WHEN pl.condition_type IS NULL
                                          OR pl.condition_type = ''
                                        THEN 'Unknown' ELSE pl.condition_type END
             ELSE ''
           END AS skey,
           count(*)::int AS pool_n
      FROM pool pl
     GROUP BY 1
  )
  -- Left-associative ((v1+v2)+v3)/n in float8 — the exact IEEE sequence of
  -- the client's prices.slice(0,n).reduce((s,v)=>s+v,0)/n (0+v1 ≡ v1).
  SELECT a.skey,
         (DATE '1970-01-01' + a.day_num::int) AS day,
         (CASE cardinality(a.vals)
            WHEN 1 THEN a.vals[1]
            WHEN 2 THEN (a.vals[1] + a.vals[2]) / 2.0::float8
            ELSE ((a.vals[1] + a.vals[2]) + a.vals[3]) / 3.0::float8
          END) AS value,
         COALESCE(pc.pool_n, 0) AS pool_size
    FROM agg a
    LEFT JOIN pool_counts pc ON pc.skey = a.skey
   ORDER BY a.skey, a.day_num;
END;
$$;

-- ---------------------------------------------------------------------------
-- Dropdown option lists (page chrome; equivalence-by-construction, not under
-- the point-parity gate). All sets are computed over the stage-1 catalog —
-- the same `listings` React state the client derives them from today.
-- Sorting is left to the client (it re-sorts with its existing comparators),
-- so no SQL collation can drift from JS sort semantics.
--   grains         : distinct truthy grain values (across ALL calibers)
--   bullet_types   : distinct truthy bullet_type
--   case_materials : distinct truthy case_material
--   calibers       : distinct truthy caliber_normalized
--   models         : when p_brand given — distinct {grain, bt} tuples over
--                    brand-matched listings with finite grain and non-empty
--                    trimmed bullet_type (modelOptions twin)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.history_filter_options(
  p_brand            text   DEFAULT NULL,
  p_brand_alias_keys text[] DEFAULT NULL,
  p_alias_keys_all   text[] DEFAULT NULL,
  p_brand_list       text[] DEFAULT NULL
) RETURNS jsonb
LANGUAGE sql STABLE AS $$
  SELECT jsonb_build_object(
    'grains', COALESCE((
       SELECT jsonb_agg(DISTINCT s.grain)
         FROM public.history_stage1_listings s
        WHERE s.grain IS NOT NULL AND s.grain <> 0
          AND s.grain::float8 <> 'NaN'::float8), '[]'::jsonb),
    'bullet_types', COALESCE((
       SELECT jsonb_agg(DISTINCT s.bullet_type)
         FROM public.history_stage1_listings s
        WHERE s.bullet_type IS NOT NULL AND s.bullet_type <> ''), '[]'::jsonb),
    'case_materials', COALESCE((
       SELECT jsonb_agg(DISTINCT s.case_material)
         FROM public.history_stage1_listings s
        WHERE s.case_material IS NOT NULL AND s.case_material <> ''), '[]'::jsonb),
    'calibers', COALESCE((
       SELECT jsonb_agg(DISTINCT s.caliber_normalized)
         FROM public.history_stage1_listings s
        WHERE s.caliber_normalized IS NOT NULL AND s.caliber_normalized <> ''), '[]'::jsonb),
    'models', CASE WHEN p_brand IS NULL THEN '[]'::jsonb ELSE COALESCE((
       SELECT jsonb_agg(DISTINCT jsonb_build_object(
                'grain', t.g, 'bt', t.bt))
         FROM (SELECT s.grain::float8 AS g,
                      btrim(s.bullet_type, E' \t\n\r\f\x0B') AS bt
                 FROM public.history_stage1_listings s
                WHERE public.history_brand_matches(s.manufacturer, s.product_url,
                        p_brand, p_brand_alias_keys, p_alias_keys_all, p_brand_list)
                  AND s.grain IS NOT NULL
                  AND s.grain::float8 <> 'NaN'::float8
                  AND s.bullet_type IS NOT NULL) t
        WHERE t.bt <> ''), '[]'::jsonb) END
  )
$$;

-- ---------------------------------------------------------------------------
-- Ticker: per requested caliber, the single cheapest stage-1 listing that is
-- in_stock, Brass, New, and inside that caliber's [lo, hi] sanity bounds
-- (registry values, passed in — PriceTicker twin: strictly-cheapest, ties
-- broken by first encounter in id order). Returns full listing rows; the
-- client vertical-filters and keeps its own TRACKED_CALIBERS ordering.
-- p_bounds: [{"cal":"9mm","lo":0.10,"hi":1.00}, ...]
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.history_ticker(
  p_bounds jsonb
) RETURNS SETOF public.listings
LANGUAGE sql STABLE AS $$
  SELECT w.*
    FROM jsonb_to_recordset(p_bounds) AS b(cal text, lo float8, hi float8)
    CROSS JOIN LATERAL (
      SELECT s.*
        FROM public.history_stage1_listings s
       WHERE s.caliber_normalized = b.cal
         AND s.case_material = 'Brass'
         AND s.condition_type = 'New'
         AND s.in_stock = TRUE
         AND s.price_per_round IS NOT NULL
         AND s.price_per_round::float8 >= b.lo
         AND s.price_per_round::float8 <= b.hi
       ORDER BY s.price_per_round::float8 ASC, s.id ASC
       LIMIT 1
    ) w
$$;

GRANT EXECUTE ON FUNCTION public.history_brand_matches(text, text, text, text[], text[], text[]) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.history_grain_matches(float8, jsonb, boolean) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.history_pool_listings(text, text, text, text, integer, jsonb, boolean, float8, text, text, text[], text[], text[], jsonb, integer) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.history_daily_floor(text, timestamptz, float8, float8, timestamptz, text, text, text, integer, jsonb, boolean, float8, text, text, text[], text[], text[], float8, text, jsonb, integer) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.history_filter_options(text, text[], text[], text[]) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.history_ticker(jsonb) TO anon, authenticated;

-- PostgREST discovers new functions from its schema cache; poke it so the
-- RPCs are callable immediately after apply (harmless if already fresh).
NOTIFY pgrst, 'reload schema';
