-- 040_homepage_segment_aggregates_drop_avg.sql
-- Drops the retired segment_avg output (and its seg_median/seg_avg CTEs)
-- from homepage_segment_aggregates(). Perf audit 2026-07-05 finding #6;
-- the "drop-avg migration" lib/pricing.js has anticipated since 2026-05-31.
--
-- WHY: segment_avg was retired by reducer-divergence Option A (DECISIONS.md
-- 2026-05-30) and is read by NOTHING — proven by a both-repo consumer sweep
-- 2026-07-10: the web reads only segment_floor (+ keys) on both the cache
-- path and the RPC fallback path (lib/pricing.js explicitly ignores the
-- column); refresh_homepage_segment_aggregates_cache() (031/032) selects
-- only keys + segment_floor + listing_count; the cache table never stored
-- it (031 header: "segment_avg is intentionally NOT cached"); the parity
-- smoke removed its tests 2026-05-31. Yet computing it — a percentile_cont
-- sort plus a second grouped pass over the full 30-day ph_avg pool — was
-- roughly HALF the function's runtime (measured 2026-07-10 pre-migration:
-- 35.6s cold / 12.4s / 8.3s warm via Management API), which is why the
-- anon-path fallback (8s statement_timeout) could never succeed and even
-- the service-role cache refresh needed a 3-attempt retry loop.
--
-- HOW: 036's body VERBATIM minus the seg_median and seg_avg CTEs and the
-- segment_avg column in RETURNS TABLE / final SELECT. Everything read is
-- bit-identical: caliber_floors VALUES block unchanged (matches
-- migrations/gen/caliber_floors.values.sql — the registry twin), ph_avg /
-- ph_floor / listing_min / seg_counts / listing_min_ranked / seg_floor
-- untouched, p_min_listings filter untouched.
--
-- SIGNATURE UNCHANGED (8 params) even though p_outlier_mult now does
-- nothing: both live callers still pass it (lib/pricing.js by name on the
-- fallback path, refresh_homepage_segment_aggregates_cache positionally),
-- and PostgREST resolves functions by argument names — dropping the param
-- would break both callers until their own deploys land. It is kept as an
-- accepted-and-ignored compatibility param; remove it only in a later
-- coordinated migration if ever worth the churn.
--
-- DROP+CREATE (not OR REPLACE) because the return-type row shape changes.
-- LANGUAGE sql bodies are parsed at execution, so no dependent object
-- blocks the drop; the refresh function keeps working unchanged because it
-- selects named columns that all still exist. Applied as one Management-API
-- batch (single transaction) — no window where the function is absent.
--
-- Revert: DROP FUNCTION (this signature), then re-run migration 036's
-- CREATE + COMMENT verbatim.

DROP FUNCTION IF EXISTS public.homepage_segment_aggregates(
    TIMESTAMPTZ, TEXT, INT, INT, INT, INT, NUMERIC, NUMERIC);

CREATE FUNCTION public.homepage_segment_aggregates(
    p_since                TIMESTAMPTZ,
    p_condition            TEXT     DEFAULT 'New',
    p_min_listings         INT      DEFAULT 5,
    p_floor_full_n         INT      DEFAULT 10,
    p_floor_small_n        INT      DEFAULT 3,
    p_floor_full_threshold INT      DEFAULT 20,
    p_outlier_mult         NUMERIC  DEFAULT 5.0,   -- compat no-op since 040
    p_price_floor          NUMERIC  DEFAULT 0.10
) RETURNS TABLE (
    caliber_normalized TEXT,
    bullet_type        TEXT,
    case_material      TEXT,
    segment_floor      NUMERIC,
    listing_count      INT
)
LANGUAGE sql
STABLE
AS $$
    WITH caliber_floors(cal, floor_ppr) AS (
        -- MUST mirror ammoraccoon-web/lib/priceBounds.js PER_CALIBER_FLOOR
        VALUES
            ('9mm',      0.10::numeric),
            ('22lr',     0.03),
            ('380acp',   0.15),
            ('38spl',    0.20),
            ('357mag',   0.20),
            ('40sw',     0.15),
            ('45acp',    0.15),
            ('223-556',  0.20),
            ('300blk',   0.25),
            ('308win',   0.20),
            ('6.5cm',    0.40),
            ('762x39',   0.25),
            ('762x54r',  0.25),
            ('12ga',     0.15),
            ('10mm',     0.20),
            ('30-06',    0.20),
            ('270win',   0.20)
    ),
    ph_avg AS (
        SELECT
            ph.listing_id,
            ph.price_per_round::numeric AS ppr,
            l.caliber_normalized        AS cal,
            l.bullet_type               AS bt,
            l.case_material             AS cm
        FROM price_history ph
        JOIN listings l ON l.id = ph.listing_id
        WHERE ph.recorded_at >= p_since
          AND ph.price_per_round IS NOT NULL
          AND ph.price_per_round::numeric > 0
          AND (p_condition IS NULL OR l.condition_type = p_condition)
          AND NOT l.is_component
          AND l.caliber_normalized IS NOT NULL
          AND l.bullet_type        IS NOT NULL
          AND l.case_material      IS NOT NULL
    ),
    ph_floor AS (
        SELECT pa.*
        FROM ph_avg pa
        LEFT JOIN caliber_floors cf ON cf.cal = pa.cal
        WHERE pa.ppr >= COALESCE(cf.floor_ppr, p_price_floor)
    ),
    listing_min AS (
        SELECT listing_id, cal, bt, cm, MIN(ppr) AS listing_min_ppr
        FROM ph_floor
        GROUP BY listing_id, cal, bt, cm
    ),
    seg_counts AS (
        SELECT cal, bt, cm, COUNT(*)::int AS listings_in_segment
        FROM listing_min
        GROUP BY cal, bt, cm
    ),
    listing_min_ranked AS (
        SELECT
            cal, bt, cm, listing_min_ppr,
            ROW_NUMBER() OVER (
                PARTITION BY cal, bt, cm
                ORDER BY listing_min_ppr ASC
            ) AS rn
        FROM listing_min
    ),
    seg_floor AS (
        SELECT
            lmr.cal, lmr.bt, lmr.cm,
            AVG(lmr.listing_min_ppr) FILTER (
                WHERE lmr.rn <= CASE
                    WHEN sc.listings_in_segment >= p_floor_full_threshold
                        THEN p_floor_full_n
                    ELSE p_floor_small_n
                END
            ) AS segment_floor
        FROM listing_min_ranked lmr
        JOIN seg_counts sc USING (cal, bt, cm)
        GROUP BY lmr.cal, lmr.bt, lmr.cm
    )
    SELECT
        sc.cal AS caliber_normalized,
        sc.bt  AS bullet_type,
        sc.cm  AS case_material,
        sf.segment_floor,
        sc.listings_in_segment AS listing_count
    FROM seg_counts sc
    LEFT JOIN seg_floor sf USING (cal, bt, cm)
    WHERE sc.listings_in_segment >= p_min_listings;
$$;

-- Belt-and-braces: 014 relied on default privileges for anon execution;
-- re-grant explicitly so the DROP+CREATE can never regress the fallback
-- path if default privileges ever drift.
GRANT EXECUTE ON FUNCTION public.homepage_segment_aggregates(
    TIMESTAMPTZ, TEXT, INT, INT, INT, INT, NUMERIC, NUMERIC)
    TO anon, authenticated, service_role;

COMMENT ON FUNCTION public.homepage_segment_aggregates(
    TIMESTAMPTZ, TEXT, INT, INT, INT, INT, NUMERIC, NUMERIC
) IS
    'Per-(caliber,bullet,case) segment_floor over a 30-day window, mirroring '
    'computeSegmentFloorAverages in lib/pricing.js. Excludes reloading '
    'components (029). Price floor is PER-CALIBER since 033 (caliber_floors '
    'VALUES table inside the body — MUST mirror lib/priceBounds.js '
    'PER_CALIBER_FLOOR); p_price_floor is the fallback for unmapped calibers. '
    'segment_avg dropped in 040 (retired 2026-05-30, read by nothing); '
    'p_outlier_mult is an accepted-and-ignored compat param. Feeds PriceDelta, '
    'the EditorialTicker, and the MarketFloorHero week-delta via '
    'homepage_segment_aggregates_cache. Bible: never prey on the ignorant — '
    'the floor must reflect buyable loaded ammo, including legit 6c/rd bulk 22lr.';
