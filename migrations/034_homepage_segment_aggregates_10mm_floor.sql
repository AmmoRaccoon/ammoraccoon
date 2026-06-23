-- 034_homepage_segment_aggregates_10mm_floor.sql
-- Adds the 10mm Auto per-caliber price floor (0.20) to homepage_segment_aggregates().
--
-- WHY: registry expansion staged 10mm (calibers.json + market_bounds_order
-- append-last) and regenerated migrations/gen/caliber_floors.values.sql with a new
-- ('10mm', 0.20) row. Per the gen-file contract that block is never applied
-- directly -- it reconciles to the live DB through THIS numbered migration. 10mm's
-- 0.20 floor mirrors lib/priceBounds.js PER_CALIBER_FLOOR (regenerated alongside).
--
-- HOW: 033's body verbatim with ONE row appended to the caliber_floors VALUES
-- table (('10mm', 0.20)). Function signature UNCHANGED (8 params); every existing
-- floor row byte-identical; ph_floor logic untouched.
--
-- ADDITIVE / ZERO LIVE EFFECT TODAY: 10mm is staged (NOT in detect_priority), so
-- there are 0 listings with caliber_normalized='10mm' -- applying this changes the
-- function's OUTPUT by exactly nothing today; it pre-stages the floor so the eventual
-- 10mm activation needs no DB migration (the .45 ACP template).
-- refresh_homepage_segment_aggregates_cache() (031/032) needs NO change (signature
-- unchanged; p_price_floor=0.10 fallback passes through).
--
-- Idempotent: CREATE OR REPLACE; re-running is a no-op replace.
-- Revert: re-run migration 033's body verbatim.

CREATE OR REPLACE FUNCTION public.homepage_segment_aggregates(
    p_since                TIMESTAMPTZ,
    p_condition            TEXT     DEFAULT 'New',
    p_min_listings         INT      DEFAULT 5,
    p_floor_full_n         INT      DEFAULT 10,
    p_floor_small_n        INT      DEFAULT 3,
    p_floor_full_threshold INT      DEFAULT 20,
    p_outlier_mult         NUMERIC  DEFAULT 5.0,
    p_price_floor          NUMERIC  DEFAULT 0.10
) RETURNS TABLE (
    caliber_normalized TEXT,
    bullet_type        TEXT,
    case_material      TEXT,
    segment_avg        NUMERIC,
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
            ('10mm',     0.20)
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
    seg_median AS (
        SELECT
            cal, bt, cm,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY ppr) AS median_ppr
        FROM ph_avg
        GROUP BY cal, bt, cm
    ),
    seg_avg AS (
        SELECT
            ph.cal, ph.bt, ph.cm,
            AVG(ph.ppr) FILTER (
                WHERE ph.ppr BETWEEN sm.median_ppr / p_outlier_mult
                                 AND sm.median_ppr * p_outlier_mult
            ) AS segment_avg
        FROM ph_avg ph
        JOIN seg_median sm USING (cal, bt, cm)
        GROUP BY ph.cal, ph.bt, ph.cm
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
        sa.segment_avg,
        sf.segment_floor,
        sc.listings_in_segment AS listing_count
    FROM seg_counts sc
    LEFT JOIN seg_avg   sa USING (cal, bt, cm)
    LEFT JOIN seg_floor sf USING (cal, bt, cm)
    WHERE sc.listings_in_segment >= p_min_listings;
$$;

COMMENT ON FUNCTION public.homepage_segment_aggregates(
    TIMESTAMPTZ, TEXT, INT, INT, INT, INT, NUMERIC, NUMERIC
) IS
    'Per-(caliber,bullet,case) segment_avg + segment_floor over a 30-day window, '
    'mirroring computeSegmentFloorAverages in lib/pricing.js. Excludes reloading '
    'components (029). As of 033 the price floor is PER-CALIBER (caliber_floors '
    'VALUES table inside the body — MUST mirror lib/priceBounds.js '
    'PER_CALIBER_FLOOR); p_price_floor is the fallback for unmapped calibers. '
    'Feeds PriceDelta, the EditorialTicker, and the MarketFloorHero week-delta '
    'via homepage_segment_aggregates_cache. Bible: never prey on the ignorant — '
    'the floor must reflect buyable loaded ammo, including legit 6c/rd bulk 22lr.';
