-- 033_homepage_segment_aggregates_percaliber_floor.sql
-- Per-caliber price floors inside homepage_segment_aggregates(), mirroring
-- lib/priceBounds.js PER_CALIBER_FLOOR in the web repo (approved table,
-- 2026-06-10).
--
-- WHY: the web's /history chart and homepage floor query moved to
-- per-caliber floors (web commit 30b237c) because the flat 10¢ floor was
-- clipping 32% of the live .22 LR market (legitimate bulk at ~6¢/rd).
-- This RPC — which feeds the PriceDelta badge baselines via the
-- homepage_segment_aggregates_cache — still filtered at the flat
-- p_price_floor=0.10, so the site disagreed with itself on .22 LR:
-- chart said ~6¢, badge baselines said ~10¢. Measured before this
-- migration: every big 22lr segment floor sat pinned at 0.1003-0.1015,
-- the signature of a truncated distribution. Expected after: 22lr floors
-- drop into the ~6-7¢ regime; 9mm (floor unchanged at 0.10) must not move.
--
-- HOW: identical to migration 029's body EXCEPT
--   1. a new 14-row caliber_floors VALUES table, and
--   2. ph_floor now keeps rows where ppr >= COALESCE(per-caliber floor,
--      p_price_floor) — exactly the JS rule
--      `PER_CALIBER_FLOOR[caliber] ?? SANE_PRICE_FLOOR`.
-- The signature is UNCHANGED (8 params; p_price_floor is now the
-- FALLBACK floor for calibers absent from the table). Because the
-- signature is unchanged, refresh_homepage_segment_aggregates_cache()
-- (migrations 031/032) needs NO change — it passes p_price_floor=0.10
-- through, which is still the correct fallback value.
--
-- PARAMETER-SYNC RULE (the 031 rule, extended): the caliber_floors
-- VALUES table below MUST mirror PER_CALIBER_FLOOR in
-- ammoraccoon-web/lib/priceBounds.js exactly, and p_price_floor's 0.10
-- default mirrors SANE_PRICE_FLOOR. If either changes in the web repo,
-- change it here in the same session — the cache and the live-RPC
-- fallback would otherwise compute different baselines than the chart.
--
-- Idempotent: CREATE OR REPLACE; re-running is a no-op replace.
-- Revert: re-run migration 029's body verbatim.

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
            ('12ga',     0.15)
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
          AND NOT l.is_component          -- 029: exclude reloading components at the source
          AND l.caliber_normalized IS NOT NULL
          AND l.bullet_type        IS NOT NULL
          AND l.case_material      IS NOT NULL
    ),
    ph_floor AS (
        -- 033: per-caliber floor, p_price_floor as fallback — the SQL twin
        -- of `PER_CALIBER_FLOOR[caliber] ?? SANE_PRICE_FLOOR`.
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
