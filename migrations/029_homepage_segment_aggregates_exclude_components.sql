-- 029_homepage_segment_aggregates_exclude_components.sql
-- STEP 3 of the is_component rollout — THIS is where the fix activates on the
-- live site. Adds `AND NOT l.is_component` to homepage_segment_aggregates so
-- reloading-component rows (bullets / empty brass) no longer contaminate the
-- 30-day segment_avg / segment_floor that feed PriceDelta, the ticker, and the
-- MarketFloorHero week-delta (web repo). See ammoraccoon-web DECISIONS.md
-- 2026-05-24 (Option B).
--
-- Identical to migration 014 EXCEPT for one added predicate in the ph_avg CTE.
-- Because ph_floor (and everything downstream) derives from ph_avg, filtering
-- once there cleans BOTH the trimmed-mean (segment_avg) and the cheapest-N
-- floor (segment_floor) branches.
--
-- DROP-IN / REVERSIBLE:
--   * Signature is UNCHANGED (same 8 params), so CREATE OR REPLACE swaps the
--     body with no DROP — the warm-rpc cron and every existing caller keep
--     working untouched.
--   * is_component is NOT NULL DEFAULT false, so `NOT l.is_component` is
--     two-valued (no NULL surprises): false rows pass, true rows are excluded.
--   * To revert: re-run migration 014's body verbatim (CREATE OR REPLACE back).
--   * Idempotent: re-running is a no-op replace.
--
-- EXPECTED EFFECT: floors RISE in segments that contained cheap component
-- rows — 9mm/JHP/Brass and 357mag/JHP/Brass specifically (the XTP/HAP/Barnes
-- bullets live there). 9mm/FMJ/Brass has ZERO flagged components and will NOT
-- move — verify against the JHP segments, not FMJ.

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
    WITH ph_avg AS (
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
        SELECT * FROM ph_avg WHERE ppr >= p_price_floor
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
    'mirroring computeSegmentAverages / computeSegmentFloorAverages in '
    'lib/pricing.js. As of migration 029, EXCLUDES reloading components via '
    'WHERE NOT l.is_component (set by the isLikelyComponent backfill + scrapers) '
    'so bullets/brass no longer drag the floor/avg. Feeds PriceDelta, the '
    'EditorialTicker, and the MarketFloorHero week-delta. Bible: never prey on '
    'the ignorant — the floor must reflect buyable loaded ammo.';
