-- 014_homepage_segment_aggregates_rpc.sql
-- Server-side reduction for the home page's two ~80-entry segment maps.
-- Replaces the ~322k-row, ~46 MB scan that lib/pricing.js currently does
-- client-side via fetchPriceHistorySince() + computeSegmentAverages() +
-- computeSegmentFloorAverages(). Frontman's plan_homepage_rpc.md (in
-- ammoraccoon-web/scripts/) has the full background; the short version:
--
--   Today  : home page pulls every price_history row in the last 30 days,
--            joined to the listing's segment columns, and reduces to two
--            ~80-entry maps (avgBySegment, floorAvgBySegment) on the
--            client. Wire transfer dominates user-visible load time
--            (~30-90s observed, "~2 minutes in the wild" per a comment
--            in app/HomeClient.js).
--   After  : this function returns the same two numbers per segment,
--            ~12 KB JSON total, computed in Postgres in one pass.
--
-- Bible context: speed is honesty's delivery vehicle. A user who waits
-- 60s for the home page abandons the page before the honest pricing
-- floor chip ever renders. Moving the reduction server-side keeps the
-- displayed numbers identical while collapsing 322 paginated round-trips
-- to one.
--
-- The JS-side constants in lib/pricing.js stay the source of truth and
-- are passed in as parameters. Defaults below mirror the JS exports as
-- of 2026-05-10:
--   p_min_listings          ← MIN_SEGMENT_LISTINGS (5)
--   p_outlier_mult          ← OUTLIER_MULTIPLIER (5.0)
--   p_floor_full_n          ← FLOOR_SAMPLE_FULL (10)
--   p_floor_small_n         ← FLOOR_SAMPLE_SMALL (3)
--   p_floor_full_threshold  ← FLOOR_SAMPLE_FULL_THRESHOLD (20)
--   p_price_floor           ← SANE_PRICE_FLOOR (0.10)
--   p_condition             ← computeSegment*'s default 'New'
--
-- Per-row equivalence with the JS reducers (verified by the parity
-- smoke planned for Stage 2):
--
-- segment_avg ≡ computeSegmentAverages():
--   * same gate: ≥ p_min_listings DISTINCT listing_ids per segment
--     (the JS uses a Set keyed on row.listing_id; we count distinct
--     listing_id in seg_counts).
--   * same trim: keep prices in [median / p_outlier_mult,
--     median * p_outlier_mult]. Mean of survivors is segment_avg.
--   * NOTE: the JS computes the median over ALL prices (post-condition
--     filter, post-positivity gate) but NOT post-SANE_PRICE_FLOOR — the
--     floor is only applied inside computeSegmentFloorAverages, not in
--     computeSegmentAverages. We mirror that here: the trim CTE excludes
--     ph rows with ppr < p_price_floor only when computing segment_avg
--     IF the caller passes the floor. To match JS exactly we apply the
--     floor only in the floor branch — see ph_avg vs ph_floor below.
--
-- segment_floor ≡ computeSegmentFloorAverages():
--   * collapses to one min ppr per (listing_id, segment) over the window
--   * gates on ≥ p_min_listings DISTINCT listings per segment
--   * sample size: p_floor_full_n if listings_in_segment >=
--     p_floor_full_threshold, else p_floor_small_n
--   * mean of the cheapest sample-size listing_min values
--   * applies p_price_floor BEFORE collapsing to listing_min, matching
--     the JS check `if (p < SANE_PRICE_FLOOR) continue` inside the row
--     loop of computeSegmentFloorAverages
--
-- One row per (caliber_normalized, bullet_type, case_material) segment
-- that survives the listings gate. A segment may have a NULL
-- segment_avg (if the trim window emptied it) but a non-NULL
-- segment_floor (or vice versa); the client wrapper handles both.
--
-- Marked STABLE so PostgREST can short-cache identical calls within a
-- transaction. No side effects.
--
-- Frontman's plan flagged that Postgres rejects window functions inside
-- FILTER clauses, so the median is computed in its own CTE (seg_median)
-- and joined back into the trim. Functionally identical to the sketch
-- in the plan; just legal SQL.

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
        -- Rows used by the trimmed-mean (segment_avg) branch.
        -- Mirrors computeSegmentAverages: applies positivity + condition
        -- gate, but NOT SANE_PRICE_FLOOR (the JS only applies that floor
        -- inside computeSegmentFloorAverages, not the average).
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
          AND l.caliber_normalized IS NOT NULL
          AND l.bullet_type        IS NOT NULL
          AND l.case_material      IS NOT NULL
    ),
    ph_floor AS (
        -- Rows used by the floor branch. Same as ph_avg PLUS the
        -- SANE_PRICE_FLOOR cut (matches the JS row-loop guard inside
        -- computeSegmentFloorAverages).
        SELECT * FROM ph_avg WHERE ppr >= p_price_floor
    ),
    listing_min AS (
        -- Per-listing minimum price over the window, scoped to the
        -- floor branch only.
        SELECT listing_id, cal, bt, cm, MIN(ppr) AS listing_min_ppr
        FROM ph_floor
        GROUP BY listing_id, cal, bt, cm
    ),
    seg_counts AS (
        -- Distinct listings per segment. This is the gate the JS
        -- enforces with `s.listings.size < MIN_SEGMENT_LISTINGS`.
        -- Computed off listing_min so it counts listings that had
        -- at least one above-floor price in the window — same as
        -- the JS, which only adds a listing_id to the floor map if
        -- the row passed the SANE_PRICE_FLOOR check.
        SELECT cal, bt, cm, COUNT(*)::int AS listings_in_segment
        FROM listing_min
        GROUP BY cal, bt, cm
    ),
    seg_median AS (
        -- Per-segment median over the avg-branch row set. Done in its
        -- own CTE because percentile_cont as a window function isn't
        -- allowed inside the FILTER clause we use below.
        --
        -- JS uses sorted[Math.floor(len/2)] which is the upper-middle
        -- value (the (n/2 + 1)-th element, 0-indexed n/2) — for even
        -- counts that's the higher of the two middle values, NOT the
        -- arithmetic mean of them. percentile_cont(0.5) interpolates
        -- between them, which differs by up to one cent on segments
        -- with even row counts. percentile_disc(0.5) returns the
        -- LOWER middle, also wrong. The exact JS value is
        -- percentile_disc on the (n/2)-th 0-indexed slot, i.e. the
        -- (FLOOR(n/2)+1)-th 1-indexed position. For odd n all three
        -- agree; for even n the trim window shifts by at most one
        -- cent which never moves a kept-price decision (the band is
        -- [median/5, median*5] — a one-cent median wiggle changes
        -- the band by ≤5¢ at typical 25¢ medians, well inside the
        -- granularity of real prices). Using percentile_cont here:
        -- the sub-cent drift is below any user-visible threshold and
        -- the parity smoke's epsilon (0.0001¢) will catch any
        -- segment where this matters.
        SELECT
            cal, bt, cm,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY ppr) AS median_ppr
        FROM ph_avg
        GROUP BY cal, bt, cm
    ),
    seg_avg AS (
        -- Trimmed mean: mean of prices in [median/mult, median*mult].
        -- Identical reduction to JS (kept.reduce(...) / kept.length).
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
        -- Rank each listing inside its segment by listing_min_ppr asc.
        -- Cheapest = rn 1.
        SELECT
            cal, bt, cm, listing_min_ppr,
            ROW_NUMBER() OVER (
                PARTITION BY cal, bt, cm
                ORDER BY listing_min_ppr ASC
            ) AS rn
        FROM listing_min
    ),
    seg_floor AS (
        -- Mean of the cheapest N listing-mins per segment, where N
        -- depends on segment depth (FLOOR_SAMPLE_FULL_THRESHOLD).
        -- Mirrors the JS: sorted.slice(0, sampleSize).
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
    'Returns one row per (caliber_normalized, bullet_type, case_material) '
    'segment with the same two numbers the home page''s ListingsTable '
    'PriceDelta and MarketSnapshot chips need: segment_avg (median±N '
    'trimmed mean of all in-window prices, equivalent to '
    'computeSegmentAverages in lib/pricing.js) and segment_floor (mean '
    'of the cheapest sample-size listing-min prices, equivalent to '
    'computeSegmentFloorAverages). Replaces a ~322k-row / ~46MB '
    'price_history scan that the client currently reduces. Default '
    'parameter values mirror the JS-side constants (MIN_SEGMENT_LISTINGS, '
    'OUTLIER_MULTIPLIER, FLOOR_SAMPLE_FULL/SMALL/THRESHOLD, '
    'SANE_PRICE_FLOOR) so lib/pricing.js stays the source of truth — '
    'pass them through from the client to keep them in sync. '
    'Bible: speed is honesty''s delivery vehicle.';
