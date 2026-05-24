-- 023_segment_daily_floor_rpc.sql
-- Captures the segment_daily_floor() RPC added to production manually by
-- Jon on 2026-05-24 via the Supabase SQL editor, so the migrations
-- directory stays the source of truth for schema state.
--
-- What it powers: the Market Floor hero sparkline on the ammoraccoon-web
-- home page (components/MarketFloorHero.js via lib/pricing.js
-- fetchSegmentDailyFloor). Returns a 30-day daily-floor BASE cpr series
-- for one (caliber, case) segment: the mean of the cheapest-N in-stock
-- listings at each time bucket, with LOCF carry-forward through sparse
-- buckets. This is the same alive-at-T algorithm the /history chart
-- computes client-side (app/history/HistoryClient.js buildPointsFromRows),
-- ported to SQL and proven bit-for-bit identical by
-- scripts/smoke-segment-daily-floor-parity.mjs in the web repo.
--
-- !!! SUPERSEDED FOR PERFORMANCE by migration 025. This original body
-- runs a correlated subquery once per (bucket x listing) — O(buckets x
-- pool) — which times out (PG 57014) on dense calibers (9mm/Brass is
-- ~2049 listings x 121 buckets ~ 248k lookups). Migration 025 replaces
-- the body with an O(rows) single-pass rewrite that returns identical
-- values. A clean rebuild replays 023 then 025 and ends at the fast
-- version; 023 is kept for lineage.
--
-- Idempotency: CREATE OR REPLACE is safe to re-run.

BEGIN;

CREATE OR REPLACE FUNCTION segment_daily_floor(
  p_caliber TEXT,
  p_case TEXT,
  p_days INT DEFAULT 30,
  p_condition TEXT DEFAULT 'New',
  p_bucket_hours INT DEFAULT 6,
  p_min_alive_fraction NUMERIC DEFAULT 0.25,
  p_fresh_hours INT DEFAULT 6,
  p_cheapest_n INT DEFAULT 3,
  p_price_floor NUMERIC DEFAULT 0.05,
  p_min_pool INT DEFAULT 5
)
RETURNS TABLE (
  day_bucket TIMESTAMPTZ,
  floor_cpr NUMERIC,
  alive_count BIGINT,
  pool_size BIGINT
)
LANGUAGE SQL STABLE
AS $$
  WITH pool AS (
    SELECT id
      FROM listings
     WHERE caliber_normalized = p_caliber
       AND case_material = p_case
       AND (p_condition IS NULL OR condition_type = p_condition)
       AND in_stock = TRUE
  ),
  pool_meta AS (
    SELECT COUNT(*)::BIGINT AS n,
           GREATEST(3, CEIL(COUNT(*) * p_min_alive_fraction))::INT AS min_alive
      FROM pool
  ),
  buckets AS (
    SELECT generate_series(
      date_trunc('hour', now()) - (p_days || ' days')::interval,
      date_trunc('hour', now()),
      (p_bucket_hours || ' hours')::interval
    ) AS t
  ),
  alive AS (
    SELECT
      b.t,
      p.id AS listing_id,
      (
        SELECT ph.price_per_round
          FROM price_history ph
         WHERE ph.listing_id = p.id
           AND ph.recorded_at <= b.t
           AND ph.recorded_at >= b.t - (p_fresh_hours || ' hours')::interval
           AND ph.price_per_round >= p_price_floor
         ORDER BY ph.recorded_at DESC
         LIMIT 1
      ) AS price
      FROM buckets b
      CROSS JOIN pool p
  ),
  ranked AS (
    SELECT t, price,
           ROW_NUMBER() OVER (PARTITION BY t ORDER BY price ASC) AS rnk
      FROM alive
     WHERE price IS NOT NULL
  ),
  per_bucket AS (
    SELECT
      b.t AS day_bucket,
      COALESCE((SELECT COUNT(*) FROM ranked r WHERE r.t = b.t), 0)::BIGINT AS alive_count,
      (SELECT AVG(price) FROM ranked r WHERE r.t = b.t AND r.rnk <= p_cheapest_n) AS raw_floor
      FROM buckets b
  ),
  gated AS (
    SELECT pb.day_bucket,
           pb.alive_count,
           CASE WHEN pb.alive_count >= pc.min_alive THEN pb.raw_floor ELSE NULL END AS gated_floor,
           pc.n AS pool_n
      FROM per_bucket pb CROSS JOIN pool_meta pc
  ),
  locf AS (
    SELECT day_bucket, alive_count, gated_floor, pool_n,
           SUM(CASE WHEN gated_floor IS NOT NULL THEN 1 ELSE 0 END)
             OVER (ORDER BY day_bucket ROWS UNBOUNDED PRECEDING) AS grp
      FROM gated
  )
  SELECT
    l.day_bucket,
    FIRST_VALUE(l.gated_floor) OVER (PARTITION BY l.grp ORDER BY l.day_bucket) AS floor_cpr,
    l.alive_count,
    l.pool_n AS pool_size
    FROM locf l
   WHERE l.grp > 0
     AND l.pool_n >= p_min_pool
   ORDER BY l.day_bucket;
$$;

COMMIT;
