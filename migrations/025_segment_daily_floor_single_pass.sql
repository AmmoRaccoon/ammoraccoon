-- 025_segment_daily_floor_single_pass.sql
-- Captures the performance rewrite of segment_daily_floor() applied to
-- production manually by Jon on 2026-05-24 via the Supabase SQL editor.
-- Replaces the migration-023 body. Same inputs, same outputs (proven
-- bit-for-bit by scripts/smoke-segment-daily-floor-parity.mjs in the web
-- repo: zero drift across honest and LOCF buckets on every segment
-- tested), but a fundamentally faster query shape.
--
-- WHY: migration 023 ran a correlated subquery once per (bucket x
-- listing) — O(buckets x pool) — which timed out (PG 57014) on dense
-- calibers. This version is a single O(rows) pass: scan the pool's
-- price_history once, snap each row to its bucket, take the latest row
-- per (listing, bucket), then cheapest-N mean + LOCF over the grid. With
-- the covering index (migration 026) the scan is index-only and the
-- densest caliber (9mm/Brass, ~2049 listings) returns in ~2s.
--
-- EQUIVALENCE INVARIANT: the single-pass binning is bit-for-bit identical
-- to the chart's alive-at-T algorithm ONLY when bucket size == freshness
-- window. Then each scrape maps to exactly one bucket and "latest row in
-- (T-fresh, T]" == "latest row whose bucket == T". The hero always calls
-- with p_bucket_hours = p_fresh_hours = 6. A mismatched call RAISEs
-- rather than silently returning a wrong line. (The /history chart uses
-- 2h cadence + 6h fresh — overlapping windows — so it keeps its own
-- client-side reducer; this RPC is the hero sparkline's path only.)
--
-- INDEX-USAGE NOTE: the date bounds are hoisted into plpgsql locals
-- (v_grid_start etc.) rather than derived in a CROSS JOIN. Postgres can
-- push locals into an index range scan; a CROSS JOIN expression it
-- cannot, and an earlier draft seq-scanned the whole table as a result.
--
-- Idempotency: CREATE OR REPLACE is safe to re-run.

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
LANGUAGE plpgsql STABLE
AS $$
#variable_conflict use_column
DECLARE
  v_grid_end   TIMESTAMPTZ := date_trunc('hour', now());
  v_grid_start TIMESTAMPTZ := date_trunc('hour', now()) - (p_days || ' days')::interval;
  v_step       INTERVAL    := (p_bucket_hours || ' hours')::interval;
  v_fresh      INTERVAL    := (p_fresh_hours  || ' hours')::interval;
BEGIN
  IF p_fresh_hours <> p_bucket_hours THEN
    RAISE EXCEPTION
      'segment_daily_floor requires p_fresh_hours = p_bucket_hours (got fresh=%, bucket=%)',
      p_fresh_hours, p_bucket_hours;
  END IF;

  RETURN QUERY
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
    SELECT generate_series(v_grid_start, v_grid_end, v_step) AS b_ts
  ),
  binned AS (
    SELECT
      ph.listing_id,
      ph.price_per_round,
      ph.recorded_at,
      CASE
        WHEN date_bin(v_step, ph.recorded_at, v_grid_start) = ph.recorded_at
          THEN ph.recorded_at
        ELSE date_bin(v_step, ph.recorded_at, v_grid_start) + v_step
      END AS bkt
      FROM price_history ph
     WHERE ph.listing_id IN (SELECT id FROM pool)
       AND ph.recorded_at >  v_grid_start - v_fresh
       AND ph.recorded_at <= v_grid_end
       AND ph.price_per_round >= p_price_floor
  ),
  latest AS (
    SELECT DISTINCT ON (listing_id, bkt)
           listing_id, bkt, price_per_round
      FROM binned
     ORDER BY listing_id, bkt, recorded_at DESC
  ),
  ranked AS (
    SELECT bkt, price_per_round,
           ROW_NUMBER() OVER (PARTITION BY bkt ORDER BY price_per_round ASC) AS rnk
      FROM latest
  ),
  per_bucket AS (
    SELECT bkt,
           COUNT(*)::BIGINT AS a_cnt,
           AVG(price_per_round) FILTER (WHERE rnk <= p_cheapest_n) AS raw_floor
      FROM ranked
     GROUP BY bkt
  ),
  grid AS (
    SELECT b.b_ts,
           COALESCE(pb.a_cnt, 0)::BIGINT AS a_cnt,
           pb.raw_floor
      FROM buckets b
      LEFT JOIN per_bucket pb ON pb.bkt = b.b_ts
  ),
  gated AS (
    SELECT g.b_ts,
           g.a_cnt,
           CASE WHEN g.a_cnt >= pm.min_alive THEN g.raw_floor ELSE NULL END AS gated_floor,
           pm.n AS pool_n
      FROM grid g CROSS JOIN pool_meta pm
  ),
  locf AS (
    SELECT b_ts, a_cnt, gated_floor, pool_n,
           SUM(CASE WHEN gated_floor IS NOT NULL THEN 1 ELSE 0 END)
             OVER (ORDER BY b_ts ROWS UNBOUNDED PRECEDING) AS grp
      FROM gated
  )
  SELECT
    l.b_ts,
    FIRST_VALUE(l.gated_floor) OVER (PARTITION BY l.grp ORDER BY l.b_ts),
    l.a_cnt,
    l.pool_n
    FROM locf l
   WHERE l.grp > 0
     AND l.pool_n >= p_min_pool
   ORDER BY l.b_ts;
END;
$$;
