-- 039_history_daily_floor_text_value.sql
-- Transport-precision fix for history_daily_floor (migration 038) — the
-- Phase B parity run stopped on exactly this: the SQL arithmetic was proven
-- BIT-EXACT against the client (read-only probe 2026-07-08:
-- `value = 0.24153333333333335::float8` → true inside the DB), but this
-- database's default `extra_float_digits = 0` truncates float8 → JSON to 15
-- significant digits on the wire, so the client received 0.241533333333333
-- instead of the exact double. See
-- ammoraccoon-web/reports/history-rpc-phase-a-2026-07-08.md (Phase B section).
--
-- THE FIX (transport only — zero arithmetic change): `value` becomes TEXT,
-- serialized INSIDE the function where a function-level
-- `SET extra_float_digits = 3` applies. PG then emits the 17-digit
-- round-trip form (Ryu shortest-exact), and the web data source
-- parseFloat()s it back to the IDENTICAL float64 the client path computes.
-- Every other column, every filter, every CTE, and the entire float8
-- arithmetic pipeline are byte-identical to migration 038's body.
--
-- Return-type changes require DROP + CREATE (CREATE OR REPLACE cannot alter
-- an existing function's return type). DROP IF EXISTS keeps re-runs safe.
--
-- ⚠️  Applied only with Jon's explicit chat approval (given 2026-07-08).

DROP FUNCTION IF EXISTS public.history_daily_floor(
  text, timestamptz, float8, float8, timestamptz, text, text, text, integer,
  jsonb, boolean, float8, text, text, text[], text[], text[], float8, text,
  jsonb, integer);

CREATE FUNCTION public.history_daily_floor(
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
) RETURNS TABLE (series_key text, day date, value text, pool_size integer)
LANGUAGE plpgsql STABLE
SET extra_float_digits = 3
AS $$
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
         AND sc.price_per_round::float8 <> 'NaN'::float8   -- see 038 NaN note
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
  -- The ONLY change from migration 038: the final ::text serialization,
  -- which runs inside this function under extra_float_digits = 3 and emits
  -- the 17-digit round-trip form — parseFloat() on the client recovers the
  -- exact float64.
  SELECT a.skey,
         (DATE '1970-01-01' + a.day_num::int) AS day,
         ((CASE cardinality(a.vals)
             WHEN 1 THEN a.vals[1]
             WHEN 2 THEN (a.vals[1] + a.vals[2]) / 2.0::float8
             ELSE ((a.vals[1] + a.vals[2]) + a.vals[3]) / 3.0::float8
           END))::text AS value,
         COALESCE(pc.pool_n, 0) AS pool_size
    FROM agg a
    LEFT JOIN pool_counts pc ON pc.skey = a.skey
   ORDER BY a.skey, a.day_num;
END;
$$;

GRANT EXECUTE ON FUNCTION public.history_daily_floor(text, timestamptz, float8, float8, timestamptz, text, text, text, integer, jsonb, boolean, float8, text, text, text[], text[], text[], float8, text, jsonb, integer) TO anon, authenticated;

-- Return type changed — PostgREST must refresh its schema cache.
NOTIFY pgrst, 'reload schema';
