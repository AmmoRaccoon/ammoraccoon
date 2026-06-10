-- 031_homepage_segment_aggregates_cache.sql
-- Precompute table for homepage_segment_aggregates() + its refresh function.
--
-- WHY: the RPC's true runtime sits at ~2.4-2.9s warm against the post-dedup
-- ~450k-row 30-day window (measured 2026-06-10) — right at the anon role's
-- statement_timeout, so live page loads intermittently lose the PriceDelta
-- "vs 30-day floor" badges (57014). Badges are 30-day comparisons, not live
-- prices, so stale-by-one-scrape-cycle is explicitly acceptable (Jon,
-- 2026-06-10). Precomputing makes the homepage read O(95 rows) regardless
-- of price_history size and immune to future table growth.
--
-- HOW IT RUNS: scripts/refresh_segment_aggregates_cache.py (service role)
-- calls refresh_homepage_segment_aggregates_cache() at the end of every
-- scrape_light.yml cron tick (~every 2h as delivered), so the cache follows
-- each scrape wave. The web reads the table first and falls back to the
-- live RPC when the table is missing, empty, or stale >24h
-- (lib/pricing.js fetchSegmentAggregates).
--
-- PARAMETER SYNC: the refresh function's defaults MIRROR the JS constants
-- in lib/pricing.js (MIN_SEGMENT_LISTINGS=5, FLOOR_SAMPLE_FULL=10,
-- FLOOR_SAMPLE_SMALL=3, FLOOR_SAMPLE_FULL_THRESHOLD=20,
-- OUTLIER_MULTIPLIER=5, SANE_PRICE_FLOOR=0.10, condition='New', 30-day
-- window) exactly as migration 014's defaults do. If those JS constants
-- ever change, change the defaults here too — the live-RPC fallback path
-- would otherwise compute with different knobs than the cache.
--
-- segment_avg is intentionally NOT cached: retired by reducer-divergence
-- Option A (DECISIONS.md 2026-05-30); nothing reads it.
--
-- Idempotent: IF NOT EXISTS / OR REPLACE / drop-then-create policy.
-- APPLY VIA DASHBOARD SQL EDITOR (the migration runner is not yet
-- connected to a direct-Postgres connection).

CREATE TABLE IF NOT EXISTS public.homepage_segment_aggregates_cache (
    caliber_normalized TEXT NOT NULL,
    bullet_type        TEXT NOT NULL,
    case_material      TEXT NOT NULL,
    segment_floor      NUMERIC,
    listing_count      INT,
    refreshed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (caliber_normalized, bullet_type, case_material)
);

-- RLS: public read-only catalog shape (Class A, matching migration 022).
-- Writes happen only via service_role (bypasses RLS) from the refresh job.
ALTER TABLE public.homepage_segment_aggregates_cache ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS anon_select_homepage_segment_aggregates_cache
    ON public.homepage_segment_aggregates_cache;
CREATE POLICY anon_select_homepage_segment_aggregates_cache
    ON public.homepage_segment_aggregates_cache
    FOR SELECT USING (true);

-- Atomic refresh: rebuild the cache from the live RPC in one transaction
-- (a reader never sees a half-rebuilt table). Returns the new row count.
-- SECURITY INVOKER (default): anon executing this would fail on the
-- DELETE/INSERT (no write policies) — only service_role can refresh.
CREATE OR REPLACE FUNCTION public.refresh_homepage_segment_aggregates_cache(
    p_since                TIMESTAMPTZ DEFAULT now() - interval '30 days',
    p_condition            TEXT        DEFAULT 'New',
    p_min_listings         INT         DEFAULT 5,
    p_floor_full_n         INT         DEFAULT 10,
    p_floor_small_n        INT         DEFAULT 3,
    p_floor_full_threshold INT         DEFAULT 20,
    p_outlier_mult         NUMERIC     DEFAULT 5.0,
    p_price_floor          NUMERIC     DEFAULT 0.10
) RETURNS INT
LANGUAGE sql VOLATILE
AS $$
    DELETE FROM public.homepage_segment_aggregates_cache;
    INSERT INTO public.homepage_segment_aggregates_cache
        (caliber_normalized, bullet_type, case_material,
         segment_floor, listing_count, refreshed_at)
    SELECT caliber_normalized, bullet_type, case_material,
           segment_floor, listing_count, now()
      FROM public.homepage_segment_aggregates(
           p_since, p_condition, p_min_listings, p_floor_full_n,
           p_floor_small_n, p_floor_full_threshold, p_outlier_mult,
           p_price_floor);
    SELECT COUNT(*)::int FROM public.homepage_segment_aggregates_cache;
$$;

REVOKE EXECUTE ON FUNCTION public.refresh_homepage_segment_aggregates_cache(
    TIMESTAMPTZ, TEXT, INT, INT, INT, INT, NUMERIC, NUMERIC)
    FROM PUBLIC, anon, authenticated;

COMMENT ON TABLE public.homepage_segment_aggregates_cache IS
    'Precomputed homepage_segment_aggregates() output (segment_floor per '
    '(caliber, bullet, case) segment). Refreshed by '
    'scripts/refresh_segment_aggregates_cache.py on the scrape_light cron; '
    'read by lib/pricing.js fetchSegmentAggregates with live-RPC fallback. '
    'Stale-by-one-cycle is by design — these are 30-day comparison '
    'baselines, not live prices.';
