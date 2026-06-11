-- 032_refresh_cache_delete_where_true.sql
-- Fix refresh_homepage_segment_aggregates_cache(): the bare
-- `DELETE FROM ... ;` in migration 031 is rejected with
-- `DELETE requires a WHERE clause` (SQLSTATE 21000) when the function is
-- called through the Supabase API (PostgREST sessions load the
-- pg-safeupdate guard; the dashboard SQL editor does not, which is why
-- 031's seed run on 2026-06-10 succeeded while every scrape_light cron
-- refresh since has failed all 3 attempts). Observed in the
-- "Refresh homepage segment aggregates cache" step logs of every
-- scrape_light run on 2026-06-10; the step is continue-on-error so the
-- workflow stayed green while the cache silently froze at its seed
-- timestamp.
--
-- FIX: `WHERE true` — semantically identical full-table delete that
-- satisfies the safeupdate clause-presence check.
--
-- Deliberately NOT TRUNCATE: TRUNCATE takes an ACCESS EXCLUSIVE lock and
-- is not MVCC-safe for concurrent readers, so homepage reads of the cache
-- (every page view) would block behind the refresh transaction — which
-- holds the ~2.5-3s homepage_segment_aggregates() call — and a reader
-- crossing the truncate could see an empty table. DELETE keeps 031's
-- stated guarantee: readers see the old rows until the refresh commits,
-- never a half-rebuilt or empty cache.
--
-- Everything else (signature, defaults, parameter sync with
-- lib/pricing.js constants, SECURITY INVOKER) is unchanged from 031.
-- CREATE OR REPLACE retains the function's existing ACL, but the REVOKE
-- is restated for the fresh-create path (a clean rebuild that somehow
-- runs 032 without 031 would otherwise leave PUBLIC execute in place).
--
-- Idempotent: CREATE OR REPLACE; re-running is a no-op in effect.

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
    DELETE FROM public.homepage_segment_aggregates_cache WHERE true;
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
