-- migrations/022_rls_lockdown.sql
--
-- Lock down RLS on the 13 public-schema tables enumerated by the web repo's
-- 2026-05-16 audit (see reports/rls-audit-2026-05-16.md in ammoraccoon-web).
--
-- Trigger: a write probe on manufacturer_ballistics succeeded with the anon
-- key, meaning the moat data was openly writable from any browser running
-- ammoraccoon.com. This migration closes that hole and twelve sibling holes
-- that were unverified-but-suspect by the same audit.
--
-- Three policy classes:
--   A. Public read-only catalog (11 tables): anon SELECT, no anon writes.
--   B. User submission (2 tables):           anon INSERT only, no anon SELECT.
--   service_role bypasses RLS by default → scrapers and Backman keep working
--   unchanged. No service_role policies are touched.
--
-- Idempotent + atomic: wraps in BEGIN/COMMIT and drops conflicting policies
-- first, so this can be re-run if anything needs to change.

BEGIN;

-- -------------------------------------------------------------------------
-- 0. Clear existing non-service_role policies on the 13 target tables.
--    Policies that target service_role only are preserved (they're no-ops
--    against RLS-bypassing service_role anyway, but leaving them avoids
--    surprising any operator who put them there intentionally).
-- -------------------------------------------------------------------------
DO $$
DECLARE
  pol record;
  target_tables text[] := ARRAY[
    'listings','price_history','retailers','coupons','rebates',
    'components','manufacturer_rebates','manufacturer_rebate_listing_matches',
    'manufacturer_rebate_eligible_products','manufacturer_ballistics',
    'manufacturer_ballistics_listing_matches',
    'retailer_requests','data_requests'
  ];
BEGIN
  FOR pol IN
    SELECT schemaname, tablename, policyname, roles
    FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename = ANY(target_tables)
      AND NOT (roles = ARRAY['service_role']::name[])
  LOOP
    EXECUTE format('DROP POLICY IF EXISTS %I ON %I.%I',
                   pol.policyname, pol.schemaname, pol.tablename);
    RAISE NOTICE 'dropped policy % on %.% (roles=%)',
                 pol.policyname, pol.schemaname, pol.tablename, pol.roles;
  END LOOP;
END $$;

-- -------------------------------------------------------------------------
-- CLASS A — Public read-only catalog (11 tables)
-- anon: SELECT only. INSERT/UPDATE/DELETE denied (no permissive policy).
-- -------------------------------------------------------------------------

ALTER TABLE public.listings ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_select_listings" ON public.listings
  FOR SELECT TO anon USING (true);

ALTER TABLE public.price_history ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_select_price_history" ON public.price_history
  FOR SELECT TO anon USING (true);

ALTER TABLE public.retailers ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_select_retailers" ON public.retailers
  FOR SELECT TO anon USING (true);

ALTER TABLE public.coupons ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_select_coupons" ON public.coupons
  FOR SELECT TO anon USING (true);

ALTER TABLE public.rebates ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_select_rebates" ON public.rebates
  FOR SELECT TO anon USING (true);

ALTER TABLE public.components ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_select_components" ON public.components
  FOR SELECT TO anon USING (true);

ALTER TABLE public.manufacturer_rebates ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_select_manufacturer_rebates" ON public.manufacturer_rebates
  FOR SELECT TO anon USING (true);

ALTER TABLE public.manufacturer_rebate_listing_matches ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_select_manufacturer_rebate_listing_matches"
  ON public.manufacturer_rebate_listing_matches
  FOR SELECT TO anon USING (true);

ALTER TABLE public.manufacturer_rebate_eligible_products ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_select_manufacturer_rebate_eligible_products"
  ON public.manufacturer_rebate_eligible_products
  FOR SELECT TO anon USING (true);

-- This is THE confirmed hole the audit found.
ALTER TABLE public.manufacturer_ballistics ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_select_manufacturer_ballistics"
  ON public.manufacturer_ballistics
  FOR SELECT TO anon USING (true);

ALTER TABLE public.manufacturer_ballistics_listing_matches ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_select_manufacturer_ballistics_listing_matches"
  ON public.manufacturer_ballistics_listing_matches
  FOR SELECT TO anon USING (true);

-- -------------------------------------------------------------------------
-- CLASS B — User submission, INSERT-only (2 tables)
-- anon: INSERT only. SELECT removed (submissions hold user contact info).
-- This also fixes the A-priority "Request ballistic data button RLS
-- violation" bug on data_requests as a side effect.
-- -------------------------------------------------------------------------

ALTER TABLE public.retailer_requests ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_insert_retailer_requests" ON public.retailer_requests
  FOR INSERT TO anon WITH CHECK (true);

ALTER TABLE public.data_requests ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_insert_data_requests" ON public.data_requests
  FOR INSERT TO anon WITH CHECK (true);

COMMIT;

-- -------------------------------------------------------------------------
-- POST-MIGRATION VERIFICATION (run separately in the SQL Editor; these
-- are NOT part of the transaction above)
-- -------------------------------------------------------------------------
--
-- 1. Confirm RLS is enabled on all 13 tables:
--    SELECT tablename, rowsecurity FROM pg_tables
--    WHERE schemaname='public' AND tablename IN (
--      'listings','price_history','retailers','coupons','rebates',
--      'components','manufacturer_rebates','manufacturer_rebate_listing_matches',
--      'manufacturer_rebate_eligible_products','manufacturer_ballistics',
--      'manufacturer_ballistics_listing_matches',
--      'retailer_requests','data_requests'
--    ) ORDER BY tablename;
--    Expect: rowsecurity=true for all 13.
--
-- 2. Confirm policy shape:
--    SELECT tablename, policyname, cmd, roles FROM pg_policies
--    WHERE schemaname='public' ORDER BY tablename, policyname;
--    Expect: 11 anon_select_* + 2 anon_insert_* policies.
--
-- 3. Re-run the manufacturer_ballistics sentinel-row write probe from the
--    anon key (the audit's _probe_ballistics_write.mjs shape — see web
--    repo git history). Expect: INSERT fails with an RLS error code 42501.
--
-- 4. End-to-end submission test: click "Request retailer" and submit;
--    click "Request ballistic data" on a /ammo/<slug> page and submit.
--    Both should succeed where the latter was broken before.
--
-- 5. Smoke the public site against the locked-down DB. Every page that
--    renders today must still render — special attention to:
--      - app/reloading-calculator/ReloadingCalculator.js  (reads components)
--      - app/HomeClient.js                                (reads coupons, rebates)
--      - app/ammo/[slug]/page.js                          (reads 6 tables)
--      - app/rebates/page.js                              (reads 5 tables)
