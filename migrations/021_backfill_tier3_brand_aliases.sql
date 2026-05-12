-- 021_backfill_tier3_brand_aliases.sql
-- One-time backfill that resolves manufacturer='Unknown' rows whose
-- product_url carries one of the 20 brand aliases added to
-- scraper_lib._BRAND_ALIASES in the Tier 3 audit (2026-05-11). The
-- cron will eventually self-heal these via the live alias scan on
-- next per-retailer scrape; this migration just compresses the
-- timeline so users see the catalog improvement same-day.
--
-- Background: The 2026-05-10 audit reduced Unknowns from 714 → 591 via
-- migration 018's slug-prefix aliases. A follow-up audit on 2026-05-11
-- (with the cron having cycled through every scraper at least once
-- since) found 422 Unknowns remaining. Bucketing by leading slug token
-- surfaced 20 brand names that were not yet in canonical
-- _BRAND_ALIASES — most were unambiguous house brands or imported
-- product lines that simply hadn't been catalogued yet (Saltech, GGG,
-- ZSR, Tela Ammo, BPS, DRT, etc.). Two pre-existing canonicals also
-- gained bare-form aliases ('frontier ' → Hornady for slugs that
-- carry the bare token without 'cartridge'/'ammo'; 'streak' → Ammo Inc
-- for the Streak Visual tracer line). See scraper_lib.py:262 (Tier 3
-- block) for the full 20-entry list and per-alias justification.
--
-- Per-pattern row counts (verified live 2026-05-11, slug-anchored):
--   '%/sk-%'                  -> SK                  16 rows
--   '%/sk22%'                 -> SK                   1 row
--   '%/american-munitions-%'  -> American Munitions   2 rows
--   '%/american-sniper-%'     -> American Sniper      5 rows
--   '%/scorpion-%'            -> Scorpion             8 rows
--   '%/frontier-%'            -> Hornady              8 rows
--   '%/black-sheep-%'         -> Black Sheep          6 rows
--   '%/streak-%'              -> Ammo Inc             6 rows
--   '%/zsr-%'                 -> ZSR                  5 rows
--   '%/ammo-zsr-%'            -> ZSR                  1 row
--   '%/ventura-tactical-%'    -> Ventura Tactical     4 rows
--   '%/super-vel-%'           -> Super Vel            4 rows
--   '%/ten-ring-%'            -> Ten Ring             4 rows
--   '%/saltech-%'             -> Saltech              4 rows
--   '%-saltech-%'             -> Saltech              6 rows
--   '%/sako-%'                -> Sako                 4 rows
--   '%/ggg-%'                 -> GGG                  4 rows
--   '%/telaammo-%'            -> Tela Ammo            4 rows
--   '%-telaammo-%'            -> Tela Ammo            1 row
--   '%/bitterroot-valley-%'   -> Bitterroot Valley    3 rows
--   '%/badlands-%'            -> Badlands             3 rows
--   '%/bps-%'                 -> BPS                  3 rows
--   '%/drt-%'                 -> DRT                  3 rows
--   ------------------------------------------------------------
--   TOTAL                                            105 rows
--
-- Verified no overlap between the slash-prefixed and hyphen-prefixed
-- variants for Saltech and Tela Ammo (the slash form anchors at slug
-- start; the hyphen form catches mid-slug occurrences in retailers
-- whose URLs put the brand after a category fragment).
--
-- Not in scope (left to natural cron self-heal):
--   - Gorilla (10 rows, all Firearms Depot opaque numeric SKUs like
--     /373751 — slug-anchored ILIKE can't catch them; full-URL
--     substring match would risk hostname pollution. The new
--     'gorilla' alias in scraper_lib will resolve these on next
--     Firearms Depot scrape via the title-text alias scan.)
--   - HSM (~13 rows, Classic Firearms): 'hsm ' was already in
--     canonical pre-Tier-3; rows are stale waiting on next scrape.
--   - Federal / CCI / PMC / Speer at RecoilGunWorks (~12 rows):
--     scraper-tier debt closed in the same commit by swapping
--     parse_brand → parse_brand_with_url in scraper_recoilgunworks
--     (Phase 2 of this PR). Self-heals next RGW scrape.
--   - Conservative SKIP tokens from the audit (great, golden,
--     walther, solgw, bear — ~31 rows): need title-pull follow-up
--     audit before adding aliases. Deferred to next session.
--
-- Protected-write semantics: every UPDATE is gated on
-- `manufacturer = 'Unknown'`. Rows that have already been classified
-- are never touched. Mirrors migration 018's never-overwrite-existing
-- rule.
--
-- Idempotency: a second run finds zero rows for every UPDATE (the
-- first run flipped them out of Unknown). Postgres reports "UPDATE 0"
-- for each statement; safe to re-apply across environments.

UPDATE listings SET manufacturer = 'SK'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/sk-%';

UPDATE listings SET manufacturer = 'SK'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/sk22%';

UPDATE listings SET manufacturer = 'American Munitions'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/american-munitions-%';

UPDATE listings SET manufacturer = 'American Sniper'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/american-sniper-%';

UPDATE listings SET manufacturer = 'Scorpion'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/scorpion-%';

UPDATE listings SET manufacturer = 'Hornady'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/frontier-%';

UPDATE listings SET manufacturer = 'Black Sheep'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/black-sheep-%';

UPDATE listings SET manufacturer = 'Ammo Inc'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/streak-%';

UPDATE listings SET manufacturer = 'ZSR'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/zsr-%';

UPDATE listings SET manufacturer = 'ZSR'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/ammo-zsr-%';

UPDATE listings SET manufacturer = 'Ventura Tactical'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/ventura-tactical-%';

UPDATE listings SET manufacturer = 'Super Vel'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/super-vel-%';

UPDATE listings SET manufacturer = 'Ten Ring'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/ten-ring-%';

UPDATE listings SET manufacturer = 'Saltech'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/saltech-%';

UPDATE listings SET manufacturer = 'Saltech'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%-saltech-%';

UPDATE listings SET manufacturer = 'Sako'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/sako-%';

UPDATE listings SET manufacturer = 'GGG'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/ggg-%';

UPDATE listings SET manufacturer = 'Tela Ammo'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/telaammo-%';

UPDATE listings SET manufacturer = 'Tela Ammo'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%-telaammo-%';

UPDATE listings SET manufacturer = 'Bitterroot Valley'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/bitterroot-valley-%';

UPDATE listings SET manufacturer = 'Badlands'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/badlands-%';

UPDATE listings SET manufacturer = 'BPS'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/bps-%';

UPDATE listings SET manufacturer = 'DRT'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/drt-%';
