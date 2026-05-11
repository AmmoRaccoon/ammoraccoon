-- 018_backfill_brand_slug_prefix_unknowns.sql
-- One-time backfill that resolves manufacturer='Unknown' rows whose
-- product_url carries one of the SKU-prefix abbreviations the new
-- parse_brand_with_url wrapper now understands, plus the holistic
-- 'century arms' brand alias added to scraper_lib._BRAND_ALIASES.
-- Closes the 2026-05-10 Unknown-rows audit gap surfaced after
-- tonight's slug-prefix alias add (commit TBD).
--
-- Background: scraper_lib.parse_brand uses unanchored substring
-- matching, which makes 3-4 letter brand abbreviations (win, fed, rem,
-- fio, cent, spr) unsafe to add to _BRAND_ALIASES — they would
-- false-positive against words like 'twin', 'federalist', 'winnow'.
-- The new parse_brand_with_url(title, url) wrapper anchors those
-- abbreviations to the start of the URL slug's last path segment, so
-- they only fire on retailer SKU prefixes (e.g. Gunbuyer's
-- 'win-38spl-130gr-fmj-300rd-can-winww38c-d.html', Shadowsmith's
-- 'rem-22lr-36gr-hvhp-dtom-1400rd', Firearms Depot's
-- 'cent-arms-mesko-9mm-mak-93gr-50-1000'). The wrapper is wired into
-- scraper_gunbuyer / scraper_shadowsmith / scraper_firearmsdepot, so
-- new scrapes from those retailers will resolve correctly going
-- forward; this migration retroactively applies the same resolution
-- to existing Unknown rows.
--
-- Per-pattern row counts (verified live 2026-05-10):
--   '%/win-%'        -> Winchester      36 rows
--   '%/fed-%'        -> Federal         27 rows
--   '%/fio-%'        -> Fiocchi         22 rows
--   '%/rem-%'        -> Remington       18 rows
--   '%/cent-%'       -> Century Arms     6 rows
--   '%/spr-%'        -> Speer            5 rows
--   '%century-arms%' -> Century Arms     9 rows
--   ----------------------------------------------
--   TOTAL                              123 rows
--
-- The /cent-% and %century-arms% patterns DO NOT overlap (verified
-- 0 rows match both), so the two Century Arms statements are
-- independent.
--
-- Protected-write semantics: every UPDATE is gated on
-- `manufacturer = 'Unknown'`. Rows that have already been classified
-- (manually or by a previous scrape) are never touched. This mirrors
-- the never-overwrite-existing rule used by parse_firearm_type wiring
-- in the rebate scrapers — when a row already has a non-Unknown
-- value, that value stands.
--
-- Idempotency: a second run of this migration finds zero rows for
-- every UPDATE (the first run flipped them out of Unknown). Postgres
-- reports "UPDATE 0" for each statement; safe to re-apply across
-- environments.
--
-- Not in scope: the 27 'hsm' / 24 'nosler' / 18 'privi' / 6 'norma'
-- / 5 'fort' / 8 'frontier' Unknown rows surfaced in the same audit
-- are not abbreviation problems — they are stale rows that predate
-- the corresponding canonical alias entries. A daily scraper cycle
-- (or a separate rescrape pass) resolves them naturally; no SQL
-- backfill needed here.

UPDATE listings SET manufacturer = 'Winchester'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/win-%';

UPDATE listings SET manufacturer = 'Federal'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/fed-%';

UPDATE listings SET manufacturer = 'Fiocchi'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/fio-%';

UPDATE listings SET manufacturer = 'Remington'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/rem-%';

UPDATE listings SET manufacturer = 'Century Arms'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/cent-%';

UPDATE listings SET manufacturer = 'Speer'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%/spr-%';

UPDATE listings SET manufacturer = 'Century Arms'
 WHERE manufacturer = 'Unknown' AND product_url ILIKE '%century-arms%';
