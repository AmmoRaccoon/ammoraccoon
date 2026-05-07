-- 011_components_parsed_columns.sql
-- Adds parsed-metadata columns to `components` so the calculator's
-- searchable/filterable component picker (Frontman half of the arc)
-- can index on caliber/grain/bullet_type/primer_size/powder_application
-- without re-tokenizing product_name on every read.
--
-- Backfill is performed out-of-band by scripts/backfill_components_parsed.py
-- (dry-run first, then --apply). Going forward the per-retailer scrapers
-- should populate these at write time so the backfill is one-shot.
--
-- Two of the five columns already exist from migration 009_components.sql
-- (`caliber TEXT`, `grain INTEGER`). The IF NOT EXISTS guards make those
-- ADDs no-ops, but be aware:
--   - 009 declared `grain INTEGER`; this migration's spec asks for NUMERIC.
--     IF NOT EXISTS will skip the ADD silently — column stays INTEGER.
--     Decide separately whether a fractional-grain bullet (e.g. 62.5gr)
--     justifies an `ALTER COLUMN grain TYPE NUMERIC`. For now, all
--     observed values are whole grains, so INTEGER is fine.
--   - `caliber` type matches (TEXT). No conflict.

ALTER TABLE components ADD COLUMN IF NOT EXISTS caliber TEXT;
ALTER TABLE components ADD COLUMN IF NOT EXISTS grain NUMERIC;
ALTER TABLE components ADD COLUMN IF NOT EXISTS bullet_type TEXT;
ALTER TABLE components ADD COLUMN IF NOT EXISTS primer_size TEXT;
ALTER TABLE components ADD COLUMN IF NOT EXISTS powder_application TEXT;
