-- 037_listings_manufacturer_sku.sql
--
-- Adds a nullable manufacturer part number (MPN / manufacturer SKU) column to
-- listings. This is the "Bucket B" structural field: an exact, cross-retailer
-- part number that a future line-aware matcher / collision resolver can key on
-- instead of guessing from titles or URL substrings.
--
-- Nullable on purpose: most retailers do not publish an MPN on the data we
-- currently fetch (see Bucket B discovery, 2026-06-27). Initial populator is
-- scraper_ammoman.py, which already parses the Magento JSON-LD `mpn` field;
-- other scrapers backfill as their sources expose it.
--
-- DB-change approval required before apply (CLAUDE.md working agreement).
-- The scraper_ammoman.py change that writes this column MUST NOT ship until
-- this migration is applied, or its upsert will fail on an unknown column.

ALTER TABLE listings ADD COLUMN IF NOT EXISTS manufacturer_sku TEXT;
