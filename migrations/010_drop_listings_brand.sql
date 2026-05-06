-- Drop the unused listings.brand column.
--
-- Audit 2026-05-05: brand IS NOT NULL count = 0 across 9,465 listings.
-- The actual brand identity lives in listings.manufacturer (9,463/9,465
-- populated). All frontend brand-display paths (getBrand, getBrandForFilter
-- in lib/listingHelpers.js, HistoryClient.js's local getBrand) read
-- listing.manufacturer and never reference listing.brand. All `'brand':`
-- writers in the scraper repo target other tables (manufacturer_ballistics,
-- manufacturer_rebates, components), not listings.
--
-- Removing the column tightens the schema and prevents new scrapers from
-- mistakenly writing to a dead field.

ALTER TABLE listings
  DROP COLUMN IF EXISTS brand;
