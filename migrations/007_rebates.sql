-- 007_rebates.sql
-- Adds the three tables backing the manufacturer rebate page:
--   manufacturer_rebates                  — one row per active manufacturer rebate program
--   manufacturer_rebate_eligible_products — child rows for tiered/per-product amounts
--   manufacturer_rebate_listing_matches   — derived cache of which listings each rebate applies to
--
-- These coexist with the pre-existing `rebates` table (v1) which the
-- frontend currently reads via supabase.from('rebates') in HomeClient
-- and ListingsTable. The new manufacturer_* tables are populated by
-- the manufacturer-site scrapers (Federal, Winchester, Remington,
-- Hornady) and will eventually replace v1, but the cutover is a
-- separate frontend change — this migration is additive only.
--
-- The matcher script (scripts/match_manufacturer_rebates_to_listings.py
-- — TBD) populates manufacturer_rebate_listing_matches by joining
-- manufacturer_rebates.brand to listings.manufacturer and fuzzy-matching
-- manufacturer_rebate_eligible_products.product_line against
-- listings.product_url. Frontend reads from
-- manufacturer_rebate_listing_matches to render rebate-eligible badges.
--
-- An "active rebate" for display purposes is computed at query time as:
--   valid_through >= current_date
--   AND submit_by  >= current_date
--   AND last_seen_active_at > now() - interval '48 hours'
-- The scraper never deletes rebate rows; it lets last_seen_active_at age
-- out so that a transient scraper failure can't wipe live rebates from
-- the page.

CREATE TABLE IF NOT EXISTS manufacturer_rebates (
    id                    SERIAL PRIMARY KEY,
    external_id           TEXT NOT NULL,
    source                TEXT NOT NULL,
    brand                 TEXT NOT NULL,
    title                 TEXT NOT NULL,
    detail_url            TEXT NOT NULL,
    source_url            TEXT NOT NULL,

    amount_min_per_unit   NUMERIC(8,2),
    amount_max_per_unit   NUMERIC(8,2),
    amount_unit           TEXT,
    amount_max_total      NUMERIC(8,2),
    min_qty_required      INTEGER,

    valid_from            DATE NOT NULL,
    valid_through         DATE NOT NULL,
    submit_by             DATE NOT NULL,

    first_seen_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_active_at   TIMESTAMPTZ NOT NULL,
    last_scraped_at       TIMESTAMPTZ NOT NULL,
    raw_terms             TEXT,
    terms_html_hash       TEXT,

    CONSTRAINT manufacturer_rebates_external_uniq UNIQUE (source, external_id)
);

CREATE INDEX IF NOT EXISTS manufacturer_rebates_active_idx
    ON manufacturer_rebates (valid_through, submit_by);
CREATE INDEX IF NOT EXISTS manufacturer_rebates_brand_idx
    ON manufacturer_rebates (brand);

CREATE TABLE IF NOT EXISTS manufacturer_rebate_eligible_products (
    id              SERIAL PRIMARY KEY,
    rebate_id       INTEGER NOT NULL REFERENCES manufacturer_rebates(id) ON DELETE CASCADE,
    product_line    TEXT NOT NULL,
    amount_override NUMERIC(8,2),
    match_pattern   TEXT
);

CREATE INDEX IF NOT EXISTS manufacturer_rebate_eligible_products_rebate_idx
    ON manufacturer_rebate_eligible_products (rebate_id);

CREATE TABLE IF NOT EXISTS manufacturer_rebate_listing_matches (
    rebate_id       INTEGER NOT NULL REFERENCES manufacturer_rebates(id) ON DELETE CASCADE,
    listing_id      INTEGER NOT NULL REFERENCES listings(id) ON DELETE CASCADE,
    matched_amount  NUMERIC(8,2) NOT NULL,
    match_reason    TEXT NOT NULL,
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (rebate_id, listing_id)
);

CREATE INDEX IF NOT EXISTS manufacturer_rebate_listing_matches_listing_idx
    ON manufacturer_rebate_listing_matches (listing_id);
