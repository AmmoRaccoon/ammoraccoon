-- 008_manufacturer_ballistics.sql
-- Adds the canonical manufacturer ballistics table + listings join cache:
--   manufacturer_ballistics                  — one row per (manufacturer SKU / product page)
--   manufacturer_ballistics_listing_matches  — derived cache mapping listings to a ballistics row
--
-- Why a separate canonical table instead of muzzle_velocity_fps on listings:
-- velocity is a product attribute (one Federal AE9DP has one published muzzle
-- velocity), not a listing attribute (the same product appears at every retailer
-- that carries it). Storing on listings would replicate the value N times and
-- create N opportunities for drift between scrape sources. The canonical row
-- lives here once; listings join via the matcher cache below.
--
-- The matcher script (scripts/match_ballistics_to_listings.py — TBD) joins
-- listings to manufacturer_ballistics by:
--   listings.manufacturer    = manufacturer_ballistics.brand
--   listings.caliber_normalized = manufacturer_ballistics.caliber_normalized
--   listings.grain           = manufacturer_ballistics.grain
--   listings.bullet_type     = manufacturer_ballistics.bullet_type
-- Result is upserted into manufacturer_ballistics_listing_matches, mirroring
-- the manufacturer_rebate_listing_matches pattern from migration 007.
--
-- The scraper never deletes ballistics rows; it lets last_seen_at age out so
-- a transient scraper failure can't wipe canonical product data. Existing
-- listings.muzzle_velocity_fps remains as an opportunistic fallback for
-- retailer-only data and is unchanged by this migration.

CREATE TABLE IF NOT EXISTS manufacturer_ballistics (
    id                    SERIAL PRIMARY KEY,
    external_id           TEXT NOT NULL,           -- stable per-page identifier: SKU when present, URL slug otherwise
    source                TEXT NOT NULL,           -- 'federal' | 'remington' | 'cci' | 'speer' | 'winchester' | 'hornady' | ...
    brand                 TEXT NOT NULL,           -- canonical brand, matches listings.manufacturer
    sku                   TEXT,                    -- manufacturer catalog code (e.g. 'AE9DP', 'Q4203', '5200') when known
    product_line          TEXT,                    -- marketing line (e.g. 'American Eagle', 'USA', 'Blazer Brass')
    caliber_normalized    TEXT,                    -- matches listings.caliber_normalized
    grain                 INTEGER,
    bullet_type           TEXT,                    -- 'FMJ', 'JHP', 'OTM', etc.

    muzzle_velocity_fps   INTEGER NOT NULL,
    muzzle_energy_ftlb    INTEGER,
    bc_g1                 NUMERIC(5,3),
    velocity_50yd         INTEGER,
    velocity_100yd        INTEGER,

    source_url            TEXT NOT NULL,

    first_seen_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at          TIMESTAMPTZ NOT NULL,
    last_scraped_at       TIMESTAMPTZ NOT NULL,
    raw_html_hash         TEXT,

    CONSTRAINT manufacturer_ballistics_external_uniq UNIQUE (source, external_id)
);

-- Brand-only lookups for "show all Federal ballistics rows" UI.
CREATE INDEX IF NOT EXISTS manufacturer_ballistics_brand_idx
    ON manufacturer_ballistics (brand);

-- Composite index supports the matcher's join shape:
-- WHERE brand = ? AND caliber_normalized = ? AND grain = ? AND bullet_type = ?
CREATE INDEX IF NOT EXISTS manufacturer_ballistics_match_idx
    ON manufacturer_ballistics (brand, caliber_normalized, grain, bullet_type);

CREATE TABLE IF NOT EXISTS manufacturer_ballistics_listing_matches (
    ballistics_id   INTEGER NOT NULL REFERENCES manufacturer_ballistics(id) ON DELETE CASCADE,
    listing_id      INTEGER NOT NULL REFERENCES listings(id) ON DELETE CASCADE,
    match_reason    TEXT NOT NULL,
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ballistics_id, listing_id)
);

-- Reverse-lookup index: "for this listing, what ballistics row applies?"
-- Frontend hits this on every product page render.
CREATE INDEX IF NOT EXISTS manufacturer_ballistics_listing_matches_listing_idx
    ON manufacturer_ballistics_listing_matches (listing_id);
