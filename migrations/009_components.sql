-- 009_components.sql
-- Adds the `components` table for reloading-component pricing (powder, primers,
-- bullets, brass). One row per variant SKU per retailer.
--
-- Why a separate table from `listings`:
-- listings carries loaded-ammunition rows keyed on (caliber, grain, bullet_type)
-- with rounds-per-box pricing. Components have a different shape:
--   - powder is priced per pound of jug
--   - primer/bullet/brass are priced per piece-count box
--   - no caliber/grain/bullet_type for powder/primer
--   - the user-facing surface is the reloading calculator, not the comparison table
-- Stuffing both into `listings` would either bloat that table with NULL columns
-- or require a discriminator that complicates every existing query.
--
-- Why `retailer_slug` (TEXT) instead of `retailer_id` (FK to retailers):
-- the retailers table is keyed to ammunition vendors and feeds the comparison
-- chart. Component vendors (Powder Valley, Midsouth Shooters Supply) are a
-- separate concern — they don't sell loaded ammo, don't appear in the chart,
-- and don't share the tax-nexus / shipping-config columns. A free TEXT slug
-- keeps the two surfaces decoupled while still allowing JOINs by name when
-- something cross-cutting (e.g. shared logos) emerges later.
--
-- Unit conventions enforced by the scrapers:
--   powder: package_size in POUNDS, package_unit = 'lbs'  (× 7000 for grains)
--   primer/bullet/brass: package_size in PIECES, package_unit = 'pieces'
--
-- The scraper never deletes rows; it lets last_seen_at age out so a transient
-- scrape failure can't wipe a retailer's catalog. Frontend filters by
-- last_seen_at recency to surface only fresh listings.

CREATE TABLE IF NOT EXISTS components (
    id                    SERIAL PRIMARY KEY,
    retailer_slug         TEXT NOT NULL,           -- 'powdervalley', 'midsouth', ...
    category              TEXT NOT NULL,           -- 'powder' | 'primer' | 'bullet' | 'brass'

    parent_sku            TEXT,                    -- WooCommerce ProductGroup SKU; equals variant_sku for single-SKU products
    variant_sku           TEXT NOT NULL,           -- per-variant SKU, unique within retailer
    product_name          TEXT NOT NULL,
    brand                 TEXT,                    -- from JSON-LD brand.name
    manufacturer          TEXT,                    -- from JSON-LD manufacturer.name (often same as brand)

    package_size          NUMERIC(10,2) NOT NULL,  -- 1, 4, 8 (lbs) or 50, 100, 1000 (pieces)
    package_unit          TEXT NOT NULL,           -- 'lbs' | 'pieces'

    price                 NUMERIC(10,2) NOT NULL,
    in_stock              BOOLEAN NOT NULL DEFAULT FALSE,

    caliber               TEXT,                    -- bullet/brass only; freeform from info table
    grain                 INTEGER,                 -- bullet only

    source_url            TEXT NOT NULL,

    first_seen_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at          TIMESTAMPTZ NOT NULL,
    last_seen_in_stock    TIMESTAMPTZ,             -- only set when in_stock=true at scrape
    last_updated          TIMESTAMPTZ NOT NULL,

    CONSTRAINT components_retailer_variant_uniq UNIQUE (retailer_slug, variant_sku),
    CONSTRAINT components_category_chk CHECK (category IN ('powder','primer','bullet','brass')),
    CONSTRAINT components_unit_chk CHECK (package_unit IN ('lbs','pieces'))
);

-- Calculator dropdown queries: "cheapest in-stock powder by brand", etc.
-- Filtered by last_seen_at on the read path.
CREATE INDEX IF NOT EXISTS components_lookup_idx
    ON components (category, brand, in_stock);

-- Recency filter — frontend will gate on last_seen_at > now() - interval to
-- avoid surfacing stale data from a broken scrape.
CREATE INDEX IF NOT EXISTS components_last_seen_idx
    ON components (last_seen_at);
