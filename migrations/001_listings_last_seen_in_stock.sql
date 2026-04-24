-- Track when each listing was last observed in stock.
-- Scrapers only set this column when in_stock=true, so the value
-- preserves the most recent in-stock timestamp even after the
-- product goes OOS.

ALTER TABLE listings
  ADD COLUMN IF NOT EXISTS last_seen_in_stock TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS listings_last_seen_in_stock_idx
  ON listings (last_seen_in_stock DESC);
