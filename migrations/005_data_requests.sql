-- Captures user-submitted requests for missing product data,
-- starting with ballistic specs from the product-page meter.
-- One row per click; the request_type column is open-ended so
-- we can reuse the table for "request a retailer", "request a
-- caliber", etc. without needing a new table per kind.

CREATE TABLE IF NOT EXISTS data_requests (
  id BIGSERIAL PRIMARY KEY,
  product_slug TEXT NOT NULL,
  product_name TEXT NOT NULL,
  request_type TEXT NOT NULL DEFAULT 'ballistic_data',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS data_requests_slug_type_idx
  ON data_requests (product_slug, request_type);

CREATE INDEX IF NOT EXISTS data_requests_created_at_idx
  ON data_requests (created_at DESC);
