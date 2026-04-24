-- Purchase limit detected from the retailer's product card
-- (e.g. "Limit 2", "Max qty: 5"). NULL means no limit detected.

ALTER TABLE listings
  ADD COLUMN IF NOT EXISTS purchase_limit INTEGER;
