-- Shipping configuration per retailer. The frontend reads these
-- columns instead of the hardcoded RETAILERS object so shipping
-- policy changes don't require a code deploy.

ALTER TABLE retailers
  ADD COLUMN IF NOT EXISTS free_ship_threshold NUMERIC,
  ADD COLUMN IF NOT EXISTS flat_ship_rate NUMERIC,
  ADD COLUMN IF NOT EXISTS ships_free_always BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS ships_free_never BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS membership_free_ship BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS notes TEXT;

-- Seed shipping policy per retailer.
-- Values marked "estimate" should be spot-checked against the retailer
-- site and updated if wrong.

-- 1. Ammunition Depot: free over $199, $9.99 flat otherwise.
UPDATE retailers SET
  free_ship_threshold = 199,
  flat_ship_rate = 9.99,
  notes = 'Free over $199, $9.99 flat otherwise'
WHERE id = 1;

-- 2. SGAmmo: always free.
UPDATE retailers SET
  ships_free_always = TRUE,
  notes = 'Always free shipping'
WHERE id = 2;

-- 3. Lucky Gunner: weight-based, use $0 placeholder.
UPDATE retailers SET
  ships_free_always = TRUE,
  notes = 'Weight-based actual; $0 placeholder until calculator wired'
WHERE id = 3;

-- 4. PSA: free over $99.
UPDATE retailers SET
  free_ship_threshold = 99,
  flat_ship_rate = 12.99,
  notes = 'Free over $99'
WHERE id = 4;

-- 5. Bud's Gun Shop: estimate.
UPDATE retailers SET
  free_ship_threshold = 99,
  flat_ship_rate = 9.99,
  notes = 'Free over $99 (estimate - verify)'
WHERE id = 5;

-- 6. AmmoMan: always free shipping is their core pitch.
UPDATE retailers SET
  ships_free_always = TRUE,
  notes = 'Free shipping is their flagship policy'
WHERE id = 6;

-- 7. Target Sports USA: free over $149, free always with membership.
UPDATE retailers SET
  free_ship_threshold = 149,
  flat_ship_rate = 12.99,
  membership_free_ship = TRUE,
  notes = 'Free over $149; free always with paid membership'
WHERE id = 7;

-- 8. Academy Sports: in-store pickup only for ammo in most states.
UPDATE retailers SET
  ships_free_never = TRUE,
  notes = 'In-store pickup only; no ship-to-home for ammo'
WHERE id = 8;

-- 9. Brownells: estimate.
UPDATE retailers SET
  free_ship_threshold = 99,
  flat_ship_rate = 4.99,
  notes = '$4.99 flat or free over $99 (estimate - verify)'
WHERE id = 9;

-- 10. Cabela's: estimate.
UPDATE retailers SET
  free_ship_threshold = 99,
  flat_ship_rate = 9.99,
  notes = 'Free over $99 (estimate - verify)'
WHERE id = 10;

-- 11. AE Ammo: always free.
UPDATE retailers SET
  ships_free_always = TRUE,
  notes = 'Always free shipping'
WHERE id = 11;

-- 12. Rivertown Munitions: estimate.
UPDATE retailers SET
  flat_ship_rate = 9.99,
  notes = 'Flat rate shipping (estimate - verify)'
WHERE id = 12;

-- 13. Ammo.com: estimate.
UPDATE retailers SET
  free_ship_threshold = 99,
  flat_ship_rate = 12.99,
  notes = 'Free over $99 (estimate - verify)'
WHERE id = 13;

-- 14. BulkAmmo: estimate.
UPDATE retailers SET
  free_ship_threshold = 99,
  flat_ship_rate = 14.99,
  notes = 'Free over $99 (estimate - verify)'
WHERE id = 14;

-- 15. True Shot Gun Club: always free, membership available.
UPDATE retailers SET
  ships_free_always = TRUE,
  membership_free_ship = TRUE,
  notes = 'Always free shipping; membership tier available'
WHERE id = 15;

-- 16. Natchez: estimate.
UPDATE retailers SET
  flat_ship_rate = 9.99,
  notes = 'Flat rate shipping (estimate - verify)'
WHERE id = 16;

-- 17. Wideners: estimate.
UPDATE retailers SET
  free_ship_threshold = 199,
  flat_ship_rate = 14.99,
  notes = 'Free over $199 (estimate - verify)'
WHERE id = 17;

-- 18. Freedom Munitions: estimate.
UPDATE retailers SET
  free_ship_threshold = 99,
  flat_ship_rate = 9.99,
  notes = 'Free over $99 (estimate - verify)'
WHERE id = 18;
