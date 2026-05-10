-- 013_retailers_policy_verified_at.sql
-- Tier A free-shipping honesty gate. Adds a policy_verified_at column
-- to retailers; Frontman's lib/shippingConfig.js then refuses to
-- compute a $0 (or any) shipping cost for any retailer whose
-- policy_verified_at IS NULL, falling back to the "ship varies" copy
-- callers already render when shippingFromConfig returns null.
--
-- Background: 2026-05-09 audit found that 19 active retailers
-- currently produce some form of free-shipping claim via the DB
-- shipping config (ships_free_always OR free_ship_threshold), but
-- only 3 of those claims could be verified live. 5 were verified
-- inaccurate (AmmoMan threshold wrong, Freedom Munitions threshold
-- $99 vs reality $199, Brownells threshold $150 vs $99-with-promo,
-- AE Ammo / Target Sports per-listing model not flat-threshold).
-- 9 could not be verified (bot-blocked storefronts). Remaining 2
-- had internal data contradictions. Migration 003's "estimate -
-- verify" notes were a TODO that was never closed; this migration
-- closes the loop programmatically — no claim renders unless a
-- human has stamped policy_verified_at.
--
-- Three retailers are verified accurate by direct evidence (live
-- homepage banner text matches the threshold field) and are
-- backfilled with policy_verified_at = NOW():
--   id=2   SGAmmo                — "Free shipping on orders over $200" matches threshold=$200
--   id=21  Bucking Horse Outpost — "FREE SHIPPING on orders over $200!" matches threshold=$200
--   id=31  Black Basin           — "Free shipping on orders over $250" matches threshold=$250
--
-- Two retailers (AE Ammo, True Shot) are description-based — the JS
-- gate DESCRIPTION_BASED_RETAILER_IDS in lib/retailerConfig.js
-- already correctly handles their per-listing "FREE SHIPPING" string
-- detection. The DB rows had ships_free_always=true alongside, which
-- conflicted with the JS gate. Flip ships_free_always to false on
-- those two AND mark policy_verified_at so the row stops claiming
-- universal free shipping while the JS gate keeps doing the right
-- thing per listing:
--   id=11  AE Ammo
--   id=15  True Shot Gun Club
--
-- All 14 remaining retailers with free-shipping claims keep their
-- existing data fields unchanged (so a future operator can verify
-- and stamp policy_verified_at without re-entering thresholds).
-- They lose the green-truck claim until verified — that's the point.

ALTER TABLE retailers
    ADD COLUMN IF NOT EXISTS policy_verified_at TIMESTAMPTZ;

COMMENT ON COLUMN retailers.policy_verified_at IS
    'When the retailer''s shipping policy fields (ships_free_always, '
    'free_ship_threshold, flat_ship_rate, ships_free_never, '
    'membership_free_ship) were last manually verified against the '
    'retailer''s live storefront. NULL means unverified — the '
    'frontend gates the green-truck "FREE" badge off until set. '
    'Bible: never prey on the ignorant. Re-stamp whenever the '
    'retailer changes policy or the data is re-checked.';

UPDATE retailers SET policy_verified_at = NOW()
WHERE id IN (2, 21, 31);

UPDATE retailers SET
    ships_free_always = FALSE,
    policy_verified_at = NOW()
WHERE id IN (11, 15);
