-- GENERATED FROM ammoraccoon/calibers.json - DO NOT EDIT.
-- Regenerate: node scripts/gen-calibers/index.mjs --write (run from ammoraccoon-web).
-- Registry sha256: 7df48e4ae6c0fe36ea73486135a25326614a9864d405b32b982169afae4a8c60
--
-- The caliber_floors CTE block below is the registry-derived twin of the
-- one inside migrations/033_homepage_segment_aggregates_percaliber_floor.sql
-- (which itself mirrors lib/priceBounds.js PER_CALIBER_FLOOR). It is NEVER
-- applied directly: when a registry edit changes a market floor, the
-- generator flags the drift and a NEW numbered migration (033's body with
-- this block substituted) goes through normal DB-change approval.

    WITH caliber_floors(cal, floor_ppr) AS (
        -- MUST mirror ammoraccoon-web/lib/priceBounds.js PER_CALIBER_FLOOR
        VALUES
            ('9mm',      0.10::numeric),
            ('22lr',     0.03),
            ('380acp',   0.15),
            ('38spl',    0.20),
            ('357mag',   0.20),
            ('40sw',     0.15),
            ('45acp',    0.15),
            ('223-556',  0.20),
            ('300blk',   0.25),
            ('308win',   0.20),
            ('6.5cm',    0.40),
            ('762x39',   0.25),
            ('762x54r',  0.25),
            ('12ga',     0.15)
    ),
