-- 012_manufacturer_rebates_firearm_type.sql
-- Adds firearm_type gating to manufacturer_rebates so the matcher can
-- refuse to claim turkey/shotshell rebates apply to handgun/rifle/
-- rimfire listings (the 2026-05-09 audit found rebate id=5 "Winchester
-- Turkey Ammunition Rebate" matching 141 non-shotshell listings via the
-- brand-only `manufacturer + url ILIKE keyword` gate, because the
-- eligible "Super-X" tier's keyword `super-x` collided with every
-- Winchester Super-X SKU regardless of firearm type — Super-X is a
-- broad Winchester sub-brand spanning handgun, rifle, rimfire, and
-- shotshell). Bible: never prey on the ignorant.
--
-- Values: 'shotshell', 'handgun', 'rifle', 'rimfire', NULL=any.
-- NULL preserves backward-compat for any future rebate that doesn't
-- need firearm-type gating.
--
-- The four currently-active rebates (id 2, 3, 4, 5) are all
-- shotshell-scoped per their titles + raw_terms (turkey ammunition
-- rebates from Federal/Remington/Winchester, plus a 16 GA rebate),
-- so they're all backfilled to 'shotshell'.

ALTER TABLE manufacturer_rebates
    ADD COLUMN IF NOT EXISTS firearm_type TEXT;

ALTER TABLE manufacturer_rebates
    ADD CONSTRAINT manufacturer_rebates_firearm_type_chk
    CHECK (firearm_type IS NULL OR firearm_type IN
        ('shotshell', 'handgun', 'rifle', 'rimfire'));

UPDATE manufacturer_rebates SET firearm_type = 'shotshell'
WHERE id IN (2, 3, 4, 5);
