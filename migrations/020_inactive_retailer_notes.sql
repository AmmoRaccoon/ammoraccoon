-- 020_inactive_retailer_notes.sql
-- Clarify the dormancy intent for the two provisioning-stub inactive
-- retailers surfaced by tonight's audit (2026-05-10). Both rows have
-- existed since the project's seed batch (created_at 2026-04-18 for
-- Cabela's, 2026-04-22 for Optics Planet) but neither ever had a
-- scraper file written, neither was scheduled in any GitHub Actions
-- workflow, and neither has produced a single row in `listings` or
-- `price_history`. The audit classified them DEAD on the four-bucket
-- rubric (DEAD / DORMANT / BROKEN / SHOULD-BE-ACTIVE).
--
-- Why not just DELETE: per the AmmoRaccoon Bible (2026-05-10) both
-- retailers are aspirational long-term targets — Cabela's is a
-- name-brand mainstream sporting-goods chain customers actively look
-- for, and Optics Planet is one of the everything-stores worth
-- watching for opportunistic ammo deals. Deleting the rows would
-- lose those signals into git history; leaving them with empty or
-- ambiguous `notes` (the state today) means a future operator
-- inheriting the schema can't tell intent-to-build from forgotten-
-- to-clean-up. The Bible is unambiguous on this: visible wishlist
-- beats invisible deletion. This migration captures the dormancy
-- rationale on the row itself so the next person reading the
-- retailers table understands what they're looking at.
--
-- Companion migration 019 (retailer_requests table, applied alongside
-- this one) is the user-facing half of the same loop — visitors can
-- vote with /request-retailer; dormant rows here document supply-side
-- intent. Together they let demand and intent inform the next scraper
-- to build.
--
-- Scope: only Cabela's (id=10) and Optics Planet (id=20). The other
-- three inactive retailers from the audit already carry correct
-- dormancy rationale and are NOT touched here:
--   * id=8  Academy Sports — `notes` already says
--           "In-store pickup only; no ship-to-home for ammo"
--           (business policy, captured at row level).
--   * id=19 Bereli — scraper file exists with explicit "currently
--           disabled — Variant pricing requires per-product Playwright
--           drilldown that we haven't finished" docstring (captured
--           at code level).
--   * id=28 Bulk Munitions — scraper file gained a Cloudflare-block
--           docstring tonight via the same swing as migration 019.
--
-- Idempotency: each UPDATE sets `notes` to a fixed string. Re-running
-- against an already-updated row produces a redundant write but no
-- semantic change — Postgres reports "UPDATE 1" each time, the row's
-- final state is unchanged. Safe to re-apply across environments. If
-- Jon ever hand-edits these notes via the Supabase dashboard, a
-- subsequent re-run of this migration would clobber his edits — at
-- which point the right move is to delete this migration's UPDATE
-- statements (they've served their purpose) rather than perpetually
-- fight with the dashboard.

UPDATE retailers
   SET notes = 'Wishlist — not yet built. Aspirational on the dream-list (per AmmoRaccoon Bible 2026-05-10). Build effort: scraper not started.'
 WHERE id = 10;

UPDATE retailers
   SET notes = 'Wishlist — not yet built. No scraper effort started. Re-evaluate when there is signal it would be worth the effort.'
 WHERE id = 20;
