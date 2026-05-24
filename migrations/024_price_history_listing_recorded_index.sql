-- 024_price_history_listing_recorded_index.sql
-- Captures an index added to production manually by Jon on 2026-05-24
-- via the Supabase SQL editor, so the migrations directory stays the
-- source of truth for schema state.
--
-- What it supports: the segment_daily_floor() RPC (migration 023, rewritten
-- in 025). Both forms look up, per listing, the most recent price_history
-- row at or before a time bucket — an equality on listing_id plus a
-- descending recorded_at range, which this composite index serves as a
-- direct seek instead of a scan.
--
-- NOTE: superseded by the covering index in migration 026, which adds
-- price_per_round as an INCLUDE column to make the RPC scan index-only
-- (no per-row heap fetch). This bare index was left in place in
-- production for now (the 026 drop step was deferred) and is harmless;
-- a future migration drops it. Kept here for lineage.
--
-- CONCURRENTLY: built without locking reads/writes so the scrapers keep
-- writing during the build. CONCURRENTLY cannot run inside a
-- transaction block — run this statement on its own, not wrapped in
-- BEGIN/COMMIT and not batched with other statements.
--
-- Idempotency: IF NOT EXISTS makes re-running a silent no-op.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_price_history_listing_recorded
    ON price_history (listing_id, recorded_at DESC);
