-- 026_price_history_listing_recorded_covering_index.sql
-- Captures the covering index added to production manually by Jon on
-- 2026-05-24 via the Supabase SQL editor, so the migrations directory
-- stays the source of truth for schema state.
--
-- What it supports: the segment_daily_floor() single-pass RPC (migration
-- 025). The plain index (migration 024) found rows fast but didn't carry
-- price_per_round, so each matched row triggered a heap fetch to read the
-- price (both for the >= p_price_floor filter and the returned value).
-- Measured on 9mm/Steel: fetch WITHOUT price 577ms (index-only) vs WITH
-- price 2.66s (heap fetches) — a gap that scaled to a statement timeout
-- on dense calibers. Adding price_per_round as an INCLUDE column makes
-- the RPC scan index-only (no heap trip); the densest caliber now returns
-- in ~2s.
--
-- PENDING FOLLOW-UP: the companion drop of the now-redundant
-- idx_price_history_listing_recorded (migration 024) was deferred — the
-- DROP did not take in production on 2026-05-24, and Jon chose to leave
-- the old index in place for now (harmless; just extra write overhead on
-- inserts). A future migration drops it. This file therefore records
-- only the covering-index create, matching current production state.
--
-- CONCURRENTLY: built without locking reads/writes. Cannot run inside a
-- transaction block — run on its own, not wrapped in BEGIN/COMMIT.
--
-- Idempotency: IF NOT EXISTS makes re-running a silent no-op.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_price_history_listing_recorded_cpr
    ON price_history (listing_id, recorded_at DESC) INCLUDE (price_per_round);
