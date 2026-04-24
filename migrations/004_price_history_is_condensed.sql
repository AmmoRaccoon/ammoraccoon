-- Marks rows in price_history as "already rolled up to one-per-day."
-- The weekly condense_history.py script sets this to TRUE on the
-- single row it inserts for each (listing_id, day) pair older than
-- 30 days, and deletes the original hourly rows.
--
-- Filtering condense_history.py by is_condensed=false makes the job
-- idempotent — re-runs ignore rows that have already been rolled up.

ALTER TABLE price_history
  ADD COLUMN IF NOT EXISTS is_condensed BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS price_history_is_condensed_idx
  ON price_history (is_condensed, recorded_at);
