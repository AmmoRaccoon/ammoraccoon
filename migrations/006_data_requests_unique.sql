-- Prevent users from spamming the data_requests table with duplicate
-- clicks on the same product. After this, a second insert with the
-- same (product_slug, request_type) pair fails with code 23505 — the
-- RequestBallisticData client switches to upsert to handle that
-- silently.

-- Dedup anything already in the table before adding the constraint,
-- otherwise the ALTER fails on existing violators. Keeps the earliest
-- row per pair (lowest id) and drops the rest.
DELETE FROM data_requests a
USING data_requests b
WHERE a.id > b.id
  AND a.product_slug = b.product_slug
  AND a.request_type = b.request_type;

ALTER TABLE data_requests
  ADD CONSTRAINT data_requests_slug_type_unique
  UNIQUE (product_slug, request_type);
