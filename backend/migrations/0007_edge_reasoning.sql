-- Migration 0007: edge reasoning fields
-- Each flagged edge now carries a short one-liner explanation plus a
-- structured array of factor drivers (pitching, park, weather, etc).
--
-- reason_short  : human-readable one-liner shown inline next to the edge
-- reason_factors: JSONB array of {label, value, impact} objects for the
--                 expanded view

ALTER TABLE edges
    ADD COLUMN IF NOT EXISTS reason_short    TEXT,
    ADD COLUMN IF NOT EXISTS reason_factors  JSONB;
