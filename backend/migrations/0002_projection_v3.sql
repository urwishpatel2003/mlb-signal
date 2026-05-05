-- Migration 0002: projection engine v3.0 schema additions
ALTER TABLE pitcher_xstats
    ADD COLUMN IF NOT EXISTS xfip    NUMERIC(5, 2),
    ADD COLUMN IF NOT EXISTS k_pct   NUMERIC(5, 4),
    ADD COLUMN IF NOT EXISTS bb9     NUMERIC(5, 2);

ALTER TABLE hitter_xstats
    ADD COLUMN IF NOT EXISTS l15_woba NUMERIC(5, 4);

CREATE TABLE IF NOT EXISTS hitter_splits (
    mlb_id       INTEGER  NOT NULL,
    season_year  INTEGER  NOT NULL,
    vs_hand      CHAR(1)  NOT NULL,
    pa           INTEGER  NOT NULL,
    est_woba     NUMERIC(5, 4),
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (mlb_id, season_year, vs_hand)
);

ALTER TABLE pitcher_projections
    ADD COLUMN IF NOT EXISTS xfip               NUMERIC(5, 2),
    ADD COLUMN IF NOT EXISTS used_l15_blend     BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS high_variance_flag BOOLEAN NOT NULL DEFAULT FALSE;