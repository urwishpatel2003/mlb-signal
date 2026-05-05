-- Migration 0003: F5 totals + moneyline support
-- Safe to run multiple times (IF NOT EXISTS / ADD COLUMN IF NOT EXISTS)

-- ============================================================================
-- games: add F5 market lines + ML odds (already have away_ml / home_ml)
-- ============================================================================
ALTER TABLE games
    ADD COLUMN IF NOT EXISTS market_f5_total        NUMERIC(4, 1),
    ADD COLUMN IF NOT EXISTS market_f5_over_price   INTEGER,
    ADD COLUMN IF NOT EXISTS market_f5_under_price  INTEGER;

-- ============================================================================
-- game_projections: add F5 edge + ML win probability columns
-- ============================================================================
ALTER TABLE game_projections
    ADD COLUMN IF NOT EXISTS proj_home_runs    NUMERIC(5, 2),
    ADD COLUMN IF NOT EXISTS proj_away_runs    NUMERIC(5, 2),
    ADD COLUMN IF NOT EXISTS market_f5_total   NUMERIC(4, 1),
    ADD COLUMN IF NOT EXISTS edge_f5           NUMERIC(5, 2),
    ADD COLUMN IF NOT EXISTS lean_f5           VARCHAR(8),
    ADD COLUMN IF NOT EXISTS home_win_prob     NUMERIC(5, 4),
    ADD COLUMN IF NOT EXISTS away_win_prob     NUMERIC(5, 4),
    ADD COLUMN IF NOT EXISTS away_ml           INTEGER,
    ADD COLUMN IF NOT EXISTS home_ml           INTEGER,
    ADD COLUMN IF NOT EXISTS away_ml_implied   NUMERIC(5, 4),
    ADD COLUMN IF NOT EXISTS home_ml_implied   NUMERIC(5, 4),
    ADD COLUMN IF NOT EXISTS ml_edge_team      VARCHAR(4),   -- team code with edge, or NULL
    ADD COLUMN IF NOT EXISTS ml_edge_pct       NUMERIC(5, 4); -- model prob - implied prob

-- ============================================================================
-- edges: add conviction_pct column if not already there (from earlier work)
-- ============================================================================
ALTER TABLE edges
    ADD COLUMN IF NOT EXISTS conviction_pct NUMERIC(5, 1);
