-- Migration 0004: ML model improvements
-- Improvements: HFA, 7-day bullpen ERA, days rest, xFIP from components, offensive scaler
-- Safe to run multiple times (ADD COLUMN IF NOT EXISTS)

-- pitcher_xstats: days since last start + xFIP components
ALTER TABLE pitcher_xstats
    ADD COLUMN IF NOT EXISTS last_start_date   DATE,
    ADD COLUMN IF NOT EXISTS days_rest         INTEGER,   -- days since last start as of refresh date
    ADD COLUMN IF NOT EXISTS fb_pct            NUMERIC(5, 4),  -- fly ball rate (for xFIP)
    ADD COLUMN IF NOT EXISTS hr_fb_rate        NUMERIC(5, 4);  -- HR/FB rate (for xFIP)

-- team_xstats: 7-day rolling bullpen ERA + offensive strength scaler
ALTER TABLE team_xstats
    ADD COLUMN IF NOT EXISTS bullpen_era_l7    NUMERIC(5, 2),  -- last-7-days bullpen ERA
    ADD COLUMN IF NOT EXISTS bullpen_ip_l7     NUMERIC(5, 1),  -- innings in that window
    ADD COLUMN IF NOT EXISTS team_wrc_plus     INTEGER,        -- wRC+ (offensive strength vs league)
    ADD COLUMN IF NOT EXISTS team_xwoba        NUMERIC(5, 4);  -- team season xwOBA (offensive)

-- game_projections: store HFA adjustment applied
ALTER TABLE game_projections
    ADD COLUMN IF NOT EXISTS hfa_applied       NUMERIC(4, 3);  -- HFA adjustment used (e.g. 0.025)
