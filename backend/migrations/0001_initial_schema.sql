-- mlb-signal database schema
-- Apply via Alembic in production; reproduced here for review and local dev.
-- Compatible with Postgres 14+ (Supabase default).

-- ============================================================================
-- DIMENSION TABLES (slowly-changing master data)
-- ============================================================================

CREATE TABLE IF NOT EXISTS teams (
    team_code        VARCHAR(4) PRIMARY KEY,         -- "BOS", "ATH", etc.
    full_name        TEXT NOT NULL,
    league           VARCHAR(2),                     -- "AL" | "NL"
    division         VARCHAR(20),
    mlb_id           INTEGER,                        -- MLB Stats API teamId
    home_park_code   VARCHAR(4),
    updated_at       TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS parks (
    park_code        VARCHAR(4) PRIMARY KEY,         -- "CIN" for GABP
    name             TEXT NOT NULL,
    city             TEXT,
    lat              NUMERIC(8, 5),
    lon              NUMERIC(8, 5),
    cf_azimuth_deg   NUMERIC(5, 1),                  -- compass bearing of CF
    elevation_ft     INTEGER,
    roof_type        VARCHAR(20),                    -- "open"|"dome"|"retractable"
    -- Park factors (100 = neutral)
    pf_runs          NUMERIC(5, 2) DEFAULT 100,
    pf_hr            NUMERIC(5, 2) DEFAULT 100,
    pf_so            NUMERIC(5, 2) DEFAULT 100,
    pf_bb            NUMERIC(5, 2) DEFAULT 100,
    pf_runs_lhb      NUMERIC(5, 2),                  -- handedness splits
    pf_runs_rhb      NUMERIC(5, 2),
    season_year      INTEGER NOT NULL,               -- factors are season-specific
    updated_at       TIMESTAMPTZ DEFAULT now(),
    UNIQUE(park_code, season_year)
);

-- ============================================================================
-- STATCAST DATA (refreshed daily via pybaseball)
-- ============================================================================

CREATE TABLE IF NOT EXISTS pitcher_xstats (
    mlb_id           INTEGER NOT NULL,
    season_year      INTEGER NOT NULL,
    last_first       TEXT NOT NULL,                  -- "Crochet, Garrett"
    pa               INTEGER NOT NULL DEFAULT 0,
    bip              INTEGER NOT NULL DEFAULT 0,
    ba               NUMERIC(5, 4),
    est_ba           NUMERIC(5, 4),
    slg              NUMERIC(5, 4),
    est_slg          NUMERIC(5, 4),
    woba             NUMERIC(5, 4),
    est_woba         NUMERIC(5, 4),
    era              NUMERIC(5, 2),
    xera             NUMERIC(5, 2),
    refreshed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (mlb_id, season_year)
);

CREATE INDEX IF NOT EXISTS idx_pitcher_xstats_name ON pitcher_xstats (last_first);

CREATE TABLE IF NOT EXISTS hitter_xstats (
    mlb_id           INTEGER NOT NULL,
    season_year      INTEGER NOT NULL,
    last_first       TEXT NOT NULL,
    pa               INTEGER NOT NULL DEFAULT 0,
    ba               NUMERIC(5, 4),
    est_ba           NUMERIC(5, 4),
    slg              NUMERIC(5, 4),
    est_slg          NUMERIC(5, 4),
    woba             NUMERIC(5, 4),
    est_woba         NUMERIC(5, 4),
    refreshed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (mlb_id, season_year)
);

CREATE INDEX IF NOT EXISTS idx_hitter_xstats_name ON hitter_xstats (last_first);

CREATE TABLE IF NOT EXISTS team_xstats (
    team_code        VARCHAR(4) NOT NULL,
    season_year      INTEGER NOT NULL,
    pa               INTEGER NOT NULL DEFAULT 0,
    woba             NUMERIC(5, 4),
    est_woba         NUMERIC(5, 4),
    refreshed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (team_code, season_year)
);

-- Player-level handedness splits (when available)
CREATE TABLE IF NOT EXISTS hitter_splits (
    mlb_id           INTEGER NOT NULL,
    season_year      INTEGER NOT NULL,
    vs_hand          CHAR(1) NOT NULL,               -- "L" | "R" (vs that pitcher hand)
    pa               INTEGER NOT NULL,
    est_woba         NUMERIC(5, 4),
    refreshed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (mlb_id, season_year, vs_hand)
);

-- ============================================================================
-- LIVE GAME DATA (refreshed multiple times per day)
-- ============================================================================

CREATE TABLE IF NOT EXISTS games (
    game_pk          INTEGER PRIMARY KEY,            -- MLB Stats API canonical ID
    game_date        DATE NOT NULL,
    game_time_et     VARCHAR(8),
    status           VARCHAR(20) NOT NULL,           -- "Scheduled"|"Live"|"Final"|"Postponed"
    away_team        VARCHAR(4) NOT NULL REFERENCES teams(team_code),
    home_team        VARCHAR(4) NOT NULL REFERENCES teams(team_code),
    away_record      VARCHAR(10),
    home_record      VARCHAR(10),
    park_code        VARCHAR(4) REFERENCES parks(park_code),
    away_pitcher_id  INTEGER,
    home_pitcher_id  INTEGER,
    away_pitcher_hand CHAR(1),
    home_pitcher_hand CHAR(1),
    away_pitcher_name TEXT,
    home_pitcher_name TEXT,
    -- Final scores (populated after game ends)
    away_score       INTEGER,
    home_score       INTEGER,
    -- Market lines (joined from odds API)
    market_total     NUMERIC(4, 1),
    market_total_over_price  INTEGER,                -- e.g. -110
    market_total_under_price INTEGER,
    away_ml          INTEGER,
    home_ml          INTEGER,
    last_line_check  TIMESTAMPTZ,
    -- Weather (from MLB Stats API + NWS)
    weather_condition TEXT,
    weather_temp_f   INTEGER,
    weather_wind     TEXT,                            -- raw "10 mph, Out To CF"
    weather_wind_mph INTEGER,
    weather_wind_deg INTEGER,                         -- compass FROM bearing
    weather_precip_pct INTEGER,
    refreshed_at     TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_games_date ON games (game_date);
CREATE INDEX IF NOT EXISTS idx_games_status ON games (status);

CREATE TABLE IF NOT EXISTS lineup_spots (
    game_pk          INTEGER NOT NULL REFERENCES games(game_pk) ON DELETE CASCADE,
    team_code        VARCHAR(4) NOT NULL,
    batting_order    INTEGER NOT NULL,
    mlb_id           INTEGER NOT NULL,
    full_name        TEXT NOT NULL,
    last_first       TEXT NOT NULL,
    bat_side         CHAR(1) NOT NULL,
    position         VARCHAR(4),
    refreshed_at     TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (game_pk, team_code, batting_order)
);

-- ============================================================================
-- MODEL OUTPUTS (immutable history; we never UPDATE, only INSERT)
-- ============================================================================

CREATE TABLE IF NOT EXISTS projection_runs (
    run_id           SERIAL PRIMARY KEY,
    run_date         DATE NOT NULL,
    run_started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    model_version    VARCHAR(20) NOT NULL,
    trigger          VARCHAR(40),                    -- "morning"|"line_move"|"lineup_confirm"|...
    n_games          INTEGER,
    n_edges          INTEGER,
    notes            TEXT
);

CREATE TABLE IF NOT EXISTS pitcher_projections (
    run_id           INTEGER NOT NULL REFERENCES projection_runs(run_id) ON DELETE CASCADE,
    game_pk          INTEGER NOT NULL,
    mlb_id           INTEGER NOT NULL,
    last_first       TEXT NOT NULL,
    team_code        VARCHAR(4) NOT NULL,
    opp_team_code    VARCHAR(4) NOT NULL,
    hand             CHAR(1) NOT NULL,
    source           VARCHAR(15) NOT NULL,            -- "statcast"|"low_sample"|"league_avg"
    pa_sample        INTEGER,
    -- Statcast inputs
    era              NUMERIC(5, 2),
    xera             NUMERIC(5, 2),
    true_era         NUMERIC(5, 2),
    xwoba_against    NUMERIC(5, 4),
    opp_lineup_xwoba NUMERIC(5, 4),                   -- weighted from lineup if available, else team
    used_actual_lineup BOOLEAN NOT NULL DEFAULT FALSE,
    -- Adjusted projections
    ip               NUMERIC(4, 2),
    outs             NUMERIC(5, 2),
    hits             NUMERIC(4, 2),
    er               NUMERIC(4, 2),
    bb               NUMERIC(4, 2),
    k                NUMERIC(4, 2),
    -- Adjustment factors applied
    wx_factor        NUMERIC(5, 3),
    pf_factor        NUMERIC(5, 3),
    PRIMARY KEY (run_id, mlb_id, game_pk)
);

CREATE TABLE IF NOT EXISTS game_projections (
    run_id           INTEGER NOT NULL REFERENCES projection_runs(run_id) ON DELETE CASCADE,
    game_pk          INTEGER NOT NULL,
    proj_total       NUMERIC(5, 2),
    proj_f5          NUMERIC(5, 2),
    market_total     NUMERIC(4, 1),
    edge_total       NUMERIC(5, 2),
    lean             VARCHAR(8),                      -- "OVER"|"UNDER"|"PASS"
    confidence_tier  INTEGER,                         -- 1, 2, 3, or NULL
    PRIMARY KEY (run_id, game_pk)
);

CREATE TABLE IF NOT EXISTS edges (
    edge_id          SERIAL PRIMARY KEY,
    run_id           INTEGER NOT NULL REFERENCES projection_runs(run_id) ON DELETE CASCADE,
    game_pk          INTEGER NOT NULL,
    kind             VARCHAR(10) NOT NULL,            -- "total"|"prop"
    category         VARCHAR(10) NOT NULL,            -- "Total"|"K"|"Hits"|"ER"|"Outs"|"BB"
    pitcher_mlb_id   INTEGER,                         -- NULL for totals
    pitcher_name     TEXT,
    team_code        VARCHAR(4),
    opp_team_code    VARCHAR(4),
    line             NUMERIC(5, 1) NOT NULL,
    proj_value       NUMERIC(5, 2) NOT NULL,
    edge             NUMERIC(5, 2) NOT NULL,
    lean             VARCHAR(8) NOT NULL,
    confidence_tier  INTEGER,
    flagged          BOOLEAN NOT NULL DEFAULT FALSE,  -- crossed conviction threshold
    notes            TEXT
);

CREATE INDEX IF NOT EXISTS idx_edges_run ON edges (run_id);
CREATE INDEX IF NOT EXISTS idx_edges_flagged ON edges (run_id, flagged);

-- ============================================================================
-- GRADING (populated by nightly grader, joins to projections for backtests)
-- ============================================================================

CREATE TABLE IF NOT EXISTS pitcher_actuals (
    game_pk          INTEGER NOT NULL,
    mlb_id           INTEGER NOT NULL,
    last_first       TEXT NOT NULL,
    ip               NUMERIC(4, 2),
    outs             INTEGER,
    h                INTEGER,
    er               INTEGER,
    bb               INTEGER,
    k                INTEGER,
    pitches          INTEGER,
    bf               INTEGER,
    refreshed_at     TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (game_pk, mlb_id)
);

CREATE TABLE IF NOT EXISTS edge_results (
    edge_id          INTEGER PRIMARY KEY REFERENCES edges(edge_id) ON DELETE CASCADE,
    actual_value     NUMERIC(5, 2),
    result           VARCHAR(8),                      -- "WIN"|"LOSS"|"PUSH"|"NO_ACTION"
    profit_units     NUMERIC(5, 2),                   -- assumes -110 unless overridden
    graded_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Materialized view recomputed nightly: rolling performance metrics
CREATE TABLE IF NOT EXISTS model_performance (
    snapshot_date    DATE PRIMARY KEY,                -- one row per night
    window_days      INTEGER NOT NULL,                -- 7, 14, 30, all
    flagged_plays    INTEGER NOT NULL,
    wins             INTEGER NOT NULL,
    losses           INTEGER NOT NULL,
    pushes           INTEGER NOT NULL,
    hit_rate         NUMERIC(5, 4),
    profit_units     NUMERIC(8, 2),
    roi              NUMERIC(6, 4),
    -- Calibration
    games_with_actuals INTEGER,
    model_mae        NUMERIC(5, 3),
    market_mae       NUMERIC(5, 3),
    model_rmse       NUMERIC(5, 3),
    market_rmse      NUMERIC(5, 3),
    notes            TEXT
);

-- ============================================================================
-- OPERATIONAL
-- ============================================================================

CREATE TABLE IF NOT EXISTS job_runs (
    job_id           SERIAL PRIMARY KEY,
    job_name         VARCHAR(40) NOT NULL,            -- "statcast_refresh"|"orchestrator"|"grader"|"line_watcher"
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at      TIMESTAMPTZ,
    status           VARCHAR(15),                     -- "running"|"success"|"failure"
    error            TEXT,
    payload          JSONB                            -- arbitrary metrics, counts, etc.
);

CREATE INDEX IF NOT EXISTS idx_job_runs_name_started ON job_runs (job_name, started_at DESC);

CREATE TABLE IF NOT EXISTS notifications (
    notification_id  SERIAL PRIMARY KEY,
    sent_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    channel          VARCHAR(20),                     -- "ntfy"|"slack"|"email"
    topic            VARCHAR(50),                     -- "edges_ready"|"lineup_change"|"job_failure"
    title            TEXT,
    body             TEXT,
    delivered        BOOLEAN
);
