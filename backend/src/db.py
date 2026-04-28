"""
Database connection + thin repository layer.

We use psycopg (psycopg3) directly with a single shared connection pool. No ORM.
The schema is small and stable; raw SQL is more honest about what's happening
and gives us better error messages when migrations need to evolve.

For local dev: set DATABASE_URL in .env (postgres://...). For Railway: it's
injected automatically via the Postgres plugin.
"""
from __future__ import annotations
import os
import logging
from contextlib import contextmanager
from typing import Optional, Iterator, Any
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

log = logging.getLogger(__name__)

_pool: Optional[ConnectionPool] = None


def _dsn() -> str:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set. Required for db connection.")
    # Supabase sometimes hands out postgres:// while psycopg3 wants postgresql://
    if dsn.startswith("postgres://"):
        dsn = "postgresql://" + dsn[len("postgres://"):]
    return dsn


def init_pool(min_size: int = 1, max_size: int = 10) -> ConnectionPool:
    global _pool
    if _pool is None:
        _pool = ConnectionPool(_dsn(), min_size=min_size, max_size=max_size,
                                kwargs={"row_factory": dict_row})
    return _pool


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


@contextmanager
def conn() -> Iterator[psycopg.Connection]:
    pool = init_pool()
    with pool.connection() as c:
        yield c


def fetchall(sql: str, params: tuple | dict | None = None) -> list[dict]:
    with conn() as c, c.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()


def fetchone(sql: str, params: tuple | dict | None = None) -> Optional[dict]:
    with conn() as c, c.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchone()


def execute(sql: str, params: tuple | dict | None = None) -> int:
    with conn() as c, c.cursor() as cur:
        cur.execute(sql, params or ())
        c.commit()
        return cur.rowcount


def execute_many(sql: str, rows: list[tuple] | list[dict]) -> int:
    if not rows:
        return 0
    with conn() as c, c.cursor() as cur:
        cur.executemany(sql, rows)
        c.commit()
        return cur.rowcount


def upsert_pitcher_xstats(rows: list[dict]) -> int:
    """Bulk upsert by (mlb_id, season_year)."""
    if not rows:
        return 0
    sql = """
        INSERT INTO pitcher_xstats
          (mlb_id, season_year, last_first, pa, bip, ba, est_ba, slg, est_slg,
           woba, est_woba, era, xera, refreshed_at)
        VALUES
          (%(mlb_id)s, %(season_year)s, %(last_first)s, %(pa)s, %(bip)s,
           %(ba)s, %(est_ba)s, %(slg)s, %(est_slg)s,
           %(woba)s, %(est_woba)s, %(era)s, %(xera)s, now())
        ON CONFLICT (mlb_id, season_year) DO UPDATE SET
          last_first = EXCLUDED.last_first,
          pa = EXCLUDED.pa, bip = EXCLUDED.bip,
          ba = EXCLUDED.ba, est_ba = EXCLUDED.est_ba,
          slg = EXCLUDED.slg, est_slg = EXCLUDED.est_slg,
          woba = EXCLUDED.woba, est_woba = EXCLUDED.est_woba,
          era = EXCLUDED.era, xera = EXCLUDED.xera,
          refreshed_at = now();
    """
    return execute_many(sql, rows)


def upsert_hitter_xstats(rows: list[dict]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO hitter_xstats
          (mlb_id, season_year, last_first, pa, ba, est_ba, slg, est_slg,
           woba, est_woba, refreshed_at)
        VALUES
          (%(mlb_id)s, %(season_year)s, %(last_first)s, %(pa)s,
           %(ba)s, %(est_ba)s, %(slg)s, %(est_slg)s,
           %(woba)s, %(est_woba)s, now())
        ON CONFLICT (mlb_id, season_year) DO UPDATE SET
          last_first = EXCLUDED.last_first,
          pa = EXCLUDED.pa,
          ba = EXCLUDED.ba, est_ba = EXCLUDED.est_ba,
          slg = EXCLUDED.slg, est_slg = EXCLUDED.est_slg,
          woba = EXCLUDED.woba, est_woba = EXCLUDED.est_woba,
          refreshed_at = now();
    """
    return execute_many(sql, rows)


def upsert_team_xstats(rows: list[dict]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO team_xstats
          (team_code, season_year, pa, woba, est_woba, refreshed_at)
        VALUES
          (%(team_code)s, %(season_year)s, %(pa)s, %(woba)s, %(est_woba)s, now())
        ON CONFLICT (team_code, season_year) DO UPDATE SET
          pa = EXCLUDED.pa,
          woba = EXCLUDED.woba, est_woba = EXCLUDED.est_woba,
          refreshed_at = now();
    """
    return execute_many(sql, rows)


def upsert_game(g: dict) -> None:
    sql = """
        INSERT INTO games (
          game_pk, game_date, game_time_et, status,
          away_team, home_team, away_record, home_record, park_code,
          away_pitcher_id, home_pitcher_id, away_pitcher_hand, home_pitcher_hand,
          away_pitcher_name, home_pitcher_name,
          away_score, home_score,
          weather_condition, weather_temp_f, weather_wind,
          refreshed_at
        ) VALUES (
          %(game_pk)s, %(game_date)s, %(game_time_et)s, %(status)s,
          %(away_team)s, %(home_team)s, %(away_record)s, %(home_record)s, %(park_code)s,
          %(away_pitcher_id)s, %(home_pitcher_id)s, %(away_pitcher_hand)s, %(home_pitcher_hand)s,
          %(away_pitcher_name)s, %(home_pitcher_name)s,
          %(away_score)s, %(home_score)s,
          %(weather_condition)s, %(weather_temp_f)s, %(weather_wind)s,
          now()
        )
        ON CONFLICT (game_pk) DO UPDATE SET
          status = EXCLUDED.status,
          away_record = EXCLUDED.away_record,
          home_record = EXCLUDED.home_record,
          away_pitcher_id = EXCLUDED.away_pitcher_id,
          home_pitcher_id = EXCLUDED.home_pitcher_id,
          away_pitcher_hand = EXCLUDED.away_pitcher_hand,
          home_pitcher_hand = EXCLUDED.home_pitcher_hand,
          away_pitcher_name = EXCLUDED.away_pitcher_name,
          home_pitcher_name = EXCLUDED.home_pitcher_name,
          away_score = EXCLUDED.away_score,
          home_score = EXCLUDED.home_score,
          weather_condition = EXCLUDED.weather_condition,
          weather_temp_f = EXCLUDED.weather_temp_f,
          weather_wind = EXCLUDED.weather_wind,
          refreshed_at = now();
    """
    execute(sql, g)


def replace_lineups(game_pk: int, team_code: str, spots: list[dict]) -> None:
    """Atomic replacement: delete existing lineup for this team and insert fresh."""
    with conn() as c, c.cursor() as cur:
        cur.execute(
            "DELETE FROM lineup_spots WHERE game_pk = %s AND team_code = %s",
            (game_pk, team_code)
        )
        if spots:
            cur.executemany(
                """
                INSERT INTO lineup_spots
                  (game_pk, team_code, batting_order, mlb_id, full_name,
                   last_first, bat_side, position, refreshed_at)
                VALUES
                  (%(game_pk)s, %(team_code)s, %(batting_order)s, %(mlb_id)s,
                   %(full_name)s, %(last_first)s, %(bat_side)s, %(position)s, now())
                """,
                [{**s, "game_pk": game_pk, "team_code": team_code} for s in spots]
            )
        c.commit()


def create_projection_run(run_date: str, model_version: str,
                           trigger: str, n_games: int) -> int:
    row = fetchone(
        """
        INSERT INTO projection_runs (run_date, model_version, trigger, n_games)
        VALUES (%s, %s, %s, %s)
        RETURNING run_id
        """,
        (run_date, model_version, trigger, n_games),
    )
    return int(row["run_id"])  # type: ignore


def insert_pitcher_projection(run_id: int, p: dict) -> None:
    sql = """
        INSERT INTO pitcher_projections (
          run_id, game_pk, mlb_id, last_first, team_code, opp_team_code,
          hand, source, pa_sample, era, xera, true_era, xwoba_against,
          opp_lineup_xwoba, used_actual_lineup,
          ip, outs, hits, er, bb, k, wx_factor, pf_factor
        ) VALUES (
          %(run_id)s, %(game_pk)s, %(mlb_id)s, %(last_first)s, %(team_code)s,
          %(opp_team_code)s, %(hand)s, %(source)s, %(pa_sample)s,
          %(era)s, %(xera)s, %(true_era)s, %(xwoba_against)s,
          %(opp_lineup_xwoba)s, %(used_actual_lineup)s,
          %(ip)s, %(outs)s, %(hits)s, %(er)s, %(bb)s, %(k)s,
          %(wx_factor)s, %(pf_factor)s
        )
    """
    execute(sql, {**p, "run_id": run_id})


def insert_game_projection(run_id: int, g: dict) -> None:
    sql = """
        INSERT INTO game_projections (
          run_id, game_pk, proj_total, proj_f5, market_total, edge_total,
          lean, confidence_tier
        ) VALUES (
          %(run_id)s, %(game_pk)s, %(proj_total)s, %(proj_f5)s,
          %(market_total)s, %(edge_total)s, %(lean)s, %(confidence_tier)s
        )
    """
    execute(sql, {**g, "run_id": run_id})


def insert_edge(run_id: int, e: dict) -> int:
    row = fetchone(
        """
        INSERT INTO edges (
          run_id, game_pk, kind, category, pitcher_mlb_id, pitcher_name,
          team_code, opp_team_code, line, proj_value, edge, lean,
          confidence_tier, flagged, notes
        ) VALUES (
          %(run_id)s, %(game_pk)s, %(kind)s, %(category)s, %(pitcher_mlb_id)s,
          %(pitcher_name)s, %(team_code)s, %(opp_team_code)s, %(line)s,
          %(proj_value)s, %(edge)s, %(lean)s, %(confidence_tier)s, %(flagged)s,
          %(notes)s
        )
        RETURNING edge_id
        """,
        {**e, "run_id": run_id},
    )
    return int(row["edge_id"])  # type: ignore


def get_latest_run(run_date: str) -> Optional[dict]:
    return fetchone(
        """
        SELECT * FROM projection_runs
        WHERE run_date = %s
        ORDER BY run_started_at DESC
        LIMIT 1
        """,
        (run_date,),
    )


def get_edges_for_run(run_id: int, flagged_only: bool = True) -> list[dict]:
    sql = "SELECT * FROM edges WHERE run_id = %s"
    if flagged_only:
        sql += " AND flagged = TRUE"
    sql += " ORDER BY ABS(edge) DESC"
    return fetchall(sql, (run_id,))


def log_job_start(job_name: str) -> int:
    row = fetchone(
        "INSERT INTO job_runs (job_name, status) VALUES (%s, 'running') RETURNING job_id",
        (job_name,),
    )
    return int(row["job_id"])  # type: ignore


def log_job_finish(job_id: int, status: str = "success",
                    error: Optional[str] = None,
                    payload: Optional[dict] = None) -> None:
    import json as _json
    execute(
        """
        UPDATE job_runs SET finished_at = now(), status = %s, error = %s,
                            payload = %s::jsonb
        WHERE job_id = %s
        """,
        (status, error, _json.dumps(payload or {}), job_id),
    )
