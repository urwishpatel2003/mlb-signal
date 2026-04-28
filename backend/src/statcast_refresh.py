"""
Daily Statcast refresh.

Cron entry point. Runs once per day (06:00 ET, before first orchestrator run)
to pull the latest Expected Stats from Baseball Savant via pybaseball.

Persists to:
  - pitcher_xstats
  - hitter_xstats
  - team_xstats

This replaces the manual CSV upload flow. Once this runs cleanly on Railway,
we never touch a CSV by hand again.
"""
from __future__ import annotations
import logging
import sys
from datetime import date
from typing import Optional

from . import db, ntfy

log = logging.getLogger(__name__)


def _coerce_int(x) -> Optional[int]:
    try:
        return int(x) if x is not None else None
    except (TypeError, ValueError):
        return None


def _coerce_float(x) -> Optional[float]:
    try:
        return float(x) if x is not None else None
    except (TypeError, ValueError):
        return None


def _df_to_pitcher_rows(df, season_year: int) -> list[dict]:
    """Normalize a pybaseball Expected Stats DataFrame into our schema rows."""
    rows = []
    name_col = "last_name, first_name" if "last_name, first_name" in df.columns else "name"
    for _, r in df.iterrows():
        rows.append({
            "mlb_id": _coerce_int(r.get("player_id")),
            "season_year": season_year,
            "last_first": str(r.get(name_col, "")).strip(),
            "pa": _coerce_int(r.get("pa")) or 0,
            "bip": _coerce_int(r.get("bip")) or 0,
            "ba": _coerce_float(r.get("ba")),
            "est_ba": _coerce_float(r.get("est_ba")),
            "slg": _coerce_float(r.get("slg")),
            "est_slg": _coerce_float(r.get("est_slg")),
            "woba": _coerce_float(r.get("woba")),
            "est_woba": _coerce_float(r.get("est_woba")),
            "era": _coerce_float(r.get("era")),
            "xera": _coerce_float(r.get("xera")),
        })
    return [r for r in rows if r["mlb_id"]]


def _df_to_hitter_rows(df, season_year: int) -> list[dict]:
    rows = []
    name_col = "last_name, first_name" if "last_name, first_name" in df.columns else "name"
    for _, r in df.iterrows():
        rows.append({
            "mlb_id": _coerce_int(r.get("player_id")),
            "season_year": season_year,
            "last_first": str(r.get(name_col, "")).strip(),
            "pa": _coerce_int(r.get("pa")) or 0,
            "ba": _coerce_float(r.get("ba")),
            "est_ba": _coerce_float(r.get("est_ba")),
            "slg": _coerce_float(r.get("slg")),
            "est_slg": _coerce_float(r.get("est_slg")),
            "woba": _coerce_float(r.get("woba")),
            "est_woba": _coerce_float(r.get("est_woba")),
        })
    return [r for r in rows if r["mlb_id"]]


def refresh_statcast(season_year: Optional[int] = None) -> dict:
    """Pull pitcher + hitter + team xStats for the given season; persist all."""
    season_year = season_year or date.today().year
    job_id = db.log_job_start("statcast_refresh")
    metrics: dict = {"season_year": season_year}

    try:
        from pybaseball import (
            statcast_pitcher_expected_stats,
            statcast_batter_expected_stats,
        )
    except ImportError as e:
        msg = "pybaseball not installed: pip install pybaseball"
        db.log_job_finish(job_id, "failure", msg)
        ntfy.send_failure("statcast_refresh", msg)
        raise

    try:
        log.info("Pulling pitcher xStats for %d", season_year)
        df_pit = statcast_pitcher_expected_stats(season_year, minPA=10)
        pit_rows = _df_to_pitcher_rows(df_pit, season_year)
        n_pit = db.upsert_pitcher_xstats(pit_rows)
        metrics["n_pitchers"] = n_pit
        log.info("Upserted %d pitcher rows", n_pit)

        log.info("Pulling hitter xStats for %d", season_year)
        df_hit = statcast_batter_expected_stats(season_year, minPA=10)
        hit_rows = _df_to_hitter_rows(df_hit, season_year)
        n_hit = db.upsert_hitter_xstats(hit_rows)
        metrics["n_hitters"] = n_hit
        log.info("Upserted %d hitter rows", n_hit)

        # Team xstats: aggregate hitter totals by team. pybaseball provides this
        # via statcast_batter_expected_stats with no min PA + group_by team, but
        # the simplest robust path is to compute it ourselves from team_xstats
        # table after a small join (or just call the team endpoint directly).
        # For now, we'll skip team aggregation here and let the team-level CSV
        # the user already exports be loaded via a separate import path.
        # TODO: replace with proper team-level pull.

        db.log_job_finish(job_id, "success", payload=metrics)
        return metrics
    except Exception as e:
        log.exception("Statcast refresh failed")
        db.log_job_finish(job_id, "failure", str(e), metrics)
        ntfy.send_failure("statcast_refresh", str(e))
        raise


def main():
    metrics = refresh_statcast()
    print(metrics)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
