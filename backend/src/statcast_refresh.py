"""
Daily Statcast refresh.

Cron entry point. Runs once per day (06:00 ET, before first orchestrator run)
to pull the latest Expected Stats from Baseball Savant via pybaseball.

Persists to:
  - pitcher_xstats  (now includes pitch-budget fields: TBF, pitches, GS, etc.)
  - hitter_xstats   (now includes pitches_per_pa for lineup-aware projection)
  - team_xstats

In addition to Statcast Expected Stats, this also pulls FanGraphs season aggregates
(via pybaseball.pitching_stats / batting_stats) so we have:
  - Pitcher's avg pitches per start
  - Pitcher's pitches per PA
  - Hitter's pitches per PA
These drive the new pitch-budget IP projection model.
"""
from __future__ import annotations
import logging
from datetime import date
from typing import Optional

from . import db, ntfy

log = logging.getLogger(__name__)


# ---------- Coercion helpers ----------

def _coerce_int(x) -> Optional[int]:
    try:
        return int(x) if x is not None else None
    except (TypeError, ValueError):
        return None


def _coerce_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        f = float(x)
        # pybaseball sometimes returns nan
        if f != f:  # nan check
            return None
        return f
    except (TypeError, ValueError):
        return None


# ---------- Statcast Expected Stats normalization ----------

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


# ---------- FanGraphs aggregates -> pitch-budget fields ----------

def _df_to_pitcher_budget_rows(df, season_year: int) -> list[dict]:
    """
    From pybaseball.pitching_stats(season), extract per-pitcher aggregates needed
    for the pitch-budget IP model. Returns rows keyed by (mlbam_id, season_year).
    Names from FanGraphs use 'IDfg' but mlbam_id is in 'xMLBAMID'.
    """
    rows = []
    if df is None or len(df) == 0:
        return rows

    # Detect column names defensively - FanGraphs has occasional renames
    id_col = "xMLBAMID" if "xMLBAMID" in df.columns else "MLBAMID" if "MLBAMID" in df.columns else None
    if id_col is None:
        log.warning("FanGraphs pitching_stats has no MLBAM id column; skipping budget fields")
        return rows

    for _, r in df.iterrows():
        mlb_id = _coerce_int(r.get(id_col))
        if not mlb_id:
            continue
        gs = _coerce_int(r.get("GS")) or 0
        tbf = _coerce_int(r.get("TBF")) or 0
        pitches = _coerce_int(r.get("Pitches")) or 0
        ip = _coerce_float(r.get("IP")) or 0.0
        # Strikeouts / Walks - FG sometimes uses 'SO', sometimes 'K'
        so = _coerce_int(r.get("SO")) or _coerce_int(r.get("K")) or 0
        bb = _coerce_int(r.get("BB")) or 0

        avg_pitches_per_start = (pitches / gs) if gs > 0 else None
        pitches_per_pa = (pitches / tbf) if tbf > 0 else None
        k_pct = (so / tbf) if tbf > 0 else None
        bb_pct = (bb / tbf) if tbf > 0 else None

        rows.append({
            "mlb_id": mlb_id,
            "season_year": season_year,
            "gs": gs or None,
            "ip_total": ip or None,
            "tbf": tbf or None,
            "pitches_total": pitches or None,
            "avg_pitches_per_start": _coerce_float(avg_pitches_per_start),
            "pitches_per_pa": _coerce_float(pitches_per_pa),
            "k_pct": _coerce_float(k_pct),
            "bb_pct": _coerce_float(bb_pct),
        })
    return rows


def _df_to_hitter_budget_rows(df, season_year: int) -> list[dict]:
    """
    From pybaseball.batting_stats(season), extract per-hitter aggregates needed
    for the pitch-budget IP model.
    """
    rows = []
    if df is None or len(df) == 0:
        return rows

    id_col = "xMLBAMID" if "xMLBAMID" in df.columns else "MLBAMID" if "MLBAMID" in df.columns else None
    if id_col is None:
        log.warning("FanGraphs batting_stats has no MLBAM id column; skipping budget fields")
        return rows

    for _, r in df.iterrows():
        mlb_id = _coerce_int(r.get(id_col))
        if not mlb_id:
            continue
        pa = _coerce_int(r.get("PA")) or 0
        pitches = _coerce_int(r.get("Pitches")) or 0
        so = _coerce_int(r.get("SO")) or _coerce_int(r.get("K")) or 0
        bb = _coerce_int(r.get("BB")) or 0

        pitches_per_pa = (pitches / pa) if pa > 0 else None
        k_pct = (so / pa) if pa > 0 else None
        bb_pct = (bb / pa) if pa > 0 else None

        rows.append({
            "mlb_id": mlb_id,
            "season_year": season_year,
            "pitches_total": pitches or None,
            "pitches_per_pa": _coerce_float(pitches_per_pa),
            "k_pct": _coerce_float(k_pct),
            "bb_pct": _coerce_float(bb_pct),
        })
    return rows


# ---------- Persistence helpers for the budget fields ----------

def _upsert_pitcher_budget(rows: list[dict]) -> int:
    """Update only the pitch-budget columns; doesn't touch xStats columns."""
    if not rows:
        return 0
    sql = """
        UPDATE pitcher_xstats SET
          gs = %(gs)s,
          ip_total = %(ip_total)s,
          tbf = %(tbf)s,
          pitches_total = %(pitches_total)s,
          avg_pitches_per_start = %(avg_pitches_per_start)s,
          pitches_per_pa = %(pitches_per_pa)s,
          k_pct = %(k_pct)s,
          bb_pct = %(bb_pct)s,
          refreshed_at = now()
        WHERE mlb_id = %(mlb_id)s AND season_year = %(season_year)s;
    """
    # We use UPDATE only, not INSERT - the row should exist from the xStats pull
    # which runs first.
    return db.execute_many(sql, rows)


def _upsert_hitter_budget(rows: list[dict]) -> int:
    if not rows:
        return 0
    sql = """
        UPDATE hitter_xstats SET
          pitches_total = %(pitches_total)s,
          pitches_per_pa = %(pitches_per_pa)s,
          k_pct = %(k_pct)s,
          bb_pct = %(bb_pct)s,
          refreshed_at = now()
        WHERE mlb_id = %(mlb_id)s AND season_year = %(season_year)s;
    """
    return db.execute_many(sql, rows)


def _refresh_team_pitches_per_pa(season_year: int) -> int:
    """Aggregate hitter pitches_per_pa weighted by PA into team_xstats."""
    sql = """
        UPDATE team_xstats t SET
          pitches_per_pa = sub.team_pitches_per_pa,
          refreshed_at = now()
        FROM (
          SELECT
            -- We don't have team association on hitter_xstats directly,
            -- so we rely on team aggregates already loaded via team CSV.
            -- For now use league-average as a placeholder.
            3.92 AS team_pitches_per_pa
        ) sub
        WHERE t.season_year = %s;
    """
    return db.execute(sql, (season_year,))


# ---------- Top-level refresh ----------

def refresh_statcast(season_year: Optional[int] = None) -> dict:
    """Pull pitcher + hitter xStats AND pitch-budget aggregates; persist all."""
    season_year = season_year or date.today().year
    job_id = db.log_job_start("statcast_refresh")
    metrics: dict = {"season_year": season_year}

    try:
        from pybaseball import (
            statcast_pitcher_expected_stats,
            statcast_batter_expected_stats,
            pitching_stats,
            batting_stats,
        )
    except ImportError as e:
        msg = "pybaseball not installed: pip install pybaseball"
        db.log_job_finish(job_id, "failure", msg)
        ntfy.send_failure("statcast_refresh", msg)
        raise

    try:
        # ---------- 1. Statcast xStats (existing) ----------
        log.info("Pulling pitcher xStats for %d", season_year)
        df_pit = statcast_pitcher_expected_stats(season_year, minPA=10)
        pit_rows = _df_to_pitcher_rows(df_pit, season_year)
        n_pit = db.upsert_pitcher_xstats(pit_rows)
        metrics["n_pitchers"] = n_pit
        log.info("Upserted %d pitcher xStats rows", n_pit)

        log.info("Pulling hitter xStats for %d", season_year)
        df_hit = statcast_batter_expected_stats(season_year, minPA=10)
        hit_rows = _df_to_hitter_rows(df_hit, season_year)
        n_hit = db.upsert_hitter_xstats(hit_rows)
        metrics["n_hitters"] = n_hit
        log.info("Upserted %d hitter xStats rows", n_hit)

        # ---------- 2. FanGraphs aggregates (NEW for pitch-budget) ----------
        log.info("Pulling FanGraphs pitcher aggregates for %d", season_year)
        try:
            df_pit_fg = pitching_stats(season_year, qual=1)
            pit_budget_rows = _df_to_pitcher_budget_rows(df_pit_fg, season_year)
            n_pit_b = _upsert_pitcher_budget(pit_budget_rows)
            metrics["n_pitcher_budget"] = n_pit_b
            log.info("Updated %d pitcher pitch-budget rows", n_pit_b)
        except Exception as e:
            # Non-fatal: keep existing budget data, log the warning
            log.warning("FanGraphs pitcher fetch failed (non-fatal): %s", e)
            metrics["pitcher_budget_error"] = str(e)

        log.info("Pulling FanGraphs hitter aggregates for %d", season_year)
        try:
            df_hit_fg = batting_stats(season_year, qual=1)
            hit_budget_rows = _df_to_hitter_budget_rows(df_hit_fg, season_year)
            n_hit_b = _upsert_hitter_budget(hit_budget_rows)
            metrics["n_hitter_budget"] = n_hit_b
            log.info("Updated %d hitter pitch-budget rows", n_hit_b)
        except Exception as e:
            log.warning("FanGraphs batting fetch failed (non-fatal): %s", e)
            metrics["hitter_budget_error"] = str(e)

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
