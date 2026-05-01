"""
Daily Statcast refresh.

Cron entry point. Runs once per day (06:00 ET, before first orchestrator run)
to pull the latest Expected Stats from Baseball Savant via pybaseball, plus
per-pitcher and per-hitter season aggregates from MLB Stats API for the
pitch-budget IP projection model.

Persists to:
  - pitcher_xstats  (xStats from Savant + workload from MLB API)
  - hitter_xstats   (xStats from Savant + patience from MLB API)
  - team_xstats

We use MLB Stats API instead of FanGraphs because Cloudflare blocks Railway IPs
from FanGraphs. MLB Stats API is the official endpoint, free, no auth, and
serves the same underlying numbers we need.
"""
from __future__ import annotations
import logging
import time
from datetime import date
from typing import Optional

import requests

from . import db, ntfy

log = logging.getLogger(__name__)

MLB_API_BASE = "https://statsapi.mlb.com/api/v1"
REQUEST_TIMEOUT = 10  # per-request timeout in seconds


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
        if f != f:  # nan
            return None
        return f
    except (TypeError, ValueError):
        return None


def _parse_ip(ip_str) -> Optional[float]:
    """MLB API returns IP as string like '33.1' meaning 33 and 1/3 innings."""
    if ip_str is None:
        return None
    try:
        s = str(ip_str)
        if "." in s:
            whole, frac = s.split(".")
            whole = int(whole)
            frac = int(frac)  # 0, 1, or 2
            return whole + frac / 3.0
        return float(s)
    except (TypeError, ValueError):
        return None


# ---------- Statcast Expected Stats normalization (existing) ----------

def _df_to_pitcher_rows(df, season_year: int) -> list[dict]:
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


# ---------- MLB Stats API: pitch-budget data ----------

def _fetch_pitcher_season_stats(mlb_id: int, season_year: int) -> Optional[dict]:
    """
    Hit MLB Stats API for one pitcher's season totals. Returns None if no data.
    """
    url = f"{MLB_API_BASE}/people/{mlb_id}/stats"
    params = {"stats": "season", "group": "pitching", "season": season_year}
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        splits = data.get("stats", [{}])[0].get("splits", [])
        if not splits:
            return None
        return splits[0].get("stat", {})
    except Exception as e:
        log.debug("Pitcher %d stats fetch failed: %s", mlb_id, e)
        return None


def _fetch_hitter_season_stats(mlb_id: int, season_year: int) -> Optional[dict]:
    """Same as above but for hitters."""
    url = f"{MLB_API_BASE}/people/{mlb_id}/stats"
    params = {"stats": "season", "group": "hitting", "season": season_year}
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        splits = data.get("stats", [{}])[0].get("splits", [])
        if not splits:
            return None
        return splits[0].get("stat", {})
    except Exception as e:
        log.debug("Hitter %d stats fetch failed: %s", mlb_id, e)
        return None


def _refresh_pitcher_budget(season_year: int) -> int:
    """
    For every pitcher in pitcher_xstats, fetch their MLB API season stats,
    compute pitch-budget fields, UPDATE the row.
    """
    # Get all pitcher IDs we know about
    rows = db.fetchall("SELECT mlb_id FROM pitcher_xstats WHERE season_year = %s", (season_year,))
    if not rows:
        return 0

    log.info("Fetching pitch-budget for %d pitchers (sequential, ~5-10 min)", len(rows))

    update_rows = []
    success = 0
    for i, row in enumerate(rows):
        mlb_id = row["mlb_id"]
        stats = _fetch_pitcher_season_stats(mlb_id, season_year)
        if not stats:
            continue

        gs = _coerce_int(stats.get("gamesStarted")) or 0
        tbf = _coerce_int(stats.get("battersFaced")) or 0
        pitches = _coerce_int(stats.get("numberOfPitches")) or 0
        ip = _parse_ip(stats.get("inningsPitched"))
        so = _coerce_int(stats.get("strikeOuts")) or 0
        bb = _coerce_int(stats.get("baseOnBalls")) or 0

        # Need both gs > 0 (started a game) AND tbf > 0 to compute meaningful budget
        if gs == 0 or tbf == 0 or pitches == 0:
            continue

        update_rows.append({
            "mlb_id": mlb_id,
            "season_year": season_year,
            "gs": gs,
            "ip_total": ip,
            "tbf": tbf,
            "pitches_total": pitches,
            "avg_pitches_per_start": pitches / gs if gs > 0 else None,
            "pitches_per_pa": pitches / tbf if tbf > 0 else None,
            "k_pct": so / tbf if tbf > 0 else None,
            "bb_pct": bb / tbf if tbf > 0 else None,
        })
        success += 1

        # Progress log every 50 pitchers
        if (i + 1) % 50 == 0:
            log.info("  ... %d/%d processed (%d with data)", i + 1, len(rows), success)

    if not update_rows:
        log.warning("No pitcher budget rows could be computed")
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
    return db.execute_many(sql, update_rows)


def _refresh_hitter_budget(season_year: int) -> int:
    """For every hitter in hitter_xstats, pull season stats, compute patience."""
    rows = db.fetchall("SELECT mlb_id FROM hitter_xstats WHERE season_year = %s", (season_year,))
    if not rows:
        return 0

    log.info("Fetching pitch-budget for %d hitters (sequential, ~5-10 min)", len(rows))

    update_rows = []
    success = 0
    for i, row in enumerate(rows):
        mlb_id = row["mlb_id"]
        stats = _fetch_hitter_season_stats(mlb_id, season_year)
        if not stats:
            continue

        pa = _coerce_int(stats.get("plateAppearances")) or 0
        pitches = _coerce_int(stats.get("numberOfPitches")) or 0
        so = _coerce_int(stats.get("strikeOuts")) or 0
        bb = _coerce_int(stats.get("baseOnBalls")) or 0

        if pa == 0 or pitches == 0:
            continue

        update_rows.append({
            "mlb_id": mlb_id,
            "season_year": season_year,
            "pitches_total": pitches,
            "pitches_per_pa": pitches / pa if pa > 0 else None,
            "k_pct": so / pa if pa > 0 else None,
            "bb_pct": bb / pa if pa > 0 else None,
        })
        success += 1

        if (i + 1) % 50 == 0:
            log.info("  ... %d/%d processed (%d with data)", i + 1, len(rows), success)

    if not update_rows:
        log.warning("No hitter budget rows could be computed")
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
    return db.execute_many(sql, update_rows)


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
        )
    except ImportError as e:
        msg = "pybaseball not installed: pip install pybaseball"
        db.log_job_finish(job_id, "failure", msg)
        ntfy.send_failure("statcast_refresh", msg)
        raise

    try:
        # ---------- 1. Statcast xStats (Baseball Savant) ----------
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

        # ---------- 2. MLB Stats API: pitch-budget data ----------
        try:
            t0 = time.time()
            n_pit_b = _refresh_pitcher_budget(season_year)
            metrics["n_pitcher_budget"] = n_pit_b
            log.info("Updated %d pitcher pitch-budget rows in %.1fs", n_pit_b, time.time() - t0)
        except Exception as e:
            log.warning("Pitcher pitch-budget refresh failed (non-fatal): %s", e)
            metrics["pitcher_budget_error"] = str(e)

        try:
            t0 = time.time()
            n_hit_b = _refresh_hitter_budget(season_year)
            metrics["n_hitter_budget"] = n_hit_b
            log.info("Updated %d hitter pitch-budget rows in %.1fs", n_hit_b, time.time() - t0)
        except Exception as e:
            log.warning("Hitter pitch-budget refresh failed (non-fatal): %s", e)
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
