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

def _fetch_pitcher_game_log(mlb_id: int, season_year: int) -> list[dict]:
    """
    Fetch game-by-game pitching log for a pitcher in this season.
    Returns list of stat dicts (one per appearance).
    Caller should filter to gamesStarted == 1 to get start-only data.
    """
    url = f"{MLB_API_BASE}/people/{mlb_id}/stats"
    params = {"stats": "gameLog", "group": "pitching", "season": season_year}
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        splits = data.get("stats", [{}])[0].get("splits", [])
        return [s.get("stat", {}) for s in splits if s.get("stat")]
    except Exception as e:
        log.debug("Pitcher %d gameLog fetch failed: %s", mlb_id, e)
        return []


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

        # Pull game-by-game log; filter to STARTS ONLY (gamesStarted == 1).
        # This eliminates role-mixing for pitchers who have both starts + relief.
        games = _fetch_pitcher_game_log(mlb_id, season_year)
        if not games:
            continue

        starts = [g for g in games if (_coerce_int(g.get("gamesStarted")) or 0) >= 1]
        if not starts:
            # Pitcher has appearances but no starts - skip entirely
            continue

        gs = len(starts)
        # Aggregate across starts only
        tbf_sum = sum(_coerce_int(g.get("battersFaced")) or 0 for g in starts)
        pitches_sum = sum(_coerce_int(g.get("numberOfPitches")) or 0 for g in starts)
        ip_sum = sum((_parse_ip(g.get("inningsPitched")) or 0) for g in starts)
        so_sum = sum(_coerce_int(g.get("strikeOuts")) or 0 for g in starts)
        bb_sum = sum(_coerce_int(g.get("baseOnBalls")) or 0 for g in starts)

        # Need meaningful workload data
        if tbf_sum == 0 or pitches_sum == 0:
            continue

        # Total games (starts + relief) for the g_total column
        g_total = len(games)

        update_rows.append({
            "mlb_id": mlb_id,
            "season_year": season_year,
            "g_total": g_total,
            "gs": gs,
            "ip_total": ip_sum,
            "tbf": tbf_sum,
            "pitches_total": pitches_sum,
            "avg_pitches_per_start": pitches_sum / gs,
            "pitches_per_pa": pitches_sum / tbf_sum,
            "k_pct": so_sum / tbf_sum,
            "bb_pct": bb_sum / tbf_sum,
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
          g_total = %(g_total)s,
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

        try:
            t0 = time.time()
            n_bp = _refresh_team_bullpen_stats(season_year)
            metrics["n_team_bullpen"] = n_bp
            log.info("Updated %d team bullpen rows in %.1fs", n_bp, time.time() - t0)
        except Exception as e:
            log.warning("Team bullpen refresh failed (non-fatal): %s", e)
            metrics["bullpen_error"] = str(e)

        db.log_job_finish(job_id, "success", payload=metrics)
        return metrics
    except Exception as e:
        log.exception("Statcast refresh failed")
        db.log_job_finish(job_id, "failure", str(e), metrics)
        ntfy.send_failure("statcast_refresh", str(e))
        raise


def _refresh_team_bullpen_stats(season_year: int) -> int:
    """
    Aggregate per-team bullpen stats from MLB Stats API.

    Logic:
      1. For each MLB team, fetch active roster.
      2. For each pitcher on roster, fetch season pitching stats.
      3. Filter to "relievers": pitchers where (G - GS) > GS.
      4. Sum ER, IP, BF across the bullpen.
      5. Compute team_bullpen_era = ER * 9 / IP.
      6. Upsert to team_xstats.

    NOTE: this reads pitcher_xstats which we just refreshed, so we don't need
    to hit MLB API again per-pitcher. We have g_total, gs, ip_total cached.
    But we DON'T have ER cached, so we need MLB API for that one field.
    Optimization for later: cache ER too. For now we re-fetch for relievers only.
    """
    log.info("Aggregating bullpen stats from %d teams", 30)

    # MLB team IDs are 108-160 with gaps; safest to fetch the team list
    teams_url = f"{MLB_API_BASE}/teams?sportId=1&season={season_year}"
    try:
        r = requests.get(teams_url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        teams_data = r.json().get("teams", [])
    except Exception as e:
        log.warning("Failed to fetch team list: %s", e)
        return 0

    team_rows = []
    for team in teams_data:
        team_id = team.get("id")
        team_code = team.get("abbreviation")
        if not team_id or not team_code:
            continue

        # Fetch roster
        roster_url = f"{MLB_API_BASE}/teams/{team_id}/roster?rosterType=active&season={season_year}"
        try:
            r = requests.get(roster_url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            roster = r.json().get("roster", [])
        except Exception as e:
            log.debug("Roster fetch failed for %s: %s", team_code, e)
            continue

        # Filter to pitchers
        pitcher_ids = [
            p.get("person", {}).get("id")
            for p in roster
            if p.get("position", {}).get("abbreviation") == "P"
        ]
        if not pitcher_ids:
            continue

        # Aggregate reliever stats
        bp_er = 0.0
        bp_ip = 0.0
        for pid in pitcher_ids:
            stats = _fetch_pitcher_season_stats(pid, season_year)
            if not stats:
                continue
            g = _coerce_int(stats.get("gamesPlayed")) or 0
            gs = _coerce_int(stats.get("gamesStarted")) or 0
            # Reliever check: more bullpen apps than starts
            if (g - gs) <= gs or g == 0:
                continue
            er = _coerce_float(stats.get("earnedRuns"))
            ip = _parse_ip(stats.get("inningsPitched"))
            if er is None or ip is None or ip <= 0:
                continue
            bp_er += er
            bp_ip += ip

        if bp_ip <= 0:
            continue

        bp_era = (bp_er * 9.0) / bp_ip
        team_rows.append({
            "team_code": team_code,
            "season_year": season_year,
            "bullpen_era": round(bp_era, 3),
            "bullpen_xera": None,  # MLB API doesn't expose xERA - leave NULL
            "bullpen_ip": round(bp_ip, 1),
        })
        log.debug("Bullpen %s: %d relievers, ERA %.2f over %.1f IP",
                  team_code, sum(1 for _ in pitcher_ids), bp_era, bp_ip)

    n = db.upsert_team_bullpen_stats(team_rows)
    log.info("Updated %d team bullpen rows", n)
    return n


def main():
    metrics = refresh_statcast()
    print(metrics)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
