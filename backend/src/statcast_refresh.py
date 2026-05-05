"""
Daily Statcast refresh.

Cron entry point. Runs once per day (06:00 ET, before first orchestrator run)
to pull the latest Expected Stats from Baseball Savant via pybaseball, plus
per-pitcher and per-hitter season aggregates from MLB Stats API for the
pitch-budget IP projection model.

Persists to:
  - pitcher_xstats  (xStats from Savant + workload + k_pct + bb9 from MLB API)
  - hitter_xstats   (xStats from Savant + patience + l15_woba from MLB API)
  - team_xstats

v3.0 additions:
  - _refresh_pitcher_ratios(): populates k_pct, bb9 on pitcher_xstats
    (xFIP is not available from MLB Stats API; populated separately when
    pybaseball exposes it, or left NULL for the 2-way ERA/xERA blend fallback)
  - _refresh_hitter_l15(): populates l15_woba on hitter_xstats using the
    MLB Stats API lastXGames endpoint (15 game rolling window)

We use MLB Stats API instead of FanGraphs because Cloudflare blocks Railway IPs
from FanGraphs. MLB Stats API is the official endpoint, free, no auth.
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
REQUEST_TIMEOUT = 10

# wOBA weights (2024 season; update yearly)
# Source: FanGraphs guts page
WOBA_WEIGHTS = {
    "uBB":  0.690,
    "HBP":  0.722,
    "single": 0.884,
    "double": 1.261,
    "triple": 1.601,
    "HR":   2.072,
}
WOBA_SCALE = 1.157   # league wOBA / league OBP scale factor


# =============================================================================
# Coercion helpers
# =============================================================================

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
        return None if f != f else f   # nan guard
    except (TypeError, ValueError):
        return None


def _parse_ip(ip_str) -> Optional[float]:
    """MLB API IP: '33.1' means 33 and 1/3 innings."""
    if ip_str is None:
        return None
    try:
        s = str(ip_str)
        if "." in s:
            whole, frac = s.split(".")
            return int(whole) + int(frac) / 3.0
        return float(s)
    except (TypeError, ValueError):
        return None


# =============================================================================
# Statcast Expected Stats normalisation (unchanged from v2)
# =============================================================================

def _df_to_pitcher_rows(df, season_year: int) -> list[dict]:
    rows = []
    name_col = "last_name, first_name" if "last_name, first_name" in df.columns else "name"
    for _, r in df.iterrows():
        rows.append({
            "mlb_id":      _coerce_int(r.get("player_id")),
            "season_year": season_year,
            "last_first":  str(r.get(name_col, "")).strip(),
            "pa":          _coerce_int(r.get("pa")) or 0,
            "bip":         _coerce_int(r.get("bip")) or 0,
            "ba":          _coerce_float(r.get("ba")),
            "est_ba":      _coerce_float(r.get("est_ba")),
            "slg":         _coerce_float(r.get("slg")),
            "est_slg":     _coerce_float(r.get("est_slg")),
            "woba":        _coerce_float(r.get("woba")),
            "est_woba":    _coerce_float(r.get("est_woba")),
            "era":         _coerce_float(r.get("era")),
            "xera":        _coerce_float(r.get("xera")),
        })
    return [r for r in rows if r["mlb_id"]]


def _df_to_hitter_rows(df, season_year: int) -> list[dict]:
    rows = []
    name_col = "last_name, first_name" if "last_name, first_name" in df.columns else "name"
    for _, r in df.iterrows():
        rows.append({
            "mlb_id":      _coerce_int(r.get("player_id")),
            "season_year": season_year,
            "last_first":  str(r.get(name_col, "")).strip(),
            "pa":          _coerce_int(r.get("pa")) or 0,
            "ba":          _coerce_float(r.get("ba")),
            "est_ba":      _coerce_float(r.get("est_ba")),
            "slg":         _coerce_float(r.get("slg")),
            "est_slg":     _coerce_float(r.get("est_slg")),
            "woba":        _coerce_float(r.get("woba")),
            "est_woba":    _coerce_float(r.get("est_woba")),
        })
    return [r for r in rows if r["mlb_id"]]


# =============================================================================
# MLB Stats API fetch helpers
# =============================================================================

def _fetch_pitcher_game_log(mlb_id: int, season_year: int) -> list[dict]:
    """Game-by-game pitching log. Filter to starts by caller."""
    url = f"{MLB_API_BASE}/people/{mlb_id}/stats"
    params = {"stats": "gameLog", "group": "pitching", "season": season_year}
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        return [s.get("stat", {}) for s in splits if s.get("stat")]
    except Exception as e:
        log.debug("Pitcher %d gameLog fetch failed: %s", mlb_id, e)
        return []


def _fetch_pitcher_season_stats(mlb_id: int, season_year: int) -> Optional[dict]:
    url = f"{MLB_API_BASE}/people/{mlb_id}/stats"
    params = {"stats": "season", "group": "pitching", "season": season_year}
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        return splits[0].get("stat", {}) if splits else None
    except Exception as e:
        log.debug("Pitcher %d season stats fetch failed: %s", mlb_id, e)
        return None


def _fetch_hitter_season_stats(mlb_id: int, season_year: int) -> Optional[dict]:
    url = f"{MLB_API_BASE}/people/{mlb_id}/stats"
    params = {"stats": "season", "group": "hitting", "season": season_year}
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        return splits[0].get("stat", {}) if splits else None
    except Exception as e:
        log.debug("Hitter %d season stats fetch failed: %s", mlb_id, e)
        return None


def _fetch_hitter_last_x_games(mlb_id: int, season_year: int,
                                 n_games: int = 15) -> Optional[dict]:
    """
    Fetch rolling last-N-games hitting stats for a hitter.
    Returns the stat dict or None if unavailable.
    """
    url = f"{MLB_API_BASE}/people/{mlb_id}/stats"
    params = {
        "stats":    "lastXGames",
        "group":    "hitting",
        "season":   season_year,
        "gameType": "R",
        "limit":    n_games,
    }
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        return splits[0].get("stat", {}) if splits else None
    except Exception as e:
        log.debug("Hitter %d lastXGames fetch failed: %s", mlb_id, e)
        return None


def _compute_woba_from_stats(stats: dict) -> Optional[float]:
    """
    Compute wOBA from raw MLB Stats API hitting stat dict using standard weights.

    MLB API fields: hits, doubles, triples, homeRuns, baseOnBalls, hitByPitch,
                    atBats, sacFlies
    singles = hits - doubles - triples - homeRuns
    """
    try:
        hits    = _coerce_int(stats.get("hits")) or 0
        doubles = _coerce_int(stats.get("doubles")) or 0
        triples = _coerce_int(stats.get("triples")) or 0
        hrs     = _coerce_int(stats.get("homeRuns")) or 0
        singles = max(0, hits - doubles - triples - hrs)
        bb      = _coerce_int(stats.get("baseOnBalls")) or 0
        hbp     = _coerce_int(stats.get("hitByPitch")) or 0
        ab      = _coerce_int(stats.get("atBats")) or 0
        sf      = _coerce_int(stats.get("sacFlies")) or 0

        numerator = (
            WOBA_WEIGHTS["uBB"]     * bb
            + WOBA_WEIGHTS["HBP"]   * hbp
            + WOBA_WEIGHTS["single"] * singles
            + WOBA_WEIGHTS["double"] * doubles
            + WOBA_WEIGHTS["triple"] * triples
            + WOBA_WEIGHTS["HR"]    * hrs
        )
        denominator = ab + bb + hbp + sf
        if denominator < 5:   # too few PA to be meaningful
            return None
        return round(numerator / denominator, 4)
    except Exception:
        return None


# =============================================================================
# Pitch-budget refresh (existing — extended to also write k_pct + bb9)
# =============================================================================

def _refresh_pitcher_budget(season_year: int) -> int:
    """
    For every pitcher in pitcher_xstats, fetch their MLB API game log,
    compute pitch-budget fields (avg_pitches_per_start, pitches_per_pa),
    AND the new v3 fields: k_pct (strikeout rate) and bb9 (walks per 9 IP).

    All written in a single UPDATE so we don't need a separate pass.
    """
    rows = db.fetchall(
        "SELECT mlb_id FROM pitcher_xstats WHERE season_year = %s", (season_year,)
    )
    if not rows:
        return 0

    log.info("Fetching pitch-budget + ratios for %d pitchers", len(rows))

    update_rows = []
    success = 0
    for i, row in enumerate(rows):
        mlb_id = row["mlb_id"]
        games = _fetch_pitcher_game_log(mlb_id, season_year)
        if not games:
            continue

        starts = [g for g in games if (_coerce_int(g.get("gamesStarted")) or 0) >= 1]
        if not starts:
            continue

        gs         = len(starts)
        tbf_sum    = sum(_coerce_int(g.get("battersFaced")) or 0 for g in starts)
        pitches_sum= sum(_coerce_int(g.get("numberOfPitches")) or 0 for g in starts)
        ip_sum     = sum((_parse_ip(g.get("inningsPitched")) or 0) for g in starts)
        so_sum     = sum(_coerce_int(g.get("strikeOuts")) or 0 for g in starts)
        bb_sum     = sum(_coerce_int(g.get("baseOnBalls")) or 0 for g in starts)

        if tbf_sum == 0 or pitches_sum == 0:
            continue

        g_total = len(games)

        # k_pct = K per batter faced (consistent with projection engine's LEAGUE_K_PCT)
        k_pct = so_sum / tbf_sum if tbf_sum > 0 else None

        # bb9 = walks per 9 innings pitched
        bb9 = (bb_sum * 9.0 / ip_sum) if ip_sum > 0 else None

        update_rows.append({
            "mlb_id":                mlb_id,
            "season_year":           season_year,
            "g_total":               g_total,
            "gs":                    gs,
            "ip_total":              ip_sum,
            "tbf":                   tbf_sum,
            "pitches_total":         pitches_sum,
            "avg_pitches_per_start": pitches_sum / gs,
            "pitches_per_pa":        pitches_sum / tbf_sum,
            "k_pct":                 k_pct,
            "bb9":                   round(bb9, 3) if bb9 is not None else None,
            "bb_pct":                bb_sum / tbf_sum if tbf_sum > 0 else None,
        })
        success += 1

        if (i + 1) % 50 == 0:
            log.info("  ... %d/%d processed (%d with data)", i + 1, len(rows), success)

    if not update_rows:
        log.warning("No pitcher budget rows computed")
        return 0

    sql = """
        UPDATE pitcher_xstats SET
          g_total               = %(g_total)s,
          gs                    = %(gs)s,
          ip_total              = %(ip_total)s,
          tbf                   = %(tbf)s,
          pitches_total         = %(pitches_total)s,
          avg_pitches_per_start = %(avg_pitches_per_start)s,
          pitches_per_pa        = %(pitches_per_pa)s,
          k_pct                 = %(k_pct)s,
          bb9                   = %(bb9)s,
          bb_pct                = %(bb_pct)s,
          refreshed_at          = now()
        WHERE mlb_id = %(mlb_id)s AND season_year = %(season_year)s;
    """
    return db.execute_many(sql, update_rows)


# =============================================================================
# Hitter budget refresh (existing — extended to also write l15_woba)
# =============================================================================

def _refresh_hitter_budget(season_year: int) -> int:
    """
    For every hitter in hitter_xstats, fetch season stats (pitches_per_pa)
    AND last-15-game wOBA (l15_woba). Both written in a single UPDATE.
    """
    rows = db.fetchall(
        "SELECT mlb_id FROM hitter_xstats WHERE season_year = %s", (season_year,)
    )
    if not rows:
        return 0

    log.info("Fetching hitter budget + L15 wOBA for %d hitters", len(rows))

    update_rows = []
    success = 0
    for i, row in enumerate(rows):
        mlb_id = row["mlb_id"]

        # Season stats for pitches_per_pa
        stats = _fetch_hitter_season_stats(mlb_id, season_year)

        # Last-15-game stats for l15_woba
        l15_stats = _fetch_hitter_last_x_games(mlb_id, season_year, n_games=15)

        if not stats and not l15_stats:
            continue

        pa      = _coerce_int((stats or {}).get("plateAppearances")) or 0
        pitches = _coerce_int((stats or {}).get("numberOfPitches")) or 0
        so      = _coerce_int((stats or {}).get("strikeOuts")) or 0
        bb      = _coerce_int((stats or {}).get("baseOnBalls")) or 0

        pitches_per_pa = pitches / pa if pa > 0 and pitches > 0 else None
        k_pct_hit      = so / pa      if pa > 0 else None
        bb_pct_hit     = bb / pa      if pa > 0 else None

        # Compute l15_woba from last-X-games stat block
        l15_woba = _compute_woba_from_stats(l15_stats) if l15_stats else None

        if pitches_per_pa is None and l15_woba is None:
            continue

        update_rows.append({
            "mlb_id":        mlb_id,
            "season_year":   season_year,
            "pitches_total": pitches or None,
            "pitches_per_pa":pitches_per_pa,
            "k_pct":         k_pct_hit,
            "bb_pct":        bb_pct_hit,
            "l15_woba":      l15_woba,
        })
        success += 1

        if (i + 1) % 50 == 0:
            log.info("  ... %d/%d processed (%d with data)", i + 1, len(rows), success)

    if not update_rows:
        log.warning("No hitter budget rows computed")
        return 0

    sql = """
        UPDATE hitter_xstats SET
          pitches_total  = COALESCE(%(pitches_total)s, pitches_total),
          pitches_per_pa = COALESCE(%(pitches_per_pa)s, pitches_per_pa),
          k_pct          = COALESCE(%(k_pct)s, k_pct),
          bb_pct         = COALESCE(%(bb_pct)s, bb_pct),
          l15_woba       = %(l15_woba)s,
          refreshed_at   = now()
        WHERE mlb_id = %(mlb_id)s AND season_year = %(season_year)s;
    """
    return db.execute_many(sql, update_rows)


# =============================================================================
# Hitter L/R splits (Improvement #6)
# =============================================================================

def _refresh_hitter_splits(season_year: int) -> int:
    """
    For every hitter in hitter_xstats, fetch vsLeft and vsRight split stats
    from MLB Stats API and upsert into the hitter_splits table.

    Used by projections.py to apply per-hitter platoon multipliers instead of
    league-average when ≥80 PA from a given side are available.
    """
    rows = db.fetchall(
        "SELECT mlb_id FROM hitter_xstats WHERE season_year = %s", (season_year,)
    )
    if not rows:
        return 0

    log.info("Fetching L/R splits for %d hitters", len(rows))

    split_rows = []
    success = 0
    for i, row in enumerate(rows):
        mlb_id = row["mlb_id"]
        url = f"{MLB_API_BASE}/people/{mlb_id}/stats"
        params = {
            "stats":    "statSplits",
            "group":    "hitting",
            "season":   season_year,
            "sitCodes": "vl,vr",   # vs left, vs right
        }
        try:
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            splits = r.json().get("stats", [{}])[0].get("splits", [])
        except Exception as e:
            log.debug("Hitter %d splits fetch failed: %s", mlb_id, e)
            continue

        for split in splits:
            sit = (split.get("split") or {}).get("code", "")
            vs_hand = "L" if sit == "vl" else "R" if sit == "vr" else None
            if vs_hand is None:
                continue
            stat = split.get("stat", {})
            pa   = _coerce_int(stat.get("plateAppearances")) or 0
            woba = _compute_woba_from_stats(stat)
            if pa == 0 or woba is None:
                continue
            split_rows.append({
                "mlb_id":      mlb_id,
                "season_year": season_year,
                "vs_hand":     vs_hand,
                "pa":          pa,
                "est_woba":    woba,
            })

        success += 1
        if (i + 1) % 50 == 0:
            log.info("  ... %d/%d processed (%d with data)", i + 1, len(rows), success)

    if not split_rows:
        log.warning("No hitter split rows computed")
        return 0

    sql = """
        INSERT INTO hitter_splits (mlb_id, season_year, vs_hand, pa, est_woba, refreshed_at)
        VALUES (%(mlb_id)s, %(season_year)s, %(vs_hand)s, %(pa)s, %(est_woba)s, now())
        ON CONFLICT (mlb_id, season_year, vs_hand) DO UPDATE SET
          pa           = EXCLUDED.pa,
          est_woba     = EXCLUDED.est_woba,
          refreshed_at = now();
    """
    return db.execute_many(sql, split_rows)


# =============================================================================
# Bullpen stats (unchanged from v2)
# =============================================================================

def _refresh_team_bullpen_stats(season_year: int) -> int:
    """Aggregate per-team bullpen ERA from MLB Stats API active rosters."""
    log.info("Aggregating bullpen stats for all 30 teams")

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
        team_id   = team.get("id")
        team_code = team.get("abbreviation")
        if not team_id or not team_code:
            continue

        roster_url = f"{MLB_API_BASE}/teams/{team_id}/roster?rosterType=active&season={season_year}"
        try:
            r = requests.get(roster_url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            roster = r.json().get("roster", [])
        except Exception as e:
            log.debug("Roster fetch failed for %s: %s", team_code, e)
            continue

        pitcher_ids = [
            p.get("person", {}).get("id")
            for p in roster
            if p.get("position", {}).get("abbreviation") == "P"
        ]
        if not pitcher_ids:
            continue

        bp_er = 0.0
        bp_ip = 0.0
        for pid in pitcher_ids:
            stats = _fetch_pitcher_season_stats(pid, season_year)
            if not stats:
                continue
            g  = _coerce_int(stats.get("gamesPlayed")) or 0
            gs = _coerce_int(stats.get("gamesStarted")) or 0
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

        team_rows.append({
            "team_code":   team_code,
            "season_year": season_year,
            "bullpen_era": round((bp_er * 9.0) / bp_ip, 3),
            "bullpen_xera": None,
            "bullpen_ip":  round(bp_ip, 1),
        })

    n = db.upsert_team_bullpen_stats(team_rows)
    log.info("Updated %d team bullpen rows", n)
    return n


# =============================================================================
# Top-level refresh
# =============================================================================

def refresh_statcast(season_year: Optional[int] = None) -> dict:
    """
    Full daily refresh pipeline:
      1. Statcast xStats (pybaseball → Baseball Savant)
      2. Pitcher pitch-budget + k_pct + bb9 (MLB Stats API game logs)
      3. Hitter patience + l15_woba (MLB Stats API season + lastXGames)
      4. Hitter L/R splits → hitter_splits table (Improvement #6)
      5. Team bullpen ERA (MLB Stats API rosters + season stats)
    """
    season_year = season_year or date.today().year
    job_id  = db.log_job_start("statcast_refresh")
    metrics: dict = {"season_year": season_year}

    try:
        from pybaseball import (
            statcast_pitcher_expected_stats,
            statcast_batter_expected_stats,
        )
    except ImportError:
        msg = "pybaseball not installed: pip install pybaseball"
        db.log_job_finish(job_id, "failure", msg)
        ntfy.send_failure("statcast_refresh", msg)
        raise

    try:
        # ---- 1. Statcast xStats ----
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

        # ---- 2. Pitcher pitch-budget + k_pct + bb9 ----
        try:
            t0 = time.time()
            n = _refresh_pitcher_budget(season_year)
            metrics["n_pitcher_budget"] = n
            log.info("Updated %d pitcher budget+ratio rows in %.1fs", n, time.time()-t0)
        except Exception as e:
            log.warning("Pitcher budget refresh failed (non-fatal): %s", e)
            metrics["pitcher_budget_error"] = str(e)

        # ---- 3. Hitter patience + l15_woba ----
        try:
            t0 = time.time()
            n = _refresh_hitter_budget(season_year)
            metrics["n_hitter_budget"] = n
            log.info("Updated %d hitter budget+L15 rows in %.1fs", n, time.time()-t0)
        except Exception as e:
            log.warning("Hitter budget refresh failed (non-fatal): %s", e)
            metrics["hitter_budget_error"] = str(e)

        # ---- 4. Hitter L/R splits ----
        try:
            t0 = time.time()
            n = _refresh_hitter_splits(season_year)
            metrics["n_hitter_splits"] = n
            log.info("Updated %d hitter split rows in %.1fs", n, time.time()-t0)
        except Exception as e:
            log.warning("Hitter splits refresh failed (non-fatal): %s", e)
            metrics["hitter_splits_error"] = str(e)

        # ---- 5. Team bullpen ERA ----
        try:
            t0 = time.time()
            n = _refresh_team_bullpen_stats(season_year)
            metrics["n_team_bullpen"] = n
            log.info("Updated %d team bullpen rows in %.1fs", n, time.time()-t0)
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


def main():
    metrics = refresh_statcast()
    print(metrics)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
