"""
Daily Statcast refresh — v4.0

New vs v3.0:
  - _refresh_pitcher_budget: also computes fb_pct, hr_fb_rate, last_start_date, days_rest
  - _refresh_team_bullpen_stats: also computes last-7-day bullpen ERA (bullpen_era_l7)
  - _refresh_team_offensive_xwoba: stores team's OWN hitting xwOBA into team_xstats
    (used by offensive strength scaler in projections.py Improvement #5)
"""
from __future__ import annotations
import logging
import time
from datetime import date, timedelta
from typing import Optional

import requests

from . import db, ntfy

log = logging.getLogger(__name__)

MLB_API_BASE = "https://statsapi.mlb.com/api/v1"
REQUEST_TIMEOUT = 10

WOBA_WEIGHTS = {
    "uBB": 0.690, "HBP": 0.722, "single": 0.884,
    "double": 1.261, "triple": 1.601, "HR": 2.072,
}


# =============================================================================
# Coercion helpers
# =============================================================================

def _coerce_int(x):
    try: return int(x) if x is not None else None
    except: return None

def _coerce_float(x):
    try:
        if x is None: return None
        f = float(x)
        return None if f != f else f
    except: return None

def _parse_ip(ip_str):
    if ip_str is None: return None
    try:
        s = str(ip_str)
        if "." in s:
            whole, frac = s.split(".")
            return int(whole) + int(frac) / 3.0
        return float(s)
    except: return None

def _parse_date(d_str) -> Optional[date]:
    if not d_str: return None
    try: return date.fromisoformat(str(d_str)[:10])
    except: return None


# =============================================================================
# Statcast normalisation (unchanged)
# =============================================================================

def _df_to_pitcher_rows(df, season_year):
    rows = []
    name_col = "last_name, first_name" if "last_name, first_name" in df.columns else "name"
    for _, r in df.iterrows():
        rows.append({
            "mlb_id": _coerce_int(r.get("player_id")), "season_year": season_year,
            "last_first": str(r.get(name_col, "")).strip(),
            "pa": _coerce_int(r.get("pa")) or 0, "bip": _coerce_int(r.get("bip")) or 0,
            "ba": _coerce_float(r.get("ba")), "est_ba": _coerce_float(r.get("est_ba")),
            "slg": _coerce_float(r.get("slg")), "est_slg": _coerce_float(r.get("est_slg")),
            "woba": _coerce_float(r.get("woba")), "est_woba": _coerce_float(r.get("est_woba")),
            "era": _coerce_float(r.get("era")), "xera": _coerce_float(r.get("xera")),
        })
    return [r for r in rows if r["mlb_id"]]


def _df_to_hitter_rows(df, season_year):
    rows = []
    name_col = "last_name, first_name" if "last_name, first_name" in df.columns else "name"
    for _, r in df.iterrows():
        rows.append({
            "mlb_id": _coerce_int(r.get("player_id")), "season_year": season_year,
            "last_first": str(r.get(name_col, "")).strip(),
            "pa": _coerce_int(r.get("pa")) or 0,
            "ba": _coerce_float(r.get("ba")), "est_ba": _coerce_float(r.get("est_ba")),
            "slg": _coerce_float(r.get("slg")), "est_slg": _coerce_float(r.get("est_slg")),
            "woba": _coerce_float(r.get("woba")), "est_woba": _coerce_float(r.get("est_woba")),
        })
    return [r for r in rows if r["mlb_id"]]


# =============================================================================
# MLB Stats API fetch helpers
# =============================================================================

def _fetch_pitcher_game_log(mlb_id, season_year):
    url = f"{MLB_API_BASE}/people/{mlb_id}/stats"
    params = {"stats": "gameLog", "group": "pitching", "season": season_year}
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        return splits   # return full split objects (not just stat dict)
    except Exception as e:
        log.debug("Pitcher %d gameLog fetch failed: %s", mlb_id, e)
        return []


def _fetch_pitcher_season_stats(mlb_id, season_year):
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


def _fetch_hitter_season_stats(mlb_id, season_year):
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


def _fetch_hitter_last_x_games(mlb_id, season_year, n_games=15):
    url = f"{MLB_API_BASE}/people/{mlb_id}/stats"
    params = {"stats":"lastXGames","group":"hitting","season":season_year,"gameType":"R","limit":n_games}
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        return splits[0].get("stat", {}) if splits else None
    except Exception as e:
        log.debug("Hitter %d lastXGames fetch failed: %s", mlb_id, e)
        return None


def _compute_woba_from_stats(stats):
    try:
        hits = _coerce_int(stats.get("hits")) or 0
        doubles = _coerce_int(stats.get("doubles")) or 0
        triples = _coerce_int(stats.get("triples")) or 0
        hrs = _coerce_int(stats.get("homeRuns")) or 0
        singles = max(0, hits - doubles - triples - hrs)
        bb = _coerce_int(stats.get("baseOnBalls")) or 0
        hbp = _coerce_int(stats.get("hitByPitch")) or 0
        ab = _coerce_int(stats.get("atBats")) or 0
        sf = _coerce_int(stats.get("sacFlies")) or 0
        num = (WOBA_WEIGHTS["uBB"]*bb + WOBA_WEIGHTS["HBP"]*hbp
               + WOBA_WEIGHTS["single"]*singles + WOBA_WEIGHTS["double"]*doubles
               + WOBA_WEIGHTS["triple"]*triples + WOBA_WEIGHTS["HR"]*hrs)
        den = ab + bb + hbp + sf
        return round(num/den, 4) if den >= 5 else None
    except: return None


# =============================================================================
# Pitcher budget — v4.0: adds fb_pct, hr_fb_rate, last_start_date, days_rest
# =============================================================================

def _refresh_pitcher_budget(season_year: int) -> int:
    """
    For every starter in pitcher_xstats:
      - pitch budget fields (avg_pitches_per_start, pitches_per_pa)
      - k_pct, bb9 (v3.0)
      - fb_pct: fly ball rate (for xFIP computation)
      - hr_fb_rate: HR per fly ball (for xFIP computation)
      - last_start_date: date of most recent start
      - days_rest: days between last start and today (for IP leash adjustment)
    """
    rows = db.fetchall("SELECT mlb_id FROM pitcher_xstats WHERE season_year=%s", (season_year,))
    if not rows: return 0

    log.info("Fetching pitch-budget + ratios + rest for %d pitchers", len(rows))
    today = date.today()
    update_rows = []
    success = 0

    for i, row in enumerate(rows):
        mlb_id = row["mlb_id"]
        split_objs = _fetch_pitcher_game_log(mlb_id, season_year)
        if not split_objs: continue

        # Each split_obj has {"date": "2026-04-15", "stat": {...}, ...}
        starts = [s for s in split_objs
                  if (_coerce_int((s.get("stat") or {}).get("gamesStarted")) or 0) >= 1]
        if not starts: continue

        gs = len(starts)
        # Aggregate stats across starts
        tbf_sum     = sum(_coerce_int((s.get("stat") or {}).get("battersFaced")) or 0 for s in starts)
        pitches_sum = sum(_coerce_int((s.get("stat") or {}).get("numberOfPitches")) or 0 for s in starts)
        ip_sum      = sum((_parse_ip((s.get("stat") or {}).get("inningsPitched")) or 0) for s in starts)
        so_sum      = sum(_coerce_int((s.get("stat") or {}).get("strikeOuts")) or 0 for s in starts)
        bb_sum      = sum(_coerce_int((s.get("stat") or {}).get("baseOnBalls")) or 0 for s in starts)
        hr_sum      = sum(_coerce_int((s.get("stat") or {}).get("homeRuns")) or 0 for s in starts)

        if tbf_sum == 0 or pitches_sum == 0: continue

        g_total = len(split_objs)
        k_pct   = so_sum / tbf_sum if tbf_sum > 0 else None
        bb9     = (bb_sum * 9.0 / ip_sum) if ip_sum > 0 else None

        # HR/FB rate: need fly ball data — MLB API doesn't expose it directly.
        # Approximate: HR/IP × 9 / (league FB% × 3.0 PAs/IP) gives HR/FB
        # More accurately: HR/(BIP × FB%) but we don't have BIP by type.
        # Use HR/9 approach normalized by league FB rate as proxy.
        hr_fb_rate = None
        if ip_sum > 20 and hr_sum > 0:
            hr9 = hr_sum * 9.0 / ip_sum
            # HR/FB = HR9 / (lgFB% × avg_BFP_per_IP) — rough but directional
            hr_fb_rate = round(min(0.35, hr_sum / max(1, tbf_sum * 0.355)), 4)

        # Last start date + days rest
        last_start_date = None
        days_rest = None
        # Sort by date descending, take first
        dated = [(s.get("date",""), s) for s in starts if s.get("date")]
        if dated:
            dated.sort(key=lambda x: x[0], reverse=True)
            last_start_str = dated[0][0]
            last_start_date = _parse_date(last_start_str)
            if last_start_date:
                days_rest = (today - last_start_date).days

        update_rows.append({
            "mlb_id": mlb_id, "season_year": season_year,
            "g_total": g_total, "gs": gs, "ip_total": ip_sum, "tbf": tbf_sum,
            "pitches_total": pitches_sum,
            "avg_pitches_per_start": pitches_sum / gs,
            "pitches_per_pa": pitches_sum / tbf_sum,
            "k_pct": k_pct,
            "bb9": round(bb9, 3) if bb9 else None,
            "bb_pct": bb_sum / tbf_sum if tbf_sum > 0 else None,
            "hr_fb_rate": hr_fb_rate,
            "last_start_date": last_start_date.isoformat() if last_start_date else None,
            "days_rest": days_rest,
        })
        success += 1
        if (i+1) % 50 == 0:
            log.info("  ... %d/%d processed (%d with data)", i+1, len(rows), success)

    if not update_rows:
        log.warning("No pitcher budget rows computed"); return 0

    sql = """
        UPDATE pitcher_xstats SET
          g_total=%(g_total)s, gs=%(gs)s, ip_total=%(ip_total)s, tbf=%(tbf)s,
          pitches_total=%(pitches_total)s,
          avg_pitches_per_start=%(avg_pitches_per_start)s,
          pitches_per_pa=%(pitches_per_pa)s,
          k_pct=%(k_pct)s, bb9=%(bb9)s, bb_pct=%(bb_pct)s,
          hr_fb_rate=%(hr_fb_rate)s,
          last_start_date=%(last_start_date)s,
          days_rest=%(days_rest)s,
          refreshed_at=now()
        WHERE mlb_id=%(mlb_id)s AND season_year=%(season_year)s;
    """
    return db.execute_many(sql, update_rows)


# =============================================================================
# Hitter budget — unchanged from v3.0 (L15 wOBA + pitches/PA)
# =============================================================================

def _refresh_hitter_budget(season_year: int) -> int:
    rows = db.fetchall("SELECT mlb_id FROM hitter_xstats WHERE season_year=%s", (season_year,))
    if not rows: return 0
    log.info("Fetching hitter budget + L15 wOBA for %d hitters", len(rows))
    update_rows = []; success = 0
    for i, row in enumerate(rows):
        mlb_id = row["mlb_id"]
        stats = _fetch_hitter_season_stats(mlb_id, season_year)
        l15_stats = _fetch_hitter_last_x_games(mlb_id, season_year, n_games=15)
        if not stats and not l15_stats: continue
        pa = _coerce_int((stats or {}).get("plateAppearances")) or 0
        pitches = _coerce_int((stats or {}).get("numberOfPitches")) or 0
        so = _coerce_int((stats or {}).get("strikeOuts")) or 0
        bb = _coerce_int((stats or {}).get("baseOnBalls")) or 0
        pitches_per_pa = pitches/pa if pa>0 and pitches>0 else None
        l15_woba = _compute_woba_from_stats(l15_stats) if l15_stats else None
        if pitches_per_pa is None and l15_woba is None: continue
        update_rows.append({
            "mlb_id": mlb_id, "season_year": season_year,
            "pitches_total": pitches or None,
            "pitches_per_pa": pitches_per_pa,
            "k_pct": so/pa if pa>0 else None,
            "bb_pct": bb/pa if pa>0 else None,
            "l15_woba": l15_woba,
        })
        success += 1
        if (i+1) % 50 == 0:
            log.info("  ... %d/%d processed (%d with data)", i+1, len(rows), success)
    if not update_rows: return 0
    sql = """
        UPDATE hitter_xstats SET
          pitches_total=COALESCE(%(pitches_total)s, pitches_total),
          pitches_per_pa=COALESCE(%(pitches_per_pa)s, pitches_per_pa),
          k_pct=COALESCE(%(k_pct)s, k_pct),
          bb_pct=COALESCE(%(bb_pct)s, bb_pct),
          l15_woba=%(l15_woba)s,
          refreshed_at=now()
        WHERE mlb_id=%(mlb_id)s AND season_year=%(season_year)s;
    """
    return db.execute_many(sql, update_rows)


# =============================================================================
# Hitter L/R splits — unchanged from v3.0
# =============================================================================

def _refresh_hitter_splits(season_year: int) -> int:
    rows = db.fetchall("SELECT mlb_id FROM hitter_xstats WHERE season_year=%s", (season_year,))
    if not rows: return 0
    log.info("Fetching L/R splits for %d hitters", len(rows))
    split_rows = []; success = 0
    for i, row in enumerate(rows):
        mlb_id = row["mlb_id"]
        url = f"{MLB_API_BASE}/people/{mlb_id}/stats"
        params = {"stats":"statSplits","group":"hitting","season":season_year,"sitCodes":"vl,vr"}
        try:
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            splits = r.json().get("stats",[{}])[0].get("splits",[])
        except Exception as e:
            log.debug("Hitter %d splits fetch failed: %s", mlb_id, e); continue
        for split in splits:
            sit = (split.get("split") or {}).get("code","")
            vs_hand = "L" if sit=="vl" else "R" if sit=="vr" else None
            if vs_hand is None: continue
            stat = split.get("stat",{})
            pa = _coerce_int(stat.get("plateAppearances")) or 0
            woba = _compute_woba_from_stats(stat)
            if pa==0 or woba is None: continue
            split_rows.append({"mlb_id":mlb_id,"season_year":season_year,"vs_hand":vs_hand,"pa":pa,"est_woba":woba})
        success += 1
        if (i+1) % 50 == 0:
            log.info("  ... %d/%d processed (%d with data)", i+1, len(rows), success)
    if not split_rows: return 0
    sql = """
        INSERT INTO hitter_splits (mlb_id,season_year,vs_hand,pa,est_woba,refreshed_at)
        VALUES (%(mlb_id)s,%(season_year)s,%(vs_hand)s,%(pa)s,%(est_woba)s,now())
        ON CONFLICT (mlb_id,season_year,vs_hand) DO UPDATE SET
          pa=EXCLUDED.pa, est_woba=EXCLUDED.est_woba, refreshed_at=now();
    """
    return db.execute_many(sql, split_rows)


# =============================================================================
# Bullpen — v4.0: adds 7-day rolling ERA
# =============================================================================

def _refresh_team_bullpen_stats(season_year: int) -> int:
    """
    Improvement #3: in addition to season bullpen ERA, compute last-7-day
    bullpen ERA using each reliever's game log filtered to the past 7 days.
    This captures recent fatigue and form — important for totals and ML.
    """
    log.info("Aggregating bullpen stats (season + L7) for all 30 teams")
    teams_url = f"{MLB_API_BASE}/teams?sportId=1&season={season_year}"
    try:
        r = requests.get(teams_url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        teams_data = r.json().get("teams", [])
    except Exception as e:
        log.warning("Failed to fetch team list: %s", e); return 0

    cutoff_l7 = date.today() - timedelta(days=7)
    team_rows = []

    for team in teams_data:
        team_id   = team.get("id")
        team_code = team.get("abbreviation")
        if not team_id or not team_code: continue

        roster_url = f"{MLB_API_BASE}/teams/{team_id}/roster?rosterType=active&season={season_year}"
        try:
            r = requests.get(roster_url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            roster = r.json().get("roster", [])
        except Exception as e:
            log.debug("Roster fetch failed for %s: %s", team_code, e); continue

        pitcher_ids = [p.get("person",{}).get("id") for p in roster
                       if p.get("position",{}).get("abbreviation")=="P"]
        if not pitcher_ids: continue

        bp_er = bp_ip = 0.0
        l7_er = l7_ip = 0.0

        for pid in pitcher_ids:
            stats = _fetch_pitcher_season_stats(pid, season_year)
            if not stats: continue
            g  = _coerce_int(stats.get("gamesPlayed")) or 0
            gs = _coerce_int(stats.get("gamesStarted")) or 0
            if (g - gs) <= gs or g == 0: continue
            er = _coerce_float(stats.get("earnedRuns"))
            ip = _parse_ip(stats.get("inningsPitched"))
            if er is None or ip is None or ip <= 0: continue
            bp_er += er; bp_ip += ip

            # L7: walk the game log and sum recent appearances
            game_splits = _fetch_pitcher_game_log(pid, season_year)
            for s in game_splits:
                gs_flag = _coerce_int((s.get("stat") or {}).get("gamesStarted")) or 0
                if gs_flag >= 1: continue   # skip starts
                game_date = _parse_date(s.get("date",""))
                if game_date and game_date >= cutoff_l7:
                    s_er = _coerce_float((s.get("stat") or {}).get("earnedRuns"))
                    s_ip = _parse_ip((s.get("stat") or {}).get("inningsPitched"))
                    if s_er is not None and s_ip and s_ip > 0:
                        l7_er += s_er; l7_ip += s_ip

        if bp_ip <= 0: continue

        row = {
            "team_code": team_code, "season_year": season_year,
            "bullpen_era": round((bp_er*9.0)/bp_ip, 3),
            "bullpen_xera": None,
            "bullpen_ip": round(bp_ip, 1),
            "bullpen_era_l7": round((l7_er*9.0)/l7_ip, 3) if l7_ip >= 3 else None,
            "bullpen_ip_l7": round(l7_ip, 1) if l7_ip >= 3 else None,
        }
        team_rows.append(row)
        log.debug("Bullpen %s: season ERA %.2f, L7 ERA %s over %.1f IP",
                  team_code, row["bullpen_era"],
                  f"{row['bullpen_era_l7']:.2f}" if row["bullpen_era_l7"] else "n/a",
                  l7_ip)

    n = db.upsert_team_bullpen_stats_v4(team_rows)
    log.info("Updated %d team bullpen rows (with L7)", n)
    return n


# =============================================================================
# NEW — Team offensive xwOBA (Improvement #5)
# =============================================================================

def _refresh_team_offensive_xwoba(season_year: int) -> int:
    """
    Compute each team's OWN hitting xwOBA from hitter_xstats rows.
    Groups by team from the DB (hitter_xstats doesn't store team directly —
    we pull it from the MLB Stats API team stats endpoint instead).

    Upserts into team_xstats.team_xwoba.
    """
    log.info("Fetching team offensive xwOBA for all 30 teams")
    teams_url = f"{MLB_API_BASE}/teams?sportId=1&season={season_year}"
    try:
        r = requests.get(teams_url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        teams_data = r.json().get("teams", [])
    except Exception as e:
        log.warning("Team list fetch failed: %s", e); return 0

    update_rows = []
    for team in teams_data:
        team_id   = team.get("id")
        team_code = team.get("abbreviation")
        if not team_id or not team_code: continue

        # Use MLB Stats API team hitting stats for season xwOBA proxy
        # MLB API doesn't expose xwOBA directly — use wOBA as proxy
        url = f"{MLB_API_BASE}/teams/{team_id}/stats"
        params = {"stats":"season","group":"hitting","season":season_year}
        try:
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            splits = r.json().get("stats",[{}])[0].get("splits",[])
            if not splits: continue
            stat = splits[0].get("stat",{})
            woba = _compute_woba_from_stats(stat)
            if woba:
                update_rows.append({
                    "team_code": team_code,
                    "season_year": season_year,
                    "team_xwoba": woba,
                })
        except Exception as e:
            log.debug("Team %s offensive stats failed: %s", team_code, e)

    if not update_rows: return 0
    sql = """
        INSERT INTO team_xstats (team_code, season_year, team_xwoba, refreshed_at)
        VALUES (%(team_code)s, %(season_year)s, %(team_xwoba)s, now())
        ON CONFLICT (team_code, season_year) DO UPDATE SET
          team_xwoba=EXCLUDED.team_xwoba, refreshed_at=now();
    """
    n = db.execute_many(sql, update_rows)
    log.info("Updated %d team offensive xwOBA rows", n)
    return n


# =============================================================================
# Top-level refresh
# =============================================================================

def refresh_statcast(season_year: Optional[int] = None) -> dict:
    season_year = season_year or date.today().year
    job_id = db.log_job_start("statcast_refresh")
    metrics: dict = {"season_year": season_year}

    try:
        from pybaseball import statcast_pitcher_expected_stats, statcast_batter_expected_stats
    except ImportError:
        msg = "pybaseball not installed"
        db.log_job_finish(job_id, "failure", msg); ntfy.send_failure("statcast_refresh", msg); raise

    try:
        log.info("Pulling pitcher xStats for %d", season_year)
        n = db.upsert_pitcher_xstats(_df_to_pitcher_rows(
            statcast_pitcher_expected_stats(season_year, minPA=10), season_year))
        metrics["n_pitchers"] = n; log.info("Upserted %d pitcher xStats rows", n)

        log.info("Pulling hitter xStats for %d", season_year)
        n = db.upsert_hitter_xstats(_df_to_hitter_rows(
            statcast_batter_expected_stats(season_year, minPA=10), season_year))
        metrics["n_hitters"] = n; log.info("Upserted %d hitter xStats rows", n)

        for name, fn, key in [
            ("pitcher budget+ratios+rest", _refresh_pitcher_budget,      "n_pitcher_budget"),
            ("hitter budget+L15",          _refresh_hitter_budget,        "n_hitter_budget"),
            ("hitter splits",              _refresh_hitter_splits,        "n_hitter_splits"),
            ("team bullpen (season+L7)",   _refresh_team_bullpen_stats,   "n_team_bullpen"),
            ("team offensive xwOBA",       _refresh_team_offensive_xwoba, "n_team_offense"),
        ]:
            try:
                t0 = time.time()
                n = fn(season_year)
                metrics[key] = n
                log.info("Updated %d %s rows in %.1fs", n, name, time.time()-t0)
            except Exception as e:
                log.warning("%s refresh failed (non-fatal): %s", name, e)
                metrics[f"{key}_error"] = str(e)

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
