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

        # L5 avg IP — last 5 starts
        if starts:
            sorted_starts = sorted(starts, key=lambda s: s.get("date",""), reverse=True)
            recent_5 = sorted_starts[:5]
            recent_ips = [(_parse_ip((s.get("stat") or {}).get("inningsPitched")) or 0) for s in recent_5]
            l5_avg_ip = round(sum(recent_ips) / len(recent_ips), 2) if recent_ips else None
        else:
            l5_avg_ip = None

        # Prior year IP — fetch season stats for previous year
        prev_year = season_year - 1
        ip_total_prev = gs_prev = None
        try:
            prev_stats = _fetch_pitcher_season_stats(mlb_id, prev_year)
            if prev_stats:
                prev_ip  = _parse_ip(prev_stats.get("inningsPitched"))
                prev_gs  = _coerce_int(prev_stats.get("gamesStarted"))
                if prev_ip and prev_gs and prev_gs >= 5:
                    ip_total_prev = round(float(prev_ip), 2)
                    gs_prev       = prev_gs
        except Exception:
            pass

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
            "l5_avg_ip": l5_avg_ip,
            "ip_total_prev": ip_total_prev,
            "gs_prev": gs_prev,
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
          l5_avg_ip=%(l5_avg_ip)s,
          ip_total_prev=%(ip_total_prev)s,
          gs_prev=%(gs_prev)s,
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
    Compute each team's OWN hitting wOBA — season average AND last 5 games.
    Blended in projections: 0.60 * season + 0.40 * L5.
    L5 captures recent form — cold/injured teams get correctly downgraded.
    """
    log.info("Fetching team offensive wOBA (season + L5) for all 30 teams")
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

        # Season wOBA
        season_woba = None
        try:
            url = f"{MLB_API_BASE}/teams/{team_id}/stats"
            r = requests.get(url, params={"stats":"season","group":"hitting","season":season_year}, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            splits = r.json().get("stats",[{}])[0].get("splits",[])
            if splits:
                season_woba = _compute_woba_from_stats(splits[0].get("stat",{}))
        except Exception as e:
            log.debug("Team %s season stats failed: %s", team_code, e)

        # L5 runs scored — fetch last 10 completed games from schedule
        # Convert avg runs scored to wOBA proxy: ~4.5 runs = .320 wOBA (league avg)
        # Formula: l5_woba_proxy = 0.320 + (l5_runs - 4.5) * 0.012
        l5_woba = None
        try:
            end_dt   = date.today().isoformat()
            start_dt = (date.today() - timedelta(days=30)).isoformat()
            sched_url = f"{MLB_API_BASE}/schedule"
            sr = requests.get(sched_url, params={
                "sportId": 1, "teamId": team_id,
                "startDate": start_dt, "endDate": end_dt,
                "gameType": "R", "hydrate": "linescore"
            }, timeout=REQUEST_TIMEOUT)
            sr.raise_for_status()
            runs_scored = []
            for date_block in sr.json().get("dates", []):
                for game in date_block.get("games", []):
                    status = (game.get("status") or {}).get("abstractGameState","")
                    if status != "Final": continue
                    teams = game.get("teams", {})
                    away = teams.get("away", {})
                    home_g = teams.get("home", {})
                    away_id = (away.get("team") or {}).get("id")
                    home_id = (home_g.get("team") or {}).get("id")
                    if away_id == team_id:
                        score = away.get("score")
                    elif home_id == team_id:
                        score = home_g.get("score")
                    else:
                        continue
                    if score is not None:
                        runs_scored.append(int(score))
            # Take last 5 completed games
            recent_5 = runs_scored[-5:] if len(runs_scored) >= 5 else runs_scored
            if recent_5:
                l5_avg_runs = sum(recent_5) / len(recent_5)
                # Convert to wOBA proxy: league avg 4.5 runs ≈ .320 wOBA
                l5_woba = round(0.320 + (l5_avg_runs - 4.5) * 0.012, 4)
                l5_woba = max(0.260, min(0.390, l5_woba))  # cap at reasonable range
        except Exception as e:
            log.debug("Team %s L5 runs fetch failed: %s", team_code, e)

        if season_woba:
            update_rows.append({
                "team_code":    team_code,
                "season_year":  season_year,
                "team_xwoba":   season_woba,
                "team_woba_l5": l5_woba,
            })
            log.debug("Team %s: season wOBA=%.3f, L5 wOBA=%s",
                      team_code, season_woba,
                      f"{l5_woba:.3f}" if l5_woba else "n/a")

    if not update_rows: return 0
    sql = """
        INSERT INTO team_xstats (team_code, season_year, team_xwoba, team_woba_l5, refreshed_at)
        VALUES (%(team_code)s, %(season_year)s, %(team_xwoba)s, %(team_woba_l5)s, now())
        ON CONFLICT (team_code, season_year) DO UPDATE SET
          team_xwoba=EXCLUDED.team_xwoba,
          team_woba_l5=EXCLUDED.team_woba_l5,
          refreshed_at=now();
    """
    n = db.execute_many(sql, update_rows)
    log.info("Updated %d team offensive wOBA rows (season + L5)", n)
    return n



# =============================================================================
# Whiff rate + contact rate from Baseball Savant pitch arsenal
# =============================================================================

SAVANT_ARSENAL_URL = (
    "https://baseballsavant.mlb.com/leaderboard/pitch-arsenal-stats"
    "?type=pitcher&pitchType=&year={year}&team=&min=1&csv=true"
)
SAVANT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/csv,text/plain,*/*",
    "Referer": "https://baseballsavant.mlb.com/",
}


def _refresh_pitcher_whiff(season_year: int) -> int:
    """
    Fetch pitch arsenal data from Baseball Savant and compute per-pitcher:
      - whiff_pct: weighted avg whiff rate across all pitch types (weight = pitch count)
      - swstr_pct: proxy via whiff weighted by usage (no direct overall swstr in this endpoint)
      - contact_pct: 1 - whiff_pct

    One row per pitcher per pitch type — aggregate by pitcher_id weighted by pitches.
    Uses MLB player_id directly — no ID crosswalk needed.
    """
    import csv, io
    url = SAVANT_ARSENAL_URL.format(year=season_year)
    try:
        r = requests.get(url, headers=SAVANT_HEADERS, timeout=20)
        r.raise_for_status()
        text = r.text
    except Exception as e:
        log.warning("Savant pitch arsenal fetch failed: %s", e)
        return 0

    # Parse CSV
    reader = csv.DictReader(io.StringIO(text.lstrip("﻿")))
    # Aggregate: {player_id: {total_pitches, weighted_whiff_sum}}
    pitcher_data: dict[int, dict] = {}
    for row in reader:
        try:
            pid     = int(row.get("player_id") or 0)
            pitches = int(row.get("pitches") or 0)
            whiff   = float(row.get("whiff_percent") or 0)
            if pid == 0 or pitches == 0:
                continue
            if pid not in pitcher_data:
                pitcher_data[pid] = {"total_pitches": 0, "whiff_sum": 0.0}
            pitcher_data[pid]["total_pitches"] += pitches
            pitcher_data[pid]["whiff_sum"]     += pitches * whiff
        except (ValueError, TypeError):
            continue

    if not pitcher_data:
        log.warning("No pitch arsenal data parsed")
        return 0

    # Build update rows — only for pitchers already in our DB
    existing = {r["mlb_id"] for r in db.fetchall(
        "SELECT mlb_id FROM pitcher_xstats WHERE season_year=%s", (season_year,))}

    update_rows = []
    for pid, data in pitcher_data.items():
        if pid not in existing:
            continue
        total = data["total_pitches"]
        if total == 0:
            continue
        whiff_pct   = round(data["whiff_sum"] / total / 100, 4)  # convert % to rate
        contact_pct = round(1.0 - whiff_pct, 4)
        update_rows.append({
            "mlb_id":      pid,
            "season_year": season_year,
            "whiff_pct":   whiff_pct,
            "contact_pct": contact_pct,
            "swstr_pct":   whiff_pct,  # use whiff as proxy for swstr
        })

    if not update_rows:
        log.warning("No whiff rows matched existing pitchers")
        return 0

    sql = """
        UPDATE pitcher_xstats SET
          whiff_pct=%(whiff_pct)s,
          contact_pct=%(contact_pct)s,
          swstr_pct=%(swstr_pct)s,
          refreshed_at=now()
        WHERE mlb_id=%(mlb_id)s AND season_year=%(season_year)s;
    """
    n = db.execute_many(sql, update_rows)
    log.info("Updated whiff/contact rate for %d pitchers", n)
    return n



# =============================================================================
# FB% and xFIP from Baseball Savant batted-ball leaderboard
# =============================================================================

SAVANT_BATTED_BALL_URL = (
    "https://baseballsavant.mlb.com/leaderboard/batted-ball"
    "?year={year}&type=pitcher&min=1&csv=true"
)


def _refresh_pitcher_fb_pct(season_year: int) -> int:
    """
    Fetch fly ball rate (fb_rate) from Baseball Savant batted-ball leaderboard.
    Also computes xFIP from fb_pct + hr_fb_rate + k_pct + bb9 when available.

    Endpoint returns: id, name, bbe, gb_rate, air_rate, fb_rate, ld_rate, ...
    id = MLB player_id — matches our mlb_id directly.
    fb_rate is already a rate (0-1 scale? or percent?) — check first row.
    """
    import csv, io
    url = SAVANT_BATTED_BALL_URL.format(year=season_year)
    try:
        r = requests.get(url, headers=SAVANT_HEADERS, timeout=20)
        r.raise_for_status()
        text = r.text
    except Exception as e:
        log.warning("Savant batted-ball fetch failed: %s", e)
        return 0

    reader = csv.DictReader(io.StringIO(text.lstrip("\ufeff")))
    rows = list(reader)
    if not rows:
        log.warning("No batted-ball rows parsed")
        return 0

    # Check scale of fb_rate from first row
    first = rows[0]
    fb_sample = float(first.get("fb_rate") or 0)
    # If > 1 it's a percentage (e.g. 35.5 means 35.5%), divide by 100
    scale = 100.0 if fb_sample > 1.0 else 1.0
    log.info("Savant batted-ball: %d rows, fb_rate scale=%s (sample=%.2f)",
             len(rows), scale, fb_sample)

    # Get existing pitcher data for xFIP computation
    existing = {r["mlb_id"]: r for r in db.fetchall(
        "SELECT mlb_id, k_pct, bb9, tbf, ip_total FROM pitcher_xstats WHERE season_year=%s",
        (season_year,)
    )}

    update_rows = []
    for row in rows:
        try:
            pid      = int(row.get("id") or 0)
            fb_rate  = float(row.get("fb_rate") or 0) / scale
            if pid == 0 or fb_rate == 0:
                continue
            if pid not in existing:
                continue

            px = existing[pid]
            fb_pct = round(fb_rate, 4)

            # Compute xFIP if we have the components
            # xFIP = ((13 * (FB * lgHR/FB)) + (3 * (BB + HBP)) - (2 * K)) / IP + const
            # We approximate using rates: xFIP ≈ (13*fb_pct*lgHR/FB*BF - 2*k_pct*BF + 3*bb9/9*IP) / IP + 3.10
            k_pct_val  = _coerce_float(px.get("k_pct"))
            bb9_val    = _coerce_float(px.get("bb9"))
            ip_val     = _parse_ip(str(px.get("ip_total") or 0))
            tbf_val    = _coerce_int(px.get("tbf")) or 0

            xfip = None
            if k_pct_val and bb9_val and ip_val and ip_val > 20 and tbf_val > 0:
                lg_hr_fb = 0.118  # league average HR/FB rate
                # Estimated components per IP
                k_per_9   = k_pct_val * (tbf_val / ip_val) * 3  # K/9 via k_pct * BF/IP * 3
                bb_per_9  = bb9_val
                fb_per_9  = fb_pct * (tbf_val / ip_val) * 3     # FB/9 via fb_pct * BF/IP * 3
                hr_exp_9  = fb_per_9 * lg_hr_fb
                xfip_raw  = (13 * hr_exp_9 + 3 * bb_per_9 - 2 * k_per_9) + 3.10
                xfip      = round(max(1.5, min(8.0, xfip_raw)), 2)

            update_rows.append({
                "mlb_id":      pid,
                "season_year": season_year,
                "fb_pct":      fb_pct,
                "xfip":        xfip,
            })
        except (ValueError, TypeError) as e:
            log.debug("Row parse error: %s", e)
            continue

    if not update_rows:
        log.warning("No fb_pct rows matched existing pitchers")
        return 0

    sql = """
        UPDATE pitcher_xstats SET
          fb_pct=%(fb_pct)s,
          xfip=%(xfip)s,
          refreshed_at=now()
        WHERE mlb_id=%(mlb_id)s AND season_year=%(season_year)s;
    """
    n = db.execute_many(sql, update_rows)
    log.info("Updated fb_pct and xfip for %d pitchers", n)
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
            ("pitcher whiff+contact rate", _refresh_pitcher_whiff,       "n_pitcher_whiff"),
            ("pitcher fb_pct+xfip",        _refresh_pitcher_fb_pct,      "n_pitcher_fb_pct"),
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
