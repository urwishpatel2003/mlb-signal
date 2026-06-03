"""
The Odds API integration — v3.1
Fetches h2h, totals, and first_five_innings markets.
"""
from __future__ import annotations
import logging, os
from typing import Optional
import requests
from . import db

from datetime import datetime as _dt, timezone as _tz, timedelta as _td
try:
    from zoneinfo import ZoneInfo as _ZoneInfo
    _ET = _ZoneInfo("America/New_York")
except Exception:
    _ET = None


def _commence_et_date(commence_time):
    """Odds API commence_time (UTC ISO) -> ET date string 'YYYY-MM-DD', to match
    Game.game_date_et. Returns None if unparseable."""
    if not commence_time:
        return None
    try:
        d = _dt.fromisoformat(str(commence_time).replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=_tz.utc)
        d = d.astimezone(_ET) if _ET is not None else d.astimezone(_tz(_td(hours=-4)))
        return d.date().isoformat()
    except Exception:
        return None

log = logging.getLogger(__name__)
BASE = "https://api.the-odds-api.com/v4"
SPORT = "baseball_mlb"
REGIONS = "us"
ODDS_FORMAT = "american"
PREFERRED = ("draftkings", "fanduel", "betmgm", "caesars", "williamhill_us")

TEAM_NAME_TO_CODE = {
    "Arizona Diamondbacks":"ARI","Atlanta Braves":"ATL","Baltimore Orioles":"BAL",
    "Boston Red Sox":"BOS","Chicago Cubs":"CHC","Chicago White Sox":"CWS",
    "Cincinnati Reds":"CIN","Cleveland Guardians":"CLE","Colorado Rockies":"COL",
    "Detroit Tigers":"DET","Houston Astros":"HOU","Kansas City Royals":"KC",
    "Los Angeles Angels":"LAA","Los Angeles Dodgers":"LAD","Miami Marlins":"MIA",
    "Milwaukee Brewers":"MIL","Minnesota Twins":"MIN","New York Mets":"NYM",
    "New York Yankees":"NYY","Athletics":"ATH","Oakland Athletics":"ATH",
    "Philadelphia Phillies":"PHI","Pittsburgh Pirates":"PIT","San Diego Padres":"SD",
    "San Francisco Giants":"SF","Seattle Mariners":"SEA","St. Louis Cardinals":"STL",
    "Tampa Bay Rays":"TB","Texas Rangers":"TEX","Toronto Blue Jays":"TOR",
    "Washington Nationals":"WSH",
}

def _api_key(): return os.environ.get("ODDS_API_KEY")
def _to_code(name): return TEAM_NAME_TO_CODE.get(name)

def _best_book(bookmakers, key, callback):
    """Walk bookmakers in preferred order, call callback(outcomes) until it returns a value."""
    bks = sorted(bookmakers, key=lambda b: PREFERRED.index(b.get("key","zzz")) if b.get("key") in PREFERRED else 99)
    for bk in bks:
        for market in bk.get("markets", []):
            if market.get("key") == key:
                result = callback(market.get("outcomes", []))
                if result is not None:
                    return result
    return None


import statistics as _stats


def _american_to_prob(odds_val):
    """American odds -> implied probability (no-vig not applied)."""
    if odds_val is None:
        return None
    o = float(odds_val)
    if o < 0:
        return (-o) / ((-o) + 100.0)
    return 100.0 / (o + 100.0)


def _all_book_totals(bookmakers):
    """Collect every book's main total line. Returns list of floats."""
    vals = []
    for bk in bookmakers:
        for market in bk.get("markets", []):
            if market.get("key") != "totals":
                continue
            # main total: take the Over outcome's point
            for o in market.get("outcomes", []):
                if o.get("name") == "Over" and o.get("point") is not None:
                    try:
                        vals.append(float(o["point"]))
                    except (TypeError, ValueError):
                        pass
    return vals


def _all_book_ml(bookmakers, away_code, home_code, to_code):
    """Collect every book's ML for away/home. Returns (away_list, home_list)."""
    away_vals, home_vals = [], []
    for bk in bookmakers:
        for market in bk.get("markets", []):
            if market.get("key") != "h2h":
                continue
            for o in market.get("outcomes", []):
                tc = to_code(o.get("name", ""))
                try:
                    price = int(o["price"])
                except (TypeError, ValueError, KeyError):
                    continue
                if tc == away_code:
                    away_vals.append(price)
                elif tc == home_code:
                    home_vals.append(price)
    return away_vals, home_vals


def _consensus_total(chosen_total, all_totals):
    """Return (median, min, max, n, warning_bool). Warn if chosen differs >0.5."""
    if not all_totals:
        return None, None, None, 0, False
    med = _stats.median(all_totals)
    warn = chosen_total is not None and abs(chosen_total - med) > 0.5
    return round(med, 1), round(min(all_totals), 1), round(max(all_totals), 1), len(all_totals), warn


def _consensus_ml(chosen_ml, all_ml):
    """Return (consensus_median_ml, warning_bool). Warn if implied prob differs >5pp."""
    if not all_ml:
        return None, False
    med = int(_stats.median(all_ml))
    cp = _american_to_prob(chosen_ml)
    mp = _american_to_prob(med)
    warn = (cp is not None and mp is not None and abs(cp - mp) > 0.05)
    return med, warn


def _get_raw(markets: str) -> list:
    key = _api_key()
    if not key: return []
    try:
        r = requests.get(f"{BASE}/sports/{SPORT}/odds",
            params={"apiKey":key,"regions":REGIONS,"markets":markets,"oddsFormat":ODDS_FORMAT}, timeout=15)
        r.raise_for_status(); return r.json()
    except requests.RequestException as e:
        log.warning("Odds API %s call failed: %s", markets, e); return []

def fetch_current_odds() -> list[dict]:
    """Fetch full-game + F5 odds. Returns merged list per game."""
    full_data = _get_raw("h2h,totals")

    # F5 totals — per-event endpoint (bulk endpoint returns 422 on our tier)
    # Reuse event IDs from full_data to avoid extra API calls
    f5_lookup: dict[tuple, dict] = {}
    api_key = _api_key()
    if api_key:
        for _game in full_data:
            _eid = _game.get("id")
            _ac  = _to_code(_game.get("away_team",""))
            _hc  = _to_code(_game.get("home_team",""))
            if not _eid or not _ac or not _hc: continue
            _edate = _commence_et_date(_game.get("commence_time"))
            try:
                _fr = requests.get(
                    f"{BASE}/sports/{SPORT}/events/{_eid}/odds",
                    params={"apiKey":api_key,"regions":REGIONS,
                            "markets":"totals_1st_5_innings","oddsFormat":ODDS_FORMAT},
                    timeout=8)
                if _fr.status_code != 200: continue
                _fd = _fr.json()
                def _parse_f5(outcomes):
                    t = ov = un = None
                    for o in outcomes:
                        if o.get("name")=="Over":  t=float(o["point"]); ov=int(o["price"])
                        elif o.get("name")=="Under": un=int(o["price"])
                    return {"market_f5_total":t,"market_f5_over_price":ov,"market_f5_under_price":un} if t else None
                _res = _best_book(_fd.get("bookmakers",[]), "totals_1st_5_innings", _parse_f5)
                if _res: f5_lookup[(_ac,_hc,_edate)] = _res
            except Exception as _fe:
                log.debug("F5 per-event fetch failed for %s@%s: %s", _ac, _hc, _fe)

    out = []
    for game in full_data:
        ac, hc = _to_code(game.get("away_team","")), _to_code(game.get("home_team",""))
        if not ac or not hc: continue
        bks = game.get("bookmakers", [])

        def parse_total(outcomes):
            t=op=up=None
            for o in outcomes:
                if o.get("name")=="Over":  t=float(o["point"]); op=int(o["price"])
                elif o.get("name")=="Under": up=int(o["price"])
            return (t,op,up) if t else None
        def parse_ml(outcomes):
            am=hm=None
            for o in outcomes:
                tc=_to_code(o.get("name",""))
                if tc==ac: am=int(o["price"])
                elif tc==hc: hm=int(o["price"])
            return (am,hm) if am and hm else None

        total_r = _best_book(bks,"totals",parse_total)
        ml_r    = _best_book(bks,"h2h",parse_ml)
        if not total_r: continue
        mt,op,up = total_r
        am,hm = ml_r if ml_r else (None,None)

        # --- cross-book consensus + deviation warning ---
        _all_tot = _all_book_totals(bks)
        _tot_med, _tot_min, _tot_max, _tot_n, _tot_warn = _consensus_total(mt, _all_tot)
        _away_ml_list, _home_ml_list = _all_book_ml(bks, ac, hc, _to_code)
        _away_ml_cons, _away_warn = _consensus_ml(am, _away_ml_list)
        _home_ml_cons, _home_warn = _consensus_ml(hm, _home_ml_list)
        _warn = bool(_tot_warn or _away_warn or _home_warn)
        _warn_detail = None
        if _warn:
            _parts = []
            if _tot_warn:
                _parts.append(f"total {mt} vs consensus {_tot_med}")
            if _away_warn:
                _parts.append(f"away ML {am} vs consensus {_away_ml_cons}")
            if _home_warn:
                _parts.append(f"home ML {hm} vs consensus {_home_ml_cons}")
            _warn_detail = "; ".join(_parts)
            log.warning("Line deviation %s@%s: %s", ac, hc, _warn_detail)

        # --- cross-book consensus + deviation warning ---
        _all_tot = _all_book_totals(bks)
        _tot_med, _tot_min, _tot_max, _tot_n, _tot_warn = _consensus_total(mt, _all_tot)
        _away_ml_list, _home_ml_list = _all_book_ml(bks, ac, hc, _to_code)
        _away_ml_cons, _away_warn = _consensus_ml(am, _away_ml_list)
        _home_ml_cons, _home_warn = _consensus_ml(hm, _home_ml_list)
        _warn = bool(_tot_warn or _away_warn or _home_warn)
        _warn_detail = None
        if _warn:
            _parts = []
            if _tot_warn:
                _parts.append(f"total {mt} vs consensus {_tot_med}")
            if _away_warn:
                _parts.append(f"away ML {am} vs consensus {_away_ml_cons}")
            if _home_warn:
                _parts.append(f"home ML {hm} vs consensus {_home_ml_cons}")
            _warn_detail = "; ".join(_parts)
            log.warning("Line deviation %s@%s: %s", ac, hc, _warn_detail)
        edate = _commence_et_date(game.get("commence_time"))
        f5 = f5_lookup.get((ac,hc,edate),{})
        out.append({"away_team":ac,"home_team":hc,"commence_time":game.get("commence_time"),"game_date_et":edate,
            "market_total":mt,"over_price":op,"under_price":up,"away_ml":am,"home_ml":hm,
            "market_f5_total":f5.get("market_f5_total"),"market_f5_over_price":f5.get("market_f5_over_price"),
            "market_f5_under_price":f5.get("market_f5_under_price"),
            "market_total_consensus":_tot_med,"market_total_min":_tot_min,"market_total_max":_tot_max,
            "market_total_n_books":_tot_n,"away_ml_consensus":_away_ml_cons,"home_ml_consensus":_home_ml_cons,
            "line_warning":_warn,"line_warning_detail":_warn_detail})
    return out

def attach_odds_to_games(games) -> int:
    odds_records = fetch_current_odds()
    if not odds_records: return 0
    odds_by_pair = {(o["away_team"],o["home_team"],o.get("game_date_et")):o for o in odds_records}
    updated = 0
    for g in games:
        rec = odds_by_pair.get((g.away_team,g.home_team,g.game_date_et))
        if not rec or rec["market_total"] is None: continue
        db.execute("""
            UPDATE games SET
              market_total=%s, market_total_over_price=%s, market_total_under_price=%s,
              away_ml=%s, home_ml=%s,
              market_f5_total=%s, market_f5_over_price=%s, market_f5_under_price=%s,
              market_total_consensus=%s, market_total_min=%s, market_total_max=%s,
              market_total_n_books=%s, away_ml_consensus=%s, home_ml_consensus=%s,
              line_warning=%s, line_warning_detail=%s,
              last_line_check=now()
            WHERE game_pk=%s""",
            (round(rec["market_total"],1),rec["over_price"],rec["under_price"],
             rec["away_ml"],rec["home_ml"],
             rec.get("market_f5_total"),rec.get("market_f5_over_price"),rec.get("market_f5_under_price"),
             rec.get("market_total_consensus"),rec.get("market_total_min"),rec.get("market_total_max"),
             rec.get("market_total_n_books"),rec.get("away_ml_consensus"),rec.get("home_ml_consensus"),
             rec.get("line_warning",False),rec.get("line_warning_detail"),
             g.game_pk))
        updated += 1
    log.info("Updated odds on %d games", updated)
    return updated

