"""
The Odds API integration — v3.1
Fetches h2h, totals, and first_five_innings markets.
"""
from __future__ import annotations
import logging, os
from typing import Optional
import requests
from . import db

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

    # F5 — separate call; non-fatal if unavailable
    f5_data = _get_raw("first_five_innings")
    f5_lookup: dict[tuple, dict] = {}
    for game in f5_data:
        ac, hc = _to_code(game.get("away_team","")), _to_code(game.get("home_team",""))
        if not ac or not hc or (ac,hc) in f5_lookup: continue
        def parse_f5(outcomes):
            t = ov = un = None
            for o in outcomes:
                if o.get("name")=="Over":  t=float(o["point"]); ov=int(o["price"])
                elif o.get("name")=="Under": un=int(o["price"])
            return {"market_f5_total":t,"market_f5_over_price":ov,"market_f5_under_price":un} if t else None
        result = _best_book(game.get("bookmakers",[]), "first_five_innings", parse_f5)
        if result: f5_lookup[(ac,hc)] = result

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
        f5 = f5_lookup.get((ac,hc),{})
        out.append({"away_team":ac,"home_team":hc,"commence_time":game.get("commence_time"),
            "market_total":mt,"over_price":op,"under_price":up,"away_ml":am,"home_ml":hm,
            "market_f5_total":f5.get("market_f5_total"),"market_f5_over_price":f5.get("market_f5_over_price"),
            "market_f5_under_price":f5.get("market_f5_under_price")})
    return out

def attach_odds_to_games(games) -> int:
    odds_records = fetch_current_odds()
    if not odds_records: return 0
    odds_by_pair = {(o["away_team"],o["home_team"]):o for o in odds_records}
    updated = 0
    for g in games:
        rec = odds_by_pair.get((g.away_team,g.home_team))
        if not rec or rec["market_total"] is None: continue
        db.execute("""
            UPDATE games SET
              market_total=%s, market_total_over_price=%s, market_total_under_price=%s,
              away_ml=%s, home_ml=%s,
              market_f5_total=%s, market_f5_over_price=%s, market_f5_under_price=%s,
              last_line_check=now()
            WHERE game_pk=%s""",
            (round(rec["market_total"],1),rec["over_price"],rec["under_price"],
             rec["away_ml"],rec["home_ml"],
             rec.get("market_f5_total"),rec.get("market_f5_over_price"),rec.get("market_f5_under_price"),
             g.game_pk))
        updated += 1
    log.info("Updated odds on %d games", updated)
    return updated
