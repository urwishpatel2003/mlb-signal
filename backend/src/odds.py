"""
The Odds API integration.

We use The Odds API (https://the-odds-api.com) to pull live game totals and
moneylines. Optional: pitcher props endpoint for proper book-line edges instead
of the ERA-anchored estimates we use today.

Auth: ODDS_API_KEY env var. Free tier gives 500 requests/month - plenty for
our needs (we hit it once per orchestrator run, ~4-5 times/day).

Region: US sportsbooks. Markets: h2h (moneyline), totals (game totals).
"""
from __future__ import annotations
import logging
import os
from typing import Optional
import requests
from . import db

log = logging.getLogger(__name__)

BASE = "https://api.the-odds-api.com/v4"
SPORT = "baseball_mlb"
REGIONS = "us"
MARKETS = "h2h,totals"
ODDS_FORMAT = "american"


def _api_key() -> Optional[str]:
    return os.environ.get("ODDS_API_KEY")


# Map The Odds API team names to our 4-letter codes
TEAM_NAME_TO_CODE = {
    "Arizona Diamondbacks": "ARI", "Atlanta Braves": "ATL", "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS", "Chicago Cubs": "CHC", "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN", "Cleveland Guardians": "CLE", "Colorado Rockies": "COL",
    "Detroit Tigers": "DET", "Houston Astros": "HOU", "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA", "Los Angeles Dodgers": "LAD", "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL", "Minnesota Twins": "MIN", "New York Mets": "NYM",
    "New York Yankees": "NYY", "Athletics": "ATH", "Oakland Athletics": "ATH",
    "Philadelphia Phillies": "PHI", "Pittsburgh Pirates": "PIT", "San Diego Padres": "SD",
    "San Francisco Giants": "SF", "Seattle Mariners": "SEA", "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB", "Texas Rangers": "TEX", "Toronto Blue Jays": "TOR",
    "Washington Nationals": "WSH",
}


def _to_code(name: str) -> Optional[str]:
    return TEAM_NAME_TO_CODE.get(name)


def fetch_current_odds() -> list[dict]:
    """
    Fetch all games' current odds. Returns list of game-odds records.
    Each record: {away_team, home_team, market_total, over_price, under_price,
                  away_ml, home_ml, last_update}
    """
    key = _api_key()
    if not key:
        log.warning("ODDS_API_KEY not set; skipping odds fetch")
        return []

    try:
        r = requests.get(
            f"{BASE}/sports/{SPORT}/odds",
            params={
                "apiKey": key,
                "regions": REGIONS,
                "markets": MARKETS,
                "oddsFormat": ODDS_FORMAT,
            },
            timeout=15,
        )
        r.raise_for_status()
    except requests.RequestException as e:
        log.warning("Odds API call failed: %s", e)
        return []

    out = []
    for game in r.json():
        away_code = _to_code(game.get("away_team", ""))
        home_code = _to_code(game.get("home_team", ""))
        if not away_code or not home_code:
            continue

        # Take DraftKings line first, fall back to FanDuel, then any other book.
        # Books only offer half-integer or integer totals; averaging across
        # books produces synthetic numbers like 8.7 that nobody actually offers.
        PREFERRED = ("draftkings", "fanduel", "betmgm", "caesars", "williamhill_us")
        market_total = None
        over_price = None
        under_price = None
        away_ml = None
        home_ml = None
        bookmakers_sorted = sorted(
            game.get("bookmakers", []),
            key=lambda bk: PREFERRED.index(bk.get("key", "zzz")) if bk.get("key", "zzz") in PREFERRED else 99,
        )
        for bk in bookmakers_sorted:
            for market in bk.get("markets", []):
                outcomes = market.get("outcomes", [])
                if market.get("key") == "totals" and market_total is None:
                    for o in outcomes:
                        if o.get("name") == "Over":
                            market_total = float(o["point"])
                            over_price = int(o["price"])
                        elif o.get("name") == "Under":
                            under_price = int(o["price"])
                elif market.get("key") == "h2h" and (away_ml is None or home_ml is None):
                    for o in outcomes:
                        team_code = _to_code(o.get("name", ""))
                        if team_code == away_code and away_ml is None:
                            away_ml = int(o["price"])
                        elif team_code == home_code and home_ml is None:
                            home_ml = int(o["price"])
            if market_total is not None and away_ml is not None and home_ml is not None:
                break  # We have everything we need from preferred book

        out.append({
            "away_team": away_code,
            "home_team": home_code,
            "commence_time": game.get("commence_time"),
            "market_total": market_total,
            "over_price": over_price,
            "under_price": under_price,
            "away_ml": away_ml,
            "home_ml": home_ml,
        })
    return out


def attach_odds_to_games(games) -> int:
    """
    Pull current odds and update each game row's market_total + ML prices.
    Returns count updated.
    """
    odds_records = fetch_current_odds()
    if not odds_records:
        return 0

    odds_by_pair = {(o["away_team"], o["home_team"]): o for o in odds_records}
    updated = 0
    for g in games:
        rec = odds_by_pair.get((g.away_team, g.home_team))
        if not rec or rec["market_total"] is None:
            continue
        db.execute(
            """
            UPDATE games SET
              market_total = %s,
              market_total_over_price = %s,
              market_total_under_price = %s,
              away_ml = %s,
              home_ml = %s,
              last_line_check = now()
            WHERE game_pk = %s
            """,
            (
                round(rec["market_total"], 1),
                rec["over_price"],
                rec["under_price"],
                rec["away_ml"],
                rec["home_ml"],
                g.game_pk,
            ),
        )
        updated += 1
    log.info("Updated odds on %d games", updated)
    return updated
