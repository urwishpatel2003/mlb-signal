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

        # Average across bookmakers for a stable consensus number
        totals: list[float] = []
        over_prices: list[int] = []
        under_prices: list[int] = []
        away_mls: list[int] = []
        home_mls: list[int] = []

        for bk in game.get("bookmakers", []):
            for market in bk.get("markets", []):
                outcomes = market.get("outcomes", [])
                if market.get("key") == "totals":
                    for o in outcomes:
                        if o.get("name") == "Over":
                            totals.append(float(o["point"]))
                            over_prices.append(int(o["price"]))
                        elif o.get("name") == "Under":
                            under_prices.append(int(o["price"]))
                elif market.get("key") == "h2h":
                    for o in outcomes:
                        team_code = _to_code(o.get("name", ""))
                        if team_code == away_code:
                            away_mls.append(int(o["price"]))
                        elif team_code == home_code:
                            home_mls.append(int(o["price"]))

        def mean_or_none(xs):
            return sum(xs) / len(xs) if xs else None

        out.append({
            "away_team": away_code,
            "home_team": home_code,
            "commence_time": game.get("commence_time"),
            "market_total": mean_or_none(totals),
            "over_price": int(mean_or_none(over_prices)) if over_prices else None,
            "under_price": int(mean_or_none(under_prices)) if under_prices else None,
            "away_ml": int(mean_or_none(away_mls)) if away_mls else None,
            "home_ml": int(mean_or_none(home_mls)) if home_mls else None,
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
