"""
Pitcher prop lines via The Odds API (the-odds-api.com).

Uses the same ODDS_API_KEY as src/odds.py. The /events/{eventId}/odds
endpoint supports player prop markets (one credit per market per region
per event).

Markets we query (1 credit each):
  pitcher_strikeouts
  pitcher_outs
  pitcher_earned_runs
  pitcher_hits_allowed

Per-run cost: ~15 events x 4 markets = 60 credits.
At 5 runs/day, ~9000 credits/month for props alone.

Bookmaker preference order: DraftKings -> FanDuel -> BetMGM -> Caesars.
First book that has a line for that market wins. We do not blend lines.

Failure modes:
  - 401/403/429: log warning, return {} (orchestrator falls back to estimate)
  - Network: same
  - Schema mismatch: same
The graceful fallback path means a temporary outage never crashes the run.
"""
from __future__ import annotations
import logging
import os
import re
import time
from typing import Optional
import requests

log = logging.getLogger(__name__)

API_BASE = "https://api.the-odds-api.com/v4"
SPORT = "baseball_mlb"
REGION = "us"

# Map our internal category code -> The Odds API market key
PROP_MARKETS = {
    "K":    "pitcher_strikeouts",
    "Outs": "pitcher_outs",
    "ER":   "pitcher_earned_runs",
    "Hits": "pitcher_hits_allowed",
}

# Reverse map for parsing responses
MARKET_TO_CAT = {v: k for k, v in PROP_MARKETS.items()}

# Bookmaker preference order. First match wins.
PREFERRED_BOOKS = ("draftkings", "fanduel", "betmgm", "caesars", "williamhill_us")

# Comma-separated markets string for one API call
ALL_MARKETS = ",".join(PROP_MARKETS.values())


def _api_key() -> Optional[str]:
    return os.environ.get("ODDS_API_KEY")


def _normalize_name(s: str) -> str:
    """Match 'Last, First' (our format) to 'First Last' (Odds API format)."""
    if not s:
        return ""
    if "," in s:
        last, _, first = s.partition(",")
        s = f"{last.strip()} {first.strip()}"
    s = re.sub(r"[^a-zA-Z\s]", "", s).strip().lower()
    s = re.sub(r"\s+", " ", s)
    for suffix in (" jr", " sr", " ii", " iii", " iv"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    return s


def _fetch_events(timeout: int = 10) -> list[dict]:
    """Get list of MLB events with IDs. Costs 1 credit."""
    key = _api_key()
    if not key:
        log.warning("ODDS_API_KEY not set, skipping prop fetch")
        return []
    url = f"{API_BASE}/sports/{SPORT}/events"
    try:
        r = requests.get(url, params={"apiKey": key}, timeout=timeout)
        if r.status_code != 200:
            log.warning("Odds API events returned %d: %s",
                        r.status_code, r.text[:200])
            return []
        return r.json()
    except (requests.RequestException, ValueError) as e:
        log.warning("Odds API events fetch failed: %s", e)
        return []


def _fetch_event_props(event_id: str, timeout: int = 10) -> Optional[dict]:
    """Get all prop markets for one event. Costs ~4 credits."""
    key = _api_key()
    if not key:
        return None
    url = f"{API_BASE}/sports/{SPORT}/events/{event_id}/odds"
    params = {
        "apiKey":      key,
        "regions":     REGION,
        "markets":     ALL_MARKETS,
        "oddsFormat":  "american",
    }
    try:
        r = requests.get(url, params=params, timeout=timeout)
        if r.status_code == 200:
            return r.json()
        log.warning("Odds API event %s returned %d", event_id, r.status_code)
        return None
    except (requests.RequestException, ValueError) as e:
        log.warning("Odds API event %s fetch failed: %s", event_id, e)
        return None


def _extract_props_from_event(payload: dict) -> dict[str, dict[str, dict]]:
    """
    Walk one event's bookmakers and extract prop lines per pitcher.
    Returns: {normalized_name: {category: {line, over_price, under_price}}}.

    Schema:
      bookmakers[].markets[].outcomes[]
        - name: 'Over' or 'Under'
        - description: pitcher name (e.g. 'Shohei Ohtani')
        - point: line value (e.g. 5.5)
        - price: american odds (e.g. -110)
    """
    out: dict[str, dict[str, dict]] = {}
    if not payload:
        return out

    bookmakers = payload.get("bookmakers") or []
    if not bookmakers:
        return out

    # Order books by our preference (preferred first, others after)
    pref_index = {b: i for i, b in enumerate(PREFERRED_BOOKS)}
    bookmakers = sorted(
        bookmakers,
        key=lambda bm: pref_index.get(bm.get("key", ""), 999)
    )

    for bm in bookmakers:
        markets = bm.get("markets") or []
        for market in markets:
            mkey = market.get("key")
            cat = MARKET_TO_CAT.get(mkey)
            if not cat:
                continue
            outcomes = market.get("outcomes") or []
            # Group outcomes by pitcher name (description)
            by_pitcher: dict[str, dict[str, dict]] = {}
            for outc in outcomes:
                pname = outc.get("description")
                side = (outc.get("name") or "").strip().lower()
                line = outc.get("point")
                price = outc.get("price")
                if not pname or line is None:
                    continue
                if pname not in by_pitcher:
                    by_pitcher[pname] = {"line": float(line)}
                if side == "over":
                    by_pitcher[pname]["over_price"] = price
                elif side == "under":
                    by_pitcher[pname]["under_price"] = price
            for pname, prop in by_pitcher.items():
                # Only set if a preferred book hasn't already provided this category
                key = _normalize_name(pname)
                if key not in out:
                    out[key] = {}
                if cat not in out[key]:  # first book wins
                    out[key][cat] = prop
    return out


def fetch_pitcher_props_for_today() -> dict[str, dict[str, dict]]:
    """
    Pull all available pitcher prop lines for today's MLB events.
    Returns: {normalized_pitcher_name: {category: {line, over_price, under_price}}}.

    Cost: ~1 (events) + 4 per event (props). For 15 games: ~61 credits.
    """
    if not _api_key():
        log.warning("ODDS_API_KEY not set, skipping pitcher props")
        return {}

    events = _fetch_events()
    if not events:
        log.warning("No MLB events returned from Odds API")
        return {}
    log.info("Odds API: fetching pitcher props for %d events", len(events))

    result: dict[str, dict[str, dict]] = {}
    fetched = 0
    for ev in events:
        eid = ev.get("id")
        if not eid:
            continue
        payload = _fetch_event_props(eid)
        if not payload:
            continue
        event_props = _extract_props_from_event(payload)
        for name_key, cats in event_props.items():
            if name_key not in result:
                result[name_key] = {}
            for cat, prop in cats.items():
                if cat not in result[name_key]:
                    result[name_key][cat] = prop
        fetched += 1
        # Be polite - small sleep so we don't hammer the API
        time.sleep(0.15)

    log.info("Odds API pitcher props: %d unique pitchers across %d events",
             len(result), fetched)
    return result


def lookup_lines(pitcher_name: str,
                 odds_lines: dict[str, dict[str, dict]]) -> Optional[dict[str, float]]:
    """
    Given a pitcher's "Last, First" name and the cached lookup, return:
      {"K": 6.5, "Outs": 16.5, "ER": 1.5, "Hits": 5.5}
    or None if pitcher isn't in the data.
    """
    if not pitcher_name or not odds_lines:
        return None
    key = _normalize_name(pitcher_name)
    pitcher_props = odds_lines.get(key)
    if not pitcher_props:
        return None
    out: dict[str, float] = {}
    for cat, prop in pitcher_props.items():
        line = prop.get("line")
        if line is not None:
            out[cat] = float(line)
    return out or None


# CLI for quick verification
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    props = fetch_pitcher_props_for_today()
    print(f"\nFound props for {len(props)} pitchers\n")
    for i, (name, cats) in enumerate(props.items()):
        if i >= 8:
            break
        print(f"  {name}:")
        for cat, prop in cats.items():
            o = prop.get("over_price", "?")
            u = prop.get("under_price", "?")
            print(f"    {cat}: line={prop['line']}, O={o}, U={u}")
