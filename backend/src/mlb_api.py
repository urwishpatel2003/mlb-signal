"""
MLB Stats API client.

This is the single source of truth for: schedule, probable pitchers, confirmed
lineups, weather, venue, final scores, and box-score lines. It's the API that
MLB.com itself consumes. No auth required, never stale, free, structured JSON.

Endpoints we use:
  /api/v1/schedule      - today's games + probables + lineups + venue + weather
  /api/v1.1/game/{id}   - full live feed (scores, box, every plate appearance)
  /api/v1/teams         - team metadata (abbrev, league, division)
  /api/v1/people/{id}   - player metadata (handedness, position, full name)

Caching:
  We cache in Postgres with an `etag` and `expires_at`. Schedule data refreshed
  every 15 minutes; box scores once final; player metadata indefinitely.

Resiliency:
  Every API call is wrapped in tenacity-style retry with exponential backoff.
  We log failures to the orchestrator for ntfy alert.
"""
from __future__ import annotations
import logging
import time
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timezone, timedelta
from typing import Optional
import requests

log = logging.getLogger(__name__)

BASE = "https://statsapi.mlb.com"
USER_AGENT = "mlb-signal/0.1 (https://github.com/urwishpatel2003/mlb-signal)"
TIMEOUT = 15


class StatsAPIError(RuntimeError):
    """Raised when the MLB Stats API returns an unrecoverable error."""


def _request(path: str, params: Optional[dict] = None, retries: int = 3) -> dict:
    """GET helper with retry + backoff. Returns parsed JSON or raises."""
    url = f"{BASE}{path}"
    last_err: Optional[Exception] = None
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params or {},
                             headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except (requests.HTTPError, requests.ConnectionError, requests.Timeout) as e:
            last_err = e
            if attempt < retries - 1:
                backoff = 2 ** attempt
                log.warning("MLB API %s failed (%s), retrying in %ds", path, e, backoff)
                time.sleep(backoff)
    raise StatsAPIError(f"MLB API {path} failed after {retries} retries: {last_err}")


# ---------- Domain types ----------

@dataclass
class ProbablePitcher:
    mlb_id: int
    full_name: str
    last_first: str       # "Crochet, Garrett" - matches Statcast CSV format
    hand: str             # "L" or "R"


@dataclass
class HitterSpot:
    mlb_id: int
    full_name: str
    last_first: str
    bat_side: str         # "L" / "R" / "S"
    position: str
    order: int            # 1-9


@dataclass
class Venue:
    mlb_id: int
    name: str
    city: str
    lat: Optional[float] = None
    lon: Optional[float] = None
    roof_type: Optional[str] = None    # "open" / "closed" / "dome" / "retractable"


@dataclass
class GameWeather:
    condition: Optional[str] = None
    temp_f: Optional[int] = None
    wind: Optional[str] = None         # MLB API gives raw string: "10 mph, Out To CF"


@dataclass
class Game:
    game_pk: int
    game_date_et: str
    game_time_et: str
    status: str                        # "Scheduled", "Live", "Final", "Postponed"
    away_team: str                     # 3-letter abbrev (e.g., "BOS")
    home_team: str
    away_record: str                   # "12-15"
    home_record: str
    venue: Optional[Venue] = None
    weather: Optional[GameWeather] = None
    away_pitcher: Optional[ProbablePitcher] = None
    home_pitcher: Optional[ProbablePitcher] = None
    away_lineup: list[HitterSpot] = field(default_factory=list)
    home_lineup: list[HitterSpot] = field(default_factory=list)
    away_score: Optional[int] = None
    home_score: Optional[int] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @property
    def lineups_confirmed(self) -> bool:
        return len(self.away_lineup) >= 9 and len(self.home_lineup) >= 9

    @property
    def is_final(self) -> bool:
        return self.status.startswith("Final") or self.status == "Game Over"


# ---------- Helpers ----------

def _normalize_name(person: dict) -> tuple[str, str]:
    """Returns (full_name, last_first). Last_first matches Statcast convention."""
    full = person.get("fullName") or ""
    last = person.get("lastName") or ""
    first = person.get("firstName") or ""
    last_first = f"{last}, {first}".strip(", ") if (last or first) else full
    return full, last_first


def _parse_pitcher(person: Optional[dict]) -> Optional[ProbablePitcher]:
    if not person or not person.get("id"):
        return None
    full, lf = _normalize_name(person)
    hand = (person.get("pitchHand") or {}).get("code", "R")
    return ProbablePitcher(
        mlb_id=int(person["id"]),
        full_name=full or lf,
        last_first=lf,
        hand=hand,
    )


def _parse_lineup(side_block: dict, players_block: dict) -> list[HitterSpot]:
    """Extract batting order from a boxscore-style team block."""
    batting_order = side_block.get("battingOrder") or []
    if not batting_order:
        return []
    out: list[HitterSpot] = []
    for idx, pid_raw in enumerate(batting_order, start=1):
        # Some entries have positions encoded; the bare ID has no '00' suffix
        pid = int(str(pid_raw)[:6])
        person_block = players_block.get(f"ID{pid}", {})
        person = person_block.get("person", {})
        position = (person_block.get("position") or {}).get("abbreviation", "")
        full, lf = _normalize_name(person)
        bat_side = (person.get("batSide") or {}).get("code", "R")
        out.append(HitterSpot(
            mlb_id=pid,
            full_name=full or lf,
            last_first=lf,
            bat_side=bat_side,
            position=position,
            order=idx,
        ))
    return out


def _parse_venue(venue_block: dict, weather_block: Optional[dict] = None) -> Venue:
    loc = venue_block.get("location") or {}
    return Venue(
        mlb_id=venue_block.get("id", 0),
        name=venue_block.get("name", ""),
        city=loc.get("city", ""),
        lat=loc.get("defaultCoordinates", {}).get("latitude"),
        lon=loc.get("defaultCoordinates", {}).get("longitude"),
        roof_type=(venue_block.get("fieldInfo") or {}).get("roofType"),
    )


def _parse_weather(weather_block: Optional[dict]) -> Optional[GameWeather]:
    if not weather_block:
        return None
    return GameWeather(
        condition=weather_block.get("condition"),
        temp_f=int(weather_block["temp"]) if weather_block.get("temp") else None,
        wind=weather_block.get("wind"),
    )


# ---------- Public API ----------

def get_schedule(target_date: Optional[date] = None) -> list[Game]:
    """
    Get today's full schedule with probable pitchers, lineups (if posted), venue,
    weather, final scores. This is THE function the orchestrator calls each run.

    `hydrate=` pulls richer data on each game (lineups, weather, probables,
    decisions, person details). Costs roughly the same as the bare endpoint.
    """
    target_date = target_date or date.today()
    params = {
        "sportId": 1,
        "date": target_date.isoformat(),
        "hydrate": (
            "probablePitcher,linescore,team,venue,weather,decisions,"
            "person(stats(group=[hitting,pitching],type=[season])),"
            "game(content(summary,media(epg))),"
            "lineups"
        ),
    }
    payload = _request("/api/v1/schedule", params=params)

    games: list[Game] = []
    for date_block in payload.get("dates", []):
        for raw in date_block.get("games", []):
            try:
                games.append(_parse_game(raw))
            except Exception as e:
                log.error("Failed to parse game %s: %s", raw.get("gamePk"), e)
    return games

TEAM_ABBR_NORMALIZATION = {
    "AZ": "ARI",
    "OAK": "ATH",
    "CHW": "CWS",
}

def _parse_game(raw: dict) -> Game:
    teams = raw.get("teams", {})
    away_t = teams.get("away", {})
    home_t = teams.get("home", {})

    def abbr(team_block: dict) -> str:
        t = team_block.get("team", {})
        raw_abbr = t.get("abbreviation") or t.get("teamCode", "").upper() or "?"
        return TEAM_ABBR_NORMALIZATION.get(raw_abbr, raw_abbr)

    def record(team_block: dict) -> str:
        rec = team_block.get("leagueRecord", {})
        if rec:
            return f"{rec.get('wins', 0)}-{rec.get('losses', 0)}"
        return ""

    # Lineups are inside the "lineups" hydrate block when posted
    lineups_block = raw.get("lineups") or {}
    away_lineup_block = lineups_block.get("awayPlayers") or []
    home_lineup_block = lineups_block.get("homePlayers") or []

    def lineup_from_hydrate(arr) -> list[HitterSpot]:
        result: list[HitterSpot] = []
        for idx, p in enumerate(arr or [], start=1):
            full, lf = _normalize_name(p)
            result.append(HitterSpot(
                mlb_id=int(p.get("id", 0)),
                full_name=full or lf,
                last_first=lf,
                bat_side=(p.get("batSide") or {}).get("code", "R"),
                position=(p.get("primaryPosition") or {}).get("abbreviation", ""),
                order=idx,
            ))
        return result

    g = Game(
        game_pk=int(raw["gamePk"]),
        game_date_et=_to_et_date(raw.get("gameDate", "")),
        game_time_et=_to_et_time(raw.get("gameDate", "")),
        status=raw.get("status", {}).get("detailedState", "Unknown"),
        away_team=abbr(away_t),
        home_team=abbr(home_t),
        away_record=record(away_t),
        home_record=record(home_t),
        venue=_parse_venue(raw.get("venue", {})),
        weather=_parse_weather(raw.get("weather")),
        away_pitcher=_parse_pitcher(away_t.get("probablePitcher")),
        home_pitcher=_parse_pitcher(home_t.get("probablePitcher")),
        away_lineup=lineup_from_hydrate(away_lineup_block),
        home_lineup=lineup_from_hydrate(home_lineup_block),
        away_score=away_t.get("score"),
        home_score=home_t.get("score"),
    )
    return g


def get_box_score(game_pk: int) -> dict:
    """
    Fetch the full live feed for a finished game. Used by the nightly grader
    to extract pitcher lines (IP/H/ER/BB/K) and final scores.

    Returns the raw JSON payload; the grader walks it for what it needs.
    """
    return _request(f"/api/v1.1/game/{game_pk}/feed/live")


def extract_pitcher_lines(box_payload: dict) -> dict[int, dict]:
    """
    From a live feed payload, pull each pitcher's final line by mlb_id.
    Returns: {mlb_id: {"ip": float, "h": int, "er": int, "bb": int, "k": int, "outs": int}}
    """
    out: dict[int, dict] = {}
    box = (box_payload.get("liveData") or {}).get("boxscore") or {}
    for side in ("away", "home"):
        team_block = box.get("teams", {}).get(side, {})
        players = team_block.get("players", {}) or {}
        for pid_key, pblock in players.items():
            stats = (pblock.get("stats") or {}).get("pitching") or {}
            if not stats or "inningsPitched" not in stats:
                continue
            ip_str = stats.get("inningsPitched", "0.0")
            # MLB encodes 5 as "5.1" (one out), 5 as "5.2" (two outs)
            try:
                whole, frac = ip_str.split(".") if "." in ip_str else (ip_str, "0")
                ip = int(whole) + (int(frac) / 3.0)
                outs = int(whole) * 3 + int(frac)
            except (ValueError, TypeError):
                ip, outs = 0.0, 0
            pid = int(pid_key.lstrip("ID"))
            out[pid] = {
                "ip": round(ip, 2),
                "outs": outs,
                "h": int(stats.get("hits") or 0),
                "er": int(stats.get("earnedRuns") or 0),
                "r": int(stats.get("runs") or 0),
                "bb": int(stats.get("baseOnBalls") or 0),
                "k": int(stats.get("strikeOuts") or 0),
                "bf": int(stats.get("battersFaced") or 0),
                "pitches": int(stats.get("pitchesThrown") or 0),
                "name": (pblock.get("person") or {}).get("fullName", ""),
            }
    return out


def get_player(mlb_id: int) -> dict:
    """Fetch a player's metadata (handedness, position, full name, etc.)."""
    payload = _request(f"/api/v1/people/{mlb_id}")
    people = payload.get("people", [])
    if not people:
        raise StatsAPIError(f"Player {mlb_id} not found")
    return people[0]


def games_by_date_range(start: date, end: date) -> dict[str, list[Game]]:
    """For backfill / rolling backtest. Returns {date_iso: [Game, ...]}."""
    params = {
        "sportId": 1,
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "hydrate": "probablePitcher,linescore,team,venue,decisions",
    }
    payload = _request("/api/v1/schedule", params=params)
    out: dict[str, list[Game]] = {}
    for date_block in payload.get("dates", []):
        d = date_block.get("date", "")
        out[d] = [_parse_game(g) for g in date_block.get("games", [])]
    return out


# ---------- CLI for quick verification ----------

if __name__ == "__main__":
    import sys
    import json as _json
    if len(sys.argv) > 1 and sys.argv[1] == "today":
        games = get_schedule()
        for g in games:
            ap = g.away_pitcher.last_first if g.away_pitcher else "?"
            hp = g.home_pitcher.last_first if g.home_pitcher else "?"
            lc = "" if g.lineups_confirmed else "-"
            print(f"  {g.game_time_et} {g.away_team:>3}@{g.home_team:<3}  "
                  f"{ap:25s} vs {hp:25s}  status={g.status:12s}  lineups={lc}")
    elif len(sys.argv) > 2 and sys.argv[1] == "box":
        box = get_box_score(int(sys.argv[2]))
        lines = extract_pitcher_lines(box)
        for pid, line in lines.items():
            print(f"  {line['name']:25s} {line['ip']} IP, {line['h']} H, "
                  f"{line['er']} ER, {line['bb']} BB, {line['k']} K, {line['pitches']} pit")
    else:
        print("Usage: python -m src.mlb_api today")
        print("       python -m src.mlb_api box <gamePk>")
