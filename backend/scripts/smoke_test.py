"""
Smoke test for post-deployment verification.

Run this after Railway deploy (or after any major dependency change) to verify
each data source is reachable and returning sane data:

  python -m scripts.smoke_test

What it tests:
  1. Postgres connection (DATABASE_URL)
  2. MLB Stats API reachable
  3. Odds API reachable (if ODDS_API_KEY set)
  4. NWS API reachable (sample city: Cleveland)
  5. Open-Meteo reachable (fallback for international parks)

Exits 0 if all pass, non-zero on any failure.
"""
from __future__ import annotations
import sys
import os
from pathlib import Path
from datetime import date

sys.path.insert(0, str(Path(__file__).parent.parent))

from src import db, mlb_api, odds, weather


def check(name: str, fn) -> bool:
    try:
        result = fn()
        print(f"  ✓ {name}: {result}")
        return True
    except Exception as e:
        print(f"  ✗ {name}: {type(e).__name__}: {e}")
        return False


def test_postgres():
    row = db.fetchone("SELECT version()")
    return f"connected ({row['version'][:30]}...)"


def test_mlb_api():
    games = mlb_api.get_schedule()
    return f"{len(games)} games today"


def test_odds_api():
    if not os.environ.get("ODDS_API_KEY"):
        return "skipped (ODDS_API_KEY not set)"
    records = odds.fetch_current_odds()
    return f"{len(records)} games with odds"


def test_nws():
    # Cleveland coords — should always have NWS data
    from src.weather import _fetch_nws
    from datetime import datetime, timezone
    target = datetime.now(timezone.utc).replace(hour=23).isoformat()
    wx = _fetch_nws(41.4962, -81.6852, target)
    if not wx:
        raise RuntimeError("NWS returned empty payload")
    return f"got {wx.get('temp_f')}°F, {wx.get('wind_raw')}"


def test_open_meteo():
    from src.weather import _fetch_openmeteo
    target = f"{date.today().isoformat()}T19:00:00+00:00"
    wx = _fetch_openmeteo(19.4326, -99.1332, target)  # Mexico City
    if not wx:
        raise RuntimeError("Open-Meteo returned empty payload")
    return f"got {wx.get('temp_f')}°F"


def main():
    print("Running smoke tests...\n")
    results = []
    results.append(check("Postgres", test_postgres))
    results.append(check("MLB Stats API", test_mlb_api))
    results.append(check("Odds API", test_odds_api))
    results.append(check("NWS", test_nws))
    results.append(check("Open-Meteo", test_open_meteo))

    n_pass = sum(results)
    print(f"\n{n_pass}/{len(results)} passed")
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
