"""
Weather enrichment via NWS (National Weather Service) API.

NWS is free and requires no auth, just a User-Agent header. Two-step lookup:
  1. /points/{lat,lon} → returns the URL of the gridpoint forecast
  2. /gridpoints/...   → hourly forecast

We pick the period closest to first pitch (gameDate from MLB Stats API is UTC).

For non-US parks (Mexico City series, future Tokyo series), we fall back to
Open-Meteo which works globally without auth.

The orchestrator passes us a Game object (with venue lat/lon and game_date)
and we return enriched weather:
  - temp_f at first pitch
  - wind_mph
  - wind_deg (FROM bearing in compass degrees)
  - condition (short text)
  - precip_pct

Domes return empty dict (no enrichment needed, the projection engine treats
empty weather as neutral 1.0 multiplier).
"""
from __future__ import annotations
import logging
import time
from datetime import datetime, timezone
from typing import Optional
import requests

log = logging.getLogger(__name__)

USER_AGENT = "mlb-signal/0.1 (mlb-models@quaint-signal.tech)"
NWS_HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/geo+json"}

# Compass cardinal → degrees (FROM convention)
DIR_TO_DEG = {
    "N": 0, "NNE": 22.5, "NE": 45, "ENE": 67.5, "E": 90, "ESE": 112.5,
    "SE": 135, "SSE": 157.5, "S": 180, "SSW": 202.5, "SW": 225, "WSW": 247.5,
    "W": 270, "WNW": 292.5, "NW": 315, "NNW": 337.5,
}


def _is_us_lat_lon(lat: Optional[float], lon: Optional[float]) -> bool:
    if lat is None or lon is None:
        return False
    # Continental US + Alaska + Hawaii rough bbox; everything else uses Open-Meteo
    return (
        (24 <= lat <= 50 and -125 <= lon <= -66)   # CONUS
        or (51 <= lat <= 72 and -180 <= lon <= -130)  # AK
        or (18 <= lat <= 23 and -161 <= lon <= -154)  # HI
    )


def _fetch_nws(lat: float, lon: float, target_iso: str) -> dict:
    """Two-step NWS lookup. Returns parsed weather dict or {} on failure."""
    points_url = f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}"
    try:
        r = requests.get(points_url, headers=NWS_HEADERS, timeout=10)
        if r.status_code != 200:
            log.warning("NWS points %s returned %s", points_url, r.status_code)
            return {}
        forecast_url = r.json().get("properties", {}).get("forecastHourly")
        if not forecast_url:
            return {}
        time.sleep(0.3)   # NWS politeness
        r2 = requests.get(forecast_url, headers=NWS_HEADERS, timeout=10)
        if r2.status_code != 200:
            return {}
        periods = r2.json().get("properties", {}).get("periods", [])
    except (requests.RequestException, ValueError) as e:
        log.warning("NWS request failed: %s", e)
        return {}

    target = datetime.fromisoformat(target_iso.replace("Z", "+00:00"))
    if not periods:
        return {}
    best = min(
        periods,
        key=lambda p: abs(
            datetime.fromisoformat(p["startTime"]).astimezone(timezone.utc) - target
        ),
    )
    return _parse_nws_period(best)


def _parse_nws_period(p: dict) -> dict:
    ws = (p.get("windSpeed") or "").replace("mph", "").strip()
    nums = [float(x) for x in ws.split() if x.replace(".", "").isdigit()]
    wind_mph = max(nums) if nums else None
    wind_dir = (p.get("windDirection") or "").upper()
    wind_deg = DIR_TO_DEG.get(wind_dir)
    pop = (p.get("probabilityOfPrecipitation") or {}).get("value", 0) or 0
    return {
        "condition": p.get("shortForecast"),
        "temp_f": p.get("temperature"),
        "wind_mph": wind_mph,
        "wind_dir": wind_dir,
        "wind_deg": wind_deg,
        "wind_raw": p.get("windSpeed"),
        "precip_pct": int(pop),
    }


def _fetch_openmeteo(lat: float, lon: float, target_iso: str) -> dict:
    """Fallback for non-US parks. Open-Meteo is free, no auth, global."""
    try:
        target = datetime.fromisoformat(target_iso.replace("Z", "+00:00"))
        date_str = target.strftime("%Y-%m-%d")
        url = (
            f"https://api.open-meteo.com/v1/forecast?"
            f"latitude={lat}&longitude={lon}&"
            f"hourly=temperature_2m,wind_speed_10m,wind_direction_10m,"
            f"precipitation_probability,weather_code&"
            f"temperature_unit=fahrenheit&wind_speed_unit=mph&"
            f"timezone=UTC&start_date={date_str}&end_date={date_str}"
        )
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            return {}
        data = r.json().get("hourly", {})
        times = data.get("time", [])
        if not times:
            return {}
        # Pick the period closest to target
        target_naive = target.replace(tzinfo=None)
        idx = min(
            range(len(times)),
            key=lambda i: abs(datetime.fromisoformat(times[i]) - target_naive),
        )
        return {
            "condition": "",
            "temp_f": int(data["temperature_2m"][idx]) if data.get("temperature_2m") else None,
            "wind_mph": int(data["wind_speed_10m"][idx]) if data.get("wind_speed_10m") else None,
            "wind_dir": "",
            "wind_deg": int(data["wind_direction_10m"][idx]) if data.get("wind_direction_10m") else None,
            "wind_raw": None,
            "precip_pct": int(data["precipitation_probability"][idx])
                if data.get("precipitation_probability") else 0,
        }
    except (requests.RequestException, ValueError, KeyError, IndexError) as e:
        log.warning("Open-Meteo failed: %s", e)
        return {}


def enrich_weather_for_game(game) -> dict:
    """
    Public entry point. Pass a Game (with .venue and .game_date_et + game_time_et).
    Returns weather dict, or {} for domes / failure.
    """
    venue = getattr(game, "venue", None)
    if not venue:
        return {}
    if (venue.roof_type or "").lower() in ("dome", "closed"):
        return {}

    lat = venue.lat
    lon = venue.lon
    if not lat or not lon:
        return {}

    # Build target time. Game time is in ET; convert to UTC.
    # We'll trust the gameDate string from MLB API which is already UTC ISO.
    # Fallback: assume 7 PM ET = 23:00 UTC (EDT) on the game date.
    try:
        from datetime import datetime as _dt, timezone as _tz, timedelta
        target_iso = f"{game.game_date_et}T{game.game_time_et}:00-04:00"
        target = _dt.fromisoformat(target_iso).astimezone(_tz.utc).isoformat()
    except (ValueError, AttributeError):
        target = f"{game.game_date_et}T23:00:00+00:00"

    if _is_us_lat_lon(lat, lon):
        wx = _fetch_nws(lat, lon, target)
        if wx:
            return wx
    # Fallback to Open-Meteo
    return _fetch_openmeteo(lat, lon, target)
