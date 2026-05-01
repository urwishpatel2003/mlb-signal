"""
Projection engine.

Given:
  - A pitcher (mlb_id) with their xStats row
  - The opposing lineup (list of HitterSpot from MLB Stats API)
  - Hitter xStats lookup (for lineup-weighted xwOBA)
  - Pitcher hand (L/R)
  - Park factors and weather adjustments

Produces:
  - A PitcherProjection: ip, outs, hits, er, bb, k + diagnostic fields

Key upgrade vs prior runs: this uses **actual confirmed lineup xwOBA** weighted
by batting-order PA (top of order ~5 PA, bottom ~3.5 PA), adjusted by individual
hitter handedness vs the pitcher's hand. Falls back to team xwOBA only when
lineups aren't yet posted.

The platoon adjustment uses league-level multipliers because per-hitter L/R splits
are noisy on small samples. We can swap in player-specific splits later (a
hitter_splits table is already defined in the schema for that).

Tested against backtest results from Apr 25-27 to ensure the numbers match
the standalone scripts that won 12-2 on game totals.
"""
from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Optional
import math
import logging

log = logging.getLogger(__name__)

LEAGUE_XWOBA = 0.320
LEAGUE_XBA = 0.245
LEAGUE_K9 = 8.5
LEAGUE_BB9 = 3.2
LEAGUE_ER9 = 4.30

# Platoon multipliers on opp xwOBA (league-average; per-hitter splits will refine).
# Keys: (batter_hand, pitcher_hand)
PLATOON_XWOBA = {
    ("L", "L"): 0.93,
    ("L", "R"): 1.04,
    ("R", "R"): 0.97,
    ("R", "L"): 1.05,
    ("S", "L"): 1.05,   # switch hitters bat from "favorable" side
    ("S", "R"): 1.04,
}

# PA weights by batting-order spot (1 = leadoff sees ~4.7 PA in 9-inning game)
PA_WEIGHTS = {1: 4.7, 2: 4.5, 3: 4.4, 4: 4.2, 5: 4.0,
              6: 3.9, 7: 3.7, 8: 3.6, 9: 3.5}


@dataclass
class HitterSpot:
    """Lightweight version, suitable for projection input."""
    mlb_id: int
    last_first: str
    bat_side: str       # "L" / "R" / "S"
    order: int


@dataclass
class PitcherProjection:
    pitcher_mlb_id: int
    last_first: str
    team_code: str
    opp_team_code: str
    hand: str
    source: str         # "statcast" | "low_sample" | "league_avg"
    pa_sample: int

    # Diagnostics
    era: Optional[float]
    xera: Optional[float]
    true_era: float
    xwoba_against: Optional[float]
    opp_lineup_xwoba: float
    used_actual_lineup: bool

    # Adjusted projection
    ip: float
    outs: float
    hits: float
    er: float
    bb: float
    k: float

    # Multipliers applied (for transparency / debugging)
    wx_factor: float
    pf_factor: float

    def to_dict(self) -> dict:
        return asdict(self)


def opp_lineup_xwoba(lineup: list[HitterSpot],
                     pitcher_hand: str,
                     hitter_xstats: dict[int, dict],
                     team_fallback: float,
                     pa_threshold: int = 30) -> tuple[float, bool]:
    """
    Lineup-weighted opposing xwOBA, adjusted for platoon.

    Returns (weighted_xwoba, used_actual_lineup).
    Falls back to `team_fallback` if fewer than 6 hitters in the lineup have
    Statcast xStats with PA >= threshold.
    """
    if not lineup:
        return team_fallback, False

    weighted_sum = 0.0
    total_w = 0.0
    matched = 0

    for spot in lineup:
        row = hitter_xstats.get(spot.mlb_id)
        if not row or (row.get("pa") or 0) < pa_threshold:
            continue
        xwoba = row.get("est_woba")
        if xwoba is None:
            continue

        # Apply platoon multiplier
        platoon = PLATOON_XWOBA.get(
            (spot.bat_side, pitcher_hand),
            1.0
        )
        adjusted_xwoba = float(xwoba) * platoon

        # Weight by typical PA for that order spot
        pa_w = PA_WEIGHTS.get(spot.order, 4.0)
        weighted_sum += adjusted_xwoba * pa_w
        total_w += pa_w
        matched += 1

    # Need at least 6 of 9 hitters with usable data to trust the lineup-weighted value
    if matched < 6 or total_w == 0:
        return team_fallback, False

    return weighted_sum / total_w, True


def _wind_components(wind_deg: Optional[float], cf_az: float) -> tuple[float, float]:
    """Returns (out_component, cross_component). Convention: wind_deg is FROM bearing."""
    if wind_deg is None:
        return 0.0, 0.0
    wind_to = (wind_deg + 180) % 360
    radians = math.radians(wind_to - cf_az)
    return math.cos(radians), abs(math.sin(radians))


def temp_run_factor(temp_f: Optional[float]) -> float:
    if temp_f is None:
        return 1.0
    # Empirical: ~0.93% per 10F vs 70F baseline
    return 1.0 + ((temp_f - 70) / 10) * 0.0093


def wind_run_factor(mph: Optional[float],
                     wind_deg: Optional[float],
                     cf_az: float) -> float:
    if mph is None or wind_deg is None or mph < 1:
        return 1.0
    out, cross = _wind_components(wind_deg, cf_az)
    return 1.0 + (mph * out / 5) * 0.023 - (mph * cross / 5) * 0.009


def _project_ip_pitch_budget(
    avg_pitches_per_start,
    pitcher_pitches_per_pa,
    lineup_pitches_per_pa,
    fallback_ip,
):
    """
    Estimate IP from pitcher's pitch budget and the opposing lineup's patience.

    Logic:
      - Effective pitches/PA = avg of pitcher's rate and lineup's rate
      - Projected PAs faced = pitcher's budget / effective rate
      - Out rate per PA ~= 0.71 (1 - league OBP)
      - IP = projected_outs / 3
      - Clamped to [3.0, 8.5] to avoid pathological projections
    """
    if not avg_pitches_per_start or not pitcher_pitches_per_pa:
        return fallback_ip
    if not lineup_pitches_per_pa or lineup_pitches_per_pa <= 0:
        lineup_pitches_per_pa = 3.92
    effective_pitches_per_pa = (pitcher_pitches_per_pa + lineup_pitches_per_pa) / 2.0
    if effective_pitches_per_pa <= 0:
        return fallback_ip
    projected_pa = avg_pitches_per_start / effective_pitches_per_pa
    out_rate_per_pa = 0.71
    projected_outs = projected_pa * out_rate_per_pa
    projected_ip = projected_outs / 3.0
    return max(3.0, min(8.5, projected_ip))


def _lineup_pitches_per_pa(opp_lineup, hitter_xstats):
    """Average pitches/PA across the confirmed lineup; 3.92 league fallback if <4 data points."""
    if not opp_lineup:
        return 3.92
    rates = []
    for spot in opp_lineup:
        h = hitter_xstats.get(spot.mlb_id)
        if h and h.get("pitches_per_pa"):
            try:
                rates.append(float(h["pitches_per_pa"]))
            except (TypeError, ValueError):
                pass
    if len(rates) < 4:
        return 3.92
    return sum(rates) / len(rates)


def project_pitcher(
    *,
    pitcher_xstats: Optional[dict],     # row from pitcher_xstats; None for rookies
    pitcher_mlb_id: int,
    pitcher_name: str,
    pitcher_hand: str,
    team_code: str,
    opp_team_code: str,
    opp_lineup: list[HitterSpot],
    hitter_xstats: dict[int, dict],
    team_xwoba_fallback: float,
    park: dict,                          # park record from `parks` table
    weather: dict,                       # {temp_f, wind_mph, wind_deg} or {} for dome
    low_sample_pa_threshold: int = 30,
) -> PitcherProjection:
    """
    Produce a single pitcher's projected line for tonight's start.

    Args:
        pitcher_xstats: full Statcast row, or None if rookie/no data
        pitcher_mlb_id: MLB person ID
        pitcher_name: "Crochet, Garrett" format
        pitcher_hand: "L" or "R"
        team_code: pitcher's team (e.g. "BOS")
        opp_team_code: opposing team (e.g. "BAL")
        opp_lineup: list of confirmed HitterSpot, or empty list
        hitter_xstats: {mlb_id: row} lookup of opposing hitters
        team_xwoba_fallback: opp team season xwOBA (used if lineup not posted)
        park: park record with {pf_runs, pf_so, pf_bb, cf_azimuth_deg, roof_type}
        weather: {temp_f, wind_mph, wind_deg} (empty dict for dome/missing)
    """
    # ---- Compute opposing offensive strength (lineup-weighted preferred) ----
    opp_xwoba, used_actual = opp_lineup_xwoba(
        opp_lineup, pitcher_hand, hitter_xstats, team_xwoba_fallback,
        pa_threshold=20,
    )
    woba_delta = opp_xwoba - LEAGUE_XWOBA  # +0.020 means strong opposing offense

    # ---- Determine the pitcher's true talent baseline ----
    pa = int((pitcher_xstats or {}).get("pa") or 0)
    if (
        pitcher_xstats is None
        or pitcher_xstats.get("xera") is None
        or pa < low_sample_pa_threshold
    ):
        # Fallback path - rookies, recently called up, etc.
        true_era = LEAGUE_ER9
        xera = era = xwoba_against = None
        h_per_pa = LEAGUE_XBA
        k_pct = 0.225
        bb9 = LEAGUE_BB9
        source = "league_avg" if pitcher_xstats is None else "low_sample"
        ip = 5.0
    else:
        xera = float(pitcher_xstats["xera"])
        era = float(pitcher_xstats.get("era") or LEAGUE_ER9)
        true_era = 0.7 * xera + 0.3 * era
        xwoba_against = float(pitcher_xstats.get("est_woba") or LEAGUE_XWOBA)
        # Hits per PA, blending xBA (skill) with BA (luck-affected) and tilting for opp lineup
        h_per_pa = (
            0.7 * float(pitcher_xstats.get("est_ba") or LEAGUE_XBA)
            + 0.3 * float(pitcher_xstats.get("ba") or LEAGUE_XBA)
            + woba_delta * 0.5
        )
        # K rate scaled off contact suppression
        k_scaler = (LEAGUE_XWOBA - xwoba_against) * 0.40
        k_pct = max(0.14, min(0.35, 0.225 + k_scaler))
        # Walk rate heuristic from quality (will refine when we have xBB rate)
        if true_era > 5.5:
            bb9 = LEAGUE_BB9 * 1.20
        elif true_era < 3.0:
            bb9 = LEAGUE_BB9 * 0.90
        else:
            bb9 = LEAGUE_BB9
        source = "statcast"
        # Leash by quality (fallback when pitch-budget data unavailable)
        if true_era > 5.5:
            fallback_ip = 4.5
        elif true_era < 3.0:
            fallback_ip = 6.0
        else:
            fallback_ip = 5.5
        avg_pps = pitcher_xstats.get("avg_pitches_per_start") if pitcher_xstats else None
        pit_ppa = pitcher_xstats.get("pitches_per_pa") if pitcher_xstats else None
        lineup_ppa = _lineup_pitches_per_pa(opp_lineup, hitter_xstats)
        ip = _project_ip_pitch_budget(avg_pps, pit_ppa, lineup_ppa, fallback_ip)

    # ---- Apply weather + park ----
    cf_az = float(park.get("cf_azimuth_deg") or 0)
    is_dome = (park.get("roof_type") or "").lower() in ("dome", "closed")
    if is_dome or not weather:
        wx_run = 1.0
    else:
        wx_run = (
            temp_run_factor(weather.get("temp_f"))
            * wind_run_factor(
                weather.get("wind_mph"),
                weather.get("wind_deg"),
                cf_az,
            )
        )

    pf_runs = float(park.get("pf_runs") or 100) / 100.0
    pf_so = float(park.get("pf_so") or 100) / 100.0
    pf_bb = float(park.get("pf_bb") or 100) / 100.0

    # ---- Roll up final projection ----
    h9 = h_per_pa * 38 * wx_run * pf_runs
    er9 = (true_era + woba_delta * 30) * wx_run * pf_runs
    k9 = k_pct * 37.5 * pf_so
    bb9_adj = bb9 * pf_bb

    # Slight leash dampener in extreme run environments
    leash_adj = 1.0 - (wx_run - 1.0) * 0.3
    ip_adj = ip * leash_adj

    return PitcherProjection(
        pitcher_mlb_id=pitcher_mlb_id,
        last_first=pitcher_name,
        team_code=team_code,
        opp_team_code=opp_team_code,
        hand=pitcher_hand,
        source=source,
        pa_sample=pa,
        era=round(era, 2) if era else None,
        xera=round(xera, 2) if xera else None,
        true_era=round(true_era, 2),
        xwoba_against=round(xwoba_against, 4) if xwoba_against else None,
        opp_lineup_xwoba=round(opp_xwoba, 4),
        used_actual_lineup=used_actual,
        ip=round(ip_adj, 2),
        outs=round(ip_adj * 3, 1),
        hits=round((h9 / 9) * ip_adj, 2),
        er=round((er9 / 9) * ip_adj, 2),
        bb=round((bb9_adj / 9) * ip_adj, 2),
        k=round((k9 / 9) * ip_adj, 2),
        wx_factor=round(wx_run, 3),
        pf_factor=round(pf_runs, 3),
    )


def project_game_total(
    *,
    away_proj: PitcherProjection,
    home_proj: PitcherProjection,
    bullpen_er9: float = 4.0,
) -> tuple[float, float]:
    """
    Combine starter projections into full game and F5 totals.

    Returns (full_game_total, f5_total).
    """
    starter_runs = away_proj.er + home_proj.er
    bullpen_innings_remaining = (9 - away_proj.ip) + (9 - home_proj.ip)
    bullpen_runs = bullpen_innings_remaining * (bullpen_er9 / 9)
    full_total = starter_runs + bullpen_runs

    # F5: just the starter contribution scaled to 5 IP each
    if away_proj.ip > 0:
        f5_away = (away_proj.er / away_proj.ip) * min(5, away_proj.ip)
    else:
        f5_away = 0
    if home_proj.ip > 0:
        f5_home = (home_proj.er / home_proj.ip) * min(5, home_proj.ip)
    else:
        f5_home = 0
    f5_total = f5_away + f5_home

    return round(full_total, 2), round(f5_total, 2)
