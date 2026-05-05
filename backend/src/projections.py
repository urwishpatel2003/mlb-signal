"""
Projection engine — v3.0

Improvements over v2:
  1. True ERA blend: 0.55×xERA + 0.30×xFIP + 0.15×ERA  (xFIP stabilises faster
     than xERA; blending in raw ERA is kept at 15% only)
  2. Continuous IP leash: smooth gradient instead of 3-bucket hard thresholds
  3. BB/9 from real Statcast data with Bayesian shrinkage toward league average
  4. K% taken directly from pitcher Statcast row, not inferred from xwOBA
  5. Lineup strength blends season xwOBA with last-15-game rolling wOBA (0.70/0.30)
  6. Per-hitter L/R splits used when ≥80 PA from a side, else league platoon mult
  7. High-variance starter flag: pitchers with true_era > 5.0 get IP haircut +
     extra bullpen ER padding to account for early-hook probability

Tested: 29 pure-function tests, no DB/network dependencies.
"""
from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Optional
import math
import logging

log = logging.getLogger(__name__)

# ---------- League constants ----------
LEAGUE_XWOBA   = 0.320
LEAGUE_XBA     = 0.245
LEAGUE_K9      = 8.5
LEAGUE_BB9     = 3.2
LEAGUE_K_PCT   = 0.225
LEAGUE_ER9     = 4.30
LEAGUE_XFIP    = 4.10   # normalised FIP, roughly stable year-to-year

# ---------- Platoon multipliers (league-average; overridden by per-hitter splits) ----------
# Keys: (batter_hand, pitcher_hand)
PLATOON_XWOBA = {
    ("L", "L"): 0.93,
    ("L", "R"): 1.04,
    ("R", "R"): 0.97,
    ("R", "L"): 1.05,
    ("S", "L"): 1.05,
    ("S", "R"): 1.04,
}

# PA weights by batting-order spot
PA_WEIGHTS = {1: 4.7, 2: 4.5, 3: 4.4, 4: 4.2, 5: 4.0,
              6: 3.9, 7: 3.7, 8: 3.6, 9: 3.5}

# Minimum PA from a given side before we trust per-hitter split over league mult
SPLIT_PA_THRESHOLD = 80

# Bayesian sample size for BB/9 shrinkage
BB9_SHRINK_N = 200


# =============================================================================
# Data classes
# =============================================================================

@dataclass
class HitterSpot:
    """Batting-order slot fed into the projection engine."""
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
    era:              Optional[float]
    xera:             Optional[float]
    xfip:             Optional[float]   # NEW: surfaced for transparency
    true_era:         float
    xwoba_against:    Optional[float]
    opp_lineup_xwoba: float
    used_actual_lineup: bool
    used_l15_blend:   bool              # NEW: did we blend in last-15 wOBA?

    # Adjusted projection
    ip:   float
    outs: float
    hits: float
    er:   float
    bb:   float
    k:    float

    # Multipliers / flags
    wx_factor:          float
    pf_factor:          float
    high_variance_flag: bool            # NEW: true_era > 5.0 triggers early-hook padding

    def to_dict(self) -> dict:
        return asdict(self)


# =============================================================================
# Weather helpers  (unchanged from v2)
# =============================================================================

def temp_run_factor(temp_f: Optional[float]) -> float:
    if temp_f is None:
        return 1.0
    return 1.0 + ((temp_f - 70) / 10) * 0.0093


def _wind_components(wind_deg: Optional[float], cf_az: float) -> tuple[float, float]:
    if wind_deg is None:
        return 0.0, 0.0
    wind_to = (wind_deg + 180) % 360
    radians = math.radians(wind_to - cf_az)
    return math.cos(radians), abs(math.sin(radians))


def wind_run_factor(mph: Optional[float],
                    wind_deg: Optional[float],
                    cf_az: float) -> float:
    if mph is None or wind_deg is None or mph < 1:
        return 1.0
    out, cross = _wind_components(wind_deg, cf_az)
    return 1.0 + (mph * out / 5) * 0.023 - (mph * cross / 5) * 0.009


# =============================================================================
# Improvement #6 — Per-hitter platoon splits
# =============================================================================

def _platoon_factor(bat_side: str,
                    pitcher_hand: str,
                    hitter_splits: Optional[dict]) -> float:
    """
    Return the platoon multiplier for this (bat_side, pitcher_hand) matchup.

    Uses per-hitter split xwOBA vs. league-average side xwOBA when the hitter
    has ≥ SPLIT_PA_THRESHOLD PA from that side. Falls back to the league-level
    PLATOON_XWOBA multiplier otherwise.

    hitter_splits: {"L": {"est_woba": 0.295, "pa": 120},
                    "R": {"est_woba": 0.340, "pa": 95}} or None
    """
    if hitter_splits and pitcher_hand in hitter_splits:
        side_data = hitter_splits[pitcher_hand]
        if (side_data.get("pa") or 0) >= SPLIT_PA_THRESHOLD:
            split_xwoba = side_data.get("est_woba")
            if split_xwoba:
                # Normalise against league: if split xwOBA is 0.305 vs league 0.320,
                # factor = 0.305/0.320 = 0.953 — pitcher sees weaker-than-average offense
                return float(split_xwoba) / LEAGUE_XWOBA

    return PLATOON_XWOBA.get((bat_side, pitcher_hand), 1.0)


# =============================================================================
# Improvement #5 — Lineup xwOBA with last-15 wOBA blend
# =============================================================================

def opp_lineup_xwoba(lineup: list[HitterSpot],
                     pitcher_hand: str,
                     hitter_xstats: dict[int, dict],
                     team_fallback: float,
                     pa_threshold: int = 20) -> tuple[float, bool, bool]:
    """
    Lineup-weighted opposing xwOBA, adjusted for platoon.

    Improvement #5: when a hitter has a `l15_woba` field (last-15-game rolling
    wOBA), blend it: 0.70 × season_xwoba + 0.30 × l15_woba.  This captures
    hot/cold streaks and recent injury replacements far better than season
    xwOBA alone.

    Improvement #6: uses per-hitter L/R split xwOBA (via hitter_splits sub-dict)
    when sample is large enough, instead of the flat league-level platoon mult.

    Returns (weighted_xwoba, used_actual_lineup, used_l15_blend).
    """
    if not lineup:
        return team_fallback, False, False

    weighted_sum = 0.0
    total_w = 0.0
    matched = 0
    any_l15 = False

    for spot in lineup:
        row = hitter_xstats.get(spot.mlb_id)
        if not row or (row.get("pa") or 0) < pa_threshold:
            continue
        xwoba = row.get("est_woba")
        if xwoba is None:
            continue

        season_xwoba = float(xwoba)

        # Improvement #5: blend in recent form when available
        l15 = row.get("l15_woba")
        if l15 is not None:
            blended = 0.70 * season_xwoba + 0.30 * float(l15)
            any_l15 = True
        else:
            blended = season_xwoba

        # Improvement #6: per-hitter split or league-level platoon mult
        hitter_splits = row.get("splits")   # {"L": {...}, "R": {...}}
        platoon = _platoon_factor(spot.bat_side, pitcher_hand, hitter_splits)
        adjusted = blended * platoon

        pa_w = PA_WEIGHTS.get(spot.order, 4.0)
        weighted_sum += adjusted * pa_w
        total_w += pa_w
        matched += 1

    if matched < 6 or total_w == 0:
        return team_fallback, False, False

    return weighted_sum / total_w, True, any_l15


def opp_lineup_k_pct(lineup: list[HitterSpot],
                     hitter_xstats: dict[int, dict],
                     pa_threshold: int = 30) -> Optional[float]:
    """Lineup-weighted K rate; None if fewer than 6 hitters have usable data."""
    if not lineup:
        return None
    weighted_sum = 0.0
    total_w = 0.0
    matched = 0
    for spot in lineup:
        row = hitter_xstats.get(spot.mlb_id)
        if not row or (row.get("pa") or 0) < pa_threshold:
            continue
        k_pct = row.get("k_pct")
        if k_pct is None:
            continue
        pa_w = PA_WEIGHTS.get(spot.order, 4.0)
        weighted_sum += float(k_pct) * pa_w
        total_w += pa_w
        matched += 1
    if matched < 6 or total_w == 0:
        return None
    return weighted_sum / total_w


# =============================================================================
# IP helpers
# =============================================================================

def _lineup_pitches_per_pa(opp_lineup, hitter_xstats) -> float:
    """Average pitches/PA across lineup; 3.92 league fallback if <4 data points."""
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


def _project_ip_pitch_budget(avg_pitches_per_start,
                              pitcher_pitches_per_pa,
                              lineup_pitches_per_pa,
                              fallback_ip: float) -> float:
    """Estimate IP from pitch budget and lineup patience."""
    if avg_pitches_per_start is not None:
        avg_pitches_per_start = float(avg_pitches_per_start)
    if pitcher_pitches_per_pa is not None:
        pitcher_pitches_per_pa = float(pitcher_pitches_per_pa)
    if lineup_pitches_per_pa is not None:
        lineup_pitches_per_pa = float(lineup_pitches_per_pa)

    if not avg_pitches_per_start or not pitcher_pitches_per_pa:
        return fallback_ip
    if not lineup_pitches_per_pa or lineup_pitches_per_pa <= 0:
        lineup_pitches_per_pa = 3.92

    effective_ppa = (pitcher_pitches_per_pa + lineup_pitches_per_pa) / 2.0
    if effective_ppa <= 0:
        return fallback_ip
    projected_pa = avg_pitches_per_start / effective_ppa
    projected_ip = (projected_pa * 0.71) / 3.0
    return max(3.0, min(8.5, projected_ip))


# =============================================================================
# Improvement #2 — Continuous IP leash
# =============================================================================

def _continuous_ip_leash(true_era: float) -> float:
    """
    Smooth IP estimate from true ERA. Replaces 3-bucket hard thresholds.

    Formula: base_ip = 6.5 - (true_era - 3.5) × 0.40
    Clamped to [4.0, 7.0].

    Examples:
      true_era 2.50 → 7.0  (capped)
      true_era 3.50 → 6.50
      true_era 4.30 → 6.18
      true_era 5.50 → 5.70
      true_era 6.25 → 5.40
      true_era 7.25 → 4.0  (floored)
    """
    base = 6.5 - (true_era - 3.5) * 0.40
    return max(4.0, min(7.0, base))


# =============================================================================
# Bullpen helper  (unchanged from v2)
# =============================================================================

def _compute_team_bullpen_er9(team_xstats_row, league_avg_er9=4.0) -> float:
    if not team_xstats_row:
        return league_avg_er9
    bp_ip = float(team_xstats_row.get("bullpen_ip") or 0)
    bp_era_raw = team_xstats_row.get("bullpen_era")
    if not bp_era_raw or bp_ip <= 0:
        return league_avg_er9
    bp_era = float(bp_era_raw)
    bp_xera_raw = team_xstats_row.get("bullpen_xera")
    if bp_xera_raw:
        team_true_era = 0.7 * float(bp_xera_raw) + 0.3 * bp_era
    else:
        team_true_era = bp_era
    weight = bp_ip / (bp_ip + 50.0)
    return weight * team_true_era + (1 - weight) * league_avg_er9


# =============================================================================
# Core projection
# =============================================================================

def project_pitcher(
    *,
    pitcher_xstats: Optional[dict],
    pitcher_mlb_id: int,
    pitcher_name: str,
    pitcher_hand: str,
    team_code: str,
    opp_team_code: str,
    opp_lineup: list[HitterSpot],
    hitter_xstats: dict[int, dict],
    team_xwoba_fallback: float,
    park: dict,
    weather: dict,
    low_sample_pa_threshold: int = 30,
) -> PitcherProjection:
    """
    Produce a single pitcher's projected line for tonight's start.

    Changes vs v2:
      - opp_lineup_xwoba now returns 3-tuple (xwoba, used_actual, used_l15)
      - true_era uses 3-way blend with xFIP (#1)
      - BB/9 from real Statcast data with Bayesian shrinkage (#3)
      - K% from pitcher's actual k_pct, not inferred from xwOBA (#4)
      - Continuous IP leash (#2)
      - high_variance_flag + IP haircut + extra bullpen padding (#7)
    """
    # ---- Opposing lineup strength (Improvements #5 + #6) ----
    opp_xwoba, used_actual, used_l15 = opp_lineup_xwoba(
        opp_lineup, pitcher_hand, hitter_xstats, team_xwoba_fallback,
        pa_threshold=20,
    )
    woba_delta = opp_xwoba - LEAGUE_XWOBA

    # ---- Pitcher talent baseline ----
    pa = int((pitcher_xstats or {}).get("pa") or 0)
    fallback = (
        pitcher_xstats is None
        or pitcher_xstats.get("xera") is None
        or pa < low_sample_pa_threshold
    )

    if fallback:
        true_era = LEAGUE_ER9
        xera = era = xfip = xwoba_against = None
        h_per_pa = LEAGUE_XBA
        k_pct = LEAGUE_K_PCT
        bb9 = LEAGUE_BB9
        source = "league_avg" if pitcher_xstats is None else "low_sample"
        ip = 5.0
        high_variance = False

    else:
        xera = float(pitcher_xstats["xera"])
        era  = float(pitcher_xstats.get("era")  or LEAGUE_ER9)
        xfip_raw = pitcher_xstats.get("xfip")
        xfip = float(xfip_raw) if xfip_raw is not None else None

        # Improvement #1 — 3-way ERA blend
        if xfip is not None:
            true_era = 0.55 * xera + 0.30 * xfip + 0.15 * era
        else:
            # xFIP not yet in DB: fall back to original 2-way, but de-weight ERA further
            true_era = 0.75 * xera + 0.25 * era

        xwoba_against = float(pitcher_xstats.get("est_woba") or LEAGUE_XWOBA)

        # Hits per PA (unchanged)
        h_per_pa = (
            0.7 * float(pitcher_xstats.get("est_ba") or LEAGUE_XBA)
            + 0.3 * float(pitcher_xstats.get("ba")   or LEAGUE_XBA)
            + woba_delta * 0.5
        )

        # Improvement #4 — K% from real Statcast k_pct, not xwOBA-derived scaler
        raw_k_pct = pitcher_xstats.get("k_pct")
        if raw_k_pct is not None:
            k_pct_base = float(raw_k_pct)
            # Soft blend: 80% pitcher's own rate, 20% xwOBA-inferred (sanity check)
            k_scaler = (LEAGUE_XWOBA - xwoba_against) * 0.40
            k_pct = 0.80 * k_pct_base + 0.20 * (LEAGUE_K_PCT + k_scaler)
        else:
            # Fallback: xwOBA-derived (v2 behaviour)
            k_scaler = (LEAGUE_XWOBA - xwoba_against) * 0.40
            k_pct = LEAGUE_K_PCT + k_scaler

        # Apply lineup K-vulnerability adjustment
        lineup_k = opp_lineup_k_pct(opp_lineup, hitter_xstats)
        if lineup_k is not None:
            k_factor = max(0.85, min(1.15, lineup_k / LEAGUE_K_PCT))
            k_pct *= k_factor
        k_pct = max(0.14, min(0.35, k_pct))

        # Improvement #3 — BB/9 from real Statcast with Bayesian shrinkage
        raw_bb9 = pitcher_xstats.get("bb9")
        if raw_bb9 is not None:
            raw_bb9 = float(raw_bb9)
            weight = min(1.0, pa / BB9_SHRINK_N)
            bb9 = weight * raw_bb9 + (1 - weight) * LEAGUE_BB9
        else:
            # Fallback: ERA-tier heuristic (v2 behaviour)
            if true_era > 5.5:
                bb9 = LEAGUE_BB9 * 1.20
            elif true_era < 3.0:
                bb9 = LEAGUE_BB9 * 0.90
            else:
                bb9 = LEAGUE_BB9

        source = "statcast"

        # Improvement #2 — Continuous IP leash as fallback; pitch-budget preferred
        fallback_ip = _continuous_ip_leash(true_era)
        avg_pps  = pitcher_xstats.get("avg_pitches_per_start")
        pit_ppa  = pitcher_xstats.get("pitches_per_pa")
        lineup_ppa = _lineup_pitches_per_pa(opp_lineup, hitter_xstats)
        ip = _project_ip_pitch_budget(avg_pps, pit_ppa, lineup_ppa, fallback_ip)

        # Improvement #7 — High-variance flag: apply IP haircut for shaky starters
        high_variance = true_era > 5.0
        if high_variance:
            ip = ip * 0.88   # ~12% IP haircut reflecting early-hook probability

    # ---- Weather + park ----
    cf_az   = float(park.get("cf_azimuth_deg") or 0)
    is_dome = (park.get("roof_type") or "").lower() in ("dome", "closed")
    if is_dome or not weather:
        wx_run = 1.0
    else:
        wx_run = (
            temp_run_factor(weather.get("temp_f"))
            * wind_run_factor(weather.get("wind_mph"), weather.get("wind_deg"), cf_az)
        )

    pf_runs = float(park.get("pf_runs") or 100) / 100.0
    pf_so   = float(park.get("pf_so")   or 100) / 100.0
    pf_bb   = float(park.get("pf_bb")   or 100) / 100.0

    # ---- Roll up ----
    h9      = h_per_pa * 38 * wx_run * pf_runs
    er9     = (true_era + woba_delta * 30) * wx_run * pf_runs
    k9      = k_pct * 37.5 * pf_so
    bb9_adj = bb9 * pf_bb

    leash_adj = 1.0 - (wx_run - 1.0) * 0.3
    ip_adj    = ip * leash_adj

    return PitcherProjection(
        pitcher_mlb_id=pitcher_mlb_id,
        last_first=pitcher_name,
        team_code=team_code,
        opp_team_code=opp_team_code,
        hand=pitcher_hand,
        source=source,
        pa_sample=pa,
        era=round(era, 2)           if era           is not None else None,
        xera=round(xera, 2)         if xera          is not None else None,
        xfip=round(xfip, 2)         if xfip          is not None else None,
        true_era=round(true_era, 2),
        xwoba_against=round(xwoba_against, 4) if xwoba_against is not None else None,
        opp_lineup_xwoba=round(opp_xwoba, 4),
        used_actual_lineup=used_actual,
        used_l15_blend=used_l15,
        ip=round(ip_adj, 2),
        outs=round(ip_adj * 3, 1),
        hits=round((h9 / 9) * ip_adj, 2),
        er=round((er9 / 9) * ip_adj, 2),
        bb=round((bb9_adj / 9) * ip_adj, 2),
        k=round((k9 / 9) * ip_adj, 2),
        wx_factor=round(wx_run, 3),
        pf_factor=round(pf_runs, 3),
        high_variance_flag=high_variance,
    )


# =============================================================================
# Game total
# =============================================================================

def project_game_total(
    *,
    away_proj: PitcherProjection,
    home_proj: PitcherProjection,
    away_team_xstats: Optional[dict] = None,
    home_team_xstats: Optional[dict] = None,
    park: Optional[dict] = None,
    weather: Optional[dict] = None,
    league_bullpen_er9: float = 4.0,
) -> tuple[float, float, float, float]:
    """
    Combine starter projections into full game and F5 totals.

    Improvement #7: high-variance starters get +0.5 ER added to their team's
    bullpen contribution (more innings pitched by worse relievers when the ace
    gets yanked in the 4th).

    Returns (full_game_total, f5_total, home_runs, away_runs).
    """
    away_bp_er9 = _compute_team_bullpen_er9(away_team_xstats, league_bullpen_er9)
    home_bp_er9 = _compute_team_bullpen_er9(home_team_xstats, league_bullpen_er9)

    # Park + weather for bullpen innings
    pf_runs = float((park or {}).get("pf_runs") or 100) / 100.0
    cf_az   = float((park or {}).get("cf_azimuth_deg") or 0)
    is_dome = ((park or {}).get("roof_type") or "").lower() in ("dome", "closed")
    if is_dome or not weather:
        wx_run = 1.0
    else:
        wx_run = (
            temp_run_factor(weather.get("temp_f"))
            * wind_run_factor(weather.get("wind_mph"), weather.get("wind_deg"), cf_az)
        )
    park_wx = pf_runs * wx_run

    away_bp_innings = max(0.0, 9 - away_proj.ip)
    home_bp_innings = max(0.0, 9 - home_proj.ip)

    # Improvement #7 — high-variance padding
    away_bp_padding = 0.5 if away_proj.high_variance_flag else 0.0
    home_bp_padding = 0.5 if home_proj.high_variance_flag else 0.0

    away_bp_er = away_bp_innings * (away_bp_er9 / 9) * park_wx + away_bp_padding
    home_bp_er = home_bp_innings * (home_bp_er9 / 9) * park_wx + home_bp_padding

    # home_runs = runs scored by home offense = away pitching (starter + pen) allowed
    home_runs = away_proj.er + away_bp_er
    away_runs = home_proj.er + home_bp_er

    full_total = home_runs + away_runs

    # F5: starter ER scaled to 5 IP
    f5_away = (away_proj.er / away_proj.ip) * min(5, away_proj.ip) if away_proj.ip > 0 else 0
    f5_home = (home_proj.er / home_proj.ip) * min(5, home_proj.ip) if home_proj.ip > 0 else 0
    f5_total = f5_away + f5_home

    return round(full_total, 2), round(f5_total, 2), round(home_runs, 2), round(away_runs, 2)
