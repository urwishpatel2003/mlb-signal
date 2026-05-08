"""
Projection engine — v4.0

New vs v3.0:
  1. HFA (Home Field Advantage): +2.5pp added to home win probability post-Skellam
  2. Skip ML edges for fallback starters (low_sample / league_avg)
  3. 7-day bullpen ERA blended with season ERA in _compute_team_bullpen_er9
  4. Days rest adjustment: ±0.3 IP on leash based on days since last start
  5. Offensive strength scaler: team xwOBA adjusts run totals independently of pitcher projection
  6. xFIP computed from components (FB%, HR/FB rate) when available
  7. Calibration-ready: win prob functions accept an external HFA override
  8. Hitter props groundwork: project_hitter_props() stub ready for wiring

All prior improvements (v3.0) retained.
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
LEAGUE_XFIP    = 4.10
LEAGUE_HR_FB   = 0.118   # league-average HR/FB rate
LEAGUE_FB_PCT  = 0.355   # league-average fly ball rate

# Home field advantage — additive on win probability
# Calibrate from graded results once 50+ ML plays graded
HOME_FIELD_ADVANTAGE = 0.025

# Platoon multipliers
PLATOON_XWOBA = {
    ("L","L"): 0.93, ("L","R"): 1.04,
    ("R","R"): 0.97, ("R","L"): 1.05,
    ("S","L"): 1.05, ("S","R"): 1.04,
}

PA_WEIGHTS = {1:4.7,2:4.5,3:4.4,4:4.2,5:4.0,6:3.9,7:3.7,8:3.6,9:3.5}
SPLIT_PA_THRESHOLD = 80
BB9_SHRINK_N = 200

# Days rest IP adjustments
DAYS_REST_IP_ADJ = {
    "short":  -0.30,   # 3 days rest or fewer
    "normal":  0.00,   # 4-5 days
    "extra":   0.15,   # 6+ days (extra rest, usually sharp)
}


# =============================================================================
# Data classes
# =============================================================================

@dataclass
class HitterSpot:
    mlb_id: int
    last_first: str
    bat_side: str
    order: int


@dataclass
class PitcherProjection:
    pitcher_mlb_id: int
    last_first: str
    team_code: str
    opp_team_code: str
    hand: str
    source: str
    pa_sample: int

    era:              Optional[float]
    xera:             Optional[float]
    xfip:             Optional[float]
    true_era:         float
    xwoba_against:    Optional[float]
    opp_lineup_xwoba: float
    used_actual_lineup: bool
    used_l15_blend:   bool

    ip:   float
    outs: float
    hits: float
    er:   float
    bb:   float
    k:    float

    wx_factor:          float
    pf_factor:          float
    high_variance_flag: bool
    days_rest:          Optional[int]   # NEW: days since last start

    def to_dict(self) -> dict:
        return asdict(self)

    @property
    def is_reliable(self) -> bool:
        """True only for statcast-sourced projections. Used to gate ML edges."""
        return self.source == "statcast"


# =============================================================================
# Weather
# =============================================================================

def temp_run_factor(temp_f: Optional[float]) -> float:
    if temp_f is None: return 1.0
    return 1.0 + ((temp_f - 70) / 10) * 0.0093

def _wind_components(wind_deg, cf_az):
    if wind_deg is None: return 0.0, 0.0
    wind_to = (wind_deg + 180) % 360
    r = math.radians(wind_to - cf_az)
    return math.cos(r), abs(math.sin(r))

def wind_run_factor(mph, wind_deg, cf_az):
    if mph is None or wind_deg is None or mph < 1: return 1.0
    out, cross = _wind_components(wind_deg, cf_az)
    return 1.0 + (mph * out / 5) * 0.023 - (mph * cross / 5) * 0.009


# =============================================================================
# Improvement #6 — xFIP from components
# =============================================================================

def compute_xfip(k: float, bb: float, hbp: float, ip: float,
                 fb_pct: Optional[float], hr_fb: Optional[float],
                 lg_hr_fb: float = LEAGUE_HR_FB) -> Optional[float]:
    """
    xFIP = ((13 × (FB × lgHR/FB)) + (3 × (BB + HBP)) - (2 × K)) / IP + const

    Uses league-average HR/FB rate to normalise HR luck out of ERA.
    const (~3.10) centres xFIP around league ERA.
    Returns None if insufficient data.
    """
    if ip is None or ip <= 0:
        return None
    fb_rate = fb_pct if fb_pct is not None else LEAGUE_FB_PCT
    # Projected FB = IP × 3 PAs × FB% (rough approximation)
    # More precisely: FB = BIP × FB%, but we use IP-based estimate
    estimated_bip = max(0, ip * 4.0 - bb - k * 0.33)
    estimated_fb  = estimated_bip * fb_rate
    xfip_num = (13 * estimated_fb * lg_hr_fb) + (3 * (bb + hbp)) - (2 * k)
    xfip = xfip_num / ip + 3.10
    return round(max(1.5, min(8.0, xfip)), 2)


# =============================================================================
# Improvement #4 — Days rest IP adjustment
# =============================================================================

def _days_rest_ip_adj(days_rest: Optional[int]) -> float:
    """Return additive IP adjustment based on days since last start."""
    if days_rest is None:
        return 0.0
    if days_rest <= 3:
        return DAYS_REST_IP_ADJ["short"]
    if days_rest >= 6:
        return DAYS_REST_IP_ADJ["extra"]
    return DAYS_REST_IP_ADJ["normal"]


# =============================================================================
# Improvement #6 — Per-hitter platoon splits
# =============================================================================

def _platoon_factor(bat_side, pitcher_hand, hitter_splits):
    if hitter_splits and pitcher_hand in hitter_splits:
        sd = hitter_splits[pitcher_hand]
        if (sd.get("pa") or 0) >= SPLIT_PA_THRESHOLD:
            xw = sd.get("est_woba")
            if xw:
                return float(xw) / LEAGUE_XWOBA
    return PLATOON_XWOBA.get((bat_side, pitcher_hand), 1.0)


# =============================================================================
# Improvement #5 — Lineup xwOBA with L15 blend + offensive strength
# =============================================================================

def opp_lineup_xwoba(lineup, pitcher_hand, hitter_xstats, team_fallback,
                     pa_threshold=20):
    """Returns (weighted_xwoba, used_actual_lineup, used_l15_blend)."""
    if not lineup:
        return team_fallback, False, False
    ws = tw = 0.0; matched = 0; any_l15 = False
    for spot in lineup:
        row = hitter_xstats.get(spot.mlb_id)
        if not row or (row.get("pa") or 0) < pa_threshold: continue
        xwoba = row.get("est_woba")
        if xwoba is None: continue
        season_xwoba = float(xwoba)
        l15 = row.get("l15_woba")
        blended = 0.70 * season_xwoba + 0.30 * float(l15) if l15 is not None else season_xwoba
        if l15 is not None: any_l15 = True
        platoon = _platoon_factor(spot.bat_side, pitcher_hand, row.get("splits"))
        pa_w = PA_WEIGHTS.get(spot.order, 4.0)
        ws += blended * platoon * pa_w
        tw += pa_w
        matched += 1
    if matched < 6 or tw == 0:
        return team_fallback, False, False
    return ws / tw, True, any_l15


def opp_lineup_k_pct(lineup, hitter_xstats, pa_threshold=30):
    if not lineup: return None
    ws = tw = 0.0; matched = 0
    for spot in lineup:
        row = hitter_xstats.get(spot.mlb_id)
        if not row or (row.get("pa") or 0) < pa_threshold: continue
        k_pct = row.get("k_pct")
        if k_pct is None: continue
        pa_w = PA_WEIGHTS.get(spot.order, 4.0)
        ws += float(k_pct) * pa_w; tw += pa_w; matched += 1
    if matched < 6 or tw == 0: return None
    return ws / tw


# =============================================================================
# IP helpers
# =============================================================================

def _lineup_pitches_per_pa(opp_lineup, hitter_xstats):
    if not opp_lineup: return 3.92
    rates = []
    for spot in opp_lineup:
        h = hitter_xstats.get(spot.mlb_id)
        if h and h.get("pitches_per_pa"):
            try: rates.append(float(h["pitches_per_pa"]))
            except: pass
    return sum(rates)/len(rates) if len(rates) >= 4 else 3.92


def _project_ip_pitch_budget(avg_pps, pit_ppa, lineup_ppa, fallback_ip):
    if avg_pps is not None: avg_pps = float(avg_pps)
    if pit_ppa is not None: pit_ppa = float(pit_ppa)
    if lineup_ppa is not None: lineup_ppa = float(lineup_ppa)
    if not avg_pps or not pit_ppa: return fallback_ip
    eff = (pit_ppa + (lineup_ppa or 3.92)) / 2.0
    if eff <= 0: return fallback_ip
    return max(3.0, min(8.5, (avg_pps / eff * 0.71) / 3.0))


def _continuous_ip_leash(true_era):
    """Smooth gradient: elite ERA→7.0 IP, bad ERA→4.0 IP."""
    return max(4.0, min(7.0, 6.5 - (true_era - 3.5) * 0.40))


# =============================================================================
# Bullpen — Improvement #3: blend 7-day ERA
# =============================================================================

def _compute_team_bullpen_er9(team_xstats_row, league_avg_er9=4.0) -> float:
    """
    Improvement #3: blend season bullpen ERA with last-7-day ERA.
    7-day ERA gets 40% weight when ≥10 IP available (captures recent fatigue/form).
    Falls back to season ERA when L7 data is absent.
    """
    if not team_xstats_row:
        return league_avg_er9

    bp_ip = float(team_xstats_row.get("bullpen_ip") or 0)
    bp_era_raw = team_xstats_row.get("bullpen_era")
    if not bp_era_raw or bp_ip <= 0:
        return league_avg_er9

    bp_era = float(bp_era_raw)
    bp_xera_raw = team_xstats_row.get("bullpen_xera")
    season_true = 0.7 * float(bp_xera_raw) + 0.3 * bp_era if bp_xera_raw else bp_era

    # Bayesian shrink season toward league
    season_weight = bp_ip / (bp_ip + 50.0)
    season_er9 = season_weight * season_true + (1 - season_weight) * league_avg_er9

    # Improvement #3: blend in L7 when available
    l7_era = team_xstats_row.get("bullpen_era_l7")
    l7_ip  = float(team_xstats_row.get("bullpen_ip_l7") or 0)
    if l7_era is not None and l7_ip >= 10:
        l7_weight = min(0.40, l7_ip / 40.0)   # max 40% weight at 40+ IP in 7 days
        return round(l7_weight * float(l7_era) + (1 - l7_weight) * season_er9, 3)

    return round(season_er9, 3)


# =============================================================================
# Improvement #5 — Offensive strength scaler
# =============================================================================

def _offensive_strength_scaler(team_xstats_row) -> float:
    """
    Scale run projection based on team's offensive xwOBA vs league.
    Returns a multiplier: strong offense (xwOBA > .320) → >1.0, weak → <1.0.
    Capped at ±10% to avoid overriding the starter-centric projection.

    Uses team_xwoba from team_xstats (team's own hitting xwOBA, not pitcher's).
    """
    if not team_xstats_row:
        return 1.0
    team_xwoba = team_xstats_row.get("team_xwoba") or team_xstats_row.get("est_woba")
    if not team_xwoba:
        return 1.0
    delta = float(team_xwoba) - LEAGUE_XWOBA   # e.g. +0.015 for strong offense
    scaler = 1.0 + delta * 3.0                  # 0.015 delta → 4.5% boost
    return max(0.90, min(1.10, scaler))


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
    opp_lineup: list,
    hitter_xstats: dict,
    team_xwoba_fallback: float,
    park: dict,
    weather: dict,
    low_sample_pa_threshold: int = 30,
) -> PitcherProjection:
    opp_xwoba, used_actual, used_l15 = opp_lineup_xwoba(
        opp_lineup, pitcher_hand, hitter_xstats, team_xwoba_fallback, pa_threshold=20)
    # Blend pitcher's own suppression ability with lineup quality.
    # Reduces the raw lineup xwOBA effect for elite pitchers (low xwOBA-against)
    # and amplifies it for bad pitchers (high xwOBA-against).
    # Use pitcher xwOBA-against when available, else fall back to league average.
    pitcher_xwoba_against = float((pitcher_xstats or {}).get("est_woba") or LEAGUE_XWOBA)
    effective_opp_xwoba = 0.60 * pitcher_xwoba_against + 0.40 * opp_xwoba
    woba_delta = effective_opp_xwoba - LEAGUE_XWOBA

    pa = int((pitcher_xstats or {}).get("pa") or 0)
    days_rest = (pitcher_xstats or {}).get("days_rest")
    fallback = (pitcher_xstats is None or pitcher_xstats.get("xera") is None
                or pa < low_sample_pa_threshold)

    if fallback:
        true_era = LEAGUE_ER9
        xera = era = xfip = xwoba_against = None
        h_per_pa = LEAGUE_XBA; k_pct = LEAGUE_K_PCT; bb9 = LEAGUE_BB9
        source = "league_avg" if pitcher_xstats is None else "low_sample"
        ip = 5.0; high_variance = False
    else:
        xera = float(pitcher_xstats["xera"])
        era  = float(pitcher_xstats.get("era") or LEAGUE_ER9)

        # Improvement #6: compute xFIP from components if available
        xfip_stored = pitcher_xstats.get("xfip")
        if xfip_stored is not None:
            xfip = float(xfip_stored)
        else:
            # Compute from FB% and HR/FB rate stored in pitcher_xstats
            fb_pct   = pitcher_xstats.get("fb_pct")
            hr_fb    = pitcher_xstats.get("hr_fb_rate")
            ip_total = float(pitcher_xstats.get("ip_total") or 0)
            k_total  = float(pitcher_xstats.get("tbf") or 0) * float(pitcher_xstats.get("k_pct") or 0)
            bb_total = float(pitcher_xstats.get("tbf") or 0) * float(pitcher_xstats.get("bb_pct") or 0)
            xfip = compute_xfip(k_total, bb_total, 0, ip_total, fb_pct, hr_fb) if ip_total > 20 else None

        # Improvement #1 in ERA blend: use xFIP when computed
        if xfip is not None:
            true_era = 0.55 * xera + 0.30 * xfip + 0.15 * era
        else:
            true_era = 0.75 * xera + 0.25 * era

        xwoba_against = float(pitcher_xstats.get("est_woba") or LEAGUE_XWOBA)
        h_per_pa = (0.7 * float(pitcher_xstats.get("est_ba") or LEAGUE_XBA)
                    + 0.3 * float(pitcher_xstats.get("ba") or LEAGUE_XBA)
                    + woba_delta * 0.25)

        # K%: use whiff rate when available — better predictor than raw k_pct
        # Whiff rate is more stable (reflects pure stuff) vs k_pct (fluctuates with sequencing)
        raw_k_pct  = pitcher_xstats.get("k_pct")
        whiff_pct  = pitcher_xstats.get("whiff_pct")

        if whiff_pct is not None and raw_k_pct is not None:
            # Blend: whiff-based K estimate + actual k_pct
            # whiff → K conversion: roughly whiff_pct * 0.85 ≈ k_pct
            whiff_k_est = float(whiff_pct) * 0.85
            k_pct = 0.50 * float(raw_k_pct) + 0.50 * whiff_k_est
        elif whiff_pct is not None:
            k_pct = float(whiff_pct) * 0.85
        elif raw_k_pct is not None:
            k_scaler = (LEAGUE_XWOBA - xwoba_against) * 0.40
            k_pct = 0.80 * float(raw_k_pct) + 0.20 * (LEAGUE_K_PCT + k_scaler)
        else:
            k_pct = LEAGUE_K_PCT + (LEAGUE_XWOBA - xwoba_against) * 0.40

        # Adjust for opponent lineup K tendency
        lineup_k = opp_lineup_k_pct(opp_lineup, hitter_xstats)
        if lineup_k is not None:
            k_pct *= max(0.85, min(1.15, lineup_k / LEAGUE_K_PCT))
        k_pct = max(0.14, min(0.38, k_pct))

        # BB9: real data with Bayesian shrinkage
        raw_bb9 = pitcher_xstats.get("bb9")
        if raw_bb9 is not None:
            weight = min(1.0, pa / BB9_SHRINK_N)
            bb9 = weight * float(raw_bb9) + (1 - weight) * LEAGUE_BB9
        else:
            bb9 = LEAGUE_BB9 * (1.20 if true_era > 5.5 else 0.90 if true_era < 3.0 else 1.0)

        source = "statcast"
        high_variance = true_era > 5.0

        # IP: three-way blend — prior year baseline + season YTD + recent L5
        # Prior year tells us if pitcher is a long horse or short starter
        # Season avg captures current year workload
        # L5 captures recent trend (pitchers stretch out as season progresses)
        ip_total_val   = float(pitcher_xstats.get("ip_total") or 0)
        gs_val         = int(pitcher_xstats.get("gs") or 0)
        l5_avg_ip      = pitcher_xstats.get("l5_avg_ip")
        ip_total_prev  = pitcher_xstats.get("ip_total_prev")
        gs_prev        = int(pitcher_xstats.get("gs_prev") or 0)

        season_avg_ip = ip_total_val / gs_val if (ip_total_val > 0 and gs_val > 0) else None
        prior_avg_ip  = float(ip_total_prev) / gs_prev if (ip_total_prev and gs_prev >= 10) else None
        l5_ip         = float(l5_avg_ip) if l5_avg_ip is not None else None

        if season_avg_ip and prior_avg_ip and l5_ip:
            # Full three-way blend: prior 30% / season 30% / L5 40%
            ip = 0.30 * prior_avg_ip + 0.30 * season_avg_ip + 0.40 * l5_ip
        elif season_avg_ip and l5_ip:
            # No prior year — blend season and L5
            ip = 0.45 * season_avg_ip + 0.55 * l5_ip
        elif season_avg_ip and prior_avg_ip:
            # No L5 — blend season and prior year
            ip = 0.50 * prior_avg_ip + 0.50 * season_avg_ip
        elif season_avg_ip:
            ip = season_avg_ip
        else:
            ip = _continuous_ip_leash(true_era)

        # Days rest adjustment: ±0.3 IP
        ip += _days_rest_ip_adj(days_rest)

        # High-variance haircut: poor pitcher likely exits early
        if high_variance:
            ip *= 0.88

        ip = max(3.0, min(8.5, ip))

    # Weather + park
    cf_az   = float(park.get("cf_azimuth_deg") or 0)
    is_dome = (park.get("roof_type") or "").lower() in ("dome","closed")
    wx_run  = 1.0 if (is_dome or not weather) else (
        temp_run_factor(weather.get("temp_f"))
        * wind_run_factor(weather.get("wind_mph"), weather.get("wind_deg"), cf_az)
    )
    pf_runs = float(park.get("pf_runs") or 100) / 100.0
    pf_so   = float(park.get("pf_so")   or 100) / 100.0
    pf_bb   = float(park.get("pf_bb")   or 100) / 100.0

    # Pitcher type classification based on whiff rate
    # Power pitcher: high whiff → faces more batters per inning (deep counts)
    # Contact pitcher: low whiff → early contact, fewer BF per inning
    whiff_pct_val = float((pitcher_xstats or {}).get("whiff_pct") or 0.22)
    contact_pct_val = float((pitcher_xstats or {}).get("contact_pct") or 0.78)

    # BF per 9 innings: contact pitchers face fewer (early outs), power face more
    # League avg: ~38 BF/9. Contact (<20% whiff): ~35. Power (>28% whiff): ~40
    if whiff_pct_val >= 0.28:
        bf_per_9 = 40.0   # power pitcher — more pitches per out
    elif whiff_pct_val <= 0.20:
        bf_per_9 = 35.0   # contact pitcher — early contact, efficient
    else:
        bf_per_9 = 35.0 + (whiff_pct_val - 0.20) / 0.08 * 5.0  # linear interpolation

    # Hits: contact pitchers allow more hits per BF
    # High contact_pct → more balls in play → more hits
    contact_hits_adj = 1.0 + (contact_pct_val - 0.78) * 0.5  # ±adjustment around league avg
    h9      = h_per_pa * bf_per_9 * wx_run * pf_runs * contact_hits_adj
    er9     = (true_era + woba_delta * 7) * wx_run * pf_runs
    k9      = k_pct * bf_per_9 * pf_so
    bb9_adj = bb9 * pf_bb

    leash_adj = 1.0 - (wx_run - 1.0) * 0.3
    ip_adj    = ip * leash_adj

    return PitcherProjection(
        pitcher_mlb_id=pitcher_mlb_id, last_first=pitcher_name,
        team_code=team_code, opp_team_code=opp_team_code, hand=pitcher_hand,
        source=source, pa_sample=pa,
        era=round(era,2)  if era  is not None else None,
        xera=round(xera,2) if xera is not None else None,
        xfip=round(xfip,2) if xfip is not None else None,
        true_era=round(true_era,2),
        xwoba_against=round(xwoba_against,4) if xwoba_against is not None else None,
        opp_lineup_xwoba=round(opp_xwoba,4),
        used_actual_lineup=used_actual, used_l15_blend=used_l15,
        ip=round(ip_adj,2), outs=round(ip_adj*3,1),
        hits=round((h9/9)*ip_adj,2), er=round((er9/9)*ip_adj,2),
        bb=round((bb9_adj/9)*ip_adj,2), k=round((k9/9)*ip_adj,2),
        wx_factor=round(wx_run,3), pf_factor=round(pf_runs,3),
        high_variance_flag=high_variance,
        days_rest=int(days_rest) if days_rest is not None else None,
    )


# =============================================================================
# Game total — Improvement #5: offensive strength scaler
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
    Improvement #3: 7-day bullpen ERA blended in _compute_team_bullpen_er9.
    Improvement #5: offensive strength scaler on each team's run total.

    Returns (full_game_total, f5_total, home_runs, away_runs).
    """
    away_bp_er9 = _compute_team_bullpen_er9(away_team_xstats, league_bullpen_er9)
    home_bp_er9 = _compute_team_bullpen_er9(home_team_xstats, league_bullpen_er9)

    pf_runs = float((park or {}).get("pf_runs") or 100) / 100.0
    cf_az   = float((park or {}).get("cf_azimuth_deg") or 0)
    is_dome = ((park or {}).get("roof_type") or "").lower() in ("dome","closed")
    wx_run  = 1.0 if (is_dome or not weather) else (
        temp_run_factor(weather.get("temp_f"))
        * wind_run_factor(weather.get("wind_mph"), weather.get("wind_deg"), cf_az)
    )
    park_wx = pf_runs * wx_run

    away_bp_innings = max(0.0, 9 - away_proj.ip)
    home_bp_innings = max(0.0, 9 - home_proj.ip)

    # High-variance padding
    away_bp_pad = 0.5 if away_proj.high_variance_flag else 0.0
    home_bp_pad = 0.5 if home_proj.high_variance_flag else 0.0

    away_bp_er = away_bp_innings * (away_bp_er9 / 9) * park_wx + away_bp_pad
    home_bp_er = home_bp_innings * (home_bp_er9 / 9) * park_wx + home_bp_pad

    # Improvement #5: offensive scaler
    # home_runs = runs scored by home offense (away pitching allows)
    # Scale by HOME team's offensive strength
    home_off_scaler = _offensive_strength_scaler(home_team_xstats)
    away_off_scaler = _offensive_strength_scaler(away_team_xstats)

    home_runs = (away_proj.er + away_bp_er) * home_off_scaler
    away_runs = (home_proj.er + home_bp_er) * away_off_scaler

    full_total = home_runs + away_runs

    f5_away = (away_proj.er / away_proj.ip) * min(5, away_proj.ip) * away_off_scaler if away_proj.ip > 0 else 0
    f5_home = (home_proj.er / home_proj.ip) * min(5, home_proj.ip) * home_off_scaler if home_proj.ip > 0 else 0
    f5_total = f5_away + f5_home

    return round(full_total,2), round(f5_total,2), round(home_runs,2), round(away_runs,2)


# =============================================================================
# Improvement #1 — HFA on win probability (called from orchestrator)
# =============================================================================

def apply_hfa(home_win_prob: float, away_win_prob: float,
              hfa: float = HOME_FIELD_ADVANTAGE) -> tuple[float, float]:
    """
    Add home field advantage to Skellam win probabilities and re-normalise.
    hfa: probability points to add to home team (default 2.5pp).
    """
    home_adj = min(0.99, home_win_prob + hfa)
    away_adj = max(0.01, away_win_prob - hfa)
    # Re-normalise in case of floating point drift
    total = home_adj + away_adj
    return round(home_adj / total, 4), round(away_adj / total, 4)


# =============================================================================
# Improvement #2 — ML reliability gate (called from orchestrator)
# =============================================================================

def ml_edge_reliable(away_proj: PitcherProjection,
                     home_proj: PitcherProjection) -> bool:
    """
    Returns False if either starter is low_sample or league_avg.
    ML edges on fallback projections are noise — skip them.
    """
    return away_proj.is_reliable and home_proj.is_reliable


# =============================================================================
# Improvement #8 — Hitter props stub
# =============================================================================

def project_hitter_hr_prob(
    hitter_xstats: dict,
    pitcher_proj: PitcherProjection,
    park: dict,
    weather: dict,
) -> Optional[float]:
    """
    Estimate P(hitter hits HR in this game) using:
      - Hitter HR/FB rate (from hitter_xstats)
      - Pitcher xFIP-implied HR rate against
      - Park HR factor
      - Weather run factor (proxy for carry)

    Returns probability or None if insufficient data.
    Stub: returns None until hitter_xstats.hr_fb_rate is populated.
    """
    hr_fb = hitter_xstats.get("hr_fb_rate")
    if hr_fb is None:
        return None

    pf_hr = float(park.get("pf_hr") or 100) / 100.0
    cf_az = float(park.get("cf_azimuth_deg") or 0)
    is_dome = (park.get("roof_type") or "").lower() in ("dome","closed")
    wx = 1.0 if (is_dome or not weather) else (
        temp_run_factor(weather.get("temp_f"))
        * wind_run_factor(weather.get("wind_mph"), weather.get("wind_deg"), cf_az)
    )

    # Expected PA ~ 4.0 for average hitter
    expected_pa = 4.0
    fb_pct = hitter_xstats.get("fb_pct") or LEAGUE_FB_PCT
    expected_fb = expected_pa * float(fb_pct)
    hr_prob = expected_fb * float(hr_fb) * pf_hr * wx

    return round(min(hr_prob, 0.99), 4)


