"""
Edge reasoning module — v2 (counterfactual against actual model).

For each flagged total/F5 edge, runs the model multiple times:
  - Once with all inputs at "neutral" (league average) → baseline
  - Once per factor with ONLY that factor at real value, others neutral
    → isolated impact of that factor

Factor impacts then SUM to approximately (proj - baseline), give or take
small interaction effects. If they don't, the missing delta is reported as
"Model interactions" so the user can see the math is incomplete.

For ML/prop edges, simpler reasoning (no full counterfactual chain needed).

Public API:
    reason_for_total(edge, ctx)  -> (short, factors)
    reason_for_f5(edge, ctx)     -> (short, factors)
    reason_for_ml(edge, ctx)     -> (short, factors)
    reason_for_prop(edge, ctx)   -> (short, factors)
"""
from __future__ import annotations
import copy
from typing import Optional
from dataclasses import replace as dc_replace

from . import projections


LEAGUE_TRUE_ERA = 4.20
LEAGUE_BULLPEN_ER9 = 4.00
LEAGUE_XWOBA = 0.320
LEAGUE_IP = 5.5
LEAGUE_K_PER_IP = 0.95
LEAGUE_BB_PER_IP = 0.32
LEAGUE_HITS_PER_IP = 1.0
NEUTRAL_TEMP = 70.0


# ============================================================================
# Helpers — build neutral inputs that produce the model's baseline projection
# ============================================================================

def _neutral_pitcher(real: 'projections.PitcherProjection') -> 'projections.PitcherProjection':
    """
    Build a synthetic 'league-average' pitcher with the same team_code/opp/hand
    as the real one (those don't affect totals). All quality inputs set to
    league average.
    """
    er = LEAGUE_TRUE_ERA / 9 * LEAGUE_IP
    return dc_replace(
        real,
        source="statcast",
        pa_sample=200,
        era=LEAGUE_TRUE_ERA,
        xera=LEAGUE_TRUE_ERA,
        xfip=LEAGUE_TRUE_ERA,
        true_era=LEAGUE_TRUE_ERA,
        xwoba_against=LEAGUE_XWOBA,
        opp_lineup_xwoba=LEAGUE_XWOBA,
        used_actual_lineup=False,
        used_l15_blend=False,
        ip=LEAGUE_IP,
        outs=LEAGUE_IP * 3,
        hits=LEAGUE_HITS_PER_IP * LEAGUE_IP,
        er=er,
        bb=LEAGUE_BB_PER_IP * LEAGUE_IP,
        k=LEAGUE_K_PER_IP * LEAGUE_IP,
        wx_factor=1.0,
        pf_factor=1.0,
        high_variance_flag=False,
    )


def _neutral_team_xstats() -> dict:
    """League-average team xstats dict."""
    return {
        "team_xwoba": LEAGUE_XWOBA,
        "est_woba": LEAGUE_XWOBA,
        "team_woba_l5": LEAGUE_XWOBA,
        "bullpen_era": LEAGUE_BULLPEN_ER9,
        "bullpen_xera": LEAGUE_BULLPEN_ER9,
        "bullpen_ip": 200.0,
        "bullpen_era_l7": LEAGUE_BULLPEN_ER9,
        "bullpen_ip_l7": 25.0,
    }


def _neutral_park() -> dict:
    return {
        "pf_runs": 100, "pf_hr": 100, "pf_so": 100, "pf_bb": 100,
        "cf_azimuth_deg": 0, "roof_type": "open",
    }


def _neutral_weather() -> dict:
    return {"temp_f": NEUTRAL_TEMP, "wind_mph": 0, "wind_deg": None}


def _run_total(away_proj, home_proj, away_team, home_team, park, weather):
    """Helper: run project_game_total and return just the relevant scalar."""
    full, f5, h, a = projections.project_game_total(
        away_proj=away_proj, home_proj=home_proj,
        away_team_xstats=away_team, home_team_xstats=home_team,
        park=park, weather=weather,
    )
    return full, f5


# ============================================================================
# Counterfactual decomposition
# ============================================================================

def _decompose_total(ctx: dict, want_f5: bool = False) -> dict:
    """
    Run the model under various neutralizations to attribute the projection
    delta to specific factors.

    Returns dict with:
      baseline:      projection with all inputs neutral
      actual:        projection with all inputs real
      factor_deltas: dict of {factor_name: impact_vs_baseline}
      residual:      actual - baseline - sum(deltas), captures interactions
    """
    away_real     = ctx["away_proj"]
    home_real     = ctx["home_proj"]
    away_team     = ctx.get("away_team_xstats") or {}
    home_team     = ctx.get("home_team_xstats") or {}
    park_real     = ctx.get("park") or {}
    weather_real  = ctx.get("weather") or {}

    # Build neutrals
    away_neu      = _neutral_pitcher(away_real)
    home_neu      = _neutral_pitcher(home_real)
    team_neu      = _neutral_team_xstats()
    park_neu      = _neutral_park()
    weather_neu   = _neutral_weather()

    idx = 1 if want_f5 else 0  # 0=full, 1=f5

    # Baseline: everything neutral
    baseline = _run_total(away_neu, home_neu, team_neu, team_neu, park_neu, weather_neu)[idx]

    # Actual
    actual = _run_total(away_real, home_real, away_team, home_team, park_real, weather_real)[idx]

    # Per-factor counterfactuals: real value for ONE factor, neutral for rest
    deltas = {}

    # Pitching (both starters real)
    deltas["pitching"] = _run_total(
        away_real, home_real, team_neu, team_neu, park_neu, weather_neu
    )[idx] - baseline

    # Lineup quality is already baked into pitcher.opp_lineup_xwoba, which is
    # part of pitcher_real. The pitching delta already includes lineup effect.
    # We can isolate lineup separately by giving pitcher quality real but
    # opp_lineup_xwoba neutral — requires constructing a hybrid pitcher.
    away_no_lu = dc_replace(away_real, opp_lineup_xwoba=LEAGUE_XWOBA)
    home_no_lu = dc_replace(home_real, opp_lineup_xwoba=LEAGUE_XWOBA)
    pitching_no_lu = _run_total(
        away_no_lu, home_no_lu, team_neu, team_neu, park_neu, weather_neu
    )[idx] - baseline
    deltas["lineup_quality"] = deltas["pitching"] - pitching_no_lu
    deltas["pitching"] = pitching_no_lu  # pitching ALONE, not pitching+lineup

    # Team offensive scaler
    deltas["offense"] = _run_total(
        away_neu, home_neu, away_team, home_team, park_neu, weather_neu
    )[idx] - baseline

    # Park
    deltas["park"] = _run_total(
        away_neu, home_neu, team_neu, team_neu, park_real, weather_neu
    )[idx] - baseline

    # Weather (only meaningful if not dome)
    if (park_real.get("roof_type") or "").lower() in ("dome", "closed"):
        deltas["weather"] = 0.0
    else:
        deltas["weather"] = _run_total(
            away_neu, home_neu, team_neu, team_neu, park_real, weather_real
        )[idx] - _run_total(
            away_neu, home_neu, team_neu, team_neu, park_real, weather_neu
        )[idx]

    # Bullpen (only for full game — F5 doesn't use bullpen)
    if not want_f5:
        # Real bullpens with everything else neutral
        deltas["bullpen"] = _run_total(
            away_neu, home_neu,
            {**team_neu, "bullpen_era": away_team.get("bullpen_era", LEAGUE_BULLPEN_ER9),
             "bullpen_ip": away_team.get("bullpen_ip", 200),
             "bullpen_era_l7": away_team.get("bullpen_era_l7"),
             "bullpen_ip_l7": away_team.get("bullpen_ip_l7", 0)},
            {**team_neu, "bullpen_era": home_team.get("bullpen_era", LEAGUE_BULLPEN_ER9),
             "bullpen_ip": home_team.get("bullpen_ip", 200),
             "bullpen_era_l7": home_team.get("bullpen_era_l7"),
             "bullpen_ip_l7": home_team.get("bullpen_ip_l7", 0)},
            park_neu, weather_neu,
        )[idx] - baseline

    # Residual = what's left after all isolated deltas (interactions)
    residual = actual - baseline - sum(deltas.values())

    return {
        "baseline": round(baseline, 2),
        "actual": round(actual, 2),
        "factor_deltas": {k: round(v, 2) for k, v in deltas.items()},
        "residual": round(residual, 2),
    }


# ============================================================================
# Formatting
# ============================================================================

def _fmt_runs(d: float) -> str:
    if abs(d) < 0.05:
        return "neutral"
    sign = "+" if d > 0 else ""
    return f"{sign}{d:.2f} runs"


def _temp_descriptor(t):
    if t is None: return "no temp"
    if t >= 85: return f"hot ({int(t)}°F)"
    if t >= 75: return f"warm ({int(t)}°F)"
    if t >= 60: return f"mild ({int(t)}°F)"
    if t >= 50: return f"cool ({int(t)}°F)"
    return f"cold ({int(t)}°F)"


def _wind_desc(mph, deg, cf_az):
    if not mph or mph < 2: return "calm"
    if deg is None or cf_az is None: return f"{int(mph)} mph"
    d = ((deg + 180) - cf_az) % 360
    if d > 180: d -= 360
    if abs(d) <= 45: return f"{int(mph)} mph blowing out"
    if abs(d) >= 135: return f"{int(mph)} mph blowing in"
    return f"{int(mph)} mph crosswind"


def _park_desc(pf):
    pct = (pf - 1.0) * 100
    if abs(pct) < 3: return f"neutral park (PF {int(pf*100)})"
    if pct >= 8:   return f"very hitter-friendly (PF {int(pf*100)})"
    if pct >= 3:   return f"hitter-friendly (PF {int(pf*100)})"
    if pct <= -8:  return f"very pitcher-friendly (PF {int(pf*100)})"
    return f"pitcher-friendly (PF {int(pf*100)})"


def _build_factor_rows(decomp: dict, ctx: dict, want_f5: bool) -> list[dict]:
    """Turn decomposition into the dashboard's {label, value, impact} dicts."""
    deltas = decomp["factor_deltas"]
    away_p = ctx["away_proj"]
    home_p = ctx["home_proj"]
    park   = ctx.get("park") or {}
    weather= ctx.get("weather") or {}

    rows = []

    # Pitching
    rows.append({
        "label": "Starting pitching",
        "value": f"{away_p.last_first.split(',')[0]} ({away_p.true_era}) vs "
                 f"{home_p.last_first.split(',')[0]} ({home_p.true_era})",
        "impact": _fmt_runs(deltas.get("pitching", 0)),
    })

    # Lineup quality (vs each starter)
    avg_lu = (float(away_p.opp_lineup_xwoba) + float(home_p.opp_lineup_xwoba)) / 2
    lu_desc = "Strong" if avg_lu >= 0.335 else ("Weak" if avg_lu <= 0.305 else "Average")
    rows.append({
        "label": "Lineup quality",
        "value": f"{lu_desc} lineups facing starters (avg xwOBA {avg_lu:.3f})",
        "impact": _fmt_runs(deltas.get("lineup_quality", 0)),
    })

    # Team offense (separate from lineup-vs-starter — this is the team's
    # own xwOBA blended with L5 form)
    away_team = ctx.get("away_team_xstats") or {}
    home_team = ctx.get("home_team_xstats") or {}
    away_off = away_team.get("team_xwoba") or away_team.get("est_woba") or LEAGUE_XWOBA
    home_off = home_team.get("team_xwoba") or home_team.get("est_woba") or LEAGUE_XWOBA
    avg_off = (float(away_off) + float(home_off)) / 2
    off_desc = "above league" if avg_off >= 0.328 else ("below league" if avg_off <= 0.312 else "near league")
    rows.append({
        "label": "Team offense",
        "value": f"Both lineups {off_desc} (avg xwOBA {avg_off:.3f})",
        "impact": _fmt_runs(deltas.get("offense", 0)),
    })

    # Park
    pf_runs = float(park.get("pf_runs") or 100) / 100.0
    park_name = park.get("park_name") or park.get("park_code") or ""
    rows.append({
        "label": "Park",
        "value": f"{park_name} {_park_desc(pf_runs)}" if park_name else _park_desc(pf_runs),
        "impact": _fmt_runs(deltas.get("park", 0)),
    })

    # Weather
    is_dome = (park.get("roof_type") or "").lower() in ("dome", "closed")
    if is_dome:
        rows.append({"label": "Weather", "value": "Dome / closed roof", "impact": "neutral"})
    else:
        cf_az = float(park.get("cf_azimuth_deg") or 0)
        wx = f"{_temp_descriptor(weather.get('temp_f'))}, " \
             f"wind {_wind_desc(weather.get('wind_mph'), weather.get('wind_deg'), cf_az)}"
        rows.append({
            "label": "Weather",
            "value": wx,
            "impact": _fmt_runs(deltas.get("weather", 0)),
        })

    # Bullpen (full game only)
    if not want_f5:
        away_bp = (ctx.get("away_team_xstats") or {}).get("bullpen_era") or LEAGUE_BULLPEN_ER9
        home_bp = (ctx.get("home_team_xstats") or {}).get("bullpen_era") or LEAGUE_BULLPEN_ER9
        avg_bp = (float(away_bp) + float(home_bp)) / 2
        bp_desc = ("Both bullpens weak" if avg_bp >= 4.50
                   else "Both bullpens strong" if avg_bp <= 3.30
                   else "Bullpens near average")
        rows.append({
            "label": "Bullpens",
            "value": f"{bp_desc} ({avg_bp:.2f} ER9)",
            "impact": _fmt_runs(deltas.get("bullpen", 0)),
        })

    # Residual (model interactions) — only if non-trivial
    if abs(decomp["residual"]) >= 0.15:
        rows.append({
            "label": "Model interactions",
            "value": "Combined effect of factors above (non-linear)",
            "impact": _fmt_runs(decomp["residual"]),
        })

    # Baseline reference at the bottom
    rows.append({
        "label": "Neutral baseline",
        "value": f"Model prediction with all inputs at league average",
        "impact": f"{decomp['baseline']:.2f} runs",
    })

    return rows


def _short_summary(edge: dict, decomp: dict, want_f5: bool) -> str:
    """Pick the 2 largest |impact| factors for the inline one-liner."""
    deltas = decomp["factor_deltas"]
    lean = edge.get("lean", "")
    edge_val = edge.get("edge", 0)

    # Match the sign: if lean is UNDER, prioritize negative deltas
    sign_matters = -1 if lean == "UNDER" else 1

    # Sort by impact magnitude in lean direction
    sorted_factors = sorted(
        deltas.items(),
        key=lambda kv: kv[1] * sign_matters,
        reverse=True,
    )

    labels = {
        "pitching": {"pos": "high-xERA SP", "neg": "low-xERA SP"},
        "lineup_quality": {"pos": "strong lineups", "neg": "weak lineups"},
        "offense": {"pos": "hot offenses", "neg": "cold offenses"},
        "park": {"pos": "hitter park", "neg": "pitcher park"},
        "weather": {"pos": "wind out/warm", "neg": "wind in/cold"},
        "bullpen": {"pos": "weak bullpens", "neg": "strong bullpens"},
    }

    top = []
    for name, delta in sorted_factors[:2]:
        if abs(delta) < 0.1:
            continue
        d = labels.get(name, {})
        top.append(d["pos"] if delta > 0 else d["neg"])

    prefix = "F5 " if want_f5 else ""
    if top:
        return f"{prefix}{lean} {edge_val:+.1f}: " + ", ".join(top)
    return f"{prefix}{lean} {edge_val:+.1f}: model vs market"


# ============================================================================
# Public API
# ============================================================================

def reason_for_total(edge: dict, ctx: dict) -> tuple[str, list[dict]]:
    try:
        decomp = _decompose_total(ctx, want_f5=False)
        rows = _build_factor_rows(decomp, ctx, want_f5=False)
        short = _short_summary(edge, decomp, want_f5=False)
        return short[:120], rows
    except Exception as e:
        return f"{edge.get('lean','')} {edge.get('edge',0):+.1f}: model vs market", []


def reason_for_f5(edge: dict, ctx: dict) -> tuple[str, list[dict]]:
    try:
        decomp = _decompose_total(ctx, want_f5=True)
        rows = _build_factor_rows(decomp, ctx, want_f5=True)
        short = _short_summary(edge, decomp, want_f5=True)
        return short[:120], rows
    except Exception as e:
        return f"F5 {edge.get('lean','')} {edge.get('edge',0):+.1f}: model vs market", []


def reason_for_ml(edge: dict, ctx: dict) -> tuple[str, list[dict]]:
    """ML: simpler — show starter matchup, win prob delta, and any high-variance flags."""
    away_proj = ctx["away_proj"]
    home_proj = ctx["home_proj"]
    home_wp = float(ctx.get("home_win_prob", 0.5))
    away_wp = float(ctx.get("away_win_prob", 0.5))
    ml_team = edge.get("lean", "")
    line = float(edge.get("line") or 0)
    implied = 100/(line+100) if line > 0 else (-line)/((-line)+100)

    if ml_team == home_proj.team_code:
        model_prob, adv_p, opp_p = home_wp, home_proj, away_proj
    else:
        model_prob, adv_p, opp_p = away_wp, away_proj, home_proj

    rows = [
        {"label": "Starter matchup",
         "value": f"{adv_p.last_first.split(',')[0]} ({adv_p.true_era}) vs "
                  f"{opp_p.last_first.split(',')[0]} ({opp_p.true_era})",
         "impact": f"{round(opp_p.true_era - adv_p.true_era, 2):+.2f} xERA advantage"},
        {"label": "Win probability",
         "value": f"Model {model_prob*100:.1f}% vs implied {implied*100:.1f}%",
         "impact": f"+{(model_prob - implied)*100:.1f}pp"},
        {"label": "Lineup vs starter",
         "value": f"Adv. lineup xwOBA {opp_p.opp_lineup_xwoba:.3f} vs "
                  f"opp lineup xwOBA {adv_p.opp_lineup_xwoba:.3f}",
         "impact": "lineup context"},
    ]
    if getattr(adv_p, "high_variance_flag", False) or getattr(opp_p, "high_variance_flag", False):
        flag_p = adv_p if getattr(adv_p, "high_variance_flag", False) else opp_p
        rows.append({
            "label": "Variance flag",
            "value": f"{flag_p.last_first.split(',')[0]} high-variance projection",
            "impact": "less confidence",
        })

    short = f"{ml_team} ML: {model_prob*100:.1f}% vs {implied*100:.1f}% implied"
    return short[:120], rows


def reason_for_prop(edge: dict, ctx: dict) -> tuple[str, list[dict]]:
    """Prop: pitcher's projected output vs the line, with key inputs."""
    cat = edge.get("category", "")
    pitcher = ctx.get("pitcher_proj")
    name = (edge.get("pitcher_name") or "").split(",")[0]
    proj = float(edge.get("proj_value") or 0)
    line = float(edge.get("line") or 0)
    lean = edge.get("lean", "")

    rows = [
        {"label": f"{cat} projection",
         "value": f"{name}: model projects {proj:.2f}",
         "impact": f"{(proj-line):+.2f} vs line {line}"},
    ]

    if pitcher:
        rows.append({
            "label": "Pitcher quality",
            "value": f"xERA {pitcher.true_era}, xwOBA-against {pitcher.xwoba_against:.3f}",
            "impact": "input",
        })
        rows.append({
            "label": "Innings projected",
            "value": f"{pitcher.ip:.1f} IP vs {pitcher.opp_team_code} lineup "
                     f"(xwOBA {pitcher.opp_lineup_xwoba:.3f})",
            "impact": "input",
        })
        if cat == "ER":
            rows.append({
                "label": "Conviction gate",
                "value": f"Poisson conviction {edge.get('conviction_pct', 0)}%",
                "impact": "min 60% to fire",
            })

    short = f"{cat} {lean} {name}: {proj:.1f} vs line {line}"
    return short[:120], rows
