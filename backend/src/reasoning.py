"""
Edge reasoning module.

For each flagged edge, produces:
  - reason_short:   one-line summary (~80 chars) for inline display
  - reason_factors: list of {label, value, impact} dicts for the expand view

The "impact" field is a counterfactual estimate: how much the projected value
would shift if that factor were set to neutral (league avg / no park / no weather).
This is the most honest decomposition available — the model isn't actually
additive by factor, but counterfactuals give real numbers rather than guesses.

Public API:
    reason_for_total(edge, ctx)  -> (short, factors)
    reason_for_f5(edge, ctx)     -> (short, factors)
    reason_for_ml(edge, ctx)     -> (short, factors)
    reason_for_prop(edge, ctx)   -> (short, factors)
"""
from __future__ import annotations
from typing import Optional


LEAGUE_TRUE_ERA_NEUTRAL = 4.20
LEAGUE_BULLPEN_ER9 = 4.00
LEAGUE_PARK_FACTOR = 1.00
NEUTRAL_TEMP = 70.0
NEUTRAL_WIND = 0.0


# ============================================================================
# Helpers
# ============================================================================

def _fmt_impact(delta: float, unit: str = "runs") -> str:
    """Format a delta as '+0.4 runs' / '-0.2 runs' / 'neutral'."""
    if abs(delta) < 0.05:
        return f"neutral {unit}"
    sign = "+" if delta > 0 else ""
    return f"{sign}{round(delta, 2)} {unit}"


def _temp_descriptor(temp_f: Optional[float]) -> str:
    if temp_f is None:
        return "no temp data"
    if temp_f >= 85:
        return f"hot ({int(temp_f)}°F)"
    if temp_f >= 75:
        return f"warm ({int(temp_f)}°F)"
    if temp_f >= 60:
        return f"mild ({int(temp_f)}°F)"
    if temp_f >= 50:
        return f"cool ({int(temp_f)}°F)"
    return f"cold ({int(temp_f)}°F)"


def _wind_descriptor(mph: Optional[float], deg: Optional[float], cf_az: Optional[float]) -> str:
    if mph is None or mph < 2:
        return "calm"
    if deg is None:
        return f"{int(mph)} mph"
    # Rough direction relative to center field
    if cf_az is None:
        return f"{int(mph)} mph"
    delta = ((deg + 180) - cf_az) % 360   # convert FROM-direction to TO-bearing, relative to CF
    if delta > 180:
        delta -= 360
    if abs(delta) <= 45:
        return f"{int(mph)} mph blowing out"
    if abs(delta) >= 135:
        return f"{int(mph)} mph blowing in"
    return f"{int(mph)} mph crosswind"


def _park_descriptor(pf_runs: float, park_name: Optional[str] = None) -> str:
    pct = (pf_runs - 1.0) * 100
    if abs(pct) < 3:
        tone = "neutral park"
    elif pct >= 8:
        tone = "very hitter-friendly"
    elif pct >= 3:
        tone = "hitter-friendly"
    elif pct <= -8:
        tone = "very pitcher-friendly"
    else:
        tone = "pitcher-friendly"
    return f"{park_name} ({tone}, PF {int(pf_runs*100)})" if park_name else f"{tone} (PF {int(pf_runs*100)})"


# ============================================================================
# Total
# ============================================================================

def reason_for_total(edge: dict, ctx: dict) -> tuple[str, list[dict]]:
    """
    Reason out an OVER/UNDER full-game total edge.

    ctx requires:
      - away_proj, home_proj (PitcherProjection)
      - park (dict)
      - weather (dict, may be empty)
      - away_team_xstats, home_team_xstats (dict or None)
      - market_total (float)
      - proj_total (float)  -- the model's prediction
    """
    away_proj   = ctx["away_proj"]
    home_proj   = ctx["home_proj"]
    park        = ctx.get("park") or {}
    weather     = ctx.get("weather") or {}
    proj_total  = float(ctx["proj_total"])
    market      = float(ctx["market_total"])

    pf_runs = float(park.get("pf_runs") or 100) / 100.0
    is_dome = (park.get("roof_type") or "").lower() in ("dome", "closed")
    park_name = park.get("park_name") or park.get("park_code")

    factors: list[dict] = []

    # --- Pitching factor: starters' combined true_era vs league neutral ---
    avg_true_era = (float(away_proj.true_era) + float(home_proj.true_era)) / 2.0
    # Each 0.5 ER9 above league translates to ~0.5 runs across ~10 IP of starter work
    pitching_delta = round((avg_true_era - LEAGUE_TRUE_ERA_NEUTRAL) * (away_proj.ip + home_proj.ip) / 18.0, 2)
    if avg_true_era >= 4.60:
        pitching_desc = f"Both starters high xERA ({away_proj.true_era}/{home_proj.true_era})"
    elif avg_true_era <= 3.60:
        pitching_desc = f"Both starters low xERA ({away_proj.true_era}/{home_proj.true_era})"
    elif abs(away_proj.true_era - home_proj.true_era) >= 1.0:
        pitching_desc = f"Mismatched starters ({away_proj.true_era}/{home_proj.true_era})"
    else:
        pitching_desc = f"League-average starters ({away_proj.true_era}/{home_proj.true_era})"
    factors.append({
        "label": "Pitching matchup",
        "value": pitching_desc,
        "impact": _fmt_impact(pitching_delta),
    })

    # --- Park ---
    park_delta = round((pf_runs - LEAGUE_PARK_FACTOR) * proj_total, 2)
    factors.append({
        "label": "Park",
        "value": _park_descriptor(pf_runs, park_name),
        "impact": _fmt_impact(park_delta),
    })

    # --- Weather ---
    if is_dome:
        factors.append({"label": "Weather", "value": "Dome / closed roof", "impact": "neutral runs"})
    else:
        cf_az = float(park.get("cf_azimuth_deg") or 0)
        temp_f = weather.get("temp_f")
        wind_mph = weather.get("wind_mph")
        wind_deg = weather.get("wind_deg")
        wx_desc = f"{_temp_descriptor(temp_f)}, wind {_wind_descriptor(wind_mph, wind_deg, cf_az)}"
        # Estimate weather impact: temp_run_factor is small (~1% per 10°F), wind larger
        temp_impact = 0.0
        if temp_f is not None:
            temp_impact = ((float(temp_f) - 70) / 10) * 0.0093 * proj_total
        wind_impact = 0.0
        if wind_mph and wind_mph >= 5 and wind_deg is not None:
            # Approximation: blowing-out wind adds ~2.5% per 10mph
            delta = ((wind_deg + 180) - cf_az) % 360
            if delta > 180: delta -= 360
            if abs(delta) <= 45:        # blowing out
                wind_impact = (wind_mph / 10) * 0.023 * proj_total
            elif abs(delta) >= 135:     # blowing in
                wind_impact = -(wind_mph / 10) * 0.023 * proj_total
        wx_delta = round(temp_impact + wind_impact, 2)
        factors.append({
            "label": "Weather",
            "value": wx_desc,
            "impact": _fmt_impact(wx_delta),
        })

    # --- Bullpens ---
    away_bp = (ctx.get("away_team_xstats") or {}).get("bullpen_era") or LEAGUE_BULLPEN_ER9
    home_bp = (ctx.get("home_team_xstats") or {}).get("bullpen_era") or LEAGUE_BULLPEN_ER9
    avg_bp = (float(away_bp) + float(home_bp)) / 2.0
    bp_innings = max(0.0, 9 - away_proj.ip) + max(0.0, 9 - home_proj.ip)
    bp_delta = round((avg_bp - LEAGUE_BULLPEN_ER9) * bp_innings / 9.0, 2)
    if avg_bp >= 4.50:
        bp_desc = f"Both bullpens above league avg ({round(avg_bp,2)} ER9)"
    elif avg_bp <= 3.30:
        bp_desc = f"Both bullpens below league avg ({round(avg_bp,2)} ER9)"
    else:
        bp_desc = f"Bullpens near league avg ({round(avg_bp,2)} ER9)"
    factors.append({"label": "Bullpens", "value": bp_desc, "impact": _fmt_impact(bp_delta)})

    # --- Short summary ---
    lean = edge.get("lean", "")
    edge_runs = edge.get("edge", 0)
    # Pick the two strongest non-neutral factors
    ranked = sorted(
        [(f, abs(_parse_impact_runs(f["impact"]))) for f in factors],
        key=lambda x: x[1], reverse=True,
    )
    top_factors = [r[0] for r in ranked if r[1] >= 0.1][:2]
    if top_factors:
        short_parts = [_brief_label(tf) for tf in top_factors]
        short = f"{lean} {edge_runs:+.1f}: " + ", ".join(short_parts)
    else:
        short = f"{lean} {edge_runs:+.1f}: model vs market"
    short = short[:100]   # safety cap

    return short, factors


# ============================================================================
# F5
# ============================================================================

def reason_for_f5(edge: dict, ctx: dict) -> tuple[str, list[dict]]:
    """F5 reasoning. Starters dominate; bullpens irrelevant. Park/weather muted."""
    away_proj = ctx["away_proj"]
    home_proj = ctx["home_proj"]
    park      = ctx.get("park") or {}
    weather   = ctx.get("weather") or {}
    proj_f5   = float(ctx["proj_f5"])

    pf_runs = float(park.get("pf_runs") or 100) / 100.0
    is_dome = (park.get("roof_type") or "").lower() in ("dome", "closed")
    park_name = park.get("park_name") or park.get("park_code")

    factors: list[dict] = []

    # --- Starters (dominant for F5) ---
    avg_true_era = (float(away_proj.true_era) + float(home_proj.true_era)) / 2.0
    pitching_delta = round((avg_true_era - LEAGUE_TRUE_ERA_NEUTRAL) * 5.0 / 9.0, 2)
    if avg_true_era >= 4.60:
        pitching_desc = f"Both starters high xERA ({away_proj.true_era}/{home_proj.true_era}) — F5 inflated"
    elif avg_true_era <= 3.60:
        pitching_desc = f"Both starters low xERA ({away_proj.true_era}/{home_proj.true_era}) — F5 suppressed"
    else:
        pitching_desc = f"Starters: {away_proj.true_era}/{home_proj.true_era} xERA"
    factors.append({"label": "Starters (F5 driver)", "value": pitching_desc, "impact": _fmt_impact(pitching_delta)})

    # --- Lineup quality vs starters ---
    away_lu = float(getattr(away_proj, "opp_lineup_xwoba", 0.320))
    home_lu = float(getattr(home_proj, "opp_lineup_xwoba", 0.320))
    avg_lu = (away_lu + home_lu) / 2.0
    lu_delta = round((avg_lu - 0.320) * 25, 2)   # ~25 runs per 0.100 xwOBA delta over F5
    if avg_lu >= 0.335:
        lu_desc = f"Strong lineups facing starters (avg xwOBA {avg_lu:.3f})"
    elif avg_lu <= 0.305:
        lu_desc = f"Weak lineups facing starters (avg xwOBA {avg_lu:.3f})"
    else:
        lu_desc = f"Average lineups (avg xwOBA {avg_lu:.3f})"
    factors.append({"label": "Lineup quality", "value": lu_desc, "impact": _fmt_impact(lu_delta)})

    # --- Park ---
    park_delta = round((pf_runs - 1.0) * proj_f5, 2)
    factors.append({"label": "Park", "value": _park_descriptor(pf_runs, park_name), "impact": _fmt_impact(park_delta)})

    # --- Weather (only if not dome and meaningful) ---
    if not is_dome and (weather.get("temp_f") or weather.get("wind_mph")):
        cf_az = float(park.get("cf_azimuth_deg") or 0)
        wx_desc = f"{_temp_descriptor(weather.get('temp_f'))}, wind {_wind_descriptor(weather.get('wind_mph'), weather.get('wind_deg'), cf_az)}"
        # Same formula as total but scaled to F5 portion
        temp_f = weather.get("temp_f")
        wind_mph = weather.get("wind_mph") or 0
        wind_deg = weather.get("wind_deg")
        temp_impact = ((float(temp_f) - 70) / 10) * 0.0093 * proj_f5 if temp_f else 0.0
        wind_impact = 0.0
        if wind_mph >= 5 and wind_deg is not None:
            delta = ((wind_deg + 180) - cf_az) % 360
            if delta > 180: delta -= 360
            if abs(delta) <= 45:
                wind_impact = (wind_mph / 10) * 0.023 * proj_f5
            elif abs(delta) >= 135:
                wind_impact = -(wind_mph / 10) * 0.023 * proj_f5
        wx_delta = round(temp_impact + wind_impact, 2)
        factors.append({"label": "Weather", "value": wx_desc, "impact": _fmt_impact(wx_delta)})

    # --- Short summary ---
    lean = edge.get("lean", "")
    edge_runs = edge.get("edge", 0)
    ranked = sorted([(f, abs(_parse_impact_runs(f["impact"]))) for f in factors], key=lambda x: x[1], reverse=True)
    top_factors = [r[0] for r in ranked if r[1] >= 0.05][:2]
    if top_factors:
        short = f"F5 {lean} {edge_runs:+.1f}: " + ", ".join(_brief_label(tf) for tf in top_factors)
    else:
        short = f"F5 {lean} {edge_runs:+.1f}: model vs market"
    return short[:100], factors


# ============================================================================
# ML (moneyline)
# ============================================================================

def reason_for_ml(edge: dict, ctx: dict) -> tuple[str, list[dict]]:
    """ML reasoning: starter quality differential is the dominant driver."""
    away_proj = ctx["away_proj"]
    home_proj = ctx["home_proj"]
    home_win_prob = float(ctx["home_win_prob"])
    away_win_prob = float(ctx["away_win_prob"])
    ml_team = edge.get("lean", "")

    factors: list[dict] = []

    # Starter advantage
    true_era_diff = float(away_proj.true_era) - float(home_proj.true_era)
    if ml_team == home_proj.team_code:
        adv_pitcher = home_proj
        opp_pitcher = away_proj
        starter_text = f"{home_proj.last_first} ({home_proj.true_era}) vs {away_proj.last_first} ({away_proj.true_era})"
    else:
        adv_pitcher = away_proj
        opp_pitcher = home_proj
        starter_text = f"{away_proj.last_first} ({away_proj.true_era}) vs {home_proj.last_first} ({home_proj.true_era})"
    starter_advantage = round(abs(true_era_diff) * 0.025, 3)   # rough win-prob impact
    factors.append({
        "label": "Starter advantage",
        "value": starter_text,
        "impact": f"+{starter_advantage*100:.1f}% win prob" if true_era_diff != 0 else "neutral",
    })

    # Model win prob vs implied
    line = float(edge.get("line") or 0)
    implied = 100 / (line + 100) if line > 0 else (-line) / ((-line) + 100)
    model_prob = home_win_prob if ml_team == home_proj.team_code else away_win_prob
    factors.append({
        "label": "Model vs market",
        "value": f"Model {round(model_prob*100,1)}% vs implied {round(implied*100,1)}%",
        "impact": f"+{round((model_prob - implied)*100, 1)}% edge",
    })

    # Variance flag if either pitcher high-variance
    if getattr(adv_pitcher, "high_variance_flag", False) or getattr(opp_pitcher, "high_variance_flag", False):
        flag_pitcher = adv_pitcher if getattr(adv_pitcher, "high_variance_flag", False) else opp_pitcher
        factors.append({
            "label": "Variance flag",
            "value": f"{flag_pitcher.last_first} high-variance projection",
            "impact": "less confidence",
        })

    short = f"{ml_team} ML: {round(model_prob*100,1)}% vs {round(implied*100,1)}% implied"
    return short[:100], factors


# ============================================================================
# Prop
# ============================================================================

def reason_for_prop(edge: dict, ctx: dict) -> tuple[str, list[dict]]:
    """Prop reasoning. Category-specific."""
    category = edge.get("category", "")
    pitcher = ctx.get("pitcher_proj")
    proj_value = float(edge.get("proj_value") or 0)
    line = float(edge.get("line") or 0)
    lean = edge.get("lean", "")
    pitcher_name = edge.get("pitcher_name", "")
    park = ctx.get("park") or {}
    weather = ctx.get("weather") or {}

    factors: list[dict] = []

    if category == "K":
        # K rate, opp K%, bullpen unused
        k_pct = getattr(pitcher, "k", 0) / max(float(getattr(pitcher, "outs", 18)) / 3, 0.1) if pitcher else 0
        factors.append({
            "label": "Strikeout rate",
            "value": f"{pitcher_name}: projected K rate {round(k_pct, 2)} per IP",
            "impact": _fmt_impact(proj_value - line, "K"),
        })
        factors.append({
            "label": "Innings projected",
            "value": f"{round(float(getattr(pitcher, 'ip', 0)), 1)} IP",
            "impact": "",
        })
    elif category == "Hits":
        factors.append({
            "label": "Hit allowance",
            "value": f"{pitcher_name}: projected {round(proj_value, 1)} hits",
            "impact": _fmt_impact(proj_value - line, "hits"),
        })
        pf_runs = float(park.get("pf_runs") or 100) / 100.0
        if abs(pf_runs - 1.0) >= 0.03:
            factors.append({
                "label": "Park",
                "value": _park_descriptor(pf_runs, park.get("park_name")),
                "impact": "",
            })
    elif category == "ER":
        factors.append({
            "label": "ER projection",
            "value": f"{pitcher_name}: {round(proj_value, 2)} ER projected",
            "impact": _fmt_impact(proj_value - line, "ER"),
        })
        factors.append({
            "label": "Conviction gate",
            "value": f"Poisson conviction {edge.get('conviction_pct', 0)}%",
            "impact": "(min 60% to fire)",
        })
    elif category == "Outs":
        factors.append({
            "label": "Innings → outs",
            "value": f"{pitcher_name}: {round(float(getattr(pitcher, 'ip', 0)), 1)} IP → {round(proj_value, 1)} outs",
            "impact": _fmt_impact(proj_value - line, "outs"),
        })
    else:
        factors.append({
            "label": category,
            "value": f"Projected {round(proj_value, 2)}, line {line}",
            "impact": _fmt_impact(proj_value - line, category.lower()),
        })

    short = f"{category} {lean} {pitcher_name}: {round(proj_value, 1)} vs {line}"
    return short[:100], factors


# ============================================================================
# Internal helpers
# ============================================================================

def _parse_impact_runs(impact_str: str) -> float:
    """Pull numeric runs value back out of '+0.4 runs' string for ranking."""
    if not impact_str or "neutral" in impact_str.lower():
        return 0.0
    try:
        parts = impact_str.split()
        return float(parts[0])
    except Exception:
        return 0.0


def _brief_label(factor: dict) -> str:
    """Compress a factor into 2-4 words for the inline summary."""
    label = factor["label"]
    val = factor["value"]
    # Extract the most relevant chunk
    if "starters" in label.lower():
        if "high xERA" in val: return "high-xERA SP"
        if "low xERA" in val:  return "low-xERA SP"
        return "SP matchup"
    if "park" in label.lower():
        if "hitter-friendly" in val: return "hitter park"
        if "pitcher-friendly" in val: return "pitcher park"
        return "park"
    if "weather" in label.lower():
        if "hot" in val or "warm" in val: return "warm/hot"
        if "cold" in val or "cool" in val: return "cold"
        if "blowing out" in val: return "wind out"
        if "blowing in" in val: return "wind in"
        return "weather"
    if "bullpen" in label.lower():
        if "above league" in val: return "weak bullpens"
        if "below league" in val: return "strong bullpens"
        return "bullpens"
    if "lineup" in label.lower():
        if "strong" in val.lower(): return "strong lineups"
        if "weak" in val.lower():   return "weak lineups"
        return "lineups"
    return label.lower().split()[0]
