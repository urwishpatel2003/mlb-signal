"""
Daily orchestrator.

This is the cron entry point. It runs the full end-to-end pipeline:

  1. Fetch today's schedule from MLB Stats API (always fresh)
  2. For each game, persist game record, lineups, weather to Postgres
  3. Pull Statcast xStats from Postgres (refreshed by separate daily job)
  4. Pull current odds from The Odds API (joined onto games)
  5. Project each starter using lineup-weighted xwOBA + handedness platoon
  6. Compute edges (game totals + props) and tag them with confidence tier
  7. Persist projection_run, pitcher_projections, game_projections, edges
  8. Emit ntfy notification with top edges
  9. (Dashboard auto-refreshes from FastAPI which queries the latest run)

Scheduling:
  - 06:00 ET - refresh Statcast (separate job, statcast_refresh.py)
  - 09:00 ET - first orchestrator run (pre-lineup, based on probables)
  - 11:00 ET - second run (some lineups posted)
  - then every 30 min until first pitch (catches lineup confirmations + line moves)
  - Each run is idempotent; we INSERT a new projection_run row each time so
    history is never overwritten and we can track how projections moved.

Failure modes:
  - MLB Stats API down: retry 3x, then ntfy alert and exit non-zero
  - Postgres down: same
  - Odds API down: log warning, proceed with NULL market_total (edges still
    flagged on relative magnitude)
  - Statcast row missing: pitcher gets `low_sample` or `league_avg` source,
    flagged in the projection row, ntfy notes count of fallbacks
"""
from __future__ import annotations
import argparse
import logging
import os
import sys
import traceback
from dataclasses import asdict
from datetime import date, datetime
from typing import Optional

from . import db, mlb_api, projections, ntfy
from .odds import attach_odds_to_games
from .weather import enrich_weather_for_game
from .park_factors import get_park_for_team

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("orchestrator")

MODEL_VERSION = "v2.0"  # reset for the new architecture


# ---------- Edge calculation ----------

import math


def poisson_tail_prob(lam: float, line: float, side: str) -> float:
    if lam <= 0:
        return 0.5
    is_half = abs(line - round(line)) > 0.01
    threshold = math.ceil(line) if side == "OVER" else math.floor(line)
    if side == "OVER":
        cdf = 0.0
        term = math.exp(-lam)
        for k in range(int(threshold)):
            cdf += term
            term *= lam / (k + 1)
        return max(0.0, min(1.0, 1.0 - cdf))
    else:
        cdf = 0.0
        term = math.exp(-lam)
        upper = int(threshold) if is_half else int(threshold) - 1
        for k in range(max(0, upper) + 1):
            cdf += term
            term *= lam / (k + 1)
        return max(0.0, min(1.0, cdf))

EDGE_THRESHOLDS = {
    "Total": 0.50,
    "K": 0.50,
    "Hits": 0.70,
    "ER": 0.50,
    "Outs": 0.70,
}

# Tier rules (confidence tiers based on data quality + edge magnitude)
def confidence_tier(edge: dict, proj: Optional[projections.PitcherProjection] = None) -> int:
    """1 = highest, 3 = lowest. None for non-flagged."""
    abs_edge = abs(edge["edge"])
    # Disqualifiers - anything keyed off low_sample / league_avg drops to tier 3
    if proj and proj.source != "statcast":
        return 3
    if edge.get("category") == "Total":
        if abs_edge >= 1.5:
            return 1
        if abs_edge >= 1.0:
            return 2
        return 3
    # Props
    if abs_edge >= 2.0:
        return 1
    if abs_edge >= 1.0:
        return 2
    return 3


def half(x: float) -> float:
    return round(x * 2) / 2


def estimate_book_lines(p: projections.PitcherProjection,
                         live_lines: Optional[dict] = None) -> dict:
    """
    Use live odds when available, else estimate. Real book lines for K/H/ER/Outs
    by pitcher are queryable from The Odds API's player props endpoint, but
    require explicit subscription tier - we'll wire that in later. For now,
    ERA-anchored estimates per the established methodology.
    """
    if live_lines:
        return live_lines

    surface_era = p.era or 4.30
    market_er = surface_era * (5.5 / 9)
    return {
        "K":    half((8.5 / 9) * 5.5),
        "Hits": 5.5,
        "ER":   half(market_er),
        "Outs": 16.5,
    }


def compute_edges_for_game(*, game_pk: int, game: dict,
                            away_proj: projections.PitcherProjection,
                            home_proj: projections.PitcherProjection,
                            market_total: Optional[float]) -> list[dict]:
    edges: list[dict] = []

    # ---- Game total ----
    full_total, f5_total = projections.project_game_total(
        away_proj=away_proj, home_proj=home_proj
    )
    if market_total is not None:
        diff = full_total - market_total
        if abs(diff) >= EDGE_THRESHOLDS["Total"]:
            lean = "OVER" if diff > 0 else "UNDER"
            conviction = poisson_tail_prob(full_total, float(market_total), lean)
            edges.append({
                "game_pk": game_pk,
                "kind": "total",
                "category": "Total",
                "pitcher_mlb_id": None,
                "pitcher_name": None,
                "team_code": game.get("away_team"),
                "opp_team_code": game.get("home_team"),
                "line": float(market_total),
                "proj_value": full_total,
                "edge": round(diff, 2),
                "lean": lean,
                "conviction_pct": round(conviction * 100, 1),
                "flagged": True,
                "notes": None,
            })

    # ---- Pitcher props ----
    for p in (away_proj, home_proj):
        if p.source != "statcast":
            continue   # never surface props on fallback projections
        lines = estimate_book_lines(p)
        proj_vals = {"K": p.k, "Hits": p.hits, "ER": p.er, "Outs": p.outs}
        for stat, line in lines.items():
            if stat == "BB":
                continue
            diff = proj_vals[stat] - line
            if abs(diff) < EDGE_THRESHOLDS.get(stat, 0.7):
                continue
            prop_lean = "OVER" if diff > 0 else "UNDER"
            prop_conviction = poisson_tail_prob(proj_vals[stat], float(line), prop_lean)
            edges.append({
                "game_pk": game_pk,
                "kind": "prop",
                "category": stat,
                "pitcher_mlb_id": p.pitcher_mlb_id,
                "pitcher_name": p.last_first,
                "team_code": p.team_code,
                "opp_team_code": p.opp_team_code,
                "line": float(line),
                "proj_value": round(proj_vals[stat], 2),
                "edge": round(diff, 2),
                "lean": prop_lean,
                "conviction_pct": round(prop_conviction * 100, 1),
                "flagged": True,
                "notes": None,
            })

    return edges


# ---------- Persistence helpers ----------

def persist_game(g: mlb_api.Game) -> None:
    park_code = get_park_for_team(g.home_team)
    weather = enrich_weather_for_game(g)  # NWS lookup if outdoor
    db.upsert_game({
        "game_pk": g.game_pk,
        "game_date": g.game_date_et,
        "game_time_et": g.game_time_et,
        "status": g.status,
        "away_team": g.away_team,
        "home_team": g.home_team,
        "away_record": g.away_record,
        "home_record": g.home_record,
        "park_code": park_code,
        "away_pitcher_id": g.away_pitcher.mlb_id if g.away_pitcher else None,
        "home_pitcher_id": g.home_pitcher.mlb_id if g.home_pitcher else None,
        "away_pitcher_hand": g.away_pitcher.hand if g.away_pitcher else None,
        "home_pitcher_hand": g.home_pitcher.hand if g.home_pitcher else None,
        "away_pitcher_name": g.away_pitcher.last_first if g.away_pitcher else None,
        "home_pitcher_name": g.home_pitcher.last_first if g.home_pitcher else None,
        "away_score": g.away_score,
        "home_score": g.home_score,
        "weather_condition": weather.get("condition"),
        "weather_temp_f": weather.get("temp_f"),
        "weather_wind": weather.get("wind_raw"),
    })

    # Lineups
    if g.away_lineup:
        db.replace_lineups(g.game_pk, g.away_team, [
            {"batting_order": s.order, "mlb_id": s.mlb_id,
             "full_name": s.full_name, "last_first": s.last_first,
             "bat_side": s.bat_side, "position": s.position}
            for s in g.away_lineup
        ])
    if g.home_lineup:
        db.replace_lineups(g.game_pk, g.home_team, [
            {"batting_order": s.order, "mlb_id": s.mlb_id,
             "full_name": s.full_name, "last_first": s.last_first,
             "bat_side": s.bat_side, "position": s.position}
            for s in g.home_lineup
        ])


# ---------- Main ----------

def run(trigger: str = "manual") -> dict:
    """Single end-to-end run. Returns metrics dict."""
    job_id = db.log_job_start(f"orchestrator:{trigger}")
    metrics: dict = {"trigger": trigger, "errors": []}
    try:
        run_date = date.today().isoformat()
        log.info("Fetching schedule for %s", run_date)
        games = mlb_api.get_schedule()
        active = [g for g in games if g.status not in ("Postponed", "Cancelled")]
        metrics["n_games"] = len(active)
        log.info("%d active games (of %d total)", len(active), len(games))

        # Persist games + lineups
        for g in active:
            persist_game(g)

        # Attach odds
        try:
            attach_odds_to_games(active)
        except Exception as e:
            log.warning("Odds API error (non-fatal): %s", e)
            metrics["errors"].append(f"odds: {e}")

        # Project each game
        run_id = db.create_projection_run(run_date, MODEL_VERSION, trigger,
                                           len(active))
        metrics["run_id"] = run_id

        # Cache hitter xStats + team xwOBA lookups for the whole slate at once
        season = date.today().year
        all_pit = {r["mlb_id"]: r for r in db.fetchall(
            "SELECT * FROM pitcher_xstats WHERE season_year = %s", (season,)
        )}
        all_hit = {r["mlb_id"]: r for r in db.fetchall(
            "SELECT * FROM hitter_xstats WHERE season_year = %s", (season,)
        )}
        all_team = {r["team_code"]: r for r in db.fetchall(
            "SELECT * FROM team_xstats WHERE season_year = %s", (season,)
        )}
        all_parks = {r["park_code"]: r for r in db.fetchall(
            "SELECT * FROM parks WHERE season_year = %s", (season,)
        )}

        all_edges = []
        n_lineup_confirmed = 0
        n_fallback_pitchers = 0
        for g in active:
            if not g.away_pitcher or not g.home_pitcher:
                log.warning("Skipping %s - missing probable pitcher", g.game_pk)
                continue

            park = all_parks.get(get_park_for_team(g.home_team)) or {}
            game_row = db.fetchone(
                "SELECT * FROM games WHERE game_pk = %s", (g.game_pk,)
            ) or {}
            weather = {
                "temp_f": game_row.get("weather_temp_f"),
                "wind_mph": game_row.get("weather_wind_mph"),
                "wind_deg": game_row.get("weather_wind_deg"),
            }

            # Project both starters
            for is_home in (False, True):
                pitcher_info = g.home_pitcher if is_home else g.away_pitcher
                team = g.home_team if is_home else g.away_team
                opp = g.away_team if is_home else g.home_team
                opp_lineup = g.away_lineup if is_home else g.home_lineup
                opp_lineup_input = [
                    projections.HitterSpot(
                        mlb_id=s.mlb_id, last_first=s.last_first,
                        bat_side=s.bat_side, order=s.order
                    ) for s in opp_lineup
                ]
                team_xwoba_fallback = float(
                    (all_team.get(opp) or {}).get("est_woba") or projections.LEAGUE_XWOBA
                )
                proj = projections.project_pitcher(
                    pitcher_xstats=all_pit.get(pitcher_info.mlb_id),
                    pitcher_mlb_id=pitcher_info.mlb_id,
                    pitcher_name=pitcher_info.last_first,
                    pitcher_hand=pitcher_info.hand,
                    team_code=team,
                    opp_team_code=opp,
                    opp_lineup=opp_lineup_input,
                    hitter_xstats=all_hit,
                    team_xwoba_fallback=team_xwoba_fallback,
                    park=park,
                    weather={} if (park.get("roof_type") or "").lower() in ("dome", "closed") else weather,
                )
                if proj.source != "statcast":
                    n_fallback_pitchers += 1
                if proj.used_actual_lineup:
                    n_lineup_confirmed += 1

                proj_dict = proj.to_dict()
                proj_dict["mlb_id"] = proj_dict.pop("pitcher_mlb_id")
                proj_dict["game_pk"] = g.game_pk
                db.insert_pitcher_projection(run_id, proj_dict)

                if is_home:
                    home_proj = proj
                else:
                    away_proj = proj

            # Game total + edges
            full_total, f5_total = projections.project_game_total(
                away_proj=away_proj, home_proj=home_proj
            )
            market_total = float(game_row.get("market_total")) if game_row.get("market_total") else None
            edge_total = (full_total - market_total) if market_total else None
            lean = "PASS"
            if edge_total is not None:
                if edge_total > 0.5:
                    lean = "OVER"
                elif edge_total < -0.5:
                    lean = "UNDER"

            db.insert_game_projection(run_id, {
                "game_pk": g.game_pk,
                "proj_total": full_total,
                "proj_f5": f5_total,
                "market_total": market_total,
                "edge_total": round(edge_total, 2) if edge_total is not None else None,
                "lean": lean,
                "confidence_tier": None,
            })

            game_edges = compute_edges_for_game(
                game_pk=g.game_pk, game=asdict(g),
                away_proj=away_proj, home_proj=home_proj,
                market_total=market_total,
            )
            for e in game_edges:
                # Tier needs the projection that drove the edge
                proj_for_tier = None
                if e["pitcher_mlb_id"] == away_proj.pitcher_mlb_id:
                    proj_for_tier = away_proj
                elif e["pitcher_mlb_id"] == home_proj.pitcher_mlb_id:
                    proj_for_tier = home_proj
                e["confidence_tier"] = confidence_tier(e, proj_for_tier)
                db.insert_edge(run_id, e)
                all_edges.append(e)

        all_edges.sort(key=lambda x: abs(x["edge"]), reverse=True)
        metrics["n_edges"] = len(all_edges)
        metrics["n_lineups_confirmed"] = n_lineup_confirmed
        metrics["n_fallback_pitchers"] = n_fallback_pitchers
        log.info("Run %d complete: %d edges, %d lineup-confirmed, %d fallbacks",
                 run_id, len(all_edges), n_lineup_confirmed, n_fallback_pitchers)

        # ---- Notification ----
        ntfy.send_edges_summary(run_id, all_edges, metrics)

        db.log_job_finish(job_id, "success", None, metrics)
        return metrics

    except Exception as e:
        tb = traceback.format_exc()
        log.error("Orchestrator failed: %s\n%s", e, tb)
        db.log_job_finish(job_id, "failure", str(e), metrics)
        try:
            ntfy.send_failure(f"orchestrator:{trigger}", str(e))
        except Exception:
            pass
        raise


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trigger", default="manual",
                    help="Trigger name: morning|line_move|lineup_confirm|manual")
    args = ap.parse_args()
    metrics = run(trigger=args.trigger)
    print(metrics)


if __name__ == "__main__":
    main()
