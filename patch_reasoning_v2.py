"""
Run from repo root: python patch_reasoning_v2.py

Two things:
  1. Replaces backend/src/reasoning.py with v2 (counterfactual against the
     real model — factor impacts now actually sum to projection delta).
  2. Adds a one-shot admin endpoint:
        GET /api/admin/recompute_reasoning/{token}?date=YYYY-MM-DD
     which re-runs reasoning against the existing projection data and
     UPDATEs reason_short / reason_factors on every edge for that date.

Prereqs:
  - reasoning_v2.py sitting in the repo root (alongside this script)
  - migration 0007_edge_reasoning.sql already applied
  - patch_edge_reasoning.py already run (so orchestrator wiring is in place)
"""
from pathlib import Path
import shutil

# ============================================================================
# 1. Replace backend/src/reasoning.py with v2
# ============================================================================
src = Path("reasoning_v2.py")
dst = Path("backend/src/reasoning.py")
if not src.exists():
    print("ERR: reasoning_v2.py not found in repo root.")
    print("     Download it alongside this patch script.")
    raise SystemExit(1)

shutil.copy(src, dst)
print(f"OK: replaced {dst} with v2 counterfactual reasoning")

# ============================================================================
# 2. Add /api/admin/recompute_reasoning/{token} endpoint to api.py
# ============================================================================
api_path = Path("backend/src/api.py")
api = api_path.read_text(encoding="utf-8")

if "/api/admin/recompute_reasoning/" in api:
    print("OK: recompute_reasoning endpoint already present")
else:
    endpoint = '''


# ============================================================================
# Recompute reasoning for an existing run_date — replaces broken reasoning
# attached during earlier runs with the v2 counterfactual version.
# ============================================================================
@app.get("/api/admin/recompute_reasoning/{token}")
def recompute_reasoning(token: str, date: str = None):
    """Re-run reasoning for every edge on a given run_date and overwrite the
    reason_short / reason_factors columns in place. Read-modify-write, idempotent."""
    _check_admin(token)
    from . import db, reasoning, projections
    from datetime import date as _date
    import json as _json
    import traceback

    target = date or _date.today().isoformat()
    summary = {"run_date": target, "n_edges": 0, "n_updated": 0,
               "n_skipped": 0, "errors": []}

    # Pull every edge for this run_date along with all the context we need
    # to re-run reasoning. We join projection_runs, edges, game_projections,
    # games, pitcher_projections.
    edge_rows = db.fetchall("""
        SELECT e.edge_id, e.game_pk, e.kind, e.category, e.lean, e.edge,
               e.line, e.proj_value, e.conviction_pct, e.confidence_tier,
               e.pitcher_mlb_id, e.pitcher_name, e.team_code, e.opp_team_code,
               e.ml_edge_pct, e.notes,
               e.over_price, e.under_price,
               gp.proj_total, gp.proj_f5, gp.proj_home_runs, gp.proj_away_runs,
               gp.home_win_prob, gp.away_win_prob,
               g.market_total, g.market_f5_total,
               g.away_team, g.home_team, g.park_code,
               g.weather_temp_f, g.weather_wind_mph, g.weather_wind_deg,
               g.weather_condition
        FROM edges e
        JOIN projection_runs pr ON pr.run_id = e.run_id
        LEFT JOIN game_projections gp ON gp.game_pk = e.game_pk AND gp.run_id = e.run_id
        LEFT JOIN games g ON g.game_pk = e.game_pk
        WHERE pr.run_date = %s AND e.flagged = TRUE
    """, (target,))

    summary["n_edges"] = len(edge_rows)
    if not edge_rows:
        return summary

    # Load season xstats once
    season = int(target[:4])
    all_pit_rows  = db.fetchall("SELECT * FROM pitcher_xstats WHERE season_year=%s", (season,))
    all_pit       = {r["mlb_id"]: r for r in all_pit_rows}
    all_team_rows = db.fetchall("SELECT * FROM team_xstats WHERE season_year=%s", (season,))
    all_team      = {r["team_code"]: r for r in all_team_rows}
    park_rows     = db.fetchall("SELECT * FROM parks WHERE season_year=%s", (season,))
    all_parks     = {r["park_code"]: r for r in park_rows}

    # Pre-cache pitcher projection rows per game_pk
    pp_rows = db.fetchall("""
        SELECT pp.*
        FROM pitcher_projections pp
        JOIN projection_runs pr ON pr.run_id = pp.run_id
        WHERE pr.run_date = %s
    """, (target,))
    pp_by_game = {}
    for r in pp_rows:
        pp_by_game.setdefault(r["game_pk"], []).append(r)

    def _hydrate_pitcher_projection(row):
        """Build a projections.PitcherProjection from a DB row."""
        return projections.PitcherProjection(
            pitcher_mlb_id=row["mlb_id"],
            last_first=row["last_first"],
            team_code=row["team_code"],
            opp_team_code=row["opp_team_code"],
            hand=row["hand"],
            source=row["source"],
            pa_sample=row.get("pa_sample"),
            era=float(row["era"]) if row.get("era") is not None else None,
            xera=float(row["xera"]) if row.get("xera") is not None else None,
            xfip=float(row["xfip"]) if row.get("xfip") is not None else None,
            true_era=float(row["true_era"]) if row.get("true_era") is not None else 4.20,
            xwoba_against=float(row["xwoba_against"]) if row.get("xwoba_against") is not None else None,
            opp_lineup_xwoba=float(row["opp_lineup_xwoba"]) if row.get("opp_lineup_xwoba") is not None else 0.320,
            used_actual_lineup=bool(row.get("used_actual_lineup", False)),
            used_l15_blend=bool(row.get("used_l15_blend", False)),
            ip=float(row["ip"]) if row.get("ip") is not None else 5.5,
            outs=float(row["outs"]) if row.get("outs") is not None else 16.5,
            hits=float(row["hits"]) if row.get("hits") is not None else 5.5,
            er=float(row["er"]) if row.get("er") is not None else 2.57,
            bb=float(row["bb"]) if row.get("bb") is not None else 1.76,
            k=float(row["k"]) if row.get("k") is not None else 5.5,
            wx_factor=float(row["wx_factor"]) if row.get("wx_factor") is not None else 1.0,
            pf_factor=float(row["pf_factor"]) if row.get("pf_factor") is not None else 1.0,
            high_variance_flag=bool(row.get("high_variance_flag", False)),
            days_rest=row.get("days_rest"),
        )

    for er in edge_rows:
        try:
            gp = er["game_pk"]
            pitchers = pp_by_game.get(gp, [])
            away_pp = next((p for p in pitchers if p["team_code"] == er["away_team"]), None)
            home_pp = next((p for p in pitchers if p["team_code"] == er["home_team"]), None)

            if not away_pp or not home_pp:
                summary["n_skipped"] += 1
                continue

            away_proj = _hydrate_pitcher_projection(away_pp)
            home_proj = _hydrate_pitcher_projection(home_pp)

            park = all_parks.get(er.get("park_code")) or {}
            weather = {
                "temp_f": er.get("weather_temp_f"),
                "wind_mph": er.get("weather_wind_mph"),
                "wind_deg": er.get("weather_wind_deg"),
                "condition": er.get("weather_condition"),
            }

            ctx = {
                "away_proj": away_proj,
                "home_proj": home_proj,
                "park": park,
                "weather": weather,
                "away_team_xstats": all_team.get(er["away_team"]),
                "home_team_xstats": all_team.get(er["home_team"]),
                "market_total": float(er["market_total"]) if er.get("market_total") is not None else None,
                "market_f5_total": float(er["market_f5_total"]) if er.get("market_f5_total") is not None else None,
                "proj_total": float(er["proj_total"]) if er.get("proj_total") is not None else None,
                "proj_f5": float(er["proj_f5"]) if er.get("proj_f5") is not None else None,
                "home_win_prob": float(er["home_win_prob"]) if er.get("home_win_prob") is not None else 0.5,
                "away_win_prob": float(er["away_win_prob"]) if er.get("away_win_prob") is not None else 0.5,
            }

            # Figure out which pitcher_proj this edge refers to for prop edges
            pft = None
            if er["kind"] == "prop":
                pft = away_proj if er.get("pitcher_mlb_id") == away_proj.pitcher_mlb_id else (
                    home_proj if er.get("pitcher_mlb_id") == home_proj.pitcher_mlb_id else None)

            edge_dict = dict(er)
            if er["kind"] == "total":
                short, factors = reasoning.reason_for_total(edge_dict, ctx)
            elif er["kind"] == "f5":
                short, factors = reasoning.reason_for_f5(edge_dict, ctx)
            elif er["kind"] == "ml":
                short, factors = reasoning.reason_for_ml(edge_dict, ctx)
            elif er["kind"] == "prop":
                short, factors = reasoning.reason_for_prop(
                    edge_dict, {**ctx, "pitcher_proj": pft})
            else:
                short, factors = None, None

            db.execute(
                "UPDATE edges SET reason_short=%s, reason_factors=%s::jsonb WHERE edge_id=%s",
                (short,
                 _json.dumps(factors) if factors is not None else None,
                 er["edge_id"]),
            )
            summary["n_updated"] += 1
        except Exception as e:
            summary["errors"].append({
                "edge_id": er.get("edge_id"),
                "kind": er.get("kind"),
                "error": str(e),
                "tb": traceback.format_exc()[-400:],
            })

    return summary
'''
    api = api.rstrip() + endpoint + "\n"
    api_path.write_text(api, encoding="utf-8")
    print("OK: /api/admin/recompute_reasoning endpoint added")

print()
print("Steps to deploy:")
print("  python -X utf8 -c \"import ast; ast.parse(open('backend/src/reasoning.py').read()); ast.parse(open('backend/src/api.py').read()); print('OK')\"")
print()
print("  git add backend/src/reasoning.py backend/src/api.py")
print("  git add patch_reasoning_v2.py reasoning_v2.py")
print("  git commit -m 'Reasoning v2: counterfactual decomposition + recompute endpoint'")
print("  git push")
print()
print("After Railway redeploys:")
print("  https://YOUR-RAILWAY-URL.up.railway.app/api/admin/recompute_reasoning/<token>?date=2026-05-18")
print()
print("Returns JSON: { n_edges, n_updated, n_skipped, errors[] }")
print("Refresh the dashboard and every edge should show correct reasoning that matches its lean.")
