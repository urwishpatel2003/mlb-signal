"""
Run from repo root: python patch_admin_diagnostics.py

Adds permanent admin diagnostic endpoints to api.py. These let you inspect
system state from the browser without needing the SQL console or Railway CLI.

All endpoints are token-gated under /api/admin/diag/*

Endpoints added:
  /api/admin/diag/xstats/{token}                  - xstats table state + LEAGUE_XWOBA
  /api/admin/diag/projection_bias/{token}         - 14-day proj vs market drift
  /api/admin/diag/edges/{token}?date=YYYY-MM-DD   - edges for date by kind/lean
  /api/admin/diag/games/{token}?date=YYYY-MM-DD   - games + projections + F5 cols
  /api/admin/diag/pitcher_projections/{token}     - 14-day pitcher proj summary
  /api/admin/diag/weather_check/{token}           - weather + projection correlation
  /api/admin/diag/jobs/{token}?job=NAME           - recent job_runs (any job)
  /api/admin/diag/sql/{token}?q=base64_query      - run any read-only SELECT (last resort)
  /api/admin/diag/index/{token}                   - list all diagnostic endpoints

Idempotent — safe to re-run.
"""
from pathlib import Path

api_path = Path("backend/src/api.py")
content = api_path.read_text(encoding="utf-8")

# Strip any previously-added diagnostic blocks (we're consolidating into one module)
# Markers used:
markers_to_strip = [
    "# F5 DIAGNOSTIC ENDPOINT",
    "# XSTATS DIAGNOSTIC ENDPOINT",
    "# ADMIN DIAGNOSTICS MODULE",
]

for marker in markers_to_strip:
    if marker in content:
        idx = content.find(marker)
        # Find the start of the block (the comment line before the marker, if any)
        block_start = content.rfind("\n\n", 0, idx)
        if block_start == -1:
            block_start = idx
        # Cut from block_start to end of file (these are always at the end)
        content = content[:block_start].rstrip() + "\n"
        print(f"Stripped previous block: {marker}")

# Now append the unified diagnostics module
diag_module = '''


# ============================================================================
# ADMIN DIAGNOSTICS MODULE — permanent endpoints for production introspection
# All endpoints token-gated. Read-only.
# ============================================================================
import base64 as _b64
import os as _os
from datetime import date as _date, timedelta as _td


def _check_admin(token: str):
    if token != _os.environ.get("ADMIN_TOKEN"):
        raise HTTPException(status_code=403, detail="Forbidden")


@app.get("/api/admin/diag/index/{token}")
def diag_index(token: str):
    """List all available diagnostic endpoints."""
    _check_admin(token)
    return {
        "endpoints": [
            {"path": "/api/admin/diag/index/{token}",
             "desc": "This index of diagnostic endpoints"},
            {"path": "/api/admin/diag/xstats/{token}",
             "desc": "xstats table state, sample values, LEAGUE_XWOBA, last refresh"},
            {"path": "/api/admin/diag/projection_bias/{token}",
             "desc": "14-day projection vs market drift"},
            {"path": "/api/admin/diag/edges/{token}?date=YYYY-MM-DD",
             "desc": "Edges flagged for a given run_date by kind/lean"},
            {"path": "/api/admin/diag/games/{token}?date=YYYY-MM-DD",
             "desc": "Games + projections + F5 cols for a given date"},
            {"path": "/api/admin/diag/pitcher_projections/{token}",
             "desc": "14-day pitcher projection summary (IP, true_era, source)"},
            {"path": "/api/admin/diag/weather_check/{token}",
             "desc": "Weather averages vs projection bias by date"},
            {"path": "/api/admin/diag/jobs/{token}?job=NAME",
             "desc": "Recent job_runs entries (statcast_refresh, orchestrator, grader)"},
            {"path": "/api/admin/diag/sql/{token}?q=BASE64",
             "desc": "Run a read-only SELECT (base64-encoded). Last resort."},
            {"path": "/api/admin/diag/f5/{token}?game_pk=NNN",
             "desc": "F5 linescore fetch + parse + write for one game"},
        ],
        "notes": "All endpoints return JSON. Use ?date= and ?job= query params where noted.",
    }


@app.get("/api/admin/diag/xstats/{token}")
def diag_xstats(token: str):
    """xstats tables: row counts, sample values, LEAGUE_XWOBA, last refresh."""
    _check_admin(token)
    from . import db, projections

    result = {"computed_season_year": _date.today().year}

    for tbl in ("pitcher_xstats", "hitter_xstats", "team_xstats"):
        try:
            rows = db.fetchall(
                f"SELECT season_year, COUNT(*) AS n FROM {tbl} "
                f"GROUP BY season_year ORDER BY season_year DESC"
            )
            result[tbl] = [dict(r) for r in rows]
        except Exception as e:
            result[tbl] = {"error": str(e)}

    try:
        rows = db.fetchall(
            "SELECT season_year, team_code, est_woba "
            "FROM team_xstats ORDER BY season_year DESC, est_woba DESC LIMIT 30"
        )
        result["team_xstats_sample"] = [dict(r) for r in rows]
    except Exception as e:
        result["team_xstats_sample"] = {"error": str(e)}

    try:
        rows = db.fetchall(
            "SELECT season_year, COUNT(*) AS n, "
            "ROUND(AVG(est_woba)::numeric, 4) AS avg_est_woba, "
            "ROUND(AVG(pa)::numeric, 1) AS avg_pa, "
            "COUNT(*) FILTER (WHERE pa >= 100) AS n_solid "
            "FROM hitter_xstats GROUP BY season_year ORDER BY season_year DESC"
        )
        result["hitter_xstats_summary"] = [dict(r) for r in rows]
    except Exception as e:
        result["hitter_xstats_summary"] = {"error": str(e)}

    try:
        result["LEAGUE_XWOBA"] = float(projections.LEAGUE_XWOBA)
    except Exception as e:
        result["LEAGUE_XWOBA_error"] = str(e)

    try:
        row = db.fetchone(
            "SELECT job_id, started_at::text, finished_at::text, status, error, payload "
            "FROM job_runs WHERE job_name='statcast_refresh' "
            "ORDER BY started_at DESC LIMIT 1"
        )
        result["last_statcast_refresh"] = dict(row) if row else None
    except Exception as e:
        result["last_statcast_refresh"] = {"error": str(e)}

    return result


@app.get("/api/admin/diag/projection_bias/{token}")
def diag_projection_bias(token: str):
    """Project vs market drift across the last 14 days."""
    _check_admin(token)
    from . import db

    rows = db.fetchall(
        "SELECT pr.run_date::text AS run_date, "
        "       COUNT(*) AS n, "
        "       ROUND(AVG(gp.proj_total - g.market_total)::numeric, 2) AS avg_diff, "
        "       ROUND(AVG(gp.proj_total)::numeric, 2) AS avg_proj, "
        "       ROUND(AVG(g.market_total)::numeric, 2) AS avg_market, "
        "       ROUND(AVG(gp.proj_home_runs)::numeric, 2) AS avg_home_proj, "
        "       ROUND(AVG(gp.proj_away_runs)::numeric, 2) AS avg_away_proj, "
        "       COUNT(*) FILTER (WHERE gp.proj_total < g.market_total) AS n_under, "
        "       COUNT(*) FILTER (WHERE gp.proj_total > g.market_total) AS n_over "
        "FROM game_projections gp "
        "JOIN games g ON g.game_pk = gp.game_pk "
        "JOIN projection_runs pr ON pr.run_id = gp.run_id "
        "WHERE g.market_total IS NOT NULL AND gp.proj_total IS NOT NULL "
        "  AND pr.run_date >= CURRENT_DATE - 14 "
        "GROUP BY pr.run_date ORDER BY pr.run_date DESC"
    )
    return {"days": [dict(r) for r in rows]}


@app.get("/api/admin/diag/edges/{token}")
def diag_edges(token: str, date: str = None):
    """Edges flagged for a given run_date, grouped by kind/lean."""
    _check_admin(token)
    from . import db

    target = date or _date.today().isoformat()
    rows = db.fetchall(
        "SELECT e.kind, e.lean, COUNT(*) AS n, "
        "       ROUND(AVG(e.edge)::numeric, 2) AS avg_edge, "
        "       ROUND(AVG(e.conviction_pct)::numeric, 1) AS avg_conviction "
        "FROM edges e JOIN projection_runs pr ON pr.run_id = e.run_id "
        "WHERE pr.run_date = %s AND e.flagged = TRUE "
        "GROUP BY e.kind, e.lean ORDER BY e.kind, e.lean",
        (target,)
    )
    detail = db.fetchall(
        "SELECT e.kind, e.category, e.lean, e.team_code, e.opp_team_code, "
        "       e.pitcher_name, e.line, e.proj_value, e.edge, e.conviction_pct, "
        "       e.confidence_tier "
        "FROM edges e JOIN projection_runs pr ON pr.run_id = e.run_id "
        "WHERE pr.run_date = %s AND e.flagged = TRUE "
        "ORDER BY e.kind, ABS(e.edge) DESC",
        (target,)
    )
    return {
        "run_date": target,
        "summary": [dict(r) for r in rows],
        "edges": [dict(r) for r in detail],
    }


@app.get("/api/admin/diag/games/{token}")
def diag_games(token: str, date: str = None):
    """Games + projections + F5 columns for a given date."""
    _check_admin(token)
    from . import db

    target = date or _date.today().isoformat()
    rows = db.fetchall(
        "SELECT g.game_pk, g.away_team, g.home_team, "
        "       g.market_total, g.market_f5_total, "
        "       g.away_score, g.home_score, "
        "       g.away_f5_runs, g.home_f5_runs, "
        "       g.weather_temp_f, g.weather_wind_mph, g.weather_wind, "
        "       gp.proj_total, gp.proj_f5, gp.proj_home_runs, gp.proj_away_runs, "
        "       (gp.proj_total - g.market_total) AS diff "
        "FROM games g "
        "LEFT JOIN game_projections gp ON gp.game_pk = g.game_pk "
        "LEFT JOIN projection_runs pr ON pr.run_id = gp.run_id "
        "WHERE g.game_date = %s "
        "ORDER BY g.game_pk",
        (target,)
    )
    return {"date": target, "games": [dict(r) for r in rows]}


@app.get("/api/admin/diag/pitcher_projections/{token}")
def diag_pitcher_projections(token: str):
    """14-day pitcher projection summary."""
    _check_admin(token)
    from . import db

    rows = db.fetchall(
        "SELECT pr.run_date::text AS run_date, "
        "       COUNT(*) AS n_pitchers, "
        "       ROUND(AVG(pp.ip)::numeric, 2) AS avg_ip, "
        "       ROUND(AVG(pp.true_era)::numeric, 2) AS avg_true_era, "
        "       ROUND(AVG(pp.k)::numeric, 2) AS avg_k, "
        "       COUNT(*) FILTER (WHERE pp.source='statcast') AS n_statcast, "
        "       COUNT(*) FILTER (WHERE pp.source='low_sample') AS n_low_sample, "
        "       COUNT(*) FILTER (WHERE pp.source='league_avg') AS n_league_avg "
        "FROM pitcher_projections pp "
        "JOIN projection_runs pr ON pr.run_id = pp.run_id "
        "WHERE pr.run_date >= CURRENT_DATE - 14 "
        "GROUP BY pr.run_date ORDER BY pr.run_date DESC"
    )
    return {"days": [dict(r) for r in rows]}


@app.get("/api/admin/diag/weather_check/{token}")
def diag_weather_check(token: str):
    """Weather averages vs projection bias per date."""
    _check_admin(token)
    from . import db

    rows = db.fetchall(
        "SELECT pr.run_date::text AS run_date, "
        "       COUNT(*) AS n, "
        "       ROUND(AVG(g.weather_temp_f)::numeric, 1) AS avg_temp, "
        "       ROUND(AVG(g.weather_wind_mph)::numeric, 1) AS avg_wind, "
        "       ROUND(AVG(gp.proj_total - g.market_total)::numeric, 2) AS avg_diff, "
        "       COUNT(*) FILTER (WHERE g.weather_temp_f < 60) AS n_cold, "
        "       COUNT(*) FILTER (WHERE g.weather_wind_mph > 15) AS n_windy "
        "FROM games g "
        "JOIN game_projections gp ON gp.game_pk = g.game_pk "
        "JOIN projection_runs pr ON pr.run_id = gp.run_id "
        "WHERE pr.run_date >= CURRENT_DATE - 14 "
        "  AND g.market_total IS NOT NULL "
        "GROUP BY pr.run_date ORDER BY pr.run_date DESC"
    )
    return {"days": [dict(r) for r in rows]}


@app.get("/api/admin/diag/jobs/{token}")
def diag_jobs(token: str, job: str = None):
    """Recent job_runs entries. Filter by job name with ?job=NAME."""
    _check_admin(token)
    from . import db

    if job:
        rows = db.fetchall(
            "SELECT job_id, job_name, started_at::text, finished_at::text, "
            "       status, error, payload "
            "FROM job_runs WHERE job_name LIKE %s "
            "ORDER BY started_at DESC LIMIT 20",
            (f"%{job}%",)
        )
    else:
        rows = db.fetchall(
            "SELECT job_id, job_name, started_at::text, finished_at::text, "
            "       status, error "
            "FROM job_runs ORDER BY started_at DESC LIMIT 30"
        )
    return {"jobs": [dict(r) for r in rows]}


@app.get("/api/admin/diag/sql/{token}")
def diag_sql(token: str, q: str = None):
    """Run an arbitrary read-only SELECT. Query must be base64-encoded.

    Safety:
      - Token-gated
      - Hard-rejects anything that doesn't start with SELECT (case-insensitive,
        after lstrip)
      - Hard-rejects presence of any of: insert, update, delete, drop, alter,
        truncate, grant, revoke
    """
    _check_admin(token)
    from . import db

    if not q:
        return {"error": "Usage: ?q=BASE64_ENCODED_SELECT_QUERY"}

    try:
        sql = _b64.b64decode(q).decode("utf-8")
    except Exception as e:
        return {"error": f"Could not decode base64: {e}"}

    s_lower = sql.strip().lower()
    if not s_lower.startswith("select"):
        return {"error": "Only SELECT statements allowed"}
    for bad in ("insert ", "update ", "delete ", "drop ", "alter ",
                "truncate ", "grant ", "revoke ", ";"):
        if bad in s_lower:
            return {"error": f"Disallowed token in SQL: {bad!r}"}

    try:
        rows = db.fetchall(sql)
        return {"sql": sql, "n_rows": len(rows),
                "rows": [dict(r) for r in rows[:100]]}
    except Exception as e:
        return {"sql": sql, "error": str(e)}


# F5 diagnostic — kept from earlier patch for completeness
@app.get("/api/admin/diag/f5/{token}")
def diag_f5(token: str, game_pk: int):
    """F5 linescore fetch + parse + write for one game."""
    _check_admin(token)
    from . import mlb_api, db
    import traceback

    result = {"game_pk": game_pk, "steps": []}
    try:
        ls = mlb_api.get_linescore(game_pk)
        result["steps"].append({"step": "fetch_linescore", "ok": True,
                                 "top_keys": list(ls.keys())})
    except Exception as e:
        result["steps"].append({"step": "fetch_linescore", "ok": False,
                                 "error": str(e), "tb": traceback.format_exc()})
        return result

    innings = ls.get("innings") or []
    result["steps"].append({"step": "parse_innings", "count": len(innings),
                             "first_5": [
                                 {"num": inn.get("num"),
                                  "away": inn.get("away"),
                                  "home": inn.get("home")}
                                 for inn in innings[:5]]})

    if len(innings) < 5:
        result["steps"].append({"step": "guard_innings_count", "passed": False})
        return result

    away_f5 = sum(int((inn.get("away") or {}).get("runs") or 0) for inn in innings[:5])
    home_f5 = sum(int((inn.get("home") or {}).get("runs") or 0) for inn in innings[:5])
    result["steps"].append({"step": "compute", "away_f5": away_f5, "home_f5": home_f5})

    try:
        row = db.fetchone(
            "SELECT away_f5_runs, home_f5_runs FROM games WHERE game_pk=%s",
            (game_pk,))
        result["steps"].append({"step": "readback",
                                 "row": dict(row) if row else None})
    except Exception as e:
        result["steps"].append({"step": "readback", "ok": False, "error": str(e)})

    return result
'''

content = content.rstrip() + diag_module + "\n"
api_path.write_text(content, encoding="utf-8")
print("OK: admin diagnostics module appended to api.py")
print()
print("Verify and push:")
print("  python -c \"import ast; ast.parse(open('backend/src/api.py').read()); print('OK')\"")
print("  git add backend/src/api.py patch_admin_diagnostics.py")
print("  git commit -m 'Admin diagnostics module: permanent introspection endpoints'")
print("  git push")
print()
print("After Railway redeploys, start at the index:")
print("  https://YOUR-RAILWAY-URL.up.railway.app/api/admin/diag/index/YOUR_TOKEN")
print()
print("The index lists every diagnostic endpoint with its purpose.")
