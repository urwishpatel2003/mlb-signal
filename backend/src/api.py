"""
FastAPI backend.

Exposes endpoints for the React dashboard:

  GET /api/health                              health check
  GET /api/slate/today                         latest run for today's slate
  GET /api/slate/{date}                        archived slate
  GET /api/run/{run_id}                        specific run by id
  GET /api/run/{run_id}/edges                  edges for a run
  GET /api/run/{run_id}/projections            pitcher projections for a run
  GET /api/performance/rolling                 rolling performance metrics
  GET /api/performance/recent                  last 30 days of graded plays

  POST /api/admin/run-now                      trigger orchestrator (auth-gated)
  POST /api/admin/grade                        trigger grader (auth-gated)

The "auth gate" is a simple shared-secret header (X-Admin-Token) compared to
ADMIN_TOKEN env var. It's not Fort Knox but it's enough to prevent randos
spinning up cron jobs.

CORS is open in development, restricted to the deployed dashboard domain in
production (set ALLOWED_ORIGINS env var).
"""
from __future__ import annotations
import os
from datetime import date, datetime, timezone, timedelta
from typing import Optional
import logging

from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from . import db

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("api")

app = FastAPI(title="mlb-signal API", version="0.1.0")

origins_env = os.environ.get("ALLOWED_ORIGINS", "*")
allow_origins = [o.strip() for o in origins_env.split(",")] if origins_env != "*" else ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")


def _require_admin(token: Optional[str]) -> None:
    if not ADMIN_TOKEN:
        raise HTTPException(503, "ADMIN_TOKEN not configured")
    if token != ADMIN_TOKEN:
        raise HTTPException(401, "Invalid admin token")


# ---------- Health ----------

@app.get("/api/health")
def health():
    try:
        row = db.fetchone("SELECT 1 AS ok")
        return {"status": "ok", "db": bool(row)}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# ---------- Slate ----------

@app.get("/api/slate/today")
def slate_today():
    et_now = datetime.now(timezone.utc) - timedelta(hours=4); return _slate_for_date(et_now.date().isoformat())


@app.get("/api/slate/{slate_date}")
def slate_for_date(slate_date: str):
    return _slate_for_date(slate_date)


def _slate_for_date(slate_date: str) -> dict:
    run = db.fetchone(
        """
        SELECT * FROM projection_runs
        WHERE run_date = %s
        ORDER BY run_started_at DESC LIMIT 1
        """,
        (slate_date,),
    )
    if not run:
        return {"date": slate_date, "run": None, "games": [], "edges": [], "projections": []}

    run_id = run["run_id"]
    games_raw = db.fetchall(
        """
        SELECT g.*, gp.proj_total, gp.proj_f5, gp.proj_home_runs, gp.proj_away_runs, gp.edge_total, gp.lean, gp.market_f5_total, gp.away_ml, gp.home_ml, gp.home_win_prob, gp.away_win_prob, gp.ml_edge_team
        FROM games g
        LEFT JOIN LATERAL (
            SELECT proj_total, proj_f5, proj_home_runs, proj_away_runs, edge_total, lean, market_f5_total, away_ml, home_ml, home_win_prob, away_win_prob, ml_edge_team, ml_edge_pct, edge_f5, lean_f5, hfa_applied
            FROM game_projections gp_inner
            JOIN projection_runs pr ON pr.run_id = gp_inner.run_id
            WHERE gp_inner.game_pk = g.game_pk AND pr.run_date = %s
            ORDER BY gp_inner.run_id DESC
            LIMIT 1
        ) gp ON TRUE
        WHERE g.game_date = %s
        ORDER BY g.game_time_et
        """,
        (slate_date, slate_date),
    )
    edges = db.fetchall(
        """
        SELECT DISTINCT ON (e.game_pk, e.kind, e.category, COALESCE(e.pitcher_mlb_id, 0), e.lean)
               e.*
        FROM edges e
        JOIN projection_runs pr ON pr.run_id = e.run_id
        WHERE pr.run_date = %s
        ORDER BY e.game_pk, e.kind, e.category, COALESCE(e.pitcher_mlb_id, 0), e.lean, e.run_id DESC
        """,
        (slate_date,),
    )
    projs = db.fetchall(
        """
        SELECT DISTINCT ON (pp.game_pk, pp.mlb_id) pp.*
        FROM pitcher_projections pp
        JOIN projection_runs pr ON pr.run_id = pp.run_id
        WHERE pr.run_date = %s
        ORDER BY pp.game_pk, pp.mlb_id, pp.run_id DESC
        """,
        (slate_date,),
    )
    return {
        "date": slate_date,
        "run": dict(run),
        "games": [dict(g) for g in games_raw],
        "edges": [dict(e) for e in edges],
        "projections": [dict(p) for p in projs],
    }


@app.get("/api/run/{run_id}")
def get_run(run_id: int):
    run = db.fetchone("SELECT * FROM projection_runs WHERE run_id = %s", (run_id,))
    if not run:
        raise HTTPException(404, f"Run {run_id} not found")
    return dict(run)


@app.get("/api/run/{run_id}/edges")
def get_run_edges(run_id: int, flagged_only: bool = True):
    sql = "SELECT * FROM edges WHERE run_id = %s"
    if flagged_only:
        sql += " AND flagged = TRUE"
    sql += " ORDER BY ABS(edge) DESC"
    return [dict(e) for e in db.fetchall(sql, (run_id,))]


@app.get("/api/admin/wipe-runs/{token}")
def wipe_runs(token: str):
    if token != os.environ.get("ADMIN_TOKEN"):
        raise HTTPException(status_code=403, detail="Forbidden")
    # Wipe edges for runs 25, 26, 27 (post-firstpitch projections with bad data)
    n_edges_before = db.fetchone("SELECT COUNT(*) AS c FROM edges WHERE run_id IN (25,26,27)")["c"]
    n_results_deleted = 0
    db.execute("""
        DELETE FROM edge_results
        WHERE edge_id IN (SELECT edge_id FROM edges WHERE run_id IN (25,26,27))
    """)
    db.execute("DELETE FROM edges WHERE run_id IN (25,26,27)")
    db.execute("DELETE FROM game_projections WHERE run_id IN (25,26,27)")
    db.execute("DELETE FROM pitcher_projections WHERE run_id IN (25,26,27)")
    db.execute("DELETE FROM projection_runs WHERE run_id IN (25,26,27)")
    db.execute("DELETE FROM model_performance")
    n_edges_after = db.fetchone("SELECT COUNT(*) AS c FROM edges WHERE run_id IN (25,26,27)")["c"]
    return {
        "edges_before": n_edges_before,
        "edges_after": n_edges_after,
        "deleted": n_edges_before - n_edges_after,
        "note": "Wiped runs 25-27. model_performance also reset.",
    }


@app.get("/api/admin/dedupe-grades/{token}")
def dedupe_grades(token: str):
    if token != os.environ.get("ADMIN_TOKEN"):
        raise HTTPException(status_code=403, detail="Forbidden")
    before = db.fetchone("SELECT COUNT(*) AS c FROM edge_results")["c"]
    db.execute("DELETE FROM model_performance")
    db.execute("""
        DELETE FROM edge_results
        WHERE edge_id IN (
          SELECT e.edge_id FROM edges e
          JOIN projection_runs pr ON pr.run_id = e.run_id
          WHERE pr.run_date = '2026-04-28' AND e.kind = 'prop'
        )
    """)
    db.execute("""
        DELETE FROM edge_results
        WHERE edge_id NOT IN (
          SELECT MAX(e.edge_id)
          FROM edges e
          WHERE e.flagged = TRUE
          GROUP BY e.game_pk, e.kind, e.category, COALESCE(e.pitcher_mlb_id, 0), e.lean
        )
    """)
    after = db.fetchone("SELECT COUNT(*) AS c FROM edge_results")["c"]
    return {"before": before, "after": after, "deleted": before - after}


@app.get("/api/run/{run_id}/projections")
def get_run_projections(run_id: int):
    return [dict(p) for p in db.fetchall(
        "SELECT * FROM pitcher_projections WHERE run_id = %s",
        (run_id,),
    )]


# ---------- Performance ----------
@app.get("/api/admin/test-pybaseball/{token}")
def test_pybaseball(token: str):
    if token != os.environ.get("ADMIN_TOKEN"): raise HTTPException(403)
    from pybaseball import pitching_stats
    df = pitching_stats(2026, qual=0)
    return {"columns": df.columns.tolist(), "sample": df.head(3).to_dict(orient="records")}



@app.get("/api/performance/rolling")
def performance_rolling():
    """Latest rolling performance snapshot per window."""
    return [dict(r) for r in db.fetchall(
        """
        SELECT DISTINCT ON (window_days) *
        FROM model_performance
        ORDER BY window_days, snapshot_date DESC
        """
    )]


@app.get("/api/performance/recent")
def performance_recent(days: int = 30):
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    return [dict(r) for r in db.fetchall(
        """
        SELECT pr.run_date, e.kind, e.category, e.line, e.proj_value, e.edge,
               e.lean, er.result, er.profit_units
        FROM edges e
        JOIN edge_results er ON er.edge_id = e.edge_id
        JOIN projection_runs pr ON pr.run_id = e.run_id
        WHERE pr.run_date >= %s AND e.flagged = TRUE
        ORDER BY pr.run_date DESC, ABS(e.edge) DESC
        """,
        (cutoff,),
    )]


# ---------- Admin actions ----------

class TriggerResponse(BaseModel):
    job_id: Optional[int] = None
    status: str
    message: str


@app.post("/api/admin/run-now", response_model=TriggerResponse)
def admin_run_now(x_admin_token: Optional[str] = Header(None)):
    _require_admin(x_admin_token)
    from .orchestrator import run as run_orch
    try:
        metrics = run_orch(trigger="manual_via_api")
        return TriggerResponse(
            job_id=metrics.get("run_id"),
            status="success",
            message=f"Run {metrics.get('run_id')}: {metrics.get('n_edges', 0)} edges",
        )
    except Exception as e:
        log.exception("Manual run failed")
        return TriggerResponse(status="failure", message=str(e))


@app.post("/api/admin/grade", response_model=TriggerResponse)
def admin_grade(target_date: Optional[str] = None,
                 x_admin_token: Optional[str] = Header(None)):
    _require_admin(x_admin_token)
    from .grader import grade_yesterday
    try:
        target = date.fromisoformat(target_date) if target_date else None
        metrics = grade_yesterday(target)
        return TriggerResponse(
            status="success",
            message=f"{metrics.get('wins', 0)}-{metrics.get('losses', 0)}-{metrics.get('pushes', 0)}",
        )
    except Exception as e:
        log.exception("Grader failed")
        return TriggerResponse(status="failure", message=str(e))
        
        



@app.get("/api/performance/by-date")
def performance_by_date():
    """
    Returns daily performance broken down by kind/category/lean,
    with the full list of plays per (kind, category, lean) bucket.

    Schema:
      [
        {
          "run_date": "2026-04-28",
          "summary": {"wins": 41, "losses": 23, "pushes": 1, "profit_units": 15.81},
          "buckets": [
            {
              "kind": "total", "category": "Total", "lean": "OVER",
              "wins": 5, "losses": 1, "pushes": 0, "profit_units": 3.4,
              "plays": [
                {"matchup": "BOS @ TOR", "pitcher_name": null,
                 "line": 8.5, "actual_value": 11, "result": "WIN",
                 "profit_units": 0.91, "edge_value": 2.88},
                ...
              ]
            },
            ...
          ]
        },
        ...
      ]
    """
    rows = db.fetchall("""
        SELECT
          pr.run_date,
          e.kind,
          e.category,
          e.lean,
          e.lean,
          e.team_code,
          e.opp_team_code,
          e.pitcher_name,
          e.line,
          e.proj_value,
          e.edge,
          er.actual_value,
          er.result,
          er.profit_units
        FROM edge_results er
        JOIN edges e ON e.edge_id = er.edge_id
        JOIN projection_runs pr ON pr.run_id = e.run_id
        WHERE e.flagged = TRUE
          AND (e.lean IN ('OVER','UNDER') OR e.kind = 'ml')
        ORDER BY pr.run_date DESC, e.kind, e.category, e.lean,
                 ABS(e.edge) DESC
    """)

    by_date = {}
    for r in rows:
        d = str(r["run_date"])
        if d not in by_date:
            by_date[d] = {
                "run_date": d,
                "summary": {"wins": 0, "losses": 0, "pushes": 0, "profit_units": 0.0},
                "buckets": {},  # keyed by (kind, category, lean)
            }

        result = r["result"]
        profit = float(r["profit_units"] or 0)
        # Tally summary
        if result == "WIN":  by_date[d]["summary"]["wins"] += 1
        elif result == "LOSS": by_date[d]["summary"]["losses"] += 1
        elif result == "PUSH": by_date[d]["summary"]["pushes"] += 1
        by_date[d]["summary"]["profit_units"] = round(
            by_date[d]["summary"]["profit_units"] + profit, 2
        )

        # Bucket by (kind, category, lean)
        # ML edges: lean is team code (e.g. 'WSH') — bucket all ML together
        bucket_lean = "ML" if r["kind"] == "ml" else r["lean"]
        bk = (r["kind"], r["category"], bucket_lean)
        b = by_date[d]["buckets"].setdefault(bk, {
            "kind": r["kind"],
            "category": r["category"],
            "lean": bucket_lean,
            "wins": 0, "losses": 0, "pushes": 0,
            "profit_units": 0.0,
            "plays": [],
        })
        if result == "WIN":  b["wins"] += 1
        elif result == "LOSS": b["losses"] += 1
        elif result == "PUSH": b["pushes"] += 1
        b["profit_units"] = round(b["profit_units"] + profit, 2)

        # Compose play row. For totals use matchup, for props use pitcher name.
        is_total = r["kind"] in ("total", "ml", "f5")
        lean_label = f" → {r['lean']}" if r["kind"] == "ml" else ""
        if r["kind"] == "ml":
            subject = f"{r['team_code'] or '?'} @ {r['opp_team_code'] or '?'}{lean_label}"
        elif is_total:
            subject = f"{r['team_code'] or '?'} @ {r['opp_team_code'] or '?'}"
        else:
            subject = r["pitcher_name"] or "?"
        b["plays"].append({
            "subject": subject,
            "team_code": r["team_code"],
            "opp_team_code": r["opp_team_code"],
            "line": float(r["line"]) if r["line"] is not None else None,
            "proj_value": float(r["proj_value"]) if r["proj_value"] is not None else None,
            "edge": float(r["edge"]) if r["edge"] is not None else None,
            "actual_value": float(r["actual_value"]) if r["actual_value"] is not None else None,
            "result": result,
            "profit_units": round(profit, 2),
        })

    # Convert dict buckets to list, in stable order
    result_list = []
    for d, payload in sorted(by_date.items(), reverse=True):
        bucket_list = sorted(
            payload["buckets"].values(),
            key=lambda b: (
                0 if b["kind"] == "total" else 1,
                {"Total": 0, "K": 1, "Outs": 2, "ER": 3, "Hits": 4, "BB": 5}.get(b["category"], 9),
                0 if b["lean"] == "OVER" else 1,
            )
        )
        payload["buckets"] = bucket_list
        result_list.append(payload)
    return result_list



@app.get("/api/performance/overall")
def performance_overall():
    """All-time totals broken down by kind/category."""
    rows = db.fetchall("""
        SELECT
          e.kind,
          e.category,
          e.lean,
          COUNT(*) FILTER (WHERE er.result = 'WIN')   AS wins,
          COUNT(*) FILTER (WHERE er.result = 'LOSS')  AS losses,
          COUNT(*) FILTER (WHERE er.result = 'PUSH')  AS pushes,
          COALESCE(SUM(er.profit_units), 0)::float    AS profit_units
        FROM edge_results er
        JOIN edges e ON e.edge_id = er.edge_id
        WHERE e.flagged = TRUE
        GROUP BY e.kind, e.category, e.lean
        ORDER BY e.kind, e.category, e.lean
    """)

    overall = {"wins": 0, "losses": 0, "pushes": 0, "profit_units": 0.0}
    by_category = []
    for r in rows:
        wins = int(r["wins"] or 0)
        losses = int(r["losses"] or 0)
        pushes = int(r["pushes"] or 0)
        profit = float(r["profit_units"] or 0)
        overall["wins"] += wins
        overall["losses"] += losses
        overall["pushes"] += pushes
        overall["profit_units"] = round(overall["profit_units"] + profit, 2)
        by_category.append({
            "kind": r["kind"],
            "lean": r["lean"],
            "category": r["category"],
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "profit_units": round(profit, 2),
        })

    # ML fav/dog breakdown using edge line (negative = fav, positive = dog)
    ml_rows = db.fetchall("""
        SELECT
            CASE WHEN e.line < 0 THEN 'FAV' ELSE 'DOG' END as ml_type,
            COUNT(*) FILTER (WHERE er.result = 'WIN')  AS wins,
            COUNT(*) FILTER (WHERE er.result = 'LOSS') AS losses,
            COUNT(*) FILTER (WHERE er.result = 'PUSH') AS pushes,
            COALESCE(SUM(er.profit_units), 0)::float     AS profit_units
        FROM edge_results er
        JOIN edges e ON e.edge_id = er.edge_id
        WHERE e.flagged = TRUE AND e.kind = 'ml'
        GROUP BY ml_type
        ORDER BY ml_type
    """)
    ml_breakdown = []
    for r in ml_rows:
        ml_breakdown.append({
            "label": "Favourite" if r["ml_type"] == "FAV" else "Underdog",
            "wins": int(r["wins"] or 0),
            "losses": int(r["losses"] or 0),
            "pushes": int(r["pushes"] or 0),
            "profit_units": round(float(r["profit_units"] or 0), 2),
        })
    return {"overall": overall, "by_category": by_category, "ml_breakdown": ml_breakdown}
# ============================================================================
# Manual trigger endpoints
# Hit these from your phone / browser to manually fire any scheduled job.
# Auth: token in URL path. Same ADMIN_TOKEN env var as other admin routes.
# ============================================================================

@app.get("/api/admin/trigger/statcast/{token}")
def trigger_statcast(token: str):
    if token != os.environ.get("ADMIN_TOKEN"):
        raise HTTPException(status_code=403, detail="Forbidden")
    from . import statcast_refresh
    try:
        result = statcast_refresh.refresh_statcast()
        return {"job": "statcast_refresh", "ok": True, "result": result}
    except Exception as e:
        return {"job": "statcast_refresh", "ok": False, "error": str(e)}


@app.get("/api/admin/trigger/orchestrator/{token}")
def trigger_orchestrator(token: str, mode: str = "manual"):
    """
    mode = "manual" | "morning" | "line_watcher"
    Default "manual" if not specified. Pass via ?mode=morning for morning.
    """
    if token != os.environ.get("ADMIN_TOKEN"):
        raise HTTPException(status_code=403, detail="Forbidden")
    from . import orchestrator
    try:
        result = orchestrator.run(trigger=mode)
        return {"job": f"orchestrator:{mode}", "ok": True, "result": result}
    except Exception as e:
        return {"job": f"orchestrator:{mode}", "ok": False, "error": str(e)}


@app.get("/api/admin/trigger/grader/{token}")
def trigger_grader(token: str, date: Optional[str] = None):
    """
    date = "YYYY-MM-DD" optional. If omitted, grades yesterday.
    """
    if token != os.environ.get("ADMIN_TOKEN"):
        raise HTTPException(status_code=403, detail="Forbidden")
    from . import grader
    from datetime import date as date_cls
    target = None
    if date:
        try:
            target = date_cls.fromisoformat(date)
        except ValueError:
            raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")
    try:
        result = grader.grade_yesterday(target_date=target)
        return {"job": "grader", "ok": True, "result": result}
    except Exception as e:
        return {"job": "grader", "ok": False, "error": str(e)}


@app.get("/api/admin/wipe-prop-history/{token}")
def wipe_prop_history(token: str):
    """
    Wipe ALL pitcher prop edges + their grades + model_performance.
    Game total edges and their grades remain untouched.
    Use this to reset prop track record after a model change.
    """
    if token != os.environ.get("ADMIN_TOKEN"):
        raise HTTPException(status_code=403, detail="Forbidden")
    try:
        # Delete graded results for prop edges
        n_results = db.execute("""
            DELETE FROM edge_results
            WHERE edge_id IN (SELECT edge_id FROM edges WHERE kind = 'prop')
        """)
        # Delete the prop edges themselves
        n_edges = db.execute("DELETE FROM edges WHERE kind = 'prop'")
        # Reset model_performance (grader will recompute next run)
        n_perf = db.execute("DELETE FROM model_performance")
        return {
            "ok": True,
            "edge_results_deleted": n_results,
            "prop_edges_deleted": n_edges,
            "model_performance_deleted": n_perf
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/admin/scheduler-status/{token}")
def scheduler_status(token: str):
    """
    Quick info: was the worker service started, and what jobs are registered?
    Note: this runs in the API process, NOT the worker. It just confirms
    the scheduler module is importable. To check actual scheduler health,
    look at the worker service logs in Railway.
    """
    if token != os.environ.get("ADMIN_TOKEN"):
        raise HTTPException(status_code=403, detail="Forbidden")
    try:
        from . import scheduler
        sched = scheduler.build_scheduler()
        jobs = []
        for j in sched.get_jobs():
            jobs.append({
                "id": j.id,
                "name": j.name,
                "next_run_time": str(j.trigger.get_next_fire_time(None, datetime.now(timezone.utc))),
            })
        return {"ok": True, "jobs": jobs}
    except Exception as e:
        return {"ok": False, "error": str(e)}


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


# ============================================================================
# Retroactively zero out profit_units for prop edges
# (Props are tracked W/L only; we don't sum them into cumulative profit.)
# ============================================================================
@app.get("/api/admin/zero_prop_units/{token}")
def zero_prop_units(token: str):
    """One-shot cleanup: set profit_units = 0 for all already-graded prop
    edges. Their result (WIN/LOSS/PUSH) is unchanged."""
    _check_admin(token)
    from . import db

    before = db.fetchone("""
        SELECT COUNT(*) AS n,
               COALESCE(SUM(er.profit_units), 0)::float AS total_profit
        FROM edge_results er
        JOIN edges e ON e.edge_id = er.edge_id
        WHERE e.kind = 'prop' AND er.profit_units != 0
    """)
    n_before = int(before["n"] or 0)
    profit_zeroed = float(before["total_profit"] or 0)

    db.execute("""
        UPDATE edge_results er
        SET profit_units = 0
        FROM edges e
        WHERE er.edge_id = e.edge_id AND e.kind = 'prop'
    """)

    # Recompute rolling perf snapshots so cumulative recap drops the props
    db.execute("DELETE FROM model_performance")

    return {
        "rows_zeroed": n_before,
        "profit_units_removed": round(profit_zeroed, 2),
        "note": "model_performance cleared — next grader run will recompute rolling sums",
    }


# ============================================================================
# League-wide stats endpoints (Stats page)
# Read-only, no auth required, scoped to current season.
# ============================================================================
@app.get("/api/stats/pitchers")
def stats_pitchers():
    """All pitchers with Statcast data + contact metrics + splits."""
    from . import db
    from datetime import date as _date
    season = _date.today().year

    rows = db.fetchall("""
        SELECT mlb_id, last_first, season_year,
               pa, bip, ba, est_ba, slg, est_slg, woba, est_woba,
               era, xera, xfip, k_pct, bb9, fb_pct, hr_fb_rate,
               babip, gb_pct, ld_pct,
               avg_exit_velo, hard_hit_pct, barrel_pct, launch_angle_avg,
               days_rest, last_start_date::text AS last_start_date,
               refreshed_at::text AS refreshed_at
        FROM pitcher_xstats
        WHERE season_year = %s
        ORDER BY pa DESC NULLS LAST, est_woba ASC NULLS LAST
    """, (season,))

    # Tack on splits keyed by mlb_id -> {vsL: {...}, vsR: {...}, home: {...}, away: {...}}
    split_rows = db.fetchall("""
        SELECT mlb_id, split_key, pa, ip, era, whip,
               avg_against, obp_against, slg_against, ops_against, k_pct, bb_pct
        FROM pitcher_pitching_splits
        WHERE season_year = %s
    """, (season,))
    splits_by_id = {}
    for sr in split_rows:
        d = splits_by_id.setdefault(sr["mlb_id"], {})
        key = sr["split_key"]
        d[key] = {k: v for k, v in dict(sr).items() if k not in ("mlb_id", "split_key")}

    out = []
    for r in rows:
        d = dict(r)
        d["splits"] = splits_by_id.get(r["mlb_id"], {})
        out.append(d)

    return {"season": season, "n": len(out), "pitchers": out}


@app.get("/api/stats/hitters")
def stats_hitters():
    """All hitters with Statcast data for the current season, with vs-LHP/vs-RHP splits."""
    from . import db
    from datetime import date as _date
    season = _date.today().year

    rows = db.fetchall("""
        SELECT mlb_id, last_first, season_year,
               pa, ba, est_ba, slg, est_slg, woba, est_woba, l15_woba,
               refreshed_at::text AS refreshed_at
        FROM hitter_xstats
        WHERE season_year = %s
        ORDER BY pa DESC NULLS LAST, est_woba DESC NULLS LAST
    """, (season,))

    # Tack on platoon splits in one query
    splits = db.fetchall("""
        SELECT mlb_id, vs_hand, pa, est_woba
        FROM hitter_splits
        WHERE season_year = %s
    """, (season,))
    by_id = {}
    for s in splits:
        d = by_id.setdefault(s["mlb_id"], {})
        d[f"vs_{s['vs_hand']}_pa"] = s["pa"]
        d[f"vs_{s['vs_hand']}_woba"] = float(s["est_woba"]) if s["est_woba"] is not None else None

    out = []
    for r in rows:
        d = dict(r)
        d.update(by_id.get(r["mlb_id"], {}))
        out.append(d)

    return {"season": season, "n": len(out), "hitters": out}


@app.get("/api/stats/teams")
def stats_teams():
    """All teams with offensive + bullpen stats for current season.

    Note: 'team_xwoba' is the team's own hitting xwOBA (offensive strength)
    written by the team_offensive_xwoba refresh job. The 'woba'/'est_woba'
    columns in this table are unused; we don't return them.
    """
    from . import db
    from datetime import date as _date
    season = _date.today().year
    rows = db.fetchall("""
        SELECT team_code, season_year,
               pa,
               team_xwoba   AS est_woba,
               team_woba_l5 AS l5_woba,
               bullpen_era, bullpen_xera, bullpen_ip,
               bullpen_era_l7, bullpen_ip_l7,
               refreshed_at::text AS refreshed_at
        FROM team_xstats
        WHERE season_year = %s
        ORDER BY team_xwoba DESC NULLS LAST
    """, (season,))
    return {"season": season, "n": len(rows), "teams": [dict(r) for r in rows]}


@app.get("/api/admin/diag/savant_pitcher_csv/{token}")
def diag_savant_pitcher_csv(token: str):
    """Show what Savant's pitcher leaderboard CSV actually returns."""
    _check_admin(token)
    import requests, csv, io
    from datetime import date as _date
    from . import statcast_refresh

    year = _date.today().year
    url = statcast_refresh.SAVANT_EXIT_VELO_URL.format(year=year)
    try:
        r = requests.get(url, headers=statcast_refresh.SAVANT_HEADERS, timeout=30)
        r.raise_for_status()
    except Exception as e:
        return {"url": url, "error": str(e)}

    text = r.text
    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames or []
    rows = []
    for i, row in enumerate(reader):
        if i >= 3:
            break
        rows.append(dict(row))

    return {
        "url": url,
        "status": r.status_code,
        "content_length": len(text),
        "n_columns": len(headers),
        "headers": headers,
        "sample_rows": rows,
    }


@app.get("/api/admin/cleanup_dedup/{token}")
def admin_cleanup_dedup(token: str, date: str = None):
    """Retroactively run cross-run total/f5 dedup for a date (or all dates).

    For each game on the date that has both a flagged total and flagged f5
    edge, keep the larger |edge| and unflag the other. Mirrors the
    cross-run dedup the orchestrator now runs automatically.

    Use ?date=YYYY-MM-DD for a single date, or no params to fix every date
    with duplicates.
    """
    _check_admin(token)
    from . import orchestrator

    if date:
        n = orchestrator._persistent_cross_run_dedup(date)
        return {"date": date, "edges_unflagged": n}

    # No date supplied — find every date with duplicates
    dates = db.fetchall(
        """
        SELECT DISTINCT pr.run_date::text AS d
        FROM edges e
        JOIN projection_runs pr ON pr.run_id = e.run_id
        WHERE e.flagged = TRUE AND e.kind IN ('total','f5')
        GROUP BY pr.run_date, e.game_pk
        HAVING COUNT(DISTINCT e.kind) > 1
        """
    )
    summary = []
    for r in dates:
        d = r["d"]
        n = orchestrator._persistent_cross_run_dedup(d)
        summary.append({"date": d, "edges_unflagged": n})
    return {"dates_processed": len(summary), "details": summary}

