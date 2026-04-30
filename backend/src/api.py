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
        SELECT g.*, gp.proj_total, gp.proj_f5, gp.edge_total, gp.lean
        FROM games g
        LEFT JOIN game_projections gp ON gp.game_pk = g.game_pk AND gp.run_id = %s
        WHERE g.game_date = %s
        ORDER BY g.game_time_et
        """,
        (run_id, slate_date),
    )
    edges = db.fetchall(
        "SELECT * FROM edges WHERE run_id = %s ORDER BY ABS(edge) DESC",
        (run_id,),
    )
    projs = db.fetchall(
        "SELECT * FROM pitcher_projections WHERE run_id = %s",
        (run_id,),
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
          AND e.lean IN ('OVER','UNDER')
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
        bk = (r["kind"], r["category"], r["lean"])
        b = by_date[d]["buckets"].setdefault(bk, {
            "kind": r["kind"],
            "category": r["category"],
            "lean": r["lean"],
            "wins": 0, "losses": 0, "pushes": 0,
            "profit_units": 0.0,
            "plays": [],
        })
        if result == "WIN":  b["wins"] += 1
        elif result == "LOSS": b["losses"] += 1
        elif result == "PUSH": b["pushes"] += 1
        b["profit_units"] = round(b["profit_units"] + profit, 2)

        # Compose play row. For totals use matchup, for props use pitcher name.
        is_total = r["kind"] == "total"
        subject = (
            f"{r['team_code'] or '?'} @ {r['opp_team_code'] or '?'}"
            if is_total
            else (r["pitcher_name"] or "?")
        )
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
          COUNT(*) FILTER (WHERE er.result = 'WIN')   AS wins,
          COUNT(*) FILTER (WHERE er.result = 'LOSS')  AS losses,
          COUNT(*) FILTER (WHERE er.result = 'PUSH')  AS pushes,
          COALESCE(SUM(er.profit_units), 0)::float    AS profit_units
        FROM edge_results er
        JOIN edges e ON e.edge_id = er.edge_id
        WHERE e.flagged = TRUE
        GROUP BY e.kind, e.category
        ORDER BY e.kind, e.category
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
            "category": r["category"],
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "profit_units": round(profit, 2),
        })

    return {"overall": overall, "by_category": by_category}
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
                "next_run_time": str(j.next_run_time) if j.next_run_time else None,
            })
        return {"ok": True, "jobs": jobs}
    except Exception as e:
        return {"ok": False, "error": str(e)}
