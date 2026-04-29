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


@app.get("/api/admin/dedupe-grades/{token}")
def dedupe_grades(token: str):
    if token != os.environ.get("ADMIN_TOKEN"):
        raise HTTPException(status_code=403, detail="Forbidden")
    before = db.fetchone("SELECT COUNT(*) AS c FROM edge_results")["c"]
    db.execute("DELETE FROM model_performance")
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
