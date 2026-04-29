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
        
        
@app.get("/api/performance/by-date")
def performance_by_date():
    """
    Returns daily performance breakdown for the Track Record dashboard.
    Schema:
      [
        {
          "run_date": "2026-04-28",
          "summary": {"wins": 41, "losses": 23, "pushes": 1, "profit_units": 15.81},
          "by_category": [
            {"kind": "total", "category": "Total", "wins": 9, "losses": 3, "pushes": 0, "profit_units": 5.4},
            {"kind": "prop", "category": "K", "wins": 8, "losses": 5, "pushes": 0, "profit_units": 2.1},
            ...
          ]
        },
        ...
      ]
    Most recent date first.
    """
    rows = db.fetchall("""
        SELECT
          pr.run_date,
          e.kind,
          e.category,
          COUNT(*) FILTER (WHERE er.result = 'WIN')   AS wins,
          COUNT(*) FILTER (WHERE er.result = 'LOSS')  AS losses,
          COUNT(*) FILTER (WHERE er.result = 'PUSH')  AS pushes,
          COALESCE(SUM(er.profit_units), 0)::float    AS profit_units
        FROM edge_results er
        JOIN edges e ON e.edge_id = er.edge_id
        JOIN projection_runs pr ON pr.run_id = e.run_id
        WHERE e.flagged = TRUE
        GROUP BY pr.run_date, e.kind, e.category
        ORDER BY pr.run_date DESC, e.kind, e.category
    """)

    by_date = {}
    for r in rows:
        d = str(r["run_date"])
        if d not in by_date:
            by_date[d] = {
                "run_date": d,
                "summary": {"wins": 0, "losses": 0, "pushes": 0, "profit_units": 0.0},
                "by_category": []
            }
        wins = int(r["wins"] or 0)
        losses = int(r["losses"] or 0)
        pushes = int(r["pushes"] or 0)
        profit = float(r["profit_units"] or 0)

        by_date[d]["summary"]["wins"] += wins
        by_date[d]["summary"]["losses"] += losses
        by_date[d]["summary"]["pushes"] += pushes
        by_date[d]["summary"]["profit_units"] = round(
            by_date[d]["summary"]["profit_units"] + profit, 2
        )

        by_date[d]["by_category"].append({
            "kind": r["kind"],
            "category": r["category"],
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "profit_units": round(profit, 2),
        })

    return list(by_date.values())


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
