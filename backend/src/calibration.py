"""
calibration.py — hinge calibration of projected totals to actual results.

HINGE:  proj_cal = proj + lift * max(0, knot - proj)
Identity at/above `knot` (protects the accurate high-proj edge); lifts the
under-projected low end. `lift` is fit to zero the low-segment bias, scaled by
`shrink`. Total and F5 fit independently.

Phase 2 (applied in the orchestrator):
  - load()            -> current {knot, lift} per target from projection_calibration
  - apply_loaded(v,p) -> apply the loaded hinge (identity if no params)
  - refit_and_store() -> re-fit on RAW projections vs actuals, upsert the params

FEEDBACK-LOOP: refit reads proj_total_raw / proj_f5_raw (the uncalibrated stored
value). The orchestrator stores raw there and the calibrated value in proj_total.
"""
from datetime import date, timedelta
from statistics import mean

from . import db

SHRINK_SWEEP = [round(0.1 * i, 1) for i in range(11)]


def _low_lift_fit(pts, knot):
    low = [(p, a) for p, a in pts if p < knot]
    if not low:
        return 0.0
    bias_low = mean(p - a for p, a in low)
    mean_gap = mean(knot - p for p, a in low)
    return (-bias_low / mean_gap) if mean_gap > 0 else 0.0


def fit_hinge(pts, knot, shrink):
    n = len(pts)
    if n < 20:
        return {"ok": False, "n": n, "reason": "need >=20 graded games in window"}
    bias_all = mean(p - a for p, a in pts)
    mae_before = mean(abs(p - a) for p, a in pts)
    low = [(p, a) for p, a in pts if p < knot]
    bias_low = mean(p - a for p, a in low) if low else 0.0
    lift_fit = _low_lift_fit(pts, knot)
    lift = max(0.0, min(1.0, shrink * lift_fit))

    def c(p):
        return p + lift * max(0.0, knot - p)

    return {
        "ok": True, "n": n, "n_low": len(low), "knot": knot, "shrink": shrink,
        "lift_fit": round(lift_fit, 4), "lift": round(lift, 4),
        "bias_low_raw": round(bias_low, 3),
        "bias_before": round(bias_all, 3),
        "bias_after": round(mean(c(p) - a for p, a in pts), 3),
        "mae_before": round(mae_before, 3),
        "mae_after": round(mean(abs(c(p) - a) for p, a in pts), 3),
    }


def apply_hinge(value, knot, lift):
    """proj_cal = proj + lift*max(0, knot - proj). Never raises the high end."""
    if value is None:
        return None
    v = float(value)
    return round(v + lift * max(0.0, knot - v), 2)


def fetch_rows(days, proj_col="proj_total", proj_f5_col="proj_f5"):
    return db.fetchall(
        f"SELECT DISTINCT ON (gp.game_pk) gp.game_pk, "
        f"  pr.run_date::text AS run_date, "
        f"  gp.{proj_col} AS proj_total, gp.{proj_f5_col} AS proj_f5, "
        f"  (g.away_score + g.home_score) AS actual_total, "
        f"  (g.away_f5_runs + g.home_f5_runs) AS actual_f5 "
        f"FROM game_projections gp "
        f"JOIN projection_runs pr ON pr.run_id = gp.run_id "
        f"JOIN games g ON g.game_pk = gp.game_pk "
        f"WHERE g.away_score IS NOT NULL AND g.home_score IS NOT NULL "
        f"  AND gp.{proj_col} IS NOT NULL "
        f"  AND pr.run_date >= (CURRENT_DATE - %s::int) "
        f"ORDER BY gp.game_pk, gp.run_id DESC",
        (days,))


def fit(days=21, shrink=0.5, knot_total=8.5, knot_f5=5.0,
        proj_col="proj_total", proj_f5_col="proj_f5"):
    rows = fetch_rows(days, proj_col, proj_f5_col)
    tot = [(float(r["proj_total"]), float(r["actual_total"])) for r in rows
           if r.get("proj_total") is not None and r.get("actual_total") is not None]
    f5 = [(float(r["proj_f5"]), float(r["actual_f5"])) for r in rows
          if r.get("proj_f5") is not None and r.get("actual_f5") is not None]
    return {"window_days": days, "shrink": shrink, "n_rows": len(rows),
            "total": fit_hinge(tot, knot_total, shrink),
            "f5": fit_hinge(f5, knot_f5, shrink)}


# --- walk-forward shrink selection -----------------------------------------

def _split(rows, proj_key, act_key, cutoff_date):
    train, test = [], []
    for r in rows:
        if r.get(proj_key) is None or r.get(act_key) is None:
            continue
        pt = (float(r[proj_key]), float(r[act_key]))
        (test if r["run_date"] >= cutoff_date else train).append(pt)
    return train, test


def _sweep(train, test, knot, sweep):
    if len(train) < 20 or len(test) < 8:
        return {"ok": False, "n_train": len(train), "n_test": len(test),
                "reason": "need >=20 train and >=8 test games"}
    lf = _low_lift_fit(train, knot)
    results = []
    for s in sweep:
        lift = max(0.0, min(1.0, s * lf))
        tb = mean((p + lift * max(0.0, knot - p)) - a for p, a in test)
        tm = mean(abs((p + lift * max(0.0, knot - p)) - a) for p, a in test)
        results.append({"shrink": s, "lift": round(lift, 4),
                        "test_bias": round(tb, 3), "test_mae": round(tm, 3)})
    best = min(results, key=lambda r: abs(r["test_bias"]))
    return {"ok": True, "n_train": len(train), "n_test": len(test), "knot": knot,
            "train_lift_fit": round(lf, 4),
            "test_bias_uncalibrated": round(mean(p - a for p, a in test), 3),
            "best_shrink": best["shrink"], "best_test_bias": best["test_bias"],
            "sweep": results}


def validate(train_days=21, test_days=7, knot_total=8.5, knot_f5=5.0, sweep=None):
    sweep = sweep or SHRINK_SWEEP
    rows = fetch_rows(train_days + test_days)
    cutoff = (date.today() - timedelta(days=test_days)).isoformat()
    tot_tr, tot_te = _split(rows, "proj_total", "actual_total", cutoff)
    f5_tr, f5_te = _split(rows, "proj_f5", "actual_f5", cutoff)
    return {"train_days": train_days, "test_days": test_days,
            "cutoff_date": cutoff, "n_rows": len(rows),
            "total": _sweep(tot_tr, tot_te, knot_total, sweep),
            "f5": _sweep(f5_tr, f5_te, knot_f5, sweep)}


# --- Phase 2: load / apply / persist ---------------------------------------

def load():
    """{'total': {'knot','lift'}, 'f5': {...}} from the calibration table.
    Returns {} (-> identity) if the table is missing, so the orchestrator never
    crashes when calibration isn't set up yet."""
    try:
        rows = db.fetchall("SELECT target, knot, lift FROM projection_calibration")
    except Exception:
        return {}
    return {r["target"]: {"knot": float(r["knot"]), "lift": float(r["lift"])}
            for r in rows}


def apply_loaded(value, params):
    """Apply a loaded hinge; identity if params is falsy."""
    if not params or value is None:
        return value
    return apply_hinge(value, params["knot"], params["lift"])


def refit_and_store(days=21, shrink=1.0, knot_total=8.5, knot_f5=5.0):
    """Re-fit the hinge on RAW projections vs actuals and upsert the params."""
    res = fit(days=days, shrink=shrink, knot_total=knot_total, knot_f5=knot_f5,
              proj_col="proj_total_raw", proj_f5_col="proj_f5_raw")
    out = {}
    for tgt in ("total", "f5"):
        r = res[tgt]
        if not r.get("ok"):
            out[tgt] = {"stored": False, **r}
            continue
        db.execute(
            "INSERT INTO projection_calibration "
            "(target,knot,lift,shrink,window_days,n,bias_before,bias_after,fit_at) "
            "VALUES (%(target)s,%(knot)s,%(lift)s,%(shrink)s,%(window_days)s,"
            "        %(n)s,%(bias_before)s,%(bias_after)s,now()) "
            "ON CONFLICT (target) DO UPDATE SET "
            "  knot=EXCLUDED.knot, lift=EXCLUDED.lift, shrink=EXCLUDED.shrink, "
            "  window_days=EXCLUDED.window_days, n=EXCLUDED.n, "
            "  bias_before=EXCLUDED.bias_before, bias_after=EXCLUDED.bias_after, "
            "  fit_at=now()",
            {"target": tgt, "knot": r["knot"], "lift": r["lift"], "shrink": shrink,
             "window_days": days, "n": r["n"],
             "bias_before": r["bias_before"], "bias_after": r["bias_after"]})
        out[tgt] = {"stored": True, "knot": r["knot"], "lift": r["lift"],
                    "n": r["n"], "bias_before": r["bias_before"],
                    "bias_after": r["bias_after"]}
    return out


# ---------------------------------------------------------------------------
# Phase 2: persisted params - load() for the orchestrator, refit_and_store()
# for the rolling update. Fits on the RAW projection columns (see feedback note).
# ---------------------------------------------------------------------------

_IDENTITY = {"total": {"knot": 8.5, "lift": 0.0},
             "f5": {"knot": 5.0, "lift": 0.0}}


def load():
    """Current calibration params: {'total': {'knot','lift'}, 'f5': {...}}.
    Returns identity (lift 0 = no change) if the table is missing or unseeded,
    so the projection path never breaks."""
    out = {k: dict(v) for k, v in _IDENTITY.items()}
    try:
        rows = db.fetchall("SELECT target, knot, lift FROM projection_calibration")
    except Exception:
        return out
    for r in rows:
        t = r["target"]
        if t in out:
            out[t] = {"knot": float(r["knot"]), "lift": float(r["lift"])}
    return out


def apply_loaded(value, params):
    """Apply a loaded {'knot','lift'} set to one projection. Safe on None."""
    if value is None or not params:
        return value
    return apply_hinge(value, params["knot"], params["lift"])


def refit_and_store(days=21, shrink=1.0, knot_total=8.5, knot_f5=5.0):
    """Fit the hinge on the RAW projection columns over the window and upsert the
    resulting lift into projection_calibration. Call after grading so the window
    includes fresh actuals."""
    rows = fetch_rows(days, proj_col="proj_total_raw", proj_f5_col="proj_f5_raw")
    tot = [(float(r["proj_total"]), float(r["actual_total"])) for r in rows
           if r.get("proj_total") is not None and r.get("actual_total") is not None]
    f5 = [(float(r["proj_f5"]), float(r["actual_f5"])) for r in rows
          if r.get("proj_f5") is not None and r.get("actual_f5") is not None]
    out = {}
    for target, pts, knot in (("total", tot, knot_total), ("f5", f5, knot_f5)):
        res = fit_hinge(pts, knot, shrink)
        if not res.get("ok"):
            out[target] = res
            continue
        db.execute(
            "INSERT INTO projection_calibration "
            "(target, knot, lift, shrink, window_days, n, bias_before, bias_after, fit_at) "
            "VALUES (%(target)s,%(knot)s,%(lift)s,%(shrink)s,%(window_days)s,%(n)s,"
            "%(bias_before)s,%(bias_after)s, now()) "
            "ON CONFLICT (target) DO UPDATE SET "
            "knot=EXCLUDED.knot, lift=EXCLUDED.lift, shrink=EXCLUDED.shrink, "
            "window_days=EXCLUDED.window_days, n=EXCLUDED.n, "
            "bias_before=EXCLUDED.bias_before, bias_after=EXCLUDED.bias_after, fit_at=now()",
            {"target": target, "knot": knot, "lift": res["lift"], "shrink": shrink,
             "window_days": days, "n": res["n"],
             "bias_before": res["bias_before"], "bias_after": res["bias_after"]})
        out[target] = res
    return {"days": days, "shrink": shrink, "stored": out}
