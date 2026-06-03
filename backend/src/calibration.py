"""
calibration.py — hinge calibration of projected totals to actual results.

Diagnosis behind this: the model is accurate (and profitable on OU) when it
projects HIGH totals, but under-projects badly when it projects LOW (good-
pitcher games still score in this environment). A flat offset/multiplier is the
wrong tool - it would lift the accurate high end too and damage the real edge
that lives there. So this uses a HINGE:

    proj_cal = proj + lift * max(0, knot - proj)

Identity at/above `knot` (leaves the accurate high-proj edge untouched); lifts
projections below the knot toward it to remove their under-bias. `lift` is fit
to zero the low-segment bias, then scaled by `shrink` (0 = no correction,
1 = full) so a short hot/cold stretch isn't permanently baked in. Total and F5
are fit independently.

FEEDBACK-LOOP NOTE: fits read the stored projection column, which TODAY is the
raw model output. When you wire application into the orchestrator, store the
calibrated value in proj_total but keep the RAW value in a new column and fit on
THAT (pass proj_col=...), or the calibration converges to identity and stops
correcting.
"""
from statistics import mean

from . import db


def fit_hinge(pts, knot, shrink):
    """pts: list of (proj, actual). Returns the fitted hinge + before/after."""
    n = len(pts)
    if n < 20:
        return {"ok": False, "n": n, "reason": "need >=20 graded games in window"}
    bias_all = mean(p - a for p, a in pts)
    mae_before = mean(abs(p - a) for p, a in pts)
    low = [(p, a) for p, a in pts if p < knot]
    if low:
        bias_low = mean(p - a for p, a in low)        # negative when under-projecting
        mean_gap = mean(knot - p for p, a in low)     # positive
        lift_fit = (-bias_low / mean_gap) if mean_gap > 0 else 0.0
    else:
        bias_low = 0.0
        lift_fit = 0.0
    lift = max(0.0, min(1.0, shrink * lift_fit))

    def c(p):
        return p + lift * max(0.0, knot - p)

    return {
        "ok": True, "n": n, "n_low": len(low),
        "knot": knot, "shrink": shrink,
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
    """Latest projection per graded game over the window. proj_col is the column
    to fit on - keep it pointed at the RAW projection once application lands."""
    return db.fetchall(
        f"SELECT DISTINCT ON (gp.game_pk) gp.game_pk, "
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
    return {
        "window_days": days, "shrink": shrink, "n_rows": len(rows),
        "total": fit_hinge(tot, knot_total, shrink),
        "f5": fit_hinge(f5, knot_f5, shrink),
    }
