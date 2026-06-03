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

`validate()` picks `shrink` honestly: fit the lift on an older train window,
apply to a held-out recent test window, sweep shrink, return the value with the
lowest OUT-OF-SAMPLE bias.

FEEDBACK-LOOP NOTE: fits read the stored projection column, which TODAY is the
raw model output. When you wire application into the orchestrator, store the
calibrated value in proj_total but keep the RAW value in a new column and fit on
THAT (pass proj_col=...), or the calibration converges to identity.
"""
from datetime import date, timedelta
from statistics import mean

from . import db

SHRINK_SWEEP = [round(0.1 * i, 1) for i in range(11)]   # 0.0 .. 1.0


def _low_lift_fit(pts, knot):
    """Raw lift that would zero the low-segment (<knot) bias. shrink-independent."""
    low = [(p, a) for p, a in pts if p < knot]
    if not low:
        return 0.0
    bias_low = mean(p - a for p, a in low)
    mean_gap = mean(knot - p for p, a in low)
    return (-bias_low / mean_gap) if mean_gap > 0 else 0.0


def fit_hinge(pts, knot, shrink):
    """pts: list of (proj, actual). Returns the fitted hinge + before/after."""
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
    return {
        "window_days": days, "shrink": shrink, "n_rows": len(rows),
        "total": fit_hinge(tot, knot_total, shrink),
        "f5": fit_hinge(f5, knot_f5, shrink),
    }


# ---------------------------------------------------------------------------
# Walk-forward shrink selection
# ---------------------------------------------------------------------------

def _split(rows, proj_key, act_key, cutoff_date):
    """(train_pts, test_pts); test = games whose latest run_date >= cutoff."""
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
    return {
        "ok": True, "n_train": len(train), "n_test": len(test), "knot": knot,
        "train_lift_fit": round(lf, 4),
        "test_bias_uncalibrated": round(mean(p - a for p, a in test), 3),
        "best_shrink": best["shrink"], "best_test_bias": best["test_bias"],
        "sweep": results,
    }


def validate(train_days=21, test_days=7, knot_total=8.5, knot_f5=5.0, sweep=None):
    sweep = sweep or SHRINK_SWEEP
    rows = fetch_rows(train_days + test_days)
    cutoff = (date.today() - timedelta(days=test_days)).isoformat()
    tot_tr, tot_te = _split(rows, "proj_total", "actual_total", cutoff)
    f5_tr, f5_te = _split(rows, "proj_f5", "actual_f5", cutoff)
    return {
        "train_days": train_days, "test_days": test_days,
        "cutoff_date": cutoff, "n_rows": len(rows),
        "total": _sweep(tot_tr, tot_te, knot_total, sweep),
        "f5": _sweep(f5_tr, f5_te, knot_f5, sweep),
        "note": "Hinge lift fit on the older train_days, applied to the held-out "
                "last test_days. best_shrink minimizes |out-of-sample bias|. "
                "best at 0.0 => calibration overfits, don't apply much; best at "
                "1.0 => persistent defect, the test window wants full lift.",
    }
