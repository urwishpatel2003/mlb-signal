"""
calibrate_f5.py — fit F5_CALIB and diagnose F5_SHAPE against graded F5 actuals.

Drop in backend/src/ and run from repo root:
    python -m backend.src.calibrate_f5 --days 30

Reads stored F5 projections (game_projections.proj_f5) and realized F5 totals
(games.away_f5_runs + home_f5_runs) for graded games, then reports:
  - signed bias (model F5 - actual F5)          -> overall over/under tendency
  - suggested F5_CALIB = mean(actual)/mean(proj) -> the constant to set
  - F5 O/U record of the model lean vs the market F5 line
  - signed bias bucketed by avg starter IP       -> tells you if the bias is
    IP-conditional, which is the only thing that justifies moving F5_SHAPE off 1.0

TWO-PHASE USE (important):
  proj_f5 reflects whatever model produced it. Run this NOW to baseline the
  *current* (old) F5 model, then deploy the v2 formula with F5_CALIB=F5_SHAPE=1.0,
  let ~2-3 weeks of new games grade, and run it AGAIN to fit CALIB on the v2
  outputs. The structural fixes (full-inning coverage + lineup tilt) are correct
  independent of calibration, so they ship at 1.0.

ASSUMPTION: db.fetchall(sql, params) -> list[dict]. If your db helper uses a
different name (db.query / db.fetch_all / ...), change the two calls in _rows().
"""
from __future__ import annotations
import argparse
from statistics import mean

from . import db


def _rows(sql, params):
    # Single point to adjust if your db layer names this differently.
    return db.fetchall(sql, params)


def fetch_f5_results(days: int):
    sql = """
        SELECT gp.game_pk,
               gp.proj_f5,
               gp.market_f5_total,
               gp.lean_f5,
               (g.away_f5_runs + g.home_f5_runs) AS actual_f5
        FROM game_projections gp
        JOIN projection_runs pr ON pr.run_id = gp.run_id
        JOIN games g            ON g.game_pk = gp.game_pk
        WHERE g.away_f5_runs IS NOT NULL
          AND g.home_f5_runs IS NOT NULL
          AND gp.proj_f5     IS NOT NULL
          AND pr.run_date >= (CURRENT_DATE - %s::int)
    """
    return _rows(sql, (days,))


def fetch_avg_starter_ip(days: int):
    """Average projected starter IP per game — for IP-bucketed bias diagnosis."""
    sql = """
        SELECT pp.game_pk, AVG(pp.ip) AS avg_ip
        FROM pitcher_projections pp
        JOIN projection_runs pr ON pr.run_id = pp.run_id
        WHERE pr.run_date >= (CURRENT_DATE - %s::int)
        GROUP BY pp.game_pk
    """
    out = {}
    for r in _rows(sql, (days,)):
        if r.get("avg_ip") is not None:
            out[r["game_pk"]] = float(r["avg_ip"])
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30, help="lookback window in days")
    args = ap.parse_args()

    rows = fetch_f5_results(args.days)
    if not rows:
        print("No graded F5 games in window. Widen --days or verify F5 grading "
              "is writing away_f5_runs/home_f5_runs.")
        return

    proj   = [float(r["proj_f5"])   for r in rows]
    actual = [float(r["actual_f5"]) for r in rows]
    n = len(rows)

    bias = mean(p - a for p, a in zip(proj, actual))
    mae  = mean(abs(p - a) for p, a in zip(proj, actual))
    suggested_calib = (mean(actual) / mean(proj)) if mean(proj) else 1.0

    # O/U record vs the market F5 line
    ou_w = ou_l = ou_push = graded = 0
    for r in rows:
        mkt = r.get("market_f5_total")
        lean = (r.get("lean_f5") or "").upper()
        if mkt is None or lean not in ("OVER", "UNDER"):
            continue
        a = float(r["actual_f5"]); mkt = float(mkt)
        if a == mkt:
            ou_push += 1
            continue
        graded += 1
        hit = (a > mkt and lean == "OVER") or (a < mkt and lean == "UNDER")
        ou_w += int(hit); ou_l += int(not hit)

    print(f"F5 calibration — last {args.days} days, {n} graded games")
    print(f"  mean proj F5    : {mean(proj):.3f}")
    print(f"  mean actual F5  : {mean(actual):.3f}")
    print(f"  signed bias     : {bias:+.3f}   (model minus actual; + = model runs hot)")
    print(f"  MAE             : {mae:.3f}")
    print(f"  suggested CALIB : {suggested_calib:.4f}   <- set F5_CALIB to this")
    if graded:
        pct = 100 * ou_w / graded
        print(f"  F5 O/U record   : {ou_w}-{ou_l}  ({pct:.1f}%)   pushes={ou_push}"
              f"   [breakeven at -110 = 52.4%]")

    # IP-bucketed bias -> F5_SHAPE diagnosis
    ip_by_game = fetch_avg_starter_ip(args.days)
    buckets = {"short  (<5.0)": [], "mid (5.0-5.8)": [], "long  (>=5.8)": []}
    for r in rows:
        ip = ip_by_game.get(r["game_pk"])
        if ip is None:
            continue
        err = float(r["proj_f5"]) - float(r["actual_f5"])
        if ip < 5.0:
            buckets["short  (<5.0)"].append(err)
        elif ip < 5.8:
            buckets["mid (5.0-5.8)"].append(err)
        else:
            buckets["long  (>=5.8)"].append(err)

    print("\n  signed bias by avg starter IP (informs F5_SHAPE):")
    for label, errs in buckets.items():
        if errs:
            print(f"    {label:<15} n={len(errs):<4} bias={mean(errs):+.3f}")
    print("\n  Read it like this: apply CALIB first. If the residual bias is roughly")
    print("  flat across the three IP buckets, leave F5_SHAPE = 1.0. If long-IP")
    print("  games stay biased hot while short-IP run cold (or vice versa), that's")
    print("  the early-inning/TTO signal -> lower F5_SHAPE if long-IP runs hot.")


if __name__ == "__main__":
    main()
