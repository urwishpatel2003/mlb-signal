"""
Run from repo root: python backfill_f5_runs.py --from 2026-04-01 [--to 2026-05-16]

Backfills F5 runs (away_f5_runs / home_f5_runs) on the games table for past
dates by re-fetching the MLB live feed and walking linescore.innings[:5].

Then re-runs the grader for each date so previously-skipped F5 edges get
graded into edge_results.

Prereqs:
  - migration 0006_f5_actuals.sql applied
  - patch_grader_f5.py applied (so grade_box_score writes the F5 columns
    and grade_yesterday picks them up)

This is idempotent: it skips games that already have F5 columns populated,
and the grader skips edges that already have an edge_results row.
"""
from __future__ import annotations
import argparse
import logging
import sys
from datetime import date, timedelta

# Import from the backend package
sys.path.insert(0, "backend")
from src import db, grader, mlb_api  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("backfill_f5")


def backfill_f5_for_date(target: date, force: bool = False) -> dict:
    """Re-fetch live feeds and populate F5 columns for all games on target date."""
    games = db.fetchall(
        "SELECT game_pk, status, away_f5_runs, home_f5_runs FROM games WHERE game_date=%s",
        (target,),
    )
    touched = skipped = failed = 0
    for g in games:
        if g["status"] in ("Postponed", "Cancelled"):
            skipped += 1
            continue
        if not force and g["away_f5_runs"] is not None and g["home_f5_runs"] is not None:
            skipped += 1
            continue
        try:
            # grade_box_score now writes F5 columns as a side effect (post-patch)
            grader.grade_box_score(g["game_pk"])
            touched += 1
        except Exception as e:
            log.warning("Failed game %s: %s", g["game_pk"], e)
            failed += 1
    log.info("  %s: touched=%d skipped=%d failed=%d", target, touched, skipped, failed)
    return {"touched": touched, "skipped": skipped, "failed": failed}


def regrade_date(target: date) -> dict:
    """Re-run grader for a date — picks up any F5 edges now that actuals exist."""
    return grader.grade_yesterday(target)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="start", required=True, help="YYYY-MM-DD start date")
    ap.add_argument("--to", dest="end", default=None, help="YYYY-MM-DD end date (default: yesterday)")
    ap.add_argument("--force", action="store_true",
                    help="Re-fetch even if F5 columns are already populated")
    ap.add_argument("--no-regrade", action="store_true",
                    help="Only populate F5 columns; do not re-run grader")
    args = ap.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if args.end else (date.today() - timedelta(days=1))
    if end < start:
        log.error("--to must be >= --from")
        sys.exit(1)

    log.info("Backfilling F5 from %s to %s (force=%s, regrade=%s)",
             start, end, args.force, not args.no_regrade)

    totals = {"touched": 0, "skipped": 0, "failed": 0}
    d = start
    while d <= end:
        r = backfill_f5_for_date(d, force=args.force)
        for k in totals:
            totals[k] += r[k]
        if not args.no_regrade:
            try:
                m = regrade_date(d)
                log.info("    regrade %s: W%s L%s P%s profit=%s",
                         d, m.get("wins"), m.get("losses"),
                         m.get("pushes"), m.get("profit_units"))
            except Exception as e:
                log.warning("    regrade %s failed: %s", d, e)
        d += timedelta(days=1)

    log.info("DONE — touched=%d skipped=%d failed=%d",
             totals["touched"], totals["skipped"], totals["failed"])


if __name__ == "__main__":
    main()
