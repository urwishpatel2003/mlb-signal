"""
Nightly grader.

Cron entry point. Runs at ~04:00 ET each morning to grade yesterday's slate:

  1. For each game with status='Final' that hasn't been graded yet:
     - Fetch the box score from MLB Stats API
     - Persist pitcher_actuals (IP, H, ER, BB, K)
  2. For each edge from yesterday's projection_runs:
     - Look up actual_value (game total, or pitcher prop)
     - Compute result (WIN/LOSS/PUSH)
     - Compute profit_units assuming -110 standard juice
     - Insert into edge_results
  3. Compute rolling performance metrics for windows: 7d, 14d, 30d, all-time
  4. Insert into model_performance
  5. Push ntfy summary

Edge cases handled:
  - Postponed games: no actuals, no grading
  - Games still in progress at grading time: skipped, retried next run
  - Pitchers who didn't appear (rain delay  next-day starter): skipped
"""
from __future__ import annotations
import logging
from datetime import date, timedelta
from typing import Optional

from . import db, mlb_api, ntfy

log = logging.getLogger(__name__)


def grade_box_score(game_pk: int) -> dict:
    """Pull box score for one game, persist pitcher_actuals."""
    box = mlb_api.get_box_score(game_pk)
    lines = mlb_api.extract_pitcher_lines(box)
    if not lines:
        return {"game_pk": game_pk, "n_pitchers": 0}

    # Also pull final score for the games table
    linescore = (box.get("liveData") or {}).get("linescore") or {}
    teams = linescore.get("teams") or {}
    away_runs = (teams.get("away") or {}).get("runs")
    home_runs = (teams.get("home") or {}).get("runs")
    if away_runs is not None and home_runs is not None:
        db.execute(
            """
            UPDATE games SET away_score=%s, home_score=%s, status='Final',
                              refreshed_at=now()
            WHERE game_pk=%s
            """,
            (away_runs, home_runs, game_pk),
        )

    # Persist pitcher lines
    rows = []
    for mlb_id, line in lines.items():
        rows.append({
            "game_pk": game_pk, "mlb_id": mlb_id,
            "last_first": line.get("name", ""),
            "ip": line["ip"], "outs": line["outs"],
            "h": line["h"], "er": line["er"],
            "bb": line["bb"], "k": line["k"],
            "pitches": line.get("pitches"), "bf": line.get("bf"),
        })
    if rows:
        sql = """
            INSERT INTO pitcher_actuals
              (game_pk, mlb_id, last_first, ip, outs, h, er, bb, k, pitches, bf, refreshed_at)
            VALUES
              (%(game_pk)s, %(mlb_id)s, %(last_first)s, %(ip)s, %(outs)s,
               %(h)s, %(er)s, %(bb)s, %(k)s, %(pitches)s, %(bf)s, now())
            ON CONFLICT (game_pk, mlb_id) DO UPDATE SET
              ip = EXCLUDED.ip, outs = EXCLUDED.outs,
              h = EXCLUDED.h, er = EXCLUDED.er,
              bb = EXCLUDED.bb, k = EXCLUDED.k,
              pitches = EXCLUDED.pitches, bf = EXCLUDED.bf,
              refreshed_at = now();
        """
        db.execute_many(sql, rows)

    return {"game_pk": game_pk, "n_pitchers": len(rows),
            "away_runs": away_runs, "home_runs": home_runs}


def actual_value_for_edge(e: dict, game_row: dict) -> Optional[float]:
    """Return the realized value that the edge was placed on."""
    if e["kind"] == "total":
        ar = game_row.get("away_score")
        hr = game_row.get("home_score")
        if ar is None or hr is None:
            return None
        return ar + hr

    # Prop: look up pitcher_actuals
    actual = db.fetchone(
        "SELECT ip, outs, h, er, bb, k FROM pitcher_actuals WHERE game_pk=%s AND mlb_id=%s",
        (e["game_pk"], e["pitcher_mlb_id"]),
    )
    if not actual:
        return None
    cat = e["category"]
    if cat == "K": return float(actual["k"])
    if cat == "Hits": return float(actual["h"])
    if cat == "ER": return float(actual["er"])
    if cat == "Outs": return float(actual["outs"])
    if cat == "BB": return float(actual["bb"])
    return None


def grade_edge(e: dict, actual: float, juice: int = -110) -> dict:
    """Apply over/under decision logic and compute profit/loss in units."""
    line = float(e["line"])
    lean = e["lean"]
    if lean == "OVER":
        if actual > line: result, profit = "WIN", _profit_for(juice)
        elif actual < line: result, profit = "LOSS", -1.0
        else: result, profit = "PUSH", 0.0
    elif lean == "UNDER":
        if actual < line: result, profit = "WIN", _profit_for(juice)
        elif actual > line: result, profit = "LOSS", -1.0
        else: result, profit = "PUSH", 0.0
    else:
        result, profit = "NO_ACTION", 0.0
    return {"result": result, "profit_units": round(profit, 4), "actual": actual}


def _profit_for(juice: int) -> float:
    """Profit (in units, where 1 unit = $100 risked) at given juice."""
    if juice < 0:
        return 100.0 / abs(juice)
    return juice / 100.0


def grade_yesterday(target_date: Optional[date] = None) -> dict:
    """Top-level entry: grade all games from `target_date`."""
    target = target_date or (date.today() - timedelta(days=1))
    job_id = db.log_job_start("grader")
    metrics: dict = {"target_date": target.isoformat()}
    try:
        # Pull all games from target_date
        games = db.fetchall(
            "SELECT * FROM games WHERE game_date = %s",
            (target,),
        )
        graded_games = 0
        for g in games:
            if g["status"] in ("Postponed", "Cancelled"):
                continue
            if g["away_score"] is None or g["home_score"] is None:
                # Force a re-pull from MLB API to get final scores
                try:
                    grade_box_score(g["game_pk"])
                    graded_games += 1
                except Exception as e:
                    log.warning("Failed to grade box %s: %s", g["game_pk"], e)
                    continue
            else:
                # Already have scores, but make sure pitcher_actuals are persisted
                existing = db.fetchone(
                    "SELECT 1 FROM pitcher_actuals WHERE game_pk = %s LIMIT 1",
                    (g["game_pk"],),
                )
                if not existing:
                    try:
                        grade_box_score(g["game_pk"])
                    except Exception as e:
                        log.warning("Failed to grade box %s: %s", g["game_pk"], e)
                graded_games += 1

        metrics["games_graded"] = graded_games

        # Now grade every edge from yesterday's runs
        edges = db.fetchall(
            """
            SELECT e.*, g.away_score, g.home_score, g.status
            FROM edges e
            JOIN games g ON g.game_pk = e.game_pk
            JOIN projection_runs pr ON pr.run_id = e.run_id
            WHERE pr.run_date = %s AND e.flagged = TRUE
            """,
            (target,),
        )

        wins = losses = pushes = 0
        total_profit = 0.0
        for e in edges:
            # Skip if already graded
            existing = db.fetchone(
                "SELECT 1 FROM edge_results WHERE edge_id = %s",
                (e["edge_id"],),
            )
            if existing:
                continue
            actual = actual_value_for_edge(e, e)
            if actual is None:
                continue
            graded = grade_edge(e, actual)
            db.execute(
                """
                INSERT INTO edge_results (edge_id, actual_value, result, profit_units)
                VALUES (%s, %s, %s, %s)
                """,
                (e["edge_id"], actual, graded["result"], graded["profit_units"]),
            )
            if graded["result"] == "WIN": wins += 1
            elif graded["result"] == "LOSS": losses += 1
            elif graded["result"] == "PUSH": pushes += 1
            total_profit += graded["profit_units"]

        metrics["wins"] = wins
        metrics["losses"] = losses
        metrics["pushes"] = pushes
        metrics["profit_units"] = round(total_profit, 2)
        metrics["flagged_plays"] = wins + losses + pushes
        metrics["hit_rate"] = wins / (wins + losses) if (wins + losses) > 0 else None

        # Update rolling performance
        update_rolling_performance(target)

        # Send summary
        ntfy.send_grader_summary(target.isoformat(), metrics)

        db.log_job_finish(job_id, "success", payload=metrics)
        return metrics
    except Exception as e:
        log.exception("Grader failed")
        db.log_job_finish(job_id, "failure", str(e), metrics)
        ntfy.send_failure("grader", str(e))
        raise


def update_rolling_performance(snapshot_date: date,
                                windows: tuple[int, ...] = (7, 14, 30)) -> None:
    """Compute & insert rolling performance metrics for each window."""
    for w in windows:
        start = snapshot_date - timedelta(days=w)
        row = db.fetchone(
            """
            SELECT
              COUNT(*) FILTER (WHERE er.result = 'WIN') AS wins,
              COUNT(*) FILTER (WHERE er.result = 'LOSS') AS losses,
              COUNT(*) FILTER (WHERE er.result = 'PUSH') AS pushes,
              COALESCE(SUM(er.profit_units), 0) AS profit
            FROM edge_results er
            JOIN edges e ON e.edge_id = er.edge_id
            JOIN projection_runs pr ON pr.run_id = e.run_id
            WHERE pr.run_date BETWEEN %s AND %s
              AND e.flagged = TRUE
            """,
            (start, snapshot_date),
        )
        if not row:
            continue
        wins = int(row["wins"] or 0)
        losses = int(row["losses"] or 0)
        pushes = int(row["pushes"] or 0)
        profit = float(row["profit"] or 0)
        flagged = wins + losses + pushes
        hit_rate = wins / (wins + losses) if (wins + losses) > 0 else None
        roi = profit / (wins + losses) if (wins + losses) > 0 else None

        # Calibration: compare game projections vs actual totals
        cal = db.fetchone(
            """
            SELECT
              COUNT(*) AS n,
              AVG(ABS(g.away_score + g.home_score - gp.proj_total)) AS model_mae,
              AVG(ABS(g.away_score + g.home_score - g.market_total)) AS market_mae
            FROM game_projections gp
            JOIN games g ON g.game_pk = gp.game_pk
            JOIN projection_runs pr ON pr.run_id = gp.run_id
            WHERE pr.run_date BETWEEN %s AND %s
              AND g.away_score IS NOT NULL
              AND g.market_total IS NOT NULL
            """,
            (start, snapshot_date),
        )
        n = int(cal["n"] or 0) if cal else 0
        model_mae = float(cal["model_mae"]) if (cal and cal["model_mae"] is not None) else None
        market_mae = float(cal["market_mae"]) if (cal and cal["market_mae"] is not None) else None

        db.execute(
            """
            INSERT INTO model_performance (
              snapshot_date, window_days, flagged_plays, wins, losses, pushes,
              hit_rate, profit_units, roi, games_with_actuals,
              model_mae, market_mae
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (snapshot_date) DO UPDATE SET
              flagged_plays = EXCLUDED.flagged_plays,
              wins = EXCLUDED.wins, losses = EXCLUDED.losses, pushes = EXCLUDED.pushes,
              hit_rate = EXCLUDED.hit_rate, profit_units = EXCLUDED.profit_units,
              roi = EXCLUDED.roi, games_with_actuals = EXCLUDED.games_with_actuals,
              model_mae = EXCLUDED.model_mae, market_mae = EXCLUDED.market_mae
            """,
            (snapshot_date, w, flagged, wins, losses, pushes,
             hit_rate, profit, roi, n, model_mae, market_mae),
        )


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="YYYY-MM-DD; default = yesterday")
    args = ap.parse_args()
    target = None
    if args.date:
        target = date.fromisoformat(args.date)
    metrics = grade_yesterday(target)
    print(metrics)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
