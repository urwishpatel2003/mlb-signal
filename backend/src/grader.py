"""
Nightly grader — v4.2

Changes vs v4.1:
  - grade_box_score: extracts F5 (first 5 innings) runs from linescore.innings
    and persists away_f5_runs/home_f5_runs on games. Requires migration 0006.
  - actual_value_for_edge: F5 branch now returns realized F5 total instead of None.
  - grade_yesterday: JOIN query includes f5 columns so F5 edges can be graded.

Changes vs v4.0:
  - grade_edge: uses actual over_price/under_price stored on the edge
    instead of hardcoded -110 juice. Falls back to -110 if price is NULL
    (old edges, game totals without individual prices).
  - INSERT into edge_results now includes juice_used column.
  - actual_value_for_edge: handles F5 and ML kinds gracefully.
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

    linescore = (box.get("liveData") or {}).get("linescore") or {}
    teams = linescore.get("teams") or {}
    away_runs = (teams.get("away") or {}).get("runs")
    home_runs = (teams.get("home") or {}).get("runs")
    if away_runs is not None and home_runs is not None:
        db.execute(
            "UPDATE games SET away_score=%s, home_score=%s, status='Final', refreshed_at=now() WHERE game_pk=%s",
            (away_runs, home_runs, game_pk),
        )

    # F5 (first 5 innings) — walk linescore.innings[:5] for F5 total grading.
    # Only persist if the game actually reached the bottom of the 5th; otherwise
    # leave NULL so F5 edges on rain-shortened games stay ungraded (safer than
    # booking a partial-inning result as a loss).
    innings = linescore.get("innings") or []
    if len(innings) >= 5:
        try:
            away_f5 = sum(int((inn.get("away") or {}).get("runs") or 0) for inn in innings[:5])
            home_f5 = sum(int((inn.get("home") or {}).get("runs") or 0) for inn in innings[:5])
            # Confirm bottom of 5th was actually played — if home was already
            # ahead and didn't bat, MLB still includes innings[4] but home.runs
            # may be missing entirely (key absent). Treat that as incomplete.
            home5 = (innings[4].get("home") or {})
            if "runs" in home5 or away_f5 > sum(int((inn.get("home") or {}).get("runs") or 0) for inn in innings[:4]):
                db.execute(
                    "UPDATE games SET away_f5_runs=%s, home_f5_runs=%s WHERE game_pk=%s",
                    (away_f5, home_f5, game_pk),
                )
        except (ValueError, TypeError, IndexError) as exc:
            log.warning("F5 inning parse failed for game %s: %s", game_pk, exc)

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
              (game_pk,mlb_id,last_first,ip,outs,h,er,bb,k,pitches,bf,refreshed_at)
            VALUES
              (%(game_pk)s,%(mlb_id)s,%(last_first)s,%(ip)s,%(outs)s,
               %(h)s,%(er)s,%(bb)s,%(k)s,%(pitches)s,%(bf)s,now())
            ON CONFLICT (game_pk,mlb_id) DO UPDATE SET
              ip=EXCLUDED.ip, outs=EXCLUDED.outs, h=EXCLUDED.h, er=EXCLUDED.er,
              bb=EXCLUDED.bb, k=EXCLUDED.k, pitches=EXCLUDED.pitches,
              bf=EXCLUDED.bf, refreshed_at=now();
        """
        db.execute_many(sql, rows)

    return {"game_pk": game_pk, "n_pitchers": len(rows),
            "away_runs": away_runs, "home_runs": home_runs}


def actual_value_for_edge(e: dict, game_row: dict) -> Optional[float]:
    """Return the realized value for an edge."""
    kind = e.get("kind", "total")

    if kind == "total":
        ar = game_row.get("away_score")
        hr = game_row.get("home_score")
        if ar is None or hr is None:
            return None
        return float(ar + hr)

    if kind == "f5":
        af5 = game_row.get("away_f5_runs")
        hf5 = game_row.get("home_f5_runs")
        if af5 is None or hf5 is None:
            return None
        return float(af5 + hf5)

    if kind == "ml":
        # ML grading: home win = home_score > away_score
        ar = game_row.get("away_score")
        hr = game_row.get("home_score")
        if ar is None or hr is None:
            return None
        # proj_value stores model win% as a number 0-100
        # actual_value: 1.0 = lean team won, 0.0 = lost
        lean_team = e.get("lean")
        home_team = e.get("opp_team_code")   # home team stored as opp_team_code for ML edges
        away_team = e.get("team_code")
        if lean_team == home_team:
            return 1.0 if hr > ar else 0.0
        else:
            return 1.0 if ar > hr else 0.0

    # Prop
    actual = db.fetchone(
        "SELECT ip,outs,h,er,bb,k FROM pitcher_actuals WHERE game_pk=%s AND mlb_id=%s",
        (e["game_pk"], e["pitcher_mlb_id"]),
    )
    if not actual:
        return None
    cat = e["category"]
    if cat == "K":    return float(actual["k"])
    if cat == "Hits": return float(actual["h"])
    if cat == "ER":   return float(actual["er"])
    if cat == "Outs": return float(actual["outs"])
    if cat == "BB":   return float(actual["bb"])
    return None


def grade_edge(e: dict, actual: float, default_juice: int = -110) -> dict:
    """
    Apply over/under decision logic and compute profit/loss in units.

    Uses actual over_price / under_price stored on the edge when available.
    Falls back to default_juice (-110) for older edges or markets without
    individual prices (e.g. game totals stored before migration 0005).

    ML edges are graded differently: lean team win = WIN, loss = LOSS, no push.
    """
    line   = float(e["line"])
    lean   = e["lean"]
    kind   = e.get("kind", "total")

    # --- ML edge: binary outcome, no line comparison ---
    if kind == "ml":
        juice = e.get("over_price") or default_juice  # ML odds stored in over_price
        won   = (actual == 1.0)
        if won:   result, profit = "WIN",  _profit_for(juice)
        else:     result, profit = "LOSS", -1.0
        return {"result": result, "profit_units": round(profit, 4),
                "actual": actual, "juice_used": juice}

    # --- O/U edge ---
    if lean == "OVER":
        juice = e.get("over_price") or default_juice
        if actual > line:   result, profit = "WIN",  _profit_for(juice)
        elif actual < line: result, profit = "LOSS", -1.0
        else:               result, profit = "PUSH",  0.0
    elif lean == "UNDER":
        juice = e.get("under_price") or default_juice
        if actual < line:   result, profit = "WIN",  _profit_for(juice)
        elif actual > line: result, profit = "LOSS", -1.0
        else:               result, profit = "PUSH",  0.0
    else:
        juice  = default_juice
        result, profit = "NO_ACTION", 0.0

    stake = float(e.get("stake_units") or 1.0)
    return {"result": result, "profit_units": round(profit * stake, 4),
            "actual": actual, "juice_used": juice, "stake_units": stake}


def _profit_for(juice: int) -> float:
    """Profit in units (1 unit = $100 risked) at given American odds."""
    if juice < 0:
        return 100.0 / abs(juice)
    return juice / 100.0


def grade_yesterday(target_date: Optional[date] = None) -> dict:
    """Top-level entry: grade all games from target_date."""
    target = target_date or (date.today() - timedelta(days=1))
    job_id = db.log_job_start("grader")
    metrics: dict = {"target_date": target.isoformat()}
    try:
        games = db.fetchall("SELECT * FROM games WHERE game_date=%s", (target,))
        graded_games = 0
        for g in games:
            if g["status"] in ("Postponed", "Cancelled"):
                continue
            if g["away_score"] is None or g["home_score"] is None:
                try:
                    grade_box_score(g["game_pk"])
                    graded_games += 1
                except Exception as e:
                    log.warning("Failed to grade box %s: %s", g["game_pk"], e)
                    continue
            else:
                existing = db.fetchone(
                    "SELECT 1 FROM pitcher_actuals WHERE game_pk=%s LIMIT 1",
                    (g["game_pk"],))
                if not existing:
                    try:
                        grade_box_score(g["game_pk"])
                    except Exception as e:
                        log.warning("Failed to grade box %s: %s", g["game_pk"], e)
                graded_games += 1

        metrics["games_graded"] = graded_games

        edges = db.fetchall(
            """
            SELECT DISTINCT ON (e.game_pk, e.kind, e.category, COALESCE(e.pitcher_mlb_id,0))
                   e.*, g.away_score, g.home_score,
                   g.away_f5_runs, g.home_f5_runs, g.status
            FROM edges e
            JOIN games g ON g.game_pk = e.game_pk
            JOIN projection_runs pr ON pr.run_id = e.run_id
            WHERE pr.run_date=%s AND e.flagged=TRUE
            ORDER BY e.game_pk, e.kind, e.category, COALESCE(e.pitcher_mlb_id,0), e.edge_id DESC
            """,
            (target,),
        )

        wins = losses = pushes = 0
        total_profit = 0.0
        for e in edges:
            existing = db.fetchone("SELECT 1 FROM edge_results WHERE edge_id=%s", (e["edge_id"],))
            if existing:
                continue
            actual = actual_value_for_edge(e, e)
            if actual is None:
                continue
            graded = grade_edge(e, actual)
            db.execute(
                """
                INSERT INTO edge_results (edge_id, actual_value, result, profit_units, juice_used)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (e["edge_id"], actual, graded["result"],
                 graded["profit_units"], graded.get("juice_used")),
            )
            if graded["result"] == "WIN":  wins += 1
            elif graded["result"] == "LOSS": losses += 1
            elif graded["result"] == "PUSH": pushes += 1
            total_profit += graded["profit_units"]

        metrics["wins"]         = wins
        metrics["losses"]       = losses
        metrics["pushes"]       = pushes
        metrics["profit_units"] = round(total_profit, 2)
        metrics["flagged_plays"]= wins + losses + pushes
        metrics["hit_rate"]     = wins / (wins + losses) if (wins + losses) > 0 else None

        update_rolling_performance(target)
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
              COUNT(*) FILTER (WHERE er.result='WIN')  AS wins,
              COUNT(*) FILTER (WHERE er.result='LOSS') AS losses,
              COUNT(*) FILTER (WHERE er.result='PUSH') AS pushes,
              COALESCE(SUM(er.profit_units), 0) AS profit
            FROM edge_results er
            JOIN edges e ON e.edge_id=er.edge_id
            JOIN projection_runs pr ON pr.run_id=e.run_id
            WHERE pr.run_date BETWEEN %s AND %s AND e.flagged=TRUE
            """,
            (start, snapshot_date),
        )
        if not row: continue
        wins    = int(row["wins"] or 0)
        losses  = int(row["losses"] or 0)
        pushes  = int(row["pushes"] or 0)
        profit  = float(row["profit"] or 0)
        flagged = wins + losses + pushes
        hit_rate = wins / (wins + losses) if (wins + losses) > 0 else None
        roi      = profit / (wins + losses) if (wins + losses) > 0 else None

        cal = db.fetchone(
            """
            SELECT
              COUNT(*) AS n,
              AVG(ABS(g.away_score+g.home_score - gp.proj_total)) AS model_mae,
              AVG(ABS(g.away_score+g.home_score - g.market_total)) AS market_mae
            FROM game_projections gp
            JOIN games g ON g.game_pk=gp.game_pk
            JOIN projection_runs pr ON pr.run_id=gp.run_id
            WHERE pr.run_date BETWEEN %s AND %s
              AND g.away_score IS NOT NULL AND g.market_total IS NOT NULL
            """,
            (start, snapshot_date),
        )
        n          = int(cal["n"] or 0) if cal else 0
        model_mae  = float(cal["model_mae"])  if (cal and cal["model_mae"]  is not None) else None
        market_mae = float(cal["market_mae"]) if (cal and cal["market_mae"] is not None) else None

        db.execute(
            """
            INSERT INTO model_performance (
              snapshot_date, window_days, flagged_plays, wins, losses, pushes,
              hit_rate, profit_units, roi, games_with_actuals, model_mae, market_mae
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (snapshot_date) DO UPDATE SET
              flagged_plays=EXCLUDED.flagged_plays,
              wins=EXCLUDED.wins, losses=EXCLUDED.losses, pushes=EXCLUDED.pushes,
              hit_rate=EXCLUDED.hit_rate, profit_units=EXCLUDED.profit_units,
              roi=EXCLUDED.roi, games_with_actuals=EXCLUDED.games_with_actuals,
              model_mae=EXCLUDED.model_mae, market_mae=EXCLUDED.market_mae
            """,
            (snapshot_date, w, flagged, wins, losses, pushes,
             hit_rate, profit, roi, n, model_mae, market_mae),
        )


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="YYYY-MM-DD; default=yesterday")
    args = ap.parse_args()
    target = date.fromisoformat(args.date) if args.date else None
    print(grade_yesterday(target))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
