"""
Run from repo root: python patch_grader_f5.py

Patches backend/src/grader.py to enable F5 (first 5 innings) grading.

Changes:
  1. grade_box_score: extract per-inning runs from linescore.innings[:5]
     and persist away_f5_runs / home_f5_runs to the games table.
  2. actual_value_for_edge: return F5 actual from game_row instead of None.
  3. grade_yesterday: include f5 columns in the edges/games JOIN query.

Prereq: run migration backend/migrations/0006_f5_actuals.sql first.
"""
from pathlib import Path

f = Path("backend/src/grader.py")
content = f.read_text(encoding="utf-8")

# ============================================================================
# 1. grade_box_score — persist F5 runs from linescore.innings
# ============================================================================

old_box = '''    linescore = (box.get("liveData") or {}).get("linescore") or {}
    teams = linescore.get("teams") or {}
    away_runs = (teams.get("away") or {}).get("runs")
    home_runs = (teams.get("home") or {}).get("runs")
    if away_runs is not None and home_runs is not None:
        db.execute(
            "UPDATE games SET away_score=%s, home_score=%s, status='Final', refreshed_at=now() WHERE game_pk=%s",
            (away_runs, home_runs, game_pk),
        )'''

new_box = '''    linescore = (box.get("liveData") or {}).get("linescore") or {}
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
            log.warning("F5 inning parse failed for game %s: %s", game_pk, exc)'''

# ============================================================================
# 2. actual_value_for_edge — return F5 actual instead of None
# ============================================================================

old_f5 = '''    if kind == "f5":
        # F5 grading: need inning-by-inning data — not yet stored.
        # Skip for now; will add when we pull play-by-play.
        return None'''

new_f5 = '''    if kind == "f5":
        af5 = game_row.get("away_f5_runs")
        hf5 = game_row.get("home_f5_runs")
        if af5 is None or hf5 is None:
            return None
        return float(af5 + hf5)'''

# ============================================================================
# 3. grade_yesterday — include f5 columns in the edges/games JOIN
# ============================================================================

old_query = '''        edges = db.fetchall(
            """
            SELECT DISTINCT ON (e.game_pk, e.kind, e.category, COALESCE(e.pitcher_mlb_id,0))
                   e.*, g.away_score, g.home_score, g.status
            FROM edges e
            JOIN games g ON g.game_pk = e.game_pk
            JOIN projection_runs pr ON pr.run_id = e.run_id
            WHERE pr.run_date=%s AND e.flagged=TRUE
            ORDER BY e.game_pk, e.kind, e.category, COALESCE(e.pitcher_mlb_id,0), e.edge_id DESC
            """,
            (target,),
        )'''

new_query = '''        edges = db.fetchall(
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
        )'''

# ============================================================================
# Also update the module docstring so v4.2 is recorded
# ============================================================================

old_header = '''"""
Nightly grader — v4.1

Changes vs v4.0:
  - grade_edge: uses actual over_price/under_price stored on the edge
    instead of hardcoded -110 juice. Falls back to -110 if price is NULL
    (old edges, game totals without individual prices).
  - INSERT into edge_results now includes juice_used column.
  - actual_value_for_edge: handles F5 and ML kinds gracefully.
"""'''

new_header = '''"""
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
"""'''

# ============================================================================
# Apply patches
# ============================================================================

patches = [
    ("module header",           old_header, new_header),
    ("grade_box_score F5",      old_box,    new_box),
    ("actual_value_for_edge F5",old_f5,     new_f5),
    ("grade_yesterday query",   old_query,  new_query),
]

issues = []
for label, old, new in patches:
    if old in content:
        content = content.replace(old, new, 1)
        print(f"OK: {label} patched")
    else:
        issues.append(label)
        print(f"WARN: {label} block not found")

if not issues:
    f.write_text(content, encoding="utf-8")
    print(f"\nOK: All F5 grader patches applied to {f}")
    print("\nNext steps:")
    print("  1. Apply migration: psql ... -f backend/migrations/0006_f5_actuals.sql")
    print("  2. (Optional) Backfill: python backfill_f5_runs.py --from 2026-04-01")
    print("  3. Re-run grader for any past dates to populate F5 edge_results")
else:
    print(f"\nPartial: {len(issues)} patterns not found: {issues}")
    if len(issues) < len(patches):
        f.write_text(content, encoding="utf-8")
        print("Written with partial patches — check WARN lines above")
    else:
        print("File NOT written — no patterns matched")
        exit(1)
