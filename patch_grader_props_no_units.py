"""
Run from repo root: python patch_grader_props_no_units.py

Props are tracked by win/loss only, not by profit/loss units. This patch:

1. Modifies grade_edge() in grader.py so prop edges always return
   profit_units = 0.0 (W/L/PUSH still computed normally).

2. Modifies grade_yesterday()'s daily metrics so total_profit excludes
   props from the per-day sum.

3. Modifies update_rolling_performance() rolling sums to also exclude
   props.

4. Adds an admin endpoint /api/admin/zero_prop_units/{token} to retro-
   actively zero out prop profit_units already in edge_results.

After applying:
  - Push the code change (grader + api)
  - After deploy, hit /api/admin/zero_prop_units/<token> once to clean
    historical data
  - Daily and cumulative recap will then exclude prop units everywhere
"""
from pathlib import Path

grader_path = Path("backend/src/grader.py")
api_path = Path("backend/src/api.py")
grader_content = grader_path.read_text(encoding="utf-8")
api_content = api_path.read_text(encoding="utf-8")

# ============================================================================
# 1. grade_edge: force profit_units = 0 for prop edges
# ============================================================================
# We add a guard right at the end of grade_edge before the final return.
# Inserting a kind=='prop' override is cleaner than rewriting every branch.

old_grade_edge_tail = '''    stake = float(e.get("stake_units") or 1.0)
    return {"result": result, "profit_units": round(profit * stake, 4),
            "actual": actual, "juice_used": juice, "stake_units": stake}'''

new_grade_edge_tail = '''    stake = float(e.get("stake_units") or 1.0)
    # Props: track W/L only, zero out profit_units (per product decision).
    # Other edge kinds (total, f5, ml) keep their profit_units intact.
    if kind == "prop":
        return {"result": result, "profit_units": 0.0,
                "actual": actual, "juice_used": juice, "stake_units": stake}
    return {"result": result, "profit_units": round(profit * stake, 4),
            "actual": actual, "juice_used": juice, "stake_units": stake}'''

if old_grade_edge_tail in grader_content:
    grader_content = grader_content.replace(old_grade_edge_tail, new_grade_edge_tail, 1)
    print("OK: grade_edge zero-out for props applied")
elif "if kind == \"prop\":\n        return {\"result\": result, \"profit_units\": 0.0" in grader_content:
    print("OK: grade_edge prop zero-out already present")
else:
    print("WARN: grade_edge tail pattern not found")

# ============================================================================
# 2. grade_yesterday: exclude props from total_profit sum
# ============================================================================
# The simplest fix is to gate the += on kind != 'prop'.

old_profit_accum = '''            if graded["result"] == "WIN":  wins += 1
            elif graded["result"] == "LOSS": losses += 1
            elif graded["result"] == "PUSH": pushes += 1
            total_profit += graded["profit_units"]'''

new_profit_accum = '''            if graded["result"] == "WIN":  wins += 1
            elif graded["result"] == "LOSS": losses += 1
            elif graded["result"] == "PUSH": pushes += 1
            # Props are tracked W/L only — exclude from cumulative profit
            if e.get("kind") != "prop":
                total_profit += graded["profit_units"]'''

if old_profit_accum in grader_content:
    grader_content = grader_content.replace(old_profit_accum, new_profit_accum, 1)
    print("OK: grade_yesterday total_profit excludes props")
elif 'if e.get("kind") != "prop":' in grader_content:
    print("OK: grade_yesterday already excludes props")
else:
    print("WARN: grade_yesterday total_profit pattern not found")

# ============================================================================
# 3. update_rolling_performance: exclude props from rolling sums
# ============================================================================
old_rolling = '''SELECT
              COUNT(*) FILTER (WHERE er.result='WIN')  AS wins,
              COUNT(*) FILTER (WHERE er.result='LOSS') AS losses,
              COUNT(*) FILTER (WHERE er.result='PUSH') AS pushes,
              COALESCE(SUM(er.profit_units), 0) AS profit
            FROM edge_results er
            JOIN edges e ON e.edge_id=er.edge_id
            JOIN projection_runs pr ON pr.run_id=e.run_id
            WHERE pr.run_date BETWEEN %s AND %s AND e.flagged=TRUE'''

new_rolling = '''SELECT
              COUNT(*) FILTER (WHERE er.result='WIN')  AS wins,
              COUNT(*) FILTER (WHERE er.result='LOSS') AS losses,
              COUNT(*) FILTER (WHERE er.result='PUSH') AS pushes,
              COALESCE(SUM(er.profit_units) FILTER (WHERE e.kind != 'prop'), 0) AS profit
            FROM edge_results er
            JOIN edges e ON e.edge_id=er.edge_id
            JOIN projection_runs pr ON pr.run_id=e.run_id
            WHERE pr.run_date BETWEEN %s AND %s AND e.flagged=TRUE'''

if old_rolling in grader_content:
    grader_content = grader_content.replace(old_rolling, new_rolling, 1)
    print("OK: update_rolling_performance excludes props from profit sum")
elif "FILTER (WHERE e.kind != 'prop')" in grader_content:
    print("OK: update_rolling_performance already excludes props")
else:
    print("WARN: update_rolling_performance rolling SQL pattern not found")

# Bump version header
old_header = '''Nightly grader — v4.3

Changes vs v4.2:
  - grade_box_score: F5 runs now fetched from /api/v1/game/{pk}/linescore
    (standalone endpoint) instead of the v1.1 live feed, which returns
    empty inning objects. Fixes silent F5 NULL bug.'''

new_header = '''Nightly grader — v4.4

Changes vs v4.3:
  - Props are tracked W/L only — profit_units forced to 0.0 in grade_edge.
  - grade_yesterday daily total_profit excludes prop edges.
  - update_rolling_performance excludes prop profit from rolling sums.

Changes vs v4.2:
  - grade_box_score: F5 runs now fetched from /api/v1/game/{pk}/linescore
    (standalone endpoint) instead of the v1.1 live feed, which returns
    empty inning objects. Fixes silent F5 NULL bug.'''

if old_header in grader_content:
    grader_content = grader_content.replace(old_header, new_header, 1)
    print("OK: grader version bumped to v4.4")

grader_path.write_text(grader_content, encoding="utf-8")

# ============================================================================
# 4. Add admin endpoint /api/admin/zero_prop_units/{token}
# ============================================================================
if "/api/admin/zero_prop_units/" in api_content:
    print("OK: zero_prop_units endpoint already present")
else:
    endpoint = '''


# ============================================================================
# Retroactively zero out profit_units for prop edges
# (Props are tracked W/L only; we don't sum them into cumulative profit.)
# ============================================================================
@app.get("/api/admin/zero_prop_units/{token}")
def zero_prop_units(token: str):
    """One-shot cleanup: set profit_units = 0 for all already-graded prop
    edges. Their result (WIN/LOSS/PUSH) is unchanged."""
    _check_admin(token)
    from . import db

    before = db.fetchone("""
        SELECT COUNT(*) AS n,
               COALESCE(SUM(er.profit_units), 0)::float AS total_profit
        FROM edge_results er
        JOIN edges e ON e.edge_id = er.edge_id
        WHERE e.kind = 'prop' AND er.profit_units != 0
    """)
    n_before = int(before["n"] or 0)
    profit_zeroed = float(before["total_profit"] or 0)

    db.execute("""
        UPDATE edge_results er
        SET profit_units = 0
        FROM edges e
        WHERE er.edge_id = e.edge_id AND e.kind = 'prop'
    """)

    # Recompute rolling perf snapshots so cumulative recap drops the props
    db.execute("DELETE FROM model_performance")

    return {
        "rows_zeroed": n_before,
        "profit_units_removed": round(profit_zeroed, 2),
        "note": "model_performance cleared — next grader run will recompute rolling sums",
    }
'''
    api_content = api_content.rstrip() + endpoint + "\n"
    api_path.write_text(api_content, encoding="utf-8")
    print("OK: /api/admin/zero_prop_units endpoint added")

print()
print("Steps:")
print('  python -X utf8 -c "import ast; ast.parse(open(\'backend/src/grader.py\').read()); ast.parse(open(\'backend/src/api.py\').read()); print(\'OK\')"')
print("  git add backend/src/grader.py backend/src/api.py patch_grader_props_no_units.py")
print("  git commit -m 'Grader v4.4: props tracked W/L only, no profit_units'")
print("  git push")
print()
print("After Railway deploys (unpause if needed):")
print("  1. Hit /api/admin/zero_prop_units/<token>  -> retroactively zeros old prop units")
print("  2. Hit /api/admin/trigger/grader/<token>   -> rebuilds rolling performance")
print("  3. Refresh dashboard. Track Record's daily and cumulative profit will no longer")
print("     include prop units. Prop W/L counts remain intact.")
