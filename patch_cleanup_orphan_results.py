"""
Run from repo root: python patch_cleanup_orphan_results.py

Adds /api/admin/cleanup_orphan_results/{token}

After cleanup_dedup unflagged 103 duplicate edges, their already-graded
results stayed in edge_results — meaning track record still counts wins/losses
from edges that no longer show as flagged in the slate.

This endpoint deletes any edge_results row whose corresponding edge has
flagged=FALSE, then clears model_performance so the next grader run
recomputes rolling stats from clean data.

Idempotent — safe to re-run.
"""
from pathlib import Path

f = Path("backend/src/api.py")
content = f.read_text(encoding="utf-8")

if "/api/admin/cleanup_orphan_results/" in content:
    print("OK: cleanup_orphan_results endpoint already present")
    raise SystemExit(0)

endpoint = '''


@app.get("/api/admin/cleanup_orphan_results/{token}")
def cleanup_orphan_results(token: str, dry_run: bool = False):
    """Delete edge_results rows belonging to unflagged edges.

    When dedup unflags duplicate edges (kind='total' vs 'f5' on same game),
    their previously-graded results stay in edge_results and pollute track
    record stats. This drops those orphans and resets model_performance so
    the grader can recompute rolling sums cleanly.

    ?dry_run=true returns the counts without modifying anything.
    """
    _check_admin(token)

    counts = db.fetchone("""
        SELECT
          COUNT(*)                                       AS n_orphans,
          COUNT(*) FILTER (WHERE e.kind = 'total')       AS n_total,
          COUNT(*) FILTER (WHERE e.kind = 'f5')          AS n_f5,
          COUNT(*) FILTER (WHERE e.kind = 'ml')          AS n_ml,
          COUNT(*) FILTER (WHERE e.kind = 'prop')        AS n_prop,
          COALESCE(SUM(er.profit_units), 0)::float       AS profit_units_removed
        FROM edge_results er
        JOIN edges e ON e.edge_id = er.edge_id
        WHERE e.flagged = FALSE
    """)

    result = {
        "n_orphans": int(counts["n_orphans"] or 0),
        "by_kind": {
            "total": int(counts["n_total"] or 0),
            "f5":    int(counts["n_f5"] or 0),
            "ml":    int(counts["n_ml"] or 0),
            "prop":  int(counts["n_prop"] or 0),
        },
        "profit_units_removed": round(float(counts["profit_units_removed"] or 0), 2),
        "dry_run": dry_run,
    }

    if dry_run or result["n_orphans"] == 0:
        return result

    db.execute("""
        DELETE FROM edge_results er
        USING edges e
        WHERE er.edge_id = e.edge_id AND e.flagged = FALSE
    """)
    db.execute("DELETE FROM model_performance")
    result["model_performance_cleared"] = True
    result["note"] = "Next grader run will rebuild rolling performance from clean data"
    return result
'''

content = content.rstrip() + endpoint + "\n"
f.write_text(content, encoding="utf-8")
print("OK: /api/admin/cleanup_orphan_results endpoint added")

print()
print("Steps:")
print('  python -X utf8 -c "import ast; ast.parse(open(\'backend/src/api.py\').read()); print(\'OK\')"')
print("  git add backend/src/api.py patch_cleanup_orphan_results.py")
print("  git commit -m 'Admin: cleanup_orphan_results endpoint'")
print("  git push")
print()
print("After Railway redeploys:")
print()
print("  1. Dry run first (preview counts, no changes):")
print("     /api/admin/cleanup_orphan_results/<token>?dry_run=true")
print()
print("  2. If counts look right, run for real:")
print("     /api/admin/cleanup_orphan_results/<token>")
print()
print("  3. Trigger the grader once to rebuild model_performance:")
print("     /api/admin/trigger/grader/<token>")
print()
print("  Track record on the dashboard will then reflect only flagged edges.")
