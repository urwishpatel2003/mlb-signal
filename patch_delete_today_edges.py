"""
Run from repo root: python patch_delete_today_edges.py

Adds /api/admin/delete_today_edges/{token}

DELETE rows from edges (cascading to edge_results) for the current run_date.
Optional ?clean_projections=true also wipes today's pitcher_projections and
game_projections for a fully fresh slate.

After hitting this, trigger orchestrator (admin panel) to regenerate.
"""
from pathlib import Path

f = Path("backend/src/api.py")
content = f.read_text(encoding="utf-8")

if "/api/admin/delete_today_edges/" in content:
    print("OK: delete_today_edges endpoint already present")
    raise SystemExit(0)

endpoint = '''


@app.get("/api/admin/delete_today_edges/{token}")
def delete_today_edges(token: str, clean_projections: bool = False, date: str = None):
    """Delete today's flagged + unflagged edges so the orchestrator can rerun fresh.

    ?date=YYYY-MM-DD to target a specific date (defaults to CURRENT_DATE).
    ?clean_projections=true also deletes pitcher_projections and game_projections
    for the same run_date (otherwise they remain attached to old run_ids).

    edge_results cascade-delete automatically via foreign key.
    """
    _check_admin(token)
    from datetime import date as _date

    target = date or _date.today().isoformat()
    summary = {"date": target}

    # Find all run_ids for the target date
    run_id_rows = db.fetchall(
        "SELECT run_id FROM projection_runs WHERE run_date = %s",
        (target,),
    )
    run_ids = [r["run_id"] for r in run_id_rows]
    summary["n_runs"] = len(run_ids)
    if not run_ids:
        summary["note"] = "no runs for this date"
        return summary

    # Count before delete
    pre_edges = db.fetchone(
        "SELECT COUNT(*) AS n FROM edges WHERE run_id = ANY(%s)",
        (run_ids,),
    )
    summary["edges_before"] = int(pre_edges["n"] or 0)

    # Delete edges (cascades to edge_results via FK ON DELETE CASCADE)
    db.execute("DELETE FROM edges WHERE run_id = ANY(%s)", (run_ids,))

    summary["edges_deleted"] = summary["edges_before"]
    summary["clean_projections"] = clean_projections

    if clean_projections:
        pre_pp = db.fetchone(
            "SELECT COUNT(*) AS n FROM pitcher_projections WHERE run_id = ANY(%s)",
            (run_ids,),
        )
        pre_gp = db.fetchone(
            "SELECT COUNT(*) AS n FROM game_projections WHERE run_id = ANY(%s)",
            (run_ids,),
        )
        db.execute("DELETE FROM pitcher_projections WHERE run_id = ANY(%s)", (run_ids,))
        db.execute("DELETE FROM game_projections WHERE run_id = ANY(%s)", (run_ids,))
        summary["pitcher_projections_deleted"] = int(pre_pp["n"] or 0)
        summary["game_projections_deleted"] = int(pre_gp["n"] or 0)

    return summary
'''

content = content.rstrip() + endpoint + "\n"
f.write_text(content, encoding="utf-8")
print("OK: /api/admin/delete_today_edges endpoint added")

print()
print("Verify and push:")
print('  python -X utf8 -c "import ast; ast.parse(open(\'backend/src/api.py\').read()); print(\'OK\')"')
print("  git add backend/src/api.py patch_delete_today_edges.py")
print("  git commit -m 'Admin: delete_today_edges endpoint'")
print("  git push")
print()
print("After Railway redeploys, hit:")
print("  https://mlb-signal-production.up.railway.app/api/admin/delete_today_edges/YOUR_TOKEN")
print("  (returns JSON with edges_deleted count)")
print()
print("For a TRULY fresh slate (also wipes projections):")
print("  https://mlb-signal-production.up.railway.app/api/admin/delete_today_edges/YOUR_TOKEN?clean_projections=true")
print()
print("Then trigger orchestrator from the admin panel.")
