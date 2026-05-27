"""
Run from repo root: python patch_persistent_dedup.py

Fixes the previous slate-only dedup. Now the dedup happens at the DB layer
so the grader sees the same edges as the dashboard:

  - Orchestrator already runs dedupe_totals_per_game within a single run
  - Slate endpoint no longer needs runtime dedup
  - NEW: a one-time cleanup pass at the end of every orchestrator run
    unflags any duplicate total/f5 edges that may have been created across
    runs (e.g. line_watcher firing a different kind than morning run)
  - Also adds an admin endpoint to retroactively dedup any existing
    duplicates already in the DB

After this patch, the grader will only grade the edge that survived dedup.
Track record numbers will exclude the unflagged duplicates.

This patch ALSO removes the cosmetic slate-only dedup from the previous
patch — no longer needed since data is clean at source.
"""
from pathlib import Path

api_path = Path("backend/src/api.py")
orch_path = Path("backend/src/orchestrator.py")
api = api_path.read_text(encoding="utf-8")
orch = orch_path.read_text(encoding="utf-8")

# ============================================================================
# 1. Remove the runtime slate dedup from previous patch (cleanup not needed)
# ============================================================================
old_slate_block = '''    edge_dicts = [dict(e) for e in edges]
    edge_dicts = _dedupe_totals_in_slate(edge_dicts)
    return {
        "date": slate_date,
        "run": dict(run),
        "games": [dict(g) for g in games_raw],
        "edges": edge_dicts,
        "projections": [dict(p) for p in projs],
    }'''

restored_slate_block = '''    return {
        "date": slate_date,
        "run": dict(run),
        "games": [dict(g) for g in games_raw],
        "edges": [dict(e) for e in edges],
        "projections": [dict(p) for p in projs],
    }'''

if old_slate_block in api:
    api = api.replace(old_slate_block, restored_slate_block, 1)
    print("OK: removed runtime slate dedup (no longer needed)")

# Remove the now-orphaned helper
helper_start_marker = "def _dedupe_totals_in_slate(edges: list) -> list:"
if helper_start_marker in api:
    start = api.index(helper_start_marker)
    # The helper ends where _slate_for_date begins
    end = api.index("def _slate_for_date(slate_date: str) -> dict:", start)
    api = api[:start] + api[end:]
    print("OK: removed _dedupe_totals_in_slate helper")

# ============================================================================
# 2. Add cross-run dedup pass to orchestrator after edges are inserted
# ============================================================================
# This runs at the end of every orchestrator run. It looks for any games
# on today's run_date that have both a flagged total AND a flagged f5 edge,
# keeps the larger |edge|, and sets flagged=FALSE on the other.

if "def _persistent_cross_run_dedup" in orch:
    print("OK: _persistent_cross_run_dedup already present in orchestrator")
else:
    new_func = '''

def _persistent_cross_run_dedup(run_date_str: str) -> int:
    """
    Look at ALL flagged total/f5 edges for run_date_str across all runs.
    For each game_pk that has both kinds flagged, keep the one with the
    larger absolute edge and set flagged=FALSE on the other.

    Returns count of edges unflagged.

    Idempotent — safe to run multiple times. The grader skips
    flagged=FALSE edges, so this guarantees display and grading agree.
    """
    rows = db.fetchall(
        """
        SELECT e.edge_id, e.game_pk, e.kind, e.edge, e.lean
        FROM edges e
        JOIN projection_runs pr ON pr.run_id = e.run_id
        WHERE pr.run_date = %s
          AND e.flagged = TRUE
          AND e.kind IN ('total', 'f5')
        """,
        (run_date_str,),
    )
    by_game = {}
    for r in rows:
        by_game.setdefault(r["game_pk"], []).append(r)

    drop_ids = []
    for gp, edges_list in by_game.items():
        has_total = any(e["kind"] == "total" for e in edges_list)
        has_f5    = any(e["kind"] == "f5"    for e in edges_list)
        if not (has_total and has_f5):
            continue
        # Within each kind, take the largest |edge| (in case of duplicates)
        best_total = max((e for e in edges_list if e["kind"] == "total"),
                         key=lambda e: abs(float(e["edge"] or 0)))
        best_f5    = max((e for e in edges_list if e["kind"] == "f5"),
                         key=lambda e: abs(float(e["edge"] or 0)))
        keep_id = best_total["edge_id"] if abs(float(best_total["edge"])) >= abs(float(best_f5["edge"])) else best_f5["edge_id"]
        # Drop every edge in this game's total/f5 list except keep_id
        for e in edges_list:
            if e["edge_id"] != keep_id:
                drop_ids.append(e["edge_id"])

    if drop_ids:
        db.execute(
            "UPDATE edges SET flagged = FALSE WHERE edge_id = ANY(%s)",
            (drop_ids,),
        )
        log.info("Cross-run dedup: unflagged %d duplicate total/f5 edges for %s",
                 len(drop_ids), run_date_str)
    return len(drop_ids)
'''
    # Insert near other helpers — right after the existing dedupe_totals_per_game function
    anchor_idx = orch.index("def compute_edges_for_game(")
    # Find the spot just before this anchor
    orch = orch[:anchor_idx] + new_func + "\n\n" + orch[anchor_idx:]
    print("OK: _persistent_cross_run_dedup function added")

# ============================================================================
# 3. Call the cross-run dedup at the end of orchestrator.run()
# ============================================================================
# We call it after edges are inserted but before sending notifications,
# so the notification correctly reflects the deduped set.

old_call_anchor = '''        all_edges.sort(key=lambda x:abs(x["edge"]),reverse=True)
        metrics.update({"n_edges":len(all_edges),'''

new_call_anchor = '''        # Cross-run dedup: if line_watcher created an f5 on a game where
        # morning already had a total (or vice-versa), unflag the smaller
        # so display and grading agree.
        try:
            n_unflagged = _persistent_cross_run_dedup(run_date)
            if n_unflagged:
                # Remove unflagged edges from all_edges so the notification
                # and metrics match what's actually flagged in the DB.
                kept = db.fetchall(
                    "SELECT edge_id FROM edges WHERE run_id=%s AND flagged=TRUE",
                    (run_id,),
                )
                kept_ids = {r["edge_id"] for r in kept}
                all_edges = [e for e in all_edges if e.get("edge_id") in kept_ids or e.get("edge_id") is None]
                metrics["n_unflagged_cross_run"] = n_unflagged
        except Exception as _e:
            log.warning("cross-run dedup failed: %s", _e)
        all_edges.sort(key=lambda x:abs(x["edge"]),reverse=True)
        metrics.update({"n_edges":len(all_edges),'''

if old_call_anchor in orch:
    orch = orch.replace(old_call_anchor, new_call_anchor, 1)
    print("OK: orchestrator.run() now calls _persistent_cross_run_dedup")
elif "_persistent_cross_run_dedup(run_date)" in orch:
    print("OK: orchestrator already calls cross-run dedup")
else:
    print("WARN: orchestrator.run() anchor not found — manual wiring needed")

orch_path.write_text(orch, encoding="utf-8")
api_path.write_text(api, encoding="utf-8")

# ============================================================================
# 4. Add an admin endpoint to retroactively clean up existing duplicates
# ============================================================================
if "/api/admin/cleanup_dedup/" in api:
    print("OK: cleanup_dedup endpoint already present")
else:
    endpoint = '''


@app.get("/api/admin/cleanup_dedup/{token}")
def admin_cleanup_dedup(token: str, date: str = None):
    """Retroactively run cross-run total/f5 dedup for a date (or all dates).

    For each game on the date that has both a flagged total and flagged f5
    edge, keep the larger |edge| and unflag the other. Mirrors the
    cross-run dedup the orchestrator now runs automatically.

    Use ?date=YYYY-MM-DD for a single date, or no params to fix every date
    with duplicates.
    """
    _check_admin(token)
    from . import orchestrator

    if date:
        n = orchestrator._persistent_cross_run_dedup(date)
        return {"date": date, "edges_unflagged": n}

    # No date supplied — find every date with duplicates
    dates = db.fetchall(
        """
        SELECT DISTINCT pr.run_date::text AS d
        FROM edges e
        JOIN projection_runs pr ON pr.run_id = e.run_id
        WHERE e.flagged = TRUE AND e.kind IN ('total','f5')
        GROUP BY pr.run_date, e.game_pk
        HAVING COUNT(DISTINCT e.kind) > 1
        """
    )
    summary = []
    for r in dates:
        d = r["d"]
        n = orchestrator._persistent_cross_run_dedup(d)
        summary.append({"date": d, "edges_unflagged": n})
    return {"dates_processed": len(summary), "details": summary}
'''
    api = api.rstrip() + endpoint + "\n"
    api_path.write_text(api, encoding="utf-8")
    print("OK: /api/admin/cleanup_dedup endpoint added")

print()
print("Steps:")
print('  python -X utf8 -c "import ast; ast.parse(open(\'backend/src/orchestrator.py\').read()); ast.parse(open(\'backend/src/api.py\').read()); print(\'OK\')"')
print("  git add backend/src/orchestrator.py backend/src/api.py patch_persistent_dedup.py")
print("  git commit -m 'Persistent dedup: unflag cross-run duplicates so grader matches display'")
print("  git push")
print()
print("After Railway redeploys:")
print("  1. Run cleanup retroactively to fix existing duplicates:")
print("       /api/admin/cleanup_dedup/<token>")
print("     (uses admin panel button if you add 'Cleanup Dedup' there)")
print("  2. Every future orchestrator run auto-runs the dedup at the end.")
print("     Display and grading will always agree.")
