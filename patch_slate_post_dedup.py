"""
Run from repo root: python patch_slate_post_dedup.py

Fixes the cross-run duplicate edges issue:

The slate endpoint pulls flagged edges from ALL runs on the date (via DISTINCT
ON which keeps one row per kind+lean+pitcher key). The orchestrator's
dedupe_totals_per_game ONLY runs within a single orchestrator invocation, so
if run #1 flags a Total and run #2 flags an F5 (or vice versa), both survive
to the API response.

Fix: after pulling edges in _slate_for_date(), collapse per-game total+f5
pairs into one (larger |edge| wins) regardless of which run produced them.
Same rule as dedupe_totals_per_game in the orchestrator — but applied to
the merged cross-run result.
"""
from pathlib import Path

api_path = Path("backend/src/api.py")
content = api_path.read_text(encoding="utf-8")

# Insert a helper near the top, before _slate_for_date
if "def _dedupe_totals_in_slate" in content:
    print("OK: _dedupe_totals_in_slate already present")
else:
    helper = '''def _dedupe_totals_in_slate(edges: list) -> list:
    """Collapse Total + F5 edges on the same game down to one.

    Mirrors dedupe_totals_per_game in the orchestrator but operates on the
    merged cross-run edge list returned by the slate endpoint. Without this,
    edges from different runs (e.g. line_watcher at 11:00 firing a Total,
    then 14:00 firing an F5) both survive the DISTINCT ON in the slate SQL.

    Per game: if both a 'total' and an 'f5' edge are present and BOTH flagged,
    keep only the one with larger absolute edge. Direction doesn't matter.
    ML and prop edges pass through unchanged.
    """
    by_game = {}
    for e in edges:
        if e.get("kind") not in ("total", "f5"):
            continue
        if not e.get("flagged"):
            continue
        by_game.setdefault(e["game_pk"], []).append(e)

    drop_ids = set()
    for gp, game_totals in by_game.items():
        if len(game_totals) < 2:
            continue
        # In a per-game list there can be at most one total and one f5
        # (DISTINCT ON in slate query already collapses duplicates within
        # the same (game, kind) key)
        total_e = next((e for e in game_totals if e["kind"] == "total"), None)
        f5_e    = next((e for e in game_totals if e["kind"] == "f5"), None)
        if not total_e or not f5_e:
            continue
        keep = total_e if abs(float(total_e.get("edge") or 0)) >= abs(float(f5_e.get("edge") or 0)) else f5_e
        drop = f5_e if keep is total_e else total_e
        drop_ids.add(drop.get("edge_id"))

    return [e for e in edges if e.get("edge_id") not in drop_ids]


'''
    # Place it directly before _slate_for_date
    anchor = "def _slate_for_date(slate_date: str) -> dict:"
    if anchor in content:
        content = content.replace(anchor, helper + anchor, 1)
        print("OK: _dedupe_totals_in_slate helper inserted before _slate_for_date")
    else:
        print("ERR: _slate_for_date anchor not found in api.py")
        raise SystemExit(1)

# Now call the helper inside _slate_for_date, just before the return.
# Anchor: the existing dict comprehension that wraps edges in the return.
old_return_block = '''    return {
        "date": slate_date,
        "run": dict(run),
        "games": [dict(g) for g in games_raw],
        "edges": [dict(e) for e in edges],
        "projections": [dict(p) for p in projs],
    }'''

new_return_block = '''    edge_dicts = [dict(e) for e in edges]
    edge_dicts = _dedupe_totals_in_slate(edge_dicts)
    return {
        "date": slate_date,
        "run": dict(run),
        "games": [dict(g) for g in games_raw],
        "edges": edge_dicts,
        "projections": [dict(p) for p in projs],
    }'''

if old_return_block in content:
    content = content.replace(old_return_block, new_return_block, 1)
    print("OK: _slate_for_date now runs cross-run dedup before returning")
elif "_dedupe_totals_in_slate(edge_dicts)" in content:
    print("OK: dedup call already present in _slate_for_date")
else:
    print("ERR: _slate_for_date return block pattern not found")
    raise SystemExit(1)

api_path.write_text(content, encoding="utf-8")

print()
print("Steps:")
print('  python -X utf8 -c "import ast; ast.parse(open(\'backend/src/api.py\').read()); print(\'OK\')"')
print("  git add backend/src/api.py patch_slate_post_dedup.py")
print("  git commit -m 'Slate API: post-merge dedup for cross-run total/f5 duplicates'")
print("  git push")
print()
print("After Railway redeploys, refresh the dashboard. Same-game total+f5")
print("pairs from different runs will collapse to one (larger |edge| wins).")
print("No orchestrator re-run needed — this is purely an API-layer fix.")
