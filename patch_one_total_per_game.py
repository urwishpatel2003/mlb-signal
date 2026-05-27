"""
Run from repo root: python patch_one_total_per_game.py

Current behavior:
  - Same-direction F5 + Full Game on same game -> keep the larger edge (dedup'd)
  - Opposite-direction F5 + Full Game on same game -> KEEP BOTH

New behavior:
  - Per game: keep only ONE total edge (whichever has larger |edge|)
  - Same direction or opposite direction, doesn't matter — always one total per game
  - ML and prop edges unaffected

This trades off some signal (opposite-direction picks were intentional in the
old design) for cleaner output (no game has two total bets going in
contradictory directions).
"""
from pathlib import Path

f = Path("backend/src/orchestrator.py")
content = f.read_text(encoding="utf-8")

old_func = '''def dedupe_totals_per_game(edges_for_game: list[dict]) -> list[dict]:
    """
    For totals only: collapse the full-game total + F5 total down to one edge
    when they lean the same direction. Larger |edge| wins.

    Conflict case (different leans) -> keep both.
    ML, prop, and any non-total edges pass through unchanged.
    """
    total_edge = next((e for e in edges_for_game if e.get("kind") == "total"), None)
    f5_edge    = next((e for e in edges_for_game if e.get("kind") == "f5"), None)

    if not total_edge or not f5_edge:
        return edges_for_game

    if total_edge.get("lean") != f5_edge.get("lean"):
        return edges_for_game

    keep = total_edge if abs(total_edge["edge"]) >= abs(f5_edge["edge"]) else f5_edge
    drop = f5_edge    if keep is total_edge else total_edge
    drop_id = id(drop)
    return [e for e in edges_for_game if id(e) != drop_id]'''

new_func = '''def dedupe_totals_per_game(edges_for_game: list[dict]) -> list[dict]:
    """
    For totals only: keep at most ONE total edge per game (full-game OR F5).
    Larger |edge| wins regardless of direction. Updated 2026-05-26 to dedup
    opposite-direction picks too — we never want two total bets on the same
    game going in contradictory directions.

    ML, prop, and any non-total edges pass through unchanged.
    """
    total_edge = next((e for e in edges_for_game if e.get("kind") == "total"), None)
    f5_edge    = next((e for e in edges_for_game if e.get("kind") == "f5"), None)

    if not total_edge or not f5_edge:
        return edges_for_game

    keep = total_edge if abs(total_edge["edge"]) >= abs(f5_edge["edge"]) else f5_edge
    drop = f5_edge    if keep is total_edge else total_edge
    drop_id = id(drop)
    return [e for e in edges_for_game if id(e) != drop_id]'''

if old_func in content:
    content = content.replace(old_func, new_func, 1)
    f.write_text(content, encoding="utf-8")
    print("OK: dedupe_totals_per_game now keeps only one total per game (any direction)")
elif "Updated 2026-05-26 to dedup" in content:
    print("OK: dedup already updated")
else:
    print("ERR: dedupe_totals_per_game function not found in expected form")
    print("     The function may have been edited manually. Check orchestrator.py")
    raise SystemExit(1)

print()
print("Effect from tomorrow's orchestrator run:")
print("  - Same-direction F5+Total -> one (larger) kept, as before")
print("  - Opposite-direction F5+Total -> NEW: one (larger) kept, other dropped")
print("  - Total edges flagged per slate: roughly halved")
print()
print("Steps:")
print('  python -X utf8 -c "import ast; ast.parse(open(\'backend/src/orchestrator.py\').read()); print(\'OK\')"')
print("  git diff backend/src/orchestrator.py")
print("  git add backend/src/orchestrator.py patch_one_total_per_game.py")
print("  git commit -m 'Dedup: one total edge per game regardless of direction'")
print("  git push")
