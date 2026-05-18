"""
Run from repo root: python patch_totals_dedup_v2.py

Replaces the existing "correlated F5+Full -> 0.5u each" logic with the
"keep larger edge, drop smaller when same direction" rule.

Final behavior:
  - Every edge = 1 unit (no stake_units field anywhere)
  - For each game: if a full-game total AND an F5 total are both flagged
    AND lean the same direction -> keep only the larger |edge|, drop the other
  - If they conflict (different leans) -> keep both
  - ML and props unaffected

The helper dedupe_totals_per_game() may already exist from a prior patch run;
this script tolerates that.
"""
from pathlib import Path

orch_path = Path("backend/src/orchestrator.py")
content = orch_path.read_text(encoding="utf-8")

# ============================================================================
# Add dedupe_totals_per_game helper if not already present
# ============================================================================

if "def dedupe_totals_per_game(" in content:
    print("OK: dedupe_totals_per_game helper already present")
else:
    helper = '''def dedupe_totals_per_game(edges_for_game: list[dict]) -> list[dict]:
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
    return [e for e in edges_for_game if id(e) != drop_id]


'''
    anchor = "def compute_edges_for_game("
    if anchor in content:
        content = content.replace(anchor, helper + anchor, 1)
        print("OK: dedupe_totals_per_game helper added")
    else:
        print("ERR: anchor not found")
        raise SystemExit(1)

# ============================================================================
# Replace the half-stake correlation block with a single dedup call
# ============================================================================

old_block = '''            # Correlated edges: F5 + Full Game same direction -> 0.5u each
            _total_e = [e for e in game_edges if e["kind"]=="total"]
            _f5_e    = [e for e in game_edges if e["kind"]=="f5"]
            for _te in _total_e:
                for _fe in _f5_e:
                    if _te["lean"] == _fe["lean"]:
                        _te["stake_units"] = 0.5
                        _fe["stake_units"] = 0.5
                        log.info("Correlated F5+Full: %s @ %s %s -> 0.5u each",
                                 _te["team_code"], _te["opp_team_code"], _te["lean"])
            for e in game_edges:
                e.setdefault("stake_units", 1.0)
                pft=(away_proj if e.get("pitcher_mlb_id")==(away_proj.pitcher_mlb_id if away_proj else None)
                     else (home_proj if e.get("pitcher_mlb_id")==(home_proj.pitcher_mlb_id if home_proj else None) else None))
                e["confidence_tier"]=confidence_tier(e,pft)
                db.insert_edge(run_id,e); all_edges.append(e)'''

new_block = '''            # Dedup totals: keep larger of full-game vs F5 when leaning same direction.
            # Different leans -> keep both. All edges remain 1 unit.
            game_edges = dedupe_totals_per_game(game_edges)
            for e in game_edges:
                pft=(away_proj if e.get("pitcher_mlb_id")==(away_proj.pitcher_mlb_id if away_proj else None)
                     else (home_proj if e.get("pitcher_mlb_id")==(home_proj.pitcher_mlb_id if home_proj else None) else None))
                e["confidence_tier"]=confidence_tier(e,pft)
                db.insert_edge(run_id,e); all_edges.append(e)'''

if old_block in content:
    content = content.replace(old_block, new_block, 1)
    print("OK: half-stake correlation block replaced with dedup")
elif "dedupe_totals_per_game(game_edges)" in content:
    print("OK: dedup wiring already present")
else:
    print("ERR: could not find half-stake block to replace")
    print("     Open backend/src/orchestrator.py and look around the line:")
    print("       '# Correlated edges: F5 + Full Game same direction'")
    print("     The exact whitespace may have drifted. Aborting without writing.")
    raise SystemExit(1)

# ============================================================================
# Bump module version comment
# ============================================================================
old_header = '"""Daily orchestrator v4.1 — prop prices stored on edges for correct grading."""'
new_header = '"""Daily orchestrator v4.2 — totals dedup (keep larger of full-game/F5 when same direction)."""'
# Match both UTF-8 em-dash and a likely-corrupted fallback
for old in [old_header,
            '"""Daily orchestrator v4.1 -- prop prices stored on edges for correct grading."""']:
    if old in content:
        content = content.replace(old, new_header, 1)
        print("OK: header bumped to v4.2")
        break

orch_path.write_text(content, encoding="utf-8")
print()
print("All patches applied. Verify with:")
print("  python -c \"import ast; ast.parse(open('backend/src/orchestrator.py').read()); print('OK')\"")
print("  git diff backend/src/orchestrator.py")
