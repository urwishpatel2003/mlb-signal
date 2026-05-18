"""
Run from repo root: python patch_edge_reasoning.py

Wires the new reasoning module into the system:
  1. Drops a new file backend/src/reasoning.py
  2. Patches orchestrator.py to call the appropriate reason_for_* fn per edge
     and attach reason_short / reason_factors to the edge dict
  3. Patches db.py insert_edge to write the new columns
  4. Reminds you to run migration 0007

Prereqs:
  - migration 0007_edge_reasoning.sql applied
"""
from pathlib import Path
import shutil

# ============================================================================
# 1. Drop reasoning.py into backend/src/
# ============================================================================
src_reasoning = Path("reasoning.py")
dst_reasoning = Path("backend/src/reasoning.py")
if not src_reasoning.exists():
    print("ERR: reasoning.py not found in repo root (alongside this script).")
    print("     Make sure you downloaded both files to the same folder.")
    raise SystemExit(1)
shutil.copy(src_reasoning, dst_reasoning)
print(f"OK: copied reasoning.py -> {dst_reasoning}")

# ============================================================================
# 2. Patch orchestrator.py
# ============================================================================
orch_path = Path("backend/src/orchestrator.py")
content = orch_path.read_text(encoding="utf-8")

# Add import
old_import = "from . import db, mlb_api, projections, ntfy, odds_props"
new_import = "from . import db, mlb_api, projections, ntfy, odds_props, reasoning"
if new_import not in content and old_import in content:
    content = content.replace(old_import, new_import, 1)
    print("OK: reasoning import added to orchestrator.py")

# Wire reasoning generation into the per-edge loop in run().
# Insert a call right before db.insert_edge that builds the reason and attaches it.
old_insert = '''            game_edges = dedupe_totals_per_game(game_edges)
            for e in game_edges:
                pft=(away_proj if e.get("pitcher_mlb_id")==(away_proj.pitcher_mlb_id if away_proj else None)
                     else (home_proj if e.get("pitcher_mlb_id")==(home_proj.pitcher_mlb_id if home_proj else None) else None))
                e["confidence_tier"]=confidence_tier(e,pft)
                db.insert_edge(run_id,e); all_edges.append(e)'''

new_insert = '''            game_edges = dedupe_totals_per_game(game_edges)
            # Build reasoning context shared across edges from this game
            _reason_ctx_base = {
                "away_proj": away_proj, "home_proj": home_proj,
                "park": park, "weather": weather,
                "away_team_xstats": all_team.get(g.away_team),
                "home_team_xstats": all_team.get(g.home_team),
                "market_total": market_total, "proj_total": full_total,
                "proj_f5": f5_total,
                "home_win_prob": home_win_prob, "away_win_prob": away_win_prob,
            }
            for e in game_edges:
                pft=(away_proj if e.get("pitcher_mlb_id")==(away_proj.pitcher_mlb_id if away_proj else None)
                     else (home_proj if e.get("pitcher_mlb_id")==(home_proj.pitcher_mlb_id if home_proj else None) else None))
                e["confidence_tier"]=confidence_tier(e,pft)
                # Attach reasoning
                try:
                    if e["kind"] == "total":
                        short, factors = reasoning.reason_for_total(e, _reason_ctx_base)
                    elif e["kind"] == "f5":
                        short, factors = reasoning.reason_for_f5(e, _reason_ctx_base)
                    elif e["kind"] == "ml":
                        short, factors = reasoning.reason_for_ml(e, _reason_ctx_base)
                    elif e["kind"] == "prop":
                        short, factors = reasoning.reason_for_prop(
                            e, {**_reason_ctx_base, "pitcher_proj": pft})
                    else:
                        short, factors = None, None
                    e["reason_short"] = short
                    e["reason_factors"] = factors
                except Exception as _re:
                    log.warning("Reasoning failed for edge %s: %s", e.get("kind"), _re)
                    e["reason_short"] = None
                    e["reason_factors"] = None
                db.insert_edge(run_id,e); all_edges.append(e)'''

if old_insert in content:
    content = content.replace(old_insert, new_insert, 1)
    print("OK: orchestrator wired to compute reasoning per edge")
elif "e[\"reason_short\"]" in content:
    print("OK: reasoning wiring already present in orchestrator")
else:
    print("WARN: insert-edge block pattern not found in orchestrator.py")
    print("     The dedup block may have shifted. Manual wiring required.")
    raise SystemExit(1)

orch_path.write_text(content, encoding="utf-8")

# ============================================================================
# 3. Patch db.py insert_edge to include reason_short, reason_factors
# ============================================================================
db_path = Path("backend/src/db.py")
db_content = db_path.read_text(encoding="utf-8")

old_insert_sql = '''def insert_edge(run_id, e):
    # Ensure over_price/under_price keys exist (default None for edges without prices)
    row = fetchone("""INSERT INTO edges (
          run_id,game_pk,kind,category,pitcher_mlb_id,pitcher_name,
          team_code,opp_team_code,line,proj_value,edge,lean,
          confidence_tier,conviction_pct,flagged,notes,
          over_price,under_price,stake_units
        ) VALUES (
          %(run_id)s,%(game_pk)s,%(kind)s,%(category)s,%(pitcher_mlb_id)s,%(pitcher_name)s,
          %(team_code)s,%(opp_team_code)s,%(line)s,%(proj_value)s,%(edge)s,%(lean)s,
          %(confidence_tier)s,%(conviction_pct)s,%(flagged)s,%(notes)s,
          %(over_price)s,%(under_price)s,%(stake_units)s
        ) RETURNING edge_id""",
        {"over_price": None, "under_price": None, "stake_units": 1.0, **e, "run_id": run_id})
    return int(row["edge_id"])'''

new_insert_sql = '''def insert_edge(run_id, e):
    import json as _json
    # Ensure optional keys exist with defaults
    row = fetchone("""INSERT INTO edges (
          run_id,game_pk,kind,category,pitcher_mlb_id,pitcher_name,
          team_code,opp_team_code,line,proj_value,edge,lean,
          confidence_tier,conviction_pct,flagged,notes,
          over_price,under_price,stake_units,
          reason_short,reason_factors
        ) VALUES (
          %(run_id)s,%(game_pk)s,%(kind)s,%(category)s,%(pitcher_mlb_id)s,%(pitcher_name)s,
          %(team_code)s,%(opp_team_code)s,%(line)s,%(proj_value)s,%(edge)s,%(lean)s,
          %(confidence_tier)s,%(conviction_pct)s,%(flagged)s,%(notes)s,
          %(over_price)s,%(under_price)s,%(stake_units)s,
          %(reason_short)s,%(reason_factors)s::jsonb
        ) RETURNING edge_id""",
        {"over_price": None, "under_price": None, "stake_units": 1.0,
         "reason_short": None, "reason_factors": None,
         **{k: (_json.dumps(v) if k == "reason_factors" and v is not None else v)
            for k, v in e.items()},
         "run_id": run_id})
    return int(row["edge_id"])'''

if old_insert_sql in db_content:
    db_content = db_content.replace(old_insert_sql, new_insert_sql, 1)
    db_path.write_text(db_content, encoding="utf-8")
    print("OK: db.insert_edge writes reason_short + reason_factors")
elif "reason_short" in db_content:
    print("OK: db.insert_edge already updated")
else:
    print("WARN: db.insert_edge pattern not found — manual update required")
    raise SystemExit(1)

print()
print("All patches applied.")
print()
print("Next steps:")
print("  1. Apply migration 0007 to your local DB:")
print("       Copy 0007_edge_reasoning.sql to backend/migrations/")
print("       cd backend && python -m scripts.bootstrap && cd ..")
print("  2. Verify:")
print("       python -c \"import ast; ast.parse(open('backend/src/orchestrator.py').read()); ast.parse(open('backend/src/db.py').read()); ast.parse(open('backend/src/reasoning.py').read()); print('OK')\"")
print("  3. Commit and push:")
print("       git add backend/migrations/0007_edge_reasoning.sql")
print("       git add backend/src/orchestrator.py backend/src/db.py backend/src/reasoning.py")
print("       git add patch_edge_reasoning.py reasoning.py 0007_edge_reasoning.sql")
print("       git commit -m 'Add edge reasoning: short summary + structured factors'")
print("       git push")
print("  4. After Railway deploys, apply migration on prod via the SQL console:")
print("       ALTER TABLE edges")
print("           ADD COLUMN IF NOT EXISTS reason_short TEXT,")
print("           ADD COLUMN IF NOT EXISTS reason_factors JSONB;")
print()
print("Tomorrow's 04:00 ET orchestrator run will populate the new fields.")
print("Existing edges get NULL — backfill skipped per choice.")
