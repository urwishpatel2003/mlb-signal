"""
Run from repo root: python patch_ml_very_high_conviction.py

Tightens moneyline edge logic to flag only very high conviction plays:

  1. Favorite edge threshold: 10pp -> 20pp
  2. Underdog edge threshold (line >= +150 or +100): 50pp -> 60pp
  3. Both starters must be source='statcast' (no low_sample or league_avg fallback)

Updates the deck text in the frontend so the disclaimer reflects the new rule.
"""
from pathlib import Path

orch_path = Path("backend/src/orchestrator.py")
app_path  = Path("frontend/src/App.jsx")
orch      = orch_path.read_text(encoding="utf-8")
app       = app_path.read_text(encoding="utf-8")

# ============================================================================
# 1. Bump EDGE_THRESHOLDS["ML"] from 0.10 to 0.20
# ============================================================================
old_thresh = 'EDGE_THRESHOLDS = {"Total":0.50,"F5":0.35,"ML":0.10,"K":0.50,"Hits":0.70,"ER":0.50,"Outs":0.70}'
new_thresh = 'EDGE_THRESHOLDS = {"Total":0.50,"F5":0.35,"ML":0.20,"K":0.50,"Hits":0.70,"ER":0.50,"Outs":0.70}'

if old_thresh in orch:
    orch = orch.replace(old_thresh, new_thresh, 1)
    print("OK: EDGE_THRESHOLDS['ML'] bumped 0.10 -> 0.20 (20pp favorites)")
elif '"ML":0.20' in orch:
    print("OK: ML threshold already at 0.20")
else:
    print("WARN: EDGE_THRESHOLDS line not found")

# ============================================================================
# 2. Tighten dog logic + add Statcast-source guard
# ============================================================================
old_ml_block = '''    if away_ml and home_ml and projections.ml_edge_reliable(away_proj,home_proj):
        ai,hi=remove_vig(american_to_implied(away_ml),american_to_implied(home_ml))
        hep=home_win_prob-hi; aep=away_win_prob-ai
        # Higher threshold for big underdogs (+150 or longer) — noisy projections
        def ml_threshold(odds): return 0.50 if odds is not None and odds >= 150 else EDGE_THRESHOLDS["ML"]
        if hep > 0 and hep>=ml_threshold(home_ml) and hep>=aep:
            ml_lean,ml_odds,wp,ep,oi=game.get("home_team"),home_ml,home_win_prob,hep,hi
        elif aep > 0 and aep>=ml_threshold(away_ml):
            ml_lean,ml_odds,wp,ep,oi=game.get("away_team"),away_ml,away_win_prob,aep,ai
        else: ml_lean=None'''

new_ml_block = '''    # ML — very high conviction only:
    #   - Both pitchers must be source='statcast' (no fallback projections)
    #   - Favorites need 20pp edge (EDGE_THRESHOLDS["ML"] = 0.20)
    #   - Underdogs (+100 or longer) need 60pp edge (effectively almost never)
    both_statcast = (away_proj.source == "statcast" and home_proj.source == "statcast")
    if (away_ml and home_ml and both_statcast
            and projections.ml_edge_reliable(away_proj, home_proj)):
        ai,hi=remove_vig(american_to_implied(away_ml),american_to_implied(home_ml))
        hep=home_win_prob-hi; aep=away_win_prob-ai
        def ml_threshold(odds):
            # Tightened: any non-favorite (+100 or longer) needs 60pp; fav needs 20pp
            return 0.60 if odds is not None and odds >= 100 else EDGE_THRESHOLDS["ML"]
        if hep > 0 and hep>=ml_threshold(home_ml) and hep>=aep:
            ml_lean,ml_odds,wp,ep,oi=game.get("home_team"),home_ml,home_win_prob,hep,hi
        elif aep > 0 and aep>=ml_threshold(away_ml):
            ml_lean,ml_odds,wp,ep,oi=game.get("away_team"),away_ml,away_win_prob,aep,ai
        else: ml_lean=None'''

if old_ml_block in orch:
    orch = orch.replace(old_ml_block, new_ml_block, 1)
    print("OK: ML logic tightened (statcast-only, 60pp dog threshold)")
elif "both_statcast" in orch:
    print("OK: ML tightening already applied")
else:
    print("WARN: ML block pattern not found in orchestrator.py")

# Apply orchestrator changes
orch_path.write_text(orch, encoding="utf-8")

# ============================================================================
# 3. Update frontend deck text so the disclaimer matches the new rule
# ============================================================================
old_deck_text = 'Skellam win probability vs vig-free implied odds &mdash; 4pp minimum edge'
new_deck_text = 'Very high conviction only &mdash; 20pp min for favorites, 60pp for dogs, both SP must be Statcast'

if old_deck_text in app:
    app = app.replace(old_deck_text, new_deck_text, 1)
    print("OK: frontend ML deck text updated")
elif new_deck_text in app:
    print("OK: frontend deck text already updated")
else:
    print("WARN: frontend deck text pattern not found (non-fatal — check manually)")

old_disclaimer = 'ML edges use Skellam distribution on projected run totals. Min threshold 4pp.'
new_disclaimer = 'ML edges use Skellam distribution on projected run totals. Very high conviction only: 20pp min for favorites, 60pp for underdogs.'

if old_disclaimer in app:
    app = app.replace(old_disclaimer, new_disclaimer, 1)
    print("OK: frontend ML disclaimer updated")
elif new_disclaimer in app:
    print("OK: frontend disclaimer already updated")
else:
    print("WARN: frontend disclaimer pattern not found (non-fatal)")

app_path.write_text(app, encoding="utf-8")

print()
print("Steps:")
print('  python -X utf8 -c "import ast; ast.parse(open(\'backend/src/orchestrator.py\').read()); print(\'OK\')"')
print("  git add backend/src/orchestrator.py frontend/src/App.jsx patch_ml_very_high_conviction.py")
print("  git commit -m 'ML: very high conviction only (20pp fav, 60pp dog, Statcast-required)'")
print("  git push")
print()
print("Effect from tomorrow's orchestrator run:")
print("  - Expect 0-2 ML flags per slate (vs the current 5-8)")
print("  - Only fires when both starters have full Statcast samples")
print("  - Underdog flags effectively eliminated (60pp is rare)")
print()
print("Existing ML edges from prior runs are NOT removed — they stay flagged.")
print("To wipe them from earlier dates, see the recompute_reasoning pattern or")
print("set flagged=FALSE in the DB for any kind='ml' edges you want hidden.")
