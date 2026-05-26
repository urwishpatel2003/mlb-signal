"""
Run from repo root: python patch_er9_undercounting_fix.py

Root cause of the ~1.5-run UNDER bias:

  Line 529 of backend/src/projections.py:
    er9 = (true_era + woba_delta * 7) * wx_run * pf_runs * 0.65
         + _hr9 * 1.4 * 0.35

The `* 0.65` weighting on the main term scales the bulk of the ER
projection down by 35%. The HR-based second term was supposed to make up
the difference, but it only adds ~0.26 ER9 for an average pitcher when it
needed to add ~1.5 to be balanced.

For an average pitcher (true_era 4.50, neutral park/weather):
  Main term:  4.50 × 0.65 = 2.93     (should be ~4.50)
  HR term:    ~0.26
  TOTAL:      3.19 ER9                 (should be ~4.50)
  Undercount: ~1.3 ER9 per starter
  Per game:   ~1.5 runs total (2 starters × ~0.83 ER)

This matches the observed -1.0 to -1.5 run projection bias exactly.

The fix: restore the simpler formula and add HR as a small additive
adjustment driven by park HR factor (so HR-friendly parks like Coors
still produce slightly higher projections), not as a 35% weight.
"""
from pathlib import Path

f = Path("backend/src/projections.py")
content = f.read_text(encoding="utf-8")

old_line = "    er9 = (true_era + woba_delta * 7) * wx_run * pf_runs * 0.65 + _hr9 * 1.4 * 0.35"

new_block = """    # FIX 2026-05-26: removed structural 0.65/0.35 split that undercounted ER9
    # by ~1.5 runs per pitcher. Main term now produces full true_era; HR
    # context adds a small +/- adjustment when park HR factor is non-neutral.
    er9 = (true_era + woba_delta * 7) * wx_run * pf_runs
    # HR-park adjustment: small +/- nudge for HR-friendly / HR-suppressed parks
    # _hr9 baseline (~0.53 for avg pitcher) deviations get a 0.4x effect
    _hr9_baseline = LEAGUE_FB_PCT * LEAGUE_HR_FB * (38.0 / 3.0)   # ~0.53
    er9 += (_hr9 - _hr9_baseline) * 0.4"""

if old_line in content:
    content = content.replace(old_line, new_block, 1)
    f.write_text(content, encoding="utf-8")
    print("OK: er9 formula restored to true_era baseline + HR-park nudge")
elif "FIX 2026-05-26: removed structural 0.65/0.35 split" in content:
    print("OK: fix already applied")
else:
    print("ERR: er9 line not found in projections.py at expected location")
    print("     Expected exact line:")
    print(f"     {old_line!r}")
    raise SystemExit(1)

print()
print("Expected effect:")
print("  - avg_proj_er9 should jump from ~3.2 to ~4.5 (matching true_era)")
print("  - avg_proj per game should jump from ~7.0 to ~8.5 (matching market)")
print("  - n_over and n_under should be roughly balanced again")
print("  - HR-friendly parks (Coors, GABP) still get small bump from HR adjustment")
print()
print("Steps:")
print('  python -X utf8 -c "import ast; ast.parse(open(\'backend/src/projections.py\').read()); print(\'OK\')"')
print("  git diff backend/src/projections.py")
print("  git add backend/src/projections.py patch_er9_undercounting_fix.py")
print("  git commit -m 'Fix er9 formula: remove 0.65/0.35 split that undercounted ER by ~1.5 runs'")
print("  git push")
print()
print("After deploy, trigger orchestrator manually to regenerate today's slate:")
print("  Admin panel > Run Orchestrator")
print("  (or /api/admin/trigger/orchestrator/YOUR_TOKEN)")
print()
print("Then verify via /api/admin/diag/projection_bias/YOUR_TOKEN")
print("  Today's avg_diff should snap from ~-1.3 to near 0.")
