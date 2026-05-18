"""
Run from repo root: python patch_disable_l7_bullpen.py

Disables the L7 (last-7-day) bullpen ERA blend that's driving the
~1.5 run projection drift. The 7-day window is too noisy and gets 40%
weight in the bullpen ER9 calc, dragging totals down across the board.

Restores the function to pre-Improvement-#3 behavior: season bullpen ERA
with Bayesian shrinkage toward league average. The data column stays
populated (so refresh keeps writing it), we just stop reading it.

Effect: projections should snap back to ~May 4-7 calibration immediately
on the next orchestrator run.

This is reversible — to re-enable later, flip `_L7_BLEND_ENABLED` to True
or just remove the gate.
"""
from pathlib import Path

f = Path("backend/src/projections.py")
content = f.read_text(encoding="utf-8")

old_block = '''    # Improvement #3: blend in L7 when available
    l7_era = team_xstats_row.get("bullpen_era_l7")
    l7_ip  = float(team_xstats_row.get("bullpen_ip_l7") or 0)
    if l7_era is not None and l7_ip >= 10:
        l7_weight = min(0.40, l7_ip / 40.0)   # max 40% weight at 40+ IP in 7 days
        return round(l7_weight * float(l7_era) + (1 - l7_weight) * season_er9, 3)

    return round(season_er9, 3)'''

new_block = '''    # Improvement #3 (DISABLED 2026-05-17): L7 blend caused systematic
    # ~1.5 run projection drift. SD bullpen showed 0.39 ERA over L7, which
    # at 40% weight collapsed bullpen ER9 projections far below realistic
    # values across the slate. Reverting to season-only ER9 with Bayesian
    # shrinkage. Re-enable after redesigning L7 weight + shrinkage.
    _L7_BLEND_ENABLED = False
    l7_era = team_xstats_row.get("bullpen_era_l7")
    l7_ip  = float(team_xstats_row.get("bullpen_ip_l7") or 0)
    if _L7_BLEND_ENABLED and l7_era is not None and l7_ip >= 10:
        l7_weight = min(0.40, l7_ip / 40.0)
        return round(l7_weight * float(l7_era) + (1 - l7_weight) * season_er9, 3)

    return round(season_er9, 3)'''

if old_block in content:
    content = content.replace(old_block, new_block, 1)
    f.write_text(content, encoding="utf-8")
    print("OK: L7 bullpen blend disabled in _compute_team_bullpen_er9")
    print()
    print("Next steps:")
    print("  python -c \"import ast; ast.parse(open('backend/src/projections.py').read()); print('OK')\"")
    print("  git add backend/src/projections.py patch_disable_l7_bullpen.py")
    print("  git commit -m 'Disable L7 bullpen blend (caused 1.5 run projection drift)'")
    print("  git push")
    print()
    print("Effect: next orchestrator run will use season-only bullpen ER9,")
    print("        restoring May 4-7 calibration (~0.0 avg_diff).")
    print("        L7 data keeps being collected; only the read path is gated.")
else:
    print("ERR: old L7 blend block not found in projections.py")
    print("     File may have already been patched or pattern shifted.")
    print("     Open backend/src/projections.py and search for:")
    print("       '# Improvement #3: blend in L7 when available'")
    print("     Then manually wrap the if-block in:  if False and ...")
    raise SystemExit(1)
