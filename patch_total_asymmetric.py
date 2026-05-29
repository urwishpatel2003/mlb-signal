"""
Run from repo root: python patch_total_asymmetric.py

Asymmetric Total threshold: OVER needs |diff| >= 1.0, UNDER needs |diff| >= 0.5.
Calibration patch — model currently OVER-drifts (+0.35 bias). Revert to
symmetric when bias stabilizes.

F5 threshold unchanged (stays symmetric at 0.75).
"""
from pathlib import Path

p = Path("backend/src/orchestrator.py")
content = p.read_text(encoding="utf-8")

old = '        if abs(diff)>=EDGE_THRESHOLDS["Total"]:\n            lean="OVER" if diff>0 else "UNDER"'

new = '''        # Asymmetric Total threshold (calibration patch 2026-05-28):
        # OVER needs 1.0+ runs gap (model currently OVER-drifting).
        # UNDER keeps 0.5 default.
        # REVERT to symmetric when projection_bias avg_diff stabilizes near zero.
        _t_over_thresh  = 1.0
        _t_under_thresh = 0.5
        if (diff > 0 and diff >= _t_over_thresh) or (diff < 0 and abs(diff) >= _t_under_thresh):
            lean="OVER" if diff>0 else "UNDER"'''

if old in content:
    content = content.replace(old, new, 1)
    p.write_text(content, encoding="utf-8")
    print("OK: Total threshold now asymmetric (OVER 1.0 / UNDER 0.5)")
elif "_t_over_thresh" in content:
    print("OK: asymmetric Total threshold already applied")
else:
    print("ERR: anchor not found — paste current code around line 147")
    raise SystemExit(1)

print()
print("Verify and push:")
print('  python -X utf8 -c "import ast; ast.parse(open(\'backend/src/orchestrator.py\').read()); print(\'OK\')"')
print("  git add backend/src/orchestrator.py patch_total_asymmetric.py")
print("  git commit -m 'Total threshold: asymmetric OVER 1.0 / UNDER 0.5 (calibration patch)'")
print("  git push")
