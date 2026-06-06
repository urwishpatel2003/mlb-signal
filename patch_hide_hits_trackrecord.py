"""
Run from repo root: python patch_hide_hits_trackrecord.py

Adds the `AND e.category IS DISTINCT FROM 'Hits'` filter to the two Track
Record queries in backend/src/api.py:
  - performance_by_date  (daily cards)
  - performance_overall  (overall + pitcher-prop card)

Anchored to the file's CURRENT state (verified via Select-String). Idempotent
and anchor-gated: applies only if the exact anchor is present; skips if already
applied; WARNs and leaves the file untouched if the anchor is missing. Writes a
.bak before changing anything. Non-destructive — historical Hits rows stay in
the DB, just hidden. Undo by deleting the two added lines.
"""
from pathlib import Path

PATH = "backend/src/api.py"

EDITS = [
    # performance_by_date — insert the filter before the ORDER BY line
    ("performance_by_date: hide Hits",
'''          AND (e.lean IN ('OVER','UNDER') OR e.kind = 'ml')
        ORDER BY pr.run_date DESC, e.kind, e.category, e.lean,''',
'''          AND (e.lean IN ('OVER','UNDER') OR e.kind = 'ml')
          AND e.category IS DISTINCT FROM 'Hits'
        ORDER BY pr.run_date DESC, e.kind, e.category, e.lean,'''),

    # performance_overall — insert the filter between WHERE and GROUP BY
    ("performance_overall: hide Hits",
'''        WHERE e.flagged = TRUE
        GROUP BY e.kind, e.category, e.lean''',
'''        WHERE e.flagged = TRUE
          AND e.category IS DISTINCT FROM 'Hits'
        GROUP BY e.kind, e.category, e.lean'''),
]


def main():
    p = Path(PATH)
    if not p.exists():
        print(f"ERR: {PATH} not found — run from repo root.")
        raise SystemExit(1)
    original = p.read_text(encoding="utf-8")
    content = original
    changed = False
    for label, old, new in EDITS:
        if old in content:
            content = content.replace(old, new, 1)
            changed = True
            print(f"  OK   {label}")
        elif new in content:
            print(f"  skip {label} (already applied)")
        else:
            print(f"  WARN {label}: anchor not found — left untouched")
    if changed:
        Path(PATH + ".bak").write_text(original, encoding="utf-8")
        p.write_text(content, encoding="utf-8")
        print(f"\n  -> wrote {PATH} (backup: {PATH}.bak)")
    else:
        print("\n  no changes written")
    print()
    print("Verify, then ship:")
    print(r'  python -X utf8 -c "import ast; ast.parse(open(' + repr(PATH) + r").read()); print('py OK')\"")
    print(f"  git add {PATH} patch_hide_hits_trackrecord.py")
    print('  git commit -m "Track Record: hide Hits prop from daily + overall (reversible)"')
    print("  git push   # Railway redeploys; refresh Track Record")


if __name__ == "__main__":
    main()
