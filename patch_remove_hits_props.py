"""
Run from repo root: python patch_remove_hits_props.py

SCOPE A — remove "Hits" as a pitcher PROP everywhere, while LEAVING the
underlying hits projection intact (projections.py hits_proj, the `hits`
dataclass field, the `hits` DB column, and the Pitchers-table "H" column are
untouched). No DB migration, no test changes, fully reversible.

Edits:
  1. odds_props.py   — drop Hits from PROP_MARKETS (stops Odds API fetch)
  2. dk_props.py     — drop Hits from SUBCAT_KEYS (stops DraftKings fetch)
  3. orchestrator.py — drop Hits from EDGE_THRESHOLDS and proj_vals (no Hits
                       edge can be generated even if a stray line leaks in)
  4. api.py          — Track Record: hide category='Hits' in performance_overall
                       and performance_by_date (historical rows stay in DB)
  5. App.jsx         — drop Hits from PROP_CATEGORIES, MARKET_LABELS, deck text

Idempotent and anchor-gated: each edit is applied only if its exact anchor is
present; if already applied it is skipped; if the anchor is missing it WARNs
and that file is left untouched for that edit. A .bak is written per changed
file. reason_for_prop()'s Hits branch in reasoning.py becomes dead code but is
harmless, so it is left in place.
"""
from pathlib import Path

# (label, old, new) — order-gated: we test `old` first, so a `new` that is a
# substring of `old` does not cause a false "already applied".
EDITS = {
    "backend/src/odds_props.py": [
        ("PROP_MARKETS: drop Hits",
'''PROP_MARKETS = {
    "K":    "pitcher_strikeouts",
    "Outs": "pitcher_outs",
    "ER":   "pitcher_earned_runs",
    "Hits": "pitcher_hits_allowed",
}''',
'''PROP_MARKETS = {
    "K":    "pitcher_strikeouts",
    "Outs": "pitcher_outs",
    "ER":   "pitcher_earned_runs",
}'''),
    ],
    "backend/src/dk_props.py": [
        ("SUBCAT_KEYS: drop Hits",
'''SUBCAT_KEYS = {
    "K":    "strikeouts-thrown",
    "Outs": "outs",
    "ER":   "earned-runs",
    "Hits": "hits-allowed",
}''',
'''SUBCAT_KEYS = {
    "K":    "strikeouts-thrown",
    "Outs": "outs",
    "ER":   "earned-runs",
}'''),
    ],
    "backend/src/orchestrator.py": [
        ("EDGE_THRESHOLDS: drop Hits",
'EDGE_THRESHOLDS = {"Total":0.50,"F5":0.75,"ML":0.20,"K":0.50,"Hits":0.70,"ER":0.50,"Outs":0.70}',
'EDGE_THRESHOLDS = {"Total":0.50,"F5":0.75,"ML":0.20,"K":0.50,"ER":0.50,"Outs":0.70}'),
        ("proj_vals: drop Hits",
'proj_vals={"K":p.k,"Hits":p.hits,"ER":p.er,"Outs":p.outs}',
'proj_vals={"K":p.k,"ER":p.er,"Outs":p.outs}'),
    ],
    "backend/src/api.py": [
        ("performance_overall: hide Hits",
'''        FROM edge_results er
        JOIN edges e ON e.edge_id = er.edge_id
        WHERE e.flagged = TRUE
        GROUP BY e.kind, e.category, e.lean''',
'''        FROM edge_results er
        JOIN edges e ON e.edge_id = er.edge_id
        WHERE e.flagged = TRUE
          AND e.category IS DISTINCT FROM 'Hits'
        GROUP BY e.kind, e.category, e.lean'''),
        ("performance_by_date: hide Hits",
'''        WHERE e.flagged = TRUE
          AND (e.lean IN ('OVER','UNDER') OR e.kind = 'ml')
        ORDER BY pr.run_date DESC, e.kind, e.category, e.lean,''',
'''        WHERE e.flagged = TRUE
          AND (e.lean IN ('OVER','UNDER') OR e.kind = 'ml')
          AND e.category IS DISTINCT FROM 'Hits'
        ORDER BY pr.run_date DESC, e.kind, e.category, e.lean,'''),
    ],
    "frontend/src/App.jsx": [
        ("PROP_CATEGORIES: drop Hits",
"const PROP_CATEGORIES = ['K', 'Outs', 'ER', 'Hits', 'Walks'];",
"const PROP_CATEGORIES = ['K', 'Outs', 'ER', 'Walks'];"),
        ("MARKET_LABELS: drop Hits",
"  K: 'Pitcher Strikeouts', Hits: 'Pitcher Hits Allowed',",
"  K: 'Pitcher Strikeouts',"),
        ("deck text: drop Hits",
"K, Hits, ER, Outs &mdash;",
"K, ER, Outs &mdash;"),
    ],
}


def apply_file(path_str, edits):
    p = Path(path_str)
    if not p.exists():
        print(f"SKIP {path_str}: file not found")
        return
    original = p.read_text(encoding="utf-8")
    content = original
    any_change = False
    for label, old, new in edits:
        if old in content:
            content = content.replace(old, new, 1)
            any_change = True
            print(f"  OK   [{path_str}] {label}")
        elif new in content:
            print(f"  skip [{path_str}] {label} (already applied)")
        else:
            print(f"  WARN [{path_str}] {label}: anchor not found — left untouched")
    if any_change:
        Path(str(p) + ".bak").write_text(original, encoding="utf-8")
        p.write_text(content, encoding="utf-8")
        print(f"  -> wrote {path_str} (backup: {path_str}.bak)")


def main():
    print("Removing 'Hits' as a pitcher prop (Scope A)...\n")
    for path_str, edits in EDITS.items():
        apply_file(path_str, edits)
    print()
    print("Verify, then commit:")
    print('  python -X utf8 -c "import ast; ast.parse(open(\'backend/src/odds_props.py\').read()); '
          'ast.parse(open(\'backend/src/dk_props.py\').read()); '
          'ast.parse(open(\'backend/src/orchestrator.py\').read()); '
          'ast.parse(open(\'backend/src/api.py\').read()); print(\'py OK\')"')
    print("  npx esbuild frontend/src/App.jsx --bundle --outfile=/dev/null --loader:.jsx=jsx  # jsx sanity")
    print("  git add backend/src/odds_props.py backend/src/dk_props.py backend/src/orchestrator.py "
          "backend/src/api.py frontend/src/App.jsx patch_remove_hits_props.py")
    print("  git commit -m 'Remove Hits pitcher prop (fetch + edge-gen + UI + track record); keep internal hits projection'")
    print("  git push")
    print()
    print("No migration. Historical Hits rows stay in the DB (hidden, reversible).")


if __name__ == "__main__":
    main()
