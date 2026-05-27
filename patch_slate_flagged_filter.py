"""
Run from repo root: python patch_slate_flagged_filter.py

The slate edges query was missing `AND e.flagged = TRUE`. Cleanup correctly
unflagged 103 duplicates but they still appeared on the dashboard because
the slate endpoint pulls every edge for the date regardless of flagged.

One-line fix to the WHERE clause.
"""
from pathlib import Path

f = Path("backend/src/api.py")
content = f.read_text(encoding="utf-8")

old_query = '''        SELECT DISTINCT ON (e.game_pk, e.kind, e.category, COALESCE(e.pitcher_mlb_id, 0), e.lean)
               e.*
        FROM edges e
        JOIN projection_runs pr ON pr.run_id = e.run_id
        WHERE pr.run_date = %s
        ORDER BY e.game_pk, e.kind, e.category, COALESCE(e.pitcher_mlb_id, 0), e.lean, e.run_id DESC'''

new_query = '''        SELECT DISTINCT ON (e.game_pk, e.kind, e.category, COALESCE(e.pitcher_mlb_id, 0), e.lean)
               e.*
        FROM edges e
        JOIN projection_runs pr ON pr.run_id = e.run_id
        WHERE pr.run_date = %s AND e.flagged = TRUE
        ORDER BY e.game_pk, e.kind, e.category, COALESCE(e.pitcher_mlb_id, 0), e.lean, e.run_id DESC'''

if old_query in content:
    content = content.replace(old_query, new_query, 1)
    f.write_text(content, encoding="utf-8")
    print("OK: slate edges query now filters on e.flagged = TRUE")
elif "WHERE pr.run_date = %s AND e.flagged = TRUE" in content:
    print("OK: already filtered")
else:
    print("ERR: slate query pattern not found")
    raise SystemExit(1)

print()
print("Steps:")
print('  python -X utf8 -c "import ast; ast.parse(open(\'backend/src/api.py\').read()); print(\'OK\')"')
print("  git add backend/src/api.py patch_slate_flagged_filter.py")
print("  git commit -m 'Slate: filter edges on flagged=TRUE'")
print("  git push")
print()
print("After Railway redeploys, refresh dashboard. The 103 unflagged duplicates")
print("from the cleanup will no longer appear.")
