"""
Run from repo root: python patch_exclude_props_from_record.py

Props should not count toward the cumulative all-time record or the daily
record (neither W/L nor units), but should STILL be tracked separately in
the per-category / per-bucket breakdown (which the frontend already shows
as its own props sub-section).

Two surgical changes in backend/src/api.py:

  1. performance_overall(): skip props when building the `overall` aggregate.
     Props stay in by_category so the frontend props sub-breakdown still works.

  2. performance_by_date(): skip props when tallying each day's `summary`.
     Props stay in buckets so the per-day props breakdown still works.

No frontend changes needed — OverallCard already separates props into their
own bucket (byCategory.filter(r=>r.kind==='prop')).
"""
from pathlib import Path

f = Path("backend/src/api.py")
content = f.read_text(encoding="utf-8")

# ============================================================================
# 1. performance_overall — exclude props from the `overall` aggregate
# ============================================================================
old_overall = '''    overall = {"wins": 0, "losses": 0, "pushes": 0, "profit_units": 0.0}
    by_category = []
    for r in rows:
        wins = int(r["wins"] or 0)
        losses = int(r["losses"] or 0)
        pushes = int(r["pushes"] or 0)
        profit = float(r["profit_units"] or 0)
        overall["wins"] += wins
        overall["losses"] += losses
        overall["pushes"] += pushes
        overall["profit_units"] = round(overall["profit_units"] + profit, 2)
        by_category.append({'''

new_overall = '''    overall = {"wins": 0, "losses": 0, "pushes": 0, "profit_units": 0.0}
    by_category = []
    for r in rows:
        wins = int(r["wins"] or 0)
        losses = int(r["losses"] or 0)
        pushes = int(r["pushes"] or 0)
        profit = float(r["profit_units"] or 0)
        # Props are tracked separately (in by_category) and excluded from the
        # cumulative all-time record per product decision.
        if r["kind"] != "prop":
            overall["wins"] += wins
            overall["losses"] += losses
            overall["pushes"] += pushes
            overall["profit_units"] = round(overall["profit_units"] + profit, 2)
        by_category.append({'''

if old_overall in content:
    content = content.replace(old_overall, new_overall, 1)
    print("OK: performance_overall excludes props from cumulative record")
elif 'if r["kind"] != "prop":' in content:
    print("OK: performance_overall already excludes props")
else:
    print("WARN: performance_overall aggregate pattern not found")

# ============================================================================
# 2. performance_by_date — exclude props from each day's summary tally
# ============================================================================
old_daily = '''        result = r["result"]
        profit = float(r["profit_units"] or 0)
        # Tally summary
        if result == "WIN":  by_date[d]["summary"]["wins"] += 1
        elif result == "LOSS": by_date[d]["summary"]["losses"] += 1
        elif result == "PUSH": by_date[d]["summary"]["pushes"] += 1
        by_date[d]["summary"]["profit_units"] = round(
            by_date[d]["summary"]["profit_units"] + profit, 2
        )'''

new_daily = '''        result = r["result"]
        profit = float(r["profit_units"] or 0)
        # Tally summary — props excluded from daily record (tracked in buckets).
        if r["kind"] != "prop":
            if result == "WIN":  by_date[d]["summary"]["wins"] += 1
            elif result == "LOSS": by_date[d]["summary"]["losses"] += 1
            elif result == "PUSH": by_date[d]["summary"]["pushes"] += 1
            by_date[d]["summary"]["profit_units"] = round(
                by_date[d]["summary"]["profit_units"] + profit, 2
            )'''

if old_daily in content:
    content = content.replace(old_daily, new_daily, 1)
    print("OK: performance_by_date excludes props from daily summary")
elif 'props excluded from daily record' in content:
    print("OK: performance_by_date already excludes props")
else:
    print("WARN: performance_by_date summary tally pattern not found")

f.write_text(content, encoding="utf-8")

print()
print("Steps:")
print('  python -X utf8 -c "import ast; ast.parse(open(\'backend/src/api.py\').read()); print(\'OK\')"')
print("  git add backend/src/api.py patch_exclude_props_from_record.py")
print("  git commit -m 'Track record: exclude props from cumulative + daily totals (keep in breakdown)'")
print("  git push")
print()
print("No frontend changes, no migration, no grader re-run needed.")
print("After deploy + dashboard refresh:")
print("  - All-time card W/L + units exclude props")
print("  - Daily card W/L + units exclude props")
print("  - Props still shown in their own sub-breakdown section (unchanged)")
