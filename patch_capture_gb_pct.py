"""
Run from repo root: python patch_capture_gb_pct.py

GB% is empty because _refresh_pitcher_fb_pct reads fb_rate from the Savant
batted-ball CSV but never reads gb_rate (which is in the SAME csv:
"id, name, bbe, gb_rate, air_rate, fb_rate, ld_rate, ...").

Three tiny edits:
  1. Parse gb_rate alongside fb_rate (same scale normalization)
  2. Add gb_pct to the update_rows dict
  3. Add gb_pct to the UPDATE SQL

No new fetch, no new endpoint. GB% populates on the next statcast refresh.
The pitcher_xstats.gb_pct column and the frontend column already exist.
"""
from pathlib import Path

f = Path("backend/src/statcast_refresh.py")
content = f.read_text(encoding="utf-8")

# ============================================================================
# 1. Parse gb_rate right after fb_rate
# ============================================================================
old_parse = '''            pid      = int(row.get("id") or 0)
            fb_rate  = float(row.get("fb_rate") or 0) / scale
            if pid == 0 or fb_rate == 0:
                continue'''

new_parse = '''            pid      = int(row.get("id") or 0)
            fb_rate  = float(row.get("fb_rate") or 0) / scale
            gb_rate  = float(row.get("gb_rate") or 0) / scale
            if pid == 0 or fb_rate == 0:
                continue'''

if old_parse in content:
    content = content.replace(old_parse, new_parse, 1)
    print("OK: gb_rate parsed from CSV")
elif "gb_rate  = float(row.get(" in content:
    print("OK: gb_rate already parsed")
else:
    print("WARN: fb_rate parse block not found")

# ============================================================================
# 2. Add gb_pct to update_rows dict
# ============================================================================
old_dict = '''            update_rows.append({
                "mlb_id":      pid,
                "season_year": season_year,
                "fb_pct":      fb_pct,
                "xfip":        xfip,
            })'''

new_dict = '''            update_rows.append({
                "mlb_id":      pid,
                "season_year": season_year,
                "fb_pct":      fb_pct,
                "gb_pct":      round(gb_rate, 4) if gb_rate > 0 else None,
                "xfip":        xfip,
            })'''

if old_dict in content:
    content = content.replace(old_dict, new_dict, 1)
    print("OK: gb_pct added to update_rows")
elif '"gb_pct":      round(gb_rate' in content:
    print("OK: gb_pct already in update_rows")
else:
    print("WARN: update_rows dict not found")

# ============================================================================
# 3. Add gb_pct to UPDATE SQL
# ============================================================================
old_sql = '''        UPDATE pitcher_xstats SET
          fb_pct=%(fb_pct)s,
          xfip=%(xfip)s,
          refreshed_at=now()
        WHERE mlb_id=%(mlb_id)s AND season_year=%(season_year)s;'''

new_sql = '''        UPDATE pitcher_xstats SET
          fb_pct=%(fb_pct)s,
          gb_pct=COALESCE(%(gb_pct)s, gb_pct),
          xfip=%(xfip)s,
          refreshed_at=now()
        WHERE mlb_id=%(mlb_id)s AND season_year=%(season_year)s;'''

if old_sql in content:
    content = content.replace(old_sql, new_sql, 1)
    print("OK: gb_pct added to UPDATE SQL")
elif "gb_pct=COALESCE(%(gb_pct)s, gb_pct)" in content:
    print("OK: UPDATE SQL already includes gb_pct")
else:
    print("WARN: UPDATE SQL not found")

f.write_text(content, encoding="utf-8")

print()
print("Steps:")
print('  python -X utf8 -c "import ast; ast.parse(open(\'backend/src/statcast_refresh.py\').read()); print(\'OK\')"')
print("  git add backend/src/statcast_refresh.py patch_capture_gb_pct.py")
print("  git commit -m 'Capture gb_pct from batted-ball CSV (same source as fb_pct)'")
print("  git push")
print()
print("After deploy, trigger statcast refresh (admin panel) once.")
print("GB% will populate for the ~373 pitchers in the batted-ball leaderboard")
print("and show in Stats > Pitchers immediately after.")
