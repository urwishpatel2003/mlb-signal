"""
Run from repo root: python patch_composite_columns.py

Adds sortable composite score columns right after the name in each table:
  - Pitchers: "Score" column (pitcherComposite)
  - Hitters:  "Score" column (hitterComposite)
  - Teams:    "OFF" + "BP" columns (offense + bullpen composites)

Keeps the existing name badges + tint. Scores are precomputed into each row
object so StatsTable can sort/color them as normal numeric fields.
"""
from pathlib import Path

app_path = Path("frontend/src/App.jsx")
app = app_path.read_text(encoding="utf-8")

# ============================================================================
# Color helper for the score columns (0-100, higher better -> good/mid/bad)
# We can reuse a simple inline since tiers already match composite tiers.
# Add a COLORSCORE helper near the composites if not present.
# ============================================================================
if "const COLORSCORE =" not in app:
    helper = '''const COLORSCORE = (v) => v == null ? null : (v >= 60 ? 'good' : (v < 42 ? 'bad' : 'mid'));

'''
    # Insert before hitterComposite (which is before all three tables)
    if "function hitterComposite" in app:
        app = app.replace("function hitterComposite", helper + "function hitterComposite", 1)
        print("OK: COLORSCORE helper inserted")
    else:
        print("WARN: anchor for COLORSCORE not found")

# ============================================================================
# 1. PITCHER table — precompute composite into rows + add Score column
# ============================================================================
old_pitcher_return = '  return <StatsTable rows={rows} columns={columns} defaultSort="pa" defaultDir="desc" />;'
new_pitcher_return = '''  const scoredRows = rows.map(r => {
    const c = pitcherComposite(r);
    return { ...r, composite: c ? c.score : null };
  });
  return <StatsTable rows={scoredRows} columns={columns} defaultSort="pa" defaultDir="desc" />;'''

if old_pitcher_return in app:
    app = app.replace(old_pitcher_return, new_pitcher_return, 1)
    print("OK: pitcher rows get precomputed composite")
elif "composite: c ? c.score" in app:
    print("OK: pitcher composite precompute already present")
else:
    print("WARN: pitcher return line not found")

# Add Score column right after the pitcher name column.
# The name column ends with `} },` (multi-line fmt). We insert after the
# closing of the last_first column object. Anchor on the PA column that
# follows it.
old_pitcher_pa = "    { key:'pa',              label:'PA',       align:'num',   type:'number', dp:0,  width:'50px' },"
new_pitcher_pa = """    { key:'composite',      label:'Score',    align:'num',   type:'number', dp:0,  width:'62px', colorFn:COLORSCORE },
    { key:'pa',              label:'PA',       align:'num',   type:'number', dp:0,  width:'50px' },"""

if old_pitcher_pa in app:
    app = app.replace(old_pitcher_pa, new_pitcher_pa, 1)
    print("OK: pitcher Score column added")
elif "key:'composite'" in app:
    print("OK: pitcher Score column already present")
else:
    print("WARN: pitcher PA column anchor not found")

# ============================================================================
# 2. HITTER table — precompute + Score column
# ============================================================================
# There are TWO `defaultSort="est_woba"` returns (hitter + team). Hitter is
# the first one. Replace only the first occurrence.
old_hitter_return = '  return <StatsTable rows={rows} columns={columns} defaultSort="est_woba" defaultDir="desc" />;'
new_hitter_return = '''  const scoredRows = rows.map(r => {
    const c = hitterComposite(r);
    return { ...r, composite: c ? c.score : null };
  });
  return <StatsTable rows={scoredRows} columns={columns} defaultSort="est_woba" defaultDir="desc" />;'''

# Replace first occurrence only (hitter)
idx = app.find(old_hitter_return)
if idx != -1:
    app = app[:idx] + new_hitter_return + app[idx+len(old_hitter_return):]
    print("OK: hitter rows get precomputed composite")
else:
    print("WARN: hitter return line not found (or already patched)")

# Add Score column after hitter name column. Anchor on the PA column.
old_hitter_pa = "    { key:'pa',         label:'PA',       align:'num',  type:'number', dp:0, width:'70px' },"
new_hitter_pa = """    { key:'composite', label:'Score',    align:'num',  type:'number', dp:0, width:'62px', colorFn:COLORSCORE },
    { key:'pa',         label:'PA',       align:'num',  type:'number', dp:0, width:'70px' },"""

if old_hitter_pa in app:
    app = app.replace(old_hitter_pa, new_hitter_pa, 1)
    print("OK: hitter Score column added")
elif app.count("key:'composite'") >= 2:
    print("OK: hitter Score column already present")
else:
    print("WARN: hitter PA column anchor not found")

# ============================================================================
# 3. TEAM table — precompute off/bp + two columns
# ============================================================================
old_team_return = '  return <StatsTable rows={rows} columns={columns} defaultSort="est_woba" defaultDir="desc" />;'
new_team_return = '''  const scoredRows = rows.map(r => {
    const off = teamOffenseComposite(r);
    const bp  = teamBullpenComposite(r);
    return { ...r, off_score: off ? off.score : null, bp_score: bp ? bp.score : null };
  });
  return <StatsTable rows={scoredRows} columns={columns} defaultSort="off_score" defaultDir="desc" />;'''

# This is now the remaining `defaultSort="est_woba"` return (team), since we
# already replaced the hitter one above.
idx2 = app.find(old_team_return)
if idx2 != -1:
    app = app[:idx2] + new_team_return + app[idx2+len(old_team_return):]
    print("OK: team rows get precomputed off/bp scores + default sort by offense")
else:
    print("WARN: team return line not found (or already patched)")

# Add OFF + BP columns after team name column. Anchor on the team xwOBA column.
old_team_woba = "    { key:'est_woba',        label:'xwOBA',        align:'num',  type:'number', fmt:fmt3, width:'100px' },"
new_team_woba = """    { key:'off_score',      label:'OFF',          align:'num',  type:'number', dp:0, width:'60px', colorFn:COLORSCORE },
    { key:'bp_score',       label:'BP',           align:'num',  type:'number', dp:0, width:'60px', colorFn:COLORSCORE },
    { key:'est_woba',        label:'xwOBA',        align:'num',  type:'number', fmt:fmt3, width:'100px' },"""

if old_team_woba in app:
    app = app.replace(old_team_woba, new_team_woba, 1)
    print("OK: team OFF + BP columns added")
elif "key:'off_score'" in app:
    print("OK: team OFF/BP columns already present")
else:
    print("WARN: team xwOBA column anchor not found")

app_path.write_text(app, encoding="utf-8")

print()
print("Verify:")
print('  Select-String -Path frontend\\src\\App.jsx -Pattern "key:.composite.|key:.off_score.|key:.bp_score." | Measure-Object')
print("  (expect 4 matches: pitcher composite, hitter composite, team off, team bp)")
print()
print("Build + push:")
print("  cd frontend && npm run build && cd ..")
print("  git add frontend/src/App.jsx patch_composite_columns.py")
print("  git commit -m 'Sortable composite score columns for pitchers/hitters/teams'")
print("  git push")
print()
print("After deploy:")
print("  - Pitchers: 'Score' column after name, click header to rank by overall quality")
print("  - Hitters:  'Score' column after name")
print("  - Teams:    'OFF' and 'BP' columns after name (default sorted by OFF)")
print("  - Name badges + tint unchanged")
