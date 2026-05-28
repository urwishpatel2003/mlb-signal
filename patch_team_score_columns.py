"""
Run from repo root: python patch_team_score_columns.py

The team OFF/BP composite columns didn't insert earlier because the xwOBA
anchor line already had colorFn:COLOR.tWOBA appended. This inserts the two
score columns right after the team name column (before xwOBA).
"""
from pathlib import Path

app_path = Path("frontend/src/App.jsx")
app = app_path.read_text(encoding="utf-8")

old = "    { key:'est_woba',        label:'xwOBA',        align:'num',  type:'number', fmt:fmt3, width:'100px', colorFn:COLOR.tWOBA },"
new = """    { key:'off_score',      label:'OFF',          align:'num',  type:'number', dp:0, width:'60px', colorFn:COLORSCORE },
    { key:'bp_score',       label:'BP',           align:'num',  type:'number', dp:0, width:'60px', colorFn:COLORSCORE },
    { key:'est_woba',        label:'xwOBA',        align:'num',  type:'number', fmt:fmt3, width:'100px', colorFn:COLOR.tWOBA },"""

if "key:'off_score'" in app:
    print("OK: team OFF/BP columns already present")
elif old in app:
    app = app.replace(old, new, 1)
    app_path.write_text(app, encoding="utf-8")
    print("OK: team OFF + BP columns inserted")
else:
    print("ERR: team xwOBA anchor still not found — paste the line again")
    raise SystemExit(1)

print()
print("Verify (expect 4):")
print('  Select-String -Path frontend\\src\\App.jsx -Pattern "key:.composite.|key:.off_score.|key:.bp_score." | Measure-Object')
print()
print("Build + push:")
print("  cd frontend && npm run build && cd ..")
print("  git add frontend/src/App.jsx patch_team_score_columns.py")
print("  git commit -m 'Add team OFF + BP sortable score columns'")
print("  git push")
