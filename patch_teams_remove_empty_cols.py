"""
Run from repo root: python patch_teams_remove_empty_cols.py

Removes PA and BP xERA columns from the Teams stats table — neither column
is populated by any current refresh job, so they were always showing as '—'.

Only touches frontend (App.jsx); backend still returns those fields in case
they're useful for other consumers.
"""
from pathlib import Path

app_path = Path("frontend/src/App.jsx")
app = app_path.read_text(encoding="utf-8")

old_cols = '''function TeamStatsTable({ rows }) {
  const columns = [
    { key:'team_code',       label:'Team',         align:'left', type:'string', width:'minmax(80px, 1fr)' },
    { key:'pa',              label:'PA',           align:'num',  type:'number', dp:0, width:'80px' },
    { key:'est_woba',        label:'xwOBA',        align:'num',  type:'number', fmt:fmt3, width:'90px' },
    { key:'l5_woba',         label:'L5 wOBA',      align:'num',  type:'number', fmt:fmt3, width:'90px' },
    { key:'bullpen_era',     label:'BP ERA',       align:'num',  type:'number', dp:2, width:'90px' },
    { key:'bullpen_xera',    label:'BP xERA',      align:'num',  type:'number', dp:2, width:'90px' },
    { key:'bullpen_ip',      label:'BP IP',        align:'num',  type:'number', dp:1, width:'90px' },
    { key:'bullpen_era_l7',  label:'BP L7 ERA',    align:'num',  type:'number', dp:2, width:'100px' },
    { key:'bullpen_ip_l7',   label:'BP L7 IP',     align:'num',  type:'number', dp:1, width:'100px' },
  ];
  return <StatsTable rows={rows} columns={columns} defaultSort="est_woba" defaultDir="desc" />;
}'''

new_cols = '''function TeamStatsTable({ rows }) {
  const columns = [
    { key:'team_code',       label:'Team',         align:'left', type:'string', width:'minmax(80px, 1fr)' },
    { key:'est_woba',        label:'xwOBA',        align:'num',  type:'number', fmt:fmt3, width:'100px' },
    { key:'l5_woba',         label:'L5 wOBA',      align:'num',  type:'number', fmt:fmt3, width:'100px' },
    { key:'bullpen_era',     label:'BP ERA',       align:'num',  type:'number', dp:2, width:'100px' },
    { key:'bullpen_ip',      label:'BP IP',        align:'num',  type:'number', dp:1, width:'100px' },
    { key:'bullpen_era_l7',  label:'BP L7 ERA',    align:'num',  type:'number', dp:2, width:'110px' },
    { key:'bullpen_ip_l7',   label:'BP L7 IP',     align:'num',  type:'number', dp:1, width:'110px' },
  ];
  return <StatsTable rows={rows} columns={columns} defaultSort="est_woba" defaultDir="desc" />;
}'''

if old_cols in app:
    app = app.replace(old_cols, new_cols, 1)
    app_path.write_text(app, encoding="utf-8")
    print("OK: removed PA and BP xERA columns from TeamStatsTable")
elif "{ key:'bullpen_xera'" not in app:
    print("OK: columns already removed")
else:
    print("WARN: TeamStatsTable column block pattern not found (may have drifted)")
    raise SystemExit(1)

print()
print("Steps:")
print("  cd frontend && npm run build && cd ..")
print("  git add frontend/src/App.jsx patch_teams_remove_empty_cols.py")
print("  git commit -m 'Stats: remove empty PA and BP xERA columns from Teams table'")
print("  git push")
