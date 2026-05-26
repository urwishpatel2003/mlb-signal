"""
Run from repo root: python patch_team_stats_fix.py

Fix: /api/stats/teams was reading 'woba' and 'est_woba' columns which are
unpopulated. Statcast refresh writes team offensive data into 'team_xwoba'
and 'team_woba_l5'. Also, 'team_wrc_plus' column exists but is never
populated by any refresh job, so we drop it from the display.

After this patch:
  - 'xwOBA' column shows team_xwoba (season-long)
  - New 'L5 wOBA' column shows team_woba_l5 (last 5 games form)
  - wRC+ column removed (data never collected)
  - 'wOBA' column kept but shown as null since refresh doesn't write it
    either — could remove the column entirely if you prefer
"""
from pathlib import Path

api_path = Path("backend/src/api.py")
app_path = Path("frontend/src/App.jsx")
api = api_path.read_text(encoding="utf-8")
app = app_path.read_text(encoding="utf-8")

# ============================================================================
# 1. Fix backend SQL — use real column names
# ============================================================================
old_sql = '''@app.get("/api/stats/teams")
def stats_teams():
    """All teams with offensive + bullpen stats for current season."""
    from . import db
    from datetime import date as _date
    season = _date.today().year
    rows = db.fetchall("""
        SELECT team_code, season_year,
               pa, woba, est_woba, team_xwoba, team_wrc_plus,
               bullpen_era, bullpen_xera, bullpen_ip,
               bullpen_era_l7, bullpen_ip_l7,
               refreshed_at::text AS refreshed_at
        FROM team_xstats
        WHERE season_year = %s
        ORDER BY est_woba DESC NULLS LAST
    """, (season,))
    return {"season": season, "n": len(rows), "teams": [dict(r) for r in rows]}'''

new_sql = '''@app.get("/api/stats/teams")
def stats_teams():
    """All teams with offensive + bullpen stats for current season.

    Note: 'team_xwoba' is the team's own hitting xwOBA (offensive strength)
    written by the team_offensive_xwoba refresh job. The 'woba'/'est_woba'
    columns in this table are unused; we don't return them.
    """
    from . import db
    from datetime import date as _date
    season = _date.today().year
    rows = db.fetchall("""
        SELECT team_code, season_year,
               pa,
               team_xwoba   AS est_woba,
               team_woba_l5 AS l5_woba,
               bullpen_era, bullpen_xera, bullpen_ip,
               bullpen_era_l7, bullpen_ip_l7,
               refreshed_at::text AS refreshed_at
        FROM team_xstats
        WHERE season_year = %s
        ORDER BY team_xwoba DESC NULLS LAST
    """, (season,))
    return {"season": season, "n": len(rows), "teams": [dict(r) for r in rows]}'''

if old_sql in api:
    api = api.replace(old_sql, new_sql, 1)
    print("OK: /api/stats/teams SQL fixed to read team_xwoba and team_woba_l5")
elif "team_xwoba   AS est_woba" in api:
    print("OK: /api/stats/teams SQL already fixed")
else:
    print("WARN: stats_teams endpoint pattern not found")

api_path.write_text(api, encoding="utf-8")

# ============================================================================
# 2. Fix frontend TeamStatsTable columns — drop wOBA/wRC+, add L5 wOBA
# ============================================================================
old_cols = '''function TeamStatsTable({ rows }) {
  const columns = [
    { key:'team_code',       label:'Team',         align:'left', type:'string', width:'minmax(80px, 1fr)' },
    { key:'pa',              label:'PA',           align:'num',  type:'number', dp:0, width:'80px' },
    { key:'woba',            label:'wOBA',         align:'num',  type:'number', fmt:fmt3, width:'80px' },
    { key:'est_woba',        label:'xwOBA',        align:'num',  type:'number', fmt:fmt3, width:'80px' },
    { key:'team_wrc_plus',   label:'wRC+',         align:'num',  type:'number', dp:0, width:'80px' },
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

if old_cols in app:
    app = app.replace(old_cols, new_cols, 1)
    print("OK: TeamStatsTable columns updated (dropped wOBA/wRC+, added L5 wOBA)")
elif "l5_woba" in app and "team_wrc_plus" not in app:
    print("OK: TeamStatsTable columns already fixed")
else:
    print("WARN: TeamStatsTable pattern not found")

app_path.write_text(app, encoding="utf-8")

print()
print("Steps:")
print('  python -X utf8 -c "import ast; ast.parse(open(\'backend/src/api.py\').read()); print(\'OK\')"')
print("  cd frontend && npm run build && cd ..")
print()
print("  git add backend/src/api.py frontend/src/App.jsx patch_team_stats_fix.py")
print("  git commit -m 'Stats: fix team table to read team_xwoba/team_woba_l5; drop wRC+'")
print("  git push")
print()
print("After deploy, the Teams tab will show real xwOBA and L5 wOBA values.")
print("BP L7 ERA/IP will still be populated from the bullpen refresh (independent).")
