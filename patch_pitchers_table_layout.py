"""
Run from repo root: python patch_pitchers_table_layout.py

The Pitchers table has 16 columns now and they overflow narrow viewports.
Fix:
  - Narrow numeric columns (50-60px instead of 65-90px)
  - Sticky first column (Pitcher name) so the name stays visible while
    horizontally scrolling
  - Set explicit min-width on the table so columns don't squash on narrow
    viewports; instead the container scrolls
  - Truncate header text slightly so labels fit
"""
from pathlib import Path

app_path = Path("frontend/src/App.jsx")
css_path = Path("frontend/src/styles.css")
app = app_path.read_text(encoding="utf-8")
css = css_path.read_text(encoding="utf-8")

# ============================================================================
# 1. Slimmer column widths in PitcherStatsTable
# ============================================================================
old_cols = '''function PitcherStatsTable({ rows }) {
  const fmtPct = v => v==null ? '\u2014' : (Number(v)*100).toFixed(1)+'%';
  const columns = [
    { key:'last_first',      label:'Pitcher',  align:'left',  type:'string', width:'minmax(160px, 1.8fr)' },
    { key:'pa',              label:'PA',       align:'num',   type:'number', dp:0,  width:'60px' },
    { key:'era',             label:'ERA',      align:'num',   type:'number', dp:2,  width:'65px' },
    { key:'xera',            label:'xERA',     align:'num',   type:'number', dp:2,  width:'65px' },
    { key:'xfip',            label:'xFIP',     align:'num',   type:'number', dp:2,  width:'65px' },
    { key:'est_woba',        label:'xwOBA',    align:'num',   type:'number', fmt:fmt3, width:'70px' },
    { key:'babip',           label:'BABIP',    align:'num',   type:'number', fmt:fmt3, width:'70px' },
    { key:'k_pct',           label:'K%',       align:'num',   type:'number', fmt:fmtPct, width:'70px' },
    { key:'bb9',             label:'BB/9',     align:'num',   type:'number', dp:2,  width:'70px' },
    { key:'gb_pct',          label:'GB%',      align:'num',   type:'number', fmt:fmtPct, width:'70px' },
    { key:'fb_pct',          label:'FB%',      align:'num',   type:'number', fmt:fmtPct, width:'70px' },
    { key:'avg_exit_velo',   label:'EV',       align:'num',   type:'number', dp:1,  width:'70px' },
    { key:'hard_hit_pct',    label:'HardHit%', align:'num',   type:'number', fmt:fmtPct, width:'90px' },
    { key:'barrel_pct',      label:'Barrel%',  align:'num',   type:'number', fmt:fmtPct, width:'80px' },
    { key:'launch_angle_avg',label:'LA',       align:'num',   type:'number', dp:1,  width:'60px' },
    { key:'__splits',        label:'Splits',   align:'num',   type:'string', width:'80px',
      fmt: (_, row) => <PitcherSplitsToggle row={row}/> },
  ];
  return <StatsTable rows={rows} columns={columns} defaultSort="pa" defaultDir="desc" />;
}'''

new_cols = '''function PitcherStatsTable({ rows }) {
  const fmtPct = v => v==null ? '\u2014' : (Number(v)*100).toFixed(1)+'%';
  const columns = [
    { key:'last_first',      label:'Pitcher',  align:'left',  type:'string', width:'minmax(150px, 1.6fr)', sticky:true },
    { key:'pa',              label:'PA',       align:'num',   type:'number', dp:0,  width:'50px' },
    { key:'era',             label:'ERA',      align:'num',   type:'number', dp:2,  width:'55px' },
    { key:'xera',            label:'xERA',     align:'num',   type:'number', dp:2,  width:'55px' },
    { key:'xfip',            label:'xFIP',     align:'num',   type:'number', dp:2,  width:'55px' },
    { key:'est_woba',        label:'xwOBA',    align:'num',   type:'number', fmt:fmt3, width:'62px' },
    { key:'babip',           label:'BABIP',    align:'num',   type:'number', fmt:fmt3, width:'62px' },
    { key:'k_pct',           label:'K%',       align:'num',   type:'number', fmt:fmtPct, width:'58px' },
    { key:'bb9',             label:'BB/9',     align:'num',   type:'number', dp:2,  width:'58px' },
    { key:'gb_pct',          label:'GB%',      align:'num',   type:'number', fmt:fmtPct, width:'58px' },
    { key:'fb_pct',          label:'FB%',      align:'num',   type:'number', fmt:fmtPct, width:'58px' },
    { key:'avg_exit_velo',   label:'EV',       align:'num',   type:'number', dp:1,  width:'55px' },
    { key:'hard_hit_pct',    label:'Hard%',    align:'num',   type:'number', fmt:fmtPct, width:'62px' },
    { key:'barrel_pct',      label:'Brl%',     align:'num',   type:'number', fmt:fmtPct, width:'58px' },
    { key:'launch_angle_avg',label:'LA',       align:'num',   type:'number', dp:1,  width:'50px' },
    { key:'__splits',        label:'',         align:'num',   type:'string', width:'68px',
      fmt: (_, row) => <PitcherSplitsToggle row={row}/> },
  ];
  return <StatsTable rows={rows} columns={columns} defaultSort="pa" defaultDir="desc" />;
}'''

if old_cols in app:
    app = app.replace(old_cols, new_cols, 1)
    print("OK: PitcherStatsTable columns narrowed")
elif "width:'62px'" in app and "Hard%" in app:
    print("OK: Pitcher table already updated")
else:
    print("WARN: PitcherStatsTable pattern not found")

# ============================================================================
# 2. Mark sticky cells via className in StatsTable render
# ============================================================================
old_header_btn = '''            return (
              <button
                key={col.key}
                className={`stats-th ${col.align||'left'} ${active?'active':''}`}
                onClick={()=>clickHeader(col)}
              >{col.label}{arrow}</button>
            );'''

new_header_btn = '''            return (
              <button
                key={col.key}
                className={`stats-th ${col.align||'left'} ${active?'active':''} ${col.sticky?'sticky':''}`}
                onClick={()=>clickHeader(col)}
              >{col.label}{arrow}</button>
            );'''

if old_header_btn in app:
    app = app.replace(old_header_btn, new_header_btn, 1)
    print("OK: header buttons now carry sticky class when configured")

old_cell_div = '''                  return <div key={col.key} className={`stats-cell ${col.align||'left'}`}>{display}</div>;'''
new_cell_div = '''                  return <div key={col.key} className={`stats-cell ${col.align||'left'} ${col.sticky?'sticky':''}`}>{display}</div>;'''

if old_cell_div in app:
    app = app.replace(old_cell_div, new_cell_div, 1)
    print("OK: cells now carry sticky class when configured")

app_path.write_text(app, encoding="utf-8")

# ============================================================================
# 3. CSS — horizontal scroll + sticky first column
# ============================================================================
extra_css = '''

/* ============================================================================
   Stats table — horizontal scroll + sticky first column
   ============================================================================ */
.stats-table {
  width: 100%;
  overflow-x: auto;
  overflow-y: visible;
  position: relative;
}

/* Force the inner grid not to shrink below its content width */
.stats-table .stats-thead,
.stats-table .stats-row {
  min-width: max-content;
}

/* Sticky first column (Pitcher name) */
.stats-th.sticky,
.stats-cell.sticky {
  position: sticky;
  left: 0;
  z-index: 2;
  background: var(--paper, #fff);
  /* Visual separator on the right edge of the sticky column */
  box-shadow: 1px 0 0 var(--rule, #ddd);
}
.stats-thead .stats-th.sticky {
  background: var(--paper-2, #f5f0e0);
}
.stats-row:hover .stats-cell.sticky {
  background: var(--paper-2, #f5f0e0);
}

/* The splits panel should still span the full visible row, including overflow */
.splits-panel {
  grid-column: 1 / -1;
  position: sticky;
  left: 0;
  width: 100%;
  max-width: 100%;
  box-sizing: border-box;
}
'''

if "/* Stats table — horizontal scroll" not in css:
    css = css.rstrip() + "\n" + extra_css
    css_path.write_text(css, encoding="utf-8")
    print("OK: horizontal scroll + sticky column CSS appended")
else:
    print("OK: layout CSS already present")

print()
print("Build and push:")
print("  cd frontend && npm run build && cd ..")
print("  git add frontend/src/App.jsx frontend/src/styles.css patch_pitchers_table_layout.py")
print("  git commit -m 'Pitchers table: narrower cols + sticky name + horizontal scroll'")
print("  git push")
print()
print("===")
print("Still pending: Savant contact data (EV, HardHit%, Barrel%, GB%) is null")
print("because column names in the CSV don't match. Hit this URL after deploying")
print("the savant diag patch from earlier and paste the JSON:")
print()
print("  https://YOUR-RAILWAY-URL.up.railway.app/api/admin/diag/savant_pitcher_csv/YOUR_TOKEN")
print()
print("I need the 'headers' array + a sample row to fix the column mapping.")
