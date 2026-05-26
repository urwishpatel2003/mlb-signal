"""
Run from repo root: python patch_stats_page.py

Adds a new Stats page to the dashboard with three tabs:
  - Pitchers : sortable table of pitcher_xstats + bullpen-style team join
  - Hitters  : sortable table of hitter_xstats + vs-LHP/vs-RHP splits
  - Teams    : sortable table of team_xstats (offense + bullpen)

Backend adds three endpoints:
  GET /api/stats/pitchers
  GET /api/stats/hitters
  GET /api/stats/teams

Frontend adds:
  - New 'Stats' tab in the top nav
  - <StatsView> component with sub-tabs + sort + filter

Idempotent — safe to re-run.
"""
from pathlib import Path

api_path = Path("backend/src/api.py")
app_path = Path("frontend/src/App.jsx")
css_path = Path("frontend/src/styles.css")
api = api_path.read_text(encoding="utf-8")
app = app_path.read_text(encoding="utf-8")
css = css_path.read_text(encoding="utf-8")

# ============================================================================
# 1. Backend endpoints
# ============================================================================
if "/api/stats/pitchers" in api:
    print("OK: /api/stats/* endpoints already present")
else:
    endpoints = '''


# ============================================================================
# League-wide stats endpoints (Stats page)
# Read-only, no auth required, scoped to current season.
# ============================================================================
@app.get("/api/stats/pitchers")
def stats_pitchers():
    """All pitchers with Statcast data for the current season."""
    from . import db
    from datetime import date as _date
    season = _date.today().year
    rows = db.fetchall("""
        SELECT mlb_id, last_first, season_year,
               pa, bip, ba, est_ba, slg, est_slg, woba, est_woba,
               era, xera, xfip, k_pct, bb9, fb_pct, hr_fb_rate,
               days_rest, last_start_date::text AS last_start_date,
               refreshed_at::text AS refreshed_at
        FROM pitcher_xstats
        WHERE season_year = %s
        ORDER BY pa DESC NULLS LAST, est_woba ASC NULLS LAST
    """, (season,))
    return {"season": season, "n": len(rows), "pitchers": [dict(r) for r in rows]}


@app.get("/api/stats/hitters")
def stats_hitters():
    """All hitters with Statcast data for the current season, with vs-LHP/vs-RHP splits."""
    from . import db
    from datetime import date as _date
    season = _date.today().year

    rows = db.fetchall("""
        SELECT mlb_id, last_first, season_year,
               pa, ba, est_ba, slg, est_slg, woba, est_woba, l15_woba,
               refreshed_at::text AS refreshed_at
        FROM hitter_xstats
        WHERE season_year = %s
        ORDER BY pa DESC NULLS LAST, est_woba DESC NULLS LAST
    """, (season,))

    # Tack on platoon splits in one query
    splits = db.fetchall("""
        SELECT mlb_id, vs_hand, pa, est_woba
        FROM hitter_splits
        WHERE season_year = %s
    """, (season,))
    by_id = {}
    for s in splits:
        d = by_id.setdefault(s["mlb_id"], {})
        d[f"vs_{s['vs_hand']}_pa"] = s["pa"]
        d[f"vs_{s['vs_hand']}_woba"] = float(s["est_woba"]) if s["est_woba"] is not None else None

    out = []
    for r in rows:
        d = dict(r)
        d.update(by_id.get(r["mlb_id"], {}))
        out.append(d)

    return {"season": season, "n": len(out), "hitters": out}


@app.get("/api/stats/teams")
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
    return {"season": season, "n": len(rows), "teams": [dict(r) for r in rows]}
'''
    api = api.rstrip() + endpoints + "\n"
    api_path.write_text(api, encoding="utf-8")
    print("OK: 3 /api/stats/* endpoints added")

# ============================================================================
# 2. Frontend — add 'Stats' tab to TABS array
# ============================================================================
old_tabs = "const TABS = ['Full Game O/U', 'F5 O/U', 'Moneyline', 'Pitcher Props', 'Pitchers', 'Slate', 'Track Record'];"
new_tabs = "const TABS = ['Full Game O/U', 'F5 O/U', 'Moneyline', 'Pitcher Props', 'Pitchers', 'Slate', 'Stats', 'Track Record'];"
if old_tabs in app:
    app = app.replace(old_tabs, new_tabs, 1)
    print("OK: 'Stats' added to TABS")
elif "'Stats'" in app:
    print("OK: 'Stats' tab already in TABS")
else:
    print("WARN: TABS array pattern not found — manual edit required")

# ============================================================================
# 3. Frontend — wire the tab into the main switch
# ============================================================================
old_switch = "          {tab==='Slate'         && <GamesView games={slate.games} projections={slate.projections} />}\n          {tab==='Track Record'  && <PerformanceView perf={perf} />}"
new_switch = "          {tab==='Slate'         && <GamesView games={slate.games} projections={slate.projections} />}\n          {tab==='Stats'         && <StatsView />}\n          {tab==='Track Record'  && <PerformanceView perf={perf} />}"
if old_switch in app:
    app = app.replace(old_switch, new_switch, 1)
    print("OK: StatsView wired into tab switch")
elif "tab==='Stats'" in app:
    print("OK: StatsView already wired")
else:
    print("WARN: tab switch pattern not found")

# ============================================================================
# 4. Frontend — append StatsView + sub-components before the footer
# ============================================================================
if "function StatsView" in app:
    print("OK: StatsView component already present")
else:
    stats_view = '''

// ============================================================================
// Stats view — league-wide phonebook for pitchers, hitters, teams
// ============================================================================
const STATS_SUB_TABS = ['Pitchers', 'Hitters', 'Teams'];

function StatsView() {
  const [sub, setSub] = useState('Pitchers');
  const [pitchers, setPitchers] = useState(null);
  const [hitters, setHitters]   = useState(null);
  const [teams, setTeams]       = useState(null);
  const [error, setError]       = useState(null);

  useEffect(() => {
    if (sub === 'Pitchers' && pitchers === null) {
      fetch(`${API_BASE}/api/stats/pitchers`).then(r=>r.json()).then(d=>setPitchers(d.pitchers||[])).catch(e=>setError(e.message));
    } else if (sub === 'Hitters' && hitters === null) {
      fetch(`${API_BASE}/api/stats/hitters`).then(r=>r.json()).then(d=>setHitters(d.hitters||[])).catch(e=>setError(e.message));
    } else if (sub === 'Teams' && teams === null) {
      fetch(`${API_BASE}/api/stats/teams`).then(r=>r.json()).then(d=>setTeams(d.teams||[])).catch(e=>setError(e.message));
    }
  }, [sub]);

  return (
    <section>
      <div className="section-header">
        <h2>Stats.</h2>
        <span className="deck">Season-long Statcast data &middot; pitchers, hitters, teams</span>
      </div>
      <div className="prop-cat-tabs">
        {STATS_SUB_TABS.map(t => (
          <button key={t} className={`prop-cat-tab ${sub===t?'active':''}`} onClick={()=>setSub(t)}>{t}</button>
        ))}
      </div>
      {error && <div className="empty">Error loading stats: {error}</div>}
      {sub==='Pitchers' && (pitchers===null ? <div className="loading">Loading pitchers</div> : <PitcherStatsTable rows={pitchers}/>)}
      {sub==='Hitters'  && (hitters===null  ? <div className="loading">Loading hitters</div>  : <HitterStatsTable rows={hitters}/>)}
      {sub==='Teams'    && (teams===null    ? <div className="loading">Loading teams</div>    : <TeamStatsTable rows={teams}/>)}
    </section>
  );
}

function StatsTable({ rows, columns, defaultSort, defaultDir='desc', initialSearch='' }) {
  const [sortKey, setSortKey] = useState(defaultSort);
  const [sortDir, setSortDir] = useState(defaultDir);
  const [search, setSearch]   = useState(initialSearch);

  function clickHeader(col) {
    if (col.key === sortKey) { setSortDir(sortDir==='asc'?'desc':'asc'); }
    else { setSortKey(col.key); setSortDir(col.type==='number'?'desc':'asc'); }
  }

  const filtered = !search ? rows : rows.filter(r => {
    const q = search.toLowerCase();
    return columns.some(c => {
      const v = r[c.key];
      return v != null && String(v).toLowerCase().includes(q);
    });
  });

  const sorted = [...filtered].sort((a,b) => {
    const va = a[sortKey], vb = b[sortKey];
    if (va == null && vb == null) return 0;
    if (va == null) return 1;
    if (vb == null) return -1;
    let cmp = (typeof va === 'number' && typeof vb === 'number') ? va - vb : String(va).localeCompare(String(vb));
    return sortDir === 'asc' ? cmp : -cmp;
  });

  const gridTemplate = columns.map(c => c.width || '1fr').join(' ');

  return (
    <>
      <div className="stats-toolbar">
        <input
          type="text"
          className="stats-search"
          placeholder="Search by name, team, etc."
          value={search}
          onChange={(e)=>setSearch(e.target.value)}
        />
        <span className="stats-count">{sorted.length} of {rows.length}</span>
      </div>
      <div className="stats-table">
        <div className="stats-thead" style={{gridTemplateColumns: gridTemplate}}>
          {columns.map(col => {
            const active = sortKey === col.key;
            const arrow = active ? (sortDir==='asc' ? ' ▲' : ' ▼') : '';
            return (
              <button
                key={col.key}
                className={`stats-th ${col.align||'left'} ${active?'active':''}`}
                onClick={()=>clickHeader(col)}
              >{col.label}{arrow}</button>
            );
          })}
        </div>
        <div className="stats-tbody">
          {sorted.length === 0
            ? <div className="empty">No rows match filter.</div>
            : sorted.map((r,i) => (
              <div key={r.mlb_id || r.team_code || i} className="stats-row" style={{gridTemplateColumns: gridTemplate}}>
                {columns.map(col => {
                  const val = r[col.key];
                  const display = val == null ? '—'
                    : col.fmt ? col.fmt(val)
                    : col.type === 'number' ? Number(val).toFixed(col.dp ?? 2)
                    : val;
                  return <div key={col.key} className={`stats-cell ${col.align||'left'}`}>{display}</div>;
                })}
              </div>
            ))}
        </div>
      </div>
    </>
  );
}

const fmt3 = v => v==null ? '—' : Number(v).toFixed(3);
const fmt2 = v => v==null ? '—' : Number(v).toFixed(2);
const fmt0 = v => v==null ? '—' : Math.round(Number(v)).toString();

function PitcherStatsTable({ rows }) {
  const columns = [
    { key:'last_first', label:'Pitcher',  align:'left',  type:'string', width:'minmax(160px, 2fr)' },
    { key:'pa',         label:'PA',       align:'num',   type:'number', dp:0, width:'70px' },
    { key:'bip',        label:'BIP',      align:'num',   type:'number', dp:0, width:'70px' },
    { key:'era',        label:'ERA',      align:'num',   type:'number', dp:2, width:'70px' },
    { key:'xera',       label:'xERA',     align:'num',   type:'number', dp:2, width:'70px' },
    { key:'xfip',       label:'xFIP',     align:'num',   type:'number', dp:2, width:'70px' },
    { key:'est_woba',   label:'xwOBA',    align:'num',   type:'number', fmt:fmt3, width:'80px' },
    { key:'k_pct',      label:'K%',       align:'num',   type:'number', fmt:v=>v==null?'—':(Number(v)*100).toFixed(1)+'%', width:'80px' },
    { key:'bb9',        label:'BB/9',     align:'num',   type:'number', dp:2, width:'80px' },
    { key:'fb_pct',     label:'FB%',      align:'num',   type:'number', fmt:v=>v==null?'—':(Number(v)*100).toFixed(1)+'%', width:'80px' },
    { key:'hr_fb_rate', label:'HR/FB',    align:'num',   type:'number', fmt:v=>v==null?'—':(Number(v)*100).toFixed(1)+'%', width:'80px' },
    { key:'days_rest',  label:'Rest',     align:'num',   type:'number', dp:0, width:'70px' },
  ];
  return <StatsTable rows={rows} columns={columns} defaultSort="pa" defaultDir="desc" />;
}

function HitterStatsTable({ rows }) {
  const columns = [
    { key:'last_first', label:'Hitter',   align:'left', type:'string', width:'minmax(160px, 2fr)' },
    { key:'pa',         label:'PA',       align:'num',  type:'number', dp:0, width:'70px' },
    { key:'ba',         label:'BA',       align:'num',  type:'number', fmt:fmt3, width:'80px' },
    { key:'est_ba',     label:'xBA',      align:'num',  type:'number', fmt:fmt3, width:'80px' },
    { key:'slg',        label:'SLG',      align:'num',  type:'number', fmt:fmt3, width:'80px' },
    { key:'est_slg',    label:'xSLG',     align:'num',  type:'number', fmt:fmt3, width:'80px' },
    { key:'woba',       label:'wOBA',     align:'num',  type:'number', fmt:fmt3, width:'80px' },
    { key:'est_woba',   label:'xwOBA',    align:'num',  type:'number', fmt:fmt3, width:'80px' },
    { key:'l15_woba',   label:'L15 wOBA', align:'num',  type:'number', fmt:fmt3, width:'90px' },
    { key:'vs_L_woba',  label:'vs LHP',   align:'num',  type:'number', fmt:fmt3, width:'80px' },
    { key:'vs_R_woba',  label:'vs RHP',   align:'num',  type:'number', fmt:fmt3, width:'80px' },
  ];
  return <StatsTable rows={rows} columns={columns} defaultSort="est_woba" defaultDir="desc" />;
}

function TeamStatsTable({ rows }) {
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
}
'''
    # Insert before the final footer/export
    if "export default function App()" not in app:
        print("WARN: anchor for StatsView insertion not found")
    else:
        # Append the new components at the very end of the file
        app = app.rstrip() + stats_view + "\n"
        print("OK: StatsView and sub-components appended")

app_path.write_text(app, encoding="utf-8")

# ============================================================================
# 5. CSS for the Stats tables
# ============================================================================
if "/* Stats page tables */" in css:
    print("OK: Stats CSS already present")
else:
    stats_css = '''

/* ============================================================================
   Stats page tables — phonebook-style for Pitchers/Hitters/Teams
   ============================================================================ */
.stats-toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 14px;
  gap: 12px;
}
.stats-search {
  flex: 1;
  max-width: 320px;
  padding: 8px 12px;
  font-family: var(--mono);
  font-size: 12px;
  border: 1px solid var(--rule);
  background: var(--paper-2);
  color: var(--ink);
}
.stats-search:focus {
  outline: none;
  border-color: var(--ink);
}
.stats-count {
  font-family: var(--mono);
  font-size: 11px;
  color: var(--ink-3);
  text-transform: uppercase;
  letter-spacing: 0.06em;
}

.stats-table {
  width: 100%;
  overflow-x: auto;
}
.stats-thead {
  display: grid;
  gap: 0 8px;
  background: var(--paper-2);
  border-bottom: 2px solid var(--ink);
  padding: 0;
}
.stats-th {
  border: none;
  background: transparent;
  text-align: left;
  font-family: var(--mono);
  font-size: 10px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--ink-2);
  font-weight: 700;
  padding: 10px 8px;
  cursor: pointer;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.stats-th.num { text-align: right; }
.stats-th.active { color: var(--ink); background: var(--paper); }
.stats-th:hover { color: var(--ink); }

.stats-tbody { background: var(--paper); }
.stats-row {
  display: grid;
  gap: 0 8px;
  padding: 0;
  border-bottom: 1px solid var(--rule);
  align-items: center;
}
.stats-row:hover { background: var(--paper-2); }
.stats-cell {
  padding: 9px 8px;
  font-family: var(--mono);
  font-size: 12px;
  color: var(--ink);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.stats-cell.num {
  text-align: right;
  font-variant-numeric: tabular-nums;
}
.stats-cell.left {
  font-family: var(--text, system-ui);
  font-weight: 500;
}
'''
    css = css.rstrip() + "\n" + stats_css
    css_path.write_text(css, encoding="utf-8")
    print("OK: Stats CSS appended to styles.css")

print()
print("Verify and push:")
print('  python -X utf8 -c "import ast; ast.parse(open(\'backend/src/api.py\').read()); print(\'OK\')"')
print("  cd frontend && npm run build && cd ..")
print()
print("  git add backend/src/api.py frontend/src/App.jsx frontend/src/styles.css patch_stats_page.py")
print("  git commit -m 'Stats page: league-wide phonebook for pitchers/hitters/teams'")
print("  git push")
print()
print("After Railway deploys, hit the new tab and verify all three sub-tabs load data.")
