"""
Run from repo root: python patch_admin_panel_v2.py

Rewrites the AdminPanel:
  - Removes password gate (still requires ADMIN_TOKEN env var to actually call endpoints)
  - Hidden by default — triple-click the masthead title "The Signal." to reveal
  - Single unified admin section with three groups:
      * Triggers (orchestrator, statcast, grader)
      * Diagnostics (10+ diag URLs as a clickable list, JSON renders inline)
      * Misc (recompute_reasoning, zero_prop_units, etc.)
"""
from pathlib import Path

app_path = Path("frontend/src/App.jsx")
css_path = Path("frontend/src/styles.css")
app = app_path.read_text(encoding="utf-8")
css = css_path.read_text(encoding="utf-8")

# ============================================================================
# 1. Remove the old AdminPanel + ADMIN_PASSWORD constant
# ============================================================================
old_pw_const = "const ADMIN_PASSWORD = 'Reddevils2003@';\n"
if old_pw_const in app:
    app = app.replace(old_pw_const, "", 1)
    print("OK: removed ADMIN_PASSWORD constant")

# Find the old AdminPanel block and strip it
old_panel_start = "// ============================================================================\n// Admin panel — password-gated trigger buttons for orchestrator/statcast/grader\n// ============================================================================\nfunction AdminPanel() {"
if old_panel_start in app:
    start_idx = app.index(old_panel_start)
    # Find the matching end of the function — count braces from `function AdminPanel() {`
    body_start = app.index("{", start_idx)
    depth = 0
    i = body_start
    while i < len(app):
        c = app[i]
        if c == "{": depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                end_idx = i + 1
                break
        i += 1
    else:
        print("ERR: couldn't find end of old AdminPanel function")
        raise SystemExit(1)
    app = app[:start_idx] + app[end_idx:].lstrip("\n")
    print("OK: removed old AdminPanel function")

# ============================================================================
# 2. Inject new AdminPanel + Masthead click handler hook
# ============================================================================
if "AdminPanelV2" in app:
    print("OK: AdminPanelV2 already present")
else:
    new_panel = '''

// ============================================================================
// Admin panel v2 — triple-click masthead to reveal; combines triggers + diag
// No password. Hidden by obscurity. ADMIN_TOKEN env var still required.
// ============================================================================
const ADMIN_TRIGGERS = [
  { name: 'Orchestrator',         endpoint: '/api/admin/trigger/orchestrator',         desc: 'Re-run today\\'s slate projection + edges' },
  { name: 'Statcast Refresh',     endpoint: '/api/admin/trigger/statcast',             desc: 'Refresh hitter/pitcher/team xstats' },
  { name: 'Grader',               endpoint: '/api/admin/trigger/grader',               desc: 'Grade yesterday\\'s flagged edges' },
];

const ADMIN_DIAGNOSTICS = [
  { name: 'Index',                endpoint: '/api/admin/diag/index',                   desc: 'List of all diagnostic endpoints' },
  { name: 'xstats',               endpoint: '/api/admin/diag/xstats',                  desc: 'xstats table state + LEAGUE_XWOBA + last refresh' },
  { name: 'Projection Bias',      endpoint: '/api/admin/diag/projection_bias',         desc: '14-day projection vs market drift' },
  { name: 'Edges (today)',        endpoint: '/api/admin/diag/edges',                   desc: 'Flagged edges by kind/lean for today' },
  { name: 'Games (today)',        endpoint: '/api/admin/diag/games',                   desc: 'Games + projections + F5 cols' },
  { name: 'Pitcher Projections',  endpoint: '/api/admin/diag/pitcher_projections',     desc: '14-day pitcher projection summary' },
  { name: 'Weather Check',        endpoint: '/api/admin/diag/weather_check',           desc: 'Weather vs projection bias' },
  { name: 'Jobs',                 endpoint: '/api/admin/diag/jobs',                    desc: 'Recent job_runs entries' },
  { name: 'Hitter Distribution',  endpoint: '/api/admin/diag/hitter_dist',             desc: 'Hitter xstats summary' },
  { name: 'Top Hitters',          endpoint: '/api/admin/diag/hitters_top',             desc: 'Top 30 hitters by xwOBA' },
  { name: 'Bottom Hitters',       endpoint: '/api/admin/diag/hitters_bottom',          desc: 'Bottom 30 hitters' },
  { name: 'Team Bullpens',        endpoint: '/api/admin/diag/team_bullpens',           desc: 'All teams bullpen ERA + L7' },
  { name: 'Pitcher Distribution', endpoint: '/api/admin/diag/pitcher_dist',            desc: 'Pitcher xstats summary' },
  { name: 'Savant Pitcher CSV',   endpoint: '/api/admin/diag/savant_pitcher_csv',      desc: 'Inspect raw Savant CSV columns' },
];

const ADMIN_MISC = [
  { name: 'Recompute Reasoning',  endpoint: '/api/admin/recompute_reasoning',          desc: 'Re-run reasoning for today\\'s edges' },
  { name: 'Zero Prop Units',      endpoint: '/api/admin/zero_prop_units',               desc: 'Set profit_units=0 for prop edges' },
];

function AdminPanelV2({ visible, onClose }) {
  const [results, setResults] = useState({});  // { endpoint: { state, data } }

  async function call(endpoint, name) {
    if (!ADMIN_TOKEN) {
      setResults(r => ({ ...r, [endpoint]: { state: 'error', data: 'ADMIN_TOKEN env var missing (VITE_ADMIN_TOKEN)' }}));
      return;
    }
    setResults(r => ({ ...r, [endpoint]: { state: 'running', data: 'Calling...' }}));
    try {
      const url = `${API_BASE}${endpoint}/${ADMIN_TOKEN}`;
      const r = await fetch(url);
      const text = await r.text();
      let parsed;
      try { parsed = JSON.parse(text); } catch { parsed = text; }
      if (!r.ok) {
        setResults(s => ({ ...s, [endpoint]: { state: 'error', data: parsed }}));
        return;
      }
      setResults(s => ({ ...s, [endpoint]: { state: 'success', data: parsed }}));
    } catch (err) {
      setResults(s => ({ ...s, [endpoint]: { state: 'error', data: err.message }}));
    }
  }

  if (!visible) return null;

  return (
    <div className="admin-v2">
      <div className="admin-v2-header">
        <span className="admin-v2-title">Admin</span>
        <button className="admin-v2-close" onClick={onClose}>Close</button>
      </div>

      <AdminGroup label="Triggers" items={ADMIN_TRIGGERS} results={results} onCall={call} note="These take 30-90s to complete." />
      <AdminGroup label="Diagnostics" items={ADMIN_DIAGNOSTICS} results={results} onCall={call} />
      <AdminGroup label="Misc" items={ADMIN_MISC} results={results} onCall={call} />
    </div>
  );
}

function AdminGroup({ label, items, results, onCall, note }) {
  return (
    <div className="admin-group">
      <h3 className="admin-group-label">{label}</h3>
      {note && <p className="admin-group-note">{note}</p>}
      <div className="admin-group-list">
        {items.map(item => {
          const res = results[item.endpoint];
          return (
            <div key={item.endpoint} className="admin-item">
              <div className="admin-item-row">
                <button
                  className={`admin-item-btn admin-state-${res?.state || 'idle'}`}
                  onClick={() => onCall(item.endpoint, item.name)}
                  disabled={res?.state === 'running'}
                >
                  {res?.state === 'running' ? '...' : item.name}
                </button>
                <span className="admin-item-desc">{item.desc}</span>
              </div>
              {res?.data !== undefined && (
                <pre className={`admin-item-output admin-state-${res.state}`}>
                  {typeof res.data === 'string' ? res.data : JSON.stringify(res.data, null, 2)}
                </pre>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
'''
    app = app.rstrip() + new_panel + "\n"
    print("OK: AdminPanelV2 appended")

# ============================================================================
# 3. Add triple-click detection on the masthead + wire AdminPanelV2 into App
# ============================================================================

# Wrap App state to track admin visibility
old_app_state = "  const [tab, setTab]     = useState('Games');\n  const [slate, setSlate] = useState(null);"
new_app_state = "  const [tab, setTab]     = useState('Games');\n  const [adminVisible, setAdminVisible] = useState(false);\n  const [slate, setSlate] = useState(null);"
if old_app_state in app and "adminVisible" not in app:
    app = app.replace(old_app_state, new_app_state, 1)
    print("OK: App.adminVisible state added")
elif "adminVisible" in app:
    print("OK: adminVisible state already present")

# Pass adminVisible handlers to Masthead
old_masthead_call = "<Masthead slate={slate} />"
new_masthead_call = "<Masthead slate={slate} onTripleClick={()=>setAdminVisible(v=>!v)} />"
if old_masthead_call in app:
    app = app.replace(old_masthead_call, new_masthead_call, 1)
    print("OK: Masthead now receives onTripleClick")

# Replace old AdminPanel render with AdminPanelV2
old_render = "      <AdminPanel />\n      <footer className=\"footer\">"
new_render = "      <AdminPanelV2 visible={adminVisible} onClose={()=>setAdminVisible(false)} />\n      <footer className=\"footer\">"
if old_render in app:
    app = app.replace(old_render, new_render, 1)
    print("OK: AdminPanelV2 wired in")
elif "<AdminPanelV2" in app:
    print("OK: AdminPanelV2 already wired")
else:
    # Maybe the old AdminPanel was already stripped — insert above footer
    footer_open = "      <footer className=\"footer\">"
    if footer_open in app and "<AdminPanelV2" not in app:
        app = app.replace(footer_open,
            "      <AdminPanelV2 visible={adminVisible} onClose={()=>setAdminVisible(false)} />\n" + footer_open, 1)
        print("OK: AdminPanelV2 inserted before footer")

# Patch Masthead component to handle triple-click on the title
old_masthead = '''function Masthead({ slate }) {
  const today   = new Date();
  const dateStr = today.toLocaleDateString('en-US',{weekday:'long',month:'long',day:'numeric',year:'numeric'});
  const nGames  = slate?.games?.length ?? 0;
  const nEdges  = (slate?.edges ?? []).length;
  const runId   = slate?.run?.run_id;
  return (
    <header className="masthead">
      <div className="masthead-top"><span>QuAInt &middot; MLB Edition</span><span>{dateStr.toUpperCase()}</span></div>
      <h1>The <span className="em">Signal</span>.</h1>
      <div className="masthead-sub">
        <div className="meta"><span>{nGames} GAMES</span><span>{nEdges} EDGES</span></div>
        <span>RUN #{runId ?? '-'}</span>
      </div>
    </header>
  );
}'''

new_masthead = '''function Masthead({ slate, onTripleClick }) {
  const today   = new Date();
  const dateStr = today.toLocaleDateString('en-US',{weekday:'long',month:'long',day:'numeric',year:'numeric'});
  const nGames  = slate?.games?.length ?? 0;
  const nEdges  = (slate?.edges ?? []).length;
  const runId   = slate?.run?.run_id;
  const clicksRef = React.useRef({ count: 0, timer: null });
  function handleTitleClick() {
    if (!onTripleClick) return;
    clicksRef.current.count += 1;
    if (clicksRef.current.timer) clearTimeout(clicksRef.current.timer);
    if (clicksRef.current.count >= 3) {
      onTripleClick();
      clicksRef.current.count = 0;
    } else {
      clicksRef.current.timer = setTimeout(() => { clicksRef.current.count = 0; }, 600);
    }
  }
  return (
    <header className="masthead">
      <div className="masthead-top"><span>QuAInt &middot; MLB Edition</span><span>{dateStr.toUpperCase()}</span></div>
      <h1 onClick={handleTitleClick} style={{cursor: 'pointer', userSelect: 'none'}}>The <span className="em">Signal</span>.</h1>
      <div className="masthead-sub">
        <div className="meta"><span>{nGames} GAMES</span><span>{nEdges} EDGES</span></div>
        <span>RUN #{runId ?? '-'}</span>
      </div>
    </header>
  );
}'''

if old_masthead in app:
    app = app.replace(old_masthead, new_masthead, 1)
    print("OK: Masthead now detects triple-click on title")
elif "clicksRef.current.count >= 3" in app:
    print("OK: Masthead triple-click already present")

app_path.write_text(app, encoding="utf-8")

# ============================================================================
# 4. Replace CSS — drop password-related styles, add v2 panel styles
# ============================================================================
# Find and strip the old admin CSS block
old_marker = "/* ============================================================================\n   Admin panel — password-gated trigger buttons\n   ============================================================================ */"
if old_marker in css:
    start = css.index(old_marker)
    # The block runs to end of file or until next /* ============ */ marker
    next_marker_pos = css.find("/* ============================================================================", start + len(old_marker))
    if next_marker_pos == -1:
        css = css[:start].rstrip() + "\n"
    else:
        css = css[:start] + css[next_marker_pos:]
    print("OK: removed old admin panel CSS")

if "/* Admin v2" in css:
    print("OK: v2 admin CSS already present")
else:
    new_css = '''

/* ============================================================================
   Admin v2 — hidden until triple-click, no password
   ============================================================================ */
.admin-v2 {
  margin: 24px 0 16px;
  padding: 16px 18px;
  background: var(--paper-2, #f5f0e0);
  border: 2px solid var(--ink, #111);
}
.admin-v2-header {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  border-bottom: 2px solid var(--ink, #111);
  padding-bottom: 10px;
  margin-bottom: 14px;
}
.admin-v2-title {
  font-family: var(--display, serif);
  font-size: 22px;
  font-weight: 700;
  color: var(--ink, #111);
}
.admin-v2-close {
  padding: 4px 12px;
  font-family: var(--mono, monospace);
  font-size: 10px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  font-weight: 700;
  background: transparent;
  color: var(--ink-2, #555);
  border: 1px solid var(--rule, #c8c8c8);
  cursor: pointer;
}
.admin-v2-close:hover {
  background: var(--ink, #111);
  color: var(--paper, #fff);
}

.admin-group {
  margin-bottom: 22px;
}
.admin-group-label {
  font-family: var(--mono, monospace);
  font-size: 11px;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  font-weight: 700;
  color: var(--ink, #111);
  border-bottom: 1px solid var(--rule, #ccc);
  padding-bottom: 4px;
  margin: 0 0 8px;
}
.admin-group-note {
  font-family: var(--mono, monospace);
  font-size: 10px;
  color: var(--ink-3, #888);
  font-style: italic;
  margin: 0 0 8px;
}
.admin-group-list {
  display: flex;
  flex-direction: column;
  gap: 4px;
}
.admin-item {
  padding: 6px 0;
  border-bottom: 1px solid var(--rule-light, #ece5d0);
}
.admin-item:last-child { border-bottom: none; }
.admin-item-row {
  display: flex;
  align-items: center;
  gap: 12px;
}
.admin-item-btn {
  min-width: 180px;
  padding: 6px 12px;
  font-family: var(--mono, monospace);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  background: var(--paper, #fff);
  color: var(--ink, #111);
  border: 1px solid var(--ink, #111);
  cursor: pointer;
  text-align: left;
}
.admin-item-btn:hover:not(:disabled) {
  background: var(--ink, #111);
  color: var(--paper, #fff);
}
.admin-item-btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}
.admin-item-desc {
  font-family: var(--mono, monospace);
  font-size: 10px;
  color: var(--ink-2, #555);
}
.admin-item-output {
  margin: 8px 0 4px 0;
  padding: 10px 12px;
  font-family: var(--mono, monospace);
  font-size: 10.5px;
  line-height: 1.4;
  background: var(--paper, #fff);
  border: 1px solid var(--rule, #ccc);
  border-left: 3px solid var(--ink-2, #555);
  max-height: 400px;
  overflow: auto;
  white-space: pre-wrap;
  word-break: break-word;
  color: var(--ink, #111);
}
.admin-state-running .admin-item-output,
.admin-item-output.admin-state-running { border-left-color: #c8a04a; background: #fff8e8; }
.admin-state-success .admin-item-output,
.admin-item-output.admin-state-success { border-left-color: #5b8e52; }
.admin-state-error .admin-item-output,
.admin-item-output.admin-state-error { border-left-color: #b8483a; background: #fbeee9; color: #7a2418; }

.admin-state-running.admin-item-btn { background: #ffe9a8; color: #5b4500; border-color: #c8a04a; }
.admin-state-success.admin-item-btn { background: #d5ebd1; color: #2c5f24; border-color: #5b8e52; }
.admin-state-error.admin-item-btn   { background: #f5d2cc; color: #7a2418; border-color: #b8483a; }
'''
    css = css.rstrip() + "\n" + new_css
    css_path.write_text(css, encoding="utf-8")
    print("OK: v2 admin CSS appended")

print()
print("Build and push:")
print("  cd frontend && npm run build && cd ..")
print("  git add frontend/src/App.jsx frontend/src/styles.css patch_admin_panel_v2.py")
print("  git commit -m 'Admin v2: triple-click reveal, no password, unified diag + triggers'")
print("  git push")
print()
print("Usage after deploy:")
print("  Triple-click 'The Signal.' in the masthead to reveal the admin panel.")
print("  Click any endpoint button to call it; JSON response renders inline below.")
print("  Triple-click again or click 'Close' to hide.")
