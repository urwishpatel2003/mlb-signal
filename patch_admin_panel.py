"""
Run from repo root: python patch_admin_panel.py

Adds an admin panel to the dashboard with three trigger buttons:
  - Run Orchestrator
  - Run Statcast Refresh
  - Run Grader

Flow:
  1. Small "Admin" toggle in the masthead reveals an unlock input
  2. User enters password "Reddevils2003@" once to reveal the buttons
  3. Each button click re-prompts for password before firing
     (per user's choice of "every click")
  4. Buttons call the existing /api/admin/trigger/* endpoints

Security note: password is in plain JS source. The actual admin endpoints
still require ADMIN_TOKEN, which is also stored client-side via env var
VITE_ADMIN_TOKEN. Both visible in compiled JS to anyone with DevTools.
This is "keep accidental clicks out", NOT real security.
"""
from pathlib import Path

app_path = Path("frontend/src/App.jsx")
css_path = Path("frontend/src/styles.css")
app = app_path.read_text(encoding="utf-8")
css = css_path.read_text(encoding="utf-8")

# ============================================================================
# 1. Inject AdminPanel + helper constants near top of file
# ============================================================================
if "ADMIN_PASSWORD" in app:
    print("OK: AdminPanel already present")
else:
    admin_const = """const ADMIN_PASSWORD = 'Reddevils2003@';
const ADMIN_TOKEN = import.meta.env.VITE_ADMIN_TOKEN || '';

"""
    # Insert right after API_BASE definition
    anchor = "const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000';"
    if anchor in app:
        app = app.replace(anchor, anchor + "\n" + admin_const, 1)
        print("OK: ADMIN_PASSWORD + ADMIN_TOKEN constants added")
    else:
        print("WARN: API_BASE anchor not found")
        raise SystemExit(1)

# ============================================================================
# 2. Add the AdminPanel component near the bottom of the file
# ============================================================================
if "function AdminPanel" in app:
    print("OK: AdminPanel component already present")
else:
    panel_component = '''

// ============================================================================
// Admin panel — password-gated trigger buttons for orchestrator/statcast/grader
// ============================================================================
function AdminPanel() {
  const [unlocked, setUnlocked] = useState(false);
  const [pwInput, setPwInput]   = useState('');
  const [status, setStatus]     = useState({});  // { jobName: { state, message } }

  function tryUnlock(e) {
    e.preventDefault();
    if (pwInput === ADMIN_PASSWORD) {
      setUnlocked(true);
      setPwInput('');
    } else {
      alert('Incorrect password');
      setPwInput('');
    }
  }

  async function runJob(jobName, endpoint) {
    const pw = window.prompt(`Confirm password to run ${jobName}:`);
    if (pw === null) return;  // cancel
    if (pw !== ADMIN_PASSWORD) {
      alert('Incorrect password — job not triggered');
      return;
    }
    if (!ADMIN_TOKEN) {
      alert('ADMIN_TOKEN env var not set on frontend — cannot trigger jobs. Set VITE_ADMIN_TOKEN in your build env.');
      return;
    }

    setStatus(s => ({ ...s, [jobName]: { state: 'running', message: 'Running...' }}));
    try {
      const url = `${API_BASE}${endpoint}/${ADMIN_TOKEN}`;
      const r = await fetch(url);
      if (!r.ok) {
        const txt = await r.text();
        setStatus(s => ({ ...s, [jobName]: { state: 'error', message: `HTTP ${r.status}: ${txt.slice(0,160)}` }}));
        return;
      }
      const data = await r.json();
      const summary = data.metrics
        ? `Ok: ${JSON.stringify(data.metrics).slice(0, 200)}`
        : data.result
          ? `Ok: ${JSON.stringify(data.result).slice(0, 200)}`
          : `Ok: ${JSON.stringify(data).slice(0, 200)}`;
      setStatus(s => ({ ...s, [jobName]: { state: 'success', message: summary }}));
    } catch (err) {
      setStatus(s => ({ ...s, [jobName]: { state: 'error', message: err.message }}));
    }
  }

  if (!unlocked) {
    return (
      <div className="admin-panel admin-locked">
        <form className="admin-unlock" onSubmit={tryUnlock}>
          <span className="admin-label">Admin</span>
          <input
            type="password"
            placeholder="Password"
            value={pwInput}
            onChange={e => setPwInput(e.target.value)}
            className="admin-pw-input"
          />
          <button type="submit" className="admin-unlock-btn">Unlock</button>
        </form>
      </div>
    );
  }

  const jobs = [
    { name: 'Orchestrator', endpoint: '/api/admin/trigger/orchestrator', desc: 'Re-run today\\'s slate projection + edges' },
    { name: 'Statcast',     endpoint: '/api/admin/trigger/statcast',     desc: 'Refresh hitter/pitcher/team xstats from Savant' },
    { name: 'Grader',       endpoint: '/api/admin/trigger/grader',       desc: 'Grade yesterday\\'s flagged edges' },
  ];

  return (
    <div className="admin-panel admin-unlocked">
      <div className="admin-header">
        <span className="admin-label">Admin</span>
        <button className="admin-lock-btn" onClick={() => setUnlocked(false)}>Lock</button>
      </div>
      <div className="admin-buttons">
        {jobs.map(job => {
          const st = status[job.name] || {};
          return (
            <div key={job.name} className="admin-job">
              <div className="admin-job-head">
                <button
                  className={`admin-trigger-btn admin-state-${st.state || 'idle'}`}
                  onClick={() => runJob(job.name, job.endpoint)}
                  disabled={st.state === 'running'}
                >
                  {st.state === 'running' ? 'Running...' : `Run ${job.name}`}
                </button>
                <span className="admin-job-desc">{job.desc}</span>
              </div>
              {st.message && (
                <div className={`admin-job-status admin-state-${st.state}`}>
                  {st.message}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
'''
    app = app.rstrip() + panel_component + "\n"
    print("OK: AdminPanel component appended")

# ============================================================================
# 3. Wire AdminPanel into the App render (above the footer)
# ============================================================================
if "<AdminPanel />" in app:
    print("OK: AdminPanel already rendered")
else:
    old_footer_open = '      <footer className="footer">'
    new_block = '''      <AdminPanel />
      <footer className="footer">'''
    if old_footer_open in app:
        app = app.replace(old_footer_open, new_block, 1)
        print("OK: <AdminPanel /> wired in above footer")
    else:
        print("WARN: footer anchor not found — manual edit required")

app_path.write_text(app, encoding="utf-8")

# ============================================================================
# 4. CSS for the admin panel
# ============================================================================
if "/* Admin panel */" in css:
    print("OK: admin panel CSS already present")
else:
    admin_css = '''

/* ============================================================================
   Admin panel — password-gated trigger buttons
   ============================================================================ */
.admin-panel {
  margin: 28px 0 16px;
  padding: 14px 18px;
  background: var(--paper-2, #f5f0e0);
  border: 1px solid var(--rule, #ddd);
  border-radius: 0;
}

.admin-locked .admin-unlock {
  display: flex;
  align-items: center;
  gap: 10px;
}
.admin-label {
  font-family: var(--mono, monospace);
  font-size: 10px;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  font-weight: 700;
  color: var(--ink-2, #555);
}
.admin-pw-input {
  padding: 6px 10px;
  font-family: var(--mono, monospace);
  font-size: 12px;
  border: 1px solid var(--rule, #c8c8c8);
  background: var(--paper, #fff);
  color: var(--ink, #111);
  width: 200px;
}
.admin-pw-input:focus {
  outline: none;
  border-color: var(--ink, #111);
}
.admin-unlock-btn,
.admin-lock-btn {
  padding: 6px 14px;
  font-family: var(--mono, monospace);
  font-size: 10px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  font-weight: 700;
  background: var(--ink, #111);
  color: var(--paper, #fff);
  border: 1px solid var(--ink, #111);
  cursor: pointer;
}
.admin-unlock-btn:hover,
.admin-lock-btn:hover {
  background: var(--ink-2, #444);
}

.admin-unlocked .admin-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 12px;
  padding-bottom: 10px;
  border-bottom: 1px solid var(--rule, #ddd);
}
.admin-lock-btn {
  background: transparent;
  color: var(--ink-2, #555);
  border-color: var(--rule, #c8c8c8);
}

.admin-buttons {
  display: flex;
  flex-direction: column;
  gap: 10px;
}
.admin-job-head {
  display: flex;
  align-items: center;
  gap: 14px;
}
.admin-job-desc {
  font-family: var(--mono, monospace);
  font-size: 11px;
  color: var(--ink-2, #555);
}
.admin-trigger-btn {
  min-width: 180px;
  padding: 8px 16px;
  font-family: var(--mono, monospace);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  background: var(--paper, #fff);
  color: var(--ink, #111);
  border: 1px solid var(--ink, #111);
  cursor: pointer;
}
.admin-trigger-btn:hover:not(:disabled) {
  background: var(--ink, #111);
  color: var(--paper, #fff);
}
.admin-trigger-btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}
.admin-state-running { background: #ffe9a8; color: #5b4500; border-color: #c8a04a; }
.admin-state-success { background: #d5ebd1; color: #2c5f24; border-color: #5b8e52; }
.admin-state-error   { background: #f5d2cc; color: #7a2418; border-color: #b8483a; }

.admin-job-status {
  margin-top: 6px;
  margin-left: 8px;
  padding: 6px 10px;
  font-family: var(--mono, monospace);
  font-size: 10px;
  background: var(--paper, #fff);
  border-left: 3px solid var(--rule, #ccc);
  color: var(--ink-2, #444);
  word-break: break-word;
}
.admin-job-status.admin-state-success { border-left-color: #5b8e52; }
.admin-job-status.admin-state-error   { border-left-color: #b8483a; color: #7a2418; }
'''
    css = css.rstrip() + "\n" + admin_css
    css_path.write_text(css, encoding="utf-8")
    print("OK: admin panel CSS appended")

print()
print("IMPORTANT — frontend needs VITE_ADMIN_TOKEN env var to make the buttons work.")
print("Add to your Railway frontend service env vars (or .env for local dev):")
print("    VITE_ADMIN_TOKEN=<your_admin_token>")
print()
print("Then rebuild:")
print("  cd frontend && npm run build && cd ..")
print()
print("  git add frontend/src/App.jsx frontend/src/styles.css patch_admin_panel.py")
print("  git commit -m 'Admin panel: password-gated trigger buttons for orchestrator/statcast/grader'")
print("  git push")
print()
print("After deploy:")
print("  - 'Admin' input appears at bottom of every page (above footer)")
print("  - Enter password Reddevils2003@ to reveal buttons")
print("  - Each button click prompts for password confirmation")
print("  - Status/result shown inline after each run")
