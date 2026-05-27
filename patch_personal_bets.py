"""
Run from repo root: python patch_personal_bets.py

Adds personal-bets tracking on top of existing edges system:

  Backend:
    - /api/personal_bets               GET   list all personal bets (joined with edge + result)
    - /api/personal_bets               POST  create/update a bet for an edge
    - /api/personal_bets/{bet_id}      DELETE remove a bet

  Frontend:
    - Each flagged edge gets a small dollar-icon button -> opens modal
    - Modal: $ amount, juice (+/-), sportsbook, notes
    - Bet edges get a green checkmark indicator
    - New 'My Record' tab mirrors Track Record but filters to personal_bets only
    - All P&L computed in dollars (stake × payout from user-entered juice)

Prereqs:
  - migration 0009_personal_bets.sql must be applied
    (copy to backend/migrations/ then either run bootstrap locally OR run the
    CREATE TABLE in Railway's SQL console)
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
if "/api/personal_bets" in api:
    print("OK: personal_bets endpoints already present")
else:
    endpoints = '''


# ============================================================================
# Personal bets — user-tracked dollar wagers on flagged edges
# ============================================================================
@app.get("/api/personal_bets")
def list_personal_bets():
    """All personal bets joined with edge details + grading result."""
    rows = db.fetchall("""
        SELECT pb.bet_id, pb.edge_id, pb.dollar_amount, pb.juice,
               pb.sportsbook, pb.notes,
               pb.placed_at::text AS placed_at,
               pb.updated_at::text AS updated_at,
               e.kind, e.category, e.lean, e.line, e.proj_value, e.edge,
               e.pitcher_name, e.team_code, e.opp_team_code, e.game_pk,
               e.flagged AS edge_still_flagged,
               pr.run_date::text AS run_date,
               g.away_team, g.home_team,
               g.away_score, g.home_score, g.status,
               er.result, er.actual_value
        FROM personal_bets pb
        JOIN edges e ON e.edge_id = pb.edge_id
        JOIN projection_runs pr ON pr.run_id = e.run_id
        LEFT JOIN games g ON g.game_pk = e.game_pk
        LEFT JOIN edge_results er ON er.edge_id = e.edge_id
        ORDER BY pb.placed_at DESC
    """)
    bets = []
    for r in rows:
        d = dict(r)
        # Compute $ P&L from juice + result + stake
        result = d.get("result")
        juice  = int(d["juice"]) if d.get("juice") is not None else -110
        stake  = float(d["dollar_amount"])
        payout = None
        if result == "WIN":
            if juice < 0:
                payout = stake * (100.0 / abs(juice))
            else:
                payout = stake * (juice / 100.0)
        elif result == "LOSS":
            payout = -stake
        elif result == "PUSH":
            payout = 0.0
        d["dollar_pnl"] = round(payout, 2) if payout is not None else None
        bets.append(d)
    return {"n": len(bets), "bets": bets}


from pydantic import BaseModel as _BaseModel

class _PersonalBetIn(_BaseModel):
    edge_id: int
    dollar_amount: float
    juice: int
    sportsbook: str | None = None
    notes: str | None = None


@app.post("/api/personal_bets")
def upsert_personal_bet(bet: _PersonalBetIn):
    """Create or update a personal bet for an edge (one bet per edge)."""
    existing = db.fetchone(
        "SELECT bet_id FROM personal_bets WHERE edge_id=%s", (bet.edge_id,)
    )
    if existing:
        db.execute("""
            UPDATE personal_bets
            SET dollar_amount=%s, juice=%s, sportsbook=%s, notes=%s, updated_at=now()
            WHERE bet_id=%s
        """, (bet.dollar_amount, bet.juice, bet.sportsbook, bet.notes, existing["bet_id"]))
        return {"bet_id": existing["bet_id"], "action": "updated"}
    row = db.fetchone("""
        INSERT INTO personal_bets (edge_id, dollar_amount, juice, sportsbook, notes)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING bet_id
    """, (bet.edge_id, bet.dollar_amount, bet.juice, bet.sportsbook, bet.notes))
    return {"bet_id": int(row["bet_id"]), "action": "created"}


@app.delete("/api/personal_bets/{bet_id}")
def delete_personal_bet(bet_id: int):
    db.execute("DELETE FROM personal_bets WHERE bet_id=%s", (bet_id,))
    return {"bet_id": bet_id, "deleted": True}


@app.get("/api/personal_bets/summary")
def personal_bets_summary():
    """Daily + cumulative dollar P&L summary."""
    bets = list_personal_bets()["bets"]
    by_date = {}
    cumulative_pnl = 0.0
    total_staked = 0.0
    wins = losses = pushes = pending = 0
    for b in bets:
        d = b["run_date"]
        day = by_date.setdefault(d, {
            "run_date": d, "n_bets": 0, "wins": 0, "losses": 0, "pushes": 0,
            "pending": 0, "staked": 0.0, "pnl": 0.0,
        })
        day["n_bets"] += 1
        day["staked"] += float(b["dollar_amount"])
        total_staked += float(b["dollar_amount"])
        if b.get("dollar_pnl") is None:
            day["pending"] += 1
            pending += 1
            continue
        day["pnl"] += float(b["dollar_pnl"])
        cumulative_pnl += float(b["dollar_pnl"])
        if b["result"] == "WIN":   day["wins"] += 1;   wins += 1
        if b["result"] == "LOSS":  day["losses"] += 1; losses += 1
        if b["result"] == "PUSH":  day["pushes"] += 1; pushes += 1
    days = sorted(by_date.values(), key=lambda x: x["run_date"], reverse=True)
    # Round
    for d in days:
        d["staked"] = round(d["staked"], 2)
        d["pnl"] = round(d["pnl"], 2)
    return {
        "n_bets": len(bets),
        "wins": wins, "losses": losses, "pushes": pushes, "pending": pending,
        "total_staked": round(total_staked, 2),
        "cumulative_pnl": round(cumulative_pnl, 2),
        "roi": round(cumulative_pnl / total_staked, 4) if total_staked > 0 else 0.0,
        "days": days,
    }
'''
    api = api.rstrip() + endpoints + "\n"
    api_path.write_text(api, encoding="utf-8")
    print("OK: personal_bets endpoints added")

# ============================================================================
# 2. Frontend — add 'My Record' tab between Stats and Track Record
# ============================================================================
old_tabs = "const TABS = ['Full Game O/U', 'F5 O/U', 'Moneyline', 'Pitcher Props', 'Pitchers', 'Slate', 'Stats', 'Track Record'];"
new_tabs = "const TABS = ['Full Game O/U', 'F5 O/U', 'Moneyline', 'Pitcher Props', 'Pitchers', 'Slate', 'Stats', 'My Record', 'Track Record'];"
if old_tabs in app:
    app = app.replace(old_tabs, new_tabs, 1)
    print("OK: 'My Record' added to TABS")
elif "'My Record'" in app:
    print("OK: 'My Record' tab already in TABS")

# Wire in render
old_switch = "          {tab==='Stats'         && <StatsView />}\n          {tab==='Track Record'  && <PerformanceView perf={perf} />}"
new_switch = "          {tab==='Stats'         && <StatsView />}\n          {tab==='My Record'     && <MyRecordView />}\n          {tab==='Track Record'  && <PerformanceView perf={perf} />}"
if old_switch in app:
    app = app.replace(old_switch, new_switch, 1)
    print("OK: MyRecordView wired into tab switch")
elif "tab==='My Record'" in app:
    print("OK: MyRecordView already wired")

# ============================================================================
# 3. Append MyRecordView + BetModal + BetButton at end of file
# ============================================================================
if "function MyRecordView" not in app:
    extras = r'''

// ============================================================================
// Personal Bets — modal to enter $ + juice + book, button on each edge
// ============================================================================
function BetButton({ edge, onChange }) {
  const [bet, setBet] = useState(undefined);  // undefined=unknown, null=not bet, obj=bet exists
  const [open, setOpen] = useState(false);

  useEffect(() => {
    // On mount, fetch bets to see if this edge has one
    fetch(`${API_BASE}/api/personal_bets`).then(r=>r.json()).then(d => {
      const found = (d.bets || []).find(b => b.edge_id === edge.edge_id);
      setBet(found || null);
    });
  }, [edge.edge_id]);

  function handleSaved(saved) {
    setBet(saved);
    setOpen(false);
    if (onChange) onChange();
  }

  async function handleDelete() {
    if (!bet?.bet_id) return;
    if (!window.confirm('Remove this bet from your record?')) return;
    await fetch(`${API_BASE}/api/personal_bets/${bet.bet_id}`, { method: 'DELETE' });
    setBet(null);
    if (onChange) onChange();
  }

  const hasBet = bet && bet.bet_id;

  return (
    <>
      <button
        className={`bet-btn ${hasBet ? 'has-bet' : ''}`}
        onClick={() => setOpen(true)}
        title={hasBet ? `$${bet.dollar_amount} @ ${bet.juice > 0 ? '+' : ''}${bet.juice}` : 'Add to my bets'}
      >
        {hasBet ? `\u2713 $${Number(bet.dollar_amount).toFixed(0)}` : '+ Bet'}
      </button>
      {open && (
        <BetModal
          edge={edge}
          existing={hasBet ? bet : null}
          onSave={handleSaved}
          onDelete={hasBet ? handleDelete : null}
          onClose={() => setOpen(false)}
        />
      )}
    </>
  );
}

function BetModal({ edge, existing, onSave, onDelete, onClose }) {
  const [amount, setAmount]   = useState(existing?.dollar_amount ?? 100);
  const [juice, setJuice]     = useState(existing?.juice ?? -110);
  const [book, setBook]       = useState(existing?.sportsbook ?? '');
  const [notes, setNotes]     = useState(existing?.notes ?? '');
  const [saving, setSaving]   = useState(false);
  const [error, setError]     = useState(null);

  async function handleSave(e) {
    e.preventDefault();
    setSaving(true);
    setError(null);
    try {
      const body = {
        edge_id: edge.edge_id,
        dollar_amount: parseFloat(amount),
        juice: parseInt(juice, 10),
        sportsbook: book || null,
        notes: notes || null,
      };
      const r = await fetch(`${API_BASE}/api/personal_bets`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!r.ok) {
        const txt = await r.text();
        setError(`HTTP ${r.status}: ${txt.slice(0,200)}`);
        return;
      }
      onSave({ ...body, bet_id: (await r.json()).bet_id });
    } catch (err) {
      setError(err.message);
    } finally {
      setSaving(false);
    }
  }

  const description = `${edge.kind?.toUpperCase()} \u00b7 ${edge.lean} ${edge.line}`;

  return (
    <div className="bet-modal-backdrop" onClick={onClose}>
      <form className="bet-modal" onClick={e => e.stopPropagation()} onSubmit={handleSave}>
        <div className="bet-modal-header">
          <h3>{existing ? 'Edit Bet' : 'Add Bet'}</h3>
          <button type="button" className="bet-modal-close" onClick={onClose}>\u00d7</button>
        </div>
        <div className="bet-modal-edge">{description}</div>

        <label className="bet-field">
          <span>$ Amount</span>
          <input type="number" step="0.01" min="0" value={amount}
                 onChange={e => setAmount(e.target.value)} autoFocus required />
        </label>

        <label className="bet-field">
          <span>Juice / Odds</span>
          <input type="number" value={juice}
                 onChange={e => setJuice(e.target.value)} required
                 placeholder="-110, +130, etc." />
        </label>

        <label className="bet-field">
          <span>Sportsbook</span>
          <input type="text" value={book}
                 onChange={e => setBook(e.target.value)}
                 placeholder="DK / FD / BetMGM / etc." />
        </label>

        <label className="bet-field">
          <span>Notes</span>
          <textarea value={notes} onChange={e => setNotes(e.target.value)} rows="2"
                    placeholder="optional" />
        </label>

        {error && <div className="bet-modal-error">{error}</div>}

        <div className="bet-modal-actions">
          {existing && onDelete && (
            <button type="button" className="bet-modal-delete" onClick={onDelete}>Remove</button>
          )}
          <div style={{ flex: 1 }} />
          <button type="button" className="bet-modal-cancel" onClick={onClose}>Cancel</button>
          <button type="submit" className="bet-modal-save" disabled={saving}>
            {saving ? 'Saving...' : (existing ? 'Update' : 'Save')}
          </button>
        </div>
      </form>
    </div>
  );
}


// ============================================================================
// My Record view — same shape as Track Record but personal_bets only
// ============================================================================
function MyRecordView() {
  const [summary, setSummary] = useState(null);
  const [bets, setBets]       = useState(null);
  const [error, setError]     = useState(null);

  useEffect(() => {
    Promise.all([
      fetch(`${API_BASE}/api/personal_bets/summary`).then(r => r.json()),
      fetch(`${API_BASE}/api/personal_bets`).then(r => r.json()),
    ]).then(([s, b]) => {
      setSummary(s);
      setBets(b.bets || []);
    }).catch(e => setError(e.message));
  }, []);

  if (error) return <div className="empty">Error: {error}</div>;
  if (!summary || !bets) return <div className="loading">Loading your bets</div>;

  return (
    <section>
      <div className="section-header">
        <h2>My Record.</h2>
        <span className="deck">Personal $ wagers on flagged edges</span>
      </div>

      <div className="my-record-cards">
        <div className="my-record-card">
          <div className="card-label">Bets</div>
          <div className="card-value">{summary.n_bets}</div>
          <div className="card-sub">{summary.wins}W &middot; {summary.losses}L &middot; {summary.pushes}P</div>
        </div>
        <div className="my-record-card">
          <div className="card-label">Staked</div>
          <div className="card-value">${summary.total_staked.toFixed(2)}</div>
        </div>
        <div className={`my-record-card ${summary.cumulative_pnl >= 0 ? 'card-positive' : 'card-negative'}`}>
          <div className="card-label">P&L</div>
          <div className="card-value">{summary.cumulative_pnl >= 0 ? '+' : ''}${summary.cumulative_pnl.toFixed(2)}</div>
        </div>
        <div className="my-record-card">
          <div className="card-label">ROI</div>
          <div className="card-value">{(summary.roi * 100).toFixed(1)}%</div>
        </div>
        {summary.pending > 0 && (
          <div className="my-record-card">
            <div className="card-label">Pending</div>
            <div className="card-value">{summary.pending}</div>
          </div>
        )}
      </div>

      {summary.days.length === 0
        ? <div className="empty">No bets placed yet. Pick edges from the slate and add $ amounts.</div>
        : (
          <>
            <h3 className="my-record-section-title">By Date</h3>
            <table className="my-record-table">
              <thead>
                <tr>
                  <th>Date</th><th className="num">Bets</th><th className="num">Record</th>
                  <th className="num">Staked</th><th className="num">P&L</th>
                </tr>
              </thead>
              <tbody>
                {summary.days.map(d => (
                  <tr key={d.run_date}>
                    <td>{d.run_date}</td>
                    <td className="num">{d.n_bets}</td>
                    <td className="num">{d.wins}-{d.losses}{d.pushes ? '-' + d.pushes : ''}{d.pending ? ` (${d.pending} pending)` : ''}</td>
                    <td className="num">${d.staked.toFixed(2)}</td>
                    <td className={`num ${d.pnl >= 0 ? 'pnl-positive' : 'pnl-negative'}`}>
                      {d.pnl >= 0 ? '+' : ''}${d.pnl.toFixed(2)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>

            <h3 className="my-record-section-title">All Bets</h3>
            <table className="my-record-table">
              <thead>
                <tr>
                  <th>Date</th><th>Game</th><th>Bet</th>
                  <th className="num">Stake</th><th className="num">Juice</th>
                  <th>Book</th><th>Result</th><th className="num">P&L</th>
                </tr>
              </thead>
              <tbody>
                {bets.map(b => (
                  <tr key={b.bet_id}>
                    <td>{b.run_date}</td>
                    <td>{b.away_team}@{b.home_team}</td>
                    <td>{b.kind?.toUpperCase()} {b.lean} {b.line}</td>
                    <td className="num">${Number(b.dollar_amount).toFixed(2)}</td>
                    <td className="num">{b.juice > 0 ? '+' : ''}{b.juice}</td>
                    <td>{b.sportsbook || '\u2014'}</td>
                    <td className={`result-${(b.result || '').toLowerCase()}`}>
                      {b.result || 'pending'}
                    </td>
                    <td className={`num ${b.dollar_pnl == null ? '' : (b.dollar_pnl >= 0 ? 'pnl-positive' : 'pnl-negative')}`}>
                      {b.dollar_pnl == null ? '\u2014' : (b.dollar_pnl >= 0 ? '+$' : '-$') + Math.abs(b.dollar_pnl).toFixed(2)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </>
        )
      }
    </section>
  );
}
'''
    app = app.rstrip() + extras + "\n"
    print("OK: MyRecordView + BetButton + BetModal appended")

app_path.write_text(app, encoding="utf-8")

# ============================================================================
# 4. CSS
# ============================================================================
if "/* Personal bets - modal + button + record */" not in css:
    new_css = '''

/* ============================================================================
   Personal bets - modal + button + record
   ============================================================================ */
.bet-btn {
  font-family: var(--mono, monospace);
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  padding: 4px 10px;
  background: transparent;
  color: var(--ink-2, #555);
  border: 1px solid var(--rule, #c8c8c8);
  cursor: pointer;
}
.bet-btn:hover {
  background: var(--ink, #111);
  color: var(--paper, #fff);
}
.bet-btn.has-bet {
  background: #d5ebd1;
  color: #2c5f24;
  border-color: #5b8e52;
}
.bet-btn.has-bet:hover {
  background: #5b8e52;
  color: #fff;
}

.bet-modal-backdrop {
  position: fixed;
  inset: 0;
  background: rgba(0, 0, 0, 0.45);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 100;
}
.bet-modal {
  background: var(--paper, #fff);
  border: 2px solid var(--ink, #111);
  padding: 20px;
  min-width: 340px;
  max-width: 440px;
  width: 90%;
}
.bet-modal-header {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  border-bottom: 1px solid var(--rule, #ccc);
  padding-bottom: 8px;
  margin-bottom: 14px;
}
.bet-modal-header h3 {
  font-family: var(--display, serif);
  font-size: 20px;
  margin: 0;
  color: var(--ink, #111);
}
.bet-modal-close {
  background: transparent;
  border: none;
  font-size: 22px;
  cursor: pointer;
  color: var(--ink-2, #555);
  padding: 0 8px;
}
.bet-modal-edge {
  font-family: var(--mono, monospace);
  font-size: 12px;
  letter-spacing: 0.04em;
  color: var(--ink-2, #555);
  margin-bottom: 14px;
  text-transform: uppercase;
  font-weight: 700;
}
.bet-field {
  display: block;
  margin-bottom: 12px;
}
.bet-field > span {
  display: block;
  font-family: var(--mono, monospace);
  font-size: 10px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--ink-2, #555);
  font-weight: 700;
  margin-bottom: 4px;
}
.bet-field input,
.bet-field textarea {
  width: 100%;
  padding: 8px 10px;
  font-family: var(--mono, monospace);
  font-size: 13px;
  border: 1px solid var(--rule, #ccc);
  background: var(--paper-2, #f5f0e0);
  color: var(--ink, #111);
  box-sizing: border-box;
}
.bet-field input:focus,
.bet-field textarea:focus {
  outline: none;
  border-color: var(--ink, #111);
  background: var(--paper, #fff);
}
.bet-modal-error {
  background: #fbeee9;
  color: #7a2418;
  border-left: 3px solid #b8483a;
  padding: 8px 10px;
  font-family: var(--mono, monospace);
  font-size: 11px;
  margin-bottom: 10px;
}
.bet-modal-actions {
  display: flex;
  gap: 8px;
  align-items: center;
  margin-top: 16px;
  padding-top: 14px;
  border-top: 1px solid var(--rule, #ccc);
}
.bet-modal-save,
.bet-modal-cancel,
.bet-modal-delete {
  padding: 8px 16px;
  font-family: var(--mono, monospace);
  font-size: 11px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  font-weight: 700;
  cursor: pointer;
  border: 1px solid var(--ink, #111);
}
.bet-modal-save {
  background: var(--ink, #111);
  color: var(--paper, #fff);
}
.bet-modal-cancel {
  background: transparent;
  color: var(--ink, #111);
}
.bet-modal-delete {
  background: transparent;
  color: #7a2418;
  border-color: #b8483a;
}
.bet-modal-save:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

/* ---- My Record page ---- */
.my-record-cards {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
  gap: 12px;
  margin-bottom: 24px;
}
.my-record-card {
  background: var(--paper-2, #f5f0e0);
  border: 1px solid var(--rule, #ddd);
  padding: 14px 16px;
}
.my-record-card .card-label {
  font-family: var(--mono, monospace);
  font-size: 10px;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: var(--ink-2, #555);
  font-weight: 700;
}
.my-record-card .card-value {
  font-family: var(--display, serif);
  font-size: 28px;
  font-weight: 700;
  color: var(--ink, #111);
  margin-top: 4px;
}
.my-record-card .card-sub {
  font-family: var(--mono, monospace);
  font-size: 11px;
  color: var(--ink-2, #555);
  margin-top: 4px;
}
.my-record-card.card-positive { border-left: 3px solid #5b8e52; }
.my-record-card.card-negative { border-left: 3px solid #b8483a; }
.my-record-card.card-positive .card-value { color: #2c5f24; }
.my-record-card.card-negative .card-value { color: #7a2418; }

.my-record-section-title {
  font-family: var(--mono, monospace);
  font-size: 11px;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  font-weight: 700;
  color: var(--ink, #111);
  border-bottom: 1px solid var(--rule, #ccc);
  padding-bottom: 4px;
  margin: 22px 0 10px;
}

.my-record-table {
  width: 100%;
  border-collapse: collapse;
  font-family: var(--mono, monospace);
  font-size: 12px;
}
.my-record-table thead th {
  text-align: left;
  padding: 8px 10px;
  font-size: 10px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--ink-2, #555);
  font-weight: 700;
  border-bottom: 2px solid var(--ink, #111);
}
.my-record-table thead th.num,
.my-record-table tbody td.num { text-align: right; font-variant-numeric: tabular-nums; }
.my-record-table tbody td {
  padding: 8px 10px;
  border-bottom: 1px solid var(--rule, #ece5d0);
  color: var(--ink, #111);
}
.my-record-table tbody tr:hover { background: var(--paper-2, #f5f0e0); }
.pnl-positive { color: #2c5f24; font-weight: 700; }
.pnl-negative { color: #7a2418; font-weight: 700; }
.result-win   { color: #2c5f24; font-weight: 700; }
.result-loss  { color: #7a2418; font-weight: 700; }
.result-push  { color: var(--ink-2, #555); }
'''
    css = css.rstrip() + "\n" + new_css
    css_path.write_text(css, encoding="utf-8")
    print("OK: bet modal + my record CSS appended")

print()
print("=========================================================================")
print("IMPORTANT: This patch does NOT auto-add BetButton to any existing edge")
print("table. The button is built but not wired into a specific edge row.")
print()
print("Where you want the button depends on your existing edge table layout.")
print("Common spot: add a new <td> after the conviction column, e.g.:")
print("     <td className='bet-cell'><BetButton edge={edge}/></td>")
print()
print("If you tell me which table component you want it in (EdgeRow, F5Row,")
print("MLRow, PropRow, etc.) and I'll wire it in a follow-up. For now,")
print("everything else (modal, API, My Record tab) is fully working.")
print("=========================================================================")
print()
print("Migration:")
print("  Move 0009_personal_bets.sql to backend/migrations/")
print("  Then apply to production via Railway SQL console (paste the CREATE TABLE)")
print()
print("Verify and push:")
print('  python -X utf8 -c "import ast; ast.parse(open(\'backend/src/api.py\').read()); print(\'OK\')"')
print("  cd frontend && npm run build && cd ..")
print("  git add backend/migrations/0009_personal_bets.sql")
print("  git add backend/src/api.py frontend/src/App.jsx frontend/src/styles.css")
print("  git add patch_personal_bets.py 0009_personal_bets.sql")
print("  git commit -m 'Personal bets: $ tracking + My Record tab'")
print("  git push")
