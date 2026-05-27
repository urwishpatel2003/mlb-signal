"""
Run from repo root: python patch_bet_opposite_side.py

Lets the user pick which side of a flagged edge they bet on:
  - Default: same as model's lean (most common)
  - Toggle in modal: bet the opposite side (fade the model)

Pieces:
  1. Migration 0010 must be applied (lean_taken column on personal_bets).
  2. Backend: model accepts lean_taken; P&L computation uses lean_taken
     against the actual_value to determine win/loss/push correctly when
     user faded the edge.
  3. Frontend modal: OVER/UNDER (or HOME team/AWAY team for ML) toggle.
  4. BetButton label shows lean_taken; My Record shows a 'Fade' indicator
     when lean_taken != edge.lean.
"""
from pathlib import Path

api_path = Path("backend/src/api.py")
app_path = Path("frontend/src/App.jsx")
css_path = Path("frontend/src/styles.css")
api = api_path.read_text(encoding="utf-8")
app = app_path.read_text(encoding="utf-8")
css = css_path.read_text(encoding="utf-8")

# ============================================================================
# 1. Backend — update Pydantic model to require lean_taken
# ============================================================================
old_model = '''class _PersonalBetIn(_BaseModel):
    edge_id: int
    dollar_amount: float
    juice: int
    sportsbook: str | None = None
    notes: str | None = None'''

new_model = '''class _PersonalBetIn(_BaseModel):
    edge_id: int
    dollar_amount: float
    juice: int
    lean_taken: str            # which side the user actually bet (OVER/UNDER/team code)
    sportsbook: str | None = None
    notes: str | None = None'''

if old_model in api:
    api = api.replace(old_model, new_model, 1)
    print("OK: _PersonalBetIn now includes lean_taken")
elif "lean_taken: str" in api:
    print("OK: _PersonalBetIn already updated")

# Update upsert endpoint to persist lean_taken
old_upsert = '''    if existing:
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
    return {"bet_id": int(row["bet_id"]), "action": "created"}'''

new_upsert = '''    if existing:
        db.execute("""
            UPDATE personal_bets
            SET dollar_amount=%s, juice=%s, lean_taken=%s, sportsbook=%s, notes=%s, updated_at=now()
            WHERE bet_id=%s
        """, (bet.dollar_amount, bet.juice, bet.lean_taken, bet.sportsbook, bet.notes, existing["bet_id"]))
        return {"bet_id": existing["bet_id"], "action": "updated"}
    row = db.fetchone("""
        INSERT INTO personal_bets (edge_id, dollar_amount, juice, lean_taken, sportsbook, notes)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING bet_id
    """, (bet.edge_id, bet.dollar_amount, bet.juice, bet.lean_taken, bet.sportsbook, bet.notes))
    return {"bet_id": int(row["bet_id"]), "action": "created"}'''

if old_upsert in api:
    api = api.replace(old_upsert, new_upsert, 1)
    print("OK: upsert endpoint now persists lean_taken")
elif "lean_taken=%s" in api:
    print("OK: upsert already persists lean_taken")

# Update list endpoint: return lean_taken AND recompute P&L based on it
old_list_sql = '''        SELECT pb.bet_id, pb.edge_id, pb.dollar_amount, pb.juice,
               pb.sportsbook, pb.notes,
               pb.placed_at::text AS placed_at,
               pb.updated_at::text AS updated_at,
               e.kind, e.category, e.lean, e.line, e.proj_value, e.edge,
               e.pitcher_name, e.team_code, e.opp_team_code, e.game_pk,
               e.flagged AS edge_still_flagged,
               pr.run_date::text AS run_date,
               g.away_team, g.home_team,
               g.away_score, g.home_score, g.status,
               er.result, er.actual_value'''

new_list_sql = '''        SELECT pb.bet_id, pb.edge_id, pb.dollar_amount, pb.juice,
               pb.lean_taken,
               pb.sportsbook, pb.notes,
               pb.placed_at::text AS placed_at,
               pb.updated_at::text AS updated_at,
               e.kind, e.category, e.lean, e.line, e.proj_value, e.edge,
               e.pitcher_name, e.team_code, e.opp_team_code, e.game_pk,
               e.flagged AS edge_still_flagged,
               pr.run_date::text AS run_date,
               g.away_team, g.home_team,
               g.away_score, g.home_score, g.status,
               er.result, er.actual_value'''

if old_list_sql in api:
    api = api.replace(old_list_sql, new_list_sql, 1)
    print("OK: list SQL now returns lean_taken")
elif "pb.lean_taken," in api:
    print("OK: list SQL already returns lean_taken")

# Replace the P&L computation to honor lean_taken vs the edge's original lean.
# If they match -> use grader result directly. If user faded -> invert WIN/LOSS.
old_pnl_block = '''    bets = []
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
    return {"n": len(bets), "bets": bets}'''

new_pnl_block = '''    bets = []
    for r in rows:
        d = dict(r)
        # The grader's `result` is computed for the EDGE'S lean.
        # If the user took the same side, that result applies directly.
        # If the user FADED the edge (took opposite), invert WIN <-> LOSS.
        result = d.get("result")
        edge_lean = d.get("lean")
        lean_taken = d.get("lean_taken") or edge_lean    # fallback if NULL
        if result == "WIN" and lean_taken != edge_lean:  result = "LOSS"
        elif result == "LOSS" and lean_taken != edge_lean: result = "WIN"
        # PUSH and None stay the same regardless of side taken
        d["user_result"] = result   # this is the per-USER result
        # Compute $ P&L from juice + user_result + stake
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
        d["is_fade"] = bool(lean_taken and edge_lean and lean_taken != edge_lean)
        bets.append(d)
    return {"n": len(bets), "bets": bets}'''

if old_pnl_block in api:
    api = api.replace(old_pnl_block, new_pnl_block, 1)
    print("OK: P&L computation honors lean_taken (fade inverts WIN/LOSS)")
elif "user_result" in api:
    print("OK: P&L already honors lean_taken")

# Update summary to use user_result instead of result
old_summary_logic = '''        if b.get("dollar_pnl") is None:
            day["pending"] += 1
            pending += 1
            continue
        day["pnl"] += float(b["dollar_pnl"])
        cumulative_pnl += float(b["dollar_pnl"])
        if b["result"] == "WIN":   day["wins"] += 1;   wins += 1
        if b["result"] == "LOSS":  day["losses"] += 1; losses += 1
        if b["result"] == "PUSH":  day["pushes"] += 1; pushes += 1'''

new_summary_logic = '''        if b.get("dollar_pnl") is None:
            day["pending"] += 1
            pending += 1
            continue
        day["pnl"] += float(b["dollar_pnl"])
        cumulative_pnl += float(b["dollar_pnl"])
        ur = b.get("user_result")
        if ur == "WIN":   day["wins"] += 1;   wins += 1
        if ur == "LOSS":  day["losses"] += 1; losses += 1
        if ur == "PUSH":  day["pushes"] += 1; pushes += 1'''

if old_summary_logic in api:
    api = api.replace(old_summary_logic, new_summary_logic, 1)
    print("OK: summary uses user_result")

api_path.write_text(api, encoding="utf-8")

# ============================================================================
# 2. Frontend BetModal — add side toggle
# ============================================================================
old_modal_state = '''function BetModal({ edge, existing, onSave, onDelete, onClose }) {
  const [amount, setAmount]   = useState(existing?.dollar_amount ?? 100);
  const [juice, setJuice]     = useState(existing?.juice ?? -110);
  const [book, setBook]       = useState(existing?.sportsbook ?? '');
  const [notes, setNotes]     = useState(existing?.notes ?? '');
  const [saving, setSaving]   = useState(false);
  const [error, setError]     = useState(null);'''

new_modal_state = '''function BetModal({ edge, existing, onSave, onDelete, onClose }) {
  // Available sides depend on edge kind.
  //   total/f5/prop -> OVER / UNDER
  //   ml            -> home team / away team
  const sideOptions = edge.kind === 'ml'
    ? [edge.team_code, edge.opp_team_code].filter(Boolean)
    : ['OVER', 'UNDER'];
  const defaultSide = existing?.lean_taken ?? edge.lean ?? sideOptions[0];
  const [side, setSide]       = useState(defaultSide);
  const [amount, setAmount]   = useState(existing?.dollar_amount ?? 100);
  const [juice, setJuice]     = useState(existing?.juice ?? -110);
  const [book, setBook]       = useState(existing?.sportsbook ?? '');
  const [notes, setNotes]     = useState(existing?.notes ?? '');
  const [saving, setSaving]   = useState(false);
  const [error, setError]     = useState(null);
  const isFade = side !== edge.lean;'''

if old_modal_state in app:
    app = app.replace(old_modal_state, new_modal_state, 1)
    print("OK: BetModal state expanded with side toggle")
elif "const [side, setSide]" in app:
    print("OK: BetModal already has side state")

# Add side to the POST body
old_body = '''      const body = {
        edge_id: edge.edge_id,
        dollar_amount: parseFloat(amount),
        juice: parseInt(juice, 10),
        sportsbook: book || null,
        notes: notes || null,
      };'''

new_body = '''      const body = {
        edge_id: edge.edge_id,
        dollar_amount: parseFloat(amount),
        juice: parseInt(juice, 10),
        lean_taken: side,
        sportsbook: book || null,
        notes: notes || null,
      };'''

if old_body in app:
    app = app.replace(old_body, new_body, 1)
    print("OK: BetModal POST body now includes lean_taken")
elif "lean_taken: side" in app:
    print("OK: BetModal POST already sends lean_taken")

# Insert the side toggle UI right after the description line and before the $ field.
# Pattern: locate the existing $ amount label.
old_dollar_label = '''        <label className="bet-field">
          <span>$ Amount</span>
          <input type="number" step="0.01" min="0" value={amount}
                 onChange={e => setAmount(e.target.value)} autoFocus required />
        </label>'''

new_dollar_label = '''        <div className="bet-field">
          <span>Side {isFade && <span className="fade-warn">(fading model)</span>}</span>
          <div className="bet-side-toggle">
            {sideOptions.map(opt => (
              <button
                key={opt}
                type="button"
                className={`bet-side-btn ${side === opt ? 'active' : ''} ${opt === edge.lean ? 'model-side' : 'fade-side'}`}
                onClick={() => setSide(opt)}
              >
                {opt}
                {opt === edge.lean && <span className="bet-side-tag">Model</span>}
              </button>
            ))}
          </div>
        </div>

        <label className="bet-field">
          <span>$ Amount</span>
          <input type="number" step="0.01" min="0" value={amount}
                 onChange={e => setAmount(e.target.value)} autoFocus required />
        </label>'''

if old_dollar_label in app:
    app = app.replace(old_dollar_label, new_dollar_label, 1)
    print("OK: BetModal shows side toggle")
elif "bet-side-toggle" in app:
    print("OK: BetModal already has side toggle UI")

# ============================================================================
# 3. BetButton label — show side taken
# ============================================================================
old_button_label = '''        {hasBet ? `\u2713 $${Number(bet.dollar_amount).toFixed(0)}` : '+ Bet'}'''

new_button_label = '''        {hasBet
          ? `\u2713 $${Number(bet.dollar_amount).toFixed(0)} ${bet.lean_taken || ''}`.trim()
          : '+ Bet'}'''

if old_button_label in app:
    app = app.replace(old_button_label, new_button_label, 1)
    print("OK: BetButton label shows side taken")
elif "bet.lean_taken" in app:
    print("OK: BetButton already shows lean_taken")

# ============================================================================
# 4. My Record table — show side + fade indicator + use user_result not result
# ============================================================================
old_bet_cell = '''                <tr key={b.bet_id}>
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
                  </tr>'''

new_bet_cell = '''                <tr key={b.bet_id} className={b.is_fade ? 'is-fade-row' : ''}>
                    <td>{b.run_date}</td>
                    <td>{b.away_team}@{b.home_team}</td>
                    <td>
                      {b.kind?.toUpperCase()} {b.lean_taken || b.lean} {b.line}
                      {b.is_fade && <span className="fade-pill">FADE</span>}
                    </td>
                    <td className="num">${Number(b.dollar_amount).toFixed(2)}</td>
                    <td className="num">{b.juice > 0 ? '+' : ''}{b.juice}</td>
                    <td>{b.sportsbook || '\u2014'}</td>
                    <td className={`result-${(b.user_result || '').toLowerCase()}`}>
                      {b.user_result || 'pending'}
                    </td>
                    <td className={`num ${b.dollar_pnl == null ? '' : (b.dollar_pnl >= 0 ? 'pnl-positive' : 'pnl-negative')}`}>
                      {b.dollar_pnl == null ? '\u2014' : (b.dollar_pnl >= 0 ? '+$' : '-$') + Math.abs(b.dollar_pnl).toFixed(2)}
                    </td>
                  </tr>'''

if old_bet_cell in app:
    app = app.replace(old_bet_cell, new_bet_cell, 1)
    print("OK: My Record row shows side taken + fade indicator")
elif "is-fade-row" in app:
    print("OK: My Record already shows fade indicator")

# Bet column header
old_th = "                  <th>Date</th><th>Game</th><th>Bet</th>\n                  <th className=\"num\">Stake</th>"
new_th = "                  <th>Date</th><th>Game</th><th>Bet (side)</th>\n                  <th className=\"num\">Stake</th>"
if old_th in app:
    app = app.replace(old_th, new_th, 1)
    print("OK: My Record header updated")

app_path.write_text(app, encoding="utf-8")

# ============================================================================
# 5. CSS for side toggle + fade indicator
# ============================================================================
if "/* Bet side toggle */" in css:
    print("OK: side toggle CSS already present")
else:
    new_css = '''

/* ============================================================================
   Bet side toggle + fade indicator
   ============================================================================ */
.bet-side-toggle {
  display: flex;
  gap: 8px;
}
.bet-side-btn {
  flex: 1;
  position: relative;
  padding: 10px 14px;
  font-family: var(--mono, monospace);
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  background: var(--paper-2, #f5f0e0);
  color: var(--ink-2, #555);
  border: 1px solid var(--rule, #c8c8c8);
  cursor: pointer;
}
.bet-side-btn:hover {
  background: var(--paper, #fff);
  color: var(--ink, #111);
}
.bet-side-btn.active {
  background: var(--ink, #111);
  color: var(--paper, #fff);
  border-color: var(--ink, #111);
}
.bet-side-btn.fade-side.active {
  background: #b8483a;
  border-color: #b8483a;
}
.bet-side-tag {
  display: block;
  font-size: 8px;
  font-weight: 600;
  opacity: 0.7;
  margin-top: 2px;
  letter-spacing: 0.1em;
}
.fade-warn {
  font-size: 9px;
  font-weight: 700;
  color: #b8483a;
  margin-left: 6px;
  letter-spacing: 0.06em;
  text-transform: uppercase;
}

/* My Record fade indicator */
.fade-pill {
  display: inline-block;
  margin-left: 6px;
  padding: 2px 6px;
  font-family: var(--mono, monospace);
  font-size: 9px;
  font-weight: 700;
  letter-spacing: 0.06em;
  background: #b8483a;
  color: #fff;
  border-radius: 2px;
  vertical-align: middle;
}
.is-fade-row {
  background: rgba(184, 72, 58, 0.04);
}
'''
    css = css.rstrip() + "\n" + new_css
    css_path.write_text(css, encoding="utf-8")
    print("OK: side toggle + fade CSS appended")

print()
print("=========================================================================")
print("Migration first:")
print("  Move 0010_personal_bets_lean.sql to backend/migrations/")
print()
print("In Railway SQL console, run:")
print("  ALTER TABLE personal_bets")
print("      ADD COLUMN IF NOT EXISTS lean_taken VARCHAR(8);")
print("  UPDATE personal_bets pb")
print("    SET lean_taken = e.lean")
print("    FROM edges e")
print("    WHERE pb.edge_id = e.edge_id AND pb.lean_taken IS NULL;")
print("  ALTER TABLE personal_bets ALTER COLUMN lean_taken SET NOT NULL;")
print()
print("Then:")
print('  python -X utf8 -c "import ast; ast.parse(open(\'backend/src/api.py\').read()); print(\'OK\')"')
print("  cd frontend && npm run build && cd ..")
print("  git add backend/migrations/0010_personal_bets_lean.sql")
print("  git add backend/src/api.py frontend/src/App.jsx frontend/src/styles.css")
print("  git add patch_bet_opposite_side.py 0010_personal_bets_lean.sql")
print("  git commit -m 'Personal bets: allow taking opposite side of flagged edges'")
print("  git push")
print("=========================================================================")
