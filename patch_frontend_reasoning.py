"""
Run from repo root: python patch_frontend_reasoning.py

Wires reason_short + reason_factors into the three edge-display components in
frontend/src/App.jsx:

  - EdgeRow  (totals + props)
  - F5Row    (F5 totals)
  - MLRow    (moneylines)

Each gets a small "+" toggle next to its conviction cell. Clicking it opens
a detail row underneath showing the factor table.

Also appends the supporting CSS classes to frontend/src/styles.css.

Idempotent — safe to re-run.
"""
from pathlib import Path

app_path = Path("frontend/src/App.jsx")
css_path = Path("frontend/src/styles.css")
app = app_path.read_text(encoding="utf-8")
css = css_path.read_text(encoding="utf-8") if css_path.exists() else ""

# ============================================================================
# 1. Add a small <ReasonDetail/> component near the top, just below `fmtOddsVal`
# ============================================================================

if "function ReasonDetail" in app:
    print("OK: ReasonDetail component already present")
else:
    reason_component = """
// ============================================================================
// Reason detail — small expandable block under any edge row
// ============================================================================
function ReasonDetail({ factors }) {
  if (!factors || factors.length === 0) return null;
  return (
    <div className="reason-detail">
      <table className="reason-factors">
        <thead>
          <tr><th>Factor</th><th>Value</th><th className="impact">Impact</th></tr>
        </thead>
        <tbody>
          {factors.map((f, i) => {
            const cls = f.impact && f.impact.startsWith('+') ? 'impact-pos'
                      : f.impact && f.impact.startsWith('-') ? 'impact-neg'
                      : 'impact-neutral';
            return (
              <tr key={i}>
                <td className="factor-label">{f.label}</td>
                <td className="factor-value">{f.value}</td>
                <td className={'factor-impact ' + cls}>{f.impact}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function ReasonToggle({ open, onClick, hasFactors }) {
  if (!hasFactors) return null;
  return (
    <button
      type="button"
      className={'reason-toggle' + (open ? ' open' : '')}
      onClick={(e) => { e.stopPropagation(); onClick(); }}
      title={open ? 'Hide reasoning' : 'Show reasoning'}
      aria-label="Toggle reasoning"
    >
      {open ? '−' : '+'}
    </button>
  );
}

"""
    # Insert right before the `function EdgeRow` line — anchors the components above it
    anchor = "function EdgeRow({ edge }) {"
    if anchor not in app:
        print("ERR: could not find EdgeRow anchor in App.jsx")
        raise SystemExit(1)
    app = app.replace(anchor, reason_component + anchor, 1)
    print("OK: ReasonDetail + ReasonToggle components added")

# ============================================================================
# 2. Patch EdgeRow to be stateful, render reason_short and toggle ReasonDetail
# ============================================================================

old_edgerow = """function EdgeRow({ edge }) {
  const isProp   = edge.kind==='prop';
  const subject  = isProp ? edge.pitcher_name?.split(',')[0]??'-' : `${edge.team_code??'?'} @ ${edge.opp_team_code??'?'}`;
  const subjectSub = isProp ? `${edge.team_code??''} v ${edge.opp_team_code??''}` : null;
  const conv=edge.conviction_pct, tier=edge.confidence_tier??3;
  const isLowTrust = tier===3||conv==null;
  let convBarClass = 'conv-bar';
  if (conv!=null){ if(conv>=75)convBarClass+=' conv-strong'; else if(conv>=60)convBarClass+=' conv-medium'; else convBarClass+=' conv-weak'; }
  const relevantOdds = edge.lean==='OVER' ? fmtOddsVal(edge.over_price) : fmtOddsVal(edge.under_price);
  return (
    <div className="edge-row">
      <div className="cell-subject">
        <div className="subject-main">{subject}</div>
        {subjectSub && <div className="subject-sub">{subjectSub}</div>}
      </div>
      <div className="cell-market">{MARKET_LABELS[edge.category]||edge.category}</div>
      <div className="cell-num">{Number(edge.line).toFixed(1)}</div>
      <div className={`cell-pick lean-${edge.lean}`}>
        {edge.lean}
        {relevantOdds&&<span className="pick-odds">{relevantOdds}</span>}
      </div>
      <div className="cell-num cell-proj">{Number(edge.proj_value).toFixed(2)}</div>
      <div className="cell-conviction">
        {isLowTrust
          ? <span className="conv-na" title="Tier 3 or fallback">n/a <span className="tier-pill">T{tier}</span></span>
          : <div className="conv-cell">
              <span className="conv-value">{Number(conv).toFixed(0)}%</span>
              <div className={convBarClass}><div className="conv-bar-fill" style={{width:`${Math.min(100,Math.max(0,conv))}%`}}/></div>
            </div>}
      </div>
    </div>
  );
}"""

new_edgerow = """function EdgeRow({ edge }) {
  const [open, setOpen] = useState(false);
  const isProp   = edge.kind==='prop';
  const subject  = isProp ? edge.pitcher_name?.split(',')[0]??'-' : `${edge.team_code??'?'} @ ${edge.opp_team_code??'?'}`;
  const subjectSub = isProp ? `${edge.team_code??''} v ${edge.opp_team_code??''}` : null;
  const conv=edge.conviction_pct, tier=edge.confidence_tier??3;
  const isLowTrust = tier===3||conv==null;
  let convBarClass = 'conv-bar';
  if (conv!=null){ if(conv>=75)convBarClass+=' conv-strong'; else if(conv>=60)convBarClass+=' conv-medium'; else convBarClass+=' conv-weak'; }
  const relevantOdds = edge.lean==='OVER' ? fmtOddsVal(edge.over_price) : fmtOddsVal(edge.under_price);
  const hasFactors = !!(edge.reason_factors && edge.reason_factors.length);
  return (
    <>
    <div className="edge-row">
      <div className="cell-subject">
        <div className="subject-main">{subject}</div>
        {subjectSub && <div className="subject-sub">{subjectSub}</div>}
        {edge.reason_short && <div className="reason-short">{edge.reason_short}</div>}
      </div>
      <div className="cell-market">{MARKET_LABELS[edge.category]||edge.category}</div>
      <div className="cell-num">{Number(edge.line).toFixed(1)}</div>
      <div className={`cell-pick lean-${edge.lean}`}>
        {edge.lean}
        {relevantOdds&&<span className="pick-odds">{relevantOdds}</span>}
      </div>
      <div className="cell-num cell-proj">{Number(edge.proj_value).toFixed(2)}</div>
      <div className="cell-conviction">
        {isLowTrust
          ? <span className="conv-na" title="Tier 3 or fallback">n/a <span className="tier-pill">T{tier}</span></span>
          : <div className="conv-cell">
              <span className="conv-value">{Number(conv).toFixed(0)}%</span>
              <div className={convBarClass}><div className="conv-bar-fill" style={{width:`${Math.min(100,Math.max(0,conv))}%`}}/></div>
            </div>}
        <ReasonToggle open={open} onClick={()=>setOpen(!open)} hasFactors={hasFactors} />
      </div>
    </div>
    {open && hasFactors && <ReasonDetail factors={edge.reason_factors} />}
    </>
  );
}"""

if old_edgerow in app:
    app = app.replace(old_edgerow, new_edgerow, 1)
    print("OK: EdgeRow patched with reasoning")
elif "edge.reason_factors && edge.reason_factors.length" in app:
    print("OK: EdgeRow already patched")
else:
    print("WARN: EdgeRow original pattern not found — manual edit required")

# ============================================================================
# 3. Patch F5Row
# ============================================================================

old_f5row = """function F5Row({ edge }) {
  const conv=edge.conviction_pct, tier=edge.confidence_tier??3;
  const relevantOdds = edge.lean==='OVER' ? fmtOddsVal(edge.over_price) : fmtOddsVal(edge.under_price);
  return (
    <div className="edge-row f5-row">
      <div className="cell-subject">
        <div className="subject-main">{edge.team_code} @ {edge.opp_team_code}</div>
        <div className="subject-sub">First 5 Innings</div>
      </div>
      <div className="cell-num">{Number(edge.line).toFixed(1)}</div>
      <div className={`cell-pick lean-${edge.lean}`}>
        {edge.lean}
        {relevantOdds&&<span className="pick-odds">{relevantOdds}</span>}
      </div>
      <div className="cell-num cell-proj">{Number(edge.proj_value).toFixed(2)}</div>
      <div className="cell-num" style={{fontWeight:700,color:edge.edge>=0?'var(--moss)':'var(--vermillion)'}}>
        {edge.edge>=0?'+':''}{Number(edge.edge).toFixed(2)}
      </div>
      <div className="cell-conviction">
        {conv!=null
          ? <span className="conv-value">{Number(conv).toFixed(0)}% <span className="tier-pill">T{tier}</span></span>
          : <span className="conv-na">n/a</span>}
      </div>
    </div>
  );
}"""

new_f5row = """function F5Row({ edge }) {
  const [open, setOpen] = useState(false);
  const conv=edge.conviction_pct, tier=edge.confidence_tier??3;
  const relevantOdds = edge.lean==='OVER' ? fmtOddsVal(edge.over_price) : fmtOddsVal(edge.under_price);
  const hasFactors = !!(edge.reason_factors && edge.reason_factors.length);
  return (
    <>
    <div className="edge-row f5-row">
      <div className="cell-subject">
        <div className="subject-main">{edge.team_code} @ {edge.opp_team_code}</div>
        <div className="subject-sub">First 5 Innings</div>
        {edge.reason_short && <div className="reason-short">{edge.reason_short}</div>}
      </div>
      <div className="cell-num">{Number(edge.line).toFixed(1)}</div>
      <div className={`cell-pick lean-${edge.lean}`}>
        {edge.lean}
        {relevantOdds&&<span className="pick-odds">{relevantOdds}</span>}
      </div>
      <div className="cell-num cell-proj">{Number(edge.proj_value).toFixed(2)}</div>
      <div className="cell-num" style={{fontWeight:700,color:edge.edge>=0?'var(--moss)':'var(--vermillion)'}}>
        {edge.edge>=0?'+':''}{Number(edge.edge).toFixed(2)}
      </div>
      <div className="cell-conviction">
        {conv!=null
          ? <span className="conv-value">{Number(conv).toFixed(0)}% <span className="tier-pill">T{tier}</span></span>
          : <span className="conv-na">n/a</span>}
        <ReasonToggle open={open} onClick={()=>setOpen(!open)} hasFactors={hasFactors} />
      </div>
    </div>
    {open && hasFactors && <ReasonDetail factors={edge.reason_factors} />}
    </>
  );
}"""

if old_f5row in app:
    app = app.replace(old_f5row, new_f5row, 1)
    print("OK: F5Row patched with reasoning")
elif "edge.reason_factors && edge.reason_factors.length" in app and "F5Row" in app and app.count("reason_factors") >= 2:
    print("OK: F5Row already patched")
else:
    print("WARN: F5Row original pattern not found — manual edit required")

# ============================================================================
# 4. Patch MLRow
# ============================================================================

old_mlrow = """function MLRow({ edge }) {
  const tier = edge.confidence_tier??3;
  const edgePp = edge.ml_edge_pct!=null ? (edge.ml_edge_pct*100).toFixed(1) : edge.edge?.toFixed(1);
  const modelPct = edge.proj_value!=null ? Number(edge.proj_value).toFixed(1)+'%' : '-';
  const impliedPct = edge.ml_edge_pct!=null && edge.proj_value!=null
    ? (Number(edge.proj_value) - Number(edge.ml_edge_pct)*100).toFixed(1)+'%' : '-';
  const isPos = (edge.ml_edge_pct??0)>0;
  // ML odds: line IS the American odds for the lean team
  const mlOdds = fmtOddsVal(edge.line);
  return (
    <div className="ml-row">
      <div className="cell-subject">
        <div className="subject-main">{edge.team_code} @ {edge.opp_team_code}</div>
        <div className="subject-sub">{edge.notes||''}</div>
      </div>
      <div className={`cell-pick lean-${isPos?'OVER':'UNDER'}`} style={{fontFamily:'var(--display)',fontWeight:700}}>
        {edge.lean}
        {mlOdds&&<span className="pick-odds">{mlOdds}</span>}
      </div>
      <div className="cell-num">{fmtOdds(edge.line)}</div>
      <div className="cell-num cell-proj">{modelPct}</div>
      <div className="cell-num">{impliedPct}</div>
      <div className="cell-num" style={{fontWeight:700,color:isPos?'var(--moss)':'var(--vermillion)'}}>
        {isPos?'+':''}{edgePp}pp
      </div>
      <div><span className="tier-pill">T{tier}</span></div>
    </div>
  );
}"""

new_mlrow = """function MLRow({ edge }) {
  const [open, setOpen] = useState(false);
  const tier = edge.confidence_tier??3;
  const edgePp = edge.ml_edge_pct!=null ? (edge.ml_edge_pct*100).toFixed(1) : edge.edge?.toFixed(1);
  const modelPct = edge.proj_value!=null ? Number(edge.proj_value).toFixed(1)+'%' : '-';
  const impliedPct = edge.ml_edge_pct!=null && edge.proj_value!=null
    ? (Number(edge.proj_value) - Number(edge.ml_edge_pct)*100).toFixed(1)+'%' : '-';
  const isPos = (edge.ml_edge_pct??0)>0;
  const mlOdds = fmtOddsVal(edge.line);
  const hasFactors = !!(edge.reason_factors && edge.reason_factors.length);
  return (
    <>
    <div className="ml-row">
      <div className="cell-subject">
        <div className="subject-main">{edge.team_code} @ {edge.opp_team_code}</div>
        <div className="subject-sub">{edge.notes||''}</div>
        {edge.reason_short && <div className="reason-short">{edge.reason_short}</div>}
      </div>
      <div className={`cell-pick lean-${isPos?'OVER':'UNDER'}`} style={{fontFamily:'var(--display)',fontWeight:700}}>
        {edge.lean}
        {mlOdds&&<span className="pick-odds">{mlOdds}</span>}
      </div>
      <div className="cell-num">{fmtOdds(edge.line)}</div>
      <div className="cell-num cell-proj">{modelPct}</div>
      <div className="cell-num">{impliedPct}</div>
      <div className="cell-num" style={{fontWeight:700,color:isPos?'var(--moss)':'var(--vermillion)'}}>
        {isPos?'+':''}{edgePp}pp
      </div>
      <div>
        <span className="tier-pill">T{tier}</span>
        <ReasonToggle open={open} onClick={()=>setOpen(!open)} hasFactors={hasFactors} />
      </div>
    </div>
    {open && hasFactors && <ReasonDetail factors={edge.reason_factors} />}
    </>
  );
}"""

if old_mlrow in app:
    app = app.replace(old_mlrow, new_mlrow, 1)
    print("OK: MLRow patched with reasoning")
elif "MLRow" in app and "edge.reason_factors" in app and app.count("reason_factors") >= 3:
    print("OK: MLRow already patched")
else:
    print("WARN: MLRow original pattern not found — manual edit required")

# Write JSX back
app_path.write_text(app, encoding="utf-8")
print("OK: App.jsx written")

# ============================================================================
# 5. Append CSS for reasoning UI
# ============================================================================

reason_css = """

/* ============================================================================
   Edge reasoning — short inline summary + expandable factor table
   ============================================================================ */
.reason-short {
  font-size: 11px;
  color: var(--ink-3, #6b6b6b);
  margin-top: 4px;
  line-height: 1.35;
  font-style: italic;
}

.reason-toggle {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 18px;
  height: 18px;
  margin-left: 6px;
  padding: 0;
  border: 1px solid var(--ink-4, #d0d0d0);
  background: transparent;
  border-radius: 3px;
  font-size: 12px;
  font-weight: 600;
  line-height: 1;
  cursor: pointer;
  color: var(--ink-2, #444);
  vertical-align: middle;
}
.reason-toggle:hover {
  background: var(--bg-hover, #f5f5f5);
  border-color: var(--ink-3, #999);
}
.reason-toggle.open {
  background: var(--ink-2, #444);
  color: white;
  border-color: var(--ink-2, #444);
}

.reason-detail {
  padding: 10px 16px 14px;
  background: var(--bg-subtle, #fafafa);
  border-bottom: 1px solid var(--ink-5, #ececec);
  font-size: 12px;
}
.reason-factors {
  width: 100%;
  border-collapse: collapse;
}
.reason-factors thead th {
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--ink-3, #888);
  font-weight: 600;
  text-align: left;
  padding: 4px 8px 6px;
  border-bottom: 1px solid var(--ink-5, #ececec);
}
.reason-factors thead th.impact { text-align: right; }
.reason-factors tbody td {
  padding: 6px 8px;
  border-bottom: 1px solid var(--ink-6, #f3f3f3);
}
.reason-factors tbody tr:last-child td { border-bottom: none; }
.factor-label {
  font-weight: 600;
  color: var(--ink-1, #222);
  width: 160px;
  white-space: nowrap;
}
.factor-value {
  color: var(--ink-2, #555);
}
.factor-impact {
  text-align: right;
  font-variant-numeric: tabular-nums;
  font-weight: 600;
  white-space: nowrap;
  width: 110px;
}
.impact-pos { color: var(--moss, #2e7d32); }
.impact-neg { color: var(--vermillion, #c62828); }
.impact-neutral { color: var(--ink-3, #888); }

/* Make conviction cell flex so the toggle sits next to the value */
.cell-conviction { display: flex; align-items: center; gap: 6px; }
"""

if "/* Edge reasoning" not in css:
    css_path.write_text(css.rstrip() + "\n" + reason_css, encoding="utf-8")
    print("OK: reasoning styles appended to styles.css")
else:
    print("OK: reasoning styles already present in styles.css")

print()
print("Frontend patched. Run a quick build to verify:")
print("  cd frontend && npm run build && cd ..")
print()
print("Or in dev mode:")
print("  cd frontend && npm run dev")
print()
print("Then commit + push as usual.")
