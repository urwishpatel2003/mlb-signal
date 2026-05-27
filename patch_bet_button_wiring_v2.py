"""
Run from repo root: python patch_bet_button_wiring_v2.py

Wires <BetButton edge={edge}/> into EdgeRow, F5Row, MLRow as a new last
cell in each row. Matches the current code structure (which includes the
ReasonToggle in cell-conviction and the <></> fragment wrapper in EdgeRow).

Also adds the matching <th>Bet</th> to each table's header.
"""
from pathlib import Path

app_path = Path("frontend/src/App.jsx")
css_path = Path("frontend/src/styles.css")
app = app_path.read_text(encoding="utf-8")
css = css_path.read_text(encoding="utf-8")

# ============================================================================
# 1. EdgesView header: add <span>Bet</span>
# ============================================================================
old_edges_head = """          <span>{kind==='game'?'Matchup':'Pitcher'}</span>
          <span>Market</span><span className="num">Line</span>
          <span>Pick</span><span className="num">Projection</span><span>Conviction</span>"""

new_edges_head = """          <span>{kind==='game'?'Matchup':'Pitcher'}</span>
          <span>Market</span><span className="num">Line</span>
          <span>Pick</span><span className="num">Projection</span><span>Conviction</span>
          <span className="th-bet">Bet</span>"""

if old_edges_head in app:
    app = app.replace(old_edges_head, new_edges_head, 1)
    print("OK: EdgesView header gets Bet column")
elif "th-bet" in app:
    print("OK: EdgesView header already has Bet column")
else:
    print("WARN: EdgesView header pattern not found")

# ============================================================================
# 2. EdgeRow: add <div className="cell-bet"> before closing </div> tag of edge-row
# ============================================================================
# In your code the structure is:
#     ...
#     <ReasonToggle .../>
#       </div>      <-- closes cell-conviction
#     </div>        <-- closes edge-row    <- insert before this
#     {open && hasFactors && <ReasonDetail .../>}
#     </>
#   );
# }

old_edge_row_close = """        <ReasonToggle open={open} onClick={()=>setOpen(!open)} hasFactors={hasFactors} />
      </div>
    </div>
    {open && hasFactors && <ReasonDetail factors={edge.reason_factors} />}
    </>
  );
}"""

new_edge_row_close = """        <ReasonToggle open={open} onClick={()=>setOpen(!open)} hasFactors={hasFactors} />
      </div>
      <div className="cell-bet"><BetButton edge={edge}/></div>
    </div>
    {open && hasFactors && <ReasonDetail factors={edge.reason_factors} />}
    </>
  );
}"""

if old_edge_row_close in app:
    app = app.replace(old_edge_row_close, new_edge_row_close, 1)
    print("OK: EdgeRow gets <BetButton/>")
elif 'className="cell-bet"' in app and "EdgeRow" in app:
    print("OK: EdgeRow may already have BetButton (cell-bet found)")
else:
    print("WARN: EdgeRow close pattern not found")

# ============================================================================
# 3. F5 header: add <span>Bet</span>
# ============================================================================
old_f5_head = """          <span>Matchup</span><span className="num">F5 Line</span>
          <span>Pick</span><span className="num">F5 Proj</span>
          <span className="num">Edge</span><span>Conviction</span>"""

new_f5_head = """          <span>Matchup</span><span className="num">F5 Line</span>
          <span>Pick</span><span className="num">F5 Proj</span>
          <span className="num">Edge</span><span>Conviction</span>
          <span className="th-bet">Bet</span>"""

if old_f5_head in app:
    app = app.replace(old_f5_head, new_f5_head, 1)
    print("OK: F5 header gets Bet column")
elif "F5 Proj" in app and "th-bet" in app:
    print("OK: F5 header already has Bet column")
else:
    print("WARN: F5 header pattern not found")

# ============================================================================
# 4. F5Row: add cell-bet before close
# ============================================================================
old_f5_row_close = """      <div className="cell-conviction">
        {conv!=null
          ? <span className="conv-value">{Number(conv).toFixed(0)}% <span className="tier-pill">T{tier}</span></span>
          : <span className="conv-na">n/a</span>}
      </div>
    </div>
  );
}"""

new_f5_row_close = """      <div className="cell-conviction">
        {conv!=null
          ? <span className="conv-value">{Number(conv).toFixed(0)}% <span className="tier-pill">T{tier}</span></span>
          : <span className="conv-na">n/a</span>}
      </div>
      <div className="cell-bet"><BetButton edge={edge}/></div>
    </div>
  );
}"""

if old_f5_row_close in app:
    app = app.replace(old_f5_row_close, new_f5_row_close, 1)
    print("OK: F5Row gets <BetButton/>")
elif 'className="cell-bet"' in app:
    print("OK: F5Row may already have BetButton")
else:
    print("WARN: F5Row close pattern not found")

# ============================================================================
# 5. ML header: add <span>Bet</span>
# ============================================================================
old_ml_head = """          <span>Matchup</span><span>Pick</span><span className="num">Odds</span>
          <span className="num">Model Win%</span><span className="num">Implied</span>
          <span className="num">Edge</span><span>Tier</span>"""

new_ml_head = """          <span>Matchup</span><span>Pick</span><span className="num">Odds</span>
          <span className="num">Model Win%</span><span className="num">Implied</span>
          <span className="num">Edge</span><span>Tier</span>
          <span className="th-bet">Bet</span>"""

if old_ml_head in app:
    app = app.replace(old_ml_head, new_ml_head, 1)
    print("OK: ML header gets Bet column")
elif "Model Win%" in app and "th-bet" in app:
    print("OK: ML header already has Bet column")
else:
    print("WARN: ML header pattern not found")

# ============================================================================
# 6. MLRow: add cell-bet before close
# ============================================================================
old_ml_row_close = """      <div><span className="tier-pill">T{tier}</span></div>
    </div>
  );
}"""

new_ml_row_close = """      <div><span className="tier-pill">T{tier}</span></div>
      <div className="cell-bet"><BetButton edge={edge}/></div>
    </div>
  );
}"""

if old_ml_row_close in app:
    app = app.replace(old_ml_row_close, new_ml_row_close, 1)
    print("OK: MLRow gets <BetButton/>")
elif 'className="cell-bet"' in app:
    print("OK: MLRow may already have BetButton")
else:
    print("WARN: MLRow close pattern not found")

app_path.write_text(app, encoding="utf-8")

# ============================================================================
# 7. CSS — grid templates with 7th/8th column for Bet
# ============================================================================
if "/* Bet column wiring */" in css:
    print("OK: bet column CSS already present")
else:
    add_css = """

/* ============================================================================
   Bet column wiring — adds Bet column to existing edge tables
   ============================================================================ */
.edges-thead,
.edge-row {
  grid-template-columns:
    minmax(160px, 1.6fr)
    minmax(110px, 1fr)
    70px
    90px
    90px
    minmax(120px, 1fr)
    90px;
}

.edges-thead.f5-thead,
.edge-row.f5-row {
  grid-template-columns:
    minmax(160px, 1.6fr)
    80px
    90px
    90px
    80px
    minmax(120px, 1fr)
    90px;
}

.ml-thead,
.ml-row {
  grid-template-columns:
    minmax(160px, 1.6fr)
    minmax(100px, 1fr)
    80px
    100px
    80px
    80px
    60px
    90px;
}

.th-bet {
  font-family: var(--mono);
  font-size: 10px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--ink-2);
  font-weight: 700;
  text-align: center;
}
.cell-bet {
  text-align: center;
  display: flex;
  align-items: center;
  justify-content: center;
}

@media (max-width: 900px) {
  .edges-thead,
  .edge-row {
    grid-template-columns:
      minmax(140px, 1.4fr)
      minmax(100px, 1fr)
      60px
      70px
      80px
      100px
      80px;
  }
}
"""
    css = css.rstrip() + "\n" + add_css
    css_path.write_text(css, encoding="utf-8")
    print("OK: bet column CSS appended")

print()
print("Verify and push:")
print("  cd frontend && npm run build && cd ..")
print("  git add frontend/src/App.jsx frontend/src/styles.css patch_bet_button_wiring_v2.py")
print("  git commit -m 'Wire BetButton into edge tables (v2 matching current code)'")
print("  git push")
print()
print("After deploy, each flagged edge row has '+ Bet' button on the right.")
print("Click -> modal opens for $ amount + juice + book + notes.")
print("Saved bets show green checkmark. Personal P&L tracked in 'My Record' tab.")
