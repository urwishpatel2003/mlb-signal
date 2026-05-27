"""
Run from repo root: python patch_bet_button_wiring.py

Adds a "Bet" column to all four flagged-edge tables:
  - EdgeRow (Full Game O/U + Pitcher Props)
  - F5Row
  - MLRow

Each row gets a <BetButton edge={edge}/> in a new last cell. Headers get a
matching empty <span> so the grid columns line up. Grid templates are bumped
to add an 80px column for the button.
"""
from pathlib import Path

app_path = Path("frontend/src/App.jsx")
css_path = Path("frontend/src/styles.css")
app = app_path.read_text(encoding="utf-8")
css = css_path.read_text(encoding="utf-8")

# ============================================================================
# 1. EdgeRow header + row (Full Game + Props use same component)
# ============================================================================
old_edges_head = '''          <span>{kind==='game'?'Matchup':'Pitcher'}</span>
          <span>Market</span><span className="num">Line</span>
          <span>Pick</span><span className="num">Projection</span><span>Conviction</span>'''

new_edges_head = '''          <span>{kind==='game'?'Matchup':'Pitcher'}</span>
          <span>Market</span><span className="num">Line</span>
          <span>Pick</span><span className="num">Projection</span><span>Conviction</span>
          <span className="th-bet">Bet</span>'''

if old_edges_head in app:
    app = app.replace(old_edges_head, new_edges_head, 1)
    print("OK: EdgesView header gets Bet column")
elif "th-bet" in app:
    print("OK: EdgesView header already updated")

old_edge_row_end = '''      <div className="cell-conviction">
        {isLowTrust
          ? <span className="conv-na" title="Tier 3 or fallback">n/a <span className="tier-pill">T{tier}</span></span>
          : <div className="conv-cell">
              <span className="conv-value">{Number(conv).toFixed(0)}%</span>
              <div className={convBarClass}><div className="conv-bar-fill" style={{width:`${Math.min(100,Math.max(0,conv))}%`}}/></div>
            </div>}
      </div>
    </div>
  );
}'''

new_edge_row_end = '''      <div className="cell-conviction">
        {isLowTrust
          ? <span className="conv-na" title="Tier 3 or fallback">n/a <span className="tier-pill">T{tier}</span></span>
          : <div className="conv-cell">
              <span className="conv-value">{Number(conv).toFixed(0)}%</span>
              <div className={convBarClass}><div className="conv-bar-fill" style={{width:`${Math.min(100,Math.max(0,conv))}%`}}/></div>
            </div>}
      </div>
      <div className="cell-bet"><BetButton edge={edge}/></div>
    </div>
  );
}'''

if old_edge_row_end in app:
    app = app.replace(old_edge_row_end, new_edge_row_end, 1)
    print("OK: EdgeRow gets <BetButton/>")
elif "cell-bet" in app:
    print("OK: EdgeRow already wired")

# ============================================================================
# 2. F5Row header + row
# ============================================================================
old_f5_head = '''          <span>Matchup</span><span className="num">F5 Line</span>
          <span>Pick</span><span className="num">F5 Proj</span>
          <span className="num">Edge</span><span>Conviction</span>'''

new_f5_head = '''          <span>Matchup</span><span className="num">F5 Line</span>
          <span>Pick</span><span className="num">F5 Proj</span>
          <span className="num">Edge</span><span>Conviction</span>
          <span className="th-bet">Bet</span>'''

if old_f5_head in app:
    app = app.replace(old_f5_head, new_f5_head, 1)
    print("OK: F5 header gets Bet column")
elif "th-bet" in app and "F5 Proj" in app:
    print("OK: F5 header already updated")

old_f5_row_end = '''      <div className="cell-conviction">
        {conv!=null
          ? <span className="conv-value">{Number(conv).toFixed(0)}% <span className="tier-pill">T{tier}</span></span>
          : <span className="conv-na">n/a</span>}
      </div>
    </div>
  );
}'''

new_f5_row_end = '''      <div className="cell-conviction">
        {conv!=null
          ? <span className="conv-value">{Number(conv).toFixed(0)}% <span className="tier-pill">T{tier}</span></span>
          : <span className="conv-na">n/a</span>}
      </div>
      <div className="cell-bet"><BetButton edge={edge}/></div>
    </div>
  );
}'''

if old_f5_row_end in app:
    app = app.replace(old_f5_row_end, new_f5_row_end, 1)
    print("OK: F5Row gets <BetButton/>")

# ============================================================================
# 3. MLRow header + row
# ============================================================================
old_ml_head = '''          <span>Matchup</span><span>Pick</span><span className="num">Odds</span>
          <span className="num">Model Win%</span><span className="num">Implied</span>
          <span className="num">Edge</span><span>Tier</span>'''

new_ml_head = '''          <span>Matchup</span><span>Pick</span><span className="num">Odds</span>
          <span className="num">Model Win%</span><span className="num">Implied</span>
          <span className="num">Edge</span><span>Tier</span>
          <span className="th-bet">Bet</span>'''

if old_ml_head in app:
    app = app.replace(old_ml_head, new_ml_head, 1)
    print("OK: ML header gets Bet column")
elif "th-bet" in app and "Model Win%" in app:
    print("OK: ML header already updated")

old_ml_row_end = '''      <div><span className="tier-pill">T{tier}</span></div>
    </div>
  );
}'''

new_ml_row_end = '''      <div><span className="tier-pill">T{tier}</span></div>
      <div className="cell-bet"><BetButton edge={edge}/></div>
    </div>
  );
}'''

if old_ml_row_end in app:
    app = app.replace(old_ml_row_end, new_ml_row_end, 1)
    print("OK: MLRow gets <BetButton/>")

app_path.write_text(app, encoding="utf-8")

# ============================================================================
# 4. CSS — append grid-template-columns overrides + bet cell styling
# ============================================================================
if "/* Bet column wiring */" in css:
    print("OK: bet column CSS already present")
else:
    add_css = '''

/* ============================================================================
   Bet column wiring — adds 80px column to existing edge tables
   ============================================================================ */
/* Full game + props (EdgeRow): add a 7th column */
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

/* F5 (F5Row): add a 7th column */
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

/* ML: add an 8th column */
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

/* Mobile: Bet button below conviction on small screens */
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

@media (max-width: 640px) {
  .edge-row {
    grid-template-columns: 1fr auto auto;
    grid-template-areas:
      "subject conv bet"
      "market  pick bet"
      "line    proj bet";
  }
  .cell-bet { grid-area: bet; align-self: center; }

  .edge-row.f5-row {
    grid-template-areas:
      "subject conv bet"
      "line    pick bet"
      "proj    edge bet";
  }

  .ml-row {
    grid-template-areas:
      "subject pick bet"
      "odds    model bet"
      "implied edge tier";
  }
}
'''
    css = css.rstrip() + "\n" + add_css
    css_path.write_text(css, encoding="utf-8")
    print("OK: bet column CSS appended")

print()
print("Build and push:")
print("  cd frontend && npm run build && cd ..")
print("  git add frontend/src/App.jsx frontend/src/styles.css patch_bet_button_wiring.py")
print("  git commit -m 'Wire BetButton into all edge tables (totals, F5, ML, props)'")
print("  git push")
print()
print("After deploy, each flagged edge row gets a '+ Bet' button on the right.")
print("Click -> modal pops up. Enter $ + juice + book + notes -> Save.")
print("Saved bets show a green checkmark with amount. Click to edit/remove.")
print("Personal P&L tracked in the 'My Record' tab.")
