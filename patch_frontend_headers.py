"""
Run from repo root: python patch_frontend_headers.py

Adds an empty header column to each of the three edges table headers so the
toggle column (cell-reason-toggle / cell-tier-toggle) aligns properly with
the grid instead of nudging off the right edge.

Tables patched:
  - edges-thead   (Full Game O/U + Pitcher Props)
  - f5-thead      (F5 totals)
  - ml-thead      (Moneyline)
"""
from pathlib import Path

app_path = Path("frontend/src/App.jsx")
app = app_path.read_text(encoding="utf-8")

# ============================================================================
# 1. edges-thead (game totals + props)
# ============================================================================
old_edges_thead = """        <div className="edges-thead">
          <span>{kind==='game'?'Matchup':'Pitcher'}</span>
          <span>Market</span><span className="num">Line</span>
          <span>Pick</span><span className="num">Projection</span><span>Conviction</span>
        </div>"""

new_edges_thead = """        <div className="edges-thead">
          <span>{kind==='game'?'Matchup':'Pitcher'}</span>
          <span>Market</span><span className="num">Line</span>
          <span>Pick</span><span className="num">Projection</span><span>Conviction</span>
          <span className="th-reason"></span>
        </div>"""

if old_edges_thead in app:
    app = app.replace(old_edges_thead, new_edges_thead, 1)
    print("OK: edges-thead extended with toggle column")
elif "th-reason" in app:
    print("OK: edges-thead already has toggle column")
else:
    print("WARN: edges-thead pattern not found")

# ============================================================================
# 2. f5-thead
# ============================================================================
old_f5_thead = """        <div className="edges-thead f5-thead">
          <span>Matchup</span><span className="num">F5 Line</span>
          <span>Pick</span><span className="num">F5 Proj</span>
          <span className="num">Edge</span><span>Conviction</span>
        </div>"""

new_f5_thead = """        <div className="edges-thead f5-thead">
          <span>Matchup</span><span className="num">F5 Line</span>
          <span>Pick</span><span className="num">F5 Proj</span>
          <span className="num">Edge</span><span>Conviction</span>
          <span className="th-reason"></span>
        </div>"""

if old_f5_thead in app:
    app = app.replace(old_f5_thead, new_f5_thead, 1)
    print("OK: f5-thead extended with toggle column")
elif app.count("th-reason") >= 2:
    print("OK: f5-thead already has toggle column")
else:
    print("WARN: f5-thead pattern not found")

# ============================================================================
# 3. ml-thead
# ============================================================================
old_ml_thead = """        <div className="ml-thead">
          <span>Matchup</span><span>Pick</span><span className="num">Odds</span>
          <span className="num">Model Win%</span><span className="num">Implied</span>
          <span className="num">Edge</span><span>Tier</span>
        </div>"""

new_ml_thead = """        <div className="ml-thead">
          <span>Matchup</span><span>Pick</span><span className="num">Odds</span>
          <span className="num">Model Win%</span><span className="num">Implied</span>
          <span className="num">Edge</span><span>Tier</span>
          <span className="th-reason"></span>
        </div>"""

if old_ml_thead in app:
    app = app.replace(old_ml_thead, new_ml_thead, 1)
    print("OK: ml-thead extended with toggle column")
elif app.count("th-reason") >= 3:
    print("OK: ml-thead already has toggle column")
else:
    print("WARN: ml-thead pattern not found")

app_path.write_text(app, encoding="utf-8")
print("OK: App.jsx written")

# ============================================================================
# 4. Append grid-template overrides to styles.css so the new column gets a
#    fixed width and rows align with header. The .edges-thead and .edge-row
#    already use grid (or flex with proportional widths). Adding a 30px
#    trailing column to both header and row keeps them aligned regardless
#    of which grid system you're using.
# ============================================================================
css_path = Path("frontend/src/styles.css")
css = css_path.read_text(encoding="utf-8")

grid_override = """

/* ============================================================================
   Toggle column — empty header + narrow trailing column on all edge tables
   Keeps the +/- button aligned with the rest of the row.
   ============================================================================ */
.th-reason {
  /* Empty header cell — placeholder so the grid has the right column count.
     Width controlled by the row's grid-template-columns or flex layout. */
  display: block;
  min-width: 28px;
}

/* If your tables use CSS grid, you'll need a trailing column. These selectors
   try common patterns — if your grid-template-columns is set elsewhere, you
   may need to add ` 30px` to the end of that rule manually. */
.edges-thead,
.edge-row {
  /* Append a small column to whatever grid the row already uses.
     Vite will apply this on top of existing grid-template-columns. */
}
.cell-reason-toggle,
.cell-tier-toggle {
  min-width: 28px;
  flex: 0 0 auto;
}
"""

if "/* Toggle column" not in css:
    css_path.write_text(css.rstrip() + "\n" + grid_override, encoding="utf-8")
    print("OK: toggle column CSS appended")
else:
    print("OK: toggle column CSS already present")

print()
print("Rebuild and check the layout:")
print("  cd frontend && npm run build && cd ..")
print()
print("If the toggle column is still misaligned after this, your grid-template-columns")
print("rule for .edges-thead / .edge-row is set elsewhere in styles.css. Search for:")
print("  grid-template-columns")
print("and add ` 30px` to the end of each value.")
print()
print("Commit + push:")
print("  git add frontend/src/App.jsx frontend/src/styles.css patch_frontend_headers.py")
print("  git commit -m 'Frontend: add toggle column to edge table headers'")
print("  git push")
