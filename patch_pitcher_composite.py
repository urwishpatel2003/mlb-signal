"""
Run from repo root: python patch_pitcher_composite.py

Adds an aggregate pitcher quality score:
  - Weighted composite of normalized stats (0-100 scale)
  - Shown as a numeric badge next to the pitcher name
  - Tints the name cell background green/yellow/red
  - Only computed when xERA + xwOBA-against + barrel% are all present
    (otherwise name shows normally, no score)

Weights (run-prevention quality):
  xwOBA-against 25, xERA 20, xFIP 15, Barrel% 12, K% 10,
  HardHit% 8, BABIP 5, BB/9 5

Mechanism:
  1. StatsTable gains rowColorFn support (colors a cell from the whole row)
  2. pitcherComposite(row) -> { score, tier } normalizes + weights each stat
  3. Pitcher name column uses fmt to render name + score badge, and
     rowColorFn to tint the cell
  4. CSS overrides the sticky-strips-color rule for the aggregate tint
"""
from pathlib import Path

app_path = Path("frontend/src/App.jsx")
css_path = Path("frontend/src/styles.css")
app = app_path.read_text(encoding="utf-8")
css = css_path.read_text(encoding="utf-8")

# ============================================================================
# 1. StatsTable — support col.rowColorFn(row) in addition to col.colorFn(val)
# ============================================================================
old_cell = '''                  const colorCls = (col.colorFn && val != null) ? ('stat-' + col.colorFn(Number(val))) : '';
                  return <div key={col.key} className={`stats-cell ${col.align||'left'} ${col.sticky?'sticky':''} ${colorCls}`}>{display}</div>;'''

new_cell = '''                  let colorCls = (col.colorFn && val != null) ? ('stat-' + col.colorFn(Number(val))) : '';
                  if (col.rowColorFn) {
                    const rc = col.rowColorFn(r);
                    if (rc) colorCls = 'stat-agg-' + rc;
                  }
                  return <div key={col.key} className={`stats-cell ${col.align||'left'} ${col.sticky?'sticky':''} ${colorCls}`}>{display}</div>;'''

if old_cell in app:
    app = app.replace(old_cell, new_cell, 1)
    print("OK: StatsTable supports rowColorFn")
elif "col.rowColorFn" in app:
    print("OK: rowColorFn already supported")
else:
    print("WARN: StatsTable cell render not found")

# ============================================================================
# 2. Composite scoring function (insert before PitcherStatsTable)
# ============================================================================
if "function pitcherComposite" not in app:
    composite = '''// ----------------------------------------------------------------------------
// Pitcher composite quality score (0-100). Higher = better pitcher.
// Each stat normalized linearly between an "elite" and "poor" anchor, clamped
// 0-100, then weighted. Requires xERA + xwOBA-against + barrel% to be present.
// ----------------------------------------------------------------------------
function pitcherComposite(r) {
  const need = [r.xera, r.est_woba, r.barrel_pct];
  if (need.some(v => v == null)) return null;   // not enough data

  // norm: map value to 0-100 where `elite` -> 100, `poor` -> 0.
  // lowerBetter=true means small values are elite.
  const norm = (v, elite, poor) => {
    if (v == null) return null;
    const t = (v - poor) / (elite - poor);   // works both directions
    return Math.max(0, Math.min(100, t * 100));
  };

  // Each entry: [value, eliteAnchor, poorAnchor, weight]
  const parts = [
    [r.est_woba,     0.270, 0.350, 25],   // xwOBA-against (low elite)
    [r.xera,         2.80,  5.20,  20],   // xERA (low elite)
    [r.xfip,         2.90,  5.20,  15],   // xFIP (low elite)
    [r.barrel_pct,   0.030, 0.110, 12],   // barrel% (low elite)
    [r.k_pct,        0.300, 0.150, 10],   // K% (HIGH elite -> elite>poor)
    [r.hard_hit_pct, 0.300, 0.450,  8],   // hardhit% (low elite)
    [r.babip,        0.260, 0.330,  5],   // babip (low elite)
    [r.bb9,          1.80,  4.20,   5],   // bb/9 (low elite)
  ];

  let wsum = 0, w = 0;
  for (const [val, elite, poor, weight] of parts) {
    const n = norm(val, elite, poor);
    if (n == null) continue;
    wsum += n * weight;
    w += weight;
  }
  if (w === 0) return null;
  const score = Math.round(wsum / w);
  const tier = score >= 65 ? 'good' : (score < 45 ? 'bad' : 'mid');
  return { score, tier };
}

'''
    anchor = "function PitcherStatsTable({ rows }) {"
    app = app.replace(anchor, composite + anchor, 1)
    print("OK: pitcherComposite function inserted")
else:
    print("OK: pitcherComposite already present")

# ============================================================================
# 3. Pitcher name column — add score badge (fmt) + tint (rowColorFn)
# ============================================================================
old_name_col = "    { key:'last_first',      label:'Pitcher',  align:'left',  type:'string', width:'minmax(150px, 1.6fr)', sticky:true },"
new_name_col = """    { key:'last_first',      label:'Pitcher',  align:'left',  type:'string', width:'minmax(170px, 1.8fr)', sticky:true,
      rowColorFn: (r) => { const c = pitcherComposite(r); return c ? c.tier : null; },
      fmt: (val, r) => {
        const c = pitcherComposite(r);
        const name = val == null ? '\\u2014' : (val.split(',')[0] || val);
        return (
          <span className="pitcher-name-cell">
            <span className="pitcher-name-text">{val}</span>
            {c && <span className={`composite-badge composite-${c.tier}`}>{c.score}</span>}
          </span>
        );
      } },"""

if old_name_col in app:
    app = app.replace(old_name_col, new_name_col, 1)
    print("OK: Pitcher name column gets score badge + tint")
elif "composite-badge" in app:
    print("OK: name column already updated")
else:
    print("WARN: Pitcher name column line not found — may have different whitespace")

app_path.write_text(app, encoding="utf-8")

# ============================================================================
# 4. CSS — aggregate tint (overrides sticky-strips-color) + badge
# ============================================================================
if "/* Pitcher composite */" not in css:
    comp_css = '''

/* Pitcher composite — aggregate tint on name cell + score badge */
/* These OVERRIDE the .stats-cell.sticky color-strip rule intentionally. */
.stats-cell.sticky.stat-agg-good { background: rgba(91, 142, 82, 0.20); }
.stats-cell.sticky.stat-agg-mid  { background: rgba(200, 160, 74, 0.16); }
.stats-cell.sticky.stat-agg-bad  { background: rgba(184, 72, 58, 0.16); }

.pitcher-name-cell {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  width: 100%;
}
.pitcher-name-text {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.composite-badge {
  flex-shrink: 0;
  min-width: 26px;
  text-align: center;
  font-family: var(--mono, monospace);
  font-size: 11px;
  font-weight: 700;
  padding: 2px 6px;
  border-radius: 3px;
  color: #fff;
}
.composite-good { background: #5b8e52; }
.composite-mid  { background: #c8a04a; }
.composite-bad  { background: #b8483a; }
'''
    css = css.rstrip() + "\n" + comp_css
    css_path.write_text(css, encoding="utf-8")
    print("OK: composite CSS appended")
else:
    print("OK: composite CSS already present")

print()
print("Build + push:")
print("  cd frontend && npm run build && cd ..")
print("  git add frontend/src/App.jsx frontend/src/styles.css patch_pitcher_composite.py")
print("  git commit -m 'Pitcher composite score: name badge + aggregate tint'")
print("  git push")
print()
print("After deploy, each pitcher with xERA + xwOBA + barrel% shows a 0-100")
print("score badge next to their name, and the name cell is tinted green/")
print("yellow/red. Pitchers missing those stats show name only (no badge/tint).")
print()
print("Tune anchors/weights in the pitcherComposite() function if the scores")
print("don't match your intuition once you see real data.")
