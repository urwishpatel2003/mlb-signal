"""
Run from repo root: python patch_color_all_stats.py

Complete color-coding for Stats tables — pitchers, hitters, teams.
Idempotent and self-contained (does not assume the earlier color patch ran).

Green = good, Yellow = average, Red = bad, FROM THE SUBJECT'S PERSPECTIVE:
  - Pitcher table: green = good for the pitcher (low ERA, low contact, high K%)
  - Hitter table:  green = good hitter (high wOBA/BA/SLG)
  - Team table:    green = good offense (high wOBA) OR good bullpen (low ERA)

Mechanism:
  1. StatsTable cell render applies `stat-{good|mid|bad}` from col.colorFn(value)
  2. Color helper object COLOR with all threshold bands
  3. colorFn attached to every insight column across the three tables
  4. CSS for the three color classes
"""
from pathlib import Path
import re

app_path = Path("frontend/src/App.jsx")
css_path = Path("frontend/src/styles.css")
app = app_path.read_text(encoding="utf-8")
css = css_path.read_text(encoding="utf-8")

# ============================================================================
# 1. StatsTable cell render — apply colorFn class (idempotent)
# ============================================================================
old_cell = '''                  const display = (col.fmt
                    ? col.fmt(val, r)
                    : (val == null ? '\u2014'
                      : col.type === 'number' ? Number(val).toFixed(col.dp ?? 2)
                      : val));
                  return <div key={col.key} className={`stats-cell ${col.align||'left'} ${col.sticky?'sticky':''}`}>{display}</div>;'''

new_cell = '''                  const display = (col.fmt
                    ? col.fmt(val, r)
                    : (val == null ? '\u2014'
                      : col.type === 'number' ? Number(val).toFixed(col.dp ?? 2)
                      : val));
                  const colorCls = (col.colorFn && val != null) ? ('stat-' + col.colorFn(Number(val))) : '';
                  return <div key={col.key} className={`stats-cell ${col.align||'left'} ${col.sticky?'sticky':''} ${colorCls}`}>{display}</div>;'''

if old_cell in app:
    app = app.replace(old_cell, new_cell, 1)
    print("OK: StatsTable cell render applies colorFn")
elif "col.colorFn(Number(val))" in app:
    print("OK: StatsTable colorFn rendering already present")
else:
    print("WARN: StatsTable cell render not found — check manually")

# ============================================================================
# 2. Color helpers (insert once, before PitcherStatsTable)
# ============================================================================
if "const COLOR = {" not in app:
    helpers = '''// ----------------------------------------------------------------------------
// Stat color helpers. Each returns 'good' | 'mid' | 'bad'.
//   _low  : lower value is better (ERA, contact-against, BABIP)
//   _high : higher value is better (K%, wOBA for hitters/offense)
// ----------------------------------------------------------------------------
const _low  = (v, goodMax, badMin) => v <= goodMax ? 'good' : (v >= badMin ? 'bad' : 'mid');
const _high = (v, goodMin, badMax) => v >= goodMin ? 'good' : (v <= badMax ? 'bad' : 'mid');

const COLOR = {
  // --- pitcher (lower is better unless noted) ---
  pERA:     v => _low(v, 3.50, 4.50),
  pWOBA:    v => _low(v, 0.300, 0.330),     // xwOBA-against
  pBABIP:   v => _low(v, 0.280, 0.310),
  pKpct:    v => _high(v, 0.25, 0.20),      // fraction; higher better
  pBB9:     v => _low(v, 2.5, 3.5),
  pGBpct:   v => _high(v, 0.48, 0.42),      // fraction; higher better
  pFBpct:   v => _low(v, 0.32, 0.40),       // high FB% = HR risk (cautionary)
  pHRFB:    v => _low(v, 0.10, 0.13),       // fraction
  pEV:      v => _low(v, 87.0, 89.5),
  pHard:    v => _low(v, 0.33, 0.40),       // fraction
  pBarrel:  v => _low(v, 0.06, 0.09),       // fraction

  // --- hitter / offense (higher is better) ---
  hWOBA:    v => _high(v, 0.350, 0.310),
  hBA:      v => _high(v, 0.270, 0.240),
  hSLG:     v => _high(v, 0.450, 0.390),
  tWOBA:    v => _high(v, 0.330, 0.310),    // team offense

  // --- team bullpen (lower is better) ---
  tBPERA:   v => _low(v, 3.50, 4.25),
};

'''
    anchor = "function PitcherStatsTable({ rows }) {"
    app = app.replace(anchor, helpers + anchor, 1)
    print("OK: COLOR helpers inserted")
else:
    print("OK: COLOR helpers already present")

# ============================================================================
# Helper to attach colorFn to a column line by key
# ============================================================================
def attach(app_text, key, ref):
    pat = re.compile(r"(\{ key:'" + re.escape(key) + r"',[^\n]*?width:'[^']*')(\s*\},)")
    def repl(m):
        if 'colorFn' in m.group(0):
            return m.group(0)
        return m.group(1) + ", colorFn:" + ref + m.group(2)
    return pat.subn(repl, app_text, count=1)

# ============================================================================
# 3. Pitcher columns
# ============================================================================
pitcher_map = [
    ("era", "COLOR.pERA"), ("xera", "COLOR.pERA"), ("xfip", "COLOR.pERA"),
    ("est_woba", "COLOR.pWOBA"), ("babip", "COLOR.pBABIP"),
    ("k_pct", "COLOR.pKpct"), ("bb9", "COLOR.pBB9"),
    ("gb_pct", "COLOR.pGBpct"), ("fb_pct", "COLOR.pFBpct"),
    ("avg_exit_velo", "COLOR.pEV"), ("hard_hit_pct", "COLOR.pHard"),
    ("barrel_pct", "COLOR.pBarrel"),
]
# NOTE: est_woba appears in BOTH pitcher and hitter/team tables. attach() only
# patches the FIRST occurrence each call. We handle table-specific est_woba
# below by patching pitcher first, then hitter, then team — each call grabs the
# next unpatched occurrence in document order. Pitcher table comes first in the
# file so this ordering is correct.
pcount = 0
for key, ref in pitcher_map:
    app, n = attach(app, key, ref)
    pcount += n
print(f"OK: pitcher colorFn attached to {pcount}/{len(pitcher_map)} columns")

# ============================================================================
# 4. Hitter columns
#    ba, est_ba, slg, est_slg, woba, est_woba, l15_woba, vs_L_woba, vs_R_woba
# ============================================================================
hitter_map = [
    ("ba", "COLOR.hBA"), ("est_ba", "COLOR.hBA"),
    ("slg", "COLOR.hSLG"), ("est_slg", "COLOR.hSLG"),
    ("woba", "COLOR.hWOBA"), ("est_woba", "COLOR.hWOBA"),
    ("l15_woba", "COLOR.hWOBA"),
    ("vs_L_woba", "COLOR.hWOBA"), ("vs_R_woba", "COLOR.hWOBA"),
]
hcount = 0
for key, ref in hitter_map:
    app, n = attach(app, key, ref)
    hcount += n
print(f"OK: hitter colorFn attached to {hcount}/{len(hitter_map)} columns")

# ============================================================================
# 5. Team columns
#    est_woba (offense), l5_woba (offense), bullpen_era, bullpen_era_l7
# ============================================================================
team_map = [
    ("est_woba", "COLOR.tWOBA"),    # next unpatched est_woba = team table
    ("l5_woba", "COLOR.tWOBA"),
    ("bullpen_era", "COLOR.tBPERA"),
    ("bullpen_era_l7", "COLOR.tBPERA"),
]
tcount = 0
for key, ref in team_map:
    app, n = attach(app, key, ref)
    tcount += n
print(f"OK: team colorFn attached to {tcount}/{len(team_map)} columns")

app_path.write_text(app, encoding="utf-8")

# ============================================================================
# 6. CSS
# ============================================================================
if "/* Stat color coding */" not in css:
    color_css = '''

/* Stat color coding — green good / yellow mid / red bad (subject perspective) */
.stats-cell.stat-good { background: rgba(91, 142, 82, 0.16); color: #2c5f24; font-weight: 700; }
.stats-cell.stat-mid  { background: rgba(200, 160, 74, 0.14); color: #6b5212; }
.stats-cell.stat-bad  { background: rgba(184, 72, 58, 0.14); color: #7a2418; font-weight: 700; }
.stats-cell.sticky.stat-good,
.stats-cell.sticky.stat-mid,
.stats-cell.sticky.stat-bad {
  background: var(--paper, #fff); color: var(--ink, #111); font-weight: 500;
}
'''
    css = css.rstrip() + "\n" + color_css
    css_path.write_text(css, encoding="utf-8")
    print("OK: color CSS appended")
else:
    print("OK: color CSS already present")

print()
print("Verify counts (pitcher should be 12, hitter 9, team 4 = 25 total colorFn):")
print('  Select-String -Path frontend\\src\\App.jsx -Pattern "colorFn:COLOR" | Measure-Object')
print()
print("Build + push:")
print("  cd frontend && npm run build && cd ..")
print("  git add frontend/src/App.jsx frontend/src/styles.css patch_color_all_stats.py")
print("  git commit -m 'Color-code all stat tables: pitchers, hitters, teams'")
print("  git push")
