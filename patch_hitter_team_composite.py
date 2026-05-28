"""
Run from repo root: python patch_hitter_team_composite.py

Adds composite quality scores, reusing the rowColorFn + badge machinery
from the pitcher composite patch.

HITTERS: single composite (higher = better hitter). Badge next to name +
         name-cell tint. Requires xwOBA + xSLG present.
  Weights: xwOBA 30, wOBA 15, xSLG 20, SLG 10, xBA 10, L15 wOBA 15

TEAMS: TWO badges side by side on the team name:
  - OFF score (offense): xwOBA + L5 wOBA
  - BP score (bullpen):  BP ERA + BP L7 ERA  (lower ERA = higher score)
  Team name cell tinted by the OFFENSE score (primary signal for totals).
"""
from pathlib import Path

app_path = Path("frontend/src/App.jsx")
css_path = Path("frontend/src/styles.css")
app = app_path.read_text(encoding="utf-8")
css = css_path.read_text(encoding="utf-8")

# ============================================================================
# 1. Composite functions (insert before HitterStatsTable)
# ============================================================================
if "function hitterComposite" not in app:
    composites = '''// ----------------------------------------------------------------------------
// Hitter composite (0-100, higher = better hitter). Requires xwOBA + xSLG.
// ----------------------------------------------------------------------------
function hitterComposite(r) {
  if (r.est_woba == null || r.est_slg == null) return null;
  const norm = (v, elite, poor) => {
    if (v == null) return null;
    const t = (v - poor) / (elite - poor);
    return Math.max(0, Math.min(100, t * 100));
  };
  const parts = [
    [r.est_woba, 0.400, 0.280, 30],   // xwOBA
    [r.woba,     0.400, 0.280, 15],   // wOBA
    [r.est_slg,  0.550, 0.350, 20],   // xSLG
    [r.slg,      0.550, 0.350, 10],   // SLG
    [r.est_ba,   0.300, 0.220, 10],   // xBA
    [r.l15_woba, 0.400, 0.280, 15],   // L15 wOBA
  ];
  let wsum = 0, w = 0;
  for (const [val, elite, poor, weight] of parts) {
    const n = norm(val, elite, poor);
    if (n == null) continue;
    wsum += n * weight; w += weight;
  }
  if (w === 0) return null;
  const score = Math.round(wsum / w);
  const tier = score >= 60 ? 'good' : (score < 42 ? 'bad' : 'mid');
  return { score, tier };
}

// ----------------------------------------------------------------------------
// Team offense composite (0-100, higher = better offense). Requires xwOBA.
// ----------------------------------------------------------------------------
function teamOffenseComposite(r) {
  if (r.est_woba == null) return null;
  const norm = (v, elite, poor) => {
    if (v == null) return null;
    const t = (v - poor) / (elite - poor);
    return Math.max(0, Math.min(100, t * 100));
  };
  const parts = [
    [r.est_woba, 0.345, 0.295, 65],   // team xwOBA
    [r.l5_woba,  0.345, 0.295, 35],   // L5 wOBA (recent form)
  ];
  let wsum = 0, w = 0;
  for (const [val, elite, poor, weight] of parts) {
    const n = norm(val, elite, poor);
    if (n == null) continue;
    wsum += n * weight; w += weight;
  }
  if (w === 0) return null;
  const score = Math.round(wsum / w);
  const tier = score >= 60 ? 'good' : (score < 42 ? 'bad' : 'mid');
  return { score, tier };
}

// ----------------------------------------------------------------------------
// Team bullpen composite (0-100, higher = better pen). Requires bullpen_era.
// Lower ERA -> higher score.
// ----------------------------------------------------------------------------
function teamBullpenComposite(r) {
  if (r.bullpen_era == null) return null;
  const norm = (v, elite, poor) => {
    if (v == null) return null;
    const t = (v - poor) / (elite - poor);
    return Math.max(0, Math.min(100, t * 100));
  };
  const parts = [
    [r.bullpen_era,    3.20, 5.00, 65],   // season BP ERA (low elite)
    [r.bullpen_era_l7, 3.20, 5.50, 35],   // L7 BP ERA (low elite, noisier)
  ];
  let wsum = 0, w = 0;
  for (const [val, elite, poor, weight] of parts) {
    const n = norm(val, elite, poor);
    if (n == null) continue;
    wsum += n * weight; w += weight;
  }
  if (w === 0) return null;
  const score = Math.round(wsum / w);
  const tier = score >= 60 ? 'good' : (score < 42 ? 'bad' : 'mid');
  return { score, tier };
}

'''
    anchor = "function HitterStatsTable({ rows }) {"
    app = app.replace(anchor, composites + anchor, 1)
    print("OK: hitter + team composite functions inserted")
else:
    print("OK: composite functions already present")

# ============================================================================
# 2. Hitter name column — badge + tint
# ============================================================================
old_hitter_name = "    { key:'last_first', label:'Hitter',   align:'left', type:'string', width:'minmax(160px, 2fr)' },"
new_hitter_name = """    { key:'last_first', label:'Hitter',   align:'left', type:'string', width:'minmax(180px, 2fr)', sticky:true,
      rowColorFn: (r) => { const c = hitterComposite(r); return c ? c.tier : null; },
      fmt: (val, r) => {
        const c = hitterComposite(r);
        return (
          <span className="pitcher-name-cell">
            <span className="pitcher-name-text">{val}</span>
            {c && <span className={`composite-badge composite-${c.tier}`}>{c.score}</span>}
          </span>
        );
      } },"""

if old_hitter_name in app:
    app = app.replace(old_hitter_name, new_hitter_name, 1)
    print("OK: Hitter name column gets composite badge + tint")
elif "hitterComposite(r)" in app and "Hitter" in app:
    print("OK: Hitter name column already updated")
else:
    print("WARN: Hitter name column line not found")

# ============================================================================
# 3. Team name column — two badges (OFF + BP), tint by offense
# ============================================================================
old_team_name = "    { key:'team_code',       label:'Team',         align:'left', type:'string', width:'minmax(80px, 1fr)' },"
new_team_name = """    { key:'team_code',       label:'Team',         align:'left', type:'string', width:'minmax(150px, 1.4fr)', sticky:true,
      rowColorFn: (r) => { const c = teamOffenseComposite(r); return c ? c.tier : null; },
      fmt: (val, r) => {
        const off = teamOffenseComposite(r);
        const bp  = teamBullpenComposite(r);
        return (
          <span className="pitcher-name-cell">
            <span className="pitcher-name-text">{val}</span>
            <span className="team-badges">
              {off && <span className={`composite-badge composite-${off.tier}`} title="Offense">{off.score}</span>}
              {bp  && <span className={`composite-badge bp-badge composite-${bp.tier}`} title="Bullpen">{bp.score}</span>}
            </span>
          </span>
        );
      } },"""

if old_team_name in app:
    app = app.replace(old_team_name, new_team_name, 1)
    print("OK: Team name column gets OFF + BP badges + offense tint")
elif "teamOffenseComposite(r)" in app:
    print("OK: Team name column already updated")
else:
    print("WARN: Team name column line not found")

app_path.write_text(app, encoding="utf-8")

# ============================================================================
# 4. CSS — team badges layout + bp badge distinction + legend hint
# ============================================================================
if "/* Team composite badges */" not in css:
    extra_css = '''

/* Team composite badges — OFF (offense) + BP (bullpen) side by side */
.team-badges {
  display: inline-flex;
  gap: 4px;
  flex-shrink: 0;
}
/* BP badge: outlined variant so it reads differently from the offense badge */
.composite-badge.bp-badge {
  background: transparent !important;
  border: 1.5px solid;
}
.composite-badge.bp-badge.composite-good { color: #5b8e52; border-color: #5b8e52; }
.composite-badge.bp-badge.composite-mid  { color: #c8a04a; border-color: #c8a04a; }
.composite-badge.bp-badge.composite-bad  { color: #b8483a; border-color: #b8483a; }
'''
    css = css.rstrip() + "\n" + extra_css
    css_path.write_text(css, encoding="utf-8")
    print("OK: team badges CSS appended")
else:
    print("OK: team badges CSS already present")

print()
print("Build + push:")
print("  cd frontend && npm run build && cd ..")
print("  git add frontend/src/App.jsx frontend/src/styles.css patch_hitter_team_composite.py")
print("  git commit -m 'Composite scores: hitters (1 badge) + teams (offense/bullpen badges)'")
print("  git push")
print()
print("After deploy:")
print("  Hitters tab  -> score badge next to name + name-cell tint")
print("  Teams tab    -> two badges (solid = offense, outlined = bullpen);")
print("                  name cell tinted by offense score")
print()
print("Badge legend: solid badge = offense (higher better),")
print("              outlined badge = bullpen (higher = better pen, i.e. low ERA)")
