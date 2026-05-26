"""
Run from repo root: python patch_pitcher_contact_splits.py

Adds pitcher contact-quality metrics + L/R + Home/Away splits to the system.

Prerequisites:
  1. Copy migration 0008_pitcher_contact_splits.sql to backend/migrations/
  2. Apply migration locally (python -m scripts.bootstrap or however you usually do)

This patch does:
  A. Backend
     - Extends _refresh_pitcher_budget to compute BABIP from existing season stats
     - Adds new _refresh_pitcher_contact() — pulls EV/hard-hit/barrel/GB% from Savant
     - Adds new _refresh_pitcher_splits() — pulls vsL/vsR/home/away from MLB API
     - Wires both into the Statcast refresh job chain
     - Extends /api/stats/pitchers endpoint to return all new fields + splits
  B. Frontend
     - Adds new columns to Stats > Pitchers table for the inline metrics
     - Adds a small "Splits" expand button per row to show L/R, home/away
"""
from pathlib import Path

statcast_path = Path("backend/src/statcast_refresh.py")
api_path      = Path("backend/src/api.py")
app_path      = Path("frontend/src/App.jsx")
css_path      = Path("frontend/src/styles.css")
statcast      = statcast_path.read_text(encoding="utf-8")
api           = api_path.read_text(encoding="utf-8")
app           = app_path.read_text(encoding="utf-8")
css           = css_path.read_text(encoding="utf-8")

# ============================================================================
# 1. statcast_refresh.py — append two new refresh functions + wire into chain
# ============================================================================
if "_refresh_pitcher_contact" in statcast:
    print("OK: pitcher contact refresh already present")
else:
    new_funcs = '''

# =============================================================================
# Pitcher contact-quality from Baseball Savant — added 2026
# Pulls: avg EV allowed, hard-hit%, barrel%, GB%, LD%, launch angle, BABIP-proxy
# =============================================================================

SAVANT_EXIT_VELO_URL = (
    "https://baseballsavant.mlb.com/leaderboard/statcast"
    "?type=pitcher&year={year}&position=&team=&min=10&csv=true"
)


def _refresh_pitcher_contact(season_year: int) -> int:
    """Pull contact-quality metrics from Savant's pitcher Statcast leaderboard.

    Columns we capture:
      - avg_exit_velo
      - hard_hit_pct      (Savant: hard_hit_percent / 100)
      - barrel_pct        (Savant: barrel_batted_rate / 100)
      - launch_angle_avg
      - gb_pct, ld_pct    (from batted ball distribution if available)
    """
    import csv, io
    url = SAVANT_EXIT_VELO_URL.format(year=season_year)
    try:
        r = requests.get(url, headers=SAVANT_HEADERS, timeout=30)
        r.raise_for_status()
        text = r.text
    except Exception as e:
        log.warning("Savant pitcher Statcast fetch failed: %s", e)
        return 0

    reader = csv.DictReader(io.StringIO(text))
    update_rows = []
    for row in reader:
        try:
            mlb_id = int(row.get("player_id") or row.get("mlb_id") or 0)
        except (TypeError, ValueError):
            continue
        if not mlb_id:
            continue

        def _pct(field):
            v = row.get(field)
            try:
                f = float(v)
                # Savant returns percentages either as 35.4 or 0.354 — normalize
                return round(f / 100.0, 4) if f > 1 else round(f, 4)
            except (TypeError, ValueError):
                return None

        def _num(field):
            v = row.get(field)
            try:
                return float(v) if v not in (None, "", "null") else None
            except (TypeError, ValueError):
                return None

        update_rows.append({
            "mlb_id":           mlb_id,
            "season_year":      season_year,
            "avg_exit_velo":    _num("exit_velocity_avg"),
            "hard_hit_pct":     _pct("hard_hit_percent"),
            "barrel_pct":       _pct("barrel_batted_rate"),
            "launch_angle_avg": _num("launch_angle_avg"),
            "gb_pct":           _pct("groundballs_percent") or _pct("gb_percent"),
            "ld_pct":           _pct("linedrives_percent") or _pct("ld_percent"),
        })

    if not update_rows:
        log.info("No Savant pitcher contact rows parsed")
        return 0

    sql = """
        UPDATE pitcher_xstats SET
          avg_exit_velo=%(avg_exit_velo)s,
          hard_hit_pct=%(hard_hit_pct)s,
          barrel_pct=%(barrel_pct)s,
          launch_angle_avg=%(launch_angle_avg)s,
          gb_pct=COALESCE(%(gb_pct)s, gb_pct),
          ld_pct=COALESCE(%(ld_pct)s, ld_pct),
          refreshed_at=now()
        WHERE mlb_id=%(mlb_id)s AND season_year=%(season_year)s;
    """
    n = db.execute_many(sql, update_rows)
    log.info("Updated %d pitcher contact-quality rows", n)
    return n


# =============================================================================
# Pitcher L/R + Home/Away splits — pulled from MLB Stats API
# =============================================================================

def _refresh_pitcher_splits(season_year: int) -> int:
    """Fetch pitching splits (vs LHB, vs RHB, home, away) for every pitcher
    in pitcher_xstats. Writes rows to pitcher_pitching_splits.
    """
    rows = db.fetchall(
        "SELECT mlb_id FROM pitcher_xstats WHERE season_year=%s", (season_year,)
    )
    if not rows:
        return 0
    log.info("Fetching pitching splits for %d pitchers", len(rows))
    insert_rows = []
    success = 0
    for i, row in enumerate(rows):
        mlb_id = row["mlb_id"]
        url = f"{MLB_API_BASE}/people/{mlb_id}/stats"
        params = {
            "stats": "statSplits",
            "group": "pitching",
            "season": season_year,
            "sitCodes": "vl,vr,h,a",   # vs L, vs R, home, away
        }
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.debug("Pitcher %d splits fetch failed: %s", mlb_id, e)
            continue

        splits_blocks = (data.get("stats") or [{}])[0].get("splits", [])
        if not splits_blocks:
            continue

        key_map = {"vl": "vsL", "vr": "vsR", "h": "home", "a": "away"}
        for sb in splits_blocks:
            sit_code = (sb.get("split") or {}).get("code")
            key = key_map.get(sit_code)
            if not key:
                continue
            stat = sb.get("stat") or {}

            def _num(field):
                v = stat.get(field)
                if v in (None, "", "null", "-.--"):
                    return None
                try:
                    return float(v)
                except (TypeError, ValueError):
                    return None

            pa = _coerce_int(stat.get("battersFaced"))
            so = _coerce_int(stat.get("strikeOuts"))
            bb = _coerce_int(stat.get("baseOnBalls"))
            ip = _parse_ip(stat.get("inningsPitched"))

            insert_rows.append({
                "mlb_id":       mlb_id,
                "season_year":  season_year,
                "split_key":    key,
                "pa":           pa,
                "ip":           ip,
                "era":          _num("era"),
                "whip":         _num("whip"),
                "avg_against":  _num("avg"),
                "obp_against":  _num("obp"),
                "slg_against":  _num("slg"),
                "ops_against":  _num("ops"),
                "k_pct":        round(so / pa, 4) if (pa and so is not None) else None,
                "bb_pct":       round(bb / pa, 4) if (pa and bb is not None) else None,
            })
        success += 1
        if (i + 1) % 50 == 0:
            log.info("  ... %d/%d processed (%d with splits data)", i+1, len(rows), success)

    if not insert_rows:
        return 0

    sql = """
        INSERT INTO pitcher_pitching_splits
          (mlb_id, season_year, split_key, pa, ip, era, whip,
           avg_against, obp_against, slg_against, ops_against,
           k_pct, bb_pct, refreshed_at)
        VALUES
          (%(mlb_id)s, %(season_year)s, %(split_key)s, %(pa)s, %(ip)s, %(era)s, %(whip)s,
           %(avg_against)s, %(obp_against)s, %(slg_against)s, %(ops_against)s,
           %(k_pct)s, %(bb_pct)s, now())
        ON CONFLICT (mlb_id, season_year, split_key) DO UPDATE SET
          pa=EXCLUDED.pa, ip=EXCLUDED.ip, era=EXCLUDED.era, whip=EXCLUDED.whip,
          avg_against=EXCLUDED.avg_against, obp_against=EXCLUDED.obp_against,
          slg_against=EXCLUDED.slg_against, ops_against=EXCLUDED.ops_against,
          k_pct=EXCLUDED.k_pct, bb_pct=EXCLUDED.bb_pct,
          refreshed_at=now();
    """
    n = db.execute_many(sql, insert_rows)
    log.info("Upserted %d pitching-split rows", n)
    return n


# =============================================================================
# BABIP — computed from season stats already pulled in _refresh_pitcher_budget
# =============================================================================
def _refresh_pitcher_babip(season_year: int) -> int:
    """Compute BABIP from existing season stats. Cheap pass over the DB."""
    rows = db.fetchall("""
        SELECT mlb_id FROM pitcher_xstats WHERE season_year=%s
    """, (season_year,))
    if not rows:
        return 0
    update_rows = []
    for r in rows:
        mlb_id = r["mlb_id"]
        stats = _fetch_pitcher_season_stats(mlb_id, season_year)
        if not stats:
            continue
        h  = _coerce_int(stats.get("hits"))     or 0
        hr = _coerce_int(stats.get("homeRuns")) or 0
        ab = _coerce_int(stats.get("atBats"))   or 0
        k  = _coerce_int(stats.get("strikeOuts")) or 0
        sf = _coerce_int(stats.get("sacFlies")) or 0
        # BABIP = (H - HR) / (AB - K - HR + SF)
        denom = ab - k - hr + sf
        babip = round((h - hr) / denom, 4) if denom > 0 else None
        if babip is None:
            continue
        update_rows.append({"mlb_id": mlb_id, "season_year": season_year, "babip": babip})
    if not update_rows:
        return 0
    sql = """
        UPDATE pitcher_xstats SET babip=%(babip)s, refreshed_at=now()
        WHERE mlb_id=%(mlb_id)s AND season_year=%(season_year)s;
    """
    n = db.execute_many(sql, update_rows)
    log.info("Updated %d pitcher BABIP rows", n)
    return n

'''
    # Insert before the "Whiff rate" block (so it's grouped with other Savant pulls)
    anchor = "# =============================================================================\n# Whiff rate + contact rate from Baseball Savant"
    if anchor in statcast:
        statcast = statcast.replace(anchor, new_funcs + "\n" + anchor, 1)
        print("OK: _refresh_pitcher_contact + _refresh_pitcher_splits + _refresh_pitcher_babip added")
    else:
        # Fallback: append at end
        statcast = statcast.rstrip() + new_funcs + "\n"
        print("OK: new refresh functions appended (anchor not found, used fallback)")

# Wire into the refresh chain
old_chain = '''            ("pitcher fb_pct+xfip",        _refresh_pitcher_fb_pct,      "n_pitcher_fb_pct"),
            ("hitter budget+L15",          _refresh_hitter_budget,        "n_hitter_budget"),'''
new_chain = '''            ("pitcher fb_pct+xfip",        _refresh_pitcher_fb_pct,      "n_pitcher_fb_pct"),
            ("pitcher contact (Savant)",   _refresh_pitcher_contact,     "n_pitcher_contact"),
            ("pitcher BABIP",              _refresh_pitcher_babip,        "n_pitcher_babip"),
            ("pitcher splits (L/R/H/A)",   _refresh_pitcher_splits,      "n_pitcher_splits"),
            ("hitter budget+L15",          _refresh_hitter_budget,        "n_hitter_budget"),'''

if old_chain in statcast:
    statcast = statcast.replace(old_chain, new_chain, 1)
    print("OK: three new refresh steps wired into Statcast job chain")
elif "_refresh_pitcher_contact" in statcast and "_refresh_pitcher_splits" in statcast and 'pitcher contact (Savant)' in statcast:
    print("OK: refresh chain already wired")
else:
    print("WARN: refresh chain pattern not found — wire manually")

statcast_path.write_text(statcast, encoding="utf-8")

# ============================================================================
# 2. api.py — extend /api/stats/pitchers to return new columns + splits subobject
# ============================================================================
old_pitcher_sql = '''@app.get("/api/stats/pitchers")
def stats_pitchers():
    """All pitchers with Statcast data for the current season."""
    from . import db
    from datetime import date as _date
    season = _date.today().year
    rows = db.fetchall("""
        SELECT mlb_id, last_first, season_year,
               pa, bip, ba, est_ba, slg, est_slg, woba, est_woba,
               era, xera, xfip, k_pct, bb9, fb_pct, hr_fb_rate,
               days_rest, last_start_date::text AS last_start_date,
               refreshed_at::text AS refreshed_at
        FROM pitcher_xstats
        WHERE season_year = %s
        ORDER BY pa DESC NULLS LAST, est_woba ASC NULLS LAST
    """, (season,))
    return {"season": season, "n": len(rows), "pitchers": [dict(r) for r in rows]}'''

new_pitcher_sql = '''@app.get("/api/stats/pitchers")
def stats_pitchers():
    """All pitchers with Statcast data + contact metrics + splits."""
    from . import db
    from datetime import date as _date
    season = _date.today().year

    rows = db.fetchall("""
        SELECT mlb_id, last_first, season_year,
               pa, bip, ba, est_ba, slg, est_slg, woba, est_woba,
               era, xera, xfip, k_pct, bb9, fb_pct, hr_fb_rate,
               babip, gb_pct, ld_pct,
               avg_exit_velo, hard_hit_pct, barrel_pct, launch_angle_avg,
               days_rest, last_start_date::text AS last_start_date,
               refreshed_at::text AS refreshed_at
        FROM pitcher_xstats
        WHERE season_year = %s
        ORDER BY pa DESC NULLS LAST, est_woba ASC NULLS LAST
    """, (season,))

    # Tack on splits keyed by mlb_id -> {vsL: {...}, vsR: {...}, home: {...}, away: {...}}
    split_rows = db.fetchall("""
        SELECT mlb_id, split_key, pa, ip, era, whip,
               avg_against, obp_against, slg_against, ops_against, k_pct, bb_pct
        FROM pitcher_pitching_splits
        WHERE season_year = %s
    """, (season,))
    splits_by_id = {}
    for sr in split_rows:
        d = splits_by_id.setdefault(sr["mlb_id"], {})
        key = sr["split_key"]
        d[key] = {k: v for k, v in dict(sr).items() if k not in ("mlb_id", "split_key")}

    out = []
    for r in rows:
        d = dict(r)
        d["splits"] = splits_by_id.get(r["mlb_id"], {})
        out.append(d)

    return {"season": season, "n": len(out), "pitchers": out}'''

if old_pitcher_sql in api:
    api = api.replace(old_pitcher_sql, new_pitcher_sql, 1)
    print("OK: /api/stats/pitchers extended with contact + splits")
elif '"splits":' in api and "barrel_pct" in api:
    print("OK: pitcher stats endpoint already extended")
else:
    print("WARN: stats_pitchers endpoint pattern not found")

api_path.write_text(api, encoding="utf-8")

# ============================================================================
# 3. Frontend — wider Pitchers table with new inline columns + splits toggle
# ============================================================================
old_pitcher_table = '''function PitcherStatsTable({ rows }) {
  const columns = [
    { key:'last_first', label:'Pitcher',  align:'left',  type:'string', width:'minmax(160px, 2fr)' },
    { key:'pa',         label:'PA',       align:'num',   type:'number', dp:0, width:'70px' },
    { key:'bip',        label:'BIP',      align:'num',   type:'number', dp:0, width:'70px' },
    { key:'era',        label:'ERA',      align:'num',   type:'number', dp:2, width:'70px' },
    { key:'xera',       label:'xERA',     align:'num',   type:'number', dp:2, width:'70px' },
    { key:'xfip',       label:'xFIP',     align:'num',   type:'number', dp:2, width:'70px' },
    { key:'est_woba',   label:'xwOBA',    align:'num',   type:'number', fmt:fmt3, width:'80px' },
    { key:'k_pct',      label:'K%',       align:'num',   type:'number', fmt:v=>v==null?'\u2014':(Number(v)*100).toFixed(1)+'%', width:'80px' },
    { key:'bb9',        label:'BB/9',     align:'num',   type:'number', dp:2, width:'80px' },
    { key:'fb_pct',     label:'FB%',      align:'num',   type:'number', fmt:v=>v==null?'\u2014':(Number(v)*100).toFixed(1)+'%', width:'80px' },
    { key:'hr_fb_rate', label:'HR/FB',    align:'num',   type:'number', fmt:v=>v==null?'\u2014':(Number(v)*100).toFixed(1)+'%', width:'80px' },
    { key:'days_rest',  label:'Rest',     align:'num',   type:'number', dp:0, width:'70px' },
  ];
  return <StatsTable rows={rows} columns={columns} defaultSort="pa" defaultDir="desc" />;
}'''

new_pitcher_table = '''function PitcherStatsTable({ rows }) {
  const fmtPct = v => v==null ? '\u2014' : (Number(v)*100).toFixed(1)+'%';
  const columns = [
    { key:'last_first',      label:'Pitcher',  align:'left',  type:'string', width:'minmax(160px, 1.8fr)' },
    { key:'pa',              label:'PA',       align:'num',   type:'number', dp:0,  width:'60px' },
    { key:'era',             label:'ERA',      align:'num',   type:'number', dp:2,  width:'65px' },
    { key:'xera',            label:'xERA',     align:'num',   type:'number', dp:2,  width:'65px' },
    { key:'xfip',            label:'xFIP',     align:'num',   type:'number', dp:2,  width:'65px' },
    { key:'est_woba',        label:'xwOBA',    align:'num',   type:'number', fmt:fmt3, width:'70px' },
    { key:'babip',           label:'BABIP',    align:'num',   type:'number', fmt:fmt3, width:'70px' },
    { key:'k_pct',           label:'K%',       align:'num',   type:'number', fmt:fmtPct, width:'70px' },
    { key:'bb9',             label:'BB/9',     align:'num',   type:'number', dp:2,  width:'70px' },
    { key:'gb_pct',          label:'GB%',      align:'num',   type:'number', fmt:fmtPct, width:'70px' },
    { key:'fb_pct',          label:'FB%',      align:'num',   type:'number', fmt:fmtPct, width:'70px' },
    { key:'avg_exit_velo',   label:'EV',       align:'num',   type:'number', dp:1,  width:'70px' },
    { key:'hard_hit_pct',    label:'HardHit%', align:'num',   type:'number', fmt:fmtPct, width:'90px' },
    { key:'barrel_pct',      label:'Barrel%',  align:'num',   type:'number', fmt:fmtPct, width:'80px' },
    { key:'launch_angle_avg',label:'LA',       align:'num',   type:'number', dp:1,  width:'60px' },
    { key:'__splits',        label:'Splits',   align:'num',   type:'string', width:'80px',
      fmt: (_, row) => <PitcherSplitsToggle row={row}/> },
  ];
  return <StatsTable rows={rows} columns={columns} defaultSort="pa" defaultDir="desc" />;
}

function PitcherSplitsToggle({ row }) {
  const [open, setOpen] = useState(false);
  const splits = row.splits || {};
  const hasSplits = ['vsL','vsR','home','away'].some(k => splits[k]);
  if (!hasSplits) return <span className="splits-na">\u2014</span>;
  return (
    <>
      <button className={'splits-toggle' + (open ? ' open' : '')} onClick={()=>setOpen(!open)}>
        {open ? 'Hide' : 'Splits'}
      </button>
      {open && <PitcherSplitsPanel splits={splits}/>}
    </>
  );
}

function PitcherSplitsPanel({ splits }) {
  const keys = [
    { k:'vsL',  label:'vs LHB' },
    { k:'vsR',  label:'vs RHB' },
    { k:'home', label:'Home'   },
    { k:'away', label:'Away'   },
  ];
  return (
    <div className="splits-panel">
      <table className="splits-table">
        <thead>
          <tr>
            <th>Split</th><th className="num">BF</th><th className="num">IP</th>
            <th className="num">ERA</th><th className="num">WHIP</th>
            <th className="num">AVG</th><th className="num">OPS</th>
            <th className="num">K%</th><th className="num">BB%</th>
          </tr>
        </thead>
        <tbody>
          {keys.map(({k, label}) => {
            const s = splits[k];
            if (!s) return null;
            return (
              <tr key={k}>
                <td>{label}</td>
                <td className="num">{s.pa ?? '\u2014'}</td>
                <td className="num">{s.ip != null ? Number(s.ip).toFixed(1) : '\u2014'}</td>
                <td className="num">{s.era != null ? Number(s.era).toFixed(2) : '\u2014'}</td>
                <td className="num">{s.whip != null ? Number(s.whip).toFixed(2) : '\u2014'}</td>
                <td className="num">{s.avg_against != null ? Number(s.avg_against).toFixed(3) : '\u2014'}</td>
                <td className="num">{s.ops_against != null ? Number(s.ops_against).toFixed(3) : '\u2014'}</td>
                <td className="num">{s.k_pct != null ? (Number(s.k_pct)*100).toFixed(1)+'%' : '\u2014'}</td>
                <td className="num">{s.bb_pct != null ? (Number(s.bb_pct)*100).toFixed(1)+'%' : '\u2014'}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}'''

if old_pitcher_table in app:
    app = app.replace(old_pitcher_table, new_pitcher_table, 1)
    print("OK: PitcherStatsTable extended with contact metrics + splits toggle")
elif "PitcherSplitsToggle" in app:
    print("OK: PitcherStatsTable already extended")
else:
    print("WARN: PitcherStatsTable pattern not found")

# Also extend StatsTable to pass the row into fmt() so the splits column can render
old_fmt_call = '''                  const display = val == null ? '\u2014'
                    : col.fmt ? col.fmt(val)
                    : col.type === 'number' ? Number(val).toFixed(col.dp ?? 2)
                    : val;'''
new_fmt_call = '''                  const display = (col.fmt
                    ? col.fmt(val, r)
                    : (val == null ? '\u2014'
                      : col.type === 'number' ? Number(val).toFixed(col.dp ?? 2)
                      : val));'''
if old_fmt_call in app:
    app = app.replace(old_fmt_call, new_fmt_call, 1)
    print("OK: StatsTable cell formatter now passes row to fmt()")
elif "col.fmt(val, r)" in app:
    print("OK: StatsTable formatter already updated")
else:
    print("WARN: StatsTable formatter pattern not found")

app_path.write_text(app, encoding="utf-8")

# ============================================================================
# 4. CSS for splits panel
# ============================================================================
if "/* Pitcher splits panel */" in css:
    print("OK: splits CSS already present")
else:
    splits_css = '''

/* ============================================================================
   Pitcher splits panel — inline expandable under any pitcher row
   ============================================================================ */
.splits-toggle {
  font-family: var(--mono, monospace);
  font-size: 10px;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  font-weight: 700;
  padding: 4px 10px;
  background: transparent;
  color: var(--ink-2, #555);
  border: 1px solid var(--rule, #c8c8c8);
  cursor: pointer;
}
.splits-toggle:hover {
  background: var(--paper-2, #f5f0e0);
  color: var(--ink, #111);
}
.splits-toggle.open {
  background: var(--ink, #111);
  color: var(--paper, #fff);
  border-color: var(--ink, #111);
}
.splits-na {
  font-family: var(--mono, monospace);
  font-size: 11px;
  color: var(--ink-3, #999);
}

.splits-panel {
  grid-column: 1 / -1;
  background: var(--paper-2, #faf6ec);
  border-top: 1px solid var(--rule, #d8d0bf);
  border-bottom: 1px solid var(--rule, #d8d0bf);
  padding: 10px 16px;
  margin-top: 4px;
}
.splits-table {
  width: 100%;
  border-collapse: collapse;
  font-family: var(--mono, monospace);
  font-size: 11px;
}
.splits-table thead th {
  text-align: left;
  padding: 4px 8px;
  font-size: 9px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--ink-3, #888);
  font-weight: 700;
  border-bottom: 1px solid var(--rule, #d8d0bf);
}
.splits-table thead th.num { text-align: right; }
.splits-table tbody td {
  padding: 6px 8px;
  color: var(--ink, #111);
  border-bottom: 1px solid var(--rule, #efe8d7);
}
.splits-table tbody td.num {
  text-align: right;
  font-variant-numeric: tabular-nums;
}
.splits-table tbody tr:last-child td { border-bottom: none; }
.splits-table tbody td:first-child {
  font-family: var(--text, system-ui);
  font-weight: 600;
  color: var(--ink-1, #222);
}
'''
    css = css.rstrip() + "\n" + splits_css
    css_path.write_text(css, encoding="utf-8")
    print("OK: splits CSS appended")

print()
print("Migration:")
print("  Copy 0008_pitcher_contact_splits.sql to backend/migrations/")
print("  Apply locally if you keep a local DB:")
print("    cd backend && python -m scripts.bootstrap && cd ..")
print()
print("Verify and push:")
print('  python -X utf8 -c "import ast; ast.parse(open(\'backend/src/statcast_refresh.py\').read()); ast.parse(open(\'backend/src/api.py\').read()); print(\'OK\')"')
print("  cd frontend && npm run build && cd ..")
print("  git add backend/migrations/0008_pitcher_contact_splits.sql")
print("  git add backend/src/statcast_refresh.py backend/src/api.py")
print("  git add frontend/src/App.jsx frontend/src/styles.css")
print("  git add patch_pitcher_contact_splits.py 0008_pitcher_contact_splits.sql")
print("  git commit -m 'Pitcher contact metrics + L/R + Home/Away splits'")
print("  git push")
print()
print("On Railway (after deploy):")
print("  1. Apply migration on production via SQL console:")
print("       (paste the contents of 0008_pitcher_contact_splits.sql)")
print("  2. Trigger statcast refresh once to populate the new columns:")
print("       https://<railway-url>/api/admin/trigger/statcast/<token>")
print("     (Or use the new admin panel button.) Will take ~15 minutes due to")
print("     the per-pitcher splits fetch loop.")
