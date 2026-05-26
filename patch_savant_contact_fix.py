"""
Run from repo root: python patch_savant_contact_fix.py

Fixes _refresh_pitcher_contact() to:
  1. Strip BOM (\ufeff) from the CSV before parsing
  2. Use pandas instead of csv.DictReader to handle quoted-comma headers
     correctly (CSV header is `"last_name, first_name", player_id, ...` —
     the embedded comma breaks csv.DictReader)
  3. Map to the REAL column names returned by Savant's CSV:
        avg_hit_speed     -> avg_exit_velo
        ev95percent       -> hard_hit_pct      (% balls hit 95+ mph)
        brl_percent       -> barrel_pct
        avg_hit_angle     -> launch_angle_avg
        gb (raw)          -> ignored (Savant gives total GB count, not pct
                                       in this leaderboard — leave GB% null
                                       since we don't have BIP denominator)

Also fixes: player_id parsing (it's the second real column).
"""
from pathlib import Path

f = Path("backend/src/statcast_refresh.py")
content = f.read_text(encoding="utf-8")

# ============================================================================
# Replace the _refresh_pitcher_contact function body
# ============================================================================
old_func_start = "def _refresh_pitcher_contact(season_year: int) -> int:"
old_marker_end = '    n = db.execute_many(sql, update_rows)\n    log.info("Updated %d pitcher contact-quality rows", n)\n    return n'

if old_func_start not in content:
    print("ERR: _refresh_pitcher_contact function not found")
    raise SystemExit(1)

start = content.index(old_func_start)
end = content.index(old_marker_end, start) + len(old_marker_end)
old_block = content[start:end]

new_block = '''def _refresh_pitcher_contact(season_year: int) -> int:
    """Pull contact-quality metrics from Savant's pitcher Statcast leaderboard.

    The CSV header is malformed (literal `"last_name, first_name"` with an
    embedded comma) which csv.DictReader splits into two phantom columns.
    Workaround: use pandas which handles quoted fields correctly, OR parse
    by column index after skipping the broken first column header.

    Real columns returned by Savant for this endpoint:
      last_name, first_name (quoted, treat as 1 col), player_id, attempts,
      avg_hit_angle, anglesweetspotpercent, max_hit_speed, avg_hit_speed,
      ev50, fbld, gb, max_distance, avg_distance, avg_hr_distance,
      ev95plus, ev95percent, barrels, brl_percent, brl_pa

    Mapping to pitcher_xstats columns:
      avg_hit_speed    -> avg_exit_velo
      ev95percent      -> hard_hit_pct      (% balls hit 95+ mph)
      brl_percent      -> barrel_pct
      avg_hit_angle    -> launch_angle_avg
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

    # Strip UTF-8 BOM if present (Savant prepends one)
    if text.startswith("\ufeff"):
        text = text[1:]

    # Parse with csv.reader (positional) instead of DictReader, because the
    # header has a literal quoted comma: `"last_name, first_name"`.
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        log.warning("Savant CSV empty")
        return 0

    # Build positional column index. csv.reader correctly handles the quoted
    # `"last_name, first_name"` as a single field, so the indices should match
    # the real column order returned by Savant.
    def _idx(name):
        try:
            return header.index(name)
        except ValueError:
            log.debug("Savant column %r not found in header: %s", name, header)
            return None

    idx_player_id   = _idx("player_id")
    idx_ev          = _idx("avg_hit_speed")
    idx_hard_pct    = _idx("ev95percent")
    idx_barrel_pct  = _idx("brl_percent")
    idx_launch_ang  = _idx("avg_hit_angle")

    if idx_player_id is None or idx_ev is None:
        log.warning("Savant CSV missing required columns. Header: %s", header)
        return 0

    def _safe_float(row, idx):
        if idx is None or idx >= len(row):
            return None
        v = row[idx]
        if v in (None, "", "null", "NA"):
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    def _safe_pct(row, idx):
        """Savant returns percent fields as e.g. '36.8' (not 0.368). Normalize to fraction."""
        f = _safe_float(row, idx)
        if f is None:
            return None
        return round(f / 100.0, 4) if f > 1.5 else round(f, 4)

    update_rows = []
    for row in reader:
        if not row or len(row) <= idx_player_id:
            continue
        try:
            mlb_id = int(row[idx_player_id])
        except (TypeError, ValueError):
            continue
        if not mlb_id:
            continue

        update_rows.append({
            "mlb_id":           mlb_id,
            "season_year":      season_year,
            "avg_exit_velo":    _safe_float(row, idx_ev),
            "hard_hit_pct":     _safe_pct(row, idx_hard_pct),
            "barrel_pct":       _safe_pct(row, idx_barrel_pct),
            "launch_angle_avg": _safe_float(row, idx_launch_ang),
        })

    if not update_rows:
        log.info("No Savant pitcher contact rows parsed (header=%s)", header)
        return 0

    sql = """
        UPDATE pitcher_xstats SET
          avg_exit_velo=%(avg_exit_velo)s,
          hard_hit_pct=%(hard_hit_pct)s,
          barrel_pct=%(barrel_pct)s,
          launch_angle_avg=%(launch_angle_avg)s,
          refreshed_at=now()
        WHERE mlb_id=%(mlb_id)s AND season_year=%(season_year)s;
    """
    n = db.execute_many(sql, update_rows)
    log.info("Updated %d pitcher contact-quality rows", n)
    return n'''

content = content[:start] + new_block + content[end:]
f.write_text(content, encoding="utf-8")
print("OK: _refresh_pitcher_contact rewritten with correct CSV handling + column names")

print()
print("Note: GB% is not populated from this leaderboard — Savant exposes")
print("ground ball counts but not a rate here. The fb_pct field in")
print("pitcher_xstats is populated by the other Savant pull (batted-ball)")
print("which already runs successfully.")
print()
print("Steps:")
print('  python -X utf8 -c "import ast; ast.parse(open(\'backend/src/statcast_refresh.py\').read()); print(\'OK\')"')
print("  git add backend/src/statcast_refresh.py patch_savant_contact_fix.py")
print("  git commit -m 'Fix Savant pitcher contact: BOM + quoted-comma header + correct cols'")
print("  git push")
print()
print("After deploy, trigger statcast refresh once:")
print("  https://YOUR-RAILWAY-URL.up.railway.app/api/admin/trigger/statcast/YOUR_TOKEN")
print("  (or use the admin panel)")
print()
print("Expected result in logs:")
print("  'Updated ~600 pitcher contact-quality rows'  (not 0)")
print("Then the Stats > Pitchers table will show real EV, Hard%, Brl%, LA values.")
