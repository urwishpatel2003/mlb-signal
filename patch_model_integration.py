"""
Run from repo root: python patch_model_integration.py

Integrates three new pitcher inputs into the projection model:

  1. Barrel% + HardHit% adjust true_era (contact-quality)
        contact_adj = (barrel - 0.06) * 12 + (hard_hit - 0.35) * 4
  2. Home/Away ERA split blended when PA >= 60 (60% split / 40% adjusted true_era)
  3. L/R splits replace pitcher xwoba_against (handedness-weighted blend by lineup
     composition; requires PA >= 80 in both splits)
  4. HR multiplier dialled back: 0.4 -> 0.15 (small park-HR nudge, barrel% now
     carries most of that signal)

Prereqs:
  - migration 0012_pitcher_projection_adjustments.sql applied (run ALTER TABLE
    in Railway SQL console after deploy)

Touches: projections.py, orchestrator.py, api.py, db.py
"""
from pathlib import Path

proj_path = Path("backend/src/projections.py")
orch_path = Path("backend/src/orchestrator.py")
api_path  = Path("backend/src/api.py")
db_path   = Path("backend/src/db.py")

proj = proj_path.read_text(encoding="utf-8")
orch = orch_path.read_text(encoding="utf-8")
api  = api_path.read_text(encoding="utf-8")
db   = db_path.read_text(encoding="utf-8")

# ============================================================================
# 1. PitcherProjection dataclass — add new fields with defaults
# ============================================================================
old_dataclass = '''    wx_factor:          float
    pf_factor:          float
    high_variance_flag: bool
    days_rest:          Optional[int]   # NEW: days since last start'''

new_dataclass = '''    wx_factor:          float
    pf_factor:          float
    high_variance_flag: bool
    days_rest:          Optional[int]
    # Adjustment-tracking fields (defaults preserve backward compatibility)
    true_era_adj:       Optional[float] = None    # post-adjustment true_era
    barrel_pct:         Optional[float] = None
    hard_hit_pct:       Optional[float] = None
    contact_adj_runs:   Optional[float] = None    # contribution from barrel+hardhit, ER9 units
    split_blend_note:   Optional[str]   = None
    lr_blend_note:      Optional[str]   = None'''

if old_dataclass in proj:
    proj = proj.replace(old_dataclass, new_dataclass, 1)
    print("OK: PitcherProjection dataclass extended")
elif "true_era_adj:" in proj:
    print("OK: dataclass already extended")
else:
    print("WARN: dataclass anchor not found")

# ============================================================================
# 2. project_pitcher signature — add is_home and pitcher_splits kwargs
# ============================================================================
old_sig = '''def project_pitcher(
    *,
    pitcher_xstats: Optional[dict],
    pitcher_mlb_id: int,
    pitcher_name: str,
    pitcher_hand: str,
    team_code: str,
    opp_team_code: str,
    opp_lineup: list,
    hitter_xstats: dict,
    team_xwoba_fallback: float,
    park: dict,
    weather: dict,
    low_sample_pa_threshold: int = 30,
) -> PitcherProjection:'''

new_sig = '''def project_pitcher(
    *,
    pitcher_xstats: Optional[dict],
    pitcher_mlb_id: int,
    pitcher_name: str,
    pitcher_hand: str,
    team_code: str,
    opp_team_code: str,
    opp_lineup: list,
    hitter_xstats: dict,
    team_xwoba_fallback: float,
    park: dict,
    weather: dict,
    low_sample_pa_threshold: int = 30,
    is_home: bool = False,
    pitcher_splits: Optional[dict] = None,
) -> PitcherProjection:'''

if old_sig in proj:
    proj = proj.replace(old_sig, new_sig, 1)
    print("OK: project_pitcher signature extended")
elif "is_home: bool = False" in proj:
    print("OK: project_pitcher signature already extended")
else:
    print("WARN: project_pitcher signature not found")

# ============================================================================
# 3. After the er9 line, add the new adjustments
#    We're replacing the existing er9 block (line ~530) with the extended one.
# ============================================================================
old_er9_block = '''    # FIX 2026-05-26: removed structural 0.65/0.35 split that undercounted ER9
    # by ~1.5 runs per pitcher. Main term now produces full true_era; HR
    # context adds a small +/- adjustment when park HR factor is non-neutral.
    er9 = (true_era + woba_delta * 7) * wx_run * pf_runs
    # HR-park adjustment: small +/- nudge for HR-friendly / HR-suppressed parks
    # _hr9 baseline (~0.53 for avg pitcher) deviations get a 0.4x effect
    _hr9_baseline = LEAGUE_FB_PCT * LEAGUE_HR_FB * (38.0 / 3.0)   # ~0.53
    er9 += (_hr9 - _hr9_baseline) * 0.4'''

new_er9_block = '''    # ========================================================================
    # MODEL INTEGRATION 2026-05-28: barrel/hardhit contact adjustment +
    #                                home/away split blend +
    #                                L/R handedness-weighted xwoba +
    #                                HR multiplier dial-back (0.4 -> 0.15)
    # ========================================================================
    # --- (a) Contact-quality adjustment to true_era from barrel + hardhit ---
    _barrel = (pitcher_xstats or {}).get("barrel_pct")
    _hardhit = (pitcher_xstats or {}).get("hard_hit_pct")
    contact_adj = 0.0
    if _barrel is not None:
        contact_adj += (float(_barrel) - 0.06) * 12.0
    if _hardhit is not None:
        contact_adj += (float(_hardhit) - 0.35) * 4.0
    true_era_adj = true_era + contact_adj

    # --- (b) Home/Away split blend (60% split / 40% adjusted overall) ---
    _split_note = None
    splits = pitcher_splits or {}
    split_key = "home" if is_home else "away"
    _sp = splits.get(split_key) or {}
    _sp_era = _sp.get("era")
    _sp_pa  = _sp.get("pa") or 0
    if _sp_era is not None and _sp_pa >= 60:
        true_era_adj = 0.60 * float(_sp_era) + 0.40 * true_era_adj
        _split_note = f"{split_key} split applied (PA={_sp_pa}, ERA={round(float(_sp_era),2)})"
    elif _sp_era is not None:
        _split_note = f"{split_key} split skipped (PA={_sp_pa} < 60)"
    else:
        _split_note = "no split data"

    # Cap adjustments
    true_era_adj = max(1.50, min(8.50, true_era_adj))

    # --- (c) L/R handedness-weighted pitcher xwoba_against ---
    # If both vsL and vsR splits have PA >= 80, use a handedness-weighted blend.
    # OPS->xwOBA estimate: xwoba ~= (ops - 0.230) * 0.45 + 0.250
    _vL = splits.get("vsL") or {}
    _vR = splits.get("vsR") or {}
    _lr_note = None
    _ops_to_xwoba = lambda ops: max(0.220, min(0.450, (float(ops) - 0.230) * 0.45 + 0.250))
    if (_vL.get("pa") or 0) >= 80 and (_vR.get("pa") or 0) >= 80 \\
            and _vL.get("ops_against") is not None and _vR.get("ops_against") is not None:
        vsL_xwoba = _ops_to_xwoba(_vL["ops_against"])
        vsR_xwoba = _ops_to_xwoba(_vR["ops_against"])
        # Lineup composition: count L vs R bats
        n_lefty  = sum(1 for s in opp_lineup if (s.bat_side or "").upper() in ("L", "S"))
        n_righty = sum(1 for s in opp_lineup if (s.bat_side or "").upper() == "R")
        n_total  = n_lefty + n_righty
        if n_total > 0:
            pct_L = n_lefty / n_total
            pct_R = n_righty / n_total
            new_xwoba_against = pct_L * vsL_xwoba + pct_R * vsR_xwoba
            _lr_note = f"vsL {round(vsL_xwoba,3)} / vsR {round(vsR_xwoba,3)} blend ({int(pct_L*100)}% L)"
        else:
            new_xwoba_against = 0.5 * vsL_xwoba + 0.5 * vsR_xwoba
            _lr_note = f"vsL {round(vsL_xwoba,3)} / vsR {round(vsR_xwoba,3)} (no lineup, 50/50)"
        # Rebuild effective_opp_xwoba with handedness-aware pitcher term
        effective_opp_xwoba = 0.60 * new_xwoba_against + 0.40 * opp_xwoba
        woba_delta = effective_opp_xwoba - LEAGUE_XWOBA
    else:
        _lr_note = "L/R splits unavailable or PA<80"

    # --- (d) ER9 with adjusted true_era + dialled-back HR adjustment ---
    er9 = (true_era_adj + woba_delta * 7) * wx_run * pf_runs
    _hr9_baseline = LEAGUE_FB_PCT * LEAGUE_HR_FB * (38.0 / 3.0)   # ~0.53
    # Dial-back: 0.4 -> 0.15 (barrel% now carries most HR signal via contact_adj)
    er9 += (_hr9 - _hr9_baseline) * 0.15'''

if old_er9_block in proj:
    proj = proj.replace(old_er9_block, new_er9_block, 1)
    print("OK: er9 block extended with contact + splits + L/R")
elif "MODEL INTEGRATION 2026-05-28" in proj:
    print("OK: er9 block already extended")
else:
    print("WARN: er9 block anchor not found")

# ============================================================================
# 4. Return PitcherProjection — add new fields
# ============================================================================
old_return = '''        wx_factor=round(wx_run,3), pf_factor=round(pf_runs,3),
        high_variance_flag=high_variance,
        days_rest=int(days_rest) if days_rest is not None else None,
    )'''

new_return = '''        wx_factor=round(wx_run,3), pf_factor=round(pf_runs,3),
        high_variance_flag=high_variance,
        days_rest=int(days_rest) if days_rest is not None else None,
        true_era_adj=round(true_era_adj, 2),
        barrel_pct=round(float(_barrel), 4) if _barrel is not None else None,
        hard_hit_pct=round(float(_hardhit), 4) if _hardhit is not None else None,
        contact_adj_runs=round(contact_adj, 3),
        split_blend_note=_split_note,
        lr_blend_note=_lr_note,
    )'''

if old_return in proj:
    proj = proj.replace(old_return, new_return, 1)
    print("OK: PitcherProjection return extended")
elif "true_era_adj=round(true_era_adj" in proj:
    print("OK: return already extended")
else:
    print("WARN: return block anchor not found")

# Also handle the fallback return earlier (when fallback=True and we never enter
# the er9 block). In fallback, true_era=LEAGUE_ER9 and no adjustments apply.
# The new fields will be passed but with None/defaults; we need to make sure
# the fallback path defines _barrel/_hardhit/contact_adj/notes so the return
# below works. Easier: initialize defaults BEFORE the fallback/non-fallback fork.
# Actually the cleanest fix: define defaults right after the function signature,
# so both branches have them. But the er9 block is in the non-fallback path.
# Inspecting: the return is at function-level, AFTER the fork merges (weather
# + park section onwards), so the er9 block always runs. Fallback path sets
# true_era=LEAGUE_ER9 first; the er9 block then computes contact_adj from
# pitcher_xstats which is None in fallback => _barrel and _hardhit are None
# from the .get() calls => contact_adj=0.0. true_era_adj=true_era=LEAGUE_ER9.
# All defaults work in fallback too. No extra changes needed.

proj_path.write_text(proj, encoding="utf-8")

# ============================================================================
# 5. db.py — extend insert_pitcher_projection SQL
# ============================================================================
old_db_sql = '''    sql = """INSERT INTO pitcher_projections (
          run_id,game_pk,mlb_id,last_first,team_code,opp_team_code,
          hand,source,pa_sample,era,xera,xfip,true_era,xwoba_against,
          opp_lineup_xwoba,used_actual_lineup,used_l15_blend,
          ip,outs,hits,er,bb,k,wx_factor,pf_factor,high_variance_flag
        ) VALUES (
          %(run_id)s,%(game_pk)s,%(mlb_id)s,%(last_first)s,%(team_code)s,%(opp_team_code)s,
          %(hand)s,%(source)s,%(pa_sample)s,%(era)s,%(xera)s,%(xfip)s,%(true_era)s,%(xwoba_against)s,
          %(opp_lineup_xwoba)s,%(used_actual_lineup)s,%(used_l15_blend)s,
          %(ip)s,%(outs)s,%(hits)s,%(er)s,%(bb)s,%(k)s,%(wx_factor)s,%(pf_factor)s,%(high_variance_flag)s
        )"""'''

new_db_sql = '''    sql = """INSERT INTO pitcher_projections (
          run_id,game_pk,mlb_id,last_first,team_code,opp_team_code,
          hand,source,pa_sample,era,xera,xfip,true_era,xwoba_against,
          opp_lineup_xwoba,used_actual_lineup,used_l15_blend,
          ip,outs,hits,er,bb,k,wx_factor,pf_factor,high_variance_flag,
          true_era_adj,barrel_pct,hard_hit_pct,contact_adj_runs,
          split_blend_note,lr_blend_note
        ) VALUES (
          %(run_id)s,%(game_pk)s,%(mlb_id)s,%(last_first)s,%(team_code)s,%(opp_team_code)s,
          %(hand)s,%(source)s,%(pa_sample)s,%(era)s,%(xera)s,%(xfip)s,%(true_era)s,%(xwoba_against)s,
          %(opp_lineup_xwoba)s,%(used_actual_lineup)s,%(used_l15_blend)s,
          %(ip)s,%(outs)s,%(hits)s,%(er)s,%(bb)s,%(k)s,%(wx_factor)s,%(pf_factor)s,%(high_variance_flag)s,
          %(true_era_adj)s,%(barrel_pct)s,%(hard_hit_pct)s,%(contact_adj_runs)s,
          %(split_blend_note)s,%(lr_blend_note)s
        )"""'''

if old_db_sql in db:
    db = db.replace(old_db_sql, new_db_sql, 1)
    db_path.write_text(db, encoding="utf-8")
    print("OK: db.insert_pitcher_projection extended")
elif "%(true_era_adj)s" in db:
    print("OK: db SQL already extended")
else:
    print("WARN: db.insert_pitcher_projection SQL not found")

# ============================================================================
# 6. orchestrator.py — pre-load splits, pass is_home + pitcher_splits
# ============================================================================
# Add splits pre-load near the top of run() loop. Anchor: where all_pit_rows
# / all_pit is built.
if "_splits_by_id" not in orch:
    # Insert a splits-loading block after the existing all_pit load
    # We need an anchor — search for the `all_pit = ` assignment in run().
    splits_loader = '''
        # Pre-load pitcher pitching splits once per run (PA/IP/ERA per split key)
        _split_rows = db.fetchall(
            "SELECT mlb_id, split_key, pa, era, ops_against "
            "FROM pitcher_pitching_splits WHERE season_year=%s",
            (season,)
        )
        _splits_by_id = {}
        for _sr in _split_rows:
            _splits_by_id.setdefault(_sr["mlb_id"], {})[_sr["split_key"]] = {
                "pa": _sr.get("pa"),
                "era": float(_sr["era"]) if _sr.get("era") is not None else None,
                "ops_against": float(_sr["ops_against"]) if _sr.get("ops_against") is not None else None,
            }
        log.info("Loaded splits for %d pitchers", len(_splits_by_id))
'''
    # Anchor: just after all_pit dict is built. Search for unique substring.
    anchors_to_try = [
        "all_pit       = {r[\"mlb_id\"]: r for r in all_pit_rows}",
        "all_pit = {r[\"mlb_id\"]: r for r in all_pit_rows}",
        "all_pit = {r['mlb_id']: r for r in all_pit_rows}",
    ]
    inserted = False
    for anchor in anchors_to_try:
        if anchor in orch:
            orch = orch.replace(anchor, anchor + splits_loader, 1)
            inserted = True
            print("OK: splits pre-load added to orchestrator")
            break
    if not inserted:
        print("WARN: orchestrator anchor for splits pre-load not found")
        print("      MANUAL ACTION: add _splits_by_id query near top of run() loop")
else:
    print("OK: splits pre-load already present")

# Pass is_home + pitcher_splits to project_pitcher call
old_call = '''                proj=projections.project_pitcher(pitcher_xstats=all_pit.get(pi.mlb_id),
                    pitcher_mlb_id=pi.mlb_id,pitcher_name=pi.last_first,pitcher_hand=pi.hand,
                    team_code=team,opp_team_code=opp,opp_lineup=opp_lu_in,hitter_xstats=all_hit,
                    team_xwoba_fallback=xwoba_fb,park=park,
                    weather={} if (park.get("roof_type") or "").lower() in ("dome","closed") else weather)'''

new_call = '''                proj=projections.project_pitcher(pitcher_xstats=all_pit.get(pi.mlb_id),
                    pitcher_mlb_id=pi.mlb_id,pitcher_name=pi.last_first,pitcher_hand=pi.hand,
                    team_code=team,opp_team_code=opp,opp_lineup=opp_lu_in,hitter_xstats=all_hit,
                    team_xwoba_fallback=xwoba_fb,park=park,
                    weather={} if (park.get("roof_type") or "").lower() in ("dome","closed") else weather,
                    is_home=is_home,
                    pitcher_splits=_splits_by_id.get(pi.mlb_id))'''

if old_call in orch:
    orch = orch.replace(old_call, new_call, 1)
    print("OK: project_pitcher call passes is_home + pitcher_splits")
elif "is_home=is_home" in orch and "_splits_by_id.get" in orch:
    print("OK: project_pitcher call already updated")
else:
    print("WARN: project_pitcher call anchor not found")

orch_path.write_text(orch, encoding="utf-8")

# ============================================================================
# 7. api.py — _hydrate_pitcher_projection reads new fields
# ============================================================================
old_hydrate_end = '''            high_variance_flag=bool(row.get("high_variance_flag", False)),
            days_rest=row.get("days_rest"),
        )'''

new_hydrate_end = '''            high_variance_flag=bool(row.get("high_variance_flag", False)),
            days_rest=row.get("days_rest"),
            true_era_adj=float(row["true_era_adj"]) if row.get("true_era_adj") is not None else None,
            barrel_pct=float(row["barrel_pct"]) if row.get("barrel_pct") is not None else None,
            hard_hit_pct=float(row["hard_hit_pct"]) if row.get("hard_hit_pct") is not None else None,
            contact_adj_runs=float(row["contact_adj_runs"]) if row.get("contact_adj_runs") is not None else None,
            split_blend_note=row.get("split_blend_note"),
            lr_blend_note=row.get("lr_blend_note"),
        )'''

if old_hydrate_end in api:
    api = api.replace(old_hydrate_end, new_hydrate_end, 1)
    api_path.write_text(api, encoding="utf-8")
    print("OK: _hydrate_pitcher_projection reads new fields")
elif "true_era_adj=float(row" in api:
    print("OK: _hydrate already reads new fields")
else:
    print("WARN: _hydrate anchor not found")

print()
print("=========================================================================")
print("Migration first:")
print("  Move 0012_pitcher_projection_adjustments.sql to backend/migrations/")
print()
print("Apply in Railway SQL console BEFORE deploying code:")
print("""
  ALTER TABLE pitcher_projections
      ADD COLUMN IF NOT EXISTS true_era_adj      NUMERIC(5, 2),
      ADD COLUMN IF NOT EXISTS barrel_pct        NUMERIC(5, 4),
      ADD COLUMN IF NOT EXISTS hard_hit_pct      NUMERIC(5, 4),
      ADD COLUMN IF NOT EXISTS contact_adj_runs  NUMERIC(5, 2),
      ADD COLUMN IF NOT EXISTS split_blend_note  TEXT,
      ADD COLUMN IF NOT EXISTS lr_blend_note     TEXT;
""")
print("Verify and push:")
print('  python -X utf8 -c "import ast; ast.parse(open(\'backend/src/projections.py\').read()); ast.parse(open(\'backend/src/orchestrator.py\').read()); ast.parse(open(\'backend/src/api.py\').read()); ast.parse(open(\'backend/src/db.py\').read()); print(\'OK\')"')
print("  git add backend/migrations/0012_pitcher_projection_adjustments.sql")
print("  git add backend/src/projections.py backend/src/orchestrator.py backend/src/api.py backend/src/db.py")
print("  git add patch_model_integration.py 0012_pitcher_projection_adjustments.sql")
print("  git commit -m 'Integrate barrel/hardhit + home/away splits + L/R splits into model'")
print("  git push")
print()
print("After deploy, trigger orchestrator manually (admin panel).")
print("Then check /api/admin/diag/projection_bias over next 1-2 days.")
print("Per-pitcher adjustments visible via /api/admin/diag/pitcher_projections.")
print("=========================================================================")
