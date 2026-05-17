"""
Run from repo root: python patch_grader_f5_fix.py

Fixes the F5 grading bug introduced in v4.2:
  - The v1.1 live feed (/game/{pk}/feed/live) returns linescore.innings as
    a list of empty dicts — no per-inning runs data.
  - The standalone linescore endpoint (/game/{pk}/linescore) DOES contain
    per-inning home.runs and away.runs.

This patch:
  1. Adds get_linescore(game_pk) to mlb_api.py
  2. Rewrites the F5 block in grade_box_score to fetch from the standalone endpoint
"""
from pathlib import Path

# ============================================================================
# 1. Add get_linescore() helper to mlb_api.py
# ============================================================================

mlb_api_path = Path("backend/src/mlb_api.py")
mlb_api_content = mlb_api_path.read_text(encoding="utf-8")

old_get_box = '''def get_box_score(game_pk: int) -> dict:
    """
    Fetch the full live feed for a finished game. Used by the nightly grader
    to extract pitcher lines (IP/H/ER/BB/K) and final scores.

    Returns the raw JSON payload; the grader walks it for what it needs.
    """
    return _request(f"/api/v1.1/game/{game_pk}/feed/live")'''

new_get_box = '''def get_box_score(game_pk: int) -> dict:
    """
    Fetch the full live feed for a finished game. Used by the nightly grader
    to extract pitcher lines (IP/H/ER/BB/K) and final scores.

    Returns the raw JSON payload; the grader walks it for what it needs.
    """
    return _request(f"/api/v1.1/game/{game_pk}/feed/live")


def get_linescore(game_pk: int) -> dict:
    """
    Fetch the standalone linescore for a finished game.

    Used by the nightly grader to extract per-inning runs (F5 grading).
    The v1.1 live feed's linescore.innings has empty inning objects;
    this v1 endpoint actually contains away.runs / home.runs per inning.

    Returns shape:
      {
        "innings": [{"num": 1, "home": {"runs": N, ...}, "away": {"runs": N, ...}}, ...],
        "teams": {"home": {"runs": N, ...}, "away": {"runs": N, ...}},
        ...
      }
    """
    return _request(f"/api/v1/game/{game_pk}/linescore")'''

if old_get_box in mlb_api_content:
    mlb_api_content = mlb_api_content.replace(old_get_box, new_get_box, 1)
    print("OK: get_linescore() added to mlb_api.py")
else:
    print("WARN: get_box_score block not found in mlb_api.py — could not add get_linescore()")
    print("       (already added? check manually)")

# ============================================================================
# 2. Rewrite the F5 block in grader.py to use get_linescore()
# ============================================================================

grader_path = Path("backend/src/grader.py")
grader_content = grader_path.read_text(encoding="utf-8")

old_f5_block = '''    # F5 (first 5 innings) — walk linescore.innings[:5] for F5 total grading.
    # Only persist if the game actually reached the bottom of the 5th; otherwise
    # leave NULL so F5 edges on rain-shortened games stay ungraded (safer than
    # booking a partial-inning result as a loss).
    innings = linescore.get("innings") or []
    if len(innings) >= 5:
        try:
            away_f5 = sum(int((inn.get("away") or {}).get("runs") or 0) for inn in innings[:5])
            home_f5 = sum(int((inn.get("home") or {}).get("runs") or 0) for inn in innings[:5])
            # Confirm bottom of 5th was actually played — if home was already
            # ahead and didn't bat, MLB still includes innings[4] but home.runs
            # may be missing entirely (key absent). Treat that as incomplete.
            home5 = (innings[4].get("home") or {})
            if "runs" in home5 or away_f5 > sum(int((inn.get("home") or {}).get("runs") or 0) for inn in innings[:4]):
                db.execute(
                    "UPDATE games SET away_f5_runs=%s, home_f5_runs=%s WHERE game_pk=%s",
                    (away_f5, home_f5, game_pk),
                )
        except (ValueError, TypeError, IndexError) as exc:
            log.warning("F5 inning parse failed for game %s: %s", game_pk, exc)'''

new_f5_block = '''    # F5 (first 5 innings) — fetch the standalone linescore endpoint, which
    # actually contains per-inning runs (the v1.1 live feed has them empty).
    # Only persist if the game reached the bottom of the 5th; otherwise leave
    # NULL so F5 edges on rain-shortened games stay ungraded.
    try:
        ls = mlb_api.get_linescore(game_pk)
        innings = ls.get("innings") or []
        if len(innings) >= 5:
            away_f5 = sum(int((inn.get("away") or {}).get("runs") or 0) for inn in innings[:5])
            home_f5 = sum(int((inn.get("home") or {}).get("runs") or 0) for inn in innings[:5])
            # Confirm bottom of 5th was actually played — if home was already
            # ahead and didn't bat, home.runs key is missing on innings[4].
            home5 = (innings[4].get("home") or {})
            home_runs_through_4 = sum(int((inn.get("home") or {}).get("runs") or 0) for inn in innings[:4])
            if "runs" in home5 or away_f5 > home_runs_through_4:
                db.execute(
                    "UPDATE games SET away_f5_runs=%s, home_f5_runs=%s WHERE game_pk=%s",
                    (away_f5, home_f5, game_pk),
                )
                log.info("F5 runs written for game %s: away=%s home=%s", game_pk, away_f5, home_f5)
            else:
                log.info("F5 not persisted for game %s (bottom of 5th not played)", game_pk)
        else:
            log.info("F5 not persisted for game %s (only %d innings)", game_pk, len(innings))
    except Exception as exc:
        log.warning("F5 linescore fetch/parse failed for game %s: %s", game_pk, exc)'''

if old_f5_block in grader_content:
    grader_content = grader_content.replace(old_f5_block, new_f5_block, 1)
    print("OK: F5 block in grader.py rewritten to use get_linescore()")
else:
    print("WARN: old F5 block not found in grader.py — pattern mismatch")
    # Debug: find the f5 line
    lines = grader_content.splitlines()
    for i, line in enumerate(lines):
        if "F5 (first 5 innings)" in line:
            print(f"  found F5 comment at line {i+1}")

# Bump grader version
old_header = '''Nightly grader — v4.2

Changes vs v4.1:
  - grade_box_score: extracts F5 (first 5 innings) runs from linescore.innings
    and persists away_f5_runs/home_f5_runs on games. Requires migration 0006.
  - actual_value_for_edge: F5 branch now returns realized F5 total instead of None.
  - grade_yesterday: JOIN query includes f5 columns so F5 edges can be graded.'''

new_header = '''Nightly grader — v4.3

Changes vs v4.2:
  - grade_box_score: F5 runs now fetched from /api/v1/game/{pk}/linescore
    (standalone endpoint) instead of the v1.1 live feed, which returns
    empty inning objects. Fixes silent F5 NULL bug.

Changes vs v4.1:
  - grade_box_score: extracts F5 (first 5 innings) runs and persists
    away_f5_runs/home_f5_runs on games. Requires migration 0006.
  - actual_value_for_edge: F5 branch now returns realized F5 total instead of None.
  - grade_yesterday: JOIN query includes f5 columns so F5 edges can be graded.'''

if old_header in grader_content:
    grader_content = grader_content.replace(old_header, new_header, 1)
    print("OK: grader version bumped to v4.3")
else:
    print("WARN: header block not found — version not bumped")

# Apply all writes
mlb_api_path.write_text(mlb_api_content, encoding="utf-8")
grader_path.write_text(grader_content, encoding="utf-8")
print("\nOK: All F5 fix patches applied")
print("\nNext steps:")
print("  1. git add backend/src/mlb_api.py backend/src/grader.py patch_grader_f5_fix.py")
print("  2. git commit -m 'F5 fix: use standalone linescore endpoint (grader v4.3)'")
print("  3. git push  (Railway auto-deploys)")
print("  4. After deploy, in Railway SQL console:")
print("       DELETE FROM pitcher_actuals WHERE game_pk IN")
print("       (SELECT game_pk FROM games WHERE game_date = '2026-05-16');")
print("  5. Hit grader endpoint: /api/admin/trigger/grader/<token>?date=2026-05-16")
