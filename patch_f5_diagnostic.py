"""
Run from repo root: python patch_f5_diagnostic.py

Adds a temporary diagnostic endpoint to api.py:
  GET /api/admin/diag/f5/{token}?game_pk=N

This endpoint:
  1. Calls mlb_api.get_linescore(game_pk) directly
  2. Returns the raw inning data and what the F5 code would compute
  3. Actually runs the UPDATE and returns whether it succeeded

This bypasses the grade_box_score wrapper to isolate where the F5 write is failing.
"""
from pathlib import Path

api_path = Path("backend/src/api.py")
content = api_path.read_text(encoding="utf-8")

# Anchor: insert right before "# ---------- Admin actions ----------" comment
# or at the end. We'll just append to the file.

diag_code = '''


# ============================================================================
# F5 DIAGNOSTIC ENDPOINT — TEMPORARY
# ============================================================================
@app.get("/api/admin/diag/f5/{token}")
def diag_f5(token: str, game_pk: int):
    """Diagnose F5 grading for one game. Returns raw inning data + what the
    F5 code would compute + actually attempts the UPDATE and reports result."""
    if token != os.environ.get("ADMIN_TOKEN"):
        raise HTTPException(status_code=403, detail="Forbidden")

    from . import mlb_api, db
    import traceback

    result = {"game_pk": game_pk, "steps": []}

    # Step 1: fetch linescore from MLB API
    try:
        ls = mlb_api.get_linescore(game_pk)
        result["steps"].append({"step": "fetch_linescore", "ok": True,
                                 "top_keys": list(ls.keys())})
    except Exception as e:
        result["steps"].append({"step": "fetch_linescore", "ok": False,
                                 "error": str(e), "tb": traceback.format_exc()})
        return result

    # Step 2: parse innings
    innings = ls.get("innings") or []
    result["steps"].append({"step": "parse_innings", "count": len(innings),
                             "first_5": [
                                 {"num": inn.get("num"),
                                  "away": inn.get("away"),
                                  "home": inn.get("home")}
                                 for inn in innings[:5]]})

    if len(innings) < 5:
        result["steps"].append({"step": "guard_innings_count", "passed": False,
                                 "reason": f"only {len(innings)} innings"})
        return result

    # Step 3: compute F5 sums
    away_f5 = sum(int((inn.get("away") or {}).get("runs") or 0) for inn in innings[:5])
    home_f5 = sum(int((inn.get("home") or {}).get("runs") or 0) for inn in innings[:5])
    home5 = innings[4].get("home") or {}
    home_runs_through_4 = sum(int((inn.get("home") or {}).get("runs") or 0)
                               for inn in innings[:4])
    result["steps"].append({"step": "compute", "away_f5": away_f5,
                             "home_f5": home_f5,
                             "home5_keys": list(home5.keys()),
                             "runs_in_home5": "runs" in home5,
                             "home_through_4": home_runs_through_4,
                             "away_f5_gt_home4": away_f5 > home_runs_through_4})

    # Step 4: check guard
    guard = ("runs" in home5) or (away_f5 > home_runs_through_4)
    result["steps"].append({"step": "guard", "passed": guard})

    if not guard:
        return result

    # Step 5: actually run the UPDATE
    try:
        rowcount = db.execute(
            "UPDATE games SET away_f5_runs=%s, home_f5_runs=%s WHERE game_pk=%s",
            (away_f5, home_f5, game_pk),
        )
        result["steps"].append({"step": "update", "ok": True, "rowcount": rowcount})
    except Exception as e:
        result["steps"].append({"step": "update", "ok": False, "error": str(e),
                                 "tb": traceback.format_exc()})

    # Step 6: read back to confirm
    try:
        row = db.fetchone(
            "SELECT away_f5_runs, home_f5_runs FROM games WHERE game_pk=%s",
            (game_pk,))
        result["steps"].append({"step": "readback", "row": dict(row) if row else None})
    except Exception as e:
        result["steps"].append({"step": "readback", "ok": False, "error": str(e)})

    return result
'''

if "/api/admin/diag/f5/" in content:
    print("WARN: diagnostic endpoint already present, skipping")
else:
    content = content.rstrip() + diag_code + "\n"
    api_path.write_text(content, encoding="utf-8")
    print("OK: F5 diagnostic endpoint added to api.py")
    print()
    print("Next:")
    print("  git add backend/src/api.py patch_f5_diagnostic.py")
    print("  git commit -m 'F5 diagnostic endpoint'")
    print("  git push")
    print()
    print("Then after deploy, hit:")
    print("  https://YOUR-RAILWAY-URL.up.railway.app/api/admin/diag/f5/YOUR_TOKEN?game_pk=823060")
    print()
    print("Returns JSON with every step of the F5 logic and what it actually did.")
