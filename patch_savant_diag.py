"""
Run from repo root: python patch_savant_diag.py

The pitcher contact refresh wrote 0 rows because my Savant CSV column
guesses (exit_velocity_avg, hard_hit_percent, etc.) don't match what
Savant actually returns. Add a diagnostic endpoint that fetches the CSV
once and returns:
  - the URL it called
  - HTTP status
  - first few rows raw
  - column headers
  - a sample row's values

Hit the endpoint, paste the JSON, and we can fix the column names.
"""
from pathlib import Path

api_path = Path("backend/src/api.py")
content = api_path.read_text(encoding="utf-8")

if "/api/admin/diag/savant_pitcher_csv/" in content:
    print("OK: Savant diagnostic endpoint already present")
    raise SystemExit(0)

endpoint = '''


@app.get("/api/admin/diag/savant_pitcher_csv/{token}")
def diag_savant_pitcher_csv(token: str):
    """Show what Savant's pitcher leaderboard CSV actually returns."""
    _check_admin(token)
    import requests, csv, io
    from datetime import date as _date
    from . import statcast_refresh

    year = _date.today().year
    url = statcast_refresh.SAVANT_EXIT_VELO_URL.format(year=year)
    try:
        r = requests.get(url, headers=statcast_refresh.SAVANT_HEADERS, timeout=30)
        r.raise_for_status()
    except Exception as e:
        return {"url": url, "error": str(e)}

    text = r.text
    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames or []
    rows = []
    for i, row in enumerate(reader):
        if i >= 3:
            break
        rows.append(dict(row))

    return {
        "url": url,
        "status": r.status_code,
        "content_length": len(text),
        "n_columns": len(headers),
        "headers": headers,
        "sample_rows": rows,
    }
'''

content = content.rstrip() + endpoint + "\n"
api_path.write_text(content, encoding="utf-8")
print("OK: /api/admin/diag/savant_pitcher_csv endpoint added")
print()
print("Steps:")
print('  python -X utf8 -c "import ast; ast.parse(open(\'backend/src/api.py\').read()); print(\'OK\')"')
print("  git add backend/src/api.py patch_savant_diag.py")
print("  git commit -m 'Diag: inspect Savant pitcher CSV headers'")
print("  git push")
print()
print("After deploy, hit:")
print("  https://YOUR-RAILWAY-URL.up.railway.app/api/admin/diag/savant_pitcher_csv/YOUR_TOKEN")
print()
print("Paste me the 'headers' array + the first 'sample_rows' entry and I'll")
print("write the column-name fix.")
