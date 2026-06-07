"""
Run from REPO ROOT: python patch_ml_threshold_schedule.py

Replaces the moneyline edge threshold with a per-price schedule:
    odds >= +100  -> 0.60   (any underdog / plus money)
    odds <= -141  -> 0.30   (favorite stronger than -140)
    -140..-100    -> 0.45   (even through -140)

Three edits to backend/src/orchestrator.py:
  1. Add a module-level ml_threshold(odds) (single source of truth)
  2. compute_edges_for_game: drop the local ml_threshold def so it uses the
     module-level one (this is the path that creates the flagged ML edges)
  3. The orchestrator-loop game_projections calc: stop using the flat
     EDGE_THRESHOLDS["ML"] and use the same schedule (so the projection row
     and the flagged edge can't disagree)

Anchor-gated + idempotent. Writes orchestrator.py.bak. If any edit prints WARN,
the file drifted from the indexed copy — paste a Select-String of that block
and it'll be re-anchored rather than forced.
"""
from pathlib import Path

PATH = "backend/src/orchestrator.py"

EDITS = [
    # 1. Add module-level ml_threshold between remove_vig and poisson_tail_prob.
    ("add module-level ml_threshold",
'''def remove_vig(a,h): t=a+h; return a/t,h/t
def poisson_tail_prob(lam,line,side):''',
'''def remove_vig(a,h): t=a+h; return a/t,h/t
def ml_threshold(odds):
    # Required model edge over no-vig implied, by moneyline price.
    # More favored = lower bar (that's where a real edge is reachable).
    if odds is None:
        return 0.45
    if odds >= 100:
        return 0.60      # any underdog / plus money
    if odds < -140:
        return 0.30      # favorite stronger than -140
    return 0.45          # even (-100) through -140
def poisson_tail_prob(lam,line,side):'''),

    # 2. compute_edges_for_game: remove the local ml_threshold def.
    ("compute_edges_for_game: use module-level ml_threshold",
'''        ai,hi=remove_vig(american_to_implied(away_ml),american_to_implied(home_ml))
        hep=home_win_prob-hi; aep=away_win_prob-ai
        def ml_threshold(odds):
            # Tightened: any non-favorite (+100 or longer) needs 60pp; fav needs 20pp
            return 0.60 if odds is not None and odds >= 100 else EDGE_THRESHOLDS["ML"]
        if hep > 0 and hep>=ml_threshold(home_ml) and hep>=aep:''',
'''        ai,hi=remove_vig(american_to_implied(away_ml),american_to_implied(home_ml))
        hep=home_win_prob-hi; aep=away_win_prob-ai
        if hep > 0 and hep>=ml_threshold(home_ml) and hep>=aep:'''),

    # 3. orchestrator-loop game_projections calc: schedule + positive-edge only.
    ("game_projections calc: use ml_threshold schedule",
'''                hep=home_win_prob-hi; aep=away_win_prob-ai
                if abs(hep)>=EDGE_THRESHOLDS["ML"] and hep>=aep: ml_edge_team,ml_edge_pct=g.home_team,round(hep,4)
                elif abs(aep)>=EDGE_THRESHOLDS["ML"]: ml_edge_team,ml_edge_pct=g.away_team,round(aep,4)''',
'''                hep=home_win_prob-hi; aep=away_win_prob-ai
                if hep>0 and hep>=ml_threshold(home_ml) and hep>=aep: ml_edge_team,ml_edge_pct=g.home_team,round(hep,4)
                elif aep>0 and aep>=ml_threshold(away_ml): ml_edge_team,ml_edge_pct=g.away_team,round(aep,4)'''),
]


def main():
    p = Path(PATH)
    if not p.exists():
        print(f"ERR: {PATH} not found. Run this from the repo root (where backend\\src lives).")
        raise SystemExit(1)
    original = p.read_text(encoding="utf-8")
    content = original
    changed = False
    for label, old, new in EDITS:
        if old in content:
            content = content.replace(old, new, 1)
            changed = True
            print(f"  OK   {label}")
        elif new in content:
            print(f"  skip {label} (already applied)")
        else:
            print(f"  WARN {label}: anchor not found — left untouched")
    if changed:
        Path(PATH + ".bak").write_text(original, encoding="utf-8")
        p.write_text(content, encoding="utf-8")
        print(f"\n  -> wrote {PATH} (backup: {PATH}.bak)")
    else:
        print("\n  no changes written")
    print()
    print("Verify, then ship:")
    print('  python -X utf8 -c "import ast; ast.parse(open(' + repr(PATH) + r").read()); print('py OK')\"")
    print(f"  git add {PATH} patch_ml_threshold_schedule.py")
    print('  git commit -m "ML: per-moneyline edge thresholds (30pp heavy fav / 45pp even-to-140 / 60pp dog)"')
    print("  git push   # confirm Railway deploys the commit, then manual orchestrator run")


if __name__ == "__main__":
    main()
