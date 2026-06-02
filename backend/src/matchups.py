"""
matchups.py — per-game lineup matchup scoring for the Slate cards.

For one game, builds both lineups joined to season Statcast + L15/L5 form and
platoon splits, each hitter scored 0-100 against the opposing listed starter.

The score reuses the SAME components the projection engine uses in
opp_lineup_xwoba, so it is consistent with how the model actually prices a
lineup:
  blended xwOBA (0.55 season / 0.25 L15 / 0.20 L5)
    -> platoon-adjusted via _platoon_factor (per-hitter vs-hand split when it
       has enough PA, else league handedness multiplier)
    -> blended 60/40 with the starter's xwoba_against (pitcher suppression)
    -> normalized to 0-100 from the hitter's perspective.

Higher score = better spot for the hitter / tougher for the pitcher.
"""
from __future__ import annotations
from datetime import date

from . import db
from .projections import LEAGUE_XWOBA, _platoon_factor

# matchup wOBA -> 0-100 anchors (avg matchup ~.320 lands ~45 = "mid").
_SCORE_LO = 0.270
_SCORE_HI = 0.380

# Tier thresholds match the app's existing composite coloring (good/mid/bad).
def _tier(score):
    return "good" if score >= 60 else ("bad" if score < 42 else "mid")


def _blended_xwoba(row):
    season = row.get("est_woba")
    if season is None:
        return None
    season = float(season)
    l15 = row.get("l15_woba")
    l5 = row.get("l5_woba")
    if l15 is not None and l5 is not None:
        return 0.55 * season + 0.25 * float(l15) + 0.20 * float(l5)
    if l15 is not None:
        return 0.70 * season + 0.30 * float(l15)
    return season


def score_hitter(row, splits, pitcher_hand, pitcher_xwoba_against):
    """Matchup breakdown for one hitter vs one starter. row is a hitter_xstats
    dict (may be empty), splits is {vs_hand: {pa, est_woba}}."""
    blended = _blended_xwoba(row)
    if blended is None:
        return {"score": None, "tier": "na", "blended": None,
                "platoon_mult": None, "matchup_woba": None, "used_split": False}
    bat_side = row.get("bat_side", "R")
    platoon = _platoon_factor(bat_side, pitcher_hand, splits)
    sp = (splits or {}).get(pitcher_hand) or {}
    used_split = (sp.get("pa") or 0) >= 80
    eff = blended * platoon
    pxw = float(pitcher_xwoba_against) if pitcher_xwoba_against is not None else LEAGUE_XWOBA
    matchup_woba = 0.60 * eff + 0.40 * pxw
    score = max(0.0, min(100.0, (matchup_woba - _SCORE_LO) / (_SCORE_HI - _SCORE_LO) * 100.0))
    return {
        "score": round(score),
        "tier": _tier(score),
        "blended": round(blended, 3),
        "platoon_mult": round(platoon, 3),
        "matchup_woba": round(matchup_woba, 3),
        "used_split": used_split,
    }


def _pitcher_xwoba(mlb_id, season):
    if not mlb_id:
        return None
    r = db.fetchone(
        "SELECT est_woba FROM pitcher_xstats WHERE mlb_id=%s AND season_year=%s",
        (mlb_id, season))
    return float(r["est_woba"]) if r and r.get("est_woba") is not None else None


def build_matchup(game_pk):
    """Assemble the full matchup payload for one game, or None if not found."""
    season = date.today().year
    game = db.fetchone("SELECT * FROM games WHERE game_pk=%s", (game_pk,))
    if not game:
        return None

    lineup_rows = db.fetchall(
        "SELECT team_code, batting_order, mlb_id, last_first, bat_side, position "
        "FROM lineup_spots WHERE game_pk=%s ORDER BY team_code, batting_order",
        (game_pk,))

    away_hand = game.get("away_pitcher_hand") or "R"
    home_hand = game.get("home_pitcher_hand") or "R"
    away_pxw = _pitcher_xwoba(game.get("away_pitcher_id"), season)
    home_pxw = _pitcher_xwoba(game.get("home_pitcher_id"), season)

    ids = [r["mlb_id"] for r in lineup_rows]
    hit: dict[int, dict] = {}
    if ids:
        for r in db.fetchall(
                "SELECT * FROM hitter_xstats WHERE season_year=%s AND mlb_id = ANY(%s)",
                (season, ids)):
            hit[r["mlb_id"]] = dict(r)
        for sr in db.fetchall(
                "SELECT mlb_id, vs_hand, pa, est_woba FROM hitter_splits "
                "WHERE season_year=%s AND mlb_id = ANY(%s)",
                (season, ids)):
            hit.setdefault(sr["mlb_id"], {}).setdefault("splits", {})[sr["vs_hand"]] = \
                {"pa": sr["pa"], "est_woba": sr["est_woba"]}

    def build_side(team_code, opp_hand, opp_pxw):
        out = []
        for lr in [r for r in lineup_rows if r["team_code"] == team_code]:
            row = dict(hit.get(lr["mlb_id"], {}))
            row["bat_side"] = lr.get("bat_side", "R")
            sc = score_hitter(row, row.get("splits"), opp_hand, opp_pxw)
            out.append({
                "order": lr["batting_order"],
                "mlb_id": lr["mlb_id"],
                "name": (lr.get("last_first") or "").split(",")[0].strip(),
                "bat_side": lr.get("bat_side"),
                "position": lr.get("position"),
                "season_xwoba": round(float(row["est_woba"]), 3) if row.get("est_woba") is not None else None,
                "k_pct": float(row["k_pct"]) if row.get("k_pct") is not None else None,
                **sc,
            })
        return out

    return {
        "game_pk": game_pk,
        "away_team": game["away_team"],
        "home_team": game["home_team"],
        # away hitters face the HOME starter, and vice versa
        "away_pitcher": {"name": (game.get("away_pitcher_name") or "").split(",")[0].strip(),
                         "hand": away_hand, "xwoba_against": away_pxw},
        "home_pitcher": {"name": (game.get("home_pitcher_name") or "").split(",")[0].strip(),
                         "hand": home_hand, "xwoba_against": home_pxw},
        "away_lineup": build_side(game["away_team"], home_hand, home_pxw),
        "home_lineup": build_side(game["home_team"], away_hand, away_pxw),
    }
