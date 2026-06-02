"""
test_f5_model.py — unit tests for the v2 F5 model.

Drop in backend/tests/ and run from repo root (no pytest / DB / network needed):
    python backend/tests/test_f5_model.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.projections import (
    HitterSpot, PitcherProjection, project_game_total, project_pitcher,
    opp_lineup_xwoba, PA_WEIGHTS_F5,
)


def approx(a, b, tol=0.02):
    return abs(a - b) < tol


def _proj(ip, er, true_era=4.0, hv=False, f5_mult=None):
    p = PitcherProjection(
        pitcher_mlb_id=1, last_first="X, Y", team_code="A", opp_team_code="B",
        hand="R", source="statcast", pa_sample=100,
        era=true_era, xera=true_era, xfip=true_era, true_era=true_era,
        xwoba_against=0.320, opp_lineup_xwoba=0.320,
        used_actual_lineup=False, used_l15_blend=False,
        ip=ip, outs=ip * 3, hits=8.0, er=er, bb=2.0, k=5.0,
        wx_factor=1.0, pf_factor=1.0, high_variance_flag=hv, days_rest=4,
    )
    if f5_mult is not None:
        p.f5_lineup_mult = f5_mult
    return p


def _lineup():
    return [HitterSpot(mlb_id=1000 + i, last_first=f"H{i}", bat_side="R", order=i + 1)
            for i in range(9)]


def _xstats(top_woba, bot_woba):
    return {1000 + i: {"est_woba": (top_woba if i < 4 else bot_woba), "pa": 200}
            for i in range(9)}


# --- coverage / structure -----------------------------------------------------

def test_full_start_no_gap_is_simple_scaling():
    # ip=6 -> no gap inning, neutral mult/SHAPE/CALIB -> (er/ip)*5 per side
    away = _proj(ip=6.0, er=2.4); home = _proj(ip=6.0, er=2.4)
    _, f5, _, _ = project_game_total(away_proj=away, home_proj=home)
    assert approx(f5, 2 * ((2.4 / 6.0) * 5.0))


def test_short_start_fills_gap_not_zero():
    # ip=4 -> 1 F5 inning is bullpen, must be filled (old min(5,ip) zeroed it)
    away = _proj(ip=4.0, er=2.4); home = _proj(ip=4.0, er=2.4)
    _, f5, _, _ = project_game_total(away_proj=away, home_proj=home)
    old_style = 2 * ((2.4 / 4.0) * min(5.0, 4.0))   # = 4.8, the buggy value
    assert f5 > old_style


def test_missing_mult_defaults_neutral_no_crash():
    away = _proj(ip=6.0, er=2.4); home = _proj(ip=6.0, er=2.4)
    assert not hasattr(away, "f5_lineup_mult")        # bare dataclass
    _, f5, _, _ = project_game_total(away_proj=away, home_proj=home)
    assert f5 > 0


# --- lineup tilt --------------------------------------------------------------

def test_f5_lineup_mult_raises_f5():
    base_a = _proj(ip=6.0, er=2.4, f5_mult=1.0)
    base_h = _proj(ip=6.0, er=2.4, f5_mult=1.0)
    tilt_a = _proj(ip=6.0, er=2.4, f5_mult=1.06)
    tilt_h = _proj(ip=6.0, er=2.4, f5_mult=1.06)
    _, f5_base, _, _ = project_game_total(away_proj=base_a, home_proj=base_h)
    _, f5_tilt, _, _ = project_game_total(away_proj=tilt_a, home_proj=tilt_h)
    assert f5_tilt > f5_base


def test_f5_weights_tilt_toward_top_of_order():
    lu = _lineup(); xs = _xstats(top_woba=0.360, bot_woba=0.290)
    full, _, _ = opp_lineup_xwoba(lu, "R", xs, 0.310)
    f5,   _, _ = opp_lineup_xwoba(lu, "R", xs, 0.310, weights=PA_WEIGHTS_F5)
    assert f5 > full     # strong top 4 weighted harder in the F5 window


def test_pa_weights_f5_structure():
    assert all(PA_WEIGHTS_F5[i] == 3.0 for i in (1, 2, 3, 4))
    assert all(PA_WEIGHTS_F5[i] == 2.0 for i in (5, 6, 7, 8, 9))


def test_project_pitcher_sets_f5_mult_attr():
    lu = _lineup(); xs = _xstats(top_woba=0.360, bot_woba=0.290)
    p = project_pitcher(
        pitcher_xstats={"pa": 150, "xera": 3.5, "era": 3.5, "est_woba": 0.300,
                        "est_ba": 0.230, "ba": 0.230, "tbf": 600, "gs": 20,
                        "ip_total": 120},
        pitcher_mlb_id=9, pitcher_name="P, X", pitcher_hand="R",
        team_code="A", opp_team_code="B", opp_lineup=lu, hitter_xstats=xs,
        team_xwoba_fallback=0.320,
        park={"pf_runs": 100, "pf_so": 100, "pf_bb": 100,
              "cf_azimuth_deg": 0, "roof_type": "open"},
        weather={},
    )
    assert hasattr(p, "f5_lineup_mult")
    assert p.f5_lineup_mult >= 1.0     # strong top of order -> tilt up (or neutral)


# --- runner -------------------------------------------------------------------

if __name__ == "__main__":
    import traceback
    tests = [(n, fn) for n, fn in sorted(globals().items())
             if n.startswith("test_") and callable(fn)]
    failed = []
    for name, fn in tests:
        try:
            fn()
            print(f"  PASS  {name}")
        except Exception as e:
            failed.append(name)
            print(f"  FAIL  {name}  ({type(e).__name__}: {e})")
            traceback.print_exc()
    print(f"\n{len(tests) - len(failed)}/{len(tests)} passed")
    sys.exit(0 if not failed else 1)
