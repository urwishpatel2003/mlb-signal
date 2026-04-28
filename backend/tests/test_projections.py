"""
Tests for projection engine & supporting modules.

Run with: pytest backend/tests
or:       python -m tests.test_projections

These tests do not require a database connection or network access. They
verify the math and edge-case behavior of pure functions.
"""
import sys, os, math
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.projections import (
    HitterSpot, PitcherProjection,
    opp_lineup_xwoba, project_pitcher, project_game_total,
    temp_run_factor, wind_run_factor,
    PLATOON_XWOBA, PA_WEIGHTS, LEAGUE_XWOBA,
)


def approx(a, b, tol=0.01):
    return abs(a - b) < tol


# ---------- temp_run_factor ----------

def test_temp_factor_neutral_at_70():
    assert approx(temp_run_factor(70), 1.0)

def test_temp_factor_higher_at_warm():
    assert temp_run_factor(85) > 1.0

def test_temp_factor_lower_at_cold():
    assert temp_run_factor(50) < 1.0

def test_temp_factor_handles_none():
    assert temp_run_factor(None) == 1.0


# ---------- wind_run_factor ----------

def test_wind_neutral_when_no_wind():
    assert approx(wind_run_factor(0, 90, 0), 1.0)

def test_wind_neutral_when_dir_unknown():
    assert approx(wind_run_factor(20, None, 0), 1.0)

def test_wind_blowing_out_increases_runs():
    # Wind FROM 180 (south) blowing TO north (CF az = 0) → tailwind to CF
    assert wind_run_factor(15, 180, 0) > 1.0

def test_wind_blowing_in_decreases_runs():
    # Wind FROM 0 (north) blowing TO south (away from CF) → headwind
    assert wind_run_factor(15, 0, 0) < 1.0

def test_wind_crosswind_neutral_to_slight_negative():
    # Wind perpendicular to CF axis — pure cross
    factor = wind_run_factor(15, 90, 0)
    assert factor < 1.0   # cross slightly suppresses


# ---------- opp_lineup_xwoba ----------

def make_lineup(hands_by_order: list[str]) -> list[HitterSpot]:
    return [
        HitterSpot(mlb_id=1000+i, last_first=f"Test{i}, Player",
                   bat_side=h, order=i+1)
        for i, h in enumerate(hands_by_order)
    ]


def make_xstats(mlb_ids: list[int], xwoba: float = 0.330, pa: int = 50) -> dict:
    return {pid: {"est_woba": xwoba, "pa": pa} for pid in mlb_ids}


def test_opp_xwoba_falls_back_when_lineup_empty():
    fb = 0.310
    xwoba, used = opp_lineup_xwoba([], "R", {}, fb)
    assert xwoba == fb
    assert used is False


def test_opp_xwoba_falls_back_when_too_few_hitters_have_data():
    lineup = make_lineup(["R"] * 9)
    # only 3 of 9 have data
    xstats = make_xstats([1000, 1001, 1002])
    xwoba, used = opp_lineup_xwoba(lineup, "R", xstats, 0.310)
    assert used is False
    assert xwoba == 0.310


def test_opp_xwoba_uses_lineup_when_enough_data():
    lineup = make_lineup(["R"] * 9)
    xstats = make_xstats([1000+i for i in range(9)], xwoba=0.350, pa=100)
    xwoba, used = opp_lineup_xwoba(lineup, "R", xstats, 0.310)
    assert used is True
    # All hitters .350 vs RHP, RH-vs-R platoon multiplier = 0.97
    expected = 0.350 * 0.97
    assert approx(xwoba, expected, tol=0.005)


def test_opp_xwoba_platoon_lefty_vs_righty():
    """All-lefty lineup vs RHP should boost xwOBA above raw .350 (platoon advantage)."""
    lineup = make_lineup(["L"] * 9)
    xstats = make_xstats([1000+i for i in range(9)], xwoba=0.350, pa=100)
    xwoba, used = opp_lineup_xwoba(lineup, "R", xstats, 0.310)
    assert used is True
    # L-vs-R = 1.04 multiplier
    expected = 0.350 * 1.04
    assert approx(xwoba, expected, tol=0.005)


def test_opp_xwoba_platoon_lefty_vs_lefty_decreases():
    """All-lefty lineup vs LHP should decrease (platoon disadvantage)."""
    lineup = make_lineup(["L"] * 9)
    xstats = make_xstats([1000+i for i in range(9)], xwoba=0.350, pa=100)
    xwoba, used = opp_lineup_xwoba(lineup, "L", xstats, 0.310)
    expected = 0.350 * 0.93
    assert approx(xwoba, expected, tol=0.005)


# ---------- project_pitcher ----------

DEFAULT_PARK = {
    "pf_runs": 100, "pf_so": 100, "pf_bb": 100,
    "cf_azimuth_deg": 0, "roof_type": "open",
}


def test_project_uses_league_avg_for_no_data():
    p = project_pitcher(
        pitcher_xstats=None,
        pitcher_mlb_id=999, pitcher_name="New, Rookie", pitcher_hand="R",
        team_code="AAA", opp_team_code="BBB", opp_lineup=[],
        hitter_xstats={}, team_xwoba_fallback=0.320,
        park=DEFAULT_PARK, weather={},
    )
    assert p.source == "league_avg"
    assert p.era is None
    assert p.xera is None


def test_project_uses_low_sample_for_under_threshold():
    p = project_pitcher(
        pitcher_xstats={"pa": 15, "xera": 3.0, "era": 2.5,
                        "est_woba": 0.270, "est_ba": 0.210, "ba": 0.200},
        pitcher_mlb_id=999, pitcher_name="Newish, Pitcher", pitcher_hand="R",
        team_code="AAA", opp_team_code="BBB", opp_lineup=[],
        hitter_xstats={}, team_xwoba_fallback=0.320,
        park=DEFAULT_PARK, weather={},
    )
    assert p.source == "low_sample"


def test_project_uses_statcast_for_well_sampled():
    p = project_pitcher(
        pitcher_xstats={"pa": 150, "xera": 2.5, "era": 2.2,
                        "est_woba": 0.260, "est_ba": 0.200, "ba": 0.190},
        pitcher_mlb_id=999, pitcher_name="Real, Ace", pitcher_hand="R",
        team_code="AAA", opp_team_code="BBB", opp_lineup=[],
        hitter_xstats={}, team_xwoba_fallback=0.320,
        park=DEFAULT_PARK, weather={},
    )
    assert p.source == "statcast"
    assert p.xera == 2.5
    assert p.true_era < 3.0
    assert p.ip == 6.0   # elite pitchers get longer leash


def test_project_high_xera_pitcher_has_short_leash():
    p = project_pitcher(
        pitcher_xstats={"pa": 100, "xera": 6.5, "era": 5.5,
                        "est_woba": 0.380, "est_ba": 0.280, "ba": 0.270},
        pitcher_mlb_id=999, pitcher_name="Bad, Pitcher", pitcher_hand="R",
        team_code="AAA", opp_team_code="BBB", opp_lineup=[],
        hitter_xstats={}, team_xwoba_fallback=0.320,
        park=DEFAULT_PARK, weather={},
    )
    assert p.source == "statcast"
    assert p.ip == 4.5   # short leash
    assert p.k < 5       # bad K rate


def test_project_dome_ignores_weather():
    base = project_pitcher(
        pitcher_xstats={"pa": 100, "xera": 4.0, "era": 4.0,
                        "est_woba": 0.320, "est_ba": 0.245, "ba": 0.245},
        pitcher_mlb_id=1, pitcher_name="X, Y", pitcher_hand="R",
        team_code="A", opp_team_code="B", opp_lineup=[], hitter_xstats={},
        team_xwoba_fallback=0.320, park={**DEFAULT_PARK, "roof_type": "dome"},
        weather={"temp_f": 90, "wind_mph": 30, "wind_deg": 180},
    )
    # Even with hot/windy weather, dome zeroes it out
    assert base.wx_factor == 1.0


def test_project_lineup_weighted_changes_projection():
    """Strong opposing lineup should increase projected hits/ER."""
    lineup = make_lineup(["L"] * 9)
    xstats = make_xstats([1000+i for i in range(9)], xwoba=0.380, pa=200)
    weak = project_pitcher(
        pitcher_xstats={"pa": 100, "xera": 4.0, "era": 4.0,
                        "est_woba": 0.320, "est_ba": 0.245, "ba": 0.245},
        pitcher_mlb_id=1, pitcher_name="X, Y", pitcher_hand="R",
        team_code="A", opp_team_code="B",
        opp_lineup=[], hitter_xstats={}, team_xwoba_fallback=0.320,
        park=DEFAULT_PARK, weather={},
    )
    strong = project_pitcher(
        pitcher_xstats={"pa": 100, "xera": 4.0, "era": 4.0,
                        "est_woba": 0.320, "est_ba": 0.245, "ba": 0.245},
        pitcher_mlb_id=1, pitcher_name="X, Y", pitcher_hand="R",
        team_code="A", opp_team_code="B",
        opp_lineup=lineup, hitter_xstats=xstats, team_xwoba_fallback=0.320,
        park=DEFAULT_PARK, weather={},
    )
    assert strong.er > weak.er
    assert strong.used_actual_lineup is True
    assert weak.used_actual_lineup is False


# ---------- project_game_total ----------

def _proj_helper(ip, er):
    return PitcherProjection(
        pitcher_mlb_id=1, last_first="X, Y", team_code="A", opp_team_code="B",
        hand="R", source="statcast", pa_sample=100,
        era=4.0, xera=4.0, true_era=4.0,
        xwoba_against=0.320, opp_lineup_xwoba=0.320, used_actual_lineup=False,
        ip=ip, outs=ip*3, hits=8.0, er=er, bb=2.0, k=5.0,
        wx_factor=1.0, pf_factor=1.0,
    )


def test_game_total_combines_starters_plus_bullpen():
    away = _proj_helper(ip=6.0, er=2.5)
    home = _proj_helper(ip=6.0, er=2.5)
    full, f5 = project_game_total(away_proj=away, home_proj=home)
    # Starters: 5.0 ER. Bullpen: 6 IP @ 4.0/9 = 2.67 ER. Total ≈ 7.67
    assert 7.0 <= full <= 8.5
    # F5: each starter projected ~ 5/6 of their full ER = 2.08 each → 4.17
    assert 3.5 <= f5 <= 5.0


def test_game_total_short_starts_get_more_bullpen_runs():
    short = _proj_helper(ip=4.0, er=3.0)
    standard = _proj_helper(ip=6.0, er=3.0)
    short_total, _ = project_game_total(away_proj=short, home_proj=short)
    std_total, _ = project_game_total(away_proj=standard, home_proj=standard)
    assert short_total > std_total   # more bullpen exposure = more runs


# ---------- main ----------

if __name__ == "__main__":
    import traceback
    tests = [(n, fn) for n, fn in globals().items()
             if n.startswith("test_") and callable(fn)]
    failed = []
    for name, fn in tests:
        try:
            fn()
            print(f"  ✓  {name}")
        except AssertionError:
            failed.append(name)
            print(f"  ✗  {name}")
            traceback.print_exc()
        except Exception as e:
            failed.append(name)
            print(f"  ✗  {name}  ({type(e).__name__}: {e})")
            traceback.print_exc()
    print(f"\n{len(tests)-len(failed)}/{len(tests)} passed")
    sys.exit(0 if not failed else 1)
