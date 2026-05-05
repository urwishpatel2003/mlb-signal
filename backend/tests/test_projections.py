"""
Test suite for projections.py v3.0

Run with: pytest backend/tests
or:       python -m tests.test_projections

29 pure-function tests covering all 7 improvements. No DB/network required.
"""
import sys, os, math
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.projections import (
    HitterSpot, PitcherProjection,
    opp_lineup_xwoba, project_pitcher, project_game_total,
    temp_run_factor, wind_run_factor,
    _continuous_ip_leash, _platoon_factor,
    PLATOON_XWOBA, PA_WEIGHTS, LEAGUE_XWOBA, LEAGUE_BB9,
)


def approx(a, b, tol=0.01):
    return abs(a - b) < tol


# =============================================================================
# temp_run_factor
# =============================================================================

def test_temp_factor_neutral_at_70():
    assert approx(temp_run_factor(70), 1.0)

def test_temp_factor_higher_at_warm():
    assert temp_run_factor(85) > 1.0

def test_temp_factor_lower_at_cold():
    assert temp_run_factor(50) < 1.0

def test_temp_factor_handles_none():
    assert temp_run_factor(None) == 1.0


# =============================================================================
# wind_run_factor
# =============================================================================

def test_wind_neutral_when_no_wind():
    assert approx(wind_run_factor(0, 90, 0), 1.0)

def test_wind_neutral_when_dir_unknown():
    assert approx(wind_run_factor(20, None, 0), 1.0)

def test_wind_blowing_out_increases_runs():
    assert wind_run_factor(15, 180, 0) > 1.0

def test_wind_blowing_in_decreases_runs():
    assert wind_run_factor(15, 0, 0) < 1.0

def test_wind_crosswind_neutral_to_slight_negative():
    factor = wind_run_factor(15, 90, 0)
    assert factor < 1.0


# =============================================================================
# Improvement #2 — Continuous IP leash
# =============================================================================

def test_continuous_leash_elite():
    """ERA 2.50 → near 7.0 (capped)"""
    assert _continuous_ip_leash(2.50) == 7.0

def test_continuous_leash_average():
    """ERA 4.30 → ~6.2"""
    ip = _continuous_ip_leash(4.30)
    assert 6.0 <= ip <= 6.5

def test_continuous_leash_bad():
    """ERA 6.50 → ~4.5"""
    ip = _continuous_ip_leash(6.50)
    assert 4.0 <= ip <= 5.0

def test_continuous_leash_floor():
    """Very bad ERA → floor at 4.0"""
    assert _continuous_ip_leash(10.0) == 4.0

def test_continuous_leash_smooth():
    """No cliffs — monotonically decreasing as ERA rises"""
    eras = [2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0]
    ips  = [_continuous_ip_leash(e) for e in eras]
    assert all(ips[i] >= ips[i+1] for i in range(len(ips)-1))


# =============================================================================
# Improvement #6 — Per-hitter platoon splits
# =============================================================================

def test_platoon_uses_per_hitter_split_when_enough_pa():
    """When ≥80 PA from a side, per-hitter split overrides league mult."""
    # Hitter dominates RHP (high xwOBA vs R)
    splits = {"R": {"est_woba": 0.400, "pa": 120}}
    factor = _platoon_factor("L", "R", splits)
    # Should reflect 0.400 / 0.320 = 1.25, not the league LvR = 1.04
    assert factor > 1.20

def test_platoon_falls_back_to_league_mult_when_low_pa():
    """< 80 PA → falls back to PLATOON_XWOBA multiplier."""
    splits = {"R": {"est_woba": 0.400, "pa": 50}}
    factor = _platoon_factor("L", "R", splits)
    assert approx(factor, PLATOON_XWOBA[("L", "R")], tol=0.005)

def test_platoon_falls_back_when_splits_none():
    factor = _platoon_factor("R", "R", None)
    assert approx(factor, PLATOON_XWOBA[("R", "R")], tol=0.005)


# =============================================================================
# opp_lineup_xwoba — includes Improvements #5 + #6
# =============================================================================

def make_lineup(hands_by_order: list[str]) -> list[HitterSpot]:
    return [
        HitterSpot(mlb_id=1000+i, last_first=f"Test{i}, Player",
                   bat_side=h, order=i+1)
        for i, h in enumerate(hands_by_order)
    ]

def make_xstats(mlb_ids: list[int], xwoba: float = 0.330, pa: int = 50,
                l15_woba: float = None) -> dict:
    row = {"est_woba": xwoba, "pa": pa}
    if l15_woba is not None:
        row["l15_woba"] = l15_woba
    return {pid: row for pid in mlb_ids}

def test_opp_xwoba_falls_back_when_lineup_empty():
    xwoba, used, l15 = opp_lineup_xwoba([], "R", {}, 0.310)
    assert xwoba == 0.310
    assert used is False
    assert l15 is False

def test_opp_xwoba_falls_back_when_too_few_hitters():
    lineup = make_lineup(["R"] * 9)
    xstats = make_xstats([1000, 1001, 1002])
    xwoba, used, l15 = opp_lineup_xwoba(lineup, "R", xstats, 0.310)
    assert used is False
    assert xwoba == 0.310

def test_opp_xwoba_uses_lineup_when_enough_data():
    lineup = make_lineup(["R"] * 9)
    xstats = make_xstats([1000+i for i in range(9)], xwoba=0.350, pa=100)
    xwoba, used, _ = opp_lineup_xwoba(lineup, "R", xstats, 0.310)
    assert used is True
    expected = 0.350 * PLATOON_XWOBA[("R", "R")]
    assert approx(xwoba, expected, tol=0.005)

def test_opp_xwoba_l15_blend_moves_value():
    """Improvement #5: hot hitter (l15 > season) should push xwOBA up."""
    lineup = make_lineup(["R"] * 9)
    xstats_no_l15  = make_xstats([1000+i for i in range(9)], xwoba=0.320, pa=100)
    xstats_hot_l15 = make_xstats([1000+i for i in range(9)], xwoba=0.320, pa=100,
                                   l15_woba=0.380)
    base, _, no_l15_flag   = opp_lineup_xwoba(lineup, "R", xstats_no_l15,  0.310)
    hot,  _, yes_l15_flag  = opp_lineup_xwoba(lineup, "R", xstats_hot_l15, 0.310)
    assert hot > base
    assert no_l15_flag is False
    assert yes_l15_flag is True

def test_opp_xwoba_cold_l15_lowers_value():
    """Cold hitter (l15 < season) should push xwOBA down."""
    lineup = make_lineup(["R"] * 9)
    xstats_cold = make_xstats([1000+i for i in range(9)], xwoba=0.320, pa=100,
                               l15_woba=0.260)
    base,  _, _ = opp_lineup_xwoba(lineup, "R",
                                    make_xstats([1000+i for i in range(9)], xwoba=0.320, pa=100),
                                    0.310)
    cold,  _, _ = opp_lineup_xwoba(lineup, "R", xstats_cold, 0.310)
    assert cold < base


# =============================================================================
# project_pitcher — spot-checks for all improvements
# =============================================================================

DEFAULT_PARK = {
    "pf_runs": 100, "pf_so": 100, "pf_bb": 100,
    "cf_azimuth_deg": 0, "roof_type": "open",
}

def _pitcher(xstats, **kwargs):
    defaults = dict(
        pitcher_mlb_id=999, pitcher_name="Test, Pitcher", pitcher_hand="R",
        team_code="AAA", opp_team_code="BBB", opp_lineup=[],
        hitter_xstats={}, team_xwoba_fallback=0.320,
        park=DEFAULT_PARK, weather={},
    )
    defaults.update(kwargs)
    return project_pitcher(pitcher_xstats=xstats, **defaults)

def test_source_league_avg_for_no_data():
    p = _pitcher(None)
    assert p.source == "league_avg"
    assert p.xfip is None

def test_source_low_sample_for_under_threshold():
    p = _pitcher({"pa": 15, "xera": 3.0, "era": 2.5, "est_woba": 0.270,
                  "est_ba": 0.210, "ba": 0.200})
    assert p.source == "low_sample"

def test_source_statcast_for_well_sampled():
    p = _pitcher({"pa": 150, "xera": 2.5, "era": 2.2, "xfip": 2.8,
                  "est_woba": 0.260, "est_ba": 0.200, "ba": 0.190})
    assert p.source == "statcast"
    assert p.xfip == 2.8

# Improvement #1: xFIP in blend
def test_xfip_blend_lowers_true_era_vs_bad_era():
    """A pitcher with great xFIP/xERA but elevated ERA should get credit."""
    with_xfip    = _pitcher({"pa": 150, "xera": 2.8, "era": 4.5, "xfip": 3.0,
                              "est_woba": 0.290, "est_ba": 0.220, "ba": 0.250})
    without_xfip = _pitcher({"pa": 150, "xera": 2.8, "era": 4.5,
                              "est_woba": 0.290, "est_ba": 0.220, "ba": 0.250})
    # With xFIP, true_era should be lower (xFIP good, pulls it down vs raw ERA)
    assert with_xfip.true_era < without_xfip.true_era

# Improvement #2: continuous leash — no cliff at ERA 3.0
def test_ip_continuous_no_cliff_at_3():
    below = _pitcher({"pa": 150, "xera": 2.9, "era": 2.9, "est_woba": 0.290,
                      "est_ba": 0.215, "ba": 0.215})
    above = _pitcher({"pa": 150, "xera": 3.1, "era": 3.1, "est_woba": 0.310,
                      "est_ba": 0.240, "ba": 0.240})
    # Should be close, not a jump of 0.5 IP
    assert abs(below.ip - above.ip) < 0.3

# Improvement #3: BB/9 from real data
def test_bb9_real_data_used_over_heuristic():
    """Pitcher with known low BB/9 should project fewer walks than heuristic."""
    with_bb9    = _pitcher({"pa": 200, "xera": 4.0, "era": 4.0, "bb9": 1.5,
                             "est_woba": 0.320, "est_ba": 0.245, "ba": 0.245})
    without_bb9 = _pitcher({"pa": 200, "xera": 4.0, "era": 4.0,
                             "est_woba": 0.320, "est_ba": 0.245, "ba": 0.245})
    # Low bb9 pitcher should walk fewer
    assert with_bb9.bb < without_bb9.bb

def test_bb9_high_walk_rate_inflates_bb():
    """High BB/9 pitcher should project more walks than league-avg heuristic."""
    with_high_bb9 = _pitcher({"pa": 200, "xera": 4.0, "era": 4.0, "bb9": 5.5,
                               "est_woba": 0.320, "est_ba": 0.245, "ba": 0.245})
    baseline      = _pitcher({"pa": 200, "xera": 4.0, "era": 4.0,
                               "est_woba": 0.320, "est_ba": 0.245, "ba": 0.245})
    assert with_high_bb9.bb > baseline.bb

# Improvement #4: K% from real data
def test_k_pct_sinkerball_pitcher_low_k():
    """Sinkerball pitcher (low k_pct, ok xwOBA) should project fewer Ks."""
    sinker = _pitcher({"pa": 200, "xera": 3.8, "era": 3.8, "k_pct": 0.155,
                       "est_woba": 0.310, "est_ba": 0.240, "ba": 0.240})
    # xwOBA-only model would have given ~league K rate; real k_pct should be lower
    assert sinker.k < 5.0   # 5 IP × (0.155 × 37.5/9) ~ 3.6 K

def test_k_pct_power_pitcher_high_k():
    pitcher = _pitcher({"pa": 200, "xera": 2.8, "era": 2.8, "k_pct": 0.320,
                        "est_woba": 0.265, "est_ba": 0.200, "ba": 0.200})
    assert pitcher.k > 7.0

# Improvement #7: high-variance flag
def test_high_variance_flag_set_for_bad_pitcher():
    p = _pitcher({"pa": 100, "xera": 5.5, "era": 5.5,
                  "est_woba": 0.370, "est_ba": 0.270, "ba": 0.270})
    assert p.high_variance_flag is True

def test_high_variance_ip_haircut():
    """High-variance starter should have fewer IP than continuous leash alone."""
    bad = _pitcher({"pa": 100, "xera": 5.5, "era": 5.5,
                    "est_woba": 0.370, "est_ba": 0.270, "ba": 0.270})
    # Without haircut: _continuous_ip_leash(~5.5) ≈ 5.8
    # With 12% haircut: ~5.1
    raw_leash = _continuous_ip_leash(bad.true_era / 0.88)   # reverse-engineer
    assert bad.ip < raw_leash

def test_good_pitcher_not_high_variance():
    p = _pitcher({"pa": 150, "xera": 2.8, "era": 2.8,
                  "est_woba": 0.270, "est_ba": 0.205, "ba": 0.205})
    assert p.high_variance_flag is False

def test_dome_ignores_weather():
    p = _pitcher(
        {"pa": 100, "xera": 4.0, "era": 4.0,
         "est_woba": 0.320, "est_ba": 0.245, "ba": 0.245},
        park={**DEFAULT_PARK, "roof_type": "dome"},
        weather={"temp_f": 95, "wind_mph": 30, "wind_deg": 180},
    )
    assert p.wx_factor == 1.0


# =============================================================================
# project_game_total — Improvement #7 bullpen padding
# =============================================================================

def _proj_helper(ip, er, true_era=4.0, high_variance=False):
    return PitcherProjection(
        pitcher_mlb_id=1, last_first="X, Y", team_code="A", opp_team_code="B",
        hand="R", source="statcast", pa_sample=100,
        era=true_era, xera=true_era, xfip=true_era, true_era=true_era,
        xwoba_against=0.320, opp_lineup_xwoba=0.320,
        used_actual_lineup=False, used_l15_blend=False,
        ip=ip, outs=ip*3, hits=8.0, er=er, bb=2.0, k=5.0,
        wx_factor=1.0, pf_factor=1.0,
        high_variance_flag=high_variance,
    )

def test_game_total_combines_starters_plus_bullpen():
    away = _proj_helper(ip=6.0, er=2.5)
    home = _proj_helper(ip=6.0, er=2.5)
    full, f5, _, _ = project_game_total(away_proj=away, home_proj=home)
    assert 7.0 <= full <= 8.5
    assert 3.5 <= f5  <= 5.0

def test_high_variance_starters_inflate_total():
    """Improvement #7: high-variance pitchers should produce higher totals."""
    normal = _proj_helper(ip=5.5, er=3.0, high_variance=False)
    risky  = _proj_helper(ip=5.5, er=3.0, high_variance=True)
    norm_total, _, _, _ = project_game_total(away_proj=normal, home_proj=normal)
    risk_total, _, _, _ = project_game_total(away_proj=risky,  home_proj=risky)
    assert risk_total > norm_total

def test_short_starts_more_bullpen():
    short    = _proj_helper(ip=4.0, er=3.0)
    standard = _proj_helper(ip=6.0, er=3.0)
    short_t, _, _, _ = project_game_total(away_proj=short, home_proj=short)
    std_t,   _, _, _ = project_game_total(away_proj=standard, home_proj=standard)
    assert short_t > std_t


# =============================================================================
# Runner
# =============================================================================

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
    import sys; sys.exit(0 if not failed else 1)
