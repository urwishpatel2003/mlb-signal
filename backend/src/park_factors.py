"""
Park factors lookup + seed data.

Park factors are season-specific and updated annually (one-time import in
February before the season). The schema has a `parks` table keyed by
(park_code, season_year); this module provides the seeding helper that
populates it.

Source: 2026 ESPN Fantasy + Statcast park factors. Coors and the Mexico City
series are special-cased for elevation.
"""
from __future__ import annotations
from datetime import date
from typing import Optional
from . import db


# Map team_code → home park_code. For 99% of teams these match (BOS lives
# at Fenway → "BOS"); ATH (Sacramento), TB (Trop), and special-series venues
# can diverge.
TEAM_HOME_PARK = {
    "ARI": "ARI", "ATL": "ATL", "BAL": "BAL", "BOS": "BOS",
    "CHC": "CHC", "CWS": "CWS", "CIN": "CIN", "CLE": "CLE",
    "COL": "COL", "DET": "DET", "HOU": "HOU", "KC":  "KC",
    "LAA": "LAA", "LAD": "LAD", "MIA": "MIA", "MIL": "MIL",
    "MIN": "MIN", "NYY": "NYY", "NYM": "NYM",
    "ATH": "ATH",   # Sutter Health Park (Sacramento)
    "PHI": "PHI", "PIT": "PIT", "SD": "SD",
    "SEA": "SEA", "SF": "SF", "STL": "STL",
    "TB":  "TB", "TEX": "TEX", "TOR": "TOR", "WSH": "WSH",
}


def get_park_for_team(team_code: str) -> Optional[str]:
    return TEAM_HOME_PARK.get(team_code)


# 2026 park metadata. Run-, HR-, K-, BB-factors per ESPN fantasy + Statcast.
# Cf_azimuth is the compass bearing of CF from home plate (used to compute wind
# components for the projection engine).
PARK_SEED = [
    # code, name, city, lat, lon, cf_az, elev, roof, runs, hr, so, bb
    ("ARI", "Chase Field",          "Phoenix",       33.4453, -112.0667,    0, 1086, "retractable", 99, 100, 100, 99),
    ("ATL", "Truist Park",          "Atlanta",       33.8907,  -84.4677,   60, 1050, "open",       101, 102,  99, 101),
    ("BAL", "Camden Yards",         "Baltimore",     39.2839,  -76.6217,   32,    0, "open",       105, 102,  99, 100),
    ("BOS", "Fenway Park",          "Boston",        42.3467,  -71.0972,   45,   21, "open",       105, 100, 100, 100),
    ("CHC", "Wrigley Field",        "Chicago",       41.9484,  -87.6553,   35,  600, "open",       101,  98, 102, 101),
    ("CWS", "Rate Field",           "Chicago",       41.8300,  -87.6338,   33,  595, "open",       103, 105,  98,  99),
    ("CIN", "Great American Ball Park","Cincinnati", 39.0975,  -84.5070,   35,  483, "open",       108, 113,  97, 102),
    ("CLE", "Progressive Field",    "Cleveland",     41.4962,  -81.6852,    1,  660, "open",        99,  98, 100, 100),
    ("COL", "Coors Field",          "Denver",        39.7559, -104.9942,    8, 5183, "open",       125, 115,  95, 102),
    ("DET", "Comerica Park",        "Detroit",       42.3390,  -83.0485,   50,  600, "open",        96,  92, 102, 100),
    ("HOU", "Daikin Park",          "Houston",       29.7572,  -95.3556,   12,   30, "retractable",102, 107,  99,  99),
    ("KC",  "Kauffman Stadium",     "Kansas City",   39.0517,  -94.4803,   65,  886, "open",       102,  91, 100,  99),  # 2026 reduced dimensions
    ("LAA", "Angel Stadium",        "Anaheim",       33.8003, -117.8827,   45,  157, "open",        98,  99, 101, 100),
    ("LAD", "Dodger Stadium",       "Los Angeles",   34.0739, -118.2400,   22,  340, "open",        99, 101, 102, 100),
    ("MIA", "loanDepot park",       "Miami",         25.7781,  -80.2197,   45,    7, "retractable", 96,  93, 102, 100),
    ("MIL", "American Family Field","Milwaukee",     43.0280,  -87.9712,   53,  650, "retractable", 99,  98,  99, 101),
    ("MIN", "Target Field",         "Minneapolis",   44.9817,  -93.2776,   -2,  815, "open",       100, 102,  99, 100),
    ("NYY", "Yankee Stadium",       "Bronx",         40.8296,  -73.9262,   25,   22, "open",       104, 110,  99, 100),
    ("NYM", "Citi Field",           "Queens",        40.7571,  -73.8458,   30,   20, "open",        96,  93, 102,  99),
    ("ATH", "Sutter Health Park",   "Sacramento",    38.5803, -121.5132,   30,   25, "open",       100, 100, 100, 100),  # temporary park
    ("PHI", "Citizens Bank Park",   "Philadelphia",  39.9061,  -75.1665,   35,   20, "open",       109, 110,  99, 100),
    ("PIT", "PNC Park",             "Pittsburgh",    40.4469,  -80.0057,   30,  730, "open",        95,  90, 101,  99),
    ("SD",  "Petco Park",           "San Diego",     32.7073, -117.1566,   12,   15, "open",        93,  88, 103, 100),
    ("SEA", "T-Mobile Park",        "Seattle",       47.5914, -122.3326,   45,   10, "retractable", 95,  92, 102, 100),
    ("SF",  "Oracle Park",          "San Francisco", 37.7786, -122.3893,   90,    0, "open",        92,  88, 103, 100),
    ("STL", "Busch Stadium",        "St. Louis",     38.6226,  -90.1928,   27,  466, "open",        97,  93, 100, 100),
    ("TB",  "Tropicana Field",      "St. Petersburg",27.7682,  -82.6534,   45,   15, "dome",        96,  94, 102,  99),
    ("TEX", "Globe Life Field",     "Arlington",     32.7475,  -97.0822,    6,  551, "retractable",103, 107,  98, 100),
    ("TOR", "Rogers Centre",        "Toronto",       43.6414,  -79.3894,    0,  300, "retractable",105, 105,  99, 100),
    ("WSH", "Nationals Park",       "Washington",    38.8730,  -77.0074,   24,   26, "open",       102, 105, 100, 101),
]


def seed_parks(season_year: int) -> int:
    """Idempotent. Insert one row per park for the given season."""
    rows = [
        {
            "park_code": p[0], "name": p[1], "city": p[2],
            "lat": p[3], "lon": p[4], "cf_azimuth_deg": p[5],
            "elevation_ft": p[6], "roof_type": p[7],
            "pf_runs": p[8], "pf_hr": p[9], "pf_so": p[10], "pf_bb": p[11],
            "season_year": season_year,
        }
        for p in PARK_SEED
    ]
    sql = """
        INSERT INTO parks (
          park_code, name, city, lat, lon, cf_azimuth_deg, elevation_ft,
          roof_type, pf_runs, pf_hr, pf_so, pf_bb, season_year
        ) VALUES (
          %(park_code)s, %(name)s, %(city)s, %(lat)s, %(lon)s,
          %(cf_azimuth_deg)s, %(elevation_ft)s, %(roof_type)s,
          %(pf_runs)s, %(pf_hr)s, %(pf_so)s, %(pf_bb)s, %(season_year)s
        )
        ON CONFLICT (park_code, season_year) DO UPDATE SET
          name = EXCLUDED.name,
          pf_runs = EXCLUDED.pf_runs,
          pf_hr = EXCLUDED.pf_hr,
          pf_so = EXCLUDED.pf_so,
          pf_bb = EXCLUDED.pf_bb,
          updated_at = now();
    """
    return db.execute_many(sql, rows)


# Team seed (for the teams table)
TEAM_SEED = [
    ("ARI", "Arizona Diamondbacks",  "NL", "West",    109, "ARI"),
    ("ATL", "Atlanta Braves",        "NL", "East",    144, "ATL"),
    ("BAL", "Baltimore Orioles",     "AL", "East",    110, "BAL"),
    ("BOS", "Boston Red Sox",        "AL", "East",    111, "BOS"),
    ("CHC", "Chicago Cubs",          "NL", "Central", 112, "CHC"),
    ("CWS", "Chicago White Sox",     "AL", "Central", 145, "CWS"),
    ("CIN", "Cincinnati Reds",       "NL", "Central", 113, "CIN"),
    ("CLE", "Cleveland Guardians",   "AL", "Central", 114, "CLE"),
    ("COL", "Colorado Rockies",      "NL", "West",    115, "COL"),
    ("DET", "Detroit Tigers",        "AL", "Central", 116, "DET"),
    ("HOU", "Houston Astros",        "AL", "West",    117, "HOU"),
    ("KC",  "Kansas City Royals",    "AL", "Central", 118, "KC"),
    ("LAA", "Los Angeles Angels",    "AL", "West",    108, "LAA"),
    ("LAD", "Los Angeles Dodgers",   "NL", "West",    119, "LAD"),
    ("MIA", "Miami Marlins",         "NL", "East",    146, "MIA"),
    ("MIL", "Milwaukee Brewers",     "NL", "Central", 158, "MIL"),
    ("MIN", "Minnesota Twins",       "AL", "Central", 142, "MIN"),
    ("NYY", "New York Yankees",      "AL", "East",    147, "NYY"),
    ("NYM", "New York Mets",         "NL", "East",    121, "NYM"),
    ("ATH", "Athletics",             "AL", "West",    133, "ATH"),
    ("PHI", "Philadelphia Phillies", "NL", "East",    143, "PHI"),
    ("PIT", "Pittsburgh Pirates",    "NL", "Central", 134, "PIT"),
    ("SD",  "San Diego Padres",      "NL", "West",    135, "SD"),
    ("SEA", "Seattle Mariners",      "AL", "West",    136, "SEA"),
    ("SF",  "San Francisco Giants",  "NL", "West",    137, "SF"),
    ("STL", "St. Louis Cardinals",   "NL", "Central", 138, "STL"),
    ("TB",  "Tampa Bay Rays",        "AL", "East",    139, "TB"),
    ("TEX", "Texas Rangers",         "AL", "West",    140, "TEX"),
    ("TOR", "Toronto Blue Jays",     "AL", "East",    141, "TOR"),
    ("WSH", "Washington Nationals",  "NL", "East",    120, "WSH"),
]


def seed_teams() -> int:
    rows = [
        {"team_code": t[0], "full_name": t[1], "league": t[2],
         "division": t[3], "mlb_id": t[4], "home_park_code": t[5]}
        for t in TEAM_SEED
    ]
    sql = """
        INSERT INTO teams (team_code, full_name, league, division, mlb_id, home_park_code)
        VALUES (%(team_code)s, %(full_name)s, %(league)s, %(division)s, %(mlb_id)s, %(home_park_code)s)
        ON CONFLICT (team_code) DO UPDATE SET
          full_name = EXCLUDED.full_name,
          league = EXCLUDED.league,
          division = EXCLUDED.division,
          mlb_id = EXCLUDED.mlb_id,
          home_park_code = EXCLUDED.home_park_code,
          updated_at = now();
    """
    return db.execute_many(sql, rows)
