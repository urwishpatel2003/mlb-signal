# mlb-signal

End-to-end automated MLB betting model. Pulls Statcast, lineups, weather, and
odds, projects every starter, computes edges, and pushes notifications when
new picks are ready. Manual review gate before placing bets.

Built as a sibling system to QuAInt Signal, sharing the same Railway/Postgres/
React-Vite stack.

## Track record going in

- Apr 25–27 manual backtests (3 days, 14 flagged plays): **12-2 on game totals (86%)**
- Pitcher props: 7-5 across confirmed lines
- Combined 19-7 (73%) at +27% ROI on 26 plays
- Calibration: model MAE 3.42 vs market MAE 4.06 on Apr 27

These are too few plays for statistical significance (need 50+). The framework's
strongest signal is **xStats divergence on starters in neutral environments**,
which has produced clean wins (Flaherty Apr 25, Nola Apr 26).

## Architecture

```
                ┌────────────── Railway cron ──────────────┐
                ▼                                            │
       ┌────────────────┐                                    │
       │  Orchestrator  │  uses MLB Stats API, Odds API,    │
       │  (every 30min) │  NWS, pybaseball                  │
       └────────┬───────┘                                    │
                │                                            │
                ▼                                            │
       ┌────────────────┐                                    │
       │   Postgres     │  ◀──── single source of truth     │
       │  (Supabase)    │                                    │
       └────────┬───────┘                                    │
                │                                            │
       ┌────────┴────────────┐                               │
       ▼                     ▼                               │
  ┌─────────┐          ┌─────────┐         ┌─────────┐      │
  │ FastAPI │          │  ntfy   │         │ git JSON │     │
  │ + React │          │  push   │         │ archive  │     │
  └─────────┘          └─────────┘         └─────────┘      │
       ▲                                                     │
       │ manual review                                       │
       │ before betting                                      │
       └────────────────────────────────────────────────────┘
```

## Deploy on Railway (one-time setup)

### 1. Create the Railway project
```
railway init mlb-signal
railway link        # link your local repo to the project
```

### 2. Add the Postgres plugin
```
railway add --plugin postgres
```
This auto-injects `DATABASE_URL` into your service environment.

### 3. Set required env vars
In the Railway dashboard → service → Variables:
```
ODDS_API_KEY     = <get from https://the-odds-api.com>
NTFY_TOPIC       = mlb-signal-uvi-<random-suffix>     # private topic
ADMIN_TOKEN      = <random 32-char string>
ALLOWED_ORIGINS  = https://your-frontend.vercel.app
```

### 4. Deploy
```
railway up
```
Railway picks up `backend/Dockerfile` and `railway.toml` automatically. The
crons will start running on the schedule defined in `railway.toml`.

### 5. Bootstrap the database
After first deploy, shell into the container or run via Railway CLI:
```
railway run python -m scripts.bootstrap
```
This applies migrations and seeds the `teams` and `parks` tables.

### 6. Trigger the first orchestrator run manually
```
railway run python -m src.orchestrator --trigger=initial
```
Or hit the API:
```
curl -X POST https://your-service.up.railway.app/api/admin/run-now \
     -H "X-Admin-Token: $ADMIN_TOKEN"
```

### 7. Subscribe to your ntfy topic
In the ntfy mobile app or web client:
```
https://ntfy.sh/mlb-signal-uvi-<random-suffix>
```
You'll get a push every time the orchestrator finishes a run.

## Frontend deploy (Vercel)

```
cd frontend
vercel
```

Set the env var `VITE_API_BASE` to the Railway service URL during the Vercel
deploy.

## Cron schedule

| Time (ET) | Job | What it does |
|---|---|---|
| 06:00 | `statcast_refresh` | Pulls latest pitcher/hitter xStats from pybaseball |
| 09:00 | `orchestrator (morning)` | First projection pass (probables only) |
| 11:00–19:00 every 30min | `orchestrator (line_watcher)` | Re-runs as lineups confirm, lines move |
| 04:00 next day | `grader` | Pulls box scores, scores predictions, updates rolling perf |

## Local dev

```
# Backend
cd backend
pip install -r requirements.txt
export DATABASE_URL=postgres://localhost/mlb_signal
python -m scripts.bootstrap
uvicorn src.api:app --reload

# Frontend (separate terminal)
cd frontend
npm install
npm run dev
```

Visit http://localhost:5173.

## Tests

```
cd backend
python -m tests.test_projections       # 22 tests, all pure-function (no DB/network)
```

## Files

```
mlb_signal/
├── backend/
│   ├── src/
│   │   ├── mlb_api.py         MLB Stats API client (schedule/lineups/box)
│   │   ├── projections.py     Projection engine (lineup-weighted xwOBA + platoon)
│   │   ├── orchestrator.py    Daily pipeline runner
│   │   ├── grader.py          Nightly grader + rolling performance
│   │   ├── statcast_refresh.py Daily Statcast pull via pybaseball
│   │   ├── odds.py            The Odds API integration
│   │   ├── weather.py         NWS + Open-Meteo client
│   │   ├── park_factors.py    Park metadata + seed data
│   │   ├── ntfy.py            Push notifications
│   │   ├── db.py              Postgres pool + repository helpers
│   │   └── api.py             FastAPI app
│   ├── migrations/
│   │   └── 0001_initial_schema.sql
│   ├── scripts/
│   │   └── bootstrap.py       One-time migration + seeding
│   ├── tests/
│   │   └── test_projections.py
│   ├── requirements.txt
│   └── Dockerfile
├── frontend/
│   ├── src/
│   │   ├── App.jsx           React dashboard
│   │   ├── main.jsx          Entry point
│   │   └── styles.css        Editorial broadsheet aesthetic
│   ├── index.html
│   ├── vite.config.js
│   └── package.json
└── railway.toml              Railway service + cron config
```

## Key model assumptions

These are baked into `src/projections.py` and should be revisited if backtest
performance drifts:

- **True ERA blend**: `0.7 × xERA + 0.3 × ERA` (xStats-weighted)
- **Platoon multipliers** (PLATOON_XWOBA): `LvL=0.93, LvR=1.04, RvR=0.97, RvL=1.05`
- **Leash by quality**: <3.0 true_era → 6.0 IP; >5.5 → 4.5 IP; else 5.5
- **Bullpen ER9**: 4.0 (could be team-specific later)
- **Edge thresholds**: Total ≥0.5, K ≥0.5, Hits ≥0.7, ER ≥0.5, Outs ≥0.7
- **Confidence tiers**: T1 = high-conviction (≥1.5 runs / well-sampled),
  T2 = medium (≥1.0), T3 = low / uses fallback projection

## What this system does NOT do

- Place bets automatically (manual review gate per your preference)
- Track ATS / spread bets (totals + pitcher props only for now)
- Player props for hitters (HR, hits, total bases) — could add later
- Live in-game adjustments

## Operational notes

- **MLB Stats API is the single source of truth.** No more RotoWire scraping.
  Lineups become available 2-3 hours before first pitch and are picked up by
  the next 30-min orchestrator run.
- **Odds API free tier = 500 requests/month.** That's ~16/day, plenty for our
  4-5 daily orchestrator runs.
- **NWS works in-region** (US parks). Open-Meteo handles Mexico City, Tokyo,
  and any future international series.
- **Statcast refresh runs daily at 06:00 ET.** Manual CSV uploads are no longer
  required.
- **Every projection run is INSERT-only.** History is never overwritten;
  rolling performance is calculable across any window.

## Calibration & monitoring

`/api/performance/rolling` returns 7/14/30-day windows showing:
- Hit rate
- ROI at -110
- Model MAE vs Market MAE (calibration)
- Profit in units

If model MAE drifts to within 0.1 of market MAE for 30+ days, the framework
has lost its edge and should be re-examined. The Apr 25-27 backtest had
model beating market by 0.4 MAE consistently.
