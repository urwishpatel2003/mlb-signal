import React, { useState, useEffect } from 'react';

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000';

const TABS = ['Edges', 'Games', 'Performance'];

export default function App() {
  const [tab, setTab] = useState('Edges');
  const [slate, setSlate] = useState(null);
  const [perf, setPerf] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      setLoading(true);
      try {
        const [slateRes, perfRes] = await Promise.all([
          fetch(`${API_BASE}/api/slate/today`),
          fetch(`${API_BASE}/api/performance/rolling`),
        ]);
        if (!slateRes.ok) throw new Error(`Slate API ${slateRes.status}`);
        const slateData = await slateRes.json();
        const perfData = perfRes.ok ? await perfRes.json() : [];
        if (!cancelled) {
          setSlate(slateData);
          setPerf(perfData);
        }
      } catch (e) {
        if (!cancelled) setError(e.message);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => { cancelled = true; };
  }, []);

  return (
    <div className="app">
      <Masthead slate={slate} />
      <nav className="tabs">
        {TABS.map(t => (
          <button
            key={t}
            className={`tab ${tab === t ? 'active' : ''}`}
            onClick={() => setTab(t)}>
            {t}
          </button>
        ))}
      </nav>

      {loading && <div className="loading">Loading slate</div>}
      {error && <div className="empty">Error: {error}</div>}
      {!loading && !error && slate && (
        <>
          {tab === 'Edges' && <EdgesView edges={slate.edges} />}
          {tab === 'Games' && <GamesView games={slate.games} projections={slate.projections} />}
          {tab === 'Performance' && <PerformanceView perf={perf} />}
        </>
      )}

      <footer className="footer">
        <span>QuAInt MLB Signal · Vol II</span>
        <span>Manual review required · Not financial advice</span>
      </footer>
    </div>
  );
}

function Masthead({ slate }) {
  const today = new Date();
  const dateStr = today.toLocaleDateString('en-US',
    { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' });
  const nGames = slate?.games?.length ?? 0;
  const nEdges = slate?.edges?.length ?? 0;
  const runId = slate?.run?.run_id;
  return (
    <header className="masthead">
      <div className="masthead-top">
        <span>QuAInt · MLB Edition</span>
        <span>{dateStr.toUpperCase()}</span>
      </div>
      <h1>The <span className="em">Signal</span>.</h1>
      <div className="masthead-sub">
        <div className="meta">
          <span>{nGames} GAMES</span>
          <span>{nEdges} EDGES</span>
        </div>
        <span>RUN #{runId ?? '—'}</span>
      </div>
    </header>
  );
}

function EdgesView({ edges }) {
  if (!edges || edges.length === 0) return <div className="empty">No edges flagged on today's slate.</div>;
  return (
    <section>
      <div className="section-header">
        <h2>Edges, ranked.</h2>
        <span className="deck">By magnitude · Tier 1 highest conviction</span>
      </div>
      <div className="edges">
        {edges.map((e, i) => <EdgeRow key={e.edge_id || i} edge={e} rank={i + 1} />)}
      </div>
    </section>
  );
}

function EdgeRow({ edge, rank }) {
  const tier = edge.confidence_tier ?? 3;
  const isProp = edge.kind === 'prop';
  const matchup = isProp
    ? edge.pitcher_name?.split(',')[0] ?? '?'
    : `${edge.team_code ?? ''} @ ${edge.opp_team_code ?? ''}`;
  const sub = isProp
    ? `${edge.team_code} v ${edge.opp_team_code} · ${edge.category}`
    : 'Game total';
  const edgeClass = edge.edge >= 0 ? 'pos' : 'neg';
  return (
    <div className="edge">
      <span className="rank">{String(rank).padStart(2, '0')}</span>
      <div className="label">
        <div className="matchup">{matchup}</div>
        <div className="sub">{sub}</div>
      </div>
      <span className={`badge tier-${tier}`}>T{tier}</span>
      <span className={`lean ${edge.lean}`}>{edge.lean}</span>
      <span className="num line">L {Number(edge.line).toFixed(1)}</span>
      <span className="num proj">→ {Number(edge.proj_value).toFixed(2)}</span>
      <span className={`edge-val ${edgeClass}`}>
        {edge.edge >= 0 ? '+' : ''}{Number(edge.edge).toFixed(2)}
      </span>
    </div>
  );
}

function GamesView({ games, projections }) {
  if (!games || games.length === 0) return <div className="empty">No games scheduled today.</div>;
  // Build a {game_pk: [away_proj, home_proj]} map
  const byGame = {};
  (projections || []).forEach(p => {
    if (!byGame[p.game_pk]) byGame[p.game_pk] = [];
    byGame[p.game_pk].push(p);
  });
  return (
    <section>
      <div className="section-header">
        <h2>Tonight's games.</h2>
        <span className="deck">Probables · projections · edge</span>
      </div>
      <div className="games">
        {games.map(g => (
          <GameCard key={g.game_pk} game={g} projs={byGame[g.game_pk] || []} />
        ))}
      </div>
    </section>
  );
}

function GameCard({ game, projs }) {
  const flag = game.lean === 'OVER' ? 'flagged-over'
              : game.lean === 'UNDER' ? 'flagged-under' : '';
  const awayProj = projs.find(p => p.team_code === game.away_team);
  const homeProj = projs.find(p => p.team_code === game.home_team);
  return (
    <div className={`game-card ${flag}`}>
      <div className="game-header">
        <span className="matchup">{game.away_team} @ {game.home_team}</span>
        <span className="time">{game.game_time_et}</span>
      </div>
      {[awayProj, homeProj].filter(Boolean).map(p => (
        <div key={p.mlb_id} className="pitcher-line">
          <span className="name">{p.last_first?.split(',')[0]}</span>
          <span className="sub">{p.team_code} · {p.hand}HP</span>
          <span className="sub">PA {p.pa_sample}</span>
          <span className="xera">xERA {Number(p.xera || 0).toFixed(2)}</span>
        </div>
      ))}
      <div className="game-totals">
        <div className="stat">
          <div className="label">Market</div>
          <div className="value">{game.market_total ?? '—'}</div>
        </div>
        <div className="stat">
          <div className="label">Proj</div>
          <div className="value">{game.proj_total ? Number(game.proj_total).toFixed(2) : '—'}</div>
        </div>
        <div className="stat edge">
          <div className="label">Edge</div>
          <div className={`value ${game.edge_total >= 0 ? 'pos' : 'neg'}`}>
            {game.edge_total ? (game.edge_total >= 0 ? '+' : '') + Number(game.edge_total).toFixed(2) : '—'}
          </div>
        </div>
      </div>
    </div>
  );
}

function PerformanceView({ perf }) {
  if (!perf || perf.length === 0) {
    return <div className="empty">No performance data yet — grade your first slate to populate.</div>;
  }
  return (
    <section>
      <div className="section-header">
        <h2>Track record.</h2>
        <span className="deck">Rolling windows · 7d / 14d / 30d</span>
      </div>
      <div className="perf-grid">
        {perf.map((p, i) => (
          <div key={i} className="perf-card">
            <div className="label">{p.window_days}-Day Window</div>
            <div className={`value ${(p.profit_units ?? 0) >= 0 ? 'pos' : 'neg'}`}>
              {p.wins}-{p.losses}
            </div>
            <div className="sub">
              Hit rate: {((p.hit_rate ?? 0) * 100).toFixed(1)}% ·
              ROI: {((p.roi ?? 0) * 100).toFixed(1)}% ·
              Profit: {(p.profit_units ?? 0).toFixed(2)} units
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}
