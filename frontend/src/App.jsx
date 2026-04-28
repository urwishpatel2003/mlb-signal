import React, { useState, useEffect } from 'react';

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000';

const TABS = ['Games', 'Pitcher Props', 'Slate', 'Track Record'];

// Map internal category codes to user-facing market labels
const MARKET_LABELS = {
  Total: 'Game Total',
  K:     'Pitcher Strikeouts',
  Hits:  'Pitcher Hits Allowed',
  ER:    'Pitcher Earned Runs',
  Outs:  'Pitcher Outs Recorded',
  BB:    'Pitcher Walks',
};

export default function App() {
  const [tab, setTab] = useState('Games');
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

  // Split edges by kind for tab routing
  const gameTotalEdges = (slate?.edges ?? []).filter(e => e.kind === 'total');
  const pitcherPropEdges = (slate?.edges ?? []).filter(e => e.kind === 'prop');

  return (
    <div className="app">
      <Masthead slate={slate}
                gameCount={gameTotalEdges.length}
                propCount={pitcherPropEdges.length} />
      <nav className="tabs">
        {TABS.map(t => {
          let count = '';
          if (t === 'Games') count = gameTotalEdges.length ? ` (${gameTotalEdges.length})` : '';
          else if (t === 'Pitcher Props') count = pitcherPropEdges.length ? ` (${pitcherPropEdges.length})` : '';
          return (
            <button
              key={t}
              className={`tab ${tab === t ? 'active' : ''}`}
              onClick={() => setTab(t)}>
              {t}{count}
            </button>
          );
        })}
      </nav>

      {loading && <div className="loading">Loading slate</div>}
      {error && <div className="empty">Error: {error}</div>}
      {!loading && !error && slate && (
        <>
          {tab === 'Games' && <EdgesView edges={gameTotalEdges} kind="game" />}
          {tab === 'Pitcher Props' && <EdgesView edges={pitcherPropEdges} kind="prop" />}
          {tab === 'Slate' && <GamesView games={slate.games} projections={slate.projections} />}
          {tab === 'Track Record' && <PerformanceView perf={perf} />}
        </>
      )}

      <footer className="footer">
        <span>QuAInt MLB Signal &middot; Vol II</span>
        <span>Manual review required &middot; Not financial advice</span>
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
        <span>QuAInt &middot; MLB Edition</span>
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

function EdgesView({ edges, kind }) {
  const emptyMsg = kind === 'game'
    ? 'No game total edges flagged tonight.'
    : 'No pitcher prop edges flagged tonight.';

  if (!edges || edges.length === 0) {
    return <div className="empty">{emptyMsg}</div>;
  }

  // Sort: highest conviction first, ties broken by edge magnitude
  const sorted = [...edges].sort((a, b) => {
    const ca = a.conviction_pct ?? -1;
    const cb = b.conviction_pct ?? -1;
    if (cb !== ca) return cb - ca;
    return Math.abs(b.edge) - Math.abs(a.edge);
  });

  const heading = kind === 'game' ? 'Game totals.' : 'Pitcher props.';
  const deck = kind === 'game'
    ? 'Over/under on combined runs scored - 9 innings'
    : 'K, Hits, ER, Outs - flagged where projection vs market diverges';

  return (
    <section>
      <div className="section-header">
        <h2>{heading}</h2>
        <span className="deck">{deck}</span>
      </div>

      <div className="edges-table">
        <div className="edges-thead">
          <span>{kind === 'game' ? 'Matchup' : 'Pitcher'}</span>
          <span>Market</span>
          <span className="num">Line</span>
          <span>Pick</span>
          <span className="num">Projection</span>
          <span>Conviction</span>
        </div>
        <div className="edges-tbody">
          {sorted.map((e, i) => <EdgeRow key={e.edge_id || i} edge={e} />)}
        </div>
      </div>
    </section>
  );
}

function EdgeRow({ edge }) {
  const isProp = edge.kind === 'prop';
  const market = MARKET_LABELS[edge.category] || edge.category;

  const subject = isProp
    ? edge.pitcher_name?.split(',')[0] ?? '—'
    : `${edge.team_code ?? '?'} @ ${edge.opp_team_code ?? '?'}`;

  const subjectSub = isProp
    ? `${edge.team_code ?? ''} v ${edge.opp_team_code ?? ''}`
    : null;

  const conv = edge.conviction_pct;
  const tier = edge.confidence_tier ?? 3;
  const isLowTrust = tier === 3 || conv == null;

  let convBarClass = 'conv-bar';
  if (conv != null) {
    if (conv >= 75) convBarClass += ' conv-strong';
    else if (conv >= 60) convBarClass += ' conv-medium';
    else convBarClass += ' conv-weak';
  }

  return (
    <div className="edge-row">
      <div className="cell-subject">
        <div className="subject-main">{subject}</div>
        {subjectSub && <div className="subject-sub">{subjectSub}</div>}
      </div>
      <div className="cell-market">{market}</div>
      <div className="cell-num">{Number(edge.line).toFixed(1)}</div>
      <div className={`cell-pick lean-${edge.lean}`}>{edge.lean}</div>
      <div className="cell-num cell-proj">{Number(edge.proj_value).toFixed(2)}</div>
      <div className="cell-conviction">
        {isLowTrust ? (
          <span className="conv-na" title="Tier 3 or fallback projection">
            n/a <span className="tier-pill">T{tier}</span>
          </span>
        ) : (
          <div className="conv-cell">
            <span className="conv-value">{Number(conv).toFixed(0)}%</span>
            <div className={convBarClass}>
              <div className="conv-bar-fill" style={{ width: `${Math.min(100, Math.max(0, conv))}%` }} />
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function GamesView({ games, projections }) {
  if (!games || games.length === 0) return <div className="empty">No games scheduled today.</div>;
  const byGame = {};
  (projections || []).forEach(p => {
    if (!byGame[p.game_pk]) byGame[p.game_pk] = [];
    byGame[p.game_pk].push(p);
  });
  return (
    <section>
      <div className="section-header">
        <h2>Tonight's slate.</h2>
        <span className="deck">Probables &middot; projections &middot; market line</span>
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
          <span className="sub">{p.team_code} &middot; {p.hand}HP</span>
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
    return <div className="empty">No performance data yet - first slate hasn't graded yet.</div>;
  }
  return (
    <section>
      <div className="section-header">
        <h2>Track record.</h2>
        <span className="deck">Rolling windows &middot; 7d / 14d / 30d</span>
      </div>
      <div className="perf-grid">
        {perf.map((p, i) => (
          <div key={i} className="perf-card">
            <div className="label">{p.window_days}-Day Window</div>
            <div className={`value ${(p.profit_units ?? 0) >= 0 ? 'pos' : 'neg'}`}>
              {p.wins}-{p.losses}
            </div>
            <div className="sub">
              Hit rate: {((p.hit_rate ?? 0) * 100).toFixed(1)}% &middot;
              ROI: {((p.roi ?? 0) * 100).toFixed(1)}% &middot;
              Profit: {(p.profit_units ?? 0).toFixed(2)} units
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}
