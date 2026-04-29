import React, { useState, useEffect } from 'react';

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000';

const TABS = ['Games', 'Pitcher Props', 'Pitchers', 'Slate', 'Track Record'];

const MARKET_LABELS = {
  Total: 'Game Total',
  K:     'Pitcher Strikeouts',
  Hits:  'Pitcher Hits Allowed',
  ER:    'Pitcher Earned Runs',
  Outs:  'Pitcher Outs Recorded',
  BB:    'Pitcher Walks',
};

const SOURCE_LABELS = {
  statcast:    { label: 'Statcast', cls: 'src-statcast' },
  low_sample:  { label: 'Low Sample', cls: 'src-low' },
  league_avg:  { label: 'League Avg', cls: 'src-league' },
};

const PITCHER_COLUMNS = [
  { key: 'last_first',         label: 'Pitcher',     align: 'left',  type: 'string' },
  { key: 'team_code',          label: 'Team',        align: 'ctr',   type: 'string' },
  { key: 'hand',               label: 'Hand',        align: 'ctr',   type: 'string' },
  { key: 'opp_team_code',      label: 'vs',          align: 'ctr',   type: 'string' },
  { key: 'pa_sample',          label: 'PA',          align: 'num',   type: 'number' },
  { key: 'era',                label: 'ERA',         align: 'num',   type: 'number' },
  { key: 'xera',               label: 'xERA',        align: 'num',   type: 'number' },
  { key: 'true_era',           label: 'tERA',        align: 'num',   type: 'number' },
  { key: 'xwoba_against',      label: 'xwOBA',       align: 'num',   type: 'number' },
  { key: 'opp_lineup_xwoba',   label: 'Opp xwOBA',   align: 'num',   type: 'number' },
  { key: 'ip',                 label: 'IP',          align: 'num',   type: 'number' },
  { key: 'k',                  label: 'K',           align: 'num',   type: 'number' },
  { key: 'bb',                 label: 'BB',          align: 'num',   type: 'number' },
  { key: 'hits',               label: 'H',           align: 'num',   type: 'number' },
  { key: 'er',                 label: 'ER',          align: 'num',   type: 'number' },
  { key: 'outs',               label: 'Outs',        align: 'num',   type: 'number' },
  { key: 'pf_factor',          label: 'PF',          align: 'num',   type: 'number' },
  { key: 'wx_factor',          label: 'Wx',          align: 'num',   type: 'number' },
  { key: 'source',             label: 'Source',      align: 'left',  type: 'string' },
];

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
        const [slateRes, overallRes, byDateRes] = await Promise.all([
          fetch(`${API_BASE}/api/slate/today`),
          fetch(`${API_BASE}/api/performance/overall`),
          fetch(`${API_BASE}/api/performance/by-date`),
        ]);
        if (!slateRes.ok) throw new Error(`Slate API ${slateRes.status}`);
        const slateData = await slateRes.json();
        const overallData = overallRes.ok ? await overallRes.json() : null;
        const byDateData = byDateRes.ok ? await byDateRes.json() : [];
        if (!cancelled) {
          setSlate(slateData);
          setPerf({ overall: overallData, byDate: byDateData });
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
          else if (t === 'Pitchers') count = slate?.projections?.length ? ` (${slate.projections.length})` : '';
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
          {tab === 'Pitchers' && <PitchersView projections={slate.projections} games={slate.games} />}
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
        <span>RUN #{runId ?? '-'}</span>
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
    ? edge.pitcher_name?.split(',')[0] ?? '-'
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

function PitchersView({ projections, games }) {
  const [sortKey, setSortKey] = useState('__default');
  const [sortDir, setSortDir] = useState('asc');

  if (!projections || projections.length === 0) {
    return <div className="empty">No pitcher projections yet for tonight's slate.</div>;
  }

  const gameTime = {};
  (games || []).forEach(g => { gameTime[g.game_pk] = g.game_time_et || ''; });

  function handleHeaderClick(colKey) {
    if (sortKey === colKey) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc');
    } else {
      const col = PITCHER_COLUMNS.find(c => c.key === colKey);
      setSortKey(colKey);
      setSortDir(col?.type === 'number' ? 'desc' : 'asc');
    }
  }

  function getSortValue(p, key) {
    if (key === '__default') {
      const gt = gameTime[p.game_pk] || '99:99';
      return `${gt}|${p.last_first || ''}`;
    }
    const v = p[key];
    if (v == null) return null;
    return v;
  }

  const sorted = [...projections].sort((a, b) => {
    const va = getSortValue(a, sortKey);
    const vb = getSortValue(b, sortKey);
    if (va == null && vb == null) return 0;
    if (va == null) return 1;
    if (vb == null) return -1;
    let cmp;
    if (typeof va === 'number' && typeof vb === 'number') {
      cmp = va - vb;
    } else {
      cmp = String(va).localeCompare(String(vb));
    }
    return sortDir === 'asc' ? cmp : -cmp;
  });

  return (
    <section>
      <div className="section-header">
        <h2>Pitcher projections.</h2>
        <span className="deck">Click any column to sort &middot; lineup-weighted xwOBA, park factor, weather</span>
      </div>

      <div className="pitchers-table">
        <div className="pitchers-thead">
          {PITCHER_COLUMNS.map(col => {
            const isActive = sortKey === col.key;
            const arrow = isActive ? (sortDir === 'asc' ? ' ▲' : ' ▼') : '';
            const alignCls = col.align === 'num' ? 'num' : col.align === 'ctr' ? 'ctr' : '';
            return (
              <button
                key={col.key}
                className={`pitchers-th ${alignCls} ${isActive ? 'active' : ''}`}
                onClick={() => handleHeaderClick(col.key)}>
                {col.label}{arrow}
              </button>
            );
          })}
        </div>
        <div className="pitchers-tbody">
          {sorted.map((p, i) => <PitcherRow key={p.mlb_id || p.pitcher_mlb_id || i} p={p} />)}
        </div>
      </div>
    </section>
  );
}

function PitcherRow({ p }) {
  const sourceInfo = SOURCE_LABELS[p.source] || { label: p.source || '?', cls: 'src-unknown' };
  const fmt = (v, d = 2) => v == null ? '-' : Number(v).toFixed(d);
  const fmt0 = v => v == null ? '-' : Math.round(Number(v)).toString();

  const oppLabel = p.used_actual_lineup
    ? <span title="Lineup-confirmed">{fmt(p.opp_lineup_xwoba, 3)}</span>
    : <span title="Team aggregate (lineup not yet posted)" className="proj-tentative">{fmt(p.opp_lineup_xwoba, 3)}*</span>;

  return (
    <div className="pitcher-row">
      <div className="cell-pitcher-name">{p.last_first?.split(',')[0] ?? '-'}</div>
      <div className="cell-ctr">{p.team_code ?? '-'}</div>
      <div className="cell-ctr">{p.hand ?? '-'}</div>
      <div className="cell-ctr">{p.opp_team_code ?? '-'}</div>
      <div className="cell-num">{fmt0(p.pa_sample)}</div>
      <div className="cell-num">{fmt(p.era)}</div>
      <div className="cell-num">{fmt(p.xera)}</div>
      <div className="cell-num">{fmt(p.true_era)}</div>
      <div className="cell-num">{fmt(p.xwoba_against, 3)}</div>
      <div className="cell-num">{oppLabel}</div>
      <div className="cell-num">{fmt(p.ip, 1)}</div>
      <div className="cell-num cell-proj">{fmt(p.k, 1)}</div>
      <div className="cell-num">{fmt(p.bb, 1)}</div>
      <div className="cell-num">{fmt(p.hits, 1)}</div>
      <div className="cell-num cell-proj">{fmt(p.er, 2)}</div>
      <div className="cell-num">{fmt(p.outs, 1)}</div>
      <div className="cell-num">{fmt(p.pf_factor, 2)}</div>
      <div className="cell-num">{fmt(p.wx_factor, 2)}</div>
      <div className="cell-source">
        <span className={`source-pill ${sourceInfo.cls}`}>{sourceInfo.label}</span>
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
          <div className="value">{game.market_total ?? '-'}</div>
        </div>
        <div className="stat">
          <div className="label">Proj</div>
          <div className="value">{game.proj_total ? Number(game.proj_total).toFixed(2) : '-'}</div>
        </div>
        <div className="stat edge">
          <div className="label">Edge</div>
          <div className={`value ${game.edge_total >= 0 ? 'pos' : 'neg'}`}>
            {game.edge_total ? (game.edge_total >= 0 ? '+' : '') + Number(game.edge_total).toFixed(2) : '-'}
          </div>
        </div>
      </div>
    </div>
  );
}

// ============================================================================
// Track Record - daily cards with category breakdown
// ============================================================================

function PerformanceView({ perf }) {
  if (!perf || !perf.byDate || perf.byDate.length === 0) {
    return <div className="empty">No graded plays yet - first slate hasn't graded.</div>;
  }

  return (
    <section>
      <div className="section-header">
        <h2>Track record.</h2>
        <span className="deck">Cumulative & daily &middot; graded against actual outcomes</span>
      </div>

      {perf.overall && <OverallCard overall={perf.overall} />}

      <div className="track-daily-stack">
        {perf.byDate.map(day => <DayCard key={day.run_date} day={day} />)}
      </div>
    </section>
  );
}

function sumCategoryGroup(group) {
  return group.reduce(
    (acc, c) => ({
      wins:   acc.wins + c.wins,
      losses: acc.losses + c.losses,
      pushes: acc.pushes + c.pushes,
      profit: acc.profit + c.profit_units,
    }),
    { wins: 0, losses: 0, pushes: 0, profit: 0 }
  );
}

function fmtSign(n) {
  return n >= 0 ? `+${n.toFixed(2)}` : n.toFixed(2);
}

function fmtRate(g) {
  const d = g.wins + g.losses;
  return d > 0 ? (g.wins / d * 100).toFixed(1) + '%' : '—';
}

function OverallCard({ overall }) {
  const o = overall.overall;
  const decisive = o.wins + o.losses;
  const hitRate = decisive > 0 ? (o.wins / decisive * 100) : 0;
  const profitClass = o.profit_units >= 0 ? 'pos' : 'neg';

  const totals = (overall.by_category || []).filter(c => c.kind === 'total');
  const props  = (overall.by_category || []).filter(c => c.kind === 'prop');
  const totalsAgg = sumCategoryGroup(totals);
  const propsAgg  = sumCategoryGroup(props);

  return (
    <div className="track-overall">
      <div className="track-overall-header">
        <div className="overall-record">
          <div className="record-label">Cumulative record</div>
          <div className="record-line">
            <span className="record-wl">{o.wins}-{o.losses}{o.pushes ? `-${o.pushes}` : ''}</span>
            <span className="record-rate">{hitRate.toFixed(1)}%</span>
            <span className={`record-units ${profitClass}`}>{fmtSign(o.profit_units)}u</span>
          </div>
        </div>
      </div>
      <div className="track-overall-split">
        <div className="split-tile">
          <div className="split-label">Game Totals</div>
          <div className="split-line">
            <span className="split-wl">{totalsAgg.wins}-{totalsAgg.losses}</span>
            <span className="split-rate">{fmtRate(totalsAgg)}</span>
            <span className={`split-units ${totalsAgg.profit >= 0 ? 'pos' : 'neg'}`}>{fmtSign(totalsAgg.profit)}u</span>
          </div>
        </div>
        <div className="split-tile">
          <div className="split-label">Pitcher Props</div>
          <div className="split-line">
            <span className="split-wl">{propsAgg.wins}-{propsAgg.losses}</span>
            <span className="split-rate">{fmtRate(propsAgg)}</span>
            <span className={`split-units ${propsAgg.profit >= 0 ? 'pos' : 'neg'}`}>{fmtSign(propsAgg.profit)}u</span>
          </div>
        </div>
      </div>
    </div>
  );
}

function DayCard({ day }) {
  const s = day.summary;
  const decisive = s.wins + s.losses;
  const hitRate = decisive > 0 ? (s.wins / decisive * 100) : 0;
  const profitClass = s.profit_units >= 0 ? 'pos' : 'neg';

  const dateLabel = new Date(day.run_date + 'T12:00:00').toLocaleDateString(
    'en-US', { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' }
  );

  const totals = day.by_category.filter(c => c.kind === 'total');
  const props  = day.by_category.filter(c => c.kind === 'prop');
  const totalsAgg = sumCategoryGroup(totals);
  const propsAgg  = sumCategoryGroup(props);

  return (
    <div className={`day-card ${profitClass}`}>
      <div className="day-header">
        <div className="day-date">{dateLabel}</div>
        <div className="day-summary">
          <span className="day-wl">{s.wins}-{s.losses}{s.pushes ? `-${s.pushes}` : ''}</span>
          <span className="day-rate">{hitRate.toFixed(1)}%</span>
          <span className={`day-units ${profitClass}`}>{fmtSign(s.profit_units)}u</span>
        </div>
      </div>

      {totals.length > 0 && (
        <div className="day-section">
          <div className="day-section-header">
            <span className="section-name">Game Totals</span>
            <span className="section-stats">
              {totalsAgg.wins}-{totalsAgg.losses}{totalsAgg.pushes ? `-${totalsAgg.pushes}` : ''} &middot; {fmtRate(totalsAgg)} &middot; <span className={totalsAgg.profit >= 0 ? 'pos' : 'neg'}>{fmtSign(totalsAgg.profit)}u</span>
            </span>
          </div>
        </div>
      )}

      {props.length > 0 && (
        <div className="day-section">
          <div className="day-section-header">
            <span className="section-name">Pitcher Props</span>
            <span className="section-stats">
              {propsAgg.wins}-{propsAgg.losses}{propsAgg.pushes ? `-${propsAgg.pushes}` : ''} &middot; {fmtRate(propsAgg)} &middot; <span className={propsAgg.profit >= 0 ? 'pos' : 'neg'}>{fmtSign(propsAgg.profit)}u</span>
            </span>
          </div>
          <div className="day-prop-grid">
            {props.map(c => (
              <div key={c.category} className="prop-tile">
                <div className="prop-cat">{MARKET_LABELS[c.category]?.replace('Pitcher ', '') ?? c.category}</div>
                <div className="prop-wl">{c.wins}-{c.losses}{c.pushes ? `-${c.pushes}` : ''}</div>
                <div className={`prop-units ${c.profit_units >= 0 ? 'pos' : 'neg'}`}>{fmtSign(c.profit_units)}u</div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
