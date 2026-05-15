import React, { useState, useEffect } from 'react';

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000';
const TABS = ['Full Game O/U', 'F5 O/U', 'Moneyline', 'Pitcher Props', 'Pitchers', 'Slate', 'Track Record'];
const MARKET_LABELS = {
  Total: 'Game Total', F5: 'F5 O/U', ML: 'Moneyline',
  K: 'Pitcher Strikeouts', Hits: 'Pitcher Hits Allowed',
  ER: 'Pitcher Earned Runs', Outs: 'Pitcher Outs Recorded', BB: 'Pitcher Walks',
};
const SOURCE_LABELS = {
  statcast:   { label: 'Statcast',   cls: 'src-statcast' },
  low_sample: { label: 'Low Sample', cls: 'src-low' },
  league_avg: { label: 'League Avg', cls: 'src-league' },
};
const PITCHER_COLUMNS = [
  { key:'last_first',       label:'Pitcher',   align:'left', type:'string' },
  { key:'team_code',        label:'Team',      align:'ctr',  type:'string' },
  { key:'hand',             label:'Hand',      align:'ctr',  type:'string' },
  { key:'opp_team_code',    label:'vs',        align:'ctr',  type:'string' },
  { key:'pa_sample',        label:'PA',        align:'num',  type:'number' },
  { key:'era',              label:'ERA',       align:'num',  type:'number' },
  { key:'xera',             label:'xERA',      align:'num',  type:'number' },
  { key:'true_era',         label:'tERA',      align:'num',  type:'number' },
  { key:'xwoba_against',    label:'xwOBA',     align:'num',  type:'number' },
  { key:'opp_lineup_xwoba', label:'Opp xwOBA', align:'num',  type:'number' },
  { key:'ip',               label:'IP',        align:'num',  type:'number' },
  { key:'k',                label:'K',         align:'num',  type:'number' },
  { key:'bb',               label:'BB',        align:'num',  type:'number' },
  { key:'hits',             label:'H',         align:'num',  type:'number' },
  { key:'er',               label:'ER',        align:'num',  type:'number' },
  { key:'outs',             label:'Outs',      align:'num',  type:'number' },
  { key:'pf_factor',        label:'PF',        align:'num',  type:'number' },
  { key:'wx_factor',        label:'Wx',        align:'num',  type:'number' },
  { key:'source',           label:'Source',    align:'left', type:'string' },
];

function fmtOdds(o) { if(o==null)return '-'; return o>0?'+'+o:String(o); }
function fmtPct(v)  { if(v==null)return '-'; return (Number(v)*100).toFixed(1)+'%'; }
function fmtSign(n) { return n>=0?'+'+n.toFixed(2):n.toFixed(2); }
function fmtRate(w,l){ const d=w+l; return d>0?Math.round(w/d*100)+'%':'--'; }

// ============================================================================
// App root
// ============================================================================
export default function App() {
  const [tab, setTab]     = useState('Games');
  const [slate, setSlate] = useState(null);
  const [perf, setPerf]   = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError]     = useState(null);

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
        const slateData   = await slateRes.json();
        const overallData = overallRes.ok ? await overallRes.json() : null;
        const byDateData  = byDateRes.ok  ? await byDateRes.json()  : [];
        if (!cancelled) { setSlate(slateData); setPerf({ overall: overallData, byDate: byDateData }); }
      } catch (e) {
        if (!cancelled) setError(e.message);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => { cancelled = true; };
  }, []);

  const allEdges       = slate?.edges ?? [];
  const gameTotalEdges = allEdges.filter(e => e.kind === 'total');
  const f5Edges        = allEdges.filter(e => e.kind === 'f5');
  const mlEdges        = allEdges.filter(e => e.kind === 'ml');
  const propEdges      = allEdges.filter(e => e.kind === 'prop');

  return (
    <div className="app">
      <Masthead slate={slate} />
      <nav className="tabs">
        {TABS.map(t => {
          let count = '';
          if (t==='Full Game O/U')         count = gameTotalEdges.length ? ` (${gameTotalEdges.length})` : '';
          if (t==='F5 O/U')     count = f5Edges.length        ? ` (${f5Edges.length})`        : '';
          if (t==='Moneyline')     count = mlEdges.length        ? ` (${mlEdges.length})`        : '';
          if (t==='Pitcher Props') count = propEdges.length      ? ` (${propEdges.length})`      : '';
          if (t==='Pitchers')      count = slate?.projections?.length ? ` (${slate.projections.length})` : '';
          return (
            <button key={t} className={`tab ${tab===t?'active':''}`} onClick={()=>setTab(t)}>
              {t}{count}
            </button>
          );
        })}
      </nav>

      {loading && <div className="loading">Loading slate</div>}
      {error   && <div className="empty">Error: {error}</div>}
      {!loading && !error && slate && (
        <>
          {tab==='Full Game O/U'         && <EdgesView edges={gameTotalEdges} kind="game" />}
          {tab==='F5 O/U'     && <F5View edges={f5Edges} games={slate.games} />}
          {tab==='Moneyline'     && <MoneylineView edges={mlEdges} games={slate.games} />}
          {tab==='Pitcher Props' && <EdgesView edges={propEdges} kind="prop" />}
          {tab==='Pitchers'      && <PitchersView projections={slate.projections} games={slate.games} />}
          {tab==='Slate'         && <GamesView games={slate.games} projections={slate.projections} />}
          {tab==='Track Record'  && <PerformanceView perf={perf} />}
        </>
      )}
      <footer className="footer">
        <span>QuAInt MLB Signal &middot; Vol II</span>
        <span>Manual review required &middot; Not financial advice</span>
      </footer>
    </div>
  );
}

// ============================================================================
// Masthead
// ============================================================================
function Masthead({ slate }) {
  const today   = new Date();
  const dateStr = today.toLocaleDateString('en-US',{weekday:'long',month:'long',day:'numeric',year:'numeric'});
  const nGames  = slate?.games?.length ?? 0;
  const nEdges  = (slate?.edges ?? []).length;
  const runId   = slate?.run?.run_id;
  return (
    <header className="masthead">
      <div className="masthead-top"><span>QuAInt &middot; MLB Edition</span><span>{dateStr.toUpperCase()}</span></div>
      <h1>The <span className="em">Signal</span>.</h1>
      <div className="masthead-sub">
        <div className="meta"><span>{nGames} GAMES</span><span>{nEdges} EDGES</span></div>
        <span>RUN #{runId ?? '-'}</span>
      </div>
    </header>
  );
}

// ============================================================================
// Edges view (game totals + pitcher props)
// ============================================================================
function EdgesView({ edges, kind }) {
  const PROP_CATEGORIES = ['K', 'Outs', 'ER', 'Hits', 'Walks'];
  const [propCategory, setPropCategory] = useState('K');
  const emptyMsg = kind==='game' ? 'No game total edges flagged tonight.' : 'No pitcher prop edges flagged tonight.';
  if (!edges || edges.length===0) return <div className="empty">{emptyMsg}</div>;
  const sorted = [...edges].sort((a,b) => {
    const ca=a.conviction_pct??-1, cb=b.conviction_pct??-1;
    return cb!==ca ? cb-ca : Math.abs(b.edge)-Math.abs(a.edge);
  });
  const displayed = kind==='prop' ? sorted.filter(e=>e.category===propCategory) : sorted;
  return (
    <section>
      <div className="section-header">
        <h2>{kind==='game'?'Game totals.':'Pitcher props.'}</h2>
        <span className="deck">{kind==='game'?'Over/under on combined runs scored - 9 innings':'K, Hits, ER, Outs &mdash; flagged where projection vs market diverges'}</span>
      </div>
      {kind==='prop' && (
        <div className="prop-cat-tabs">
          {PROP_CATEGORIES.map(cat => {
            const count = sorted.filter(e=>e.category===cat).length;
            return <button key={cat} className={`prop-cat-tab ${propCategory===cat?'active':''}`} onClick={()=>setPropCategory(cat)}>{MARKET_LABELS[cat]||cat} ({count})</button>;
          })}
        </div>
      )}
      <div className="edges-table">
        <div className="edges-thead">
          <span>{kind==='game'?'Matchup':'Pitcher'}</span>
          <span>Market</span><span className="num">Line</span>
          <span>Pick</span><span className="num">Projection</span><span>Conviction</span>
        </div>
        <div className="edges-tbody">
          {displayed.length===0
            ? <div className="empty">No {MARKET_LABELS[propCategory]||propCategory} edges flagged.</div>
            : displayed.map((e,i) => <EdgeRow key={e.edge_id||i} edge={e} />)}
        </div>
      </div>
    </section>
  );
}

function fmtOddsVal(o){ if(o==null)return null; return o>0?'+'+o:String(o); }

function EdgeRow({ edge }) {
  const isProp   = edge.kind==='prop';
  const subject  = isProp ? edge.pitcher_name?.split(',')[0]??'-' : `${edge.team_code??'?'} @ ${edge.opp_team_code??'?'}`;
  const subjectSub = isProp ? `${edge.team_code??''} v ${edge.opp_team_code??''}` : null;
  const conv=edge.conviction_pct, tier=edge.confidence_tier??3;
  const isLowTrust = tier===3||conv==null;
  let convBarClass = 'conv-bar';
  if (conv!=null){ if(conv>=75)convBarClass+=' conv-strong'; else if(conv>=60)convBarClass+=' conv-medium'; else convBarClass+=' conv-weak'; }
  const relevantOdds = edge.lean==='OVER' ? fmtOddsVal(edge.over_price) : fmtOddsVal(edge.under_price);
  return (
    <div className="edge-row">
      <div className="cell-subject">
        <div className="subject-main">{subject}</div>
        {subjectSub && <div className="subject-sub">{subjectSub}</div>}
      </div>
      <div className="cell-market">{MARKET_LABELS[edge.category]||edge.category}</div>
      <div className="cell-num">{Number(edge.line).toFixed(1)}</div>
      <div className={`cell-pick lean-${edge.lean}`}>
        {edge.lean}
        {relevantOdds&&<span className="pick-odds">{relevantOdds}</span>}
      </div>
      <div className="cell-num cell-proj">{Number(edge.proj_value).toFixed(2)}</div>
      <div className="cell-conviction">
        {isLowTrust
          ? <span className="conv-na" title="Tier 3 or fallback">n/a <span className="tier-pill">T{tier}</span></span>
          : <div className="conv-cell">
              <span className="conv-value">{Number(conv).toFixed(0)}%</span>
              <div className={convBarClass}><div className="conv-bar-fill" style={{width:`${Math.min(100,Math.max(0,conv))}%`}}/></div>
            </div>}
      </div>
    </div>
  );
}

// ============================================================================
// F5 view
// ============================================================================
function F5View({ edges, games }) {
  if (!edges || edges.length===0) return (
    <section>
      <div className="section-header"><h2>F5 totals.</h2><span className="deck">First 5 innings O/U &mdash; starter ERA dominates this market</span></div>
      <div className="empty">No F5 edges flagged tonight.</div>
    </section>
  );
  const sorted = [...edges].sort((a,b)=>Math.abs(b.edge)-Math.abs(a.edge));
  return (
    <section>
      <div className="section-header"><h2>F5 totals.</h2><span className="deck">First 5 innings &mdash; starter matchup drives edge</span></div>
      <div className="edges-table">
        <div className="edges-thead f5-thead">
          <span>Matchup</span><span className="num">F5 Line</span>
          <span>Pick</span><span className="num">F5 Proj</span>
          <span className="num">Edge</span><span>Conviction</span>
        </div>
        <div className="edges-tbody">
          {sorted.map((e,i) => <F5Row key={e.edge_id||i} edge={e} />)}
        </div>
      </div>
    </section>
  );
}

function F5Row({ edge }) {
  const conv=edge.conviction_pct, tier=edge.confidence_tier??3;
  const relevantOdds = edge.lean==='OVER' ? fmtOddsVal(edge.over_price) : fmtOddsVal(edge.under_price);
  return (
    <div className="edge-row f5-row">
      <div className="cell-subject">
        <div className="subject-main">{edge.team_code} @ {edge.opp_team_code}</div>
        <div className="subject-sub">First 5 Innings</div>
      </div>
      <div className="cell-num">{Number(edge.line).toFixed(1)}</div>
      <div className={`cell-pick lean-${edge.lean}`}>
        {edge.lean}
        {relevantOdds&&<span className="pick-odds">{relevantOdds}</span>}
      </div>
      <div className="cell-num cell-proj">{Number(edge.proj_value).toFixed(2)}</div>
      <div className="cell-num" style={{fontWeight:700,color:edge.edge>=0?'var(--moss)':'var(--vermillion)'}}>
        {edge.edge>=0?'+':''}{Number(edge.edge).toFixed(2)}
      </div>
      <div className="cell-conviction">
        {conv!=null
          ? <span className="conv-value">{Number(conv).toFixed(0)}% <span className="tier-pill">T{tier}</span></span>
          : <span className="conv-na">n/a</span>}
      </div>
    </div>
  );
}

// ============================================================================
// Moneyline view
// ============================================================================
function MoneylineView({ edges, games }) {
  if (!edges || edges.length===0) return (
    <section>
      <div className="section-header"><h2>Moneyline.</h2><span className="deck">Win probability vs book implied odds</span></div>
      <div className="empty">No ML edges flagged tonight.</div>
    </section>
  );
  const sorted = [...edges].sort((a,b)=>Math.abs(b.ml_edge_pct||b.edge)-Math.abs(a.ml_edge_pct||a.edge));
  return (
    <section>
      <div className="section-header"><h2>Moneyline.</h2><span className="deck">Skellam win probability vs vig-free implied odds &mdash; 4pp minimum edge</span></div>
      <div className="ml-table">
        <div className="ml-thead">
          <span>Matchup</span><span>Pick</span><span className="num">Odds</span>
          <span className="num">Model Win%</span><span className="num">Implied</span>
          <span className="num">Edge</span><span>Tier</span>
        </div>
        <div className="ml-tbody">
          {sorted.map((e,i) => <MLRow key={e.edge_id||i} edge={e} />)}
        </div>
      </div>
      <p className="ml-disclaimer">ML edges use Skellam distribution on projected run totals. Min threshold 4pp.</p>
    </section>
  );
}

function MLRow({ edge }) {
  const tier = edge.confidence_tier??3;
  const edgePp = edge.ml_edge_pct!=null ? (edge.ml_edge_pct*100).toFixed(1) : edge.edge?.toFixed(1);
  const modelPct = edge.proj_value!=null ? Number(edge.proj_value).toFixed(1)+'%' : '-';
  const impliedPct = edge.ml_edge_pct!=null && edge.proj_value!=null
    ? (Number(edge.proj_value) - Number(edge.ml_edge_pct)*100).toFixed(1)+'%' : '-';
  const isPos = (edge.ml_edge_pct??0)>0;
  // ML odds: line IS the American odds for the lean team
  const mlOdds = fmtOddsVal(edge.line);
  return (
    <div className="ml-row">
      <div className="cell-subject">
        <div className="subject-main">{edge.team_code} @ {edge.opp_team_code}</div>
        <div className="subject-sub">{edge.notes||''}</div>
      </div>
      <div className={`cell-pick lean-${isPos?'OVER':'UNDER'}`} style={{fontFamily:'var(--display)',fontWeight:700}}>
        {edge.lean}
        {mlOdds&&<span className="pick-odds">{mlOdds}</span>}
      </div>
      <div className="cell-num">{fmtOdds(edge.line)}</div>
      <div className="cell-num cell-proj">{modelPct}</div>
      <div className="cell-num">{impliedPct}</div>
      <div className="cell-num" style={{fontWeight:700,color:isPos?'var(--moss)':'var(--vermillion)'}}>
        {isPos?'+':''}{edgePp}pp
      </div>
      <div><span className="tier-pill">T{tier}</span></div>
    </div>
  );
}

// ============================================================================
// Pitchers table
// ============================================================================
function PitchersView({ projections, games }) {
  const [sortKey, setSortKey] = useState('__default');
  const [sortDir, setSortDir] = useState('asc');
  if (!projections||projections.length===0) return <div className="empty">No pitcher projections yet.</div>;
  const gameTime = {};
  (games||[]).forEach(g=>{ gameTime[g.game_pk]=g.game_time_et||''; });
  function handleHeaderClick(key){
    if(sortKey===key){ setSortDir(sortDir==='asc'?'desc':'asc'); }
    else { const col=PITCHER_COLUMNS.find(c=>c.key===key); setSortKey(key); setSortDir(col?.type==='number'?'desc':'asc'); }
  }
  function getSortVal(p,key){
    if(key==='__default') return `${gameTime[p.game_pk]||'99:99'}|${p.last_first||''}`;
    const v=p[key]; return v??null;
  }
  const sorted = [...projections].sort((a,b)=>{
    const va=getSortVal(a,sortKey), vb=getSortVal(b,sortKey);
    if(va==null&&vb==null)return 0; if(va==null)return 1; if(vb==null)return -1;
    let cmp=typeof va==='number'&&typeof vb==='number'?va-vb:String(va).localeCompare(String(vb));
    return sortDir==='asc'?cmp:-cmp;
  });
  return (
    <section>
      <div className="section-header"><h2>Pitcher projections.</h2><span className="deck">Click column to sort &middot; lineup-weighted xwOBA, park factor, weather</span></div>
      <div className="pitchers-table">
        <div className="pitchers-thead">
          {PITCHER_COLUMNS.map(col=>{
            const isActive=sortKey===col.key;
            const arrow=isActive?(sortDir==='asc'?' ▲':' ▼'):'';
            return <button key={col.key} className={`pitchers-th ${col.align==='num'?'num':col.align==='ctr'?'ctr':''} ${isActive?'active':''}`} onClick={()=>handleHeaderClick(col.key)}>{col.label}{arrow}</button>;
          })}
        </div>
        <div className="pitchers-tbody">{sorted.map((p,i)=><PitcherRow key={p.mlb_id||p.pitcher_mlb_id||i} p={p}/>)}</div>
      </div>
    </section>
  );
}

function PitcherRow({ p }) {
  const si = SOURCE_LABELS[p.source]||{label:p.source||'?',cls:'src-unknown'};
  const fmt  = (v,d=2) => v==null?'-':Number(v).toFixed(d);
  const fmt0 = v => v==null?'-':Math.round(Number(v)).toString();
  const oppLabel = p.used_actual_lineup
    ? <span title="Lineup-confirmed">{fmt(p.opp_lineup_xwoba,3)}</span>
    : <span title="Team aggregate" className="proj-tentative">{fmt(p.opp_lineup_xwoba,3)}*</span>;
  return (
    <div className="pitcher-row">
      <div className="cell-pitcher-name">{p.last_first?.split(',')[0]??'-'}</div>
      <div className="cell-ctr">{p.team_code??'-'}</div>
      <div className="cell-ctr">{p.hand??'-'}</div>
      <div className="cell-ctr">{p.opp_team_code??'-'}</div>
      <div className="cell-num">{fmt0(p.pa_sample)}</div>
      <div className="cell-num">{fmt(p.era)}</div>
      <div className="cell-num">{fmt(p.xera)}</div>
      <div className="cell-num">{fmt(p.true_era)}</div>
      <div className="cell-num">{fmt(p.xwoba_against,3)}</div>
      <div className="cell-num">{oppLabel}</div>
      <div className="cell-num">{fmt(p.ip,1)}</div>
      <div className="cell-num cell-proj">{fmt(p.k,1)}</div>
      <div className="cell-num">{fmt(p.bb,1)}</div>
      <div className="cell-num">{fmt(p.hits,1)}</div>
      <div className="cell-num cell-proj">{fmt(p.er,2)}</div>
      <div className="cell-num">{fmt(p.outs,1)}</div>
      <div className="cell-num">{fmt(p.pf_factor,2)}</div>
      <div className="cell-num">{fmt(p.wx_factor,2)}</div>
      <div className="cell-source"><span className={`source-pill ${si.cls}`}>{si.label}</span></div>
    </div>
  );
}

// ============================================================================
// Slate / Games view
// ============================================================================
function GamesView({ games, projections }) {
  if (!games||games.length===0) return <div className="empty">No games scheduled today.</div>;
  const byGame = {};
  (projections||[]).forEach(p=>{ if(!byGame[p.game_pk])byGame[p.game_pk]=[]; byGame[p.game_pk].push(p); });
  return (
    <section>
      <div className="section-header"><h2>Tonight's slate.</h2><span className="deck">Probables &middot; projections &middot; market line</span></div>
      <div className="games">{games.map(g=><GameCard key={g.game_pk} game={g} projs={byGame[g.game_pk]||[]}/>)}</div>
    </section>
  );
}

function GameCard({ game, projs }) {
  const flag = game.lean==='OVER'?'flagged-over':game.lean==='UNDER'?'flagged-under':'';
  const awayProj = projs.find(p=>p.team_code===game.away_team);
  const homeProj = projs.find(p=>p.team_code===game.home_team);
  return (
    <div className={`game-card ${flag}`}>
      <div className="game-header">
        <span className="matchup">
          {game.away_team}{game.proj_away_runs!=null&&<span className="team-proj">{Number(game.proj_away_runs).toFixed(1)}</span>}
          {' @ '}
          {game.home_team}{game.proj_home_runs!=null&&<span className="team-proj">{Number(game.proj_home_runs).toFixed(1)}</span>}
        </span>
        <span className="time">{game.game_time_et}</span>
      </div>
      {[awayProj,homeProj].filter(Boolean).map(p=>(
        <div key={p.mlb_id} className="pitcher-line">
          <span className="name">{p.last_first?.split(',')[0]}</span>
          <span className="sub">{p.team_code} &middot; {p.hand}HP</span>
          <span className="sub">PA {p.pa_sample}</span>
          <span className="xera">xERA {Number(p.xera||0).toFixed(2)}</span>
        </div>
      ))}
      <div className="game-totals">
        <div className="stat"><div className="label">Market</div><div className="value">{game.market_total??'-'}</div></div>
        <div className="stat"><div className="label">Proj</div><div className="value">{game.proj_total?Number(game.proj_total).toFixed(2):'-'}</div></div>
        <div className="stat edge">
          <div className="label">Edge</div>
          <div className={`value ${(game.edge_total??0)>=0?'pos':'neg'}`}>
            {game.edge_total?(game.edge_total>=0?'+':'')+Number(game.edge_total).toFixed(2):'-'}
          </div>
        </div>
        {game.market_f5_total&&<div className="stat"><div className="label">F5 Line</div><div className="value">{game.market_f5_total}</div></div>}
        {game.away_ml&&<div className="stat"><div className="label">ML</div><div className="value" style={{fontSize:'11px'}}>{fmtOdds(game.away_ml)}/{fmtOdds(game.home_ml)}</div></div>}
      </div>
    </div>
  );
}

// ============================================================================
// Track Record
// ============================================================================
function PerformanceView({ perf }) {
  if (!perf||!perf.byDate||perf.byDate.length===0) return <div className="empty">No graded plays yet.</div>;
  const ov         = perf.overall||{};
  const summary    = ov.overall||{};
  const byCategory = ov.by_category||[];
  return (
    <section>
      <div className="section-header"><h2>Track record.</h2><span className="deck">Cumulative &amp; daily</span></div>
      <OverallCard summary={summary} byCategory={byCategory} mlBreakdown={ov.ml_breakdown||[]}/>
      <div className="track-daily-stack">{perf.byDate.map(day=><DayCard key={day.run_date} day={day}/>)}</div>
    </section>
  );
}

function SubBreakdown({ rows, labelMap }) {
  if (!rows || rows.length === 0) return null;
  return (
    <div className="tr-sub-breakdown">
      {rows.map((r,i) => {
        const label = labelMap ? (labelMap[r.lean] || r.lean) : r.lean;
        const p = r.profit_units || 0;
        return (
          <div key={i} className="tr-sub-row">
            <span className="tr-sub-label">{label}</span>
            <span className="tr-sub-record">{r.wins}-{r.losses}{r.pushes>0?'-'+r.pushes:''}</span>
            <span className="tr-sub-rate">{fmtRate(r.wins,r.losses)}</span>
            <span className={'tr-sub-profit '+(p>=0?'pos':'neg')}>{fmtSign(p)}u</span>
          </div>
        );
      })}
    </div>
  );
}

function OverallCard({ summary, byCategory, mlBreakdown }) {
  const w=summary.wins||0, l=summary.losses||0, p=summary.pushes||0;
  const profit = summary.profit_units||0;

  // Group by kind
  const totals = byCategory.filter(r=>r.kind==='total');
  const f5s    = byCategory.filter(r=>r.kind==='f5');
  const mls    = byCategory.filter(r=>r.kind==='ml');
  const props  = byCategory.filter(r=>r.kind==='prop');

  const sum = arr => ({
    w: arr.reduce((s,r)=>s+(r.wins||0),0),
    l: arr.reduce((s,r)=>s+(r.losses||0),0),
    p: arr.reduce((s,r)=>s+(r.pushes||0),0),
    profit: arr.reduce((s,r)=>s+(r.profit_units||0),0),
  });

  const tot = sum(totals);
  const f5  = sum(f5s);
  const ml  = sum(mls);
  const prp = sum(props);

  // ML: classify as fav (negative odds) or dog (positive odds)
  // We don't have odds in byCategory yet — just show total for now
  // OVER/UNDER breakdown from lean field
  const totOver  = totals.filter(r=>r.lean==='OVER');
  const totUnder = totals.filter(r=>r.lean==='UNDER');
  const f5Over   = f5s.filter(r=>r.lean==='OVER');
  const f5Under  = f5s.filter(r=>r.lean==='UNDER');

  return (
    <div className="tr-overall">
      <div className="tr-overall-label">ALL-TIME RECORD</div>
      <div className="tr-headline">
        <span className="tr-record">{w}-{l}{p>0?'-'+p:''}</span>
        <span className="tr-rate">{fmtRate(w,l)}</span>
        <span className={'tr-profit '+(profit>=0?'pos':'neg')}>{fmtSign(profit)}u</span>
      </div>

      <div className="tr-split">
        {/* Game Totals */}
        <div className="tr-split-tile">
          <div className="tr-split-label">Full Game O/U</div>
          <div className="tr-split-record">{tot.w}-{tot.l}</div>
          <div className="tr-split-rate">{fmtRate(tot.w,tot.l)}</div>
          <div className={'tr-split-profit '+(tot.profit>=0?'pos':'neg')}>{fmtSign(tot.profit)}u</div>
          <SubBreakdown rows={[...totOver.map(r=>({...r,lean:'OVER'})),...totUnder.map(r=>({...r,lean:'UNDER'}))]} />
        </div>

        {/* F5 */}
        {f5s.length>0&&(
        <div className="tr-split-tile">
          <div className="tr-split-label">F5 O/U</div>
          <div className="tr-split-record">{f5.w}-{f5.l}</div>
          <div className="tr-split-rate">{fmtRate(f5.w,f5.l)}</div>
          <div className={'tr-split-profit '+(f5.profit>=0?'pos':'neg')}>{fmtSign(f5.profit)}u</div>
          <SubBreakdown rows={[...f5Over.map(r=>({...r,lean:'OVER'})),...f5Under.map(r=>({...r,lean:'UNDER'}))]} />
        </div>
        )}

        {/* Moneyline */}
        {mls.length>0&&(
        <div className="tr-split-tile">
          <div className="tr-split-label">Moneyline</div>
          <div className="tr-split-record">{ml.w}-{ml.l}</div>
          <div className="tr-split-rate">{fmtRate(ml.w,ml.l)}</div>
          <div className={'tr-split-profit '+(ml.profit>=0?'pos':'neg')}>{fmtSign(ml.profit)}u</div>
          {mlBreakdown&&mlBreakdown.length>0&&(
            <div className="tr-sub-breakdown">
              {mlBreakdown.map((r,i)=>(
                <div key={i} className="tr-sub-row">
                  <span className="tr-sub-label">{r.label}</span>
                  <span className="tr-sub-record">{r.wins}-{r.losses}</span>
                  <span className="tr-sub-rate">{fmtRate(r.wins,r.losses)}</span>
                  <span className={'tr-sub-profit '+(r.profit_units>=0?'pos':'neg')}>{fmtSign(r.profit_units)}u</span>
                </div>
              ))}
            </div>
          )}
        </div>
        )}

        {/* Pitcher Props */}
        <div className="tr-split-tile">
          <div className="tr-split-label">Pitcher Props</div>
          <div className="tr-split-record">{prp.w}-{prp.l}</div>
          <div className="tr-split-rate">{fmtRate(prp.w,prp.l)}</div>
          <div className="tr-split-profit" style={{color:'var(--ink-3)',fontSize:'10px'}}>—</div>
        </div>
      </div>

      {props.length>0&&(
        <div className="tr-prop-breakdown">
          {[...new Set(props.map(r=>r.category))].map(cat=>{
            const catRows = props.filter(r=>r.category===cat);
            const cw=catRows.reduce((s,r)=>s+(r.wins||0),0);
            const cl=catRows.reduce((s,r)=>s+(r.losses||0),0);
            return (
              <div key={cat} className="tr-prop-tile">
                <div className="tr-prop-cat">{cat}</div>
                <div className="tr-prop-record">{cw}-{cl}</div>
                <div className="tr-prop-profit" style={{color:'var(--ink-3)',fontSize:'10px'}}>—</div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

function PlayRow({ play }) {
  const res = (play.result||'').toLowerCase();
  return (
    <div className={'play-row result-'+res}>
      <span className="play-subject">{play.subject}</span>
      <span className="play-line">{play.line}</span>
      <span className="play-actual">{play.actual_value!=null?play.actual_value:'-'}</span>
      <span className={'play-result res-'+res}>{play.result}</span>
      <span className={(play.profit_units||0)>=0?'play-profit pos':'play-profit neg'}>{fmtSign(play.profit_units||0)}u</span>
    </div>
  );
}

function MLPlayRow({ play }) {
  const res = (play.result || '').toLowerCase();
  const odds = play.line != null ? (play.line > 0 ? '+'+Math.round(play.line) : Math.round(play.line)) : '-';
  // Subject format: "MIN @ WSH" — lean team is in play.subject after the arrow or we use team_code
  const matchup = play.subject || '-';
  return (
    <div className={'play-row result-'+res}>
      <span className="play-subject">{matchup}</span>
      <span className="play-line">{odds}</span>
      <span className="play-actual">{play.actual_value===1.0?'WIN':'LOSS'}</span>
      <span className={'play-result res-'+res}>{play.result}</span>
      <span className={(play.profit_units||0)>=0?'play-profit pos':'play-profit neg'}>{fmtSign(play.profit_units||0)}u</span>
    </div>
  );
}

function BucketBlock({ bucket, showUnits=true }) {
  const [open,setOpen] = useState(false);
  const p = bucket.profit_units||0;
  const pushStr = bucket.pushes>0?'-'+bucket.pushes:'';
  return (
    <div className="bucket-block">
      <div className="bucket-block-header" onClick={()=>setOpen(!open)}>
        <span className="bucket-block-name">{bucket.lean}</span>
        <div className="bucket-block-stats">
          <span>{bucket.wins}-{bucket.losses}{pushStr}</span>
          {showUnits
            ? <span className={p>=0?'pos':'neg'}>{fmtSign(p)}u</span>
            : <span style={{color:'var(--ink-3)',fontSize:'10px'}}>—</span>
          }
          <span className="bucket-toggle">{open?'▲':'▼'}</span>
        </div>
      </div>
      {open&&(
        <div className="bucket-plays">
          {(bucket.plays||[]).map((play,i)=><PlayRow key={i} play={play} showUnits={showUnits}/>)}
        </div>
      )}
    </div>
  );
}

function PropCatBlock({ cat, buckets }) {
  const [open,setOpen] = useState(false);
  const w = buckets.reduce((s,b)=>s+(b.wins||0),0);
  const l = buckets.reduce((s,b)=>s+(b.losses||0),0);
  const p = buckets.reduce((s,b)=>s+(b.profit_units||0),0);
  const pushes = buckets.reduce((s,b)=>s+(b.pushes||0),0);
  const pushStr = pushes>0?'-'+pushes:'';
  return (
    <div className="prop-cat-block">
      <div className="prop-cat-header" onClick={()=>setOpen(!open)}>
        <span className="prop-cat-name">{cat}</span>
        <div className="prop-cat-stats">
          <span>{w}-{l}{pushStr}</span>
          <span className={p>=0?'pos':'neg'}>{fmtSign(p)}u</span>
          <span className="bucket-toggle">{open?'▲':'▼'}</span>
        </div>
      </div>
      {open&&(
        <div className="prop-cat-body">
          {buckets.map((b,i)=><BucketBlock key={i} bucket={b}/>)}
        </div>
      )}
    </div>
  );
}

function GroupBlock({ label, children, wins, losses, pushes, profit, showUnits=true }) {
  const [open,setOpen] = useState(false);
  const p = profit||0;
  const pushStr = pushes>0?'-'+pushes:'';
  return (
    <div className="day-group-block">
      <div className="day-group-header" onClick={()=>setOpen(!open)}>
        <span className="day-group-name">{label}</span>
        <div className="day-group-stats">
          <span>{wins}-{losses}{pushStr}</span>
          {showUnits
            ? <span className={p>=0?'pos':'neg'}>{fmtSign(p)}u</span>
            : <span style={{color:'var(--ink-3)',fontSize:'10px'}}>—</span>
          }
          <span className="bucket-toggle">{open?'▲':'▼'}</span>
        </div>
      </div>
      {open&&<div className="day-group-body">{children}</div>}
    </div>
  );
}

function DayCard({ day }) {
  const [open,setOpen] = useState(false);
  const s = day.summary||{};
  const w=s.wins||0, l=s.losses||0, p=s.pushes||0;
  const profit = s.profit_units||0;
  const buckets = day.buckets||[];
  const pushStr = p>0?'-'+p:'';

  const totalBuckets = buckets.filter(b=>b.kind==='total');
  const totW      = totalBuckets.reduce((s,b)=>s+(b.wins||0),0);
  const totL      = totalBuckets.reduce((s,b)=>s+(b.losses||0),0);
  const totP      = totalBuckets.reduce((s,b)=>s+(b.pushes||0),0);
  const totProfit = totalBuckets.reduce((s,b)=>s+(b.profit_units||0),0);

  const f5Buckets  = buckets.filter(b=>b.kind==='f5');
  const f5W       = f5Buckets.reduce((s,b)=>s+(b.wins||0),0);
  const f5L       = f5Buckets.reduce((s,b)=>s+(b.losses||0),0);
  const f5P       = f5Buckets.reduce((s,b)=>s+(b.pushes||0),0);
  const f5Profit  = f5Buckets.reduce((s,b)=>s+(b.profit_units||0),0);

  const mlBuckets  = buckets.filter(b=>b.kind==='ml');
  const mlW       = mlBuckets.reduce((s,b)=>s+(b.wins||0),0);
  const mlL       = mlBuckets.reduce((s,b)=>s+(b.losses||0),0);
  const mlP       = mlBuckets.reduce((s,b)=>s+(b.pushes||0),0);
  const mlProfit  = mlBuckets.reduce((s,b)=>s+(b.profit_units||0),0);

  const propBuckets = buckets.filter(b=>b.kind==='prop');
  const propCats  = [...new Set(propBuckets.map(b=>b.category))];
  const prpW      = propBuckets.reduce((s,b)=>s+(b.wins||0),0);
  const prpL      = propBuckets.reduce((s,b)=>s+(b.losses||0),0);
  const prpP      = propBuckets.reduce((s,b)=>s+(b.pushes||0),0);
  const prpProfit = propBuckets.reduce((s,b)=>s+(b.profit_units||0),0);

  return (
    <div className={'day-card '+(profit>=0?'pos':'neg')}>
      <div className="day-header" onClick={()=>setOpen(!open)}>
        <span className="day-date">{day.run_date}</span>
        <div className="day-summary">
          <span className="day-wl">{w}-{l}{pushStr}</span>
          <span className="day-rate">{w+l>0?Math.round(w/(w+l)*100)+'%':'--'}</span>
          <span className={'day-units '+(profit>=0?'pos':'neg')}>{fmtSign(profit)}u</span>
        </div>
        <span className="day-toggle">{open?'▲':'▼'}</span>
      </div>
      {open&&(
        <div className="day-body">
          {totalBuckets.length>0&&(
            <GroupBlock label="Full Game O/U" wins={totW} losses={totL} pushes={totP} profit={totProfit}>
              {totalBuckets.map((b,i)=><BucketBlock key={i} bucket={b}/>)}
            </GroupBlock>
          )}
          {f5Buckets.length>0&&(
            <GroupBlock label="F5 O/U" wins={f5W} losses={f5L} pushes={f5P} profit={f5Profit}>
              {f5Buckets.map((b,i)=><BucketBlock key={i} bucket={b}/>)}
            </GroupBlock>
          )}
          {mlBuckets.length>0&&(
            <GroupBlock label="Moneyline" wins={mlW} losses={mlL} pushes={mlP} profit={mlProfit}>
              {mlBuckets.flatMap(b=>b.plays||[]).map((play,i)=><MLPlayRow key={i} play={play}/>)}
            </GroupBlock>
          )}
          {propCats.length>0&&(
            <GroupBlock label="Pitcher Props" wins={prpW} losses={prpL} pushes={prpP} profit={null} showUnits={false}>
              {propCats.map(cat=>(
                <PropCatBlock key={cat} cat={cat} buckets={propBuckets.filter(b=>b.category===cat)}/>
              ))}
            </GroupBlock>
          )}
        </div>
      )}
    </div>
  );
}

