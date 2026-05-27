import React, { useState, useEffect } from 'react';

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000';
const ADMIN_TOKEN = import.meta.env.VITE_ADMIN_TOKEN || '';


const TABS = ['Full Game O/U', 'F5 O/U', 'Moneyline', 'Pitcher Props', 'Pitchers', 'Slate', 'Stats', 'My Record', 'Track Record'];
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
  const [adminVisible, setAdminVisible] = useState(false);
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
      <Masthead slate={slate} onTripleClick={()=>setAdminVisible(v=>!v)} />
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
          {tab==='Stats'         && <StatsView />}
          {tab==='My Record'     && <MyRecordView />}
          {tab==='Track Record'  && <PerformanceView perf={perf} />}
        </>
      )}
      <AdminPanelV2 visible={adminVisible} onClose={()=>setAdminVisible(false)} />
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
function Masthead({ slate, onTripleClick }) {
  const today   = new Date();
  const dateStr = today.toLocaleDateString('en-US',{weekday:'long',month:'long',day:'numeric',year:'numeric'});
  const nGames  = slate?.games?.length ?? 0;
  const nEdges  = (slate?.edges ?? []).length;
  const runId   = slate?.run?.run_id;
  const clicksRef = React.useRef({ count: 0, timer: null });
  function handleTitleClick() {
    if (!onTripleClick) return;
    clicksRef.current.count += 1;
    if (clicksRef.current.timer) clearTimeout(clicksRef.current.timer);
    if (clicksRef.current.count >= 3) {
      onTripleClick();
      clicksRef.current.count = 0;
    } else {
      clicksRef.current.timer = setTimeout(() => { clicksRef.current.count = 0; }, 600);
    }
  }
  return (
    <header className="masthead">
      <div className="masthead-top"><span>QuAInt &middot; MLB Edition</span><span>{dateStr.toUpperCase()}</span></div>
      <h1 onClick={handleTitleClick} style={{cursor: 'pointer', userSelect: 'none'}}>The <span className="em">Signal</span>.</h1>
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
          <span className="th-bet">Bet</span>
          <span className="th-reason"></span>
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


// ============================================================================
// Reason detail — small expandable block under any edge row
// ============================================================================
function ReasonDetail({ factors }) {
  if (!factors || factors.length === 0) return null;
  return (
    <div className="reason-detail">
      <table className="reason-factors">
        <thead>
          <tr><th>Factor</th><th>Value</th><th className="impact">Impact</th></tr>
        </thead>
        <tbody>
          {factors.map((f, i) => {
            const cls = f.impact && f.impact.startsWith('+') ? 'impact-pos'
                      : f.impact && f.impact.startsWith('-') ? 'impact-neg'
                      : 'impact-neutral';
            return (
              <tr key={i}>
                <td className="factor-label">{f.label}</td>
                <td className="factor-value">{f.value}</td>
                <td className={'factor-impact ' + cls}>{f.impact}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function ReasonToggle({ open, onClick, hasFactors }) {
  if (!hasFactors) return null;
  return (
    <button
      type="button"
      className={'reason-toggle' + (open ? ' open' : '')}
      onClick={(e) => { e.stopPropagation(); onClick(); }}
      title={open ? 'Hide reasoning' : 'Show reasoning'}
      aria-label="Toggle reasoning"
    >
      {open ? '−' : '+'}
    </button>
  );
}

function EdgeRow({ edge }) {
  const [open, setOpen] = useState(false);
  const isProp   = edge.kind==='prop';
  const subject  = isProp ? edge.pitcher_name?.split(',')[0]??'-' : `${edge.team_code??'?'} @ ${edge.opp_team_code??'?'}`;
  const subjectSub = isProp ? `${edge.team_code??''} v ${edge.opp_team_code??''}` : null;
  const conv=edge.conviction_pct, tier=edge.confidence_tier??3;
  const isLowTrust = tier===3||conv==null;
  let convBarClass = 'conv-bar';
  if (conv!=null){ if(conv>=75)convBarClass+=' conv-strong'; else if(conv>=60)convBarClass+=' conv-medium'; else convBarClass+=' conv-weak'; }
  const relevantOdds = edge.lean==='OVER' ? fmtOddsVal(edge.over_price) : fmtOddsVal(edge.under_price);
  const hasFactors = !!(edge.reason_factors && edge.reason_factors.length);
  return (
    <>
    <div className="edge-row">
      <div className="cell-subject">
        <div className="subject-main">{subject}</div>
        {subjectSub && <div className="subject-sub">{subjectSub}</div>}
        {edge.reason_short && <div className="reason-short">{edge.reason_short}</div>}
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
        <ReasonToggle open={open} onClick={()=>setOpen(!open)} hasFactors={hasFactors} />
      </div>
    </div>
    {open && hasFactors && <ReasonDetail factors={edge.reason_factors} />}
    </>
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
          <span className="th-bet">Bet</span>
          <span className="th-reason"></span>
        </div>
        <div className="edges-tbody">
          {sorted.map((e,i) => <F5Row key={e.edge_id||i} edge={e} />)}
        </div>
      </div>
    </section>
  );
}

function F5Row({ edge }) {
  const [open, setOpen] = useState(false);
  const conv=edge.conviction_pct, tier=edge.confidence_tier??3;
  const relevantOdds = edge.lean==='OVER' ? fmtOddsVal(edge.over_price) : fmtOddsVal(edge.under_price);
  const hasFactors = !!(edge.reason_factors && edge.reason_factors.length);
  return (
    <>
    <div className="edge-row f5-row">
      <div className="cell-subject">
        <div className="subject-main">{edge.team_code} @ {edge.opp_team_code}</div>
        <div className="subject-sub">First 5 Innings</div>
        {edge.reason_short && <div className="reason-short">{edge.reason_short}</div>}
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
        <ReasonToggle open={open} onClick={()=>setOpen(!open)} hasFactors={hasFactors} />
      </div>
    </div>
    {open && hasFactors && <ReasonDetail factors={edge.reason_factors} />}
    </>
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
      <div className="section-header"><h2>Moneyline.</h2><span className="deck">Very high conviction only &mdash; 20pp min for favorites, 60pp for dogs, both SP must be Statcast</span></div>
      <div className="ml-table">
        <div className="ml-thead">
          <span>Matchup</span><span>Pick</span><span className="num">Odds</span>
          <span className="num">Model Win%</span><span className="num">Implied</span>
          <span className="num">Edge</span><span>Tier</span>
          <span className="th-bet">Bet</span>
          <span className="th-reason"></span>
        </div>
        <div className="ml-tbody">
          {sorted.map((e,i) => <MLRow key={e.edge_id||i} edge={e} />)}
        </div>
      </div>
      <p className="ml-disclaimer">ML edges use Skellam distribution on projected run totals. Very high conviction only: 20pp min for favorites, 60pp for underdogs.</p>
    </section>
  );
}

function MLRow({ edge }) {
  const [open, setOpen] = useState(false);
  const tier = edge.confidence_tier??3;
  const edgePp = edge.ml_edge_pct!=null ? (edge.ml_edge_pct*100).toFixed(1) : edge.edge?.toFixed(1);
  const modelPct = edge.proj_value!=null ? Number(edge.proj_value).toFixed(1)+'%' : '-';
  const impliedPct = edge.ml_edge_pct!=null && edge.proj_value!=null
    ? (Number(edge.proj_value) - Number(edge.ml_edge_pct)*100).toFixed(1)+'%' : '-';
  const isPos = (edge.ml_edge_pct??0)>0;
  const mlOdds = fmtOddsVal(edge.line);
  const hasFactors = !!(edge.reason_factors && edge.reason_factors.length);
  return (
    <>
    <div className="ml-row">
      <div className="cell-subject">
        <div className="subject-main">{edge.team_code} @ {edge.opp_team_code}</div>
        <div className="subject-sub">{edge.notes||''}</div>
        {edge.reason_short && <div className="reason-short">{edge.reason_short}</div>}
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
      <div>
        <span className="tier-pill">T{tier}</span>
        <ReasonToggle open={open} onClick={()=>setOpen(!open)} hasFactors={hasFactors} />
      </div>
    </div>
    {open && hasFactors && <ReasonDetail factors={edge.reason_factors} />}
    </>
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

// ============================================================================
// Stats view — league-wide phonebook for pitchers, hitters, teams
// ============================================================================
const STATS_SUB_TABS = ['Pitchers', 'Hitters', 'Teams'];

function StatsView() {
  const [sub, setSub] = useState('Pitchers');
  const [pitchers, setPitchers] = useState(null);
  const [hitters, setHitters]   = useState(null);
  const [teams, setTeams]       = useState(null);
  const [error, setError]       = useState(null);

  useEffect(() => {
    if (sub === 'Pitchers' && pitchers === null) {
      fetch(`${API_BASE}/api/stats/pitchers`).then(r=>r.json()).then(d=>setPitchers(d.pitchers||[])).catch(e=>setError(e.message));
    } else if (sub === 'Hitters' && hitters === null) {
      fetch(`${API_BASE}/api/stats/hitters`).then(r=>r.json()).then(d=>setHitters(d.hitters||[])).catch(e=>setError(e.message));
    } else if (sub === 'Teams' && teams === null) {
      fetch(`${API_BASE}/api/stats/teams`).then(r=>r.json()).then(d=>setTeams(d.teams||[])).catch(e=>setError(e.message));
    }
  }, [sub]);

  return (
    <section>
      <div className="section-header">
        <h2>Stats.</h2>
        <span className="deck">Season-long Statcast data &middot; pitchers, hitters, teams</span>
      </div>
      <div className="prop-cat-tabs">
        {STATS_SUB_TABS.map(t => (
          <button key={t} className={`prop-cat-tab ${sub===t?'active':''}`} onClick={()=>setSub(t)}>{t}</button>
        ))}
      </div>
      {error && <div className="empty">Error loading stats: {error}</div>}
      {sub==='Pitchers' && (pitchers===null ? <div className="loading">Loading pitchers</div> : <PitcherStatsTable rows={pitchers}/>)}
      {sub==='Hitters'  && (hitters===null  ? <div className="loading">Loading hitters</div>  : <HitterStatsTable rows={hitters}/>)}
      {sub==='Teams'    && (teams===null    ? <div className="loading">Loading teams</div>    : <TeamStatsTable rows={teams}/>)}
    </section>
  );
}

function StatsTable({ rows, columns, defaultSort, defaultDir='desc', initialSearch='' }) {
  const [sortKey, setSortKey] = useState(defaultSort);
  const [sortDir, setSortDir] = useState(defaultDir);
  const [search, setSearch]   = useState(initialSearch);

  function clickHeader(col) {
    if (col.key === sortKey) { setSortDir(sortDir==='asc'?'desc':'asc'); }
    else { setSortKey(col.key); setSortDir(col.type==='number'?'desc':'asc'); }
  }

  const filtered = !search ? rows : rows.filter(r => {
    const q = search.toLowerCase();
    return columns.some(c => {
      const v = r[c.key];
      return v != null && String(v).toLowerCase().includes(q);
    });
  });

  const sorted = [...filtered].sort((a,b) => {
    const va = a[sortKey], vb = b[sortKey];
    if (va == null && vb == null) return 0;
    if (va == null) return 1;
    if (vb == null) return -1;
    let cmp = (typeof va === 'number' && typeof vb === 'number') ? va - vb : String(va).localeCompare(String(vb));
    return sortDir === 'asc' ? cmp : -cmp;
  });

  const gridTemplate = columns.map(c => c.width || '1fr').join(' ');

  return (
    <>
      <div className="stats-toolbar">
        <input
          type="text"
          className="stats-search"
          placeholder="Search by name, team, etc."
          value={search}
          onChange={(e)=>setSearch(e.target.value)}
        />
        <span className="stats-count">{sorted.length} of {rows.length}</span>
      </div>
      <div className="stats-table">
        <div className="stats-thead" style={{gridTemplateColumns: gridTemplate}}>
          {columns.map(col => {
            const active = sortKey === col.key;
            const arrow = active ? (sortDir==='asc' ? ' ▲' : ' ▼') : '';
            return (
              <button
                key={col.key}
                className={`stats-th ${col.align||'left'} ${active?'active':''} ${col.sticky?'sticky':''}`}
                onClick={()=>clickHeader(col)}
              >{col.label}{arrow}</button>
            );
          })}
        </div>
        <div className="stats-tbody">
          {sorted.length === 0
            ? <div className="empty">No rows match filter.</div>
            : sorted.map((r,i) => (
              <div key={r.mlb_id || r.team_code || i} className="stats-row" style={{gridTemplateColumns: gridTemplate}}>
                {columns.map(col => {
                  const val = r[col.key];
                  const display = (col.fmt
                    ? col.fmt(val, r)
                    : (val == null ? '—'
                      : col.type === 'number' ? Number(val).toFixed(col.dp ?? 2)
                      : val));
                  return <div key={col.key} className={`stats-cell ${col.align||'left'} ${col.sticky?'sticky':''}`}>{display}</div>;
                })}
              </div>
            ))}
        </div>
      </div>
    </>
  );
}

const fmt3 = v => v==null ? '—' : Number(v).toFixed(3);
const fmt2 = v => v==null ? '—' : Number(v).toFixed(2);
const fmt0 = v => v==null ? '—' : Math.round(Number(v)).toString();

function PitcherStatsTable({ rows }) {
  const fmtPct = v => v==null ? '—' : (Number(v)*100).toFixed(1)+'%';
  const columns = [
    { key:'last_first',      label:'Pitcher',  align:'left',  type:'string', width:'minmax(150px, 1.6fr)', sticky:true },
    { key:'pa',              label:'PA',       align:'num',   type:'number', dp:0,  width:'50px' },
    { key:'era',             label:'ERA',      align:'num',   type:'number', dp:2,  width:'55px' },
    { key:'xera',            label:'xERA',     align:'num',   type:'number', dp:2,  width:'55px' },
    { key:'xfip',            label:'xFIP',     align:'num',   type:'number', dp:2,  width:'55px' },
    { key:'est_woba',        label:'xwOBA',    align:'num',   type:'number', fmt:fmt3, width:'62px' },
    { key:'babip',           label:'BABIP',    align:'num',   type:'number', fmt:fmt3, width:'62px' },
    { key:'k_pct',           label:'K%',       align:'num',   type:'number', fmt:fmtPct, width:'58px' },
    { key:'bb9',             label:'BB/9',     align:'num',   type:'number', dp:2,  width:'58px' },
    { key:'gb_pct',          label:'GB%',      align:'num',   type:'number', fmt:fmtPct, width:'58px' },
    { key:'fb_pct',          label:'FB%',      align:'num',   type:'number', fmt:fmtPct, width:'58px' },
    { key:'avg_exit_velo',   label:'EV',       align:'num',   type:'number', dp:1,  width:'55px' },
    { key:'hard_hit_pct',    label:'Hard%',    align:'num',   type:'number', fmt:fmtPct, width:'62px' },
    { key:'barrel_pct',      label:'Brl%',     align:'num',   type:'number', fmt:fmtPct, width:'58px' },
    { key:'launch_angle_avg',label:'LA',       align:'num',   type:'number', dp:1,  width:'50px' },
    { key:'__splits',        label:'',         align:'num',   type:'string', width:'68px',
      fmt: (_, row) => <PitcherSplitsToggle row={row}/> },
  ];
  return <StatsTable rows={rows} columns={columns} defaultSort="pa" defaultDir="desc" />;
}

function PitcherSplitsToggle({ row }) {
  const [open, setOpen] = useState(false);
  const splits = row.splits || {};
  const hasSplits = ['vsL','vsR','home','away'].some(k => splits[k]);
  if (!hasSplits) return <span className="splits-na">—</span>;
  return (
    <>
      <button className={'splits-toggle' + (open ? ' open' : '')} onClick={()=>setOpen(!open)}>
        {open ? 'Hide' : 'Splits'}
      </button>
      {open && <PitcherSplitsPanel splits={splits}/>}
    </>
  );
}

function PitcherSplitsPanel({ splits }) {
  const keys = [
    { k:'vsL',  label:'vs LHB' },
    { k:'vsR',  label:'vs RHB' },
    { k:'home', label:'Home'   },
    { k:'away', label:'Away'   },
  ];
  return (
    <div className="splits-panel">
      <table className="splits-table">
        <thead>
          <tr>
            <th>Split</th><th className="num">BF</th><th className="num">IP</th>
            <th className="num">ERA</th><th className="num">WHIP</th>
            <th className="num">AVG</th><th className="num">OPS</th>
            <th className="num">K%</th><th className="num">BB%</th>
          </tr>
        </thead>
        <tbody>
          {keys.map(({k, label}) => {
            const s = splits[k];
            if (!s) return null;
            return (
              <tr key={k}>
                <td>{label}</td>
                <td className="num">{s.pa ?? '—'}</td>
                <td className="num">{s.ip != null ? Number(s.ip).toFixed(1) : '—'}</td>
                <td className="num">{s.era != null ? Number(s.era).toFixed(2) : '—'}</td>
                <td className="num">{s.whip != null ? Number(s.whip).toFixed(2) : '—'}</td>
                <td className="num">{s.avg_against != null ? Number(s.avg_against).toFixed(3) : '—'}</td>
                <td className="num">{s.ops_against != null ? Number(s.ops_against).toFixed(3) : '—'}</td>
                <td className="num">{s.k_pct != null ? (Number(s.k_pct)*100).toFixed(1)+'%' : '—'}</td>
                <td className="num">{s.bb_pct != null ? (Number(s.bb_pct)*100).toFixed(1)+'%' : '—'}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function HitterStatsTable({ rows }) {
  const columns = [
    { key:'last_first', label:'Hitter',   align:'left', type:'string', width:'minmax(160px, 2fr)' },
    { key:'pa',         label:'PA',       align:'num',  type:'number', dp:0, width:'70px' },
    { key:'ba',         label:'BA',       align:'num',  type:'number', fmt:fmt3, width:'80px' },
    { key:'est_ba',     label:'xBA',      align:'num',  type:'number', fmt:fmt3, width:'80px' },
    { key:'slg',        label:'SLG',      align:'num',  type:'number', fmt:fmt3, width:'80px' },
    { key:'est_slg',    label:'xSLG',     align:'num',  type:'number', fmt:fmt3, width:'80px' },
    { key:'woba',       label:'wOBA',     align:'num',  type:'number', fmt:fmt3, width:'80px' },
    { key:'est_woba',   label:'xwOBA',    align:'num',  type:'number', fmt:fmt3, width:'80px' },
    { key:'l15_woba',   label:'L15 wOBA', align:'num',  type:'number', fmt:fmt3, width:'90px' },
    { key:'vs_L_woba',  label:'vs LHP',   align:'num',  type:'number', fmt:fmt3, width:'80px' },
    { key:'vs_R_woba',  label:'vs RHP',   align:'num',  type:'number', fmt:fmt3, width:'80px' },
  ];
  return <StatsTable rows={rows} columns={columns} defaultSort="est_woba" defaultDir="desc" />;
}

function TeamStatsTable({ rows }) {
  const columns = [
    { key:'team_code',       label:'Team',         align:'left', type:'string', width:'minmax(80px, 1fr)' },
    { key:'est_woba',        label:'xwOBA',        align:'num',  type:'number', fmt:fmt3, width:'100px' },
    { key:'l5_woba',         label:'L5 wOBA',      align:'num',  type:'number', fmt:fmt3, width:'100px' },
    { key:'bullpen_era',     label:'BP ERA',       align:'num',  type:'number', dp:2, width:'100px' },
    { key:'bullpen_ip',      label:'BP IP',        align:'num',  type:'number', dp:1, width:'100px' },
    { key:'bullpen_era_l7',  label:'BP L7 ERA',    align:'num',  type:'number', dp:2, width:'110px' },
    { key:'bullpen_ip_l7',   label:'BP L7 IP',     align:'num',  type:'number', dp:1, width:'110px' },
  ];
  return <StatsTable rows={rows} columns={columns} defaultSort="est_woba" defaultDir="desc" />;
}

// ============================================================================
// Admin panel v2 — triple-click masthead to reveal; combines triggers + diag
// No password. Hidden by obscurity. ADMIN_TOKEN env var still required.
// ============================================================================
const ADMIN_TRIGGERS = [
  { name: 'Orchestrator',         endpoint: '/api/admin/trigger/orchestrator',         desc: 'Re-run today\'s slate projection + edges' },
  { name: 'Statcast Refresh',     endpoint: '/api/admin/trigger/statcast',             desc: 'Refresh hitter/pitcher/team xstats' },
  { name: 'Grader',               endpoint: '/api/admin/trigger/grader',               desc: 'Grade yesterday\'s flagged edges' },
];

const ADMIN_DIAGNOSTICS = [
  { name: 'Index',                endpoint: '/api/admin/diag/index',                   desc: 'List of all diagnostic endpoints' },
  { name: 'xstats',               endpoint: '/api/admin/diag/xstats',                  desc: 'xstats table state + LEAGUE_XWOBA + last refresh' },
  { name: 'Projection Bias',      endpoint: '/api/admin/diag/projection_bias',         desc: '14-day projection vs market drift' },
  { name: 'Edges (today)',        endpoint: '/api/admin/diag/edges',                   desc: 'Flagged edges by kind/lean for today' },
  { name: 'Games (today)',        endpoint: '/api/admin/diag/games',                   desc: 'Games + projections + F5 cols' },
  { name: 'Pitcher Projections',  endpoint: '/api/admin/diag/pitcher_projections',     desc: '14-day pitcher projection summary' },
  { name: 'Weather Check',        endpoint: '/api/admin/diag/weather_check',           desc: 'Weather vs projection bias' },
  { name: 'Jobs',                 endpoint: '/api/admin/diag/jobs',                    desc: 'Recent job_runs entries' },
  { name: 'Hitter Distribution',  endpoint: '/api/admin/diag/hitter_dist',             desc: 'Hitter xstats summary' },
  { name: 'Top Hitters',          endpoint: '/api/admin/diag/hitters_top',             desc: 'Top 30 hitters by xwOBA' },
  { name: 'Bottom Hitters',       endpoint: '/api/admin/diag/hitters_bottom',          desc: 'Bottom 30 hitters' },
  { name: 'Team Bullpens',        endpoint: '/api/admin/diag/team_bullpens',           desc: 'All teams bullpen ERA + L7' },
  { name: 'Pitcher Distribution', endpoint: '/api/admin/diag/pitcher_dist',            desc: 'Pitcher xstats summary' },
  { name: 'Savant Pitcher CSV',   endpoint: '/api/admin/diag/savant_pitcher_csv',      desc: 'Inspect raw Savant CSV columns' },
];

const ADMIN_MISC = [
  { name: 'Recompute Reasoning',  endpoint: '/api/admin/recompute_reasoning',          desc: 'Re-run reasoning for today\'s edges' },
  { name: 'Zero Prop Units',      endpoint: '/api/admin/zero_prop_units',               desc: 'Set profit_units=0 for prop edges' },
];

function AdminPanelV2({ visible, onClose }) {
  const [results, setResults] = useState({});  // { endpoint: { state, data } }

  async function call(endpoint, name) {
    if (!ADMIN_TOKEN) {
      setResults(r => ({ ...r, [endpoint]: { state: 'error', data: 'ADMIN_TOKEN env var missing (VITE_ADMIN_TOKEN)' }}));
      return;
    }
    setResults(r => ({ ...r, [endpoint]: { state: 'running', data: 'Calling...' }}));
    try {
      const url = `${API_BASE}${endpoint}/${ADMIN_TOKEN}`;
      const r = await fetch(url);
      const text = await r.text();
      let parsed;
      try { parsed = JSON.parse(text); } catch { parsed = text; }
      if (!r.ok) {
        setResults(s => ({ ...s, [endpoint]: { state: 'error', data: parsed }}));
        return;
      }
      setResults(s => ({ ...s, [endpoint]: { state: 'success', data: parsed }}));
    } catch (err) {
      setResults(s => ({ ...s, [endpoint]: { state: 'error', data: err.message }}));
    }
  }

  if (!visible) return null;

  return (
    <div className="admin-v2">
      <div className="admin-v2-header">
        <span className="admin-v2-title">Admin</span>
        <button className="admin-v2-close" onClick={onClose}>Close</button>
      </div>

      <AdminGroup label="Triggers" items={ADMIN_TRIGGERS} results={results} onCall={call} note="These take 30-90s to complete." />
      <AdminGroup label="Diagnostics" items={ADMIN_DIAGNOSTICS} results={results} onCall={call} />
      <AdminGroup label="Misc" items={ADMIN_MISC} results={results} onCall={call} />
    </div>
  );
}

function AdminGroup({ label, items, results, onCall, note }) {
  return (
    <div className="admin-group">
      <h3 className="admin-group-label">{label}</h3>
      {note && <p className="admin-group-note">{note}</p>}
      <div className="admin-group-list">
        {items.map(item => {
          const res = results[item.endpoint];
          return (
            <div key={item.endpoint} className="admin-item">
              <div className="admin-item-row">
                <button
                  className={`admin-item-btn admin-state-${res?.state || 'idle'}`}
                  onClick={() => onCall(item.endpoint, item.name)}
                  disabled={res?.state === 'running'}
                >
                  {res?.state === 'running' ? '...' : item.name}
                </button>
                <span className="admin-item-desc">{item.desc}</span>
              </div>
              {res?.data !== undefined && (
                <pre className={`admin-item-output admin-state-${res.state}`}>
                  {typeof res.data === 'string' ? res.data : JSON.stringify(res.data, null, 2)}
                </pre>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ============================================================================
// Personal Bets — modal to enter $ + juice + book, button on each edge
// ============================================================================
function BetButton({ edge, onChange }) {
  const [bet, setBet] = useState(undefined);  // undefined=unknown, null=not bet, obj=bet exists
  const [open, setOpen] = useState(false);

  useEffect(() => {
    // On mount, fetch bets to see if this edge has one
    fetch(`${API_BASE}/api/personal_bets`).then(r=>r.json()).then(d => {
      const found = (d.bets || []).find(b => b.edge_id === edge.edge_id);
      setBet(found || null);
    });
  }, [edge.edge_id]);

  function handleSaved(saved) {
    setBet(saved);
    setOpen(false);
    if (onChange) onChange();
  }

  async function handleDelete() {
    if (!bet?.bet_id) return;
    if (!window.confirm('Remove this bet from your record?')) return;
    await fetch(`${API_BASE}/api/personal_bets/${bet.bet_id}`, { method: 'DELETE' });
    setBet(null);
    if (onChange) onChange();
  }

  const hasBet = bet && bet.bet_id;

  return (
    <>
      <button
        className={`bet-btn ${hasBet ? 'has-bet' : ''}`}
        onClick={() => setOpen(true)}
        title={hasBet ? `$${bet.dollar_amount} @ ${bet.juice > 0 ? '+' : ''}${bet.juice}` : 'Add to my bets'}
      >
        {hasBet ? `\u2713 $${Number(bet.dollar_amount).toFixed(0)}` : '+ Bet'}
      </button>
      {open && (
        <BetModal
          edge={edge}
          existing={hasBet ? bet : null}
          onSave={handleSaved}
          onDelete={hasBet ? handleDelete : null}
          onClose={() => setOpen(false)}
        />
      )}
    </>
  );
}

function BetModal({ edge, existing, onSave, onDelete, onClose }) {
  const [amount, setAmount]   = useState(existing?.dollar_amount ?? 100);
  const [juice, setJuice]     = useState(existing?.juice ?? -110);
  const [book, setBook]       = useState(existing?.sportsbook ?? '');
  const [notes, setNotes]     = useState(existing?.notes ?? '');
  const [saving, setSaving]   = useState(false);
  const [error, setError]     = useState(null);

  async function handleSave(e) {
    e.preventDefault();
    setSaving(true);
    setError(null);
    try {
      const body = {
        edge_id: edge.edge_id,
        dollar_amount: parseFloat(amount),
        juice: parseInt(juice, 10),
        sportsbook: book || null,
        notes: notes || null,
      };
      const r = await fetch(`${API_BASE}/api/personal_bets`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!r.ok) {
        const txt = await r.text();
        setError(`HTTP ${r.status}: ${txt.slice(0,200)}`);
        return;
      }
      onSave({ ...body, bet_id: (await r.json()).bet_id });
    } catch (err) {
      setError(err.message);
    } finally {
      setSaving(false);
    }
  }

  const description = `${edge.kind?.toUpperCase()} \u00b7 ${edge.lean} ${edge.line}`;

  return (
    <div className="bet-modal-backdrop" onClick={onClose}>
      <form className="bet-modal" onClick={e => e.stopPropagation()} onSubmit={handleSave}>
        <div className="bet-modal-header">
          <h3>{existing ? 'Edit Bet' : 'Add Bet'}</h3>
          <button type="button" className="bet-modal-close" onClick={onClose}>\u00d7</button>
        </div>
        <div className="bet-modal-edge">{description}</div>

        <label className="bet-field">
          <span>$ Amount</span>
          <input type="number" step="0.01" min="0" value={amount}
                 onChange={e => setAmount(e.target.value)} autoFocus required />
        </label>

        <label className="bet-field">
          <span>Juice / Odds</span>
          <input type="number" value={juice}
                 onChange={e => setJuice(e.target.value)} required
                 placeholder="-110, +130, etc." />
        </label>

        <label className="bet-field">
          <span>Sportsbook</span>
          <input type="text" value={book}
                 onChange={e => setBook(e.target.value)}
                 placeholder="DK / FD / BetMGM / etc." />
        </label>

        <label className="bet-field">
          <span>Notes</span>
          <textarea value={notes} onChange={e => setNotes(e.target.value)} rows="2"
                    placeholder="optional" />
        </label>

        {error && <div className="bet-modal-error">{error}</div>}

        <div className="bet-modal-actions">
          {existing && onDelete && (
            <button type="button" className="bet-modal-delete" onClick={onDelete}>Remove</button>
          )}
          <div style={{ flex: 1 }} />
          <button type="button" className="bet-modal-cancel" onClick={onClose}>Cancel</button>
          <button type="submit" className="bet-modal-save" disabled={saving}>
            {saving ? 'Saving...' : (existing ? 'Update' : 'Save')}
          </button>
        </div>
      </form>
    </div>
  );
}


// ============================================================================
// My Record view — same shape as Track Record but personal_bets only
// ============================================================================
function MyRecordView() {
  const [summary, setSummary] = useState(null);
  const [bets, setBets]       = useState(null);
  const [error, setError]     = useState(null);

  useEffect(() => {
    Promise.all([
      fetch(`${API_BASE}/api/personal_bets/summary`).then(r => r.json()),
      fetch(`${API_BASE}/api/personal_bets`).then(r => r.json()),
    ]).then(([s, b]) => {
      setSummary(s);
      setBets(b.bets || []);
    }).catch(e => setError(e.message));
  }, []);

  if (error) return <div className="empty">Error: {error}</div>;
  if (!summary || !bets) return <div className="loading">Loading your bets</div>;

  return (
    <section>
      <div className="section-header">
        <h2>My Record.</h2>
        <span className="deck">Personal $ wagers on flagged edges</span>
      </div>

      <div className="my-record-cards">
        <div className="my-record-card">
          <div className="card-label">Bets</div>
          <div className="card-value">{summary.n_bets}</div>
          <div className="card-sub">{summary.wins}W &middot; {summary.losses}L &middot; {summary.pushes}P</div>
        </div>
        <div className="my-record-card">
          <div className="card-label">Staked</div>
          <div className="card-value">${summary.total_staked.toFixed(2)}</div>
        </div>
        <div className={`my-record-card ${summary.cumulative_pnl >= 0 ? 'card-positive' : 'card-negative'}`}>
          <div className="card-label">P&L</div>
          <div className="card-value">{summary.cumulative_pnl >= 0 ? '+' : ''}${summary.cumulative_pnl.toFixed(2)}</div>
        </div>
        <div className="my-record-card">
          <div className="card-label">ROI</div>
          <div className="card-value">{(summary.roi * 100).toFixed(1)}%</div>
        </div>
        {summary.pending > 0 && (
          <div className="my-record-card">
            <div className="card-label">Pending</div>
            <div className="card-value">{summary.pending}</div>
          </div>
        )}
      </div>

      {summary.days.length === 0
        ? <div className="empty">No bets placed yet. Pick edges from the slate and add $ amounts.</div>
        : (
          <>
            <h3 className="my-record-section-title">By Date</h3>
            <table className="my-record-table">
              <thead>
                <tr>
                  <th>Date</th><th className="num">Bets</th><th className="num">Record</th>
                  <th className="num">Staked</th><th className="num">P&L</th>
                </tr>
              </thead>
              <tbody>
                {summary.days.map(d => (
                  <tr key={d.run_date}>
                    <td>{d.run_date}</td>
                    <td className="num">{d.n_bets}</td>
                    <td className="num">{d.wins}-{d.losses}{d.pushes ? '-' + d.pushes : ''}{d.pending ? ` (${d.pending} pending)` : ''}</td>
                    <td className="num">${d.staked.toFixed(2)}</td>
                    <td className={`num ${d.pnl >= 0 ? 'pnl-positive' : 'pnl-negative'}`}>
                      {d.pnl >= 0 ? '+' : ''}${d.pnl.toFixed(2)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>

            <h3 className="my-record-section-title">All Bets</h3>
            <table className="my-record-table">
              <thead>
                <tr>
                  <th>Date</th><th>Game</th><th>Bet</th>
                  <th className="num">Stake</th><th className="num">Juice</th>
                  <th>Book</th><th>Result</th><th className="num">P&L</th>
                </tr>
              </thead>
              <tbody>
                {bets.map(b => (
                  <tr key={b.bet_id}>
                    <td>{b.run_date}</td>
                    <td>{b.away_team}@{b.home_team}</td>
                    <td>{b.kind?.toUpperCase()} {b.lean} {b.line}</td>
                    <td className="num">${Number(b.dollar_amount).toFixed(2)}</td>
                    <td className="num">{b.juice > 0 ? '+' : ''}{b.juice}</td>
                    <td>{b.sportsbook || '\u2014'}</td>
                    <td className={`result-${(b.result || '').toLowerCase()}`}>
                      {b.result || 'pending'}
                    </td>
                    <td className={`num ${b.dollar_pnl == null ? '' : (b.dollar_pnl >= 0 ? 'pnl-positive' : 'pnl-negative')}`}>
                      {b.dollar_pnl == null ? '\u2014' : (b.dollar_pnl >= 0 ? '+$' : '-$') + Math.abs(b.dollar_pnl).toFixed(2)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </>
        )
      }
    </section>
  );
}

