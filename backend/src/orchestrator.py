"""Daily orchestrator v4.1 â€” prop prices stored on edges for correct grading."""
from __future__ import annotations
import argparse, logging, math, traceback
from dataclasses import asdict
from datetime import date, datetime, timezone, timedelta
from typing import Optional

from . import db, mlb_api, projections, ntfy, odds_props
from .odds import attach_odds_to_games
from .weather import enrich_weather_for_game
from .park_factors import get_park_for_team

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("orchestrator")
MODEL_VERSION = "v4.1"
_DK_LINES: dict = {}
EDGE_THRESHOLDS = {"Total":0.50,"F5":0.35,"ML":0.10,"K":0.50,"Hits":0.70,"ER":0.50,"Outs":0.70}

def american_to_implied(o): return 100/(o+100) if o>0 else -o/(-o+100)
def remove_vig(a,h): t=a+h; return a/t,h/t
def poisson_tail_prob(lam,line,side):
    if lam<=0: return 0.5
    is_half=abs(line-round(line))>0.01; threshold=math.ceil(line) if side=="OVER" else math.floor(line)
    term=math.exp(-lam)
    if side=="OVER":
        cdf=0.0
        for k in range(int(threshold)): cdf+=term; term*=lam/(k+1)
        return max(0.0,min(1.0,1.0-cdf))
    else:
        upper=int(threshold) if is_half else int(threshold)-1; cdf=0.0
        for k in range(max(0,upper)+1): cdf+=term; term*=lam/(k+1)
        return max(0.0,min(1.0,cdf))

def _bessel_i(n,x,terms=40):
    n=abs(n); result=0.0; term=(x/2)**n/math.factorial(min(n,170))
    for m in range(terms):
        result+=term; denom=(m+1)*(m+1+n)
        if denom==0: break
        term*=(x/2)**2/denom
    return result

def skellam_win_prob(hr,ar):
    mu1,mu2=max(0.01,hr),max(0.01,ar); sp=2*math.sqrt(mu1*mu2)
    ef=math.exp(-(mu1+mu2)); ratio=mu1/mu2; hw=aw=tie=0.0
    for k in range(-20,21):
        try: pmf=ef*(ratio**(k/2))*_bessel_i(k,sp)
        except: continue
        if k>0: hw+=pmf
        elif k<0: aw+=pmf
        else: tie+=pmf
    hw+=tie/2; aw+=tie/2; t=hw+aw
    return (round(hw/t,4),round(aw/t,4)) if t>0 else (0.5,0.5)

def confidence_tier(edge,proj=None):
    ae=abs(edge.get("edge",0)); cat=edge.get("category","")
    if proj and proj.source!="statcast": return 3
    if cat=="Total": return 1 if ae>=1.5 else (2 if ae>=1.0 else 3)
    if cat=="F5":    return 1 if ae>=1.0 else (2 if ae>=0.6 else 3)
    if cat=="ML":
        ep=abs(edge.get("ml_edge_pct") or 0); return 1 if ep>=0.08 else (2 if ep>=0.05 else 3)
    return 1 if ae>=2.0 else (2 if ae>=1.0 else 3)

def compute_edges_for_game(*,game_pk,game,away_proj,home_proj,
    market_total,market_f5_total,away_ml,home_ml,
    full_total,f5_total,home_runs,away_runs,home_win_prob,away_win_prob,
    game_row=None,away_team_xstats=None,home_team_xstats=None):
    edges=[]; gr=game_row or {}

    if market_total is not None:
        diff=full_total-market_total
        if abs(diff)>=EDGE_THRESHOLDS["Total"]:
            lean="OVER" if diff>0 else "UNDER"
            edges.append({"game_pk":game_pk,"kind":"total","category":"Total",
                "pitcher_mlb_id":None,"pitcher_name":None,
                "team_code":game.get("away_team"),"opp_team_code":game.get("home_team"),
                "line":float(market_total),"proj_value":full_total,"edge":round(diff,2),
                "lean":lean,"conviction_pct":round(poisson_tail_prob(full_total,market_total,lean)*100,1),
                "ml_edge_pct":None,"flagged":True,"notes":None,
                "over_price":gr.get("market_total_over_price"),
                "under_price":gr.get("market_total_under_price")})

    if market_f5_total is not None:
        diff_f5=f5_total-market_f5_total
        if abs(diff_f5)>=EDGE_THRESHOLDS["F5"]:
            lean_f5="OVER" if diff_f5>0 else "UNDER"
            edges.append({"game_pk":game_pk,"kind":"f5","category":"F5",
                "pitcher_mlb_id":None,"pitcher_name":None,
                "team_code":game.get("away_team"),"opp_team_code":game.get("home_team"),
                "line":float(market_f5_total),"proj_value":f5_total,"edge":round(diff_f5,2),
                "lean":lean_f5,"conviction_pct":round(poisson_tail_prob(f5_total,market_f5_total,lean_f5)*100,1),
                "ml_edge_pct":None,"flagged":True,"notes":None,
                "over_price":gr.get("market_f5_over_price"),
                "under_price":gr.get("market_f5_under_price")})

    if away_ml and home_ml and projections.ml_edge_reliable(away_proj,home_proj):
        ai,hi=remove_vig(american_to_implied(away_ml),american_to_implied(home_ml))
        hep=home_win_prob-hi; aep=away_win_prob-ai
        # Higher threshold for big underdogs (+150 or longer) — noisy projections
        def ml_threshold(odds): return 0.50 if odds is not None and odds >= 150 else EDGE_THRESHOLDS["ML"]
        if hep > 0 and hep>=ml_threshold(home_ml) and hep>=aep:
            ml_lean,ml_odds,wp,ep,oi=game.get("home_team"),home_ml,home_win_prob,hep,hi
        elif aep > 0 and aep>=ml_threshold(away_ml):
            ml_lean,ml_odds,wp,ep,oi=game.get("away_team"),away_ml,away_win_prob,aep,ai
        else: ml_lean=None
        if ml_lean:
            edges.append({"game_pk":game_pk,"kind":"ml","category":"ML",
                "pitcher_mlb_id":None,"pitcher_name":None,
                "team_code":game.get("away_team"),"opp_team_code":game.get("home_team"),
                "line":float(ml_odds),"proj_value":round(wp*100,1),"edge":round(ep*100,2),
                "lean":ml_lean,"conviction_pct":round(wp*100,1),"ml_edge_pct":round(ep,4),
                "flagged":True,"notes":f"Model {round(wp*100,1)}% vs implied {round(oi*100,1)}%",
                "over_price":int(ml_odds),"under_price":None})

    for p in (away_proj,home_proj):
        if p.source!="statcast": continue
        pitcher_dk=odds_props.lookup_lines(p.last_first,_DK_LINES) if _DK_LINES else None
        if pitcher_dk: pitcher_dk={cat:{"line":v,"over_price":None,"under_price":None} if not isinstance(v,dict) else v for cat,v in pitcher_dk.items()}
        if not pitcher_dk: continue
        proj_vals={"K":p.k,"Hits":p.hits,"ER":p.er,"Outs":p.outs}
        for category,prop_data in pitcher_dk.items():
            if category not in proj_vals: continue
            line=prop_data.get("line") if isinstance(prop_data,dict) else prop_data
            if line is None: continue
            proj_val=proj_vals[category]; diff=proj_val-float(line)
            if abs(diff)<EDGE_THRESHOLDS.get(category,0.5): continue
            lean="OVER" if diff>0 else "UNDER"
            # ER props: use Poisson probability as primary gate (not raw edge size)
            # Single-game ER is high variance — need >60% probability to fire
            if category == "ER":
                conviction = poisson_tail_prob(proj_val, float(line), lean)
                if conviction < 0.60: continue
                # Override the raw edge threshold for ER — Poisson gate is sufficient
                diff = proj_val - float(line)  # recalc to ensure correct sign
            over_price =prop_data.get("over_price")  if isinstance(prop_data,dict) else None
            under_price=prop_data.get("under_price") if isinstance(prop_data,dict) else None
            edges.append({"game_pk":game_pk,"kind":"prop","category":category,
                "pitcher_mlb_id":p.pitcher_mlb_id,"pitcher_name":p.last_first,
                "team_code":p.team_code,"opp_team_code":p.opp_team_code,
                "line":float(line),"proj_value":round(proj_val,2),"edge":round(diff,2),
                "lean":lean,"conviction_pct":round(poisson_tail_prob(proj_val,float(line),lean)*100,1),
                "ml_edge_pct":None,"flagged":True,"notes":None,
                "over_price":int(over_price)   if over_price  is not None else None,
                "under_price":int(under_price) if under_price is not None else None})
    return edges

def persist_game(g):
    weather={}
    try: weather=enrich_weather_for_game(g)
    except Exception as e: log.debug("Weather failed: %s",e); weather={}
    db.upsert_game({"game_pk":g.game_pk,
        "game_date":g.game_date_et if g.game_date_et else None,
        "game_time_et":g.game_time_et,"status":g.status,
        "away_team":g.away_team,"home_team":g.home_team,
        "away_record":g.away_record,"home_record":g.home_record,
        "park_code":get_park_for_team(g.home_team),
        "away_pitcher_id":g.away_pitcher.mlb_id if g.away_pitcher else None,
        "home_pitcher_id":g.home_pitcher.mlb_id if g.home_pitcher else None,
        "away_pitcher_hand":g.away_pitcher.hand if g.away_pitcher else None,
        "home_pitcher_hand":g.home_pitcher.hand if g.home_pitcher else None,
        "away_pitcher_name":g.away_pitcher.last_first if g.away_pitcher else None,
        "home_pitcher_name":g.home_pitcher.last_first if g.home_pitcher else None,
        "away_score":g.away_score,"home_score":g.home_score,
        "weather_condition":weather.get("condition"),"weather_temp_f":weather.get("temp_f"),
        "weather_wind":weather.get("wind_raw"),"weather_wind_mph":weather.get("wind_mph"),
        "weather_wind_deg":weather.get("wind_deg"),"weather_precip_pct":weather.get("precip_pct")})
    if g.away_lineup:
        db.replace_lineups(g.game_pk,g.away_team,[
            {"batting_order":s.order,"mlb_id":s.mlb_id,"full_name":s.full_name,
             "last_first":s.last_first,"bat_side":s.bat_side,"position":s.position} for s in g.away_lineup])
    if g.home_lineup:
        db.replace_lineups(g.game_pk,g.home_team,[
            {"batting_order":s.order,"mlb_id":s.mlb_id,"full_name":s.full_name,
             "last_first":s.last_first,"bat_side":s.bat_side,"position":s.position} for s in g.home_lineup])

def run(trigger="manual"):
    job_id=db.log_job_start(f"orchestrator:{trigger}")
    metrics={"trigger":trigger,"errors":[]}
    try:
        et_now = datetime.now(timezone.utc) - timedelta(hours=4)
        run_date = et_now.date().isoformat()
        games=mlb_api.get_schedule(target_date=et_now.date())
        active=[g for g in games if g.status in ("Scheduled","Pre-Game","Warmup","Delayed Start")]
        metrics["n_games"]=len(active); log.info("%d active games",len(active))
        for g in active: persist_game(g)
        try: attach_odds_to_games(active)
        except Exception as e: log.warning("Odds attach failed: %s",e)
        global _DK_LINES
        try: _DK_LINES=odds_props.fetch_pitcher_props_for_today()
        except Exception as e: log.warning("DK props failed: %s",e); _DK_LINES={}
        run_id=db.create_projection_run(run_date,MODEL_VERSION,trigger,len(active))
        metrics["run_id"]=run_id; season=et_now.year
        all_pit={r["mlb_id"]:r for r in db.fetchall("SELECT * FROM pitcher_xstats WHERE season_year=%s",(season,))}
        all_hit={r["mlb_id"]:r for r in db.fetchall("SELECT * FROM hitter_xstats WHERE season_year=%s",(season,))}
        for sr in db.fetchall("SELECT mlb_id,vs_hand,pa,est_woba FROM hitter_splits WHERE season_year=%s",(season,)):
            row=all_hit.get(sr["mlb_id"])
            if row: row.setdefault("splits",{})[sr["vs_hand"]]={"pa":sr["pa"],"est_woba":sr["est_woba"]}
        log.info("Loaded %d hitters, %d with splits",len(all_hit),sum(1 for r in all_hit.values() if "splits" in r))
        all_team={r["team_code"]:r for r in db.fetchall("SELECT * FROM team_xstats WHERE season_year=%s",(season,))}
        all_parks={r["park_code"]:r for r in db.fetchall("SELECT * FROM parks WHERE season_year=%s",(season,))}
        all_edges=[]; n_lu=n_fb=0
        for g in active:
            if not g.away_pitcher or not g.home_pitcher:
                metrics["skipped_no_pitcher"]=metrics.get("skipped_no_pitcher",0)+1; continue
            if len(g.away_lineup or [])<9 or len(g.home_lineup or [])<9:
                metrics["skipped_no_lineup"]=metrics.get("skipped_no_lineup",0)+1; continue
            db_row=db.fetchone("SELECT skip_projection FROM games WHERE game_pk=%s",(g.game_pk,))
            if db_row and db_row.get("skip_projection"):
                metrics["skipped_manual"]=metrics.get("skipped_manual",0)+1; continue
            park=all_parks.get(get_park_for_team(g.home_team)) or {}
            game_row=db.fetchone("SELECT * FROM games WHERE game_pk=%s",(g.game_pk,)) or {}
            weather={"temp_f":game_row.get("weather_temp_f"),"wind_mph":game_row.get("weather_wind_mph"),"wind_deg":game_row.get("weather_wind_deg")}
            market_total=float(game_row["market_total"]) if game_row.get("market_total") else None
            market_f5_total=float(game_row["market_f5_total"]) if game_row.get("market_f5_total") else None
            away_ml=game_row.get("away_ml"); home_ml=game_row.get("home_ml")
            away_proj=home_proj=None
            for is_home in (False,True):
                pi=g.home_pitcher if is_home else g.away_pitcher
                team=g.home_team if is_home else g.away_team
                opp=g.away_team if is_home else g.home_team
                opp_lu=g.away_lineup if is_home else g.home_lineup
                opp_lu_in=[projections.HitterSpot(mlb_id=s.mlb_id,last_first=s.last_first,bat_side=s.bat_side,order=s.order) for s in opp_lu]
                xwoba_fb=float((all_team.get(opp) or {}).get("est_woba") or projections.LEAGUE_XWOBA)
                proj=projections.project_pitcher(pitcher_xstats=all_pit.get(pi.mlb_id),
                    pitcher_mlb_id=pi.mlb_id,pitcher_name=pi.last_first,pitcher_hand=pi.hand,
                    team_code=team,opp_team_code=opp,opp_lineup=opp_lu_in,hitter_xstats=all_hit,
                    team_xwoba_fallback=xwoba_fb,park=park,
                    weather={} if (park.get("roof_type") or "").lower() in ("dome","closed") else weather)
                if proj.source!="statcast": n_fb+=1
                if proj.used_actual_lineup: n_lu+=1
                pd=proj.to_dict(); pd["mlb_id"]=pd.pop("pitcher_mlb_id"); pd["game_pk"]=g.game_pk
                db.insert_pitcher_projection(run_id,pd)
                if is_home: home_proj=proj
                else: away_proj=proj
            full_total,f5_total,home_runs,away_runs=projections.project_game_total(
                away_proj=away_proj,home_proj=home_proj,
                away_team_xstats=all_team.get(g.away_team),home_team_xstats=all_team.get(g.home_team),
                park=park,weather=weather)
            raw_hw,raw_aw=skellam_win_prob(home_runs,away_runs)
            home_win_prob,away_win_prob=projections.apply_hfa(raw_hw,raw_aw)
            hfa_applied=projections.HOME_FIELD_ADVANTAGE
            edge_total=round(full_total-market_total,2) if market_total else None
            edge_f5=round(f5_total-market_f5_total,2) if market_f5_total else None
            lean=("OVER" if edge_total>0 else "UNDER") if edge_total else "PASS"
            lean_f5=("OVER" if edge_f5>0 else "UNDER") if edge_f5 else "PASS"
            away_ml_implied=home_ml_implied=ml_edge_team=ml_edge_pct=None
            if away_ml and home_ml:
                ai,hi=remove_vig(american_to_implied(away_ml),american_to_implied(home_ml))
                away_ml_implied,home_ml_implied=round(ai,4),round(hi,4)
                hep=home_win_prob-hi; aep=away_win_prob-ai
                if abs(hep)>=EDGE_THRESHOLDS["ML"] and hep>=aep: ml_edge_team,ml_edge_pct=g.home_team,round(hep,4)
                elif abs(aep)>=EDGE_THRESHOLDS["ML"]: ml_edge_team,ml_edge_pct=g.away_team,round(aep,4)
            db.insert_game_projection(run_id,{"game_pk":g.game_pk,"proj_total":full_total,
                "proj_f5":f5_total,"proj_home_runs":home_runs,"proj_away_runs":away_runs,
                "market_total":market_total,"edge_total":edge_total,"lean":lean,"confidence_tier":None,
                "market_f5_total":market_f5_total,"edge_f5":edge_f5,"lean_f5":lean_f5,
                "home_win_prob":home_win_prob,"away_win_prob":away_win_prob,
                "away_ml":away_ml,"home_ml":home_ml,
                "away_ml_implied":away_ml_implied,"home_ml_implied":home_ml_implied,
                "ml_edge_team":ml_edge_team,"ml_edge_pct":ml_edge_pct,"hfa_applied":hfa_applied})
            game_edges = compute_edges_for_game(game_pk=g.game_pk,game=asdict(g),
                away_proj=away_proj,home_proj=home_proj,
                market_total=market_total,market_f5_total=market_f5_total,
                away_ml=away_ml,home_ml=home_ml,full_total=full_total,f5_total=f5_total,
                home_runs=home_runs,away_runs=away_runs,
                home_win_prob=home_win_prob,away_win_prob=away_win_prob,
                game_row=game_row,away_team_xstats=all_team.get(g.away_team),
                home_team_xstats=all_team.get(g.home_team))
            # Correlated edges: F5 + Full Game same direction -> 0.5u each
            _total_e = [e for e in game_edges if e["kind"]=="total"]
            _f5_e    = [e for e in game_edges if e["kind"]=="f5"]
            for _te in _total_e:
                for _fe in _f5_e:
                    if _te["lean"] == _fe["lean"]:
                        _te["stake_units"] = 0.5
                        _fe["stake_units"] = 0.5
                        log.info("Correlated F5+Full: %s @ %s %s -> 0.5u each",
                                 _te["team_code"], _te["opp_team_code"], _te["lean"])
            for e in game_edges:
                e.setdefault("stake_units", 1.0)
                pft=(away_proj if e.get("pitcher_mlb_id")==(away_proj.pitcher_mlb_id if away_proj else None)
                     else (home_proj if e.get("pitcher_mlb_id")==(home_proj.pitcher_mlb_id if home_proj else None) else None))
                e["confidence_tier"]=confidence_tier(e,pft)
                db.insert_edge(run_id,e); all_edges.append(e)
        all_edges.sort(key=lambda x:abs(x["edge"]),reverse=True)
        metrics.update({"n_edges":len(all_edges),
            "n_f5_edges":sum(1 for e in all_edges if e["kind"]=="f5"),
            "n_ml_edges":sum(1 for e in all_edges if e["kind"]=="ml"),
            "n_prop_edges":sum(1 for e in all_edges if e["kind"]=="prop"),
            "n_lineups_confirmed":n_lu,"n_fallback_pitchers":n_fb})
        log.info("Run %d: %d edges (%d F5, %d ML, %d props)",run_id,
                 metrics["n_edges"],metrics["n_f5_edges"],metrics["n_ml_edges"],metrics["n_prop_edges"])
        ntfy.send_edges_summary(run_id,all_edges,metrics)
        db.log_job_finish(job_id,"success",None,metrics)
        return metrics
    except Exception as e:
        log.error("Orchestrator failed: %s\n%s",e,traceback.format_exc())
        db.log_job_finish(job_id,"failure",str(e),metrics)
        try: ntfy.send_failure(f"orchestrator:{trigger}",str(e))
        except: pass
        raise

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--trigger",default="manual")
    print(run(trigger=ap.parse_args().trigger))

if __name__=="__main__": main()














