"""
ntfy push notification client.

Sends three flavors of push:
  1. edges_ready    — initial morning run finished, top picks summarized
  2. lineup_change  — projection moved meaningfully because lineup confirmed/changed
  3. failure        — any cron job hit an error (orchestrator/grader/refresh)

Topic is configured via NTFY_TOPIC env var. Server defaults to ntfy.sh but can
be overridden with NTFY_SERVER (for self-hosted instances).

We persist every notification to the `notifications` table for audit and to
prevent duplicate alerts (e.g. same line-move triggering 5x in 15 min).
"""
from __future__ import annotations
import logging
import os
from typing import Optional
import requests
from . import db

log = logging.getLogger(__name__)

NTFY_SERVER = os.environ.get("NTFY_SERVER", "https://ntfy.sh")
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "")  # set in Railway env


def _is_configured() -> bool:
    if not NTFY_TOPIC:
        log.warning("NTFY_TOPIC not set; notification skipped")
        return False
    return True


def _send(title: str, body: str, priority: str = "default",
           tags: Optional[list[str]] = None) -> bool:
    if not _is_configured():
        return False
    headers = {
        "Title": title.encode("ascii", "ignore").decode("ascii"),
        "Priority": priority,
    }
    if tags:
        headers["Tags"] = ",".join(tags)
    try:
        r = requests.post(
            f"{NTFY_SERVER}/{NTFY_TOPIC}",
            data=body.encode("utf-8"),
            headers=headers,
            timeout=10,
        )
        ok = r.status_code == 200
        # Persist for audit
        try:
            db.execute(
                """
                INSERT INTO notifications (channel, topic, title, body, delivered)
                VALUES ('ntfy', %s, %s, %s, %s)
                """,
                (NTFY_TOPIC, title, body, ok),
            )
        except Exception:
            pass   # never let logging-failure break the actual send
        return ok
    except requests.RequestException as e:
        log.warning("ntfy send failed: %s", e)
        return False


def send_edges_summary(run_id: int, edges: list[dict], metrics: dict) -> bool:
    """Edges-ready push. Top 5 by magnitude with tier."""
    title = f"MLB: {metrics.get('n_edges', 0)} edges, run {run_id}"
    lines: list[str] = []
    lines.append(
        f"{metrics.get('n_games', 0)} games · "
        f"{metrics.get('n_lineups_confirmed', 0)} lineups confirmed · "
        f"{metrics.get('n_fallback_pitchers', 0)} fallback pitchers"
    )
    lines.append("")
    top = sorted(edges, key=lambda e: abs(e["edge"]), reverse=True)[:5]
    for i, e in enumerate(top, start=1):
        tier = e.get("confidence_tier", "?")
        if e["kind"] == "total":
            lines.append(
                f"{i}. T{tier} · Total {e['lean']:5} {e['line']} → {e['proj_value']:.2f} ({e['edge']:+.2f})"
            )
        else:
            lines.append(
                f"{i}. T{tier} · {e['pitcher_name'].split(',')[0]} {e['category']} {e['lean']:5} {e['line']} → {e['proj_value']:.2f} ({e['edge']:+.2f})"
            )
    body = "\n".join(lines)
    return _send(title, body, priority="default", tags=["baseball", "chart"])


def send_lineup_change(game_pk: int, away_team: str, home_team: str,
                        delta_runs: float) -> bool:
    title = f"⚾ Lineup confirmed: {away_team} @ {home_team}"
    body = f"Projection moved {delta_runs:+.2f} runs after lineups confirmed."
    return _send(title, body, priority="low", tags=["baseball"])


def send_line_move(game_pk: int, away_team: str, home_team: str,
                    old_total: float, new_total: float) -> bool:
    title = f"⚾ Line moved: {away_team} @ {home_team}"
    body = f"Total moved {old_total} → {new_total}. New projection run triggered."
    return _send(title, body, priority="low")


def send_failure(job_name: str, error: str) -> bool:
    title = f"⚠ {job_name} FAILED"
    body = f"Error: {error[:500]}"
    return _send(title, body, priority="urgent", tags=["warning"])


def send_grader_summary(snapshot_date: str, perf: dict) -> bool:
    title = f"⚾ Last night graded · {perf.get('wins',0)}-{perf.get('losses',0)}"
    lines = [
        f"Date: {snapshot_date}",
        f"Flagged: {perf.get('flagged_plays', 0)} plays",
        f"Record: {perf.get('wins', 0)}-{perf.get('losses', 0)}-{perf.get('pushes', 0)}",
        f"Hit rate: {(perf.get('hit_rate') or 0) * 100:.1f}%",
        f"ROI: {(perf.get('roi') or 0) * 100:+.1f}%",
        f"Model MAE: {perf.get('model_mae', 0):.2f} vs Market MAE: {perf.get('market_mae', 0):.2f}",
    ]
    return _send(title, "\n".join(lines), priority="default")
