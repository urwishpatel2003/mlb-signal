"""
Worker service entry point.

Runs as a separate Railway service (mlb-signal-worker), distinct from the API.
Boots an APScheduler that fires the 4 jobs on schedule:

  06:00 ET daily            statcast_refresh
  09:00 ET daily            orchestrator (morning)
  every 30 min, 11AM-7PM ET orchestrator (line_watcher)
  04:00 ET daily            grader

Timezone is handled via zoneinfo so DST transitions in March/November don't
require manual schedule edits.

Each job runs inside a try/except so a crash in one job never takes down
the scheduler thread. Errors are logged but the scheduler keeps running.

Job exclusivity: max_instances=1 on each job means if a previous fire is
still running when the next one is due, the new fire is skipped (logged
as "missed"). That prevents pile-ups.
"""
from __future__ import annotations

import logging
import signal
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from . import grader, orchestrator, statcast_refresh

# Tiny HTTP server in a background thread so Railway's healthcheck passes.
# The scheduler is the actual workload - this is just to keep Railway happy.
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/api/health", "/health", "/"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"ok","service":"scheduler"}')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        # Quiet the default access logging
        return


def _run_health_server():
    port = int(os.environ.get("PORT", 8000))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    server.serve_forever()


# Boot the health server in a daemon thread
threading.Thread(target=_run_health_server, daemon=True).start()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("scheduler")

ET = ZoneInfo("America/New_York")


# ---------- Job wrappers ----------
# Every job is wrapped in try/except so apscheduler's thread never sees an
# unhandled exception. APScheduler does have its own error handling but this
# gives us cleaner logs and predictable behavior.

def job_statcast_refresh():
    log.info("[statcast_refresh] starting")
    try:
        result = statcast_refresh.refresh_statcast()
        log.info("[statcast_refresh] done: %s", result)
    except Exception as e:
        log.exception("[statcast_refresh] FAILED: %s", e)


def job_orchestrator_morning():
    log.info("[orchestrator_morning] starting")
    try:
        result = orchestrator.run(trigger="morning")
        log.info("[orchestrator_morning] done: %s", result)
    except Exception as e:
        log.exception("[orchestrator_morning] FAILED: %s", e)


def job_orchestrator_line_watcher():
    log.info("[line_watcher] starting")
    try:
        result = orchestrator.run(trigger="line_watcher")
        log.info("[line_watcher] done: %s", result)
    except Exception as e:
        log.exception("[line_watcher] FAILED: %s", e)


def job_grader():
    log.info("[grader] starting")
    try:
        result = grader.grade_yesterday()
        log.info("[grader] done: %s", result)
    except Exception as e:
        log.exception("[grader] FAILED: %s", e)


# ---------- Scheduler setup ----------

def build_scheduler() -> BlockingScheduler:
    sched = BlockingScheduler(timezone=ET)

    # 06:00 ET — Statcast refresh (refresh xstats for all pitchers + hitters)
    sched.add_job(
        job_statcast_refresh,
        trigger=CronTrigger(hour=6, minute=0, timezone=ET),
        id="statcast_refresh",
        name="Statcast refresh (06:00 ET)",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=600,  # 10 min - if scheduler was down briefly, still run
    )

    # 09:00 ET — Morning orchestrator (first slate run, ntfy push)
    sched.add_job(
        job_orchestrator_morning,
        trigger=CronTrigger(hour=9, minute=0, timezone=ET),
        id="orchestrator_morning",
        name="Orchestrator morning (09:00 ET)",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=600,
    )

    # Every 30 min, 11:00 to 19:30 ET — Line watcher
    # 11:00, 11:30, 12:00, 12:30 ... 19:00, 19:30
    sched.add_job(
        job_orchestrator_line_watcher,
        trigger=CronTrigger(hour="11-19", minute="0,30", timezone=ET),
        id="orchestrator_line_watcher",
        name="Line watcher (every 30 min, 11:00-19:30 ET)",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )

    # 04:00 ET — Grader (scores yesterday's plays after all games done)
    sched.add_job(
        job_grader,
        trigger=CronTrigger(hour=4, minute=0, timezone=ET),
        id="grader",
        name="Grader (04:00 ET)",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=1800,  # 30 min grace - grader is most important
    )

    return sched


def main():
    sched = build_scheduler()

    # Print the schedule on boot so we can see in logs that everything's
    # registered correctly
    log.info("Scheduler boot - %s ET", datetime.now(ET).strftime("%Y-%m-%d %H:%M"))
    for job in sched.get_jobs():
        log.info("  registered: %s | next run at %s", job.name, job.next_run_time)

    # Graceful shutdown on SIGTERM (Railway sends this on stop/restart)
    def shutdown(signum, frame):
        log.info("Received signal %d, shutting down scheduler", signum)
        sched.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    log.info("Scheduler running - press Ctrl+C to stop")
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped")


if __name__ == "__main__":
    main()
