"""
One-time bootstrap.

Run after Postgres is provisioned and DATABASE_URL is set:

    python -m scripts.bootstrap

What it does:
  1. Applies the migration in migrations/0001_initial_schema.sql
  2. Seeds the `teams` table (30 MLB clubs)
  3. Seeds the `parks` table for the current season

This is idempotent — safe to run multiple times. ON CONFLICT DO UPDATE
clauses ensure data is refreshed but not duplicated.
"""
from __future__ import annotations
import os
import sys
import logging
from pathlib import Path
from datetime import date

sys.path.insert(0, str(Path(__file__).parent.parent))

from src import db
from src.park_factors import seed_parks, seed_teams

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("bootstrap")


def apply_migrations() -> None:
    migrations_dir = Path(__file__).parent.parent / "migrations"
    sql_files = sorted(migrations_dir.glob("*.sql"))
    if not sql_files:
        log.warning("No migration files found in %s", migrations_dir)
        return

    for f in sql_files:
        log.info("Applying migration: %s", f.name)
        with open(f, "r") as fh:
            sql = fh.read()
        with db.conn() as c:
            c.execute(sql)
            c.commit()
        log.info("  ✓ %s applied", f.name)


def main():
    if not os.environ.get("DATABASE_URL"):
        log.error("DATABASE_URL not set")
        sys.exit(1)

    log.info("Step 1: applying migrations...")
    apply_migrations()

    log.info("Step 2: seeding teams (30 clubs)...")
    n = seed_teams()
    log.info("  ✓ %d team rows upserted", n)

    log.info("Step 3: seeding parks for %d...", date.today().year)
    n = seed_parks(date.today().year)
    log.info("  ✓ %d park rows upserted", n)

    log.info("Bootstrap complete. System is ready.")
    log.info("Next: trigger orchestrator manually with `python -m src.orchestrator --trigger=initial`")


if __name__ == "__main__":
    main()
