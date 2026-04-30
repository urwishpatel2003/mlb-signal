import sys
from src import db, orchestrator

# Wipe yesterday's pitcher prop grades (against fake estimated lines)
n = db.execute("""
    DELETE FROM edge_results
    WHERE edge_id IN (
      SELECT e.edge_id FROM edges e
      JOIN projection_runs pr ON pr.run_id = e.run_id
      WHERE pr.run_date = '2026-04-29' AND e.kind = 'prop'
    )
""")
print(f"Wiped April 29 prop grades")
db.execute("DELETE FROM model_performance")
print("Wiped model_performance")

# Now run today's orchestrator
result = orchestrator.run('initial')
print(f"Orchestrator result: {result}")
